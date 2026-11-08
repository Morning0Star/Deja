"""Streams and pushed events: streaming responses (the app's and upstream ones read with httpx) and Postgres
LISTEN/NOTIFY. Each is recorded, then replayed with the network / database unreachable."""

import asyncio
import os

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from test_adapters import DEAD_PORT_DSN, PG_DSN, reachable

import deja
from deja import recording_loop
from deja.fileformat import read
from deja.replay import replay


def record_concurrently(factory, calls, path):
    out = {}

    async def main():
        app = factory()
        app.add_middleware(deja.DejaMiddleware, directory=os.path.dirname(path))
        rec = asyncio.get_running_loop().deja
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
                marker = rec.mark("test")
                out["responses"] = await asyncio.gather(*(client.request(m, u) for m, u in calls))
                out["path"] = rec.dump(path, since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    return out["path"], out["responses"]


def test_streaming_response_replays(tmp_path):
    def factory():
        app = FastAPI()

        @app.get("/s")
        async def s():
            async def gen():
                for i in range(3):
                    await asyncio.sleep(0.01)
                    yield f"chunk{i}\n"

            return StreamingResponse(gen())

        return app

    path, responses = record_concurrently(factory, [("GET", "/s"), ("GET", "/s")], str(tmp_path / "s.deja"))
    assert [r.text for r in responses] == ["chunk0\nchunk1\nchunk2\n"] * 2
    r = replay(path, factory, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def token_app(upstream):
    """Relays an upstream token stream (as an LLM SDK reads one) and keeps a shared count of tokens seen."""

    def factory():
        app = FastAPI()
        seen = {"tokens": 0}

        @app.get("/ask")
        async def ask():
            words = []
            async with httpx.AsyncClient(transport=httpx.MockTransport(upstream), base_url="http://llm") as c:
                async with c.stream("POST", "/v1/stream", json={"q": "hi"}) as resp:
                    async for chunk in resp.aiter_text():
                        words.append(chunk)
                        n = seen["tokens"]
                        await asyncio.sleep(0)  # work between chunks: the two requests interleave here
                        seen["tokens"] = n + 1
            return {"text": "".join(words), "status": resp.status_code, "seen": seen["tokens"]}

        return app

    return factory


def test_httpx_stream_replays_chunk_by_chunk_without_the_network(tmp_path):
    calls = []

    async def tokens():
        for w in ("Hel", "lo ", "there"):
            await asyncio.sleep(0.01)
            yield w.encode()

    def upstream(request):
        calls.append(request.url.path)
        return httpx.Response(200, headers={"content-type": "text/plain"}, content=tokens())

    def unreachable(request):
        raise AssertionError("replay must not call the upstream")

    path, responses = record_concurrently(token_app(upstream), [("GET", "/ask"), ("GET", "/ask")],
                                          str(tmp_path / "stream.deja"))
    bodies = [r.json() for r in responses]
    assert [b["text"] for b in bodies] == ["Hello there"] * 2 and calls == ["/v1/stream"] * 2
    ops = [e[2] for e in read(path)[1] if e[0] == "O"]
    assert ops.count("http.chunk") == 8, ops  # 3 chunks + end of body, per request
    for _ in range(3):
        r = replay(path, token_app(unreachable), deadline_s=5)
        assert r.followed and r.same_responses, r.summary()


pg = pytest.mark.skipif(not reachable("127.0.0.1", int(PG_DSN.rsplit(":", 1)[1].split("/")[0])),
                        reason="test Postgres not running")


def notify_app(dsn):
    """GET /wait blocks until a notification arrives on a LISTEN connection; POST /send sends one."""

    def factory():
        import asyncpg

        app = FastAPI()
        state = {}

        @app.on_event("startup")
        async def start():
            state["pool"] = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
            state["listen"] = await asyncpg.connect(dsn)
            state["inbox"] = asyncio.Queue()

            def on_note(conn, pid, channel, payload):
                state["inbox"].put_nowait(payload)

            await state["listen"].add_listener("deja_events", on_note)

        @app.on_event("shutdown")
        async def stop():
            await state["listen"].close()
            await state["pool"].close()

        @app.get("/wait")
        async def wait():
            got = await asyncio.wait_for(state["inbox"].get(), 5)
            return {"got": got}

        @app.post("/send")
        async def send():
            await asyncio.sleep(0.05)  # let /wait start waiting first
            await state["pool"].execute("SELECT pg_notify('deja_events', $1)", "order-7 paid")
            return {"sent": True}

        return app

    return factory


@pg
def test_listen_notify_replays_without_the_database(tmp_path):
    path, responses = record_concurrently(notify_app(PG_DSN), [("GET", "/wait"), ("POST", "/send")],
                                          str(tmp_path / "notify.deja"))
    assert responses[0].json() == {"got": "order-7 paid"}, [r.text for r in responses]
    for _ in range(3):
        r = replay(path, notify_app(DEAD_PORT_DSN), deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
