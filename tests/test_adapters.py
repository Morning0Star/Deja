"""M3: I/O adapters. Each test records an app doing real I/O, then replays it with the I/O unreachable."""

import asyncio
import json
import os
import random
import socket

import httpx
import pytest
from fastapi import FastAPI

import deja
from deja import codec, recording_loop
from deja.replay import Schedule, replay

PG_DSN = os.environ.get("DEJA_TEST_PG_DSN", "postgresql://deja:deja@127.0.0.1:55432/deja")
REDIS_URL = os.environ.get("DEJA_TEST_REDIS_URL", "redis://127.0.0.1:56379/0")
DEAD_PORT_DSN = "postgresql://nobody@127.0.0.1:1/none"


def reachable(host, port):
    try:
        socket.create_connection((host, port), timeout=0.5).close()
        return True
    except OSError:
        return False


def record_requests(app_factory, calls, tmp_path, name="rec.deja"):
    """Run `calls` against the app on a recording loop (through the middleware) and save the whole run."""
    out = {}

    async def main():
        app = app_factory()
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://app") as client:
                marker = rec.mark("test")
                out["responses"] = [await client.request(m, u) for m, u in calls]
                out["path"] = rec.dump(str(tmp_path / name), since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    return out["path"], out["responses"]


# ---------------------------------------------------------------------- codec
def test_codec_round_trip():
    import datetime as dt
    import decimal
    import uuid

    values = [None, 1, 2.5, "x", b"\x00", dt.datetime(2026, 11, 7, 1, 2, 3), dt.date(2026, 1, 1),
              decimal.Decimal("10.50"), uuid.uuid4(), [1, (2, 3)], {"a": [1]},
              codec.FakeRecord(["id", "name"], [1, "ada"])]
    for v in values:
        assert codec.decode(json.loads(json.dumps(codec.encode(v)))) == v
    rec = codec.decode(codec.encode(codec.FakeRecord(["id", "name"], [1, "ada"])))
    assert rec["name"] == "ada" and rec[0] == 1 and list(rec.keys()) == ["id", "name"]


# ---------------------------------------------------------------------- httpx
def make_pricing_app(calls):
    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"price": random.randint(1, 10_000)})

    def factory():
        app = FastAPI()

        @app.get("/quote")
        async def quote():
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://pricing") as c:
                r = await c.get("/price")
            return {"quoted": r.json()["price"]}

        return app

    return factory


def test_httpx_calls_replay_from_the_recording(tmp_path):
    calls = []
    factory = make_pricing_app(calls)
    path, responses = record_requests(factory, [("GET", "/quote")], tmp_path)
    recorded_price = responses[0].json()["quoted"]
    assert calls == ["/price"]

    for _ in range(5):
        r = replay(path, factory, deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
        assert json.loads(next(iter(r.replayed.values()))[1])["quoted"] == recorded_price
    assert calls == ["/price"], "replay must not call the pricing service"


# ---------------------------------------------------------------------- asyncpg
@pytest.mark.skipif(not reachable("127.0.0.1", 55432), reason="test Postgres not running")
def test_postgres_replays_with_the_database_unreachable(tmp_path, monkeypatch):
    from gallery import _pg, wallet

    monkeypatch.setattr(_pg, "DSN", PG_DSN)
    path, responses = record_requests(
        wallet.create_app,
        [("POST", "/reset"), ("POST", "/purchase"), ("GET", "/check")],
        tmp_path,
    )
    assert any(e for e in Schedule.load(path).io.values()), "the recording should contain Postgres calls"

    monkeypatch.setattr(_pg, "DSN", DEAD_PORT_DSN)  # nothing listens there: any real connection would fail
    r = replay(path, wallet.create_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


# ---------------------------------------------------------------------- redis
@pytest.mark.skipif(not reachable("127.0.0.1", 56379), reason="test Redis not running")
def test_redis_lost_update_replays_without_redis(tmp_path):
    import redis.asyncio as aioredis

    state = {"url": REDIS_URL}

    def factory():
        app = FastAPI()
        client = aioredis.from_url(state["url"])

        @app.post("/reset")
        async def reset():
            await client.set("views", 0)
            return {"ok": True}

        @app.post("/view")
        async def view():  # lost update: GET, await, SET
            n = int(await client.get("views") or 0)
            await asyncio.sleep(0)
            await client.set("views", n + 1)
            return {"views": n + 1}

        @app.get("/count")
        async def count():
            return {"views": int(await client.get("views") or 0)}

        return app

    path, responses = record_requests(factory, [("POST", "/reset"), ("POST", "/view"), ("POST", "/view"),
                                                ("GET", "/count")], tmp_path)
    assert responses[-1].json() == {"views": 2}
    ops = [e.op for q in Schedule.load(path).io.values() for e in q]
    assert ops.count("redis.cmd") >= 6, ops  # set, get+set twice, get

    state["url"] = "redis://127.0.0.1:1/0"  # unreachable during replay
    r = replay(path, factory, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def test_changed_query_is_reported(tmp_path):
    calls = []
    factory = make_pricing_app(calls)
    path, _ = record_requests(factory, [("GET", "/quote")], tmp_path)

    def changed():
        app = FastAPI()

        @app.get("/quote")
        async def quote():
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)),
                                         base_url="http://pricing") as c:
                await c.get("/price?currency=EUR")  # different request than recorded
            return {"quoted": 0}

        return app

    r = replay(path, changed, deadline_s=3)
    assert not r.followed
    assert "I/O MISMATCH" in r.summary()


# ---------------------------------------------------------------------- worker threads (run_in_executor)
@pytest.mark.parametrize("delay", [0, 0.05], ids=["instant", "slow"])
def test_worker_thread_results_replay_without_running_threads(tmp_path, delay):
    """Instant functions may finish before the await (no suspension on newer CPython); slow ones always suspend.
    Both shapes must replay step for step without running the function."""
    import time as _t

    calls = []

    def slow_lookup(x):
        if delay:
            _t.sleep(delay)
        calls.append(x)
        return {"x": x, "n": random.randint(1, 10_000)}

    def factory():
        app = FastAPI()

        @app.get("/lookup")
        async def lookup():
            r = await asyncio.get_running_loop().run_in_executor(None, slow_lookup, 7)
            return r

        return app

    path, responses = record_requests(factory, [("GET", "/lookup")], tmp_path)
    recorded = responses[0].json()
    assert calls == [7]
    for _ in range(5):
        r = replay(path, factory, deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
        assert json.loads(next(iter(r.replayed.values()))[1]) == recorded
    assert calls == [7], "replay must not run the worker-thread function"
