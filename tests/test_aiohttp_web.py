"""aiohttp's own web server: the same lost update recorded with deja_middleware and replayed by the aiohttp harness."""

import asyncio
import os

import pytest

aiohttp = pytest.importorskip("aiohttp")
from aiohttp import web  # noqa: E402

from deja import recording_loop  # noqa: E402
from deja.aiohttp_web import deja_middleware  # noqa: E402
from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.replay import replay  # noqa: E402


def make_app(directory="deja-recordings"):
    stock = {"widget": 1}

    async def buy(request):
        body = await request.json()
        n = stock["widget"]
        await asyncio.sleep(0.01)
        stock["widget"] = n - body["qty"]
        return web.json_response({"left": stock["widget"]})

    app = web.Application(middlewares=[deja_middleware(directory=directory)])
    app.router.add_post("/buy", buy)
    return app


def test_aiohttp_lost_update_recorded_and_replayed(tmp_path):
    path = os.path.join(str(tmp_path), "aio.deja")
    out = {}

    async def main():
        runner = web.AppRunner(make_app(str(tmp_path)), access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        rec = asyncio.get_running_loop().deja
        async with aiohttp.ClientSession() as s:
            marker = rec.mark("t")

            async def call():
                async with s.post(f"http://127.0.0.1:{port}/buy", json={"qty": 1}) as r:
                    return await r.json()

            out["r"] = await asyncio.gather(call(), call())
            rec.dump(path, since=marker)
        await runner.cleanup()

    asyncio.run(main(), loop_factory=recording_loop)
    assert out["r"] == [{"left": 0}, {"left": 0}]
    c = analyze(*read(path))["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]"
    for _ in range(3):
        r = replay(path, lambda: make_app(str(tmp_path / "replay")), deadline_s=5)
        assert r.followed and r.same_responses, r.summary()


def quote_app():
    """A FastAPI app that calls a pricing service with the aiohttp client."""
    from fastapi import FastAPI

    app = FastAPI()
    calls = []

    @app.get("/quote/{item}")
    async def quote(item: str):
        async with aiohttp.ClientSession(base_url="http://127.0.0.1:1") as s:  # nothing listens there in replay
            async with s.get(f"/price/{item}", params={"cur": "eur"}) as r:
                r.raise_for_status()
                price = (await r.json())["price"]
                text = await r.text()
        calls.append(item)
        return {"item": item, "price": price, "raw": text}

    return app


def test_aiohttp_client_calls_replay_without_the_network(tmp_path, monkeypatch):
    import httpx

    import deja

    async def fake_upstream(port_holder):
        async def price(request):
            return web.json_response({"price": len(request.match_info["item"]) * 10, "cur": request.query["cur"]})

        app = web.Application()
        app.router.add_get("/price/{item}", price)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port_holder.append(site._server.sockets[0].getsockname()[1])
        return runner

    out = {}

    async def main():
        ports = []
        runner = await fake_upstream(ports)
        app = quote_app()
        # point the app at the live upstream while recording
        orig = aiohttp.ClientSession.__init__

        def init(self, *a, base_url=None, **kw):
            orig(self, *a, base_url=f"http://127.0.0.1:{ports[0]}" if base_url else None, **kw)

        monkeypatch.setattr(aiohttp.ClientSession, "__init__", init)
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            m = rec.mark("t")
            out["r"] = await asyncio.gather(c.get("/quote/tea"), c.get("/quote/coffee"))
            out["path"] = rec.dump(str(tmp_path / "quote.deja"), since=m)
        await runner.cleanup()  # the upstream is gone for the replay (same URL, nothing listening)

    asyncio.run(main(), loop_factory=recording_loop)
    assert [x.json()["price"] for x in out["r"]] == [30, 60]
    r = replay(out["path"], quote_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
