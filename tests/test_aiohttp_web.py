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
