"""deja shrink: a recording with unrelated requests shrinks to the racing pair; a request the race depends on stays."""

import asyncio
import os

import httpx
from fastapi import FastAPI

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay
from deja.shrink import shrink


def make_app():
    app = FastAPI()
    stock = {"widget": 0}
    hits = {"n": 0}

    @app.post("/restock")
    async def restock():  # the race needs this: without it the widget is out of stock
        stock["widget"] = 1
        return {}

    @app.get("/ping")
    async def ping():  # unrelated
        hits["n"] += 1
        await asyncio.sleep(0.005)
        return {"pong": hits["n"]}

    @app.post("/buy")
    async def buy():
        n = stock["widget"]
        await asyncio.sleep(0.01)
        if n > 0:
            stock["widget"] = n - 1
            return {"ok": True}
        return {"ok": False}

    return app


def test_shrink_keeps_only_what_the_race_needs(tmp_path):
    out = {}

    async def main():
        app = make_app()
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            m = rec.mark("t")
            await c.get("/ping")
            await c.post("/restock")
            await asyncio.gather(c.get("/ping"), c.post("/buy"), c.post("/buy"), c.get("/ping"))
            out["path"] = rec.dump(os.path.join(str(tmp_path), "big.deja"), since=m)

    asyncio.run(main(), loop_factory=recording_loop)
    small = str(tmp_path / "small.deja")
    got = shrink(out["path"], make_app, small, deadline_s=5)
    assert got["kept"] == ["q1", "q3", "q4"], got  # restock and the two purchases
    assert sorted(got["removed"]) == ["q0", "q2", "q5"]
    c = analyze(*read(small))["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]"
    r = replay(small, make_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
