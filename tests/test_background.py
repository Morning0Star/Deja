"""Background tasks the app starts at startup race with requests: the race is recorded, explained and replayed."""

import asyncio
import contextlib
import os

import httpx
from fastapi import FastAPI

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay


def make_app():
    stock = {"widget": 5}

    async def restock():  # a background job: tops the stock up every so often
        while True:
            await asyncio.sleep(0.05)
            n = stock["widget"]
            await asyncio.sleep(0.1)  # e.g. asks the warehouse how many to add (a wide window: no timing luck)
            stock["widget"] = n + 10

    @contextlib.asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(restock())
        yield
        task.cancel()

    app = FastAPI(lifespan=lifespan)

    @app.get("/ping")
    async def ping():  # a first request: Déjà starts tracking the app's state when one arrives
        return {}

    @app.post("/buy")
    async def buy():
        n = stock["widget"]
        await asyncio.sleep(0.02)
        stock["widget"] = n - 1
        return {"left": stock["widget"]}

    return app


async def lifespan(app, state):
    """Drive the ASGI lifespan (through the middleware, as uvicorn does)."""
    inbox, outbox = asyncio.Queue(), asyncio.Queue()
    task = asyncio.ensure_future(
        app({"type": "lifespan", "asgi": {"version": "3.0"}, "state": state}, inbox.get, outbox.put)
    )
    await inbox.put({"type": "lifespan.startup"})
    assert (await outbox.get())["type"] == "lifespan.startup.complete"
    return task, inbox


def test_request_and_background_task_race(tmp_path):
    out = {}

    async def main():
        app = make_app()
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        loop = asyncio.get_running_loop()
        started = loop.time()  # the job starts with the app; its 2nd round reads at +200 ms and writes at +300 ms
        task, inbox = await lifespan(app, {})
        rec = loop.deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            await c.get("/ping")  # as in a running app, an earlier request has switched tracking on
            marker = rec.mark("t")
            await asyncio.sleep(max(0.0, started + 0.23 - loop.time()))  # the job is between its read and write
            out["r"] = await c.post("/buy")
            await asyncio.sleep(0.15)  # until after the job's write
            out["path"] = rec.dump(os.path.join(str(tmp_path), "bg.deja"), since=marker)
        await inbox.put({"type": "lifespan.shutdown"})

    asyncio.run(main(), loop_factory=recording_loop)
    a = analyze(*read(out["path"]))
    assert any(r["method"] == "TASK" and r["path"].endswith("restock") for r in a["requests"])
    c = next(c for c in a["conflicts"] if "b0" in (c["a"], c["b"]))
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]", a["story"]
    assert "background task" in c["story"]
    r = replay(out["path"], make_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
