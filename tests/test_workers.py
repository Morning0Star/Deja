"""Two worker processes (here: two threads, each with its own loop, recorder and app) double-book through a shared
database. Neither worker's recording alone shows the race; the group's merged recording does, and replays."""

import asyncio
import contextlib
import glob
import os
import threading

import httpx
import pytest
from fastapi import FastAPI

asyncpg = pytest.importorskip("asyncpg")

import deja  # noqa: E402
from deja import recording_loop  # noqa: E402
from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.group import merge  # noqa: E402
from deja.replay import replay  # noqa: E402
from test_adapters import DEAD_PORT_DSN, PG_DSN, reachable  # noqa: E402

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", int(PG_DSN.rsplit(":", 1)[1].split("/")[0])),
                                reason="test Postgres not running")


def booking_app(dsn):
    state = {}

    @contextlib.asynccontextmanager
    async def lifespan(app):
        state["pool"] = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        yield
        await state["pool"].close()

    app = FastAPI(lifespan=lifespan)

    @app.post("/book/{room}")
    async def book(room: int):
        async with state["pool"].acquire() as c:
            taken = await c.fetchval("SELECT count(*) FROM deja_wbook WHERE room = $1", room)
            if taken:
                return {"ok": False}
            await asyncio.sleep(0.05)
            await c.execute("INSERT INTO deja_wbook (room) VALUES ($1)", room)
            if await c.fetchval("SELECT count(*) FROM deja_wbook WHERE room = $1", room) > 1:
                deja.flag("room booked twice")
        return {"ok": True}

    return app


def test_race_across_workers_is_found_in_the_merged_recording(tmp_path):
    async def setup():
        c = await asyncpg.connect(PG_DSN)
        await c.execute("CREATE TABLE IF NOT EXISTS deja_wbook (id serial PRIMARY KEY, room int)")
        await c.execute("TRUNCATE deja_wbook")
        await c.close()

    asyncio.run(setup())
    group, barrier, answers = str(tmp_path / "group"), threading.Barrier(2), {}

    def worker(name):
        async def main():
            app = booking_app(PG_DSN)
            app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path / name), group=group, worker=name)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
                    await c.post(f"/book/{100 + int(name[1])}")  # warm up (first request starts the watcher)
                    barrier.wait()
                    answers[name] = (await c.post("/book/7")).json()
                    await asyncio.sleep(1.5)  # the watchers answer the group's requests

        asyncio.run(main(), loop_factory=recording_loop)

    threads = [threading.Thread(target=worker, args=(n,)) for n in ("w1", "w2")]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert answers == {"w1": {"ok": True}, "w2": {"ok": True}}, "room 7 booked through both workers"
    folders = [d for d in glob.glob(os.path.join(group, "*")) if len(glob.glob(os.path.join(d, "*.deja"))) == 2]
    assert folders, os.listdir(group)
    path = merge(glob.glob(os.path.join(folders[0], "*.deja")), str(tmp_path / "merged.deja"))
    a = analyze(*read(path))
    c = next(c for c in a["conflicts"] if "deja_wbook" in c["resource"])
    assert c["pattern"] == "check-then-act" and {c["a"][1:], c["b"][1:]} != {c["a"][1:]}, a["story"]
    r = replay(path, lambda: booking_app(DEAD_PORT_DSN), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
