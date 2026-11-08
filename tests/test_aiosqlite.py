"""aiosqlite: a lost update on a counter row, found and replayed after the database file is gone."""

import asyncio
import contextlib
import os

import pytest
from fastapi import FastAPI

aiosqlite = pytest.importorskip("aiosqlite")

from test_streams import record_concurrently  # noqa: E402

from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.replay import replay  # noqa: E402


def counter_app(path):
    def factory():
        @contextlib.asynccontextmanager
        async def lifespan(app):
            async with aiosqlite.connect(path) as db:
                await db.execute("CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, n INTEGER)")
                await db.execute("INSERT OR REPLACE INTO counters VALUES ('visits', 0)")
                await db.commit()
            yield

        app = FastAPI(lifespan=lifespan)

        @app.post("/visit")
        async def visit():
            async with aiosqlite.connect(path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute("SELECT n FROM counters WHERE name = ?", ("visits",)) as cur:
                    row = await cur.fetchone()
                await asyncio.sleep(0.01)
                await db.execute("UPDATE counters SET n = :n WHERE name = :name", {"n": row["n"] + 1, "name": "visits"})
                await db.commit()
                rows = await db.execute_fetchall("SELECT name, n FROM counters")
            return {"n": row["n"] + 1, "all": [list(r) for r in rows]}

        return app

    return factory


def test_aiosqlite_lost_update_found_and_replayed_without_the_file(tmp_path):
    db = str(tmp_path / "app.db")
    path, responses = record_concurrently(counter_app(db), [("POST", "/visit"), ("POST", "/visit")],
                                          str(tmp_path / "s.deja"))
    assert [r.json()["n"] for r in responses] == [1, 1], "two visits, counted once"
    a = analyze(*read(path))
    c = a["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "counters[name=visits]", a["story"]
    os.remove(db)
    r = replay(path, counter_app(db), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
    assert not os.path.exists(db), "replay must not open the database"
