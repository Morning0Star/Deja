"""Against a real Postgres: a check-then-insert inside transactions. Each request's insert only becomes visible when
it commits, so both checks find nothing and both insert. The report has to know that; replay runs without it."""

import asyncio
import contextlib

import asyncpg
import pytest
from fastapi import FastAPI

from test_adapters import DEAD_PORT_DSN, PG_DSN, reachable
from test_streams import record_concurrently
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", int(PG_DSN.rsplit(":", 1)[1].split("/")[0])),
                                reason="test Postgres not running")


def booking_app(dsn):
    def factory():
        state = {}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["pool"] = await asyncpg.create_pool(dsn, min_size=2, max_size=2)
            async with state["pool"].acquire() as conn:
                await conn.execute("CREATE TABLE IF NOT EXISTS deja_bookings (id serial PRIMARY KEY, room int, day int)")
                await conn.execute("TRUNCATE deja_bookings")
            yield
            await state["pool"].close()

        app = FastAPI(lifespan=lifespan)

        @app.post("/book/{room}/{day}")
        async def book(room: int, day: int):
            async with state["pool"].acquire() as conn:
                async with conn.transaction():
                    taken = await conn.fetch("SELECT id FROM deja_bookings WHERE room = $1 AND day = $2", room, day)
                    if taken:
                        return {"ok": False}
                    await conn.execute("INSERT INTO deja_bookings (room, day) VALUES ($1, $2)", room, day)
                    await asyncio.sleep(0.03)  # e.g. charging a card before the transaction commits
            return {"ok": True}

        return app

    return factory


def test_double_booking_inside_transactions_is_found_and_replays_without_the_database(tmp_path):
    path, responses = record_concurrently(booking_app(PG_DSN), [("POST", "/book/5/10"), ("POST", "/book/5/10")],
                                          str(tmp_path / "book.deja"))
    assert [r.json() for r in responses] == [{"ok": True}, {"ok": True}], "both booked room 5 for day 10"
    a = analyze(*read(path))
    c = a["conflicts"][0]
    assert c["pattern"] == "check-then-act" and c["resource"] == "deja_bookings[room=5, day=10]", a["story"]
    assert c["confidence"] == "certain"
    r = replay(path, booking_app(DEAD_PORT_DSN), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
