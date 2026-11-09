"""Against a real Postgres: a check-then-insert inside transactions. Each request's insert only becomes visible when
it commits, so both checks find nothing and both insert. The report has to know that; replay runs without it."""

import asyncio
import contextlib

import pytest
from fastapi import FastAPI

asyncpg = pytest.importorskip("asyncpg")  # not on PyPy

from test_adapters import DEAD_PORT_DSN, PG_DSN, reachable  # noqa: E402
from test_streams import record_concurrently  # noqa: E402

from deja import sqltouch  # noqa: E402
from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.replay import replay  # noqa: E402

pytestmark = pytest.mark.skipif(
    not reachable("127.0.0.1", int(PG_DSN.rsplit(":", 1)[1].split("/")[0])), reason="test Postgres not running"
)


def booking_app(dsn):
    def factory():
        state = {}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["pool"] = await asyncpg.create_pool(dsn, min_size=2, max_size=2)
            async with state["pool"].acquire() as conn:
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS deja_bookings (id serial PRIMARY KEY, room int, day int)"
                )
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
    path, responses = record_concurrently(
        booking_app(PG_DSN), [("POST", "/book/5/10"), ("POST", "/book/5/10")], str(tmp_path / "book.deja")
    )
    assert [r.json() for r in responses] == [{"ok": True}, {"ok": True}], "both booked room 5 for day 10"
    a = analyze(*read(path))
    c = a["conflicts"][0]
    # without sqlglot the simple SQL rules name a row by its first key only
    row = "deja_bookings[room=5, day=10]" if sqltouch.available() else "deja_bookings[room=5]"
    assert c["pattern"] == "check-then-act" and c["resource"] == row, a["story"]
    assert c["confidence"] == "certain"
    r = replay(path, booking_app(DEAD_PORT_DSN), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def notice_app(dsn):
    """A request runs a statement that raises NOTICEs; a log listener collects them; another request reads them."""

    def factory():
        state = {"notes": []}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["conn"] = await asyncpg.connect(dsn)
            state["conn"].add_log_listener(lambda conn, msg: state["notes"].append(f"{msg.severity}: {msg.message}"))
            yield
            await state["conn"].close()

        app = FastAPI(lifespan=lifespan)

        @app.post("/migrate")
        async def migrate():
            await state["conn"].execute("DO $$ BEGIN RAISE NOTICE 'step %', 1; RAISE WARNING 'careful'; END $$")
            return {}

        @app.get("/notes")
        async def notes():
            for _ in range(200):  # until both server messages arrived (slow under coverage tracing)
                if len(state["notes"]) >= 2:
                    break
                await asyncio.sleep(0.01)
            return {"notes": list(state["notes"])}

        return app

    return factory


def test_server_log_messages_replay_without_the_database(tmp_path):
    path, responses = record_concurrently(
        notice_app(PG_DSN), [("POST", "/migrate"), ("GET", "/notes")], str(tmp_path / "notice.deja")
    )
    assert responses[1].json()["notes"] == ["NOTICE: step 1", "WARNING: careful"]
    r = replay(path, notice_app(DEAD_PORT_DSN), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def kill_app(dsn):
    def factory():
        state = {"closed": False}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["conn"], state["admin"] = await asyncpg.connect(dsn), await asyncpg.connect(dsn)
            state["conn"].add_termination_listener(lambda conn: state.update(closed=True))
            yield
            await state["admin"].close()

        app = FastAPI(lifespan=lifespan)

        @app.post("/kill")
        async def kill():
            await state["admin"].execute("SELECT pg_terminate_backend($1)", state["conn"].get_server_pid())
            return {}

        @app.get("/closed")
        async def closed():
            for _ in range(100):
                if state["closed"]:
                    return {"closed": True}
                await asyncio.sleep(0.01)
            return {"closed": False}

        return app

    return factory


def test_termination_listener_replays_without_the_database(tmp_path):
    path, responses = record_concurrently(
        kill_app(PG_DSN), [("POST", "/kill"), ("GET", "/closed")], str(tmp_path / "kill.deja")
    )
    assert responses[1].json() == {"closed": True}
    r = replay(path, kill_app(DEAD_PORT_DSN), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
