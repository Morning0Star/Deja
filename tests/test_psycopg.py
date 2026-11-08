"""psycopg 3 (async): a check-then-insert recorded against Postgres, found, and replayed with no database."""

import asyncio
import contextlib

import pytest
from fastapi import FastAPI

psycopg = pytest.importorskip("psycopg")
psycopg_pool = pytest.importorskip("psycopg_pool")

import httpx  # noqa: E402
from test_adapters import PG_DSN, reachable  # noqa: E402

import deja  # noqa: E402
from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.loop import RecordingSelectorLoop  # noqa: E402
from deja.replay import replay  # noqa: E402

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", int(PG_DSN.rsplit(":", 1)[1].split("/")[0])),
                                reason="test Postgres not running")
DEAD = "postgresql://nobody@127.0.0.1:1/none"


def record_concurrently(factory, calls, path):
    """psycopg's async mode needs a selector loop (on Windows the default is the proactor)."""
    import os

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

    asyncio.run(main(), loop_factory=RecordingSelectorLoop)
    return out["path"], out["responses"]


def signup_app(dsn, pooled):
    def factory():
        state = {}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            if pooled:
                size = 1 if pooled == "tight" else 2  # tight: requests wait for the one connection
                state["pool"] = psycopg_pool.AsyncConnectionPool(dsn, min_size=size, max_size=size, open=False)
                await state["pool"].open()
            async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as c:
                await c.execute("CREATE TABLE IF NOT EXISTS deja_users (name text, email text)")
                await c.execute("TRUNCATE deja_users")
            yield
            if pooled:
                await state["pool"].close()

        app = FastAPI(lifespan=lifespan)

        @contextlib.asynccontextmanager
        async def conn():
            if pooled:
                async with state["pool"].connection() as c:
                    yield c
            else:
                async with await psycopg.AsyncConnection.connect(dsn) as c:
                    yield c

        @app.post("/signup/{name}")
        async def signup(name: str):
            async with conn() as c:
                async with c.cursor() as cur:
                    await cur.execute("SELECT email FROM deja_users WHERE name = %s", (name,))
                    taken = await cur.fetchone()
                    if taken:
                        return {"ok": False}
                    await asyncio.sleep(0.02)
                    await cur.execute("INSERT INTO deja_users (name, email) VALUES (%(n)s, %(e)s)",
                                      {"n": name, "e": f"{name}@x"})
                    return {"ok": True, "rows": cur.rowcount}

        return app

    return factory


@pytest.mark.parametrize("pooled", [False, True, "tight"])
def test_psycopg_check_then_insert_found_and_replayed(tmp_path, pooled):
    calls = [("POST", "/signup/ada"), ("POST", "/signup/ada")] + ([("POST", "/signup/bob")] if pooled == "tight" else [])
    path, responses = record_concurrently(signup_app(PG_DSN, pooled), calls, str(tmp_path / "p.deja"))
    if pooled != "tight":
        assert [r.json() for r in responses] == [{"ok": True, "rows": 1}] * 2
    a = analyze(*read(path))
    r = replay(path, signup_app(DEAD, pooled), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
    if pooled == "tight":
        return  # one connection at a time: the requests ran one after the other, no race to find
    c = a["conflicts"][0]
    assert c["pattern"] == "check-then-act" and c["resource"] == "deja_users[name=ada]", a["story"]
    r = replay(path, signup_app(DEAD, pooled), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def sync_app(dsn):
    """Blocking psycopg in a plain def endpoint (a worker thread)."""
    def factory():
        app = FastAPI()

        @app.post("/visit/{page}")
        def visit(page: str):
            with psycopg.connect(dsn) as c:
                with c.cursor() as cur:
                    cur.execute("SELECT count(*) FROM deja_users WHERE name = %s", (page,))
                    n = cur.fetchone()[0]
                    cur.execute("INSERT INTO deja_users (name, email) VALUES (%s, %s)", (page, "v"))
            return {"before": n}

        return app

    return factory


def test_sync_psycopg_in_threads_replays_without_the_database(tmp_path):
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        c.execute("CREATE TABLE IF NOT EXISTS deja_users (name text, email text)")
        c.execute("DELETE FROM deja_users WHERE name = 'home'")
    path, responses = record_concurrently(sync_app(PG_DSN), [("POST", "/visit/home"), ("POST", "/visit/home")],
                                          str(tmp_path / "s.deja"))
    assert sorted(r.json()["before"] for r in responses) in ([0, 0], [0, 1])
    r = replay(path, sync_app(DEAD), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
