"""SQLAlchemy (async ORM over asyncpg), cursors and bulk copy: record against Postgres, replay with it unreachable."""

import asyncio
import io
import os

import httpx
import pytest
from fastapi import FastAPI
from test_adapters import DEAD_PORT_DSN, PG_DSN, reachable

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay

sa = pytest.importorskip("sqlalchemy")
from sqlalchemy import Integer, select  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column  # noqa: E402

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", int(PG_DSN.rsplit(":", 1)[1].split("/")[0])),
                                reason="test Postgres not running")


class Base(DeclarativeBase):
    pass


class Wallet(Base):
    __tablename__ = "deja_sa_wallet"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    balance: Mapped[int] = mapped_column(Integer)


def orm_app(dsn):
    def factory():
        engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://"))
        Session = async_sessionmaker(engine, expire_on_commit=False)
        app = FastAPI()

        @app.on_event("startup")
        async def start():
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            async with Session() as s, s.begin():
                w = await s.get(Wallet, 1)
                if w is None:
                    s.add(Wallet(id=1, balance=100))
                else:
                    w.balance = 100

        @app.on_event("shutdown")
        async def stop():
            await engine.dispose()

        @app.post("/buy")
        async def buy():
            async with Session() as s:
                w = await s.get(Wallet, 1)  # read ...
                before = w.balance
                await asyncio.sleep(0.3)  # ... think (long enough for the other buyer to read too) ...
                w.balance = before - 60  # ... write: two buyers both start from 100
                await s.commit()
                return {"before": before, "after": w.balance}

        @app.get("/wallets")
        async def wallets():
            async with Session() as s:
                rows = (await s.scalars(select(Wallet).where(Wallet.balance > 0).order_by(Wallet.id))).all()
                return [{"id": w.id, "balance": w.balance} for w in rows]

        return app

    return factory


def record_concurrently(factory, calls, path):
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

    asyncio.run(main(), loop_factory=recording_loop)
    return out["path"], out["responses"]


def test_sqlalchemy_orm_lost_update_replays_without_the_database(tmp_path):
    path, responses = record_concurrently(orm_app(PG_DSN), [("POST", "/buy"), ("POST", "/buy")],
                                          str(tmp_path / "orm.deja"))
    bodies = [r.json() for r in responses]
    assert all(r.status_code == 200 for r in responses), bodies

    for _ in range(5):
        r = replay(path, orm_app(DEAD_PORT_DSN), deadline_s=10)
        assert r.followed and r.same_responses, r.summary()

    assert [b["before"] for b in bodies] == [100, 100], "both buyers read before either saved: the race happened"
    a = analyze(*read(path))
    c = a["conflicts"][0]  # explained from the SQL SQLAlchemy generated, with no app hints
    assert c["pattern"] == "lost-update" and c["resource"] == "deja_sa_wallet[id=1]" and c["confidence"] == "certain"
    assert "balance = 40" in c["write_a"] and "lost update on deja_sa_wallet[id=1]" in a["story"]


def test_sqlalchemy_range_query_replays(tmp_path):
    path, responses = record_concurrently(orm_app(PG_DSN), [("GET", "/wallets")], str(tmp_path / "range.deja"))
    assert responses[0].json() == [{"id": 1, "balance": 100}]
    r = replay(path, orm_app(DEAD_PORT_DSN), deadline_s=10)
    assert r.followed and r.same_responses, r.summary()


def raw_app(dsn):
    def factory():
        import asyncpg

        app = FastAPI()
        state = {}

        @app.on_event("startup")
        async def start():
            state["pool"] = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
            await state["pool"].execute("CREATE TABLE IF NOT EXISTS deja_copy (id int, name text)")
            await state["pool"].execute("TRUNCATE deja_copy")

        @app.on_event("shutdown")
        async def stop():
            await state["pool"].close()

        @app.post("/load")
        async def load():
            pool = state["pool"]
            status = await pool.copy_records_to_table("deja_copy", records=[(1, "ada"), (2, "bob"), (3, "cy")])
            buf = io.BytesIO()
            await pool.copy_from_query("SELECT name FROM deja_copy WHERE id > $1 ORDER BY id", 1, output=buf,
                                       format="csv")
            async with pool.acquire() as conn, conn.transaction():
                names = [r["name"] async for r in conn.cursor("SELECT name FROM deja_copy ORDER BY id", prefetch=2)]
                cur = await conn.cursor("SELECT id FROM deja_copy ORDER BY id")
                await cur.forward(1)
                rest = [r["id"] for r in await cur.fetch(5)]
            return {"status": status, "csv": buf.getvalue().decode(), "names": names, "rest": rest}

        return app

    return factory


def test_cursors_and_bulk_copy_replay(tmp_path):
    path, responses = record_concurrently(raw_app(PG_DSN), [("POST", "/load")], str(tmp_path / "copy.deja"))
    assert responses[0].json() == {"status": "COPY 3", "csv": "bob\ncy\n", "names": ["ada", "bob", "cy"],
                                   "rest": [2, 3]}
    r = replay(path, raw_app(DEAD_PORT_DSN), deadline_s=10)
    assert r.followed and r.same_responses, r.summary()
    kinds = {(t["kind"], t["resource"]) for t in analyze(*read(path))["touches"]}
    assert ("write", "deja_copy") in kinds and ("read", "deja_copy") in kinds
