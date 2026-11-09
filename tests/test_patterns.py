"""Race patterns beyond one value: write skew, torn reads, deadlocks; isolation levels; Redis keys."""

import asyncio
import contextlib

import httpx
import pytest
from fastapi import FastAPI

import deja
from deja import events as ev
from deja import recording_loop
from deja.analysis import analyze, call_touches
from deja.fileformat import read
from deja.replay import replay


def record(factory, calls, path):
    out = {}

    async def main():
        app = factory()
        app.add_middleware(deja.DejaMiddleware, directory=str(path.parent))
        rec = asyncio.get_running_loop().deja
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
                marker = rec.mark("t")
                out["r"] = await asyncio.gather(*(c.post(u) for u in calls))
                out["path"] = rec.dump(str(path), since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    return out["path"], out["r"]


def oncall_app():
    app = FastAPI()
    oncall = {"ada": True, "bob": True}

    @app.post("/leave/{name}")
    async def leave(name: str):
        if oncall["ada"] and oncall["bob"]:  # someone else is still on call
            await asyncio.sleep(0.01)
            oncall[name] = False
            return {"left": True}
        return {"left": False}

    return app


def test_write_skew_two_doctors_both_leave(tmp_path):
    path, r = record(oncall_app, ["/leave/ada", "/leave/bob"], tmp_path / "skew.deja")
    assert [x.json() for x in r] == [{"left": True}, {"left": True}], "nobody is on call now"
    a = analyze(*read(path))
    c = a["conflicts"][0]
    assert c["pattern"] == "write-skew" and c["confidence"] == "certain"
    assert {c["x"], c["y"]} == {"oncall[ada]", "oncall[bob]"} and "write skew" in a["story"]
    assert replay(path, oncall_app, deadline_s=5).same_responses


def bank_app():
    app = FastAPI()
    balances = {"x": 100, "y": 0}
    halfway = asyncio.Event()  # makes the recording sure to read in the middle (not tracked: not a container)

    @app.post("/transfer")
    async def transfer():
        balances["x"] -= 10
        halfway.set()
        await asyncio.sleep(0.01)  # e.g. an audit call between the two halves
        balances["y"] += 10
        return {}

    @app.post("/total")
    async def total():
        await halfway.wait()
        return {"total": balances["x"] + balances["y"]}

    return app


def test_torn_read_sees_a_transfer_half_done(tmp_path):
    path, r = record(bank_app, ["/transfer", "/total"], tmp_path / "torn.deja")
    assert r[1].json() == {"total": 90}, "the total missed the money in flight"
    c = analyze(*read(path))["conflicts"][0]
    assert c["pattern"] == "torn-read" and {c["x"], c["y"]} == {"balances[x]", "balances[y]"}


# ---------------------------------------------------------------- Postgres: deadlock, isolation
from test_adapters import DEAD_PORT_DSN, PG_DSN, reachable  # noqa: E402

pg = pytest.mark.skipif(
    not reachable("127.0.0.1", int(PG_DSN.rsplit(":", 1)[1].split("/")[0])), reason="test Postgres not running"
)


def lock_app(dsn):
    import asyncpg

    def factory():
        state = {}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["pool"] = await asyncpg.create_pool(dsn, min_size=2, max_size=2)
            async with state["pool"].acquire() as c:
                await c.execute("CREATE TABLE IF NOT EXISTS deja_acct (id int PRIMARY KEY, bal int)")
                await c.execute(
                    "INSERT INTO deja_acct VALUES (1, 100), (2, 100) ON CONFLICT (id) DO UPDATE SET bal = 100"
                )
            yield
            await state["pool"].close()

        app = FastAPI(lifespan=lifespan)

        @app.post("/move/{a}/{b}")
        async def move(a: int, b: int):
            try:
                async with state["pool"].acquire() as c:
                    async with c.transaction():
                        await c.execute("UPDATE deja_acct SET bal = bal - 1 WHERE id = $1", a)
                        await asyncio.sleep(0.05)
                        await c.execute("UPDATE deja_acct SET bal = bal + 1 WHERE id = $1", b)
                return {"ok": True}
            except Exception as exc:
                return {"ok": False, "error": type(exc).__name__}

        return app

    return factory


@pg
def test_deadlock_is_explained_and_replays_without_the_database(tmp_path):
    pytest.importorskip("asyncpg")
    path, r = record(lock_app(PG_DSN), ["/move/1/2", "/move/2/1"], tmp_path / "dead.deja")
    assert sorted(x.json()["ok"] for x in r) == [False, True], [x.json() for x in r]
    a = analyze(*read(path))
    c = a["conflicts"][0]
    assert c["pattern"] == "deadlock" and {c["x"], c["y"]} == {"deja_acct[id=1]", "deja_acct[id=2]"}, a["story"]
    assert "deadlock" in a["story"]
    r2 = replay(path, lock_app(DEAD_PORT_DSN), deadline_s=5)
    assert r2.followed and r2.same_responses, r2.summary()


def _io(path, op, sql, params, value, t, detail=None):
    return [
        (ev.IO_START, path, op, t),
        (
            ev.IO_END,
            path,
            op,
            "d",
            "ok",
            value,
            t + 1,
            detail if detail is not None else {"sql": sql, "params": params},
        ),
    ]


def _lost_update(level):
    out = [(ev.REQUEST, 1, "r0", "POST", "/a", [], b"", 0), (ev.REQUEST, 2, "r0", "POST", "/a", [], b"", 1)]
    for i, q in enumerate(("q1", "q2")):
        out += _io(q, "pg.tx.start", None, None, None, 5 + i, {"isolation": level})
    for i, q in enumerate(("q1", "q2")):
        out += _io(q, "pg.fetchval", "SELECT bal FROM acct WHERE id = $1", [1], 100, 10 + i)
    out += _io("q1", "pg.execute", "UPDATE acct SET bal = $1 WHERE id = $2", [90, 1], "UPDATE 1", 20)
    out += _io("q1", "pg.tx.commit", None, None, None, 22, {})
    out += _io("q2", "pg.execute", "UPDATE acct SET bal = $1 WHERE id = $2", [80, 1], "UPDATE 1", 24)
    out += _io("q2", "pg.tx.commit", None, None, None, 26, {})
    return out


def test_serializable_transactions_that_both_committed_are_not_a_race():
    assert analyze({}, _lost_update("read_committed"))["conflicts"][0]["pattern"] == "lost-update"
    assert analyze({}, _lost_update("serializable"))["conflicts"] == []


# ---------------------------------------------------------------- Redis
def _redis(args, op="redis.cmd"):
    detail = {"args": args} if op == "redis.cmd" else {"commands": args}
    return [(t["kind"], t["resource"]) for t in call_touches({"op": op, "detail": detail})]


def test_redis_commands_name_every_key():
    assert _redis(["MGET", "a", "b"]) == [("read", "redis:a"), ("read", "redis:b")]
    assert _redis(["MSET", "a", "1", "b", "2"]) == [("write", "redis:a"), ("write", "redis:b")]
    assert _redis(["DEL", "a", "b"]) == [("write", "redis:a"), ("write", "redis:b")]
    assert _redis(["EVAL", "return 1", "2", "k1", "k2", "arg"]) == [("write", "redis:k1"), ("write", "redis:k2")]
    assert _redis(["WATCH", "k"]) == [("read", "redis:k")]
    assert _redis([["GET", "a"], ["SET", "b", "1"]], op="redis.pipeline") == [("read", "redis:a"), ("write", "redis:b")]
