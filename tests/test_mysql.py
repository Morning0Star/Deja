"""aiomysql: a lost update on a stock row in MySQL, found, and replayed with the server unreachable."""

import asyncio
import contextlib

import pytest
from fastapi import FastAPI

aiomysql = pytest.importorskip("aiomysql")

from test_adapters import reachable  # noqa: E402
from test_streams import record_concurrently  # noqa: E402

from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.replay import replay  # noqa: E402

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", 53306), reason="test MySQL not running")
LIVE = dict(host="127.0.0.1", port=53306, user="root", password="deja", db="deja")
DEAD = dict(host="127.0.0.1", port=1, user="nobody", password="", db="none")


def stock_app(cfg):
    def factory():
        state = {}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["pool"] = await aiomysql.create_pool(minsize=1, maxsize=2, **cfg)
            async with state["pool"].acquire() as c:
                async with c.cursor() as cur:
                    await cur.execute("CREATE TABLE IF NOT EXISTS deja_stock (sku VARCHAR(20) PRIMARY KEY, qty INT)")
                    await cur.execute("REPLACE INTO deja_stock VALUES ('w1', 5)")
                await c.commit()
            yield
            state["pool"].close()
            await state["pool"].wait_closed()

        app = FastAPI(lifespan=lifespan)

        @app.post("/buy/{sku}")
        async def buy(sku: str):
            async with state["pool"].acquire() as c:
                async with c.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT qty FROM deja_stock WHERE sku = %s", (sku,))
                    qty = (await cur.fetchone())["qty"]
                    await asyncio.sleep(0.02)
                    changed = await cur.execute(
                        "UPDATE deja_stock SET qty = %(q)s WHERE sku = %(s)s", {"q": qty - 1, "s": sku}
                    )
                await c.commit()
            return {"left": qty - 1, "changed": changed}

        return app

    return factory


def test_mysql_lost_update_found_and_replayed(tmp_path):
    path, responses = record_concurrently(
        stock_app(LIVE), [("POST", "/buy/w1"), ("POST", "/buy/w1")], str(tmp_path / "my.deja")
    )
    # two sold, stock went down by one (MySQL counts the second UPDATE as 0 rows changed: it wrote the same 4)
    assert sorted(r.json()["changed"] for r in responses) == [0, 1] and {r.json()["left"] for r in responses} == {4}
    a = analyze(*read(path))
    c = a["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "deja_stock[sku=w1]", a["story"]
    r = replay(path, stock_app(DEAD), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
