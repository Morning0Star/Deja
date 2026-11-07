"""Benchmark target: a small shop API on FastAPI + Postgres (asyncpg) that also calls an outside pricing service.

    BENCH_PG_DSN=postgresql://... BENCH_STUB_URL=http://127.0.0.1:8901 DEJA=1 \
        uvicorn benchmarks.app:app --loop deja.loop:recording_loop

With ``DEJA=1`` the Déjà middleware is added (run it on the recording loop); with ``DEJA=0`` it's a plain app.
``GET /items/{id}`` is one indexed SELECT; ``POST /orders`` is a SELECT, an HTTP call to the pricing stub, and an
INSERT ... RETURNING.
"""

import os
from contextlib import asynccontextmanager

import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DSN = os.environ.get("BENCH_PG_DSN", "postgresql://deja:deja@127.0.0.1:55432/deja")
STUB = os.environ.get("BENCH_STUB_URL", "http://127.0.0.1:8901")
DEJA = os.environ.get("DEJA") == "1"
ITEMS = 1000

state: dict = {}


@asynccontextmanager
async def lifespan(app):
    pool = await asyncpg.create_pool(DSN, min_size=5, max_size=20)
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS bench_items (id int PRIMARY KEY, name text NOT NULL, price int NOT NULL);
        CREATE TABLE IF NOT EXISTS bench_orders (id bigserial PRIMARY KEY, item_id int NOT NULL, qty int NOT NULL,
                                                 total int NOT NULL);
    """)
    if await pool.fetchval("SELECT count(*) FROM bench_items") < ITEMS:
        await pool.executemany("INSERT INTO bench_items VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                               [(i, f"item-{i}", 100 + i % 900) for i in range(1, ITEMS + 1)])
    state["pool"] = pool
    state["http"] = httpx.AsyncClient(base_url=STUB, timeout=5)
    yield
    await state["http"].aclose()
    await pool.close()


app = FastAPI(lifespan=lifespan)


class Order(BaseModel):
    item_id: int
    qty: int = 1


@app.get("/items/{item_id}")
async def get_item(item_id: int):
    row = await state["pool"].fetchrow("SELECT id, name, price FROM bench_items WHERE id = $1", item_id)
    if row is None:
        raise HTTPException(404)
    return dict(row)


@app.post("/orders")
async def create_order(order: Order):
    pool = state["pool"]
    price = await pool.fetchval("SELECT price FROM bench_items WHERE id = $1", order.item_id)
    if price is None:
        raise HTTPException(404)
    quote = (await state["http"].get("/quote", params={"item": order.item_id, "qty": order.qty})).json()
    total = price * order.qty - quote["discount"]
    order_id = await pool.fetchval(
        "INSERT INTO bench_orders (item_id, qty, total) VALUES ($1, $2, $3) RETURNING id", order.item_id, order.qty, total)
    return {"order_id": order_id, "total": total}


@app.get("/__deja/size")
async def deja_size():
    """Benchmark only: compressed size of the recorder's buffer and how many requests it covers."""
    import asyncio
    import gzip
    import json

    rec = getattr(asyncio.get_running_loop(), "deja", None)
    if rec is None:
        return {"bytes": 0, "requests": 0}
    events = rec.events()
    raw = "\n".join(json.dumps(e, default=repr, separators=(",", ":")) for e in events).encode()
    requests = sum(1 for e in events if e[0] == "Q")
    return {"bytes": len(gzip.compress(raw)), "requests": requests, "events": len(events)}


if DEJA:
    import deja

    app.add_middleware(deja.DejaMiddleware, directory=os.environ.get("DEJA_DIR", "deja-recordings"))
