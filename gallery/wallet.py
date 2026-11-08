"""Bug 3, double charge: two purchases both see enough balance, both call the payment provider, both deduct.

Invariant: total charged <= starting balance (100).
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import deja

from . import _io, _pg

START = 100


class Purchase(BaseModel):
    amount: int = 100


async def payment_provider(amount: int) -> str:
    """Stand-in for an HTTP call to a payment API."""
    await _io.io()
    return f"pay_{amount}"


class MemoryStore:
    def __init__(self):
        self.balance = START
        self.charged = 0
        deja.track(self, "wallet")

    async def reset(self):
        self.balance, self.charged = START, 0

    async def get_balance(self):
        await _io.io()
        return self.balance

    async def deduct(self, before, amount):
        await _io.io()
        self.balance = before - amount
        self.charged += amount

    async def state(self):
        return self.balance, self.charged


class PgStore:
    def __init__(self):
        self.pool = None

    async def open(self):
        self.pool = await _pg.pool()
        await self.pool.execute(
            "CREATE TABLE IF NOT EXISTS gallery_wallet (id int PRIMARY KEY, balance int NOT NULL, charged int NOT NULL)"
        )

    async def reset(self):
        await self.pool.execute(
            "INSERT INTO gallery_wallet VALUES (1, $1, 0) ON CONFLICT (id) DO UPDATE SET balance = $1, charged = 0", START
        )

    async def get_balance(self):
        return await self.pool.fetchval("SELECT balance FROM gallery_wallet WHERE id = 1")

    async def deduct(self, before, amount):
        await self.pool.execute(
            "UPDATE gallery_wallet SET balance = $1, charged = charged + $2 WHERE id = 1", before - amount, amount
        )

    async def state(self):
        row = await self.pool.fetchrow("SELECT balance, charged FROM gallery_wallet WHERE id = 1")
        return row["balance"], row["charged"]


def create_app(safe: bool = False) -> FastAPI:
    store = PgStore() if _pg.DSN else MemoryStore()
    lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        if isinstance(store, PgStore):
            await store.open()
        yield

    app = FastAPI(lifespan=lifespan)
    deja.labels({"POST /purchase": "purchase", "wallet.balance": "the wallet balance",
                 "gallery_wallet[id=1]": "the wallet balance"})

    async def _purchase(amount):
        balance = await store.get_balance()
        if balance < amount:
            raise HTTPException(402, "insufficient funds")
        await payment_provider(amount)
        await store.deduct(balance, amount)

    @app.post("/purchase")
    async def purchase(body: Purchase):
        if safe:
            async with lock:
                await _purchase(body.amount)
        else:
            await _purchase(body.amount)
        return {"ok": True}

    @app.post("/reset")
    async def reset():
        await store.reset()
        return {"ok": True}

    @app.get("/check")
    async def check():
        balance, charged = await store.state()
        return {"violated": charged > START, "detail": f"balance={balance} charged={charged}"}

    return app


app = create_app()
