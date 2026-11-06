"""Bug 1, check-then-act: two signups for the same username both pass the "is it taken?" check.

Invariant: each username exists at most once.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import _io, _pg


class Signup(BaseModel):
    username: str


class MemoryStore:
    def __init__(self):
        self.users: list[str] = []

    async def reset(self):
        self.users = []

    async def exists(self, name):
        await _io.io()
        return name in self.users

    async def insert(self, name):
        await _io.io()
        self.users.append(name)

    async def count(self, name):
        return self.users.count(name)


class PgStore:
    def __init__(self):
        self.pool = None

    async def open(self):
        self.pool = await _pg.pool()
        await self.pool.execute("CREATE TABLE IF NOT EXISTS gallery_users (username text NOT NULL)")  # no UNIQUE: that's the bug

    async def reset(self):
        await self.pool.execute("TRUNCATE gallery_users")

    async def exists(self, name):
        return await self.pool.fetchval("SELECT EXISTS (SELECT 1 FROM gallery_users WHERE username = $1)", name)

    async def insert(self, name):
        await self.pool.execute("INSERT INTO gallery_users (username) VALUES ($1)", name)

    async def count(self, name):
        return await self.pool.fetchval("SELECT count(*) FROM gallery_users WHERE username = $1", name)


def create_app(safe: bool = False) -> FastAPI:
    store = PgStore() if _pg.DSN else MemoryStore()
    lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        if isinstance(store, PgStore):
            await store.open()
        yield

    app = FastAPI(lifespan=lifespan)

    async def _signup(name):
        if await store.exists(name):
            raise HTTPException(409, "taken")
        await store.insert(name)

    @app.post("/signup", status_code=201)
    async def signup(body: Signup):
        if safe:
            async with lock:
                await _signup(body.username)
        else:
            await _signup(body.username)
        return {"ok": True}

    @app.post("/reset")
    async def reset():
        await store.reset()
        return {"ok": True}

    @app.get("/check/{name}")
    async def check(name: str):
        n = await store.count(name)
        return {"violated": n > 1, "detail": f"{name} exists {n} times"}

    return app


app = create_app()
