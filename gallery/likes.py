"""Bug 2, lost update: concurrent "like" requests read the same count and overwrite each other's increment.

Invariant: likes == number of successful like requests.
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

import deja

from . import _io, _pg


class MemoryStore:
    def __init__(self):
        self.likes = 0
        self.accepted = 0

    async def reset(self):
        self.likes = self.accepted = 0

    async def get(self):
        await _io.io()
        deja.touch("post.likes", "read", self.likes)
        return self.likes

    async def put(self, n):
        await _io.io()
        self.likes = n
        self.accepted += 1
        deja.touch("post.likes", "write", n)

    async def state(self):
        return self.likes, self.accepted


class PgStore:
    def __init__(self):
        self.pool = None

    async def open(self):
        self.pool = await _pg.pool()
        await self.pool.execute(
            "CREATE TABLE IF NOT EXISTS gallery_likes (id int PRIMARY KEY, likes int NOT NULL, accepted int NOT NULL)"
        )

    async def reset(self):
        await self.pool.execute(
            "INSERT INTO gallery_likes VALUES (1, 0, 0) ON CONFLICT (id) DO UPDATE SET likes = 0, accepted = 0"
        )

    async def get(self):
        return await self.pool.fetchval("SELECT likes FROM gallery_likes WHERE id = 1")

    async def put(self, n):
        # the bug: write the value computed in Python instead of `likes = likes + 1`
        await self.pool.execute("UPDATE gallery_likes SET likes = $1, accepted = accepted + 1 WHERE id = 1", n)

    async def state(self):
        row = await self.pool.fetchrow("SELECT likes, accepted FROM gallery_likes WHERE id = 1")
        return row["likes"], row["accepted"]


def create_app(safe: bool = False) -> FastAPI:
    store = PgStore() if _pg.DSN else MemoryStore()
    lock = asyncio.Lock()

    @asynccontextmanager
    async def lifespan(app):
        if isinstance(store, PgStore):
            await store.open()
        yield

    app = FastAPI(lifespan=lifespan)

    async def _like():
        n = await store.get()
        await store.put(n + 1)

    @app.post("/like")
    async def like():
        if safe:
            async with lock:
                await _like()
        else:
            await _like()
        return {"ok": True}

    @app.post("/reset")
    async def reset():
        await store.reset()
        return {"ok": True}

    @app.get("/check")
    async def check():
        likes, accepted = await store.state()
        return {"violated": likes != accepted, "detail": f"likes={likes} accepted={accepted}"}

    return app


app = create_app()
