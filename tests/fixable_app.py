"""The same purchase, racy and fixed (used by test_verify)."""

import asyncio

from fastapi import FastAPI


def make_app(fixed=False):
    app = FastAPI()
    stock = {"widget": 1}
    lock = asyncio.Lock()

    async def take():
        n = stock["widget"]
        await asyncio.sleep(0.01)
        if n > 0:
            stock["widget"] = n - 1
            return True
        return False

    @app.post("/buy")
    async def buy():
        if fixed:
            async with lock:
                return {"ok": await take()}
        return {"ok": await take()}

    return app


def make_racy():
    return make_app(False)


def make_fixed():
    return make_app(True)
