"""Apps wrapped the way real ones often are: a plain ASGI middleware around FastAPI, and Socket.IO around FastAPI
(as KNOI-style backends do). Used by test_wrapped_apps."""

import asyncio

from fastapi import FastAPI


def make_fastapi():
    app = FastAPI()
    stock = {"widget": 1}

    @app.post("/buy")
    async def buy():
        n = stock["widget"]
        await asyncio.sleep(0.01)
        stock["widget"] = n - 1
        return {"ok": n > 0}

    return app


class Wrapper:
    """A plain ASGI middleware that keeps the wrapped app as ``.app`` (like Starlette's)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)


def make_wrapped():
    return Wrapper(make_fastapi())
