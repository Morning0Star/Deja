"""An app with shared in-memory state and no deja.track calls (used by test_autotrack)."""

import asyncio

from fastapi import FastAPI

CARTS = {}  # module-level state
REMEMBERED = {}  # also bound as a default argument below: Déjà must leave it alone


def remember(key, into=REMEMBERED):
    into[key] = True


class Store:
    def __init__(self):
        self.orders = []


def make_app():
    app = FastAPI()
    stock = {"widget": 1}  # captured by the handlers' closures
    store = Store()
    taken = []
    app.state.visits = set()

    @app.post("/buy")
    async def buy():
        n = stock["widget"]
        await asyncio.sleep(0.01)
        if n > 0:
            stock["widget"] = n - 1
            return {"ok": True}
        return {"ok": False}

    @app.post("/order/{oid}")
    async def order(oid: str):
        if oid not in store.orders:
            await asyncio.sleep(0.01)
            store.orders.append(oid)
        return {"orders": len(store.orders)}

    @app.post("/seat/{name}")
    async def seat(name: str):
        if not any(t == name for t in taken):  # iterates the list: a whole-container read
            await asyncio.sleep(0.01)
            taken.append(name)
        return {"taken": list(taken)}

    @app.post("/cart/{cid}")
    async def cart(cid: str):
        CARTS[cid] = CARTS.get(cid, 0) + 1
        remember(cid)
        app.state.visits.add(cid)
        return {"n": CARTS[cid]}

    return app
