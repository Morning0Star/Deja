"""An app whose shared state is all in a helper module and no deja.track calls (used by test_autotrack)."""

from fastapi import FastAPI

import shop_services as services


def make_app():
    services.reset()
    app = FastAPI()

    @app.post("/hold/{item}")
    async def hold(item: str):
        return {"held": await services.hold(item)}

    @app.post("/sell/{item}")
    async def sell(item: str):
        await services.sell(item)
        return {}

    @app.post("/queue/{job}")
    async def queue(job: str):
        return {"queued": await services.enqueue(job)}

    @app.post("/visit")
    async def visit():
        return {"visits": await services.visit()}

    @app.post("/pay")
    async def pay():
        return {"balance": await services.pay(30)}

    @app.post("/ticket/{name}")
    def ticket(name: str):
        return {"winner": services.take_ticket(name)}

    return app
