"""A FastAPI app with a race: two purchases can both pass the balance check."""

import asyncio

from fastapi import FastAPI, HTTPException

import deja

START = 100


def create_app(directory: str = "deja-recordings") -> FastAPI:
    app = FastAPI()
    wallet = {"balance": START, "charged": 0}

    @app.post("/purchase")
    async def purchase(amount: int = 100):
        balance = wallet["balance"]
        if balance < amount:
            raise HTTPException(402, "insufficient funds")
        await asyncio.sleep(0.01)  # the payment provider's API call
        wallet["balance"] = balance - amount
        wallet["charged"] += amount
        if wallet["charged"] > START:
            deja.flag("charged more than the starting balance")
        return {"balance": wallet["balance"]}

    app.add_middleware(deja.DejaMiddleware, directory=directory)
    return app
