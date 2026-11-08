"""Shared state held in a pydantic model (used by test_autotrack)."""

import asyncio

from fastapi import FastAPI
from pydantic import BaseModel


class Wallet(BaseModel):
    balance: int = 100


def make_app():
    app = FastAPI()
    wallet = Wallet()

    @app.post("/pay")
    async def pay():
        b = wallet.balance
        await asyncio.sleep(0.01)
        wallet.balance = b - 30
        return {"balance": wallet.balance, "same": wallet == Wallet(balance=wallet.balance)}

    return app
