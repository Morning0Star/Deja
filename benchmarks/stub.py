"""The "outside pricing service" the benchmark app calls: answers after ~2 ms like a nearby internal API."""

import asyncio
import random

from fastapi import FastAPI

app = FastAPI()


@app.get("/quote")
async def quote(item: int, qty: int = 1):
    await asyncio.sleep(0.002)
    return {"item": item, "discount": random.randint(0, 20)}
