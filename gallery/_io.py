"""Simulated I/O for the in-memory stores.

Each call runs a short sleep on a worker thread, like a blocking driver behind `run_in_executor`. The duration comes
from os.urandom, not a seeded RNG, so timing differs on every run the way real network and disk latency does, and a
recorder can't "cheat" by replaying random numbers.
"""

import asyncio
import os
import time

MAX_MS = float(os.environ.get("DEJA_GALLERY_IO_MS", "3"))


def _sleep():
    time.sleep(int.from_bytes(os.urandom(2), "big") / 65535 * MAX_MS / 1000)


async def io() -> None:
    await asyncio.get_running_loop().run_in_executor(None, _sleep)
