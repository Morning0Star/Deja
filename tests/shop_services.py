"""Shared state that lives in a helper module, reached only through the functions the routes call (shop_app)."""

import asyncio
import time
from collections import Counter, defaultdict, deque

HOLDS = defaultdict(int)
SALES = Counter()
QUEUE = deque()
visits = 0
tickets = 1
_slow = None  # which buyer's thread is slow (set by the tests)


class Wallet:
    def __init__(self):
        self.balance = 100


wallet = Wallet()


def reset():
    global visits, tickets
    visits, tickets = 0, 1
    HOLDS.clear()
    SALES.clear()
    QUEUE.clear()
    wallet.balance = 100


async def hold(item):
    n = HOLDS[item]  # a defaultdict: a missing key reads as 0
    await asyncio.sleep(0.01)
    HOLDS[item] = n + 1
    return HOLDS[item]


async def sell(item):
    n = SALES[item]
    await asyncio.sleep(0.01)
    SALES[item] = n + 1


async def enqueue(job):
    if job not in QUEUE:
        await asyncio.sleep(0.01)
        QUEUE.append(job)
    return len(QUEUE)


async def visit():
    global visits
    n = visits
    await asyncio.sleep(0.01)
    visits = n + 1
    return visits


async def pay(amount):
    b = wallet.balance
    await asyncio.sleep(0.01)
    wallet.balance = b - amount
    return wallet.balance


def take_ticket(name):  # runs in a worker thread (a plain def endpoint calls it)
    global tickets
    time.sleep(0.15 if name == _slow else 0)
    if tickets > 0:
        tickets -= 1
        return name
    return None


SESSIONS = {}


async def login(user):
    global SESSIONS
    if user not in SESSIONS:
        await asyncio.sleep(0.01)
        SESSIONS[user] = True
    return len(SESSIONS)


def reset_sessions():
    global SESSIONS
    SESSIONS = {}  # replaced whole: the new dict must be tracked too
