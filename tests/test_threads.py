"""Worker threads: FastAPI runs plain ``def`` endpoints and dependencies in anyio worker threads. Their accesses to
shared state are recorded under the request, and replay runs the threads again in the recorded order."""

import asyncio
import os
import random
import time

import httpx
from fastapi import Depends, FastAPI

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay


def make_app():
    app = FastAPI()
    stock = {"widget": 1}
    seats = []

    @app.post("/buy")
    def buy():  # a plain def endpoint: runs in a worker thread
        n = stock["widget"]
        time.sleep(0.02)
        if n > 0:
            stock["widget"] = n - 1
            return {"ok": True, "ticket": random.randint(1, 10**9)}
        return {"ok": False}

    def free_seat(name: str):  # a sync dependency: also a worker thread
        return name not in seats

    @app.post("/seat/{name}")
    async def seat(name: str, free: bool = Depends(free_seat)):
        if free:
            await asyncio.sleep(0.01)
            seats.append(name)
        return {"seats": len(seats)}

    @app.get("/count")
    async def count():
        await asyncio.sleep(0.005)
        return {"left": stock["widget"]}

    return app


def record(calls, path, factory=None):
    out = {}

    async def main():
        app = (factory or make_app)()
        app.add_middleware(deja.DejaMiddleware, directory=os.path.dirname(path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            marker = rec.mark("test")
            out["responses"] = await asyncio.gather(*(client.request(m, u) for m, u in calls))
            out["path"] = rec.dump(path, since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    return out["path"], out["responses"]


def test_sync_endpoints_lost_update_found_and_replayed(tmp_path):
    path, responses = record([("POST", "/buy"), ("POST", "/buy")], str(tmp_path / "buy.deja"))
    assert [r.json()["ok"] for r in responses] == [True, True], "both threads sold the last widget"
    header, events = read(path)
    touches = [e for e in events if e[0] == "T"]
    assert touches and all(".t" in e[1] for e in touches), "accesses are recorded under the thread call's path"
    c = analyze(header, events)["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]" and c["confidence"] == "certain"
    for _ in range(3):  # the threads are ordered by the schedule, not by luck
        r = replay(path, make_app, deadline_s=5)
        assert r.followed and r.same_responses, r.summary()


def test_sync_dependency_check_then_act_with_async_endpoint(tmp_path):
    path, responses = record([("POST", "/seat/ada"), ("POST", "/seat/ada"), ("GET", "/count")],
                             str(tmp_path / "seat.deja"))
    assert sorted(r.json()["seats"] for r in responses[:2]) == [1, 2]  # (which thread ends first is the OS's call)
    a = analyze(*read(path))
    c = a["conflicts"][0]
    assert c["pattern"] == "check-then-act" and c["resource"] == "seats[ada]"
    r = replay(path, make_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def slow_app(slow):
    """Whoever is not slow gets the last widget: unless replay orders the threads as recorded."""
    app = FastAPI()
    stock = {"widget": 1}

    @app.post("/buy/{name}")
    def buy(name: str):
        time.sleep(0.15 if name == slow else 0)
        if stock["widget"] > 0:
            stock["widget"] -= 1
            return {"winner": name}
        return {"winner": None}

    return app


def test_replay_orders_threads_even_when_timing_changes(tmp_path):
    path = str(tmp_path / "slow.deja")
    out = {}

    async def main():
        app = slow_app(slow="a")
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            marker = rec.mark("test")
            out["r"] = await asyncio.gather(client.post("/buy/a"), client.post("/buy/b"))
            rec.dump(path, since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    assert [r.json()["winner"] for r in out["r"]] == [None, "b"]
    r = replay(path, lambda: slow_app(slow="b"), deadline_s=5)  # now b is the slow one
    assert r.followed and r.same_responses, r.summary()


def to_thread_app(slow, calls):
    """asyncio.to_thread (loop.run_in_executor underneath): one helper changes shared state, one only does I/O."""
    app = FastAPI()
    stock = {"widget": 1}

    def take(name):
        time.sleep(0.15 if name == slow else 0)
        if stock["widget"] > 0:
            stock["widget"] -= 1
            return name
        return None

    def lookup(name):  # blocking I/O, no shared state: its result comes back from the recording in replay
        calls.append(name)
        return f"profile of {name}"

    @app.post("/take/{name}")
    async def take_route(name: str):
        profile = await asyncio.to_thread(lookup, name)
        return {"winner": await asyncio.to_thread(take, name), "profile": profile}

    return app


def test_asyncio_to_thread_reruns_only_calls_that_changed_state(tmp_path):
    path, calls = str(tmp_path / "to_thread.deja"), []
    out = {}

    async def main():
        app = to_thread_app("a", calls)
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            marker = rec.mark("test")
            out["r"] = await asyncio.gather(client.post("/take/a"), client.post("/take/b"))
            rec.dump(path, since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    assert [r.json()["winner"] for r in out["r"]] == [None, "b"] and sorted(calls) == ["a", "b"]
    r = replay(path, lambda: to_thread_app("b", calls), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
    assert sorted(calls) == ["a", "b"], "the I/O-only helper must not run again during replay"


def callback_app():
    """A plain def endpoint that calls back into the event loop between two accesses to shared state."""
    import anyio.from_thread

    app = FastAPI()
    stock = {"widget": 1}

    async def audit(name):
        await asyncio.sleep(0.01)
        return name

    @app.post("/buy/{name}")
    def buy(name: str):
        n = stock["widget"]
        anyio.from_thread.run(audit, name)  # waits for the event loop
        if n > 0:
            stock["widget"] = n - 1
            return {"ok": True}
        return {"ok": False}

    return app


def test_thread_waiting_for_the_loop_does_not_stall_replay(tmp_path):
    path, responses = record([("POST", "/buy/a"), ("POST", "/buy/b")], str(tmp_path / "cb.deja"), callback_app)
    t0 = time.monotonic()
    r = replay(path, callback_app, deadline_s=10)
    assert r.followed and r.same_responses, r.summary()
    assert time.monotonic() - t0 < 8, "replay waited for the deadline"
