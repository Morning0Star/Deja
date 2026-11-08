import asyncio
import os
import random
import time
import uuid

import httpx

import deja
from deja import events as ev
from deja import fileformat, shims
from deja.loop import recording_loop


def run(coro_fn):
    loop = recording_loop()
    try:
        return loop.run_until_complete(coro_fn()), loop.deja
    finally:
        loop.close()


def reads(rec):
    return [(e[2], e[3]) for e in rec.events() if e[0] == ev.READ]


def test_reads_inside_tasks_are_recorded():
    async def main():
        return time.time(), random.random(), uuid.uuid4(), os.urandom(4)

    (t, r, u, b), rec = run(main)
    got = dict(reads(rec))
    assert got["time.time"] == t and got["random.random"] == r
    assert got["uuid.uuid4"] == str(u) and got["os.urandom"] == b


def test_reads_outside_tasks_are_not_recorded():
    async def main():
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, random.random)  # worker thread, no task

    _, rec = run(main)
    assert reads(rec) == []


def test_shims_are_removed_when_the_loop_closes():
    loop = recording_loop()
    assert getattr(time.time, "__deja_shim__", False)
    loop.close()
    assert not getattr(time.time, "__deja_shim__", False)
    assert not shims.installed()


def test_loop_clock_bypasses_the_shims():
    async def main():
        await asyncio.sleep(0.001)  # call_later -> loop.time()

    _, rec = run(main)
    assert not any(src.startswith("time.monotonic") for src, _ in reads(rec))


# ------------------------------------------------------------------ middleware
def make_app():
    from fastapi import FastAPI

    app = FastAPI()

    @app.post("/ok")
    async def ok(body: dict):
        return {"echo": body}

    @app.get("/boom")
    async def boom():
        await asyncio.sleep(0)  # a real handler awaits something before failing
        raise RuntimeError("kaboom")

    @app.get("/flagged")
    async def flagged():
        deja.flag("invariant broken")
        return {"ok": True}

    return app


def drive(tmp_path, *calls):
    dumps = []
    app = make_app()
    app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path), on_dump=lambda p, r: dumps.append((p, r)))

    async def main():
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            out = [await c.request(m, u, json=j, headers={"Authorization": "Bearer secret"}) for m, u, j in calls]
        await asyncio.sleep(0.2)  # let the executor finish writing files
        return out

    responses, rec = run(main)
    return responses, rec, dumps


def test_good_requests_are_tracked_but_not_saved(tmp_path):
    (r,), rec, dumps = drive(tmp_path, ("POST", "/ok", {"a": 1}))
    assert r.status_code == 200 and dumps == []
    q = [e for e in rec.events() if e[0] == ev.REQUEST][0]
    assert q[3:5] == ("POST", "/ok") and b'"a"' in q[6]
    assert ["authorization", "<redacted>"] in [list(h) for h in q[5]]


def test_exception_saves_a_recording(tmp_path):
    (r,), rec, dumps = drive(tmp_path, ("GET", "/boom", None))
    assert r.status_code == 500 and len(dumps) == 1
    path, reason = dumps[0]
    header, events = fileformat.read(path)
    assert "RuntimeError" in reason and header["reason"] == reason
    assert any(e[0] == ev.EXCEPTION and e[2] == "RuntimeError" for e in events)
    assert any(e[0] == ev.STEP for e in events)


def test_flag_saves_a_recording(tmp_path):
    (r,), rec, dumps = drive(tmp_path, ("GET", "/flagged", None))
    assert r.status_code == 200 and dumps and dumps[0][1] == "flag: invariant broken"


def test_clock_names_bound_before_recording_and_datetime_replay(tmp_path):
    import os
    import time as _time

    import clock_app
    import httpx

    from deja.replay import replay

    out = {}

    async def main():
        app = clock_app.make_app()
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            m = rec.mark("t")
            out["r"] = (await c.get("/stamp")).json()
            out["path"] = rec.dump(os.path.join(str(tmp_path), "clock.deja"), since=m)

    asyncio.run(main(), loop_factory=deja.recording_loop)
    assert out["r"]["real"] is True, "the stand-ins give real datetimes"
    _time.sleep(0.05)
    r = replay(out["path"], clock_app.make_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()  # every clock value came back from the recording
