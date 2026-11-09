"""CPU cost per request, recording off vs on, measured in-process (no HTTP client in the way).

    python benchmarks/profile_request.py                 # compare
    python benchmarks/profile_request.py --profile on    # cProfile the recording run, top functions by own time

Drives benchmarks/app.py directly through ASGI with N concurrent workers. Needs Postgres (BENCH_PG_DSN) and the
pricing stub (started here in-process on a thread).
"""

import argparse
import asyncio
import cProfile
import importlib
import io
import json
import os
import pstats
import random
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def start_stub(port=8901):
    import uvicorn

    config = uvicorn.Config("benchmarks.stub:app", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.05)
    return server


async def call(app, method, path, body=b""):
    sent = False
    status = None

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        await asyncio.Event().wait()

    async def send(m):
        nonlocal status
        if m["type"] == "http.response.start":
            status = m["status"]

    p, _, q = path.partition("?")
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": p,
        "raw_path": p.encode(),
        "query_string": q.encode(),
        "root_path": "",
        "headers": [(b"content-type", b"application/json"), (b"host", b"bench")],
        "client": ("127.0.0.1", 1),
        "server": ("bench", 80),
        "state": {},
    }
    await app(scope, receive, send)
    return status


async def workload(app, n, concurrency):
    rng = random.Random(1)
    plan = []
    for _ in range(n):
        i = rng.randint(1, 1000)
        if rng.random() < 0.7:
            plan.append(("GET", f"/items/{i}", b""))
        else:
            plan.append(("POST", "/orders", json.dumps({"item_id": i, "qty": 1}).encode()))
    it = iter(plan)

    async def worker():
        for method, path, body in it:
            assert await call(app, method, path, body) == 200

    await asyncio.gather(*(worker() for _ in range(concurrency)))


def run(mode, n, concurrency, profile=False):
    os.environ["DEJA"] = "1" if mode == "on" else "0"
    sys.modules.pop("benchmarks.app", None)
    app = importlib.import_module("benchmarks.app").app

    async def main():
        async with app.router.lifespan_context(app):
            await workload(app, 200, concurrency)  # warm-up
            prof = cProfile.Profile() if profile else None
            t_cpu, t_wall = time.process_time(), time.perf_counter()
            if prof:
                prof.enable()
            await workload(app, n, concurrency)
            if prof:
                prof.disable()
            return time.process_time() - t_cpu, time.perf_counter() - t_wall, prof

    if mode == "on":
        from deja import recording_loop

        return asyncio.run(main(), loop_factory=recording_loop)
    return asyncio.run(main())


def main():
    p = argparse.ArgumentParser()
    p.add_argument("-n", type=int, default=3000)
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--profile", choices=["off", "on"])
    a = p.parse_args()
    start_stub()
    if a.profile:
        cpu, wall, prof = run(a.profile, a.n, a.concurrency, profile=True)
        s = io.StringIO()
        pstats.Stats(prof, stream=s).sort_stats("tottime").print_stats(30)
        print(s.getvalue())
        return
    res = {"off": [], "on": []}
    for rep in range(a.reps):
        for mode in ("off", "on") if rep % 2 == 0 else ("on", "off"):
            cpu, wall, _ = run(mode, a.n, a.concurrency)
            res[mode].append(cpu / a.n * 1e6)
            print(f"rep {rep + 1} {mode:3s}: {cpu / a.n * 1e6:7.0f} us CPU/request  ({a.n / wall:6.0f} req/s wall)")
    off, on = sorted(res["off"])[len(res["off"]) // 2], sorted(res["on"])[len(res["on"]) // 2]
    print(
        f"\nmedian CPU per request: off {off:.0f} us, on {on:.0f} us  ->  +{on - off:.0f} us (+{(on / off - 1) * 100:.1f}%)"
    )


if __name__ == "__main__":
    main()
