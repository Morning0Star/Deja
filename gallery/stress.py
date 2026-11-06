"""Measure how often each gallery bug actually happens.

    python -m gallery.stress all --trials 300
    python -m gallery.stress wallet --trials 500 --safe
    python -m gallery.stress likes --url http://127.0.0.1:8000     # against a running uvicorn instead of in-process

Each trial: reset, fire the scenario's concurrent requests (each start staggered by a small random delay), then ask the
app whether its invariant was violated.
"""

import argparse
import asyncio
import os
import statistics
import sys
import time

import httpx

SCENARIOS = {
    # name: (module, requests per trial, method, path, json body, check path)
    "signup": ("gallery.signup", 2, "POST", "/signup", {"username": "ada"}, "/check/ada"),
    "likes": ("gallery.likes", 3, "POST", "/like", None, "/check"),
    "wallet": ("gallery.wallet", 2, "POST", "/purchase", {"amount": 100}, "/check"),
}


def stagger_s(max_ms):
    return int.from_bytes(os.urandom(2), "big") / 65535 * max_ms / 1000


async def trial(client, scenario, stagger_ms):
    _, n, method, path, body, check = SCENARIOS[scenario]
    await client.post("/reset")

    async def one():
        await asyncio.sleep(stagger_s(stagger_ms))
        return await client.request(method, path, json=body)

    await asyncio.gather(*(one() for _ in range(n)))
    return (await client.get(check)).json()


async def run(scenario, trials, safe, url, stagger_ms, record_dir=None):
    if url:
        client = httpx.AsyncClient(base_url=url, timeout=30)
        lifespan = None
    else:
        import importlib

        app = importlib.import_module(SCENARIOS[scenario][0]).create_app(safe=safe)
        if record_dir:
            import deja

            app.add_middleware(deja.DejaMiddleware, directory=record_dir)
        lifespan = app.router.lifespan_context(app)
        await lifespan.__aenter__()
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gallery", timeout=30)
    hits, times, example = 0, [], None
    try:
        rec = getattr(asyncio.get_running_loop(), "deja", None) if record_dir else None
        for i in range(trials):
            marker = rec.mark(f"{scenario} trial {i}") if rec else None
            t = time.perf_counter()
            r = await trial(client, scenario, stagger_ms)
            times.append(time.perf_counter() - t)
            if r["violated"]:
                hits += 1
                example = example or r["detail"]
                if rec:
                    os.makedirs(record_dir, exist_ok=True)
                    rec.dump(os.path.join(record_dir, f"{scenario}-trial{i:04d}.deja"), since=marker,
                             reason=f"invariant violated: {r['detail']}", scenario=scenario, safe=safe)
    finally:
        await client.aclose()
        if lifespan:
            await lifespan.__aexit__(None, None, None)
    return hits, statistics.median(times) * 1000, example


def main():
    p = argparse.ArgumentParser()
    p.add_argument("scenario", choices=[*SCENARIOS, "all"])
    p.add_argument("--trials", type=int, default=200)
    p.add_argument("--safe", action="store_true", help="use the locked variant (should never violate)")
    p.add_argument("--url", help="hit a running server instead of the in-process ASGI app")
    p.add_argument("--record", metavar="DIR", help="run on Déjà's recording loop and save every violating trial")
    p.add_argument("--stagger-ms", type=float, default=float(os.environ.get("DEJA_GALLERY_STAGGER_MS", "60")))
    a = p.parse_args()
    store = "postgres" if os.environ.get("DEJA_PG_DSN") else "memory"
    print(f"python {sys.version.split()[0]} {sys.platform} store={store} io<= {os.environ.get('DEJA_GALLERY_IO_MS', '3')} ms "
          f"stagger<= {a.stagger_ms} ms safe={a.safe}")
    for name in SCENARIOS if a.scenario == "all" else [a.scenario]:
        loop_factory = None
        if a.record:
            from deja import recording_loop as loop_factory
        hits, med, example = asyncio.run(run(name, a.trials, a.safe, a.url, a.stagger_ms, a.record),
                                         loop_factory=loop_factory)
        print(f"  {name:7s} violated {hits:4d}/{a.trials} ({hits / a.trials:6.1%})  median trial {med:6.1f} ms"
              + (f"  e.g. {example}" if example else ""))


if __name__ == "__main__":
    main()
