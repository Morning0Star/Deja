# Déjà

Replay the asyncio race condition you couldn't reproduce.

Déjà records the real scheduling order of a running asyncio app (which task ran when, what each database and HTTP
call returned, what the clock and random number generator said) and replays it on your laptop with a deterministic
event loop. The bug that only happened when two requests overlapped in production happens again, every time, with
no database or network needed.

![A Déjà report: two purchases both read balance 100 before either writes](docs/img/report.png)

> Status: working end to end for ASGI apps (FastAPI, Starlette) on CPython 3.12 and 3.14, tested on Linux, Windows
> and macOS. Not production-hardened yet; see [what's not covered](#not-covered-yet).
> Design and prior art: [docs/DESIGN.md](docs/DESIGN.md) · Architecture and decisions: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
> Feasibility spikes: [spikes/RESULTS.md](spikes/RESULTS.md) · Benchmarks: [benchmarks/RESULTS.md](benchmarks/RESULTS.md) ·
> Race gallery: [gallery/README.md](gallery/README.md)

## Try it

```bash
pip install -e ".[dev,gallery]"

# record: run the race gallery on Déjà's recording loop; every trial that breaks its invariant is saved
python -m gallery.stress wallet --trials 50 --record recordings/

# see it: the requests and the exact interleaving that caused the double charge
deja show recordings/wallet-fail-trial0003.deja
deja report recordings/wallet-fail-trial0003.deja --app gallery.wallet:create_app   # HTML, with a replay verdict

# replay it 100 times: same interleaving, same double charge, every time
deja replay recordings/wallet-fail-trial0003.deja --app gallery.wallet:create_app --times 100
```

In your own app:

```python
import deja
app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")   # saves on 5xx, exception, or deja.flag()
# uvicorn app:app --loop deja.loop:recording_loop
```

Replay with the app configured the same way it was recorded (same store, same feature flags). If the code or its
configuration takes a different path, Déjà reports where the replay diverged instead of pretending it matched.

## How it works

1. **Record.** A subclass of the standard asyncio event loop logs which task each loop pass ran (one integer per
   step, so recording is cheap), with stable task ids derived from where each task was created. A middleware marks
   each HTTP request's task as request scope, and adapters for asyncpg (including SQLAlchemy async, cursors, bulk
   copy and LISTEN/NOTIFY), httpx (including streamed bodies, chunk by chunk), redis and `run_in_executor` record
   what each call returned. Everything sits in a fixed-size ring buffer; a 5xx, an exception or `deja.flag()` saves
   the window around the failing request to a small `.deja` file.
2. **Replay.** A replay loop with a virtual clock re-sends the recorded requests to a fresh copy of the app and only
   lets request tasks run in the recorded order. Database, HTTP and worker-thread calls get their recorded answers;
   the steps those libraries took internally are collapsed into the single step where the app resumed.
3. **Check.** If the code no longer behaves as recorded (a task can't take its recorded step, or a query differs),
   replay stops and says where. Replaying a recorded race against a fixed version shows the fix blocks it.

## Results

Acceptance run (`python -m gallery.acceptance --recordings 5 --replays 100`), CPython 3.12 and 3.14:

| App | Bug in plain runs | Failing recordings replayed | Passing recordings replayed |
|---|---|---|---|
| signup (check-then-act) | 24% | bug in 500/500 | bug in 0/500 |
| likes (lost update) | 52% | bug in 500/500 | bug in 0/500 |
| wallet (double charge) | 37% | bug in 500/500 | bug in 0/500 |

The same holds with the apps on Postgres, including with the database container stopped before replaying.

Overhead (FastAPI + Postgres + an HTTP call per order, k6 at 100 to 400 req/s, GitHub Actions Linux, 2 vCPU):
throughput unchanged, no failed requests, about +12% server CPU, +1 to 3 ms at p99, about 0.1 KB of recording per
request. Details and method: [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## Why

asyncio code races at every `await`. Two requests read the same row, both pass the check, both write. In tests the
timing never lines up, so the bug "can't be reproduced". Existing tools record a single request sequentially,
simulate random schedules in tests, or profile where time goes. None of them capture the interleaving that actually
happened and play it back. ([prior art](docs/DESIGN.md#2-what-exists-checked-2026-11-07-primary-sources))

## Not covered yet

- anyio worker threads (`to_thread`), WebSockets, Postgres log and termination listeners.
- Range queries (`WHERE price > 100`) are checked against the rows the recording shows (what a query returned,
  what a write set). When the recorded data can't decide whether the range covered a row, the conflict stays
  *possible*. Conditions Déjà can't evaluate (functions, `LIKE`, subqueries) also stay possible.
- In-memory state is tracked automatically when the route handlers can reach it (module variables, closures, the
  app's own objects, `app.state`) and it is a plain `dict`, `list` or `set` that can be swapped safely. Anything
  else needs `deja.track(obj)` or `deja.touch`.
- uvloop (record on the standard loop), threads and free-threaded builds, Python versions other than 3.12 and 3.14.
- Worker-thread functions are not re-run during replay, so their side effects don't happen; only their results
  come back.
- Overhead target of 5% CPU not reached yet (about 12% measured).

## Requirements

Python 3.12 or 3.14. The import name is `deja`; the PyPI distribution will be `deja-replay` (`deja` is taken on PyPI).
