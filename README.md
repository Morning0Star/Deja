# Déjà

Replay the asyncio race condition you couldn't reproduce.

Déjà records the real scheduling order of a running asyncio app (which task ran when, what each I/O call returned,
what the clock and random number generator said) and replays it on your laptop with a deterministic event loop.
The bug that only happened when two requests overlapped in production happens again, every time.

> Status: record and replay work for ASGI apps with in-memory state (milestones M0–M2). Database and HTTP-client
> replay (M3) is next. Not production-ready yet.
> Problem and prior art: [docs/DESIGN.md](docs/DESIGN.md) · Architecture and milestones: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
> Feasibility spikes: [spikes/RESULTS.md](spikes/RESULTS.md) · Race gallery: [gallery/README.md](gallery/README.md)

## Try it

```bash
pip install -e ".[dev,gallery]"

# record: run the race gallery on Déjà's recording loop; every trial that breaks its invariant is saved
python -m gallery.stress wallet --trials 50 --record recordings/

# look at one: the requests and the exact interleaving that caused the double charge
deja show recordings/wallet-fail-trial0003.deja

# replay it 100 times: same interleaving, same double charge, every time
deja replay recordings/wallet-fail-trial0003.deja --app gallery.wallet:create_app --times 100
```

In your own app:

```python
import deja
app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")   # saves on 5xx, exception, or deja.flag()
# uvicorn app:app --loop deja.loop:recording_loop
```

## Results so far

Acceptance run (`python -m gallery.acceptance --recordings 5 --replays 100`), CPython 3.12 and 3.14 on Windows 11:

| App | Bug in plain runs | Failing recordings replayed | Passing recordings replayed |
|---|---|---|---|
| signup (check-then-act) | 24% | bug in 500/500 | bug in 0/500 |
| likes (lost update) | 52% | bug in 500/500 | bug in 0/500 |
| wallet (double charge) | 37% | bug in 500/500 | bug in 0/500 |

Every one of the 3,000 replays per Python version followed the recording step for step. Replaying a recorded double
charge against the *fixed* app reports a divergence: the lock stops the second purchase from taking its recorded step.

## Why

asyncio code races at every `await`. Two requests read the same row, both pass the check, both write. In tests
the timing never lines up, so the bug "can't be reproduced". Existing tools either record a single request
sequentially, simulate random schedules in tests, or profile where time goes. None capture the interleaving that
actually happened in production and play it back.

## Still to come

- I/O adapters (M3): replay database and HTTP-client calls from the recording, so replay needs no database or network.
- A published overhead benchmark on a quiet Linux machine (M4).
- More Python versions, CI on Linux/macOS/Windows, uvloop support.

## Requirements

Python 3.12+ (3.14 recommended). Standard asyncio event loop first; uvloop later.

## Install name

The import name is `deja`; the PyPI distribution will be `deja-replay` (`deja` is taken on PyPI).
