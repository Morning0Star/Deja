# Déjà

Replay the asyncio race condition you couldn't reproduce.

Déjà records the real scheduling order of a running asyncio app (which task ran when, what each I/O call returned,
what the clock and random number generator said) and replays it on your laptop with a deterministic event loop.
The bug that only happened when two requests overlapped in production happens again, every time.

> Status: planning done, building next. Nothing here is usable yet.
> Problem and prior art: [docs/DESIGN.md](docs/DESIGN.md) · Architecture and milestones: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
> Feasibility spikes: [spikes/RESULTS.md](spikes/RESULTS.md)

## Why

asyncio code races at every `await`. Two requests read the same row, both pass the check, both write. In tests
the timing never lines up, so the bug "can't be reproduced". Existing tools either record a single request
sequentially, simulate random schedules in tests, or profile where time goes. None capture the interleaving that
actually happened in production and play it back.

## Planned

- `deja.record`: a low-overhead recorder you switch on in a FastAPI/asyncio app. It keeps a rolling window of
  scheduling events and dumps it when a request fails or is flagged.
- `deja replay <recording>`: runs the same code under a deterministic loop that forces the recorded order and feeds
  back recorded I/O, and reports the exact step where a replay diverges.
- A gallery of real asyncio race bugs, each reproduced on every run, and published recording overhead numbers.

## Requirements

Python 3.12+ (3.14 recommended). Standard asyncio event loop first; uvloop later.

## Install name

The import name is `deja`; the PyPI distribution will be `deja-replay` (`deja` is taken on PyPI).
