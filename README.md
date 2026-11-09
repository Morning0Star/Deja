# Déjà

Replay the asyncio race condition you couldn't reproduce.

Déjà records the real scheduling order of a running asyncio app (which task ran when, what each database and HTTP
call returned, what the clock and random number generator said) and replays it on your laptop with a deterministic
event loop. The bug that only happened when two requests overlapped in production happens again, every time, with
no database or network needed.

![A Déjà report: two purchases both read balance 100 before either writes](https://raw.githubusercontent.com/Morning0Star/Deja/main/docs/img/report.png)

> Status: working end to end for ASGI apps (FastAPI, Starlette, Litestar, Quart, Django), aiohttp servers and queue
> workers, on CPython 3.12 to 3.15 and PyPy 3.12, tested on Linux, Windows and macOS. Not production-hardened yet;
> see [what's not covered](#not-covered-yet).
> **Documentation: [morning0star.github.io/Deja](https://morning0star.github.io/Deja/)** (source in [docs/](docs/)) ·
> Design and prior art: [docs/DESIGN.md](docs/DESIGN.md) · Architecture and decisions: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) ·
> Feasibility spikes: [spikes/RESULTS.md](spikes/RESULTS.md) · Benchmarks: [benchmarks/RESULTS.md](benchmarks/RESULTS.md) ·
> Race gallery: [gallery/README.md](gallery/README.md)

## Install

```bash
pip install deja-replay
```

Python 3.12 or newer. The import name is `deja` and the command is `deja`. Adapters switch on for the libraries you
have installed; nothing else is needed.

## Try it

From a clone of this repository, with the race gallery (small FastAPI apps with real races):

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
# uvicorn app:app --loop deja.loop:recording_loop        (uvicorn 0.36 or newer)
```

Other kinds of app:

```python
# aiohttp web server
from deja.aiohttp_web import deja_middleware
app = web.Application(middlewares=[deja_middleware(directory="deja-recordings")])

# a queue consumer / scheduled job: each block is one "request"
async with deja.scope("orders", message) as job:
    job.result = await handle_order(message)          # replay: deja replay rec.deja --app myworker:handlers

# several worker processes (uvicorn --workers N): one recording of a race across them
app.add_middleware(deja.DejaMiddleware, group="/shared/deja-group")   # then: deja merge /shared/deja-group/<id>/
```

Then, from a recording:

| Command | What it does |
|---|---|
| `deja show` / `deja report` | the requests, the race in plain words, and an HTML report with a timeline |
| `deja replay` | the recorded interleaving again, with no database or network |
| `deja step` | replay paused before any step: where each request will resume, its locals, pdb |
| `deja verify` / `deja diff` | does a fix stop the race? how two versions of the app differ on it |
| `deja export-test` | a pytest regression test that fails while the race reproduces |
| `deja shrink` | the recording cut down to the requests the race needs |
| `deja explore` | other orderings of the same requests, to find races that haven't happened yet |
| `deja merge` | one recording from several worker processes |

Replay with the app configured the same way it was recorded (same store, same feature flags). If the code changed,
replay lists the modules that differ; if it takes a different path, Déjà reports where the replay diverged instead
of pretending it matched.

## How it works

1. **Record.** A subclass of the standard asyncio event loop logs which task each loop pass ran (one integer per
   step, so recording is cheap), with stable task ids derived from where each task was created. A middleware marks
   each HTTP request, WebSocket connection, job and the app's background tasks as request scope, and adapters record
   what each call returned:

   | Kind | Libraries |
   |---|---|
   | SQL | asyncpg (with SQLAlchemy async, transactions, cursors, bulk copy, LISTEN/NOTIFY, log listeners), psycopg 3 (async and sync, pools), aiomysql, aiosqlite |
   | Other stores | redis (commands, pipelines, pub/sub), PyMongo async, aiobotocore and boto3 (S3, DynamoDB, SQS...), aiokafka |
   | HTTP and RPC | httpx (async, sync, streamed chunk by chunk), aiohttp client, requests, grpc.aio |
   | Threads | plain `def` endpoints, `asyncio.to_thread`, `run_in_executor` |

   Clocks, random numbers and dates read inside a request are recorded too. Everything sits in a fixed-size ring
   buffer; a 5xx, an exception, close code 1011 or `deja.flag()` saves the window around the failing request to a
   small `.deja` file.
2. **Explain.** The report names the race (lost update, check-then-act, stale read, write skew, torn read,
   deadlock), the requests and the order of their reads and writes. It finds them in SQL (rows, ranges evaluated
   against the recorded data, subqueries, guarded inserts, transactions counted at commit, isolation levels),
   Redis, MongoDB documents, S3 objects, outside calls, and the app's in-memory state, which Déjà tracks on its own:
   containers (and the values inside them), objects and module variables the routes' code can reach.
3. **Replay.** A replay loop with a virtual clock re-sends the recorded requests (and WebSocket messages) to a fresh
   copy of the app and only lets request tasks run in the recorded order. Database, HTTP and blocking calls get
   their recorded answers; worker threads run again and wait at each access to shared state until the recording
   says it's their turn.
4. **Check.** If the code no longer behaves as recorded (a task can't take its recorded step, or a query differs),
   replay stops and says where. Replaying a recorded race against a fixed version shows the fix blocks it.

## Results

Acceptance run (`python -m gallery.acceptance --recordings 5 --replays 100`), CPython 3.12 and 3.14, November 2026:

| App | Bug in plain runs | Failing recordings replayed | Passing recordings replayed |
|---|---|---|---|
| signup (check-then-act) | 24% | bug in 500/500 | bug in 0/500 |
| likes (lost update) | 52% | bug in 500/500 | bug in 0/500 |
| wallet (double charge) | 37% | bug in 500/500 | bug in 0/500 |

The same holds with the apps on Postgres, including with the database container stopped before replaying.

Overhead, measured in November 2026 (FastAPI + Postgres + an HTTP call per order, k6 at 100 to 400 req/s, GitHub
Actions Linux, 2 vCPU):
throughput unchanged, no failed requests, about +12% server CPU, +1 to 3 ms at p99, about 0.1 KB of recording per
request. Details and method: [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## Why

asyncio code races at every `await`. Two requests read the same row, both pass the check, both write. In tests the
timing never lines up, so the bug "can't be reproduced". Existing tools record a single request sequentially,
simulate random schedules in tests, or profile where time goes. None of them capture the interleaving that actually
happened and play it back. ([prior art](docs/DESIGN.md#2-what-exists-checked-2026-11-07-primary-sources))

## Not covered yet

What Déjà can't decide, it says so ("possible conflict") rather than guess. The limits:

- **SQL conditions** are evaluated against what the recording shows: the rows a query returned and the values a
  write set. Values only the server knows (`now()`, defaults, sequences, triggers) and functions Déjà doesn't
  evaluate leave a conflict *possible*. A subquery can prove a row matched, never that none did. Writes from outside
  the recording (another service, a cron job) can't be seen.
- **Shared state** is found by following the routes' code. State held only by library objects or reached with
  computed names (`getattr(obj, name)`) needs `deja.track(obj)` or `deja.touch`. A container that something else
  also holds (a default argument, a local) is left alone rather than split in two; on PyPy, values stored inside
  tracked containers stay plain (no reference counts to check that safely).
- **Worker threads** are ordered at their accesses to shared state; work in between runs freely, and an access made
  in the middle of an event-loop step replays just after it.
- **Background tasks** are ordered from the recording window on; their state before it is rebuilt by running them
  from startup, with startup's recorded answers.
- **Long WebSocket connections**: a connection whose start fell out of the ring buffer is left out of the replay.
- **Exploring orderings** (`deja explore`) uses the recorded database answers, so orderings in which the app would
  have read different rows are approximate (marked).
- **Several workers** are merged by time on one machine (shared clock); across machines they aren't.
- **Mixed recordings**: one that holds both HTTP requests and `deja.scope` jobs is analysed but not replayed.
- **Other randomness**: `random.shuffle`, `random.sample`, `secrets` and the app's own `random.Random` aren't
  recorded and can make a replay diverge (the `random` module's functions, UUIDs, `os.urandom` and clocks are).
- uvloop (record on the standard loop), free-threaded builds, Trio.
- Overhead: about 6–17% CPU per request in this repository's benchmark on a laptop (the noise is large), about 12%
  on CI earlier; the 5% target needs the per-step log moved to C.

The full list, with what to do about each: [Limits](https://morning0star.github.io/Deja/limits/).

## Requirements

CPython 3.12, 3.13, 3.14 or 3.15, or PyPy 3.12. sqlglot is the only dependency; adapters load for the libraries you
have. The import name is `deja`; the PyPI distribution will be `deja-replay` (`deja` is taken on PyPI).

## Privacy

Recordings hold what the app saw: request bodies, SQL parameters, rows, API responses. Treat them like database
dumps and read [SECURITY.md](SECURITY.md) before sharing one.

## Contributing

Bug reports with a recording (private data removed) are the most useful; see [CONTRIBUTING.md](CONTRIBUTING.md) for
setup and tests, and [CHANGELOG.md](CHANGELOG.md) for what changed.

## License

[Apache License 2.0](LICENSE). Copyright 2026 Kushagra Sagar; see [NOTICE](NOTICE). Contributions are accepted under
the same licence (section 5 of the licence), which includes a patent grant from each contributor.
