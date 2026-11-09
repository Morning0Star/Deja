# Changelog

All notable changes to Déjà are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Until 1.0, minor versions may change
the recording format or the Python API; recordings from older versions keep loading (see `deja.fileformat`).

## [Unreleased]

### Fixed
- Shared state is found through the layers an app is wrapped in: python-socketio's `ASGIApp` around FastAPI and
  ASGI middleware that keeps the wrapped app as `.app`. Socket.IO event handlers (functions and class-based
  namespaces) are followed too. Before, an app wrapped in Socket.IO had nothing tracked.

## [0.1.0] - 2026-11-09

First public release.

### Recording
- Recording event loop for asyncio (CPython 3.12–3.15, PyPy 3.12): one integer per step, stable task identities,
  a ring buffer, and a causal-window `.deja` file saved on a 5xx, an exception, close code 1011 or `deja.flag()`.
- Middleware for ASGI apps (FastAPI, Starlette, Litestar, Quart, Django), aiohttp servers, WebSocket connections,
  and `deja.scope` for queue consumers and scheduled jobs. The app's background tasks are recorded too.
- Adapters: asyncpg (SQLAlchemy async, transactions, cursors, copy, LISTEN/NOTIFY, log and termination listeners),
  psycopg 3 (async and sync, pools), aiomysql, aiosqlite, redis (pipelines, pub/sub), PyMongo async, aiobotocore and
  boto3, aiokafka, httpx (async, sync, streams), aiohttp client, requests, grpc.aio; worker threads
  (`anyio.to_thread`, `asyncio.to_thread`, `run_in_executor`); clocks, random numbers, UUIDs and dates.
- Several worker processes record one race together (`group=`, `deja merge`).
- A saved window keeps every request that overlapped the failing one, including those that finished before it
  (earlier builds dropped a racing request that ended first). A worker's slice of a group recording likewise runs
  to the end of the requests still in flight when the window closed.

### Analysis and report
- Lost updates, check-then-act races (including over ranges and guarded inserts), stale reads, write skew, torn
  reads and deadlocks, in SQL (with transactions, isolation levels and conditions evaluated on recorded rows),
  Redis, MongoDB, S3 / DynamoDB, outside calls and in-memory state.
- In-memory state is tracked automatically: containers (and values inside them), the app's objects, pydantic
  models and module variables the routes' code reaches.
- HTML report with the story of each race, key moments and a zoomable timeline.
- A background task that loops (reads and writes the same state round after round) is checked against requests
  in every round, not only its first; a deadlock is found even when the other transaction commits before the
  deadlock error is logged (the order Linux produces).

### Replay and tools
- Deterministic replay of the recorded interleaving with a virtual clock, no database or network needed; worker
  threads are ordered at their accesses to shared state.
- `deja verify`, `diff`, `export-test`, `shrink`, `explore` (other orderings) and `step` (pause and inspect).
- Source fingerprints, hash-seed replay, tracking allow / deny lists, versioned recording format.
- The CLI documents every option, shows defaults, and exits with 0 (success), 1 (check failed) or 2 (couldn't run,
  for example an unreadable recording) without a traceback.

### Documentation
- A documentation site (MkDocs Material): getting started, how it works, guides for each kind of app and feature,
  CLI and Python API reference, supported libraries, configuration, recording format, limits and troubleshooting.
  Its examples run in the test suite, and tests check that it covers every command, option and public name.
- Type annotations and Google-style docstrings on the public API.

### Project
- Licensed under the Apache License 2.0 (`LICENSE`, `NOTICE`).
- Releases are published by `.github/workflows/publish.yml` through PyPI trusted publishing (TestPyPI first);
  the version is set in one place, `src/deja/__init__.py`.
- `py.typed`: type checkers use Déjà's annotations. mypy checks the package in CI.
- Coverage is measured and enforced: the Linux 3.14 CI job fails below 80% (about 82% of statements and
  branches today).
- `compose.yaml` starts every service the tests use (`docker compose up -d --wait`), locally and in CI.
- The PyPy job runs the whole suite; tests whose driver doesn't run on PyPy (asyncpg, grpcio) skip themselves.
  A threads test timed the whole replay, including the heap scan that finds shared state (seconds on PyPy with a
  big heap); it now times only the replayed steps, so it no longer fails at random when the machine is busy.
- pre-commit hooks (ruff and file checks) and monthly Dependabot updates.
- The HTML report loads nothing from the network: no web fonts, and a content security policy that forbids it.
- Quitting `deja step` at a pause no longer loses the paused step; a stopped replay ends its leftover tasks
  cleanly (no "coroutine was never awaited" or "Task was destroyed but it is pending" messages).

[Unreleased]: https://github.com/Morning0Star/Deja/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Morning0Star/Deja/releases/tag/v0.1.0
