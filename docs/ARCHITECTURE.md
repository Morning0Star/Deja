# Déjà architecture (v1 plan, 2026-11-07)

Builds on [DESIGN.md](DESIGN.md) (problem, prior art) and the feasibility spikes in [../spikes/RESULTS.md](../spikes/RESULTS.md).
Every decision below cites the evidence it rests on.

## 1. Success criteria

| # | Criterion | Target |
|---|---|---|
| S1 | Reproduction | A recorded failing run replays to the same failure ≥ 99/100 times; a recorded passing run replays clean ≥ 99/100 |
| S2 | Overhead, real service | ≤ 5% p99 latency and ≤ 5% throughput loss for a FastAPI + Postgres + HTTP-client app at a stated load and hardware |
| S3 | Overhead, worst case | ≤ 15% on the pure-scheduling microbenchmark (spike S2 workload) |
| S4 | Recording size | Reported per request and per second at the S2 load; target ≤ 10 KB per request excluding I/O payloads |
| S5 | Coverage | CPython 3.12–3.15; Linux, Windows, macOS; Selector and Proactor loops |
| S6 | No framework lock-in | Works on an existing FastAPI/Starlette/plain asyncio app with a few lines of setup, no code rewrite |

## 2. System overview

```
RECORD (production)                                         REPLAY (laptop / CI)
──────────────────────────────────────────────────          ─────────────────────────────────────────────
 app code  ──awaits──▶ asyncio                               same app code
             │                                                │
 RecordingLoop._run_once  ── logs each ready batch            ReplayLoop._run_once
   (task ids, plumbing ids)                                    - virtual clock, timers on it
 TaskRegistry  ── stable ids from the creation tree            - runs plumbing FIFO
 Shims ── time / random / uuid / urandom reads                 - forces recorded task-step order
 Adapters ── httpx, asyncpg, SQLAlchemy, redis,                - adapters return recorded results
             run_in_executor results                           - divergence detector
 RequestTracker ── request ids via contextvars              Harness ── re-injects recorded HTTP requests
 RingBuffer ── fixed memory, rolling window                   (ASGI) or calls an entry coroutine
 Trigger ── 5xx / exception / deja.flag() ──▶ .deja file ───▶ Report ── what ran, where it diverged
```

## 3. Components

### 3.1 Loops (`deja/loop/`)

- `RecordingLoop` and `ReplayLoop` are subclasses of `SelectorEventLoop` and (Windows) `ProactorEventLoop`.
- **Single hook point: `_run_once`.** Déjà carries its own copy of CPython's `_run_once` per minor version
  (`deja/loop/_runonce_312.py`, `_313`, `_314`, `_315`), because the recorder must log the exact batch *after* the
  selector and expired timers have been moved into `_ready`, and the replayer must choose which handle runs.
  Rationale: spike S2 (per-pass logging +3–10% vs per-callback wrapping +55–138%), spike S1-C (reordering works).
- Version gate: on an unknown Python version Déjà refuses to record (warns, runs the app unrecorded). Never guess.

### 3.2 Task identity (`deja/ids.py`)

- `TaskRegistry` assigns each task a **path id** at first scheduling: parent path + child ordinal, e.g. `0.2.1`
  (request task 0 → its 3rd child → that child's 2nd child). Parent = `asyncio.current_task()` at creation.
- Never use task names (spike S1: on 3.12 the first step is scheduled before the name is set), `id()` (reused), or
  set/hash ordering (Temporal sdk-python #2069).
- Non-task callbacks ("plumbing": future done-callbacks, gather, TaskGroup, call_soon user callbacks) get
  `(owner path, kind, ordinal)`. In replay they run FIFO (spike S3) but are still checked for presence.
- Eager tasks (`asyncio.eager_task_factory`, 3.12+) run their first step synchronously without `call_soon`.
  The registry must catch them through the task factory; covered by a dedicated test before M1 is done.

### 3.3 Event model (`deja/events.py`)

Compact tuples in the ring buffer, one per fact. Times are loop-relative nanoseconds.

| Event | Fields | Emitted by |
|---|---|---|
| `BATCH` | pass number, count | loop |
| `STEP` | task path | loop (per handle in batch) |
| `PLUMB` | owner path, kind, ordinal | loop |
| `TASK_NEW` | path, parent path, coroutine qualname | registry |
| `TIMER` | owner, due (relative) | loop |
| `IO` | adapter, op, call ordinal, args digest, result ref | adapters |
| `READ` | source (`time.time`, `monotonic`, `random`, `uuid4`, `urandom`...), value | shims |
| `REQ` | request id, phase (start/end), route, status | request tracker |
| `EXC` | task path, exception type, message digest | loop |

### 3.4 Ring buffer and triggers (`deja/buffer.py`)

- Fixed-size, preallocated, per loop; overwrites oldest. Size by memory budget (default 64 MB) not count.
- An index from request id to buffer range.
- Triggers: HTTP 5xx, unhandled task exception, `deja.flag()` from app code, or an assertion hook.
- On trigger, Déjà writes the **causal window**: every event from the start of the earliest request that overlapped
  the failing one, up to the failure. Writing happens off the loop thread (snapshot then hand off).

### 3.5 Recording file (`.deja`)

- Gzipped JSON Lines, stdlib only: one header line, then events, then a blob table for I/O payloads.
- Header: format version, Déjà version, Python version + build, platform, loop class, app entry point, adapter list,
  wall-clock start, and a hash of the app's source files so replay can warn on code drift.
- Payload codecs per adapter (JSON for HTTP bodies and DB rows; base64 for bytes). Redaction hook for secrets
  (headers like `Authorization`, configured fields) applied before anything is written.

### 3.6 Non-determinism shims (`deja/shims.py`)

- Patch `time.time`, `time.monotonic`, `time.perf_counter`, `random.*`, `uuid.uuid4`, `os.urandom`, `secrets.*`
  with wrappers that record values when called from a recorded task and replay them in order.
- The loop's own clock is not patched; `ReplayLoop.time()` is virtual (spike S3).

### 3.7 I/O adapters (`deja/adapters/`)

```python
class Adapter(Protocol):
    name: str
    def install(self, rt: Runtime) -> None: ...     # patch the client library
    def uninstall(self) -> None: ...
```
- Record: call through, store `(op, args digest, result or exception)` with a per-task call ordinal.
- Replay: no network; return the recorded result for the matching `(task path, ordinal)`; if args digest differs,
  raise a divergence.
- v1 adapters: `httpx.AsyncClient` (transport), `asyncpg` (connection methods), SQLAlchemy async (execute events),
  `redis.asyncio`, and `loop.run_in_executor` (result + completion order; spike S3 used executor I/O).
- Un-adapted I/O still works in replay by waiting on the real selector without reordering (spike S3), but is flagged
  as "live I/O" in the report because its results may differ.

### 3.8 Replay engine (`deja/replay.py`)

Per loop pass (generalised from spike S3):
1. Promote timers due on the virtual clock.
2. Run ready plumbing handles in FIFO order.
3. Run the ready handle whose task path equals the next recorded `STEP`.
4. If it isn't ready: jump the virtual clock to the next timer; else feed due adapter completions; else wait on the
   real selector (live I/O) with a bounded timeout.
5. Divergence when: the expected step never becomes ready, an adapter sees different args, an unexpected task or
   exception appears, or the run ends early or late. Report: step number, expected vs actual task path, the
   coroutine and source line, the last N events.

### 3.9 Harness and API

- Record:
  ```python
  import deja
  deja.install(app, adapters=["httpx", "asyncpg"], buffer_mb=64)   # ASGI middleware + loop factory
  # or: asyncio.run(main(), loop_factory=deja.recording_loop)
  ```
  For uvicorn: `uvicorn app:app --loop deja.uvicorn:loop` (custom loop setup).
- Replay: `deja replay crash.deja --app app.main:app` re-sends the recorded HTTP requests at their recorded virtual
  times through the ASGI app; `deja replay crash.deja --entry pkg.mod:main` for plain asyncio.
- Inspect: `deja show crash.deja` prints requests, tasks, the interleaving around the failure, and (M5) a per-request
  breakdown: queued in the loop vs running vs awaiting each adapter.
- Later: `pytest --deja-replay crash.deja` turns a recording into a regression test.

## 4. Decision log

| # | Decision | Why |
|---|---|---|
| D1 | Hook `_run_once` with a vendored per-version copy | Spike S2: per-pass logging is 10–40× cheaper than wrapping callbacks; recorder needs the post-select batch |
| D2 | Stable path ids from the task creation tree | Spike S1 (3.12 names unset at first schedule); Temporal #2069 (hash order) |
| D3 | Capture I/O at client-library level | Socket-level capture can't map bytes to tasks cheaply; adapters give typed results and redaction |
| D4 | Standard loops first, uvloop later | uvloop is Cython; private loop internals differ. Record under the stdlib loop in v1 |
| D5 | Stdlib-only core | Easy to drop into production; adapters import their library lazily |
| D6 | Python 3.12+ | `asyncio.run(loop_factory=...)`, eager tasks exist, 3.11 support adds cost for little reach |
| D7 | Refuse to record on unknown Python versions | A wrong `_run_once` copy would silently produce bad recordings |

## 5. Testing strategy

- **Acceptance (per gallery bug):** record failing → replay 100× → 100 failures; record passing → replay 100× → 0
  failures (the spike S3 protocol).
- **Determinism fuzzing:** a seeded schedule fuzzer (seedloop-style) generates many interleavings of the gallery
  apps; every recorded run must replay to an identical event stream.
- **Cross-version matrix:** CPython 3.12–3.15 × Linux/Windows/macOS × Selector/Proactor in CI.
- **Golden files:** `.deja` format round-trip and backward compatibility.
- **Overhead tracking:** spike S2 microbenchmark in CI with a regression threshold.

## 6. Benchmark method (M4)

- App: FastAPI + uvicorn, Postgres via asyncpg, an outbound httpx call to a stub service, realistic handlers.
- Load: k6 (or wrk) at fixed rates (e.g. 200, 500, 1,000 req/s) for 5 minutes after warm-up.
- Report: throughput, p50/p95/p99 latency, CPU, RSS, recording bytes per request; recorder off vs on; 3 runs each.
- Hardware: an Oracle Always Free ARM VM (stated exactly) plus the dev laptop for reference.

## 7. Milestones (revised)

| # | Deliverable | Est. | Exit criteria |
|---|---|---|---|
| M0 | Race gallery: check-then-act, lost update, double charge (FastAPI + in-memory and Postgres variants) + stress script | 1 weekend | Each bug hits only some of the time; hit rates measured |
| M1 | Recorder: vendored `_run_once` 3.12/3.14, registry with path ids + eager tasks, ring buffer, triggers, `.deja` writer, shims | 2 weekends | Gallery failures produce `.deja` files; S3 overhead on microbench |
| M2 | Replay engine + divergence report + harness (ASGI, entry coroutine) | 2 weekends | S1 met for in-memory gallery apps |
| M3 | Adapters: executor, httpx, asyncpg, SQLAlchemy async, redis | 2 weekends | S1 met with no DB or network running |
| M4 | Benchmark + results write-up | 1 weekend | S2–S4 measured and published |
| M5 | `deja show` + per-request breakdown + 3.13/3.15 copies + CI matrix | 1–2 weekends | S5 met; README demo GIF |

## 8. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Real-service overhead above 5% | Medium | Per-pass logging (D1); `array`-backed buffer; sampling mode; optional C accelerator later |
| `_run_once` internals change between versions | High (known) | Vendored per-version copies + version gate (D7) + CI matrix |
| Eager tasks / TaskGroup / cancellation ordering edge cases | Medium | Dedicated tests in M1; fuzzing |
| Recording size under load | Medium | Causal-window dumps, payload limits, redaction |
| Code drift between record and replay | Medium | Source hash in header; warn before replay |
| uvloop users can't record | Certain in v1 | Documented; record on a staging pod with the stdlib loop |
