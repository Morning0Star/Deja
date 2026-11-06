# Déjà design (v0, 2026-11-07)

## 1. Problem

In asyncio, every `await` is a point where another task can run. A handler that reads shared state, awaits, then
writes based on what it read is a race. Whether it fails depends on the exact order the event loop ran task steps
and on when I/O completed. Production produces the bad order now and then; tests almost never do. The bug report
says "can't reproduce".

Goal: capture the order that actually happened, and replay it deterministically on a developer machine.

## 2. What exists (checked 2026-11-07, primary sources)

| Tool | What it does | Why it doesn't solve this |
|---|---|---|
| JaySoft-Rewind (alpha) | Records one request's dependency calls, time, randomness; replays them | README: "operations within one capture are sequential... does not reconstruct... scheduling" |
| Keploy | eBPF capture of API + DB traffic, replays as mocks for tests | Replays the network, not task interleaving; not built for production volume |
| DBOS, Temporal | Replay workflows from saved step results | Only code written inside their framework; Temporal sdk-python #2069 (open) shows replay order can differ |
| seedloop, simloop, simloom | Deterministic simulation: seeded schedules, virtual time, fault injection | Explore random schedules in tests; don't record what production did |
| Austin 4.1, CPython 3.14 `python -m asyncio ps/pstree`, 3.15 Tachyon, Scalene | Live task graphs and task-aware profiles | Observe and profile; no record/replay |
| pyttd, CodeTracer | Time-travel debugging of a single program | Not production, no asyncio scheduling model |

## 3. What has to be captured

From asyncio's event loop semantics:

1. **Ready-queue order.** `call_soon` callbacks run FIFO. Task steps are scheduled with `loop.call_soon`, so the order
   of executed handles *is* the interleaving.
2. **Batch boundaries.** Each loop iteration runs only the handles that were ready when the iteration began; work
   added during the pass waits. Replay must keep the same batches.
3. **Timers.** `call_later/call_at` live in a heap keyed by `loop.time()`. Replay needs a virtual clock.
4. **I/O readiness and results.** Which socket became ready, and what came back. v1 captures results at the client
   library level (HTTP client, DB driver), not raw sockets.
5. **Non-determinism inside handlers.** `time.time()`, `time.monotonic()`, `random`, `uuid4`, `os.urandom`.
6. **Request identity.** Which HTTP request each task belongs to (contextvars; mind that tasks copy context at creation).

## 4. Architecture

```
          production process                                developer machine
 ┌────────────────────────────────────┐            ┌──────────────────────────────────┐
 │ app (FastAPI / plain asyncio)       │            │ same app code                     │
 │   │                                 │  recording │   │                               │
 │ RecordingLoop (asyncio loop subclass│ ─────────▶ │ ReplayLoop (deterministic)        │
 │   + wrapped call_soon/call_at)      │   (.deja)  │   forces recorded handle order    │
 │ task factory: task ids, request ids │            │   virtual clock                   │
 │ I/O adapters: httpx, asyncpg/SQLA   │            │ I/O adapters return recorded data │
 │ patched clock/random/uuid           │            │ divergence detector + report      │
 │ ring buffer → dump on error / flag  │            └──────────────────────────────────┘
 └────────────────────────────────────┘
```

**Recorder.** A loop subclass (via loop factory / policy) wraps every scheduled callback with a sequence number and
the owning task id. Executing a wrapped handle appends one compact event to a per-loop ring buffer. A task factory
assigns stable task ids (creation order + parent) and request ids. Adapters record each I/O call's arguments digest
and result. Clock/random reads are recorded in order. On error or a flag (`deja.flag()`), the window covering the
involved requests is written to a `.deja` file.

**Replay.** `ReplayLoop` runs the same entry point. Instead of popping the ready queue FIFO, it picks the handle whose
(task id, step index) matches the next recorded event; timers fire on the virtual clock; adapters return recorded
results in order. If the expected handle isn't ready, or an adapter is called with different arguments, replay stops
with a divergence report (expected vs actual step, task, source line).

**Stable task identity** is the hard core: ids must match between record and replay, so they are derived from
creation order within the parent task, not from `id()` or hashing (the exact trap in Temporal #2069).

## 5. Non-goals (v1)

- uvloop (C loop; method wrapping may not apply). Record under the standard loop first; uvloop later.
- Threads, multiprocessing, free-threaded builds.
- Raw socket capture; only instrumented client libraries.
- Reconstructing full process memory. Replay re-executes code; state comes from replayed inputs.

## 6. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M0 | Race gallery: 3 small FastAPI apps with real asyncio races (check-then-act, lost update, double charge) + a stress script | Each bug reproduces only some of the time under load, with measured hit rates |
| M1 | Recorder for the standard loop: handle order, batches, timers, clock/random, task + request ids, ring buffer, dump | A recording of a failing run is written; format documented |
| M2 | ReplayLoop + divergence detector | Every gallery bug replays to the same failure 100/100 times |
| M3 | I/O adapters: httpx, asyncpg / SQLAlchemy async | Gallery apps replay with no network or database running |
| M4 | Overhead benchmark under uvicorn (req/s, p50/p99, CPU, memory) recorder on vs off | Published numbers at a stated request rate and hardware |
| M5 | Per-request time breakdown from the recording (queued vs running vs awaiting I/O) and a readable replay report | Report for each gallery bug |
| Later | uvloop, step backwards in replay, pytest plugin to turn a recording into a regression test | |

## 7. Risks and open questions

- Overhead of wrapping every handle; target < 5% at a stated load. Measure in M4 before promising anything.
- C-accelerated `_asyncio.Task` internals: confirm task steps always go through `loop.call_soon` on 3.12 to 3.15.
- Private loop APIs (`_ready`, `_run_once`) differ across versions; isolate behind one module with version tests.
- Recording size under load; sampling or per-request windows may be needed.
- Context-copy semantics for request ids across `create_task`, `gather`, `TaskGroup`.

## 8. Sources

- asyncio event loop docs: https://docs.python.org/3/library/asyncio-eventloop.html
- asyncio developer notes (run-once batches): https://asyncio-notes.readthedocs.io/en/latest/2_asyncio-eventloop.html
- asyncio CLI tools / call graph: https://docs.python.org/3/library/asyncio-tools.html · https://docs.python.org/3/library/asyncio-graph.html
- Austin features: https://p403n1x87.github.io/austin/latest/features/
- State of Python profiling 2026: https://p403n1x87.github.io/the-state-of-python-profiling-in-2026.html
- JaySoft-Rewind: https://github.com/iamjpsonkar/JaySoft-Rewind · Temporal sdk-python #2069: https://github.com/temporalio/sdk-python/issues/2069
- seedloop: https://github.com/klimavojtech2002/seedloop · simloop: https://github.com/dhruvl/simloop
- aiocop (loop method wrapping incl. uvloop): https://pypi.org/project/aiocop/ · aiotrace (context propagation): https://pypi.org/project/aiotrace/
- PEP 669: https://peps.python.org/pep-0669/
