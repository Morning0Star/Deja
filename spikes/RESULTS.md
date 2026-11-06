# Feasibility spikes (2026-11-07, Windows 11, CPython 3.12.7 and 3.14.0)

Throwaway experiments that test the assumptions the architecture rests on. Re-run with `py -3.12 spikes/<file>` and
`py -3.14 spikes/<file>`.

## S1: interception ([s1_interception.py](s1_interception.py))

| Assumption | Result |
|---|---|
| A. The C `_asyncio.Task` schedules every step through `loop.call_soon` (overridable) | **Yes**, on 3.12 and 3.14 (9 task-owned `call_soon` calls for 4 tasks: first steps, re-steps after `sleep(0)`, wakeups) |
| B. Future completion / gather plumbing also goes through `call_soon` | **Yes** (`gather._done_callback`, user done-callbacks, `_run_until_complete_cb`) |
| C. A loop subclass can choose which ready handle runs next (via private `_ready` / `_run_once`) | **Yes**: reversing `_ready` reversed task execution order on both versions |
| D. A task factory works with the C Task | **Yes** (called for all 4 tasks) |
| F. Selector and Proactor loops behave the same | **Yes**; Proactor adds 2 internal `call_soon` calls of its own |

Finding that changes the design: on **3.12 a task's first step is scheduled inside `Task.__init__` before its name is
set** (it shows as `Task-N`, not the given name). Task identity must be assigned by Déjà at first scheduling, never
read from names or `id()`.

## S2: overhead ([s2_overhead.py](s2_overhead.py))

Worst case: 2,000 tasks × 51 steps that do nothing but yield (102,000 steps). Best of 5.

| Variant | 3.12 | 3.14 |
|---|---|---|
| plain loop | 2,257 ns/step | 2,034 ns/step |
| wrap every callback (closure) | +55% | +70% |
| wrap + record one tuple per step | +115% | +138% |
| **log the ready batch once per loop pass (no wrappers)** | **+2.9%** | **+9.7%** |

Decision: hook the loop pass, not individual callbacks. Real handlers spend most time awaiting I/O, so the share in a
real service will be lower than this pure-scheduling worst case; to be measured in M4.

## S3: record and replay a real race ([s3_record_replay.py](s3_record_replay.py))

Two requests charge 100 from a balance of 100 (check-then-act across awaits). The "I/O" runs on worker threads with
OS-random durations that are **not** recorded, so timing differs on every run.

| | 3.12 | 3.14 |
|---|---|---|
| plain runs with a double charge | 168/200 | 165/200 |
| replay of a recorded **failing** run | **100/100 fail** | **100/100 fail** |
| replay of a recorded **passing** run | **0/100 fail** | **0/100 fail** |

The replay loop uses a virtual clock, runs future/gather plumbing in FIFO order, forces task steps into the recorded
order, and waits on real I/O (self-pipe) without running anything out of order. Recording only the task-step order
(8 integers) was enough to fully control the outcome.

Caveats: the spike recorder still wraps callbacks (fine for a spike, too slow for production per S2); eager tasks,
cancellation, TaskGroup, timers racing I/O, and network I/O are not covered yet.
