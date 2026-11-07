# Benchmark results

## Run 2: 2026-11-08, after the first optimisation round (commit 2e22f47)

Same workflow and settings as run 1. Note: this runner was slower overall (plain scheduler microbenchmark 1,410 ns per
step vs 1,083 in run 1), so compare *overhead percentages within a run*, not absolute milliseconds across runs.

| req/s | mode | achieved req/s | p50 ms | p95 ms | p99 ms | server CPU % | RSS MB | failed | recording KB/request |
|---|---|---|---|---|---|---|---|---|---|
| 100 | off | 100.0 | 1.13 | 6.13 | 6.78 | 14.1 | 64 | 0 | - |
| 100 | on | 100.0 | 1.22 | 6.43 | 7.68 | 15.8 | 81 | 0 | 0.10 |
| 250 | off | 250.0 | 1.18 | 7.16 | 7.87 | 34.0 | 66 | 0 | - |
| 250 | on | 250.0 | 1.35 | 7.68 | 10.91 | 38.2 | 89 | 0 | 0.10 |
| 400 | off | 400.0 | 1.80 | 9.01 | 12.24 | 56.0 | 68 | 0 | - |
| 400 | on | 400.0 | 2.12 | 10.38 | 15.39 | 62.7 | 91 | 0 | 0.11 |

| | Run 1 | Run 2 |
|---|---|---|
| CPU overhead | +13 to +16% | **+12%** at every rate |
| Extra memory | +17 to +53 MB | **+17 to +23 MB** (smaller default buffer) |
| p50 overhead | +0.08 to +0.27 ms | +0.09 to +0.32 ms |
| p99 overhead | not trustworthy (outliers in both modes) | **+0.9 to +3.2 ms**, consistent across all 3 repetitions |
| Recording size | 0.12 KB/request | 0.10 KB/request |
| Scheduler worst case | +26% | +25% |

This run had no outliers, so the tail numbers are usable: recording adds about 1 to 3 ms at p99 and about 12% CPU, with
throughput unchanged and no failed or dropped requests.

What changed (profiling showed ~40% less time in Déjà's own code per request): pass-through adapter wrappers return
the library call directly instead of adding a coroutine frame (adapter entries per 3,000 requests: 47,752 → 20,281);
clock reads made inside a recorded library call are no longer recorded (~10 → 0 per request); the argument digest
is computed at dump time; the default buffer holds 200,000 events instead of 500,000.

Remaining ideas: a C accelerator for the `_run_once` hot loop and the event append; sampling (record a fraction of
requests); measuring on a dedicated VM for publishable numbers.

## Run 1: 2026-11-07, GitHub Actions `ubuntu-latest`

Machine: 2 vCPU (AMD EPYC 9V74, shared), 7.7 GiB RAM, CPython 3.14.10, Postgres 17 in Docker on the same machine.
Server pinned to CPU 0, k6 pinned to CPU 1. App: [app.py](app.py) (70% `GET /items/{id}`: one SELECT; 30%
`POST /orders`: SELECT + HTTP call to a 2 ms pricing stub + INSERT). 30 s per run after a 5 s warm-up, 3 repetitions,
modes alternated. Median of 3 shown. Workflow: `.github/workflows/benchmark.yml`.

| req/s | mode | achieved req/s | p50 ms | p95 ms | p99 ms | server CPU % | RSS MB | failed | recording KB/request |
|---|---|---|---|---|---|---|---|---|---|
| 100 | off | 100.0 | 0.94 | 5.70 | 10.79 | 11.4 | 64 | 0 | - |
| 100 | on | 100.0 | 1.02 | 5.96 | 48.76 | 13.0 | 81 | 0 | 0.12 |
| 250 | off | 250.0 | 1.02 | 6.24 | 8.96 | 27.1 | 66 | 0 | - |
| 250 | on | 250.0 | 1.16 | 6.79 | 14.96 | 30.7 | 108 | 0 | 0.12 |
| 400 | off | 400.0 | 0.97 | 6.64 | 10.85 | 42.5 | 68 | 0 | - |
| 400 | on | 399.9 | 1.24 | 14.60 | 32.15 | 49.2 | 121 | 0 | 0.13 |

Scheduler worst case (`sched_overhead.py`, tasks that only yield): +26.0% on the same runner.

### What this shows

- **Throughput is unaffected** at every rate tested; no failed requests.
- **Median latency** rises by 0.08 to 0.27 ms per request.
- **CPU** rises by a consistent 13 to 16% (the clearest signal in the data).
- **Recording size is tiny:** about 0.12 KB per request compressed, including the database rows and HTTP responses
  needed for replay (target was 10 KB).
- **Memory** grows by 17 to 53 MB as the ring buffer fills (default capacity 500,000 events; bounded).
- **Tail latency (p95/p99) is not trustworthy from this run.** The shared runner produced outliers in both modes
  (recording *off* had a 178 ms p99 in one repetition; recording *on* had 255 ms in another, with 23 dropped requests).
  Three 30-second repetitions on a shared 2-vCPU machine can't separate a tail effect from noise.

### Against the targets (docs/ARCHITECTURE.md §1)

| Target | Result |
|---|---|
| S2: ≤ 5% throughput loss | **Met** (0%) |
| S2: ≤ 5% p99 latency | **Not established**: noise dominates; needs a quiet machine and longer runs |
| S3: worst case ≤ 15% | **Not met**: +26% on this runner |
| S4: ≤ 10 KB per request | **Met** (0.12 KB) |

### Next

1. Profile where the extra CPU goes (candidates: encoding every DB row and HTTP response at record time, the
   middleware's per-request bookkeeping, the hot-path append) and cut it.
2. Make the buffer capacity default smaller or size-based to cap memory.
3. Re-run on a dedicated Linux VM with 5+ repetitions and 60 s runs for publishable tail-latency numbers.
