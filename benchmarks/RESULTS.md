# Benchmark results

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
