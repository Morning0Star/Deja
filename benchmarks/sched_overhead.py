"""Worst-case recorder overhead: tasks that do nothing but yield (the spike S2 workload), plain loop vs RecordingLoop.

    python benchmarks/sched_overhead.py
Target (architecture S3): <= 15%.
"""

import asyncio
import sys
import time

from deja.loop import recording_loop

N, K, REPEAT = 2000, 50, 15


async def job():
    for _ in range(K):
        await asyncio.sleep(0)


async def main():
    await asyncio.gather(*(asyncio.create_task(job()) for _ in range(N)))


def once(factory):
    loop = factory()
    t = time.process_time()  # CPU time of this process: robust to other load on a desktop machine
    loop.run_until_complete(main())
    dt = time.process_time() - t
    loop.close()
    return dt


def bench_pair(a, b):
    """Alternate the two variants so machine noise hits both equally; return medians."""
    ta, tb = [], []
    for _ in range(REPEAT):
        ta.append(once(a))
        tb.append(once(b))
    ta.sort(); tb.sort()
    return ta[len(ta) // 2], tb[len(tb) // 2]


if __name__ == "__main__":
    base = asyncio.SelectorEventLoop if sys.platform != "win32" else asyncio.ProactorEventLoop
    steps = N * (K + 1)
    plain, rec = bench_pair(base, lambda: recording_loop())
    print(f"{sys.version.split()[0]} {sys.platform} {base.__name__} steps={steps}")
    print(f"  plain      {plain*1e3:8.1f} ms  {plain/steps*1e9:6.0f} ns/step")
    print(f"  recording  {rec*1e3:8.1f} ms  {rec/steps*1e9:6.0f} ns/step  (+{(rec/plain-1)*100:.1f}%)")
