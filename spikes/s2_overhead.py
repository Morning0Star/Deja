"""Spike S2: cost of wrapping every scheduled callback and logging one event per executed handle.

Workload: N tasks, each yielding K times (asyncio.sleep(0)), so ~N*K task steps.
Compares the plain loop, a loop that only wraps call_soon, and one that also records a tuple per step.
"""

import asyncio
import sys
import time

N, K, REPEAT = 2000, 50, 5


def make(base, record):
    class L(base):
        def __init__(self):
            super().__init__()
            self.events = []
            self.seq = 0

        def call_soon(self, callback, *args, context=None):
            self.seq += 1
            seq = self.seq
            if record:
                ev = self.events

                def wrapped(*a):
                    ev.append((seq, time.perf_counter_ns()))
                    return callback(*a)
            else:
                def wrapped(*a):
                    return callback(*a)
            return super().call_soon(wrapped, *args, context=context)

    return L


async def job():
    for _ in range(K):
        await asyncio.sleep(0)


async def main():
    await asyncio.gather(*(job() for _ in range(N)))


def bench(factory):
    best = float("inf")
    for _ in range(REPEAT):
        loop = factory()
        t = time.perf_counter()
        loop.run_until_complete(main())
        best = min(best, time.perf_counter() - t)
        loop.close()
    return best


if __name__ == "__main__":
    base = asyncio.SelectorEventLoop
    steps = N * (K + 1)
    plain = bench(base)
    wrap = bench(make(base, record=False))
    rec = bench(make(base, record=True))
    print(f"{sys.version.split()[0]} {sys.platform} steps={steps}")
    for name, t in (("plain", plain), ("wrap", wrap), ("wrap+record", rec)):
        print(f"  {name:12s} {t*1e3:8.1f} ms  {t/steps*1e9:7.0f} ns/step  (+{(t/plain-1)*100:5.1f}%)")


def make_batch(base):
    """Variant B: no wrappers. Log the ready batch once per loop iteration (private _ready / _run_once)."""
    class L(base):
        def __init__(self):
            super().__init__()
            self.events = []

        def _run_once(self):
            ready = self._ready
            if ready:
                ap = self.events.append
                for h in ready:
                    ap(id(h._callback))
            return super()._run_once()

    return L


if __name__ == "__main__":
    batch = bench(make_batch(asyncio.SelectorEventLoop))
    print(f"  {'batch-log':12s} {batch*1e3:8.1f} ms  {batch/steps*1e9:7.0f} ns/step  (+{(batch/plain-1)*100:5.1f}%)")
