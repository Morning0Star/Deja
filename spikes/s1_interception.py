"""Spike S1: can a loop subclass see every task step and future callback, and pick the next ready handle?

Checks assumptions A-D and F from docs/DESIGN.md. Run with each interpreter:
    py -3.12 spikes/s1_interception.py
    py -3.14 spikes/s1_interception.py
"""

import asyncio
import collections
import sys


def make_loop_cls(base):
    class Probe(base):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self.soon_calls = collections.Counter()
            self.order = []  # (task name or callback kind) in execution order

        def call_soon(self, callback, *args, context=None):
            owner = getattr(callback, "__self__", None)
            kind = type(owner).__name__ if owner is not None else getattr(callback, "__qualname__", repr(callback))
            self.soon_calls[kind] += 1
            name = owner.get_name() if isinstance(owner, asyncio.Task) else kind

            def wrapped(*a):
                self.order.append(name)
                return callback(*a)

            return super().call_soon(wrapped, *args, context=context)

        # Assumption C: reorder the ready queue before each iteration (private API, version-sensitive)
        def _run_once(self):
            if getattr(self, "reverse_ready", False) and len(self._ready) > 1:
                self._ready.reverse()
            return super()._run_once()

    return Probe


async def worker(name, shared, log):
    v = shared["n"]
    await asyncio.sleep(0)  # the race window
    shared["n"] = v + 1
    log.append(name)


async def scenario():
    shared = {"n": 0}
    log = []
    fut = asyncio.get_running_loop().create_future()
    fut.add_done_callback(lambda f: None)
    asyncio.get_running_loop().call_later(0, fut.set_result, 1)
    await fut
    await asyncio.gather(*(asyncio.create_task(worker(f"w{i}", shared, log), name=f"w{i}") for i in range(3)))
    return shared["n"], log


def run(base, reverse):
    cls = make_loop_cls(base)
    loop = cls()
    created = []
    loop.set_task_factory(lambda l, coro, **kw: (created.append(1), asyncio.Task(coro, loop=l, **kw))[1])
    loop.reverse_ready = reverse
    try:
        result = loop.run_until_complete(scenario())
    finally:
        loop.close()
    return result, loop.soon_calls, loop.order, len(created)


def main():
    print(sys.version.split()[0], sys.platform, "C Task:", asyncio.Task.__module__)
    bases = [("Selector", asyncio.SelectorEventLoop)]
    if sys.platform == "win32":
        bases.append(("Proactor", asyncio.ProactorEventLoop))
    for label, base in bases:
        for reverse in (False, True):
            (n, log), calls, order, created = run(base, reverse)
            print(f"[{label} reverse={reverse}] final n={n} finish_order={log} task_factory_calls={created}")
            print("   call_soon by owner:", dict(calls))
            print("   exec order:", order)


if __name__ == "__main__":
    main()
