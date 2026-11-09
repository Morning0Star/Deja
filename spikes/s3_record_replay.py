"""Spike S3: record the step order of a racy program, then force that order to reproduce the bug every time.

The program: two "requests" each charge 100 from a balance of 100 with a check-then-act race across awaits.
A double charge happens only when the second request reads before the first one writes.

Recording: a loop subclass that tags each executed task step with a stable task id (first-seen order) and records
the random draws. Replay: a loop with a virtual clock that, each pass, runs the ready handle matching the next
recorded task id, jumping the clock to the next timer when the expected task isn't ready yet.
"""

import asyncio
import heapq
import random
import sys


# ------------------------------------------------------------------ the racy app
def io_jitter():
    """Simulated I/O on a worker thread; duration comes from the OS, not a recorded RNG."""
    import os, time

    time.sleep(int.from_bytes(os.urandom(2), "big") / 65535 * 0.003)


async def handler(name, db, charges, rng):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, io_jitter)  # request arrives
    balance = db["balance"]  # read
    await loop.run_in_executor(None, io_jitter)  # "database round trip"
    if balance >= 100:  # check
        db["balance"] = balance - 100  # act
        charges.append(name)


async def app(rng):
    db, charges = {"balance": 100}, []
    await asyncio.gather(handler("A", db, charges, rng), handler("B", db, charges, rng))
    return charges


class RecordedRandom:
    """Records draws (record mode) or replays them (replay mode)."""

    def __init__(self, draws=None):
        self.replay = draws is not None
        self.draws = list(draws) if draws else []
        self.i = 0
        self._r = random.Random()

    def uniform(self, a, b):
        if self.replay:
            v = self.draws[self.i]
            self.i += 1
            return v
        v = self._r.uniform(a, b)
        self.draws.append(v)
        return v


# ------------------------------------------------------------------ stable task ids
class TaskIds:
    def __init__(self):
        self.ids = {}

    def of(self, handle):
        owner = getattr(handle._callback, "__self__", None)
        if not isinstance(owner, asyncio.Task):
            return None
        key = id(owner)
        if key not in self.ids:
            self.ids[key] = (len(self.ids), owner)  # keep owner alive so id() isn't reused
        return self.ids[key][0]


# ------------------------------------------------------------------ recorder
class RecordingLoop(asyncio.SelectorEventLoop):
    def __init__(self):
        super().__init__()
        self.tids = TaskIds()
        self.trace = []

    def call_soon(self, callback, *args, context=None):
        h = super().call_soon(callback, *args, context=context)
        self.tids.of(h)  # assign ids in scheduling order
        orig = h._callback
        trace, tid = self.trace, self.tids.of(h)

        def run(*a):
            if tid is not None:
                trace.append(tid)
            return orig(*a)

        h._callback = run
        run.__self__ = None  # plain function; keep tid captured above
        return h


# ------------------------------------------------------------------ replayer
class Divergence(Exception):
    pass


class ReplayLoop(asyncio.SelectorEventLoop):
    def __init__(self, expected):
        super().__init__()
        self.expected = list(expected)
        self.pos = 0
        self.vt = 0.0
        self.tids = TaskIds()
        self.polls = 0

    def time(self):
        return self.vt

    def call_soon(self, callback, *args, context=None):
        h = super().call_soon(callback, *args, context=context)
        self.tids.of(h)
        return h

    def _promote_timers(self):
        while self._scheduled and self._scheduled[0]._when <= self.vt:
            h = heapq.heappop(self._scheduled)
            h._scheduled = False
            if not h._cancelled:
                self._ready.append(h)

    def _run_once(self):
        while True:
            self._promote_timers()
            # non-task callbacks (future/gather plumbing) run in FIFO order first
            for i, h in enumerate(self._ready):
                if h._cancelled or self.tids.of(h) is None:
                    del self._ready[i]
                    if not h._cancelled:
                        h._run()
                    return
            want = self.expected[self.pos] if self.pos < len(self.expected) else None
            for i, h in enumerate(self._ready):
                if self.tids.of(h) == want:
                    del self._ready[i]
                    self.pos += 1
                    h._run()
                    return
            if self._scheduled:  # expected task not ready yet: jump virtual time to the next timer
                self.vt = self._scheduled[0]._when
                continue
            if want is None and not self._ready:
                return
            # wait for real I/O (thread completions arrive via the self-pipe) without running anything out of order
            self._process_events(self._selector.select(0.01))
            self.polls += 1
            if self.polls > 2000:
                raise Divergence(
                    f"step {self.pos}: expected task {want}, ready {[self.tids.of(h) for h in self._ready]}"
                )


# ------------------------------------------------------------------ experiment
def run_plain(trials):
    hits = 0
    for _ in range(trials):
        loop = asyncio.SelectorEventLoop()
        hits += len(loop.run_until_complete(app(RecordedRandom()))) == 2
        loop.close()
    return hits


def record_failure(max_tries=500):
    for attempt in range(1, max_tries + 1):
        loop = RecordingLoop()
        rng = RecordedRandom()
        charges = loop.run_until_complete(app(rng))
        loop.close()
        if len(charges) == 2:
            return attempt, loop.trace, rng.draws
    raise RuntimeError("bug never occurred")


def replay(trace, draws, trials):
    hits = 0
    for _ in range(trials):
        loop = ReplayLoop(trace)
        charges = loop.run_until_complete(app(RecordedRandom(draws)))
        loop.close()
        hits += len(charges) == 2
    return hits


if __name__ == "__main__":
    print(sys.version.split()[0], sys.platform)
    print(f"plain runs: double charge in {run_plain(200)}/200")
    attempt, trace, draws = record_failure()
    print(f"recorded a failing run on attempt {attempt}: {len(trace)} task steps, {len(draws)} random draws")
    print(f"replay: double charge in {replay(trace, draws, 100)}/100")


def record_pass(max_tries=500):
    for attempt in range(1, max_tries + 1):
        loop = RecordingLoop()
        charges = loop.run_until_complete(app(RecordedRandom()))
        loop.close()
        if len(charges) == 1:
            return attempt, loop.trace
    raise RuntimeError("never passed")


if __name__ == "__main__":
    attempt, ok_trace = record_pass()
    print(f"recorded a PASSING run on attempt {attempt}; trace {ok_trace}")
    print(f"replay of passing run: double charge in {replay(ok_trace, [], 100)}/100 (should be 0)")
    _, bad_trace, _ = record_failure()
    print(f"failing trace {bad_trace}")
