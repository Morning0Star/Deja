"""Explore other orderings of a recording's requests, to find races that haven't happened yet.

    deja explore recording.deja --app myapp:create_app [--tries 50] [--seed 1]

The recorded requests are replayed again and again under orderings that did *not* happen: every serial order
(each request runs alone, start to finish, one after the other) and seeded random interleavings (at each step the
next ready request is chosen at random). Each run's answers are an *outcome*. An outcome that no serial order
produces is a race: the requests only answer that way when their steps interleave. The report lists each such
outcome with the order that produced it, so it can be replayed and fixed.

What it can and can't see: in-memory state is real during exploration, so races on it are found in any order.
Database and HTTP answers still come from the recording (there is no database), so an ordering in which the app
would have read different rows isn't modelled; calls the app makes that weren't recorded get empty answers. Those
runs are marked approximate.
"""

import itertools
import random
from collections.abc import Callable
from typing import Any

from .replay import ASGIHarness, ReplayLoop, Schedule, _replay_rid, close_replay_loop  # noqa: F401
from .scope import JobHarness


class ExploreLoop(ReplayLoop):
    """A replay loop that picks the next request step by a policy instead of following the recording."""

    def __init__(self, schedule, policy, deadline_s=10.0):
        super().__init__(schedule, deadline_s=deadline_s)
        self.policy = policy  # ("serial", [rids]) | ("random", seed)
        self.rng = random.Random(policy[1] if policy[0] == "random" else 0)
        self.order: list = []  # the controlled steps taken, in order
        self.rids = sorted(schedule.requests)

    def _root_done(self, rid):
        t = self.harness.tasks.get(rid)
        return t is not None and t.done()

    def _run_once(self):
        ready = self._ready
        while True:
            self._promote_timers()
            for i, h in enumerate(ready):  # environment first, as in replay
                if h._cancelled:
                    del ready[i]
                    return
                if not self.armed or not self._controlled(self._path_of(h)):
                    del ready[i]
                    self._run(h)
                    return
            if not self.armed:
                if ready:
                    continue
                if self._scheduled:
                    self.vt = self._scheduled[0]._when
                    continue
                self._poll(0.01)
                self._check_deadline()
                return
            # start requests: serial starts the next one only once the previous finished; random starts them all
            if self.policy[0] == "serial":
                pending = [r for r in self.policy[1] if not self._root_done(r)]
                if pending and not self.harness.started(pending[0]):
                    self.harness.start(pending[0])
                    self._progress()
                    continue
                allowed = {f"q{pending[0]}"} if pending else set()
            else:
                for rid in self.rids:
                    if not self.harness.started(rid):
                        self.harness.start(rid)
                        self._progress()
                allowed = None
            choices = [
                i
                for i, h in enumerate(ready)
                if self._controlled(self._path_of(h))
                and (allowed is None or self._path_of(h).split(".", 1)[0] in allowed)
            ]
            if choices:
                i = self.rng.choice(choices) if self.policy[0] == "random" else choices[0]
                h = ready[i]
                del ready[i]
                path = self._path_of(h)
                self.order.append(path)
                self._run(h, path)
                return
            if all(self._root_done(r) for r in self.rids):
                if not self.done.is_set():
                    self.done.set()
                    continue
            before = len(ready)
            self._poll(0)
            if len(ready) != before:
                continue
            if self._scheduled:
                self._jump()
                continue
            self._poll(0.01)
            self._check_deadline()
            return


def run_once(schedule, app_factory, policy, deadline_s=10.0) -> dict:
    """One exploration run: ``{"responses", "order", "approximate", "error"}``."""

    loop = ExploreLoop(schedule, policy, deadline_s=deadline_s)
    app = app_factory()
    harness = JobHarness(loop, app) if isinstance(app, dict) else ASGIHarness(loop, app)
    loop.harness = harness
    error = None

    async def drive():
        await harness.startup()
        from .autotrack import prepare

        if not isinstance(app, dict):
            prepare(app)
        loop._progress()
        loop.armed = True
        await loop.done.wait()
        await harness.shutdown()

    try:
        loop.run_until_complete(drive())
    except Exception as exc:  # Divergence (stuck) or a crash: an outcome too
        error = f"{type(exc).__name__}: {str(exc)[:200]}"
    finally:
        loop.deja.release_threads()
        close_replay_loop(loop)
    return {
        "responses": dict(harness.results),
        "order": loop.order,
        "error": error,
        "approximate": bool(loop.deja.io_misses),
    }


def _key(run):
    return tuple(sorted((rid, tuple(v) if isinstance(v, (list, tuple)) else v) for rid, v in run["responses"].items()))


def explore(
    recording: "str | Schedule",
    app_factory: Callable[[], Any],
    tries: int = 50,
    seed: int = 1,
    deadline_s: float = 10.0,
) -> dict:
    """Replay a recording's requests in orderings that didn't happen, to find races that haven't happened yet.

    First every serial order (each request alone, start to finish; for more than five requests, first-to-last
    and last-to-first), then ``tries`` seeded random interleavings. An interleaving whose answers no serial order
    gives is a race.

    Database and HTTP answers still come from the recording, so an ordering in which the app would have read
    different rows isn't modelled; runs that made calls the recording has no answer for are marked approximate.

    Args:
        recording: Path of a ``.deja`` file (or a loaded ``Schedule``).
        app_factory: Builds the app, as for [`replay`][deja.replay.replay].
        tries: Random interleavings to run.
        seed: Seed of the first one; run ``i`` uses ``seed + i``, so a race found can be run again.
        deadline_s: Seconds without progress before a run gives up.

    Returns:
        ``{"requests", "serial", "races", "tries"}``. Each race has the ``seed`` that produced it and the
        ``run``: its ``responses``, ``order`` of steps, ``error`` and ``approximate`` flag. Pass the result to
        [`summary`][deja.explore.summary] for plain text.
    """
    schedule = recording if isinstance(recording, Schedule) else Schedule.load(recording)
    rids = sorted(schedule.requests)
    serial_orders = list(itertools.permutations(rids)) if len(rids) <= 5 else [tuple(rids), tuple(reversed(rids))]
    serial: dict = {}
    for order in serial_orders:
        run = run_once(schedule, app_factory, ("serial", list(order)), deadline_s)
        serial.setdefault(_key(run), (order, run))
    races = {}
    for i in range(tries):
        run = run_once(schedule, app_factory, ("random", seed + i), deadline_s)
        k = _key(run)
        if k not in serial and k not in races:
            races[k] = {"seed": seed + i, "run": run}
    return {
        "requests": rids,
        "serial": [{"order": list(o), "responses": r["responses"]} for o, r in serial.values()],
        "races": list(races.values()),
        "tries": tries,
    }


def summary(result) -> str:
    """The result of [`explore`][deja.explore.explore] as plain text, as ``deja explore`` prints it."""

    def fmt(resps):
        out = []
        for rid in sorted(resps):
            status, body = resps[rid]
            text = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else str(body)
            out.append(f"request {rid}: {status} {text[:70]}")
        return "; ".join(out)

    lines = [f"{len(result['serial'])} distinct outcome(s) from running the requests one at a time"]
    for s in result["serial"]:
        lines.append(f"  order {s['order']}: {fmt(s['responses'])}")
    if not result["races"]:
        lines.append(f"no other outcome in {result['tries']} random interleavings")
    for r in result["races"]:
        run = r["run"]
        lines.append(
            f"RACE (seed {r['seed']}{', approximate' if run['approximate'] else ''}): "
            f"{fmt(run['responses'])}" + (f"  [{run['error']}]" if run["error"] else "")
        )
        lines.append(f"  steps: {' '.join(p.split('.', 1)[0] for p in run['order'])}")
    return "\n".join(lines)
