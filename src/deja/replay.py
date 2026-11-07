"""Replay a ``.deja`` recording against the same app, forcing the recorded interleaving.

What is replayed is *request scope*: every step of every ``q{rid}`` task (a request) and its children, in the
recorded order. Everything else (lifespan, plumbing callbacks, the harness itself) is environment and runs freely in
FIFO order, as in spike S3.

Per loop pass (``ReplayLoop._run_once``):
  1. promote timers that are due on the virtual clock;
  2. run the first ready *environment* handle, if any;
  3. otherwise take the next schedule entry: ``("start", rid)`` creates the request task and runs its first step;
     ``("step", path)`` runs the ready handle of that task;
  4. if the expected task isn't ready: poll real I/O (worker threads, sockets) without blocking; if nothing
     arrived, jump the virtual clock to the next timer; if there is none, wait on real I/O up to a deadline;
  5. past the deadline the replay stops with a divergence report.

Values the app read from clocks / random / uuid inside request scope are handed back from the recording.
"""

import asyncio
import collections
import contextvars
import time as _time

from . import events as ev
from .fileformat import read as read_file
from .loop import _RecordingMixin
from .recorder import Recorder

_replay_rid: contextvars.ContextVar = contextvars.ContextVar("deja_replay_rid", default=None)
_real_monotonic = _time.monotonic


class Divergence(Exception):
    """The replay could not follow the recording."""


# ---------------------------------------------------------------------- schedule
class Schedule:
    def __init__(self, header: dict, events: list):
        self.header = header
        self.entries: list[tuple] = []  # ("start", rid) | ("step", path)
        self.requests: dict[int, tuple] = {}
        self.responses: dict[int, tuple] = {}
        self.reads: dict[str, collections.deque] = collections.defaultdict(collections.deque)
        for e in events:
            code = e[0]
            if code == ev.REQUEST:
                self.entries.append(("start", e[1]))
                self.requests[e[1]] = e
            elif code == ev.STEP and e[1] and e[1].startswith("q"):
                self.entries.append(("step", e[1]))
            elif code == ev.READ and e[1] and e[1].startswith("q"):
                self.reads[e[1]].append((e[2], e[3]))
            elif code == ev.RESPONSE:
                self.responses[e[1]] = (e[2], e[3] if len(e) > 3 else b"")

    @classmethod
    def load(cls, path: str) -> "Schedule":
        return cls(*read_file(path))

    def expected_path(self, i: int) -> str:
        kind, val = self.entries[i]
        return f"q{val}" if kind == "start" else val

    def __len__(self):
        return len(self.entries)


# ---------------------------------------------------------------------- recorder used during replay
class ReplayRecorder(Recorder):
    """Same event log as recording (useful for reports), but request scope comes from the harness and reads come
    back from the recording."""

    def __init__(self, schedule: Schedule):
        super().__init__()
        self.schedule = schedule
        self.read_misses: list[tuple] = []

    def request_start(self, method, path, headers, body) -> int:
        rid = _replay_rid.get()
        self.buffer.append((ev.REQUEST, rid, self.current_path(), method, path, [list(h) for h in headers], body))
        return rid

    def request_end(self, rid, status, body=b""):
        self.buffer.append((ev.RESPONSE, rid, status, body[:65536]))

    def forget_request(self, rid):
        pass

    def read(self, source, value):
        path = self.current_path()
        if path is None or not path.startswith("q"):
            return value
        queue = self.schedule.reads.get(path)
        if queue and queue[0][0] == source:
            recorded = queue.popleft()[1]
            self.buffer.append((ev.READ, path, source, recorded))
            return recorded
        self.read_misses.append((path, source))
        return value


# ---------------------------------------------------------------------- loop
class ReplayLoop(_RecordingMixin, asyncio.SelectorEventLoop):
    def __init__(self, schedule: Schedule, deadline_s: float = 10.0):
        super().__init__()
        self._deja_setup(ReplayRecorder(schedule))
        self.schedule = schedule
        self.pos = 0
        self.vt = 0.0
        self.armed = False  # the schedule is enforced only once the harness has finished startup
        self.harness = None
        self.trace: list[str] = []  # controlled steps actually run, in order
        self.extra: list[str] = []  # controlled steps run after the schedule was exhausted
        self.deadline_s = deadline_s
        self._last_progress = _real_monotonic()
        self.done = asyncio.Event()

    def time(self):
        return self.vt

    # ------------------------------------------------------------------ classification
    def _path_of(self, handle):
        owner = getattr(handle._callback, "__self__", None)
        if isinstance(owner, asyncio.Task):
            return self.deja.ids.path(owner)
        return None

    def _controlled(self, path) -> bool:
        return path is not None and path.startswith("q")

    # ------------------------------------------------------------------ helpers
    def _promote_timers(self):
        import heapq

        while self._scheduled and self._scheduled[0]._cancelled:
            heapq.heappop(self._scheduled)._scheduled = False
        while self._scheduled and self._scheduled[0]._when <= self.vt:
            h = heapq.heappop(self._scheduled)
            h._scheduled = False
            self._ready.append(h)

    def _poll(self, timeout):
        self._process_events(self._selector.select(timeout))

    def _progress(self):
        self._last_progress = _real_monotonic()

    def _check_deadline(self):
        if _real_monotonic() - self._last_progress > self.deadline_s:
            raise Divergence(self.report())

    def report(self) -> str:
        i = self.pos
        expected = self.schedule.expected_path(i) if i < len(self.schedule) else "(end of recording)"
        ready = [p for p in (self._path_of(h) for h in self._ready) if self._controlled(p)]
        recent = self.trace[-8:]
        return (f"replay diverged at step {i} of {len(self.schedule)}: expected {expected}, "
                f"ready request tasks {ready or 'none'}; last steps {recent}")

    def _run(self, handle, path=None):
        if path is not None:
            self.trace.append(path)
        self._progress()
        handle._run()

    # ------------------------------------------------------------------ the hook
    def _run_once(self):
        ready = self._ready
        while True:
            self._promote_timers()

            # environment first, FIFO
            for i, h in enumerate(ready):
                if h._cancelled:
                    del ready[i]
                    return
                path = self._path_of(h)
                if not self.armed or not self._controlled(path):
                    del ready[i]
                    self._run(h)
                    return

            if not self.armed:  # startup: behave like a normal loop on the virtual clock
                if ready:
                    continue
                if self._scheduled:
                    self.vt = self._scheduled[0]._when
                    continue
                self._poll(0.01)
                self._check_deadline()
                return

            sched = self.schedule
            if self.pos >= len(sched):
                if not self.done.is_set():
                    self.done.set()
                    continue  # waking the driver made environment work ready; run that first
                if ready:  # controlled steps beyond the recording: run them, but note them
                    h = ready.popleft()
                    path = self._path_of(h)
                    self.extra.append(path)
                    self._run(h, path)
                    return
                if self._scheduled:
                    self.vt = self._scheduled[0]._when
                    continue
                self._poll(0.05)
                self._check_deadline()
                return

            kind, val = sched.entries[self.pos]
            want = f"q{val}" if kind == "start" else val
            if kind == "start" and not self.harness.started(val):
                self.harness.start(val)
                self._progress()
                continue
            for i, h in enumerate(ready):
                if self._path_of(h) == want:
                    del ready[i]
                    self.pos += 1
                    self._run(h, want)
                    return

            # expected task not ready yet
            before = len(ready)
            self._poll(0)
            if len(ready) != before:
                continue
            if self._scheduled:
                self.vt = max(self.vt, self._scheduled[0]._when)
                continue
            self._poll(0.01)
            self._check_deadline()


# ---------------------------------------------------------------------- ASGI harness
class ASGIHarness:
    def __init__(self, loop: ReplayLoop, app):
        self.loop = loop
        self.app = app
        self.tasks: dict[int, asyncio.Task] = {}
        self.results: dict[int, tuple] = {}
        self.state: dict = {}
        self._lifespan = None

    def started(self, rid) -> bool:
        return rid in self.tasks

    def start(self, rid):
        task = self.loop.create_task(self._serve(rid))
        self.loop.deja.ids.rebind(task, f"q{rid}")
        self.tasks[rid] = task

    async def _serve(self, rid):
        _replay_rid.set(rid)
        q = self.loop.schedule.requests[rid]
        _, _, _, method, target, headers, body = q[:7]
        path, _, query = target.partition("?")
        scope = {
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": method, "scheme": "http",
            "path": path, "raw_path": path.encode("latin-1"), "query_string": query.encode("latin-1"),
            "root_path": "", "headers": [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers],
            "client": ("127.0.0.1", 50000), "server": ("deja-replay", 80), "state": dict(self.state),
        }
        sent = False
        status, out = None, []

        async def receive():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body or b"", "more_body": False}
            await asyncio.get_running_loop().create_future()  # the client never disconnects

        async def send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body":
                out.append(message.get("body", b""))

        try:
            await self.app(scope, receive, send)
        except Exception as exc:  # the recorded run may have failed the same way
            self.results[rid] = (status or 500, f"{type(exc).__name__}: {exc}".encode())
            return
        self.results[rid] = (status, b"".join(out))

    async def startup(self):
        inbox, outbox = asyncio.Queue(), asyncio.Queue()
        scope = {"type": "lifespan", "asgi": {"version": "3.0"}, "state": self.state}

        async def receive():
            return await inbox.get()

        async def send(message):
            await outbox.put(message)

        async def run():
            try:
                await self.app(scope, receive, send)
            except Exception:
                await outbox.put({"type": "lifespan.unsupported"})

        self._lifespan = (asyncio.ensure_future(run()), inbox, outbox)
        await inbox.put({"type": "lifespan.startup"})
        msg = await outbox.get()
        if msg["type"] == "lifespan.startup.failed":
            raise RuntimeError(f"app startup failed: {msg.get('message')}")

    async def shutdown(self):
        if not self._lifespan:
            return
        task, inbox, outbox = self._lifespan
        if not task.done():
            await inbox.put({"type": "lifespan.shutdown"})
            try:
                await asyncio.wait_for(outbox.get(), 5)
            except Exception:
                pass


# ---------------------------------------------------------------------- entry point
class ReplayResult:
    def __init__(self, schedule, loop, harness, divergence):
        self.recorded = schedule.responses
        self.replayed = harness.results
        self.divergence = divergence
        self.steps_expected = len(schedule)
        self.steps_followed = loop.pos
        self.extra_steps = list(loop.extra)
        self.read_misses = list(loop.deja.read_misses)

    @property
    def followed(self) -> bool:
        return self.divergence is None and self.steps_followed == self.steps_expected

    @property
    def same_responses(self) -> bool:
        return all(self.replayed.get(rid) == resp for rid, resp in self.recorded.items())

    def summary(self) -> str:
        lines = [f"followed {self.steps_followed}/{self.steps_expected} recorded steps"
                 + (f", {len(self.extra_steps)} extra" if self.extra_steps else "")]
        if self.divergence:
            lines.append("DIVERGED: " + self.divergence)
        for rid in sorted(set(self.recorded) | set(self.replayed)):
            rec, rep = self.recorded.get(rid), self.replayed.get(rid)
            mark = "same" if rec == rep else "DIFFERENT"
            lines.append(f"  request {rid}: recorded {_fmt(rec)} | replayed {_fmt(rep)}  [{mark}]")
        return "\n".join(lines)


def _fmt(resp):
    if resp is None:
        return "-"
    status, body = resp
    text = body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body)
    return f"{status} {text[:80]}"


def replay(recording, app_factory, deadline_s: float = 10.0) -> ReplayResult:
    """Replay ``recording`` (a path or a Schedule) against a fresh app from ``app_factory()``."""
    schedule = recording if isinstance(recording, Schedule) else Schedule.load(recording)
    loop = ReplayLoop(schedule, deadline_s=deadline_s)
    app = app_factory()
    harness = ASGIHarness(loop, app)
    loop.harness = harness
    divergence = None

    async def drive():
        await harness.startup()
        loop.armed = True
        await loop.done.wait()
        pending = [t for t in harness.tasks.values() if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=deadline_s)
        await harness.shutdown()

    try:
        loop.run_until_complete(drive())
    except Divergence as d:
        divergence = str(d)
    finally:
        try:
            for t in asyncio.all_tasks(loop):
                t.cancel()
            loop.run_until_complete(asyncio.sleep(0))
        except Exception:
            pass
        loop.close()
    return ReplayResult(schedule, loop, harness, divergence)
