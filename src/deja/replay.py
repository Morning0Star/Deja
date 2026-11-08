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
import os
import threading
import time as _time

from . import events as ev
from .fileformat import read as read_file
from .loop import _RecordingMixin
from .recorder import Recorder
from .threads import is_thread_path

_replay_rid: contextvars.ContextVar = contextvars.ContextVar("deja_replay_rid", default=None)
_real_monotonic = _time.monotonic
ENV_BURST = 64  # environment handles run in a row before a waiting request step gets its turn


class Divergence(Exception):
    """The replay could not follow the recording."""


# ---------------------------------------------------------------------- schedule
class IOEntry:
    __slots__ = ("op", "digest", "kind", "value", "yielded", "kids")

    def __init__(self, op, digest, kind, value, yielded, kids):
        self.op, self.digest, self.kind, self.value, self.yielded, self.kids = op, digest, kind, value, yielded, kids


class Schedule:
    """The part of a recording replay must reproduce.

    I/O calls recorded by adapters are collapsed: every step the task took inside the library (between IO_START
    and IO_END) is dropped except the last one, which is the step where the app resumed with the result. Reads and
    child tasks that happened inside the library are dropped too (the library will not run during replay); the
    number of such children is kept so later child paths stay identical.
    """

    def __init__(self, header: dict, events: list):
        self.header = header
        self.entries: list[tuple] = []  # ("start", rid) | ("step", path)
        self.requests: dict[int, tuple] = {}
        self.responses: dict[int, tuple] = {}
        self.reads: dict[str, collections.deque] = collections.defaultdict(collections.deque)
        self.io: dict[str, collections.deque] = collections.defaultdict(collections.deque)
        entries: list = []
        open_io: dict[str, dict] = {}
        ignored: set[str] = set()
        started: set[str] = set()  # "q{rid}" of the requests whose start is in the recording
        self.skipped: set[str] = set()  # requests (long connections) that began before the recording window

        def is_ignored(path):
            if path.split(".", 1)[0] not in started:  # began before the recording window: can't be replayed
                if path.startswith("q"):
                    self.skipped.add(path.split(".", 1)[0])
                return True
            return any(path == p or path.startswith(p + ".") for p in ignored)

        for e in events:
            code = e[0]
            if code == ev.REQUEST:
                entries.append(("start", e[1]))
                self.requests[e[1]] = e
                started.add(f"q{e[1]}")
            elif code == ev.BATCH:
                for call in open_io.values():
                    call["passes"] += 1
            elif code == ev.STEP:
                path = e[1]
                if not path or not path.startswith("q") or is_ignored(path):
                    continue
                if path in open_io:
                    open_io[path]["steps"].append(len(entries))
                entries.append(("step", path))
            elif code == ev.TASK_NEW:
                path, parent = e[1], e[2]
                if parent is not None and (parent in open_io or is_ignored(parent)):
                    if parent in open_io:
                        open_io[parent]["kids"] += 1
                    ignored.add(path)
            elif code == ev.IO_START and e[1] and e[1].startswith("q") and not is_ignored(e[1]):
                open_io[e[1]] = {"op": e[2], "steps": [], "kids": 0, "passes": 0}
            elif code == ev.IO_END and e[1] in open_io:
                call = open_io.pop(e[1])
                for i in call["steps"][:-1]:
                    entries[i] = None  # library-internal wakeups: replay will not have them
                # library calls: suspended iff the task stepped inside; executor calls: iff the loop ran a pass
                yielded = bool(call["steps"]) or (e[2] == "executor" and call["passes"] > 0)
                self.io[e[1]].append(IOEntry(e[2], e[3], e[4], e[5], yielded, call["kids"]))
            elif code == ev.READ and e[1] and e[1].startswith("q"):
                if e[1] in open_io or is_ignored(e[1]):
                    continue
                self.reads[e[1]].append((e[2], e[3]))
            elif code == ev.RESPONSE:
                self.responses[e[1]] = (e[2], e[3] if len(e) > 3 else b"")
            elif code == ev.NOTIFY:
                entries.append(("notify", tuple(e[1:5])))
            elif code == ev.TOUCH and is_thread_path(e[1]) and e[1].startswith("q") and not is_ignored(e[1]):
                entries.append(("thread", e[1]))  # a worker thread's access to tracked state: replay gates it here
        self.entries = [x for x in entries if x is not None]
        self.thread_gates = collections.Counter(v for k, v in self.entries if k == "thread")

    @classmethod
    def load(cls, path: str) -> "Schedule":
        return cls(*read_file(path))

    def expected_path(self, i: int) -> str:
        kind, val = self.entries[i]
        if kind == "start":
            return f"q{val}"
        if kind == "notify":
            return f"notification on {val[2]}"
        return f"worker thread {val}" if kind == "thread" else val

    def __len__(self):
        return len(self.entries)


# ---------------------------------------------------------------------- recorder used during replay
class ReplayRecorder(Recorder):
    """Same event log as recording (useful for reports), but request scope comes from the harness and reads come
    back from the recording."""

    replaying = True

    def __init__(self, schedule: Schedule):
        super().__init__()
        self.schedule = schedule
        self.read_misses: list[tuple] = []
        self.io_misses: list[str] = []
        self._io = {k: collections.deque(v) for k, v in schedule.io.items()}
        self.listeners: dict[int, tuple] = {}  # listener id -> (connection, channel, callback), see adapters.asyncpg
        self.ended: dict[int, tuple] = {}  # rid -> (status, body) as the middleware reported it (WebSockets)
        self._outside_n: dict = {}  # out-of-request calls answered so far, per call
        # worker threads (deja.threads): how many tracked accesses each thread call has reached / been let through
        self._gate = threading.Condition()
        self._arrived: collections.Counter = collections.Counter()
        self._granted: collections.Counter = collections.Counter()
        self._finished: set = set()
        self._parked: set = set()  # threads waiting for the event loop (anyio.from_thread)
        self._released = False
        self.gate_timeout = 10.0

    # ------------------------------------------------------------------ worker threads
    def thread_gate(self, path):
        """In a worker thread: wait until the schedule reaches this thread's next recorded access."""
        with self._gate:
            k = self._arrived[path]
            self._arrived[path] = k + 1
            self._gate.notify_all()
            if k >= self.schedule.thread_gates[path]:
                return  # an access the recording doesn't have: nothing to order it against
            deadline = _real_monotonic() + self.gate_timeout
            while self._granted[path] <= k and not self._released:
                left = deadline - _real_monotonic()
                if left <= 0:
                    self.io_misses.append(f"{path}: worker thread waited too long for its turn")
                    return
                self._gate.wait(left)

    def thread_end(self, path):
        with self._gate:
            self._finished.add(path)
            self._gate.notify_all()

    def thread_park(self, path, parked):
        with self._gate:
            (self._parked.add if parked else self._parked.discard)(path)
            self._gate.notify_all()

    def thread_turn(self, path, timeout):
        """On the loop: let ``path`` through its next access, and wait until it has done it (it parks at its next
        access or returns). False if the thread didn't get there in time; None if, before getting there, it is
        waiting for the event loop (the loop must run, then try again)."""
        with self._gate:
            k = self._granted[path]
            end = _real_monotonic() + timeout
            while self._arrived[path] <= k and path not in self._finished:  # not at this access yet
                if path in self._parked:
                    return None
                left = end - _real_monotonic()
                if left <= 0:
                    return False
                self._gate.wait(min(left, 0.05))
            self._granted[path] = k + 1
            self._gate.notify_all()
            while (self._arrived[path] <= k + 1 and path not in self._finished
                   and path not in self._parked):  # doing it, then on to the next (or waiting for the loop)
                left = end - _real_monotonic()
                if left <= 0:
                    return False
                self._gate.wait(min(left, 0.05))
            return True

    def release_threads(self):
        with self._gate:
            self._released = True
            self._gate.notify_all()

    def request_start(self, method, path, headers, body) -> int:
        rid = _replay_rid.get()
        self.buffer.append((ev.REQUEST, rid, self.current_path(), method, path, [list(h) for h in headers], body))
        return rid

    def request_end(self, rid, status, body=b""):
        self.buffer.append((ev.RESPONSE, rid, status, body[:65536], None))
        self.ended[rid] = (status, body[:65536])

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


    def outside(self, op, digest):
        """The recorded result of an out-of-request call (see adapters._outside): the n-th call gets the n-th result
        (the last one once they run out), or None."""
        got = (self.schedule.header.get("catalog") or {}).get("outside", {}).get(f"{op}:{digest}")
        if not got:
            return None
        if isinstance(got[0], str):  # recordings before 0.0.2 kept one ("ok" | "err", value) per call
            return got
        n = self._outside_n.get((op, digest), 0)
        self._outside_n[(op, digest)] = n + 1
        return got[min(n, len(got) - 1)]

    async def _suspend_once(self):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        loop.call_soon(fut.set_result, None)
        await fut

    async def io_replay(self, path, op, digest, decode):
        """Answer an adapter call from the recording, suspending exactly when the recorded call suspended."""
        from . import codec
        from .adapters import OPTIONAL_OPS

        queue = self._io.get(path)
        if op == "ws.receive" and not queue and path.startswith("q") and "." not in path:
            if int(path[1:]) not in self.schedule.responses:
                # the connection was still open when the recording was saved: the client sent nothing more
                await asyncio.get_running_loop().create_future()
        # A client-side cache can make an optional call (prepare) happen in one run and not the other: the recorded
        # app had the statement cached from before the recorded window, or the fresh replay app has it cached now.
        if op in OPTIONAL_OPS and (not queue or queue[0].op != op):
            return decode(None)  # not prepared in the recording: answer at once (a cache hit didn't suspend either)
        while queue and queue[0].op in OPTIONAL_OPS and op != queue[0].op:
            skipped = queue.popleft()  # prepared in the recording but cached in replay: keep its suspension
            if skipped.yielded:
                await self._suspend_once()
        if not queue:
            self.io_misses.append(f"{path}: {op} was not called here in the recording")
            return None
        entry = queue.popleft()
        if entry.op != op or entry.digest != digest:
            self.io_misses.append(f"{path}: recorded {entry.op} but replay called {op}"
                                  + ("" if entry.op != op else " with different arguments"))
        if entry.yielded:
            await self._suspend_once()
        if entry.kids:
            self.ids._kids[path] = self.ids._kids.get(path, 0) + entry.kids
        if entry.kind == "err":
            raise codec.decode_exc(entry.value)
        return decode(entry.value)


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
        self._env_streak = 0
        self.code_changed: list = []  # app modules whose source differs from the recording's

    def time(self):
        return self.vt

    def run_in_executor(self, executor, func, *args):
        from . import adapters, codec

        path = self.deja.current_path()
        queue = self.deja._io.get(path) if path else None
        if not queue or queue[0].op != adapters.EXECUTOR_OP:
            return asyncio.SelectorEventLoop.run_in_executor(self, executor, func, *args)  # not recorded: run live
        entry = queue.popleft()
        key = (getattr(func, "__qualname__", None) or type(func).__name__, args)
        if entry.digest != adapters.digest(key):
            self.deja.io_misses.append(f"{path}: worker-thread call {key[0]} with different arguments")
        tpath = self.deja.thread_path(path)
        if self.schedule.thread_gates[tpath]:  # it changed tracked state: run it again, in the recorded order
            from .loop import _in_thread

            return asyncio.SelectorEventLoop.run_in_executor(self, executor, _in_thread(self.deja, tpath, func), *args)
        fut = self.create_future()
        if entry.kind == "err":
            resolve, value = fut.set_exception, codec.decode_exc(entry.value)
        else:
            resolve, value = fut.set_result, codec.decode(entry.value)
        if entry.yielded:
            self.call_soon(resolve, value)  # the recorded await suspended: resolve on a later pass
        else:
            resolve(value)  # the recorded future was already done: the await must not suspend either
        return fut

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
        ex = self._default_executor
        threads = sum(t.is_alive() for t in getattr(ex, "_threads", ())) if ex else 0
        queued = ex._work_queue.qsize() if ex and hasattr(ex, "_work_queue") else 0
        return (f"replay diverged at step {i} of {len(self.schedule)}: expected {expected}, "
                f"ready request tasks {ready or 'none'}; last steps {recent}; "
                f"[diag: ready handles {len(self._ready)}, timers {len(self._scheduled)}, "
                f"executor threads {threads}, queued jobs {queued}, virtual time {self.vt:.3f}]")

    def _deliver(self, lid, pid, channel, payload):
        """Hand a recorded notification to the listener the replayed app registered in the same order."""
        import inspect

        listeners = self.deja.listeners
        entry = listeners.get(lid) or next((x for x in listeners.values() if x[1] == channel), None)
        if entry is None:
            self.deja.io_misses.append(f"notification on {channel}: no listener was registered during replay")
            return
        conn, _, callback = entry
        if inspect.iscoroutinefunction(callback):
            self.create_task(callback(conn, pid, channel, payload))
        else:
            self.call_soon(callback, conn, pid, channel, payload)

    def _run(self, handle, path=None):
        if path is not None:
            self.trace.append(path)
        if path is not None or not self.armed:  # only request steps count as progress once the schedule runs:
            self._progress()  # environment work that loops forever (a retried cancellation) must still time out
        handle._run()

    # ------------------------------------------------------------------ the hook
    def _run_once(self):
        ready = self._ready
        while True:
            self._promote_timers()

            # environment first, FIFO; but a burst of environment work (anyio re-delivering a cancellation every
            # pass) must not starve the recorded step that is already waiting
            starved = self.armed and self._env_streak >= ENV_BURST
            for i, h in enumerate(ready):
                if starved:
                    break
                if h._cancelled:
                    del ready[i]
                    return
                path = self._path_of(h)
                if not self.armed or not self._controlled(path):
                    del ready[i]
                    self._env_streak += 1
                    self._run(h)
                    if self.armed:
                        self._check_deadline()
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
            if kind == "thread":
                turn = self.deja.thread_turn(val, self.deadline_s)
                if turn is None:  # the thread is waiting for the loop: run the loop (its timers too), then try again
                    self._poll(0)
                    if not ready and self._scheduled:
                        self.vt = max(self.vt, self._scheduled[0]._when)
                        continue
                    if not ready:
                        self._poll(0.001)
                    self._check_deadline()
                    return
                self.pos += 1
                if not turn:
                    self.deja.io_misses.append(f"{val}: worker thread did not reach its recorded access")
                self.trace.append(val)
                self._progress()
                continue
            if kind == "notify":
                self.pos += 1
                self._deliver(*val)
                self._progress()
                continue
            want = f"q{val}" if kind == "start" else val
            if kind == "start" and not self.harness.started(val):
                self.harness.start(val)
                self._progress()
                continue
            for i, h in enumerate(ready):
                if self._path_of(h) == want:
                    del ready[i]
                    self.pos += 1
                    self._env_streak = 0
                    self._run(h, want)
                    return
            if starved:  # the step isn't ready after all: let the environment work run again
                self._env_streak = 0
                continue

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
        if method == "WS":
            return await self._serve_websocket(rid, target, headers)
        path, _, query = target.partition("?")
        scope = {
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": method, "scheme": "http",
            "path": path, "raw_path": path.encode("latin-1"), "query_string": query.encode("latin-1"),
            "root_path": "", "headers": [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers],
            "client": ("127.0.0.1", 50000), "server": ("deja-replay", 80), "state": dict(self.state),
        }
        sent = False
        status, out = None, []
        complete = asyncio.Event()

        async def receive():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body or b"", "more_body": False}
            await complete.wait()  # like uvicorn and httpx: the client goes away once the response is complete
            return {"type": "http.disconnect"}

        async def send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body":
                out.append(message.get("body", b""))
                if not message.get("more_body", False):
                    complete.set()

        try:
            await self.app(scope, receive, send)
        except Exception as exc:  # the recorded run may have failed the same way
            self.results[rid] = (status or 500, f"{type(exc).__name__}: {exc}".encode())
            return
        self.results[rid] = (status, b"".join(out))

    async def _serve_websocket(self, rid, target, headers):
        """A recorded WebSocket connection: the client's messages come back from the recording."""
        from . import codec
        from .adapters import digest
        from .asgi import WS_ERROR, WS_RECEIVE, WS_SEND, ws_send_key, ws_transcript

        path, _, query = target.partition("?")
        scope = {
            "type": "websocket", "asgi": {"version": "3.0"}, "http_version": "1.1", "scheme": "ws", "path": path,
            "raw_path": path.encode("latin-1"), "query_string": query.encode("latin-1"), "root_path": "",
            "headers": [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers],
            "client": ("127.0.0.1", 50000), "server": ("deja-replay", 80), "subprotocols": [],
            "state": dict(self.state),
        }
        rec, sent, me = self.loop.deja, [], f"q{rid}"

        async def receive():  # only reached when the replayed app has no DejaMiddleware (it answers these itself)
            return await rec.io_replay(me, WS_RECEIVE, digest((WS_RECEIVE,)), codec.decode)

        async def send(message):
            if message.get("type") in ("websocket.send", "websocket.close"):
                sent.append(message)
            await rec.io_replay(me, WS_SEND, digest(ws_send_key(message)), codec.decode)

        code = None
        try:
            await self.app(scope, receive, send)
        except Exception:
            code = WS_ERROR
        if rid in rec.ended:  # the app's own DejaMiddleware saw the connection
            self.results[rid] = rec.ended[rid]
        else:
            closed = next((m.get("code", 1000) for m in sent if m.get("type") == "websocket.close"), None)
            self.results[rid] = (code or closed or 1000, ws_transcript(sent))

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
        self.io_misses = list(loop.deja.io_misses)
        self.code_changed = list(loop.code_changed)
        self.skipped = sorted(schedule.skipped)
        seed = schedule.header.get("hash_seed")
        self.hash_seed_differs = bool(seed) and os.environ.get("PYTHONHASHSEED") != seed
        self.hash_seed = seed

    @property
    def followed(self) -> bool:
        return self.divergence is None and self.steps_followed == self.steps_expected and not self.io_misses

    @property
    def same_responses(self) -> bool:
        return all(self.replayed.get(rid) == resp for rid, resp in self.recorded.items())

    def summary(self) -> str:
        lines = [f"followed {self.steps_followed}/{self.steps_expected} recorded steps"
                 + (f", {len(self.extra_steps)} extra" if self.extra_steps else "")]
        if self.skipped:
            lines.append("NOTE: left out " + ", ".join(self.skipped) + ": they began before the recording window "
                         "(long WebSocket connections); a bigger buffer (Recorder(max_events=...)) keeps them")
        if self.hash_seed_differs:
            lines.append(f"NOTE: recorded with PYTHONHASHSEED={self.hash_seed}; set the same value to replay set "
                         "ordering exactly (deja replay does this itself)")
        if self.code_changed:
            lines.append("NOTE: the app's code differs from the recording in " + ", ".join(self.code_changed[:8])
                         + (" and more" if len(self.code_changed) > 8 else ""))
        if self.divergence:
            lines.append("DIVERGED: " + self.divergence)
        for m in self.io_misses[:5]:
            lines.append("I/O MISMATCH: " + m)
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


def replay(recording, app_factory, deadline_s: float = 10.0, track: bool = True) -> ReplayResult:
    """Replay ``recording`` (a path or a Schedule) against a fresh app from ``app_factory()``.

    ``track`` tracks the app's shared in-memory state as :class:`deja.DejaMiddleware` does (see
    :mod:`deja.autotrack`); replay needs it to put worker threads' accesses in the recorded order."""
    schedule = recording if isinstance(recording, Schedule) else Schedule.load(recording)
    loop = ReplayLoop(schedule, deadline_s=deadline_s)
    app = app_factory()
    harness = ASGIHarness(loop, app)
    loop.harness = harness
    divergence = None

    async def drive():
        await harness.startup()
        from .autotrack import Rules, prepare
        from .source import changed

        # as the middleware does on the first request: worker threads are ordered by tracked accesses, and the
        # app's clock names must point at the recording wrappers to get the recorded values back
        rules = (schedule.header.get("catalog") or {}).get("track") or {}
        _, current = prepare(app, track and rules.get("on", True),
                             Rules(rules.get("skip"), rules.get("only")))
        recorded = (schedule.header.get("catalog") or {}).get("source") or {}
        loop.code_changed = changed(recorded, current) if recorded else []
        loop.armed = True
        await loop.done.wait()
        # connections still open at the end of the recording stay open: don't wait for those
        pending = [t for rid, t in harness.tasks.items() if not t.done() and rid in schedule.responses]
        if pending:
            await asyncio.wait(pending, timeout=deadline_s)
        await harness.shutdown()

    try:
        loop.run_until_complete(drive())
    except Divergence as d:
        divergence = str(d)
    finally:
        loop.deja.release_threads()
        try:
            for t in asyncio.all_tasks(loop):
                t.cancel()
            loop.run_until_complete(asyncio.sleep(0))
        except Exception:
            pass
        loop.close()
    return ReplayResult(schedule, loop, harness, divergence)
