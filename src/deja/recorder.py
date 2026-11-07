"""The recording runtime: one Recorder per recording loop.

Holds the ring buffer of events, the task registry, and the bookkeeping needed to cut a *causal window* out of the
buffer when something goes wrong: every event from the start of the earliest request that overlapped the failing
one, up to now.
"""

import asyncio
import collections
import itertools
import sys
import time

from . import events as ev
from .ids import TaskRegistry

DEFAULT_MAX_EVENTS = 500_000


class Recorder:
    def __init__(self, max_events: int = DEFAULT_MAX_EVENTS, redact_headers=("authorization", "cookie", "x-api-key")):
        self.buffer: collections.deque = collections.deque(maxlen=max_events)
        self.ids = TaskRegistry()
        self.ids.on_new = self._on_task_new
        self.ids.on_gone = self._on_task_gone
        self.redact = {h.lower() for h in redact_headers}
        self._rids = itertools.count()
        self._active: dict[int, tuple] = {}  # request id -> its REQUEST event (start marker)
        self._overlap: dict[int, set] = {}  # request id -> ids of requests active at any time during it
        self._outer: dict[int, tuple] = {}  # request id -> (task, its path before request scope)
        self.started_at = time.time()
        self.dumps: list[str] = []
        self.loop = None

    # ------------------------------------------------------------------ events
    def _on_task_new(self, path, parent_path, task):
        coro = task.get_coro()
        name = getattr(coro, "__qualname__", type(coro).__name__) if coro is not None else None
        self.buffer.append((ev.TASK_NEW, path, parent_path, name, id(task)))

    def _on_task_gone(self, task_id):
        self.buffer.append((ev.TASK_GONE, task_id))

    def current_path(self):
        task = asyncio.current_task()
        return self.ids.path(task) if task is not None else None

    def read(self, source: str, value):
        """Record a non-deterministic value read inside a task (called by the shims). Returns ``value`` unchanged;
        the replay recorder overrides this to hand back recorded values."""
        self.buffer.append((ev.READ, self.current_path(), source, value))
        return value

    def mark(self, label: str = "") -> tuple:
        """Append a marker; ``dump(path, since=marker)`` saves everything from it on."""
        marker = (ev.MARK, label)
        self.buffer.append(marker)
        return marker

    def flag(self, reason: str = ""):
        self.buffer.append((ev.FLAG, self.current_path(), reason))

    # ------------------------------------------------------------------ requests
    def request_start(self, method: str, path: str, headers, body: bytes) -> int:
        """Open request scope: record the request and re-label the current task ``q{rid}``.

        Replay reproduces request scope only (the ``q`` tasks); everything outside it is environment.
        """
        rid = next(self._rids)
        hdrs = [[k, "<redacted>" if k.lower() in self.redact else v] for k, v in headers]
        task = asyncio.current_task()
        outer = self.ids.path(task) if task is not None else None
        marker = (ev.REQUEST, rid, outer, method, path, hdrs, body)
        self.buffer.append(marker)
        if task is not None:
            self.ids.rebind(task, f"q{rid}")
            self._outer[rid] = (task, outer)
        for other in self._active:
            self._overlap[other].add(rid)
        self._overlap[rid] = set(self._active)
        self._active[rid] = marker
        return rid

    def request_end(self, rid: int, status: int, body: bytes = b""):
        self.buffer.append((ev.RESPONSE, rid, status, body[:65536]))
        self._active.pop(rid, None)
        task, outer = self._outer.pop(rid, (None, None))
        if task is not None and outer is not None:
            self.ids.rebind(task, outer)  # leave request scope

    def request_exception(self, rid: int, exc: BaseException):
        self.buffer.append((ev.EXCEPTION, rid, type(exc).__qualname__, str(exc)[:500]))

    def forget_request(self, rid: int):
        self._overlap.pop(rid, None)
        for s in self._overlap.values():
            s.discard(rid)

    # ------------------------------------------------------------------ resolving the hot-path entries
    def events(self) -> list:
        """Resolve the buffer into event tuples.

        Steps are stored as the owner's ``id()``. One forward pass rebuilds the id -> path map from TASK_NEW and
        TASK_GONE events, so an id reused by a later task resolves to the right path at each point in time.
        Owners that aren't tasks (futures, handles) resolve to PLUMB.
        """
        out = []
        idmap: dict[int, str] = {}
        live = self.ids.get
        step, plumb = ev.STEP, (ev.PLUMB,)
        for item in self.buffer:
            t = type(item)
            if t is int:
                path = idmap.get(item) or live(item)
                out.append((step, path) if path is not None else plumb)
            elif t is str:
                out.append(plumb)
            elif item[0] == ev.TASK_NEW:
                idmap[item[4]] = item[1]
                out.append(item[:4])
            elif item[0] == ev.TASK_GONE:
                idmap.pop(item[1], None)
            else:
                out.append(item)
        return out

    # ------------------------------------------------------------------ windows
    def window(self, rid: int | None = None) -> list:
        """Events of the causal window around request ``rid`` (or the whole buffer when ``rid`` is None)."""
        events = self.events()
        if rid is None:
            return events
        related = {rid} | self._overlap.get(rid, set())
        starts = {e[1] for e in events if e[0] == ev.REQUEST and e[1] in related}
        for i, e in enumerate(events):
            if e[0] == ev.REQUEST and e[1] in starts:
                return events[i:]
        return events  # start marker already evicted: give everything we still have

    def header(self) -> dict:
        loop = self.loop
        return {
            "format": "deja/1",
            "deja": __import__("deja").__version__,
            "python": sys.version.split()[0],
            "implementation": sys.implementation.name,
            "platform": sys.platform,
            "loop": type(loop).__name__ if loop is not None else None,
            "started_at": self.started_at,
            "events_capacity": self.buffer.maxlen,
        }

    def since(self, marker: tuple) -> list:
        """Events from ``marker`` (identity match) to now."""
        start = next((i for i, x in enumerate(self.buffer) if x is marker), 0)
        rest = list(self.buffer)[start:]
        saved, self.buffer = self.buffer, rest  # resolve only the slice
        try:
            return self.events()
        finally:
            self.buffer = saved

    def dump(self, path: str, rid: int | None = None, since: tuple | None = None, **extra) -> str:
        from .fileformat import write

        header = self.header()
        header.update(extra)
        events = self.since(since) if since is not None else self.window(rid)
        write(path, header, events)
        self.dumps.append(path)
        return path
