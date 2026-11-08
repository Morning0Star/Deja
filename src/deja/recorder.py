"""The recording runtime: one Recorder per recording loop.

Holds the ring buffer of events, the task registry, and the bookkeeping needed to cut a *causal window* out of the
buffer when something goes wrong: every event from the start of the earliest request that overlapped the failing
one, up to now.
"""

import asyncio
import collections
import contextvars
import itertools
import os
import sys
import time

from . import events as ev
from .fileformat import FORMAT
from .ids import TaskRegistry
from .threads import _thread

_now = time.perf_counter_ns  # bound before the shims patch anything; time.perf_counter_ns is never shimmed

DEFAULT_MAX_EVENTS = 200_000  # ~15-20k requests of history for a typical API; memory stays in the tens of MB


LABELS: dict[str, str] = {}  # friendly names for routes, resources and calls (deja.labels)
# set while the app starts up (ASGI lifespan): app tasks created then are background tasks, b0, b1...
background = contextvars.ContextVar("deja_background", default=False)


def in_scope(path) -> bool:
    """A request (q...) or a background task of the app (b...): what recording orders and the report explains."""
    return bool(path) and path[0] in "qb"
CALL_KINDS: dict[str, str] = {}  # "METHOD host/path" pattern -> "read" | "write" | "ignore" (deja.calls)


def _detail(op, key):
    """Readable details of an I/O call for reports: the SQL and parameters, the HTTP target, the Redis command."""
    from .codec import encode

    try:
        if op.startswith(("pg.", "sql.")) and isinstance(key, tuple) and len(key) == 3:
            _, query, args = key
            return {"sql": str(query)[:4000], "params": [encode(a) for a in list(args)[:50]]}
        if op == "http.send" and isinstance(key, tuple) and len(key) == 3:
            method, url, body = key
            if isinstance(body, (bytes, bytearray)):
                body = bytes(body[:2000]).decode("utf-8", "replace")
            return {"method": method, "url": url, "body": body}
        if op == "redis.cmd" and isinstance(key, tuple):
            return {"args": [str(a)[:200] for a in list(key[1])[:20]]}
        if op == "redis.pipeline" and isinstance(key, tuple):
            import ast

            try:
                commands = [[str(a)[:200] for a in c[:20]] for c in ast.literal_eval(str(key[1]))[:50]]
            except Exception:
                commands = None
            return {"commands": commands} if commands else {"commands_text": str(key[1])[:2000]}
        if op == "pg.tx.start" and isinstance(key, tuple):
            return {"isolation": key[1] if len(key) > 1 else "read_committed"}
        if op == "ws.send" and isinstance(key, tuple) and len(key) == 5:
            _, kind, text, data, code = key
            what = text if text is not None else (f"<{len(data)} bytes>" if data is not None else
                                                   f"close {code}" if kind == "websocket.close" else kind)
            return {"message": str(what)[:2000]}
        if op.startswith("mongo.") and isinstance(key, tuple) and len(key) == 4:
            from .codec import decode

            return {"collection": key[1], "method": key[2], "args": decode(key[3])}
        if op == "kafka.send" and isinstance(key, tuple) and len(key) == 4:
            return {"topic": key[1]}
        if op == "aws.call" and isinstance(key, tuple) and len(key) == 4:
            from .codec import decode

            params = decode(key[3])
            keep = {k: (v if not isinstance(v, (bytes, bytearray)) else f"<{len(v)} bytes>")
                    for k, v in (params or {}).items() if k in ("Bucket", "Key", "Prefix", "TableName", "Key",
                                                                 "QueueUrl", "ConditionExpression")}
            return {"service": key[1], "operation": key[2], "params": keep}
        if op in ("grpc.call", "grpc.stream") and isinstance(key, tuple) and len(key) == 3:
            return {"method": key[1], "request_bytes": len(key[2] or b"")}
        if op == "executor" and isinstance(key, tuple):
            return {"function": key[0], "args": repr(key[1])[:500]}
        if isinstance(key, tuple) and len(key) == 1:
            return {}
        return {"key": repr(key)[:500]}
    except Exception:
        return {}


def _digest(key):
    from .adapters import digest

    return key if isinstance(key, str) else digest(key)


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
        self.catalog: dict = {}  # adapter metadata replay needs (statement columns, startup call results)
        self._listener_ids = itertools.count()  # LISTEN callbacks, numbered in registration order
        self._thread_n: dict[str, int] = {}  # task path -> worker-thread calls made so far (deja.threads)
        self._bg = itertools.count()  # background tasks of the app, numbered in creation order

    # ------------------------------------------------------------------ events
    def _on_task_new(self, path, parent_path, task):
        coro = task.get_coro()
        name = getattr(coro, "__qualname__", type(coro).__name__) if coro is not None else None
        self.buffer.append((ev.TASK_NEW, path, parent_path, name, id(task)))

    def _on_task_gone(self, task_id):
        self.buffer.append((ev.TASK_GONE, task_id))

    def current_path(self):
        t = _thread.get()
        if t is not None and t[0] is self:  # inside a worker-thread call made from request scope
            return t[1]
        try:
            task = asyncio.current_task()
        except RuntimeError:  # another thread, no event loop
            return None
        return self.ids.path(task) if task is not None else None

    def background_name(self, coro) -> str | None:
        """``b{n}`` for a task the app starts during startup running its own code (else None: library internals
        such as a pool's maintenance task stay environment work)."""
        code = getattr(coro, "cr_code", None) or getattr(coro, "gi_code", None)
        if code is None:
            return None
        from .autotrack import _app_file

        if not _app_file(code.co_filename):
            return None
        name = f"b{next(self._bg)}"
        self.catalog.setdefault("background", {})[name] = getattr(coro, "__qualname__", code.co_name)
        return name

    def thread_path(self, path: str) -> str:
        """Path of the next worker-thread call made by task ``path`` (``q3.t0``, ``q3.t1``...)."""
        n = self._thread_n.get(path, 0)
        self._thread_n[path] = n + 1
        return f"{path}.t{n}"

    def thread_end(self, path: str):
        """A worker-thread call returned (called in the thread). Replay uses it to release the event loop."""

    def thread_park(self, path: str, parked: bool):
        """A worker thread starts / stops waiting for the event loop (replay only)."""

    def read(self, source: str, value):
        """Record a non-deterministic value read inside a task (called by the shims). Returns ``value`` unchanged;
        the replay recorder overrides this to hand back recorded values."""
        self.buffer.append((ev.READ, self.current_path(), source, value))
        return value

    def io_start(self, path, op):
        self.buffer.append((ev.IO_START, path, op, _now()))

    def io_end(self, path, op, digest, kind, value):
        self.buffer.append((ev.IO_END, path, op, digest, kind, value, _now()))

    def touch(self, resource: str, kind: str, value=None):
        """App-reported access to in-memory state (``deja.touch``): kind is "read" or "write"."""
        from .codec import encode

        self.buffer.append((ev.TOUCH, self.current_path(), str(resource), kind, encode(value), _now()))

    def next_listener(self) -> int:
        return next(self._listener_ids)

    def notify(self, lid: int, pid, channel: str, payload):
        """A pushed notification (Postgres NOTIFY) reached app listener ``lid``; replay delivers it at this point."""
        self.buffer.append((ev.NOTIFY, lid, pid, channel, payload, _now()))

    def mark(self, label: str = "") -> tuple:
        """Append a marker; ``dump(path, since=marker)`` saves everything from it on."""
        marker = (ev.MARK, label)
        self.buffer.append(marker)
        return marker

    def flag(self, reason: str = ""):
        self.buffer.append((ev.FLAG, self.current_path(), reason, _now()))

    # ------------------------------------------------------------------ requests
    def request_start(self, method: str, path: str, headers, body: bytes) -> int:
        """Open request scope: record the request and re-label the current task ``q{rid}``.

        Replay reproduces request scope only (the ``q`` tasks); everything outside it is environment.
        """
        rid = next(self._rids)
        hdrs = [[k, "<redacted>" if k.lower() in self.redact else v] for k, v in headers]
        task = asyncio.current_task()
        outer = self.ids.path(task) if task is not None else None
        marker = (ev.REQUEST, rid, outer, method, path, hdrs, body, _now())
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
        self.buffer.append((ev.RESPONSE, rid, status, body[:65536], _now()))
        self._active.pop(rid, None)
        root = f"q{rid}"
        for k in [k for k in self._thread_n if k == root or k.startswith(root + ".")]:
            del self._thread_n[k]
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
            elif item[0] == ev.IO_END:  # digest and readable details are computed here, off the hot path
                out.append((item[0], item[1], item[2], _digest(item[3]), item[4], item[5],
                            item[6] if len(item) > 6 else None, _detail(item[2], item[3])))
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
            "format": f"deja/{FORMAT}",
            "deja": __import__("deja").__version__,
            # set / iteration order of str keys depends on it; replay can only match it if it was fixed
            "hash_seed": os.environ.get("PYTHONHASHSEED"),
            "python": sys.version.split()[0],
            "implementation": sys.implementation.name,
            "platform": sys.platform,
            "loop": type(loop).__name__ if loop is not None else None,
            "started_at": self.started_at,
            "events_capacity": self.buffer.maxlen,
            "labels": dict(LABELS),
            "call_kinds": dict(CALL_KINDS),
            "clock": "perf_counter_ns",
            "catalog": {k: dict(v) for k, v in self.catalog.items()},
        }

    _TIME_AT = {ev.BATCH: 2, ev.REQUEST: 7, ev.RESPONSE: 4, ev.IO_START: 3, ev.IO_END: 6, ev.TOUCH: 5, ev.FLAG: 3,
                ev.NOTIFY: 5}

    def slice(self, t0: int, t1: int) -> list:
        """Events between perf_counter_ns ``t0`` and ``t1`` (each event takes the time of the latest timestamp before
        it), for a group request (see :mod:`deja.group`)."""
        events = self.events()
        starts, ends = {}, {}
        for e in events:  # requests still running when the window opened: include them from their start
            if e[0] == ev.REQUEST and len(e) > 7:
                starts[e[1]] = e[7]
            elif e[0] == ev.RESPONSE and len(e) > 4:
                ends[e[1]] = e[4]
        for rid, t in starts.items():
            if t < t0 and (ends.get(rid) is None or ends[rid] >= t0):
                t0 = t
        out, now = [], None
        for e in events:
            i = self._TIME_AT.get(e[0])
            if i is not None and len(e) > i and isinstance(e[i], int):
                now = e[i]
            if now is not None and t0 <= now <= t1:
                out.append(e)
            elif now is not None and now > t1:
                break
        return out

    def first_time(self, events) -> int | None:
        for e in events:
            i = self._TIME_AT.get(e[0])
            if i is not None and len(e) > i and isinstance(e[i], int):
                return e[i]
        return None

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
