"""Several worker processes (uvicorn / gunicorn ``--workers N``): one recording of a race that crossed them.

    app.add_middleware(deja.DejaMiddleware, directory="deja-recordings", group="/shared/deja-group")
    deja merge /shared/deja-group/<id>/ -o race.deja        # one recording, every worker's requests
    deja replay race.deja --app myapp:create_app             # one app per worker, each with its own memory

Each worker records on its own. When one saves a recording, it also posts the time window to the group directory;
every worker (the saving one too) notices within half a second and writes its own events for that window to
``<group>/<id>/<worker>.deja``. All processes on a machine share the same high-resolution clock, so the files line
up: :func:`merge` interleaves them by time (each loop pass carries its timestamp) and renames each worker's
requests and tasks so they don't clash. The report then sees races between requests on different workers (two
bookings of one room through two processes); replay gives each worker its own copy of the app, since workers
don't share memory.
"""

import glob
import json
import os
import time
from collections.abc import Iterable

from . import events as ev
from .fileformat import read, write

POLL_S = 0.5
OFFSET = 100_000  # request ids of worker k become rid + k * OFFSET in a merged recording


def post(group: str, t0: int, t1: int, origin: str, reason: str) -> str:
    """Ask every worker of ``group`` for its events between ``t0`` and ``t1`` (perf_counter_ns)."""
    os.makedirs(os.path.join(group, "requests"), exist_ok=True)
    gid = f"{time.strftime('%Y%m%d-%H%M%S')}-{origin}-{t1 % 100000}"
    tmp = os.path.join(group, "requests", gid + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"t0": t0, "t1": t1, "origin": origin, "reason": reason}, f)
    os.replace(tmp, os.path.join(group, "requests", gid + ".json"))
    return gid


class Watcher:
    """Runs on a worker's loop: answers the group's requests with this worker's slice of events."""

    def __init__(self, rec, group: str, worker: str):
        self.rec, self.group, self.worker = rec, group, worker
        self.done: set = set()
        self._handle = None

    def start(self, loop):
        self._handle = loop.call_later(POLL_S, self._check, loop)

    def _check(self, loop):
        try:
            self.answer()
        finally:
            if not loop.is_closed():
                self._handle = loop.call_later(POLL_S, self._check, loop)

    def answer(self):
        for path in sorted(glob.glob(os.path.join(self.group, "requests", "*.json"))):
            gid = os.path.basename(path)[:-5]
            if gid in self.done:
                continue
            self.done.add(gid)
            try:
                with open(path, encoding="utf-8") as f:
                    req = json.load(f)
            except (OSError, ValueError):
                continue
            events = self.rec.slice(req["t0"], req["t1"])
            if not any(e[0] == ev.REQUEST or (e[0] == ev.STEP and e[1] and e[1][0] in "qb") for e in events):
                continue  # nothing of this worker in that window
            out = os.path.join(self.group, gid)
            os.makedirs(out, exist_ok=True)
            header = self.rec.header()
            header.update(worker=self.worker, window=[req["t0"], req["t1"]], reason=req.get("reason"))
            write(os.path.join(out, f"{self.worker}.deja"), header, events)


# ---------------------------------------------------------------------- merging
def _rename(path, k):
    """Worker k's task path, unique in the merged recording (the scope letter stays first)."""
    if not path or k == 0:
        return path
    root, dot, rest = path.partition(".")
    if root[0] == "q":
        root = f"q{int(root[1:]) + k * OFFSET}"
    elif root[0] == "b":
        root = f"b{int(root[1:]) + k * OFFSET}"
    else:
        root = f"{root[0]}w{k}_{root[1:]}"
    return root + dot + rest


def _blocks(events):
    """Split a worker's events into (time, events) blocks: each loop pass starts one, with its timestamp."""
    blocks, cur, t = [], [], None
    for e in events:
        if e[0] == ev.BATCH:
            if cur:
                blocks.append([t, cur])
            cur, t = [e], (e[2] if len(e) > 2 else None)
        else:
            cur.append(e)
    if cur:
        blocks.append([t, cur])
    last = None
    for b in blocks:  # events before the first pass take the next pass's time
        if b[0] is None:
            b[0] = last
        last = b[0]
    first = next((b[0] for b in blocks if b[0] is not None), 0)
    for b in blocks:
        if b[0] is None:
            b[0] = first
    return blocks


def _renamed(e, k):
    code = e[0]
    if code in (ev.STEP, ev.READ, ev.IO_START, ev.IO_END, ev.FLAG, ev.TOUCH):
        return (code, _rename(e[1], k)) + tuple(e[2:])
    if code == ev.TASK_NEW:
        return (code, _rename(e[1], k), _rename(e[2], k)) + tuple(e[3:])
    if code == ev.REQUEST:
        return (code, e[1] + k * OFFSET, _rename(e[2], k)) + tuple(e[3:])
    if code in (ev.RESPONSE, ev.EXCEPTION):
        rid = e[1] + k * OFFSET if isinstance(e[1], int) else _rename(e[1], k)
        return (code, rid) + tuple(e[2:])
    if code == ev.NOTIFY:
        return (code, e[1] + k * OFFSET) + tuple(e[2:])
    return tuple(e)


def merge(paths: Iterable[str], out: str) -> str:
    """Merge the recordings that several workers saved for one race into one recording.

    Each worker's events are interleaved by the time they happened (all processes on one machine share a clock),
    and each worker's request and task ids are renamed so they don't clash: worker ``k``'s request ``r`` becomes
    ``r + k * 100000``.

    Args:
        paths: The ``.deja`` files in one ``<group>/<id>/`` folder, one per worker.
        out: Path of the merged ``.deja`` file to write.

    Returns:
        ``out``.
    """
    parts = []
    for k, path in enumerate(sorted(paths)):
        header, events = read(path)
        parts.append((k, header, [_renamed(e, k) for e in events]))
    blocks = []
    for k, header, events in parts:
        blocks += [(t, k, i, evs) for i, (t, evs) in enumerate(_blocks(events))]
    blocks.sort(key=lambda b: (b[0] or 0, b[1], b[2]))
    merged = [e for _, _, _, evs in blocks for e in evs]
    head = dict(parts[0][1])
    outside, background = {}, {}
    for k, h, _ in parts:
        for key, results in ((h.get("catalog") or {}).get("outside") or {}).items():
            outside.setdefault(key, results)
        for name, coro in ((h.get("catalog") or {}).get("background") or {}).items():
            background[_rename(name, k)] = coro
    head["catalog"] = {**(head.get("catalog") or {}), "outside": outside, "background": background}
    head["workers"] = [{"worker": h.get("worker", f"w{k}"), "offset": k * OFFSET} for k, h, _ in parts]
    write(out, head, merged)
    return out


def worker_of(rid, header) -> int:
    """Index of the worker a merged request id came from."""
    return (rid // OFFSET) if header.get("workers") and isinstance(rid, int) else 0
