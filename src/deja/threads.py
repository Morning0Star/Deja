"""Worker threads: ``anyio.to_thread.run_sync``, which runs FastAPI / Starlette's plain ``def`` endpoints and
dependencies, sync generators and sync file responses.

Unlike ``loop.run_in_executor`` (whose result is recorded and handed back without running the function, see
:mod:`deja.loop`), a worker-thread call runs app code, so replay runs it again. What makes that deterministic:

* **Identity.** Each call made inside request scope gets a path under the calling task (``q3.t0``, ``q3.t1``...).
  Code in the thread reports to the recorder under that path: tracked state (:mod:`deja.track`, ``deja.touch``) and
  non-deterministic reads (clocks, random, uuid). The report puts them under the request that made the call.
* **Order.** While recording, each access to tracked state from a thread lands in the event log between the event
  loop's steps, where it happened. Replay turns those positions into schedule entries: the thread waits at each
  access until the schedule reaches it, and the loop waits for the thread to get there (and, once it has gone on, to
  park at its next access or finish). So accesses from threads and from request tasks interleave as recorded, and
  two threads racing on the same state race the same way again.

The granularity is one access to tracked state: work in between (computation, other memory) runs freely, and an
access that happened in the middle of one loop step replays just after it. A thread that calls back into the event
loop (``anyio.from_thread.run`` / ``run_sync``) is *parked* while it waits, so the loop keeps going instead of
waiting for the thread.
"""

import asyncio
import contextvars

from . import adapters

_thread: contextvars.ContextVar = contextvars.ContextVar("deja_thread", default=None)  # (recorder, path)
_original = None
_from_thread: dict = {}  # anyio.from_thread functions replaced while installed


def current():
    """``(recorder, path)`` inside a worker-thread call made from request scope, else None."""
    return _thread.get()


def is_thread_path(path) -> bool:
    return bool(path) and path.rpartition(".")[2][:1] == "t"


def _wrap(func):
    async def run_sync_in_worker_thread(cls, func_, args, abandon_on_cancel=False, limiter=None):
        loop = asyncio._get_running_loop()
        rec = getattr(loop, "deja", None) if loop is not None else None
        path = rec.current_path() if rec is not None and not adapters._inside.get() else None
        if not path or path[0] not in "qb":
            return await func(cls, func_, args, abandon_on_cancel, limiter)
        tpath = rec.thread_path(path)

        def runner(*a):
            token = _thread.set((rec, tpath))
            try:
                return func_(*a)
            finally:
                rec.thread_end(tpath)
                _thread.reset(token)

        runner.__qualname__ = getattr(func_, "__qualname__", "runner")
        return await func(cls, runner, args, abandon_on_cancel, limiter)

    return run_sync_in_worker_thread


def _parking(fn):
    """``anyio.from_thread.run``: while the thread waits for the event loop, replay must not wait for the thread."""

    def call(*args, **kwargs):
        t = _thread.get()
        if t is None or not getattr(t[0], "replaying", False):
            return fn(*args, **kwargs)
        t[0].thread_park(t[1], True)
        try:
            return fn(*args, **kwargs)
        finally:
            t[0].thread_park(t[1], False)

    call.__wrapped__ = fn
    return call


def install():
    global _original
    try:
        import anyio.from_thread
        from anyio._backends._asyncio import AsyncIOBackend
    except ImportError:  # anyio not installed
        return
    if _original is None:
        _original = AsyncIOBackend.__dict__["run_sync_in_worker_thread"]
        AsyncIOBackend.run_sync_in_worker_thread = classmethod(_wrap(_original.__func__))
        for name in ("run", "run_sync"):
            _from_thread[name] = getattr(anyio.from_thread, name)
            setattr(anyio.from_thread, name, _parking(_from_thread[name]))


def uninstall():
    global _original
    if _original is not None:
        import anyio.from_thread
        from anyio._backends._asyncio import AsyncIOBackend

        AsyncIOBackend.run_sync_in_worker_thread = _original
        _original = None
        for name, fn in _from_thread.items():
            setattr(anyio.from_thread, name, fn)
        _from_thread.clear()
