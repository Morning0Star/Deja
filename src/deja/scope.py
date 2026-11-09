"""Request scope for work that isn't a web request: queue consumers, scheduled jobs, Kafka / Redis handlers.

    async for msg in consumer:
        async with deja.scope("orders", msg.value) as job:   # one message = one "request"
            job.result = await handle_order(msg.value)

Each block is recorded like a request (its payload, its result, the order of its steps against everything else in
request scope) and saved on an exception or ``deja.flag()``. To replay, give a mapping from scope name to handler:

    replay("recording.deja", lambda: {"orders": handle_order})
    # deja replay recording.deja --app myworker:handlers     (a module attribute or factory returning the mapping)

The replay calls each handler with the recorded payload, in the recorded interleaving, and compares the results.
"""

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Callable
from typing import Any

from . import codec
from .asgi import _State, current_request, save

KIND = "JOB"  # the "method" of a scope in the recording


class Job:
    """One [`deja.scope`][deja.scope.scope] block: its ``name``, its ``payload``, and the ``result`` the block sets."""

    __slots__ = ("name", "payload", "rid", "result")

    def __init__(self, name, payload, rid):
        self.name, self.payload, self.rid, self.result = name, payload, rid, None


def _body(payload) -> bytes:
    return json.dumps(codec.encode(payload), default=str).encode()


def payload_of(body: bytes):
    try:
        return codec.decode(json.loads(body or b"null"))
    except (ValueError, TypeError):
        return body


@contextlib.asynccontextmanager
async def scope(
    name: str,
    payload: Any = None,
    directory: str = "deja-recordings",
    on_dump: Callable[[str, str], object] | None = None,
) -> AsyncIterator["Job"]:
    """Record a block of work that isn't a web request as one request.

    Use it in queue consumers, scheduled jobs and message handlers. The block is recorded like a request: its
    payload, its result and the order of its steps against everything else being recorded. A recording is saved
    when an exception leaves the block or code inside it calls [`deja.flag`][deja.flag]; the exception is re-raised.
    Outside a recording loop the block just runs.

    To replay, pass ``--app`` a mapping (or a factory returning one) from scope name to an ``async`` handler. Each
    recorded job is replayed as ``await handler(payload)`` and its return value compared with ``job.result``.

    Args:
        name: The kind of job (``"orders"``). Replay looks up the handler by this name.
        payload: The job's input. It is stored in the recording and passed to the handler in replay, so it must
            be JSON-serialisable (``bytes``, tuples and sets are fine).
        directory: Folder for the ``.deja`` files.
        on_dump: Called as ``on_dump(path, reason)`` after a recording is written.

    Yields:
        The job, with ``name``, ``payload`` and ``result``. Set ``job.result`` to what the job produced, so replay
            can compare it.

    Example:
        ```python
        async for msg in consumer:
            async with deja.scope("orders", msg.value) as job:
                job.result = await handle_order(msg.value)

        handlers = {"orders": handle_order}   # deja replay rec.deja --app myworker:handlers
        ```
    """
    rec = getattr(asyncio.get_running_loop(), "deja", None)
    if rec is None:
        yield Job(name, payload, None)
        return
    if not getattr(rec, "_jobs_prepared", False):  # as the middleware's first request: track what the job reaches
        rec._jobs_prepared = True
        _prepare(rec)
    rid = rec.request_start(KIND, name, [], _body(payload))
    job = Job(name, payload, rid)
    state = _State(rid)
    token = current_request.set(state)
    reason, status = None, 200
    try:
        yield job
    except BaseException as exc:
        status = 500
        rec.request_exception(rid, exc)
        reason = f"exception {type(exc).__name__}"
        raise
    finally:
        current_request.reset(token)
        rec.request_end(rid, status, _body(job.result))
        if reason is None and state.flagged:
            reason = f"flag: {state.flagged}"
        if reason is not None:
            save(rec, rid, reason, directory, on_dump)
        rec.forget_request(rid)


def _prepare(rec):
    """Follow the code of the function that opened the first scope (and what it calls), like the middleware."""
    import gc
    import os
    import sys
    import types

    from .autotrack import prepare

    here = os.path.dirname(os.path.abspath(__file__))
    frame = sys._getframe(1)
    while frame is not None and (frame.f_code.co_filename.startswith(here) or "contextlib" in frame.f_code.co_filename):
        frame = frame.f_back
    if frame is None:
        return
    fns = [f for f in gc.get_referrers(frame.f_code) if isinstance(f, types.FunctionType)]
    if not fns:
        return
    app = type("Jobs", (), {"routes": [type("Route", (), {"endpoint": fns[0]})()]})()
    _, rec.catalog["source"] = prepare(app)


class JobHarness:
    """Replay harness for scopes: each recorded job is a task calling its handler with the recorded payload."""

    def __init__(self, loop, handlers: dict):
        self.loop, self.handlers = loop, handlers
        self.tasks: dict[int, asyncio.Task] = {}
        self.results: dict[int, tuple] = {}

    def started(self, rid) -> bool:
        return rid in self.tasks

    def start(self, rid):
        task = self.loop.create_task(self._run(rid))
        self.loop.deja.ids.rebind(task, f"q{rid}")
        self.tasks[rid] = task

    async def _run(self, rid):
        from .replay import _replay_rid

        _replay_rid.set(rid)
        _, _, _, _, name, _, body = self.loop.schedule.requests[rid][:7]
        handler = self.handlers.get(name)
        if handler is None:
            self.results[rid] = (500, f"no handler for {name!r}".encode())
            return
        try:
            async with scope(name, payload_of(body)) as job:
                job.result = await handler(job.payload)
        except Exception:
            pass
        self.results[rid] = self.loop.deja.ended.get(rid, (500, b""))

    async def startup(self):
        pass

    async def shutdown(self):
        pass
