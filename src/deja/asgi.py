"""ASGI middleware: request tracking and the triggers that turn the rolling buffer into a ``.deja`` file.

    app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")   # FastAPI / Starlette
    uvicorn app:app --loop deja.loop:recording_loop

For each HTTP request it records method, path, headers (sensitive ones redacted) and the full body (needed to
re-send the request during replay), then the response status. A recording is written when the response status is
5xx, an exception escapes the app, or app code calls ``deja.flag()`` during the request. Without a recording loop
the middleware is a pass-through.

On the first request it also tracks the app's shared in-memory state for the report (``track=False`` turns that
off; see :mod:`deja.autotrack`).
"""

import asyncio
import contextvars
import logging
import os
import time
import warnings

log = logging.getLogger("deja")

current_request: contextvars.ContextVar = contextvars.ContextVar("deja_request", default=None)

MAX_BODY = 1 << 20  # 1 MiB recorded per request; larger bodies are truncated in the recording (marked)


class _State:
    __slots__ = ("rid", "flagged")

    def __init__(self, rid):
        self.rid = rid
        self.flagged = None


def flag(reason: str = "") -> None:
    """Mark the current request (or task) as bad so its causal window gets saved."""
    loop = asyncio._get_running_loop()
    rec = getattr(loop, "deja", None) if loop is not None else None
    if rec is None:
        return
    rec.flag(reason)
    state = current_request.get()
    if state is not None:
        state.flagged = reason or "flagged"


class DejaMiddleware:
    def __init__(self, app, directory: str = "deja-recordings", on_dump=None, track: bool = True):
        self.app = app
        self.directory = directory
        self.on_dump = on_dump  # optional callback(path, reason) e.g. for logging or tests
        self.track = track  # track shared in-memory state automatically (see deja.autotrack)
        self.tracked: list = []
        self._warned = False
        self._scanned = False

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        loop = asyncio.get_running_loop()
        rec = getattr(loop, "deja", None)
        if rec is None:
            if not self._warned:
                self._warned = True
                warnings.warn("deja: not running on a recording loop; requests are not recorded", RuntimeWarning)
            return await self.app(scope, receive, send)
        if self.track and not self._scanned:  # once, on the first request: startup has built the app's state
            self._scanned = True
            from .autotrack import auto_track

            self.tracked = auto_track(scope.get("app"))

        # Read the whole body first: replay needs it, and it keeps the recorded request self-contained.
        chunks, size, more = [], 0, True
        while more:
            message = await receive()
            if message["type"] != "http.request":
                break
            body = message.get("body", b"")
            size += len(body)
            if size <= MAX_BODY:
                chunks.append(body)
            more = message.get("more_body", False)
        body = b"".join(chunks)
        sent = False

        async def replay_receive():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in scope.get("headers", [])]
        target = (scope.get("raw_path") or scope["path"].encode()).decode("latin-1")
        if scope.get("query_string"):
            target += "?" + scope["query_string"].decode("latin-1")
        rid = rec.request_start(scope.get("method", ""), target, headers,
                                body if size <= MAX_BODY else body + b"...<truncated>")
        state = _State(rid)
        token = current_request.set(state)
        status = None
        out = []

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body" and sum(map(len, out)) < 65536:
                out.append(message.get("body", b""))
            await send(message)

        reason = None
        try:
            await self.app(scope, replay_receive, send_wrapper)
        except BaseException as exc:
            rec.request_exception(rid, exc)
            reason = f"exception {type(exc).__name__}"
            raise
        finally:
            current_request.reset(token)
            rec.request_end(rid, status if status is not None else 500, b"".join(out))
            if reason is None:
                if status is not None and status >= 500:
                    reason = f"status {status}"
                elif state.flagged:
                    reason = f"flag: {state.flagged}"
            if reason is not None:
                self._dump(rec, rid, reason)
            rec.forget_request(rid)

    def _dump(self, rec, rid, reason):
        os.makedirs(self.directory, exist_ok=True)
        path = os.path.join(self.directory, f"{time.strftime('%Y%m%d-%H%M%S')}-r{rid}.deja")
        header = rec.header()
        header["reason"] = reason
        header["request"] = rid
        events = rec.window(rid)  # snapshot on the loop thread; the file write happens off it

        def write():
            from .fileformat import write as write_file

            write_file(path, header, events)
            rec.dumps.append(path)
            log.warning("deja: saved %s (%s)", path, reason)
            if self.on_dump:
                self.on_dump(path, reason)

        try:
            asyncio.get_running_loop().run_in_executor(None, write)
        except RuntimeError:
            write()
