"""ASGI middleware: request tracking and the triggers that turn the rolling buffer into a ``.deja`` file.

    app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")   # FastAPI / Starlette
    uvicorn app:app --loop deja.loop:recording_loop

For each HTTP request it records method, path, headers (sensitive ones redacted) and the full body (needed to
re-send the request during replay), then the response status. A recording is written when the response status is
5xx, an exception escapes the app, or app code calls ``deja.flag()`` during the request. Without a recording loop
the middleware is a pass-through.

WebSocket connections are recorded too. A connection is request scope from connect to close; every message the
app receives and every message it sends is one recorded I/O call, so replay feeds the same messages back at the
same points (no client needed) and checks that the app sends the same ones. A connection's recording is saved when
it ends with close code 1011 (server error) or an exception, or when app code calls ``deja.flag()`` while handling
a message (saved as soon as the app waits for the next one, so a long-lived connection doesn't have to end first).

On the first request it also tracks the app's shared in-memory state for the report (``track=False`` turns that
off; see :mod:`deja.autotrack`).
"""

import asyncio
import contextvars
import logging
import os
import time
import warnings
from collections.abc import Callable, Iterable
from typing import Any

log = logging.getLogger("deja")

current_request: contextvars.ContextVar = contextvars.ContextVar("deja_request", default=None)

MAX_BODY = 1 << 20  # 1 MiB recorded per request; larger bodies are truncated in the recording (marked)
WS_RECEIVE, WS_SEND = "ws.receive", "ws.send"
WS_ERROR = 1011


def ws_transcript(sent: list) -> bytes:
    """What a WebSocket connection sent, as the recorded "response body": one line per message."""
    lines = []
    for m in sent:
        if m.get("type") == "websocket.send":
            lines.append(m["text"] if m.get("text") is not None else "<bytes " + (m.get("bytes") or b"").hex() + ">")
        elif m.get("type") == "websocket.close":
            lines.append(f"<close {m.get('code', 1000)}>")
    return "\n".join(lines).encode()[:65536]


def ws_send_key(message):
    return (WS_SEND, message.get("type"), message.get("text"), message.get("bytes"), message.get("code"))


class _State:
    __slots__ = ("rid", "flagged")

    def __init__(self, rid):
        self.rid = rid
        self.flagged = None


def flag(reason: str = "") -> None:
    """Save a recording of the current request, although it didn't fail.

    Call it where the app notices something wrong that isn't an error: a negative balance, a seat sold twice. The
    recording is written when the request ends; on a WebSocket connection, as soon as the app waits for the next
    message. Inside a [`deja.scope`][deja.scope.scope] block it saves that job. Outside a recording loop it does nothing.

    Args:
        reason: A few words on what went wrong. The recording's header stores it as ``"flag: <reason>"`` and the
            report shows it.

    Example:
        ```python
        if account.balance < 0:
            deja.flag("balance went negative")
        ```
    """
    loop = asyncio._get_running_loop()
    rec = getattr(loop, "deja", None) if loop is not None else None
    if rec is None:
        return
    rec.flag(reason)
    state = current_request.get()
    if state is not None:
        state.flagged = reason or "flagged"


class DejaMiddleware:
    """ASGI middleware that records every request and saves a recording when one goes wrong.

    Works with any ASGI app: FastAPI, Starlette, Litestar, Quart, Django (ASGI). Serve the app on the recording
    loop (``uvicorn app:app --loop deja.loop:recording_loop``); on any other loop the middleware passes requests
    through and warns once.

    Each HTTP request is recorded with its method, path, headers (sensitive ones redacted, see
    [`deja.Recorder`][deja.Recorder]) and body, and each WebSocket connection with every message in and out. A recording of
    the request's causal window (it, and the requests that overlapped it) is written when the response is a 5xx,
    an exception escapes the app, a WebSocket connection closes with code 1011, or app code calls
    [`deja.flag`][deja.flag].

    Args:
        app: The ASGI app to wrap. ``app.add_middleware(DejaMiddleware, ...)`` passes it for you.
        directory: Folder for the ``.deja`` files, created when the first one is written.
        on_dump: Called as ``on_dump(path, reason)`` after each recording is written; use it to log or alert.
        track: Find the shared in-memory state the routes' code can reach and track it, so the report sees races
            on it and replay can order worker threads (see the Shared state guide).
        track_skip: ``fnmatch`` patterns of state not to track, matched against its name (``store.cache``) or
            its class (``myapp.models.Account``).
        track_only: If given, track only state matching these patterns.
        background: Record the background tasks the app starts during startup (lifespan) and order them like
            requests.
        group: A folder shared by several worker processes of the same app, so a race across workers ends up in
            one recording (see the Several workers guide). Defaults to the ``DEJA_GROUP``
            environment variable.
        worker: This process's name within ``group``. Defaults to ``pid<process id>``.

    Example:
        ```python
        app = FastAPI()
        app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")
        ```
    """

    def __init__(
        self,
        app: Callable[..., Any],
        directory: str = "deja-recordings",
        on_dump: Callable[[str, str], object] | None = None,
        track: bool = True,
        track_skip: Iterable[str] = (),
        track_only: Iterable[str] | None = None,
        background: bool = True,
        group: str | None = None,
        worker: str | None = None,
    ):
        self.app = app
        self.directory = directory
        self.on_dump = on_dump  # optional callback(path, reason) e.g. for logging or tests
        self.track = track  # track shared in-memory state automatically (see deja.autotrack)
        self.background = background  # order the app's background tasks (started at startup) like requests
        self.group = group or os.environ.get("DEJA_GROUP")  # shared by worker processes (see deja.group)
        self.worker = worker or f"pid{os.getpid()}"
        self.track_rules = {
            "skip": list(track_skip or ()),
            "only": list(track_only) if track_only is not None else None,
        }
        self.tracked: list = []
        self._warned = False
        self._scanned = False

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan" and self.background:  # tasks the app starts now are background tasks
            from .recorder import background

            token = background.set(True)
            try:
                return await self.app(scope, receive, send)
            finally:
                background.reset(token)
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)
        loop = asyncio.get_running_loop()
        rec = getattr(loop, "deja", None)
        if rec is None:
            if not self._warned:
                self._warned = True
                warnings.warn("deja: not running on a recording loop; requests are not recorded", RuntimeWarning)
            return await self.app(scope, receive, send)
        if not self._scanned:  # once, on the first request: startup has built the app's state
            self._scanned = True
            from .autotrack import Rules, prepare

            rec.catalog["track"] = {**self.track_rules, "on": self.track}  # replay applies the same rules
            self.tracked, rec.catalog["source"] = prepare(
                scope.get("app") or self.app, self.track, Rules(**self.track_rules)
            )
            if self.group:
                from .group import Watcher

                rec.group = (self.group, self.worker)
                Watcher(rec, self.group, self.worker).start(asyncio.get_running_loop())
        if scope["type"] == "websocket":
            return await self._websocket(rec, scope, receive, send)

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
        rid = rec.request_start(
            scope.get("method", ""), target, headers, body if size <= MAX_BODY else body + b"...<truncated>"
        )
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

    async def _websocket(self, rec, scope, receive, send):
        from .adapters import io_call

        headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in scope.get("headers", [])]
        target = (scope.get("raw_path") or scope["path"].encode()).decode("latin-1")
        if scope.get("query_string"):
            target += "?" + scope["query_string"].decode("latin-1")
        rid = rec.request_start("WS", target, headers, b"")
        state = _State(rid)
        token = current_request.set(state)
        sent, code = [], None

        async def ws_receive():
            nonlocal code
            if state.flagged:  # the message that flagged is handled: save now, the connection may stay open
                self._dump(rec, rid, f"flag: {state.flagged}")
                state.flagged = None
            message = await io_call(WS_RECEIVE, (WS_RECEIVE,), receive)
            if message.get("type") == "websocket.disconnect" and code is None:
                code = message.get("code", 1000)
            return message

        async def ws_send(message):
            nonlocal code
            if message.get("type") in ("websocket.send", "websocket.close"):
                sent.append(message)
            if message.get("type") == "websocket.close" and code is None:
                code = message.get("code", 1000)
            await io_call(WS_SEND, ws_send_key(message), lambda: send(message))

        reason = None
        try:
            await self.app(scope, ws_receive, ws_send)
        except BaseException as exc:
            rec.request_exception(rid, exc)
            reason = f"exception {type(exc).__name__}"
            code = WS_ERROR
            raise
        finally:
            current_request.reset(token)
            rec.request_end(rid, code if code is not None else 1000, ws_transcript(sent))
            if reason is None:
                if code == WS_ERROR:
                    reason = f"close code {code}"
                elif state.flagged:
                    reason = f"flag: {state.flagged}"
            if reason is not None:
                self._dump(rec, rid, reason)
            rec.forget_request(rid)

    def _dump(self, rec, rid, reason):
        save(rec, rid, reason, self.directory, self.on_dump)


def save(rec, rid, reason, directory, on_dump=None):
    """Write the causal window around request ``rid`` to ``directory`` (the file write happens off the loop)."""
    if getattr(rec, "replaying", False):  # replaying a recording must not write new ones
        return
    os.makedirs(directory, exist_ok=True)
    stem = os.path.join(directory, f"{time.strftime('%Y%m%d-%H%M%S')}-r{rid}")
    path, n = stem + ".deja", 1
    while os.path.exists(path) or path in rec.dumps:  # a connection flagged twice in one second
        path, n = f"{stem}-{n}.deja", n + 1
    header = rec.header()
    header["reason"] = reason
    header["request"] = rid
    events = rec.window(rid)  # snapshot on the loop thread; the file write happens off it
    group = getattr(rec, "group", None)
    if group:  # other workers of the group save the same time window (deja.group)
        from .group import post

        t0 = rec.first_time(events)
        if t0 is not None:
            header["group_request"] = post(group[0], t0, time.perf_counter_ns(), group[1], reason)

    def write():
        from .fileformat import write as write_file

        write_file(path, header, events)
        rec.dumps.append(path)
        log.warning("deja: saved %s (%s)", path, reason)
        if on_dump:
            on_dump(path, reason)

    try:
        asyncio.get_running_loop().run_in_executor(None, write)
    except RuntimeError:
        write()
