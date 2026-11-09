"""aiohttp web server support (aiohttp apps aren't ASGI).

    from deja.aiohttp_web import deja_middleware
    app = web.Application(middlewares=[deja_middleware(directory="deja-recordings")])
    # run it on the recording loop: web.run_app(app, loop=deja.recording_loop())

The middleware records each request (method, path, headers, body) and the response, marks the handler's task as
request scope, and saves the causal window on a 5xx, an exception or ``deja.flag()``, like
:class:`deja.DejaMiddleware`. ``deja replay`` / :func:`deja.replay.replay` accept an aiohttp ``Application`` (or a
factory returning one): the harness serves it on a local port and re-sends the recorded requests; a header tells
the recorder which recorded request each one is.
"""

import asyncio
import contextvars

from aiohttp import web

from .asgi import _State, current_request, save

RID_HEADER = "X-Deja-Replay-Request"  # set by the replay harness: which recorded request this is
MAX_BODY = 1 << 20


def deja_middleware(
    directory: str = "deja-recordings", on_dump=None, track: bool = True, track_skip=(), track_only=None
):
    rules = {"skip": list(track_skip or ()), "only": list(track_only) if track_only is not None else None}
    scanned = []

    @web.middleware
    async def middleware(request, handler):
        rec = getattr(asyncio.get_running_loop(), "deja", None)
        if rec is None:
            return await handler(request)
        if not scanned:  # once, on the first request: startup has built the app's state
            scanned.append(True)
            from .autotrack import Rules, prepare

            rec.catalog["track"] = {**rules, "on": track}
            _, rec.catalog["source"] = prepare(request.app, track, Rules(**rules))
        body = await request.read()
        headers = [(k, v) for k, v in request.headers.items() if k != RID_HEADER]
        replay_rid = request.headers.get(RID_HEADER)
        if replay_rid is not None and getattr(rec, "replaying", False):
            from .replay import _replay_rid

            _replay_rid.set(int(replay_rid))
        rid = rec.request_start(
            request.method,
            request.path_qs,
            headers,
            body if len(body) <= MAX_BODY else body[:MAX_BODY] + b"...<truncated>",
        )
        state = _State(rid)
        token = current_request.set(state)
        status, out, reason = 500, b"", None
        try:
            response = await handler(request)
            status = response.status
            out = response.body if isinstance(getattr(response, "body", None), (bytes, bytearray)) else b""
            return response
        except web.HTTPException as exc:  # raised responses (404, redirects) are answers, not crashes
            status, out = exc.status, (exc.text or "").encode()
            raise
        except BaseException as exc:
            rec.request_exception(rid, exc)
            reason = f"exception {type(exc).__name__}"
            raise
        finally:
            current_request.reset(token)
            rec.request_end(rid, status, bytes(out))
            if reason is None:
                if status >= 500:
                    reason = f"status {status}"
                elif state.flagged:
                    reason = f"flag: {state.flagged}"
            if reason is not None:
                save(rec, rid, reason, directory, on_dump)
            rec.forget_request(rid)

    return middleware


class AiohttpHarness:
    """Replay harness for an aiohttp ``Application``: a local server, and one client call per recorded request."""

    starts_in_env = True  # the server creates each request's task; the middleware puts it in request scope

    def __init__(self, loop, app):
        self.loop, self.app = loop, app
        self.tasks: dict[int, asyncio.Task] = {}
        self.results: dict[int, tuple] = {}
        self._runner = self._session = None
        self._port = None

    def started(self, rid) -> bool:
        return rid in self.tasks

    def start(self, rid):  # the client is environment; the server's handler task becomes q{rid} in the middleware
        self.tasks[rid] = self.loop.create_task(self._send(rid), context=contextvars.Context())

    async def _send(self, rid):
        from .adapters import _inside

        _inside.set(True)  # the harness's own client: never answered from the recording
        _, _, _, method, target, headers, body = self.loop.schedule.requests[rid][:7]
        hdrs = [(k, v) for k, v in headers if k.lower() not in ("host", "content-length", "transfer-encoding")]
        hdrs.append((RID_HEADER, str(rid)))
        try:
            async with self._session.request(
                method, f"http://127.0.0.1:{self._port}{target}", headers=hdrs, data=body or None
            ) as resp:
                self.results[rid] = (resp.status, await resp.read())
        except Exception as exc:
            self.results[rid] = (500, f"{type(exc).__name__}: {exc}".encode())

    async def startup(self):
        import aiohttp

        self._runner = web.AppRunner(self.app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self._port = site._server.sockets[0].getsockname()[1]
        self._session = aiohttp.ClientSession(auto_decompress=False)

    async def shutdown(self):
        if self._session is not None:
            await self._session.close()
        if self._runner is not None:
            await self._runner.cleanup()
