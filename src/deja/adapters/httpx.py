"""httpx adapter: wraps ``AsyncClient.send`` (every request goes through it).

Recording stores status, headers and body of each response; replay builds an ``httpx.Response`` from them without
opening a connection.

Streaming requests (``client.stream(...)``, ``send(stream=True)``; how LLM SDKs read token streams) are recorded
chunk by chunk: the response head is one I/O call, and every chunk the app reads from the body is another. Replay
hands the same chunks back in the same order, suspending where the recorded read suspended, so an app that does work
between chunks interleaves with other requests exactly as it did.
"""

import base64

import httpx

from . import _context, io_call, io_call_sync, passthrough, patch, replaying

_undo = []


def _encode(resp):
    return {
        "__t": "httpx",
        "status": resp.status_code,
        "headers": [[k, v] for k, v in resp.headers.multi_items()],
        "content": base64.b64encode(resp.content).decode(),
        "method": resp.request.method,
        "url": str(resp.request.url),
    }


def _decode(v):
    return httpx.Response(
        v["status"],
        headers=v["headers"],
        content=base64.b64decode(v["content"]),
        request=httpx.Request(v["method"], v["url"]),
    )


def _encode_head(resp):
    return {
        "__t": "httpx",
        "status": resp.status_code,
        "headers": [[k, v] for k, v in resp.headers.multi_items()],
        "stream": True,
        "method": resp.request.method,
        "url": str(resp.request.url),
    }


def _decode_head(v):
    return httpx.Response(
        v["status"],
        headers=v["headers"],
        stream=_ChunkStream(None, v["url"]),
        request=httpx.Request(v["method"], v["url"]),
    )


class _ChunkStream(httpx.AsyncByteStream):
    """A response body read one recorded chunk at a time. ``inner`` is the live stream (None during replay)."""

    def __init__(self, inner, url):
        self._inner, self._url, self._i, self._it = inner, url, 0, None

    async def _next(self):
        if self._it is None:
            self._it = self._inner.__aiter__()
        try:
            return await self._it.__anext__()
        except StopAsyncIteration:
            return None  # end of body

    async def __aiter__(self):
        while True:
            if replaying() or _context()[1] is not None:
                chunk = await io_call("http.chunk", ("http.chunk", self._url, self._i), self._next)
            elif self._inner is not None:  # recording, outside a request: read live, keep nothing
                chunk = await self._next()
            else:
                chunk = None
            self._i += 1
            if chunk is None:
                return
            yield chunk

    async def aclose(self):
        if self._inner is not None and not replaying():
            await self._inner.aclose()


async def _send_stream(client, original, request, args, kwargs, body):
    resp = await io_call(
        "http.send",
        (request.method, str(request.url), body),
        lambda: original(client, request, *args, stream=True, **kwargs),
        encode=_encode_head,
        decode=_decode_head,
    )
    if resp is not None and not isinstance(resp.stream, _ChunkStream):
        resp.stream = _ChunkStream(resp.stream, str(request.url))
    return resp


def install():
    def make(original):
        def send(self, request, *args, stream=False, **kwargs):
            if passthrough():
                return original(self, request, *args, stream=stream, **kwargs)
            try:
                body = request.content
            except Exception:
                body = None
            if stream:
                return _send_stream(self, original, request, args, kwargs, body)
            return io_call(
                "http.send",
                (request.method, str(request.url), body),
                lambda: original(self, request, *args, stream=stream, **kwargs),
                encode=_encode,
                decode=_decode,
            )

        send.__wrapped__ = original
        return send

    _undo.append(patch(httpx.AsyncClient, "send", make))

    def make_sync(original):  # httpx.Client (sync): in worker threads and blocking calls in handlers
        def send(self, request, *args, stream=False, **kwargs):
            try:
                body = request.content
            except Exception:
                body = None

            def real():
                resp = original(self, request, *args, stream=stream, **kwargs)
                resp.read()  # recorded whole (a sync stream is replayed in one piece)
                return resp

            return io_call_sync(
                "http.send", (request.method, str(request.url), body), real, encode=_encode, decode=_decode
            )

        send.__wrapped__ = original
        return send

    _undo.append(patch(httpx.Client, "send", make_sync))


def uninstall():
    while _undo:
        _undo.pop()()
