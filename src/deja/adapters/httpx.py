"""httpx adapter: wraps ``AsyncClient.send`` (every request goes through it).

Recording stores status, headers and body of non-streaming responses; replay builds an ``httpx.Response`` from them
without opening a connection. Streaming requests (``stream=True``) run live and are not recorded yet.
"""

import base64

import httpx

from . import io_call, passthrough, patch

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
        v["status"], headers=v["headers"], content=base64.b64decode(v["content"]),
        request=httpx.Request(v["method"], v["url"]),
    )


def install():
    def make(original):
        def send(self, request, *args, stream=False, **kwargs):
            if stream or passthrough():
                return original(self, request, *args, stream=stream, **kwargs)
            try:
                body = request.content
            except Exception:
                body = None
            return io_call(
                "http.send", (request.method, str(request.url), body),
                lambda: original(self, request, *args, stream=stream, **kwargs),
                encode=_encode, decode=_decode,
            )

        send.__wrapped__ = original
        return send

    _undo.append(patch(httpx.AsyncClient, "send", make))


def uninstall():
    while _undo:
        _undo.pop()()
