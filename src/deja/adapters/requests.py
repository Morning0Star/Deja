"""requests adapter: ``Session.send`` (every ``requests.get/post`` goes through it), for blocking HTTP calls in worker
threads (plain ``def`` endpoints, ``asyncio.to_thread``) or directly in handlers.

Recorded with status, headers and body; replay builds a ``requests.Response`` without the network.
"""

import base64

import requests
from requests.structures import CaseInsensitiveDict

from . import io_call_sync, patch

_undo = []


def _encode(resp):
    return {
        "__t": "requests",
        "status": resp.status_code,
        "reason": resp.reason,
        "headers": [[k, v] for k, v in resp.headers.items()],
        "url": resp.url,
        "encoding": resp.encoding,
        "content": base64.b64encode(resp.content or b"").decode(),
    }


def _decode(v):
    r = requests.Response()
    if not isinstance(v, dict):
        return r
    r.status_code, r.reason, r.url, r.encoding = v["status"], v.get("reason"), v.get("url"), v.get("encoding")
    r.headers = CaseInsensitiveDict(v.get("headers") or [])
    r._content = base64.b64decode(v.get("content") or "")
    return r


def install():
    def make(original):
        def send(self, request, **kwargs):
            body = request.body
            if isinstance(body, (bytes, bytearray)):
                body = bytes(body[:2000]).decode("utf-8", "replace")
            return io_call_sync(
                "http.send",
                (request.method, request.url, body),
                lambda: original(self, request, **kwargs),
                encode=_encode,
                decode=_decode,
            )

        send.__wrapped__ = original
        return send

    _undo.append(patch(requests.Session, "send", make))


def uninstall():
    while _undo:
        _undo.pop()()
