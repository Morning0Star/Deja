"""aiohttp client adapter: wraps ``ClientSession._request`` (every ``session.get/post/request`` goes through it).

Recording reads the whole response body (so it can be stored) and records status, headers and body; the app gets
the real response, its body already read. Replay returns a stand-in response with the same status, headers and
body (``read`` / ``text`` / ``json`` / ``content.iter_chunked`` / ``raise_for_status``...) without connecting.

A streamed body is recorded whole: replay hands it back in one piece, so work the app did between chunks is not
ordered against other requests chunk by chunk (httpx streams are; see :mod:`deja.adapters.httpx`).
"""

import base64
import json as _json

import aiohttp
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from . import io_call, passthrough, patch

_undo = []


def _body_text(kw):
    if kw.get("json") is not None:
        return _json.dumps(kw["json"], sort_keys=True, default=str)
    data = kw.get("data")
    if isinstance(data, (bytes, bytearray)):
        return bytes(data[:2000]).decode("utf-8", "replace")
    if isinstance(data, (str, dict, list, tuple)):
        return str(data)[:2000]
    return None


def _encode(resp):
    return {
        "__t": "aiohttp",
        "status": resp.status,
        "reason": resp.reason,
        "headers": [[k, v] for k, v in resp.headers.items()],
        "url": str(resp.url),
        "method": resp.method,
        "body": base64.b64encode(resp._body or b"").decode(),
    }


class _Content:
    """The body as an ``aiohttp.StreamReader``-like object."""

    def __init__(self, body: bytes):
        self._body, self._pos = body, 0

    async def read(self, n=-1):
        if n is None or n < 0:
            n = len(self._body) - self._pos
        out = self._body[self._pos : self._pos + n]
        self._pos += len(out)
        return out

    async def readany(self):
        return await self.read()

    async def readline(self):
        end = self._body.find(b"\n", self._pos)
        return await self.read((end + 1 - self._pos) if end >= 0 else -1)

    async def iter_chunked(self, n):
        while self._pos < len(self._body):
            yield await self.read(n)

    async def iter_any(self):
        if self._pos < len(self._body):
            yield await self.read()

    def __aiter__(self):  # line by line, as StreamReader does
        return self._lines()

    async def _lines(self):
        while self._pos < len(self._body):
            yield await self.readline()

    def at_eof(self):
        return self._pos >= len(self._body)


class FakeClientResponse:
    """Stands in for ``aiohttp.ClientResponse`` during replay."""

    def __init__(self, v):
        self.status, self.reason, self.method = v["status"], v.get("reason"), v.get("method", "GET")
        self.headers = CIMultiDictProxy(CIMultiDict(v.get("headers") or []))
        self.url = self.real_url = URL(v.get("url", ""))
        self._body = base64.b64decode(v.get("body") or "")
        self.content = _Content(self._body)
        self.history, self.cookies = (), {}

    @property
    def ok(self):
        return self.status < 400

    @property
    def content_type(self):
        return self.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()

    @property
    def charset(self):
        ct = self.headers.get("Content-Type", "")
        return ct.split("charset=")[1].split(";")[0].strip() if "charset=" in ct else None

    @property
    def request_info(self):
        return aiohttp.RequestInfo(self.url, self.method, CIMultiDictProxy(CIMultiDict()), self.url)

    async def read(self):
        return self._body

    async def text(self, encoding=None, errors="strict"):
        return self._body.decode(encoding or self.charset or "utf-8", errors)

    async def json(self, *, encoding=None, loads=_json.loads, content_type="application/json"):
        return loads(self._body.decode(encoding or self.charset or "utf-8")) if self._body else None

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                self.request_info, (), status=self.status, message=self.reason or "", headers=self.headers
            )

    def release(self):
        pass

    def close(self):
        pass

    async def wait_for_close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _decode(v):
    return FakeClientResponse(v)


def install():
    def make(original):
        async def _request(self, method, str_or_url, **kwargs):
            if passthrough():
                return await original(self, method, str_or_url, **kwargs)
            url = (
                str(URL(str(self._base_url or "")).join(URL(str(str_or_url))))
                if getattr(self, "_base_url", None)
                else str(str_or_url)
            )
            if kwargs.get("params"):
                url = str(URL(url).update_query(kwargs["params"]))

            async def real():
                resp = await original(self, method, str_or_url, **kwargs)
                await resp.read()  # recorded whole; the app's later read() returns the same bytes
                return resp

            return await io_call(
                "http.send", (method.upper(), url, _body_text(kwargs)), real, encode=_encode, decode=_decode
            )

        _request.__wrapped__ = original
        return _request

    _undo.append(patch(aiohttp.ClientSession, "_request", make))


def uninstall():
    while _undo:
        _undo.pop()()
