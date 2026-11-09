"""aiobotocore adapter (S3, DynamoDB, SQS... any AWS API): wraps ``AioBaseClient._make_api_call``.

Each API call is one recorded I/O call keyed by service, operation and parameters. A streamed body (``get_object``
``Body``) is read whole while recording and handed to the app as a re-readable body with the same methods
(``read``, ``iter_chunks``, ``iter_lines``, ``async with``); replay returns the recorded response, body and all,
without credentials or network. ``ClientError`` keeps its error response, so ``e.response["Error"]["Code"]`` is the
same in replay.
"""

from aiobotocore.client import AioBaseClient

from .. import codec
from . import io_call, io_live, passthrough, patch

_undo: list = []


class Body:
    """A response body already read: the subset of ``aiobotocore.response.StreamingBody`` apps use."""

    def __init__(self, data: bytes):
        self._data, self._pos = data, 0

    async def read(self, amt=None):
        if amt is None or amt < 0:
            amt = len(self._data) - self._pos
        out = self._data[self._pos : self._pos + amt]
        self._pos += len(out)
        return out

    async def iter_chunks(self, chunk_size=1024):
        while self._pos < len(self._data):
            yield await self.read(chunk_size)

    async def iter_lines(self, chunk_size=1024, keepends=False):
        rest = self._data[self._pos :]
        self._pos = len(self._data)
        for line in rest.splitlines(keepends):
            yield line

    def close(self):
        pass

    async def aclose(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _encode(resp):
    if "__aws_error__" in resp:
        return {"__t": "aws_error", "v": codec.encode(resp["__aws_error__"]), "op": resp["op"]}
    out = {}
    for k, v in resp.items():
        out[k] = {"__t": "body", "v": codec.encode(v._data)} if isinstance(v, Body) else v
    return {"__t": "aws", "v": codec.encode(out)}


def _decode(v):
    if isinstance(v, dict) and v.get("__t") == "aws_error":
        return {"__aws_error__": codec.decode(v["v"]), "op": v["op"]}
    if not isinstance(v, dict) or v.get("__t") != "aws":
        return codec.decode(v)
    resp = codec.decode(v["v"])
    for k, x in list(resp.items()):
        if isinstance(x, dict) and x.get("__t") == "body":
            resp[k] = Body(codec.decode(x["v"]))
    return resp


def install():
    def make(original):
        async def _make_api_call(self, operation_name, api_params):
            if passthrough():
                return await original(self, operation_name, api_params)
            service = self.meta.service_model.service_name

            caught = []

            async def real():
                try:
                    resp = await original(self, operation_name, api_params)
                except Exception as exc:  # ClientError: recorded as data (its class is generated per client)
                    if not isinstance(getattr(exc, "response", None), dict):
                        raise
                    caught.append(exc)
                    return {"__aws_error__": exc.response, "op": operation_name}
                for k, v in list(resp.items()):
                    if hasattr(v, "read") and not isinstance(v, (bytes, Body)):  # a streamed body: keep it
                        data = await v.read()
                        close = getattr(v, "close", None)
                        if callable(close):
                            close()
                        resp[k] = Body(data)
                return resp

            key = ("aws", service, operation_name, codec.encode(api_params))
            resp = await io_call("aws.call", key, real, encode=_encode, decode=_decode, default=dict)
            if isinstance(resp, dict) and "__aws_error__" in resp:
                if caught:
                    raise caught[0]
                error = resp["__aws_error__"]
                raise self.exceptions.from_code(error.get("Error", {}).get("Code", ""))(error, resp["op"])
            return resp

        _make_api_call.__wrapped__ = original
        return _make_api_call

    _undo.append(patch(AioBaseClient, "_make_api_call", make))

    # creating a client (session.create_client) and closing it: built for real in replay too, but their internal
    # steps (model loading, credential caches, the HTTP session) don't count as the request's
    from aiobotocore.session import ClientCreatorContext

    def make_live(op):
        def make(original):
            async def method(self, *args):
                if passthrough():
                    return await original(self, *args)
                return await io_live(op, (op,), lambda: original(self, *args))

            method.__wrapped__ = original
            return method

        return make

    _undo.append(patch(ClientCreatorContext, "__aenter__", make_live("aws.create")))
    _undo.append(patch(ClientCreatorContext, "__aexit__", make_live("aws.close")))


def uninstall():
    while _undo:
        _undo.pop()()
