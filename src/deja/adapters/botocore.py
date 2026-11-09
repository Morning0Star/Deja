"""botocore / boto3 (sync) adapter: ``BaseClient._make_api_call`` for blocking AWS calls in worker threads or
handlers. Same recording as :mod:`deja.adapters.aiobotocore`: parameters in the key, response and streamed body
(read whole) recorded, ``ClientError`` raised again as the client's own class in replay.
"""

import botocore.client

from .. import codec
from . import io_call_sync, patch

_undo: list = []


class Body:
    """A response body already read (``StreamingBody``'s ``read`` / ``iter_chunks`` / ``iter_lines`` / ``close``)."""

    def __init__(self, data: bytes):
        self._data, self._pos = data, 0

    def read(self, amt=None):
        if amt is None or amt < 0:
            amt = len(self._data) - self._pos
        out = self._data[self._pos : self._pos + amt]
        self._pos += len(out)
        return out

    def iter_chunks(self, chunk_size=1024):
        while self._pos < len(self._data):
            yield self.read(chunk_size)

    def iter_lines(self, chunk_size=1024, keepends=False):
        rest = self._data[self._pos :]
        self._pos = len(self._data)
        yield from rest.splitlines(keepends)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _encode(resp):
    if "__aws_error__" in resp:
        return {"__t": "aws_error", "v": codec.encode(resp["__aws_error__"]), "op": resp["op"]}
    out = {k: ({"__t": "body", "v": codec.encode(v._data)} if isinstance(v, Body) else v) for k, v in resp.items()}
    return {"__t": "aws", "v": codec.encode(out)}


def _decode(v):
    if isinstance(v, dict) and v.get("__t") == "aws_error":
        return {"__aws_error__": codec.decode(v["v"]), "op": v["op"]}
    if not isinstance(v, dict) or v.get("__t") != "aws":
        return codec.decode(v) or {}
    resp = codec.decode(v["v"])
    for k, x in list(resp.items()):
        if isinstance(x, dict) and x.get("__t") == "body":
            resp[k] = Body(codec.decode(x["v"]))
    return resp


def install():
    def make(original):
        def _make_api_call(self, operation_name, api_params):
            caught = []

            def real():
                try:
                    resp = original(self, operation_name, api_params)
                except Exception as exc:
                    if not isinstance(getattr(exc, "response", None), dict):
                        raise
                    caught.append(exc)
                    return {"__aws_error__": exc.response, "op": operation_name}
                for k, v in list(resp.items()):
                    if hasattr(v, "read") and not isinstance(v, (bytes, Body)):
                        resp[k] = Body(v.read())
                return resp

            key = ("aws", self.meta.service_model.service_name, operation_name, codec.encode(api_params))
            resp = io_call_sync("aws.call", key, real, encode=_encode, decode=_decode)
            if isinstance(resp, dict) and "__aws_error__" in resp:
                if caught:
                    raise caught[0]
                error = resp["__aws_error__"]
                raise self.exceptions.from_code(error.get("Error", {}).get("Code", ""))(error, resp["op"])
            return resp

        _make_api_call.__wrapped__ = original
        return _make_api_call

    _undo.append(patch(botocore.client.BaseClient, "_make_api_call", make))


def uninstall():
    while _undo:
        _undo.pop()()
