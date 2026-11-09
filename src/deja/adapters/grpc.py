"""grpc.aio client adapter: unary-unary and unary-stream calls on ``grpc.aio`` channels.

Each call is one recorded I/O call keyed by the method and the serialized request; the response is stored
serialized and turned back into a message with the stub's own deserializer during replay, so no server is needed.
A server-streaming call is recorded as the list of its messages. Errors keep their status code and details, so
``except grpc.aio.AioRpcError as e: e.code()`` behaves the same in replay. Client-streaming calls run live.
"""

import base64

import grpc
from grpc.aio import _channel

from .. import codec
from . import io_call, passthrough, patch

_undo = []


def _serialize(msg, serializer=None):
    if isinstance(msg, (bytes, bytearray)):
        return bytes(msg)
    if hasattr(msg, "SerializeToString"):
        return msg.SerializeToString()
    if serializer is not None:
        return serializer(msg)
    return None


def _encode(msg):
    raw = _serialize(msg)
    return {"__t": "grpc", "b64": base64.b64encode(raw).decode()} if raw is not None else codec.encode(msg)


def _decoder(deserializer):
    def decode(v):
        if isinstance(v, dict) and v.get("__t") == "grpc":
            raw = base64.b64decode(v["b64"])
            return deserializer(raw) if deserializer is not None else raw
        return codec.decode(v)

    return decode


def _method(mc):
    m = mc._method
    return m.decode() if isinstance(m, bytes) else str(m)


class _Call:
    """What a wrapped unary-unary stub call returns: awaitable, like ``grpc.aio.UnaryUnaryCall``."""

    def __init__(self, coro):
        self._coro = coro

    def __await__(self):
        return self._coro.__await__()


class _StreamCall:
    """A recorded server stream: ``async for`` it, or ``await call.read()`` message by message."""

    def __init__(self, coro):
        self._coro, self._items = coro, None

    async def _load(self):
        if self._items is None:
            self._items = list(await self._coro)

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        await self._load()
        while self._items:
            yield self._items.pop(0)

    async def read(self):
        await self._load()
        return self._items.pop(0) if self._items else grpc.aio.EOF


def install():
    def make_unary(original):
        def __call__(self, request, **kwargs):
            if passthrough():
                return original(self, request, **kwargs)
            key = ("grpc", _method(self), _serialize(request, self._request_serializer))

            async def real():
                return await original(self, request, **kwargs)

            return _Call(io_call("grpc.call", key, real, encode=_encode, decode=_decoder(self._response_deserializer)))

        __call__.__wrapped__ = original
        return __call__

    def make_stream(original):
        def __call__(self, request, **kwargs):
            if passthrough():
                return original(self, request, **kwargs)
            key = ("grpc", _method(self), _serialize(request, self._request_serializer))
            decode_one = _decoder(self._response_deserializer)

            async def real():
                return [m async for m in original(self, request, **kwargs)]

            return _StreamCall(
                io_call(
                    "grpc.stream",
                    key,
                    real,
                    encode=lambda ms: [_encode(m) for m in ms],
                    decode=lambda vs: [decode_one(v) for v in vs or []],
                    default=list,
                )
            )

        __call__.__wrapped__ = original
        return __call__

    _undo.append(patch(_channel.UnaryUnaryMultiCallable, "__call__", make_unary))
    _undo.append(patch(_channel.UnaryStreamMultiCallable, "__call__", make_stream))


def uninstall():
    while _undo:
        _undo.pop()()
