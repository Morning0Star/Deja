"""Encode I/O results into JSON-safe values for ``.deja`` files, and decode them back during replay.

Tagged objects look like ``{"__t": "<tag>", ...}``. Anything unknown is stored as its ``repr`` (and replays as that
string), which the recording marks so a replay can warn.
"""

import base64
import datetime as dt
import decimal
import importlib
import uuid


class FakeRecord:
    """Stands in for ``asyncpg.Record`` during replay: index by position or name, iterate values, ``keys()`` etc."""

    __slots__ = ("_keys", "_values", "_index")

    def __init__(self, keys, values):
        self._keys = tuple(keys)
        self._values = tuple(values)
        self._index = {k: i for i, k in enumerate(self._keys)}

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self._values[key]
        return self._values[self._index[key]]

    def get(self, key, default=None):
        i = self._index.get(key)
        return default if i is None else self._values[i]

    def keys(self):
        return iter(self._keys)

    def values(self):
        return iter(self._values)

    def items(self):
        return iter(zip(self._keys, self._values))

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def __contains__(self, key):
        return key in self._index

    def __eq__(self, other):
        if isinstance(other, FakeRecord):
            return self._keys == other._keys and self._values == other._values
        return tuple(self._values) == tuple(other) if isinstance(other, (tuple, list)) else NotImplemented

    def __hash__(self):
        return hash(self._values)

    def __repr__(self):
        return "<Record " + " ".join(f"{k}={v!r}" for k, v in zip(self._keys, self._values)) + ">"


def encode(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"__t": "bytes", "v": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, dt.datetime):
        return {"__t": "datetime", "v": v.isoformat()}
    if isinstance(v, dt.date):
        return {"__t": "date", "v": v.isoformat()}
    if isinstance(v, dt.time):
        return {"__t": "time", "v": v.isoformat()}
    if isinstance(v, dt.timedelta):
        return {"__t": "timedelta", "v": v.total_seconds()}
    if isinstance(v, decimal.Decimal):
        return {"__t": "decimal", "v": str(v)}
    if isinstance(v, uuid.UUID):
        return {"__t": "uuid", "v": str(v)}
    if isinstance(v, (list, tuple)):
        return {"__t": "tuple" if isinstance(v, tuple) else "list", "v": [encode(x) for x in v]}
    if isinstance(v, dict):
        return {"__t": "dict", "v": [[encode(k), encode(x)] for k, x in v.items()]}
    if hasattr(v, "keys") and hasattr(v, "values") and type(v).__name__ == "Record":  # asyncpg.Record
        return {"__t": "record", "k": list(v.keys()), "v": [encode(x) for x in v.values()]}
    if isinstance(v, FakeRecord):
        return {"__t": "record", "k": list(v.keys()), "v": [encode(x) for x in v.values()]}
    return {"__t": "repr", "v": repr(v)}


def decode(v):
    if not isinstance(v, dict) or "__t" not in v:
        return v
    t, x = v["__t"], v.get("v")
    if t == "bytes":
        return base64.b64decode(x)
    if t == "datetime":
        return dt.datetime.fromisoformat(x)
    if t == "date":
        return dt.date.fromisoformat(x)
    if t == "time":
        return dt.time.fromisoformat(x)
    if t == "timedelta":
        return dt.timedelta(seconds=x)
    if t == "decimal":
        return decimal.Decimal(x)
    if t == "uuid":
        return uuid.UUID(x)
    if t == "list":
        return [decode(i) for i in x]
    if t == "tuple":
        return tuple(decode(i) for i in x)
    if t == "dict":
        return {decode(k): decode(i) for k, i in x}
    if t == "record":
        return FakeRecord(v["k"], [decode(i) for i in x])
    if t == "repr":
        return x
    if t == "grpc":
        return base64.b64decode(v["b64"])
    if t == "dbapi":  # a DB-API driver's result (adapters._dbapi), as the report reads it: rows with column names
        if v.get("rows") is None:
            return v.get("status")
        cols = v.get("columns") or []
        out = []
        for r in v["rows"]:
            r = decode(r)
            if isinstance(r, FakeRecord):
                out.append(r)
            elif isinstance(r, dict):
                out.append(FakeRecord(list(r), list(r.values())))
            else:
                out.append(FakeRecord(cols[:len(r)], list(r)))
        return out
    raise ValueError(f"unknown codec tag {t!r}")


def encode_exc(exc: BaseException) -> dict:
    cls = type(exc)
    out = {"__t": "exc", "type": f"{cls.__module__}:{cls.__qualname__}", "msg": str(exc)}
    if cls.__module__.startswith("grpc") and callable(getattr(exc, "code", None)):  # status code and details
        try:
            out["grpc"] = [exc.code().name, exc.details()]
        except Exception:
            pass
    return out


def decode_exc(v: dict) -> BaseException:
    if v.get("grpc"):
        try:
            import grpc
            from grpc.aio import AioRpcError, Metadata

            return AioRpcError(grpc.StatusCode[v["grpc"][0]], Metadata(), Metadata(), details=v["grpc"][1])
        except Exception:
            pass
    module, _, qualname = v["type"].partition(":")
    try:
        obj = importlib.import_module(module)
        for part in qualname.split("."):
            obj = getattr(obj, part)
        try:
            return obj(v["msg"])
        except Exception:
            exc = obj.__new__(obj)
            exc.args = (v["msg"],)
            return exc
    except Exception:
        return RuntimeError(f"{v['type']}: {v['msg']}")
