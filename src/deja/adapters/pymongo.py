"""PyMongo async adapter (``pymongo.AsyncMongoClient``; Motor is deprecated in its favour).

Collection methods are recorded calls (``find_one``, ``insert_one``, ``update_one``, ``find_one_and_update``,
``count_documents``, ``aggregate``...); a ``find()`` cursor fetches every document once, on its first
``to_list`` / ``async for``, and serves the app from that copy. Results come back as the same pymongo types
(``InsertOneResult.inserted_id``, ``UpdateResult.modified_count``...) and ``ObjectId`` values stay ``ObjectId``.
Replay sends nothing to the server.
"""

import weakref

from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.results import BulkWriteResult, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult

from .. import codec
from . import io_call, passthrough, patch

_undo = []
_cursors: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()  # cursor -> fetched documents
METHODS = ("insert_one", "insert_many", "find_one", "update_one", "update_many", "replace_one", "delete_one",
           "delete_many", "find_one_and_update", "find_one_and_replace", "find_one_and_delete", "count_documents",
           "estimated_document_count", "distinct", "bulk_write")


def _args(args, kwargs):
    kw = {k: v for k, v in kwargs.items() if k not in ("session", "comment")}
    try:
        return codec.encode({"args": list(args), "kwargs": kw})
    except Exception:
        return codec.encode({"args": [repr(a) for a in args], "kwargs": repr(kw)})


def _encode(r):
    if isinstance(r, InsertOneResult):
        return {"__t": "mongo", "type": "InsertOneResult", "v": [codec.encode(r.inserted_id), r.acknowledged]}
    if isinstance(r, InsertManyResult):
        return {"__t": "mongo", "type": "InsertManyResult", "v": [codec.encode(r.inserted_ids), r.acknowledged]}
    if isinstance(r, (UpdateResult, DeleteResult)):
        return {"__t": "mongo", "type": type(r).__name__, "v": [codec.encode(r.raw_result), r.acknowledged]}
    if isinstance(r, BulkWriteResult):
        return {"__t": "mongo", "type": "BulkWriteResult", "v": [codec.encode(r.bulk_api_result), r.acknowledged]}
    return codec.encode(r)


_RESULTS = {"InsertOneResult": InsertOneResult, "InsertManyResult": InsertManyResult, "UpdateResult": UpdateResult,
            "DeleteResult": DeleteResult, "BulkWriteResult": BulkWriteResult}


def _decode(v):
    if isinstance(v, dict) and v.get("__t") == "mongo":
        a, ack = v["v"]
        return _RESULTS[v["type"]](codec.decode(a), ack)
    return codec.decode(v)


class ListCursor:
    """Documents already fetched, with the cursor methods apps use (``to_list``, ``async for``, ``next``)."""

    def __init__(self, docs):
        self._docs = list(docs or [])

    async def to_list(self, length=None):
        out = self._docs if length is None else self._docs[:length]
        self._docs = [] if length is None else self._docs[length:]
        return list(out)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._docs:
            raise StopAsyncIteration
        return self._docs.pop(0)

    async def next(self):
        return await self.__anext__()

    async def close(self):
        self._docs = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


async def _load(cursor, to_list):
    """Fetch all of a ``find()`` cursor's documents once (recorded), keyed by its query."""
    docs = _cursors.get(cursor)
    if docs is None:
        key = ("mongo", cursor.collection.full_name, "find", codec.encode({
            "filter": getattr(cursor, "_spec", None), "projection": getattr(cursor, "_projection", None),
            "sort": getattr(cursor, "_ordering", None), "skip": getattr(cursor, "_skip", 0),
            "limit": getattr(cursor, "_limit", 0)}))
        docs = list(await io_call("mongo.find", key, lambda: to_list(cursor, None), default=list) or [])
        _cursors[cursor] = docs
    return docs


def install():
    def make_method(name):
        def make(original):
            async def method(self, *args, **kwargs):
                if passthrough():
                    return await original(self, *args, **kwargs)
                key = ("mongo", self.full_name, name, _args(args, kwargs))
                return await io_call(f"mongo.{name}", key, lambda: original(self, *args, **kwargs), encode=_encode,
                                     decode=_decode)

            method.__wrapped__ = original
            return method

        return make

    for name in METHODS:
        _undo.append(patch(AsyncCollection, name, make_method(name)))

    def make_aggregate(original):
        async def aggregate(self, pipeline, *args, **kwargs):
            if passthrough():
                return await original(self, pipeline, *args, **kwargs)
            key = ("mongo", self.full_name, "aggregate", _args((pipeline,) + args, kwargs))

            async def real():
                cur = await original(self, pipeline, *args, **kwargs)
                return await cur.to_list(None)

            return ListCursor(await io_call("mongo.aggregate", key, real, default=list))

        aggregate.__wrapped__ = original
        return aggregate

    _undo.append(patch(AsyncCollection, "aggregate", make_aggregate))
    to_list = AsyncCursor.to_list

    def make_to_list(original):
        async def to_list_(self, length=None):
            if passthrough():
                return await original(self, length)
            docs = await _load(self, original)
            out, rest = (docs, []) if length is None else (docs[:length], docs[length:])
            _cursors[self] = rest
            return list(out)

        return to_list_

    def make_anext(original):
        async def __anext__(self):
            if passthrough():
                return await original(self)
            docs = await _load(self, to_list)
            if not docs:
                raise StopAsyncIteration
            return docs.pop(0)

        return __anext__

    _undo.append(patch(AsyncCursor, "to_list", make_to_list))
    _undo.append(patch(AsyncCursor, "__anext__", make_anext))
    _undo.append(patch(AsyncCursor, "next", make_anext))


def uninstall():
    while _undo:
        _undo.pop()()
