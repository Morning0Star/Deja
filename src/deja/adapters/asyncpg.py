"""asyncpg adapter.

Recording: wraps ``Connection`` / ``Pool`` query methods, ``Pool.acquire()``, and ``Transaction`` start/commit/rollback.
Pool methods record once (the nested connection call is suppressed).

Replay: ``asyncpg.create_pool`` and ``asyncpg.connect`` return fakes that answer from the recording, so no database
is needed. Out-of-scope calls on the fakes (e.g. ``CREATE TABLE`` at startup) return empty defaults.

Not covered yet: prepared statements, cursors, ``copy_*``, listeners.
"""

import asyncpg
import asyncpg.connection
import asyncpg.pool
import asyncpg.transaction

from . import io_call, patch, replaying

QUERY_METHODS = ("execute", "executemany", "fetch", "fetchrow", "fetchval")
DEFAULTS = {"execute": "", "executemany": None, "fetch": list, "fetchrow": None, "fetchval": None}

_undo = []


def _wrap_query(name):
    def make(original):
        async def method(self, query, *args, **kwargs):
            return await io_call(f"pg.{name}", (name, query, args), lambda: original(self, query, *args, **kwargs))

        method.__name__ = name
        method.__wrapped__ = original
        return method

    return make


def _wrap_simple(op):
    def make(original):
        async def method(self, *args, **kwargs):
            return await io_call(op, (op,), lambda: original(self, *args, **kwargs))

        method.__wrapped__ = original
        return method

    return make


# ---------------------------------------------------------------------- fakes for replay
async def _fake_query(name, query, args):
    return await io_call(f"pg.{name}", (name, query, args), None, default=DEFAULTS[name])


class FakeTransaction:
    async def start(self):
        await io_call("pg.tx.start", ("pg.tx.start",), None)

    async def commit(self):
        await io_call("pg.tx.commit", ("pg.tx.commit",), None)

    async def rollback(self):
        await io_call("pg.tx.rollback", ("pg.tx.rollback",), None)

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()
        return False


class FakeConnection:
    def __init__(self):
        for name in QUERY_METHODS:
            setattr(self, name, self._method(name))

    @staticmethod
    def _method(name):
        async def method(query, *args, **kwargs):
            return await _fake_query(name, query, args)

        return method

    def transaction(self, **kwargs):
        return FakeTransaction()

    def is_closed(self):
        return False

    async def close(self, **kwargs):
        pass


class _FakeAcquire:
    def __init__(self):
        self.conn = FakeConnection()

    async def __aenter__(self):
        await io_call("pg.acquire", ("pg.acquire",), None)
        return self.conn

    async def __aexit__(self, *exc):
        await io_call("pg.release", ("pg.release",), None)
        return False

    def __await__(self):
        return self.__aenter__().__await__()


class FakePool(FakeConnection):
    def acquire(self, **kwargs):
        return _FakeAcquire()

    async def release(self, conn, **kwargs):
        pass

    async def close(self):
        pass

    def terminate(self):
        pass

    def __await__(self):  # `await asyncpg.create_pool(...)`
        async def ready():
            return self

        return ready().__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def install():
    for cls in (asyncpg.connection.Connection, asyncpg.pool.Pool):
        for name in QUERY_METHODS:
            _undo.append(patch(cls, name, _wrap_query(name)))
    _undo.append(patch(asyncpg.pool.PoolAcquireContext, "__aenter__", _wrap_simple("pg.acquire")))
    _undo.append(patch(asyncpg.pool.PoolAcquireContext, "__aexit__", _wrap_simple("pg.release")))
    for name in ("start", "commit", "rollback"):
        _undo.append(patch(asyncpg.transaction.Transaction, name, _wrap_simple(f"pg.tx.{name}")))

    def make_create_pool(original):
        def create_pool(*args, **kwargs):
            return FakePool() if replaying() else original(*args, **kwargs)

        return create_pool

    def make_connect(original):
        async def connect(*args, **kwargs):
            return FakeConnection() if replaying() else await original(*args, **kwargs)

        return connect

    _undo.append(patch(asyncpg, "create_pool", make_create_pool))
    _undo.append(patch(asyncpg, "connect", make_connect))


def uninstall():
    while _undo:
        _undo.pop()()
