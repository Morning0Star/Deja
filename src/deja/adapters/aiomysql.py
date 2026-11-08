"""aiomysql adapter: statements with their rows, transactions (implicit, ``begin`` / ``commit`` / ``rollback``),
connections and pools.

Statements are recorded as ``sql.query`` calls with ``$n`` placeholders (see :mod:`deja.adapters._dbapi`). A
statement on a non-autocommit connection outside a transaction opens one (InnoDB), recorded as ``sql.tx.start``.
Waiting for a pool connection, connecting and closing a cursor are single calls. Replay hands out stand-in pools,
connections and cursors (``execute`` returns the affected row count, like aiomysql's) without a server.
"""

import weakref

import aiomysql
from aiomysql import connection as conn_mod
from aiomysql import cursors
from aiomysql import pool as pool_mod

from . import _dbapi, io_call, passthrough, patch, replaying

_undo = []
OP, FLAVOR = "sql.query", "mysql"
_open: "weakref.WeakSet" = weakref.WeakSet()  # connections with a transaction open (MySQL's own flag lags a SELECT)


async def _noop():
    return None


class _Awaitable:
    """Awaitable and async context manager around a coroutine, like aiomysql's ``_ContextManager``."""

    def __init__(self, coro, on_exit=None):
        self._coro, self._obj, self._on_exit = coro, None, on_exit

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, *exc):
        if self._on_exit is not None:
            await self._on_exit(self._obj)


# ---------------------------------------------------------------------- recording (and live) side
def _wrap_execute(original, original_fetchall):
    async def execute(self, query, args=None):
        if passthrough():
            return await original(self, query, args)
        conn = self.connection
        if not conn.get_autocommit() and conn not in _open:
            _open.add(conn)
            await io_call("sql.tx.start", ("sql.tx.start",), _noop)

        def describe():
            cols = [d[0] for d in self.description] if self.description else None
            return cols, self.rowcount, self.lastrowid, None

        async def fetchall():
            return list(await original_fetchall(self))

        res = await _dbapi.run(OP, FLAVOR, self, query, args, lambda: original(self, query, args), fetchall, describe)
        return res["rowcount"]

    execute.__wrapped__ = original
    return execute


def _wrap_fetch(original, how):
    async def fetch(self, *args):
        if _dbapi.state(self) is None:
            return await original(self, *args)
        if how == "one":
            return _dbapi.fetchone(self)
        if how == "many":
            return _dbapi.fetch(self, args[0] if args else self.arraysize)
        return _dbapi.fetch(self)

    fetch.__wrapped__ = original
    return fetch


def _call(op, original):
    async def method(self, *args, **kwargs):
        if passthrough():
            return await original(self, *args, **kwargs)
        return await io_call(op, (op,), lambda: original(self, *args, **kwargs), encode=lambda v: None,
                             decode=lambda v: None)

    method.__wrapped__ = original
    return method


def _tx(op, original, opens):
    async def method(self, *args, **kwargs):
        (_open.add if opens else _open.discard)(self)
        if passthrough():
            return await original(self, *args, **kwargs)
        return await io_call(op, (op,), lambda: original(self, *args, **kwargs), encode=lambda v: None,
                             decode=lambda v: None)

    method.__wrapped__ = original
    return method


def _wrap_acquire_inner(original):
    async def _acquire(self):
        if passthrough():
            return await original(self)
        return await io_call("sql.acquire", ("sql.acquire",), lambda: original(self), encode=lambda v: None)

    _acquire.__wrapped__ = original
    return _acquire


# ---------------------------------------------------------------------- replay side
class FakeCursor(_dbapi.FakeCursor):
    def __init__(self, conn):
        super().__init__(conn, OP, FLAVOR)

    async def execute(self, query, args=None):
        await self.connection._before_statement(query)
        res = await _dbapi.run(OP, FLAVOR, self, query, args, None, None, None)
        return res["rowcount"]

    async def executemany(self, query, args):
        total = 0
        for a in args:
            total += (await self.execute(query, a)) or 0
        return total

    async def close(self):
        await io_call("sql.cursor.close", ("sql.cursor.close",), None)
        self.closed = True


class FakeConnection:
    """Stands in for ``aiomysql.Connection`` during replay."""

    def __init__(self, autocommit=False):
        self._autocommit, self._in_tx, self.closed = autocommit, False, False

    async def _before_statement(self, sql):
        if not self._autocommit and not self._in_tx:
            await io_call("sql.tx.start", ("sql.tx.start",), None)
            self._in_tx = True

    def cursor(self, *cursor_classes):
        async def make():
            return FakeCursor(self)

        return _Awaitable(make(), on_exit=lambda cur: cur.close())

    async def begin(self):
        await io_call("sql.tx.start", ("sql.tx.start",), None)
        self._in_tx = True

    async def commit(self):
        await io_call("sql.tx.commit", ("sql.tx.commit",), None)
        self._in_tx = False

    async def rollback(self):
        await io_call("sql.tx.rollback", ("sql.tx.rollback",), None)
        self._in_tx = False

    async def autocommit(self, value):
        self._autocommit = bool(value)

    def get_autocommit(self):
        return self._autocommit

    def get_transaction_status(self):
        return self._in_tx

    def close(self):
        self.closed = True

    async def ensure_closed(self):
        self.closed = True

    async def ping(self, reconnect=True):
        pass


class FakePool:
    def __init__(self, autocommit=False):
        self._autocommit = autocommit

    def acquire(self):
        async def get():
            await io_call("sql.acquire", ("sql.acquire",), None)
            return FakeConnection(self._autocommit)

        async def back(conn):
            await io_call("sql.release", ("sql.release",), None)

        return _Awaitable(get(), on_exit=back)

    def release(self, conn):
        import asyncio

        fut = asyncio.get_running_loop().create_future()
        fut.set_result(None)
        return fut

    def close(self):
        pass

    def terminate(self):
        pass

    async def wait_closed(self):
        pass

    async def clear(self):
        pass


def install():
    cur = cursors.Cursor
    fetchall = cur.fetchall
    _undo.append(patch(cur, "execute", lambda o: _wrap_execute(o, fetchall)))
    _undo.append(patch(cur, "fetchone", lambda o: _wrap_fetch(o, "one")))
    _undo.append(patch(cur, "fetchmany", lambda o: _wrap_fetch(o, "many")))
    _undo.append(patch(cur, "fetchall", lambda o: _wrap_fetch(o, "all")))
    _undo.append(patch(cur, "close", lambda o: _call("sql.cursor.close", o)))
    conn = conn_mod.Connection
    _undo.append(patch(conn, "begin", lambda o: _tx("sql.tx.start", o, True)))
    _undo.append(patch(conn, "commit", lambda o: _tx("sql.tx.commit", o, False)))
    _undo.append(patch(conn, "rollback", lambda o: _tx("sql.tx.rollback", o, False)))
    _undo.append(patch(conn, "_connect", lambda o: _call("sql.connect", o)))
    _undo.append(patch(pool_mod.Pool, "_acquire", _wrap_acquire_inner))
    from aiomysql import utils

    def make_release(original):  # leaving `async with pool.acquire()`: release starts a wake-up task; one call
        async def __aexit__(self, *exc):
            if passthrough():
                return await original(self, *exc)
            return await io_call("sql.release", ("sql.release",), lambda: original(self, *exc),
                                 encode=lambda v: None, decode=lambda v: None)

        return __aexit__

    _undo.append(patch(utils._PoolAcquireContextManager, "__aexit__", make_release))

    def make_create_pool(original):
        def create_pool(*args, **kwargs):
            if replaying():
                async def fake():
                    return FakePool(kwargs.get("autocommit", False))

                return _Awaitable(fake())
            return original(*args, **kwargs)

        return create_pool

    def make_connect(original):
        def connect(*args, **kwargs):
            if replaying():
                async def fake():
                    await io_call("sql.connect", ("sql.connect",), None)
                    return FakeConnection(kwargs.get("autocommit", False))

                return _Awaitable(fake(), on_exit=lambda c: c.ensure_closed())
            return original(*args, **kwargs)

        return connect

    for mod in (aiomysql, pool_mod):
        if hasattr(mod, "create_pool"):
            _undo.append(patch(mod, "create_pool", make_create_pool))
    for mod in (aiomysql, conn_mod):
        if hasattr(mod, "connect"):
            _undo.append(patch(mod, "connect", make_connect))


def uninstall():
    while _undo:
        _undo.pop()()
