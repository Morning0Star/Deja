"""aiosqlite adapter: statements, fetches, transactions, connect / close.

aiosqlite runs every call on its own thread and waits for the result, so each call is recorded as one I/O call
(``sql.query`` with its rows, ``sql.cursor``, ``sql.tx.start`` / ``commit`` / ``rollback``, ``sql.connect`` /
``sql.close``) and replay answers them without opening the database file. A write statement opens sqlite's
implicit transaction (unless ``isolation_level`` is None), which the analysis needs to know when the write became
visible. Placeholders (``?``, ``:name``) are rewritten to ``$n`` for the SQL analysis.
"""

import re

import aiosqlite
import aiosqlite.core as core
import aiosqlite.cursor as cursor_mod
from aiosqlite.context import contextmanager

from . import _dbapi, io_call, passthrough, patch, replaying

_undo = []
OP, FLAVOR = "sql.query", "sqlite"
_DML = re.compile(r"^\s*(insert|update|delete|replace)\b", re.I)


async def _noop():
    return None


def _real_execute(original, original_fetchall):
    async def execute(self, sql, parameters=None):
        if passthrough():
            return await original(self, sql, parameters)
        conn = self._conn
        if _DML.match(str(sql)) and conn.isolation_level is not None and not conn.in_transaction:
            await io_call("sql.tx.start", ("sql.tx.start",), _noop)

        def describe():
            cols = [d[0] for d in self.description] if self.description else None
            return cols, self.rowcount, self.lastrowid, None

        await _dbapi.run(OP, FLAVOR, self, sql, parameters, lambda: original(self, sql, parameters or []),
                         lambda: original_fetchall(self), describe)
        return self

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


def _wrap_aiter(original):
    def __aiter__(self):
        if _dbapi.state(self) is None:
            return original(self)

        async def rows():
            while True:
                row = _dbapi.fetchone(self)
                if row is None:
                    return
                yield row

        return rows()

    return __aiter__


def _call(op, original, encode=lambda v: None, decode=lambda v: None):
    async def method(self, *args, **kwargs):
        if passthrough():
            return await original(self, *args, **kwargs)
        return await io_call(op, (op,), lambda: original(self, *args, **kwargs), encode=encode, decode=decode)

    method.__wrapped__ = original
    return method


# ---------------------------------------------------------------------- replay side
class _Result:
    """aiosqlite's Result for the stand-ins: awaitable, and an async context manager that closes the cursor."""

    def __init__(self, coro):
        self._coro, self._obj = coro, None

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, *exc):
        if isinstance(self._obj, _FakeCursor):
            await self._obj.close()


def _fake_cm(method):
    def wrapper(self, *args, **kwargs):
        return _Result(method(self, *args, **kwargs))

    return wrapper


class FakeConnection:
    """Stands in for ``aiosqlite.Connection`` during replay."""

    def __init__(self, isolation_level=""):
        self.isolation_level, self.row_factory, self.text_factory = isolation_level, None, str
        self._in_tx, self.total_changes = False, 0

    @property
    def in_transaction(self):
        return self._in_tx

    async def _before_statement(self, sql):
        if _DML.match(str(sql)) and self.isolation_level is not None and not self._in_tx:
            await io_call("sql.tx.start", ("sql.tx.start",), None)
            self._in_tx = True

    @_fake_cm
    async def cursor(self):
        await io_call("sql.cursor", ("sql.cursor",), None)
        return _FakeCursor(self)

    @_fake_cm
    async def execute(self, sql, parameters=None):
        cur = await self.cursor()
        await cur.execute(sql, parameters)
        return cur

    @_fake_cm
    async def executemany(self, sql, parameters):
        cur = await self.cursor()
        await cur.executemany(sql, parameters)
        return cur

    @_fake_cm
    async def execute_fetchall(self, sql, parameters=None):
        cur = await self.execute(sql, parameters)
        return await cur.fetchall()

    async def commit(self):
        await io_call("sql.tx.commit", ("sql.tx.commit",), None)
        self._in_tx = False

    async def rollback(self):
        await io_call("sql.tx.rollback", ("sql.tx.rollback",), None)
        self._in_tx = False

    async def close(self):
        await io_call("sql.close", ("sql.close",), None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


class _FakeCursor(_dbapi.FakeCursor):
    def __init__(self, conn):
        super().__init__(conn, OP, FLAVOR)

    async def close(self):
        await io_call("sql.cursor.close", ("sql.cursor.close",), None)
        self.closed = True


class _FakeConnect:
    """What ``aiosqlite.connect()`` returns during replay: awaitable, and an async context manager."""

    def __init__(self, kwargs):
        self._kwargs = kwargs
        self._conn = None

    async def _open(self):
        await io_call("sql.connect", ("sql.connect",), None)
        self._conn = FakeConnection(self._kwargs.get("isolation_level", ""))
        return self._conn

    def __await__(self):
        return self._open().__await__()

    async def __aenter__(self):
        return await self._open()

    async def __aexit__(self, *exc):
        await self._conn.close()


def install():
    cur = cursor_mod.Cursor
    fetchall = cur.fetchall
    _undo.append(patch(cur, "execute", lambda o: _real_execute(o, fetchall)))
    _undo.append(patch(cur, "fetchone", lambda o: _wrap_fetch(o, "one")))
    _undo.append(patch(cur, "fetchmany", lambda o: _wrap_fetch(o, "many")))
    _undo.append(patch(cur, "fetchall", lambda o: _wrap_fetch(o, "all")))
    _undo.append(patch(cur, "__aiter__", _wrap_aiter))
    _undo.append(patch(cur, "close", lambda o: _call("sql.cursor.close", o)))
    conn = core.Connection

    def make_cursor(original):
        @contextmanager
        async def cursor(self):
            if passthrough():
                return await original.__wrapped__(self)
            return await io_call("sql.cursor", ("sql.cursor",), lambda: original.__wrapped__(self),
                                 encode=lambda v: None, decode=lambda v: None)

        return cursor

    def make_execute(original):
        @contextmanager
        async def execute(self, sql, parameters=None):
            if passthrough():
                return await original.__wrapped__(self, sql, parameters)
            c = await self.cursor()
            await c.execute(sql, parameters)
            return c

        return execute

    def make_executemany(original):
        @contextmanager
        async def executemany(self, sql, parameters):
            if passthrough():
                return await original.__wrapped__(self, sql, parameters)
            c = await self.cursor()
            for p in parameters:  # one recorded call per row: the analysis sees every row written
                await c.execute(sql, p)
            return c

        return executemany

    def make_fetchall(original):
        @contextmanager
        async def execute_fetchall(self, sql, parameters=None):
            if passthrough():
                return await original.__wrapped__(self, sql, parameters)
            c = await self.execute(sql, parameters)
            return await c.fetchall()

        return execute_fetchall

    _undo.append(patch(conn, "cursor", make_cursor))
    _undo.append(patch(conn, "execute", make_execute))
    _undo.append(patch(conn, "executemany", make_executemany))
    _undo.append(patch(conn, "execute_fetchall", make_fetchall))
    _undo.append(patch(conn, "commit", lambda o: _call("sql.tx.commit", o)))
    _undo.append(patch(conn, "rollback", lambda o: _call("sql.tx.rollback", o)))
    _undo.append(patch(conn, "close", lambda o: _call("sql.close", o)))
    _undo.append(patch(conn, "_connect", lambda o: _call("sql.connect", o, decode=lambda v: None)))

    def make_connect(original):
        def connect(database, *args, **kwargs):
            if replaying():
                return _FakeConnect(kwargs)
            return original(database, *args, **kwargs)

        connect.__wrapped__ = original
        return connect

    for mod in (aiosqlite, core):
        _undo.append(patch(mod, "connect", make_connect))


def uninstall():
    while _undo:
        _undo.pop()()
