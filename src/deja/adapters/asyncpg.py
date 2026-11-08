"""asyncpg adapter.

Recording wraps:

* ``Connection`` / ``Pool`` query methods (``execute``, ``executemany``, ``fetch``, ``fetchrow``, ``fetchval``,
  ``fetchmany``), ``Pool.acquire()``, and ``Transaction`` start/commit/rollback;
* prepared statements (``Connection.prepare`` and the statement's ``fetch*`` / ``executemany`` / ``get_statusmsg``),
  which is how SQLAlchemy's asyncpg dialect runs every query;
* cursors (``async for row in conn.cursor(...)`` and ``await conn.cursor(...)`` with ``fetch`` / ``fetchrow`` /
  ``forward``);
* bulk copy (``copy_to_table``, ``copy_records_to_table``, ``copy_from_table``, ``copy_from_query``; the data a
  ``copy_from_*`` wrote to ``output`` is recorded and written again on replay).

Pool methods record once (the nested connection call is suppressed). Column descriptions of every prepared statement
go into the recording's catalog, so a replayed ORM can describe results even for statements prepared before the
recorded window.

Replay: ``asyncpg.create_pool`` and ``asyncpg.connect`` return fakes that answer from the recording, so no database
is needed. Out-of-request calls on the fakes (startup work such as an ORM reading the server version) are answered
from the recording's catalog when it has them, else with empty defaults.

LISTEN/NOTIFY: each callback given to ``add_listener`` is wrapped so the recording notes when a notification reached
it (channel, payload, and the listener's registration number). The replayed app registers its listeners on fake
connections in the same order, and replay calls them with the recorded payload at the recorded point.

Not covered: log listeners and termination listeners (accepted and ignored on replay).
"""

import inspect

import asyncpg
import asyncpg.connection
import asyncpg.cursor
import asyncpg.pool
import asyncpg.prepared_stmt
import asyncpg.transaction
from asyncpg import types as pgtypes
from asyncpg import utils as pgutils

from .. import codec
from . import OUTSIDE_LIMIT, _context, _get_loop, digest, io_call, passthrough, patch, replaying

QUERY_METHODS = ("execute", "executemany", "fetch", "fetchrow", "fetchval", "fetchmany")
STMT_METHODS = ("fetch", "fetchrow", "fetchval", "fetchmany", "executemany")
COPY_METHODS = ("copy_to_table", "copy_records_to_table", "copy_from_table", "copy_from_query")
DEFAULTS = {"execute": "", "executemany": None, "fetch": list, "fetchrow": None, "fetchval": None, "fetchmany": list}
CATALOG_LIMIT = 2000

_undo = []


def _wrap_query(name):
    def make(original):
        def method(self, query, *args, **kwargs):  # plain function: returns the awaitable, adds no frame
            if passthrough():
                return original(self, query, *args, **kwargs)
            return io_call(f"pg.{name}", _stmt_key(name, query, args), lambda: original(self, query, *args, **kwargs))

        method.__name__ = name
        method.__wrapped__ = original
        return method

    return make


def _wrap_simple(op):
    def make(original):
        def method(self, *args, **kwargs):
            if passthrough():
                return original(self, *args, **kwargs)
            return io_call(op, (op,), lambda: original(self, *args, **kwargs))

        method.__wrapped__ = original
        return method

    return make


def _wrap_tx_exit(original):
    """``async with conn.transaction()`` commits through a private method, not ``commit()``: record it as the
    same commit / rollback call the replay fake makes."""

    def __aexit__(self, extype, ex, tb):
        if passthrough():
            return original(self, extype, ex, tb)
        op = "pg.tx.commit" if extype is None else "pg.tx.rollback"
        return io_call(op, (op,), lambda: original(self, extype, ex, tb))

    __aexit__.__wrapped__ = original
    return __aexit__


# ---------------------------------------------------------------------- prepared statements
def _stmt_key(name, query, args):
    """Digest key; ``fetchmany`` / ``executemany`` take one list of argument tuples."""
    if name in ("fetchmany", "executemany"):
        return (name, query, args[0] if args else ())
    return (name, query, args)


def _attrs(stmt):
    return [[a.name, a.type.oid, a.type.name, a.type.kind, a.type.schema] for a in stmt.get_attributes()]


def _from_attrs(v):
    return tuple(pgtypes.Attribute(n, pgtypes.Type(oid, tn, kind, schema)) for n, oid, tn, kind, schema in (v or []))


def _catalog():
    rec = getattr(_get_loop(), "deja", None)
    if rec is None:
        return None
    if getattr(rec, "replaying", False):
        return (rec.schedule.header.get("catalog") or {}).get("pg.attributes", {})
    return rec.catalog.setdefault("pg.attributes", {})


def _wrap_prepare(original):
    async def prepare(self, query, *args, **kwargs):
        if passthrough():
            return await original(self, query, *args, **kwargs)
        stmt = await io_call("pg.prepare", ("prepare", query), lambda: original(self, query, *args, **kwargs),
                             encode=lambda _: None)
        cat = _catalog()
        if cat is not None and query not in cat and len(cat) < CATALOG_LIMIT:
            try:
                cat[query] = _attrs(stmt)
            except Exception:
                pass
        return stmt

    prepare.__wrapped__ = original
    return prepare


def _wrap_stmt(name):
    def make(original):
        def method(self, *args, **kwargs):
            if passthrough():
                return original(self, *args, **kwargs)
            key = _stmt_key(name, self._query, args)
            rec, path = _context()
            if path is None and rec is not None:  # startup work: keep the status too ("UPDATE 1" -> rowcount)
                return _outside_stmt(rec, self, name, key, lambda: original(self, *args, **kwargs))
            return io_call(f"pg.{name}", key, lambda: original(self, *args, **kwargs))

        method.__name__ = name
        method.__wrapped__ = original
        return method

    return make


async def _outside_stmt(rec, stmt, name, key, real):
    result = await io_call(f"pg.{name}", key, real)
    out = rec.catalog.get("outside")
    if out is not None and len(out) < OUTSIDE_LIMIT:
        out.setdefault(f"pg.status:{digest(key)}", ("ok", stmt.get_statusmsg()))
    return result


def _wrap_status(original):
    def get_statusmsg(self):  # "UPDATE 1": SQLAlchemy turns it into rowcount, so replay must return the same
        value = original(self)
        rec, path = _context()
        return rec.read("pg.status", value) if path is not None else value

    get_statusmsg.__wrapped__ = original
    return get_statusmsg


# ---------------------------------------------------------------------- cursors
def _wrap_cursor(op, name, nargs=False):
    def make(original):
        def method(self, *args, **kwargs):
            if passthrough():
                return original(self, *args, **kwargs)
            tag = f"{name}:{args[0]}" if nargs and args else name
            return io_call(op, (tag, self._query, self._args), lambda: original(self, *args, **kwargs),
                           encode=(lambda _: None) if name == "cursor.open" else codec.encode)

        method.__name__ = original.__name__
        method.__wrapped__ = original
        return method

    return make


# ---------------------------------------------------------------------- bulk copy
def _copy_key(name, args, kwargs):
    if name == "copy_from_query":
        return (name, args[0] if args else kwargs.get("query"), tuple(args[1:]))
    table = args[0] if args else kwargs.get("table_name")
    if kwargs.get("schema_name"):
        table = f"{kwargs['schema_name']}.{table}"
    sql = f"SELECT * FROM {table}" if name == "copy_from_table" else f"COPY {table} FROM STDIN"
    return (name, sql, (kwargs.get("columns"),))


async def _deliver(output, chunks):
    """Write copy-out data to the caller's output: a coroutine function, a file-like object or a path."""
    if callable(output) and not hasattr(output, "write"):
        for c in chunks:
            r = output(c)
            if inspect.isawaitable(r):
                await r
    elif hasattr(output, "write"):
        for c in chunks:
            output.write(c)
    else:
        with open(output, "wb") as f:
            for c in chunks:
                f.write(c)


def _wrap_copy(name):
    def make(original):
        async def method(self, *args, **kwargs):
            if passthrough():
                return await original(self, *args, **kwargs)
            key = _copy_key(name, args, kwargs)
            if not name.startswith("copy_from"):
                return await io_call(f"pg.{name}", key, lambda: original(self, *args, **kwargs))
            output = kwargs["output"]

            async def run():  # capture what the server sent, then hand it to the caller's output
                chunks = []

                async def sink(data):
                    chunks.append(bytes(data))

                status = await original(self, *args, **{**kwargs, "output": sink})
                await _deliver(output, chunks)
                return {"status": status, "chunks": chunks}

            return (await io_call(f"pg.{name}", key, run))["status"]

        method.__name__ = name
        method.__wrapped__ = original
        return method

    return make


_listener_wrappers: dict = {}  # (id(connection), channel, callback) -> the wrapper registered with asyncpg


def _wrap_add_listener(original):
    async def add_listener(self, channel, callback):
        rec = getattr(_get_loop(), "deja", None)
        if rec is None or replaying():
            return await original(self, channel, callback)
        lid = rec.next_listener()
        if inspect.iscoroutinefunction(callback):
            async def wrapper(con, pid, ch, payload):
                rec.notify(lid, pid, ch, payload)
                return await callback(con, pid, ch, payload)
        else:
            def wrapper(con, pid, ch, payload):
                rec.notify(lid, pid, ch, payload)
                return callback(con, pid, ch, payload)
        _listener_wrappers[(id(self), channel, callback)] = wrapper
        return await original(self, channel, wrapper)

    add_listener.__wrapped__ = original
    return add_listener


def _wrap_remove_listener(original):
    async def remove_listener(self, channel, callback):
        return await original(self, channel, _listener_wrappers.pop((id(self), channel, callback), callback))

    remove_listener.__wrapped__ = original
    return remove_listener


# ---------------------------------------------------------------------- fakes for replay
async def _fake_query(name, query, args):
    return await io_call(f"pg.{name}", _stmt_key(name, query, args), None, default=DEFAULTS[name])


def _stop():
    raise StopAsyncIteration


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


class FakePreparedStatement:
    def __init__(self, query):
        self._query = query
        self._last = None  # key of the last call, to look up its status outside a request

    def get_query(self):
        return self._query

    def get_name(self):
        return ""

    def get_parameters(self):
        return ()

    def get_attributes(self):
        return _from_attrs((_catalog() or {}).get(self._query))

    def get_statusmsg(self):
        rec, path = _context()
        if path is not None:
            return rec.read("pg.status", "")
        hit = rec.outside("pg.status", digest(self._last)) if rec is not None and self._last else None
        return hit[1] if hit else ""

    def cursor(self, *args, prefetch=None, timeout=None):
        return FakeCursorFactory(self._query, args)

    def __getattr__(self, name):
        if name not in STMT_METHODS:
            raise AttributeError(name)

        async def method(*args, **kwargs):
            self._last = _stmt_key(name, self._query, args)
            return await _fake_query(name, self._query, args)

        return method


class FakeCursorFactory:
    def __init__(self, query, args):
        self._query, self._args = query, args

    def __aiter__(self):
        return FakeCursor(self._query, self._args)

    def __await__(self):
        return FakeCursor(self._query, self._args)._init().__await__()


class FakeCursor:
    def __init__(self, query, args):
        self._query, self._args = query, args

    def __aiter__(self):
        return self

    def __anext__(self):
        return io_call("pg.cursor.next", ("cursor.next", self._query, self._args), None, default=_stop)

    async def _init(self):
        await io_call("pg.cursor.open", ("cursor.open", self._query, self._args), None)
        return self

    async def fetch(self, n, *, timeout=None):
        return await io_call("pg.cursor.fetch", (f"cursor.fetch:{n}", self._query, self._args), None, default=list)

    async def fetchrow(self, *, timeout=None):
        return await io_call("pg.cursor.fetchrow", ("cursor.fetchrow", self._query, self._args), None)

    async def forward(self, n, *, timeout=None):
        return await io_call("pg.cursor.forward", (f"cursor.forward:{n}", self._query, self._args), None, default=0)


class FakeConnection:
    def __init__(self):
        self._channels = set()
        for name in QUERY_METHODS:
            setattr(self, name, self._method(name))
        for name in COPY_METHODS:
            setattr(self, name, self._copy(name))

    @staticmethod
    def _method(name):
        async def method(query, *args, **kwargs):
            return await _fake_query(name, query, args)

        return method

    @staticmethod
    def _copy(name):
        async def method(*args, **kwargs):
            got = await io_call(f"pg.{name}", _copy_key(name, args, kwargs), None, default="COPY 0")
            if not name.startswith("copy_from"):
                return got
            if not isinstance(got, dict):
                got = {"status": "COPY 0", "chunks": []}
            await _deliver(kwargs["output"], got["chunks"])
            return got["status"]

        return method

    async def prepare(self, query, *args, **kwargs):
        return await io_call("pg.prepare", ("prepare", query), None, decode=lambda _: FakePreparedStatement(query),
                             default=lambda: FakePreparedStatement(query))

    def cursor(self, query, *args, prefetch=None, timeout=None, record_class=None):
        return FakeCursorFactory(query, args)

    def transaction(self, **kwargs):
        return FakeTransaction()

    def is_closed(self):
        return False

    def is_in_transaction(self):
        return False

    async def close(self, **kwargs):
        pass

    def terminate(self):
        pass

    async def reset(self, **kwargs):
        pass

    # setup an ORM does on connect: accepted and ignored on replay
    async def set_type_codec(self, *args, **kwargs):
        pass

    async def set_builtin_type_codec(self, *args, **kwargs):
        pass

    async def reset_type_codec(self, *args, **kwargs):
        pass

    async def reload_schema_state(self):
        pass

    async def add_listener(self, channel, callback):
        if channel not in self._channels:  # the real connection sends LISTEN once per channel
            self._channels.add(channel)
            await _fake_query("fetch", f"LISTEN {pgutils._quote_ident(channel)}", ())
        rec = getattr(_get_loop(), "deja", None)
        if rec is not None and hasattr(rec, "listeners"):
            rec.listeners[rec.next_listener()] = (self, channel, callback)

    async def remove_listener(self, channel, callback):
        rec = getattr(_get_loop(), "deja", None)
        listeners = getattr(rec, "listeners", {})
        for lid, (conn, ch, cb) in list(listeners.items()):
            if conn is self and ch == channel and cb == callback:
                del listeners[lid]
        if channel in self._channels and not any(c is self and ch == channel for c, ch, _ in listeners.values()):
            self._channels.discard(channel)
            await _fake_query("fetch", f"UNLISTEN {pgutils._quote_ident(channel)}", ())

    def add_log_listener(self, *args, **kwargs):
        pass

    def remove_log_listener(self, *args, **kwargs):
        pass

    def add_termination_listener(self, *args, **kwargs):
        pass

    def remove_termination_listener(self, *args, **kwargs):
        pass

    def get_server_pid(self):
        return 0

    def get_server_version(self):
        return pgtypes.ServerVersion(16, 0, 0, "final", 0)

    def get_settings(self):
        return None


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
            if hasattr(cls, name):
                _undo.append(patch(cls, name, _wrap_query(name)))
        for name in COPY_METHODS:
            if hasattr(cls, name):
                _undo.append(patch(cls, name, _wrap_copy(name)))
    _undo.append(patch(asyncpg.pool.PoolAcquireContext, "__aenter__", _wrap_simple("pg.acquire")))
    _undo.append(patch(asyncpg.pool.PoolAcquireContext, "__aexit__", _wrap_simple("pg.release")))
    for name in ("start", "commit", "rollback"):
        _undo.append(patch(asyncpg.transaction.Transaction, name, _wrap_simple(f"pg.tx.{name}")))
    _undo.append(patch(asyncpg.transaction.Transaction, "__aexit__", _wrap_tx_exit))
    _undo.append(patch(asyncpg.connection.Connection, "prepare", _wrap_prepare))
    _undo.append(patch(asyncpg.connection.Connection, "add_listener", _wrap_add_listener))
    _undo.append(patch(asyncpg.connection.Connection, "remove_listener", _wrap_remove_listener))
    stmt = asyncpg.prepared_stmt.PreparedStatement
    for name in STMT_METHODS:
        _undo.append(patch(stmt, name, _wrap_stmt(name)))
    _undo.append(patch(stmt, "get_statusmsg", _wrap_status))
    cur = asyncpg.cursor
    _undo.append(patch(cur.CursorIterator, "__anext__", _wrap_cursor("pg.cursor.next", "cursor.next")))
    _undo.append(patch(cur.Cursor, "_init", _wrap_cursor("pg.cursor.open", "cursor.open")))
    _undo.append(patch(cur.Cursor, "fetch", _wrap_cursor("pg.cursor.fetch", "cursor.fetch", nargs=True)))
    _undo.append(patch(cur.Cursor, "fetchrow", _wrap_cursor("pg.cursor.fetchrow", "cursor.fetchrow")))
    _undo.append(patch(cur.Cursor, "forward", _wrap_cursor("pg.cursor.forward", "cursor.forward", nargs=True)))

    def make_create_pool(original):
        def create_pool(*args, **kwargs):
            return FakePool() if replaying() else original(*args, **kwargs)

        return create_pool

    def make_connect(original):
        async def connect(*args, **kwargs):  # one I/O call, so a connection opened inside a request replays as one
            if passthrough():
                return await original(*args, **kwargs)
            if replaying():
                await io_call("pg.connect", ("pg.connect",), None)
                return FakeConnection()
            return await io_call("pg.connect", ("pg.connect",), lambda: original(*args, **kwargs), encode=lambda _: None)

        return connect

    _undo.append(patch(asyncpg, "create_pool", make_create_pool))
    _undo.append(patch(asyncpg, "connect", make_connect))


def uninstall():
    while _undo:
        _undo.pop()()
