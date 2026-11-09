"""psycopg 3 (async) adapter: ``AsyncCursor.execute`` / ``executemany`` and the cursor's fetches, transactions, and
``psycopg_pool.AsyncConnectionPool``.

Statements are recorded as ``pg.query`` calls with ``$n`` placeholders (see :mod:`deja.adapters._dbapi`), so the
report reads them like asyncpg's. Transactions are recorded as ``pg.tx.start`` / ``commit`` / ``rollback``: the
implicit one psycopg opens before the first statement of a non-autocommit connection, ``conn.transaction()``
blocks (savepoints inside them), and ``commit()`` / ``rollback()``. Replay connects nothing: ``AsyncConnection
.connect`` and the pool hand out stand-in connections that answer from the recording.
"""

import contextlib

import psycopg
from psycopg import pq

from . import _dbapi, io_call, passthrough, patch, replaying

_undo = []
OP, FLAVOR = "pg.query", "psycopg"


def _sql(query, cur=None):
    if hasattr(query, "as_string"):
        try:
            return query.as_string(cur)
        except Exception:
            return str(query)
    return query.decode() if isinstance(query, bytes) else str(query)


async def _noop():
    return None


async def _begin_if_idle(conn, autocommit, idle):
    """Record the transaction psycopg opens implicitly before a statement on a non-autocommit connection."""
    if not autocommit and idle:
        await io_call("pg.tx.start", ("pg.tx.start",), _noop)


# ---------------------------------------------------------------------- recording (and live) side
def _wrap_execute(original, fetchall):
    async def execute(self, query, params=None, **kwargs):
        if passthrough():
            return await original(self, query, params, **kwargs)
        conn = self.connection
        await _begin_if_idle(conn, conn.autocommit, conn.info.transaction_status == pq.TransactionStatus.IDLE)

        def describe():
            cols = [d.name for d in self.description] if self.description else None
            return cols, self.rowcount, None, self.statusmessage

        await _dbapi.run(
            OP,
            FLAVOR,
            self,
            _sql(query, self),
            params,
            lambda: original(self, query, params, **kwargs),
            lambda: fetchall(self),
            describe,
        )
        return self

    execute.__wrapped__ = original
    return execute


def _wrap_fetch(original, how):
    async def fetch(self, *args, **kwargs):
        if _dbapi.state(self) is None:
            return await original(self, *args, **kwargs)
        if how == "one":
            return _dbapi.fetchone(self)
        if how == "many":
            size = args[0] if args else kwargs.get("size") or self.arraysize
            return _dbapi.fetch(self, size)
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

    __aiter__.__wrapped__ = original
    return __aiter__


def _wrap_executemany(original):
    async def executemany(self, query, params_seq, **kwargs):
        if passthrough():
            return await original(self, query, params_seq, **kwargs)
        for params in params_seq:  # one recorded call per row: the analysis sees every row written
            await self.execute(query, params)
        return None

    executemany.__wrapped__ = original
    return executemany


def _wrap_conn_op(op):
    def make(original):
        async def method(self, *args, **kwargs):
            if passthrough():
                return await original(self, *args, **kwargs)
            return await io_call(op, (op,), lambda: original(self, *args, **kwargs))

        method.__wrapped__ = original
        return method

    return make


def _wrap_pool_op(op):
    def make(original):
        async def method(self, *args, **kwargs):
            if passthrough():
                return await original(self, *args, **kwargs)
            return await io_call(
                op, (op,), lambda: original(self, *args, **kwargs), encode=lambda v: None, decode=lambda v: None
            )

        method.__wrapped__ = original
        return method

    return make


def _wrap_close(original):
    async def close(self):
        if passthrough() or self.closed:
            return await original(self)
        return await io_call("pg.close", ("pg.close",), lambda: original(self))

    close.__wrapped__ = original
    return close


def _wrap_tx_enter(original):
    async def __aenter__(self):
        if passthrough():
            return await original(self)
        return await io_call("pg.tx.start", ("pg.tx.start",), lambda: original(self), decode=lambda v: self)

    __aenter__.__wrapped__ = original
    return __aenter__


def _wrap_tx_exit(original):
    async def __aexit__(self, exc_type, exc, tb):
        if passthrough():
            return await original(self, exc_type, exc, tb)
        op = "pg.tx.commit" if exc_type is None else "pg.tx.rollback"
        return await io_call(op, (op,), lambda: original(self, exc_type, exc, tb), decode=lambda v: False)

    __aexit__.__wrapped__ = original
    return __aexit__


# ---------------------------------------------------------------------- replay side
class _Info:
    def __init__(self, conn):
        self._conn = conn

    @property
    def transaction_status(self):
        return pq.TransactionStatus.INTRANS if self._conn._in_tx else pq.TransactionStatus.IDLE


class FakeTransaction:
    def __init__(self, conn):
        self.connection = conn

    async def __aenter__(self):
        await io_call("pg.tx.start", ("pg.tx.start",), None)
        self.connection._depth += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        op = "pg.tx.commit" if exc_type is None else "pg.tx.rollback"
        await io_call(op, (op,), None)
        self.connection._depth -= 1
        return False


class FakeConnection:
    """Stands in for ``psycopg.AsyncConnection`` during replay."""

    def __init__(self, autocommit=False, row_factory=None, pooled=False):
        self.autocommit, self.row_factory, self._pooled = autocommit, row_factory, pooled
        self._in_tx, self._depth, self.closed, self.broken = False, 0, False, False
        self.info = _Info(self)

    async def _before_statement(self, sql):
        if not self._depth:
            await _begin_if_idle(self, self.autocommit, not self._in_tx)
            if not self.autocommit:
                self._in_tx = True

    def cursor(self, *args, **kwargs):
        return _dbapi.FakeCursor(self, OP, FLAVOR)

    async def execute(self, query, params=None, **kwargs):
        cur = self.cursor()
        await cur.execute(_sql(query), params)
        return cur

    async def commit(self):
        await io_call("pg.tx.commit", ("pg.tx.commit",), None)
        self._in_tx = False

    async def rollback(self):
        await io_call("pg.tx.rollback", ("pg.tx.rollback",), None)
        self._in_tx = False

    def transaction(self, *args, **kwargs):
        return FakeTransaction(self)

    async def set_autocommit(self, value):
        self.autocommit = value

    async def close(self):
        if not self.closed:
            await io_call("pg.close", ("pg.close",), None)
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()
        if not self._pooled:  # a pool's connection goes back to the pool instead
            await self.close()
        return False


class FakePool:
    """Stands in for ``psycopg_pool.AsyncConnectionPool`` during replay."""

    def __init__(self, conninfo="", *args, kwargs=None, **options):
        self._autocommit = bool((kwargs or {}).get("autocommit", False))

    async def open(self, *args, **kwargs):
        pass

    async def wait(self, *args, **kwargs):
        pass

    async def close(self, *args, **kwargs):
        pass

    @contextlib.asynccontextmanager
    async def connection(self, *args, **kwargs):  # as the real pool: getconn, the connection's context, putconn
        conn = await self.getconn()
        try:
            async with conn:
                yield conn
        finally:
            await self.putconn(conn)

    async def getconn(self, *args, **kwargs):
        await io_call("pg.acquire", ("pg.acquire",), None)
        return FakeConnection(self._autocommit, pooled=True)

    async def putconn(self, conn):
        await io_call("pg.release", ("pg.release",), None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _wrap_connect(original):
    async def connect(cls, conninfo="", **kwargs):
        if passthrough():
            return await original.__func__(cls, conninfo, **kwargs)

        def fake(_=None):
            return FakeConnection(kwargs.get("autocommit", False), kwargs.get("row_factory"))

        # one recorded call: the steps connecting takes collapse into one, and replay hands back a stand-in
        return await io_call(
            "pg.connect",
            ("pg.connect",),
            lambda: original.__func__(cls, conninfo, **kwargs),
            encode=lambda c: None,
            decode=fake,
            default=fake,
        )

    return classmethod(connect)


def install():
    cur = psycopg.AsyncCursor
    fetchall = cur.fetchall
    _undo.append(patch(cur, "execute", lambda o: _wrap_execute(o, fetchall)))
    _undo.append(patch(cur, "executemany", _wrap_executemany))
    _undo.append(patch(cur, "fetchone", lambda o: _wrap_fetch(o, "one")))
    _undo.append(patch(cur, "fetchmany", lambda o: _wrap_fetch(o, "many")))
    _undo.append(patch(cur, "fetchall", lambda o: _wrap_fetch(o, "all")))
    _undo.append(patch(cur, "__aiter__", _wrap_aiter))
    conn = psycopg.AsyncConnection
    _undo.append(patch(conn, "commit", _wrap_conn_op("pg.tx.commit")))
    _undo.append(patch(conn, "rollback", _wrap_conn_op("pg.tx.rollback")))
    _undo.append(patch(conn, "close", _wrap_close))
    original_connect = conn.__dict__["connect"]
    conn.connect = _wrap_connect(original_connect)
    _undo.append(lambda: setattr(conn, "connect", original_connect))
    tx = psycopg.AsyncTransaction
    _undo.append(patch(tx, "__aenter__", _wrap_tx_enter))
    _undo.append(patch(tx, "__aexit__", _wrap_tx_exit))
    _install_sync()
    try:
        import psycopg_pool
    except ImportError:
        return
    pool = psycopg_pool.AsyncConnectionPool
    for name, op in (("getconn", "pg.acquire"), ("putconn", "pg.release")):  # waiting for a connection: one call
        _undo.append(patch(pool, name, _wrap_pool_op(op)))
    real_new = pool.__new__

    def __new__(cls, *args, **kwargs):  # replay: the stand-in pool, created like the real one
        if replaying():
            return FakePool(*args, **kwargs)
        return real_new(cls) if real_new is object.__new__ else real_new(cls, *args, **kwargs)

    pool.__new__ = staticmethod(__new__)
    _undo.append(lambda: setattr(pool, "__new__", real_new))


def uninstall():
    while _undo:
        _undo.pop()()


# ---------------------------------------------------------------------- the blocking API (psycopg.Connection)
def _sync_tx_op(op):
    from . import io_call_sync

    return io_call_sync(op, (op,), lambda: None, encode=lambda v: None, decode=lambda v: None)


class SyncFakeTransaction:
    def __init__(self, conn):
        self.connection = conn

    def __enter__(self):
        _sync_tx_op("pg.tx.start")
        self.connection._depth += 1
        return self

    def __exit__(self, exc_type, exc, tb):
        _sync_tx_op("pg.tx.commit" if exc_type is None else "pg.tx.rollback")
        self.connection._depth -= 1
        return False


class SyncFakeConnection:
    """Stands in for ``psycopg.Connection`` during replay."""

    def __init__(self, autocommit=False):
        self.autocommit, self._in_tx, self._depth, self.closed = autocommit, False, 0, False

    def _before_statement(self, sql):
        if not self._depth and not self.autocommit and not self._in_tx:
            _sync_tx_op("pg.tx.start")
            self._in_tx = True

    def cursor(self, *args, **kwargs):
        return _dbapi.SyncFakeCursor(self, OP, FLAVOR)

    def execute(self, query, params=None, **kwargs):
        cur = self.cursor()
        cur.execute(_sql(query), params)
        return cur

    def commit(self):
        _sync_tx_op("pg.tx.commit")
        self._in_tx = False

    def rollback(self):
        _sync_tx_op("pg.tx.rollback")
        self._in_tx = False

    def transaction(self, *args, **kwargs):
        return SyncFakeTransaction(self)

    def close(self):
        if not self.closed:
            _sync_tx_op("pg.close")
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
        return False


def _install_sync():
    from . import _sync_context, io_call_sync

    def in_scope():
        return _sync_context()[0] is not None

    cur = psycopg.Cursor
    fetchall = cur.fetchall

    def make_execute(original):
        def execute(self, query, params=None, **kwargs):
            if not in_scope():
                return original(self, query, params, **kwargs)
            conn = self.connection
            if not conn.autocommit and conn.info.transaction_status == pq.TransactionStatus.IDLE:
                _sync_tx_op("pg.tx.start")

            def describe():
                cols = [d.name for d in self.description] if self.description else None
                return cols, self.rowcount, None, self.statusmessage

            _dbapi.run_sync(
                OP,
                FLAVOR,
                self,
                _sql(query, self),
                params,
                lambda: original(self, query, params, **kwargs),
                lambda: fetchall(self),
                describe,
            )
            return self

        return execute

    def make_fetch(original, how):
        def fetch(self, *args, **kwargs):
            if _dbapi.state(self) is None:
                return original(self, *args, **kwargs)
            if how == "one":
                return _dbapi.fetchone(self)
            if how == "many":
                return _dbapi.fetch(self, (args[0] if args else kwargs.get("size")) or self.arraysize)
            return _dbapi.fetch(self)

        return fetch

    def make_iter(original):
        def __iter__(self):
            if _dbapi.state(self) is None:
                return original(self)
            return iter(_dbapi.fetch(self))

        return __iter__

    def make_conn_op(op):
        def make(original):
            def method(self, *args, **kwargs):
                if not in_scope():
                    return original(self, *args, **kwargs)
                return io_call_sync(
                    op, (op,), lambda: original(self, *args, **kwargs), encode=lambda v: None, decode=lambda v: None
                )

            return method

        return make

    def make_tx_enter(original):
        def __enter__(self):
            if in_scope():
                _sync_tx_op("pg.tx.start")
            return original(self)

        return __enter__

    def make_tx_exit(original):
        def __exit__(self, exc_type, exc, tb):
            if in_scope():
                _sync_tx_op("pg.tx.commit" if exc_type is None else "pg.tx.rollback")
            return original(self, exc_type, exc, tb)

        return __exit__

    _undo.append(patch(cur, "execute", make_execute))
    _undo.append(patch(cur, "fetchone", lambda o: make_fetch(o, "one")))
    _undo.append(patch(cur, "fetchmany", lambda o: make_fetch(o, "many")))
    _undo.append(patch(cur, "fetchall", lambda o: make_fetch(o, "all")))
    _undo.append(patch(cur, "__iter__", make_iter))
    conn = psycopg.Connection
    _undo.append(patch(conn, "commit", make_conn_op("pg.tx.commit")))
    _undo.append(patch(conn, "rollback", make_conn_op("pg.tx.rollback")))

    def make_close(original):
        def close(self):
            if self.closed or not in_scope():
                return original(self)
            return io_call_sync(
                "pg.close", ("pg.close",), lambda: original(self), encode=lambda v: None, decode=lambda v: None
            )

        return close

    _undo.append(patch(conn, "close", make_close))
    _undo.append(patch(psycopg.Transaction, "__enter__", make_tx_enter))
    _undo.append(patch(psycopg.Transaction, "__exit__", make_tx_exit))
    original_connect = conn.__dict__["connect"]

    def connect(cls, conninfo="", **kwargs):
        rec, path = _sync_context()
        if rec is not None and getattr(rec, "replaying", False):
            _sync_tx_op("pg.connect")
            return SyncFakeConnection(kwargs.get("autocommit", False))
        if rec is None:
            return original_connect.__func__(cls, conninfo, **kwargs)
        return io_call_sync(
            "pg.connect",
            ("pg.connect",),
            lambda: original_connect.__func__(cls, conninfo, **kwargs),
            encode=lambda v: None,
            decode=lambda v: None,
        )

    conn.connect = classmethod(connect)
    _undo.append(lambda: setattr(conn, "connect", original_connect))
    module_connect = psycopg.connect  # `psycopg.connect` is Connection.connect, bound when psycopg was imported
    psycopg.connect = conn.connect
    _undo.append(lambda: setattr(psycopg, "connect", module_connect))
