"""Shared record / replay for DB-API-style async drivers (psycopg 3, aiosqlite).

Recording runs the real ``execute``, then fetches every row once and keeps them for the cursor: the app's own
``fetchone`` / ``fetchmany`` / ``fetchall`` / ``async for`` read from that copy. One recorded call holds the rows,
the column names, the row count and the last row id. Replay answers ``execute`` from the recording on stand-in
connections and cursors, without a database.

Placeholders (``%s``, ``%(name)s``, ``?``, ``:name``) are rewritten to ``$n`` in the recorded SQL, so the report's
SQL analysis reads statements from every driver the same way.
"""

import re
import weakref

from .. import codec
from . import io_call

_cursors: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()  # real cursor -> its recorded result
_QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def to_dollar(sql, params):
    """``(sql with $n placeholders, params as a list)`` for any DB-API paramstyle (outside quoted text)."""
    sql = str(sql)
    named = isinstance(params, dict)
    order, out, pos = [], [], 0

    def repl(m):
        tok = m.group(0)
        if tok == "%%":
            return "%"
        if tok in ("%s", "?"):
            order.append(len(order))
        else:
            order.append(tok[2:-2] if tok.startswith("%(") else tok[1:])
        return f"${len(order)}"

    pattern = re.compile(r"%%|%\(\w+\)s|%s|\?|(?<![:\w]):[A-Za-z_]\w*")
    for m in _QUOTED.finditer(sql):
        out.append(pattern.sub(repl, sql[pos:m.start()]))
        out.append(m.group(0))
        pos = m.end()
    out.append(pattern.sub(repl, sql[pos:]))
    if params is None:
        values = []
    elif named:
        values = [params.get(k) for k in order]
    else:
        values = list(params)
    return "".join(out), [codec.encode(v) for v in values]


def _row(r):
    if isinstance(r, (tuple, list, dict)) or r is None:
        return codec.encode(r)
    if hasattr(r, "keys"):  # sqlite3.Row and friends: index and key access
        keys = list(r.keys())
        return codec.encode(codec.FakeRecord(keys, [r[k] for k in keys]))
    return codec.encode(tuple(r))


def encode_result(res):
    return {"__t": "dbapi", "columns": res.get("columns"), "rowcount": res.get("rowcount"),
            "lastrowid": res.get("lastrowid"), "status": res.get("status"),
            "rows": None if res.get("rows") is None else [_row(r) for r in res["rows"]]}


def decode_result(v):
    if not isinstance(v, dict):
        return {"rows": None, "columns": None, "rowcount": -1, "lastrowid": None, "status": None}
    return {"columns": v.get("columns"), "rowcount": v.get("rowcount", -1), "lastrowid": v.get("lastrowid"),
            "status": v.get("status"), "rows": None if v.get("rows") is None else [codec.decode(r) for r in v["rows"]]}


async def run(op, flavor, cur, sql, params, real_execute, fetchall, describe):
    """Record (or replay) one ``execute`` on ``cur``. ``real_execute()`` runs it; ``fetchall()`` reads every row;
    ``describe()`` gives ``(columns, rowcount, lastrowid, status)`` after it."""
    text, values = to_dollar(sql, params)

    async def real():
        await real_execute()
        cols, rowcount, lastrowid, status = describe()
        rows = await fetchall() if cols else None
        return {"rows": rows, "columns": cols, "rowcount": rowcount, "lastrowid": lastrowid, "status": status}

    res = await io_call(op, (flavor, text, values), real, encode=encode_result, decode=decode_result,
                        default=lambda: decode_result(None))
    remember(cur, res)
    return res


def remember(cur, res):
    _cursors[cur] = {"rows": list(res["rows"]) if res.get("rows") is not None else None, "pos": 0, **{
        k: res.get(k) for k in ("columns", "rowcount", "lastrowid", "status")}}


def state(cur):
    return _cursors.get(cur)


def fetch(cur, n=None):
    st = _cursors.get(cur)
    rows = st["rows"] or []
    if n is None:
        out, st["pos"] = rows[st["pos"]:], len(rows)
        return out
    out = rows[st["pos"]:st["pos"] + n]
    st["pos"] += len(out)
    return out


def fetchone(cur):
    got = fetch(cur, 1)
    return got[0] if got else None


class FakeCursor:
    """A cursor answering from the recording (replay). ``op`` / ``flavor`` match the recorded calls."""

    def __init__(self, conn, op, flavor):
        self.connection, self._op, self._flavor = conn, op, flavor
        self.arraysize, self.closed = 1, False
        remember(self, {"rows": None, "columns": None, "rowcount": -1, "lastrowid": None, "status": None})

    async def execute(self, sql, params=None, *args, **kwargs):
        await self.connection._before_statement(sql)
        await run(self._op, self._flavor, self, sql, params, None, None, None)
        return self

    async def executemany(self, sql, seq, *args, **kwargs):
        for params in seq:
            await self.execute(sql, params)
        return self

    async def fetchone(self):
        return fetchone(self)

    async def fetchmany(self, size=None):
        return fetch(self, size or self.arraysize)

    async def fetchall(self):
        return fetch(self)

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        while True:
            row = fetchone(self)
            if row is None:
                return
            yield row

    @property
    def rowcount(self):
        return state(self)["rowcount"]

    @property
    def lastrowid(self):
        return state(self)["lastrowid"]

    @property
    def statusmessage(self):
        return state(self)["status"]

    @property
    def description(self):
        cols = state(self)["columns"]
        return [(c, None, None, None, None, None, None) for c in cols] if cols else None

    async def close(self):
        self.closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()
        return False
