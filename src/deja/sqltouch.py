"""Which tables and rows one SQL statement reads or writes, using sqlglot (optional dependency).

Returns a list of touches ``{"resource", "kind", "table", "rows", "where", "writes", "lock", "precise"}``:

* ``rows`` is a list of ``{column: value}`` dicts when the statement picks rows by key equality (``id = 1``,
  ``a = 1 AND b = 2``, ``id IN (1, 2)``, ``id = ANY($1)``); then there is one touch per row
  (``orders[id=7]``) and ``precise`` is True.
* otherwise the touch is table-level (``items``) with the condition text in ``where`` (``price > 100``) and
  ``precise`` False. It also carries ``_match(row) -> True | False | None``, which evaluates the condition against
  known column values of one row (None when they don't decide it). The analysis uses it, and the rows a query
  returned, to work out which rows a range query covered; what stays undecided gives "possible" conflicts.

Returns None when sqlglot is not installed or cannot parse the statement; callers fall back to simple rules.
"""

try:
    import sqlglot
    from sqlglot import exp
except ImportError:  # pragma: no cover - optional
    sqlglot = None

from . import codec


class Expr(str):
    """A SQL expression (not a literal value); printed without quotes."""


def available() -> bool:
    return sqlglot is not None


def _param(node, params):
    """Value of a literal / $n placeholder, or None if the node isn't one."""
    if isinstance(node, (exp.Cast, exp.Paren)):  # $1::INTEGER (SQLAlchemy writes these), ($1)
        return _param(node.this, params)
    if isinstance(node, exp.Parameter):
        inner = node.this
        if isinstance(inner, exp.Literal) and not inner.is_string:
            i = int(inner.this) - 1
            if 0 <= i < len(params):
                return True, codec.decode(params[i])
        return False, None
    if isinstance(node, exp.Literal):
        if node.is_string:
            return True, node.this
        try:
            return True, int(node.this)
        except ValueError:
            return True, float(node.this)
    if isinstance(node, exp.Boolean):
        return True, node.this
    if isinstance(node, exp.Null):
        return True, None
    if isinstance(node, exp.Neg):
        ok, v = _param(node.this, params)
        return (True, -v) if ok and isinstance(v, (int, float)) else (False, None)
    return False, None


def _text(node, params):
    """SQL text of an expression with $n placeholders filled in."""
    copy = node.copy()
    for p in list(copy.find_all(exp.Parameter)):
        ok, v = _param(p, params)
        if ok:
            p.replace(exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v) if isinstance(v, (int, float))
                      else exp.Null() if v is None else exp.Literal.string(repr(v)))
    return copy.sql(dialect="postgres")


def _conjuncts(node):
    if isinstance(node, exp.And):
        yield from _conjuncts(node.this)
        yield from _conjuncts(node.expression)
    elif isinstance(node, exp.Paren):
        yield from _conjuncts(node.this)
    else:
        yield node


def _rows(where, table_names, params):
    """(rows, leftover condition text) for a WHERE clause, considering columns of the given table/alias names."""
    if where is None:
        return None, None
    keys, multi, rest = {}, None, []
    for c in _conjuncts(where.this if isinstance(where, exp.Where) else where):
        col = c.this if isinstance(c, (exp.EQ, exp.In)) else None
        if isinstance(col, exp.Column) and (not col.table or col.table in table_names):
            name = col.name
            if isinstance(c, exp.EQ):
                rhs = c.expression
                if isinstance(rhs, exp.Any):  # id = ANY($1)
                    inner = rhs.this.this if isinstance(rhs.this, exp.Paren) else rhs.this
                    ok, v = _param(inner, params)
                    if ok and isinstance(v, (list, tuple)):
                        multi = (name, list(v))
                        continue
                ok, v = _param(rhs, params)
                if ok:
                    keys[name] = v
                    continue
            elif isinstance(c, exp.In) and c.expressions:
                vals = [_param(x, params) for x in c.expressions]
                if all(ok for ok, _ in vals):
                    multi = (name, [v for _, v in vals])
                    continue
        rest.append(_text(c, params))
    leftover = " AND ".join(rest) or None
    if multi:
        return [{**keys, multi[0]: v} for v in multi[1]], leftover
    return ([keys] if keys else None), leftover


def _value(node, row, tables, params):
    """(known, value) of an operand for one row: a column of this table, a literal or a parameter."""
    if isinstance(node, exp.Paren):
        return _value(node.this, row, tables, params)
    if isinstance(node, exp.Column):
        if (node.table and node.table not in tables) or node.name not in row:
            return False, None
        return True, row[node.name]
    return _param(node, params)


def _cmp(op, a, b):
    if a is None or b is None:
        return False  # SQL: a comparison with NULL is never true
    try:
        return op(a, b)
    except TypeError:
        return None


def _eval(node, row, tables, params):
    """Truth of a condition for one row: True, False, or None when the known values don't decide it."""
    import operator as o

    if isinstance(node, exp.Where):
        node = node.this
    if isinstance(node, exp.Paren):
        return _eval(node.this, row, tables, params)
    if isinstance(node, (exp.And, exp.Or)):
        a, b = _eval(node.this, row, tables, params), _eval(node.expression, row, tables, params)
        decisive = isinstance(node, exp.Or)  # OR: one True decides it; AND: one False does
        if a is decisive or b is decisive:
            return decisive
        return (not decisive) if a is not None and b is not None else None
    if isinstance(node, exp.Not):
        v = _eval(node.this, row, tables, params)
        return None if v is None else not v
    ops = {exp.EQ: o.eq, exp.NEQ: o.ne, exp.GT: o.gt, exp.GTE: o.ge, exp.LT: o.lt, exp.LTE: o.le}
    if type(node) in ops:
        (ka, a), (kb, b) = _value(node.this, row, tables, params), _value(node.expression, row, tables, params)
        return _cmp(ops[type(node)], a, b) if ka and kb else None
    if isinstance(node, exp.In) and node.expressions:
        ka, a = _value(node.this, row, tables, params)
        vals = [_value(x, row, tables, params) for x in node.expressions]
        if not ka or not all(k for k, _ in vals):
            return None
        return a is not None and a in [v for _, v in vals]
    if isinstance(node, exp.Between):
        ka, a = _value(node.this, row, tables, params)
        (kl, lo), (kh, hi) = (_value(node.args["low"], row, tables, params),
                              _value(node.args["high"], row, tables, params))
        if not (ka and kl and kh):
            return None
        lo_ok, hi_ok = _cmp(o.ge, a, lo), _cmp(o.le, a, hi)
        return None if lo_ok is None or hi_ok is None else lo_ok and hi_ok
    if isinstance(node, (exp.Column, exp.Boolean)):  # WHERE active / WHERE TRUE
        known, v = _value(node, row, tables, params)
        return bool(v) if known and v is not None else (False if known else None)
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Null):
        ka, a = _value(node.this, row, tables, params)
        return (a is None) if ka else None
    return None


def _matcher(where, tables, params):
    if where is None:
        return None
    return lambda row: _eval(where, row, tables, params)


def _name(t: "exp.Table") -> str:
    return ".".join(p for p in (t.args.get("db") and t.db, t.name) if p)


def _resources(table, rows, where):
    if rows and not where:
        return [(f"{table}[{', '.join(f'{k}={v}' for k, v in r.items())}]", r, True) for r in rows]
    return [(table, None, False)]


def touches(sql: str, params: list):
    if sqlglot is None:
        return None
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return None
    if tree is None:
        return None
    ctes = {c.alias for c in tree.find_all(exp.CTE)}
    out = []

    def full(where_node):
        """The whole condition as text (a range touch shows all of it, key equalities included)."""
        if where_node is None:
            return None
        return _text(where_node.this if isinstance(where_node, exp.Where) else where_node, params)

    def add(table, kind, rows, where, match=None, where_node=None, **extra):
        for res, row, precise in _resources(table, rows, where):
            t = {"resource": res, "kind": kind, "table": table, "rows": [row] if row else None,
                 "where": where if precise or where_node is None else full(where_node), "precise": precise, **extra}
            if not precise and match is not None:
                t["_match"] = match
            out.append(t)

    def read_tables(node, skip=()):
        for t in node.find_all(exp.Table):
            n = _name(t)
            if n and n not in ctes and n not in skip:
                yield t, n

    seen = set()

    def reads(node, skip=()):
        """Each table a SELECT (or subquery / CTE body) reads, filtered by the WHERE of the SELECT it sits in."""
        for t, n in read_tables(node, skip):
            sel = t.parent_select
            where = sel.args.get("where") if sel is not None else None
            lock = bool(sel.args.get("locks")) if sel is not None else False
            names = {t.alias_or_name, n}
            rows, cond = _rows(where, names, params)
            for res, row, precise in _resources(n, rows, cond):
                if (res, cond) not in seen:
                    seen.add((res, cond))
                    out.append({"resource": res, "kind": "read", "table": n, "rows": [row] if row else None,
                                "where": cond if precise else full(where), "precise": precise, "lock": lock})
                    if sel is not None and any(sel.args.get(k) for k in ("joins", "limit", "offset", "having")):
                        out[-1]["partial"] = True  # rows also filtered by a join / LIMIT: "no WHERE" isn't "all"
                    if sel is not None and len(sel.expressions) == 1 and isinstance(sel.expressions[0], exp.Column):
                        out[-1]["column"] = sel.expressions[0].name  # fetchval-style scalar: its value is this column
                    if not precise and where is not None:
                        out[-1]["_match"] = _matcher(where, names, params)

    if isinstance(tree, exp.Insert):
        target = tree.this.this if isinstance(tree.this, exp.Schema) else tree.this
        table = _name(target)
        cols = [c.name for c in tree.this.expressions] if isinstance(tree.this, exp.Schema) else []
        source = tree.expression
        writes, rows = {}, None
        if isinstance(source, exp.Values) and source.expressions:
            first = source.expressions[0]
            vals = first.expressions if isinstance(first, exp.Tuple) else [first]
            for i, v in enumerate(vals):
                ok, val = _param(v, params)
                writes[cols[i] if i < len(cols) else f"col{i + 1}"] = val if ok else Expr(_text(v, params))
            conflict = tree.args.get("conflict")
            key_cols = [c.name for c in (conflict.args.get("conflict_keys") or [])] if conflict is not None else []
            key_cols = [k for k in key_cols if k in writes] or ([cols[0]] if cols else [])
            if key_cols and all(not isinstance(writes[k], Expr) for k in key_cols):
                rows = [{k: writes[k] for k in key_cols}]
        elif source is not None:
            reads(source)
        add(table, "write", rows, None, writes=writes or None,
            upsert=tree.args.get("conflict") is not None, insert=True)
        return out

    if isinstance(tree, exp.Update):
        table = _name(tree.this)
        alias = tree.this.alias_or_name
        writes = {}
        for a in tree.expressions:
            if isinstance(a, exp.EQ) and isinstance(a.this, exp.Column):
                ok, v = _param(a.expression, params)
                writes[a.this.name] = v if ok else Expr(_text(a.expression, params))
        rows, where = _rows(tree.args.get("where"), {alias, table}, params)
        add(table, "write", rows, where, _matcher(tree.args.get("where"), {alias, table}, params),
            tree.args.get("where"), writes=writes)
        reads(tree, skip={table})
        return out

    if isinstance(tree, exp.Delete):
        table = _name(tree.this)
        names = {tree.this.alias_or_name, table}
        rows, where = _rows(tree.args.get("where"), names, params)
        add(table, "write", rows, where, _matcher(tree.args.get("where"), names, params), tree.args.get("where"),
            writes={"(deleted)": True})
        return out

    if isinstance(tree, (exp.Select, exp.Union)) or tree.find(exp.Select):
        reads(tree)
        return out

    if isinstance(tree, exp.Copy) and isinstance(tree.this, exp.Table):  # COPY t FROM STDIN (bulk load) / TO STDOUT
        add(_name(tree.this), "write" if tree.args.get("kind") else "read", None, None,
            **({"writes": {"(bulk copy)": True}} if tree.args.get("kind") else {}))
        return out

    if isinstance(tree, exp.TruncateTable):
        for t in tree.expressions:
            add(_name(t), "write", None, None, writes={"(truncated)": True})
        return out
    return []
