"""Which tables and rows one SQL statement reads or writes, using sqlglot (optional dependency).

Returns a list of touches ``{"resource", "kind", "table", "rows", "where", "writes", "lock", "precise"}``:

* ``rows`` is a list of ``{column: value}`` dicts when the statement picks rows by key equality (``id = 1``,
  ``a = 1 AND b = 2``, ``id IN (1, 2)``, ``id = ANY($1)``); then there is one touch per row
  (``orders[id=7]``) and ``precise`` is True.
* otherwise the touch is table-level (``items``) with the condition text in ``where`` (``price > 100``) and
  ``precise`` False: it may cover rows other requests touched, so conflicts built on it are "possible".

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

    def add(table, kind, rows, where, **extra):
        for res, row, precise in _resources(table, rows, where):
            out.append({"resource": res, "kind": kind, "table": table, "rows": [row] if row else None,
                        "where": where, "precise": precise, **extra})

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
            rows, cond = _rows(where, {t.alias_or_name, n}, params)
            for res, row, precise in _resources(n, rows, cond):
                if (res, cond) not in seen:
                    seen.add((res, cond))
                    out.append({"resource": res, "kind": "read", "table": n, "rows": [row] if row else None,
                                "where": cond, "precise": precise, "lock": lock})

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
            upsert=tree.args.get("conflict") is not None)
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
        add(table, "write", rows, where, writes=writes)
        reads(tree, skip={table})
        return out

    if isinstance(tree, exp.Delete):
        table = _name(tree.this)
        rows, where = _rows(tree.args.get("where"), {tree.this.alias_or_name, table}, params)
        add(table, "write", rows, where, writes={"(deleted)": True})
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
