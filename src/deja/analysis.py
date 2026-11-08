"""Turn a recording into facts a person can read: what each request touched, which race pattern happened, and a
plain-language story built from templates. No LLM; every sentence is traceable to recorded events.

    from deja.analysis import analyze
    result = analyze(header, events)
    result["story"], result["moments"], result["conflicts"], result["touches"]
"""

import re
from collections import defaultdict
from urllib.parse import urlsplit

from . import codec
from . import events as ev
from . import sqltouch
from .sqltouch import Expr as _Expr
from .threads import is_thread_path

# ---------------------------------------------------------------------- SQL -> resources (rule based)
_TABLE = r'([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)?|"[^"]+")'
_KEY = re.compile(r"\b([A-Za-z_]\w*)\s*=\s*(\$\d+|'(?:[^']|'')*'|-?\d+(?:\.\d+)?)", re.I)


def _resolve(token, params):
    if token.startswith("$"):
        i = int(token[1:]) - 1
        if 0 <= i < len(params):
            return codec.decode(params[i])
        return token
    if token.startswith("'"):
        return token[1:-1].replace("''", "'")
    try:
        return int(token)
    except ValueError:
        return float(token)


def _key(where, params):
    m = _KEY.search(where or "")
    return (m.group(1), _resolve(m.group(2), params)) if m else None


def _res(table, key):
    table = table.strip('"')
    return f"{table}[{key[0]}={key[1]}]" if key else table


def _assignments(sets, params):
    out = {}
    for part in re.split(r",(?![^()]*\))", sets):
        if "=" in part:
            col, val = part.split("=", 1)
            val = val.strip()
            m = re.fullmatch(r"\$\d+|'(?:[^']|'')*'|-?\d+(?:\.\d+)?", val)
            if m:
                out[col.strip()] = _resolve(val, params)
            else:  # an expression such as "charged + $2": show it with its parameters filled in
                out[col.strip()] = _Expr(re.sub(r"\$\d+", lambda t: repr(_resolve(t.group(0), params)), val))
    return out


def sql_touches(sql: str, params: list) -> list[dict]:
    """``[{"resource", "kind": "read"|"write", "table", "precise", "where", "writes"}]`` for one statement.

    Uses sqlglot when installed (``pip install deja[sql]``): joins, subqueries, CTEs, IN / ANY lists, upserts.
    Otherwise (or if sqlglot can't parse it) simple rules that understand one table and one ``col = value``.
    """
    parsed = sqltouch.touches(str(sql), params)
    if parsed is not None:
        return parsed
    out = _rule_touches(sql, params)
    for t in out:
        t["table"] = t["resource"].split("[", 1)[0]
        t["precise"] = "[" in t["resource"]
    return out


def _rule_touches(sql, params):
    s = " ".join(str(sql).split())
    low = s.lower()
    m = re.match(rf"select\s+(.+?)\s+from\s+{_TABLE}(.*)$", s, re.I)
    if m and low.startswith("select"):
        cols = [c.strip() for c in re.split(r",(?![^()]*\))", m.group(1))]
        return [{"resource": _res(m.group(2), _key(m.group(3), params)), "kind": "read", "columns": cols}]
    m = re.match(rf"update\s+{_TABLE}\s+set\s+(.+?)(?:\s+where\s+(.+))?$", s, re.I)
    if m:
        return [{"resource": _res(m.group(1), _key(m.group(3), params)), "kind": "write",
                 "writes": _assignments(m.group(2), params)}]
    m = re.match(rf"insert\s+into\s+{_TABLE}\s*(?:\(([^)]*)\))?\s*values\s*\((.*?)\)", s, re.I)
    if m:
        cols = [c.strip().strip('"') for c in (m.group(2) or "").split(",") if c.strip()]
        vals = [v.strip() for v in re.split(r",(?![^()]*\))", m.group(3))]
        resolved = [_resolve(v, params) if re.fullmatch(r"\$\d+|'(?:[^']|'')*'|-?\d+(?:\.\d+)?", v) else v for v in vals]
        key = (cols[0], resolved[0]) if cols and resolved else (("id", resolved[0]) if resolved else None)
        return [{"resource": _res(m.group(1), key), "kind": "write",
                 "writes": dict(zip(cols, resolved)) if cols else {}}]
    m = re.match(rf"delete\s+from\s+{_TABLE}(?:\s+where\s+(.+))?$", s, re.I)
    if m:
        return [{"resource": _res(m.group(1), _key(m.group(2), params)), "kind": "write", "writes": {"(deleted)": True}}]
    m = re.match(rf"truncate\s+(?:table\s+)?{_TABLE}", s, re.I)
    if m:
        return [{"resource": _res(m.group(1), None), "kind": "write", "writes": {"(truncated)": True}}]
    return []


REDIS_READ = {"GET", "MGET", "HGET", "HGETALL", "HMGET", "HEXISTS", "HLEN", "EXISTS", "SMEMBERS", "SISMEMBER", "SCARD",
              "LRANGE", "LLEN", "LINDEX", "ZRANGE", "ZSCORE", "ZCARD", "TTL", "PTTL", "STRLEN", "TYPE", "WATCH"}
_REDIS_ALL_KEYS = {"MGET", "EXISTS", "DEL", "UNLINK", "WATCH", "TOUCH", "SUNION", "SINTER", "SDIFF"}


_AWS_READ = ("Get", "Head", "List", "Describe", "Query", "Scan", "BatchGet")


def _aws_key(key):
    """DynamoDB key ``{"id": {"S": "1"}}`` as ``id=1``."""
    parts = []
    for name, typed in (key or {}).items():
        v = next(iter(typed.values())) if isinstance(typed, dict) and typed else typed
        parts.append(f"{name}={v}")
    return ", ".join(parts)


def _aws_touches(d):
    service, operation, p = d["service"], d["operation"], d.get("params") or {}
    kind = "read" if operation.startswith(_AWS_READ) else "write"
    if service == "s3" and p.get("Bucket"):
        res = f"s3 {p['Bucket']}/{p['Key']}" if p.get("Key") else f"s3 {p['Bucket']}"
        if operation.startswith("List") and p.get("Prefix") is not None:
            res = f"s3 {p['Bucket']}/{p['Prefix']}*"
    elif service == "dynamodb" and p.get("TableName"):
        key = p.get("Key")
        res = f"dynamodb {p['TableName']}[{_aws_key(key)}]" if isinstance(key, dict) else f"dynamodb {p['TableName']}"
        if p.get("ConditionExpression") and kind == "write":  # a conditional write checks and writes in one step
            return [{"resource": res, "kind": "write", "writes": {"(conditional)": True}}]
    elif p.get("QueueUrl"):
        res = f"{service} {str(p['QueueUrl']).rsplit('/', 1)[-1]}"
        kind = "write"  # receiving hides the message from others: it changes the queue
    else:
        res = f"{service} {operation}"
    t = {"resource": res, "kind": kind}
    if kind == "write":
        t["writes"] = {f"({operation})": True}
    return [t]


def _redis_touches(args):
    """Touches of one Redis command: every key it names (MGET / DEL / MSET / EVAL keys), read or written."""
    if not args:
        return []
    cmd, rest = str(args[0]).upper(), [str(a) for a in args[1:]]
    if not rest:
        return []
    kind = "read" if cmd in REDIS_READ else "write"
    if cmd in _REDIS_ALL_KEYS:
        keys, values = rest, [None] * len(rest)
    elif cmd in ("MSET", "MSETNX"):
        keys, values = rest[0::2], rest[1::2]
    elif cmd in ("EVAL", "EVALSHA", "FCALL"):  # a script runs atomically on the server: one write of its keys
        try:
            n = int(rest[1])
        except (IndexError, ValueError):
            return []
        keys, values = rest[2:2 + n], [None] * n
    else:
        keys, values = rest[:1], rest[1:2] or [None]
    out = []
    for k, v in zip(keys, values):
        t = {"resource": f"redis:{k}", "kind": kind}
        if kind == "write":
            t["writes"] = {"value": v} if v is not None else {f"({cmd.lower()})": True}
        out.append(t)
    return out


def call_touches(io: dict, kinds: dict | None = None) -> list[dict]:
    op, d = io["op"], io.get("detail") or {}
    if op.startswith(("pg.", "sql.")) and "sql" in d:
        return sql_touches(d["sql"], d.get("params") or [])
    if op == "http.send" and "url" in d:
        import fnmatch

        u = urlsplit(d["url"])
        res = f"{d['method']} {u.netloc}{u.path}"
        kind = next((v for k, v in (kinds or {}).items() if fnmatch.fnmatchcase(res, k)), None)
        if kind == "ignore":
            return []
        return [{"resource": res, "kind": kind or ("read" if d["method"] in ("GET", "HEAD") else "write")}]
    if op == "redis.cmd" and d.get("args"):
        return _redis_touches(d["args"])
    if op == "redis.pipeline" and d.get("commands"):  # MULTI / EXEC: the server runs them as one step
        out = []
        for c in d["commands"]:
            out += _redis_touches(c)
        return out
    if op == "aws.call" and d.get("operation"):
        return _aws_touches(d)
    if op in ("grpc.call", "grpc.stream") and d.get("method"):
        import fnmatch

        res = f"grpc {d['method']}"
        kind = next((v for k, v in (kinds or {}).items() if fnmatch.fnmatchcase(res, k)), None)
        if kind == "ignore":
            return []
        verb = d["method"].rsplit("/", 1)[-1]
        guess = "read" if verb.startswith(("Get", "List", "Read", "Find", "Search", "Describe", "Check", "Watch")) else "write"
        return [{"resource": res, "kind": kind or guess}]
    if op == "executor" and d.get("function"):
        return [{"resource": f"worker:{d['function']}", "kind": "call"}]
    return []


# ---------------------------------------------------------------------- helpers
def short(value, limit=60):
    """One-line human form of an encoded result."""
    if isinstance(value, dict) and value.get("__t") == "aws":
        return "AWS response"
    if isinstance(value, dict) and value.get("__t") == "grpc":
        return "gRPC response"
    if isinstance(value, dict) and value.get("__t") in ("httpx", "aiohttp"):
        return f"HTTP {value.get('status')}"
    if isinstance(value, dict) and value.get("__t") == "exc":
        return "raised " + value.get("type", "").split(":")[-1]
    try:
        v = codec.decode(value)
    except Exception:
        v = value
    if isinstance(v, list) and len(v) == 1 and isinstance(v[0], codec.FakeRecord):
        v = v[0]  # one row: show the row itself
    if isinstance(v, codec.FakeRecord):
        text = ", ".join(f"{k}={x!r}" for k, x in v.items())
    elif isinstance(v, list) and v and all(isinstance(r, codec.FakeRecord) for r in v):
        text = f"{len(v)} rows" if len(v) > 3 else "; ".join(", ".join(f"{k}={x!r}" for k, x in r.items()) for r in v)
    elif isinstance(v, list) and len(v) > 3:
        text = f"{len(v)} rows"
    else:
        text = repr(v)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _root(path):
    return path.split(".", 1)[0] if path else None


def _fmt_writes(w):
    if w and list(w) == ["value"]:  # app-reported write (deja.touch): just the new value
        return repr(w["value"])
    parts = []
    for k, v in (w or {}).items():
        if k.startswith("("):
            parts.append(k.strip("()"))
        elif isinstance(v, _Expr):
            parts.append(f"{k} = {v}")
        else:
            parts.append(f"{k} = {v!r}")
    return ", ".join(parts) or "a new value"


# ---------------------------------------------------------------------- main entry
def analyze(header: dict, events: list) -> dict:
    labels = dict(header.get("labels") or {})
    t_base = None
    now = None
    seq = 0
    requests: dict[int, dict] = {}
    ios: list[dict] = []
    open_io: dict[str, dict] = {}
    steps: list[dict] = []
    touches: list[dict] = []
    messages: dict[str, int] = defaultdict(int)  # WebSocket connection -> messages received so far
    tx: dict[str, list] = {}  # task -> stack of the writes made in each open transaction / savepoint
    iso: dict[str, dict] = {}  # task -> {"level", "snap"} of its open transaction (REPEATABLE READ / SERIALIZABLE)
    held: dict[str, list] = {}  # task -> rows its open transaction wrote or locked (for deadlocks)
    deadlocks: list = []

    def stamp(t):
        nonlocal t_base, now
        if t is None:
            return now
        if t_base is None:
            t_base = t
        now = (t - t_base) / 1e6  # ms since the first timestamp
        return now

    for e in events:
        seq += 1
        code = e[0]
        if code == ev.BATCH:
            stamp(e[2] if len(e) > 2 else None)
        elif code == ev.REQUEST:
            t = stamp(e[7] if len(e) > 7 else None)
            body = e[6]
            requests[e[1]] = {"id": f"q{e[1]}", "rid": e[1], "method": e[3], "path": e[4],
                              "body": body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else body,
                              "start": t, "end": None, "status": None, "response": None, "seq": seq,
                              "kind": "websocket" if e[3] == "WS" else "http", "failed": False}
        elif code == ev.RESPONSE and e[1] in requests:
            t = stamp(e[4] if len(e) > 4 else None)
            body = e[3] if len(e) > 3 else b""
            r = requests[e[1]]
            r.update(end=t, status=e[2],
                     response=body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else body,
                     failed=(e[2] == 1011) if r["method"] == "WS" else e[2] >= 500)
        elif code == ev.STEP and e[1] and e[1].startswith("q"):
            steps.append({"seq": seq, "t": now, "task": e[1], "req": _root(e[1])})
        elif code == ev.IO_START and e[1] and e[1].startswith("q"):
            open_io[e[1]] = {"t0": stamp(e[3] if len(e) > 3 else None), "seq0": seq}
        elif code == ev.IO_END and e[1] and e[1].startswith("q"):
            start = open_io.pop(e[1], {"t0": now, "seq0": seq})
            t1 = stamp(e[6] if len(e) > 6 else None)
            io = {"seq": seq, "seq0": start["seq0"], "task": e[1], "req": _root(e[1]), "op": e[2], "t0": start["t0"],
                  "t1": t1, "ok": e[4] == "ok", "result": short(e[5]), "raw": e[5],
                  "detail": e[7] if len(e) > 7 else None}
            io["touches"] = call_touches(io, header.get("call_kinds"))
            ios.append(io)
            try:
                decoded = codec.decode(e[5])
            except Exception:
                decoded = e[5]
            if e[2] == "ws.receive" and isinstance(decoded, dict) and decoded.get("type") == "websocket.receive":
                messages[io["req"]] += 1
            both = any(x["kind"] == "write" for x in io["touches"]) and any(x["kind"] == "read" for x in io["touches"])
            inserted = _inserted(decoded)
            for tch in io["touches"]:
                if tch["kind"] == "write" and tch.get("insert") and inserted == 0:
                    continue  # INSERT 0 0: its guard found a row, nothing was written
                value = decoded if tch["kind"] == "read" else None
                if tch.get("guard") and inserted is not None:  # WHERE NOT EXISTS (...): inserted iff it found nothing
                    value = [] if inserted else True
                # a statement that reads and then writes (a guarded insert, UPDATE ... FROM) reads when it starts:
                # two that overlap in the database both read before either wrote
                at_start = both and tch["kind"] == "read"
                row = {**tch, "seq": io["seq0"] if at_start else seq, "t": io["t0"] if at_start else t1,
                       "req": io["req"], "task": e[1], "op": e[2], "value": value, "_result": decoded,
                       "result": io["result"], "source": "recorded call"}
                if not e[2].startswith(("pg.", "sql.")):  # an HTTP endpoint, an S3 object, a gRPC method: one thing
                    row.setdefault("precise", True)
                level = iso.get(e[1])
                if level is not None and e[2].startswith(("pg.", "sql.")):
                    if level["snap"] is None:
                        level["snap"] = (io["seq0"], io["t0"])  # the snapshot: taken by the first statement
                    if tch["kind"] == "read":  # sees the database as of the snapshot, whatever committed since
                        row["seq"], row["t"] = level["snap"]
                    row["serializable"] = level["level"] == "serializable"
                if e[4] == "ok" and tx.get(e[1]) and (tch["kind"] == "write" or tch.get("lock")):
                    held.setdefault(e[1], []).append((tch["resource"], seq, t1))
                if tch["kind"] == "write" and tx.get(e[1]):
                    tx[e[1]][-1].append(row)  # inside a transaction: other requests see it once it commits
                else:
                    touches.append(row)
            if e[2].endswith(".tx.start") and e[4] == "ok":
                if not tx.get(e[1]):
                    level = (io.get("detail") or {}).get("isolation") or "read_committed"
                    if level in ("repeatable_read", "serializable"):
                        iso[e[1]] = {"level": level, "snap": None}
                tx.setdefault(e[1], []).append([])
            elif e[2].endswith((".tx.commit", ".tx.rollback")) and tx.get(e[1]):
                done = tx[e[1]].pop()
                if e[2].endswith(".tx.rollback") or e[4] != "ok":
                    pass  # rolled back: those writes never happened
                elif tx[e[1]]:
                    tx[e[1]][-1].extend(done)  # a savepoint released into the transaction around it
                else:
                    touches.extend({**w, "seq": seq, "t": t1, "committed": True} for w in done)
                if not tx[e[1]]:
                    iso.pop(e[1], None)
                    held.pop(e[1], None)
            if e[4] == "err" and "deadlock" in str((io.get("raw") or {}).get("type", "")).lower():
                deadlocks.append({"io": io, "held": {k: list(v) for k, v in held.items() if k != e[1]},
                                  "mine": list(held.get(e[1], []))})
        elif code == ev.TOUCH and e[1] and e[1].startswith("q"):
            t = stamp(e[5] if len(e) > 5 else None)
            touches.append({"resource": e[2], "kind": e[3], "seq": seq, "t": t, "req": _root(e[1]), "task": e[1],
                            "op": "deja.touch", "value": codec.decode(e[4]),
                            "writes": {"value": codec.decode(e[4])} if e[3] == "write" else None,
                            "source": "reported by the app", "message": messages.get(_root(e[1])) or None,
                            "thread": is_thread_path(e[1])})

    for stack in tx.values():  # still open when the recording ends: not visible to anyone yet
        stack.clear()
    touches.sort(key=lambda t: t["seq"])
    touches = _narrow(touches)
    conflicts = _conflicts(touches)
    skew = [c for c in _write_skew(touches, conflicts)  # a range check-then-act already tells this one
            if not any(r.get("range") and {r["a"], r["b"]} == {c["a"], c["b"]} and {c["x"], c["y"]} <= set(r["rows"])
                       for r in conflicts)]
    conflicts = skew + _torn_reads(touches) + _deadlocks(deadlocks, ios) + conflicts
    order = {"deadlock": 0, "lost-update": 1, "check-then-act": 2, "write-skew": 3, "torn-read": 4, "stale-read": 5}
    conflicts.sort(key=lambda c: (c["confidence"] != "certain", order[c["pattern"]], c["t0"] or 0))
    names = _names(conflicts, requests)
    for c in conflicts:
        c["names"] = {r: names.get(r, r) for r in (c["a"], c["b"])}
    story = _story(conflicts, requests, names, header, labels)
    moments = _moments(conflicts, touches, names, labels, requests)
    for c in conflicts:  # every conflict explains itself; the top-level story and moments are the first one's
        c["story"] = _story([c], requests, names, {}, labels)
        c["moments"] = _moments([c], touches, names, labels, requests)
    return {
        "requests": [requests[k] for k in sorted(requests)],
        "steps": steps, "ios": [{k: v for k, v in io.items() if k != "raw"} for io in ios],
        "touches": [{k: (v if k != "value" else short(codec.encode(v))) for k, v in t.items()
                     if k[0] != "_" and k != "only"} for t in touches],
        "conflicts": conflicts, "story": story, "moments": moments, "names": names, "labels": labels,
        "reason": header.get("reason"), "duration_ms": now or 0,
    }


def _inserted(result):
    """Rows an INSERT wrote, from its result: the status string (``INSERT 0 1``) or the rows RETURNING gave."""
    if isinstance(result, str):
        m = re.fullmatch(r"INSERT \d+ (\d+)", result.strip())
        return int(m.group(1)) if m else None
    if isinstance(result, list):
        return len(result)
    return None


def _absent(v):
    """'Nothing found': None, False, or an empty result. Zero is a value (a counter at 0), not absence."""
    if v is None or v is False:
        return True
    return isinstance(v, (list, tuple, str, dict)) and len(v) == 0


def _record_dict(v):
    if isinstance(v, list) and len(v) == 1:
        v = v[0]
    return dict(v.items()) if isinstance(v, codec.FakeRecord) else None


def _row_res(table, cols, row):
    return f"{table}[{', '.join(f'{k}={row[k]}' for k in cols)}]"


def _known_values(touches):
    """Row resource -> [(seq, {column: value})]: what the recording shows about each row, from the rows a query
    returned and the values a write set."""
    known = defaultdict(list)
    for t in touches:
        if not t.get("precise") or t.get("source") != "recorded call":
            continue
        if t["kind"] == "read":
            v = t.get("value")
            d = _record_dict(v)
            if d is None and t.get("column") and not isinstance(v, (list, dict, codec.FakeRecord)):
                d = {t["column"]: v}
            kind = "read"
        else:
            d = {k: v for k, v in (t.get("writes") or {}).items() if k[0] != "(" and not isinstance(v, _Expr)}
            kind = "insert" if t.get("insert") else "write"
        if d is not None and t.get("rows"):
            d = {**t["rows"][0], **d}
        known[t["resource"]].append((t["seq"], kind, d or {}))
    return known


def _values_at(known, res, seq):
    """Column values of one row as of ``seq``: what was known before it; else what the next fact after it shows,
    if that fact is a read (nothing changed the row in between) or the insert that created it (would this query
    have covered the row another request was about to add?). A later update says nothing about the earlier value."""
    merged = {}
    for s, kind, d in known.get(res) or []:
        if s < seq:
            merged.update(d)
    if merged:
        return merged
    nxt = next(((kind, d) for s, kind, d in sorted(known.get(res) or [], key=lambda x: x[0]) if s > seq), None)
    return nxt[1] if nxt and nxt[0] in ("read", "insert") else {}


def _narrow(touches):
    """Resolve reads and writes that covered many rows into the specific rows they covered.

    * A range query (``WHERE price > 100``) is checked against every row other touches name: the rows it returned
      (when the result has the key columns) decide it; otherwise its condition is evaluated on the row's recorded
      values. Covered rows become exact touches; excluded rows drop out; only undecided rows stay "possible".
    * A whole-container read of tracked in-memory state (iterating, ``len()``) is matched against each key the
      other touches name, using the keys it saw.
    """
    rows_of, keysets = defaultdict(set), defaultdict(list)
    app_rows = set()
    for t in touches:
        if t["kind"] not in ("read", "write"):
            continue
        if t.get("source") == "reported by the app":
            if not t["resource"].endswith("*]"):
                app_rows.add(t["resource"])
        elif t.get("precise") and t.get("table") and t.get("rows"):
            rows_of[t["table"]].add(t["resource"])
            cols = tuple(t["rows"][0])
            if cols not in keysets[t["table"]]:
                keysets[t["table"]].append(cols)
    known = _known_values(touches)
    out = []
    for t in touches:
        out.append(t)
        if t["kind"] not in ("read", "write"):
            continue
        if t.get("insert") and t.get("precise") and t.get("writes"):
            w = {k: v for k, v in t["writes"].items() if k[0] != "(" and not isinstance(v, _Expr)}
            for cols in keysets.get(t["table"], []):
                alias = _row_res(t["table"], cols, w) if all(c in w for c in cols) else None
                if alias and alias != t["resource"] and alias in rows_of[t["table"]]:
                    out.append({**t, "resource": alias, "rows": [{c: w[c] for c in cols}], "derived": True})
        if t.get("source") == "reported by the app":
            if t["resource"].endswith("*]"):
                prefix, snap = t["resource"][:-2], t.get("value")
                keys = sorted(r for r in app_rows if r.startswith(prefix))
                if keys:
                    t["expanded"] = True
                for r in keys:
                    k = r[len(prefix):-1]
                    seen = any(str(x) == k for x in snap) if isinstance(snap, list) else snap
                    out.append({**t, "resource": r, "value": seen, "derived": True, "expanded": False})
            continue
        if t.get("precise") or not t.get("table") or "where" not in t or not rows_of.get(t["table"]):
            continue
        table = t["table"]
        result = t.get("_result")
        recs = result if isinstance(result, list) and all(isinstance(x, codec.FakeRecord) for x in result) else None
        covered = None
        if recs is not None:
            for cols in keysets[table]:
                if all(all(c in r for c in cols) for r in recs):
                    covered = {_row_res(table, cols, r): r for r in recs}
                    break
        match = t.get("_match")
        everything = t["where"] is None and match is None and not t.get("partial")  # no WHERE: every row
        cond = f"{table}" + (f" WHERE {t['where']}" if t.get("where") else "")
        possible, missing = set(), set()
        for r in sorted(rows_of[table]):
            if covered is not None and r in covered:
                decided, value = True, covered[r]
            else:
                if everything:
                    decided = True
                elif match is not None:
                    decided = match(_values_at(known, r, t["seq"]),
                                    lambda table, seq=t["seq"]: [_values_at(known, x, seq) for x in
                                                                 sorted(rows_of.get(table, ()))])
                else:
                    decided = None
                # not among the rows returned: the row was not there (yet) when the query ran
                value = ([] if covered is not None else t.get("value")) if t["kind"] == "read" else None
            if decided is None:
                possible.add(r)
            elif decided:
                if covered is not None and r not in covered:
                    missing.add(r)  # matched the condition but wasn't returned: the row did not exist yet
                out.append({**{k: v for k, v in t.items() if k != "_match"}, "resource": r, "precise": True,
                            "value": value, "derived": True, "narrowed": cond})
        t["expanded"], t["only"] = True, possible
        if missing and t["kind"] == "read":
            t["_missing"], t["range"] = missing, cond
    return out


def _range_conflicts(touches):
    """Check-then-act over a range: two requests each looked for rows matching a condition, found none, and each
    then inserted a row matching it (two bookings of one room for overlapping days). Each row is different, so the
    per-row analysis sees no repeated write; the conflict is on the range itself."""
    reads = sorted((t for t in touches if t.get("_missing")), key=lambda t: t["seq"])
    writes = [t for t in touches if t["kind"] == "write" and t.get("precise")]
    out = []
    for i, ta in enumerate(reads):
        for tb in reads[i + 1:]:
            if tb["req"] == ta["req"] or not (ta["range"] == tb["range"] or ta["_missing"] & tb["_missing"]):
                continue
            wa = next((w for w in writes if w["req"] == ta["req"] and w["resource"] in ta["_missing"]
                       and w["seq"] > ta["seq"]), None)
            wb = next((w for w in writes if w["req"] == tb["req"] and w["resource"] in tb["_missing"]
                       and w["seq"] > tb["seq"]), None)
            if not (wa and wb and tb["seq"] < wa["seq"]):
                continue
            involved = [ta, tb, wa, wb]
            out.append({"pattern": "check-then-act", "resource": ta["range"], "a": ta["req"], "b": tb["req"],
                        "confidence": "certain", "broad": None, "narrowed": None, "range": True,
                        "read_a": "no rows", "read_b": "no rows",
                        "write_a": f"{wa['resource']}: {_fmt_writes(wa.get('writes'))}",
                        "write_b": f"{wb['resource']}: {_fmt_writes(wb.get('writes'))}",
                        "last_writer": tb["req"] if wb["seq"] > wa["seq"] else ta["req"],
                        "seqs": [t["seq"] for t in involved],
                        "rows": sorted({wa["resource"], wb["resource"]}),
                        "t0": min(t["t"] for t in involved if t["t"] is not None),
                        "t1": max(t["t"] for t in involved if t["t"] is not None)})
    return out


def _conflicts(touches):
    """Pairs of requests whose reads and writes of one resource interleaved.

    A touch of a specific row (``orders[id=7]``), a Redis key or an app-reported resource is exact. A query that
    covered a whole table or a range (``WHERE price > 100``) may include rows other requests touched; it is matched
    against every row of that table and the conflict is marked ``confidence: "possible"``.
    """
    by_res = defaultdict(list)
    broad = defaultdict(list)  # table -> table-level touches
    for t in touches:
        if t["kind"] not in ("read", "write"):
            continue
        if t.get("expanded") and not t.get("only"):  # fully resolved into row touches by _narrow
            continue
        exact = t.get("precise") or t["resource"].startswith("redis:") or t.get("source") == "reported by the app"
        if exact:
            by_res[t["resource"]].append(t)
        elif t.get("table"):
            broad[t["table"]].append({**t, "broad": True})
    for table, acc in broad.items():
        rows = [r for r in by_res if r.split("[", 1)[0] == table]
        for t in acc:
            for r in rows:
                if t.get("only") is None or r in t["only"]:
                    by_res[r].append(t)
            if not rows:
                by_res[table].append(t)
    out, seen = [], set()
    for res, acc in by_res.items():
        acc.sort(key=lambda t: t["seq"])
        reqs = []
        for t in acc:
            if t["req"] not in reqs:
                reqs.append(t["req"])
        for i, a in enumerate(reqs):
            for b in reqs[i + 1:]:
                ra = [t for t in acc if t["req"] == a and t["kind"] == "read"]
                rb = [t for t in acc if t["req"] == b and t["kind"] == "read"]
                wa = [t for t in acc if t["req"] == a and t["kind"] == "write"]
                wb = [t for t in acc if t["req"] == b and t["kind"] == "write"]
                if not (ra and rb):
                    continue
                if all(t.get("serializable") for t in ra + rb + wa + wb):
                    continue  # both SERIALIZABLE and both committed: Postgres would have aborted one otherwise
                r_a, r_b = ra[0], rb[0]
                first_wa = next((w for w in wa if w["seq"] > r_a["seq"]), None)
                if r_a["seq"] < r_b["seq"] and (first_wa is None or r_b["seq"] < first_wa["seq"]):
                    later_wb = next((w for w in wb if w["seq"] > r_b["seq"]), None)
                    involved = [r_a, r_b] + [w for w in (first_wa, later_wb) if w]
                    possible = any(t.get("broad") for t in involved)
                    key = (res.split("[", 1)[0], a, b) if possible else (res, a, b)
                    if key in seen:
                        continue
                    seen.add(key)
                    if first_wa and later_wb:
                        pattern = "check-then-act" if _absent(r_a["value"]) and _absent(r_b["value"]) else "lost-update"
                    elif first_wa or later_wb:
                        pattern = "stale-read"
                    else:
                        continue
                    out.append({"pattern": pattern, "resource": res, "a": a, "b": b,
                                "confidence": "possible" if possible else "certain",
                                "broad": next((f"{t['table']}" + (f" WHERE {t['where']}" if t.get("where") else "")
                                               for t in involved if t.get("broad")), None),
                                "narrowed": next((t["narrowed"] for t in involved if t.get("narrowed")), None),
                                "read_a": short(codec.encode(r_a["value"])), "read_b": short(codec.encode(r_b["value"])),
                                "write_a": _fmt_writes(first_wa.get("writes")) if first_wa else None,
                                "write_b": _fmt_writes(later_wb.get("writes")) if later_wb else None,
                                "last_writer": (b if later_wb and (not first_wa or later_wb["seq"] > first_wa["seq"])
                                                else a),
                                "seqs": [t["seq"] for t in involved],
                                "t0": min(t["t"] for t in involved if t["t"] is not None),
                                "t1": max(t["t"] for t in involved if t["t"] is not None)})
    ranged = _range_conflicts(touches)
    for rc in ranged:  # the rows it inserted are explained by the range conflict; drop their per-row echoes
        out = [c for c in out if not ({c["a"], c["b"]} == {rc["a"], rc["b"]} and c["resource"] in rc["rows"])]
    out += ranged
    order = {"lost-update": 0, "check-then-act": 1, "stale-read": 2}
    out.sort(key=lambda c: (c["confidence"] != "certain", order[c["pattern"]], c["t0"] or 0))
    return out


def _exact(touches):
    """Touches of one specific thing (a row, a key, in-memory state), not ranges still undecided."""
    out = []
    for t in touches:
        if t["kind"] not in ("read", "write") or (t.get("expanded") and not t.get("only")):
            continue
        if t.get("precise") or t["resource"].startswith("redis:") or t.get("source") == "reported by the app":
            if not t["resource"].endswith("*]"):
                out.append(t)
    return out


def _by_req(touches):
    reads, writes = defaultdict(dict), defaultdict(dict)
    for t in sorted(touches, key=lambda t: t["seq"]):
        (reads if t["kind"] == "read" else writes)[t["req"]].setdefault(t["resource"], t)
    return reads, writes


def _conflict(pattern, resource, a, b, involved, **fields):
    return {"pattern": pattern, "resource": resource, "a": a, "b": b, "confidence": "certain", "broad": None,
            "narrowed": None, "read_a": None, "read_b": None, "write_a": None, "write_b": None, "last_writer": b,
            "seqs": [t["seq"] for t in involved], "t0": min((t["t"] for t in involved if t["t"] is not None), default=0),
            "t1": max((t["t"] for t in involved if t["t"] is not None), default=0), **fields}


def _write_skew(touches, conflicts):
    """Each request read something the other then changed, and they changed *different* things: A read X and Y and
    wrote X; B read X and Y and wrote Y, each before seeing the other's write. No single row was overwritten, yet
    together they can break a rule about X and Y (at least one doctor on call). Removes the stale-read echoes."""
    reads, writes = _by_req(_exact(touches))
    out, pairs = [], set()
    reqs = sorted(set(reads) | set(writes))
    for i, a in enumerate(reqs):
        for b in reqs[i + 1:]:
            for x, wa in writes[a].items():
                rb = reads[b].get(x)
                if x in writes[b] or rb is None or rb["seq"] >= wa["seq"]:
                    continue
                for y, wb in writes[b].items():
                    ra = reads[a].get(y)
                    if y == x or y in writes[a] or ra is None or ra["seq"] >= wb["seq"]:
                        continue
                    if (a, b) in pairs:
                        break
                    pairs.add((a, b))
                    involved = sorted([ra, rb, wa, wb], key=lambda t: t["seq"])
                    out.append(_conflict("write-skew", f"{x} and {y}", a, b, involved, x=x, y=y,
                                         read_a=f"{y} = {short(codec.encode(ra['value']))}",
                                         read_b=f"{x} = {short(codec.encode(rb['value']))}",
                                         write_a=f"{x}: {_fmt_writes(wa.get('writes'))}",
                                         write_b=f"{y}: {_fmt_writes(wb.get('writes'))}"))
    for c in out:  # the same story told twice as two stale reads
        conflicts[:] = [d for d in conflicts if not (d["pattern"] == "stale-read" and {d["a"], d["b"]} == {c["a"], c["b"]}
                                                    and d["resource"] in (c["x"], c["y"]))]
    return out


def _torn_reads(touches):
    """A request changed X and then Y as one update; another request read X after the change and Y before it, so it
    saw the update half done. (Writes in one database transaction commit together, so they never tear.)"""
    exact = sorted(_exact(touches), key=lambda t: t["seq"])
    by_req = defaultdict(list)
    for t in exact:
        by_req[t["req"]].append(t)
    out, seen = [], set()
    for a, ts in by_req.items():
        ws = [t for t in ts if t["kind"] == "write"]
        for i, w1 in enumerate(ws):
            for w2 in ws[i + 1:]:
                if w2["resource"] == w1["resource"] or w2["seq"] <= w1["seq"]:
                    continue
                for b, other in by_req.items():
                    if b == a or (a, b) in seen:
                        continue
                    r1 = next((t for t in other if t["kind"] == "read" and t["resource"] == w1["resource"]
                               and w1["seq"] < t["seq"]), None)
                    r2 = next((t for t in other if t["kind"] == "read" and t["resource"] == w2["resource"]
                               and t["seq"] < w2["seq"]), None)
                    if r1 is None or r2 is None or not (r1["seq"] < w2["seq"] or w1["seq"] < r2["seq"]):
                        continue
                    seen.add((a, b))
                    x, y = w1["resource"], w2["resource"]
                    involved = sorted([w1, r1, r2, w2], key=lambda t: t["seq"])
                    out.append(_conflict("torn-read", f"{x} and {y}", a, b, involved, x=x, y=y,
                                         read_b=f"{x} = {short(codec.encode(r1['value']))}, "
                                                f"{y} = {short(codec.encode(r2['value']))}",
                                         write_a=f"{x}: {_fmt_writes(w1.get('writes'))}, then "
                                                 f"{y}: {_fmt_writes(w2.get('writes'))}", last_writer=a))
    return out


def _deadlocks(found, ios):
    """Postgres reported a deadlock: the stopped transaction held some rows and waited for one another request's
    open transaction held, while that request waited for one of the first's."""
    out = []
    for d in found:
        io = d["io"]
        waited = {t["resource"] for t in io["touches"]}
        for task, rows in d["held"].items():
            hit = next((r for r in rows if r[0] in waited), None)
            if hit is None:
                continue
            # what the other request was waiting for: its statement that overlapped the stopped one and touched a
            # row the stopped transaction held (it often finishes first: the deadlock error released those locks)
            mine = {r[0] for r in d["mine"]}
            want = next((t["resource"] for x in ios if x["task"] == task and x["seq0"] < io["seq"]
                         and x["seq"] > io["seq0"] for t in x["touches"] if t["resource"] in mine), None)
            a, b = _root(task), io["req"]
            involved = [{"seq": hit[1], "t": hit[2]}, {"seq": io["seq0"], "t": io["t0"]},
                        {"seq": io["seq"], "t": io["t1"]}]
            out.append(_conflict("deadlock", f"{hit[0]} and {want or 'a row'}", a, b, involved, x=hit[0],
                                 y=want or "a row", write_a=f"held {hit[0]}", write_b=f"waited for {hit[0]}",
                                 last_writer=b))
            break
    return out


def _names(conflicts, requests):
    names, letters = {}, iter("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    for c in conflicts:
        for r in (c["a"], c["b"]):
            if r not in names:
                names[r] = next(letters)
    return names


def _label(text, labels):
    return labels.get(text, text)


def _req_label(req, labels):
    if req.get("method") == "WS":
        return _label(f"WebSocket {req['path'].split('?')[0]}", labels)
    return _label(f"{req['method']} {req['path'].split('?')[0]}", labels)


def _noun(req):
    return "connection" if req.get("method") == "WS" else "request"


def _story(conflicts, requests, names, header, labels):
    reason = header.get("reason")
    if not conflicts:
        base = "Déjà found no conflicting reads and writes between requests in this recording."
        return base + (f" It was saved because: {reason}." if reason else "")
    c = conflicts[0]
    ra = requests.get(int(c["a"][1:]), {})
    rb = requests.get(int(c["b"][1:]), {})
    A, B, res = names[c["a"]], names[c["b"]], _label(c["resource"], labels)
    what_a, what_b = _req_label(ra, labels) if ra else c["a"], _req_label(rb, labels) if rb else c["b"]
    if what_a == what_b:
        noun = _noun(ra)
        who = (f"Two connections to {ra['path'].split('?')[0]} ({A} and {B})" if noun == "connection"
               else f"Two {what_a} requests ({A} and {B})")
    else:
        who = f"{_noun(ra).capitalize()} {A} ({what_a}) and {_noun(rb)} {B} ({what_b})"
    if c["pattern"] == "write-skew":
        x, y = _label(c["x"], labels), _label(c["y"], labels)
        text = (f"{who} ran at the same time. {A} read {y} ({c['read_a']}) and then changed {x} "
                f"({c['write_a']}); {B} read {x} ({c['read_b']}) and then changed {y} ({c['write_b']}). Each "
                f"decided from a value the other was about to change, and neither saw the other's write. No single "
                f"value was overwritten, but together the two changes can break a rule that holds for {x} and {y} "
                f"together. This is write skew.")
    elif c["pattern"] == "torn-read":
        x, y = _label(c["x"], labels), _label(c["y"], labels)
        text = (f"{who} ran at the same time. {A} changed {x} and then {y} as one update ({c['write_a']}). In "
                f"between, {B} read both ({c['read_b']}): {x} already changed, {y} not yet. {B} saw the update half "
                f"done. This is a torn read of {x} and {y}.")
    elif c["pattern"] == "deadlock":
        x, y = _label(c["x"], labels), _label(c["y"], labels)
        text = (f"{who} ran at the same time inside database transactions. {A} held {x} and waited for {y}; "
                f"{B} held {y} and waited for {x}. Neither could go on, so Postgres stopped {B}'s transaction with "
                f"a deadlock error. Taking the locks in the same order in every request avoids it.")
    elif c["pattern"] == "lost-update":
        last = names[c["last_writer"]]
        first = A if last == B else B
        text = (f"{who} ran at the same time. {A} read {res} and got {c['read_a']}. Before {A} saved its change, "
                f"{B} also read {res} and got {c['read_b']}. Both then saved their own result "
                f"({A}: {c['write_a']}; {B}: {c['write_b']}). {last} saved last and overwrote {first}'s change, "
                f"so one update was lost. This is a lost update on {res}.")
    elif c["pattern"] == "check-then-act" and c.get("range"):
        text = (f"{who} ran at the same time. Both looked for {res} first and both found no rows. Both then "
                f"added one ({A}: {c['write_a']}; {B}: {c['write_b']}), so the range that should have held at most "
                f"one now holds two. This is a check-then-act race on {res}.")
    elif c["pattern"] == "check-then-act":
        text = (f"{who} ran at the same time. Both checked {res} first and both saw that it was not there "
                f"({c['read_a']} and {c['read_b']}). Both then created it, so it was created twice. "
                f"This is a check-then-act race on {res}.")
    else:
        writer = A if c["write_a"] else B
        text = (f"{who} ran at the same time. {B} read {res} ({c['read_b']}) while {A} was in the middle of "
                f"changing it; {writer} then wrote {c['write_a'] or c['write_b']}. {B} acted on a stale value.")
    if c.get("narrowed") and c["confidence"] == "certain":
        text += (f" (One query covered {c['narrowed']}; Déjà checked that condition against the recorded rows "
                 f"to find that it included {res}.)")
    if c["confidence"] == "possible":
        text = ("Possible conflict. " + text + f" Déjà is not certain: one query covered {c['broad']} rather than "
                f"a specific row, so it may not have included the same row.")
    if reason:
        text += f" The app reported: {reason}."
    if len(conflicts) > 1:
        text += f" {len(conflicts) - 1} more conflict{'s' if len(conflicts) > 2 else ''} below."
    return text


def _moments(conflicts, touches, names, labels, requests=None):
    if not conflicts:
        return []
    c = conflicts[0]
    reqs = {c["a"], c["b"]}
    lo, hi = min(c["seqs"]), max(c["seqs"])
    stale_seq = c["seqs"][1]
    # the conflict's own accesses, and other accesses to the same things (not unrelated state in between)
    involved = {t["resource"] for t in touches if t["seq"] in c["seqs"]} | {c["resource"]} | set(c.get("rows") or ())
    other = "request"
    if requests and any((requests.get(int(r[1:])) or {}).get("method") == "WS" for r in reqs):
        other = "connection"
    out = []
    for t in sorted(touches, key=lambda t: t["seq"]):
        if t["req"] not in reqs or not (lo <= t["seq"] <= hi) or t.get("derived"):
            continue
        if t["seq"] not in c["seqs"] and t["resource"] not in involved and t.get("op") != "http.send":
            continue
        if t["kind"] == "call" and t["resource"] not in labels:  # unlabelled worker-thread calls are noise here
            continue
        who = names.get(t["req"], t["req"])
        res = _label(t["resource"], labels)
        if t["resource"].endswith("*]"):  # iterating / len() of tracked in-memory state
            res = "all of " + _label(t["resource"].split("[", 1)[0], labels)
        if t["kind"] == "read":
            text = (f"{who} calls {res} → {t.get('result')}" if t.get("op") == "http.send"
                    else f"{who} reads {res} → {short(codec.encode(t['value']))}")
        elif t.get("op") == "http.send":
            text = f"{who} calls {res} → {t.get('result')}"
        elif t["kind"] == "write":
            text = f"{who} writes {res} ({_fmt_writes(t.get('writes'))})"
        else:
            text = f"{who} calls {res}"
        if t.get("message"):
            text += f" (message {t['message']})"
        if t.get("thread"):
            text += " (in a worker thread)"
        out.append({"seq": t["seq"], "t": t["t"], "req": t["req"], "who": who, "text": text,
                    "warning": f"before the other {other} saved its change" if t["seq"] == stale_seq else None,
                    "in_conflict": t["seq"] in c["seqs"], "resource": t["resource"], "kind": t["kind"]})
    return out
