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
    """``[{"resource", "kind": "read"|"write", "columns", "writes"}]`` for one statement; [] if not understood."""
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


REDIS_READ = {"GET", "MGET", "HGET", "HGETALL", "EXISTS", "SMEMBERS", "LRANGE", "ZRANGE", "TTL", "STRLEN"}


def call_touches(io: dict) -> list[dict]:
    op, d = io["op"], io.get("detail") or {}
    if op.startswith("pg.") and "sql" in d:
        return sql_touches(d["sql"], d.get("params") or [])
    if op == "http.send" and "url" in d:
        u = urlsplit(d["url"])
        res = f"{d['method']} {u.netloc}{u.path}"
        return [{"resource": res, "kind": "read" if d["method"] in ("GET", "HEAD") else "write"}]
    if op == "redis.cmd" and d.get("args"):
        cmd, *rest = d["args"]
        if rest:
            kind = "read" if cmd.upper() in REDIS_READ else "write"
            t = {"resource": f"redis:{rest[0]}", "kind": kind}
            if kind == "write" and len(rest) > 1:
                t["writes"] = {"value": rest[1]}
            return [t]
    if op == "executor" and d.get("function"):
        return [{"resource": f"worker:{d['function']}", "kind": "call"}]
    return []


# ---------------------------------------------------------------------- helpers
def short(value, limit=60):
    """One-line human form of an encoded result."""
    if isinstance(value, dict) and value.get("__t") == "httpx":
        return f"HTTP {value.get('status')}"
    if isinstance(value, dict) and value.get("__t") == "exc":
        return "raised " + value.get("type", "").split(":")[-1]
    try:
        v = codec.decode(value)
    except Exception:
        v = value
    if isinstance(v, codec.FakeRecord):
        text = ", ".join(f"{k}={x!r}" for k, x in v.items())
    elif isinstance(v, list) and len(v) > 3:
        text = f"{len(v)} rows"
    else:
        text = repr(v)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _root(path):
    return path.split(".", 1)[0] if path else None


class _Expr(str):
    """A SQL expression (not a literal value); printed without quotes."""


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
                              "start": t, "end": None, "status": None, "response": None, "seq": seq}
        elif code == ev.RESPONSE and e[1] in requests:
            t = stamp(e[4] if len(e) > 4 else None)
            body = e[3] if len(e) > 3 else b""
            r = requests[e[1]]
            r.update(end=t, status=e[2],
                     response=body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else body)
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
            io["touches"] = call_touches(io)
            ios.append(io)
            for tch in io["touches"]:
                value = None
                if tch["kind"] == "read":
                    try:
                        value = codec.decode(e[5])
                    except Exception:
                        value = e[5]
                touches.append({**tch, "seq": seq, "t": t1, "req": io["req"], "task": e[1], "op": e[2],
                                "value": value, "result": io["result"], "source": "recorded call"})
        elif code == ev.TOUCH and e[1] and e[1].startswith("q"):
            t = stamp(e[5] if len(e) > 5 else None)
            touches.append({"resource": e[2], "kind": e[3], "seq": seq, "t": t, "req": _root(e[1]), "task": e[1],
                            "op": "deja.touch", "value": codec.decode(e[4]),
                            "writes": {"value": codec.decode(e[4])} if e[3] == "write" else None,
                            "source": "reported by the app"})

    conflicts = _conflicts(touches)
    names = _names(conflicts, requests)
    for c in conflicts:
        c["names"] = {r: names.get(r, r) for r in (c["a"], c["b"])}
    story = _story(conflicts, requests, names, header, labels)
    moments = _moments(conflicts, touches, names, labels)
    return {
        "requests": [requests[k] for k in sorted(requests)],
        "steps": steps, "ios": [{k: v for k, v in io.items() if k != "raw"} for io in ios],
        "touches": [{k: (v if k != "value" else short(codec.encode(v))) for k, v in t.items()} for t in touches],
        "conflicts": conflicts, "story": story, "moments": moments, "names": names, "labels": labels,
        "reason": header.get("reason"), "duration_ms": now or 0,
    }


def _absent(v):
    """'Nothing found': None, False, or an empty result. Zero is a value (a counter at 0), not absence."""
    if v is None or v is False:
        return True
    return isinstance(v, (list, tuple, str, dict)) and len(v) == 0


def _conflicts(touches):
    by_res = defaultdict(list)
    for t in touches:
        rowish = "[" in t["resource"] or t["resource"].startswith("redis:") or t.get("source") == "reported by the app"
        if t["kind"] in ("read", "write") and rowish:
            by_res[t["resource"]].append(t)
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
                r_a, r_b = ra[0], rb[0]
                first_wa = next((w for w in wa if w["seq"] > r_a["seq"]), None)
                if r_a["seq"] < r_b["seq"] and (first_wa is None or r_b["seq"] < first_wa["seq"]):
                    key = (res, a, b)
                    if key in seen:
                        continue
                    seen.add(key)
                    later_wb = next((w for w in wb if w["seq"] > r_b["seq"]), None)
                    if first_wa and later_wb:
                        pattern = "check-then-act" if _absent(r_a["value"]) and _absent(r_b["value"]) else "lost-update"
                    elif first_wa or later_wb:
                        pattern = "stale-read"
                    else:
                        continue
                    involved = [r_a, r_b] + [w for w in (first_wa, later_wb) if w]
                    out.append({"pattern": pattern, "resource": res, "a": a, "b": b,
                                "read_a": short(codec.encode(r_a["value"])), "read_b": short(codec.encode(r_b["value"])),
                                "write_a": _fmt_writes(first_wa.get("writes")) if first_wa else None,
                                "write_b": _fmt_writes(later_wb.get("writes")) if later_wb else None,
                                "last_writer": (b if later_wb and (not first_wa or later_wb["seq"] > first_wa["seq"])
                                                else a),
                                "seqs": [t["seq"] for t in involved],
                                "t0": min(t["t"] for t in involved if t["t"] is not None),
                                "t1": max(t["t"] for t in involved if t["t"] is not None)})
    order = {"lost-update": 0, "check-then-act": 1, "stale-read": 2}
    out.sort(key=lambda c: (order[c["pattern"]], c["t0"] or 0))
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
    return _label(f"{req['method']} {req['path'].split('?')[0]}", labels)


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
    who = (f"Two {what_a} requests ({A} and {B})" if what_a == what_b
           else f"Request {A} ({what_a}) and request {B} ({what_b})")
    if c["pattern"] == "lost-update":
        last = names[c["last_writer"]]
        first = A if last == B else B
        text = (f"{who} ran at the same time. {A} read {res} and got {c['read_a']}. Before {A} saved its change, "
                f"{B} also read {res} and got {c['read_b']}. Both then saved their own result "
                f"({A}: {c['write_a']}; {B}: {c['write_b']}). {last} saved last and overwrote {first}'s change, "
                f"so one update was lost. This is a lost update on {res}.")
    elif c["pattern"] == "check-then-act":
        text = (f"{who} ran at the same time. Both checked {res} first and both saw that it was not there "
                f"({c['read_a']} and {c['read_b']}). Both then created it, so it was created twice. "
                f"This is a check-then-act race on {res}.")
    else:
        writer = A if c["write_a"] else B
        text = (f"{who} ran at the same time. {B} read {res} ({c['read_b']}) while {A} was in the middle of "
                f"changing it; {writer} then wrote {c['write_a'] or c['write_b']}. {B} acted on a stale value.")
    if reason:
        text += f" The app reported: {reason}."
    if len(conflicts) > 1:
        text += f" {len(conflicts) - 1} more conflict{'s' if len(conflicts) > 2 else ''} below."
    return text


def _moments(conflicts, touches, names, labels):
    if not conflicts:
        return []
    c = conflicts[0]
    reqs = {c["a"], c["b"]}
    lo, hi = min(c["seqs"]), max(c["seqs"])
    stale_seq = c["seqs"][1]
    out = []
    for t in sorted(touches, key=lambda t: t["seq"]):
        if t["req"] not in reqs or not (lo <= t["seq"] <= hi):
            continue
        if t["kind"] == "call" and t["resource"] not in labels:  # unlabelled worker-thread calls are noise here
            continue
        who = names.get(t["req"], t["req"])
        res = _label(t["resource"], labels)
        if t["kind"] == "read":
            text = (f"{who} calls {res} → {t.get('result')}" if t.get("op") == "http.send"
                    else f"{who} reads {res} → {short(codec.encode(t['value']))}")
        elif t.get("op") == "http.send":
            text = f"{who} calls {res} → {t.get('result')}"
        elif t["kind"] == "write":
            text = f"{who} writes {res} ({_fmt_writes(t.get('writes'))})"
        else:
            text = f"{who} calls {res}"
        out.append({"seq": t["seq"], "t": t["t"], "req": t["req"], "who": who, "text": text,
                    "warning": "before the other request saved its change" if t["seq"] == stale_seq else None,
                    "in_conflict": t["seq"] in c["seqs"], "resource": t["resource"], "kind": t["kind"]})
    return out
