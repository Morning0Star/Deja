"""A self-contained HTML report for one ``.deja`` recording: what happened, the requests, and the interleaving.

    deja report crash.deja                                   # writes crash.html next to it
    deja report crash.deja --app app.main:create_app --times 20   # also replays it and adds the verdict

The interleaving is drawn as swimlanes: one column per request, one row per event-loop pass in which any request
ran. Each cell shows that request's step, the I/O calls it made (with the recorded answer), and start / end /
flag markers. Passes where no request ran are folded into a single "n passes without request activity" row.
"""

import datetime as dt
import html

from . import codec
from . import events as ev
from .fileformat import read

CSS = """
:root { --bg:#fbfbfa; --fg:#1a1c1e; --muted:#5a6068; --line:#d9dcdf; --cell:#ffffff; --accent:#0b6bcb;
        --bad:#b42318; --bad-bg:#fdecea; --io:#0f5132; --mono: ui-monospace, "Cascadia Mono", Menlo, Consolas, monospace;
        --sans: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; color-scheme: light; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#111315; --fg:#eceff1; --muted:#a3abb3; --line:#30353a; --cell:#181b1e; --accent:#6cb2ff;
          --bad:#ff8a80; --bad-bg:#3a1714; --io:#7ee2a8; color-scheme: dark; }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--fg); font: 15px/1.5 var(--sans); }
main { max-width: 1200px; margin: 0 auto; padding: 32px 20px 64px; }
h1 { font-size: 22px; margin: 0 0 4px; }
h2 { font-size: 16px; margin: 36px 0 10px; }
.reason { font-size: 17px; margin: 8px 0 18px; }
.facts { display: flex; flex-wrap: wrap; gap: 8px 28px; color: var(--muted); font-size: 13px; }
.facts b { color: var(--fg); font-weight: 600; }
.scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 6px; background: var(--cell); }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { text-align: left; vertical-align: top; padding: 7px 10px; border-bottom: 1px solid var(--line); }
th { font-weight: 600; color: var(--muted); background: var(--bg); position: sticky; top: 0; }
td.mono, .mono { font-family: var(--mono); font-size: 12.5px; }
tr.bad td { background: var(--bad-bg); }
.lanes td { min-width: 190px; border-left: 1px solid var(--line); }
.lanes td.pass { min-width: 64px; color: var(--muted); font-family: var(--mono); border-left: 0; }
.lanes th.req { min-width: 190px; border-left: 1px solid var(--line); }
.lanes th.bad { color: var(--bad); }
.fold td { color: var(--muted); font-style: normal; font-size: 12px; text-align: center; background: var(--bg); }
.ev { display: block; font-family: var(--mono); font-size: 12px; line-height: 1.45; }
.step { color: var(--fg); }
.io { color: var(--io); }
.start, .end { color: var(--accent); }
.flag, .err { color: var(--bad); font-weight: 600; }
.verdict { padding: 12px 14px; border: 1px solid var(--line); border-radius: 6px; background: var(--cell);
           font-family: var(--mono); font-size: 13px; white-space: pre-wrap; }
.how { color: var(--muted); font-size: 13px; }
code { font-family: var(--mono); font-size: 12.5px; }
"""


def _short(value, limit=60):
    """A one-line, human summary of an encoded I/O result."""
    if isinstance(value, dict) and value.get("__t") == "httpx":
        return f"HTTP {value.get('status')}"
    if isinstance(value, dict) and value.get("__t") == "exc":
        return f"raised {value.get('type', '').split(':')[-1]}"
    try:
        v = codec.decode(value)
    except Exception:
        v = value
    if isinstance(v, list) and len(v) > 3:
        text = f"[{len(v)} rows]"
    else:
        text = repr(v)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _body(b, limit=80):
    if isinstance(b, (bytes, bytearray)):
        b = b.decode("utf-8", "replace")
    b = str(b or "")
    return b if len(b) <= limit else b[: limit - 3] + "..."


def _root(path):
    return path.split(".", 1)[0] if path else None


def build(header: dict, events: list, replay_summary: str | None = None, source: str = "") -> str:
    e = html.escape
    requests, responses, flagged = {}, {}, set()
    for x in events:
        if x[0] == ev.REQUEST:
            requests[x[1]] = x
        elif x[0] == ev.RESPONSE:
            responses[x[1]] = x
        elif x[0] == ev.FLAG and x[1]:
            root = _root(x[1])
            if root and root.startswith("q"):
                flagged.add(int(root[1:]))
    lanes = sorted(requests)
    bad = {rid for rid, r in responses.items() if r[2] >= 500} | flagged

    # ---- swimlane rows: one per loop pass with request activity
    rows, current, pass_no, idle = [], None, 0, 0

    def flush():
        nonlocal current
        if current and any(current.values()):
            rows.append((pass_no, current))
        current = None

    for x in events:
        code = x[0]
        if code == ev.BATCH:
            flush()
            pass_no += 1
            current = {}
            continue
        if current is None:
            current = {}
        lane = cell = None
        if code == ev.REQUEST:
            lane, cell = x[1], f'<span class="ev start">start {e(x[3])} {e(_body(x[4], 40))}</span>'
        elif code == ev.RESPONSE:
            lane, cell = x[1], f'<span class="ev end">end, {x[2]} {e(_body(x[3] if len(x) > 3 else "", 50))}</span>'
        elif code == ev.STEP and x[1] and x[1].startswith("q"):
            sub = x[1].split(".", 1)[1] if "." in x[1] else ""
            lane, cell = int(_root(x[1])[1:]), f'<span class="ev step">runs{e(" (child ." + sub + ")") if sub else ""}</span>'
        elif code == ev.IO_END and x[1] and x[1].startswith("q"):
            klass = "err" if x[4] == "err" else "io"
            lane = int(_root(x[1])[1:])
            cell = f'<span class="ev {klass}">{e(x[2])} &rarr; {e(_short(x[5]))}</span>'
        elif code == ev.FLAG and x[1] and _root(x[1]).startswith("q"):
            lane, cell = int(_root(x[1])[1:]), f'<span class="ev flag">flag: {e(str(x[2]))}</span>'
        if lane is not None and lane in requests:
            current.setdefault(lane, []).append(cell)
    flush()

    # fold idle passes
    body_rows = []
    prev = 0
    for n, cells in rows:
        if n - prev > 1:
            gap = n - prev - 1
            body_rows.append(f'<tr class="fold"><td colspan="{len(lanes) + 1}">{gap} pass{"es" if gap > 1 else ""} '
                             f'without request activity</td></tr>')
        tds = "".join(f'<td>{"".join(cells.get(rid, []))}</td>' for rid in lanes)
        body_rows.append(f'<tr><td class="pass">{n}</td>{tds}</tr>')
        prev = n

    req_rows = []
    for rid in lanes:
        q = requests[rid]
        r = responses.get(rid)
        status = r[2] if r else "?"
        resp = _body(r[3] if r and len(r) > 3 else "", 90)
        req_rows.append(
            f'<tr class="{"bad" if rid in bad else ""}"><td class="mono">q{rid}</td><td class="mono">{e(q[3])}</td>'
            f'<td class="mono">{e(_body(q[4], 60))}</td><td class="mono">{e(_body(q[6], 60))}</td>'
            f'<td class="mono">{e(str(status))}</td><td class="mono">{e(resp)}</td></tr>')

    lane_heads = "".join(f'<th class="req{" bad" if rid in bad else ""}">q{rid} {e(requests[rid][3])} '
                         f'{e(_body(requests[rid][4], 28))}</th>' for rid in lanes)
    started = header.get("started_at")
    when = dt.datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S") if started else "?"
    reason = header.get("reason") or "saved on demand"
    facts = [("Python", header.get("python")), ("Platform", header.get("platform")), ("Loop", header.get("loop")),
             ("Recorder started", when), ("Requests", len(lanes)), ("Events", len(events))]
    replay_block = (f'<h2>Replay</h2><div class="verdict">{e(replay_summary)}</div>' if replay_summary else
                    f'<h2>Replay</h2><p class="how">Reproduce it: <code>deja replay {e(source or "recording.deja")} '
                    f'--app your.module:create_app --times 100</code></p>')

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Déjà recording{(" · " + e(source)) if source else ""}</title><style>{CSS}</style></head>
<body><main>
<h1>Déjà recording</h1>
<div class="how mono">{e(source)}</div>
<p class="reason">{e(reason)}</p>
<div class="facts">{"".join(f"<span>{e(k)} <b>{e(str(v))}</b></span>" for k, v in facts)}</div>

<h2>Requests</h2>
<div class="scroll"><table>
<tr><th>Request</th><th>Method</th><th>Path</th><th>Body sent</th><th>Status</th><th>Response</th></tr>
{"".join(req_rows)}
</table></div>

<h2>Interleaving</h2>
<p class="how">One column per request, one row per event-loop pass in which a request ran. Database and HTTP calls show
the answer that was recorded. Read top to bottom: this is the order the tasks actually took turns.</p>
<div class="scroll"><table class="lanes">
<tr><th>Pass</th>{lane_heads}</tr>
{"".join(body_rows)}
</table></div>

{replay_block}
</main></body></html>
"""


def write_report(recording: str, out: str | None = None, replay_summary: str | None = None) -> str:
    import os

    header, events = read(recording)
    out = out or os.path.splitext(recording)[0] + ".html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(build(header, events, replay_summary, source=os.path.basename(recording)))
    return out
