"""``deja report``: one self-contained, interactive HTML page for a recording, story first.

Top: what went wrong, in plain words (from ``deja.analysis``, no LLM). Then the key moments, numbered and clickable.
Then a zoomable, pannable swimlane timeline (canvas) with a minimap, a details panel, what each request touched,
and the replay verdict. Works offline; the data is embedded in the page.

    deja report crash.deja
    deja report crash.deja --app app.main:create_app --times 20     # adds a replay verdict
"""

import html
import json
import os

from .analysis import analyze
from .fileformat import read

PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Déjà report · __SOURCE__</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Chakra+Petch:wght@500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap">
<style>
:root { --bg:#06080b; --panel:#0b1016; --panel-2:#0f161e; --line:#1d2a36; --line-2:#2a3c4c; --fg:#e4eef6; --muted:#8ea4b6;
  --cyan:#2fe4ff; --amber:#ffb52e; --red:#ff3d5e; --lime:#a6ff5c;
  --display:"Chakra Petch","Bahnschrift","DIN Alternate",system-ui,sans-serif;
  --mono:"JetBrains Mono",ui-monospace,"Cascadia Mono",Menlo,Consolas,monospace;
  --chamfer:polygon(10px 0,100% 0,100% calc(100% - 10px),calc(100% - 10px) 100%,0 100%,0 10px); color-scheme:dark; }
*{box-sizing:border-box} html,body{background:var(--bg);color:var(--fg)} body{margin:0;font:13px/1.55 var(--mono)}
.wrap{max-width:1400px;margin:0 auto;padding:0 max(16px,env(safe-area-inset-right,0px)) 56px max(16px,env(safe-area-inset-left,0px))}
button{font:inherit;color:inherit} :focus-visible{outline:2px solid var(--cyan);outline-offset:2px}
.bar{position:sticky;top:0;z-index:5;background:var(--bg);border-bottom:1px solid var(--line)}
.bar .wrap{display:flex;align-items:center;gap:12px 22px;flex-wrap:wrap;padding-top:12px;padding-bottom:12px}
.brand{font:700 18px/1 var(--display);letter-spacing:.14em;text-transform:uppercase}.brand span{color:var(--cyan)}
.file{color:var(--muted);min-width:0;overflow-wrap:anywhere}
.pill{margin-left:auto;font:600 12px/1 var(--display);letter-spacing:.12em;text-transform:uppercase;padding:8px 12px;clip-path:var(--chamfer)}
.pill.ok{background:var(--lime);color:var(--bg)} .pill.bad{background:var(--red);color:var(--bg)} .pill.none{border:1px solid var(--line-2);color:var(--muted)}
h2{font:600 13px/1 var(--display);letter-spacing:.18em;text-transform:uppercase;color:var(--muted);margin:30px 0 12px;display:flex;align-items:center;gap:10px}
h2::after{content:"";flex:1;height:1px;background:var(--line)}
.story{margin-top:22px;background:var(--panel);clip-path:var(--chamfer);display:grid;grid-template-columns:6px 1fr;gap:0 18px}
.story::before{content:"";background:var(--red)} .story.clean::before{background:var(--lime)}
.story .in{padding:16px 18px 16px 0} .kicker{font:700 11px/1 var(--display);letter-spacing:.2em;text-transform:uppercase;color:var(--red)}
.story.clean .kicker{color:var(--lime)}
.story p{margin:10px 0 0;font:500 clamp(15px,1.6vw,18px)/1.6 var(--display);max-width:95ch}
.grid2{display:grid;grid-template-columns:minmax(0,1fr) 360px;gap:16px;align-items:start}
.moments{list-style:none;margin:0;padding:0;counter-reset:m;border:1px solid var(--line);background:var(--panel)}
.moments li{counter-increment:m;display:grid;grid-template-columns:34px 1fr;gap:10px;padding:10px 14px;border-bottom:1px solid var(--line);cursor:pointer}
.moments li:last-child{border-bottom:0} .moments li:hover,.moments li.on{background:var(--panel-2)}
.moments li::before{content:counter(m);font:700 14px/22px var(--display);text-align:center;border:1px solid var(--line-2);height:24px}
.moments li.warn::before{border-color:var(--red);color:var(--red)} .moments li.on::before{background:var(--cyan);color:var(--bg);border-color:var(--cyan)}
.moments .w{display:block;color:var(--red);font-size:12px}
.btn{background:var(--panel-2);border:1px solid var(--line-2);padding:8px 12px;cursor:pointer;font:600 12px/1 var(--display);letter-spacing:.1em;text-transform:uppercase;white-space:nowrap}
.btn:hover{border-color:var(--cyan);color:var(--cyan)} .btn[aria-pressed="true"]{background:var(--cyan);color:var(--bg);border-color:var(--cyan)}
.tl{background:var(--panel);border:1px solid var(--line);min-width:0}
.tools{display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:10px;border-bottom:1px solid var(--line)}
.tools .hint{margin-left:auto;color:var(--muted);font-size:11px}
.legend{display:flex;flex-wrap:wrap;gap:6px 18px;padding:8px 10px;border-bottom:1px solid var(--line);color:var(--muted);font-size:11px}
.sw{display:inline-block;width:18px;height:8px;vertical-align:middle;margin-right:6px}
.sw.run{background:var(--cyan)} .sw.wait{background:repeating-linear-gradient(135deg,var(--amber) 0 3px,transparent 3px 6px);border:1px solid var(--amber)}
.sw.conf{background:rgba(255,61,94,.25);border:1px solid var(--red)} .sw.r{background:var(--cyan);border-radius:50%;width:9px;height:9px}
.sw.wr{background:var(--red);border-radius:50%;width:9px;height:9px}
canvas{display:block;width:100%;touch-action:none} #main{cursor:grab} #main.dragging{cursor:grabbing}
#mini{height:50px;border-top:1px solid var(--line);cursor:pointer}
.scrub{display:flex;gap:10px;align-items:center;padding:10px;border-top:1px solid var(--line);flex-wrap:wrap}
.scrub input{flex:1;min-width:150px;accent-color:var(--cyan)} .scrub .pos{color:var(--muted);min-width:110px;font-variant-numeric:tabular-nums}
#tip{position:fixed;z-index:20;pointer-events:none;background:var(--panel-2);border:1px solid var(--line-2);padding:8px 10px;font-size:12px;max-width:360px;display:none}
.insp{background:var(--panel);border:1px solid var(--line);clip-path:var(--chamfer);min-width:0;position:sticky;top:70px}
.insp header{padding:12px 14px;border-bottom:1px solid var(--line)} .insp .kind{font:700 11px/1 var(--display);letter-spacing:.2em;text-transform:uppercase;color:var(--cyan)}
.insp .kind.io{color:var(--amber)} .insp .kind.req{color:var(--lime)} .insp .kind.bad{color:var(--red)}
.insp h3{margin:6px 0 0;font:600 16px/1.35 var(--display);overflow-wrap:anywhere}
.insp dl{margin:0;padding:6px 14px 14px;display:grid;grid-template-columns:90px minmax(0,1fr);gap:8px 10px}
.insp dt{color:var(--muted);font-size:11px;letter-spacing:.08em;text-transform:uppercase;padding-top:2px} .insp dd{margin:0;overflow-wrap:anywhere}
.insp pre{margin:0;white-space:pre-wrap;background:var(--bg);border:1px solid var(--line);padding:8px;font:12px/1.45 var(--mono)}
.touch{display:inline-block;padding:2px 6px;margin:0 4px 4px 0;border:1px solid var(--line-2);font-size:12px}
.touch.write{border-color:var(--red);color:var(--red)} .touch.read{border-color:var(--cyan);color:var(--cyan)}
.empty{padding:14px;color:var(--muted)}
.scroll{overflow-x:auto;border:1px solid var(--line);background:var(--panel)}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{padding:9px 12px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap;vertical-align:top}
th{color:var(--muted);font:600 11px/1 var(--display);letter-spacing:.14em;text-transform:uppercase;background:var(--panel-2)}
tbody tr{cursor:pointer} tbody tr:hover td{background:var(--panel-2)} tbody tr.sel td{background:rgba(47,228,255,.08)} tbody tr.hot td:first-child{color:var(--red)}
.acc{display:inline-block;margin-right:6px;padding:1px 5px;border:1px solid;font-size:11px} .acc.read{color:var(--cyan)} .acc.write{color:var(--red)}
.verdict{background:var(--panel);border:1px solid var(--line);padding:12px 14px;white-space:pre-wrap;font-size:12.5px}
.note{color:var(--muted);font-size:12px;margin:10px 0 0;max-width:100ch}
@media (max-width:1000px){.grid2{grid-template-columns:1fr}.insp{position:static}.tools .hint{display:none}}
@media (max-width:560px){.pill{margin-left:0}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head>
<body>
<header class="bar"><div class="wrap">
  <div class="brand">Déjà <span>//</span> Report</div>
  <div class="file" id="file"></div>
  <div class="pill" id="pill"></div>
</div></header>
<main class="wrap">
  <section class="story" id="story"><div class="in"><div class="kicker" id="kicker"></div><p id="storytext"></p></div></section>

  <h2>Key moments</h2>
  <div class="grid2">
    <div>
      <ol class="moments" id="moments"></ol>
      <p class="note" id="mnote">Click a moment to see it on the timeline. Play steps through them in order.</p>
    </div>
    <aside class="insp" id="insp" aria-live="polite"><div class="empty">Click a moment, a bar or a resource to see its details.</div></aside>
  </div>

  <h2>Timeline</h2>
  <section class="tl" aria-label="Timeline of requests">
    <div class="tools">
      <button class="btn" id="fit">Fit</button>
      <button class="btn" id="zin">Zoom +</button>
      <button class="btn" id="zout">Zoom −</button>
      <button class="btn" id="goconf" hidden>Zoom to the race</button>
      <button class="btn" id="detail" aria-pressed="false">Show every step</button>
      <button class="btn" id="clear" hidden>Clear filter</button>
      <span class="hint">Scroll to zoom · drag to pan · click anything · keys + − ← → F</span>
    </div>
    <div class="legend">
      <span><i class="sw wait"></i>waiting on the database, an API or a worker</span>
      <span><i class="sw r"></i>read</span><span><i class="sw wr"></i>write</span>
      <span><i class="sw conf"></i>the race</span><span id="lg-steps" hidden><i class="sw run"></i>code running</span>
    </div>
    <canvas id="main" aria-label="Swimlane timeline, one lane per request"></canvas>
    <canvas id="mini" aria-label="Overview; drag the window to move"></canvas>
    <div class="scrub">
      <button class="btn" id="play">Play</button>
      <input type="range" id="step" min="0" max="1" value="0" aria-label="Moment">
      <span class="pos" id="pos"></span>
    </div>
  </section>

  <h2>What each request touched</h2>
  <div class="scroll"><table id="res"><thead></thead><tbody></tbody></table></div>
  <p class="note">Inside a cell, read left to right: the order that request touched the resource, with the time in ms.
    Click a row to keep only that resource on the timeline.</p>

  <h2>Replay</h2>
  <div class="verdict" id="verdict"></div>
</main>
<div id="tip"></div>
<script id="data" type="application/json">__DATA__</script>
<script>
(() => {
"use strict";
const D = JSON.parse(document.getElementById("data").textContent);
const $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const L = t => (D.labels && D.labels[t]) || t;

// ---------------------------------------------------------------- header, story, verdict
$("file").textContent = D.source + (D.reason ? " · saved because: " + D.reason : "");
const pill = $("pill");
if (D.replay) { pill.textContent = D.replay.matched + " / " + D.replay.times + " replays matched";
  pill.className = "pill " + (D.replay.matched === D.replay.times ? "ok" : "bad"); }
else { pill.textContent = "not replayed in this report"; pill.className = "pill none"; }
const C0 = D.conflicts[0];
const PAT = {"lost-update": "Lost update", "check-then-act": "Check-then-act race", "stale-read": "Stale read"};
$("kicker").textContent = C0 ? (C0.confidence === "possible" ? "Possible " + PAT[C0.pattern].toLowerCase() : PAT[C0.pattern]) + " on " + L(C0.resource) : "No race found between requests";
if (!C0) $("story").classList.add("clean");
$("storytext").textContent = D.story;
$("verdict").textContent = D.replay ? D.replay.summary :
  "Not replayed here. Reproduce it with:  deja replay " + D.source + " --app your.module:create_app --times 100";

// ---------------------------------------------------------------- lanes: conflict requests first, named A, B...
const N = D.names || {};
const REQ = D.requests.slice().sort((a, b) => (N[a.id] ? 0 : 1) - (N[b.id] ? 0 : 1) || a.start - b.start);
const laneName = r => (N[r.id] ? N[r.id] + " · " : "") + L(r.method + " " + r.path.split("?")[0]);
const T_MIN = Math.min(0, ...REQ.map(r => r.start ?? 0)) - 0.5;
const T_END = Math.max(D.duration_ms || 1, ...REQ.map(r => r.end ?? 0)) + 0.5;

// I/O bars with a friendly label from what they touched
const IOS = D.ios.map(io => {
  const t = (io.touches || [])[0];
  const msg = io.op === "ws.receive" ? (io.result && io.result !== "None" ? "receives " + io.result : "waits for a message")
            : io.op === "ws.send" ? "sends " + ((io.detail || {}).message || "") : null;
  const friendly = msg || (t ? (io.op === "http.send" ? "calls " + L(t.resource) : t.kind === "read" ? "reads " + L(t.resource)
                   : t.kind === "write" ? "writes " + L(t.resource) : "runs " + L(t.resource)) : io.op);
  return {...io, type: "io", friendly};
});
const TOUCH = D.touches.map(t => ({...t, type: "touch"}));
const STEPS = D.steps.map(s => ({...s, type: "step"}));
const MOMENTS = D.moments;

// ---------------------------------------------------------------- canvas
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const C = {bg: css("--bg"), panel: css("--panel"), line: css("--line"), line2: css("--line-2"), fg: css("--fg"),
           muted: css("--muted"), cyan: css("--cyan"), amber: css("--amber"), red: css("--red"), lime: css("--lime")};
const main = $("main"), mini = $("mini"), tip = $("tip"), insp = $("insp");
const LANE = 66, RULER = 26;
main.style.height = (RULER + 10 + REQ.length * LANE) + "px";
let view = {t0: T_MIN, t1: T_END}, hover = null, selected = null, filter = null, every = false, cursor = 0, LEFT = 200, hits = [];
function size(cv) { const r = devicePixelRatio || 1, w = cv.clientWidth, h = cv.clientHeight;
  if (cv.width !== Math.round(w * r) || cv.height !== Math.round(h * r)) { cv.width = Math.round(w * r); cv.height = Math.round(h * r); }
  const g = cv.getContext("2d"); g.setTransform(r, 0, 0, r, 0, 0); return [g, w, h]; }
const laneY = id => RULER + 8 + REQ.findIndex(r => r.id === id) * LANE;
const X = (t, w) => LEFT + (t - view.t0) / (view.t1 - view.t0) * (w - LEFT - 10);
const T = (x, w) => view.t0 + (x - LEFT) / (w - LEFT - 10) * (view.t1 - view.t0);
const dimmed = it => filter && !((it.touches || []).some(t => t.resource === filter) || it.resource === filter);
function niceStep(span, px) { const target = span / Math.max(2, px / 90), p = Math.pow(10, Math.floor(Math.log10(target)));
  for (const m of [1, 2, 5, 10]) if (m * p >= target) return m * p; return 10 * p; }
const fmt = (v, st) => (st < 0.1 ? v.toFixed(2) : st < 1 ? v.toFixed(1) : v.toFixed(0)) + " ms";
function hatch(g) { const p = document.createElement("canvas"); p.width = p.height = 8; const q = p.getContext("2d");
  q.strokeStyle = C.amber; q.lineWidth = 2; q.beginPath(); q.moveTo(-2, 10); q.lineTo(10, -2); q.moveTo(-2, 2); q.lineTo(2, -2);
  q.moveTo(6, 10); q.lineTo(10, 6); q.stroke(); return g.createPattern(p, "repeat"); }

function draw() {
  LEFT = main.clientWidth < 560 ? 92 : 210;
  const [g, w, h] = size(main); hits = [];
  g.fillStyle = C.panel; g.fillRect(0, 0, w, h);
  const st = niceStep(view.t1 - view.t0, w - LEFT);
  g.font = "11px " + css("--mono"); g.textBaseline = "middle";
  for (let t = Math.ceil(view.t0 / st) * st; t <= view.t1; t += st) { const x = X(t, w); if (x < LEFT) continue;
    g.strokeStyle = C.line; g.beginPath(); g.moveTo(x + .5, RULER); g.lineTo(x + .5, h); g.stroke();
    g.fillStyle = C.muted; g.fillText(fmt(t, st), x + 4, RULER / 2); }
  g.save(); g.beginPath(); g.rect(LEFT, RULER, w - LEFT, h - RULER); g.clip();
  REQ.forEach((r, i) => { const y = RULER + 8 + i * LANE; if (i) { g.strokeStyle = C.line; g.beginPath(); g.moveTo(0, y - 4.5); g.lineTo(w, y - 4.5); g.stroke(); }
    if (r.start != null) { const x0 = X(r.start, w), x1 = X(r.end ?? T_END, w);
      g.strokeStyle = C.line2; g.lineWidth = 2; g.beginPath(); g.moveTo(x0, y + 50); g.lineTo(x1, y + 50); g.stroke(); g.lineWidth = 1;
      g.fillStyle = C.lime; g.beginPath(); g.moveTo(x0, y + 45); g.lineTo(x0 + 7, y + 50); g.lineTo(x0, y + 55); g.fill();
      g.fillStyle = r.failed ? C.red : C.fg; g.fillRect(x1 - 5, y + 47, 6, 6);
      hits.push([{type: "req", ...r}, x0 - 4, y + 42, x1 + 4, y + 58]); } });
  if (C0) { const x0 = X(C0.t0, w), x1 = X(C0.t1, w);
    [C0.a, C0.b].forEach(id => { const y = laneY(id); g.fillStyle = "rgba(255,61,94,.11)"; g.fillRect(x0, y - 3, x1 - x0, LANE - 8);
      g.strokeStyle = C.red; g.setLineDash([4, 4]); g.strokeRect(x0 + .5, y - 2.5, x1 - x0, LANE - 9); g.setLineDash([]); }); }
  const pat = hatch(g);
  if (every) for (const s of STEPS) { if (!REQ.some(r => r.id === s.req)) continue; const x = X(s.t, w), y = laneY(s.req);
    if (x < LEFT - 4 || x > w) continue; g.fillStyle = C.cyan; g.globalAlpha = filter ? .25 : 1; g.fillRect(x, y + 2, 4, 10); g.globalAlpha = 1;
    hits.push([s, x - 2, y, x + 6, y + 14]); }
  for (const io of IOS) { if (!REQ.some(r => r.id === io.req)) continue;
    const y = laneY(io.req) + 16, x0 = X(io.t0, w), x1 = Math.max(X(io.t1, w), x0 + 3); if (x1 < LEFT || x0 > w) continue;
    g.globalAlpha = dimmed(io) ? .2 : 1; g.fillStyle = pat; g.fillRect(x0, y, x1 - x0, 16);
    g.strokeStyle = (selected === io || hover === io) ? C.fg : C.amber; g.lineWidth = selected === io ? 2 : 1; g.strokeRect(x0 + .5, y + .5, x1 - x0 - 1, 15); g.lineWidth = 1;
    const label = io.friendly + (io.result ? " → " + io.result : ""), lx = Math.max(x0, LEFT), room = x1 - lx;
    g.font = "11px " + css("--mono"); const tw = g.measureText(label).width;
    const txt = room > tw + 12 ? label : room > 70 ? io.friendly : room > 30 ? io.op.split(".").pop() : "";
    if (txt) { const tw2 = g.measureText(txt).width; g.fillStyle = C.panel; g.fillRect(lx + 3, y + 2, Math.min(tw2 + 6, room - 6), 12);
      g.fillStyle = C.amber; g.fillText(txt, lx + 6, y + 8.5); }
    g.globalAlpha = 1; hits.push([io, x0, y, x1, y + 16]); }
  for (const t of TOUCH) { if (t.kind !== "read" && t.kind !== "write") continue; if (!REQ.some(r => r.id === t.req)) continue;
    const x = X(t.t, w), y = laneY(t.req) + 38; if (x < LEFT - 6 || x > w + 6) continue;
    g.globalAlpha = dimmed(t) ? .2 : 1; g.fillStyle = t.kind === "read" ? C.cyan : C.red; g.beginPath(); g.arc(x, y, 5, 0, 7); g.fill();
    if (selected === t || hover === t) { g.strokeStyle = C.fg; g.lineWidth = 2; g.stroke(); g.lineWidth = 1; }
    g.globalAlpha = 1; hits.push([t, x - 7, y - 7, x + 7, y + 7]); }
  MOMENTS.forEach((m, i) => { const x = X(m.t, w), y = laneY(m.req); if (x < LEFT - 10 || x > w + 10) return;
    const on = cursor === i + 1; g.fillStyle = on ? C.cyan : m.warning ? C.red : C.fg; g.font = "700 11px " + css("--display");
    const lab = String(i + 1), bw = 16; g.fillRect(x - bw / 2, y + 26, bw, 14); g.fillStyle = C.bg; g.textAlign = "center"; g.fillText(lab, x, y + 33.5); g.textAlign = "left"; });
  if (C0 && MOMENTS.length > 1) { const a = MOMENTS.find(m => m.req === C0.a && m.kind === "read"), b = MOMENTS.find(m => m.req === C0.b && m.kind === "read");
    if (a && b) { g.strokeStyle = C.red; g.setLineDash([3, 3]); g.beginPath(); g.moveTo(X(a.t, w), laneY(a.req) + 38); g.lineTo(X(b.t, w), laneY(b.req) + 38); g.stroke(); g.setLineDash([]); } }
  if (cursor) { const m = MOMENTS[cursor - 1], x = X(m.t, w); g.strokeStyle = C.cyan; g.lineWidth = 1.5; g.beginPath(); g.moveTo(x + .5, RULER); g.lineTo(x + .5, h); g.stroke(); g.lineWidth = 1; }
  g.restore();
  g.fillStyle = C.panel; g.fillRect(0, RULER + 1, LEFT - 2, h - RULER - 1);
  g.strokeStyle = C.line2; g.beginPath(); g.moveTo(LEFT - 1.5, RULER); g.lineTo(LEFT - 1.5, h); g.stroke();
  REQ.forEach((r, i) => { const y = RULER + 8 + i * LANE;
    if (i) { g.strokeStyle = C.line; g.beginPath(); g.moveTo(0, y - 4.5); g.lineTo(LEFT - 2, y - 4.5); g.stroke(); }
    g.fillStyle = N[r.id] ? C.red : C.fg; g.font = "700 13px " + css("--display");
    g.fillText(N[r.id] ? NOUN(r.id) + " " + N[r.id] : r.id, 10, y + 12);
    g.fillStyle = C.muted; g.font = "11px " + css("--mono");
    const sub = L(r.method + " " + r.path.split("?")[0]); g.fillText(sub.length > (LEFT > 150 ? 26 : 11) ? sub.slice(0, LEFT > 150 ? 25 : 10) + "…" : sub, 10, y + 30);
    g.fillText((r.kind === "websocket" ? "closed " : "status ") + (r.status ?? "?"), 10, y + 46); });
  drawMini();
}
function drawMini() { const [g, w, h] = size(mini); g.fillStyle = C.bg; g.fillRect(0, 0, w, h);
  const mx = t => 8 + (t - T_MIN) / (T_END - T_MIN) * (w - 16);
  REQ.forEach((r, i) => { const y = 6 + i * ((h - 12) / REQ.length);
    if (r.start != null) { g.fillStyle = C.line2; g.fillRect(mx(r.start), y + 4, Math.max(1, mx(r.end ?? T_END) - mx(r.start)), 2); }
    IOS.filter(x => x.req === r.id).forEach(io => { g.fillStyle = C.amber; g.globalAlpha = .7; g.fillRect(mx(io.t0), y + 2, Math.max(1, mx(io.t1) - mx(io.t0)), 6); g.globalAlpha = 1; }); });
  if (C0) { g.fillStyle = "rgba(255,61,94,.2)"; g.fillRect(mx(C0.t0), 2, Math.max(2, mx(C0.t1) - mx(C0.t0)), h - 4); }
  const a = mx(view.t0), b = mx(view.t1); g.fillStyle = "rgba(6,8,11,.55)"; g.fillRect(0, 0, a, h); g.fillRect(b, 0, w - b, h);
  g.strokeStyle = C.cyan; g.lineWidth = 1.5; g.strokeRect(a + .5, 1.5, b - a - 1, h - 3); g.lineWidth = 1; }

function clampView() { const span = Math.min(Math.max(view.t1 - view.t0, 0.05), T_END - T_MIN);
  const t0 = Math.max(T_MIN, Math.min(view.t0, T_END - span)); view = {t0, t1: t0 + span}; }
function zoomAt(t, f) { view = {t0: t - (t - view.t0) * f, t1: t + (view.t1 - t) * f}; clampView(); draw(); }
function animateTo(t0, t1) { const s = {...view}, d = matchMedia("(prefers-reduced-motion: reduce)").matches ? 1 : 260, st = performance.now();
  const f = now => { const k = Math.min(1, (now - st) / d), e = 1 - Math.pow(1 - k, 3);
    view = {t0: s.t0 + (t0 - s.t0) * e, t1: s.t1 + (t1 - s.t1) * e}; clampView(); draw(); if (k < 1) requestAnimationFrame(f); };
  requestAnimationFrame(f); }
function focusOn(t) { const span = Math.min(view.t1 - view.t0, (T_END - T_MIN) / 2);
  if (t < view.t0 + span * .1 || t > view.t1 - span * .1 || (view.t1 - view.t0) > span) animateTo(t - span * .4, t + span * .6); }

main.addEventListener("wheel", e => { e.preventDefault(); const r = main.getBoundingClientRect(), w = main.clientWidth;
  if (e.shiftKey || Math.abs(e.deltaX) > Math.abs(e.deltaY)) { const dt = (e.deltaX || e.deltaY) / (w - LEFT) * (view.t1 - view.t0);
    view = {t0: view.t0 + dt, t1: view.t1 + dt}; clampView(); draw(); }
  else zoomAt(T(e.clientX - r.left, w), Math.exp(e.deltaY * 0.0015)); }, {passive: false});
let drag = null;
main.addEventListener("pointerdown", e => { drag = {x: e.clientX, v: {...view}, moved: false}; main.setPointerCapture(e.pointerId); });
main.addEventListener("pointermove", e => { const r = main.getBoundingClientRect();
  if (drag) { const dx = e.clientX - drag.x; if (Math.abs(dx) > 3) { drag.moved = true; main.classList.add("dragging"); }
    const dt = dx / (main.clientWidth - LEFT) * (drag.v.t1 - drag.v.t0); view = {t0: drag.v.t0 - dt, t1: drag.v.t1 - dt}; clampView(); draw(); return; }
  const x = e.clientX - r.left, y = e.clientY - r.top, hit = hits.slice().reverse().find(([, a, b, c, d]) => x >= a && x <= c && y >= b && y <= d);
  const it = hit ? hit[0] : null; if (it !== hover) { hover = it; draw(); }
  if (it) { tip.style.display = "block"; tip.innerHTML = tipHtml(it); tip.style.left = Math.min(e.clientX + 14, innerWidth - 370) + "px"; tip.style.top = (e.clientY + 14) + "px"; }
  else tip.style.display = "none"; });
main.addEventListener("pointerup", () => { main.classList.remove("dragging"); if (drag && !drag.moved && hover) select(hover); drag = null; });
main.addEventListener("pointerleave", () => { tip.style.display = "none"; if (hover) { hover = null; draw(); } });
let md = null;
mini.addEventListener("pointerdown", e => { const r = mini.getBoundingClientRect(), x = e.clientX - r.left;
  const t = T_MIN + (x - 8) / (mini.clientWidth - 16) * (T_END - T_MIN), span = view.t1 - view.t0;
  if (t < view.t0 || t > view.t1) { view = {t0: t - span / 2, t1: t + span / 2}; clampView(); draw(); }
  md = {x: e.clientX, v: {...view}}; mini.setPointerCapture(e.pointerId); });
mini.addEventListener("pointermove", e => { if (!md) return; const dt = (e.clientX - md.x) / (mini.clientWidth - 16) * (T_END - T_MIN);
  view = {t0: md.v.t0 + dt, t1: md.v.t1 + dt}; clampView(); draw(); });
mini.addEventListener("pointerup", () => { md = null; });
$("fit").onclick = () => animateTo(T_MIN, T_END);
$("zin").onclick = () => zoomAt((view.t0 + view.t1) / 2, .6);
$("zout").onclick = () => zoomAt((view.t0 + view.t1) / 2, 1.6);
if (C0) { const b = $("goconf"); b.hidden = false; b.onclick = () => { const pad = (C0.t1 - C0.t0) * .25 + .5; animateTo(C0.t0 - pad, C0.t1 + pad); }; }
$("detail").onclick = () => { every = !every; $("detail").setAttribute("aria-pressed", every); $("lg-steps").hidden = !every; draw(); };
addEventListener("keydown", e => { if (e.target.tagName === "INPUT") return; const mid = (view.t0 + view.t1) / 2, sp = view.t1 - view.t0;
  if (e.key === "+" || e.key === "=") zoomAt(mid, .7); else if (e.key === "-") zoomAt(mid, 1.4);
  else if (e.key === "ArrowRight") { view = {t0: view.t0 + sp * .15, t1: view.t1 + sp * .15}; clampView(); draw(); }
  else if (e.key === "ArrowLeft") { view = {t0: view.t0 - sp * .15, t1: view.t1 - sp * .15}; clampView(); draw(); }
  else if (e.key === "f" || e.key === "F") animateTo(T_MIN, T_END); });

// ---------------------------------------------------------------- details
const who = id => N[id] ? NOUN(id) + " " + N[id] + " (" + id + ")" : id;
const reqOf = id => D.requests.find(r => r.id === id) || {};
function NOUN(id) { return (D.requests.find(r => r.id === id) || {}).kind === "websocket" ? "Connection" : "Request"; }
const chips = ts => (ts || []).map(t => '<span class="touch ' + t.kind + '">' + t.kind.toUpperCase() + " " + esc(L(t.resource)) + "</span>").join("");
function tipHtml(it) {
  if (it.type === "io") return "<b>" + esc(it.friendly) + "</b><br>" + esc(who(it.req)) + " · " + (it.t1 - it.t0).toFixed(2) + " ms<br>→ " + esc(it.result);
  if (it.type === "touch") return "<b>" + esc(who(it.req)) + " " + it.kind + "s " + esc(L(it.resource)) + "</b><br>" + (it.kind === "read" ? "got " + esc(it.value) : "") + " at " + (it.t ?? 0).toFixed(2) + " ms";
  if (it.type === "step") return "<b>" + esc(who(it.req)) + "</b> code runs at " + (it.t ?? 0).toFixed(2) + " ms";
  return "<b>" + esc(who(it.id)) + "</b> " + esc(it.method) + " " + esc(it.path) + " → " + esc(it.status);
}
function select(it) {
  selected = it; draw(); if (!it) return;
  if (it.type === "io") { const d = it.detail || {}, r = reqOf(it.req);
    insp.innerHTML = '<header><div class="kind io">Recorded call · ' + esc(it.op) + '</div><h3>' + esc(it.friendly) + '</h3></header><dl>' +
      '<dt>' + (r.kind === "websocket" ? "Connection" : "Request") + '</dt><dd>' + esc(who(it.req)) + " · " + esc(r.method) + " " + esc(r.path) + '</dd>' +
      (d.message ? '<dt>Message</dt><dd><pre>' + esc(d.message) + '</pre></dd>' : '') +
      '<dt>Waited</dt><dd>' + it.t0.toFixed(2) + " to " + it.t1.toFixed(2) + " ms (" + (it.t1 - it.t0).toFixed(2) + ' ms)</dd>' +
      (d.sql ? '<dt>SQL</dt><dd><pre>' + esc(d.sql) + '</pre></dd><dt>Params</dt><dd>' + esc(JSON.stringify(d.params)) + '</dd>' : '') +
      (d.url ? '<dt>HTTP</dt><dd><pre>' + esc(d.method + " " + d.url + (d.body ? "\n" + d.body : "")) + '</pre></dd>' : '') +
      (d.args ? '<dt>Command</dt><dd><pre>' + esc(d.args.join(" ")) + '</pre></dd>' : '') +
      (d.function ? '<dt>Function</dt><dd>' + esc(d.function) + " " + esc(d.args || "") + '</dd>' : '') +
      '<dt>Answer</dt><dd><pre>' + esc(it.result) + '</pre></dd><dt>Touched</dt><dd>' + (chips(it.touches) || "nothing Déjà could name") + '</dd>' +
      '<dt>Replay</dt><dd>answered from the recording, no real call</dd></dl>'; }
  else if (it.type === "touch") { insp.innerHTML = '<header><div class="kind ' + (it.kind === "write" ? "bad" : "") + '">' + it.kind + '</div><h3>' + esc(who(it.req)) + " " + it.kind + "s " + esc(L(it.resource)) + '</h3></header><dl>' +
      '<dt>At</dt><dd>' + (it.t ?? 0).toFixed(2) + ' ms</dd>' + (it.kind === "read" ? '<dt>Got</dt><dd>' + esc(it.value) + '</dd>' : '<dt>Wrote</dt><dd>' + esc(JSON.stringify(it.writes)) + '</dd>') +
      '<dt>Source</dt><dd>' + (it.source === "reported by the app" ? "in-memory state (tracked, or deja.touch)" + (it.thread ? " · in a worker thread" : "") : esc(it.source) + (it.op ? " · " + esc(it.op) : "")) + '</dd><dt>Resource</dt><dd>' + esc(it.resource) + '</dd></dl>'; }
  else if (it.type === "step") { insp.innerHTML = '<header><div class="kind">Code running</div><h3>' + esc(who(it.req)) + '</h3></header><dl><dt>Task</dt><dd>' + esc(it.task) + '</dd><dt>At</dt><dd>' + (it.t ?? 0).toFixed(2) + ' ms</dd><dt>Replay</dt><dd>replay forces this exact turn order</dd></dl>'; }
  else if (it.kind === "websocket") { insp.innerHTML = '<header><div class="kind req">WebSocket connection</div><h3>' + esc(it.path) + '</h3></header><dl>' +
      '<dt>Connection</dt><dd>' + esc(who(it.id)) + '</dd><dt>Time</dt><dd>' + (it.start ?? 0).toFixed(2) + " to " + (it.end ?? 0).toFixed(2) + ' ms</dd>' +
      '<dt>Close code</dt><dd>' + esc(it.status ?? "still open") + '</dd><dt>Sent</dt><dd><pre>' + esc(it.response || "(nothing)") + '</pre></dd></dl>'; }
  else { insp.innerHTML = '<header><div class="kind req">Request</div><h3>' + esc(it.method) + " " + esc(it.path) + '</h3></header><dl>' +
      '<dt>Request</dt><dd>' + esc(who(it.id)) + '</dd><dt>Time</dt><dd>' + (it.start ?? 0).toFixed(2) + " to " + (it.end ?? 0).toFixed(2) + ' ms</dd>' +
      '<dt>Body</dt><dd><pre>' + esc(it.body || "(empty)") + '</pre></dd><dt>Status</dt><dd>' + esc(it.status) + '</dd><dt>Response</dt><dd><pre>' + esc(it.response) + '</pre></dd></dl>'; }
}

// ---------------------------------------------------------------- moments + play
const ol = $("moments");
if (!MOMENTS.length) { ol.innerHTML = '<li style="cursor:default">No key moments: Déjà did not find two requests reading and writing the same thing out of order.</li>'; $("mnote").hidden = true; }
else ol.innerHTML = MOMENTS.map((m, i) => '<li class="' + (m.warning ? "warn" : "") + '" data-i="' + (i + 1) + '" tabindex="0"><span>' + esc(m.text) +
  (m.warning ? '<span class="w">' + esc(m.warning) + '</span>' : '') + '</span></li>').join("");
function goMoment(n) { cursor = n; $("step").value = n; $("pos").textContent = MOMENTS.length ? "moment " + n + " / " + MOMENTS.length : "";
  ol.querySelectorAll("li").forEach(li => li.classList.toggle("on", +li.dataset.i === n));
  if (n) { const m = MOMENTS[n - 1]; focusOn(m.t);
    const t = TOUCH.find(x => x.seq === m.seq) || IOS.find(x => x.seq === m.seq); if (t) select(t); else draw(); } else draw(); }
ol.addEventListener("click", e => { const li = e.target.closest("li[data-i]"); if (li) goMoment(+li.dataset.i); });
ol.addEventListener("keydown", e => { if (e.key === "Enter") { const li = e.target.closest("li[data-i]"); if (li) goMoment(+li.dataset.i); } });
$("step").max = MOMENTS.length; $("step").oninput = () => goMoment(+$("step").value);
let timer = null;
$("play").onclick = () => { if (timer) { clearInterval(timer); timer = null; $("play").textContent = "Play"; return; }
  if (!MOMENTS.length) return; $("play").textContent = "Pause"; if (cursor >= MOMENTS.length) goMoment(0);
  timer = setInterval(() => { if (cursor >= MOMENTS.length) { clearInterval(timer); timer = null; $("play").textContent = "Play"; return; } goMoment(cursor + 1); }, 1100); };

// ---------------------------------------------------------------- resources
const RES = [...new Set(TOUCH.filter(t => t.kind === "read" || t.kind === "write").map(t => t.resource))];
$("res").querySelector("thead").innerHTML = "<tr><th>Resource</th>" + REQ.map(r => "<th>" + esc(N[r.id] ? NOUN(r.id) + " " + N[r.id] : r.id) + " · " + esc(L(r.method + " " + r.path.split("?")[0])) + "</th>").join("") + "</tr>";
const tb = $("res").querySelector("tbody");
tb.innerHTML = RES.map(res => "<tr data-res=\"" + esc(res) + "\" class=\"" + (C0 && res === C0.resource ? "hot" : "") + "\" tabindex=\"0\"><td>" + esc(L(res)) + (L(res) !== res ? '<br><span style="color:var(--muted)">' + esc(res) + "</span>" : "") + "</td>" +
  REQ.map(r => "<td>" + (TOUCH.filter(t => t.req === r.id && t.resource === res && (t.kind === "read" || t.kind === "write")).map(t =>
    '<span class="acc ' + t.kind + '">' + t.kind[0].toUpperCase() + " " + (t.t ?? 0).toFixed(1) + "</span>").join("") || '<span style="color:var(--muted)">·</span>') + "</td>").join("") + "</tr>").join("")
  || '<tr><td colspan="' + (REQ.length + 1) + '" style="color:var(--muted)">Nothing named. Postgres, httpx and Redis calls are named automatically; in-memory state needs deja.touch().</td></tr>';
function setFilter(res) { filter = filter === res ? null : res; tb.querySelectorAll("tr").forEach(tr => tr.classList.toggle("sel", tr.dataset.res === filter));
  $("clear").hidden = !filter; $("clear").textContent = filter ? "Clear filter: " + L(filter) : "Clear filter"; draw(); }
tb.addEventListener("click", e => { const tr = e.target.closest("tr[data-res]"); if (tr) setFilter(tr.dataset.res); });
tb.addEventListener("keydown", e => { if (e.key === "Enter") { const tr = e.target.closest("tr[data-res]"); if (tr) setFilter(tr.dataset.res); } });
$("clear").onclick = () => setFilter(filter);

addEventListener("resize", draw);
(document.fonts ? document.fonts.ready : Promise.resolve()).then(draw);
goMoment(0); draw();
if (MOMENTS.length) { const k = MOMENTS.findIndex(m => m.warning); goMoment(k >= 0 ? k + 1 : 1); }
})();
</script>
</body></html>
"""


def build(header: dict, events: list, replay=None, source: str = "") -> str:
    """``replay``: None, a :class:`deja.replay.ReplayResult`, or ``{"times", "matched", "summary"}``."""
    if replay is not None and not isinstance(replay, dict):  # one ReplayResult
        replay = {"times": 1, "matched": int(replay.followed and replay.same_responses), "summary": replay.summary()}
    data = analyze(header, events)
    data["source"] = source
    data["replay"] = replay
    payload = json.dumps(data, default=repr).replace("</", "<\\/")
    return PAGE.replace("__DATA__", payload).replace("__SOURCE__", html.escape(source))


def write_report(recording: str, out: str | None = None, replay=None) -> str:
    header, events = read(recording)
    out = out or os.path.splitext(recording)[0] + ".html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(build(header, events, replay, source=os.path.basename(recording)))
    return out
