# Visualisation and "what did each request touch": research and design (2026-11-08)

## 1. Research

### How existing tools show this kind of data

| Tool | What it shows | Ideas worth taking |
|---|---|---|
| Distributed tracing (Jaeger, Grafana Tempo) | Waterfall: time on the x axis, one row per span, bar length = duration; colour by service or error; Jaeger also colours the critical path | Time axis with real durations; gaps between bars mean waiting; errors marked on the bar |
| Perfetto / Chrome trace viewer | Zoomable, pannable timeline of slices, instant events, counters and *flow arrows* between slices; reads Chrome JSON | A free, powerful viewer for an export; flow arrows for "this I/O result woke that task" |
| Replay.io (time-travel debugger) | A timeline with a draggable *focus window*, scrubbing through an execution | Focus window and scrubber for replay |
| Pernosco (rr-based) | Searching state over time; threads, stacks, data, source | Search "everything that touched X" |
| Raceway (mode7labs, alpha, 0 stars, MIT) | Vector-clock race detection, web UI with zoomable timeline, causal tree, race highlighting, variable audit trail | The conflict rule; a per-variable audit trail |

**Raceway is the closest prior art for the "what did it touch" idea, and it differs from Déjà in three ways.**
- **Capture is manual.** Its README says Python apps call `client.track_state_change("user.balance", ..., "Read")` themselves; automatic Python instrumentation is "planned".
- **It needs a server.** Events are sent to a Rust server over HTTP.
- **It has no replay.** It detects and visualises, but it can't reproduce a failing interleaving.

Déjà captures I/O automatically, has the exact interleaving (one event loop, so the recorded order *is* the order), and can replay. Its conflict rule is the standard one: two accesses conflict when they're concurrent, touch the same thing, and at least one is a write. Déjà can apply it to the recorded resources directly.

### Building blocks

- **Timeline rendering.**
  - DOM-based timelines (vis-timeline) handle tens to hundreds of items.
  - Canvas scales to 100k+.
  - [flame-chart-js](https://github.com/pyatyispyatil/flame-chart-js) (MIT, canvas, zoom, marks, waterfall) is a good reference, but it has no flow arrows or swimlanes.
  - A small custom canvas renderer fits Déjà's lanes + arrows + conflicts best.
- **Perfetto export.** Chrome JSON with `X` slices, `I` instants and `s/t/f` flows opens directly in [ui.perfetto.dev](https://perfetto.dev/docs/getting-started/other-formats). A page can hand a trace to it with `window.open` + `postMessage` ([deep linking](https://perfetto.dev/docs/visualization/deep-linking-to-perfetto-ui)). Traces stay in the browser and are "not uploaded anywhere".
- **What a SQL query touched.**
  - [sqlglot](https://github.com/tobymao/sqlglot) parses SQL in pure Python.
  - [sql-glider](https://pypi.org/project/sql-glider/) shows the pattern on top of it: tables classified as read (SELECT, JOIN), written (INSERT, UPDATE, DELETE) or both.
  - Déjà goes one level finer: with the query parameters it can name the row (`gallery_wallet[id=1]`) when the WHERE clause is an equality on a key.

## 2. What Déjà records today, and the gaps

| Needed for the viewer | Today |
|---|---|
| Order of turns | Yes (exact) |
| Real time of each turn and each I/O call (for a time axis and zoom) | **No**: only order, no timestamps |
| Query text and parameters | **No**: only a fingerprint is kept in the file |
| What each call touched (table, row, Redis key, URL) | **No** |
| In-memory shared state (a Python dict or object both requests use) | **No**: invisible to Déjà today |

## 3. Design

### 3.1 Capture (recorder changes)

1. **Time.** One clock read per loop pass, plus start and end time for each I/O call. That's cheap: a few reads per pass, not per step.
2. **Keep query text and parameters in the file** (truncated), with redaction rules for sensitive columns and parameters.
3. **Resources ("touches").** Each I/O call is turned into `(resource, read|write)`:
   - SQL via sqlglot: the table, plus the row when the key is an equality (`wallet[id=1]`). `SELECT`/`FOR UPDATE` count as read/lock; `INSERT`/`UPDATE`/`DELETE` as write.
   - Redis: the key and the command (`GET` = read, `SET`/`INCR` = write).
   - HTTP: `METHOD host/path` (`GET` = read, other methods = write).
   - Worker threads: the function name (unknown kind).
4. **In-memory state:** an explicit `deja.touch("wallet:1", "read")` API. Also, optionally, tracked containers that report reads and writes automatically. This is the part Raceway does manually; Déjà offers both.

### 3.2 Analysis

- **Per request:** everything it read and wrote, in order.
- **Conflicts:** request A reads R, then request B reads or writes R, then A writes R, all before A finishes. That's a lost update or double spend. More generally, two interleaved requests touching the same resource where at least one writes. Each conflict links the exact steps involved.
- **Waiting:** time each request spent running vs waiting on the database, HTTP and so on.

### 3.3 Viewer (`deja report`, interactive, still one self-contained HTML file)

- **Timeline (canvas):**
  - one lane per request, with child tasks as sub-lanes;
  - a real time axis;
  - solid bars for "running" and hatched bars for "waiting on pg.fetchval";
  - markers for start, end and flag;
  - **conflict highlights**, a red bracket plus an arrow between the steps that clash;
  - optional flow arrows for "this result woke that task".
- **Navigation:**
  - wheel or pinch to zoom around the cursor, drag to pan;
  - a minimap overview bar with a draggable focus window;
  - "fit", "zoom to request" and "zoom to conflict" buttons;
  - keyboard shortcuts.
- **Click anything** to open a details panel: the step number and task, timing, the I/O call, SQL text and parameters, the recorded result (rows), and the resources it touched.
- **Resources view:** a matrix of resources × requests with R/W marks in order. Click a resource to filter the timeline to everything that touched it (an audit trail, like Raceway's).
- **Conflicts panel** at the top, in plain words: "q13 and q14 both read `gallery_wallet[id=1]` before either wrote it". Click to jump.
- **Replay verdict and scrubber:** if replayed, a cursor you can drag through the recorded order. A step-by-step "replay to here" comes later.
- **Export:** `deja export --perfetto` for anyone who wants the full Perfetto UI.

### 3.4 Phases

1. Capture: time, query text and parameters, redaction, resources, `deja.touch`.
2. Analysis: per-request touches, conflicts, waiting.
3. Viewer: canvas timeline, zoom/pan/minimap, details panel, resources matrix, conflicts panel.
4. Perfetto export.
5. Later: `deja ui` (a local dashboard of all recordings with a Replay button), step-through replay.

## 4. Risks

- **Overhead:** timestamps and resource parsing must stay off the hot path. Parse SQL at dump or view time, not while recording.
- **Privacy:** query parameters and rows can contain personal data. Redaction defaults matter, and recordings stay local.
- **SQL parsing limits:** dynamic SQL, CTEs and ORMs. Unknown shapes fall back to table-level, or "unknown".
- **In-memory state is only visible if the app reports it** (`deja.touch`) or uses tracked containers.
