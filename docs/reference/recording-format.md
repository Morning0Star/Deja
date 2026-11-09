# Recording format

A `.deja` file is gzipped [JSON Lines](https://jsonlines.org/): one header object on the first line, then one event
per line as a JSON array. Read and write recordings with [`deja.fileformat.read`](python-api.md#deja.fileformat.read)
and [`write`](python-api.md#deja.fileformat.write) rather than by hand; they handle the encoding below and older
formats.

```python
from deja.fileformat import read

header, events = read("wallet-fail.deja")
header["reason"]      # 'invariant violated: balance=0 charged=200'
events[0]             # ('Q', 1, 'r0.0', 'POST', '/purchase', [...], b'{"amount":100}', ...)
```

## Encoding

- `bytes` values are written as `{"b64": "<base64>"}` and read back as `bytes`.
- Tuples are written as arrays; `read` returns each event as a tuple.
- Sets are written as sorted arrays.

## Header

| Field | Meaning |
|---|---|
| `format` | the layout, `deja/1` |
| `deja` | the Déjà version that wrote it |
| `reason` | why it was saved: `status 500`, `exception ValueError`, `flag: ...`, `close code 1011` |
| `request` | the id of the request that triggered it |
| `python`, `implementation`, `platform`, `loop` | where it was recorded |
| `hash_seed` | the `PYTHONHASHSEED` the app ran with, or `null` if it was random |
| `started_at` | when the recorder started (Unix time) |
| `events_capacity` | the recorder's buffer size |
| `labels`, `call_kinds` | what [`deja.labels`](python-api.md#deja.labels) and [`deja.calls`](python-api.md#deja.calls) set |
| `catalog` | what replay needs beyond the events: results of startup calls, column descriptions, background task names, source fingerprints of the app's modules, the tracking rules |

Tools add their own fields: `deja shrink` writes `shrunk_from` and `removed_requests`, `deja merge` writes `workers`.

## Events

Each event is an array whose first element is a one-letter code. The leading fields are:

| Code | Fields | Meaning |
|---|---|---|
| `B` | count | a loop pass is about to run `count` ready callbacks |
| `S` | task path | one step of that task ran |
| `P` | | a callback that isn't a task step ran (future callbacks, `gather`, `call_soon`) |
| `N` | path, parent path, coroutine name | a task was created |
| `G` | id | a task was garbage-collected |
| `R` | task path, source, value | a clock, random or UUID read inside a request (`time.time`, `os.urandom`...) |
| `Q` | request id, outer task path, method, path, headers, body | a request started; its task is now `q<id>` |
| `A` | request id, status, response body | a request finished |
| `X` | request id or task path, exception type, message | an exception escaped a request |
| `F` | task path, reason | [`deja.flag()`](python-api.md#deja.flag) was called |
| `I` | task path, operation | a recorded call started |
| `O` | task path, operation, arguments digest, `"ok"` or `"err"`, result | that call finished |
| `T` | task path, resource, `"read"` or `"write"`, value | an access to shared state |
| `M` | label | a marker from [`Recorder.mark()`](python-api.md#deja.Recorder.mark) |
| `L` | listener id, source, channel, payload | a message was pushed to a listener outside requests (Postgres `NOTIFY`, Redis pub/sub, Kafka) |

Many events carry more fields after these: timestamps (`perf_counter_ns`, the clock named in the header's `clock`
field), and details such as a call's arguments. A tool reading recordings should use the leading fields and ignore
the rest.

In a `Q` event, the method `WS` marks a WebSocket connection and `JOB` a [`deja.scope`](python-api.md#deja.scope.scope)
block (its path is the scope's name). Background tasks have no `Q` event; their steps carry `b` paths.

## Task paths

| Path | Is |
|---|---|
| `q3` | request 3's task |
| `q3.0`, `q3.1` | the first and second tasks it created |
| `q3.t0` | its first call into a worker thread |
| `b0` | the app's first background task |
| `r0.1` | a task outside request scope (the server's, a test's) |

## Versions

The format is versioned so recordings keep working as Déjà changes. Reading a recording upgrades an older format
step by step to the current one. A recording written by a *newer* Déjà is refused with a message saying so, rather
than replayed wrongly. Until 1.0, a minor version of Déjà may introduce a new format; old recordings stay readable.
