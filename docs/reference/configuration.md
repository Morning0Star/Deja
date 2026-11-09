# Configuration

Déjà has no configuration file. Everything is set in code, through the middleware's options and the recorder, plus
one environment variable.

## Middleware options

| Option | Default | Meaning |
|---|---|---|
| `directory` | `"deja-recordings"` | folder for recordings, created when the first one is written |
| `on_dump` | `None` | `on_dump(path, reason)` is called after each recording is written |
| `track` | `True` | track the shared state the routes' code reaches ([Shared state](../guides/shared-state.md)) |
| `track_skip` | `()` | `fnmatch` patterns of state names or classes not to track |
| `track_only` | `None` | if set, track only state matching these patterns |
| `background` | `True` | record tasks started at startup as [background tasks](../guides/background-tasks.md) |
| `group` | `None` | shared folder for a [group of workers](../guides/workers.md); defaults to `DEJA_GROUP` |
| `worker` | `pid<process id>` | this worker's name in the group |

`deja.aiohttp_web.deja_middleware` takes the first five. `deja.scope` takes `directory` and `on_dump`.

## The recorder

The recording loop creates a [`Recorder`](python-api.md#deja.Recorder) with default settings. To change them, create
one yourself and give it to the loop.

### Buffer size

```python
deja.Recorder(max_events=1_000_000)
```

The ring buffer holds the last `max_events` events, 200,000 by default: about 15,000 to 20,000 requests of a typical
API, in tens of megabytes. A request whose start has left the buffer when a recording is saved can't be replayed,
so raise it if requests or WebSocket connections live long, or if a race spans a long stretch of traffic.

### Redacted headers

```python
deja.Recorder(
    redact_headers=("authorization", "cookie", "x-api-key", "x-session-token")
)
```

Request headers in this list (case-insensitive) are recorded as `<redacted>`. The default list is `authorization`,
`cookie` and `x-api-key`. Passing `redact_headers` replaces it, so include those names when adding more.

Headers are the only thing redacted. Bodies, query parameters and the answers to the app's calls are recorded as they
are; see [Security](../security.md).

### Giving the loop a recorder

`recording_loop` takes the recorder as its argument. A loop factory is called with no arguments, so wrap it:

=== "uvicorn"

    ```python
    # myapp/deja_loop.py
    import deja

    def loop():
        return deja.recording_loop(deja.Recorder(max_events=1_000_000))
    ```

    ```bash
    uvicorn myapp:app --loop myapp.deja_loop:loop
    ```

=== "asyncio.run"

    ```python
    import functools
    import deja

    recorder = deja.Recorder(max_events=1_000_000)
    factory = functools.partial(deja.recording_loop, recorder)
    asyncio.run(main(), loop_factory=factory)
    ```

## Environment variables

| Variable | Meaning |
|---|---|
| `DEJA_GROUP` | default for the middleware's `group` option |
| `PYTHONHASHSEED` | see below |

### Hash seed

The iteration order of a `set` of strings (and of a dict built from one) depends on Python's hash seed, which is
random for every process unless `PYTHONHASHSEED` is set. An app whose behaviour depends on such an order can only
replay it exactly with the same seed.

The recording stores the seed the app ran with. If it was fixed, the CLI replays with the same value by running
itself again with it; in Python, `ReplayResult.summary()` notes the difference. If production runs with a random
seed, set a fixed `PYTHONHASHSEED` on the instances you record to get exact set ordering in replay.

## Supported Pythons

Déjà records on the Python versions whose event loop internals it has been checked against: CPython 3.12, 3.13,
3.14 and 3.15, and PyPy 3.12. On any other version `recording_loop()` warns and returns a plain loop, so the app runs
unrecorded rather than recorded wrongly.
