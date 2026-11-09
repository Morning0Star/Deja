# ASGI apps

Déjà works with any ASGI app through one middleware, [`deja.DejaMiddleware`](../reference/python-api.md#deja.DejaMiddleware).
It is tested with FastAPI, Starlette, Litestar, Quart and Django.

## Add the middleware

=== "FastAPI / Starlette"

    ```python
    import deja
    from fastapi import FastAPI

    def create_app():
        app = FastAPI()
        app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")
        # ... routes ...
        return app

    app = create_app()
    ```

=== "Litestar / Quart"

    Wrap the ASGI app:

    ```python
    import deja

    app = deja.DejaMiddleware(create_app(), directory="deja-recordings")
    ```

=== "Django"

    In `asgi.py`:

    ```python
    import deja
    from django.core.asgi import get_asgi_application

    application = deja.DejaMiddleware(
        get_asgi_application(), directory="deja-recordings"
    )
    ```

## Run it on the recording loop

The middleware marks what a request is; the recording loop records the order in which tasks run. Both are needed.

=== "uvicorn"

    ```bash
    uvicorn myapp:app --loop deja.loop:recording_loop
    ```

    `--loop module:factory` needs uvicorn 0.36 or newer.

=== "From Python"

    ```python
    import asyncio
    import uvicorn
    import deja

    server = uvicorn.Server(uvicorn.Config("myapp:app", port=8000))
    asyncio.run(server.serve(), loop_factory=deja.recording_loop)
    ```

On any other loop the middleware passes requests straight through and warns once:
`deja: not running on a recording loop; requests are not recorded`.

!!! warning "uvloop"
    Déjà records on the standard asyncio loop. If you use uvloop in production, run the instance you record on with
    the standard loop.

## When a recording is saved

A recording is written to `directory` when any of these happen during a request:

| Trigger | Recording's `reason` |
|---|---|
| The response status is 500 or higher | `status 500` (the status) |
| An exception escapes the app | `exception ValueError` (the exception's type) |
| App code calls [`deja.flag("...")`](../reference/python-api.md#deja.flag) | `flag: ...` |

Use `deja.flag` for bugs that don't fail on their own: an invariant your code can check, such as a negative balance
or a seat sold twice.

To act on each new recording (log it, upload it, alert someone), pass `on_dump`:

```python
def saved(path, reason):
    log.warning("deja saved %s (%s)", path, reason)

app.add_middleware(deja.DejaMiddleware, directory="deja-recordings", on_dump=saved)
```

## Replay

Give `--app` the module and attribute of your app, or of a function that builds it:

```bash
deja replay deja-recordings/<file>.deja --app myapp:create_app
```

A factory is better than a module-level app: replay calls it once per replay (`--times N` replays N times), and each
replay needs fresh state. `--app` accepts:

- a function whose name starts with `create`, `make` or `build`: a factory, called with no arguments for each replay;
- anything else at that attribute: the app itself, reused as it is.

`--app myapp` without an attribute means `myapp:app`. The module is imported from the current directory first, as
uvicorn does.

The app's startup (lifespan) runs before the requests are sent again, as it did in production, with the recorded
answers to the calls it made.

Configure the app the way it was recorded: same settings, same feature flags, same choice of database driver. The
database doesn't need to be reachable. If the app's code changed since the recording, replay says which modules
differ.

## What the middleware records

For each HTTP request: method, path and query string, headers and the full body (up to 1 MiB; a larger body is cut
and marked). The `Authorization`, `Cookie` and `X-Api-Key` headers are recorded as `<redacted>`; to change that list
see [Configuration](../reference/configuration.md#redacted-headers). Then the response status and up to 64 KiB of the
response body, which replay compares.

WebSocket connections are recorded too; see [WebSockets](websockets.md).

## Options

All options are listed in the [API reference](../reference/python-api.md#deja.DejaMiddleware). The ones you are most
likely to need:

| Option | Default | Use it to |
|---|---|---|
| `directory` | `"deja-recordings"` | choose where recordings go |
| `on_dump` | `None` | be told about each recording |
| `track`, `track_skip`, `track_only` | `True`, `()`, `None` | control [shared-state tracking](shared-state.md) |
| `background` | `True` | record [background tasks](background-tasks.md) started at startup |
| `group`, `worker` | `None` | record a race across [several worker processes](workers.md) |
