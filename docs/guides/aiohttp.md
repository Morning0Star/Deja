# aiohttp servers

aiohttp's web server isn't ASGI, so it has its own middleware,
[`deja.aiohttp_web.deja_middleware`](../reference/python-api.md#deja.aiohttp_web.deja_middleware). It records the same
things and saves on the same triggers as [`DejaMiddleware`](asgi.md).

## Record

```python
import deja
from aiohttp import web
from deja.aiohttp_web import deja_middleware

def create_app():
    app = web.Application(
        middlewares=[deja_middleware(directory="deja-recordings")]
    )
    app.add_routes(routes)
    return app

if __name__ == "__main__":
    web.run_app(create_app(), loop=deja.recording_loop())
```

A recording is saved when a handler returns a 5xx, raises, or calls [`deja.flag()`](../reference/python-api.md#deja.flag).
The aiohttp *client* (`aiohttp.ClientSession`) is recorded separately, by its [adapter](../reference/adapters.md), in
any kind of app.

## Replay

```bash
deja replay deja-recordings/<file>.deja --app myapp:create_app
```

`--app` takes an `aiohttp.web.Application` or a function returning one. Replay serves the app on a free local port
and sends the recorded requests to it, with a header (`X-Deja-Replay-Request`) that tells Déjà which recorded request
each one is. Nothing else listens on that port, and the app's own outgoing calls are still answered from the
recording.

## Options

`deja_middleware` takes `directory`, `on_dump`, `track`, `track_skip` and `track_only`, with the same meaning as for
[`DejaMiddleware`](../reference/python-api.md#deja.DejaMiddleware). Background tasks and worker groups are only
supported through the ASGI middleware.
