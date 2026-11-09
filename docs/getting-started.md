# Getting started

This page takes you from installing Déjà to replaying a real race, first on an example app that ships with the
repository, then on your own.

## Install

```bash
pip install deja-replay
```

The distribution is `deja-replay`; the import name and the command are both `deja`. Déjà needs Python 3.12 or newer
and one dependency, [sqlglot](https://github.com/tobymao/sqlglot), for reading SQL. Support for each database and
HTTP library switches on by itself when that library is installed.

## See a race replayed

The repository has a *race gallery*: small FastAPI apps with real races in them. Clone it and install the gallery's
dependencies:

```bash
git clone https://github.com/Morning0Star/Deja && cd Deja
pip install -e ".[gallery]"
```

### 1. Record

`gallery.stress` sends pairs of overlapping requests to an app running on Déjà's recording loop and checks the
app's invariant after each trial. Here the wallet starts at 100 and two purchases of 100 run at once; only one
should succeed.

```console
$ python -m gallery.stress wallet --trials 30 --record recordings/
  wallet  violated   15/30 ( 50.0%)  median trial   53.4 ms  e.g. balance=0 charged=200
```

Every trial that broke the invariant was saved as `recordings/wallet-fail-trialNNNN.deja`. How many fail differs
from run to run; that is the point.

### 2. Look at it

```console
$ deja show recordings/wallet-fail-trial0000.deja
  reason: invariant violated: balance=0 charged=200
  ...
requests:
  q0  POST /reset  -> 200  body=b''
  q1  POST /purchase  -> 200  body=b'{"amount":100}'
  q2  POST /purchase  -> 200  body=b'{"amount":100}'
  q3  GET /check  -> 200  body=b''
interleaving of request tasks (one line per loop pass):
  [start q0]
  [start q1] [start q2]
  q2 q1
  q1
  q1 q2
  q2
  [start q3]
```

`deja report` writes the same recording as an HTML page that explains the race:

```bash
deja report recordings/wallet-fail-trial0000.deja --app gallery.wallet:create_app
```

> Two purchase requests (A and B) ran at the same time. A read the wallet balance and got 100. Before A saved its
> change, B also read the wallet balance and got 100. Both then saved their own result (A: 0; B: 0). A saved last
> and overwrote B's change, so one update was lost. This is a lost update on the wallet balance.

### 3. Replay it

```console
$ deja replay recordings/wallet-fail-trial0000.deja --app gallery.wallet:create_app --times 100
followed 10/10 recorded steps
  ...
100/100 replays matched the recording
```

Every replay takes the recorded steps in the recorded order, and the app answers exactly as it did when the bug
happened. `--app` names the app as `module:attribute`; here `create_app` is a factory, called once per replay so each
replay starts from fresh state.

## Add Déjà to your app

=== "FastAPI / Starlette"

    ```python
    import deja
    from fastapi import FastAPI

    app = FastAPI()
    app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")
    ```

    ```bash
    uvicorn myapp:app --loop deja.loop:recording_loop
    ```

=== "Other ASGI frameworks"

    Wrap the ASGI app: Litestar, Quart and Django (ASGI) all work the same way.

    ```python
    import deja
    application = deja.DejaMiddleware(
        get_asgi_application(), directory="deja-recordings"
    )
    ```

    ```bash
    uvicorn mysite.asgi:application --loop deja.loop:recording_loop
    ```

=== "aiohttp"

    ```python
    import deja
    from aiohttp import web
    from deja.aiohttp_web import deja_middleware

    app = web.Application(
        middlewares=[deja_middleware(directory="deja-recordings")]
    )
    web.run_app(app, loop=deja.recording_loop())
    ```

=== "Queue consumer"

    ```python
    import asyncio
    import deja

    async def consume():
        async for msg in consumer:
            async with deja.scope("orders", msg.value) as job:
                job.result = await handle_order(msg.value)

    asyncio.run(consume(), loop_factory=deja.recording_loop)
    ```

Two things are needed: the middleware (or `deja.scope`) marks what a request is, and the **recording loop** records
the order in which tasks run. Without the recording loop the middleware does nothing and warns once.

!!! note "uvicorn version"
    `--loop module:factory` needs uvicorn 0.36 or newer. With an older uvicorn, start the server from Python:
    `asyncio.run(server.serve(), loop_factory=deja.recording_loop)`.

From then on, a request that returns a 5xx or raises is saved to `deja-recordings/`. So is any request in which
your code calls [`deja.flag`](reference/python-api.md#deja.flag), which is how to catch a bug that doesn't raise:

```python
if wallet.balance < 0:
    deja.flag("balance went negative")
```

To replay one, point `--app` at your app or at a function that builds it:

```bash
deja replay deja-recordings/<file>.deja --app myapp:create_app
```

Replay with the app configured the way it was recorded: same settings, same feature flags. Your database and the
services you call don't have to be running; their answers come from the recording.

!!! warning "Recordings hold your data"
    A recording contains request bodies, query parameters and the rows and responses your app received. Treat it
    like a database dump. See [Security](security.md).

## Next

- [How it works](how-it-works.md)
- [Reading a report](guides/reading-reports.md)
- [Fixing a race](guides/fixing-races.md)
