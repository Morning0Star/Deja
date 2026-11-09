# Background tasks

Many apps start long-running tasks at startup: a loop that refreshes a cache, tops up stock, flushes metrics or
listens for notifications. These tasks race with requests as easily as requests race with each other.

## What is recorded

With an ASGI app, every task the app creates during startup (the ASGI *lifespan*: FastAPI's `lifespan`, Starlette's
`on_startup`) is a *background task*. Déjà numbers them in creation order, `b0`, `b1`, ..., records their steps
like a request's, and the report calls them by their function's name.

```python
@contextlib.asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(restock())    # recorded as b0, "restock"
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")
```

If a request and `restock` both read and then write the same stock count, the report shows a lost update between
the request and "background task restock".

Recording background tasks is on by default; pass `DejaMiddleware(app, background=False)` to leave them out. Tasks
started later, from inside a request, are part of that request (`q3.0`, `q3.1`...), not background tasks.

## How replay handles them

A background task has usually been running long before the recording window opens. Replay runs the app's startup
again, so the task starts again too, and lets it run freely, using the answers its calls got at startup, until
the recording window begins. From there on its steps are put in the recorded order like a request's.

So the task's state before the window is rebuilt, not recorded: if the task did something before the window that
depended on outside answers the recording doesn't hold, replay may differ, and says where.

## aiohttp and jobs

Background tasks are recorded through the ASGI lifespan only. In an aiohttp app or a consumer, wrap each round of
the background work (one refresh, one flush) in a [`deja.scope`](jobs.md) block to record it as a job.
