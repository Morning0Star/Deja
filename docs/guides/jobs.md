# Queue consumers and jobs

Races don't only happen in web requests. Two Kafka messages handled at once, two scheduled jobs that overlap, a
Redis stream consumer with several handlers in flight: [`deja.scope`](../reference/python-api.md#deja.scope.scope) records
each unit of work as if it were a request.

## Record

Wrap each job in `deja.scope(name, payload)` and put what it produced in `job.result`:

```python
import asyncio
import deja

async def handle_order(order):
    ...
    return {"reserved": order["id"]}

handlers = {"orders": handle_order}   # what replay will call

async def consume(consumer):
    async for msg in consumer:
        asyncio.create_task(one(msg.value))

async def one(order):
    async with deja.scope("orders", order) as job:
        job.result = await handlers["orders"](order)

asyncio.run(main(), loop_factory=deja.recording_loop)
```

A recording is saved when an exception leaves the block (the exception still propagates) or when code inside it
calls [`deja.flag()`](../reference/python-api.md#deja.flag). Outside a recording loop, `deja.scope` does nothing and
the block runs as usual.

!!! note "Jobs only race when they overlap"
    A consumer that handles one message at a time, awaiting each before taking the next, can't race with itself.
    The example above starts a task per message, so jobs run concurrently, as most real consumers do.

The `payload` is stored in the recording and handed back to the handler in replay. It must be serialisable as JSON;
`bytes`, tuples and sets are handled. Keep it to what the handler needs.

## Replay

Replay needs to know which function handles each kind of job. Give `--app` a mapping from scope name to an `async`
handler, or a function returning one:

```bash
deja replay deja-recordings/<file>.deja --app myworker:handlers
```

```python
# myworker.py
def make_handlers():             # a factory: fresh state for every replay
    return {"orders": handle_order}
```

Each recorded job is replayed as `await handlers[name](payload)`, in the recorded interleaving, and its return value
is compared with the recorded `job.result`. If your handler takes its input in another shape, wrap it:

```python
handlers = {"orders": lambda order: handle_order(order["id"], order["qty"])}
```

The lambda returns the coroutine, which is what replay awaits.

## Jobs next to an ASGI app

A process can run both: an ASGI app with [`DejaMiddleware`](asgi.md) and a consumer with `deja.scope`, on the same
recording loop. Jobs then appear in the same recordings as requests, and the report explains a race between a
request and a job. Replay, though, takes either an app or a mapping of handlers, so a recording that mixes HTTP
requests and jobs can be read and reported on but not replayed yet (see [Limits](../limits.md)).
