# Worker threads

Async apps still run code in threads: FastAPI and Starlette run plain `def` endpoints and dependencies in a thread
pool, and apps call `asyncio.to_thread` or `loop.run_in_executor` for blocking work. Déjà records these calls when
a request makes them, and replays them in the recorded order.

## Each call belongs to its request

A thread call made inside a request gets an id under that request: `q3.t0`, `q3.t1`, and so on. What the code in
the thread does is recorded under that id and shown under the request in the report: its accesses to
[shared state](shared-state.md), the clocks and random numbers it read, and the blocking calls it made (`requests`,
boto3, psycopg's sync API; see [Supported libraries](../reference/adapters.md)).

## Replay: run again, or answered from the recording

**`anyio.to_thread.run_sync`**, which FastAPI and Starlette use for `def` endpoints, sync dependencies and sync file
responses, runs the app's code again in replay:

```python
@app.post("/book")
def book(seat: str):              # FastAPI runs this in a worker thread
    if seat not in booked:
        booked.add(seat)
        return {"booked": seat}
    return {"taken": seat}
```

**`loop.run_in_executor` and `asyncio.to_thread`** (which goes through `run_in_executor`) record the function's
result. In replay the function runs again only if it touched tracked shared state while recording; otherwise its
recorded result (or exception) is handed back without running it, so blocking I/O inside it doesn't happen twice.

## How the order is kept

While recording, each access a thread makes to tracked state lands in the event log between the loop's steps, where
it happened. In replay a thread waits at each such access until the recording says it's its turn, and the loop
waits for the thread to get there. So two threads racing on the same state race the same way again, and the report
explains the race like one between tasks.

The unit is one access to tracked state. Work between two accesses (computation, untracked memory) runs freely, and
an access made in the middle of one event-loop step replays just after it. For state Déjà doesn't track by itself,
report the access with [`deja.touch`](../reference/python-api.md#deja.touch): that also makes the thread wait for its
turn there.

A thread that calls back into the event loop (`anyio.from_thread.run`) is set aside while it waits, so the loop
keeps going.
