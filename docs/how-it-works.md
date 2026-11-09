# How it works

Déjà has four parts: a recording loop, adapters that record what calls returned, an analysis that explains the
race, and a replay loop that runs the app again in the recorded order. This page explains each one at the level you
need to use Déjà well. The design notes ([Architecture](ARCHITECTURE.md)) give the reasons behind each choice.

## Recording

### The recording loop

An asyncio event loop runs one task at a time. Each pass of the loop takes the callbacks that are ready and runs
them; a task runs until its next `await` that has to wait. The order in which those steps run is what decides a
race.

Déjà's recording loop is a subclass of the standard loop (the selector loop on Linux and macOS, the proactor loop on
Windows) that logs which task each step belonged to. Logging a step costs one integer appended to a buffer, so
recording is cheap enough to leave on.

Each task gets a stable id derived from where it was created: `q3` is request 3's task, `q3.0` the first task it
created, `q3.t1` its second call into a worker thread. The same code creates the same ids in replay, which is how
replay knows which task is which.

### Request scope

Not every task is recorded in detail, only those in *request scope*: tasks handling an HTTP request, a WebSocket
connection or a [`deja.scope`](guides/jobs.md) job, the tasks they create, and the app's
[background tasks](guides/background-tasks.md). The middleware puts a request's task in scope when the request
arrives and records the request itself: method, path, headers (sensitive ones redacted) and body.

### What else is recorded

| What | How |
|---|---|
| Database, cache, queue and HTTP calls | Adapters wrap each client library and record each call with its result (rows, documents, responses) or the exception it raised. See [Supported libraries](reference/adapters.md). |
| Worker threads (`def` endpoints, `asyncio.to_thread`, `run_in_executor`) | Each call is recorded under its request, with its accesses to shared state, so replay can put them back in order; an executor call that touched no shared state just gets its recorded result. See [Worker threads](guides/threads.md). |
| Clocks, random numbers, UUIDs, `os.urandom` | Each value read inside a request is recorded and returned again in replay. |
| In-memory state | Reads and writes of the containers and objects the app's code reaches are recorded, for the report. See [Shared state](guides/shared-state.md). |

### The ring buffer and when a recording is saved

Everything goes into a fixed-size ring buffer, by default 200,000 events: roughly the last 15,000 to 20,000
requests of a typical API, in tens of megabytes. Nothing is written to disk until something goes wrong:

- a response with a 5xx status,
- an exception escaping the app,
- a WebSocket connection closing with code 1011,
- app code calling [`deja.flag()`](reference/python-api.md#deja.flag).

Then Déjà saves the failing request's *causal window*: every event from the start of the earliest request that
overlapped it up to the moment of saving. The file is written off the event loop so the app isn't held up.

## Analysis

The recording shows every read and write each request made, in the order they happened. The analysis finds pairs
of requests whose accesses to the same thing conflict, and names the pattern:

| Pattern | What happened |
|---|---|
| **Lost update** | Both requests read a value, then both wrote a new one based on what they read; one write overwrote the other. |
| **Check-then-act** | Both checked that something was absent or allowed (a free seat, an unused username), then both acted on it. |
| **Stale read** | A request acted on a value that another request had changed after it was read. |
| **Write skew** | Each request read what the other later changed, and each wrote something different, so together they broke a rule neither broke alone. |
| **Torn read** | A request read two related values while another request was half-way through changing both. |
| **Deadlock** | Two transactions each waited for a lock the other held. |

It understands SQL (rows, ranges, transactions and isolation levels), Redis keys, MongoDB documents, S3 and DynamoDB
items, outside HTTP calls and in-memory state. When it can't decide whether two accesses touched the same thing, it
says the conflict is *possible* rather than certain. [Reading a report](guides/reading-reports.md) explains the
output.

## Replay

Replay builds a fresh copy of the app and runs it on a replay loop:

1. **The requests are sent again** by a harness: ASGI calls for an ASGI app, real HTTP requests to a local port for
   aiohttp, handler calls for `deja.scope` jobs. No client is needed.
2. **Only the recorded order is allowed.** At each step the loop runs the task the recording says ran next. Library
   plumbing (callbacks that aren't app tasks) runs as it comes.
3. **Calls get their recorded answers.** Every adapter answers from the recording instead of the network, so the
   database and the services the app calls don't have to exist.
4. **Time is virtual.** Clocks and timers follow the recording, so a `sleep(5)` takes no real time and a timeout
   fires at the recorded moment.

If the app can't take a recorded step (the task the recording expects isn't ready, or it makes a call the recording
has no answer for), replay stops and says where, instead of carrying on with a different interleaving. So a replay
that reports `followed 10/10 recorded steps` and the same responses really did happen the way it was recorded.

This is also what makes [checking a fix](guides/fixing-races.md) possible: replay the recorded race against the
fixed code and see whether it can still happen.

## Cost

In this repository's benchmark (FastAPI with Postgres and an outside HTTP call per order, 100 to 400 requests a
second on a 2-vCPU GitHub Actions runner), recording left throughput unchanged, added about 12% server CPU and 1 to
3 ms at the 99th percentile, and stored about 0.1 KB per request. The method and raw numbers are in
[benchmarks/RESULTS.md](https://github.com/Morning0Star/Deja/blob/main/benchmarks/RESULTS.md).
