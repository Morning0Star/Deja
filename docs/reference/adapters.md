# Supported libraries

An *adapter* records what a client library's calls returned and hands the same answers back in replay, without
the network. Each adapter switches on by itself when its library is installed; there is nothing to configure.

Only calls made inside request scope (a request, a job, a background task) are recorded. Calls made at startup, such
as creating a pool or reading the server version, run normally while recording; their results are kept in the
recording's catalog (up to 20 results per distinct call) so the app's startup works in replay too.

## Databases

| Library | What is recorded | Replay |
|---|---|---|
| **asyncpg** | `execute`, `executemany`, `fetch`, `fetchrow`, `fetchval`, `fetchmany` on connections and pools; prepared statements; cursors; transactions; bulk copy (`copy_to_table`, `copy_records_to_table`, `copy_from_table`, `copy_from_query`); `LISTEN`/`NOTIFY` listeners, log listeners and termination listeners | `create_pool` and `connect` return stand-ins that answer from the recording; listeners are called with the recorded payload at the recorded point |
| **SQLAlchemy (async, asyncpg dialect)** | everything, through asyncpg's prepared statements; column descriptions are kept, so the ORM can describe results | as asyncpg |
| **psycopg 3** | async and sync cursors (`execute`, `executemany`, fetches), transactions (implicit, `conn.transaction()` blocks with savepoints, `commit()`, `rollback()`), `psycopg_pool` pools | connections and pools are stand-ins; nothing connects |
| **aiomysql** | statements and their rows, transactions (implicit on InnoDB, `begin`, `commit`, `rollback`), connections and pools | stand-in pools, connections and cursors |
| **aiosqlite** | statements, fetches, cursors, transactions, connect and close | answered without opening the database file |

SQL from every driver is read with [sqlglot](https://github.com/tobymao/sqlglot) for the analysis: which tables and
rows a statement read or wrote, evaluated against the rows the recording shows.

## Other stores and queues

| Library | What is recorded | Replay |
|---|---|---|
| **redis** (redis-py asyncio) | `execute_command` (every command), pipelines, pub/sub: messages read in a request, and messages pushed to a background listener | answered from the recording; no server |
| **PyMongo async** (`AsyncMongoClient`) | collection methods (`find_one`, `insert_one`, `update_one`, `find_one_and_update`, `count_documents`, `aggregate`...) and `find()` cursors | results come back as the same pymongo types, `ObjectId` included |
| **aiobotocore** | every AWS API call (S3, DynamoDB, SQS...), streamed bodies read whole, `ClientError` with its error response | no credentials or network |
| **boto3 / botocore** (sync) | the same, for blocking calls in threads or handlers | as aiobotocore |
| **aiokafka** | producer `send` / `send_and_wait` with their `RecordMetadata`; consumer reads in a request; messages pushed to a background consumer loop | nothing is started, stopped or committed |

## HTTP and RPC

| Library | What is recorded | Replay |
|---|---|---|
| **httpx** | `AsyncClient` and `Client` requests: status, headers and body. Streamed responses (`client.stream(...)`, how LLM SDKs read token streams) chunk by chunk, so work done between chunks stays in order | responses built without connecting |
| **aiohttp client** | `ClientSession` requests: status, headers and the whole body | a stand-in response with `read`, `text`, `json`, `content.iter_chunked`, `raise_for_status`... |
| **requests** | `Session.send` (every `requests.get/post`): status, headers, body | a `requests.Response` built without the network |
| **grpc.aio** | unary-unary and unary-stream calls; errors with their status code and details | messages rebuilt with the stub's own deserializer |

An aiohttp client response is recorded whole, so work done between its chunks isn't ordered chunk by chunk (httpx
streams are). gRPC client-streaming calls aren't recorded; they run live.

## Inside the runtime

| What | What is recorded |
|---|---|
| `time.time`, `time.time_ns`, `time.monotonic`, `time.monotonic_ns`, `time.perf_counter` | each value read inside a request |
| `datetime.now()`, `datetime.utcnow()`, `date.today()` in the app's own modules | each value read inside a request |
| `random.random`, `uniform`, `randint`, `randrange`, `choice`, `getrandbits`; `uuid.uuid4`; `os.urandom` | each value read inside a request |
| `loop.run_in_executor`, `asyncio.to_thread`, anyio worker threads | see [Worker threads](../guides/threads.md) |
| WebSocket messages | see [WebSockets](../guides/websockets.md) |

Other sources of randomness (`random.shuffle`, `random.sample`, `secrets`, a `random.Random` instance of the app's
own) aren't recorded. Code that uses them inside a request can take a different path in replay; replay then says
where it diverged.

## Errors

A call that raised while recording raises again in replay, as the library's own exception class with the same
message (for AWS, the same error response; for gRPC, the same status code). Code that handles a failed call takes
the same path in replay.

## Libraries not listed

A library without an adapter still runs in replay, for real, against whatever it connects to. If it isn't
reachable, the app gets the error it would get in production, and the replay usually diverges at that point.
Calls made through `run_in_executor` are recorded whatever the library. Requests for new adapters are welcome; see
[Contributing](../contributing.md).
