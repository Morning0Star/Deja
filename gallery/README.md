# Race gallery (M0)

Small FastAPI apps with real asyncio race conditions. They are Déjà's acceptance tests: every later milestone must
record a failing run of each and replay it to the same failure 100/100 times, and replay a passing run clean.

| App | Bug | Invariant | Concurrent requests |
|---|---|---|---|
| [signup.py](signup.py) | check-then-act: both requests see the username as free, both insert | username exists at most once | 2 |
| [likes.py](likes.py) | lost update: read count, await, write count + 1 | likes == accepted requests | 3 |
| [wallet.py](wallet.py) | double charge: both see balance 100, both call the payment provider, both deduct | charged ≤ 100 | 2 |

Every app has `create_app(safe=True)`: the same code under an `asyncio.Lock`, which must never violate.

"I/O" in the in-memory stores runs on worker threads for an OS-random 0–3 ms (`_io.py`), and the stress client staggers
request starts by 0–60 ms, so the interleaving differs on every run without any seeded randomness a recorder could
replay. Set `DEJA_PG_DSN` to run the same apps against Postgres (tables are created without constraints on purpose).

## Measured hit rates

`python -m gallery.stress all --trials 300` (in-process ASGI), Windows 11, CPython 3.14.0, memory store, 2026-11-07:

| App | Buggy | Safe |
|---|---|---|
| signup | 73/300 (24.3%) | 0/300 |
| likes | 155/300 (51.7%) | 0/300 |
| wallet | 111/300 (37.0%) | 0/300 |

Over real HTTP (uvicorn, `--url`): wallet 28/100 (28%).

Postgres store (postgres:17-alpine in Docker on localhost, same day, 300 trials each):

| App | Buggy | Safe |
|---|---|---|
| signup | 143/300 (47.7%) | 0/300 |
| likes | 238/300 (79.3%) | 0/300 |
| wallet | 139/300 (46.3%) | 0/300 |

Local database for development (throwaway, localhost only):

```bash
docker run -d --name deja-pg -e POSTGRES_USER=deja -e POSTGRES_PASSWORD=deja -e POSTGRES_DB=deja \
  -p 127.0.0.1:55432:5432 postgres:17-alpine
export DEJA_PG_DSN=postgresql://deja:deja@127.0.0.1:55432/deja
```

## Run

```bash
pip install -e ".[dev,gallery]"
python -m gallery.stress all --trials 300
python -m gallery.stress all --trials 300 --safe
uvicorn gallery.wallet:app --port 8000 & python -m gallery.stress wallet --url http://127.0.0.1:8000
pytest tests/test_gallery.py
```
