"""I/O adapters: record what a client library returned, hand it back during replay without touching the network.

Every adapter funnels its calls through :func:`io_call`. Only calls made inside *request scope* (a ``q`` task) are
recorded or replayed; anything else (startup, background tasks) runs live while recording, and gets a harmless
default during replay when the library has been replaced by a fake.

Recording emits ``IO_START`` before the call and ``IO_END`` (with the encoded result or exception) after it. The
replay schedule collapses every step the library took in between into a single resumption step, so the app sees the
same "await, then result" shape whether or not the network is there.
"""

import asyncio
import contextvars
import hashlib
import importlib
import threading

from .. import codec

_get_loop = asyncio._get_running_loop
_inside = contextvars.ContextVar("deja_inside_io", default=False)  # suppress nested adapter calls

KNOWN = ("asyncpg", "httpx", "redis", "anyio", "aiohttp", "psycopg", "aiosqlite", "grpc", "aiobotocore", "pymongo",
         "aiomysql", "aiokafka", "requests", "botocore")
EXECUTOR_OP = "executor"  # loop.run_in_executor, recorded by the loop itself
OPTIONAL_OPS = {"pg.prepare"}  # may be skipped by a client-side cache (SQLAlchemy's statement cache); see replay
OUTSIDE_LIMIT = 500  # distinct out-of-request calls (startup queries) whose results are kept for replay
OUTSIDE_PER_CALL = 20  # results kept per distinct call, in order (the same query run once per pool connection)
OUTSIDE_MAX_ROWS = 200
_lock = threading.Lock()
_installed: dict[str, int] = {}


def digest(*parts) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(repr(p).encode("utf-8", "replace"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


def _context():
    """(recorder, path) when the current call should be recorded or replayed, else (recorder_or_None, None)."""
    loop = _get_loop()
    rec = getattr(loop, "deja", None) if loop is not None else None
    if rec is None or _inside.get():
        return rec, None
    path = rec.current_path()
    if not path or path[0] not in "qb":
        return rec, None
    if path[0] == "b" and getattr(rec, "replaying", False) and not rec.armed:
        return rec, None  # a background task during replay's startup: answered like startup work
    return rec, path


def passthrough() -> bool:
    """True when a wrapper can hand back the library call untouched (no extra coroutine frame):
    nested inside a recorded call, or not running on a Déjà loop at all."""
    if _inside.get():
        return True
    loop = _get_loop()
    return loop is None or getattr(loop, "deja", None) is None


def replaying() -> bool:
    loop = _get_loop()
    return getattr(getattr(loop, "deja", None), "replaying", False)


async def io_call(op: str, key, real, *, encode=codec.encode, decode=codec.decode, default=None):
    """Run (record) or substitute (replay) one awaited I/O call.

    ``real`` is a zero-argument coroutine function doing the actual call. ``key`` feeds the argument digest that
    replay checks. ``default`` is returned for out-of-scope calls during replay when the real library is faked.
    """
    if _inside.get():  # nested call inside a recorded one (pool -> connection -> acquire): cheapest possible exit
        return await real()
    rec, path = _context()
    if rec is None:
        return await real()
    if getattr(rec, "replaying", False):
        if path is None:  # startup / background call: answer from the catalog, else a harmless default
            hit = rec.outside(op, digest(key))
            if hit is not None:
                if hit[0] == "err":
                    raise codec.decode_exc(hit[1])
                return decode(hit[1])
            return default() if callable(default) else default
        return await rec.io_replay(path, op, digest(key), decode)
    if path is None:
        return await _outside(rec, op, key, real, encode)
    rec.io_start(path, op)
    token = _inside.set(True)
    try:
        result = await real()
    except BaseException as exc:
        rec.io_end(path, op, key, "err", codec.encode_exc(exc))
        raise
    finally:
        _inside.reset(token)
    rec.io_end(path, op, key, "ok", encode(result))
    if path[0] == "b":  # replay runs background tasks from startup: keep their answers like startup work's
        _keep(rec, op, key, ("ok", encode(result)))
    return result


def _keep(rec, op, key, entry):
    out = rec.catalog.setdefault("outside", {})
    d = f"{op}:{digest(key)}"
    seen = out.get(d)
    if (seen is None and len(out) < OUTSIDE_LIMIT) or (seen is not None and len(seen) < OUTSIDE_PER_CALL):
        try:
            out.setdefault(d, []).append(entry)
        except Exception:
            pass


def _sync_context():
    """(recorder, path) for a blocking call made in request scope: in a worker thread a request started, or on the
    loop thread inside a request (a blocking call in an async handler); else (None, None)."""
    if _inside.get():
        return None, None
    from ..threads import _thread

    t = _thread.get()
    if t is not None:
        return t
    rec, path = _context()
    return (rec, path) if path is not None else (None, None)


def io_call_sync(op: str, key, real, *, encode=codec.encode, decode=codec.decode):
    """The blocking twin of :func:`io_call`, for sync clients (requests, sync httpx, boto3, sync psycopg) used in
    worker threads or directly in handlers. Calls outside request scope run untouched."""
    rec, path = _sync_context()
    if rec is None:
        return real()
    if getattr(rec, "replaying", False):
        return rec.io_replay_sync(path, op, digest(key), decode)
    rec.io_start(path, op)
    token = _inside.set(True)
    try:
        result = real()
    except BaseException as exc:
        rec.io_end(path, op, key, "err", codec.encode_exc(exc))
        raise
    finally:
        _inside.reset(token)
    rec.io_end(path, op, key, "ok", encode(result))
    return result


async def io_live(op: str, key, real):
    """A call that must really run in replay too (it builds an object the app keeps, like an AWS client), but whose
    internal steps depend on library caches: recorded as one call; in replay it runs live in a background task
    while the request waits for it as one step, so a cold or warm cache can't change the request's steps."""
    if _inside.get():
        return await real()
    rec, path = _context()
    if rec is None or path is None:
        return await real()
    if not getattr(rec, "replaying", False):
        return await io_call(op, key, real, encode=lambda v: None)
    return await rec.io_replay_live(path, op, digest(key), real)


async def _outside(rec, op, key, real, encode):
    """Out-of-request call while recording: run it live, and keep the results of each distinct call, in order, so
    replay can answer startup work (an ORM reading the server version, CREATE TABLE IF NOT EXISTS, a query each pool
    connection runs) without a database."""
    out = rec.catalog.setdefault("outside", {})
    d = f"{op}:{digest(key)}"
    seen = out.get(d)
    if (seen is None and len(out) >= OUTSIDE_LIMIT) or (seen is not None and len(seen) >= OUTSIDE_PER_CALL):
        return await real()
    seen = out.setdefault(d, [])
    try:  # no _inside here: the call may run request handling inline (an ASGI test client) or spawn tasks
        result = await real()
    except BaseException as exc:
        seen.append(("err", codec.encode_exc(exc)))
        raise
    if not (isinstance(result, list) and len(result) > OUTSIDE_MAX_ROWS):
        try:
            seen.append(("ok", encode(result)))
        except Exception:
            pass
    return result


def install(names=KNOWN):
    """Install adapters for the given libraries that are importable (reference-counted)."""
    done = []
    with _lock:
        for name in names:
            try:
                mod = importlib.import_module(f".{name}", __name__)
            except ImportError:
                continue  # library not installed
            _installed[name] = _installed.get(name, 0) + 1
            if _installed[name] == 1:
                mod.install()
            done.append(name)
    return done


def uninstall(names):
    with _lock:
        for name in names:
            if name not in _installed:
                continue
            _installed[name] -= 1
            if _installed[name] == 0:
                del _installed[name]
                importlib.import_module(f".{name}", __name__).uninstall()


def patch(target, attr, make):
    """Replace ``target.attr`` with ``make(original)``; returns an undo function."""
    original = getattr(target, attr)
    setattr(target, attr, make(original))
    return lambda: setattr(target, attr, original)
