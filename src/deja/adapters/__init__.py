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

KNOWN = ("asyncpg", "httpx", "redis")
EXECUTOR_OP = "executor"  # loop.run_in_executor, recorded by the loop itself
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
    return rec, (path if path is not None and path.startswith("q") else None)


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
        if path is None:
            return default() if callable(default) else default
        return await rec.io_replay(path, op, digest(key), decode)
    if path is None:
        return await real()
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
