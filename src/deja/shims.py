"""Record non-deterministic reads made inside recorded tasks: clocks, random numbers, UUIDs, OS randomness.

``install()`` replaces module attributes (``time.time``, ``random.random``, ``uuid.uuid4``, ``os.urandom``...) with
wrappers. A wrapper only records when it is called from a task running on a recording loop; everywhere else
(threads, other loops, import time) it is a plain pass-through. The recording loop's own clock bypasses the shims.

Not covered (documented limitation): names bound before install (``from time import time``), and C-level calls such
as ``datetime.now()``.
"""

import asyncio
import os
import random
import threading
import time
import uuid

_get_loop = asyncio._get_running_loop  # returns None instead of raising when no loop runs in this thread
_current_task = asyncio.current_task

TARGETS = [
    (time, "time"), (time, "time_ns"), (time, "monotonic"), (time, "monotonic_ns"), (time, "perf_counter"),
    (random, "random"), (random, "uniform"), (random, "randint"), (random, "randrange"), (random, "choice"),
    (random, "getrandbits"),
    (uuid, "uuid4"),
    (os, "urandom"),
]

_lock = threading.Lock()
_installs = 0
_originals: dict[tuple, object] = {}


def _encode(value):
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _wrap(module, name, fn):
    source = f"{module.__name__}.{name}"

    def shim(*args, **kwargs):
        value = fn(*args, **kwargs)
        loop = _get_loop()
        if loop is not None:
            rec = getattr(loop, "deja", None)
            if rec is not None and _current_task(loop) is not None:
                rec.read(source, _encode(value))
        return value

    shim.__wrapped__ = fn
    shim.__name__ = getattr(fn, "__name__", name)
    shim.__deja_shim__ = True
    return shim


def install():
    """Install the shims (reference-counted; safe to call once per recording loop)."""
    global _installs
    with _lock:
        _installs += 1
        if _installs > 1:
            return
        for module, name in TARGETS:
            fn = getattr(module, name)
            _originals[(module, name)] = fn
            setattr(module, name, _wrap(module, name, fn))


def uninstall():
    global _installs
    with _lock:
        if _installs == 0:
            return
        _installs -= 1
        if _installs:
            return
        for (module, name), fn in _originals.items():
            setattr(module, name, fn)
        _originals.clear()


def installed() -> bool:
    return _installs > 0
