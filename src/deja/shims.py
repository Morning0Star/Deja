"""Record non-deterministic reads made inside recorded tasks: clocks, random numbers, UUIDs, OS randomness, dates.

``install()`` replaces module attributes (``time.time``, ``random.random``, ``uuid.uuid4``, ``os.urandom``...) with
wrappers. A wrapper only records when it is called from a request (a task on a recording loop, or a worker thread
it started); everywhere else it is a plain pass-through. The recording loop's own clock bypasses the shims.

:func:`rebind` then fixes the app's own modules (the middleware calls it on the first request with the modules the
routes reach): names bound to the originals before ``install()`` (``from time import time``) point at the
wrappers, and ``datetime`` / ``date`` (the classes, or the ``datetime`` module) point at stand-ins whose ``now()``
/ ``utcnow()`` / ``today()`` record. The stand-ins only change those methods: constructing one gives a real
``datetime``, and ``isinstance(x, datetime)`` is true for real datetimes. They are not installed globally:
libraries subclass ``date`` with their own metaclasses, and annotations evaluated at import time keep the real
classes.
"""

import asyncio
import datetime as _dt
import os
import random
import threading
import time
import types
import uuid

from .adapters import _inside as _inside_io
from .threads import _thread

_get_loop = asyncio._get_running_loop  # returns None instead of raising when no loop runs in this thread
_current_task = asyncio.current_task

TARGETS = [
    (time, "time"),
    (time, "time_ns"),
    (time, "monotonic"),
    (time, "monotonic_ns"),
    (time, "perf_counter"),
    (random, "random"),
    (random, "uniform"),
    (random, "randint"),
    (random, "randrange"),
    (random, "choice"),
    (random, "getrandbits"),
    (uuid, "uuid4"),
    (os, "urandom"),
]

_lock = threading.Lock()
_installs = 0
_originals: dict[tuple, object] = {}
_shim_of: dict[int, object] = {}  # id(original function or class) -> its wrapper, while installed
_REAL_DATETIME, _REAL_DATE = _dt.datetime, _dt.date


def _encode(value):
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, _dt.date):  # datetime is a date too
        return value.isoformat()
    return value


def _decode(source, value):
    if source == "uuid.uuid4":
        return uuid.UUID(value)
    if source == "os.urandom" and isinstance(value, str):
        return value.encode("latin-1")
    if source.startswith("datetime.") and isinstance(value, str):
        return (_REAL_DATE if source == "datetime.date.today" else _REAL_DATETIME).fromisoformat(value)
    return value


def _recorder():
    loop = _get_loop()
    if loop is not None:
        rec = getattr(loop, "deja", None)
        return rec if rec is not None and _current_task(loop) is not None else None
    t = _thread.get()  # a worker-thread call made from request scope
    return t[0] if t is not None else None


def _wrap(source, fn):
    def shim(*args, **kwargs):
        value = fn(*args, **kwargs)
        if _inside_io.get():  # a library's own clock read inside a recorded I/O call: not part of the app's story
            return value
        rec = _recorder()
        if rec is not None:
            encoded = _encode(value)
            got = rec.read(source, encoded)  # recording returns `encoded`; replay may return the recorded value
            if got is not encoded:
                value = _decode(source, got)
        return value

    shim.__wrapped__ = fn
    shim.__name__ = getattr(fn, "__name__", source.rpartition(".")[2])
    shim.__deja_shim__ = True
    return shim


class _RealType(type):
    """``isinstance`` / ``issubclass`` against the stand-in answer for the real class."""

    def __instancecheck__(cls, obj):
        return isinstance(obj, cls._deja_real)

    def __subclasscheck__(cls, sub):
        return issubclass(sub, cls._deja_real)


class DejaDatetime(_REAL_DATETIME, metaclass=_RealType):  # type: ignore[misc,valid-type]
    _deja_real = _REAL_DATETIME

    def __new__(cls, *args, **kwargs):  # constructing gives a real datetime
        return _REAL_DATETIME(*args, **kwargs)

    _now = staticmethod(_wrap("datetime.datetime.now", _REAL_DATETIME.now))
    _utcnow = staticmethod(
        _wrap("datetime.datetime.utcnow", lambda: _REAL_DATETIME.now(_dt.timezone.utc).replace(tzinfo=None))
    )
    _today = staticmethod(_wrap("datetime.datetime.today", _REAL_DATETIME.today))

    @classmethod
    def now(cls, tz=None):
        return cls._now(tz)

    @classmethod
    def utcnow(cls):
        return cls._utcnow()

    @classmethod
    def today(cls):
        return cls._today()


DejaDatetime.__name__ = DejaDatetime.__qualname__ = "datetime"
DejaDatetime.__module__ = "datetime"


class DejaDate(_REAL_DATE, metaclass=_RealType):  # type: ignore[misc,valid-type]
    _deja_real = _REAL_DATE

    def __new__(cls, *args, **kwargs):
        return _REAL_DATE(*args, **kwargs)

    _today = staticmethod(_wrap("datetime.date.today", _REAL_DATE.today))

    @classmethod
    def today(cls):
        return cls._today()


DejaDate.__name__ = DejaDate.__qualname__ = "date"
DejaDate.__module__ = "datetime"


class _DatetimeModule(types.ModuleType):
    """``import datetime`` in an app module: the real module, with the recording ``datetime`` and ``date``."""

    datetime: type
    date: type

    def __getattr__(self, name):
        return getattr(_dt, name)


_DATETIME_MODULE = _DatetimeModule("datetime", _dt.__doc__)
_DATETIME_MODULE.datetime, _DATETIME_MODULE.date = DejaDatetime, DejaDate


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
            shim = _wrap(f"{module.__name__}.{name}", fn)
            setattr(module, name, shim)
            _shim_of[id(fn)] = shim
        _shim_of[id(_REAL_DATETIME)] = DejaDatetime
        _shim_of[id(_REAL_DATE)] = DejaDate
        _shim_of[id(_dt)] = _DATETIME_MODULE


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
        _shim_of.clear()


def installed() -> bool:
    return _installs > 0


def rebind(namespaces) -> list:
    """Point names the app bound to the originals before install (``from time import time``) at the wrappers.

    ``namespaces`` are module dicts of the app. Returns ``module.name`` of each name changed. The wrappers stay
    pass-throughs outside a recording, so leaving them in place after the loop closes is harmless."""
    done: list = []
    if not _shim_of:
        return done
    for g in namespaces:
        for k, v in list(g.items()):
            shim = _shim_of.get(id(v))
            if shim is not None and not k.startswith("__"):
                g[k] = shim
                done.append(f"{g.get('__name__')}.{k}")
    return done
