"""Make in-memory state visible to the report without calling ``deja.touch`` by hand.

    self.users = deja.track(set(), "users", key="username")   # "ada" in users -> read users[username=ada]
    self.carts = deja.track({}, "carts")                         # carts[42] = c  -> write carts[42]
    deja.track(self, "wallet")                                   # self.balance -> read wallet.balance

* ``dict`` / ``list`` / ``set``: returns a tracked copy (use the returned object). Key or item operations are
  reported per key: ``x in d``, ``d[k]``, ``d.get(k)``, ``d[k] = v``, ``del d[k]``, ``pop``, ``setdefault``,
  ``update``; for lists and sets ``in``, ``count``, ``append``, ``add``, ``remove``, ``discard``.
  Iterating or ``len()`` is not reported.
* any other object with a ``__dict__``: tracked in place (its class is swapped for a subclass) and returned.
  Reading or assigning a public data attribute is reported as ``name.attr``; methods and ``_private`` names are not.

Reports go through :func:`deja.touch`, so they are recorded only inside a request on a recording loop and cost one
check otherwise. Tracking only feeds the report; record and replay work without it.
"""

from .annotate import touch

_MISSING = object()


def _res(name, key, k):
    return f"{name}[{key}={k}]" if key else f"{name}[{k}]"


class TrackedDict(dict):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, data, name, key=None):
        super().__init__(data)
        self._deja_name, self._deja_key = name, key

    def _r(self, k, kind, v):
        touch(_res(self._deja_name, self._deja_key, k), kind, v)

    def __getitem__(self, k):
        v = super().__getitem__(k)
        self._r(k, "read", v)
        return v

    def get(self, k, default=None):
        v = super().get(k, _MISSING)
        self._r(k, "read", None if v is _MISSING else v)
        return default if v is _MISSING else v

    def __contains__(self, k):
        found = super().__contains__(k)
        self._r(k, "read", found)
        return found

    def __setitem__(self, k, v):
        super().__setitem__(k, v)
        self._r(k, "write", v)

    def __delitem__(self, k):
        super().__delitem__(k)
        self._r(k, "write", None)

    def pop(self, k, *default):
        v = super().pop(k, *default)
        self._r(k, "write", None)
        return v

    def setdefault(self, k, default=None):
        had = super().__contains__(k)
        v = super().setdefault(k, default)
        self._r(k, "read", v if had else None)
        if not had:
            self._r(k, "write", v)
        return v

    def update(self, *args, **kw):
        for k, v in dict(*args, **kw).items():
            self[k] = v


class _Members:
    """Shared membership reporting for tracked lists and sets."""

    __slots__ = ()

    def _r(self, item, kind, v):
        touch(_res(self._deja_name, self._deja_key, item), kind, v)

    def __contains__(self, item):
        found = super().__contains__(item)
        self._r(item, "read", found)
        return found

    def remove(self, item):
        super().remove(item)
        self._r(item, "write", None)


class TrackedList(_Members, list):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, data, name, key=None):
        super().__init__(data)
        self._deja_name, self._deja_key = name, key

    def count(self, item):
        n = super().count(item)
        self._r(item, "read", n)
        return n

    def append(self, item):
        super().append(item)
        self._r(item, "write", item)


class TrackedSet(_Members, set):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, data, name, key=None):
        super().__init__(data)
        self._deja_name, self._deja_key = name, key

    def add(self, item):
        super().add(item)
        self._r(item, "write", item)

    def discard(self, item):
        super().discard(item)
        self._r(item, "write", None)


_classes: dict = {}


def _tracked_class(cls):
    if cls in _classes:
        return _classes[cls]

    class Tracked(cls):
        __slots__ = ()

        def __getattribute__(self, attr):
            v = super().__getattribute__(attr)
            if attr[0] != "_" and attr in object.__getattribute__(self, "__dict__") and not callable(v):
                touch(f"{object.__getattribute__(self, '__dict__')['_deja_name']}.{attr}", "read", v)
            return v

        def __setattr__(self, attr, v):
            super().__setattr__(attr, v)
            if attr[0] != "_":
                touch(f"{object.__getattribute__(self, '__dict__')['_deja_name']}.{attr}", "write", v)

    Tracked.__name__, Tracked.__qualname__, Tracked.__module__ = cls.__name__, cls.__qualname__, cls.__module__
    _classes[cls] = Tracked
    return Tracked


def track(obj, name: str = None, key: str = None):
    """Report reads and writes of ``obj`` to the recording. Returns the object to use (see module docs)."""
    if isinstance(obj, (TrackedDict, TrackedList, TrackedSet)):
        return obj
    if isinstance(obj, dict):
        return TrackedDict(obj, name or "dict", key)
    if isinstance(obj, list):
        return TrackedList(obj, name or "list", key)
    if isinstance(obj, set):
        return TrackedSet(obj, name or "set", key)
    if not hasattr(obj, "__dict__"):
        raise TypeError(f"deja.track can't track {type(obj).__name__}: it has no __dict__")
    obj.__dict__["_deja_name"] = name or type(obj).__name__.lower()
    if type(obj) not in _classes.values():
        obj.__class__ = _tracked_class(type(obj))
    return obj
