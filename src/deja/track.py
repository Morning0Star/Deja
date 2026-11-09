"""Make in-memory state visible to the report without calling ``deja.touch`` by hand.

    self.users = deja.track(set(), "users", key="username")   # "ada" in users -> read users[username=ada]
    self.carts = deja.track({}, "carts")                         # carts[42] = c  -> write carts[42]
    deja.track(self, "wallet")                                   # self.balance -> read wallet.balance

Most apps don't need these calls: :class:`deja.DejaMiddleware` tracks the state its routes can reach on its own
(see :mod:`deja.autotrack`).

* ``dict``, ``defaultdict``, ``OrderedDict``, ``Counter``, ``list``, ``set``, ``deque``: returns a tracked copy of
  the same type (use the returned object). Key or item operations are reported per key: ``x in d``, ``d[k]``,
  ``d.get(k)``, ``d[k] = v``, ``del d[k]``, ``pop``, ``setdefault``, ``update``, ``d[k] += 1`` on a counter; for
  lists, sets and deques ``in``, ``count``, ``append``, ``extend``, ``insert``, ``add``, ``remove``, ``discard``,
  ``pop`` / ``popleft`` (the item taken). Reading the whole container (iterating, ``len()``, ``bool()``, ``keys()``
  / ``values()`` / ``items()``, indexing a list) is one read of ``name[*]`` with the keys it saw (up to 200; past
  that, the count), so the report can tell whether it covered a key another request changed. ``clear()`` and
  assigning by index are a write of ``name[*]``.
* any other object with a ``__dict__``: tracked in place (its class is swapped for a subclass) and returned.
  Reading or assigning a public data attribute is reported as ``name.attr``; methods and ``_private`` names are not.
* **values inside** a tracked dict: a new ``dict`` / ``list`` / ``set``... stored in it (``carts[c] = {"qty": 1}``,
  ``setdefault``, a ``defaultdict``'s default) is stored as a tracked copy named ``carts[c]``, so
  ``carts[c]["qty"] += 1`` reports ``carts[c][qty]``. Only a value nothing else refers to is copied (checked with the
  reference count and the caller's local variables), so no alias is split; a value something else also holds stays
  plain. An object of the app's own classes stored in it is tracked in place, as ``carts[c].attr``. (On PyPy, which
  has no reference counts, nested containers stay plain.)

Reports are recorded only inside a request on a recording loop and cost one check otherwise. During replay, an
operation made in a worker thread first waits for its turn in the recorded order (see :mod:`deja.threads`); that
is why every operation announces itself *before* it runs. Tracking only feeds the report and orders threads;
record and replay work without it.
"""

import collections
import sys
import threading

from . import annotate
from .threads import _thread

_MISSING = object()
SNAPSHOT_LIMIT = 200
_local = threading.local()  # .busy: a report encodes its value, which may read another tracked container


def touch(resource, kind, value):
    """Record one access (recording only; nested accesses made while encoding a value are not reported)."""
    if getattr(_local, "busy", False):
        return
    rec = annotate._recorder()
    if rec is None:
        return
    _local.busy = True
    try:
        rec.touch(resource, kind, value)
    finally:
        _local.busy = False


def _turn(n=1):
    """Replay, in a worker thread: wait until the schedule reaches the next ``n`` recorded accesses."""
    t = _thread.get()
    if t is not None and getattr(t[0], "replaying", False):
        for _ in range(n):
            t[0].thread_gate(t[1])


def _calibrate():
    """The reference count a brand-new value has inside ``__setitem__`` on this interpreter (3.14 borrows)."""
    if sys.implementation.name != "cpython":
        return None
    seen = []

    class Probe(dict):
        def __setitem__(self, k, v):
            seen.append(sys.getrefcount(v))

    Probe()["x"] = {}
    return seen[0]


_FRESH_REFS = _calibrate()
_refcount = getattr(sys, "getrefcount", lambda v: 1 << 30)  # PyPy has none: nothing counts as fresh there
_OWN_FILE = __file__


def _fresh(v, refs) -> bool:
    """True when ``v`` (with ``refs`` references inside our ``__setitem__``) is held by nothing else: no other
    container (reference count) and no local variable of the code storing it (which 3.14 doesn't count)."""
    if _FRESH_REFS is None or refs > _FRESH_REFS:
        return False
    f = sys._getframe(2)
    for _ in range(40):
        if f is None:
            break
        if f.f_code.co_filename != _OWN_FILE:
            try:
                if any(x is v for x in f.f_locals.values()):
                    return False
            except Exception:
                return False
        f = f.f_back
    return True


def _child(parent, k, v):
    """A tracked copy of container ``v`` stored under key ``k`` of ``parent``."""
    return copy_of(v, _res(parent._deja_name, parent._deja_key, k))


_APP_TYPES: dict = {}  # type -> whether its instances are the app's own (decided once per class)


def _app_object(v, parent, k):
    """Track an app object found in a tracked container in place (same object, so always safe)."""
    t = type(v)
    ok = _APP_TYPES.get(t)
    if ok is None:
        from .autotrack import _is_app_object

        ok = _APP_TYPES[t] = (
            t not in _classes.values() and hasattr(v, "__dict__") and _is_app_object(v) and _swappable(t)
        )
    if not ok or (annotate._recorder() is None and _thread.get() is None):
        return
    try:
        track_object(v, _res(parent._deja_name, parent._deja_key, k))
    except TypeError:
        _APP_TYPES[t] = False


def _res(name, key, k):
    return f"{name}[{key}={k}]" if key else f"{name}[{k}]"


class _Tracked:
    """Shared reporting. ``_deja_name`` is None for copies made by the container's own methods: those are silent."""

    __slots__ = ()

    def _on(self, n=1):
        """Called before an operation that reports ``n`` accesses: True when it should report them."""
        if self._deja_name is None or getattr(_local, "busy", False):
            return False
        _turn(n)
        return True

    def _r(self, k, kind, v):
        touch(_res(self._deja_name, self._deja_key, k), kind, v)

    def _whole(self, kind="read"):
        """One access to the whole container: a read snapshots the keys (only when recording)."""
        if not self._on() or annotate._recorder() is None:
            return
        _local.busy = True  # taking the snapshot may call our own methods again (PyPy's views call __len__)
        try:
            n = self._deja_len()
            v = (list(self._keys()) if n <= SNAPSHOT_LIMIT else n) if kind == "read" else None
        finally:
            _local.busy = False
        touch(_res(self._deja_name, self._deja_key, "*"), kind, v)


# ---------------------------------------------------------------------- mappings
class _MappingOps(_Tracked):
    __slots__ = ()

    def __getitem__(self, k):
        on = self._on()
        try:
            v = super().__getitem__(k)
        except KeyError:
            if on:
                self._r(k, "read", None)  # a failed lookup is a read that found nothing
            raise
        if on:
            self._r(k, "read", v)
            _app_object(v, self, k)
        return v

    def get(self, k, default=None):
        on = self._on()
        v = dict.get(self, k, _MISSING)
        if on:
            self._r(k, "read", None if v is _MISSING else v)
            if v is not _MISSING:
                _app_object(v, self, k)
        return default if v is _MISSING else v

    def __contains__(self, k):
        on = self._on()
        found = dict.__contains__(self, k)
        if on:
            self._r(k, "read", found)
        return found

    def __setitem__(self, k, v):
        refs = _refcount(v)  # first: anything below adds references
        on = self._on()
        if self._deja_name is not None and type(v) in COPIES and _fresh(v, refs):
            v = _child(self, k, v)
        super().__setitem__(k, v)
        if on:
            self._r(k, "write", v)

    def __delitem__(self, k):
        on = self._on()
        try:
            super().__delitem__(k)
        except KeyError:
            if on:
                self._r(k, "read", None)
            raise
        if on:
            self._r(k, "write", None)

    def pop(self, k, *default):
        on = self._on()
        had = dict.__contains__(self, k)
        try:
            return super().pop(k, *default)
        finally:
            if on:
                self._r(k, "write" if had else "read", None)

    def popitem(self, *a, **kw):
        on = self._on()
        try:
            k, v = super().popitem(*a, **kw)
        except KeyError:
            if on:
                touch(_res(self._deja_name, self._deja_key, "*"), "read", [])
            raise
        if on:
            self._r(k, "write", None)
        return k, v

    def setdefault(self, k, default=None):
        refs = _refcount(default)
        had = dict.__contains__(self, k)
        on = self._on(1 if had else 2)
        if not had and self._deja_name is not None and type(default) in COPIES and _fresh(default, refs):
            default = _child(self, k, default)
        v = super().setdefault(k, default)
        if on:
            self._r(k, "read", v if had else None)
            if not had:
                self._r(k, "write", v)
        return v

    def clear(self):
        self._whole("write")
        super().clear()

    # whole-container reads
    def _deja_len(self):
        return dict.__len__(self)

    def _keys(self):
        return list(dict.keys(self))

    def __iter__(self):
        self._whole()
        return super().__iter__()

    def __len__(self):
        self._whole()
        return dict.__len__(self)

    def keys(self):
        self._whole()
        return super().keys()

    def values(self):
        self._whole()
        return super().values()

    def items(self):
        self._whole()
        return super().items()

    def copy(self):  # a plain copy of the base type (the container's own copy() would build a tracked one)
        make, args = self.__reduce__()[:2]
        return make(*args)

    __copy__ = copy


class TrackedDict(_MappingOps, dict):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, *a, **kw):
        self._deja_name = self._deja_key = None
        dict.__init__(self, *a, **kw)

    def update(self, *a, **kw):  # dict.update would bypass __setitem__
        for k, v in dict(*a, **kw).items():
            self[k] = v

    def __ior__(self, other):
        self.update(other)
        return self

    def __reduce__(self):  # copy / pickle give a plain dict
        return dict, (dict.copy(self),)


class TrackedDefaultDict(_MappingOps, collections.defaultdict):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, *a, **kw):
        self._deja_name = self._deja_key = None
        collections.defaultdict.__init__(self, *a, **kw)

    update = TrackedDict.update
    __ior__ = TrackedDict.__ior__

    def __missing__(self, k):  # 3.14.x inserts the default without __setitem__: do it here, reported, everywhere
        if self.default_factory is None:
            raise KeyError(k)
        v = self.default_factory()
        if self._deja_name is not None and type(v) in COPIES and _FRESH_REFS is not None:
            v = _child(self, k, v)  # made right here: nothing else holds it
        self[k] = v
        return v

    def __reduce__(self):
        return collections.defaultdict, (self.default_factory, dict.copy(self))


class TrackedOrderedDict(_MappingOps, collections.OrderedDict):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, *a, **kw):
        self._deja_name = self._deja_key = None
        collections.OrderedDict.__init__(self, *a, **kw)

    def _keys(self):
        return list(collections.OrderedDict.keys(self))

    def move_to_end(self, k, last=True):
        on = self._on()
        super().move_to_end(k, last)
        if on:
            self._r(k, "write", "(moved)")

    def __reduce__(self):
        return collections.OrderedDict, (list(collections.OrderedDict.items(self)),)


class TrackedCounter(_MappingOps, collections.Counter):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, *a, **kw):
        self._deja_name = self._deja_key = None
        collections.Counter.__init__(self, *a, **kw)  # update() goes through get / __setitem__: reported per key

    def __reduce__(self):
        return collections.Counter, (dict.copy(self),)


# ---------------------------------------------------------------------- sequences and sets
class _MemberOps(_Tracked):
    __slots__ = ()

    def __contains__(self, item):
        on = self._on()
        found = super().__contains__(item)
        if on:
            self._r(item, "read", found)
        return found

    def remove(self, item):
        on = self._on()
        try:
            super().remove(item)
        except (KeyError, ValueError):
            if on:
                self._r(item, "read", False)
            raise
        if on:
            self._r(item, "write", None)

    def _take(self, method, *a):
        """pop / popleft: a write of the item taken (a read of the whole, empty container when there is none)."""
        on = self._on()
        try:
            item = method(self, *a)
        except (KeyError, IndexError):
            if on:
                touch(_res(self._deja_name, self._deja_key, "*"), "read", [])
            raise
        if on:
            self._r(item, "write", None)
        return item

    def _add_all(self, method, items):
        items = list(items)
        on = self._on(len(items))
        method(self, items)
        if on:
            for item in items:
                self._r(item, "write", item)

    def clear(self):
        self._whole("write")
        super().clear()

    def _keys(self):
        return list(super().__iter__())

    def __iter__(self):
        self._whole()
        return super().__iter__()

    def __len__(self):
        self._whole()
        return super().__len__()


class TrackedList(_MemberOps, list):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, *a):
        self._deja_name = self._deja_key = None
        list.__init__(self, *a)

    def _deja_len(self):
        return list.__len__(self)

    def count(self, item):
        on = self._on()
        n = list.count(self, item)
        if on:
            self._r(item, "read", n)
        return n

    def append(self, item):
        on = self._on()
        list.append(self, item)
        if on:
            self._r(item, "write", item)

    def insert(self, i, item):
        on = self._on()
        list.insert(self, i, item)
        if on:
            self._r(item, "write", item)

    def extend(self, items):
        self._add_all(list.extend, items)

    def __iadd__(self, items):
        self.extend(items)
        return self

    def pop(self, *a):
        return self._take(list.pop, *a)

    def __getitem__(self, i):
        self._whole()
        return list.__getitem__(self, i)

    def __setitem__(self, i, v):
        self._whole("write")
        list.__setitem__(self, i, v)

    def __delitem__(self, i):
        self._whole("write")
        list.__delitem__(self, i)

    def index(self, item, *args):
        self._whole()
        return list.index(self, item, *args)

    def sort(self, *a, **kw):  # reorders every item: a write of the whole list
        self._whole("write")
        list.sort(self, *a, **kw)

    def reverse(self):
        self._whole("write")
        list.reverse(self)

    def copy(self):
        return list(list.__iter__(self))

    def __reduce__(self):
        return list, (list.copy(self),)


class TrackedSet(_MemberOps, set):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, *a):
        self._deja_name = self._deja_key = None
        set.__init__(self, *a)

    def _deja_len(self):
        return set.__len__(self)

    def add(self, item):
        on = self._on()
        set.add(self, item)
        if on:
            self._r(item, "write", item)

    def discard(self, item):
        on = self._on()
        set.discard(self, item)
        if on:
            self._r(item, "write", None)

    def update(self, *its):
        for it in its:
            self._add_all(set.update, it)

    def __ior__(self, other):
        self.update(other)
        return self

    def difference_update(self, *its):
        for it in its:
            for item in list(it):
                self.discard(item)

    def __isub__(self, other):
        self.difference_update(other)
        return self

    def pop(self):
        return self._take(set.pop)

    def copy(self):
        return set(set.__iter__(self))

    def __reduce__(self):
        return set, (set.copy(self),)


class TrackedDeque(_MemberOps, collections.deque):
    __slots__ = ("_deja_name", "_deja_key")

    def __init__(self, *a, **kw):
        self._deja_name = self._deja_key = None
        collections.deque.__init__(self, *a, **kw)

    def _deja_len(self):
        return collections.deque.__len__(self)

    def append(self, item):
        on = self._on()
        collections.deque.append(self, item)
        if on:
            self._r(item, "write", item)

    def appendleft(self, item):
        on = self._on()
        collections.deque.appendleft(self, item)
        if on:
            self._r(item, "write", item)

    def extend(self, items):
        self._add_all(collections.deque.extend, items)

    def extendleft(self, items):
        self._add_all(collections.deque.extendleft, items)

    def pop(self):
        return self._take(collections.deque.pop)

    def popleft(self):
        return self._take(collections.deque.popleft)

    def count(self, item):
        on = self._on()
        n = collections.deque.count(self, item)
        if on:
            self._r(item, "read", n)
        return n

    def __getitem__(self, i):
        self._whole()
        return collections.deque.__getitem__(self, i)

    def rotate(self, n=1):
        self._whole("write")
        collections.deque.rotate(self, n)

    def reverse(self):
        self._whole("write")
        collections.deque.reverse(self)

    def copy(self):
        return collections.deque(collections.deque.__iter__(self), self.maxlen)

    def __copy__(self):
        return self.copy()

    def __reduce__(self):
        return collections.deque, (list(collections.deque.__iter__(self)), self.maxlen)


# exact type -> its tracked copy's class (deja.autotrack swaps exactly these)
COPIES = {
    dict: TrackedDict,
    collections.defaultdict: TrackedDefaultDict,
    collections.OrderedDict: TrackedOrderedDict,
    collections.Counter: TrackedCounter,
    list: TrackedList,
    set: TrackedSet,
    collections.deque: TrackedDeque,
}
TRACKED = tuple(COPIES.values())


def copy_of(obj, name, key=None):
    """A tracked copy of a container whose exact type is in :data:`COPIES`."""
    cls = COPIES[type(obj)]
    if cls is TrackedDefaultDict:
        new = cls(obj.default_factory, obj)
    elif cls is TrackedDeque:
        new = cls(obj, obj.maxlen)
    else:
        new = cls(obj)
    new._deja_name, new._deja_key = name, key
    return new


# ---------------------------------------------------------------------- objects
_classes: dict = {}


def _reported(self, attr):
    """True for a public data attribute in the instance dict (containers tracked on their own are reported by key)."""
    if attr[0] == "_":
        return False
    d = object.__getattribute__(self, "__dict__")
    if attr not in d:
        return False
    v = d[attr]
    return not callable(v) and not isinstance(v, TRACKED)


def _new_as(cls, *args):
    return cls.__new__(cls, *args)


_swap_lock = threading.RLock()


class _as_original:
    """Show tracked objects as their original class for a moment (to compare them)."""

    def __init__(self, *objs):
        self.objs = [(o, type(o)) for o in objs if type(o) in _classes.values()]

    def __enter__(self):
        _swap_lock.acquire()
        for o, t in self.objs:
            object.__setattr__(o, "__class__", t.__mro__[1])

    def __exit__(self, *exc):
        try:
            for o, t in self.objs:
                object.__setattr__(o, "__class__", t)
        finally:
            _swap_lock.release()


def _tracked_class(cls):
    if cls in _classes:
        return _classes[cls]

    class Tracked(cls):
        __slots__ = ()

        def __getattribute__(self, attr):
            if getattr(_local, "busy", False) or not _reported(self, attr):
                return super().__getattribute__(attr)
            _turn()
            v = super().__getattribute__(attr)
            touch(f"{object.__getattribute__(self, '__dict__')['_deja_name']}.{attr}", "read", v)
            return v

        def __setattr__(self, attr, v):
            if attr[0] == "_" or getattr(_local, "busy", False) or isinstance(v, TRACKED) or callable(v):
                return super().__setattr__(attr, v)
            _turn()
            super().__setattr__(attr, v)
            touch(f"{object.__getattribute__(self, '__dict__')['_deja_name']}.{attr}", "write", v)

        def __reduce_ex__(self, protocol):  # pickle / copy as the original class
            r = cls.__reduce_ex__(self, protocol)
            if isinstance(r, tuple) and len(r) > 1 and isinstance(r[1], tuple) and r[1] and r[1][0] is Tracked:
                r = (_new_as, (cls,) + r[1][1:]) + r[2:]  # (pickle refuses __newobj__ with another class)
            return r

        def __eq__(self, other):  # equal to an untracked object of the same class (dataclasses, pydantic compare types)
            with _as_original(self, other):
                return self == other

        __hash__ = cls.__hash__

    Tracked.__name__, Tracked.__qualname__, Tracked.__module__ = cls.__name__, cls.__qualname__, cls.__module__
    _classes[cls] = Tracked
    return Tracked


def _swappable(cls) -> bool:
    """Classes whose instances can be tracked in place: plain classes, ABCs and pydantic models (enums and other
    metaclasses build their instances in ways a subclass would break)."""
    import abc

    meta = type(cls)
    return meta in (type, abc.ABCMeta) or meta.__module__.startswith("pydantic.")


def track_object(obj, name):
    """Track ``obj``'s public data attributes in place (swap its class for a reporting subclass)."""
    if type(obj) not in _classes.values():
        obj.__class__ = _tracked_class(type(obj))
    obj.__dict__["_deja_name"] = name
    return obj


def track(obj, name: str = None, key: str = None):
    """Report reads and writes of ``obj`` to the recording. Returns the object to use (see module docs)."""
    if isinstance(obj, TRACKED):
        return obj
    if type(obj) in COPIES:
        return copy_of(obj, name or type(obj).__name__.lower(), key)
    for base in (
        collections.defaultdict,
        collections.OrderedDict,
        collections.Counter,
        dict,
        list,
        set,
        collections.deque,
    ):  # a subclass of its own: track a copy of the closest supported type
        if isinstance(obj, base):
            plain = collections.defaultdict(obj.default_factory, obj) if base is collections.defaultdict else base(obj)
            return copy_of(plain, name or base.__name__.lower(), key)
    if not hasattr(obj, "__dict__"):
        raise TypeError(f"deja.track can't track {type(obj).__name__}: it has no __dict__")
    return track_object(obj, name or type(obj).__name__.lower())
