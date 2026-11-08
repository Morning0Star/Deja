"""Track shared in-memory state automatically, so the report sees it without ``deja.track`` calls.

On the first request, :class:`deja.DejaMiddleware` looks at what the app's route handlers (and their FastAPI
dependencies) can reach:

* module-level variables of the modules that define them;
* variables their closures captured (state created inside an app factory);
* public attributes of the app's own objects found there (``store.carts`` on a ``Store()`` instance);
* ``app.state``.

Each plain ``dict``, ``list`` or ``set`` among them is replaced by a tracked copy (see :mod:`deja.track`), named
after the variable (``carts``, ``store.carts``).

Swapping an object is only safe if every reference to it is swapped too, so Déjà looks for all referrers with
:func:`gc.get_referrers` and only touches an object whose referrers are module namespaces, instance attribute dicts,
``app.state`` or closure cells. An object also held anywhere else (a default argument, a tuple, another list, a
running coroutine) is left alone. Subclasses (``defaultdict``, ``OrderedDict``) are left alone too, because the
tracked copy would lose their behaviour.

Pass ``DejaMiddleware(app, track=False)`` to switch this off.
"""

import gc
import inspect
import os
import sys
import sysconfig
import types

from .track import TrackedDict, TrackedList, TrackedSet

PLAIN = {dict: TrackedDict, list: TrackedList, set: TrackedSet}
_LIBRARY_DIRS = tuple(os.path.normcase(os.path.abspath(p)) for p in {
    sysconfig.get_paths().get("stdlib"), sysconfig.get_paths().get("platstdlib"),
    sysconfig.get_paths().get("purelib"), sysconfig.get_paths().get("platlib")} if p)


def _endpoints(app):
    """Route handlers and their dependencies (Starlette / FastAPI), unwrapped."""
    seen, stack, out = set(), [getattr(app, "routes", None) or getattr(getattr(app, "router", None), "routes", [])], []
    while stack:
        for route in stack.pop() or []:
            sub = getattr(route, "routes", None) or getattr(getattr(route, "app", None), "routes", None)
            if sub and id(sub) not in seen:
                seen.add(id(sub))
                stack.append(sub)
            fns = [getattr(route, "endpoint", None)]
            dep = getattr(route, "dependant", None)
            todo = list(getattr(dep, "dependencies", []) or [])
            while todo:
                d = todo.pop()
                fns.append(getattr(d, "call", None))
                todo.extend(getattr(d, "dependencies", []) or [])
            for fn in fns:
                if callable(fn):
                    try:
                        fn = inspect.unwrap(fn)
                    except ValueError:
                        pass
                    if isinstance(fn, types.FunctionType):
                        out.append(fn)
    return out


def _app_file(path):
    """True for a source file of the app itself (not the standard library or an installed package)."""
    if not path:
        return False
    path = os.path.normcase(os.path.abspath(path))
    return not path.startswith(_LIBRARY_DIRS) and "site-packages" not in path


def _is_app_object(v):
    """An instance of a class from the app's own code."""
    if isinstance(v, (type, types.ModuleType, types.FunctionType, types.MethodType)) or not hasattr(v, "__dict__"):
        return False
    mod = sys.modules.get(type(v).__module__)
    path = getattr(mod, "__file__", None)
    return _app_file(path) if path else type(v).__module__ == "__main__"


def _candidates(app):
    """(objects, names, owner dicts): plain containers reachable from the app, and the namespaces they were found in."""
    objs, names, owners, seen = [], [], set(), set()

    def add(v, name):
        if type(v) in PLAIN and id(v) not in seen:
            seen.add(id(v))
            objs.append(v)
            names.append(name)

    def scan(namespace, prefix=""):
        owners.add(id(namespace))
        for k, v in list(namespace.items()):
            if not isinstance(k, str) or k.startswith("_"):
                continue
            add(v, prefix + k)
            if not prefix and _is_app_object(v):
                scan(vars(v), f"{k}.")

    state = getattr(getattr(app, "state", None), "_state", None)
    if isinstance(state, dict):
        scan(state)
    for fn in _endpoints(app):
        g = fn.__globals__
        if not (_app_file(g.get("__file__")) or g.get("__name__") == "__main__"):
            continue  # a library's own routes (FastAPI's /docs)
        scan(g)
        for name, cell in zip(fn.__code__.co_freevars, fn.__closure__ or ()):
            try:
                v = cell.cell_contents
            except ValueError:  # not filled yet
                continue
            add(v, name)
            if _is_app_object(v):
                scan(vars(v), f"{name}.")
    return objs, names, owners


def _swap(objs, names, owners):
    """Replace each object by a tracked copy wherever it is referenced, when that is provably everywhere."""
    if not objs:
        return []
    module_dicts = {id(vars(m)) for m in list(sys.modules.values()) if m is not None and hasattr(m, "__dict__")}
    index = {id(o): i for i, o in enumerate(objs)}
    where = [[] for _ in objs]
    unsafe = set()
    for r in gc.get_referrers(*objs):
        if r is objs or (type(r) is tuple and len(r) == len(objs) and all(a is b for a, b in zip(r, objs))):
            continue  # our own list, and the argument tuple of this very call
        if type(r) is dict and (id(r) in owners or id(r) in module_dicts):
            for k, v in r.items():
                i = index.get(id(v))
                if i is not None and v is objs[i]:
                    where[i].append((r, k))
            if id(r) in index:  # a candidate inside another candidate: the outer copy would keep the old inner
                for v in r.values():
                    if id(v) in index:
                        unsafe.add(index[id(v)])
        elif type(r) is types.CellType:
            i = index.get(id(r.cell_contents))
            if i is not None:
                where[i].append((r, None))
        elif type(getattr(r, "__dict__", None)) is dict and id(r.__dict__) in owners:
            # Python 3.12+ keeps instance attributes inline, so the object itself refers to them
            for k, v in r.__dict__.items():
                i = index.get(id(v))
                if i is not None and v is objs[i]:
                    where[i].append((r.__dict__, k))
        else:
            for v in gc.get_referents(r):
                if id(v) in index and v is objs[index[id(v)]]:
                    unsafe.add(index[id(v)])
    done = []
    for i, (obj, name) in enumerate(zip(objs, names)):
        if i in unsafe or not where[i]:
            continue
        tracked = PLAIN[type(obj)](obj, name)
        for holder, key in where[i]:
            if key is None:
                holder.cell_contents = tracked
            else:
                holder[key] = tracked
        done.append(name)
    return done


def auto_track(app) -> list:
    """Track the shared in-memory state reachable from ``app``'s routes. Returns the names now tracked."""
    try:
        objs, names, owners = _candidates(app)
        return _swap(objs, names, owners)
    except Exception:  # never let the report's helper break the app
        return []
