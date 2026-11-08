"""Track shared in-memory state automatically, so the report sees it without ``deja.track`` calls.

On the first request, :class:`deja.DejaMiddleware` (and the replay harness) looks at the app's code: its route
handlers and FastAPI dependencies, and every function, class and module of the app they use, followed through
their code (``services.reserve()`` -> ``services`` -> ``HOLDS``). What it finds there:

* **Containers.** Each ``dict``, ``defaultdict``, ``OrderedDict``, ``Counter``, ``list``, ``set`` or ``deque`` held
  by a module variable, a closure (state created inside an app factory), an attribute of one of the app's objects
  (``store.carts``) or ``app.state`` is replaced by a tracked copy (see :mod:`deja.track`), named after the
  variable (``carts``, ``store.carts``, ``services.HOLDS``).

  Swapping an object is only safe if every reference to it is swapped too, so Déjà finds all referrers with
  :func:`gc.get_referrers` and only swaps an object whose referrers are module namespaces, instance attributes,
  ``app.state`` or closure cells. An object also held anywhere else (a default argument, a tuple, another list, a
  running coroutine) is left alone.
* **Objects.** An app object whose public attributes request code assigns (``self.balance -= amount``) is tracked
  in place: its class is swapped for a subclass that reports reads and writes of its data attributes.
* **Module variables.** A module-level number or string that request code reassigns with ``global`` (``global
  visits; visits += 1``) can't be swapped, so the functions that read or assign it are watched with
  :mod:`sys.monitoring` line events instead: only those functions, and only the lines that use the variable report.

What it can't see: state held only by library objects, state reached through ``getattr`` with computed names, and
objects inside containers (``accounts[7].balance``: the read of ``accounts[7]`` is reported, the attribute is
not). ``deja.track`` and ``deja.touch`` cover those.

Pass ``DejaMiddleware(app, track=False)`` to switch this off, or choose what is tracked with patterns (``fnmatch``
wildcards) matched against the name (``store.cache``, ``services.*``) or the class (``myapp.models.Account``):
``DejaMiddleware(app, track_skip=["*.cache", "myapp.clients.*"])`` leaves those alone, ``track_only=[...]`` tracks
only those. Use it for objects whose class must not be swapped (code that checks ``type(x) is Cls``). The patterns
are saved in the recording and replay applies the same ones.
"""

import dis
import fnmatch
import gc
import importlib
import inspect
import logging
import os
import sys
import sysconfig
import types

from . import annotate
from .track import COPIES, TRACKED, copy_of, track_object

_track = importlib.import_module("deja.track")  # (`from . import track` is the deja.track function)

log = logging.getLogger("deja")
PLAIN = COPIES  # exact types that get a tracked copy
MAX_FUNCTIONS = 2000
SCALARS = (int, float, complex, str, bytes, bool, type(None), tuple, frozenset)
_LIBRARY_DIRS = tuple(os.path.normcase(os.path.abspath(p)) for p in {
    sysconfig.get_paths().get("stdlib"), sysconfig.get_paths().get("platstdlib"),
    sysconfig.get_paths().get("purelib"), sysconfig.get_paths().get("platlib")} if p)
_DEJA_DIR = os.path.normcase(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------- what belongs to the app
def _app_file(path):
    """True for a source file of the app itself (not the standard library, an installed package or Déjà)."""
    if not path:
        return False
    path = os.path.normcase(os.path.abspath(path))
    return not path.startswith(_LIBRARY_DIRS) and "site-packages" not in path and not path.startswith(_DEJA_DIR)


def _app_globals(g):
    return isinstance(g, dict) and (_app_file(g.get("__file__")) or g.get("__name__") == "__main__")


def _app_module(m):
    return isinstance(m, types.ModuleType) and _app_globals(vars(m))


def _app_class(c):
    if not isinstance(c, type):
        return False
    mod = sys.modules.get(c.__module__)
    return _app_module(mod) if mod is not None else c.__module__ == "__main__"


def _is_app_object(v):
    """An instance of a class from the app's own code."""
    if isinstance(v, (type, types.ModuleType, types.FunctionType, types.MethodType)) or not hasattr(v, "__dict__"):
        return False
    return _app_class(type(v))


def _function(v):
    """The app function behind a callable (bound method, staticmethod, decorated, callable object), or None."""
    if isinstance(v, (staticmethod, classmethod, types.MethodType)):
        v = v.__func__
    if callable(v) and not isinstance(v, type):
        try:
            v = inspect.unwrap(v)
        except ValueError:
            pass
    if callable(v) and not isinstance(v, (type, types.FunctionType)):
        v = getattr(type(v), "__call__", None)  # a dependency class instance
    return v if isinstance(v, types.FunctionType) and _app_globals(v.__globals__) else None


def _endpoints(app):
    """Route handlers, unwrapped: Starlette / FastAPI (and their dependencies), Litestar, Quart / Flask, Django."""
    out = []
    for fn in _other_framework_handlers(app):
        fn = _function(fn)
        if fn is not None:
            out.append(fn)
    routes = getattr(app, "routes", None) or getattr(getattr(app, "router", None), "routes", [])
    seen, stack = set(), [routes if isinstance(routes, (list, tuple)) else []]  # (aiohttp's is a method)
    while stack:
        for route in stack.pop() or []:
            sub = getattr(route, "routes", None) or getattr(getattr(route, "app", None), "routes", None)
            if sub and id(sub) not in seen:
                seen.add(id(sub))
                stack.append(sub)
            fns = [getattr(route, "endpoint", None)]
            for handler in getattr(route, "route_handlers", None) or []:  # Litestar
                fns.append(getattr(handler, "fn", None))
            dep = getattr(route, "dependant", None)
            todo = list(getattr(dep, "dependencies", []) or [])
            while todo:
                d = todo.pop()
                fns.append(getattr(d, "call", None))
                todo.extend(getattr(d, "dependencies", []) or [])
            for fn in fns:
                fn = _function(fn)
                if fn is not None:
                    out.append(fn)
    return out


def _other_framework_handlers(app):
    """Handlers of frameworks that don't keep Starlette-style routes."""
    router = getattr(app, "router", None)
    if type(app).__module__.startswith("aiohttp.") and router is not None:  # aiohttp web.Application
        for route in router.routes():
            yield getattr(route, "handler", None)
    views = getattr(app, "view_functions", None)  # Quart / Flask
    if isinstance(views, dict):
        yield from views.values()
    if type(app).__module__.startswith("django."):  # Django's ASGIHandler: the URL resolver has the views
        try:
            from django.urls import get_resolver
        except ImportError:
            return
        stack = list(get_resolver().url_patterns)
        while stack:
            p = stack.pop()
            if hasattr(p, "url_patterns"):
                stack.extend(p.url_patterns)
            elif getattr(p, "callback", None) is not None:
                yield p.callback


def _codes(code):
    """A code object and every code object nested in it (inner functions, comprehensions, lambdas)."""
    out, todo = [], [code]
    while todo:
        c = todo.pop()
        out.append(c)
        todo.extend(x for x in c.co_consts if isinstance(x, types.CodeType))
    return out


class _Reach:
    """The app code the endpoints can reach: functions, the modules they live in, what they assign."""

    def __init__(self, app):
        self.functions: list = []
        self.modules: dict = {}  # id(globals) -> globals of the app modules seen
        self.stored_attrs: set = set()  # attribute names assigned anywhere in that code
        self.globals_written: dict = {}  # id(globals) -> names assigned with `global`
        self._seen: set = set()
        self._classes: set = set()
        todo = _endpoints(app)
        self.endpoint_modules = {id(f.__globals__) for f in todo}
        while todo and len(self.functions) < MAX_FUNCTIONS:
            fn = todo.pop(0)
            if id(fn) in self._seen:
                continue
            self._seen.add(id(fn))
            self.functions.append(fn)
            todo.extend(self._visit(fn))

    def name(self, g, var):
        """``var`` in an endpoint's module; ``module.var`` in any other module of the app."""
        if id(g) in self.endpoint_modules:
            return var
        return f"{str(g.get('__name__', '')).rpartition('.')[2]}.{var}"

    def _methods(self, cls):
        if cls in self._classes:
            return []
        self._classes.add(cls)
        out = []
        for klass in cls.__mro__:
            if not _app_class(klass):
                continue
            for v in vars(klass).values():
                f = _function(v.fget) if isinstance(v, property) else _function(v)
                if f is not None:
                    out.append(f)
        return out

    def _visit(self, fn):
        g = fn.__globals__
        self.modules[id(g)] = g
        names = set()
        for code in _codes(fn.__code__):
            names.update(code.co_names)
            for ins in dis.get_instructions(code):
                if ins.opname == "STORE_GLOBAL":
                    self.globals_written.setdefault(id(g), set()).add(ins.argval)
                elif ins.opname == "STORE_ATTR":
                    self.stored_attrs.add(ins.argval)
        values = [g[n] for n in names if n in g]
        for cell in fn.__closure__ or ():
            try:
                values.append(cell.cell_contents)
            except ValueError:
                pass
        found = []
        for v in values:
            found.extend(self._follow(v, names))
        return found

    def _follow(self, v, names):
        if _app_module(v):
            self.modules[id(vars(v))] = vars(v)
            out = []
            for n in names:
                a = vars(v).get(n)
                if a is not None and a is not v and not isinstance(a, types.ModuleType):
                    out.extend(self._follow(a, ()))
            return out
        if _app_class(v):
            return self._methods(v)
        if _is_app_object(v):
            return self._methods(type(v))
        if isinstance(v, (dict, list, tuple)) and len(v) <= 200:  # a table of handlers: {"orders": handle_order}
            out = []
            for x in (v.values() if isinstance(v, dict) else v):
                f = _function(x) if callable(x) else None
                if f is not None:
                    out.append(f)
            return out
        f = _function(v)
        return [f] if f is not None else []


# ---------------------------------------------------------------------- containers
class Rules:
    """Which names / classes automatic tracking may touch (``track_skip`` / ``track_only`` patterns)."""

    def __init__(self, skip=(), only=None):
        self.skip, self.only = list(skip or ()), (list(only) if only is not None else None)

    def __call__(self, label, obj=None) -> bool:
        names = [label]
        if obj is not None:
            names.append(f"{type(obj).__module__}.{type(obj).__qualname__}")
        if any(fnmatch.fnmatchcase(n, p) for p in self.skip for n in names):
            return False
        return self.only is None or any(fnmatch.fnmatchcase(n, p) for p in self.only for n in names)

    def to_dict(self):
        return {"skip": self.skip, "only": self.only}


ALL = Rules()


def _candidates(app, reach, wanted=ALL):
    """(containers, names, owner dicts, app objects): what the app's code can reach, and where it was found."""
    objs, names, owners, seen, apps = [], [], set(), set(), {}

    def add(v, name):
        if type(v) in PLAIN and id(v) not in seen and wanted(name, v):
            seen.add(id(v))
            objs.append(v)
            names.append(name)

    def scan(namespace, prefix="", depth=0):
        owners.add(id(namespace))
        for k, v in list(namespace.items()):
            if not isinstance(k, str) or k.startswith("_"):
                continue
            add(v, prefix + k)
            if depth < 2 and _is_app_object(v) and id(v) not in apps and wanted(prefix + k, v):
                apps[id(v)] = (v, prefix + k)
                scan(vars(v), f"{prefix}{k}.", depth + 1)

    state = getattr(getattr(app, "state", None), "_state", None)
    if isinstance(state, dict):
        scan(state)
    for g in reach.modules.values():
        scan(g, reach.name(g, ""))
    for fn in reach.functions:
        for name, cell in zip(fn.__code__.co_freevars, fn.__closure__ or ()):
            try:
                v = cell.cell_contents
            except ValueError:  # not filled yet
                continue
            add(v, name)
            if _is_app_object(v) and id(v) not in apps and wanted(name, v):
                apps[id(v)] = (v, name)
                scan(vars(v), f"{name}.", 1)
    return objs, names, owners, apps


def _swap(objs, names, owners):
    """Replace each container by a tracked copy wherever it is referenced, when that is provably everywhere."""
    if not objs:
        return []
    module_dicts = {id(vars(m)) for m in list(sys.modules.values()) if m is not None and hasattr(m, "__dict__")}
    index = {id(o): i for i, o in enumerate(objs)}
    where = [[] for _ in objs]
    unsafe = set()
    for r in gc.get_referrers(*objs):
        if r is objs or (type(r) is tuple and len(r) == len(objs) and all(a is b for a, b in zip(r, objs))):
            continue  # our own list, and the argument tuple of this very call
        if type(r) is types.FrameType and r.f_code in _OWN_CODE:
            continue  # Déjà's own frames (PyPy reports running frames as referrers; CPython doesn't)
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
                if id(v) in index and v is objs[index[id(v)]] and getattr(v, "__dict__", None) is not r:
                    unsafe.add(index[id(v)])  # (PyPy: an object's own attribute dict refers back to it)
    # locals of functions running right now (any thread) don't show up as referrers: look at the stacks
    for frame in sys._current_frames().values():
        while frame is not None:
            if frame.f_code not in _OWN_CODE:  # Déjà's own frames hold the candidates too
                try:
                    values = list(frame.f_locals.values())
                except Exception:
                    values = []
                for v in values:
                    i = index.get(id(v))
                    if i is not None and v is objs[i]:
                        unsafe.add(i)
            frame = frame.f_back
    done = []
    for i, (obj, name) in enumerate(zip(objs, names)):
        if i in unsafe or not where[i]:
            continue
        tracked = copy_of(obj, name)
        for holder, key in where[i]:
            if key is None:
                holder.cell_contents = tracked
            else:
                holder[key] = tracked
        done.append(name)
    return done


# ---------------------------------------------------------------------- objects
def _track_objects(apps, stored_attrs):
    """Track app objects in place when the app's code assigns one of their public data attributes."""
    done = []
    for obj, name in apps.values():
        cls = type(obj)
        if cls in _track._classes.values() or not _track._swappable(cls):  # enums and other metaclasses: leave them
            continue
        attrs = {k for k, v in vars(obj).items() if k[0] != "_" and not callable(v) and not isinstance(v, TRACKED)}
        if not attrs & stored_attrs:
            continue
        try:
            track_object(obj, name)
        except TypeError:  # __class__ can't be assigned (an incompatible layout)
            continue
        done.append(name)
    return done


# ---------------------------------------------------------------------- module variables (sys.monitoring)
_lines: dict = {}  # code -> {line: (globals, reads, writes)}; reads / writes are (variable, label) pairs
_pending: dict = {}  # id(frame) -> (frame, globals, writes): writes to report once their line has run
_tool = None


_containers: set = set()  # labels of module variables holding containers (reassigned whole, e.g. CACHE = {})


def _flush(frame):
    p = _pending.get(id(frame))
    if p is not None and p[0] is frame:
        del _pending[id(frame)]
        g = p[1]
        for var, label in p[2]:
            if label in _containers:  # a new container replaced the tracked one: track the new one too
                v = g.get(var)
                if type(v) in PLAIN:
                    _swap([v], [label], {id(g)})
                _track.touch(f"{label}[*]", "write", None)
            else:
                _track.touch(label, "write", g.get(var))


def _on_line(code, line):
    frame = sys._getframe(1)
    if _pending:
        _flush(frame)  # the previous line of this frame assigned a watched variable: it has the new value now
    entry = _lines.get(code, {}).get(line)
    if entry is None:
        return
    g, reads, writes = entry
    if annotate._recorder() is None:
        _track._turn(len(reads) + len(writes))  # replay, in a worker thread: wait for this line's turn
        return
    for var, label in reads:
        _track.touch(label, "read", g.get(var))
    if writes:
        if len(_pending) > 1000:  # frames that raised before their next line
            _pending.clear()
        _pending[id(frame)] = (frame, g, writes)


def _on_return(code, offset, retval):
    if _pending:
        _flush(sys._getframe(1))


def _monitor(reach, wanted=ALL):
    """Watch the lines that read or assign module variables the app's code reassigns with ``global``."""
    global _tool
    mon = getattr(sys, "monitoring", None)
    if mon is None:
        return []
    targets = {}  # id(globals) -> {variable: label}
    for gid, written in reach.globals_written.items():
        g = reach.modules.get(gid)
        for var in written:
            if g is None or var not in g or not wanted(reach.name(g, var), g[var]):
                continue
            if isinstance(g[var], SCALARS):
                targets.setdefault(gid, {})[var] = reach.name(g, var)
            elif type(g[var]) in PLAIN or isinstance(g[var], TRACKED):  # reassigned whole: watch its assignments
                label = reach.name(g, var)
                targets.setdefault(gid, {})[var] = label
                _containers.add(label)
    if not targets:
        return []
    if _tool is None:
        for tid in (4, 3, 5, 2):
            try:
                mon.use_tool_id(tid, "deja")
            except ValueError:  # taken by a debugger, coverage or a profiler
                continue
            _tool = tid
            mon.register_callback(tid, mon.events.LINE, _on_line)
            mon.register_callback(tid, mon.events.PY_RETURN, _on_return)
            break
        if _tool is None:
            log.warning("deja: every sys.monitoring tool slot is taken (a debugger, coverage or a profiler?); "
                        "module variables %s are not tracked", ", ".join(sorted(
                            label for t in targets.values() for label in t.values())))
            return []
    for fn in reach.functions:
        g = fn.__globals__
        want = targets.get(id(g))
        if not want:
            continue
        for code in _codes(fn.__code__):
            if code in _lines:
                continue
            lines = {}
            for ins in dis.get_instructions(code):
                if ins.opname in ("LOAD_GLOBAL", "STORE_GLOBAL") and ins.argval in want:
                    line = ins.positions.lineno if ins.positions else None
                    if line is None:
                        continue
                    r, w = lines.setdefault(line, ([], []))
                    if ins.opname == "STORE_GLOBAL":
                        w.append((ins.argval, want[ins.argval]))
                    elif want[ins.argval] not in _containers:  # a container's reads are reported by the container
                        r.append((ins.argval, want[ins.argval]))
            if lines:
                _lines[code] = {ln: (g, tuple(dict.fromkeys(r)), tuple(dict.fromkeys(w)))
                                for ln, (r, w) in lines.items()}
                mon.set_local_events(_tool, code, mon.events.LINE | mon.events.PY_RETURN)
    return sorted({label for t in targets.values() for label in t.values()})


_OWN_CODE = {f.__code__ for f in (_swap, _flush, _on_line, _on_return)}


# ---------------------------------------------------------------------- entry point
def prepare(app, track=True, rules=ALL) -> tuple:
    """What the middleware (on the first request) and replay (after startup) do once per app: fingerprint the
    source the routes reach, point clock / random / date names in those modules at the recording wrappers
    (:func:`deja.shims.rebind`), and track shared state. Returns ``(tracked names, source fingerprint)``."""
    import time

    from .shims import rebind
    from .source import fingerprint

    t0 = time.perf_counter()
    try:
        reach = _Reach(app)
    except Exception:
        log.warning("deja: could not follow the app's routes", exc_info=True)
        return [], {}
    source = fingerprint(app, reach)
    try:
        rebind(reach.modules.values())
    except Exception:
        log.warning("deja: could not rebind clock names in the app", exc_info=True)
    tracked = auto_track(app, reach, rules) if track else []
    log.info("deja: prepared the app in %.0f ms (%d functions, %d modules; tracking %s)",
             1000 * (time.perf_counter() - t0), len(reach.functions), len(reach.modules), ", ".join(tracked) or "nothing")
    return tracked, source


def auto_track(app, reach=None, rules=ALL) -> list:
    """Track the shared in-memory state reachable from ``app``'s routes. Returns the names now tracked."""
    try:
        reach = reach or _Reach(app)
        objs, names, owners, apps = _candidates(app, reach, rules)
        done = _swap(objs, names, owners)
        done += _track_objects(apps, reach.stored_attrs)
        done += _monitor(reach, rules)
        return done
    except Exception:  # never let the report's helper break the app
        log.warning("deja: automatic tracking failed; in-memory state needs deja.track()", exc_info=True)
        return []
