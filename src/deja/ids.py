"""Stable task identity.

Every task gets a *path* derived from where it was created: a root task is ``r0``, ``r1``... in creation order; a
child is its parent's path plus its ordinal among that parent's children (``r0.2.1``). Given the same execution
order, a replay creates the same tasks in the same order and so assigns the same paths.

Deliberately NOT used: task names (on 3.12 a task's first step is scheduled before its name is set, spike S1),
``id()`` values (reused after garbage collection), and set/hash ordering (Temporal sdk-python #2069).

Eager tasks run their first step inside their own constructor, so children can appear before the parent object is
registered. The task factory therefore *reserves* a path before constructing the task; the first time the
under-construction task is looked up, it claims the reserved path.
"""

import weakref


class TaskRegistry:
    __slots__ = ("_paths", "_refs", "_kids", "_roots", "_orphans", "_pending", "on_new", "on_gone")

    def __init__(self):
        self._paths: dict[int, str] = {}  # id(task) -> path; entries removed when the task is collected
        self._refs: dict[int, weakref.ref] = {}  # id(task) -> weakref whose callback forgets the task
        self._kids: dict[str, int] = {}  # path -> number of children created so far
        self._roots = 0
        self._orphans = 0
        self._pending: list[list] = []  # stack of [path, parent_path, claimed] for tasks under construction
        self.on_new = None  # callback(path, parent_path, task)
        self.on_gone = None  # callback(task_id) when a registered task is garbage-collected

    # ------------------------------------------------------------------ creation via the task factory
    def reserve(self, parent, root=None) -> list:
        if root is not None:  # a named root (a background task: b0, b1...)
            path, parent_path = root, None
        elif parent is None:
            path, parent_path = f"r{self._roots}", None
            self._roots += 1
        else:
            parent_path = self.path(parent)
            n = self._kids.get(parent_path, 0)
            self._kids[parent_path] = n + 1
            path = f"{parent_path}.{n}"
        slot = [path, parent_path, False]
        self._pending.append(slot)
        return slot

    def release(self, slot, task) -> str:
        """Finish construction: bind the reserved path to ``task`` unless it was already claimed during an eager step."""
        if self._pending and self._pending[-1] is slot:
            self._pending.pop()
        else:
            self._pending.remove(slot)
        if task is not None and not slot[2]:
            self._bind(task, slot)
        return slot[0]

    def register(self, task, parent=None) -> str:
        """Register a task created outside the factory path (convenience for tests and adapters)."""
        known = self._paths.get(id(task))
        if known is not None:
            return known
        slot = self.reserve(parent)
        return self.release(slot, task)

    def rebind(self, task, path: str) -> str:
        """Give a live task a new path (request scope: ``q{rid}``); returns the old one.

        Children created afterwards are numbered under the new path. Emits ``on_new`` so recordings map the task's
        id to the new path from this point on. The weakref that forgets the task is keyed by id, so it still fires.
        """
        key = id(task)
        old = self._paths.get(key) or self.path(task)
        self._paths[key] = path
        self._kids[path] = 0
        if self.on_new is not None:
            self.on_new(path, None, task)
        return old

    # ------------------------------------------------------------------ lookup
    def path(self, task) -> str:
        path = self._paths.get(id(task))
        if path is not None:
            return path
        for slot in reversed(self._pending):  # an eager task looking itself up mid-construction
            if not slot[2]:
                self._bind(task, slot)
                return slot[0]
        key = id(task)
        path = f"o{self._orphans}"
        self._orphans += 1
        self._paths[key] = path
        self._track(task, key, path)
        return path

    def get(self, task_id: int):
        """Hot-path lookup by ``id(task)``; None for unknown objects."""
        return self._paths.get(task_id)

    # ------------------------------------------------------------------ internals
    def _bind(self, task, slot):
        slot[2] = True
        key = id(task)
        self._paths[key] = slot[0]
        self._track(task, key, slot[0])
        if self.on_new is not None:
            self.on_new(slot[0], slot[1], task)

    def _track(self, task, key, path):
        # weakref.ref with a callback is several times cheaper than weakref.finalize (no global registry)
        self._refs[key] = weakref.ref(task, lambda r, k=key, p=path: self._forget(k, p, r))

    def _forget(self, key, path, ref=None):
        # On CPython the weakref callback runs while the dying task still owns its id. On PyPy it can run later,
        # after a new task got the same id: then the entry is the new task's and must stay.
        if ref is not None and self._refs.get(key) is not ref:
            return
        self._refs.pop(key, None)
        current = self._paths.pop(key, None)
        self._kids.pop(path, None)
        if current is not None and current != path:
            self._kids.pop(current, None)
        if self.on_gone is not None:
            self.on_gone(key)

    def __len__(self):
        return len(self._paths)
