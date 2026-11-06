"""Event codes. Events are small tuples ``(code, *fields)`` appended to the ring buffer on the hot path.

| code | fields | meaning |
|---|---|---|
| ``B`` | count | a loop pass is about to run ``count`` ready handles |
| ``S`` | task path | one step (or wakeup) of that task ran |
| ``P`` | | a non-task callback ("plumbing": future done-callbacks, gather, user call_soon) ran |
| ``N`` | path, parent path, coroutine name, id | a task was created (id = its ``id()``, used only to resolve steps) |
| ``G`` | id | a task was garbage-collected (its ``id()`` may be reused after this) |
| ``R`` | task path, source, value | a non-deterministic read inside a task (time, random, uuid, urandom) |
| ``Q`` | request id, task path, method, path, headers, body | an HTTP request started |
| ``A`` | request id, status | an HTTP request finished |
| ``X`` | request id or task path, exception type, message | an exception escaped a request |
| ``F`` | task path, reason | ``deja.flag()`` was called |
| ``M`` | label | a marker placed by ``Recorder.mark()`` (e.g. the start of a test trial) |

In the in-memory ring buffer a step is stored as a bare ``int`` (the owner's ``id()``) or the string ``"P"``, to keep
the hot path cheap; ``Recorder.events()`` resolves them into ``("S", path)`` / ``("P",)`` tuples.
"""

BATCH = "B"
STEP = "S"
PLUMB = "P"
TASK_NEW = "N"
TASK_GONE = "G"
READ = "R"
REQUEST = "Q"
RESPONSE = "A"
EXCEPTION = "X"
FLAG = "F"
MARK = "M"
