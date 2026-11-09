"""Optional hints an app can give Déjà to make reports read like the domain.

    deja.touch("cart:42", "write", new_total)        # report an access to in-memory state
    deja.labels({"POST /purchase": "Purchase",
                 "POST payments/charge": "Charge card",
                 "gallery_wallet.balance": "Wallet balance"})

Both are no-ops outside a recording loop. Neither is needed for record or replay; they only feed the report.
"""

import asyncio
from typing import Any

from .recorder import CALL_KINDS, LABELS
from .threads import _thread


def _recorder():
    """The recorder when the current code runs inside a request being recorded, else None."""
    loop = asyncio._get_running_loop()
    rec = getattr(loop, "deja", None) if loop is not None else None
    if rec is None:
        t = _thread.get()  # a worker-thread call made from request scope
        rec = t[0] if t is not None else None
    if rec is None or getattr(rec, "replaying", False):
        return None
    path = rec.current_path()
    return rec if path and path[0] in "qb" else None


def touch(resource: str, kind: str = "read", value: Any = None) -> None:
    """Report an access to shared state that Déjà can't see by itself.

    Use it for state held by a library object or reached with computed names. The report treats the access like
    any other read or write, so two requests touching the same ``resource`` can show up as a race. In a worker
    thread during replay, it also waits until the recording says it's this access's turn. Outside a recorded
    request it costs one check.

    Args:
        resource: Name of the thing accessed, the same string for every access to it (``"cart:42"``,
            ``"inventory[sku-1]"``).
        kind: ``"read"`` or ``"write"``.
        value: The value read or written, shown in the report. Optional.

    Raises:
        ValueError: If ``kind`` is neither ``"read"`` nor ``"write"``.

    Example:
        ```python
        total = cache.get(f"cart:{cart_id}")
        deja.touch(f"cart:{cart_id}", "read", total)
        ```
    """
    if kind not in ("read", "write"):
        raise ValueError('kind must be "read" or "write"')
    rec = _recorder()
    if rec is not None:
        rec.touch(resource, kind, value)
    else:
        _gate()


def _gate() -> bool:
    """During replay, in a worker thread: wait until the schedule reaches this access (see deja.threads)."""
    t = _thread.get()
    if t is not None and getattr(t[0], "replaying", False):
        t[0].thread_gate(t[1])
        return True
    return False


def labels(mapping: dict) -> None:
    """Give requests, calls and state readable names in reports.

    Labels only change what the report says; recording and replay ignore them. Call it once, at import or
    startup. Later calls add to (and override) earlier ones.

    Args:
        mapping: Raw name to label. Keys are what the report would otherwise show: a route (``"POST /purchase"``),
            an outside call (``"POST payments/charge"``) or a piece of state (``"gallery_wallet.balance"``).

    Example:
        ```python
        deja.labels({
            "POST /purchase": "Purchase",
            "POST payments/charge": "Charge card",
            "gallery_wallet.balance": "Wallet balance",
        })
        ```
    """
    LABELS.update({str(k): str(v) for k, v in mapping.items()})


def calls(mapping: dict) -> None:
    """Say what an outside HTTP call does to shared state, when its method doesn't.

    The report treats each outside call as a read or a write of the remote resource, so that two requests calling
    the same endpoint can show up as a race. By default GET and HEAD read and every other method writes.

    Args:
        mapping: ``"METHOD host/path"`` pattern (``fnmatch`` wildcards) to ``"read"``, ``"write"`` or
            ``"ignore"`` (leave the call out of the analysis).

    Raises:
        ValueError: If a value isn't ``"read"``, ``"write"`` or ``"ignore"``.

    Example:
        ```python
        deja.calls({
            "POST pricing.internal/quote": "read",      # a POST that only computes a price
            "GET payments.example/*/capture": "write",  # a GET that changes something
            "GET metrics.internal/*": "ignore",         # not part of the story
        })
        ```
    """
    for k, v in mapping.items():
        if v not in ("read", "write", "ignore"):
            raise ValueError(f'deja.calls: {k!r} must map to "read", "write" or "ignore", not {v!r}')
        CALL_KINDS[str(k)] = v
