"""Optional hints an app can give Déjà to make reports read like the domain.

    deja.touch("cart:42", "write", new_total)        # report an access to in-memory state
    deja.labels({"POST /purchase": "Purchase",
                 "POST payments/charge": "Charge card",
                 "gallery_wallet.balance": "Wallet balance"})

Both are no-ops outside a recording loop. Neither is needed for record or replay; they only feed the report.
"""

import asyncio

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


def touch(resource: str, kind: str = "read", value=None) -> None:
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
    LABELS.update({str(k): str(v) for k, v in mapping.items()})


def calls(mapping: dict) -> None:
    """Say what an outside HTTP call does to shared state, when its method doesn't:

        deja.calls({"POST pricing.internal/quote": "read",     # a POST that only computes a price
                    "GET payments.example/*/capture": "write",  # a GET that changes something
                    "GET metrics.internal/*": "ignore"})        # not part of the story

    Keys are ``"METHOD host/path"`` with ``fnmatch`` wildcards. By default GET and HEAD read, everything else writes.
    """
    for k, v in mapping.items():
        if v not in ("read", "write", "ignore"):
            raise ValueError(f'deja.calls: {k!r} must map to "read", "write" or "ignore", not {v!r}')
        CALL_KINDS[str(k)] = v
