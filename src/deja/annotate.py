"""Optional hints an app can give Déjà to make reports read like the domain.

    deja.touch("cart:42", "write", new_total)        # report an access to in-memory state
    deja.labels({"POST /purchase": "Purchase",
                 "POST payments/charge": "Charge card",
                 "gallery_wallet.balance": "Wallet balance"})

Both are no-ops outside a recording loop. Neither is needed for record or replay; they only feed the report.
"""

import asyncio

from .recorder import LABELS


def touch(resource: str, kind: str = "read", value=None) -> None:
    if kind not in ("read", "write"):
        raise ValueError('kind must be "read" or "write"')
    loop = asyncio._get_running_loop()
    rec = getattr(loop, "deja", None) if loop is not None else None
    if rec is None or getattr(rec, "replaying", False):
        return
    path = rec.current_path()
    if path is not None and path.startswith("q"):
        rec.touch(resource, kind, value)


def labels(mapping: dict) -> None:
    LABELS.update({str(k): str(v) for k, v in mapping.items()})
