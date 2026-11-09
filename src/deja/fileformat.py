"""The ``.deja`` recording file: gzipped JSON Lines.

Line 1 is the header object. Every following line is one event as a JSON array. ``bytes`` values are written as
``{"b64": "..."}``; tuples become arrays. Stdlib only.

The header's ``format`` names the layout (``deja/1``). Reading upgrades older layouts step by step through
:data:`UPGRADES` (each turns format N into N+1), and refuses a newer one with a message that says so, so old
recordings keep loading as Déjà changes and a recording made by a newer Déjà never replays wrongly.
"""

import base64
import gzip
import json

FORMAT = 1  # what write() produces


class FormatError(ValueError):
    """A recording this version of Déjà can't read."""


UPGRADES: dict = {}  # format N -> function(header, events) -> (header, events) in format N + 1


def _default(o):
    if isinstance(o, (bytes, bytearray, memoryview)):
        return {"b64": base64.b64encode(bytes(o)).decode()}
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    return repr(o)


def _decode(v):
    if isinstance(v, dict) and set(v) == {"b64"}:
        return base64.b64decode(v["b64"])
    if isinstance(v, list):
        return [_decode(x) for x in v]
    return v


def write(path: str, header: dict, events: list) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(json.dumps(header, default=_default) + "\n")
        for e in events:
            f.write(json.dumps(e, default=_default, separators=(",", ":")) + "\n")


def version(header: dict) -> int:
    fmt = str(header.get("format", ""))
    if not fmt.startswith("deja/"):
        raise FormatError(f"not a Déjà recording (format {fmt!r})")
    try:
        return int(fmt.split("/", 1)[1])
    except ValueError:
        raise FormatError(f"unknown recording format {fmt!r}") from None


def read(path: str) -> tuple[dict, list]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        header = json.loads(f.readline())
        events = [tuple(_decode(json.loads(line))) for line in f]
    n = version(header)
    if n > FORMAT:
        raise FormatError(
            f"{path} was recorded by a newer Déjà ({header.get('deja')}, format deja/{n}); "
            f"this one reads up to deja/{FORMAT}: upgrade deja-replay"
        )
    while n < FORMAT:
        header, events = UPGRADES[n](header, events)
        n += 1
        header["format"] = f"deja/{n}"
    return header, events
