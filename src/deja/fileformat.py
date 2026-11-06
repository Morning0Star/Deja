"""The ``.deja`` recording file: gzipped JSON Lines.

Line 1 is the header object. Every following line is one event as a JSON array. ``bytes`` values are written as
``{"b64": "..."}``; tuples become arrays. Stdlib only.
"""

import base64
import gzip
import json


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


def read(path: str) -> tuple[dict, list]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        header = json.loads(f.readline())
        events = [tuple(_decode(json.loads(line))) for line in f]
    return header, events
