"""Shrink a recording to the requests the race needs.

    deja shrink recording.deja --app myapp:create_app -o small.deja

The requests in the first conflict are kept. Every other request is a candidate: Déjà first tries removing all of
them, then (if the race no longer replays) one at a time, keeping each removal only if a replay of the smaller
recording still follows the recorded steps and the kept requests answer exactly as recorded. A request that
changed state the race depends on (a write the racing requests read) therefore stays. The events before the first
kept request are dropped too.
"""

from .analysis import analyze
from .fileformat import read, write
from .replay import Schedule, replay


def _root(path):
    return path.split(".", 1)[0] if path else None


def without(header, events, removed: set):
    """The recording with requests ``removed`` (``q3``...) taken out, and nothing before the first kept request."""
    out = []
    for e in events:
        code = e[0]
        if code in ("Q", "A", "X") and isinstance(e[1], int) and f"q{e[1]}" in removed:
            continue
        path = e[1] if code in ("S", "R", "I", "O", "T", "F", "N") else None
        if isinstance(path, str) and _root(path) in removed:
            continue
        if code == "N" and isinstance(e[2], str) and _root(e[2]) in removed:
            continue
        out.append(e)
    first = next((i for i, e in enumerate(out) if e[0] == "Q"), 0)
    return header, out[first:]


def _ok(header, events, factory, keep, recorded, deadline_s):
    r = replay(Schedule(header, events), factory, deadline_s=deadline_s)
    return r.followed and all(
        r.replayed.get(int(q[1:])) == recorded.get(int(q[1:])) for q in keep if q[0] == "q" and int(q[1:]) in recorded
    )


def shrink(recording: str, app_factory, out: str, deadline_s: float = 10.0) -> dict:
    """Write the smallest recording found to ``out``; returns ``{"kept", "removed", "events"}``."""
    header, events = read(recording)
    a = analyze(header, events)
    if not a["conflicts"]:
        raise ValueError("no race in this recording to shrink to")
    c = a["conflicts"][0]
    keep = {c["a"], c["b"]}
    everyone = {f"q{e[1]}" for e in events if e[0] == "Q"}
    recorded = Schedule(header, events).responses
    candidates = sorted(everyone - keep, key=lambda q: int(q[1:]))
    removed = set()
    if candidates and _ok(*without(header, events, set(candidates)), app_factory, keep, recorded, deadline_s):
        removed = set(candidates)
    else:
        for q in candidates:
            trial = removed | {q}
            if _ok(*without(header, events, trial), app_factory, keep, recorded, deadline_s):
                removed = trial
    h, small = without(header, events, removed)
    h = {**h, "shrunk_from": recording, "removed_requests": sorted(removed)}
    write(out, h, small)
    return {"kept": sorted(everyone - removed), "removed": sorted(removed), "events": (len(events), len(small))}
