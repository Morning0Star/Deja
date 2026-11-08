"""A fingerprint of the app's own source code, so a replay can say when the code changed since the recording.

The fingerprint is a hash of each source file of the app that the routes' code reaches (the same modules
:mod:`deja.autotrack` follows), keyed by module name. The recording keeps it in its header; replay computes it again
for the app it replays and lists the modules that differ. A different hash doesn't stop the replay (a comment
changed, or the fix you are testing): it explains a divergence before you go looking for one.
"""

import hashlib

MAX_FILES = 300


def fingerprint(app, reach=None) -> dict:
    """``{module name: short sha1 of its source file}`` for the app's modules the routes reach."""
    from .autotrack import _Reach, _app_globals

    try:
        reach = reach or _Reach(app)
    except Exception:
        return {}
    out = {}
    for g in list(reach.modules.values())[:MAX_FILES]:
        path, name = g.get("__file__"), g.get("__name__")
        if not path or not name or not _app_globals(g):
            continue
        try:
            with open(path, "rb") as f:
                out[name] = hashlib.sha1(f.read()).hexdigest()[:12]
        except OSError:
            continue
    return out


def changed(recorded: dict, current: dict) -> list:
    """Modules whose source differs (or that only one side has), sorted."""
    if not recorded or not current:
        return []
    return sorted(k for k in set(recorded) | set(current) if recorded.get(k) != current.get(k))
