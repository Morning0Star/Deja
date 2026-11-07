"""Command line.

    deja show recording.deja
    deja replay recording.deja --app gallery.wallet:create_app [--times 100] [--deadline 10]
    deja report recording.deja [--app gallery.wallet:create_app --times 20] [-o report.html]

``--app`` is ``module:attribute``. If the attribute is callable and not itself an ASGI app instance it is called with
no arguments to build a fresh app (``create_app``); a module-level app object is reused as is.
"""

import argparse
import collections
import importlib
import sys

from . import __version__
from . import events as ev
from .fileformat import read


def _load_factory(spec: str):
    import os

    if os.getcwd() not in sys.path:  # like uvicorn: `--app pkg.mod:app` resolves from the current directory
        sys.path.insert(0, os.getcwd())
    module, _, attr = spec.partition(":")
    obj = getattr(importlib.import_module(module), attr or "app")
    if callable(obj) and not hasattr(obj, "router") and getattr(obj, "__name__", "").startswith(("create", "make", "build")):
        return obj
    return lambda: obj


def cmd_show(a) -> int:
    header, events = read(a.recording)
    print(f"{a.recording}")
    for k in ("reason", "python", "platform", "loop", "format"):
        if k in header:
            print(f"  {k}: {header[k]}")
    counts = collections.Counter(e[0] for e in events)
    print(f"  events: {len(events)}  " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print("requests:")
    status = {e[1]: e[2] for e in events if e[0] == ev.RESPONSE}
    for e in events:
        if e[0] == ev.REQUEST:
            body = e[6][:60] if isinstance(e[6], (bytes, bytearray)) else e[6]
            print(f"  q{e[1]}  {e[3]} {e[4]}  -> {status.get(e[1], '?')}  body={body!r}")
    print("interleaving of request tasks (one line per loop pass):")
    line = []
    for e in events:
        if e[0] == ev.BATCH and line:
            print("  " + " ".join(line))
            line = []
        elif e[0] == ev.STEP and e[1] and e[1].startswith("q"):
            line.append(e[1])
        elif e[0] == ev.REQUEST:
            line.append(f"[start q{e[1]}]")
        elif e[0] == ev.FLAG:
            line.append(f"[flag {e[2]!r}]")
    if line:
        print("  " + " ".join(line))
    return 0


def cmd_replay(a) -> int:
    from .replay import Schedule, replay

    factory = _load_factory(a.app)
    schedule = Schedule.load(a.recording)
    followed = 0
    last = None
    for i in range(a.times):
        last = replay(schedule, factory, deadline_s=a.deadline)
        followed += last.followed and last.same_responses
        if not (last.followed and last.same_responses):
            break
    print(last.summary())
    print(f"\n{followed}/{a.times} replays matched the recording" if a.times > 1 else "")
    return 0 if followed == a.times else 1


def cmd_report(a) -> int:
    from .report import write_report

    verdict = None
    if a.app:
        from .replay import Schedule, replay

        factory = _load_factory(a.app)
        schedule = Schedule.load(a.recording)
        ok, last = 0, None
        for _ in range(a.times):
            last = replay(schedule, factory, deadline_s=a.deadline)
            ok += last.followed and last.same_responses
        verdict = {"times": a.times, "matched": ok,
                   "summary": f"{ok}/{a.times} replays matched the recording" + "\n\n" + last.summary()}
    out = write_report(a.recording, a.out, verdict)
    print(out)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="deja", description="Record and replay asyncio interleavings.")
    p.add_argument("-V", "--version", action="version", version=f"deja {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("show", help="summarise a recording")
    s.add_argument("recording")
    s.set_defaults(fn=cmd_show)
    r = sub.add_parser("replay", help="replay a recording against an ASGI app")
    r.add_argument("recording")
    r.add_argument("--app", required=True, help="module:attribute (an ASGI app or a create_app factory)")
    r.add_argument("--times", type=int, default=1)
    r.add_argument("--deadline", type=float, default=10.0, help="seconds without progress before reporting divergence")
    r.set_defaults(fn=cmd_replay)
    rp = sub.add_parser("report", help="write an HTML report (optionally replaying the recording too)")
    rp.add_argument("recording")
    rp.add_argument("-o", "--out", help="output .html (default: next to the recording)")
    rp.add_argument("--app", help="module:attribute; when given, replay --times times and include the verdict")
    rp.add_argument("--times", type=int, default=20)
    rp.add_argument("--deadline", type=float, default=10.0)
    rp.set_defaults(fn=cmd_report)
    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
