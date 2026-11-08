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


def _same_hash_seed(schedule):
    """Re-run this command with the recording's PYTHONHASHSEED (set / dict-of-str order depends on it)."""
    import os
    import subprocess
    import sys

    seed = schedule.header.get("hash_seed")
    if not seed or os.environ.get("PYTHONHASHSEED") == seed or os.environ.get("DEJA_NO_REEXEC"):
        return None
    env = {**os.environ, "PYTHONHASHSEED": seed, "DEJA_NO_REEXEC": "1"}
    return subprocess.call([sys.executable, "-m", "deja.cli", *sys.argv[1:]], env=env)


def cmd_replay(a) -> int:
    from .replay import Schedule, replay

    schedule = Schedule.load(a.recording)
    code = _same_hash_seed(schedule)
    if code is not None:
        return code
    factory = _load_factory(a.app)
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

        schedule = Schedule.load(a.recording)
        code = _same_hash_seed(schedule)
        if code is not None:
            return code
        factory = _load_factory(a.app)
        ok, last = 0, None
        for _ in range(a.times):
            last = replay(schedule, factory, deadline_s=a.deadline)
            ok += last.followed and last.same_responses
        verdict = {"times": a.times, "matched": ok,
                   "summary": f"{ok}/{a.times} replays matched the recording" + "\n\n" + last.summary()}
    out = write_report(a.recording, a.out, verdict)
    print(out)
    return 0


def cmd_verify(a) -> int:
    from .replay import Schedule
    from .verify import verify

    code = _same_hash_seed(Schedule.load(a.recording))
    if code is not None:
        return code
    v = verify(a.recording, _load_factory(a.app), times=a.times, deadline_s=a.deadline)
    print(v.summary())
    return 0 if v.fixed else 1


def cmd_diff(a) -> int:
    from .replay import Schedule
    from .verify import diff

    code = _same_hash_seed(Schedule.load(a.recording))
    if code is not None:
        return code
    print(diff(a.recording, _load_factory(a.app), _load_factory(a.against), deadline_s=a.deadline))
    return 0


def cmd_shrink(a) -> int:
    from .shrink import shrink

    got = shrink(a.recording, _load_factory(a.app), a.out, deadline_s=a.deadline)
    print(f"{a.out}: kept {', '.join(got['kept'])}; removed {', '.join(got['removed']) or 'nothing'}; "
          f"{got['events'][1]} of {got['events'][0]} events")
    return 0


def cmd_merge(a) -> int:
    import glob
    import os

    from .group import merge

    paths = sorted(glob.glob(os.path.join(a.folder, "*.deja")))
    if not paths:
        print(f"no worker recordings in {a.folder}")
        return 1
    print(merge(paths, a.out))
    return 0


def cmd_export_test(a) -> int:
    from .verify import export_test

    print(export_test(a.recording, a.app, a.out))
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
    v = sub.add_parser("verify", help="replay a recorded race against (fixed) code: does it still happen?")
    v.add_argument("recording")
    v.add_argument("--app", required=True, help="module:attribute of the code to check")
    v.add_argument("--times", type=int, default=5)
    v.add_argument("--deadline", type=float, default=10.0)
    v.set_defaults(fn=cmd_verify)
    d = sub.add_parser("diff", help="replay one recording against two versions of an app and compare")
    d.add_argument("recording")
    d.add_argument("--app", required=True, help="module:attribute of the old version")
    d.add_argument("--against", required=True, help="module:attribute of the new version")
    d.add_argument("--deadline", type=float, default=10.0)
    d.set_defaults(fn=cmd_diff)
    sh = sub.add_parser("shrink", help="cut a recording down to the requests the race needs")
    sh.add_argument("recording")
    sh.add_argument("--app", required=True, help="module:attribute of the app to replay against")
    sh.add_argument("-o", "--out", required=True)
    sh.add_argument("--deadline", type=float, default=10.0)
    sh.set_defaults(fn=cmd_shrink)
    mg = sub.add_parser("merge", help="merge the workers' recordings of one group request (deja.group)")
    mg.add_argument("folder", help="<group>/<request id>/ holding one .deja per worker")
    mg.add_argument("-o", "--out", required=True)
    mg.set_defaults(fn=cmd_merge)
    x = sub.add_parser("export-test", help="write a pytest regression test for the recorded race")
    x.add_argument("recording")
    x.add_argument("--app", required=True, help="module:attribute the test will import")
    x.add_argument("-o", "--out", required=True, help="the test file to write (the recording is copied beside it)")
    x.set_defaults(fn=cmd_export_test)
    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
