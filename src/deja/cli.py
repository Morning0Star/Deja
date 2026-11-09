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
from .fileformat import FormatError, read


def _load_factory(spec: str):
    import os

    if os.getcwd() not in sys.path:  # like uvicorn: `--app pkg.mod:app` resolves from the current directory
        sys.path.insert(0, os.getcwd())
    module, _, attr = spec.partition(":")
    obj = getattr(importlib.import_module(module), attr or "app")
    if (
        callable(obj)
        and not hasattr(obj, "router")
        and getattr(obj, "__name__", "").startswith(("create", "make", "build"))
    ):
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
    followed = ran = 0
    last = None
    for _ in range(a.times):
        last = replay(schedule, factory, deadline_s=a.deadline)
        ran += 1
        followed += last.followed and last.same_responses
        if not (last.followed and last.same_responses):
            break
    print(last.summary())
    if a.times > 1:
        stopped = f" (stopped at the first mismatch, of {a.times} asked)" if ran < a.times else ""
        print(f"\n{followed}/{ran} replays matched the recording{stopped}")
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
        verdict = {
            "times": a.times,
            "matched": ok,
            "summary": f"{ok}/{a.times} replays matched the recording" + "\n\n" + last.summary(),
        }
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


def cmd_step(a) -> int:
    from .replay import Schedule, replay
    from .stepper import Stepper

    schedule = Schedule.load(a.recording)
    code = _same_hash_seed(schedule)
    if code is not None:
        return code
    r = replay(schedule, _load_factory(a.app), deadline_s=a.deadline, on_step=Stepper(at=a.at, task=a.task))
    print(r.summary())
    return 0


def cmd_explore(a) -> int:
    from .explore import explore, summary

    result = explore(a.recording, _load_factory(a.app), tries=a.tries, seed=a.seed, deadline_s=a.deadline)
    print(summary(result))
    return 1 if result["races"] else 0


def cmd_shrink(a) -> int:
    from .shrink import shrink

    try:
        got = shrink(a.recording, _load_factory(a.app), a.out, deadline_s=a.deadline)
    except ValueError as exc:  # no race in the recording
        print(f"deja shrink: {exc}", file=sys.stderr)
        return 1
    print(
        f"{a.out}: kept {', '.join(got['kept'])}; removed {', '.join(got['removed']) or 'nothing'}; "
        f"{got['events'][1]} of {got['events'][0]} events"
    )
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


APP_HELP = (
    "module:attribute of your app (attribute defaults to app): an ASGI app, an aiohttp Application, a dict of job "
    "handlers, or a function whose name starts with create, make or build that returns one of those"
)
DEADLINE_HELP = "seconds without progress before the replay reports where it diverged"


class _Help(argparse.HelpFormatter):
    """Shows an option's default when it has a real one (not None or a flag)."""

    def _get_help_string(self, action):
        text = action.help or ""
        if (
            action.default not in (None, False, argparse.SUPPRESS)
            and "%(default)" not in text
            and action.option_strings
        ):
            text += " (default: %(default)s)"
        return text


def main(argv=None) -> int:
    fmt = _Help
    p = argparse.ArgumentParser(
        prog="deja",
        description="Record the scheduling order of an asyncio app, explain its races, and replay them.",
        epilog="Documentation: https://github.com/Morning0Star/Deja#readme",
    )
    p.add_argument("-V", "--version", action="version", version=f"deja {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    def command(name, fn, text, app=True, deadline=10.0):
        c = sub.add_parser(
            name,
            help=text,
            description=text[0].upper() + text[1:] + ("" if text.endswith("?") else "."),
            formatter_class=fmt,
        )
        c.add_argument("recording", help="the .deja file")
        if app:
            c.add_argument("--app", required=True, help=APP_HELP)
        if deadline is not None:
            c.add_argument("--deadline", type=float, default=deadline, help=DEADLINE_HELP)
        c.set_defaults(fn=fn)
        return c

    command("show", cmd_show, "summarise a recording: its requests, steps and calls", app=False, deadline=None)
    r = command("replay", cmd_replay, "replay a recording against your app, with no database or network")
    r.add_argument("--times", type=int, default=1, help="replay this many times; stops at the first mismatch")
    rp = command(
        "report", cmd_report, "write an HTML report of a recording, optionally with a replay verdict", app=False
    )
    rp.add_argument("-o", "--out", help="the .html file to write (default: next to the recording)")
    rp.add_argument("--app", help="also replay against this app (given as for replay) and show the verdict")
    rp.add_argument("--times", type=int, default=20, help="replays for the verdict")
    v = command("verify", cmd_verify, "check a fix: does the recorded race still happen with this code?")
    v.add_argument("--times", type=int, default=5, help="replays to try; fixed means none reproduced the race")
    d = command("diff", cmd_diff, "replay one recording against two versions of your app and compare them")
    d.add_argument("--against", required=True, help="module:attribute of the other version")
    st = command(
        "step", cmd_step, "replay step by step, pausing before recorded steps to inspect the request", deadline=600.0
    )
    st.add_argument("--at", type=int, help="run to this step number, then pause (default: pause at every step)")
    st.add_argument("--task", help="run until this task (q1, q0.2...) is about to run, then pause")
    ex = command("explore", cmd_explore, "try other orderings of the recorded requests to find races not yet seen")
    ex.add_argument("--tries", type=int, default=50, help="random interleavings to try")
    ex.add_argument("--seed", type=int, default=1, help="seed of the first random interleaving")
    sh = command("shrink", cmd_shrink, "cut a recording down to the requests its race needs")
    sh.add_argument("-o", "--out", required=True, help="the smaller .deja file to write")
    mg = sub.add_parser(
        "merge",
        help="merge the recordings several workers saved for one race",
        description="Merge the recordings several worker processes saved for one race (DejaMiddleware(group=...)).",
        formatter_class=fmt,
    )
    mg.add_argument("folder", help="the <group>/<id>/ folder holding one .deja file per worker")
    mg.add_argument("-o", "--out", required=True, help="the merged .deja file to write")
    mg.set_defaults(fn=cmd_merge)
    x = command(
        "export-test",
        cmd_export_test,
        "write a pytest test that fails while the recorded race reproduces",
        deadline=None,
    )
    x.add_argument(
        "-o", "--out", required=True, help="the test file to write; the recording is copied to recordings/ next to it"
    )
    a = p.parse_args(argv)
    try:
        return a.fn(a)
    except (OSError, FormatError) as exc:  # a missing or unreadable recording: say so, no traceback
        print(f"deja {a.cmd}: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
