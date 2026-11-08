"""Check a fix against a recorded race, compare two versions of an app, and turn a recording into a pytest test.

    deja verify recording.deja --app myapp:create_app            # does the fixed code still race the same way?
    deja diff recording.deja --app old:create_app --against new:create_app
    deja export-test recording.deja --app myapp:create_app -o tests/test_double_charge.py

A race is *still there* when the replay follows the recorded interleaving and the app answers what it answered
when the bug happened. A fix shows up as one of: the replay can't take the recorded steps (a lock, a transaction,
an atomic statement now orders the requests), or it can and the answers change (a check now catches it).
"""

import os
import re
import shutil

from .analysis import analyze
from .fileformat import read
from .replay import Schedule, replay


class Verdict:
    def __init__(self, recording, results, story):
        self.recording, self.results, self.story = recording, results, story

    @property
    def reproduced(self) -> int:
        """Replays that went exactly as recorded (the race happened again)."""
        return sum(r.followed and r.same_responses for r in self.results)

    @property
    def fixed(self) -> bool:
        return self.reproduced == 0

    def how(self, r) -> str:
        if r.followed and r.same_responses:
            return "the recorded race happened again"
        if r.divergence:
            return ("the replay could not take the recorded steps: the code now orders these requests differently "
                    "(" + r.divergence.split(":", 1)[0] + ")")
        changed = [rid for rid in r.recorded if r.replayed.get(rid) != r.recorded[rid]]
        if changed:
            return "the requests ran in the recorded order but answered differently: " + ", ".join(
                f"request {rid} {_fmt(r.recorded[rid])} -> {_fmt(r.replayed.get(rid))}" for rid in changed[:4])
        return "the replay took different I/O calls: " + "; ".join(r.io_misses[:3])

    def summary(self) -> str:
        n = len(self.results)
        head = (f"FIXED: the race no longer reproduces ({n} of {n} replays differ from the recording)" if self.fixed
                else f"STILL THERE: {self.reproduced} of {n} replays reproduced the recorded race")
        lines = [head, "", "The race: " + self.story, "", "What happened in replay: " + self.how(self.results[-1])]
        notes = [ln for ln in self.results[-1].summary().splitlines() if ln.startswith("NOTE")]
        return "\n".join(lines + ([""] + notes if notes else []))


def _fmt(resp):
    if resp is None:
        return "(no answer)"
    status, body = resp
    text = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else str(body)
    return f"{status} {text[:60]}"


def verify(recording: str, app_factory, times: int = 5, deadline_s: float = 10.0) -> Verdict:
    """Replay ``recording`` against ``app_factory`` ``times`` times and say whether the race still happens."""
    schedule = Schedule.load(recording)
    header, events = read(recording)
    story = analyze(header, events)["story"]
    results = [replay(schedule, app_factory, deadline_s=deadline_s) for _ in range(times)]
    return Verdict(recording, results, story)


def diff(recording: str, old_factory, new_factory, deadline_s: float = 10.0) -> str:
    """Replay one recording against two versions of the app and describe how they differ."""
    schedule = Schedule.load(recording)
    a = replay(schedule, old_factory, deadline_s=deadline_s)
    b = replay(schedule, new_factory, deadline_s=deadline_s)
    lines = [f"old: followed {a.steps_followed}/{a.steps_expected} steps" + (" (diverged)" if a.divergence else ""),
             f"new: followed {b.steps_followed}/{b.steps_expected} steps" + (" (diverged)" if b.divergence else "")]
    if b.divergence and not a.divergence:
        lines.append("the new version can't take the recorded steps: " + b.divergence.split(";", 1)[0])
    for rid in sorted(set(a.replayed) | set(b.replayed) | set(a.recorded)):
        ra, rb, rec = a.replayed.get(rid), b.replayed.get(rid), a.recorded.get(rid)
        mark = "same" if ra == rb else "DIFFERENT"
        lines.append(f"  request {rid}: recorded {_fmt(rec)} | old {_fmt(ra)} | new {_fmt(rb)}  [{mark}]")
    for label, r in (("old", a), ("new", b)):
        for m in r.io_misses[:3]:
            lines.append(f"  {label} I/O: {m}")
    changed = sorted(set(b.code_changed) - set(a.code_changed))
    if changed:
        lines.append("code that differs between them: " + ", ".join(changed))
    return "\n".join(lines)


def export_test(recording: str, app_spec: str, out: str) -> str:
    """Write a pytest regression test for the recorded race (and copy the recording next to it).

    The test fails while the race reproduces and passes once a fix stops it; keep it to catch the race coming back.
    """
    header, events = read(recording)
    a = analyze(header, events)
    c = a["conflicts"][0] if a["conflicts"] else None
    slug = re.sub(r"[^a-z0-9]+", "_", f"{c['pattern']} {c['resource']}".lower()).strip("_")[:60] if c else "recording"
    folder = os.path.join(os.path.dirname(os.path.abspath(out)), "recordings")
    os.makedirs(folder, exist_ok=True)
    copy = os.path.join(folder, os.path.basename(recording))
    if os.path.abspath(copy) != os.path.abspath(recording):
        shutil.copyfile(recording, copy)
    module, _, attr = app_spec.partition(":")
    story = a["story"].replace('"""', "'''")
    body = f'''"""Regression test generated by Déjà from {os.path.basename(recording)}.

{_wrap(story)}

It replays the recorded interleaving against the current code: it fails while the race still happens and passes
once a fix prevents it.
"""

import os

from deja.verify import verify

RECORDING = os.path.join(os.path.dirname(__file__), "recordings", {os.path.basename(recording)!r})


def test_{slug}():
    from {module} import {attr} as app_factory

    verdict = verify(RECORDING, app_factory, times=3)
    assert verdict.fixed, verdict.summary()
'''
    with open(out, "w", encoding="utf-8") as f:
        f.write(body)
    return out


def _wrap(text, width=110):
    words, lines, line = text.split(), [], ""
    for w in words:
        if len(line) + len(w) + 1 > width:
            lines.append(line)
            line = w
        else:
            line = f"{line} {w}".strip()
    return "\n".join(lines + [line])
