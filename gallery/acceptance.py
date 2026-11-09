"""M2 acceptance: recorded failures must replay as failures, recorded passes as passes.

    python -m gallery.acceptance --recordings 5 --replays 100

For each gallery app: record trials (failing and passing) on Déjà's recording loop, then replay every recording
`--replays` times against a fresh copy of the same (buggy) app and read the app's own invariant check from the
replayed responses.
"""

import argparse
import asyncio
import glob
import json
import os
import shutil
import sys
import tempfile
import time

from deja import recording_loop
from deja.replay import Schedule, replay

from . import stress

MODULES = {"signup": "gallery.signup", "likes": "gallery.likes", "wallet": "gallery.wallet"}


def violated(result) -> bool | None:
    last = max(result.replayed) if result.replayed else None
    if last is None:
        return None
    status, body = result.replayed[last]
    try:
        return json.loads(body)["violated"]
    except Exception:
        return None


def collect(scenario, workdir, want):
    """Record trials until there are `want` failing and `want` passing recordings."""
    for _ in range(50):
        fails = glob.glob(os.path.join(workdir, f"{scenario}-fail-*.deja"))
        passes = glob.glob(os.path.join(workdir, f"{scenario}-pass-*.deja"))
        if len(fails) >= want and len(passes) >= want:
            return sorted(fails)[:want], sorted(passes)[:want]
        sub = tempfile.mkdtemp(dir=workdir)
        asyncio.run(stress.run(scenario, 20, False, None, 60, sub, record_passing=True), loop_factory=recording_loop)
        for f in os.listdir(sub):  # keep names unique across batches: <scenario>-<fail|pass>-trialNNNN-<batch>.deja
            os.replace(os.path.join(sub, f), os.path.join(workdir, f"{f[:-5]}-{os.path.basename(sub)}.deja"))
        shutil.rmtree(sub, ignore_errors=True)
    raise RuntimeError(f"{scenario}: could not collect {want} failing and {want} passing recordings")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--recordings", type=int, default=3, help="failing and passing recordings per app")
    p.add_argument("--replays", type=int, default=20, help="replays per recording")
    p.add_argument("--only", choices=list(MODULES))
    a = p.parse_args()
    import importlib

    print(
        f"python {sys.version.split()[0]} {sys.platform}  recordings/app={a.recordings} (+{a.recordings} passing)"
        f"  replays/recording={a.replays}"
    )
    workdir = tempfile.mkdtemp(prefix="deja-acceptance-")
    ok_all = True
    for scenario in [a.only] if a.only else MODULES:
        factory = importlib.import_module(MODULES[scenario]).create_app
        fails, passes = collect(scenario, workdir, a.recordings)
        for label, files, expect in (("failing", fails, True), ("passing", passes, False)):
            same, followed, total, diverged = 0, 0, 0, []
            t0 = time.perf_counter()
            for f in files:
                schedule = Schedule.load(f)
                for _ in range(a.replays):
                    r = replay(schedule, factory, deadline_s=5)
                    total += 1
                    followed += r.followed
                    same += violated(r) is expect
                    if r.divergence and len(diverged) < 2:
                        diverged.append(r.divergence)
            ok = same == total and followed == total
            ok_all &= ok
            print(
                f"  {scenario:7s} {label}: bug {'reproduced' if expect else 'absent'} in {same}/{total} replays, "
                f"followed recording in {followed}/{total}  ({(time.perf_counter() - t0) / total * 1000:.0f} ms/replay)"
                f"  {'OK' if ok else 'FAIL'}"
            )
            for d in diverged:
                print("     ", d)
    shutil.rmtree(workdir, ignore_errors=True)
    print("ACCEPTANCE", "PASSED" if ok_all else "FAILED")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
