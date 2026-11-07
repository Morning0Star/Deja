import asyncio
import glob
import json
import os

import pytest

from deja import recording_loop
from deja.replay import Schedule, replay
from gallery import stress, wallet


def record(tmp_path, scenario="wallet", trials=40):
    asyncio.run(stress.run(scenario, trials, False, None, 60, str(tmp_path), record_passing=True),
                loop_factory=recording_loop)
    fails = sorted(glob.glob(os.path.join(tmp_path, f"{scenario}-fail-*.deja")))
    passes = sorted(glob.glob(os.path.join(tmp_path, f"{scenario}-pass-*.deja")))
    assert fails and passes, "need at least one failing and one passing trial"
    return fails[0], passes[0]


def check_response(result):
    last = max(result.replayed)
    return json.loads(result.replayed[last][1])


def test_failing_recording_replays_as_failure(tmp_path):
    fail, _ = record(tmp_path)
    for _ in range(10):
        r = replay(fail, wallet.create_app, deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
        assert check_response(r)["violated"] is True


def test_passing_recording_replays_clean(tmp_path):
    _, ok = record(tmp_path)
    for _ in range(10):
        r = replay(ok, wallet.create_app, deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
        assert check_response(r)["violated"] is False


def test_code_change_is_reported(tmp_path):
    """Replaying a recorded double charge against the fixed (locked) app must not silently 'pass'."""
    fail, _ = record(tmp_path)
    r = replay(fail, lambda: wallet.create_app(safe=True), deadline_s=2)
    assert not (r.followed and r.same_responses)
    assert "DIVERGED" in r.summary() or "DIFFERENT" in r.summary()


def test_schedule_contains_only_request_scope(tmp_path):
    fail, _ = record(tmp_path)
    s = Schedule.load(fail)
    assert s.entries[0][0] == "start"
    assert all(kind == "start" or path.startswith("q") for kind, path in s.entries)
    assert len(s.requests) == 4  # reset, two purchases, check
