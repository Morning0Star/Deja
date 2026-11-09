import asyncio
import glob
import json
import os

from deja import recording_loop
from deja.replay import Schedule, replay
from gallery import stress, wallet


def record(tmp_path, scenario="wallet", trials=40):
    asyncio.run(
        stress.run(scenario, trials, False, None, 60, str(tmp_path), record_passing=True), loop_factory=recording_loop
    )
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


def test_replay_notes_when_the_apps_code_changed(tmp_path):
    import shutil
    import sys

    src = tmp_path / "pkg_shop"
    src.mkdir()
    here = __import__("os").path.dirname(__file__)
    shutil.copy(f"{here}/shop_services.py", src / "shop_services_v.py")
    (src / "shop_app_v.py").write_text(open(f"{here}/shop_app.py").read().replace("shop_services", "shop_services_v"))
    sys.path.insert(0, str(src))
    try:
        import shop_app_v

        out = {}

        async def main():
            import asyncio

            import httpx

            import deja

            app = shop_app_v.make_app()
            app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
            rec = asyncio.get_running_loop().deja
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
                m = rec.mark("t")
                await asyncio.gather(c.post("/hold/x"), c.post("/hold/x"))
                out["path"] = rec.dump(str(tmp_path / "v.deja"), since=m)

        import asyncio

        from deja import recording_loop

        asyncio.run(main(), loop_factory=recording_loop)
        from deja.replay import replay

        assert replay(out["path"], shop_app_v.make_app, deadline_s=5).code_changed == []
        with open(src / "shop_services_v.py", "a") as f:
            f.write("\n# a fix\n")
        r = replay(out["path"], shop_app_v.make_app, deadline_s=5)
        assert r.code_changed == ["shop_services_v"] and "code differs" in r.summary()
    finally:
        sys.path.remove(str(src))
        for m in ("shop_app_v", "shop_services_v"):
            sys.modules.pop(m, None)


def test_hash_seed_is_recorded_and_replay_reruns_with_it(tmp_path, monkeypatch):
    import subprocess
    import sys

    from deja.recorder import Recorder

    monkeypatch.setenv("PYTHONHASHSEED", "123")
    assert Recorder().header()["hash_seed"] == "123"
    path = tmp_path / "seed.deja"
    from deja.fileformat import write

    write(str(path), {**Recorder().header()}, [])
    monkeypatch.setenv("PYTHONHASHSEED", "0")
    # the CLI re-runs itself with the recording's seed: the child sees 123
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import os\nclass App:\n    pass\ndef create_app():\n"
        "    open(os.environ['PROBE_OUT'], 'w').write(os.environ['PYTHONHASHSEED'])\n"
        "    async def app(scope, receive, send):\n        pass\n    return app\n"
    )
    out = tmp_path / "seen.txt"
    env = {**__import__("os").environ, "PROBE_OUT": str(out), "PYTHONPATH": str(tmp_path)}
    subprocess.call(
        [sys.executable, "-m", "deja.cli", "replay", str(path), "--app", "probe:create_app"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    assert out.read_text() == "123"
