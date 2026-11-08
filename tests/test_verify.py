"""deja verify / diff / export-test: a recorded race checked against a fix."""

import asyncio
import os
import subprocess
import sys

import fixable_app
import httpx

import deja
from deja import recording_loop
from deja.verify import diff, export_test, verify


def record(tmp_path):
    out = {}

    async def main():
        app = fixable_app.make_racy()
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            m = rec.mark("t")
            await asyncio.gather(c.post("/buy"), c.post("/buy"))
            out["path"] = rec.dump(str(tmp_path / "buy.deja"), since=m)

    asyncio.run(main(), loop_factory=recording_loop)
    return out["path"]


def test_verify_tells_a_fix_from_no_fix_and_diff_compares(tmp_path):
    path = record(tmp_path)
    still = verify(path, fixable_app.make_racy, times=2)
    assert not still.fixed and still.reproduced == 2 and "STILL THERE" in still.summary()
    fixed = verify(path, fixable_app.make_fixed, times=2)
    assert fixed.fixed and "FIXED" in fixed.summary(), fixed.summary()
    text = diff(path, fixable_app.make_racy, fixable_app.make_fixed)
    assert "old: followed" in text and "new: followed" in text


def test_exported_test_fails_on_the_race_and_passes_on_the_fix(tmp_path):
    path = record(tmp_path)
    tests = tmp_path / "suite"
    tests.mkdir()
    here = os.path.dirname(__file__)
    env = {**os.environ, "PYTHONPATH": here + os.pathsep + os.environ.get("PYTHONPATH", "")}
    for spec, expect in (("fixable_app:make_racy", 1), ("fixable_app:make_fixed", 0)):
        target = tests / "test_race.py"
        export_test(path, spec, str(target))
        assert (tests / "recordings" / "buy.deja").exists()
        code = subprocess.call([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(target)],
                               env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert code == expect, spec
