"""deja explore: from a recording where two purchases ran one after the other (no race), find the interleaving in
which both buy the last widget."""

import asyncio
import os

import httpx

import deja
import fixable_app
from deja import recording_loop
from deja.explore import explore, summary


def record_serial(tmp_path, factory):
    out = {}

    async def main():
        app = factory()
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            m = rec.mark("t")
            out["r"] = [(await c.post("/buy")).json(), (await c.post("/buy")).json()]  # one after the other
            out["path"] = rec.dump(os.path.join(str(tmp_path), "serial.deja"), since=m)

    asyncio.run(main(), loop_factory=recording_loop)
    return out


def test_explore_finds_a_race_that_did_not_happen(tmp_path):
    out = record_serial(tmp_path, fixable_app.make_racy)
    assert out["r"] == [{"ok": True}, {"ok": False}], "recorded without the race"
    result = explore(out["path"], fixable_app.make_racy, tries=20, seed=1, deadline_s=5)
    assert len(result["serial"]) == 2  # whichever runs first buys the widget
    assert result["races"], summary(result)
    both = result["races"][0]["run"]["responses"]
    assert all(b"true" in body for _, body in both.values()), summary(result)
    assert "RACE" in summary(result)


def test_explore_finds_nothing_on_the_fixed_code(tmp_path):
    out = record_serial(tmp_path, fixable_app.make_fixed)
    result = explore(out["path"], fixable_app.make_fixed, tries=20, seed=1, deadline_s=5)
    assert result["races"] == [], summary(result)
