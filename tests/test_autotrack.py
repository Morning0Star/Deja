"""Shared in-memory state is tracked automatically: the report explains races on it with no deja.track calls."""

import asyncio
import os

import httpx

import autotrack_app
import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay
from deja.track import TrackedDict, TrackedList, TrackedSet


def record(calls, path, track=True):
    out = {}

    async def main():
        app = autotrack_app.make_app()
        mw = {}

        def capture(app_, **kw):
            mw["m"] = deja.DejaMiddleware(app_, **kw)
            return mw["m"]

        app.add_middleware(capture, directory=os.path.dirname(path), track=track)
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            marker = rec.mark("test")
            out["responses"] = await asyncio.gather(*(client.post(u) for u in calls))
            out["path"] = rec.dump(path, since=marker)
        out["app"], out["tracked"] = app, mw["m"].tracked

    asyncio.run(main(), loop_factory=recording_loop)
    return out


def test_closure_state_lost_update_found_without_track_calls(tmp_path):
    out = record(["/buy", "/buy"], str(tmp_path / "buy.deja"))
    assert [r.json() for r in out["responses"]] == [{"ok": True}, {"ok": True}], "both sold the last widget"
    c = analyze(*read(out["path"]))["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]" and c["confidence"] == "certain"
    r = replay(out["path"], autotrack_app.make_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def test_object_attribute_and_iteration_check_then_act(tmp_path):
    out = record(["/order/o1", "/order/o1"], str(tmp_path / "order.deja"))
    c = analyze(*read(out["path"]))["conflicts"][0]
    assert c["pattern"] == "check-then-act" and c["resource"] == "store.orders[o1]"

    out = record(["/seat/ada", "/seat/ada"], str(tmp_path / "seat.deja"))
    a = analyze(*read(out["path"]))
    c = a["conflicts"][0]  # found from `any(t == name for t in taken)`: a read of the whole list
    assert c["pattern"] == "check-then-act" and c["resource"] == "taken[ada]" and c["confidence"] == "certain"
    assert any(m["text"].startswith("A reads all of taken") for m in a["moments"])


def test_what_is_tracked_and_what_is_left_alone(tmp_path):
    out = record(["/cart/c1"], str(tmp_path / "cart.deja"))
    # (module-level CARTS may have been swapped by an earlier test in this process: `tracked` lists new swaps)
    assert {"stock", "store.orders", "taken", "visits"} <= set(out["tracked"])
    assert isinstance(autotrack_app.CARTS, TrackedDict) and autotrack_app.CARTS["c1"] >= 1
    assert isinstance(out["app"].state.visits, TrackedSet)
    # REMEMBERED is also a default argument (a tuple holds it): swapping it would split the state in two
    assert type(autotrack_app.REMEMBERED) is dict and autotrack_app.REMEMBERED.get("c1") is True
    assert autotrack_app.remember.__defaults__[0] is autotrack_app.REMEMBERED

    out = record(["/order/o2"], str(tmp_path / "off.deja"), track=False)
    assert out["tracked"] == [] and not isinstance(out["app"].state.visits, (TrackedSet, TrackedList))
