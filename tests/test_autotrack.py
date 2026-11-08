"""Shared in-memory state is tracked automatically: the report explains races on it with no deja.track calls."""

import asyncio
import os

import httpx
import pytest

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


def record_shop(calls, path):
    import shop_app

    out = {}

    async def main():
        app = shop_app.make_app()
        app.add_middleware(deja.DejaMiddleware, directory=os.path.dirname(path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            marker = rec.mark("test")
            out["responses"] = await asyncio.gather(*(client.post(u) for u in calls))
            out["path"] = rec.dump(path, since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    return out


@pytest.mark.parametrize("url, pattern, resource", [
    ("/hold/x", "lost-update", "shop_services.HOLDS[x]"),        # defaultdict, in a helper module
    ("/sell/x", "lost-update", "shop_services.SALES[x]"),        # Counter
    ("/queue/j1", "check-then-act", "shop_services.QUEUE[j1]"),  # deque
    ("/visit", "lost-update", "shop_services.visits"),           # a module variable reassigned with `global`
    ("/pay", "lost-update", "shop_services.wallet.balance"),     # an attribute of an app object
])
def test_state_in_helper_modules_is_found(tmp_path, url, pattern, resource):
    import shop_app

    out = record_shop([url, url], str(tmp_path / "shop.deja"))
    conflicts = analyze(*read(out["path"]))["conflicts"]
    assert conflicts, f"no conflict found for {url}"
    c = conflicts[0]
    assert (c["pattern"], c["resource"], c["confidence"]) == (pattern, resource, "certain"), c
    r = replay(out["path"], shop_app.make_app, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def test_module_variable_in_worker_threads_replays_in_order(tmp_path):
    import shop_app
    import shop_services

    shop_services._slow = "a"
    try:
        out = record_shop(["/ticket/a", "/ticket/b"], str(tmp_path / "ticket.deja"))
        assert [r.json()["winner"] for r in out["responses"]] == [None, "b"]
        touches = [e for e in read(out["path"])[1] if e[0] == "T" and e[2] == "shop_services.tickets"]
        assert len(touches) >= 4 and all(".t" in e[1] for e in touches), touches  # recorded inside the threads
        # (b read and wrote before a looked: in order, not a race; the report rightly finds no conflict)
        assert analyze(*read(out["path"]))["conflicts"] == []
        shop_services._slow = "b"  # replay with the other thread slow: the recorded order must still win
        r = replay(out["path"], shop_app.make_app, deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
    finally:
        shop_services._slow = None


def test_container_replaced_whole_with_global_stays_tracked(tmp_path):
    import shop_app

    out = {}

    async def main():
        app = shop_app.make_sessions_app()
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            await client.post("/reset")  # SESSIONS = {}: a new, untracked dict unless Déjà swaps it
            marker = rec.mark("test")
            await asyncio.gather(client.post("/login/ada"), client.post("/login/ada"))
            out["path"] = rec.dump(str(tmp_path / "s.deja"), since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    c = analyze(*read(out["path"]))["conflicts"][0]
    assert c["pattern"] == "check-then-act" and c["resource"] == "shop_services.SESSIONS[ada]"


def test_track_skip_and_only_patterns():
    from deja.autotrack import Rules, auto_track

    app = autotrack_app.make_app()
    got = auto_track(app, rules=Rules(skip=["taken", "store.*"]))
    assert "taken" not in got and "store.orders" not in got and "stock" in got
    app = autotrack_app.make_app()
    assert auto_track(app, rules=Rules(only=["stock"])) == ["stock"]
    assert not Rules(skip=["autotrack_app.Store"])("store", autotrack_app.Store())  # by class name
