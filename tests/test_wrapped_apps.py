"""Shared state is found through the layers real apps wrap themselves in: ASGI middleware that keeps ``.app``, and
python-socketio's ``ASGIApp`` around FastAPI, including the Socket.IO event handlers (found in a real backend: the
outermost app had no routes, so nothing was tracked)."""

import asyncio
import os

import httpx
import pytest
import socketio_app
import wrapped_app

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.autotrack import _endpoints, _layers, prepare
from deja.fileformat import read
from deja.replay import replay
from deja.track import TrackedDict


def test_state_behind_a_plain_wrapper_is_found_and_replays(tmp_path):
    out = {}

    async def main():
        app = deja.DejaMiddleware(wrapped_app.make_wrapped(), directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            marker = rec.mark("t")
            out["r"] = await asyncio.gather(c.post("/buy"), c.post("/buy"))
            out["path"] = rec.dump(str(tmp_path / "wrapped.deja"), since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    assert [r.json() for r in out["r"]] == [{"ok": True}, {"ok": True}]
    c = analyze(*read(out["path"]))["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]", c
    r = replay(out["path"], wrapped_app.make_wrapped, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def test_socketio_around_fastapi_routes_and_event_handlers_are_followed():
    pytest.importorskip("socketio")
    app = socketio_app.make_socketio()
    assert len(_layers(app)) == 2  # ASGIApp, then the FastAPI app inside it
    names = {fn.__name__ for fn in _endpoints(app)}
    assert {"buy", "start", "on_answer"} <= names, names
    tracked, _ = prepare(app)
    assert "SESSIONS" in tracked, tracked
    assert isinstance(socketio_app.SESSIONS, TrackedDict)


def test_layers_stop_at_the_app_and_never_loop():
    class Loop:
        def __call__(self, scope, receive, send):
            pass

    a = Loop()
    a.app = a  # a wrapper that points at itself must not hang
    assert _layers(a) == [a]
    assert os.path.basename(__file__) == "test_wrapped_apps.py"
