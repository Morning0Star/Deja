"""WebSockets: every message in and out of a connection is recorded; replay feeds the client's messages back at the
recorded points, with no client, and the report explains races between connections."""

import asyncio
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay


def make_app(middleware=None):
    app = FastAPI()
    booked = set()
    stats = {"messages": 0}

    @app.websocket("/seats")
    async def seats(ws: WebSocket):
        await ws.accept()
        try:
            async for seat in ws.iter_text():
                stats["messages"] += 1
                if seat == "flag":
                    deja.flag("operator asked")
                    await ws.send_text("flagged")
                    continue
                if seat not in booked:
                    await asyncio.sleep(0.01)
                    booked.add(seat)
                    await ws.send_text(f"booked {seat}")
                else:
                    await ws.send_text(f"taken {seat}")
        except WebSocketDisconnect:
            pass

    if middleware is not None:
        app.add_middleware(deja.DejaMiddleware, directory=middleware)
    return app


class Client:
    """A minimal in-process WebSocket client for an ASGI app."""

    def __init__(self, app, path):
        self.app, self.path = app, path
        self.to_app, self.from_app = asyncio.Queue(), asyncio.Queue()
        self.task = None

    async def connect(self):
        scope = {"type": "websocket", "asgi": {"version": "3.0"}, "scheme": "ws", "path": self.path,
                 "raw_path": self.path.encode(), "query_string": b"", "root_path": "", "headers": [],
                 "client": ("127.0.0.1", 1), "server": ("app", 80), "subprotocols": []}
        await self.to_app.put({"type": "websocket.connect"})
        self.task = asyncio.ensure_future(self.app(scope, self.to_app.get, self.from_app.put))
        assert (await self.from_app.get())["type"] == "websocket.accept"

    async def ask(self, text):
        await self.to_app.put({"type": "websocket.receive", "text": text})
        return (await self.from_app.get())["text"]

    async def close(self):
        await self.to_app.put({"type": "websocket.disconnect", "code": 1000})
        await self.task


def record(tmp_path, script):
    out = {}

    async def main():
        app = make_app(middleware=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        marker = rec.mark("test")
        out["answers"] = await script(app)
        out["path"] = rec.dump(str(tmp_path / "ws.deja"), since=marker)
        out["dumps"] = list(rec.dumps)

    asyncio.run(main(), loop_factory=recording_loop)
    return out


async def two_bookers(app):
    a, b = Client(app, "/seats"), Client(app, "/seats")
    await asyncio.gather(a.connect(), b.connect())
    first = await asyncio.gather(a.ask("s1"), b.ask("s1"))  # both book seat s1 at the same time
    second = [await a.ask("s2"), await b.ask("s2")]  # one after the other: the second sees it taken
    await asyncio.gather(a.close(), b.close())
    return first + second


def test_two_connections_double_book_and_replay(tmp_path):
    out = record(tmp_path, two_bookers)
    assert out["answers"] == ["booked s1", "booked s1", "booked s2", "taken s2"]
    header, events = read(out["path"])
    a = analyze(header, events)
    c = a["conflicts"][0]
    assert c["pattern"] == "check-then-act" and c["resource"] == "booked[s1]" and c["confidence"] == "certain"
    assert "connections" in a["story"] and "message 1" in " ".join(m["text"] for m in a["moments"])
    for factory in (lambda: make_app(middleware=str(tmp_path / "replay")), make_app):  # with / without middleware
        r = replay(out["path"], factory, deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
        assert [resp[1] for resp in r.replayed.values()] == [b"booked s1\nbooked s2", b"booked s1\ntaken s2"]
    assert not os.path.exists(tmp_path / "replay"), "replay must not save recordings"


def test_flag_mid_connection_saves_while_it_stays_open(tmp_path):
    async def script(app):
        a = Client(app, "/seats")
        await a.connect()
        answers = [await a.ask("s1"), await a.ask("flag")]
        await asyncio.sleep(0.05)  # the file is written off the loop
        return answers, a

    out = record(tmp_path, script)
    assert out["answers"][0] == ["booked s1", "flagged"]
    flagged = [p for p in out["dumps"] if os.path.basename(p) != "ws.deja"]
    assert len(flagged) == 1, out["dumps"]
    header, _ = read(flagged[0])
    assert header["reason"] == "flag: operator asked"
    r = replay(flagged[0], make_app, deadline_s=5)  # the connection is still open at the end of this recording
    assert r.followed, r.summary()
