"""Socket.IO around FastAPI, as KNOI-style backends do. SESSIONS is reachable only from the Socket.IO event
handlers, so finding it proves the handlers are followed (used by test_wrapped_apps)."""

import asyncio

from wrapped_app import make_fastapi

SESSIONS: dict = {}


def make_socketio():
    import socketio

    sio = socketio.AsyncServer(async_mode="asgi")

    @sio.event
    async def start(sid, data):  # check-then-act on module-level state, like an STT session table
        if data["session"] not in SESSIONS:
            await asyncio.sleep(0.01)
            SESSIONS[data["session"]] = sid
        return SESSIONS[data["session"]]

    class Quiz(socketio.AsyncNamespace):
        async def on_answer(self, sid, data):
            SESSIONS.setdefault("answers", 0)

    sio.register_namespace(Quiz("/quiz"))
    return socketio.ASGIApp(sio, make_fastapi())
