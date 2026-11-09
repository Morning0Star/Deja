"""PyMongo async: a check-then-insert signup on MongoDB, found, and replayed with the server unreachable."""

import asyncio
import contextlib

import pytest
from fastapi import FastAPI

pymongo = pytest.importorskip("pymongo")
from pymongo import AsyncMongoClient  # noqa: E402
from test_adapters import reachable  # noqa: E402
from test_streams import record_concurrently  # noqa: E402

from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.replay import replay  # noqa: E402

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", 27217), reason="test MongoDB not running")
URL, DEAD = "mongodb://127.0.0.1:27217", "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200"


def signup_app(url):
    def factory():
        state = {}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["client"] = AsyncMongoClient(url)
            state["users"] = state["client"]["deja"]["users"]
            await state["users"].delete_many({})
            yield
            await state["client"].close()

        app = FastAPI(lifespan=lifespan)

        @app.post("/signup/{email}")
        async def signup(email: str):
            users = state["users"]
            if await users.find_one({"email": email}):
                return {"ok": False}
            await asyncio.sleep(0.02)
            res = await users.insert_one({"email": email, "plan": "free"})
            await users.update_one({"_id": res.inserted_id}, {"$inc": {"logins": 1}})
            everyone = [u["email"] async for u in users.find({}, {"email": 1}).sort("email")]
            return {"ok": True, "id_is_object_id": type(res.inserted_id).__name__, "everyone": everyone}

        return app

    return factory


def test_mongo_check_then_insert_found_and_replayed(tmp_path):
    path, responses = record_concurrently(
        signup_app(URL), [("POST", "/signup/ada@x"), ("POST", "/signup/ada@x")], str(tmp_path / "m.deja")
    )
    assert [r.json()["ok"] for r in responses] == [True, True], "two accounts for one email"
    assert responses[0].json()["id_is_object_id"] == "ObjectId"
    a = analyze(*read(path))
    c = a["conflicts"][0]
    assert c["pattern"] == "check-then-act" and c["resource"] == "deja.users[email=ada@x]", a["story"]
    r = replay(path, signup_app(DEAD), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
