"""Redis pub/sub: a background listener updates shared state when a message arrives. The message is a pushed event:
replay hands it to the listener at the recorded point, with Redis unreachable."""

import asyncio
import contextlib

import pytest
import redis.asyncio as aioredis
from fastapi import FastAPI
from test_adapters import reachable
from test_streams import record_concurrently

from deja.replay import replay

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", 56379), reason="test Redis not running")
URL, DEAD = "redis://127.0.0.1:56379/0", "redis://127.0.0.1:1/0"


def jobs_app(url):
    def factory():
        state = {"done": {}}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["redis"] = aioredis.Redis.from_url(url)
            ps = state["redis"].pubsub()
            await ps.subscribe("jobs")

            async def listen():
                async for msg in ps.listen():
                    if msg["type"] == "message":
                        state["done"][msg["data"].decode()] = True

            task = asyncio.create_task(listen())
            yield
            task.cancel()
            await state["redis"].aclose()

        app = FastAPI(lifespan=lifespan)

        @app.post("/submit/{job}")
        async def submit(job: str):
            await state["redis"].publish("jobs", job)
            return {"sent": job}

        @app.get("/wait/{job}")
        async def wait(job: str):
            for i in range(200):
                if state["done"].get(job):
                    return {"done": True, "polls": i}
                await asyncio.sleep(0.005)
            return {"done": False}

        return app

    return factory


def test_pubsub_message_to_a_background_listener_replays_without_redis(tmp_path):
    path, responses = record_concurrently(
        jobs_app(URL), [("GET", "/wait/j1"), ("POST", "/submit/j1")], str(tmp_path / "ps.deja")
    )
    assert responses[0].json()["done"] is True
    for _ in range(3):
        r = replay(path, jobs_app(DEAD), deadline_s=5)
        assert r.followed and r.same_responses, r.summary()
