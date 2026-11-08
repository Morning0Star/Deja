"""aiokafka: a request sends an order; a background consumer marks it seen; another request waits for that.
Replayed with the broker unreachable: the message reaches the consumer at the recorded point."""

import asyncio
import contextlib
import uuid

import pytest
from fastapi import FastAPI

pytest.importorskip("aiokafka")
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: E402

from test_adapters import reachable  # noqa: E402
from test_streams import record_concurrently  # noqa: E402
from deja.replay import replay  # noqa: E402

pytestmark = pytest.mark.skipif(not reachable("127.0.0.1", 59092), reason="test Kafka not running")
LIVE, DEAD = "127.0.0.1:59092", "127.0.0.1:1"


def orders_app(servers, topic):
    def factory():
        state = {"seen": set()}

        @contextlib.asynccontextmanager
        async def lifespan(app):
            state["producer"] = AIOKafkaProducer(bootstrap_servers=servers)
            consumer = AIOKafkaConsumer(topic, bootstrap_servers=servers, auto_offset_reset="earliest",
                                        group_id=f"g-{topic}")
            await state["producer"].start()
            await consumer.start()

            async def consume():
                async for msg in consumer:
                    state["seen"].add(msg.value.decode())

            task = asyncio.create_task(consume())
            yield
            task.cancel()
            await consumer.stop()
            await state["producer"].stop()

        app = FastAPI(lifespan=lifespan)

        @app.post("/order/{oid}")
        async def order(oid: str):
            md = await state["producer"].send_and_wait(topic, oid.encode())
            return {"partition": md.partition, "first": md.offset == 0}

        @app.get("/seen/{oid}")
        async def seen(oid: str):
            for i in range(500):
                if oid in state["seen"]:
                    return {"seen": True}
                await asyncio.sleep(0.01)
            return {"seen": False}

        return app

    return factory


def test_kafka_message_to_a_background_consumer_replays_without_the_broker(tmp_path):
    topic = f"orders-{uuid.uuid4().hex[:8]}"
    path, responses = record_concurrently(orders_app(LIVE, topic), [("GET", "/seen/o1"), ("POST", "/order/o1")],
                                          str(tmp_path / "k.deja"))
    assert responses[0].json() == {"seen": True} and responses[1].json() == {"partition": 0, "first": True}
    r = replay(path, orders_app(DEAD, topic), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
