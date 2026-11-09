"""aiokafka adapter: producer sends, and consumers in requests or in background loops.

* ``send`` / ``send_and_wait`` are recorded calls that return the same ``RecordMetadata`` (topic, partition,
  offset...). ``send`` waits for delivery inside the recorded call and hands back a finished future.
* A consumer read inside a request (``getone`` / ``getmany`` / ``async for``) is a recorded call.
* A consumer running outside requests (a background ``async for msg in consumer`` loop) receives *pushed events*:
  the recording notes where each message arrived and replay hands it to the same consumer (numbered in creation
  order) at that point, as for Postgres ``NOTIFY`` and Redis pub/sub.

Replay starts, stops and commits nothing: no broker is needed.
"""

import asyncio

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, RecordMetadata, TopicPartition

from .. import codec
from . import _context, io_call, passthrough, patch, replaying

_undo = []
PUSHED = "kafka"


def _enc_meta(md):
    return {
        "__t": "kafka_meta",
        "v": [md.topic, md.partition, md.offset, md.timestamp, md.timestamp_type, md.log_start_offset],
    }


def _dec_meta(v):
    if not isinstance(v, dict):
        return v
    topic, partition, offset, ts, ts_type, start = v["v"]
    return RecordMetadata(topic, partition, TopicPartition(topic, partition), offset, ts, ts_type, start)


def _enc_rec(r):
    return [
        r.topic,
        r.partition,
        r.offset,
        r.timestamp,
        r.timestamp_type,
        codec.encode(r.key),
        codec.encode(r.value),
        r.checksum,
        r.serialized_key_size,
        r.serialized_value_size,
        codec.encode(list(r.headers or [])),
    ]


def _dec_rec(v):
    t, p, o, ts, tt, k, val, ck, ks, vs, h = v
    return ConsumerRecord(
        t, p, o, ts, tt, codec.decode(k), codec.decode(val), ck, ks, vs, [tuple(x) for x in codec.decode(h)]
    )


def _enc_many(batch):
    return [[tp.topic, tp.partition, [_enc_rec(r) for r in recs]] for tp, recs in batch.items()]


def _dec_many(v):
    return {TopicPartition(t, p): [_dec_rec(r) for r in recs] for t, p, recs in v or []}


def _consumer_id(c):
    lid = getattr(c, "_deja_id", None)
    if lid is None:
        rec, _ = _context()
        if rec is None:
            return None
        lid = c._deja_id = rec.next_listener()
        if getattr(rec, "replaying", False):
            rec.pubsubs[lid] = c
    return lid


async def _noop(*args, **kwargs):
    return None


def install():
    def make_send_and_wait(original):
        async def send_and_wait(self, topic, value=None, key=None, *args, **kwargs):
            if passthrough():
                return await original(self, topic, value, key, *args, **kwargs)
            return await io_call(
                "kafka.send",
                ("kafka", topic, codec.encode(key), codec.encode(value)),
                lambda: original(self, topic, value, key, *args, **kwargs),
                encode=_enc_meta,
                decode=_dec_meta,
            )

        return send_and_wait

    def make_send(original):
        async def send(self, topic, value=None, key=None, *args, **kwargs):
            if passthrough():
                return await original(self, topic, value, key, *args, **kwargs)

            async def real():
                fut = await original(self, topic, value, key, *args, **kwargs)
                return await fut

            md = await io_call(
                "kafka.send",
                ("kafka", topic, codec.encode(key), codec.encode(value)),
                real,
                encode=_enc_meta,
                decode=_dec_meta,
            )
            done = asyncio.get_running_loop().create_future()
            done.set_result(md)
            return done

        return send

    def make_read(name, encode, decode, empty):
        def make(original):
            async def read(self, *args, **kwargs):
                if passthrough():
                    return await original(self, *args, **kwargs)
                lid = _consumer_id(self)
                rec, path = _context()
                if path is not None and path[0] == "q":  # inside a request: an ordinary recorded call
                    return await io_call(
                        f"kafka.{name}",
                        ("kafka", name),
                        lambda: original(self, *args, **kwargs),
                        encode=encode,
                        decode=decode,
                        default=empty,
                    )
                if replaying():
                    got = await rec.pubsub_read(lid, True, None)
                    return decode(got)
                result = await original(self, *args, **kwargs)
                if lid is not None and result:
                    rec.notify(lid, PUSHED, name, encode(result))
                return result

            read.__wrapped__ = original
            return read

        return make

    def make_noop_in_replay(original):
        async def method(self, *args, **kwargs):
            if not passthrough() and replaying():
                return None
            return await original(self, *args, **kwargs)

        method.__wrapped__ = original
        return method

    _undo.append(patch(AIOKafkaProducer, "send_and_wait", make_send_and_wait))
    _undo.append(patch(AIOKafkaProducer, "send", make_send))
    for cls in (AIOKafkaProducer, AIOKafkaConsumer):
        for name in ("start", "stop"):
            _undo.append(patch(cls, name, make_noop_in_replay))
    _undo.append(patch(AIOKafkaConsumer, "commit", make_noop_in_replay))
    _undo.append(patch(AIOKafkaConsumer, "getone", make_read("getone", _enc_rec, _dec_rec, None)))
    _undo.append(patch(AIOKafkaConsumer, "getmany", make_read("getmany", _enc_many, _dec_many, dict)))

    def make_anext(original):  # `async for msg in consumer` is getone() until stopped
        async def __anext__(self):
            if passthrough():
                return await original(self)
            return await self.getone()

        return __anext__

    _undo.append(patch(AIOKafkaConsumer, "__anext__", make_anext))


def uninstall():
    while _undo:
        _undo.pop()()
