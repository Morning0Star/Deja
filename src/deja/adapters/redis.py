"""redis-py asyncio adapter: wraps ``Redis.execute_command``, ``Pipeline.execute`` and pub/sub.

A ``Redis`` client doesn't connect until its first command, so replay needs no fake client: the wrapped methods
answer from the recording.

Pub/sub (``client.pubsub()``): each ``PubSub`` is numbered in creation order. A message read inside a request
(``await pubsub.get_message(...)`` in a handler) is a recorded call like any other. A message that reaches a
listener running outside requests (a background ``async for msg in pubsub.listen()`` loop) is a pushed event: the
recording notes where it arrived and replay hands it to the same listener at that point, as for Postgres
``LISTEN`` / ``NOTIFY``. During replay, subscribe commands and connecting need no server.
"""

import asyncio

import redis.asyncio.client as rclient

from .. import codec
from . import _context, io_call, passthrough, patch, replaying

_undo = []
PUSHED = "redis"  # the "pid" of a pushed pub/sub message in the recording's notification events


def _pubsub_id(ps):
    lid = getattr(ps, "_deja_id", None)
    if lid is None:
        rec, _ = _context()
        if rec is None:
            return None
        lid = ps._deja_id = rec.next_listener()
        if getattr(rec, "replaying", False):
            rec.pubsubs[lid] = ps
    return lid


def install():
    def make_cmd(original):
        def execute_command(self, *args, **options):
            if passthrough():
                return original(self, *args, **options)
            return io_call("redis.cmd", ("cmd", args), lambda: original(self, *args, **options))

        execute_command.__wrapped__ = original
        return execute_command

    def make_pipe(original):
        def execute(self, raise_on_error=True):
            if passthrough():
                return original(self, raise_on_error)
            stack = [c[0] if isinstance(c, tuple) else c for c in getattr(self, "command_stack", [])]
            return io_call("redis.pipeline", ("pipe", repr(stack)), lambda: original(self, raise_on_error))

        execute.__wrapped__ = original
        return execute

    def make_ps_cmd(original):
        async def execute_command(self, *args):
            if passthrough():
                return await original(self, *args)
            _pubsub_id(self)
            if replaying():
                return None  # SUBSCRIBE / PING: no server during replay; the responses come back as messages
            return await original(self, *args)

        execute_command.__wrapped__ = original
        return execute_command

    def make_ps_connect(original):
        async def connect(self):
            if not passthrough() and replaying():
                return None
            return await original(self)

        connect.__wrapped__ = original
        return connect

    def make_ps_read(original):
        async def parse_response(self, block=True, timeout=0):
            if passthrough():
                return await original(self, block, timeout)
            lid = _pubsub_id(self)
            rec, path = _context()
            if path is not None:  # inside a request: an ordinary recorded call
                return await io_call("redis.pubsub", ("pubsub", block), lambda: original(self, block, timeout))
            if replaying():
                return await rec.pubsub_read(lid, block, timeout)
            response = await original(self, block, timeout)
            if lid is not None and isinstance(response, (list, tuple)) and response:
                rec.notify(lid, PUSHED, str(response[1] if len(response) > 1 else ""), codec.encode(list(response)))
            return response

        parse_response.__wrapped__ = original
        return parse_response

    _undo.append(patch(rclient.Redis, "execute_command", make_cmd))
    _undo.append(patch(rclient.Pipeline, "execute", make_pipe))
    _undo.append(patch(rclient.PubSub, "execute_command", make_ps_cmd))
    _undo.append(patch(rclient.PubSub, "connect", make_ps_connect))
    _undo.append(patch(rclient.PubSub, "parse_response", make_ps_read))


def uninstall():
    while _undo:
        _undo.pop()()


async def wait_for_message(queue: list, waiters: list, block, timeout):
    """Replay side of a background pub/sub read: the next delivered message, or None after ``timeout``."""
    if queue:
        return queue.pop(0)
    fut = asyncio.get_running_loop().create_future()
    waiters.append(fut)
    try:
        if block or timeout is None:
            return await fut
        return await asyncio.wait_for(fut, timeout or 0)
    except asyncio.TimeoutError:
        return None
    finally:
        if fut in waiters:
            waiters.remove(fut)
