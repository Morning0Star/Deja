"""redis-py asyncio adapter: wraps ``Redis.execute_command`` and ``Pipeline.execute``.

A ``Redis`` client doesn't connect until its first command, so replay needs no fake client: the wrapped methods
answer from the recording.
"""

import redis.asyncio.client as rclient

from . import io_call, patch

_undo = []


def install():
    def make_cmd(original):
        async def execute_command(self, *args, **options):
            return await io_call("redis.cmd", ("cmd", args), lambda: original(self, *args, **options))

        execute_command.__wrapped__ = original
        return execute_command

    def make_pipe(original):
        async def execute(self, raise_on_error=True):
            stack = [c[0] if isinstance(c, tuple) else c for c in getattr(self, "command_stack", [])]
            return await io_call("redis.pipeline", ("pipe", repr(stack)), lambda: original(self, raise_on_error))

        execute.__wrapped__ = original
        return execute

    _undo.append(patch(rclient.Redis, "execute_command", make_cmd))
    _undo.append(patch(rclient.Pipeline, "execute", make_pipe))


def uninstall():
    while _undo:
        _undo.pop()()
