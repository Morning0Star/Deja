"""Recording event loops.

``RecordingSelectorLoop`` / ``RecordingProactorLoop`` replace ``_run_once`` with an instrumented copy of CPython's
own (identical in 3.12 and 3.14 apart from how the select timeout is clamped). The only additions: one ``BATCH``
event per pass, and one ``STEP`` / ``PLUMB`` event per handle that actually runs. Logging per pass instead of
wrapping each callback is what keeps overhead low (spike S2: +3-10% vs +55-138%).

The copy is derived from CPython's ``Lib/asyncio/base_events.py`` (PSF License).
"""

import asyncio
import heapq
import sys
import time
from asyncio import base_events

from . import _compat, adapters, codec, shims
from . import events as ev
from .recorder import Recorder

_monotonic = time.monotonic  # captured before shims patch time.monotonic
_perf_ns = time.perf_counter_ns  # one read per loop pass gives the report a real time axis
_Task = asyncio.Task


class _RecordingMixin:
    deja: Recorder

    def _deja_setup(self, recorder: Recorder | None):
        self.deja = recorder or Recorder()
        self.deja.loop = self
        self._deja_inner_factory = None
        super().set_task_factory(self._deja_task_factory)
        shims.install()
        self._deja_shimmed = True
        self._deja_adapters = adapters.install()

    def close(self):
        try:
            super().close()
        finally:
            if getattr(self, "_deja_shimmed", False):
                self._deja_shimmed = False
                shims.uninstall()
                adapters.uninstall(self._deja_adapters)

    # The loop's clock must never go through the time shims (they would record loop internals as app reads).
    def time(self):
        return _monotonic()

    # ------------------------------------------------------------------ task identity
    def set_task_factory(self, factory):
        self._deja_inner_factory = factory  # keep the user's factory (e.g. eager_task_factory) underneath ours

    def get_task_factory(self):
        return self._deja_inner_factory

    def _deja_task_factory(self, loop, coro, **kwargs):
        ids = self.deja.ids
        slot = ids.reserve(asyncio.current_task(loop))
        task = None
        try:
            inner = self._deja_inner_factory
            task = inner(loop, coro, **kwargs) if inner is not None else _Task(coro, loop=loop, **kwargs)
        finally:
            ids.release(slot, task)
        return task

    # ------------------------------------------------------------------ worker-thread calls
    def run_in_executor(self, executor, func, *args):
        """Record what a worker-thread call returned (inside request scope), so replay needs no real threads.

        The function's side effects do not happen during replay; only its return value (or exception) comes back.
        """
        fut = super().run_in_executor(executor, func, *args)
        rec = self.deja
        if getattr(rec, "replaying", False) or adapters._inside.get():
            return fut
        path = rec.current_path()
        if path is None or not path.startswith("q"):
            return fut
        key = (getattr(func, "__qualname__", None) or type(func).__name__, args)
        rec.io_start(path, adapters.EXECUTOR_OP)

        def done(f):
            if f.cancelled():
                rec.io_end(path, adapters.EXECUTOR_OP, key, "err", {"__t": "exc", "type": "asyncio:CancelledError", "msg": ""})
            elif f.exception() is not None:
                rec.io_end(path, adapters.EXECUTOR_OP, key, "err", codec.encode_exc(f.exception()))
            else:
                rec.io_end(path, adapters.EXECUTOR_OP, key, "ok", codec.encode(f.result()))

        if fut.done():  # completed before we returned: the caller's await will not suspend
            done(fut)
        else:
            fut.add_done_callback(done)
        return fut

    # ------------------------------------------------------------------ the hook
    def _run_once(self):
        sched_count = len(self._scheduled)
        if (sched_count > base_events._MIN_SCHEDULED_TIMER_HANDLES and
                self._timer_cancelled_count / sched_count > base_events._MIN_CANCELLED_TIMER_HANDLES_FRACTION):
            new_scheduled = []
            for handle in self._scheduled:
                if handle._cancelled:
                    handle._scheduled = False
                else:
                    new_scheduled.append(handle)
            heapq.heapify(new_scheduled)
            self._scheduled = new_scheduled
            self._timer_cancelled_count = 0
        else:
            while self._scheduled and self._scheduled[0]._cancelled:
                self._timer_cancelled_count -= 1
                handle = heapq.heappop(self._scheduled)
                handle._scheduled = False

        timeout = None
        if self._ready or self._stopping:
            timeout = 0
        elif self._scheduled:
            timeout = self._scheduled[0]._when - self.time()
            if timeout > base_events.MAXIMUM_SELECT_TIMEOUT:
                timeout = base_events.MAXIMUM_SELECT_TIMEOUT
            elif timeout < 0:
                timeout = 0

        event_list = self._selector.select(timeout)
        self._process_events(event_list)
        event_list = None

        end_time = self.time() + self._clock_resolution
        while self._scheduled:
            handle = self._scheduled[0]
            if handle._when >= end_time:
                break
            handle = heapq.heappop(self._scheduled)
            handle._scheduled = False
            self._ready.append(handle)

        ntodo = len(self._ready)
        if not ntodo:
            return
        # Hot path: one int per handle, the id() of the callback's owner (the task, for task steps), or the
        # interned PLUMB marker for plain functions. Ints and interned strings are not tracked by the cyclic GC,
        # which matters: holding 100k+ callback objects made the GC scan the buffer repeatedly (+22-40%).
        # Recorder.events() maps ids back to task paths with TASK_NEW / TASK_GONE events.
        append = self.deja.buffer.append
        append((ev.BATCH, ntodo, _perf_ns()))
        ready = self._ready
        plumb = ev.PLUMB
        if not self._debug:
            for i in range(ntodo):
                handle = ready.popleft()
                if handle._cancelled:
                    continue
                owner = getattr(handle._callback, "__self__", None)
                append(plumb if owner is None else id(owner))
                handle._run()
            handle = None
            return
        for i in range(ntodo):
            handle = ready.popleft()
            if handle._cancelled:
                continue
            owner = getattr(handle._callback, "__self__", None)
            append(plumb if owner is None else id(owner))
            if self._debug:
                try:
                    self._current_handle = handle
                    t0 = self.time()
                    handle._run()
                    dt = self.time() - t0
                    if dt >= self.slow_callback_duration:
                        base_events.logger.warning(
                            "Executing %s took %.3f seconds", base_events._format_handle(handle), dt)
                finally:
                    self._current_handle = None
            else:
                handle._run()
        handle = None


class RecordingSelectorLoop(_RecordingMixin, asyncio.SelectorEventLoop):
    def __init__(self, selector=None, recorder: Recorder | None = None):
        super().__init__(selector)
        self._deja_setup(recorder)


if sys.platform == "win32":
    class RecordingProactorLoop(_RecordingMixin, asyncio.ProactorEventLoop):
        def __init__(self, proactor=None, recorder: Recorder | None = None):
            super().__init__(proactor)
            self._deja_setup(recorder)


def recording_loop(recorder: Recorder | None = None) -> asyncio.AbstractEventLoop:
    """Loop factory for ``asyncio.run(main(), loop_factory=deja.recording_loop)``.

    Uses the platform's default loop type. On unverified Python versions, warns and returns a plain loop.
    """
    if not _compat.supported():
        _compat.warn_unsupported()
        return asyncio.new_event_loop()
    if sys.platform == "win32":
        return RecordingProactorLoop(recorder=recorder)
    return RecordingSelectorLoop(recorder=recorder)
