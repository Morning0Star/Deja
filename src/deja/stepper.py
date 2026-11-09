"""Step through a replay: pause before any recorded step and look at the request that's about to run.

    deja step recording.deja --app myapp:create_app            # pause before every request step
    deja step recording.deja --app myapp:create_app --at 12    # run to step 12, then pause

Because replay is deterministic, step 12 is the same moment every time. At a pause the request that's about to
run is suspended at an ``await``; its stack shows where it will resume. Commands:

    n / Enter   run this step, pause before the next one
    c           continue to the end (or the next --at / --task stop)
    s PATH      continue until task PATH (q1, q0.2...) is about to run
    w           where: the task's stack, innermost last
    l           the local variables where it will resume
    p EXPR      evaluate EXPR there (locals and globals of that frame)
    t           the last accesses to shared state and recorded calls
    pdb         open pdb on that frame (inspection: the task is suspended)
    q           stop the replay

``replay(recording, factory, on_step=callback)`` gives the same pause to Python code: ``callback(info)`` is called
before each recorded step with ``info.index``, ``info.path``, ``info.task``, ``info.frame`` and ``info.loop``.
"""

import pprint


class Stop(Exception):
    """Raised by a step callback to end the replay."""


def frames(task):
    """Every frame of a suspended task, outermost first: the chain of awaited coroutines (``Task.get_stack`` only
    gives the outermost one for a suspended coroutine)."""
    out, coro = [], task.get_coro() if task is not None else None
    while coro is not None:
        frame = getattr(coro, "cr_frame", None) or getattr(coro, "gi_frame", None) or getattr(coro, "ag_frame", None)
        if frame is None:
            break
        out.append(frame)
        coro = getattr(coro, "cr_await", None) or getattr(coro, "gi_yieldfrom", None) or getattr(coro, "ag_await", None)
    return out


def _app_frame(fs):
    """The innermost frame in the app's own code (else the innermost frame)."""
    from .autotrack import _app_file

    for f in reversed(fs):
        if _app_file(f.f_code.co_filename):
            return f
    return fs[-1] if fs else None


class StepInfo:
    def __init__(self, loop, index, path, task):
        self.loop, self.index, self.path, self.task = loop, index, path, task

    @property
    def frame(self):
        """The innermost frame of the app's code where the task will resume (or the innermost frame at all)."""
        return _app_frame(frames(self.task))


def _where(info):
    out = []
    for f in frames(info.task):
        out.append(f"  {f.f_code.co_filename}:{f.f_lineno} in {f.f_code.co_name}")
    return "\n".join(out) or "  (not started yet)"


def _recent(info, n=8):
    from . import events as ev

    lines = []
    sched = info.loop.schedule  # what the recording shows happened before this step
    upto = sched.at[info.loop.pos] if info.loop.pos < len(sched.at) else len(sched.events)
    for e in list(sched.events[:upto])[::-1]:
        if e[0] == ev.TOUCH:
            lines.append(f"  {e[1]} {e[3]}s {e[2]} = {e[4]!r}")
        elif e[0] == ev.IO_END:
            lines.append(f"  {e[1]} called {e[2]} -> {str(e[5])[:60]}")
        if len(lines) >= n:
            break
    return "\n".join(reversed(lines)) or "  (nothing yet)"


class Stepper:
    """The interactive pause (``deja step``). ``ask`` / ``say`` are swappable for tests."""

    def __init__(self, at=None, task=None, ask=input, say=print):
        self.at, self.task, self.ask, self.say = at, task, ask, say
        self.mode = "step" if at is None and task is None else "run"

    def __call__(self, info: StepInfo):
        if self.mode == "run":
            if (self.at is not None and info.index >= self.at) or (self.task is not None and info.path == self.task):
                self.mode = "step"
            else:
                return
        if self.mode == "continue":
            return
        self.say(f"step {info.index}: {info.path} is about to run")
        frame = info.frame
        self.say(
            f"  resumes at {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}"
            if frame
            else "  (starts now)"
        )
        while True:
            try:
                cmd = self.ask("(deja) ").strip()
            except EOFError:
                cmd = "c"
            if cmd in ("", "n"):
                return
            if cmd == "c":
                self.mode = "continue"
                return
            if cmd.startswith("s "):
                self.task, self.at, self.mode = cmd[2:].strip(), None, "run"
                return
            if cmd == "q":
                raise Stop()
            frame = info.frame
            if cmd == "w":
                self.say(_where(info))
            elif cmd == "l":
                self.say(pprint.pformat(dict(frame.f_locals)) if frame else "(no frame)")
            elif cmd.startswith("p "):
                try:
                    self.say(repr(eval(cmd[2:], frame.f_globals, dict(frame.f_locals))) if frame else "(no frame)")
                except Exception as exc:
                    self.say(f"{type(exc).__name__}: {exc}")
            elif cmd == "t":
                self.say(_recent(info))
            elif cmd == "pdb":
                import pdb

                if frame is not None:
                    pdb.Pdb().interaction(frame, None)
            else:
                self.say("commands: n c s PATH w l p EXPR t pdb q")
