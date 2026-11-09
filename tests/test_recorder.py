import asyncio
import warnings

from deja import _compat, fileformat
from deja import events as ev
from deja.loop import recording_loop
from deja.recorder import Recorder


def run(coro_fn, recorder=None, setup=None):
    loop = recording_loop(recorder)
    if setup:
        setup(loop)
    try:
        return loop.run_until_complete(coro_fn()), loop.deja
    finally:
        loop.close()


async def tree():
    async def leaf():
        await asyncio.sleep(0)

    async def mid():
        await asyncio.gather(asyncio.create_task(leaf()), asyncio.create_task(leaf()))

    await asyncio.gather(asyncio.create_task(mid()), asyncio.create_task(leaf()))


def task_news(rec):
    return [(e[1], e[2]) for e in rec.events() if e[0] == ev.TASK_NEW]


def steps(rec):
    return [e[1] for e in rec.events() if e[0] == ev.STEP]


def test_paths_follow_the_creation_tree():
    _, rec = run(tree)
    assert task_news(rec) == [
        ("r0", None),  # run_until_complete's task
        ("r0.0", "r0"),  # mid
        ("r0.1", "r0"),  # leaf
        ("r0.0.0", "r0.0"),  # mid's leaves
        ("r0.0.1", "r0.0"),
    ]


def test_every_task_step_is_logged_in_batches():
    _, rec = run(tree)
    assert set(steps(rec)) == {"r0", "r0.0", "r0.1", "r0.0.0", "r0.0.1"}
    batches = [e[1] for e in rec.events() if e[0] == ev.BATCH]
    ran = [e for e in rec.events() if e[0] in (ev.STEP, ev.PLUMB)]
    assert sum(batches) >= len(ran) > 0  # cancelled handles are counted in a batch but don't run


def test_same_program_same_order():
    _, a = run(tree)
    _, b = run(tree)
    assert steps(a) == steps(b)


def test_eager_tasks_are_registered():
    def setup(loop):
        loop.set_task_factory(asyncio.eager_task_factory)

    _, rec = run(tree, setup=setup)
    paths = [p for p, _ in task_news(rec)]
    assert len(paths) == 5 and len(set(paths)) == 5
    assert rec.buffer  # still records


def test_user_task_factory_is_kept():
    seen = []

    def factory(loop, coro, **kw):
        seen.append(1)
        return asyncio.Task(coro, loop=loop, **kw)

    def setup(loop):
        loop.set_task_factory(factory)
        assert loop.get_task_factory() is factory

    run(tree, setup=setup)
    assert len(seen) == 5


def test_flag_and_reads_carry_the_task_path():
    async def main():
        rec = asyncio.get_running_loop().deja
        rec.flag("invariant broken")
        rec.read("time.time", 123.0)

    _, rec = run(main)
    assert any(e[:3] == (ev.FLAG, "r0", "invariant broken") for e in rec.events())
    assert (ev.READ, "r0", "time.time", 123.0) in rec.events()


def test_file_round_trip(tmp_path):
    rec = Recorder()
    rec.buffer.extend([(ev.BATCH, 1), (ev.STEP, "r0"), (ev.REQUEST, 0, "r0", "POST", "/x", [["a", "b"]], b"\x00body")])
    p = rec.dump(str(tmp_path / "t.deja"))
    header, events = fileformat.read(p)
    assert header["format"] == "deja/1"
    assert events[2][-1] == b"\x00body"
    assert [e[0] for e in events] == [ev.BATCH, ev.STEP, ev.REQUEST]


def test_unverified_python_does_not_record(monkeypatch):
    monkeypatch.setattr(_compat, "VERIFIED", set())
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        loop = recording_loop()
    try:
        assert not hasattr(loop, "deja")
        assert any("not verified" in str(x.message) for x in w)
    finally:
        loop.close()


def test_selector_loop_records_too():
    from deja.loop import RecordingSelectorLoop

    loop = RecordingSelectorLoop()
    try:
        loop.run_until_complete(tree())
        assert set(steps(loop.deja)) == {"r0", "r0.0", "r0.1", "r0.0.0", "r0.0.1"}
        assert loop.deja.header()["loop"] == "RecordingSelectorLoop"
    finally:
        loop.close()


def test_recording_from_a_newer_deja_is_refused_clearly(tmp_path):
    from deja.fileformat import FormatError, read, write

    write(str(tmp_path / "new.deja"), {"format": "deja/99", "deja": "9.9"}, [])
    try:
        read(str(tmp_path / "new.deja"))
    except FormatError as e:
        assert "newer" in str(e)
    else:
        raise AssertionError("read a recording from the future")
