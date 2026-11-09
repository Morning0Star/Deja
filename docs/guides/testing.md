# Using Déjà in tests

You don't need production traffic to use Déjà. A test can drive the app itself on the recording loop, save a
recording, and replay it, which is also the quickest way to try Déjà on your own code.

## Record and replay in one test

The app, with the race:

```python title="wallet.py"
--8<-- "docs/examples/wallet.py"
```

The test: two purchases at once on the recording loop, then replay and verify.

```python title="record_and_replay.py"
--8<-- "docs/examples/record_and_replay.py"
```

What happens:

1. `asyncio.run(..., loop_factory=deja.recording_loop)` runs the requests on the recording loop. Both purchases read
   the balance before either writes it; the second one finds more charged than the wallet held and calls
   `deja.flag()`, so the middleware saves a recording. `loop.deja.dumps` lists the files saved.
2. [`replay`](../reference/python-api.md#deja.replay.replay) runs the same requests against a fresh app in the
   recorded order: `followed` and `same_responses` are true.
3. [`verify`](../reference/python-api.md#deja.verify.verify) says the race is still there, because the code isn't
   fixed. After a fix, `verdict.fixed` becomes true.

Both examples are in the repository under `docs/examples/` and run as part of its test suite.

## Save a recording without a failure

To record a stretch of activity whether or not anything fails, mark the buffer and dump from the mark:

```python
rec = asyncio.get_running_loop().deja
marker = rec.mark("trial 3")
...                                   # drive the app
rec.dump("trial3.deja", since=marker)
```

See [`Recorder.mark`](../reference/python-api.md#deja.Recorder.mark) and
[`Recorder.dump`](../reference/python-api.md#deja.Recorder.dump).

## A regression test from a recording

`deja export-test` turns a recording into a pytest test that fails while the race reproduces (see
[Fixing a race](fixing-races.md#5-keep-it-fixed)). The generated test only needs `deja` installed:

```python
from deja.verify import verify

def test_lost_update_wallet_balance():
    from gallery.wallet import create_app as app_factory

    verdict = verify(RECORDING, app_factory, times=3)
    assert verdict.fixed, verdict.summary()
```

## Tips

- Use a factory for the app, so each replay gets fresh state.
- Replaying doesn't write recordings, even though the app still has the middleware.
- If the recording was made with a fixed `PYTHONHASHSEED`, run replays with the same value (`deja replay` does this
  itself); see [Configuration](../reference/configuration.md#hash-seed).
