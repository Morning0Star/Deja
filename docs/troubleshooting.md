# Troubleshooting

## No recordings are saved

- **The app isn't on the recording loop.** The middleware warns once: `deja: not running on a recording loop;
  requests are not recorded`. Start the server with `--loop deja.loop:recording_loop` (uvicorn 0.36 or newer) or
  `asyncio.run(..., loop_factory=deja.recording_loop)`. See [ASGI apps](guides/asgi.md#run-it-on-the-recording-loop).
- **Nothing triggered a save.** Only a 5xx, an exception, a WebSocket close code 1011 or `deja.flag()` saves a
  recording. A race that returns 200 with wrong data needs a `deja.flag()` where your code can tell.
- **The Python version isn't verified.** On an unverified version the recording loop warns and runs the app
  unrecorded. See [Supported Pythons](reference/configuration.md#supported-pythons).
- **The middleware isn't outermost.** Add `DejaMiddleware` last with `add_middleware` (Starlette runs the last one
  added first), or wrap the finished app, so it sees every request and exception.

## The replay diverges

`DIVERGED: ...` means the app couldn't take a recorded step. Usually:

- **The code changed.** The summary lists the modules that differ from the recording. Replay against the code that
  was running when it was recorded (the same commit), then against your change.
- **The configuration differs.** Replay builds the app with `--app`; give it the same settings, feature flags and
  database driver as the recorded instance.
- **The app read something Déjà doesn't record**, such as `random.shuffle` or a library without an adapter. See
  [Limits](limits.md#replay).
- **The hash seed differs.** If the summary says `recorded with PYTHONHASHSEED=...`, run with that value (the CLI does
  it itself; in Python, set it before starting the interpreter).

`I/O MISMATCH: ...` means the app made a call the recording has no answer for: it took a different path from the
recorded one, most often for one of the reasons above.

## The replay hangs, then stops after the deadline

The replay waited `--deadline` seconds (default 10) without progress, then reports which task it was waiting for.
That task is blocked on something replay can't supply: a connection to a service without an adapter, a thread
waiting for an event the recording doesn't hold. Raise `--deadline` only if the app does slow real work at startup.

## A request is "left out"

`NOTE: left out q4: they began before the recording window` means that request's start had left the ring buffer when
the recording was saved. See [Long connections](guides/websockets.md#long-connections).

## The report says the race is only "possible"

Déjà couldn't prove both requests touched the same thing, usually because a SQL condition depends on a value only the
database knows. See [Certain and possible](guides/reading-reports.md#certain-and-possible).

## A type check fails while recording

Tracking swaps a container for a tracked subclass of the same type, and an object's class for a subclass. Code that
checks `type(x) is dict` or `type(obj) is Account` can see the difference. Leave that state alone with `track_skip`
(see [Choose what is tracked](guides/shared-state.md#choose-what-is-tracked)), and please report it.

## Getting help

Open an [issue](https://github.com/Morning0Star/Deja/issues) with the output of `deja show` and, if you can share
it, the recording and a minimal app (remove anything private first; see [Security](security.md)).
