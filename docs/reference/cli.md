# Command line

```text
deja [-h] [-V] COMMAND ...
```

| Command | Does |
|---|---|
| [`show`](#deja-show) | summarise a recording: its requests, steps and calls |
| [`replay`](#deja-replay) | replay a recording against your app, with no database or network |
| [`report`](#deja-report) | write an HTML report of a recording, optionally with a replay verdict |
| [`verify`](#deja-verify) | check a fix: does the recorded race still happen with this code? |
| [`diff`](#deja-diff) | replay one recording against two versions of your app and compare them |
| [`step`](#deja-step) | replay step by step, pausing before recorded steps to inspect the request |
| [`explore`](#deja-explore) | try other orderings of the recorded requests to find races not yet seen |
| [`shrink`](#deja-shrink) | cut a recording down to the requests its race needs |
| [`merge`](#deja-merge) | merge the recordings several workers saved for one race |
| [`export-test`](#deja-export-test) | write a pytest test that fails while the recorded race reproduces |

`-V`, `--version` prints the installed version. Every command takes `-h`, `--help`.

## Common options

`--app APP`
:   Your app, as `module:attribute`; the attribute defaults to `app`. It can be an ASGI app, an aiohttp
    `Application`, a dict of [job handlers](../guides/jobs.md#replay), or a function whose name starts with `create`,
    `make` or `build` that returns one of those. A function is called for each replay, so each replay starts from
    fresh state; prefer one. The module is imported with the current directory on `sys.path`, as uvicorn does.

`--deadline SECONDS`
:   How long a replay may go without progress before it stops and reports where it got stuck. Default 10 (600 for
    `step`, which waits for you).

If the recording was made with a fixed `PYTHONHASHSEED` and the current process has a different one, the commands
that replay (`replay`, `report --app`, `verify`, `diff`, `step`) run themselves again with the recorded value,
since set and dict ordering of strings depends on it.

## Exit status

| Status | Meaning |
|---|---|
| 0 | success: replays matched, the race is fixed, nothing new was found |
| 1 | the check failed: a replay didn't match (`replay`), the race still happens (`verify`), a new race was found (`explore`), there was no race to shrink to (`shrink`), no worker recordings (`merge`) |
| 2 | the command couldn't run: a missing or unreadable recording, wrong arguments |

So `deja verify` and `deja replay` can gate a CI job directly.

## deja show

```text
deja show recording
```

Prints the recording's header (reason, Python version, platform, loop, format), its event counts, every request
with its status and the start of its body, and the interleaving of request tasks, one line per loop pass. Needs no
app.

```console
$ deja show wallet-fail.deja
wallet-fail.deja
  reason: invariant violated: balance=0 charged=200
  python: 3.14.0
  platform: win32
  loop: RecordingProactorLoop
  format: deja/1
  events: 121  A=4  B=14  I=6  M=1  N=10  O=6  P=29  Q=4  R=24  S=11  T=12
requests:
  q0  POST /reset  -> 200  body=b''
  q1  POST /purchase  -> 200  body=b'{"amount":100}'
  q2  POST /purchase  -> 200  body=b'{"amount":100}'
  q3  GET /check  -> 200  body=b''
interleaving of request tasks (one line per loop pass):
  [start q0]
  [start q1] [start q2]
  q2 q1
  q1
  q1 q2
  q2
  [start q3]
```

The letters in `events` are the [event codes](recording-format.md#events).

## deja replay

```text
deja replay recording --app APP [--deadline SECONDS] [--times N]
```

Replays the recording against a fresh app and compares each request's answer with the recorded one.

`--times N`
:   Replay N times (default 1). Stops at the first replay that doesn't match.

```console
$ deja replay wallet-fail.deja --app gallery.wallet:create_app --times 3
followed 10/10 recorded steps
  request 0: recorded 200 {"ok":true} | replayed 200 {"ok":true}  [same]
  request 1: recorded 200 {"ok":true} | replayed 200 {"ok":true}  [same]
  request 2: recorded 200 {"ok":true} | replayed 200 {"ok":true}  [same]
  request 3: recorded 200 {"violated":true,"detail":"balance=0 charged=200"} | replayed 200 {"violated":true,"detail":"balance=0 charged=200"}  [same]

3/3 replays matched the recording
```

Lines that can appear in the output:

| Line | Means |
|---|---|
| `followed N/M recorded steps` | the replay took N of the M recorded steps in order |
| `DIVERGED: ...` | where the replay couldn't follow the recording: which task was due, and what happened instead |
| `I/O MISMATCH: ...` | the app made a call the recording has no answer for; it took a different path |
| `NOTE: the app's code differs from the recording in ...` | these modules changed since the recording was made |
| `NOTE: left out q4: they began before the recording window` | a request whose start had left the buffer; see [WebSockets](../guides/websockets.md#long-connections) |
| `NOTE: recorded with PYTHONHASHSEED=...` | run with that seed for exact set ordering (the CLI does this itself) |

## deja report

```text
deja report recording [-o OUT] [--app APP] [--times N] [--deadline SECONDS]
```

Writes the [HTML report](../guides/reading-reports.md) and prints its path.

`-o`, `--out OUT`
:   The `.html` file to write. Default: the recording's path with `.html`.

`--app APP`
:   Also replay against this app and show the verdict in the report. Without it the report needs no app.

`--times N`
:   Replays for the verdict (default 20).

## deja verify

```text
deja verify recording --app APP [--times N] [--deadline SECONDS]
```

Replays a recorded race against changed code and says whether it still happens. Exits 0 when fixed, 1 when not.
See [Fixing a race](../guides/fixing-races.md#4-fix-the-code-and-check-the-fix).

`--times N`
:   Replays to try (default 5). The race counts as fixed only if none of them reproduced it.

```console
$ deja verify wallet-fail.deja --app gallery.wallet:create_app
STILL THERE: 5 of 5 replays reproduced the recorded race

The race: Two purchase requests (A and B) ran at the same time. A read the wallet balance and got 100. ...

What happened in replay: the recorded race happened again
```

## deja diff

```text
deja diff recording --app APP --against OTHER [--deadline SECONDS]
```

Replays the recording against two versions of the app and prints, for each, the steps it followed and each request's
recorded, old and new answer, marked `same` or `DIFFERENT`, plus the modules whose code differs between them.

`--against OTHER`
:   The other version, as `module:attribute`.

## deja step

```text
deja step recording --app APP [--at N] [--task PATH] [--deadline SECONDS]
```

Replays the recording and pauses before recorded steps, with the request that's about to run suspended at its
`await`. Without `--at` or `--task` it pauses before every step.

`--at N`
:   Run to step N, then pause.

`--task PATH`
:   Run until task `PATH` (`q1`, `q0.2`...) is about to run, then pause.

At a pause:

| Command | Does |
|---|---|
| `n` or Enter | run this step, pause before the next |
| `c` | continue to the end, or to the next `--at` / `--task` stop |
| `s PATH` | continue until task `PATH` is about to run |
| `w` | where: the task's stack, innermost last |
| `l` | the local variables where it will resume |
| `p EXPR` | evaluate `EXPR` in that frame |
| `t` | the last accesses to shared state and recorded calls |
| `pdb` | open pdb on that frame (the task is suspended; inspect, don't resume it) |
| `q` | stop the replay |

## deja explore

```text
deja explore recording --app APP [--tries N] [--seed S] [--deadline SECONDS]
```

Runs the recorded requests in every serial order (each alone, start to finish; first-to-last and last-to-first when
there are more than five), then in `--tries` random interleavings, and prints the outcomes no serial order gives.
Exits 1 if it found one. See [Fixing a race](../guides/fixing-races.md#find-races-that-havent-happened-yet).

`--tries N`
:   Random interleavings to try (default 50).

`--seed S`
:   Seed of the first random interleaving (default 1). Try `i` uses `S + i`, so a race found is printed with the
    seed that reproduces it.

## deja shrink

```text
deja shrink recording --app APP -o OUT [--deadline SECONDS]
```

Writes a smaller recording with only the requests the race needs, and prints what it kept.

`-o`, `--out OUT`
:   The smaller `.deja` file.

```console
$ deja shrink wallet-fail.deja --app gallery.wallet:create_app -o small.deja
small.deja: kept q1, q2; removed q0, q3; 89 of 121 events
```

## deja merge

```text
deja merge folder -o OUT
```

Merges the per-worker recordings of one race into one. See [Several workers](../guides/workers.md).

`folder`
:   The `<group>/<id>/` folder holding one `.deja` file per worker.

`-o`, `--out OUT`
:   The merged `.deja` file.

## deja export-test

```text
deja export-test recording --app APP -o OUT
```

Writes a pytest test that fails while the recorded race reproduces, copies the recording to `recordings/` next to
it, and prints the test's path. See [Using Déjà in tests](../guides/testing.md#a-regression-test-from-a-recording).

`-o`, `--out OUT`
:   The test file to write.
