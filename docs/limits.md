# Limits

Déjà says what it can't decide rather than guess: a conflict it can't prove is reported as *possible*, and a replay
that can't follow the recording stops and says where. These are the limits as of this release.

## Analysis

**SQL conditions** are evaluated against what the recording shows: the rows a query returned and the values a write
set. Values only the server knows (`now()`, column defaults, sequences, triggers) and functions Déjà doesn't
evaluate leave a conflict *possible*. A subquery can prove that a row matched, never that none did. Writes made
from outside the recording (another service, a cron job) can't be seen.

**Shared state** is found by following the routes' code. State held only by library objects, or reached with
computed names (`getattr(obj, name)`), needs [`deja.track`](reference/python-api.md#deja.track.track) or
[`deja.touch`](reference/python-api.md#deja.touch). A container that something else also holds (a default
argument, a local variable) is left alone rather than split in two. See [Shared state](guides/shared-state.md).

## Replay

**Worker threads** are ordered at their accesses to tracked shared state. Work in between runs freely, and an
access made in the middle of an event-loop step replays just after it.

**Background tasks** are ordered from the start of the recording window. Their state before it is rebuilt by running
them from startup with the recorded startup answers, not recorded.

**Long requests and WebSocket connections** whose start has left the ring buffer can't be replayed and are left out
(the replay says so). A bigger buffer keeps them: see [Configuration](reference/configuration.md#buffer-size).

**Mixed recordings**: replay takes either an app or a mapping of job handlers, so a recording that holds both HTTP
requests and `deja.scope` jobs can be analysed but not replayed.

**Randomness** is recorded for the `random` module's functions, `uuid.uuid4`, `os.urandom` and the clocks
(see [Supported libraries](reference/adapters.md#inside-the-runtime)). Other sources, such as `random.shuffle`,
`secrets` or the app's own `random.Random`, aren't, and can make a replay diverge.

**Libraries without an adapter** run for real in replay. See
[Supported libraries](reference/adapters.md#libraries-not-listed).

**Exploring orderings** (`deja explore`) uses the recorded database answers, so orderings in which the app would
have read different rows are approximate, and marked so.

## Environments

- **Several workers** can be merged only when they run on one machine, since merging relies on their shared clock.
- **uvloop**: Déjà records on the standard asyncio loop.
- **Free-threaded Python** (3.13t, 3.14t) and **Trio** aren't supported.
- **Unverified Python versions**: on a version newer than those listed in [Configuration](reference/configuration.md#supported-pythons),
  the recording loop warns and runs unrecorded.
- **PyPy**: values stored inside tracked containers stay plain (no reference counts to check that safely).

## Cost

Recording costs CPU on every loop step. This repository's benchmark measured about 12% extra server CPU on CI and
6 to 17% on a laptop, where the noise is large, with throughput unchanged. The design target of 5% would need the
per-step log moved to C. Details: [benchmarks/RESULTS.md](https://github.com/Morning0Star/Deja/blob/main/benchmarks/RESULTS.md).

Found a limit that isn't listed? Please [open an issue](https://github.com/Morning0Star/Deja/issues).
