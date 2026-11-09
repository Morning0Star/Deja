# Fixing a race

A recording turns "it happens sometimes" into a bug you can run. This page goes from a recording to a fix you can
trust, using the double-charge recording from [Getting started](../getting-started.md).

## 1. Understand it

```bash
deja report wallet-fail.deja --app gallery.wallet:create_app
```

The [report](reading-reports.md) names the race (a lost update on the wallet balance), the two requests and the
order of their reads and writes. For a closer look, step through the replay.

## 2. Step through it

`deja step` replays the recording and pauses before recorded steps, with the request that's about to run suspended
at its `await`:

```console
$ deja step wallet-fail.deja --app gallery.wallet:create_app --at 3
step 3: q2 is about to run
  resumes at gallery/_io.py:20 in io
(deja) w
  ...
  gallery/wallet.py:112 in purchase
  gallery/wallet.py:100 in _purchase
  gallery/wallet.py:39 in get_balance
  gallery/_io.py:20 in io
(deja) t
  q0 writes wallet.balance = 100
  q0 writes wallet.charged = 0
  q2 called executor -> None
  q2 reads wallet.balance = 100
  q1 called executor -> None
```

Because replay is deterministic, step 3 is the same moment every time. At a pause:

| Command | Does |
|---|---|
| `n` or Enter | run this step, pause before the next |
| `c` | continue to the end (or the next `--at` / `--task` stop) |
| `s PATH` | continue until task `PATH` (`q1`, `q0.2`...) is about to run |
| `w` | where: the task's stack, innermost last |
| `l` | the local variables where it will resume |
| `p EXPR` | evaluate `EXPR` in that frame |
| `t` | the last accesses to shared state and recorded calls |
| `pdb` | open pdb on that frame |
| `q` | stop the replay |

## 3. Make it smaller

A recording from production may hold dozens of requests that have nothing to do with the race. `deja shrink` keeps
only those the race needs:

```console
$ deja shrink wallet-fail.deja --app gallery.wallet:create_app -o small.deja
small.deja: kept q1, q2; removed q0, q3; 89 of 121 events
```

It keeps the two requests of the race, then removes each other request whose removal still lets the race replay
with the same answers. A request that changed state the race depends on stays.

## 4. Fix the code and check the fix

Change the code (a lock, a transaction, an atomic `UPDATE ... SET balance = balance - $1 WHERE balance >= $1`), then
replay the recorded race against it:

```console
$ deja verify wallet-fail.deja --app gallery.wallet:create_app
STILL THERE: 5 of 5 replays reproduced the recorded race

The race: Two purchase requests (A and B) ran at the same time. A read the wallet balance and got 100. ...

What happened in replay: the recorded race happened again
```

That was the unfixed code. With a fix in place, `verify` prints `FIXED` and why:

- **the replay can't take the recorded steps**: the code now orders the requests differently (the lock made B wait
  for A), so the recorded interleaving can't happen any more; or
- **the requests ran in the recorded order but answered differently**: a check now catches the conflict (B gets
  `402 insufficient funds`).

`deja verify` exits with status 0 when the race is fixed and 1 when it is still there, so it can gate a CI job.

To compare two versions side by side, use `deja diff`:

```bash
deja diff wallet-fail.deja --app myapp_old:create_app --against myapp:create_app
```

## 5. Keep it fixed

```console
$ deja export-test wallet-fail.deja --app gallery.wallet:create_app -o tests/test_wallet_race.py
tests/test_wallet_race.py
```

This writes a pytest test and copies the recording to `tests/recordings/`. The test replays the recording and fails
while the race reproduces, so once the fix is in, it passes, and it fails again if the race ever comes back.

## Find races that haven't happened yet

A recording holds one interleaving. `deja explore` replays the same requests in others: every serial order (each
request alone, start to finish), then seeded random interleavings. An outcome that no serial order gives is a race.

```console
$ deja explore small.deja --app gallery.wallet:create_app
2 distinct outcome(s) from running the requests one at a time
  order [1, 2]: request 1: 200 {"ok":true}; request 2: 402 {"detail":"insufficient funds"}
  order [2, 1]: request 1: 402 {"detail":"insufficient funds"}; request 2: 200 {"ok":true}
RACE (seed 1): request 1: 200 {"ok":true}; request 2: 200 {"ok":true}
  steps: q1 q2 q2 q1 q1 q1 q2 q2
```

Run alone, one purchase succeeds and the other is refused; interleaved, both succeed. Exploring works from a
recording in which nothing went wrong, too: record a normal pair of requests and let `explore` look for the
interleaving that breaks them.

Database and HTTP answers still come from the recording, so an ordering in which the app would have read different
rows isn't modelled. Runs that made calls the recording has no answer for are marked *approximate*.
