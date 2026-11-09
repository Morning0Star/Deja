# Python API

The names most apps need are importable from `deja`. The tools for working with recordings live in their own
modules: `deja.replay`, `deja.verify`, `deja.explore`, `deja.shrink`, `deja.stepper`, `deja.group`, `deja.report`,
`deja.analysis` and `deja.fileformat`.

Déjà is pre-1.0: names documented on this page are the public API, and anything else may change between minor
versions.

## Recording

::: deja.DejaMiddleware
    options:
      members: false

::: deja.aiohttp_web.deja_middleware

::: deja.scope.scope
    options:
      heading: deja.scope
      toc_label: scope

::: deja.flag

::: deja.recording_loop

::: deja.Recorder
    options:
      members: [mark, dump]

## Shared state and report hints

::: deja.track.track
    options:
      heading: deja.track
      toc_label: track

::: deja.touch

::: deja.labels

::: deja.calls

## Replay

::: deja.replay.replay

::: deja.replay.ReplayResult
    options:
      members: [followed, same_responses, summary]

::: deja.stepper.Stepper
    options:
      members: false

::: deja.stepper.StepInfo
    options:
      members: [frame]

## Checking a fix

::: deja.verify.verify

::: deja.verify.Verdict
    options:
      members: [reproduced, fixed, how, summary]

::: deja.verify.diff

::: deja.verify.export_test

## Exploring and shrinking

::: deja.explore.explore

::: deja.explore.summary

::: deja.shrink.shrink

## Recordings

::: deja.group.merge

::: deja.report.write_report

::: deja.analysis.analyze

::: deja.fileformat.read

::: deja.fileformat.write

::: deja.fileformat.FormatError
