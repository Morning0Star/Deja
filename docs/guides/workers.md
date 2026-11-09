# Several workers

Production apps usually run several worker processes (`uvicorn --workers 4`, gunicorn with uvicorn workers). Two
requests that race through a shared database can land on different workers, and then neither worker's recording
shows the race on its own. A *group* makes the workers record it together.

## Record

Give every worker the same `group` folder, one all of them can write to:

```python
app.add_middleware(
    deja.DejaMiddleware, directory="deja-recordings", group="/shared/deja-group"
)
```

or set the `DEJA_GROUP` environment variable instead of passing `group`.

When one worker saves a recording, it also posts the time window to the group folder. Every worker, the saving one
too, notices within half a second and writes its own events for that window to `<group>/<id>/<worker>.deja`.
Workers are named `pid<process id>` unless you pass `worker="..."`.

## Merge

```bash
deja merge /shared/deja-group/<id>/ -o race.deja
```

All processes on one machine share a high-resolution clock, so the workers' events line up by time. `deja merge`
interleaves them and renames each worker's requests so they don't clash: worker *k*'s request *r* becomes
*r* + *k* × 100000. The merged report then sees the race between requests on different workers.

## Replay

```bash
deja replay race.deja --app myapp:create_app
```

Workers don't share memory, so replay builds one app per worker (`--app` must be a factory) and sends each worker's
requests to its own app, in the merged order.

## Limits

- The workers must run on **one machine**: merging relies on their shared clock. Workers on different machines
  can't be merged.
- A worker notices a group request within half a second. A worker whose buffer has already dropped that window by
  then has nothing to add.
