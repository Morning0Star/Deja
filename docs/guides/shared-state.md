# Shared state

Many races are on the app's own memory: a dict of carts, a set of booked seats, a counter on an object. For the
report to explain such a race, and for replay to put [worker threads](threads.md) in order, Déjà has to see each
read and write of that state. Most apps need to do nothing for that.

## Found automatically

On the first request, the middleware follows the app's code: its route handlers and dependencies, and the
functions, classes and modules they use, through their code (`services.reserve()` leads to the `services` module,
which leads to its `HOLDS` dict). What it finds there is tracked:

| Kind of state | Example | Reported as |
|---|---|---|
| A `dict`, `defaultdict`, `OrderedDict`, `Counter`, `list`, `set` or `deque` in a module variable, a closure, an attribute of an app object or `app.state` | `carts = {}` | `carts[42]` |
| An app object whose attributes request code assigns | `self.balance -= amount` | `wallet.balance` |
| A module-level number or string that request code reassigns with `global` | `global visits; visits += 1` | `visits` |
| A new container stored inside a tracked one | `carts[c] = {"qty": 1}`, then `carts[c]["qty"] += 1` | `carts[c][qty]` |
| A pydantic model held by the app | `wallet.balance -= 30` | `wallet.balance` |

A container is tracked by swapping it for a tracked copy of the same type. That is only safe if every reference to
it is swapped too, so Déjà checks who else holds it, and leaves a container alone when something other than a module,
an object attribute, `app.state` or a closure holds it (a default argument, a tuple, a running coroutine). An object
is tracked in place: its class is swapped for a subclass that reports its public data attributes.

Module variables reassigned with `global` can't be swapped, so the functions that use them are watched with
[`sys.monitoring`](https://docs.python.org/3/library/sys.monitoring.html), only those functions, only those lines.

Replay tracks the same state, with the same rules, which are saved in the recording.

## Reads of a whole container

Reading a whole container (iterating it, `len()`, `bool()`, `keys()`, `values()`, `items()`, indexing a list) is
one read of `name[*]`, with the keys it saw (up to 200). The report uses that to tell whether such a read covered a
key another request changed, for example a request that summed every cart while another request added one.

## What tracking can't find

- state held only by library objects (a client's internal cache);
- state reached with computed names (`getattr(obj, name)`, `globals()[key]`);
- attributes of objects stored in containers: for `accounts[7].balance`, the read of `accounts[7]` is reported but
  not the attribute, unless the object is one of the app's own classes stored in a tracked dict.

For those, track the state yourself or report the access.

## Track something yourself

[`deja.track(obj, name)`](../reference/python-api.md#deja.track.track) tracks a container or object Déjà didn't find.
Containers are copied, so **use the returned object**:

```python
# "ada" in users   ->  read users[username=ada]
self.users = deja.track(set(), "users", key="username")

# carts[42] = c     ->  write carts[42]
self.carts = deja.track({}, "carts")

# wallet.balance    ->  read wallet.balance
deja.track(self.wallet, "wallet")
```

`key` names the field that identifies an item in a set or list of records, so the report can say which item.

## Report an access yourself

[`deja.touch(resource, kind, value)`](../reference/python-api.md#deja.touch) reports one read or write of anything,
by name:

```python
total = await cache.get(f"cart:{cart_id}")
deja.touch(f"cart:{cart_id}", "read", total)
```

Use the same `resource` string for every access to the same thing.

## Choose what is tracked

Tracking changes the type of what it tracks (a tracked dict is a subclass of `dict`). Code that checks
`type(x) is dict`, or a class whose instances must keep their exact class, can break. Leave such state alone with
patterns (`fnmatch` wildcards) matched against the state's name or its class:

```python
app.add_middleware(
    deja.DejaMiddleware,
    track_skip=["*.cache", "myapp.clients.*"],    # leave these alone
)
# or: track only these
app.add_middleware(deja.DejaMiddleware, track_only=["store.*"])
# or: track nothing automatically
app.add_middleware(deja.DejaMiddleware, track=False)
```

With `track=False`, races on in-memory state aren't explained, and worker threads are ordered only at accesses you
report with `deja.touch`. Recording and replay of requests and their calls are unaffected.

## On PyPy

PyPy has no reference counts, so Déjà can't check that nothing else holds a value stored inside a tracked container.
Values inside tracked containers stay plain on PyPy; the containers themselves are tracked.
