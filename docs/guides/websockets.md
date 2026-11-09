# WebSockets

[`DejaMiddleware`](asgi.md) records WebSocket connections as well as HTTP requests. Nothing extra is needed.

## What is recorded

A connection is one request, from connect to close. Every message the app receives and every message it sends is
recorded at the point where it happened, so a race between two connections (two clients booking the same seat) is
recorded like a race between two HTTP requests, and the report says which message of which connection did what.

## When a recording is saved

| Trigger | Recording's `reason` |
|---|---|
| The connection closes with code 1011 (server error) | `close code 1011` |
| An exception escapes the app | `exception ...` |
| App code calls [`deja.flag()`](../reference/python-api.md#deja.flag) while handling a message | `flag: ...` |

A flag saves the recording as soon as the app waits for the next message, so a long-lived connection doesn't have to
close first.

## Replay

Replay needs no client: it feeds each connection the messages it received, at the recorded points, and checks that
the app sends the same ones. The "response" compared for a connection is what it sent, one line per message:

```text
booked s1
taken s2
```

## Long connections

A connection whose start has already left the [ring buffer](../reference/configuration.md#buffer-size) when a
recording is saved can't be replayed, because replay has to start it from the beginning. It is left out, and the
replay says so:

```text
NOTE: left out q4: they began before the recording window (long WebSocket connections); a bigger buffer
(Recorder(max_events=...)) keeps them
```

If your connections live for a long time, give the recorder a bigger buffer.
