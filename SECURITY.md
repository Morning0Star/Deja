# Security

## Recordings contain your data

A `.deja` recording is a copy of what the app saw and did in the recorded window: request bodies, SQL statements and
their parameters, rows returned by the database, outside HTTP and API responses, WebSocket messages, and values read
from in-memory state. Only the `Authorization`, `Cookie` and `X-Api-Key` request headers are redacted
(`Recorder(redact_headers=...)` adds more). Treat recordings like production database dumps:

- keep them where production data is allowed, and delete them when the bug is fixed;
- remove anything private before attaching one to a public issue;
- don't record traffic you aren't allowed to store.

## Don't open recordings you don't trust

Loading a recording can import modules installed in your environment (to rebuild recorded exceptions), and replaying
one runs your app's code with the recorded inputs. Only replay or report on recordings you or your team made.

## Reporting a vulnerability

Please report security problems privately through GitHub: **Security → Report a vulnerability** on this repository.
You'll get an answer within a week. Please don't open a public issue for them.

## Supported versions

Déjà is pre-1.0; only the latest release gets fixes.
