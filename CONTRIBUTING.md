# Contributing to Déjà

Thanks for helping. Bug reports with a recording are the most useful contribution: a `.deja` file (with anything
private removed, see [SECURITY.md](SECURITY.md)) and the app code, or a minimal app that reproduces the problem.

## Set up

```bash
git clone https://github.com/Morning0Star/Deja && cd Deja
python -m venv .venv && . .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev,gallery]"                        # add [frameworks,drivers] for every adapter test
```

Tests that need a service skip when it isn't running. To run them all, start the containers the CI uses:

```bash
docker run -d --name deja-pg -e POSTGRES_USER=deja -e POSTGRES_PASSWORD=deja -e POSTGRES_DB=deja \
  -p 127.0.0.1:55432:5432 postgres:17-alpine
docker run -d --name deja-redis -p 127.0.0.1:56379:6379 redis:7-alpine
docker run -d --name deja-mongo -p 127.0.0.1:27217:27017 mongo:7
docker run -d --name deja-mysql -p 127.0.0.1:53306:3306 -e MYSQL_ROOT_PASSWORD=deja -e MYSQL_DATABASE=deja mysql:8.4
```

(Kafka: see `.github/workflows/tests.yml`.)

## Before a pull request

```bash
ruff check src tests          # lint (configured in pyproject.toml)
python -m pytest -q           # the suite; please run it on the oldest and newest supported Python if you can
python -m gallery.acceptance --recordings 2 --replays 20   # the race gallery still records and replays
```

- Keep changes focused, with a test that fails without them. A change to replay or analysis should come with a
  recording (or a test that records one) showing the behaviour.
- Déjà patches library internals; a new adapter needs a test that records against the real service and replays
  with it stopped.
- Write docstrings and messages in plain words: the report is read by people chasing a bug at 2 a.m.

## Design

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) explains the recording and replay model and lists the design decisions
(D1–D33) with their reasons. Please read the relevant ones before changing those areas.
