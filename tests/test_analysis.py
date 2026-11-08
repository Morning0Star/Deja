import asyncio
import glob
import os

import pytest

from deja import events as ev, recording_loop
from deja.analysis import analyze, sql_touches
from deja.fileformat import read


# ---------------------------------------------------------------------- SQL rules
@pytest.mark.parametrize("sql,params,expected", [
    ("SELECT balance FROM gallery_wallet WHERE id = 1", [], ("gallery_wallet[id=1]", "read")),
    ("select price from bench_items where id = $1", [42], ("bench_items[id=42]", "read")),
    ("SELECT EXISTS (SELECT 1 FROM gallery_users WHERE username = $1)", ["ada"], ("gallery_users[username=ada]", "read")),
    ("UPDATE gallery_wallet SET balance = $1, charged = charged + $2 WHERE id = 1", [0, 100], ("gallery_wallet[id=1]", "write")),
    ("INSERT INTO gallery_users (username) VALUES ($1)", ["ada"], ("gallery_users[username=ada]", "write")),
    ("DELETE FROM sessions WHERE token = 'abc'", [], ("sessions[token=abc]", "write")),
    ("TRUNCATE gallery_users", [], ("gallery_users", "write")),
])
def test_sql_rules(sql, params, expected):
    (t,) = sql_touches(sql, params)
    assert (t["resource"], t["kind"]) == expected


def test_update_shows_values_and_expressions():
    (t,) = sql_touches("UPDATE w SET balance = $1, charged = charged + $2 WHERE id = 1", [0, 100])
    assert t["writes"]["balance"] == 0 and str(t["writes"]["charged"]) == "charged + 100"


def test_unknown_sql_is_not_guessed():
    assert sql_touches("VACUUM", []) == []
    assert sql_touches("EXPLAIN ANALYZE SELECT 1", []) == []
    for t in sql_touches("WITH x AS (SELECT 1) SELECT * FROM x", []):  # whatever it returns, it must be well formed
        assert t["kind"] in ("read", "write") and t["resource"]


# ---------------------------------------------------------------------- patterns on synthetic recordings
def _touch(req, res, kind, value, t):
    return (ev.TOUCH, req, res, kind, value, t)


def _req(rid, path):
    return (ev.REQUEST, rid, "r0", "POST", path, [], b"", 0)


def test_lost_update_story_and_moments():
    events = [_req(1, "/buy"), _req(2, "/buy"),
              _touch("q1", "wallet", "read", 100, 1_000_000), _touch("q2", "wallet", "read", 100, 2_000_000),
              _touch("q1", "wallet", "write", 0, 3_000_000), _touch("q2", "wallet", "write", 0, 4_000_000)]
    a = analyze({"reason": "charged 200", "labels": {"POST /buy": "purchase"}}, events)
    (c,) = a["conflicts"]
    assert c["pattern"] == "lost-update" and c["resource"] == "wallet"
    assert "Two purchase requests (A and B)" in a["story"]
    assert "B saved last and overwrote A's change" in a["story"] and "The app reported: charged 200" in a["story"]
    assert [m["text"] for m in a["moments"]] == [
        "A reads wallet → 100", "B reads wallet → 100", "A writes wallet (0)", "B writes wallet (0)"]
    assert a["moments"][1]["warning"]


def test_check_then_act():
    events = [_req(1, "/signup"), _req(2, "/signup"),
              _touch("q1", "users[ada]", "read", False, 1), _touch("q2", "users[ada]", "read", False, 2),
              _touch("q1", "users[ada]", "write", "ada", 3), _touch("q2", "users[ada]", "write", "ada", 4)]
    (c,) = analyze({}, events)["conflicts"]
    assert c["pattern"] == "check-then-act"


def test_serial_requests_are_not_a_conflict():
    events = [_req(1, "/buy"), _req(2, "/buy"),
              _touch("q1", "wallet", "read", 100, 1), _touch("q1", "wallet", "write", 0, 2),
              _touch("q2", "wallet", "read", 0, 3), _touch("q2", "wallet", "write", 0, 4)]
    a = analyze({}, events)
    assert a["conflicts"] == [] and "no conflicting" in a["story"]


# ---------------------------------------------------------------------- real gallery recordings
@pytest.mark.parametrize("scenario,pattern", [("wallet", "lost-update"), ("likes", "lost-update"),
                                              ("signup", "check-then-act")])
def test_gallery_failures_are_explained(tmp_path, scenario, pattern):
    from gallery import stress

    asyncio.run(stress.run(scenario, 40, False, None, 60, str(tmp_path)), loop_factory=recording_loop)
    files = sorted(glob.glob(os.path.join(tmp_path, f"{scenario}-fail-*.deja")))
    assert files, "need a failing trial"
    a = analyze(*read(files[0]))
    assert a["conflicts"] and a["conflicts"][0]["pattern"] == pattern
    assert a["moments"] and any(m["warning"] for m in a["moments"])
    assert a["duration_ms"] > 0 and all(r["start"] is not None for r in a["requests"])


def test_call_kinds_override_the_http_method():
    from deja.analysis import call_touches

    io = {"op": "http.send", "detail": {"method": "POST", "url": "http://pricing.internal/quote?x=1", "body": ""}}
    assert call_touches(io)[0]["kind"] == "write"
    assert call_touches(io, {"POST pricing.internal/quote": "read"})[0]["kind"] == "read"
    assert call_touches(io, {"POST pricing.internal/*": "ignore"}) == []
    import pytest

    import deja

    with pytest.raises(ValueError):
        deja.calls({"GET x/y": "maybe"})
