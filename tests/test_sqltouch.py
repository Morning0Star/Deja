import pytest

from deja import events as ev, sqltouch
from deja.analysis import analyze, sql_touches

pytestmark = pytest.mark.skipif(not sqltouch.available(), reason="sqlglot not installed")


def res(sql, params=()):
    return [(t["kind"], t["resource"]) for t in sql_touches(sql, list(params))]


def test_subquery_and_cte_rows_are_named():
    assert res("SELECT EXISTS (SELECT 1 FROM users WHERE username = $1)", ["ada"]) == [("read", "users[username=ada]")]
    got = res("WITH x AS (SELECT id FROM orders WHERE user_id = $1) SELECT * FROM items JOIN x ON items.order_id = x.id",
              [5])
    assert ("read", "orders[user_id=5]") in got and ("read", "items") in got and all("x" != r for _, r in got)


def test_multi_row_and_composite_keys():
    assert res("SELECT * FROM items WHERE id = ANY($1) FOR UPDATE", [[3, 4]]) == [("read", "items[id=3]"),
                                                                                    ("read", "items[id=4]")]
    assert res("DELETE FROM carts WHERE id IN (1, 2)") == [("write", "carts[id=1]"), ("write", "carts[id=2]")]
    assert res("UPDATE w SET balance = $1 WHERE id = 1 AND owner = $2", [0, "u7"]) == [("write", "w[id=1, owner=u7]")]


def test_range_is_table_level_with_condition():
    [t] = sql_touches("SELECT * FROM items WHERE price > $1", [100])
    assert t["resource"] == "items" and t["where"] == "price > 100" and not t["precise"]


def test_upsert_and_insert_select():
    [t] = sql_touches("INSERT INTO users (username, email) VALUES ($1, $2) ON CONFLICT (username) "
                      "DO UPDATE SET email = EXCLUDED.email RETURNING id", ["ada", "a@x"])
    assert t["resource"] == "users[username=ada]" and t["writes"] == {"username": "ada", "email": "a@x"}
    assert res("INSERT INTO audit (msg) SELECT name FROM users WHERE id = $1", [2]) == [("read", "users[id=2]"),
                                                                                         ("write", "audit")]


def test_update_from_reads_the_other_table():
    got = res("UPDATE stock s SET qty = s.qty - o.qty FROM orders o WHERE o.id = $1 AND s.item_id = o.item_id", [9])
    assert ("write", "stock") in got and ("read", "orders") in got


def test_fallback_rules_when_sqlglot_missing(monkeypatch):
    monkeypatch.setattr(sqltouch, "sqlglot", None)
    [t] = sql_touches("SELECT balance FROM wallet WHERE id = $1", [1])
    assert t["resource"] == "wallet[id=1]" and t["precise"] and t["table"] == "wallet"


def _io(path, sql, params, value, seq_t):
    return [(ev.IO_START, path, "pg.fetchval", seq_t),
            (ev.IO_END, path, "pg.fetchval", "d", "ok", value, seq_t + 1, {"sql": sql, "params": params})]


def test_range_read_against_row_write_is_a_possible_conflict():
    events = [
        (ev.REQUEST, 1, "r0", "POST", "/restock", [], b"", 0),
        (ev.REQUEST, 2, "r0", "POST", "/buy", [], b"", 1),
        *_io("q1", "SELECT count(*) FROM items WHERE qty < $1", [5], 3, 10),
        *_io("q2", "SELECT qty FROM items WHERE id = $1", [7], 4, 20),
        *_io("q1", "UPDATE items SET qty = qty + 10 WHERE qty < $1", [5], "UPDATE 3", 30),
        *_io("q2", "UPDATE items SET qty = $1 WHERE id = $2", [3, 7], "UPDATE 1", 40),
        (ev.RESPONSE, 1, 200, b"", 50), (ev.RESPONSE, 2, 200, b"", 51),
    ]
    a = analyze({}, events)
    [c] = a["conflicts"]
    assert c["confidence"] == "possible" and c["resource"] == "items[id=7]" and "qty < 5" in c["broad"]
    assert a["story"].startswith("Possible conflict.")

