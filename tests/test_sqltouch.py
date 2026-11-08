import pytest

from deja import codec, events as ev, sqltouch
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


def restock_vs_buy(buyer_reads, buyer_sql="SELECT qty FROM items WHERE id = $1"):
    """Request 1 restocks every item with qty < 5; request 2 reads item 7 and sets its qty."""
    return [
        (ev.REQUEST, 1, "r0", "POST", "/restock", [], b"", 0),
        (ev.REQUEST, 2, "r0", "POST", "/buy", [], b"", 1),
        *_io("q1", "SELECT count(*) FROM items WHERE qty < $1", [5], 3, 10),
        *_io("q2", buyer_sql, [7], buyer_reads, 20),
        *_io("q1", "UPDATE items SET qty = qty + 10 WHERE qty < $1", [5], "UPDATE 3", 30),
        *_io("q2", "UPDATE items SET qty = $1 WHERE id = $2", [3, 7], "UPDATE 1", 40),
        (ev.RESPONSE, 1, 200, b"", 50), (ev.RESPONSE, 2, 200, b"", 51),
    ]


def test_range_read_undecided_by_the_data_is_a_possible_conflict():
    # the buyer read the item's name, not its qty: nothing recorded says whether qty < 5 covered item 7
    a = analyze({}, restock_vs_buy("widget", "SELECT name FROM items WHERE id = $1"))
    [c] = a["conflicts"]
    assert c["confidence"] == "possible" and c["resource"] == "items[id=7]" and "qty < 5" in c["broad"]
    assert a["story"].startswith("Possible conflict.")


def test_range_checked_against_recorded_values():
    # the buyer read qty = 4, so "qty < 5" covered item 7: a certain lost update, found from the data
    a = analyze({}, restock_vs_buy(4))
    [c] = a["conflicts"]
    assert c["confidence"] == "certain" and c["resource"] == "items[id=7]" and c["pattern"] == "lost-update"
    assert "items WHERE qty < 5" in c["narrowed"] and "checked that condition" in a["story"]
    # qty = 9: the restock's range did not include item 7, so there is no conflict on it
    assert analyze({}, restock_vs_buy(9))["conflicts"] == []


def test_range_read_result_rows_name_the_rows_it_covered():
    rec = codec.encode([codec.FakeRecord(["id", "qty"], [7, 2]), codec.FakeRecord(["id", "qty"], [8, 1])])

    def events(listed):
        return [
            (ev.REQUEST, 1, "r0", "GET", "/low", [], b"", 0),
            (ev.REQUEST, 2, "r0", "POST", "/buy", [], b"", 1),
            *_io("q1", "SELECT id, qty FROM items WHERE qty < $1", [5], listed, 10),
            *_io("q2", "SELECT qty FROM items WHERE id = $1", [7], 2, 20),
            *_io("q1", "UPDATE items SET qty = qty + 10 WHERE id = $1", [7], "UPDATE 1", 30),
            *_io("q2", "UPDATE items SET qty = $1 WHERE id = $2", [1, 7], "UPDATE 1", 40),
            (ev.RESPONSE, 1, 200, b"", 50), (ev.RESPONSE, 2, 200, b"", 51),
        ]

    [c] = analyze({}, events(rec))["conflicts"]
    assert c["confidence"] == "certain" and c["resource"] == "items[id=7]" and c["read_a"].startswith("id=7")


def test_range_read_that_missed_a_row_inserted_later_is_check_then_act():
    # both requests look for a booking of room 5 on a date range, find none, and both insert one
    events = [(ev.REQUEST, 1, "r0", "POST", "/book", [], b"", 0), (ev.REQUEST, 2, "r0", "POST", "/book", [], b"", 1)]
    for i, (rid, bid) in enumerate(((1, 101), (2, 102))):
        events += _io(f"q{rid}", "SELECT id FROM bookings WHERE room = $1 AND day BETWEEN $2 AND $3",
                      [5, 10, 12], codec.encode([]), 10 + i)
    for i, (rid, bid) in enumerate(((1, 101), (2, 102))):
        events += _io(f"q{rid}", "INSERT INTO bookings (id, room, day) VALUES ($1, $2, $3)", [bid, 5, 11],
                      "INSERT 0 1", 30 + i)
    a = analyze({}, events)
    [c] = a["conflicts"]
    assert c["pattern"] == "check-then-act" and c["confidence"] == "certain"
    assert c["resource"] == "bookings WHERE room = 5 AND day BETWEEN 10 AND 12"
    assert "both found no rows" in a["story"] and "bookings[id=101]" in a["story"]

