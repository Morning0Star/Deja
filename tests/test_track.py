import pytest

import deja
from deja import codec, events as ev
from deja.loop import recording_loop
from deja.track import TrackedDict


def touches_from(fn):
    """Run fn() inside a fake request on a recording loop; return the (resource, kind, value) touches it made."""
    loop = recording_loop()
    try:
        rec = loop.deja

        async def main():
            rid = rec.request_start("POST", "/x", [], b"")
            try:
                fn()
            finally:
                rec.request_end(rid, 200, b"")

        loop.run_until_complete(main())
        return [(e[2], e[3], e[4]) for e in rec.events() if e[0] == ev.TOUCH]
    finally:
        loop.close()


def test_dict_reports_per_key():
    d = deja.track({"a": 1}, "carts")

    def work():
        assert "a" in d and d["a"] == 1 and d.get("b") is None
        d["b"] = 2
        d.setdefault("c", 3)
        d.pop("a")

    got = touches_from(work)
    assert ("carts[a]", "read", True) in got and ("carts[a]", "read", 1) in got
    assert ("carts[b]", "read", None) in got and ("carts[b]", "write", 2) in got
    assert ("carts[c]", "write", 3) in got and ("carts[a]", "write", None) in got
    assert isinstance(d, TrackedDict) and d == {"b": 2, "c": 3}


def test_list_and_set_membership_with_key_name():
    users = deja.track([], "users", key="username")
    tags = deja.track(set(), "tags")

    def work():
        assert "ada" not in users
        users.append("ada")
        tags.add("x")
        tags.discard("x")

    got = touches_from(work)
    assert got[:2] == [("users[username=ada]", "read", False), ("users[username=ada]", "write", "ada")]
    assert ("tags[x]", "write", "x") in got and ("tags[x]", "write", None) in got


def test_object_attributes_tracked_in_place():
    class Wallet:
        def __init__(self):
            self.balance = 100
            self._secret = 1

        def spend(self, n):
            self.balance -= n

    w = Wallet()
    assert deja.track(w, "wallet") is w and type(w).__name__ == "Wallet" and isinstance(w, Wallet)
    got = touches_from(lambda: w.spend(30))
    assert got == [("wallet.balance", "read", 100), ("wallet.balance", "write", 70)]
    assert touches_from(lambda: w._secret) == []


def test_untracked_outside_a_request_and_rejects_slots():
    d = deja.track({}, "d")
    d["k"] = 1  # no running loop: a no-op, no error

    class S:
        __slots__ = ("x",)

    with pytest.raises(TypeError):
        deja.track(S(), "s")



def test_iteration_and_len_report_a_whole_container_read():
    d = deja.track({"a": 1, "b": 2}, "carts")
    users = deja.track(["ada"], "users", key="username")

    def work():
        assert sorted(d) == ["a", "b"] and len(d) == 2 and list(d.items()) == [("a", 1), ("b", 2)]
        assert users[0] == "ada" and users
        assert dict(d) == {"a": 1, "b": 2}

    got = touches_from(work)
    got = [(r, k, codec.decode(v)) for r, k, v in got]
    assert ("carts[*]", "read", ["a", "b"]) in got and ("users[username=*]", "read", ["ada"]) in got
    import copy
    assert type(copy.copy(d)) is dict and type(copy.deepcopy(users)) is list
