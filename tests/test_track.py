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


def test_defaultdict_counter_ordereddict_keep_their_behaviour():
    import collections
    import copy
    import pickle

    holds = deja.track(collections.defaultdict(list), "holds")
    sales = deja.track(collections.Counter(), "sales")
    lru = deja.track(collections.OrderedDict(a=1, b=2), "lru")

    def work():
        holds["x"].append(1)  # missing key: default_factory runs, and it is reported
        sales["y"] += 2
        sales.update(["y", "z"])
        lru.move_to_end("a")
        lru.popitem(last=False)

    got = touches_from(work)
    assert ("holds[x]", "write", codec.encode([1])) in got or any(r == "holds[x]" and k == "write" for r, k, _ in got)
    assert ("sales[y]", "read", 0) in got and ("sales[y]", "write", 2) in got and ("sales[y]", "write", 3) in got
    assert ("sales[z]", "write", 1) in got
    assert ("lru[a]", "write", "(moved)") in got and ("lru[b]", "write", None) in got
    assert holds["x"] == [1] and holds["new"] == [] and sales == {"y": 3, "z": 1} and list(lru) == ["a"]
    # copies and pickles are plain containers of the original type
    for obj, base in ((holds, collections.defaultdict), (sales, collections.Counter), (lru, collections.OrderedDict)):
        for c in (copy.copy(obj), obj.copy(), pickle.loads(pickle.dumps(obj))):
            assert type(c) is base and c == obj


def test_deque_list_and_set_mutations():
    import collections

    jobs = deja.track(collections.deque(maxlen=5), "jobs")
    seats = deja.track([], "seats")
    tags = deja.track({"a"}, "tags")

    def work():
        nonlocal tags
        jobs.append("j1")
        jobs.appendleft("j0")
        assert jobs.popleft() == "j0"
        seats.extend(["s1", "s2"])
        seats.insert(0, "s0")
        assert seats.pop() == "s2"
        seats[0] = "z"
        tags |= {"b"}
        tags.update(["c"])
        tags -= {"a"}
        with pytest.raises(KeyError):
            tags.remove("missing")
        tags.clear()

    got = touches_from(work)
    assert ("jobs[j1]", "write", "j1") in got and ("jobs[j0]", "write", None) in got
    assert [g for g in got if g[0].startswith("seats[s")] == [
        ("seats[s1]", "write", "s1"), ("seats[s2]", "write", "s2"), ("seats[s0]", "write", "s0"),
        ("seats[s2]", "write", None)]
    assert ("seats[*]", "write", None) in got  # assignment by index
    assert ("tags[b]", "write", "b") in got and ("tags[c]", "write", "c") in got and ("tags[a]", "write", None) in got
    assert ("tags[missing]", "read", False) in got and ("tags[*]", "write", None) in got
    assert type(jobs.copy()) is collections.deque and jobs.maxlen == 5 and list(jobs) == ["j1"]
    assert seats == ["z", "s1"] and tags == set()


def test_failed_lookup_is_a_read_of_nothing():
    d = deja.track({}, "d")

    def work():
        with pytest.raises(KeyError):
            d["k"]

    assert touches_from(work) == [("d[k]", "read", None)]
