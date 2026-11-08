"""deja.scope: jobs from a queue consumer race on shared state; the race is found and replayed via handlers."""

import asyncio
import os

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay


def make_handlers():
    stock = {"widget": 1}

    async def reserve(order):
        n = stock["widget"]
        await asyncio.sleep(0.01)
        if n >= order["qty"]:
            stock["widget"] = n - order["qty"]
            return {"reserved": order["id"]}
        return {"reserved": None}

    return {"orders": reserve}


def test_jobs_race_found_and_replayed(tmp_path):
    out = {}

    async def main():
        handlers = make_handlers()
        queue, jobs = asyncio.Queue(), []
        rec = asyncio.get_running_loop().deja

        async def worker():
            while True:
                order = await queue.get()

                async def one(order=order):
                    async with deja.scope("orders", order, directory=str(tmp_path)) as job:
                        job.result = await handlers["orders"](order)

                jobs.append(asyncio.create_task(one()))

        w = asyncio.create_task(worker())
        marker = rec.mark("t")
        for oid in ("o1", "o2"):
            await queue.put({"id": oid, "qty": 1})
        while len(jobs) < 2:
            await asyncio.sleep(0.01)
        await asyncio.gather(*jobs)
        out["path"] = rec.dump(os.path.join(str(tmp_path), "jobs.deja"), since=marker)
        w.cancel()

    asyncio.run(main(), loop_factory=recording_loop)
    a = analyze(*read(out["path"]))
    c = a["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]", a["story"]
    assert "orders jobs" in a["story"]
    r = replay(out["path"], make_handlers, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
    assert sorted(v[1] for v in r.replayed.values()) == [b'{"__t": "dict", "v": [["reserved", "o1"]]}',
                                                         b'{"__t": "dict", "v": [["reserved", "o2"]]}']
