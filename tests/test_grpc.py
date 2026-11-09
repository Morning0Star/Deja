"""grpc.aio client calls: recorded against an in-process server, replayed with the server gone (errors too)."""

import asyncio
import os

import pytest

pytest.importorskip("grpc")
import grpc  # noqa: E402
import grpc.aio  # noqa: E402,F401
import httpx  # noqa: E402
from fastapi import FastAPI  # noqa: E402

import deja  # noqa: E402
from deja import recording_loop  # noqa: E402
from deja.replay import replay  # noqa: E402

STOCK = {b"tea": 3}


async def start_server():
    async def get_stock(request, context):
        if request not in STOCK:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"no {request.decode()}")
        return str(STOCK[request]).encode()

    async def watch(request, context):
        for i in range(3):
            yield f"{request.decode()}:{i}".encode()

    handler = grpc.method_handlers_generic_handler(
        "inventory.Stock",
        {
            "GetStock": grpc.unary_unary_rpc_method_handler(get_stock),
            "Watch": grpc.unary_stream_rpc_method_handler(watch),
        },
    )
    server = grpc.aio.server()
    server.add_generic_rpc_handlers((handler,))
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, port


def make_app(port_box):
    app = FastAPI()

    @app.get("/stock/{item}")
    async def stock(item: str):
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port_box[0]}") as ch:
            get = ch.unary_unary("/inventory.Stock/GetStock")
            watch = ch.unary_stream("/inventory.Stock/Watch")
            try:
                n = int(await get(item.encode()))
            except grpc.aio.AioRpcError as e:
                return {"error": e.code().name, "details": e.details()}
            events = [m.decode() async for m in watch(item.encode())]
        return {"n": n, "events": events}

    return app


def test_grpc_calls_replay_without_the_server(tmp_path):
    out, port_box = {}, []

    async def main():
        server, port = await start_server()
        port_box.append(port)
        app = make_app(port_box)
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as c:
            m = rec.mark("t")
            out["r"] = await asyncio.gather(c.get("/stock/tea"), c.get("/stock/coffee"))
            out["path"] = rec.dump(os.path.join(str(tmp_path), "g.deja"), since=m)
        await server.stop(None)

    asyncio.run(main(), loop_factory=recording_loop)
    assert out["r"][0].json() == {"n": 3, "events": ["tea:0", "tea:1", "tea:2"]}
    assert out["r"][1].json() == {"error": "NOT_FOUND", "details": "no coffee"}
    r = replay(out["path"], lambda: make_app(port_box), deadline_s=5)  # the server is stopped
    assert r.followed and r.same_responses, r.summary()
