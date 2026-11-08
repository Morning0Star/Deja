"""Blocking clients in worker threads and handlers: requests, sync httpx, boto3. Recorded under the request
(and its thread), replayed with the network gone."""

import asyncio
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest
from fastapi import FastAPI

requests = pytest.importorskip("requests")

import deja  # noqa: E402
from deja import recording_loop  # noqa: E402
from deja.replay import replay  # noqa: E402


def start_upstream():
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            body = f'{{"path": "{self.path}"}}'.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def make_app(base):
    app = FastAPI()

    @app.get("/sync")
    def sync_endpoint():  # a worker thread
        a = requests.get(f"{base}/a", timeout=5).json()["path"]
        with httpx.Client() as c:
            b = c.get(f"{base}/b").json()["path"]
        return {"a": a, "b": b}

    @app.get("/blocking")
    async def blocking():  # a blocking call right in an async handler (bad practice, but common)
        return {"c": requests.get(f"{base}/c", timeout=5).json()["path"]}

    return app


def test_sync_clients_replay_without_the_network(tmp_path):
    srv = start_upstream()
    base = f"http://127.0.0.1:{srv.server_port}"
    out = {}

    async def main():
        app = make_app(base)
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            m = rec.mark("t")
            out["r"] = await asyncio.gather(client.get("/sync"), client.get("/blocking"))
            out["path"] = rec.dump(os.path.join(str(tmp_path), "sync.deja"), since=m)

    asyncio.run(main(), loop_factory=recording_loop)
    srv.shutdown()
    srv.server_close()
    assert [x.json() for x in out["r"]] == [{"a": "/a", "b": "/b"}, {"c": "/c"}]
    r = replay(out["path"], lambda: make_app(base), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
