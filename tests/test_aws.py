"""aiobotocore: a lost update on an S3 object, recorded against an in-process moto server, replayed after it stops."""

import asyncio
import json
import os

import pytest

pytest.importorskip("aiobotocore")
moto_server = pytest.importorskip("moto.server")
import httpx  # noqa: E402
from aiobotocore.session import get_session  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from fastapi import FastAPI  # noqa: E402

import deja  # noqa: E402
from deja import recording_loop  # noqa: E402
from deja.analysis import analyze  # noqa: E402
from deja.fileformat import read  # noqa: E402
from deja.replay import replay  # noqa: E402

CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test", "region_name": "us-east-1"}


def make_app(endpoint):
    app = FastAPI()
    readers = []  # both requests read before either writes (the first S3 call is slow, so make the race sure)

    def s3():
        return get_session().create_client("s3", endpoint_url=endpoint, **CREDS)

    @app.post("/like/{post}")
    async def like(post: str):
        async with s3() as c:
            obj = await c.get_object(Bucket="likes", Key=post)
            async with obj["Body"] as body:
                n = json.loads(await body.read())["n"]
            readers.append(post)
            while len(readers) < 2:
                await asyncio.sleep(0.01)
            await c.put_object(Bucket="likes", Key=post, Body=json.dumps({"n": n + 1}).encode())
        return {"n": n + 1}

    @app.get("/missing")
    async def missing():
        async with s3() as c:
            try:
                await c.get_object(Bucket="likes", Key="nope")
            except ClientError as e:
                return {"code": e.response["Error"]["Code"]}

    return app


def test_s3_lost_update_replays_without_the_server(tmp_path):
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    endpoint = f"http://127.0.0.1:{server._server.server_port}"
    out = {}

    async def main():
        async with get_session().create_client("s3", endpoint_url=endpoint, **CREDS) as c:
            await c.create_bucket(Bucket="likes")
            await c.put_object(Bucket="likes", Key="p1", Body=b'{"n": 0}')
        app = make_app(endpoint)
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            m = rec.mark("t")
            out["r"] = await asyncio.gather(client.post("/like/p1"), client.post("/like/p1"), client.get("/missing"))
            out["path"] = rec.dump(os.path.join(str(tmp_path), "s3.deja"), since=m)

    try:
        asyncio.run(main(), loop_factory=recording_loop)
    finally:
        server.stop()
    assert [x.json() for x in out["r"]] == [{"n": 1}, {"n": 1}, {"code": "NoSuchKey"}]
    c = analyze(*read(out["path"]))["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "s3 likes/p1", c
    r = replay(out["path"], lambda: make_app(endpoint), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()


def sync_app(endpoint):
    import botocore.session

    app = FastAPI()

    @app.get("/obj/{key}")
    def get_obj(key: str):  # boto3 / botocore in a worker thread
        c = botocore.session.get_session().create_client("s3", endpoint_url=endpoint, **CREDS)
        try:
            return {"body": c.get_object(Bucket="sync", Key=key)["Body"].read().decode()}
        except c.exceptions.NoSuchKey:
            return {"missing": key}

    return app


def test_sync_botocore_in_threads_replays_without_the_server(tmp_path):
    import botocore.session

    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    endpoint = f"http://127.0.0.1:{server._server.server_port}"
    c = botocore.session.get_session().create_client("s3", endpoint_url=endpoint, **CREDS)
    c.create_bucket(Bucket="sync")
    c.put_object(Bucket="sync", Key="k1", Body=b"hello")
    out = {}

    async def main():
        app = sync_app(endpoint)
        app.add_middleware(deja.DejaMiddleware, directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            m = rec.mark("t")
            out["r"] = await asyncio.gather(client.get("/obj/k1"), client.get("/obj/k2"))
            out["path"] = rec.dump(os.path.join(str(tmp_path), "sync.deja"), since=m)

    try:
        asyncio.run(main(), loop_factory=recording_loop)
    finally:
        server.stop()
    assert [x.json() for x in out["r"]] == [{"body": "hello"}, {"missing": "k2"}]
    r = replay(out["path"], lambda: sync_app(endpoint), deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
