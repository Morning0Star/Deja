"""Déjà is plain ASGI: the same lost update recorded and replayed on Litestar, Quart and Django, with the shared state
found automatically in each framework's handlers."""

import asyncio
import os
import sys

import httpx
import pytest

import deja
from deja import recording_loop
from deja.analysis import analyze
from deja.fileformat import read
from deja.replay import replay


def litestar_app():
    from litestar import Litestar, post

    stock = {"widget": 1}

    @post("/buy")
    async def buy() -> dict:
        n = stock["widget"]
        await asyncio.sleep(0.01)
        stock["widget"] = n - 1
        return {"left": stock["widget"]}

    return Litestar([buy])


def quart_app():
    from quart import Quart

    app = Quart("shop")
    stock = {"widget": 1}

    @app.post("/buy")
    async def buy():
        n = stock["widget"]
        await asyncio.sleep(0.01)
        stock["widget"] = n - 1
        return {"left": stock["widget"]}

    return app


def django_app():
    import django
    from django.conf import settings

    if not settings.configured:
        settings.configure(ROOT_URLCONF="django_shop", ALLOWED_HOSTS=["*"], SECRET_KEY="test", DEBUG=False,
                           MIDDLEWARE=[], INSTALLED_APPS=[])
        django.setup()
    sys.modules.pop("django_shop", None)
    import django_shop  # a fresh stock for each app, like the factories above

    from django.core.asgi import get_asgi_application
    from django.urls import clear_url_caches

    clear_url_caches()
    return get_asgi_application()


FRAMEWORKS = {"litestar": litestar_app, "quart": quart_app, "django": django_app}


@pytest.mark.parametrize("name", list(FRAMEWORKS))
def test_lost_update_on_other_asgi_frameworks(tmp_path, name):
    pytest.importorskip(name)
    factory = FRAMEWORKS[name]
    path = os.path.join(str(tmp_path), f"{name}.deja")
    out = {}

    async def main():
        app = deja.DejaMiddleware(factory(), directory=str(tmp_path))
        rec = asyncio.get_running_loop().deja
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as client:
            marker = rec.mark("test")
            out["r"] = await asyncio.gather(client.post("/buy"), client.post("/buy"))
            rec.dump(path, since=marker)

    asyncio.run(main(), loop_factory=recording_loop)
    assert [r.json() for r in out["r"]] == [{"left": 0}, {"left": 0}], [r.text for r in out["r"]]
    c = analyze(*read(path))["conflicts"][0]
    assert c["pattern"] == "lost-update" and c["resource"] == "stock[widget]"
    r = replay(path, factory, deadline_s=5)
    assert r.followed and r.same_responses, r.summary()
