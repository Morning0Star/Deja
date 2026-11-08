"""Clock and date reads bound the ways apps bind them before Déjà starts (used by test_shims_asgi)."""

import datetime
from datetime import date
from datetime import datetime as dt
from time import time

from fastapi import FastAPI


def make_app():
    app = FastAPI()

    @app.get("/stamp")
    async def stamp():
        return {"dt": dt.now().isoformat(), "mod": datetime.datetime.utcnow().isoformat(), "day": date.today().isoformat(),
                "t": time(), "real": isinstance(datetime.datetime(2020, 1, 1), dt) and type(dt.now()) is type(datetime.datetime(2020, 1, 1))}

    return app
