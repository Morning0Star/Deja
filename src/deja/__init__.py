"""Déjà: record an asyncio app's real scheduling order and replay its races deterministically.

Recording (M1):

    import deja
    app.add_middleware(deja.DejaMiddleware, directory="deja-recordings")
    # uvicorn app:app --loop deja.loop:recording_loop
    # or: asyncio.run(main(), loop_factory=deja.recording_loop)

    deja.flag("balance went negative")   # save the current request's causal window
    deja.track(store, "wallet")           # show reads/writes of in-memory state in the report
"""

__version__ = "0.0.2"

from .annotate import calls, labels, touch  # noqa: E402
from .asgi import DejaMiddleware, flag  # noqa: E402
from .loop import recording_loop  # noqa: E402
from .scope import scope  # noqa: E402
from .recorder import Recorder  # noqa: E402
from .track import track  # noqa: E402

__all__ = ["DejaMiddleware", "Recorder", "calls", "flag", "labels", "recording_loop", "scope", "touch", "track",
           "__version__"]
