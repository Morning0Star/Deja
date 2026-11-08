"""anyio worker threads (``anyio.to_thread.run_sync``): see :mod:`deja.threads`."""

import anyio  # noqa: F401  (ImportError here means anyio is not installed: the adapter is skipped)

from ..threads import install, uninstall  # noqa: F401
