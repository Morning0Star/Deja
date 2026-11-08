"""Python versions Déjà has verified its event-loop hook against.

The recorder replaces ``BaseEventLoop._run_once`` with an instrumented copy. A wrong copy would silently produce bad
recordings, so recording is refused on versions that haven't been checked (architecture decision D7).
"""

import sys
import warnings

VERIFIED = {(3, 12), (3, 13), (3, 14), (3, 15)}


def supported() -> bool:
    return sys.version_info[:2] in VERIFIED


def warn_unsupported():
    warnings.warn(
        f"deja: Python {sys.version_info[0]}.{sys.version_info[1]} is not verified; running without recording "
        f"(verified: {', '.join('%d.%d' % v for v in sorted(VERIFIED))})",
        RuntimeWarning,
        stacklevel=3,
    )
