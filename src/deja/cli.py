"""Command-line entry point. Commands arrive with milestones M1 (record) and M2 (replay)."""

import sys

from . import __version__


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in ("-V", "--version"):
        print(f"deja {__version__}")
        return 0
    print("deja: nothing to run yet. See docs/DESIGN.md for the plan.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
