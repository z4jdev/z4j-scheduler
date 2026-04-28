"""``python -m z4j_scheduler`` entry point.

Identical to invoking the ``z4j-scheduler`` console script
declared in ``pyproject.toml``. Useful when the script is not on
PATH (test environments, ad-hoc Docker shells).
"""

from __future__ import annotations

from z4j_scheduler.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
