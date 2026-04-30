"""Single source of truth for the package version.

Imported by ``__init__.py`` and surfaced by the ``z4j-scheduler
version`` CLI subcommand. Keep in sync with ``pyproject.toml``'s
``[project] version`` line on every release.
"""

from __future__ import annotations

__version__: str = "1.3.0"

__all__ = ["__version__"]
