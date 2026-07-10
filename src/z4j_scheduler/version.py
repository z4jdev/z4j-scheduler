"""Single source of truth for the package version.

Imported by ``__init__.py`` and surfaced by the ``z4j-scheduler
version`` CLI subcommand. Drift-proof: reports the installed wheel
version (tracks ``pyproject.toml`` automatically) and falls back to
the z4j-core protocol version for source checkouts with no installed
metadata. Previously this hardcoded the string, which drifted to
1.5.0 while the wheel shipped 1.6.7.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__: str = _pkg_version("z4j-scheduler")
except PackageNotFoundError:  # source checkout, no installed metadata
    from z4j_core.version import __version__  # type: ignore[no-redef]

__all__ = ["__version__"]
