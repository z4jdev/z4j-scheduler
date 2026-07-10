"""Regenerate gRPC stubs from ``proto/scheduler.proto``.

Run from the package root:

    cd packages/z4j-scheduler
    python scripts/regen_proto.py

This script:

1. Compiles the .proto file via ``grpc_tools.protoc`` into both:
   - ``packages/z4j-scheduler/src/z4j_scheduler/proto/``
   - ``packages/z4j/backend/src/z4j_brain/scheduler_grpc/proto/``
   so brain and scheduler always stay in sync on the wire shape.

2. Rewrites the relative ``import scheduler_pb2`` line that
   ``grpcio-tools`` emits into a package-qualified import:
   - In z4j-scheduler:
     ``from z4j_scheduler.proto import scheduler_pb2``
   - In z4j-brain:
     ``from z4j_brain.scheduler_grpc.proto import scheduler_pb2``

3. Writes ``__init__.py`` re-exports if missing.

Requires ``grpcio-tools`` installed in the active env. The package's
dev extra includes it; if you're running outside that env, install
ad-hoc via ``pip install grpcio-tools``.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
MONOREPO_ROOT = PACKAGE_ROOT.parent.parent
PROTO_FILE = PACKAGE_ROOT / "proto" / "scheduler.proto"

SCHEDULER_OUT = PACKAGE_ROOT / "src" / "z4j_scheduler" / "proto"
# The brain backend lives under packages/z4j/backend (it was
# packages/z4j-brain pre-1.4 consolidation); keep both stub copies in
# sync so the wire shape never drifts between scheduler and brain.
BRAIN_OUT = (
    MONOREPO_ROOT
    / "packages"
    / "z4j"
    / "backend"
    / "src"
    / "z4j_brain"
    / "scheduler_grpc"
    / "proto"
)


def _run_protoc(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - inputs are package-internal paths
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{PACKAGE_ROOT / 'proto'}",
            f"--python_out={out_dir}",
            f"--grpc_python_out={out_dir}",
            f"--pyi_out={out_dir}",
            str(PROTO_FILE),
        ],
        check=True,
    )


def _rewrite_grpc_import(out_dir: Path, package_path: str) -> None:
    """Rewrite ``import scheduler_pb2`` to package-qualified import."""
    grpc_file = out_dir / "scheduler_pb2_grpc.py"
    text = grpc_file.read_text(encoding="utf-8")
    needle = "import scheduler_pb2 as scheduler__pb2"
    replacement = f"from {package_path} import scheduler_pb2 as scheduler__pb2"
    if needle not in text:
        # Already rewritten or grpcio-tools changed format; warn loudly
        # so future-us catches the regen drift instead of silently
        # producing broken stubs.
        print(  # noqa: T201
            f"WARNING: expected import line not found in {grpc_file}; manual review needed",
            file=sys.stderr,
        )
        return
    grpc_file.write_text(text.replace(needle, replacement), encoding="utf-8")


def _ensure_init(out_dir: Path, doc: str) -> None:
    """Create __init__.py if missing. Preserves existing content."""
    init = out_dir / "__init__.py"
    if init.exists() and init.read_text(encoding="utf-8").strip():
        return
    init.write_text(doc, encoding="utf-8")


def main() -> int:
    if not PROTO_FILE.exists():
        print(f"proto file missing: {PROTO_FILE}", file=sys.stderr)  # noqa: T201
        return 1

    print(f"compiling {PROTO_FILE.name} -> {SCHEDULER_OUT}")  # noqa: T201
    _run_protoc(SCHEDULER_OUT)
    _rewrite_grpc_import(SCHEDULER_OUT, "z4j_scheduler.proto")

    if BRAIN_OUT.parent.exists():
        # Brain side may not exist in standalone scheduler checkouts;
        # only regenerate if the brain package is present.
        print(f"compiling {PROTO_FILE.name} -> {BRAIN_OUT}")  # noqa: T201
        _run_protoc(BRAIN_OUT)
        _rewrite_grpc_import(BRAIN_OUT, "z4j_brain.scheduler_grpc.proto")
    else:
        print(  # noqa: T201
            f"skipping brain stubs (path not present: {BRAIN_OUT.parent})",
        )

    print("done")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())


# Keep ``shutil`` imported even though it's currently unused - planned
# follow-up adds backup-then-replace semantics for the regen so a
# failed protoc run does not leave half-written stubs on disk.
_ = shutil
