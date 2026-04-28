"""Generated gRPC stubs from ``proto/scheduler.proto``.

Stubs are committed (not generated at install time) so the
``grpcio-tools`` toolchain is build-only. Regenerate via:

    cd packages/z4j-scheduler
    python scripts/regen_proto.py

The same .proto file is shared with z4j-brain (which generates its
own copy of the stubs into ``backend/src/z4j_brain/scheduler_grpc/proto/``).
Both copies must be regenerated together when the proto changes -
the regen script handles both sides automatically.

Generated modules re-exported for ergonomic imports:

- :mod:`scheduler_pb2` - message classes
- :mod:`scheduler_pb2_grpc` - service stubs (server + client)

The generated files have one hand-edited line: the relative
``import scheduler_pb2`` in ``scheduler_pb2_grpc.py`` is rewritten
to ``from z4j_scheduler.proto import scheduler_pb2`` so the stubs
can be imported as a package. The regen script reapplies this fix
automatically.
"""

from __future__ import annotations

from z4j_scheduler.proto import scheduler_pb2, scheduler_pb2_grpc

__all__ = ["scheduler_pb2", "scheduler_pb2_grpc"]
