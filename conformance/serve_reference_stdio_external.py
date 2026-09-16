# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Serve the reference conformance service over stdio, externalizing everything.

The ``vgi-rpc-conformance`` CLI grows no storage flag on ``--pipe``, and the
reference's ``client_worker.py`` refuses ``--external`` on a raw transport
outright — so a client-role run against the reference peer has no externalizing
byte-stream server to point ``TestExternalByteStream`` at. ``RpcServer`` takes
``external_location`` directly, which is all this needs: fifteen lines here buy
the leg that matters most.

Why it matters more than the TypeScript-vs-TypeScript leg: this port's server
externalizes one batch per object, so its own payloads are single-batch by
construction and the resolver's multi-batch path never runs. The reference
uploads the whole cycle — the turn's log batches *followed by* its data batch —
which is the shape ``docs/WIRE_PROTOCOL.md`` §12 describes and the shape that
has already cost two ports their logs.

Usage: ``serve_reference_stdio_external.py <fake-storage-base-url> [threshold]``
"""

from __future__ import annotations

import sys

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.conformance.fake_storage import FakeStorageBackend
from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.rpc import RpcServer, serve_stdio


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(f"usage: {sys.argv[0]} <fake-storage-base-url> [externalize-threshold-bytes]")
    storage_url = sys.argv[1]
    threshold = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    server = RpcServer(
        ConformanceService,
        ConformanceServiceImpl(),
        enable_describe=True,
        external_location=ExternalLocationConfig(
            storage=FakeStorageBackend(storage_url),
            externalize_threshold_bytes=threshold,
            # The fake store vends http://127.0.0.1 URLs, which the default
            # HTTPS-only validator would (correctly) refuse.
            url_validator=None,
        ),
    )
    serve_stdio(server)


if __name__ == "__main__":
    main()
