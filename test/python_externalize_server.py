# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Test harness: boot a Python conformance server with upload-URL support.

Spawned by `test/client-externalize.test.ts` to drive the TS client's
request externalization against the canonical Python implementation.

Args:
    --max-request-bytes N : advertised inline request cap (default 4096).

Prints:
    PORT:<n>             : the chosen TCP port (machine-readable).
"""
from __future__ import annotations

import argparse
import socket
import sys
import threading

import waitress

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.conformance.fake_storage import FakeStorageBackend, serve_in_thread
from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.http import make_wsgi_app
from vgi_rpc.rpc import RpcServer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-request-bytes", type=int, default=4096)
    parser.add_argument(
        "--externalize-threshold",
        type=int,
        default=1_048_576,
        help="Response batch byte threshold for server-side externalization (default 1 MiB).",
    )
    args = parser.parse_args()

    storage_url, _shutdown = serve_in_thread()
    storage = FakeStorageBackend(storage_url)

    impl = ConformanceServiceImpl()
    # Server resolves inbound externalized request batches by fetching from
    # the embedded URL — this is the receiving half of the upload-URL flow.
    external_config = ExternalLocationConfig(
        storage=storage,
        externalize_threshold_bytes=args.externalize_threshold,
        url_validator=None,
    )
    server = RpcServer(
        ConformanceService,
        impl,
        enable_describe=True,
        external_location=external_config,
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = int(s.getsockname()[1])

    app = make_wsgi_app(
        server,
        upload_url_provider=storage,
        max_request_bytes=args.max_request_bytes,
        max_upload_bytes=64 * 1024 * 1024,
    )
    print(f"PORT:{port}", flush=True)

    # Run waitress in a thread so we can keep main alive on a sentinel.
    thread = threading.Thread(
        target=lambda: waitress.serve(app, host="127.0.0.1", port=port, _quiet=True),
        daemon=True,
    )
    thread.start()
    # Block on stdin so the parent can stop us by closing the pipe.
    try:
        sys.stdin.read()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
