# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Python shim that drives this port's client for conformance.

Everything port-agnostic lives in ``vgi_rpc.conformance.client_driver`` in the
Python reference, and the control protocol it speaks is written down in that
repository's ``tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md``.  What is
left here is the only TypeScript-shaped part: where this repository's driver
lives, and that Bun runs it.

``ClientDriver`` takes argv and splits ``VGI_CLIENT_DRIVER`` with
``shlex.split``, so ``bun run conformance/client-driver.ts`` needs no wrapper
script.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from vgi_rpc.conformance.client_driver import ClientDriver, ClientDriverProxy
from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.log import Message

# ``VGI_CLIENT_DRIVER`` wins when set (CI sets it); this is the
# developer-machine fallback.
_DEFAULT_DRIVER = ["bun", "run", str(Path(__file__).parent / "conformance" / "client-driver.ts")]

DRIVER = ClientDriver.from_env(default=_DEFAULT_DRIVER)


def TsClientProxy(  # noqa: N802 - class-like name, matching the other ports' shims
    transport: str,
    target: object,
    on_log: Callable[[Message], None] | None = None,
    *,
    external_config: ExternalLocationConfig | None = None,
    compression_level: int | None = 1,
    headers: Mapping[str, str] | None = None,
) -> ClientDriverProxy:
    """Open one driver-backed connection."""
    return DRIVER.connect(
        transport,
        target,
        on_log,
        external_config=external_config,
        compression_level=compression_level,
        headers=headers,
    )


ts_http_connect = DRIVER.http_connect
ts_http_capabilities = DRIVER.http_capabilities
ts_request_upload_urls = DRIVER.request_upload_urls
ts_http_introspect = DRIVER.http_introspect
