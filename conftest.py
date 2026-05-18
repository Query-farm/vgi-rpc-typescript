# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""pytest collection hooks for the TS conformance suite.

Lives at the project root next to `test_ts_conformance.py` so the hooks
take effect automatically without `--rootdir` gymnastics.
"""

from __future__ import annotations

import pytest

# The stdio worker's `IncrementalStream` (src/wire/writer.ts) uses arrow-js's
# `RecordBatchStreamWriter` directly because the exchange protocol is lockstep
# — the client reads each batch before sending the next input, so we can't
# buffer-then-emit. flechette has no equivalent streaming surface, so the
# stdio worker effectively requires the arrow-js backend. workerd/browser
# deployments use HTTP (no stdio), so this is fine in practice; mark the
# stdio-flechette stream tests xfail rather than re-implementing incremental
# encoding atop flechette.
_FLECHETTE_PIPE_STREAM_XFAIL_CLASSES = frozenset(
    {
        "TestProducerStream",
        "TestProducerStreamWithHeader",
        "TestExchangeStream",
        "TestExchangeStreamWithHeader",
        "TestCancel",
        "TestExchangeCastCompatible",
        "TestErrorRecovery",
        "TestDynamicRichHeader",
        "TestDynamicSchemaProducer",
        "TestRichHeaderExchange",
        # TestLargeData exercises producer + exchange streams (200k+
        # row batches) — same IncrementalStream/arrow-js coupling.
        "TestLargeData",
    }
)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark flechette-pipe stream tests as xfail with a clear reason."""
    marker = pytest.mark.xfail(
        reason=(
            "stdio (`pipe`) worker uses arrow-js's RecordBatchStreamWriter for "
            "lockstep incremental writes — flechette has no equivalent surface. "
            "Workerd/browser deployments use HTTP, so this gap is intentional."
        ),
        strict=False,
        run=False,
    )
    for item in items:
        if "[flechette-pipe]" not in item.nodeid:
            continue
        # Walk up parents to find the test class name (pytest.Class).
        cls_name = ""
        node: object = item
        while node is not None:
            n = getattr(node, "name", "")
            if isinstance(n, str) and n.startswith("Test"):
                cls_name = n
                break
            node = getattr(node, "parent", None)
        if cls_name in _FLECHETTE_PIPE_STREAM_XFAIL_CLASSES:
            item.add_marker(marker)
