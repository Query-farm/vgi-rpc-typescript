"""Run Python conformance tests against the TypeScript/Bun conformance worker."""
import contextlib
import os
import shutil
import subprocess
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.log import Message
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

_TS_DIR = os.path.dirname(os.path.abspath(__file__))
_BUNDLE_DIR = os.path.join(_TS_DIR, ".conformance-bundles")
BUN_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance.ts")]
BUN_HTTP_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http.ts")]
BUN_HTTP_ZSTD_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http-zstd.ts")]
BUN_HTTP_AUTH_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http-auth.ts")]
# Flechette variants — same source, different Arrow backend via Node's
# conditional resolution (workerd → impl-flechette, default → impl-arrowjs).
# Bun resolves the `imports` map in package.json by `--conditions`.
BUN_FLECHETTE_WORKER = ["bun", "--conditions=workerd", "run", os.path.join(_TS_DIR, "examples", "conformance.ts")]
BUN_FLECHETTE_HTTP_WORKER = [
    "bun",
    "--conditions=workerd",
    "run",
    os.path.join(_TS_DIR, "examples", "conformance-http.ts"),
]


def _start_http_server(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: float = 10.0,
) -> tuple[subprocess.Popen[bytes], int]:
    """Start an HTTP server subprocess and return (process, port)."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    assert proc.stdout is not None
    line = proc.stdout.readline().decode().strip()
    assert line.startswith("PORT:"), f"Expected PORT:<n>, got: {line!r}"
    port = int(line.split(":", 1)[1])

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/", timeout=1.0)
            break
        except (httpx.ConnectError, httpx.ConnectTimeout):
            time.sleep(0.1)
        except httpx.HTTPStatusError:
            break  # Server is up, just returned an error status

    return proc, port


def _bundle_for_runtime(entry: str, outfile: str) -> None:
    """Use bun build to create a self-contained JS bundle."""
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    subprocess.run(
        ["bun", "build", entry, "--outfile", outfile, "--target", "node", "--format", "esm"],
        check=True,
        capture_output=True,
    )


@pytest.fixture(scope="session")
def ts_transport() -> Iterator[SubprocessTransport]:
    transport = SubprocessTransport(BUN_WORKER)
    yield transport
    transport.close()


@pytest.fixture(scope="session")
def ts_flechette_transport() -> Iterator[SubprocessTransport]:
    """Stdio worker pinned to the flechette Arrow backend via --conditions=workerd."""
    transport = SubprocessTransport(BUN_FLECHETTE_WORKER)
    yield transport
    transport.close()


@pytest.fixture(scope="session")
def ts_http_port() -> Iterator[int]:
    """Start Bun conformance HTTP server."""
    proc, port = _start_http_server(BUN_HTTP_WORKER)
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def ts_flechette_http_port() -> Iterator[int]:
    """Start Bun conformance HTTP server pinned to the flechette Arrow backend."""
    proc, port = _start_http_server(BUN_FLECHETTE_HTTP_WORKER)
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_port(ts_http_port: int) -> int:
    """Alias used by the upstream TestHealth conformance suite."""
    return ts_http_port


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """Bun conformance HTTP server with reject-all authenticate, for TestHealth."""
    proc, port = _start_http_server(BUN_HTTP_AUTH_WORKER)
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_fake_storage() -> Iterator[str]:
    """Run the in-process Python fake-storage HTTP service."""
    from vgi_rpc.conformance.fake_storage import serve_in_thread

    base_url, shutdown = serve_in_thread()
    try:
        yield base_url
    finally:
        shutdown()


@pytest.fixture(scope="session")
def conformance_http_with_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun conformance HTTP server wired to the fake storage (no compression)."""
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--fake-storage", conformance_fake_storage])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_with_zstd_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun conformance HTTP server wired to the fake storage with zstd compression."""
    proc, port = _start_http_server(
        [*BUN_HTTP_WORKER, "--fake-storage", conformance_fake_storage, "--compression", "zstd"]
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_externalize_always_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun conformance HTTP server that externalizes EVERY non-empty response batch.

    Server-side externalization threshold is 1 byte (so every data-bearing
    batch flows through the upload-URL pointer mechanism), while the
    inline-request cap stays at 1 MiB so normal-sized client requests are
    not 413-rejected. Used as a transport variant in ``conformance_conn``
    so the entire conformance suite verifies that externalization is
    observationally indistinguishable from inline transmission.
    """
    proc, port = _start_http_server(
        [
            *BUN_HTTP_WORKER,
            "--fake-storage",
            conformance_fake_storage,
            "--externalize-threshold",
            "1",
            "--max-request-bytes",
            "1048576",
        ]
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def ts_http_zstd_port() -> Iterator[int]:
    """Start Bun conformance HTTP server with zstd response compression."""
    proc, port = _start_http_server(BUN_HTTP_ZSTD_WORKER)
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_strict_cap_port() -> Iterator[int]:
    """Bun conformance HTTP server with tight body + external caps for strict-fail tests.

    Mirrors Python's `tests/serve_conformance_http_strict.py`: 1 MiB cap on
    both inline and externalized responses so producer/unary/exchange tests
    that emit oversized payloads provably trip the strict-fail path.
    """
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--strict"])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def ts_node_http_port() -> Iterator[int]:
    """Start Node.js conformance HTTP server."""
    if not shutil.which("node"):
        pytest.skip("node not available")
    bundle = os.path.join(_BUNDLE_DIR, "conformance-http-node.js")
    _bundle_for_runtime(os.path.join(_TS_DIR, "examples", "conformance-http-node.ts"), bundle)
    proc, port = _start_http_server(["node", bundle])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def ts_node_http_zstd_port() -> Iterator[int]:
    """Start Node.js conformance HTTP server with zstd response compression."""
    if not shutil.which("node"):
        pytest.skip("node not available")
    bundle = os.path.join(_BUNDLE_DIR, "conformance-http-node.js")
    _bundle_for_runtime(os.path.join(_TS_DIR, "examples", "conformance-http-node.ts"), bundle)
    proc, port = _start_http_server(
        ["node", bundle],
        env={**os.environ, "VGI_COMPRESSION_LEVEL": "3"},
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def ts_deno_http_port() -> Iterator[int]:
    """Start Deno conformance HTTP server."""
    if not shutil.which("deno"):
        pytest.skip("deno not available")
    bundle = os.path.join(_BUNDLE_DIR, "conformance-http-deno.js")
    _bundle_for_runtime(os.path.join(_TS_DIR, "examples", "conformance-http-deno.ts"), bundle)
    proc, port = _start_http_server(["deno", "run", "--allow-all", bundle])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def ts_deno_http_zstd_port() -> Iterator[int]:
    """Start Deno conformance HTTP server with zstd response compression."""
    if not shutil.which("deno"):
        pytest.skip("deno not available")
    bundle = os.path.join(_BUNDLE_DIR, "conformance-http-deno.js")
    _bundle_for_runtime(os.path.join(_TS_DIR, "examples", "conformance-http-deno.ts"), bundle)
    proc, port = _start_http_server(
        ["deno", "run", "--allow-all", bundle],
        env={**os.environ, "VGI_COMPRESSION_LEVEL": "3"},
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


_DEFAULT_TRANSPORTS = [
    "pipe", "subprocess",
    "http", "http-zstd",
    "http_externalize_always",
    "node-http", "node-http-zstd",
    "deno-http", "deno-http-zstd",
]

# Flechette Arrow backend — same TS source, different `imports` condition.
# Opt-in via VGI_TEST_FLECHETTE=1 because the flechette backend currently
# has known wire-encoding gaps (list buffer layout, batch metadata
# attachment on zero-row batches, several wide-type serialization issues).
# Tracking issue: see TODO at top of src/arrow/impl-flechette/index.ts.
_TRANSPORTS = _DEFAULT_TRANSPORTS + (
    ["flechette-pipe", "flechette-http"] if os.environ.get("VGI_TEST_FLECHETTE") == "1" else []
)


@pytest.fixture(params=_TRANSPORTS)
def conformance_conn(
    request: pytest.FixtureRequest,
    ts_transport: SubprocessTransport,
    ts_http_port: int,
    ts_http_zstd_port: int,
) -> ConnFactory:
    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if request.param == "pipe":

            @contextlib.contextmanager
            def _pipe_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport(BUN_WORKER)
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()

            return _pipe_conn()
        elif request.param == "http":
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{ts_http_port}",
                on_log=on_log,
            )
        elif request.param == "http-zstd":
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{ts_http_zstd_port}",
                on_log=on_log,
                compression_level=3,
            )
        elif request.param == "http_externalize_always":
            from vgi_rpc.external import ExternalLocationConfig

            ext_port = request.getfixturevalue("conformance_http_externalize_always_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{ext_port}",
                on_log=on_log,
                # Server hands out http://127.0.0.1 download URLs from the
                # in-process fake storage; disable the HTTPS-only validator.
                external_location=ExternalLocationConfig(url_validator=None),
            )
        elif request.param == "node-http":
            port = request.getfixturevalue("ts_node_http_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
            )
        elif request.param == "node-http-zstd":
            port = request.getfixturevalue("ts_node_http_zstd_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
                compression_level=3,
            )
        elif request.param == "deno-http":
            port = request.getfixturevalue("ts_deno_http_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
            )
        elif request.param == "deno-http-zstd":
            port = request.getfixturevalue("ts_deno_http_zstd_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
                compression_level=3,
            )
        elif request.param == "flechette-pipe":

            @contextlib.contextmanager
            def _flechette_pipe_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport(BUN_FLECHETTE_WORKER)
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()

            return _flechette_pipe_conn()
        elif request.param == "flechette-http":
            port = request.getfixturevalue("ts_flechette_http_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
            )
        else:
            # "subprocess" — shared transport
            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, ts_transport, on_log)

            return _conn()

    return factory


# Import all test classes from the conformance pytest suite (shipped with the package)
from vgi_rpc.conformance._pytest_suite import *  # noqa: F401,F403,E402


from vgi_rpc.rpc import AnnotatedBatch, RpcError  # noqa: E402


# Override: allow TestLargeData on all transports (the upstream suite may
# skip non-pipe transports, but the TS worker handles them fine).
class TestLargeData(TestLargeData):  # type: ignore[no-redef]  # noqa: F811
    @pytest.fixture(autouse=True)
    def _skip_non_pipe(self) -> None:
        pass


# Override: the TS server drains client input after stream init errors, so
# these tests work on all transports (the upstream suite skips them).
class TestProducerStream(TestProducerStream):  # type: ignore[no-redef]  # noqa: F811
    def test_produce_error_on_init(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy, pytest.raises(RpcError, match="intentional init error"):
            list(proxy.produce_error_on_init())


class TestExchangeStream(TestExchangeStream):  # type: ignore[no-redef]  # noqa: F811
    def test_error_on_init(self, conformance_conn: ConnFactory) -> None:
        with conformance_conn() as proxy:
            with pytest.raises(RpcError, match="intentional exchange init error"):
                session = proxy.exchange_error_on_init()
                # HTTP raises during init; pipe/subprocess raises on first exchange.
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))


# The stdio worker's `IncrementalStream` (src/wire/writer.ts) uses arrow-js's
# `RecordBatchStreamWriter` directly because the exchange protocol is lockstep
# — the client reads each batch before sending the next input, so we can't
# buffer-then-emit. flechette has no equivalent streaming surface, so the
# stdio worker effectively requires the arrow-js backend. workerd/browser
# deployments use HTTP (no stdio), so this is fine in practice; mark the
# stdio-flechette stream tests xfail rather than re-implementing incremental
# encoding atop flechette.
_FLECHETTE_PIPE_STREAM_XFAIL_CLASSES = {
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
}


# Hook lives in conftest.py (next to this file) — pytest does not pick up
# `pytest_collection_modifyitems` defined inside a test module.
