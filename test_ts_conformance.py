"""Run Python conformance tests against the TypeScript/Bun conformance worker."""
import contextlib
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

_VGI_RPC_PYTHON_PATH = os.environ.get(
    "VGI_RPC_PYTHON_PATH", "/Users/rusty/Development/vgi-rpc"
)
sys.path.insert(0, _VGI_RPC_PYTHON_PATH)

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.log import Message
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

_TS_DIR = os.path.dirname(os.path.abspath(__file__))
_BUNDLE_DIR = os.path.join(_TS_DIR, ".conformance-bundles")
BUN_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance.ts")]
BUN_HTTP_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http.ts")]
BUN_HTTP_ZSTD_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http-zstd.ts")]


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
def ts_http_port() -> Iterator[int]:
    """Start Bun conformance HTTP server."""
    proc, port = _start_http_server(BUN_HTTP_WORKER)
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


@pytest.fixture(params=[
    "pipe", "subprocess",
    "http", "http-zstd",
    "node-http", "node-http-zstd",
    "deno-http", "deno-http-zstd",
])
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
        else:
            # "subprocess" — shared transport
            @contextlib.contextmanager
            def _conn() -> Iterator[_RpcProxy]:
                yield _RpcProxy(ConformanceService, ts_transport, on_log)

            return _conn()

    return factory


# Import all tests from the conformance test module
from tests.test_conformance import *  # noqa: F401,F403,E402


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
