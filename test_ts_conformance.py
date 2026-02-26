"""Run Python conformance tests against the TypeScript/Bun conformance worker."""
import contextlib
import os
import sys
from collections.abc import Callable, Iterator
from typing import Any

import pytest

_VGI_RPC_PYTHON_PATH = os.environ.get(
    "VGI_RPC_PYTHON_PATH", "/Users/rusty/Development/vgi-rpc"
)
sys.path.insert(0, _VGI_RPC_PYTHON_PATH)

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.log import Message
from vgi_rpc.rpc import SubprocessTransport, _RpcProxy

_TS_DIR = os.path.dirname(os.path.abspath(__file__))
BUN_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance.ts")]


@pytest.fixture(scope="session")
def ts_transport() -> Iterator[SubprocessTransport]:
    transport = SubprocessTransport(BUN_WORKER)
    yield transport
    transport.close()


ConnFactory = Callable[..., contextlib.AbstractContextManager[Any]]


@pytest.fixture(params=["pipe", "subprocess"])
def conformance_conn(
    request: pytest.FixtureRequest,
    ts_transport: SubprocessTransport,
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
                # pipe/subprocess raises on first exchange
                session.exchange(AnnotatedBatch.from_pydict({"value": [1.0]}))
