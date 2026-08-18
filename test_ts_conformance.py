"""Run Python conformance tests against the TypeScript/Bun conformance worker."""
import contextlib
import os
import shutil
import subprocess
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

# vgi-rpc's `http` extra carries httpx2, not httpx — it has since 0.39. This
# file only ever got httpx because the old `>=0.37.0` floor happened to
# resolve 0.38.0, whose extra still named the older package. Raising the floor
# is what exposed it, so import what the extra actually installs and keep the
# fallback for anyone with an older vgi-rpc in their environment.
try:
    import httpx2 as httpx
except ModuleNotFoundError:  # pragma: no cover - pre-0.39 environments
    import httpx

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
BUN_HTTP_PROOF_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http-proof.ts")]
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
def conformance_http_no_compression_port() -> Iterator[int]:
    """Bun conformance HTTP server with response compression explicitly OFF.

    Response compression is on by default (zstd level 1), so every other HTTP
    fixture now advertises a non-empty ``VGI-Supported-Encodings``.  The
    present-but-empty advertisement — positively stating "I speak no
    compression", as distinct from an absent header meaning "legacy server,
    assume zstd" — is only reachable through an explicit
    ``compressionLevel: null``, which ``--response-compression off`` passes.

    The name is load-bearing: the shared suite's
    ``TestHttpCompressionNegotiationConformance::test_empty_advertisement_means_never_compressed``
    looks this fixture up by literal name via ``request.getfixturevalue`` and
    *skips* if it is absent, so a rename here silently stops testing the TS
    worker rather than failing.
    """
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--response-compression", "off"])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """Bun conformance HTTP server with reject-all authenticate, for TestHealth."""
    proc, port = _start_http_server(BUN_HTTP_AUTH_WORKER)
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_auth_reason_port() -> Iterator[int]:
    """Bun HTTP worker that honours ``X-Conformance-Auth-Reason``.

    Backs the shared ``TestUnauthorized`` reason-code tests. Membership in the
    closed set is not enough on its own — a server answering every 401 with
    ``unauthorized`` satisfies that. These tests prove the codes are
    *discriminated*, which is what makes them worth branching on.

    The reject-all worker already reads the header, so it serves double duty;
    it runs as a second process only because both fixtures are session-scoped.
    """
    proc, port = _start_http_server(BUN_HTTP_AUTH_WORKER)
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_cold_call_cache_port() -> Iterator[int]:
    """Bun conformance HTTP server booted with the call-state cache disabled.

    Backs the shared ``TestColdCallStateCache`` group, which pins the rule that
    a client echoes the call token on every continuation. With the cache warm
    the server resolves a call it already saw, so a client that never echoes
    still works — and only breaks once a continuation lands on a process with
    no cached entry. Disabling the cache makes every turn take that path.

    The fixture name is load-bearing: the shared suite looks it up with
    ``getfixturevalue`` and silently skips if it is missing.
    """
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--no-call-state-cache"])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_access_log(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[int, Path]]:
    """Bun conformance HTTP server writing JSONL access records, as ``(port, path)``.

    Backs the shared ``TestRequestId`` correlation case: asserting that the
    ``X-Request-ID`` on a response equals the ``request_id`` in the record
    means reading back what the server logged for a request the suite made,
    which nothing observable on the wire can substitute for.

    Its own process, because the plain worker deliberately runs with no access
    log at all — that is the configuration every other HTTP group is measured
    against.

    The fixture name is load-bearing: the shared suite looks it up with
    ``getfixturevalue`` and skips the correlation case if it is missing.
    """
    log_path = tmp_path_factory.mktemp("accesslog") / "conformance.jsonl"
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--access-log", str(log_path)])
    yield port, log_path
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_introspect_port() -> Iterator[int]:
    """Bun conformance HTTP server with token introspection enabled.

    Backs the shared ``TestTokenIntrospection`` group. It needs its own process
    because the endpoint is absent unless explicitly enabled — which
    ``TestTokenIntrospectionOffMode`` asserts against the plain worker.

    ``--introspect`` also turns on the ``X-Conformance-Principal`` authenticator,
    so the introspector allowlist has a caller identity to check. The resolver's
    fixed constants live in ``examples/conformance-http.ts`` and must match
    ``_INTROSPECTOR`` / ``_SUBJECT_TOKEN`` / ``_SUBJECT_PRINCIPAL`` /
    ``_JWS_TRAP_TOKEN`` in the shared suite.

    The fixture name is load-bearing — the suite looks it up with
    ``getfixturevalue`` and skips the whole group if it is missing.
    """
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--introspect"])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_cors_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun conformance HTTP server configured to allow the CORS test origin.

    Backs the shared ``TestCors`` group, which checks that a browser client can
    actually *read* the capability headers this worker advertises. It needs a
    second process because CORS is strictly opt-in: the plain worker must keep
    granting no origin at all, which is what ``TestCorsOffMode`` asserts.

    The fixture name is load-bearing — the suite looks it up with
    ``getfixturevalue`` and skips the whole group if it is missing — and so is
    the origin, which the suite hardcodes as its ``Origin`` request header.

    Storage mode is deliberate: the derived exposure check can only catch a
    missing entry for a header the worker actually advertises, so a *plain*
    worker here would silently skip the conditional half of the capability
    set -- the size caps and the upload-URL trio -- which are exactly the
    exposures a port is most likely to miss.
    """
    proc, port = _start_http_server(
        [
            *BUN_HTTP_WORKER,
            "--fake-storage",
            conformance_fake_storage,
            "--cors-origin",
            "https://conformance.example",
        ]
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Sticky failure-path fixtures (upstream TestSticky; see the reference repo's
# docs/sticky-sessions-spec.md §9.1)
# ---------------------------------------------------------------------------

# Shared AEAD key for the peer pair. Both workers can open each other's session
# tokens, which is the point: the rejection under test has to come from the
# server_id comparison, not from a decrypt failure.
_STICKY_PEER_TOKEN_KEY = "5f" * 32


@pytest.fixture(scope="session")
def conformance_http_sticky_short_ttl_port() -> Iterator[int]:
    """A sticky worker whose default session TTL is short enough to outwait.

    Backs ``TestSticky::test_expired_session_surfaces_session_lost``; the main
    worker's 300s default is not something a test can sit out.
    """
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--sticky-ttl", "1"])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_sticky_peer_ports() -> Iterator[tuple[int, int]]:
    """Two sticky workers sharing one AEAD key but reporting distinct server ids.

    Backs ``TestSticky::test_token_from_other_worker_rejected``. The worker
    otherwise hardcodes ``conformance-http`` as its server id, so without the
    explicit ``--server-id`` both peers would look like the same worker and the
    test would have nothing to reject.
    """
    proc_a, port_a = _start_http_server(
        [*BUN_HTTP_WORKER, "--token-key", _STICKY_PEER_TOKEN_KEY, "--server-id", "conformance-peer-a"]
    )
    proc_b, port_b = _start_http_server(
        [*BUN_HTTP_WORKER, "--token-key", _STICKY_PEER_TOKEN_KEY, "--server-id", "conformance-peer-b"]
    )
    try:
        yield port_a, port_b
    finally:
        for proc in (proc_a, proc_b):
            proc.terminate()
            proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_sticky_auth_port() -> Iterator[int]:
    """A sticky worker that authenticates the ``X-Conformance-Principal`` header.

    Backs ``TestSticky::test_cross_principal_replay_rejected``, which needs one
    worker reachable as two identities. Note this is the plain worker plus a
    flag, not ``BUN_HTTP_AUTH_WORKER`` — that one is reject-all and has no
    sticky sessions.
    """
    proc, port = _start_http_server([*BUN_HTTP_WORKER, "--sticky-auth"])
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def proof_worker_factory() -> Iterator[Callable[..., Any]]:
    """Spawn Bun workers gated on proxy proof, for the shared TestProxyProof group.

    The shared suite owns the matrix; this only has to know how to start one
    worker for a given configuration.
    """
    from vgi_rpc.conformance.proof_harness import ProofWorker, ProofWorkerConfig

    @contextlib.contextmanager
    def spawn(config: ProofWorkerConfig) -> Iterator[ProofWorker]:
        cmd = [
            *BUN_HTTP_PROOF_WORKER,
            "--proof-mode",
            config.mode,
            "--proof-origin-id",
            config.origin_id,
            "--proof-secrets",
            config.secrets,
            "--proof-skew",
            str(config.skew_seconds),
        ]
        if not config.replay_cache:
            cmd.append("--proof-no-replay-cache")
        proc, port = _start_http_server(cmd)
        try:
            # The Bun proof worker mounts under /vgi, mirroring the other ports.
            yield ProofWorker(port=port, prefix="/vgi", config=config)
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    yield spawn


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
def conformance_http_external_security_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun worker with independent external-fetch caps and per-hop URL policy."""
    proc, port = _start_http_server(
        [
            *BUN_HTTP_WORKER,
            "--fake-storage",
            conformance_fake_storage,
            "--max-request-bytes",
            "1048576",
            "--max-fetch-bytes",
            "4096",
            "--max-decompressed-fetch-bytes",
            "8192",
            "--reject-localhost-redirects",
        ]
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
def conformance_http_externalized_cap_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun conformance HTTP server whose *external-channel* cap is the one that bites.

    Backs the shared ``TestExternalizedResponseCap`` group.  Two settings make
    this fixture mean what it says:

    * ``--max-externalized-response-bytes`` is tight (64 KiB), so an
      externalised response overshoots it.
    * ``--max-response-bytes`` is deliberately *generous* (8 MiB).  An
      externalised payload leaves only a pointer batch on the wire, so the body
      cap must never be what fails here — with both tight the group would pass
      while proving nothing about the external channel.

    ``--externalize-threshold`` stays at the worker's 4 KiB default so a modest
    payload still externalises, which is what lets the under-cap control travel
    the same channel without tripping the cap.
    """
    proc, port = _start_http_server(
        [
            *BUN_HTTP_WORKER,
            "--fake-storage",
            conformance_fake_storage,
            "--max-externalized-response-bytes",
            str(64 * 1024),
            "--max-response-bytes",
            str(8 * 1024 * 1024),
        ]
    )
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


@pytest.fixture(params=_TRANSPORTS)
def conformance_describe(
    request: pytest.FixtureRequest,
    ts_transport: SubprocessTransport,
    ts_http_port: int,
    ts_http_zstd_port: int,
) -> "ServiceDescription":
    """Introspect the TS worker under test via a real ``__describe__`` call.

    Parallels ``conformance_conn`` (same transport matrix) so the upstream
    ``TestDescribeConformance`` suite validates ``__describe__`` over the wire
    against the actual Bun/Node/Deno worker rather than an in-process Python
    server.  The TS server enables describe by default.
    """
    from vgi_rpc.http import http_introspect
    from vgi_rpc.introspect import introspect

    param = request.param
    if param in ("pipe", "flechette-pipe"):
        cmd = BUN_FLECHETTE_WORKER if param == "flechette-pipe" else BUN_WORKER
        transport = SubprocessTransport(cmd)
        try:
            return introspect(transport)
        finally:
            transport.close()
    if param == "subprocess":
        return introspect(ts_transport)
    # Everything else is HTTP — resolve the right port for the variant.
    if param == "http":
        port = ts_http_port
    elif param == "http-zstd":
        port = ts_http_zstd_port
    elif param == "http_externalize_always":
        port = request.getfixturevalue("conformance_http_externalize_always_port")
    elif param == "node-http":
        port = request.getfixturevalue("ts_node_http_port")
    elif param == "node-http-zstd":
        port = request.getfixturevalue("ts_node_http_zstd_port")
    elif param == "deno-http":
        port = request.getfixturevalue("ts_deno_http_port")
    elif param == "deno-http-zstd":
        port = request.getfixturevalue("ts_deno_http_zstd_port")
    elif param == "flechette-http":
        port = request.getfixturevalue("ts_flechette_http_port")
    else:
        raise AssertionError(f"unhandled transport for conformance_describe: {param}")
    return http_introspect(base_url=f"http://127.0.0.1:{port}")


# Import all test classes from the conformance pytest suite (shipped with the package)
from vgi_rpc.conformance._pytest_suite import *  # noqa: F401,F403,E402

from vgi_rpc.introspect import ServiceDescription  # noqa: E402


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

# The repo-local ``TestResponseCompressionDisabled`` that used to live here was
# removed once the same ground landed centrally as
# ``TestHttpCompressionNegotiationConformance::test_empty_advertisement_means_never_compressed``
# (vgi-rpc-python be1a7a6), which now drives the ``--response-compression off``
# worker through the ``conformance_http_no_compression_port`` fixture above.
# A local copy would keep passing while this port drifted from the other four
# SDKs — precisely the failure the shared suite exists to catch.
