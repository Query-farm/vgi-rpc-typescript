"""Run Python conformance tests against the TypeScript/Bun conformance worker."""
import contextlib
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Protocol

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

# --- Which half is under test, and against what -----------------------------
#
# ROLE "server" (the default) is the original arrangement: the Python reference
# *client* drives this port's server.
#
# ROLE "client" is the other direction — this port's client, driven through the
# JSONL driver in `conformance/client-driver.ts`. It is the direction nothing
# tested until now: `httpConnect`/`pipeConnect` had only ever run against this
# repository's own server, which accepts both bare and namespaced request
# paths. A permissive server cannot validate a client, and the same blindness
# let a sibling port ship a client sending bare paths for weeks — green at
# home, 730 failures against the reference.
#
# SERVER picks the peer. `VGI_CONFORMANCE_SERVER=python` is therefore the gate
# that counts in client role; `typescript` is a regression check whose value is
# in *localising* a failure when the two disagree, not in proving conformance.
ROLE = os.environ.get("VGI_CONFORMANCE_ROLE", "server")
SERVER = os.environ.get("VGI_CONFORMANCE_SERVER", "typescript")

if ROLE == "client" and SERVER not in ("typescript", "python"):
    raise RuntimeError(f"VGI_CONFORMANCE_SERVER={SERVER!r} is not one of 'typescript', 'python'")
if ROLE == "server" and SERVER != "typescript":
    raise RuntimeError("VGI_CONFORMANCE_SERVER only applies in client role")

_TS_DIR = os.path.dirname(os.path.abspath(__file__))
_BUNDLE_DIR = os.path.join(_TS_DIR, ".conformance-bundles")
BUN_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance.ts")]
BUN_HTTP_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http.ts")]
BUN_HTTP_ZSTD_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http-zstd.ts")]
BUN_HTTP_AUTH_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http-auth.ts")]
BUN_HTTP_PROOF_WORKER = ["bun", "run", os.path.join(_TS_DIR, "examples", "conformance-http-proof.ts")]
BUN_TRANSPORT_KIND_WORKER = [
    "bun",
    "run",
    os.path.join(_TS_DIR, "examples", "conformance-transport-kind.ts"),
]
# The `vgi_rpc.Identity.v1` fixture worker. One binary, two configurations,
# selected by `--identity`; see IDENTITY_CONFORMANCE_FIXTURE.md §1. Kept out of
# BUN_HTTP_WORKER on purpose — the shared group asserts against the *plain*
# worker that a deployment configuring no hook hosts no identity protocol at
# all, and that property dies the moment the plain worker configures one.
BUN_HTTP_IDENTITY_WORKER = [
    "bun",
    "run",
    os.path.join(_TS_DIR, "examples", "conformance-http-identity.ts"),
]
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


# --- The reference peer -----------------------------------------------------
#
# Only needed in client role with SERVER=python. The *scripts* live in the
# reference repository (not in its wheel), so a client run against the
# reference needs a checkout of it as well as the installed package.
#
#   VGI_RPC_PYTHON_REPO  checkout root of vgi-rpc-python
#   VGI_RPC_PYTHON_BIN   interpreter that has vgi_rpc importable
#   VGI_RPC_CONFORMANCE_CLI  the `vgi-rpc-conformance` entry point
_REF_REPO = Path(os.environ.get("VGI_RPC_PYTHON_REPO") or Path.home() / "Development" / "vgi-rpc-python")
# Default to the interpreter running this suite: it is the one that already
# imports `vgi_rpc`, which is exactly what the serve scripts need. CI installs
# the reference into that same interpreter.
_REF_PY = os.environ.get("VGI_RPC_PYTHON_BIN") or sys.executable
_PY_TESTS = Path(os.environ.get("VGI_PY_TESTS_DIR") or _REF_REPO / "tests")
_PY_SERVE_HTTP = str(_PY_TESTS / "serve_conformance_http.py")
_PY_SERVE_STRICT = str(_PY_TESTS / "serve_conformance_http_strict.py")
_PY_SERVE_AUTH = str(_PY_TESTS / "serve_conformance_http_auth.py")
_PY_SERVE_PROOF = str(_PY_TESTS / "serve_conformance_http_proof.py")


def _ref_conformance_cli() -> str:
    """Resolve the reference `vgi-rpc-conformance` entry point."""
    explicit = os.environ.get("VGI_RPC_CONFORMANCE_CLI")
    if explicit:
        return explicit
    vendored = _REF_REPO / ".venv" / "bin" / "vgi-rpc-conformance"
    if vendored.exists():
        return str(vendored)
    found = shutil.which("vgi-rpc-conformance")
    if found:
        return found
    raise RuntimeError(
        "vgi-rpc-conformance is not on PATH and no VGI_RPC_CONFORMANCE_CLI was set; "
        "a client run against the reference server needs it"
    )


def _stdio_worker_cmd() -> list[str]:
    """The argv the driver spawns for a byte-stream connection."""
    if SERVER == "python":
        return [_ref_conformance_cli(), "--pipe", "--describe"]
    return BUN_WORKER


def _free_port() -> int:
    """Reserve a loopback port for a server that cannot report its own."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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


def _start_variant(
    variant: str,
    ts_cmd: list[str],
    py_args: list[str] | None = None,
    *,
    timeout: float = 10.0,
) -> tuple[subprocess.Popen[bytes], int]:
    """Start the HTTP worker backing one fixture, for whichever SERVER is under test.

    Every HTTP fixture routes through here so a client-role run can be pointed
    at the reference server without each fixture growing its own branch. A
    `py_args` of ``None`` means the reference has no equivalent configuration,
    which is a skip rather than a failure — the fixture's group is about a
    server property this run is not exercising.
    """
    if SERVER == "python":
        if py_args is None:
            pytest.skip(f"the Python reference server has no {variant!r} HTTP variant")
        return _start_http_server([_REF_PY, *py_args], timeout=timeout)
    return _start_http_server(ts_cmd, timeout=timeout)


def _wait_for_tcp(host: str, port: int, timeout: float = 10.0) -> None:
    """Wait for a listener without issuing an HTTP request."""
    deadline = time.monotonic() + timeout
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    raise TimeoutError(f"TCP listener {host}:{port} did not become ready: {last_error}")


@contextlib.contextmanager
def _start_discovery_server(cmd: list[str], prefix: str) -> Iterator[str]:
    """Start one listener worker and yield its machine-readable address."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        if not line.startswith(prefix):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
            stderr = b"" if proc.stderr is None else proc.stderr.read()
            raise RuntimeError(
                f"Expected {prefix}<value> from {cmd!r}, got {line!r}; "
                f"stderr={stderr.decode(errors='replace')!r}"
            )
        yield line[len(prefix) :]
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)


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
    proc, port = _start_variant("plain", BUN_HTTP_WORKER, [_PY_SERVE_HTTP, "--http"])
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


@pytest.fixture
def conformance_resource_soak_target() -> Iterator[Any]:
    """Expose one isolated Bun HTTP worker to the shared resource soak."""
    if ROLE != "server":
        pytest.skip("the resource soak measures this port's server, not its client")
    from vgi_rpc.conformance._resource_soak_pytest import (
        ResourceSoakLimits,
        ResourceSoakTarget,
    )

    proc, port = _start_http_server(BUN_HTTP_WORKER)
    try:
        def connect() -> contextlib.AbstractContextManager[Any]:
            return http_connect(ConformanceService, f"http://127.0.0.1:{port}")

        yield ResourceSoakTarget(
            name="typescript-bun-http",
            pid=proc.pid,
            connect=connect,
            limits=ResourceSoakLimits(
                rss_growth_bytes=64 * 1024 * 1024,
                rss_slope_bytes_per_epoch=8 * 1024 * 1024,
                descriptor_growth=4,
                thread_growth=4,
                child_growth=0,
            ),
            warmup_multiplier=2,
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


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
    proc, port = _start_variant(
        "no_compression",
        [*BUN_HTTP_WORKER, "--response-compression", "off"],
        [_PY_SERVE_HTTP, "--http", "--no-compression"],
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_small_request_cap_port() -> Iterator[int]:
    """Bun HTTP worker with the shared suite's canonical 4 KiB request cap."""
    proc, port = _start_variant(
        "small_request_cap",
        [*BUN_HTTP_WORKER, "--max-request-bytes", "4096"],
        [_PY_SERVE_HTTP, "--http", "--max-request-bytes", "4096"],
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="class")
def conformance_http_serve_start_fail_once_port() -> Iterator[int]:
    """HTTP worker whose first lifecycle notification fails and then retries.

    Readiness is intentionally TCP-only: an HTTP probe would itself consume
    the injected first lifecycle failure before the shared test can observe it.
    """
    cmd = (
        [_REF_PY, _PY_SERVE_HTTP, "--http", "--fail-serve-start-once"]
        if SERVER == "python"
        else [*BUN_HTTP_WORKER, "--fail-serve-start-once"]
    )
    with _start_discovery_server(cmd, "PORT:") as raw_port:
        port = int(raw_port)
        _wait_for_tcp("127.0.0.1", port)
        yield port


@pytest.fixture(scope="session")
def conformance_transport_kind_probes() -> Iterator[tuple[tuple[str, Callable[[], str]], ...]]:
    """Expose real wire probes for every TypeScript server transport."""
    if ROLE != "server":
        pytest.skip("transport-kind probes measure this port's server, not its client")

    class _KindProbe(Protocol):
        # The wire routing key, declared rather than inherited from the class
        # name. `_protocol_wire_name` falls back to `__name__` when this is
        # absent, so without it the client stamps `vgi_rpc.protocol` as
        # "_KindProbe" -- a private Python identifier nobody ever meant as a
        # wire name -- and the worker correctly refuses a protocol it does not
        # host. Invisible until vgi-rpc-python cfe9838 started stamping the key
        # in request builders that had previously sent none; C++ had four stubs
        # with the same shape, this is the fifth. Any locally declared probe
        # stub needs this line.
        protocol_name = "TransportKindProbe"

        def report_transport_kind(self) -> str: ...

    from vgi_rpc.http import http_connect
    from vgi_rpc.rpc import SubprocessTransport, _RpcProxy, tcp_connect, unix_connect

    pipe_transport = SubprocessTransport(BUN_TRANSPORT_KIND_WORKER)
    tmpdir = tempfile.mkdtemp(prefix="vgi-ts-kind-", dir="/tmp")
    unix_path = os.path.join(tmpdir, "kind.sock")
    try:
        with contextlib.ExitStack() as stack:
            http_port = int(
                stack.enter_context(
                    _start_discovery_server([*BUN_TRANSPORT_KIND_WORKER, "--http"], "PORT:")
                )
            )
            _wait_for_tcp("127.0.0.1", http_port)

            tcp_addr = stack.enter_context(
                _start_discovery_server(
                    [*BUN_TRANSPORT_KIND_WORKER, "--tcp", "127.0.0.1:0"],
                    "TCP:",
                )
            )
            tcp_host, _, tcp_port_raw = tcp_addr.rpartition(":")
            tcp_port = int(tcp_port_raw)

            unix_server_path: str | None = None
            if sys.platform != "win32":
                unix_server_path = stack.enter_context(
                    _start_discovery_server(
                        [*BUN_TRANSPORT_KIND_WORKER, "--unix", unix_path],
                        "UNIX:",
                    )
                )

            def _pipe_probe() -> str:
                return str(_RpcProxy(_KindProbe, pipe_transport, None).report_transport_kind())

            def _http_probe() -> str:
                with http_connect(_KindProbe, f"http://127.0.0.1:{http_port}") as proxy:
                    return str(proxy.report_transport_kind())

            def _tcp_probe() -> str:
                with tcp_connect(_KindProbe, tcp_host, tcp_port) as proxy:
                    return str(proxy.report_transport_kind())

            probes: list[tuple[str, Callable[[], str]]] = [
                ("pipe", _pipe_probe),
                ("http", _http_probe),
                ("tcp", _tcp_probe),
            ]
            if unix_server_path is not None:

                def _unix_probe() -> str:
                    assert unix_server_path is not None
                    with unix_connect(_KindProbe, unix_server_path) as proxy:
                        return str(proxy.report_transport_kind())

                probes.append(("unix", _unix_probe))

            yield tuple(probes)
    finally:
        pipe_transport.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


def _start_auth_worker() -> tuple[subprocess.Popen[bytes], int]:
    """Start the reject-all authenticating worker for whichever SERVER is under test."""
    py_port = _free_port()
    return _start_variant(
        "auth",
        BUN_HTTP_AUTH_WORKER,
        [_PY_SERVE_AUTH, "--port", str(py_port)],
    )


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """Bun conformance HTTP server with reject-all authenticate, for TestHealth."""
    proc, port = _start_auth_worker()
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
    proc, port = _start_auth_worker()
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
    proc, port = _start_variant(
        "cold_call_cache",
        [*BUN_HTTP_WORKER, "--no-call-state-cache"],
        [_PY_SERVE_HTTP, "--http", "--no-call-state-cache"],
    )
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
    proc, port = _start_variant(
        "access_log",
        [*BUN_HTTP_WORKER, "--access-log", str(log_path)],
        [_PY_SERVE_HTTP, "--http", "--access-log", str(log_path)],
    )
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
    proc, port = _start_variant(
        "introspect",
        [*BUN_HTTP_WORKER, "--introspect"],
        [_PY_SERVE_HTTP, "--http", "--introspect"],
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_identity_port() -> Iterator[int]:
    """Bun worker hosting ``vgi_rpc.Identity.v1`` with *both* hooks configured.

    Backs the shared identity group (``TestIdentityWireShape`` and the eleven
    classes after it).  The protocol is nearly all guards and every guard reads
    deployment policy, so the group cannot assert anything against a worker
    whose allowlist, resolver and minter are unknown — the whole policy is
    pinned in ``IDENTITY_CONFORMANCE_FIXTURE.md`` §3 and implemented in
    ``examples/conformance-http-identity.ts``.

    Its own process, and deliberately not the plain worker plus a flag:
    ``TestIdentityAbsentByDefault`` asserts against ``conformance_http_port``
    that a deployment configuring no hook hosts no identity protocol at all,
    and adding identity there would make that property untestable.

    The fixture name is load-bearing — the group looks it up with
    ``getfixturevalue`` and skips, loudly and by name, if it is missing.
    """
    proc, port = _start_variant(
        "identity",
        [*BUN_HTTP_IDENTITY_WORKER, "--identity", "both"],
        [_PY_SERVE_HTTP, "--http", "--identity", "both"],
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_identity_introspect_only_port() -> Iterator[int]:
    """The same binary with the mint hook left out.

    Backs ``TestIdentityNarrowing``.  That an unconfigured hook makes its method
    *absent* rather than hosted-and-refusing — and shrinks the ``protocol_hash``
    with it — is only observable against a second worker configured with one
    hook, so it cannot be folded into the fixture above.
    """
    proc, port = _start_variant(
        "identity_introspect_only",
        [*BUN_HTTP_IDENTITY_WORKER, "--identity", "introspect-only"],
        [_PY_SERVE_HTTP, "--http", "--identity", "introspect-only"],
    )
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
    proc, port = _start_variant(
        "cors",
        [
            *BUN_HTTP_WORKER,
            "--fake-storage",
            conformance_fake_storage,
            "--cors-origin",
            "https://conformance.example",
        ],
        [
            _PY_SERVE_HTTP,
            "--http",
            "--fake-storage",
            conformance_fake_storage,
            "--cors-origin",
            "https://conformance.example",
        ],
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
    proc, port = _start_variant(
        "sticky_short_ttl",
        [*BUN_HTTP_WORKER, "--sticky-ttl", "1"],
        [_PY_SERVE_HTTP, "--http", "--sticky-ttl", "1"],
    )
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
    # The reference mints a random server id per process, so its two peers
    # differ without an explicit flag; the Bun worker hardcodes one and needs it.
    proc_a, port_a = _start_variant(
        "sticky_peer_a",
        [*BUN_HTTP_WORKER, "--token-key", _STICKY_PEER_TOKEN_KEY, "--server-id", "conformance-peer-a"],
        [_PY_SERVE_HTTP, "--http", "--token-key", _STICKY_PEER_TOKEN_KEY],
    )
    proc_b, port_b = _start_variant(
        "sticky_peer_b",
        [*BUN_HTTP_WORKER, "--token-key", _STICKY_PEER_TOKEN_KEY, "--server-id", "conformance-peer-b"],
        [_PY_SERVE_HTTP, "--http", "--token-key", _STICKY_PEER_TOKEN_KEY],
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
    proc, port = _start_variant(
        "sticky_auth",
        [*BUN_HTTP_WORKER, "--sticky-auth"],
        [_PY_SERVE_HTTP, "--http", "--sticky-auth"],
    )
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
        py_port = _free_port()
        py_args = [
            _PY_SERVE_PROOF,
            "--port",
            str(py_port),
            # The Bun proof worker mounts under /vgi and the fixture below
            # reports that prefix for both; the reference defaults to none.
            "--prefix",
            "/vgi",
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
            py_args.append("--proof-no-replay-cache")
        proc, port = _start_variant("proof", cmd, py_args)
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
    proc, port = _start_variant(
        "storage",
        [*BUN_HTTP_WORKER, "--fake-storage", conformance_fake_storage],
        [_PY_SERVE_HTTP, "--http", "--fake-storage", conformance_fake_storage],
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_with_zstd_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun conformance HTTP server wired to the fake storage with zstd compression."""
    proc, port = _start_variant(
        "zstd_storage",
        [*BUN_HTTP_WORKER, "--fake-storage", conformance_fake_storage, "--compression", "zstd"],
        [_PY_SERVE_HTTP, "--http", "--fake-storage", conformance_fake_storage, "--compression", "zstd"],
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_http_external_security_port(conformance_fake_storage: str) -> Iterator[int]:
    """Bun worker with independent external-fetch caps and per-hop URL policy."""
    _external_security_flags = [
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
    proc, port = _start_variant(
        "external_security",
        [*BUN_HTTP_WORKER, *_external_security_flags],
        [_PY_SERVE_HTTP, "--http", *_external_security_flags],
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
    _externalize_always_flags = [
        "--fake-storage",
        conformance_fake_storage,
        "--externalize-threshold",
        "1",
        "--max-request-bytes",
        "1048576",
    ]
    proc, port = _start_variant(
        "externalize_always",
        [*BUN_HTTP_WORKER, *_externalize_always_flags],
        [_PY_SERVE_HTTP, "--http", *_externalize_always_flags],
    )
    yield port
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture(scope="session")
def ts_http_zstd_port() -> Iterator[int]:
    """Start Bun conformance HTTP server with zstd response compression."""
    # The reference compresses responses by default, so its plain server is
    # the zstd peer; the Bun worker needs the dedicated entry point.
    proc, port = _start_variant("zstd", BUN_HTTP_ZSTD_WORKER, [_PY_SERVE_HTTP, "--http"])
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
    proc, port = _start_variant("strict", [*BUN_HTTP_WORKER, "--strict"], [_PY_SERVE_STRICT])
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
    _externalized_cap_flags = [
        "--fake-storage",
        conformance_fake_storage,
        "--max-externalized-response-bytes",
        str(64 * 1024),
        "--max-response-bytes",
        str(8 * 1024 * 1024),
    ]
    proc, port = _start_variant(
        "externalized_cap",
        [*BUN_HTTP_WORKER, *_externalized_cap_flags],
        [_PY_SERVE_STRICT, *_externalized_cap_flags],
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
    "http-node", "http-node-zstd",
    "http-deno", "http-deno-zstd",
]

# Flechette Arrow backend — same TS source, different `imports` condition.
# Opt-in via VGI_TEST_FLECHETTE=1 because the flechette backend currently
# has known wire-encoding gaps (list buffer layout, batch metadata
# attachment on zero-row batches, several wide-type serialization issues).
# Tracking issue: see TODO at top of src/arrow/impl-flechette/index.ts.
_TRANSPORTS = _DEFAULT_TRANSPORTS + (
    ["flechette-pipe", "flechette-http"] if os.environ.get("VGI_TEST_FLECHETTE") == "1" else []
)

# In client role the parameter names a *client* configuration, not a server
# build: the node/deno/flechette entries are alternate builds of this port's
# server and say nothing about its client, and `subprocess` is `pipe` with a
# shared transport the driver does not have. What is left is one byte-stream
# connection plus the three HTTP shapes whose client paths differ —
# uncompressed, zstd request bodies, and every response arriving as a pointer.
_CLIENT_TRANSPORTS = ["pipe", "http", "http-zstd", "http_externalize_always"]

if ROLE == "client":
    _TRANSPORTS = _CLIENT_TRANSPORTS

    # Route the HTTP feature tests — external location, sticky sessions,
    # response caps, upload URLs — through the driver. They import
    # `http_connect` / `http_capabilities` / `request_upload_urls` *inside* the
    # test body, so without this they would quietly exercise the Python client
    # and prove nothing about this port.
    import ts_client_proxy as _shim

    _shim.DRIVER.install_http_overrides()


def _client_factory(
    param: str,
    on_log: Callable[[Message], None] | None,
    http_port: int | None,
    zstd_port: int | None,
    ext_port: int | None,
) -> contextlib.AbstractContextManager[Any]:
    """Open one driver-backed connection for a `conformance_conn` parameter."""
    from vgi_rpc.external import ExternalLocationConfig

    from ts_client_proxy import TsClientProxy

    external_config = None
    compression_level: int | None = None
    if param == "pipe":
        transport: str = "stdio"
        target: Any = _stdio_worker_cmd()
    elif param == "http":
        transport, target = "http", f"http://127.0.0.1:{http_port}"
    elif param == "http-zstd":
        transport, target = "http", f"http://127.0.0.1:{zstd_port}"
        compression_level = 3
    elif param == "http_externalize_always":
        transport, target = "http", f"http://127.0.0.1:{ext_port}"
        # The fake storage vends http:// download URLs; the client's default
        # validator is HTTPS-only. Resolution itself stays the *client's* job.
        external_config = ExternalLocationConfig(url_validator=None)
    else:
        raise AssertionError(f"unknown client transport {param!r}")

    @contextlib.contextmanager
    def _conn() -> Iterator[Any]:
        proxy = TsClientProxy(
            transport,
            target,
            on_log,
            external_config=external_config,
            compression_level=compression_level,
        )
        try:
            yield proxy
        finally:
            proxy.close()

    return _conn()


@pytest.fixture(params=_TRANSPORTS)
def conformance_conn(
    request: pytest.FixtureRequest,
    ts_http_port: int,
    ts_http_zstd_port: int,
) -> ConnFactory:
    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if ROLE == "client":
            return _client_factory(
                request.param,
                on_log,
                ts_http_port,
                ts_http_zstd_port,
                request.getfixturevalue("conformance_http_externalize_always_port")
                if request.param == "http_externalize_always"
                else None,
            )
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
        elif request.param == "http-node":
            port = request.getfixturevalue("ts_node_http_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
            )
        elif request.param == "http-node-zstd":
            port = request.getfixturevalue("ts_node_http_zstd_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
                compression_level=3,
            )
        elif request.param == "http-deno":
            port = request.getfixturevalue("ts_deno_http_port")
            return http_connect(
                ConformanceService,
                f"http://127.0.0.1:{port}",
                on_log=on_log,
            )
        elif request.param == "http-deno-zstd":
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
                yield _RpcProxy(ConformanceService, request.getfixturevalue("ts_transport"), on_log)

            return _conn()

    return factory


@pytest.fixture(params=["pipe", "subprocess"])
def conformance_raw_conn(request: pytest.FixtureRequest) -> ConnFactory:
    """Connect only through the default persistent byte-stream transports."""

    def factory(
        on_log: Callable[[Message], None] | None = None,
    ) -> contextlib.AbstractContextManager[Any]:
        if ROLE == "client":
            # Deliberately *not* driver-backed. This fixture feeds hand-built,
            # deliberately malformed request bytes straight onto a transport
            # and then checks the connection still works — a statement about
            # the server, which a client cannot be made to utter. Under client
            # role it therefore drives the server under test with the
            # reference transport, exactly as server role does.
            @contextlib.contextmanager
            def _raw_server_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport(_stdio_worker_cmd())
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()

            return _raw_server_conn()
        if request.param == "pipe":

            @contextlib.contextmanager
            def _pipe_conn() -> Iterator[_RpcProxy]:
                transport = SubprocessTransport(BUN_WORKER)
                try:
                    yield _RpcProxy(ConformanceService, transport, on_log)
                finally:
                    transport.close()

            return _pipe_conn()

        @contextlib.contextmanager
        def _shared_conn() -> Iterator[_RpcProxy]:
            yield _RpcProxy(ConformanceService, request.getfixturevalue("ts_transport"), on_log)

        return _shared_conn()

    return factory


@pytest.fixture(params=_TRANSPORTS)
def conformance_describe(
    request: pytest.FixtureRequest,
    ts_http_port: int,
    ts_http_zstd_port: int,
) -> "ServiceDescription":
    """Introspect the TS worker under test over the wire.

    Parallels ``conformance_conn`` (same transport matrix) so the upstream
    ``TestDescribeConformance`` suite validates introspection against the
    actual Bun/Node/Deno worker rather than an in-process Python server.

    Introspection is ``vgi_rpc.Reflection.v1`` -- ``list_protocols`` then
    ``describe`` -- which is what ``introspect`` / ``http_introspect`` now
    call.  The TS server hosts it by default.
    """
    from vgi_rpc.http import http_introspect
    from vgi_rpc.introspect import introspect

    param = request.param
    if ROLE == "client":
        # Introspection under test is the *client's*: the driver relays the
        # description its client decoded, rather than a second one this side
        # decoded from the same bytes.
        with _client_factory(
            param,
            None,
            ts_http_port,
            ts_http_zstd_port,
            request.getfixturevalue("conformance_http_externalize_always_port")
            if param == "http_externalize_always"
            else None,
        ) as proxy:
            return proxy.describe()
    if param in ("pipe", "flechette-pipe"):
        cmd = BUN_FLECHETTE_WORKER if param == "flechette-pipe" else BUN_WORKER
        transport = SubprocessTransport(cmd)
        try:
            return introspect(transport)
        finally:
            transport.close()
    if param == "subprocess":
        return introspect(request.getfixturevalue("ts_transport"))
    # Everything else is HTTP — resolve the right port for the variant.
    if param == "http":
        port = ts_http_port
    elif param == "http-zstd":
        port = ts_http_zstd_port
    elif param == "http_externalize_always":
        port = request.getfixturevalue("conformance_http_externalize_always_port")
    elif param == "http-node":
        port = request.getfixturevalue("ts_node_http_port")
    elif param == "http-node-zstd":
        port = request.getfixturevalue("ts_node_http_zstd_port")
    elif param == "http-deno":
        port = request.getfixturevalue("ts_deno_http_port")
    elif param == "http-deno-zstd":
        port = request.getfixturevalue("ts_deno_http_zstd_port")
    elif param == "flechette-http":
        port = request.getfixturevalue("ts_flechette_http_port")
    else:
        raise AssertionError(f"unhandled transport for conformance_describe: {param}")
    external_location = None
    if param == "http_externalize_always":
        from vgi_rpc.external import ExternalLocationConfig

        # Reflection is an ordinary protocol, so against a server that
        # externalizes everything its reply arrives as a pointer batch like any
        # other.  The old ``__describe__`` fast path was exempt only by
        # accident of answering before dispatch.  Server hands out
        # ``http://127.0.0.1`` download URLs from the in-process fake storage,
        # so the HTTPS-only validator has to be off.
        external_location = ExternalLocationConfig(url_validator=None)
    return http_introspect(base_url=f"http://127.0.0.1:{port}", external_location=external_location)


# Import all test classes from the conformance pytest suite (shipped with the package)
from vgi_rpc.conformance._pytest_suite import *  # noqa: F401,F403,E402

from vgi_rpc.introspect import ServiceDescription  # noqa: E402


# Override: allow TestLargeData on all transports (the upstream suite may
# skip non-pipe transports, but the TS worker handles them fine).
class TestLargeData(TestLargeData):  # type: ignore[no-redef]  # noqa: F811
    @pytest.fixture(autouse=True)
    def _skip_non_pipe(self) -> None:
        pass


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
