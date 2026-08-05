#!/usr/bin/env python3
"""Assert an access log actually *covers* stream calls.

``vgi-rpc-test --access-log`` validates every record it finds against
``vgi_rpc/access_log.schema.json``, and ``--require-request-data`` closes the
one hole the schema leaves open — but only for ``method_type == "unary"``.
Neither notices when a log is silent about streams, and neither notices when
``stream_id`` is present, schema-valid and meaningless. Both of those shipped
here: the HTTP transport wrote 32 zeros as the stream id of *every* stream on
the server, and stamped no ``request_data`` on ``/init``, and a validator
reporting PASS over records that never carried a real value is not evidence.

So this checks the rules of ``docs/access-log-spec.md`` §4.2/§4.3/§5 that only
have teeth once you look at streams:

* stream records exist at all (a log with none proves nothing about streams);
* every stream record carries a ``stream_id`` of 32 lowercase hex characters,
  and never the all-zeros sentinel the emitter reserves for "no stream was
  established";
* stream ids distinguish streams — a constant, however well-formed, cannot
  pass;
* the id chains a stream's turns: with ``--require-continuations`` at least one
  id must span more than one record, and all records sharing an id must name
  one method and contain exactly one init;
* ``request_data`` rides on ``/init`` and on nothing else — required there by
  §4.3, forbidden on continuations by §5.

``--http`` additionally requires the fields §4.4 defines for HTTP transports:
``http_status`` on every record, and a ``request_id`` that is unique per
record (the id names one request, so a repeat means two requests were joined
that should not have been).

Usage:
    check_access_log_streams.py /tmp/access.jsonl --http --require-continuations

Exit code is 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from collections import Counter, defaultdict

STREAM_ID_RE = re.compile(r"^[0-9a-f]{32}$")
#: Reserved by the emitter for a stream record whose request failed before any
#: stream existed. Legal, but it can never be a real chain id.
NO_STREAM_SENTINEL = "0" * 32


def _load(path: pathlib.Path) -> list[dict]:
    """Parse JSONL, keeping only ``vgi_rpc.access`` records."""
    records = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("logger") == "vgi_rpc.access":
            records.append(obj)
    return records


def _is_init(record: dict) -> bool:
    """True when the record describes a call's entry rather than a continuation.

    ``request_data`` is the marker at DEBUG; at INFO the emitter substitutes
    ``truncated: "payload_omitted"`` for the same field, so both count.
    """
    return "request_data" in record or record.get("truncated") == "payload_omitted"


def check(records: list[dict], *, http: bool, require_continuations: bool) -> list[str]:
    """Return a list of failure messages; empty means conformant."""
    failures: list[str] = []
    if not records:
        return ["no vgi_rpc.access records found"]

    streams = [r for r in records if r.get("method_type") == "stream"]
    if not streams:
        return [
            f"{len(records)} records, none with method_type=stream. A validator that "
            f"passes over a log with no stream records has checked nothing about streams."
        ]

    ids: list[str] = []
    for i, rec in enumerate(streams):
        sid = rec.get("stream_id")
        method = rec.get("method", "?")
        if not isinstance(sid, str) or not STREAM_ID_RE.match(sid):
            failures.append(f"stream record {i} (method={method}): stream_id={sid!r} is not 32 lowercase hex chars")
            continue
        if sid == NO_STREAM_SENTINEL:
            failures.append(
                f"stream record {i} (method={method}): stream_id is the all-zeros sentinel, "
                f"so this record names no stream"
            )
            continue
        ids.append(sid)

    if len(set(ids)) < 2:
        failures.append(
            f"{len(streams)} stream records carry {len(set(ids))} distinct stream_id(s); "
            f"an id that does not distinguish streams cannot group a stream's turns"
        )

    groups: dict[str, list[dict]] = defaultdict(list)
    for rec in streams:
        sid = rec.get("stream_id")
        if isinstance(sid, str) and sid != NO_STREAM_SENTINEL:
            groups[sid].append(rec)

    if require_continuations and not any(len(g) > 1 for g in groups.values()):
        failures.append(
            f"no stream_id spans more than one record across {len(groups)} stream(s); "
            f"a per-request id does not chain a stream's turns"
        )

    for sid, group in groups.items():
        methods = {r.get("method") for r in group}
        if len(methods) > 1:
            failures.append(f"stream_id {sid} spans several methods {sorted(map(str, methods))}")
        inits = [r for r in group if _is_init(r)]
        if len(inits) != 1:
            failures.append(
                f"stream_id {sid} (method={group[0].get('method')}) has {len(inits)} init records "
                f"among {len(group)}; exactly one turn carries request_data"
            )

    # §4.3: required on unary and stream init, absent on continuations.
    for i, rec in enumerate(records):
        is_stream = rec.get("method_type") == "stream"
        if is_stream and not _is_init(rec) and "request_data" in rec:
            failures.append(
                f"record {i} (method={rec.get('method')}): a stream continuation carries request_data, "
                f"which §5 says only init records do"
            )

    if http:
        missing_status = [r for r in records if not isinstance(r.get("http_status"), int)]
        if missing_status:
            failures.append(
                f"{len(missing_status)}/{len(records)} records carry no http_status; "
                f"§4.4 defines it for HTTP transports"
            )
        request_ids = [r.get("request_id") for r in records]
        if any(not isinstance(v, str) or not v for v in request_ids):
            missing = sum(1 for v in request_ids if not isinstance(v, str) or not v)
            failures.append(f"{missing}/{len(records)} records carry no request_id")
        else:
            repeats = [rid for rid, n in Counter(request_ids).items() if n > 1]
            if repeats:
                failures.append(
                    f"{len(repeats)} request_id(s) appear on more than one record "
                    f"(e.g. {repeats[0]!r}); an id shared by two requests joins unrelated work"
                )

    return failures


def main() -> int:
    """Run the checks over a JSONL access log."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("path", type=pathlib.Path, help="JSONL access log written by the worker under test")
    parser.add_argument("--http", action="store_true", help="Also require the HTTP-only fields of §4.4")
    parser.add_argument(
        "--require-continuations",
        action="store_true",
        help="Require at least one stream_id to span several records (HTTP, where each turn is its own request)",
    )
    args = parser.parse_args()

    if not args.path.exists():
        print(f"FAIL: no such file: {args.path}", file=sys.stderr)
        return 1

    records = _load(args.path)
    failures = check(records, http=args.http, require_continuations=args.require_continuations)
    streams = sum(1 for r in records if r.get("method_type") == "stream")
    if failures:
        print(f"FAIL: {len(records)} records ({streams} stream), {len(failures)} problems", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1
    print(f"PASS: {len(records)} records ({streams} stream) cover streams conformantly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
