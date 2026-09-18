# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
"""A listener at PATH whose accept queue is full: a worker too busy to take another connection.

Usage: full-accept-queue.py PATH [--stale]

Binds PATH, listens with a backlog of 0 and never accepts on its own, then fills
the queue with non-blocking connects it holds open -- full means the next
connect failed (EAGAIN on Linux, ECONNREFUSED on macOS). Prints ``READY <n>``
with the number of connections queued, then reads commands from stdin, one per
line:

    accept   accept (and drop) one queued connection, freeing a slot

EOF on stdin closes everything and exits.

``--stale`` instead leaves a socket file with no listener behind -- what a
worker that died without unlinking leaves -- prints ``READY 0`` and exits.

Python rather than TypeScript because Node and Bun accept every connection as
soon as it arrives: neither can hold a listener that does not accept, which is
the whole condition under test. Mirrors ``_fill_accept_queue`` in the
reference's ``tests/test_launcher.py``.
"""

import socket
import sys


def main() -> int:
    path = sys.argv[1]
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(0)
    if "--stale" in sys.argv[2:]:
        listener.close()
        print("READY 0", flush=True)
        return 0
    queued: list[socket.socket] = []
    for _ in range(256):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.setblocking(False)
        try:
            client.connect(path)
        except (BlockingIOError, ConnectionRefusedError):
            client.close()
            break
        queued.append(client)
    else:
        print("the accept queue never filled", file=sys.stderr, flush=True)
        return 1
    print(f"READY {len(queued)}", flush=True)
    for line in sys.stdin:
        if line.strip() == "accept":
            conn, _ = listener.accept()
            conn.close()
            print("ACCEPTED", flush=True)
    for client in queued:
        client.close()
    listener.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
