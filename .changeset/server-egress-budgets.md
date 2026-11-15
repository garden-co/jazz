---
"jazz-tools": patch
---

The server (`jazz-tools server`) now bounds the output it holds for clients that fall behind. A session closed for this gets a retryable `Backpressure` error, then reconnects and resumes. That happens in three cases:

- **One session holds too much.** A session may hold at most 256 MiB of output its socket hasn't written yet (`JAZZ_SESSION_BACKLOG_MAX_BYTES`). After one result bigger than that, a session may hold that result plus 64 MiB until it has drained; then the 256 MiB bound applies again. The floor stays 64 MiB (`JAZZ_SESSION_BACKLOG_BYTES`).
- **A client reads too slowly.** While its output waits, a client must read at least 1 KiB/s on average (`JAZZ_SLOW_READER_MIN_BYTES_PER_SEC`). Below that rate for `JAZZ_SLOW_READER_TIMEOUT_SECS` (default 120), its session is closed. Progress counts in raw bytes written to its socket, so a slow link moving a large message is still reading. A client that trickles a few bytes a second to keep a connection open is closed like one that stopped.
- **All sessions together hold too much.** `JAZZ_EGRESS_BUDGET_BYTES` is a best-effort budget, not a hard cap. Past it, sessions that have been below the minimum read rate for `JAZZ_EGRESS_STALL_GRACE_SECS` (default 10) are closed, biggest first. Sessions reading faster are left alone, so the total can exceed the budget by what they hold; each of those is still capped per session. The default budget is a quarter of the memory limit, or 2 GiB without one.

The writer's own queue counts toward these bounds. Kernel socket buffers don't. These settings apply to `jazz-tools server`; a server started through the Node bindings uses the defaults.
