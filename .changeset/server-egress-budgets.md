---
"jazz-tools": patch
---

The server (`jazz-tools server`) now bounds the output it holds for clients that fall behind. A session closed for this gets a retryable `Backpressure` error, then reconnects and resumes. That happens in three cases:

- **One session holds too much.** A session may hold at most 256 MiB of output its socket hasn't written yet (`JAZZ_SESSION_BACKLOG_MAX_BYTES`). After one result bigger than that, a session may hold that result plus 64 MiB until it has drained; then the 256 MiB bound applies again. The floor stays 64 MiB (`JAZZ_SESSION_BACKLOG_BYTES`).
- **All sessions together hold too much.** `JAZZ_EGRESS_BUDGET_BYTES` is a best-effort budget, not a hard cap. Past it, sessions whose socket has written nothing for 30 s are closed, biggest first. Sessions whose socket is still writing are left alone, so the total can exceed the budget by what they hold; each of those is still capped per session. The default is a quarter of the memory limit, or 2 GiB without one.
- **A client stops reading.** A session whose socket writes nothing for `JAZZ_SLOW_READER_TIMEOUT_SECS` (default 300) while its output waits is closed. Progress is measured in raw bytes written, so a slow client making any progress within the timeout stays connected.

The writer's own queue now counts toward these bounds. Kernel socket buffers don't. These settings apply to `jazz-tools server`; a server started through the Node bindings uses the defaults.
