---
"jazz-tools": patch
---

The server (`jazz-tools server`) now bounds the output it holds for clients that fall behind. A client that falls behind is closed with a retryable `Backpressure` error, then reconnects and resumes. That happens in three cases:

- **One session holds too much.** A session may hold at most 256 MiB of output its connection hasn't taken yet (`JAZZ_SESSION_BACKLOG_MAX_BYTES`). A first result bigger than that still drains, with 64 MiB of room for what follows it. The floor stays 64 MiB (`JAZZ_SESSION_BACKLOG_BYTES`).
- **All sessions together hold too much.** Past `JAZZ_EGRESS_BUDGET_BYTES`, the sessions holding the most among those that have stopped reading are closed first. Sessions that are reading are never closed for this. The default is a quarter of the memory limit, or 2 GiB without one.
- **A client stops reading.** A session whose client reads nothing at all for `JAZZ_SLOW_READER_TIMEOUT_SECS` (default 300) while its output waits is closed. A slow client that keeps reading is never closed, however much it has left to read.

These settings apply to `jazz-tools server`. A server started through the Node bindings uses the defaults.
