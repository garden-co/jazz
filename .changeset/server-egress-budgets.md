---
"jazz-tools": patch
---

The server now bounds the unread output it holds for clients. If a client falls behind, its session is closed with a retryable `Backpressure` error, and the client reconnects and resumes. That happens in three cases:

- **One session holds too much:** at most 256 MiB unread (`JAZZ_SESSION_BACKLOG_MAX_BYTES`), however big its first result was. The floor stays 64 MiB (`JAZZ_SESSION_BACKLOG_BYTES`).
- **All sessions together hold too much:** over `JAZZ_EGRESS_BUDGET_BYTES`, the sessions holding the most are closed first. The default is a quarter of the memory limit, or 2 GiB if no limit is set.
- **A client stops reading:** a session that reads nothing for `JAZZ_SLOW_READER_TIMEOUT_SECS` (default 300) while its output is at the budget is closed.
