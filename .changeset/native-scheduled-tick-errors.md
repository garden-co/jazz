---
"jazz-tools": patch
---

Report scheduled core-tick failures on connected runtimes through the existing
terminal transport error path without also throwing them as uncaught exceptions.
Recheck queued work before running it and preserve terminal foreground
retirement and fatal diagnostics without transport.

A failed core tick now surfaces as a `CoreTickFailedError` with the stable code
`core_tick_failed` (detect it with `isCoreTickFailedError`), whose `cause` is
the native error with its core `code`. Because the failure belongs to the core
rather than to one connection, it is still published when it happens while a
reconnect replaces the connection the tick began on, instead of being dropped
as stale.
