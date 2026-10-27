---
"jazz-tools": patch
---

A write's `wait({ tier: "global" })` and a serializable transaction's `wait()` now stay pending through network outages and reconnects instead of rejecting once the client reports the outage. They settle when the server acknowledges the write and reject only on a failure the client stops retrying, such as a protocol error or a disabled or revoked account. An expired credential the app renews also keeps them pending. Pass `timeoutMs` to bound a wait: it then rejects with the new `WriteWaitTimeoutError` (`code: "timeout"`), and the write stays queued. Remote reads still fail during an outage.
