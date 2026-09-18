---
"jazz-tools": minor
---

Add independently generated account recovery creation/import, root registration and
recovery-backed device approval, rotation delivery, local-first secret protection,
and read-only recovery status. Recovery does not restore revoked membership, reuse
a device private key, or report registration alone as proof of recoverability.
Validate the final recovery delivery verifier before publication, and sanitise
material-import parser and private-key adapter failures without retaining their
diagnostic text or causes.

Correctness WASM builds retain development safety checks with basic optimization.
Bound worker and browser-package concurrency to avoid oversubscribing multi-client
lifecycle tests. Correctness-fixture and aggregate CI budgets are separate from
performance canaries; assertion and polling contracts remain unchanged.
Browser file workers also respect host CPU capacity, and package-content checks
read both keyed and array-shaped npm pack receipts without changing their assertions.
