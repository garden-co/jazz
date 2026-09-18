---
"jazz-tools": minor
---

Add independently generated account recovery creation/import, root registration and
recovery-backed device approval, rotation delivery, local-first secret protection,
and read-only recovery status. Recovery does not restore revoked membership, reuse
a device private key, or report registration alone as proof of recoverability.

Correctness WASM builds retain development safety checks with basic optimization.
Bound worker and browser-package concurrency so multi-client lifecycle tests keep
their existing deadlines; the complete CI job has a separate execution budget.
