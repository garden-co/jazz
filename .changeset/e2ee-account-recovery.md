---
"jazz-tools": patch
---

Add independently generated account recovery creation/import, root registration and
recovery-backed device approval, root retirement with explicit epoch rotation,
local-first secret protection, and read-only recovery status. Retirement excludes
the retired root from future authority and deliveries without revoking devices
already admitted through it or erasing prior epoch keys. Validate the final
recovery delivery verifier before publication, and sanitise material-import
parser and private-key adapter failures without retaining their diagnostic text
or causes.

Correctness WASM builds retain development safety checks with basic optimization.
Bound worker and browser-package concurrency so multi-client lifecycle tests keep
their existing deadlines; the complete CI job has a separate execution budget.
