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
Read registered recovery protectors at the current global authority tier.
Clear temporary private-key arrays when inspecting recovery-material metadata,
and propagate operational history-verifier failures instead of hiding them by
trying another recovery delivery or protector. Invalid-candidate fallback and
sanitised material-import errors remain supported.

Correctness WASM builds retain development safety checks with basic optimization.
Bound worker and browser-package concurrency so multi-client lifecycle tests keep
their existing deadlines; the complete CI job has a separate execution budget.
Browser file workers also respect host CPU capacity, and package-content checks
read both keyed and array-shaped npm pack receipts without changing their assertions.
