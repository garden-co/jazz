---
"jazz-tools": patch
"jazz-rn": patch
---

Expose materialized local query previews before a stronger requested read tier is ready, while keeping `isLoading` tied to requested readiness and publishing monotonic `highestSettledAt` metadata across framework bindings and native relay envelopes.
