---
"jazz-tools": patch
---

Add Rust core owner-local accepted observations over a shared frozen cut. Persist original per-operation submission provenance with pending writes, then retain exact accepted row origins after Global reconciliation so durable owners can observe held data after reopening. Local observations remain partial, use current policies and exclude pending writes; browser-worker adapters are separate.
