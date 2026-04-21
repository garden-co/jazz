---
"jazz-tools": patch
---

Require a published permissions head before session-scoped writes can rely on backend authority. Persisted writes against enforcing backends without a current permissions head now reject explicitly with `permissions_head_missing`, and synced-query tests now publish permissions before expecting backend-visible rows or cross-schema authorization results. Session-scoped queries still withhold authoritative remote scope before a permissions head exists, but explicit query/subscription rejection is deferred for now.
