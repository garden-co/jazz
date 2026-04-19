---
"jazz-tools": patch
---

Require a published permissions head before session-scoped queries or writes can rely on backend authority. Backends without a current permissions head now reject those operations explicitly instead of returning empty results or ambiguous local fallbacks.
