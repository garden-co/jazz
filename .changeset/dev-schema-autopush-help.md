---
"jazz-tools": patch
---

Allow development-mode clients to auto-publish the current structural schema from `schema.ts` without an admin secret, while keeping non-schema catalogue writes admin-only. Improve `jazz-tools --help` and the docs so the CLI and publishing workflow more clearly explain when schema auto-push is enough versus when to run `permissions push` or `migrations push`.
