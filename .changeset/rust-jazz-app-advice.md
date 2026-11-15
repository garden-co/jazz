---
"jazz-tools": patch
---

**Rust: permission advice.** `jazz.can_read::<X>(id)`, `can_insert(NewX::new(…))`, `can_update(id, XUpdate { … })` and `can_delete::<X>(id)` ask the server whether this user may do that, without doing it, like TS's `canRead`, `canInsert`, `canUpdate` and `canDelete`. They return `Advice::Allowed`, `Denied` or `Unknown` (`Unknown` when no server can say). The answers come from `JazzClient::permission_advice`.
