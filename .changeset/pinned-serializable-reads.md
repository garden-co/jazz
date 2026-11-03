---
"jazz-tools": patch
---

Validate a createJazz session's serializable reads by its permissions bundle, so serializable transactions no longer always fail with a serialization conflict when the store's own permissions differ from the bundle's.
