---
"jazz-tools": patch
---

`jazz-tools` now routes its sync, schema, permissions, migration, and introspection requests under app-scoped server paths like `/apps/<appId>/...` instead of relying on a configurable `serverPathPrefix`. Server-backed CLI commands now take `<appId>` when resolving those endpoints.
