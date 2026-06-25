---
"jazz-tools": patch
"jazz-napi": patch
---

Removed `TestingServer` and `pushSchemaCatalogue` from `jazz-tools/testing` exports and consolidated `startLocalJazzServer` and `deploy` as the canonical way of starting a Jazz sync server and publishing schema changes.
