---
"jazz-tools": patch
---

`createPolicyTestApp` now publishes the schema versions and migrations the app ships, as `createJazz` does, so policy tests keep working after an app's first migration instead of failing with "stores otherwise than its upstream".
