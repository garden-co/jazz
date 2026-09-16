---
"jazz-tools": patch
---

Fix generated typed-app relation names to match the runtime pluralizer, including irregular plural names. Add `jazz-tools schema relations` to emit a schema-bound catalogue that `defineApp` and `defineSliceableApp` validate and consume for exact TypeScript relation keys.
