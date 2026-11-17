---
"jazz-tools": patch
---

`allowedTo.*` inside an `exists` (its conditions, a `.hop`, a relation's `.where` or a relation query) is now refused up front instead of by the engine at load: TypeScript rejects it, `definePermissions` throws "`allowedTo.read(…)` inside `exists` is not supported yet: repeat the other table's read rule there instead (e.g. share it as a helper function)" at the `allowedTo` call, and `jazz.permissions.json`'s reader refuses it naming the rule (`tables.profiles.read`) and pointing at the `allowedTo`. A policy the engine refuses when the schema compiles is now reported at its own rule (`jazz.permissions.json: tables.profiles.read: …`, and its line in schema.ts) rather than at a table an `exists` in it reads (`$.members.policies…`).
