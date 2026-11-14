---
"jazz-tools": patch
---

Let bundlers drop the parts of jazz-tools an app doesn't use. `schema` is now an ES namespace, so a client that never calls `schema.definePermissions` no longer ships the permissions DSL, and `package.json` declares `sideEffects`. `schema` has the same members as before.
