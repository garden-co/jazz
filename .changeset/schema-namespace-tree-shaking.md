---
"jazz-tools": patch
---

Let bundlers drop the parts of jazz-tools an app doesn't use. `schema` is now an ES namespace, so a client that never calls `schema.definePermissions` no longer ships the permissions DSL, and `package.json` declares `sideEffects`. `schema` has the same members as before. It is a module namespace object now, frozen and without a prototype: `vi.spyOn(schema, …)` / `jest.spyOn(schema, …)`, `schema.hasOwnProperty(…)` and assigning to its members throw. Spy on or wrap your own functions instead, and use `Object.hasOwn(schema, …)`.
