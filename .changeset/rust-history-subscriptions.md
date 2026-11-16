---
"jazz-tools": patch
---

**Rust: subscribe to history reads in `jazz_app`**, as in TypeScript. `jazz.subscribe(todos::history().id(id))` streams each accepted version as a row of its own, and new versions arrive as they are accepted. Subscription deltas now name the result they change with `key` (on `Added`, `Updated` and `Removed`), and `Rows::apply` applies them by key. A live union that holds a row once per matching arm now keeps each occurrence right.
