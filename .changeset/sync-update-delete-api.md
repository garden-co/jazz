---
"jazz-tools": patch
---

Split the local-first update/delete APIs in `jazz-tools`.

- `db.update(...)` and `db.delete(...)` now apply immediately and return `void`.
- `db.updateDurable(...)` and `db.deleteDurable(...)` wait for the requested durability tier before resolving.
- `db.deleteFrom(...)` has been renamed to `db.delete(...)`.
