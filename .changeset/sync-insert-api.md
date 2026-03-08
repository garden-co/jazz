---
"jazz-tools": patch
---

Split the local-first insert APIs in `jazz-tools`.

- `db.insert(...)` now applies the write immediately and returns the inserted row synchronously.
- `db.insertDurable(...)` waits for the requested durability tier before resolving.
