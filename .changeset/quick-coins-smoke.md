---
"jazz-tools": patch
"jazz-napi": patch
"jazz-wasm": patch
"jazz-rn": patch
---

Refine the local-first write APIs across the stacked insert/update/delete follow-up work.

- Insert operations now return the full inserted row instead of just its id.
- In `jazz-tools`, base `Db` write methods are now synchronous local-first APIs: `db.insert(...)`, `db.update(...)`, and `db.delete(...)`.
- When durability-tier acknowledgement matters, use the explicit async variants: `db.insertDurable(...)`, `db.updateDurable(...)`, and `db.deleteDurable(...)`.
- `db.deleteFrom(...)` / `db.deleteFromDurable(...)` have been renamed to `db.delete(...)` / `db.deleteDurable(...)`.
