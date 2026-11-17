---
"jazz-tools": patch
---

**Rust: branches and drafts.** `jazz.on_branch(view)` gives a handle that reads and writes through a branch view, as TS's `branch` and `base` options: `jazz.on_branch(Branch::from("draft").over("main"))`. Its `all`, `one`, `get` and `subscribe` see the head's rows, falling back to the base's; its `update`, `upsert` and `delete` land in the head, copying an inherited row first; `insert` and `restore` land in the head too. A `Branch` is a scalar for a schema with one branch column (`Branch::from("draft")`), or qualified: `Branch::by(documents::workspace_id(), ws).and(documents::branch_id(), id)`.

`jazz.capture_snapshot()` returns a `Snapshot` for a frozen base (`Branch::over_snapshot(base, snapshot)`), stored as TS's `serializeSnapshotRef` string with `to_persisted` / `from_persisted`. `Transaction::on_branch(view)` writes into branches inside a transaction.

`JazzClient` gains `on_branch(head, base)`, `branch_selector`, `branch_columns` and `capture_snapshot`. Not supported yet through a branch: reads inside a transaction, streaming inserts and byte-range reads; they fail with an error.
