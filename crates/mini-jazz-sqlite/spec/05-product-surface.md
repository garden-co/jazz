# Product Surface

## 8. Product Surface Goals

The high-level product shape should remain familiar:

- `schema.ts`
- `permissions.ts`
- typed table handles produced by an app definition
- one-shot reads such as `all` and `one`
- simple writes such as `insert`, `update`, and `delete`
- live query subscriptions
- one explicit parameterized transaction constructor

The product API is table-first. A table handle is both a typed query root and
the write target for that table. Query builders describe relational intent;
write calls describe row mutations; subscription APIs describe long-lived query
interest.

Examples of product-shaped operations the core should continue to support:

```ts
db.todos.all(...)
db.todos.one(...)
db.todos.insert(...)
db.todos.update(...)
db.todos.upsert(...)
db.todos.delete(...)
db.todos.subscribeAll(...)
```

Selection is product-visible query semantics. The public row `id` is always
available in selected result rows. `select("*")` selects all root fields for the
current query root. Selected root fields may coexist with includes, and selected
semantic system fields may be filtered, ordered, and projected through nested
includes where the query builder allows them.

The new core should remove batch terminology from the product surface. Product
code talks about transactions:

```ts
db.transaction({ mode: "mergeable" });
db.transaction({ mode: "exclusive" });
```

Simple writes create mergeable transactions by default.

Application-visible rows keep ordinary `id` plus selected semantic system
fields. The physical layout must not leak generated table names, physical ids,
integer enum values, visibility temp tables, or generated SQL.

### 8.1 Product Operation Semantics

Each simple write call creates one sealed mergeable transaction unless wrapped
in an explicit transaction. Product APIs should not expose batch terminology.
`insert` is create-only: inserting an already-visible row id fails with a
constraint/semantic error instead of silently updating. Product APIs should also
offer an explicit `upsert` operation for idempotent create-or-update workflows.
For mergeable transactions, concurrent upserts of the same row reconcile through
ordinary merge/conflict semantics. For exclusive transactions, upsert is
validated globally against the authority-visible row state and read/write set,
so create-vs-update races resolve through exclusive acceptance or rejection.
An explicit transaction with no staged mutations is a no-op. It should not
create transaction/history state; the final product API should avoid exposing a
meaningful transaction id for that no-op.

Future discussion: upsert across distributed tiers deserves its own design pass
and tests. Mergeable upsert is reconciliation-shaped and may restore a deleted
row by appending a new visible version. Exclusive upsert is
authority-state-shaped and remains underspecified for updates over an existing
row: it may need explicit expected-version/read-set semantics rather than
blindly treating a globally ordered same-row write as non-conflicting. The
product API should make that difference predictable without making common
idempotent writes awkward.

Mergeable transactions are locally visible immediately and reconcile through
merge/conflict semantics. Exclusive transactions are not visible in ordinary
reads until the global authority accepts them. Local previews of exclusive
writes are an advanced UI feature, not default database state; if introduced,
they must be opt-in, marked unsettled/preview in subscriptions, removed on
rejection, and never synced as visible current rows.

Delete, restore, conflict resolution, and branch-source edits are semantic
operations, not storage shortcuts:

- delete checks delete policy, or update fallback only where the schema/policy
  explicitly allows it
- restore/undelete appends a new visible version derived from preserved history
  and reuses insert semantics for authorization; stale delete history replay
  must not hide a later restored row
- conflict resolution is an ordinary transaction and may require explicit
  conflict-resolution permission where a product schema declares it
- branch creation, source edits, and merge metadata edits are backing-row or
  branch-metadata writes governed by branch backing-row permissions

Open issues:

- exact v2 syntax for `indexOnly(...)`
- how much transaction/durability state is exposed on typed handles
- whether/how to expose opt-in local exclusive previews
