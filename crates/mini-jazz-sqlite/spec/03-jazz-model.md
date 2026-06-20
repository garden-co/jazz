# Jazz Model

## 6. Jazz Model

A Jazz database is a relational database with application tables, row history,
transaction metadata, policy metadata, branch metadata, and catalogue metadata.

Application rows are not semantically stored as mutable rows. Writes append
history. Current tables are serving indexes.

The central rule is:

```text
append-only history is truth;
current projections are rebuildable serving indexes.
```

If history plus transaction outcome disagree with a current projection, history
plus outcome wins.

### 6.1 Example Write

Alice inserts a todo:

```ts
await db.todos.insert({
  id: "todo_1",
  title: "Write RFC",
  done: false,
  project: "project_1",
});
```

The core:

1. creates one public transaction id
2. assigns Alice's node-local epoch
3. records transaction outcome `pending`
4. appends a `todos` history row
5. records observed/read/write facts
6. updates Alice's local current projection
7. publishes local subscription diffs

Later, an edge or global authority may add durability receipts or reject the
transaction. The public transaction id does not change.
