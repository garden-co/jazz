# Fine-Grained Branching And Merging User-Facing Spec

This spec explains the user-facing shape of branches in Jazz. The technical design lives in
[2026-05-14-fine-grained-branching-and-merging-design.md](2026-05-14-fine-grained-branching-and-merging-design.md).

## What Branches Mean In Jazz

Branches are draft spaces for app data, identified by stable branch ids.

A branch lets an app or user make changes without changing what normal `main` readers see. This is
useful for draft workflows, collaborative editing, review screens, and "try this before publishing"
flows.

The first version provides **write visibility isolation**:

- Writes made on a branch id do not appear in normal `main` reads.
- Reads from that branch id see current `main` plus the branch's own changed rows.
- Branch data is not secret. A caller with normal read permission can read a branch if they
  explicitly ask for that branch id.
- Branches are sparse. Jazz stores only rows changed on the branch, not a full copy of `main`.

That means a branch initially behaves like a lightweight overlay:

```text
branch read = current main + branch changes
main read   = current main only
```

Merging publishes branch changes back to `main`. Diffing lets callers preview what a merge would do
for a specific query before merging.

Future improvements can strengthen this model:

- **Fully isolated branches:** a branch could read from its own fixed snapshot instead of current
  `main`, and could have stronger branch-specific access control.
- **Branch ancestry:** a branch could be based on another branch, not only on `main`.
- **Query-based isolation:** a branch-like workflow could be scoped to one query or subset of data,
  instead of a named branch that can touch any row.
- **Built-in branch metadata helpers:** Jazz could provide a default branch metadata schema and
  helpers for branch listing, display names, closed branches, or explicit branch cleanup later.

These are future improvements. The first version keeps branches sparse and simple.

## User-Facing APIs

Branch ids are passed directly to the core APIs. There is no required branch registry in the first
version.

Real apps should usually store branch metadata in a normal app table and use that row's Jazz-created
id as the branch id. This avoids global name collisions, lets apps rename branches by updating a
display field, and gives apps a normal place to store permissions and lifecycle state.

Jazz can provide a default branch metadata schema later, but it should be userland data, not a
required part of core branch reads, diffs, or merges. Apps should be able to extend it with their
own fields.

```ts
const { value: branch } = db.insert(app.branches, {
  name: "Alice draft",
  ownerId: alice.id,
  status: "open",
});

const draft = db.branch(branch.id);
```

Here Jazz creates `branch.id`. The app does not manually create the branch id.

### Branch-Scoped Database View

`db.branch(branchId)` returns a database view where reads and writes use that branch.

```ts
const draft = db.branch(branch.id);

await draft.insert(app.todos, {
  projectId,
  title: "Write API docs",
  done: false,
});
```

The inserted row is visible through `draft`, but not through normal `db` reads from `main`.

### Query Builder Branch Selection

Queries can select a branch directly.

```ts
const rows = await db.all(
  app.todos
    .branch(branch.id)
    .where({ projectId }),
);
```

Query-level branch selection uses the same overlay behavior as `db.branch(branchId)`.

If both are present, the query-level branch wins:

```ts
const draft = db.branch(aliceBranch.id);

const rows = await draft.all(
  app.todos
    .branch(bobBranch.id)
    .where({ projectId }),
);
```

This query reads `bobBranch.id`, not `aliceBranch.id`.

### Query Builder Diff

Diff is exposed on the query builder.

```ts
const diff = await db.all(
  app.todos
    .branch(branch.id)
    .where({ projectId })
    .diff("main"),
);
```

This means:

```text
source = todos in the selected branch for this query
target = todos in main
```

The diff result is close to a normal query result. Each returned row has the normal row fields plus
a `$diff` magic column.

```ts
type QueryDiffRow<Row> = Row & {
  $diff: {
    kind: "insert" | "update" | "delete" | "unchanged" | "error";
    changed: string[];
    conflicts: string[];
    error?: {
      code: "unresolved_parent" | "missing_common_ancestor" | "schema_error" | "merge_strategy_error";
      message: string;
    };
  };
};
```

For inserts and updates, the row fields are the values that merge would write if it ran at the same
observed source and target state.

For deletes, the row fields are the target row being removed, and `$diff.kind` is `"delete"`.

`$diff.changed` lists changed column names. `$diff.conflicts` lists columns where both sides changed
and the merge strategy says the overlap should be surfaced as a conflict.

The diff scope includes rows matching the query on the branch side or the `main` side. Jazz then
computes the merge preview for that union of rows. This avoids hiding a row just because a branch
edit moved it out of the query filter.

### Merge

Merging writes branch changes back to `main`.

```ts
await db.branch(branch.id).merge();
await db.branch(branch.id).merge("main");
```

Merge uses the same merge strategies as diff, but it does not stop just because diff would report
conflicts. Conflicts are for review and UI. Merge still resolves through the configured strategies.

The merge target defaults to `main`. The first version only supports merging into `main`.

Merge only includes the local version of the branch that is visible when merge starts. It does not
wait for remote sync. If another device has written to the branch but that write has not arrived
locally yet, this merge does not include it.

Repeated merges are allowed. They may create extra history, and concurrent repeated merges may
create a diamond in `main` history. Jazz should still show one correct visible result and must not
double-apply the same branch change.

### Typical Product Flow

An app can use the APIs like this:

1. Create a normal `branches` row and use its Jazz-created row id as the branch id.
2. Write draft changes through `db.branch(branch.id)`.
3. Render draft views with query-builder `.branch(branch.id)`.
4. Preview publish impact with query-builder `.diff("main")`.
5. Show changed rows using the normal row fields plus `$diff`.
6. Publish with `db.branch(branch.id).merge()`.
