# Fine-Grained Branching And Merging User-Facing Spec

This spec explains the user-facing shape of branches in Jazz. The technical design lives in
[2026-05-14-fine-grained-branching-and-merging-design.md](2026-05-14-fine-grained-branching-and-merging-design.md).

## What Branches Mean In Jazz

Branches are draft spaces for app data, identified by stable Jazz object ids.

A branch lets an app or user make changes without changing what normal `main` readers see. This is
useful for draft workflows, collaborative editing, review screens, and "try this before publishing"
flows.

The first version provides **write visibility isolation**:

- Writes made on a branch id do not appear in normal `main` reads.
- Reads from that branch id see current `main` plus the branch's own changed rows.
- Branch access is controlled by explicit `forBranch(...)` rules in `permissions.ts`.
- Branches are sparse. Jazz stores only rows changed on the branch, not a full copy of `main`.

That means a branch initially behaves like a lightweight overlay:

```text
branch read = current main + branch changes
main read   = current main only
```

Merging publishes branch changes back to `main`. Diffing lets callers preview what a merge would do
for a specific query before merging.

Future improvements can extend this model:

- **Branch ancestry:** a branch could be based on another branch, not only on `main`.
- **Built-in branch metadata helpers:** Jazz could later provide optional helpers for branch
  listing, display names, closed branches, or explicit branch cleanup.

These are future improvements. The first version keeps branches sparse and simple.

## User-Facing APIs

Branch ids are passed directly to the core APIs. A non-`main` branch id must be a Jazz object id.
There is no required app-facing Jazz-managed branch registry in the first version.

Most apps that want branch metadata can model it with their own `branches` table.

This keeps branch creation simple: create the app object, then branch on that object's id. Jazz
creates the row id; the app does not manually create a branch id.

```ts
const { value: branch } = db.insert(app.branches, {
  projectId,
  name: "Alice's draft",
  ownerId: session.user_id,
});

const draft = db.branch(branch.id);
```

Here `branch.id` is both the branch metadata row id and the branch id.

The id does not have to come from a `branches` table. Any Jazz-created row id can identify a branch:
`db.branch(project.id)`, `db.branch(document.id)`, and `db.branch(branch.id)` all use the same core
API. The table that owns the id chooses which `forBranch(...)` rule can apply, leaving branch
management and naming as app-level choices.

### Branch Permissions

Branch access is deny-by-default. A table must declare which backing tables may act as branch
anchors for that table.

For `db.branch(branch.id)`, the backing row is the `branches` row with id `branch.id`.

```ts
export default definePermissions(app, ({ policy, session }) => {
  policy.todos.forBranch(policy.branches, ({ $branch }) => {
    policy.todos.allowRead.where({ projectId: $branch.projectId });

    policy.todos.allowInsert.where({
      projectId: $branch.projectId,
      createdBy: session.user_id,
    });

    policy.todos.allowUpdate.where({ projectId: $branch.projectId });
    policy.todos.allowDelete.where({ projectId: $branch.projectId });
  });
});
```

In this example:

- creating a branch metadata row creates an object id that can be used as a branch id
- Jazz resolves `branch.id` to the `branches` row
- `$branch` is that resolved `branches` row
- reading todos through `db.branch(branch.id)` requires the branch-scoped todo read rule to pass
- writing todos through `db.branch(branch.id)` requires the matching branch-scoped todo write rule
  to pass
- diffing todos from `db.branch(branch.id)` requires the matching branch-scoped todo read rule
- merging `db.branch(branch.id)` requires the matching branch-scoped rules for the source data and
  normal write permission for the rows written to `main`

Jazz does not infer that `todos.projectId` points at `$branch.projectId`. The policy says that
explicitly.

If `policy.todos.forBranch(policy.branches, ...)` is missing, todo reads and writes through that
branch fail even if normal `policy.todos.allowRead` or `policy.todos.allowUpdate` rules exist.

One table may support more than one branch backing type by declaring multiple blocks:

```ts
policy.todos.forBranch(policy.projects, ({ $branch }) => {
  policy.todos.allowRead.where({ projectId: $branch.id });
});

policy.todos.forBranch(policy.workspaces, ({ $branch }) => {
  policy.todos.allowRead.where({ workspaceId: $branch.id });
});
```

When a branch id is selected, Jazz looks up the row's table and chooses the matching block. If the
branch id points to some other table, access fails.

Normal permissions on `branches`, `projects`, or other backing tables still matter when users query
or edit those rows directly. They are separate from branch access.

### Branch-Scoped Database View

`db.branch(branchId)` returns a database view where reads and writes use that branch.

```ts
const draft = db.branch(branch.id);

await draft.insert(app.todos, {
  projectId: branch.projectId,
  title: "Write API docs",
  done: false,
});
```

The inserted row is visible through `draft`, but not through normal `db` reads from `main`.

### Query Builder Branch Selection

Queries can select a branch directly.

```ts
const rows = await db.all(app.todos.branch(branch.id).where({ projectId: branch.projectId }));
```

Query-level branch selection uses the same overlay behavior as `db.branch(branchId)`.

If both are present, the query-level branch wins:

```ts
const draft = db.branch(aliceBranch.id);

const rows = await draft.all(
  app.todos.branch(bobBranch.id).where({ projectId: bobBranch.projectId }),
);
```

This query reads `bobBranch.id`, not `aliceBranch.id`.

### Query Builder Diff

Diff is exposed on the query builder.

```ts
const diff = await db.all(
  app.todos.branch(branch.id).where({ projectId: branch.projectId }).diff(),
);
```

This means:

```text
source = todos in the selected branch for this query
target = todos in current main
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
      code:
        | "unresolved_parent"
        | "missing_common_ancestor"
        | "schema_error"
        | "merge_strategy_error";
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
```

Merge uses the same three inputs as diff:

```text
base, source, target
```

Merge uses the configured merge strategies, but it does not stop just because diff would report
conflicts. Conflicts are for review and UI. Merge still resolves through those strategies.

Merge publishes to `main`. The first version does not expose a merge target argument.

Merge only includes the local version of the branch that is visible when merge starts. It does not
wait for remote sync. If another device has written to the branch but that write has not arrived
locally yet, this merge does not include it.

### Typical Product Flow

An app can use the APIs like this:

1. Create the app object that scopes the draft, such as a project.
2. Create a branch metadata row, such as `app.branches`, for that draft.
3. Use that branch row's Jazz-created id as the branch id.
4. Define `forBranch(...)` rules for the tables that can be read or written through that branch.
5. Write draft changes through `db.branch(branch.id)`.
6. Render draft views with query-builder `.branch(branch.id)`.
7. Preview publish impact with query-builder `.diff()`.
8. Show changed rows using the normal row fields plus `$diff`.
9. Publish with `db.branch(branch.id).merge()`.
