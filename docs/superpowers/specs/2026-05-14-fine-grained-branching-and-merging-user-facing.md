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
- Branch access inherits from the normal permissions on the object id passed to `db.branch(...)`.
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
- **Branch-specific permissions:** apps could later override the inherited object permissions with
  explicit branch permission hooks.
- **Built-in branch metadata helpers:** Jazz could later provide optional helpers for branch
  listing, display names, closed branches, or explicit branch cleanup.

These are future improvements. The first version keeps branches sparse and simple.

## User-Facing APIs

Branch ids are passed directly to the core APIs. A non-`main` branch id must be a Jazz object id.
There is no required branch registry in the first version.

The object id can come from whatever app object scopes the draft. For example, if a project owns the
draft workflow, use the project row id as the branch id.

This keeps branch creation simple: create the app object, then branch on that object's id. Jazz
creates the row id; the app does not manually create a branch id.

```ts
const { value: project } = db.insert(app.projects, {
  name: "Website redesign",
  ownerId: session.user_id,
});

const draft = db.branch(project.id);
```

Here `project.id` is both the project row id and the branch id.

### Inherited Permissions

Branch access inherits from the normal permissions on the backing object.

For `db.branch(project.id)`, the backing object is the `projects` row with id `project.id`.

```ts
export default definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.where({ ownerId: session.user_id });
  policy.projects.allowInsert.where({ ownerId: session.user_id });
  policy.projects.allowUpdate.where({ ownerId: session.user_id });
});
```

In this example:

- creating a project creates an object id that can be used as a branch id
- the common pattern is that the created project is readable and updatable by its creator
- reading from `db.branch(project.id)` requires read permission on the project
- writing through `db.branch(project.id)` requires update permission on the project
- diffing from `db.branch(project.id)` requires read permission on the project
- merging `db.branch(project.id)` requires update permission on the project and normal write
  permission for the rows written to `main`

Normal row permissions still apply to the data inside the branch. Being allowed to use
`db.branch(project.id)` does not bypass table permissions for todos, comments, or other rows.

Branch-specific permission hooks are future work. The first version only uses inherited object
permissions.

### Branch-Scoped Database View

`db.branch(branchId)` returns a database view where reads and writes use that branch.

```ts
const draft = db.branch(project.id);

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
    .branch(project.id)
    .where({ projectId: project.id }),
);
```

Query-level branch selection uses the same overlay behavior as `db.branch(branchId)`.

If both are present, the query-level branch wins:

```ts
const draft = db.branch(aliceProject.id);

const rows = await draft.all(
  app.todos
    .branch(bobProject.id)
    .where({ projectId: bobProject.id }),
);
```

This query reads `bobProject.id`, not `aliceProject.id`.

### Query Builder Diff

Diff is exposed on the query builder.

```ts
const diff = await db.all(
  app.todos
    .branch(project.id)
    .where({ projectId: project.id })
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
await db.branch(project.id).merge();
await db.branch(project.id).merge("main");
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

1. Create the app object that scopes the draft, such as a project.
2. Use that object's Jazz-created row id as the branch id.
3. Write draft changes through `db.branch(project.id)`.
4. Render draft views with query-builder `.branch(project.id)`.
5. Preview publish impact with query-builder `.diff("main")`.
6. Show changed rows using the normal row fields plus `$diff`.
7. Publish with `db.branch(project.id).merge()`.
