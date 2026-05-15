# Fine-Grained Branching And Merging Design

## Goal

Add sparse branches for draft and collaborative editing.

Branches provide write isolation by visibility only. A normal read from `main` does not see branch
writes. Non-main branch ids are Jazz object ids, and access to a branch inherits from the normal
permissions on that backing object.

The first implementation supports branch reads and writes as overlays on `main`, scoped diffs from
a branch query to `main`, and merges from a branch to `main`.

## Non-Goals

- No durable Jazz-managed branch registry in the MVP. Apps may create their own branch metadata
  tables.
- No branch lifecycle state such as open, closed, or archived.
- No formal idempotence guarantee for merge. Repeating the same merge may create extra history.
- No supported branch-of-branch API in the MVP.
- No branch-specific permission API in the MVP. Branch permissions inherit from backing object
  permissions.

## Branch Model

`main` is the reserved system branch. Every non-main branch id must be a Jazz object id. The object
that owns that id is the branch's backing object for permissions.

A branch exists because rows have been written with that branch id.

Example:

```text
main
  todo-1: m3

branch-1
  todo-1: b1, parent = main:m3
```

Branches are sparse. They store only rows changed on that branch. Unchanged rows are read from
current `main`.

The MVP fallback chain is always:

```text
branch -> main
```

Because there is no branch registry or branch ancestry metadata, the system has no durable source
for a longer fallback chain. Branch-of-branch can be added later, but it needs an explicit ancestry
mechanism.

## Permission Model

Branch permissions inherit from the branch backing object.

There is no separate branch creation API. Creating a branch means creating the backing object
through normal `db.insert(...)`, then using that object's Jazz-created id as the branch id.

Most apps that want branch metadata can create an app-level `branches` table and use its row ids as
branch ids.

```ts
const { value: branch } = db.insert(app.branches, {
  projectId,
  name: "Alice's draft",
  ownerId: session.user_id,
});

const draft = db.branch(branch.id);
```

The common app pattern is that a user who can insert a branch metadata row also creates it with
fields that make the row readable and updatable by that same user. After creation, branch access
follows the normal read/update policy for the created branch row.

The backing object does not have to live in `app.branches`. Any Jazz-created row id can identify a
branch, such as a project id, document id, or app-specific workflow row id. The table that owns the
id defines the permission anchor.

In enforcing runtimes:

- `db.branch(objectId)` must resolve `objectId` to a visible backing object.
- Branch reads and query-builder branch reads require read permission on the backing object.
- Branch writes require update permission on the backing object.
- Branch diff requires read permission on the backing object.
- Branch merge requires update permission on the backing object and must also pass normal target-row
  write permissions on `main`.

Normal row and table permissions still apply to the data read or written through the branch. The
backing object permission is an additional gate for using the branch id; it does not grant access to
unrelated rows.

In permissive local runtimes without a loaded permission bundle, branch id object resolution may be
best-effort, matching existing local permissive behavior. Enforcing runtimes must fail closed if the
backing object cannot be resolved or is not allowed by policy.

Branch-specific permission hooks, such as `allowBranchRead` or `allowBranchMerge`, are future work.
The MVP keeps the rule simple by inheriting from normal object permissions.

## Parent Links

Jazz already keeps row history, so branch ancestry should be represented through parent links,
not a separate baseline table.

For the first write to a row on a branch:

1. Load the current visible frontier for that row on `main`.
2. Write a normal row-history entry on the branch.
3. Set the branch write's parents to the visible `main` frontier.

If the row does not exist on `main`, the branch insert has no `main` parent. If the row is already
deleted on `main`, the branch write uses that visible delete frontier as its parent.

For later writes to the same row on the branch, parent to the previous branch tip as usual.

Example:

```text
main
  m3

branch-1
  b1 parent = main:m3
  b2 parent = branch-1:b1
```

For merge, write normal rows to `main` with parents from both sides:

```text
main
  m4 parent = [main:m3, branch-1:b2]
```

That records that `main` incorporated the branch state.

Parent references must be resolvable across branches. For branch and merge ancestry, the logical
parent reference is `(branch_id, batch_id)`. The row id is implicit because parent lists are
row-local.

Durable storage may compact same-branch parents, but cross-branch parents must preserve the branch
id. Implementations must not rely on a bare `batch_id` being enough to resolve parent history.

## API Shape

Expose branch object ids directly.

```ts
const { value: project } = db.insert(app.projects, {
  name: "Website redesign",
  ownerId: session.user_id,
});

const { value: branch } = db.insert(app.branches, {
  projectId: project.id,
  name: "Alice's draft",
  ownerId: session.user_id,
});

db.branch(branch.id);
app.todos.branch(branch.id).where({ projectId: branch.projectId }).diff("main");
db.branch(branch.id).merge();
```

`db.branch(objectId)` returns a branch-scoped database view. Reads use overlay behavior. Writes
target the branch id.

The query builder must also accept a branch selector. Query-builder branch selection uses the same
overlay semantics as `db.branch(objectId)`. If a query is built from a branch-scoped database view
and also selects a branch directly, the query-level branch wins because it is the closest explicit
choice.

Diff is exposed through the query builder, not as a whole-branch API. Callers scope the diff by
building the query they want to inspect, then call `.diff(targetBranch)`.

Merge is exposed on the branch-scoped database view. `db.branch(sourceObjectId).merge()` merges the
source branch into `main`. The MVP does not expose a merge target argument.

## Read Semantics

Branch reads are overlays on current `main`.

For direct row loads:

```text
load row from branch
if missing:
  load row from main
```

For queries and scans:

```text
current main query result
minus main rows overridden or deleted on branch
plus branch rows that match the query
```

Deletes require branch tombstones. If `main` has `todo-1` and `branch-1` deletes it, reads from
`branch-1` must hide `todo-1` even though it still exists on `main`.

Branch indexes must reflect branch rows. Query planning must understand that a branch row overrides
the corresponding main row.

## Diff Semantics

Query-builder diff compares a source branch query with a target branch.

```ts
app.todos.branch(branch.id).where({ projectId: branch.projectId }).diff("main");
```

The source branch comes from the query builder's `.branch(...)` selection, or from the enclosing
branch-scoped database view if the query does not select a branch directly. The target branch is the
argument passed to `.diff(...)`.

The diff candidate set is concrete and non-circular:

1. Evaluate the query against the source branch overlay.
2. Evaluate the same query against the target branch.
3. Take the union of those row ids.
4. Compute the merged preview only for that candidate set.

This prevents a branch edit from hiding a row just because it no longer matches the query on one
side. A row that would match only after merge, but matches neither source nor target before merge,
is not included by query-scoped diff in the first version.

Within that scope, diff compares the source overlay rows with target rows, including branch
tombstones and branch inserts.

For each changed row:

```text
base   = latest common ancestor(source tip, target tip)
source = current visible row on source
target = current visible row on target
```

Per column:

```text
source_changed = source != base
target_changed = target != base
```

The column merge strategy decides:

- the value that merge would produce
- whether overlapping edits are a conflict or normal automatic merge
- the explanation to include in the diff

Conflicts are surfaced by diff only. They do not block merge.

If a parent link or common ancestor cannot be resolved, diff should report an error for that row
instead of guessing.

Diff should return query-shaped rows with a `$diff` magic column. The main value should be the same
row shape callers already get from ordinary queries.

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

For inserts and updates, the row fields are the values that `db.branch(source).merge()` would write
if the merge ran at the same observed source and target tips. For deletes, the row fields are the
target-side row being removed, marked with `$diff.kind = "delete"`.

The preview is not a lock. A later merge may produce a different value if either side changed after
the diff was computed.

## Merge Semantics

`db.branch(source).merge()` uses the same three inputs as diff, with `main` as the target:

```text
base, source, target
```

It computes the merged row using the column merge strategies and writes the result to `main` as
normal row-history entries.

Merge only considers the branch and target versions visible in the local runtime at the time the
merge starts. It does not wait for remote sync or include remote branch changes that have not
arrived locally yet. If another device writes to the same branch but that write is not visible
locally when `merge(...)` runs, that write is not part of this merge.

Merge does not stop because diff would have reported conflicts. Conflicts are informational for
diff. Merge always resolves through the merge strategies.

If a merge strategy cannot compute a value, merge fails before writing anything for that merge
batch.

Repeated merges may write extra history. Concurrent repeated merges may also produce a diamond on
`main`, for example when two callers merge the same source branch tip into the same target tip at
the same time.

Visible resolution must treat equivalent duplicate merge outputs as one logical contribution. In
particular, if multiple `main` frontier tips incorporate the same source branch tip and produce the
same merged user values, merge strategies must not count that source change more than once. This is
required for strategies such as counters, where blindly summing each diamond tip as an independent
delta would double-apply the same branch change.

If concurrent merge outputs differ because they observed different source or target inputs, normal
row-history merge strategy rules apply to those distinct outputs.

Delete/update combinations follow existing delete semantics and merge-strategy behavior.

## Schema Behavior

Branch reads and merges must respect existing schema/lens behavior.

If source and target rows are stored under different schema branches, diff and merge may proceed
only when the runtime can resolve the needed lens path. If no valid lens path exists, diff/merge
must fail with a schema error.

## Branch-Of-Branch Evaluation

MVP does not support branch-of-branch.

The parent-link model does not block it. A future branch can parent its first writes to another
branch's visible row, the same way MVP branch writes parent to `main`.

The missing piece is read fallback ancestry:

```text
branch-2 -> branch-1 -> main
```

Without explicit parent branch metadata, this chain is ambiguous. Do not expose branch-of-branch
until there is a clear ancestry source.

## Testing

Prefer integration tests that exercise the app-facing API.

Required coverage:

- branch write is invisible from `main`
- branch read falls back to current `main`
- non-main branch ids must be Jazz object ids
- enforcing runtimes deny branch access when the backing object is not readable
- branch writes require update permission on the backing object
- merge requires update permission on the backing object plus normal target-row write permission
- query-builder branch selection uses branch overlay reads
- query-level branch selection overrides a branch-scoped database default
- query-builder diff compares the selected source branch with the target branch
- query-builder diff includes rows matching the query on the source or target side
- branch edit overrides current `main`
- branch delete hides current `main`
- first branch write parents to the current `main` frontier
- later branch write parents to the previous branch tip
- `db.branch(source).merge()` writes to `main`
- merge writes to `main` with parents from both locally visible current `main` and locally visible
  branch tip
- merge excludes branch writes that are not locally visible when merge starts
- diff detects strategy-defined overlap
- merge resolves through merge strategies
- repeated merge may create extra history while visible result remains stable
- concurrent repeated merges can produce a diamond without double-applying the same source change
- cross-branch parent links resolve correctly
- cross-branch parent refs include branch id plus batch id
- diff reports an error for unresolved parent/common-ancestor state
- query-builder diff scope is the union of source-query and target-query matches
- diff returns query-shaped rows with `$diff` magic-column metadata
- schema/lens failures produce clear diff/merge errors

## Main Implementation Risk

Direct row overlay reads are straightforward. Query overlay is the hard part.

A branch edit can change whether a row matches a filter, join, or index-backed query. The query
manager must subtract overridden main rows and add matching branch rows consistently.

This should be tested at the query level, not only through direct row loads.
