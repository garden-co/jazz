# Fine-Grained Branching And Merging Design

## Goal

Add named, sparse branches for draft and collaborative editing.

Branches provide write isolation by visibility only. A normal read from `main` does not see branch
writes. Branch data is not secret: a caller with normal read permission may read a branch if they
explicitly query that branch name.

The first implementation supports branch reads and writes as overlays on `main`, scoped diffs from
a branch query to `main`, and merges from a branch to `main`.

## Non-Goals

- No durable branch registry in the MVP.
- No branch lifecycle state such as open, closed, or archived.
- No formal idempotence guarantee for merge. Repeating the same merge may create extra history.
- No supported branch-of-branch API in the MVP.
- No branch-level access control.

## Branch Model

A branch is a normal branch name used in row history. It exists because rows have been written with
that branch name.

Example:

```text
main
  todo-1: m3

draft/alice
  todo-1: b1, parent = main:m3
```

Branches are sparse. They store only rows changed on that branch. Unchanged rows are read from
current `main`.

The MVP fallback chain is always:

```text
branch -> main
```

Because there is no branch registry, the system has no durable source for a longer fallback chain.
Branch-of-branch can be added later, but it needs an explicit ancestry mechanism.

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

draft/alice
  b1 parent = main:m3
  b2 parent = draft/alice:b1
```

For merge, write normal rows to `main` with parents from both sides:

```text
main
  m4 parent = [main:m3, draft/alice:b2]
```

That records that `main` incorporated the branch state.

Parent references must be resolvable across branches. If `batch_id` is not globally resolvable,
parent refs must include enough information to find the parent row, such as `(branch_name,
batch_id)`.

## API Shape

Expose branch names directly.

```ts
db.branch("draft/alice");
db.table("todos").branch("draft/alice").where({ projectId }).diff("main");
db.mergeBranch("draft/alice", "main");
```

`db.branch(name)` returns a branch-scoped database view. Reads use overlay behavior. Writes target
the branch name.

The query builder must also accept a branch selector. Query-builder branch selection uses the same
overlay semantics as `db.branch(name)`. If a query is built from a branch-scoped database view and
also selects a branch directly, the query-level branch wins because it is the closest explicit
choice.

Diff is exposed through the query builder, not as a whole-branch API. Callers scope the diff by
building the query they want to inspect, then call `.diff(targetBranch)`.

The MVP should only support merge target `main` unless the implementation explicitly supports more.
`mergeBranch(source, target)` must reject `source === target`.

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

Deletes require branch tombstones. If `main` has `todo-1` and `draft/alice` deletes it, reads from
`draft/alice` must hide `todo-1` even though it still exists on `main`.

Branch indexes must reflect branch rows. Query planning must understand that a branch row overrides
the corresponding main row.

## Diff Semantics

Query-builder diff compares a source branch query with a target branch.

```ts
db.table("todos").branch("draft/alice").where({ projectId }).diff("main");
```

The source branch comes from the query builder's `.branch(...)` selection, or from the enclosing
branch-scoped database view if the query does not select a branch directly. The target branch is the
argument passed to `.diff(...)`.

The diff scope includes any row that matches the query in the source branch overlay, the target
branch, or the merged preview. This prevents a branch edit from hiding a row just because it no
longer matches the query on one side.

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
- whether overlapping edits are a conflict, warning, or normal automatic merge
- the explanation to include in the diff

Conflicts are surfaced by diff only. They do not block merge.

If a parent link or common ancestor cannot be resolved, diff should report an error for that row
instead of guessing.

Diff should also expose a preview-shaped result that callers can render or inspect before merging.
The exact names can follow existing project style during implementation, but the shape should carry
these concepts:

```ts
type BranchDiffPreview = {
  sourceBranch: string;
  targetBranch: string;
  rows: BranchDiffRowPreview[];
  summary: {
    inserted: number;
    updated: number;
    deleted: number;
    unchanged: number;
    conflicts: number;
    warnings: number;
    errors: number;
  };
};

type BranchDiffRowPreview = {
  rowId: string;
  table: string;
  kind: "insert" | "update" | "delete" | "unchanged" | "error";
  merged?: Record<string, unknown>;
  columns: BranchDiffColumnPreview[];
  errors: BranchDiffError[];
};

type BranchDiffColumnPreview = {
  column: string;
  strategy: string;
  base: unknown;
  source: unknown;
  target: unknown;
  preview: unknown;
  status: "source_only" | "target_only" | "same_change" | "auto_merged" | "conflict" | "warning";
  explanation: string;
};

type BranchDiffError = {
  code: "unresolved_parent" | "missing_common_ancestor" | "schema_error" | "merge_strategy_error";
  message: string;
};
```

`preview` is the value that `mergeBranch(source, target)` would write for that column if the merge
ran at the same observed source and target tips. It is a preview, not a lock. A later merge may
produce a different value if either side changed after the diff was computed.

## Merge Semantics

`mergeBranch(source, target)` uses the same three inputs as diff:

```text
base, source, target
```

It computes the merged row using the column merge strategies and writes the result to `target` as
normal row-history entries.

Merge does not stop because diff would have reported conflicts. Conflicts are informational for
diff. Merge always resolves through the merge strategies.

If a merge strategy cannot compute a value, merge fails before writing anything for that merge
batch.

Repeated merges may write extra history. The visible result should still resolve correctly through
normal row-history rules.

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
feature/bob -> draft/alice -> main
```

Without a branch registry or explicit parent branch argument, this chain is ambiguous. Do not expose
branch-of-branch until there is a clear ancestry source.

## Testing

Prefer integration tests that exercise the app-facing API.

Required coverage:

- branch write is invisible from `main`
- branch read falls back to current `main`
- query-builder branch selection uses branch overlay reads
- query-level branch selection overrides a branch-scoped database default
- query-builder diff compares the selected source branch with the target branch
- query-builder diff includes rows matching the query on the source, target, or preview side
- branch edit overrides current `main`
- branch delete hides current `main`
- first branch write parents to the current `main` frontier
- later branch write parents to the previous branch tip
- merge writes to `main` with parents from both current `main` and branch tip
- diff detects strategy-defined overlap
- merge resolves through merge strategies
- repeated merge may create extra history while visible result remains stable
- cross-branch parent links resolve correctly
- diff reports an error for unresolved parent/common-ancestor state
- diff returns preview values for each changed column
- schema/lens failures produce clear diff/merge errors

## Main Implementation Risk

Direct row overlay reads are straightforward. Query overlay is the hard part.

A branch edit can change whether a row matches a filter, join, or index-backed query. The query
manager must subtract overridden main rows and add matching branch rows consistently.

This should be tested at the query level, not only through direct row loads.
