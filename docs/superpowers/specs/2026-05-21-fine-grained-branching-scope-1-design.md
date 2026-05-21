# Fine-Grained Branching Scope 1 Design

Scope 1 adds the user-facing API and permission contract needed to work with branch ids on top of
the current branching system. It does not change branch isolation, branch storage, branch writes,
diffing, or merging behavior.

## Goals

- Add a simple branch-view API: `db.createBranch(branchId)`.
- Keep branch metadata app-modeled. Apps create rows in their own tables, then use the created row
  id as the branch id.
- Add an outer `policy.forBranch(...)` permission API that groups branch-scoped table policies by
  backing branch table.
- Deny branch-scoped access unless the branch id resolves to a readable backing row and a matching
  `policy.forBranch(...)` block exists.

## Non-Goals

- Scope 1 does not make branch writes isolated from `main`.
- Scope 1 does not add merge, diff, branch deletion, branch archival, or branch metadata ownership.
- Scope 1 does not add a Jazz-managed branch registry.
- Scope 1 does not add a separate branch-opening permission. Opening a branch is governed by normal
  `allowRead` on the backing row.

## Branch View API

`db.createBranch(branchId)` returns a branch-scoped `Db` view.

```ts
const branchDb = db.createBranch(
  db.insert(app.branches, {
    projectId,
    name: "Alice's draft",
    ownerId: session.user_id,
  }).value.id,
);
```

The method is synchronous and cheap. It does not create the backing row and does not eagerly validate
the branch id. This keeps it aligned with existing local-first write ergonomics: callers can insert a
branch metadata row and immediately derive a branch view from its local id.

The returned branch view carries the selected branch id as default branch context for reads,
subscriptions, batches, and transactions. A query-level branch selection may still override the view
branch when that query API exists.

## Backing Row Resolution

A non-`main` branch id is a Jazz object id. When a branch-scoped operation first needs authorization,
Jazz resolves the id to its backing row.

Resolution succeeds only when:

- the id resolves locally to a row table
- the backing row exists and is not hard-deleted in the current view used for authorization
- normal `allowRead` on the backing table passes for the current session
- permissions define `policy.forBranch(policy.<backingTable>, ...)`

If any step fails, the branch-scoped operation fails closed. Normal permissions on the backing row
remain separate from branch-scoped permissions on app data.

## Permission API

Branch permissions are grouped by backing table:

```ts
export default definePermissions(app, ({ policy, session }) => {
  policy.branches.allowRead.where({ ownerId: session.user_id });
  policy.branches.allowInsert.where({ ownerId: session.user_id });

  policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
    branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });

    branchPolicy.todos.allowInsert.where({
      projectId: $branch.projectId,
      createdBy: session.user_id,
    });

    branchPolicy.todos.allowUpdate.where({ projectId: $branch.projectId });
    branchPolicy.todos.allowDelete.where({ projectId: $branch.projectId });
  });
});
```

`$branch` is the resolved backing row. `branchPolicy` exposes the same table policy builders as
`policy`, but rules registered through it apply only when the selected branch id resolved through
that `forBranch` backing table.

One backing table can govern many app tables:

```ts
policy.forBranch(policy.projects, ({ $branch, branchPolicy }) => {
  branchPolicy.todos.allowRead.where({ projectId: $branch.id });
  branchPolicy.comments.allowRead.where({ projectId: $branch.id });
});
```

One app table can be branchable through more than one backing table by declaring more than one
`forBranch` block:

```ts
policy.forBranch(policy.projects, ({ $branch, branchPolicy }) => {
  branchPolicy.todos.allowRead.where({ projectId: $branch.id });
});

policy.forBranch(policy.workspaces, ({ $branch, branchPolicy }) => {
  branchPolicy.todos.allowRead.where({ workspaceId: $branch.id });
});
```

## Runtime Authorization Contract

For a branch-scoped read of `todos` through a branch id backed by `branches`, authorization is:

1. Resolve the branch id to a `branches` row.
2. Check `policy.branches.allowRead` against that row.
3. Find `policy.forBranch(policy.branches, ...)`.
4. Check `branchPolicy.todos.allowRead` against each candidate todo row with `$branch` bound to the
   resolved backing row.

For branch-scoped inserts, updates, and deletes, the same backing-row resolution applies, then the
matching branch-scoped operation rule is evaluated. The actual write semantics stay unchanged in
Scope 1; later scopes will decide branch write storage and isolation behavior.

## Errors

Scope 1 should produce distinct errors for:

- branch id cannot be resolved to a row
- the backing row is not readable by the current session
- no `forBranch` block exists for the backing table
- no branch-scoped rule exists for the target table and operation

The API does not need a new error hierarchy in Scope 1. Existing permission-denied and query/write
failure channels can carry these reasons as messages or codes.

## Testing

Scope 1 needs high-level tests that read like app usage:

- insert a `branches` metadata row and derive `branchDb` with `db.createBranch(branch.value.id)`
- allow a user to read the backing row and prove branch-scoped reads use the matching
  `policy.forBranch(...)` block
- deny branch-scoped reads when the backing row fails normal `allowRead`
- deny branch-scoped reads when `policy.forBranch(...)` is missing
- compile multiple target table rules inside one `policy.forBranch(...)` block
- compile more than one backing table for the same target table

These tests should focus on API and permission behavior. They should not assert branch isolation,
merge, or diff behavior.
