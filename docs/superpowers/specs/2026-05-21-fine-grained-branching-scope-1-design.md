# Fine-Grained Branching Scope 1 Design

Scope 1 defines the user-facing API and permission contract for working with branch ids: the
`db.branch(branchId)` view, the `useAll(query, { branch })` reactive read, query-level `.branch(id)`
selection, and the `policy.forBranch(...)` permission API.

> **Compatibility flag — this design cannot be implemented on top of the current branch layer.**
> Scope 1 was originally framed as a thin API/permission layer "on top of the current branching
> system" that changes neither branch storage, writes, nor isolation. Prototyping the API and
> permission model below showed that premise does not hold: the design is **essentially incompatible
> with the current branch layer** and can only be implemented by changing it. See
> [Compatibility with the current branch layer](#compatibility-with-the-current-branch-layer).

## Goals

- Add a branch-view API: `db.branch(branchId)`, plus reactive `useAll(query, { branch })` and
  query-level `.branch(id)` selection.
- Keep branch metadata app-modeled. Apps create rows in their own tables, then use the created row
  id as the branch id.
- Add an outer `policy.forBranch(...)` permission API that groups branch-scoped table policies by
  backing branch table.
- Deny branch-scoped access unless the branch id resolves to a readable backing row and a matching
  `policy.forBranch(...)` block exists.

## Non-Goals

- Scope 1 does not add merge, diff, branch deletion, branch archival, or branch metadata ownership.
- Scope 1 does not add a Jazz-managed branch registry.
- Scope 1 does not add a separate branch-opening permission. Opening a branch is governed by normal
  `allowRead` on the backing row.

> Originally a non-goal was "Scope 1 does not make branch writes isolated from `main`." That no longer
> holds: the API and permission model here only function with branch-isolated reads and writes, so
> isolation is a prerequisite, not a later scope. See below.

## Compatibility With The Current Branch Layer

This design **cannot** be implemented as a pure API/permission layer on top of the current branching
system. The two are essentially incompatible, for these reasons:

- **The current runtime binds a branch at load; this design requires per-operation branch routing.**
  Today a runtime/connection is opened for one `userBranch`, and its writes are pinned to that branch
  — the write path rejects a write whose target branch differs from the runtime's own branch
  ("outside the current schema family"). `db.branch(id)`, `useAll(query, { branch })`, and
  query-level `.branch(id)` all require one runtime to read and write arbitrary branches per
  operation. Supporting that is a breaking change to the runtime's branch model: drop load-time
  branch binding, carry the target branch on every operation, and relax the write-path guard to allow
  any user branch within the same env/schema.

- **The permission contract is only meaningful with branch isolation.** Deny-by-default branch
  access, `$branch`-bound rules, and branch-scoped CRUD presuppose that a branch insert lands on the
  branch and stays invisible to `main`, and that a branch read sees only that branch. This cannot sit
  unchanged on top of `main`; it depends on branch-isolated storage and routing.

- **Synced, enforced branch writes are not supported by the current sync/catalogue layer.** When a
  user client writes into a new branch through an enforcing server, the write fails at the
  sync/catalogue layer: registering the new branch's schema catalogue is rejected for non-admin
  clients (`CatalogueWriteDenied`). Branch writes therefore do not currently sync or enforce on the
  client→server path, even though they work in-process. Authorizing branch registration/writes for
  user clients is a prerequisite this design depends on and the current layer does not provide.

In short, the permission and API design here is sound and was validated in-process, but landing it
end-to-end requires reworking the branch storage/routing/sync layers — not just adding an API on top.
Scopes 2–4 (writes, merge, isolation) are therefore not independent follow-ons to Scope 1; the
write/isolation/sync work is a prerequisite for Scope 1 to function over a server.

## Branch View API

`db.branch(branchId)` returns a branch-scoped `Db` view.

```ts
const branchDb = db.branch(
  db.insert(app.branches, {
    projectId,
    name: "Alice's draft",
    ownerId: session.user_id,
  }).value.id,
);

await branchDb.insert(app.todos, { projectId, title: "Write API docs", ownerId });
const draftTodos = await branchDb.all(app.todos.where({ projectId }));
```

The method is synchronous and cheap. It does not create the backing row and does not eagerly validate
the branch id. This keeps it aligned with existing local-first write ergonomics: callers can insert a
branch metadata row and immediately derive a branch view from its local id.

### Branch routing is per operation, not a runtime mode

The branch is a per-operation target carried on each read, write, subscription, batch, and
transaction — it is **not** a mode the runtime is opened in. One runtime/connection serves every
branch by tagging each operation with its target branch; the write path resolves and composes the
target branch per write rather than being pinned to one branch at open time. A query-level
`.branch(id)` selection overrides the view's branch for that query.

This per-operation model is deliberate and is the core reason the design is incompatible with the
current branch layer (which binds a runtime to one branch at load). See
[Compatibility with the current branch layer](#compatibility-with-the-current-branch-layer).

### Reactive reads

Framework hooks (`useAll`, React/React Native/Svelte/Vue) scope to a branch through a `branch` option
on the query options:

```ts
const todos = useAll(app.todos.where({ projectId }), { branch: branch.id });
```

Distinct `branch` values key independent subscriptions, so several components or tabs can observe
different branches at the same time.

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
matching branch-scoped operation rule is evaluated, with `$branch` bound to literal backing-row
values before evaluation. The write is routed to the target branch per operation. Unlike the original
framing, the write path does change: it must accept writes to a branch the runtime was not opened on,
and the write must be branch-isolated for these rules to be meaningful (see
[Compatibility with the current branch layer](#compatibility-with-the-current-branch-layer)).

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

- insert a `branches` metadata row and derive `branchDb` with `db.branch(branch.value.id)`
- allow a user to read the backing row and prove branch-scoped reads use the matching
  `policy.forBranch(...)` block
- deny branch-scoped reads when the backing row fails normal `allowRead`
- deny branch-scoped reads when `policy.forBranch(...)` is missing
- compile multiple target table rules inside one `policy.forBranch(...)` block
- compile more than one backing table for the same target table

These tests should focus on API and permission behavior. They should not assert branch isolation,
merge, or diff behavior.
