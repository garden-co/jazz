# Policies

## 10. Policies

Policies are part of the database model. They shape reads, writes,
subscriptions, sync scope, and authority validation.

The policy language should preserve the current `permissions.ts` product shape
almost exactly. Internal lowering may change, but app authors should not learn a
new policy vocabulary just because storage changed.

Policies must be SQL-lowerable. This includes:

- ordinary row policies
- inherited/relational policies
- branch policies
- recursive policies

Policy operations:

- read policies shape row visibility and sync delivery
- insert policies check proposed row values plus session context
- update policies check old visible row, proposed row values, and session
  context
- delete policies prefer explicit delete rules, but may fall back to update
  policy semantics
- restore/undelete reuses insert policy semantics over the restored visible row
- branch policies are ordinary row policies on branch backing rows that
  influence downstream row visibility in that branch view
- catalogue publication is admin/core-controlled rather than ordinary row policy

Policies may depend on rows other than the result row. In the running example,
a todo read may depend on the referenced project row and the project membership
rows that authorize Alice.

Policy evaluation always happens in an explicit read context. The same policy
expression may produce different answers under:

- main current
- branch overlay plus latest main
- branch overlay plus pinned base snapshot
- historical global epoch snapshot

Local validation, edge validation, sync export, subscription invalidation, and
policy read-set recording must use the same read context for one operation. A
write through a pinned branch must not accidentally validate against latest main
when the referenced policy row has no branch overlay; it must validate against
the branch's pinned base snapshot.

Policy dependencies must be represented as observed facts separately from
ordinary result dependencies. A row included only for policy enforcement should
not necessarily appear as a query include.

Write-policy validation records policy read facts. These facts are transitive:
if a todo write is allowed because its project is readable, and the project is
readable only because an org is readable, the transaction's policy read facts
include both the project and the org. These facts are read-set material for
replay, validation, causality reasoning, sync scope, and future diagnostics.

Policy failures should not let ordinary clients distinguish hidden rows from
nonexistent rows. Trusted peers and authorities may keep richer debug logs.

Recursive policies are in scope. v0 rejects policy cycles and supports bounded
acyclic recursive policy chains that lower to SQL. Recursive policy lowering
must work in all read contexts listed above, including pinned branch base
snapshots.

Open issues:

- exact SQL-lowerable policy IR
- how to bound recursive policy evaluation
- edge policy-readiness strategy
- redaction rules for policy denial/rejection explanations
- compact representation and indexing of transitive policy read facts
