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

Branch read policies are evaluated in two layers:

1. the session must be able to read the branch's app-visible backing row
2. the target row must pass the table's branch-view read rule

The first implemented branch-view rule shape is field equality between the
target row and the backing row, for example `todo.project == branch.project`.
When a table declares a branch-view read rule, branch reads for that table must
use it rather than silently falling back to the table's main read policy.
Missing branch-view rules for a declared branch policy deny. Normal main reads
never use branch-view policies.

Branch-view write policies use the same backing-row context. A write may be
allowed because the proposed row field matches a backing-row field. If the
backing row is hidden or the branch-view write rule is missing for a table with
branch policy, the write is denied. Denial is represented as rejected history
and projection repair, consistent with ordinary local-first policy failure
semantics. Backing rows used by branch-view write policies are recorded as
policy read-set facts, just like parent rows used by ordinary reference
policies.

## Server Permission Validation Flow

Permission enforcement follows the current Jazz Tools server flow:

1. Untrusted client writes enter the sync layer as pending permission checks
   with client id, session, operation, branch, table metadata, old content when
   known, new content when present, and row provenance.
2. The query/runtime layer drains pending checks and resolves the structural
   schema for the target branch. If the schema is temporarily unavailable, the
   check is requeued; if it stays unavailable past the wait budget, the write is
   rejected.
3. The runtime resolves the current authorization schema from the published
   permissions head for the target branch context. Dynamic servers are
   fail-closed before the permissions head is available. A loaded empty
   permissions bundle is still enforcing and grants nothing implicitly.
4. Missing tables, malformed row content, missing required old/new content, and
   missing provenance fail closed.
5. Insert checks evaluate the table insert `WITH CHECK` policy against the
   proposed row and payload provenance.
6. Update checks evaluate `USING` against the old visible row/provenance and
   `WITH CHECK` against the proposed row/provenance. If both clauses are
   present, both must pass. If neither clause exists in enforcing mode, the
   update is rejected.
7. Delete checks evaluate explicit delete `USING` when present, otherwise the
   effective update `USING` fallback. Missing explicit policy in enforcing mode
   rejects.
8. Approval applies the pending payload and may settle the sealed batch.
   Rejection records a replayable rejected fate or emits a permission-denied
   error for non-row payloads.

Read/query permission validation is the same authority boundary, not a client
cleanup pass. A server answering a user-scoped query must compile the read
policy into the query plan, evaluate it in the same session, branch, schema,
lens, and snapshot context as the query itself, and export only rows and policy
dependency rows visible in that context. Query-scope repair rows are part of the
same user-facing delivery surface: they must be collected with the checked-out
branch view and requester read policy, never with admin/system bypass, and the
history exported for them must stay branch-scoped. A repair scan may consider
history to find rows that left a predicate or page, but it must not use that as
permission to send unrelated branch history or rows the requester cannot read.

Read visibility planning is centralized for read surfaces. Query execution,
sync export, query-scope repair, and policy dependency export must derive their
current-row, snapshot-row, and effective-branch policy SQL from the same
read-visibility context: requester, bypass mode, branch, branch sources, base
snapshot epoch, and schema. Read paths must not call raw policy lowering
directly, because that makes it too easy for query and export to disagree about
which rows are visible. Write permission validation remains a separate flow.

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
- explicit inherit-main branch policy semantics
- branch-view query-scope repair coverage
- how to bound recursive policy evaluation
- edge policy-readiness strategy
- redaction rules for policy denial/rejection explanations
- compact representation and indexing of transitive policy read facts
