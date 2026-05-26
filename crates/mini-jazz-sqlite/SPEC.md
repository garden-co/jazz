# Jazz Relational Core On An Embedded Database

Status: Draft v2.

Date: 2026-05-25.

Audience: database engineers and systems engineers who do not know existing
Jazz internals.

## Overview

This document describes how to implement Jazz as a local-first relational
database on top of a simple embedded database. SQLite is the recommended first
implementation target, and examples use SQLite syntax, but SQLite is not part
of Jazz's public semantics.

Jazz is Jazz because it combines four product ideas in one database model:

1. **Local-first sync.** Every replica can write locally, keep durable local
   state, subscribe to relational queries, and sync only the history and
   metadata needed for those queries.
2. **Branching history everywhere.** Writes produce durable row history.
   Branches are visibility views over that history, not copied databases.
   Historical snapshots are normal read modes.
3. **Policy-first relational access.** Row-level security participates in query
   planning, write validation, sync delivery, and authority validation.
4. **Multi-version schemas with lenses.** Multiple schema versions can coexist.
   Lenses describe how older stored data is read through newer schema views and
   how writes move forward.

The embedded database provides local ACID transactions, durable storage,
B-tree indexes, and a query planner. Jazz provides distributed semantics:
stable public identities, transaction outcomes, row history, branch views,
observed facts, sync bundles, subscriptions, policies, conflicts, and schema
lenses.

The design separates:

- the public product model, which uses stable public ids and application schema
  concepts
- distributed semantics, expressed as transactions, outcomes, durability
  receipts, visibility relations, observed facts, and projections over history
- embedded-database lowering, expressed as generated tables, indexes, compact
  local ids, and database-specific query plans

This is not a product API proposal. It is a semantic and architectural spec for
the core that future product APIs should lower into.

## 1. How To Read

The document uses "must", "should", and "may" in their ordinary engineering
sense. They are design intent, not standards-track compliance language.

The main body defines behavior first and storage lowering later. SQL blocks are
lowering sketches unless explicitly described as semantic requirements.

Open issue sections are part of the spec. They identify places where the next
implementation attempt should either choose a behavior or record why the
behavior remains intentionally open.

## 2. One Running Example

The examples use a small app with:

- `projects`
- `todos`
- Alice and Bob as users
- one branch called `draft`
- a policy saying a user can read a todo when they can read its project

A simplified schema might look like:

```ts
export const schema = defineApp({
  tables: {
    projects: table({
      title: text(),
    }),
    todos: table({
      title: text(),
      done: boolean(),
      project: ref("projects"),
      // Declared index intent. The engine may also generate indexes.
      byProjectAndCreated: indexOnly(["project", "$createdAt"]),
    }),
  },
});
```

A query might ask:

```ts
db.todos.all({
  where: { done: false },
  include: { project: true },
  orderBy: [{ field: "$createdAt", direction: "desc" }],
});
```

This query returns semantic todo rows. Internally, it also observes facts: which
todo rows were results, which project rows were needed for includes and policy,
which predicate/range/page-boundary facts made the answer valid, which branch
view was used, and which catalogue revision interpreted the query.

Those observed facts are why Jazz can sync query scope rather than whole tables.
A result payload alone is not enough to keep another replica correct.

## 3. Core Invariants

The core invariants are:

- Public ids are stable across replicas.
- Physical ids are local implementation details and never cross API or wire
  boundaries.
- Every write is represented as one sealed transaction.
- Row history is append-only with respect to application state.
- Transaction outcome and durability determine which history is visible.
- Rejected transactions remain stored but are invisible to ordinary reads.
- Current projections are rebuildable serving indexes over history.
- Branch visibility is independent from global acceptance.
- Queries produce semantic rows plus observed facts.
- Sync bundles are derived from observed facts; they are not table dumps.
- Policies are evaluated before row delivery and before accepting writes.
- Schema/catalogue state is explicit and versioned.
- Subscriptions use the same query semantics as one-shot reads.

These invariants are more important than any particular SQLite table layout.

## 4. Goals And Non-Goals

The core must provide:

- local ACID writes using embedded database transactions
- stable public row and transaction identities across replicas
- optimistic local writes
- authority-observed acceptance and rejection
- append-only row history as the source of truth
- fast current reads using derived projection tables
- historical reads over accepted history
- branch reads over explicit source provenance
- query-scoped sync
- live subscriptions with semantic row diffs
- row-granular exclusive conflict correctness
- per-column metadata for merge, invalidation, policy explanations, and UI
- SQL-lowerable policy and schema-lens hooks

The core should support:

- compact storage and memory representation for hot metadata
- generated covering and partial indexes derived from schema/query intent
- deterministic projection rebuild
- projection-diff effects for incoming sync and subscriptions
- typed read/write sets for authority validation
- branch provenance precise enough for future multi-base branches

This spec does not specify:

- final TypeScript DSL syntax
- final wire encoding
- final authentication system
- production policy language
- production schema-lens completeness
- networking transports
- custom SQLite VFS behavior
- page compression
- garbage collection of history
- final UI behavior for conflict resolution

## 5. Terminology

### 5.1 Public Id

A public id is a stable identifier visible in APIs and sync. Public ids identify
rows, transactions, nodes, branches, schemas, and other externally meaningful
objects.

Public ids must not be replaced when a local transaction becomes globally
accepted. Authority acceptance enriches the existing public transaction.

### 5.2 Physical Id

A physical id is a local integer surrogate used in hot tables and indexes.
Physical ids are not part of API or wire semantics.

### 5.3 Node

A node is a local writer identity such as a device, process, browser worker, or
authority participant. One user may have many nodes. A tab may either be its own
node or use a shared worker node, depending on topology. Node ids are durable
writer identities; principals are authorization identities.

### 5.4 Principal And Session

A principal is the actor the database believes is acting: user, admin, service
actor, or trusted runtime peer.

A session is the execution context for a query, write, or sync connection. It
carries principal, trust role, auth mode, policy context, and runtime context.

### 5.5 Trust Role

Trust role answers: "Is this connection allowed to say this?"

- untrusted client
- admin
- trusted peer

Trust role is distinct from runtime placement. An edge runtime is usually a
trusted peer relative to clients, but edge/global/local placement is a topology
fact, not the same thing as authorization role.

### 5.6 Durability Tier

Durability tier answers: "How far has this transaction or query answer settled?"

- local
- edge
- global

Durability tier is distinct from transaction outcome. A transaction may be
locally durable while still pending, edge durable and replayable, globally
accepted, or rejected.

### 5.7 Transaction

A transaction is the unified write unit. One write call creates one sealed
transaction. Explicit transaction APIs may stage multiple row mutations before
sealing.

Transactions have a conflict mode:

- `mergeable`: can be accepted independently and reconciled later
- `exclusive`: requires authority validation before global acceptance

The product API may describe these as eventually consistent and globally
consistent transactions, but the core should keep conflict mode and durability
separate.

### 5.8 Transaction Outcome

Transaction outcome is the semantic result of a transaction:

- `pending`
- `accepted`
- `rejected`

Outcome is not the same as durability. Edge acceptance of a mergeable
transaction is a replayable durability/acceptance receipt; global acceptance is
the final authority outcome for exclusive transactions.

The v0 prototype represents edge-accepted mergeable transactions as
`outcome = accepted` plus an edge receipt and no `global_epoch`. Later global
acceptance enriches the same public transaction id with `global_epoch` and a
global receipt.

Transaction outcome updates are monotonic. They are not last-write-wins. The
prototype order is:

```text
pending < accepted < rejected
```

`rejected` is terminal for ordinary visibility even if later bundles carry
accepted/global metadata for the same transaction. Durable metadata may still
attach for diagnostics, reconciliation, or audit, but it must not resurrect the
rejected version.

### 5.9 Authority

An authority is a node allowed to assign global epochs and final
acceptance/rejection for exclusive transactions. This spec assumes one logical
global authority state machine per app. Future sharding is internal.

### 5.10 Logical Row And History Row

A logical row is the application row identity visible to users and sync peers.
Logical row public ids are globally unique.

A history row is one version of a logical row written by one transaction.
History rows are append-only for application state.

### 5.11 Current Projection

A current projection is a derived table containing visible row state for a
materialized branch/view context. Main must have a current projection. Other
branch projections are optional serving indexes.

### 5.12 Branch View

A branch view is a product-visible branch plus the engine source list used to
read it:

```text
branch id
backing row
sources
precedence
policy context
provenance metadata
```

Branches are visibility views over shared history. They do not copy databases.

### 5.13 Visibility Relation

A visibility relation is a SQL-usable relation that says which transactions and
sources a read may see. A read combines transaction visibility with branch view,
policy, schema, and query predicates.

### 5.14 Observed Facts

Observed facts are the facts a query or validation path observed: result rows,
dependency rows, absences, ranges, policy dependencies, page boundaries,
branch/source context, schema/lens context, and visible versions.

Observed facts are the common substrate for:

- sync scope
- subscription invalidation
- transaction read sets
- authority validation
- explanations

### 5.15 Scope And Bundle

Scope is the subset of observed facts needed to reproduce, sync, or invalidate a
query answer.

A bundle is the sync payload derived from scope: history, transaction records,
outcomes, durability receipts, branch metadata, catalogue metadata, and facts.

### 5.16 Catalogue Revision

A catalogue revision is the app metadata context used to interpret data:
structural schema, permissions, migrations/lenses, explicit indexes, merge
strategies, and related heads.

The spec still mentions schema heads and permission heads where useful, but the
preferred reader model is one catalogue revision referenced by runtime work.

### 5.17 Lens

A lens is a SQL-lowerable migration edge between schema versions. Lenses live in
`migrations/`.

### 5.18 Semantic System Field And Physical Engine Column

Semantic system fields are user-facing `$...` fields such as `$createdAt`.

Physical engine columns are generated storage columns such as `j_created_at`.
The layout/query codec maps between them.

### 5.19 Projection-Diff Effect

A projection-diff effect is an engine event derived by comparing visible
projection state before and after a local write, incoming sync application, or
outcome/receipt change. Subscriptions may use rerun-and-diff as the correctness
baseline, but projection-diff effects remain a useful shared artifact for
projection repair, sync apply, and listener scheduling.

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

## 7. Auth, Principals, Sessions, And Roles

Every query, write, and incoming sync application is evaluated under a session.
The session feeds policy evaluation, observed facts, sync delivery, validation,
write provenance, and error reporting.

Policy evaluation should see the same session context whether work is evaluated
in a local client, browser worker, edge server, or global authority.

Hosted auth integrations authenticate sessions and produce principals according
to app configuration. For JWT-based auth, the app configuration chooses which
claim becomes the Jazz principal id.

Local anonymous users may have durable local principals, but account-linking or
migration from anonymous principals to hosted principals is not specified here.

Admin sessions bypass row policy entirely. They are still represented as
sessions for audit, provenance, catalogue checks, and operational controls.

Untrusted clients cannot forge authority-only facts such as global acceptance,
rejection, durability receipts, or catalogue publication.

Trusted peers may accept mergeable transactions on behalf of an authenticated
session according to their policy authority role. Once an edge accepts such a
transaction, downstream clients may treat that acceptance as authoritative for
visibility in the edge trust topology; the original session authentication does
not have to be replayed by every downstream client.

Exclusive transactions are different: they require final fate from the global
authority. If an edge or other intermediary forwards an exclusive transaction
instead of deciding it locally, it must forward enough authenticated session
context for the global authority to evaluate the transaction under the same
principal, admin/trust role, and policy context as the initiating session.

Non-admin sessions fail closed when required policy metadata is missing.

Application-visible provenance fields include at least:

- `$createdBy`
- `$updatedBy`

Open issues:

- exact session wire shape
- valid JWT/auth claim configuration
- anonymous-to-hosted principal migration
- whether service actors and admins share one principal namespace with users
- which provenance fields are visible by default under policy

## 8. Product Surface Goals

The high-level product shape should remain familiar:

- `schema.ts`
- `permissions.ts`
- typed table handles produced by an app definition
- one-shot reads such as `all` and `one`
- simple writes such as `insert`, `update`, and `delete`
- live query subscriptions
- one explicit parameterized transaction constructor

The product API is table-first. A table handle is both a typed query root and
the write target for that table. Query builders describe relational intent;
write calls describe row mutations; subscription APIs describe long-lived query
interest.

Examples of product-shaped operations the core should continue to support:

```ts
db.todos.all(...)
db.todos.one(...)
db.todos.insert(...)
db.todos.update(...)
db.todos.delete(...)
db.todos.subscribeAll(...)
```

The new core should remove batch terminology from the product surface. Product
code talks about transactions:

```ts
db.transaction({ mode: "mergeable" });
db.transaction({ mode: "exclusive" });
```

Simple writes create mergeable transactions by default.

Application-visible rows keep ordinary `id` plus selected semantic system
fields. The physical layout must not leak generated table names, physical ids,
integer enum values, visibility temp tables, or generated SQL.

Open issues:

- exact v2 syntax for `indexOnly(...)`
- exact selected semantic system fields beyond `id`
- how much transaction/durability state is exposed on typed handles

## 9. Schemas, Catalogue, Migrations, And Lenses

Catalogue state tells the runtime how to interpret rows, policies, indexes,
merge strategies, and lenses.

The developer-facing project shape is:

```text
schema.ts
permissions.ts
migrations/
```

`schema.ts` defines:

- structural schema
- relations
- merge strategies
- explicit `indexOnly(...)` declarations
- branch-backing table declarations
- file/blob conventions
- future confidentiality metadata

`permissions.ts` is required, even when it declares an empty explicit
permission bundle. A runtime must not infer permissive policy from a missing
permission file or bundle.

`migrations/` contains reviewed migration/lens modules between schema hashes.
Lenses belong in migrations; there is no separate top-level `lenses/` workflow.

Explicit indexes and merge strategies are part of the schema hash. If two
schema versions differ only by index declarations or merge strategy
declarations, the system should derive automatic lens compatibility because row
value shape did not change.

Catalogue publication is admin/core controlled. Edge runtimes learn catalogue
state from the global authority through a separate catalogue sync lane.
Catalogue sync is not ordinary query-scoped row sync.

Runtime work should reference a catalogue revision. A catalogue revision
contains or points to:

- structural schema definitions
- permission bundles and active permission head
- migration/lens edges
- merge strategy declarations
- explicit index declarations

Permission catalogue state is keyed by app id plus head version. The exact app
head/permission head shape remains open, but the preferred model is that normal
runtime work names a single catalogue revision rather than separately guessing
schema and permission heads.

Lenses must be SQL-lowerable in v0. An implementation may initially support
only narrow rename/project lenses.

Writes through an old schema view are copy-on-write into the current schema
version:

1. read old data through lenses into the current schema view
2. apply the write in the current schema
3. append a new history row in the current schema layout

Nullability and defaults are semantic schema features, not incidental SQLite
behavior. Omitted insert fields receive declared defaults before policy checks,
history writes, sync export, and projection rebuild. Explicit `null` on an
optional field is row content and must not be treated as omission. A not-equal
null predicate means "present optional value."

Open issues:

- exact catalogue revision/head representation
- SQL-lowerable lens IR
- schema/lens compatibility across branches
- generated index inspection workflow
- cross-schema conflict candidates and serving indexes over lens unions

Developer workflow:

- `jazz-tools validate` validates schema and permissions together
- validation emits explicit-policy diagnostics
- migration creation compares stored schema hashes and emits reviewed stubs
- migration push publishes reviewed migration/lens edges
- catalogue push publishes app-id/head-version permission bundles and heads
- dev tooling should inspect schema/lens connectivity, permission heads,
  generated indexes, and storage layout

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

## 11. Transactions

Every transaction has:

- public transaction id
- physical transaction id
- writer node
- node-local epoch
- optional global epoch assigned by authority
- conflict mode
- transaction kind
- outcome
- durability receipts/frontier
- creation time
- typed metadata containing write facts and persisted observed facts

Transaction kinds include:

- data
- branch metadata
- schema metadata
- permission metadata

Outcome values are:

```text
pending
awaiting_deps
accepted
rejected
```

Durability/acceptance receipts track where a transaction has become replayable:

```text
local
edge
global
```

For v0, the hot transaction row may store the current outcome and global epoch
mutably. Rejection details should live in a side table keyed by transaction id
or physical transaction id, not as a wide field on the hot transaction row.

The local write path:

1. allocates transaction id and local epoch
2. begins an embedded database transaction
3. records the transaction
4. appends all history rows
5. records write facts and persisted observed facts
6. updates or invalidates current projections
7. commits the embedded database transaction
8. publishes local subscription diffs

Patch updates preserve omitted fields from the effective visible row. The
effective base may be a current branch row, a row inherited from branch sources,
or a pinned historical base snapshot. Unknown user fields fail closed before
history/projection writes; they are not silently dropped.

Authority acceptance enriches the existing transaction. It must not create a new
public transaction id.

Authority rejection keeps the transaction and history rows. Visibility and
projection repair make rejected versions disappear from ordinary reads.

An edge that cannot validate a transaction because required policy-influencing
facts are missing should mark it `awaiting_deps`, request or subscribe to the
missing facts, and re-evaluate after they arrive. `awaiting_deps` is not
acceptance and must not make an authority-accepted version visible. Globally
consistent exclusive transactions must always receive final `accepted` or
`rejected` fate from the global authority.

Edge acceptance of mergeable transactions sets the transaction outcome to
accepted and records an edge receipt without a global epoch. Global acceptance
later records the global epoch and global receipt on the same transaction.

Multiple transactions may share one global epoch. A global epoch is an authority
batch/order point, not a unique transaction coordinate. Deterministic ordering
within one global epoch uses a stable tie-breaker such as physical transaction
number or public transaction id, depending on the storage context.

Waiting semantics:

- waiting on a mergeable transaction may target local, edge, or global
  durability
- waiting on an exclusive transaction with any tier other than global is a
  runtime error
- waiting on an exclusive transaction at global resolves only after global
  acceptance or rejects if the authority rejects it

Open issues:

- exact durability receipt layout
- explicit fate partial order and merge rules for all incoming-sync cases
- timeout/retry behavior for transactions that remain `awaiting_deps`
- audit-grade append-only fate/receipt history

## 12. Row History And Current Projection

For each structural schema version of each application table, the engine creates
an append-only history table.

History rows contain enough data to rebuild current projection:

- logical row id
- transaction id
- branch/view context or source metadata
- operation: insert, update, delete
- application column values
- immutable creation metadata
- update metadata
- conflict metadata or explicit empty conflict state
- engine edit metadata needed for sync and API semantics

Deletes are ordinary append-only history rows. Restore/undelete is also
append-only: restoring a deleted row writes a new transaction/version derived
from preserved deleted-row values rather than erasing or mutating the delete
tombstone. Product API naming and authorization rules for restore are specified
by higher-level APIs, but the storage semantics remain append-only.

Delete is a history row version, not physical removal.

Main must have a current projection for fast ordinary reads. Current projection
rows contain the resolved visible row value plus conflict metadata.

Projection rebuild:

1. ignore rejected transactions
2. consider history visible in the projection's branch view
3. group candidates by logical row
4. apply branch source precedence
5. apply transaction ordering for linear histories
6. preserve concurrent candidates when merge strategy cannot reduce them
7. apply delete semantics

Accepted global transactions are ordered by `(global_epoch, tie_breaker)`,
because several transactions may share a global epoch. Local pending
transactions are ordered by `(node, local_epoch)` only within one node.
Cross-node same-row pending writes are conflict candidates unless a merge rule
resolves them.

Remote pending history must not displace durable accepted/global current state.
It may materialize only when no durable version exists for that row and branch.
Local pending mergeable writes may sort after durable rows for optimistic UX.
Pending exclusive writes are not visible until globally accepted.

If a delete and update are concurrent visible candidates, the reducer must apply
a specified merge/delete rule or preserve candidates. It must not silently pick
one by incidental database row order.

Open issues:

- full concurrent-row merge semantics
- exact conflict metadata shape
- hot branch projection heuristics

## 13. Visibility And Snapshots

Reads are defined by visibility, not by physical storage location.

The baseline read modes are:

- **current projection read**: fast read from a current projection, usually main
- **global epoch snapshot**: accepted history through a global epoch
- **full vector snapshot**: accepted/global/local/dot visibility through a
  closed additive vector
- **branch view read**: read through an explicit branch source list

### 13.1 Current Projection Read

Main current projection is required. Hot branch projections are optional. If no
projection exists for a branch, read through history and branch visibility.

Current reads may include local optimistic mergeable transactions from the
originating runtime. Pending exclusive transactions are not visible until
globally accepted.

When a branch has a pinned base snapshot, its effective current read is:

1. branch-local overlay rows and tombstones
2. otherwise main history at or below the branch base epoch
3. filtered through policy in that same effective context

Latest main state after the branch base is not visible through that branch
unless it is explicitly merged into the branch view.

### 13.2 Global Epoch Snapshot

A global epoch snapshot reads accepted history where:

```text
tx.outcome = accepted
tx.global_epoch <= requested_epoch
```

Rejected and pending transactions are not visible.

### 13.3 Full Vector Snapshot

A full vector snapshot contains:

- global base epoch
- node-local bases
- explicitly included transaction dots

There are no excludes in v0.

A transaction dot is one transaction named precisely, normally by public
transaction id. Dots are used for sparse visibility beyond broad base epochs.

Informative predicate:

```text
visible(tx, snapshot) =
  tx.outcome != rejected
  AND (
    (
      tx.outcome = accepted
      AND tx.global_epoch IS NOT NULL
      AND tx.global_epoch <= snapshot.global_base
    )
    OR (
      snapshot.local_base[tx.node] IS NOT NULL
      AND tx.local_epoch <= snapshot.local_base[tx.node]
    )
    OR tx.tx_id IN snapshot.includes
  )
```

Snapshot vectors should be canonicalized by removing local bases and includes
already covered by the global base. Canonicalization must not change
visibility.

When a local transaction becomes globally accepted, replicas learn:

```text
tx_id -> global_epoch
```

Receivers preserve the public transaction id and may compact future vectors once
the global base covers that global epoch.

Global epoch order is authority order, not complete causality. Causality for
validation and merge decisions comes from persisted observed facts and write
facts.

Remote node-local bases are valid only when the snapshot explicitly names that
remote node coordinate. They are not inferred from the presence of remote
pending history.

Open issues:

- compact vector encoding
- local-to-global upgrade broadcast format
- remote local-coordinate trust rules

## 14. Branch Views

Branches are product-visible objects and engine visibility views. They are not
database copies.

Applications declare branch-backing tables explicitly in schema. A branch has:

- ordinary app-visible backing row
- branch id
- source list
- source precedence
- exact provenance metadata
- policy context

A branch source list is the ordered/provenanced list of other branches whose
visible contents participate in this branch view. Source lists are executable
branch state: they affect reads, writes, sync scope, conflict candidates, and
read-set validation. They are not only explanatory UI metadata.

Branch creation uses a dedicated API that creates the backing row and engine
branch metadata. `db.branch(branchId)` returns a branch-scoped handle and should
fail early if the backing row is not visible under policy.

Branch access has two policy layers:

- can the session see/use/change the branch backing row?
- can the session see or mutate this row through that branch view?

A branch-local transaction may be globally accepted while invisible to main.
Global acceptance means durable/valid history, not visible in every branch.

The v0 branch view shape is:

```text
branch id
source version
sources: [
  { source branch, source snapshot/epoch/vector, precedence }
]
provenance metadata
```

Visible row selection:

```text
for each logical row:
  collect versions visible from the branch source graph
  walk sources transitively; cycles are invalid catalogue state
  apply source-depth precedence:
    branch-local rows shadow direct sources
    direct sources shadow deeper transitive sources
    same-depth candidates remain conflicts
  expose unresolved same-depth candidates until explicitly resolved
  filter deleted winners unless requested
```

Writes use the same graph with stricter base selection. A branch-local write may
use an inherited row as its base only when that row has exactly one effective
candidate after source-depth precedence. If multiple same-depth candidates are
visible, ordinary update/delete must fail as ambiguous; explicit conflict
resolution creates a branch-local row, after which ordinary writes use that
local row as their base.

Branch source lists are mutable authoritative snapshots, not grow-only sets.
Incoming branch records must be replay-ordered, for example by a monotone source
version, so stale sync cannot re-add removed sources. Even a query refresh with
no row history may need to carry branch metadata if the checked-out branch's
source list changed while disconnected.

Baseline branch features:

- branch-backing table declaration
- branch create from main at pinned global epoch
- branch-local writes
- branch reads over overlay plus pinned main base
- branch reads over transitive acyclic source graphs
- branch sync including branch-local rows and base-only rows
- branch policy/write validation against branch overlay plus pinned base
- branch query-scope repair scoped by branch id
- replay-ordered branch source-list mutation

Deferred branch features:

- hot branch projections
- metadata-only merge commits
- product-grade branch merge APIs over multi-source graphs

Branch merge should preferably become a metadata transaction changing branch
sources rather than copying rows. Multi-base conflicts should remain visible
candidates until resolved.

Open issues:

- exact provenance encoding
- user-facing multi-base conflict metadata and resolution workflow
- branch source table layout and source-version encoding
- whether branch-local query repair should use active query-descriptor state,
  predicate history indexes, or both

## 15. Queries And Observed Facts

Queries are relational plans that produce semantic rows and observed facts.

A query plan contains:

- SQL or relational IR
- bindings
- row decoder
- include decoder
- visibility/branch plan
- policy plan
- observed-fact collector
- expected index information when relevant

Includes follow ordinary relational semantics:

- required includes lower to inner joins
- optional includes lower to left joins

If a required include is missing or unauthorized, the parent row is filtered out.
If an optional include is missing or unauthorized, the parent row remains and
the include is null.

Optional missing includes must produce absence facts. A receiver cannot
reproduce an optional-null result from row locators alone. Absence facts are
standing query descriptors while the corresponding subscription/sync session is
active: if the absent row later materializes in the same branch/view context,
refresh should deliver it; if a previously delivered optional include is later
deleted or hidden, refresh should repair the semantic include back to null
without removing the parent row.

Observed fact kinds include:

- result row
- dependency/include row
- absence
- predicate
- range
- policy dependency
- page boundary
- branch/source
- catalogue/schema/lens

Each observed fact records:

- kind
- table/schema identity
- branch view or source context
- row locator or normalized predicate/range
- observed visible transaction/version when applicable
- reason

Observed facts may repeat with different reasons. Sync bundles dedupe concrete
rows/transactions later.

Predicate/range/absence facts must compare by normalized expression, normalized
bound values, table/schema identity, and branch/source context. The exact normal
form is open; until then only planner-supported predicate forms are stable.
Planner-supported predicate forms currently include equality, text contains,
`IN`, `!=`, `!= null` as present optional value, selected semantic system-field
predicates, ordered page descriptors, absence descriptors, and recursive ref
descriptors.

Query-scoped sync must include enough repair information for a receiver that
previously synced the same scope to remove stale rows. Exporting only the
current result rows is insufficient. If a row previously matched `done = false`
and now has `done = true`, the refresh must send the row's new visible version.
If the row was deleted, the refresh must send the tombstone. This is ordinary
history, not an authoritative result snapshot.

The v0 prototype repair strategy for equality predicates is:

1. collect current result rows
2. also collect rows whose local history ever matched the equality predicate
3. export current/history versions for those repair rows
4. attach a predicate observed fact carrying table, field, value, and branch id
5. dedupe concrete history records before encoding the bundle

This strategy is correct enough for the prototype, but may over-export. A
production implementation should use active downstream query descriptors,
predicate indexes, or both so repair candidates can be bounded by actual active
interest, not only by local "ever matched" history.

Query descriptors are the sync/resubscribe unit. They are active session state
owned by the downstream runtime and replayed to upstream peers after reconnect
or upstream restart. Queries should not be persisted to disk as durable user
data; ordinary app clients resubscribe after app restart, and durable cache
tiers/edges learn active interest by downstream replay. Data received for a
query may remain cached after it leaves that query's active result set. Evicting
uninteresting cached data is an asynchronous cache-management concern, not
eager query-scope contraction.

Example: querying open todos includes:

- todo rows that matched `done = false`
- project rows included in the semantic result
- project/member rows needed by policy
- a predicate fact for `done = false`
- ordering/page-boundary facts for `$createdAt`
- the catalogue revision used to decode rows

Open issues:

- relation inference from schema metadata
- compact predicate/range closure
- page-boundary fact shape
- active query-descriptor replay protocol across reconnects and upstream
  restarts
- cache eviction policy for data no longer covered by active query descriptors
- efficient repair candidate discovery for rows that leave predicate/range
  scopes

## 16. Sync Bundles

Sync is query-scoped. It is not table replication.

Given query scope, a sender exports enough data for a receiver with compatible
catalogue and policy context to reproduce the query locally.

Bundles contain:

- transaction records
- transaction outcomes and durability receipts
- branch view/source metadata
- history rows
- observed facts needed for reproduction/invalidation
- catalogue entries when needed
- file/blob metadata and bytes when in scope and authorized

The Attempt 3 bundle shape is:

```text
branches: branch id, base global epoch, source branch ids
txs: tx id, node id, local epoch, global epoch, conflict mode, outcome,
     rejection code, receipt tiers, creation time
reads: transaction row-read facts, currently scoped to exported transaction ids
query_reads: active query descriptors with branch/table/operator/field/value
             plus ordering/window/absence/recursive-ref metadata when needed
history: row versions with branch id, tx id, op, values, and system metadata
```

This is a prototype wire shape, not the final encoding. It captures the product
boundary that matters: public ids and semantic facts cross the wire; physical
ids do not.

Bundles use public ids on the wire. Incoming sync hydrates public ids into local
physical ids before touching hot tables.

Bundles are not authoritative result snapshots. Receivers apply history,
outcome, receipts, branch metadata, and catalogue data, then run queries
locally.

Scope contraction is part of query-scoped sync. When a refreshed query scope no
longer contains a row that the receiver may currently show for that scope, the
bundle must carry enough facts/history to make a local rerun remove it. This can
happen because of updates, deletes, transaction outcome changes, branch source
changes, policy dependency changes, or catalogue/lens changes.

Scope contraction removes the row from that query's semantic result. It does not
require eager deletion of the row from the receiver's local store if another
future local query may use it. Local devices and edges are local-first caches:
they may retain previously learned rows outside active scopes until an
asynchronous eviction policy decides the data is no longer useful or permitted
to keep.

Bundle assembly must dedupe concrete history rows and transaction records even
when the same row is included for multiple reasons: result, dependency, policy,
repair, snapshot base, and branch provenance.

Table-scope and query-scope exports have different obligations. Table-scope
exports include table tombstones needed to converge table replicas. Query-scope
exports include only rows/facts needed by the query, its policy dependencies,
and its repair obligations; they should avoid unrelated tombstone leakage.

Branch-scoped sync carries several provenance classes:

- active branch metadata
- source branch metadata and history needed for source candidates
- pinned main-base snapshot history
- branch-local overlay history and tombstones

If a receiver lacks required catalogue state, it should wait or fail closed. The
query-scoped bundle is not the primary discovery mechanism for an app's
catalogue graph.

Open issues:

- compact reconnect summaries
- exact bundle encoding
- whether future policy dependencies can use opaque proofs
- how much negative/repair information should be represented explicitly versus
  as ordinary history for repair rows
- read-set sync for predicate/range/absence facts; current row read-set sync is
  scoped to transactions whose history is exported
- cache eviction policy and authorization revalidation for retained
  out-of-scope data

## 17. Subscriptions

One-shot queries and live subscriptions share query semantics.

A subscription is a long-lived query interest that keeps previous semantic rows
and observed facts so later changes can be delivered as semantic diffs.

The baseline implementation reruns the query and diffs full semantic rows.
Projection-diff effects may be used as an internal scheduling/invalidation
artifact, but subscription callbacks expose semantic row diffs.

Subscription state includes:

- query plan or query AST
- previous ordered semantic rows
- dependency payloads for included rows
- previous observed facts/scope
- invalidation metadata

Diff categories:

- all
- added
- updated
- removed

Tiered delivery:

- `tier: "local"` may publish local durable state plus local optimistic
  mergeable transactions
- `tier: "edge"` waits until the connected edge has settled contributing state
- `tier: "global"` waits until contributing state is globally settled

One-shot queries with a requested tier wait for the same settled condition as
the first subscription delivery at that tier.

Every subscription update is tier-gated, not only the first result.

A query settled signal means: for this query, branch view, catalogue revision,
policy context, and durability tier, the runtime has applied the row history,
transaction outcomes, durability receipts, branch metadata, catalogue metadata,
and policy facts required to publish the current semantic result.

Rows may arrive before a query is settled. Missing catalogue or sync state that
may still arrive should keep the query unsettled rather than immediately error.
It becomes an error after timeout, cancellation, or irrecoverable failure.

Invalidation may start coarse but must be correct. Useful invalidation facts:

- result/dependency row overlap
- predicate/range overlap
- branch/source changes
- transaction outcome/receipt changes
- catalogue/lens activation changes
- policy dependency changes
- old/new order keys for ordered pages
- column masks for projection/predicate precision

Row-id cursors alone are insufficient for ordered-page invalidation because a
row outside the page may move inside the page when its order key changes.

## 18. Incoming Sync Application

Incoming sync application is semantic, not insert-only.

It should:

1. hydrate public ids to physical ids
2. upsert transaction records
3. upsert outcomes and durability receipts
4. upsert branch/source metadata
5. insert missing history rows
6. insert or update catalogue state when present
7. repair or invalidate affected projections
8. produce projection-diff effects
9. rerun/diff affected subscriptions

Raw history insertion and application-visible effects are different facts. A
received history row may be old, rejected, hidden by branch visibility, or
non-changing for the current projection.

Duplicate incoming sync application must be idempotent.

Incoming transaction fate is merged monotonically. A stale pending or accepted
bundle must not downgrade a rejected transaction; a stale pending bundle must
not downgrade an accepted/global transaction; late global metadata enriches the
same transaction rather than replacing it.

The prototype authority path currently applies an untrusted bundle, validates
pending transactions, rejects invalid ones, and repairs projection. Tests cover
important pollution cases, but the desired production shape is staging
validation before publishing proposal rows into application-visible current
projection.

Receivers are not allowed to trust the sender's query result as final. They
apply transaction/history/fate/fact data, repair or rebuild projections, and
rerun the query locally. Predicate observed facts may be used to repair stale
scope-local projection rows, but correctness still comes from local query
execution.

Downstream runtimes replay active query descriptors to upstream peers after
disconnects and upstream restarts. This replay should trickle upward through
workers, edges, and global services. Queries are not durable disk state; app
restart normally recreates them by resubscribing from application code.

Open issues:

- affected-row discovery should become narrower than broad projection repair,
  but broad repair is acceptable as a correctness baseline
- in-memory receiver-side storage for active query descriptors and scope
  contraction
- whether incoming predicate facts should directly mutate current projection or
  only schedule rerun/repair work
- staged apply/validate/publish pipeline for untrusted authority intake

## 19. Authority Validation

Exclusive transactions must be validated by an authority before global
acceptance.

Authority-visible history is the history visible to the authority in the
transaction's branch view and catalogue/policy context, excluding unaccepted
proposals that are not valid inputs to the validation decision.

Validation checks:

- row reads still observe the same visible version
- absence reads are still absent
- range reads remain valid
- policy dependencies still authorize the operation
- declared constraints remain true

The authority conflict item for exclusive writes is the logical row. Two
exclusive transactions that write different columns of the same row are not
automatically safe merely because column masks are disjoint.

Column masks are auxiliary metadata for:

- mergeable transactions
- conflict UI
- subscription invalidation
- policy/error explanation
- semantic diffs

Persisted transaction read sets should be a canonical subset of observed facts.
Write facts record table/schema identity, row id, operation, write base, and
column masks.

Read/write sets must be typed in memory. Durable encoding should begin inline on
transaction metadata. Hot side tables may be added when quantitative
measurements justify them.

Read-set entry kinds include:

```text
row
absence
range
policy
page_boundary
```

For updates and deletes, the write path must record the previously visible row
version as the write base.

Read/write sets replace explicit parent pointers as the first-order causality
and validation mechanism. Merge operations may need to walk read/write sets and
history; slow merge walks are acceptable initially.

Open issues:

- predicate/range read-set encoding
- validation indexing strategy
- side tables vs inline metadata for hot validation

## 20. Conflict Candidates And Resolution

Current projection rows expose:

- resolved value
- conflict metadata, empty when no conflict is visible

Conflict metadata may contain:

- candidate transaction ids
- candidate values or encrypted opaque values
- changed column masks
- base/read-set information
- resolution metadata

At minimum, durable non-empty conflict metadata identifies the candidate
transactions and whether the stored visible value is resolved or unresolved.
When a conflict is cleared, the history row must carry an explicit cleared
conflict state so rebuild does not resurrect old metadata.

Mergeable transactions may use per-column or per-field metadata to merge
automatically. Exclusive transactions remain row-granular for correctness.

Conflict resolution is an ordinary transaction that reads the conflicted row,
writes the chosen value, records resolved candidates, and clears/updates
conflict metadata.

Open issues:

- candidate ordering
- multi-base branch conflict shape
- per-column UI/conflict metadata shape

## 21. Semantic System Fields

Semantic system fields may be exposed with `$` names:

```text
$rowId
$txId
$createdAt
$updatedAt
$createdBy
$updatedBy
```

`$createdAt` and `$updatedAt` are system fields. Queries must be able to filter
and sort over both user columns and semantic system fields.

Physical application row tables use `j_` engine columns. Pure system tables do
not need the `j_` prefix because all their columns are engine-owned.

User columns whose names collide with the reserved physical prefix are escaped
by the layout codec.

Open issues:

- which semantic system fields are required vs optional
- which fields are queryable, synced, or policy-protected by default

## 22. Product Runtime And Topology

The semantic runtime roles are:

- local replica
- trusted peer / edge
- global authority

Runtime topology changes where storage lives and where queries settle. It must
not change query, write, policy, branch, or sync meaning.

Browser durable mode may use:

- main-thread in-memory runtime
- durable worker runtime
- SharedWorker or tab broker

The main thread may run queries directly against an in-memory core. In durable
browser topology, it talks to the worker/tab broker as a trusted upstream peer.
The worker owns durable storage and upstream sync.

Memory-only runtimes are first-class for tests, demos, and the full distributed
system harness. The important property is controllable topology and
in-memory-ness, not browser APIs.

Edges may permanently reject mergeable transactions when schema validation,
policy evaluation, quotas, or other receive-time checks fail. Edge policy
evaluation may be slightly stale with respect to permission-influencing rows;
that staleness is an accepted product tradeoff for mergeable transactions.

The global authority owns global epochs, exclusive transaction
acceptance/rejection, global durability, and catalogue publication.

Hosted apps have app id, sync URL, global authority placement, optional edge
placement, catalogue heads/revisions, hosted auth configuration, quotas, upload
limits, and observability namespace.

Transport should stay thin. It carries typed sync and catalogue messages; it
does not implement a second query engine.

Reconnect should use replay-window recovery first and full scope/frontier
snapshot fallback when the replay window is insufficient. Active subscriptions
are desired state and should be replayed on reconnect.

Open issues:

- how edges discover policy-influencing rows
- edge policy-readiness/freshness model
- replay-window and reconnect encoding
- SharedWorker/tab-broker ownership handoff
- SQLite WASM startup and binary-size constraints
- OPFS/locality behavior
- React Native/native packaging constraints

## 23. Files, Images, And Binary Data

Files are not part of the relational core in the same way rows are. The core
requirements are:

- rows may reference external blobs
- blob metadata is ordinary policy-controlled relational data
- blob durability may gate transaction publication at a tier
- blob fetch must be authorized through the same session/policy model
- immutable blob chunks may be shared by digest across branches

Applications declare file metadata and chunk/part tables according to Jazz
conventions. File bytes may live in SQLite blobs, OPFS/blob storage, object
storage, filesystem storage, or another byte store.

File content is immutable in v0. Replacing a file creates a new content version.

For now, query-scoped sync may include file bytes when scoped rows reference
files and the receiving session is authorized. Future protocols may use
authorized fetch handles or separate blob transfer.

Deletes or permission changes on owning rows may cascade to file access
according to declared relation semantics. File serving must re-check session and
policy rather than treating stored bytes as public once uploaded.

Open issues:

- conventional schema for file and part tables
- upload limits and validation
- partial/resumable upload protocol
- mutable file/chunk strategy
- whether chunks are ordinary rows or specialized byte-store entries

## 24. Errors And Explanations

Errors are structured, discriminable, and usable from write promises and global
runtime callbacks.

Application-facing surfaces:

- write promise rejection
- transaction outcome rejection
- global rejection/error callback
- subscription error callback
- query failure
- sync connection error

Promise rejection and global callback should receive the same error object shape
for the same transaction outcome.

Errors carry stable machine codes plus human-readable messages. Human messages
may evolve; machine codes are the compatibility surface.

Likely machine-code families:

- `policy_denied`
- `constraint_failed`
- `conflict_rejected`
- `schema_missing`
- `schema_incompatible`
- `catalogue_missing`
- `permission_missing`
- `transport_failed`
- `quota_exceeded`
- `storage_failed`
- `invalid_transaction`
- `exclusive_requires_global`
- `auth_failed`

Transaction rejection details are durable side data keyed by transaction id.
They are not a wide field on the hot transaction row.

Policy denial and validation explanations should be as detailed as safe without
leaking privileged information. Ordinary clients must not distinguish hidden
rows from nonexistent rows through error detail. Trusted-peer and authority logs
should preserve richer details.

For ordinary untrusted clients, policy and validation rejection detail should be
minimal: a stable code such as `permission_denied` and the attempted write that
failed, identified by table, row id, and operation within the transaction.
Details such as hidden dependency row ids, recursive policy paths, and whether a
particular hidden row exists are privileged diagnostics and belong only on
trusted-peer or authority-side surfaces.

Developer diagnostics may be richer and less stable than application errors.
Useful diagnostics include SQL lowering traces, policy lowering traces, missing
index advice, recursive policy unsupported-shape reports, schema/lens graph
errors, generated physical layout explanations, and subscription invalidation
explanations.

Open issues:

- exact stable code taxonomy
- public error object shape
- timeout defaults for unsettled queries/subscriptions
- redaction rules

## 25. Wire/Public Boundary

APIs and wire protocols use public ids.

Hot storage may use physical integer surrogates for:

- nodes
- transactions
- rows
- branches
- tables, schemas, and columns

On export:

```text
physical ids -> public ids -> bundle
```

On incoming sync:

```text
bundle public ids -> physical ids -> embedded database writes
```

Physical ids must not leak into public equality, ordering, persistence, or sync
semantics.

The identity codec should be centralized. SQL-generating subsystems must not
invent ad hoc conversions.

## 26. Embedded Database Lowering

This section describes the selected lowering strategy for SQLite-like embedded
databases.

Physical storage baseline:

- local integer surrogates for hot keys
- integer enum discriminants, not text labels
- composite primary keys with `WITHOUT ROWID` where useful
- generated covering and partial indexes
- current projection for hot main reads
- query-time visibility for historical and branch correctness baselines

### 26.1 Transaction Tables

Sketch:

```sql
CREATE TABLE jazz_tx (
  tx_num INTEGER PRIMARY KEY,
  tx_id TEXT NOT NULL UNIQUE,
  node_num INTEGER NOT NULL,
  local_epoch INTEGER NOT NULL,
  global_epoch INTEGER,
  kind INTEGER NOT NULL,
  conflict_mode INTEGER NOT NULL,
  outcome INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  metadata_blob BLOB NOT NULL,
  UNIQUE (node_num, local_epoch)
);

CREATE TABLE jazz_tx_receipt (
  tx_num INTEGER NOT NULL,
  tier INTEGER NOT NULL,
  observed_at INTEGER NOT NULL,
  authority_node_num INTEGER,
  receipt_blob BLOB,
  PRIMARY KEY (tx_num, tier)
) WITHOUT ROWID;

CREATE TABLE jazz_tx_rejection (
  tx_num INTEGER PRIMARY KEY,
  code INTEGER NOT NULL,
  detail_blob BLOB NOT NULL
);
```

This sketch encodes the v2 split between outcome, durability receipt, and
rejection detail.

`global_epoch` is intentionally not unique. Multiple transactions may share one
authority epoch. Indexes should support lookup/order by `(global_epoch, tx_num)`
or equivalent stable tie-breaker.

### 26.2 History And Current Tables

Sketch:

```sql
CREATE TABLE todos_v1_history (
  row_num INTEGER NOT NULL,
  branch_num INTEGER NOT NULL,
  tx_num INTEGER NOT NULL,
  op INTEGER NOT NULL,

  title TEXT,
  done INTEGER,
  project_row_num INTEGER,

  j_created_at INTEGER NOT NULL,
  j_updated_at INTEGER NOT NULL,
  j_conflict_blob BLOB,
  j_edit_metadata_blob BLOB,

  PRIMARY KEY (row_num, branch_num, tx_num)
) WITHOUT ROWID;

CREATE TABLE todos_v1_current (
  row_num INTEGER NOT NULL,
  branch_num INTEGER NOT NULL,
  visible_tx_num INTEGER NOT NULL,
  is_deleted INTEGER NOT NULL,

  title TEXT,
  done INTEGER,
  project_row_num INTEGER,

  j_created_at INTEGER NOT NULL,
  j_updated_at INTEGER NOT NULL,
  j_conflict_blob BLOB,
  j_edit_metadata_blob BLOB,

  PRIMARY KEY (row_num, branch_num)
) WITHOUT ROWID;
```

### 26.3 Branch View Tables

Sketch:

```sql
CREATE TABLE jazz_branch (
  branch_num INTEGER PRIMARY KEY,
  branch_id TEXT NOT NULL UNIQUE,
  current_head_tx_num INTEGER,
  source_version INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE jazz_branch_history (
  branch_num INTEGER NOT NULL,
  tx_num INTEGER NOT NULL,
  op INTEGER NOT NULL,
  provenance_blob BLOB NOT NULL,
  PRIMARY KEY (branch_num, tx_num)
) WITHOUT ROWID;

CREATE TABLE jazz_branch_source (
  branch_num INTEGER NOT NULL,
  source_ordinal INTEGER NOT NULL,
  source_branch_num INTEGER NOT NULL,
  source_global_epoch INTEGER,
  source_vector_blob BLOB,
  precedence INTEGER NOT NULL,
  provenance_ref_blob BLOB,
  PRIMARY KEY (branch_num, source_ordinal)
) WITHOUT ROWID;
```

### 26.4 Identity Mapping

Logical mappings:

```sql
CREATE TABLE jazz_node (
  node_num INTEGER PRIMARY KEY,
  node_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_row_id (
  row_num INTEGER PRIMARY KEY,
  table_num INTEGER NOT NULL,
  row_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_branch_id (
  branch_num INTEGER PRIMARY KEY,
  branch_id TEXT NOT NULL UNIQUE
);
```

The implementation may combine identity mapping with hot tables when the
public/physical boundary remains clear.

### 26.5 Indexes

Indexes are part of the lowering plan, not handwritten per feature.

The planner should generate:

- point lookup indexes for row identity
- covering indexes for current queries
- covering history indexes for snapshot and branch reads
- partial indexes for selective predicates
- authority-validation indexes when read sets become hot

Example:

```sql
CREATE INDEX todos_v1_current_open_created
  ON todos_v1_current(branch_num, done, j_created_at DESC, row_num);
```

Performance tests should retain `EXPLAIN QUERY PLAN` output for risky lowerings.

Generated indexes must remain compatible with lenses. A covering index generated
for one structural schema may not directly serve another schema view.

Performance risks:

- mapping tables add insert and boundary lookup cost
- inline transaction metadata may become expensive for authority validation
- broad projection repair may be too slow after sync application/rejection
- rerun-and-diff subscriptions may be too coarse for large result sets
- predicate/range scope may become too large
- generated indexes may overfit query shapes and inflate writes

## 27. Security, Privacy, And Encryption

Query-scoped sync can leak information if scope is over-approximated across
authorization boundaries. Enforcing runtimes must evaluate policy before sending
bundles to untrusted clients.

Rejected transactions and history rows remain stored. Implementations must
consider whether rejection reasons or rejected row values are safe to sync.

Per-column end-to-end encryption is the long-term encryption model. Table-level
or row-level E2EE are not the primary target.

Confidentiality classes may include:

- server-readable
- client-decrypted
- encrypted but indexable
- opaque blob

Server-enforced policies must not depend on client-only encrypted fields unless
the field is explicitly server-readable or has a defined index/proof mechanism.

Server-readable values can participate in server-side policy, indexes,
predicates, ordering, sync scope, and authority validation.

Client-decrypted values are stored and synced as opaque encrypted bytes. They
can be queried after local decryption, but an untrusted server or edge cannot
filter, sort, index, or enforce policy over their plaintext.

Sync facts themselves can leak information. Predicate/range/absence facts,
policy dependencies, rejection reasons, and conflict metadata may reveal
information even when row values are encrypted. Future protocols may need
opaque or summarized facts; v0 may send full facts where policy allows.

File content digests should be treated as privacy-sensitive because they leak
equality across branches, users, or sessions.

Conflict metadata for encrypted fields should mark opaque encrypted blobs as
conflicting without exposing plaintext candidate values.

Generated indexes must declare what they leak. They should require columns to be
server-readable or explicitly indexable-encrypted.

Open issues:

- confidentiality metadata syntax in `schema.ts`
- key management and sharing
- encrypted index/proof mechanisms
- policy compiler diagnostics
- encrypted file digest strategy

## 28. Data Export And External Sync

Export, ingest, and external connectors are userland patterns, not core
database semantics.

Ordinary user export should be expressible as normal policy-filtered queries,
optionally with userland expansion for includes, files, or history.

Restore is admin-only and likely expressed through embedded database
snapshotting/restoring plus blob storage backup. Non-admin restore is out of
scope.

External connectors should be built above the core as application or service
code. They may write Jazz transactions using service/admin sessions, source
branches, or application tables, but the core does not prescribe connector
semantics.

Open issues:

- operational backup format for SQLite/native/browser storage
- hosted convenience export APIs built from normal queries

## 29. Platform Bindings And Packaging

Rust is the semantic source of truth for query execution, transactions, sync,
subscriptions, policy evaluation, catalogue application, conflict metadata, and
tiered delivery.

TypeScript and framework packages provide schema/query DSLs, generated types,
tooling integration, and idiomatic UI bindings over those semantics.

Bindings must agree on:

- row and result semantics
- transaction modes, outcomes, and durability receipts
- subscription diff semantics
- tiered query delivery semantics
- policy/session semantics
- branch/source selection
- schema/catalogue/lens interpretation
- conflict metadata shape
- error/rejection shape

Framework integrations should be thin adapters over the same reactive Jazz
client. Jazz's reactive machinery lives in the core/client runtime.

Platform storage choices remain binding-specific:

- browser durable mode: SQLite WASM plus browser storage such as OPFS where
  available
- Node/NAPI and server runtimes: native SQLite through Rust
- React Native/native mobile: native SQLite integration
- edge/global authority runtimes: native Rust SQLite or another embedded
  database behind the same lowering contract

Package boundaries are implementation guidance, not product semantics. The
current Jazz package model is a reasonable starting point.

Open issues:

- SQLite WASM binary size and startup budget
- OPFS availability and fallback behavior
- SharedWorker/tab-broker support
- React Native SQLite packaging
- NAPI/native distribution
- generated TypeScript types and Rust catalogue codec lockstep

## 30. Undefined Areas

The following areas remain intentionally underspecified:

- transaction outcome/receipt encoding
- compact dotted vector encoding
- local-to-global vector upgrade broadcast
- predicate/range scope closure
- query-scope repair candidate bounding
- active query-descriptor replay across reconnects and upstream restarts
- retained-data cache eviction for rows no longer covered by active queries
- authority validation over large read sets
- multi-base branch conflict semantics
- branch provenance encoding
- policy language and recursive policy bounds
- recursive policy lowering performance and diagnostics
- full schema lens semantics
- reconnect summaries
- subscription settlement and reconnection protocol
- hot branch projection heuristics
- audit-grade append-only receipt history
- garbage collection and compaction

## Appendix A: Attempt 3 Implementation Status And Strategy

Attempt 3 is no longer throwaway. It should remain the working prototype until
we learn a reason to restart. Future work may still freely reshape internals,
but the current code has proved enough whole-system composition to be worth
evolving.

All Attempt 3 stores should use SQLite, including memory-only stores. In-memory
means in-memory SQLite, not a parallel fake implementation. This keeps storage
boundaries honest across local tests, browser-like topologies, edge replicas,
and global authority replicas.

The implementation should organize around data artifacts and verbs rather than
manager objects.

Core artifacts:

- `SchemaDef`
- `CatalogueRevision`
- `PhysicalLayout`
- `IdCodec`
- `EnumCodec`
- `TablePlan`
- `ProjectionPlan`
- `VisibilityPlan`
- `QueryPlan`
- `ObservedFacts`
- `ReadSet`
- `WriteSet`
- `SyncBundle`
- `Effect`

Core verbs:

- `lower_schema`
- `open_store`
- `apply_local_write`
- `run_query`
- `export_scope`
- `apply_bundle`
- `validate_at_authority`
- `repair_projection`
- `poll_subscription`

Suggested slices:

1. physical layout, id codec, enum codec, and DDL
2. local write/query/current projection
3. deterministic projection rebuild
4. observed facts and query scope
5. subscriptions
6. sync export/apply
7. authority validation
8. branch visibility
9. historical snapshots
10. conflict candidates
11. narrow but real policies
12. narrow but real lenses

Implemented slices so far:

- SQLite-backed in-memory and file-backed runtimes
- schema-driven DDL for narrow structural schemas
- local writes, generic transactions, updates, deletes, and current projection
- deterministic projection rebuild from history and transaction fate
- public ids with local physical surrogates
- transaction fate, edge/global receipts, rejection repair, and idempotent sync
- query-scoped sync bundles using public ids
- branch metadata, branch-local writes, pinned main base snapshots, and sparse
  overlays
- branch provenance sync for simple branch sources
- equality query lowering with predicate observed facts
- query-scope repair for rows that leave equality predicates via update or
  delete
- one-shot subscriptions via rerun-and-diff semantic row diffs
- narrow read/write policies, including ref-readable policies
- transitive policy read-set recording for recursive write policies
- trusted edge validation of untrusted bundles
- recursive query reads over self refs
- recursive query-scope export of deleted subtrees
- cycle rejection and bounded acyclic recursive policy lowering
- narrow schema lenses for renamed fields and refs
- system-column prefix escaping

Tests should be product-shaped integration tests using projects, todos, Alice,
Bob, and a core authority.

The full distributed system harness should support memory-only topologies using
in-memory SQLite so tests can run several local/edge/global runtimes without
browser-specific APIs. It should also support durable SQLite-file nodes in the
same topology so crash safety and reconciliation can be tested.

Performance tests should measure layout overhead, id representation, enum
representation, read/write-set storage, query plans, and memory representation.

Attempt 3 should bias toward whole-system tests over narrow helper tests. The
goal is to learn whether the semantic model composes under realistic distributed
conditions, not only whether individual SQL statements work.

Recommended harness shape:

- create several SQLite-backed runtimes in one process
- mix in-memory SQLite nodes and durable SQLite-file nodes
- assign each runtime a node id, principal/session, catalogue revision, and
  optional upstream peer
- support local, edge, and global roles
- allow explicit message passing rather than hidden synchronous replication
- allow dropped, delayed, duplicated, and reordered bundles
- expose query/subscription observations as testable events
- expose transaction outcomes, receipts, observed facts, and projection diffs
- provide deterministic clocks/epochs for repeatable tests
- support crash/reopen of durable nodes
- support disconnect/reconnect and replay-window/full-snapshot recovery

The first harness should be boring and explicit. It does not need production
transport, threads, async scheduling, or browser APIs. It does need SQLite from
the start, clean boundaries between runtime, storage, sync, policy, and query
planning, and enough topology to prove that local replicas, trusted peers/edges,
and the global authority keep the same invariants when messages move in
uncomfortable orders.

The harness should mirror browser-plus-cloud product topology early:

- browser main-thread-like in-memory SQLite runtime
- browser worker/tab-broker-like durable SQLite runtime
- optional edge SQLite runtime
- global authority SQLite runtime

Attempt 3 should include policies and lenses, even if the first slices are
narrow. The goal is to prove that the whole system composes, not to defer the two
features most likely to change scope, query planning, validation, and sync.

Implementation lessons from Attempt 3:

- The useful architecture is verb-shaped: write, validate, apply, export,
  repair, read, subscribe. Thin data artifacts are useful, but manager-style
  abstractions should not become the design center.
- SQLite is a good semantic substrate for the prototype. Recursive CTEs,
  transactions, projection tables, and ordinary indexes are already carrying
  real Jazz semantics.
- Correctness depends on making read contexts explicit. The same logical policy
  must evaluate against main current, branch overlay, pinned base snapshot, or
  historical snapshot depending on the operation.
- Query-scoped sync needs repair semantics from the beginning. A bundle cannot
  merely export current result rows and hope the receiver removes stale rows.
- Read/write sets are becoming the bridge between policy, validation,
  replayability, causality, and future conflict explanation.
- Whole-system tests are more valuable than narrow helper tests for this design:
  most important bugs appeared only when branch snapshots, policies, sync, and
  query scopes were composed.

Known implementation tensions:

- Query-scope repair currently uses local history that ever matched a supported
  equality predicate. This is correct for the prototype but can over-export.
- Projection repair is intentionally broad in several incoming-sync paths.
- Recursive policy/query lowering works for narrow acyclic cases, but helper
  SQL is duplicated and needs consolidation.
- Exclusive transaction conflict handling is row-coarse for write conflicts, but
  versioned read/write-set validation now covers several row, absence, policy,
  branch-source, update, and delete cases. Predicate/range validation remains
  incomplete.
- Branch source/provenance now has executable transitive source graphs,
  source-depth precedence, source-list replay ordering, and conflict behavior,
  but product branch backing-row permissions and merge APIs remain incomplete.
- Active query descriptors now drive reconnect refresh and subscription
  recovery in the prototype. They should be replayed by downstream clients
  rather than persisted as durable query state.
- Lenses are currently field-level storage-name mappings for text/ref renames.
  There is no schema-versioned catalogue, inverse lens graph, compatibility
  graph, or copy-forward storage yet; physical table names are still
  `schema_v1`.
- Conflict candidates are exposed through side APIs and conflict-aware row
  reads; product conflict metadata shape and resolved-from-candidates
  provenance remain incomplete.
- Predicate facts now cover equality, contains, IN, not-equal, null-present,
  selected system fields, ordered pages, absence, and recursive refs in the
  prototype. Range predicates and a final compact predicate model remain open.
- Recursive query reads use two strategies: current projection can use recursive
  CTEs, while pinned branch snapshot reads may fall back to in-memory traversal
  over already visible rows. This is a correctness-first shortcut, not the final
  planner shape.
- Receipt representation is minimal. Receipt tiers and timestamps exist, but
  authority identity, signatures, and detailed receipt payload semantics remain
  open.
- Trusted/admin policy bypass exists in the harness, but audit/provenance
  semantics for bypassed writes are thin.

## Appendix B: Rationale

Append-only history plus rebuildable projections handles rejection repair,
restart/rebuild, sync replay, and historical reads with one source of truth.

Outcome plus durability receipts is preferred over a single overloaded fate enum
because local pending, edge durability, global acceptance, and rejection are
different axes.

Local integer surrogates and integer enum discriminants are the physical
baseline because repeated text ids and string enums are expensive in hot rows.

Query-scoped sync is preferred over table replication because clients should
receive the history/facts needed for active queries, not unrelated table state.

Rerun-and-diff subscriptions are the correctness baseline because one-shot and
live query semantics stay aligned.

Most file/blob behavior is kept as a blob adapter contract because otherwise
the spec grows a second storage system beside the relational core.

## Appendix C: Future Revisits

Future work may revisit:

- fixed-width binary public ids
- append-only audit receipts
- hot branch projections
- indexed read/write-set side tables
- custom SQLite VFS/page compression
- opaque policy proofs
- compact encrypted indexes
- query-scope repair via durable observed-fact indexes rather than broad
  "ever matched" scans
- consolidating snapshot/effective-branch SQL builders into one read-context
  lowering layer

## Appendix D: Invariants To Test

Attempt 3 should turn as many of these as practical into integration tests. A
few may remain assertion-level checks or design review items until the relevant
feature exists.

### D.1 Identity Invariants

- Public row ids are stable across replicas.
- Public transaction ids are stable across local-to-global acceptance.
- Physical ids never cross API or sync boundaries.
- Rehydrating the same public id on one replica returns the same physical id.
- Different replicas may assign different physical ids to the same public id.
- Logical row ids are globally unique.
- Node ids are writer identities, not authorization principals.
- One principal may write from multiple nodes.

### D.2 Transaction Invariants

- One simple write creates one sealed transaction.
- One explicit transaction may contain multiple row mutations and still seals as
  one transaction.
- A sealed transaction is immutable except for outcome/receipt enrichment.
- Authority acceptance enriches an existing transaction instead of replacing its
  public id.
- Rejection preserves the transaction record and history rows.
- Rejection details live outside the hot transaction row.
- Mergeable transactions may publish optimistically at local tier.
- Pending exclusive transactions are not visible until globally accepted.
- Waiting on an exclusive transaction at local or edge tier is a runtime error.
- Waiting on an exclusive transaction at global tier resolves on acceptance and
  rejects on rejection.
- Edge-accepted mergeable transactions produce replayable receipt state.
- Edge-accepted mergeable transactions are accepted and visible without a
  global epoch.
- Later global acceptance enriches the same public transaction id.
- Rejected outcome is terminal for ordinary visibility.
- Stale incoming fate cannot downgrade accepted/global or rejected state.
- Multiple transactions may share one global epoch.
- Transaction info APIs expose outcome, rejection, receipts, and global epoch
  consistently after sync.
- Duplicate incoming transaction records are idempotent.

### D.3 History And Projection Invariants

- History rows are append-only for application state.
- Deletes are history versions, not physical history removal.
- Main current projection is rebuildable from history plus transaction
  outcome/receipts.
- Rebuilding a projection twice from the same inputs is byte-for-byte
  deterministic where the physical format is deterministic.
- If current projection and history disagree, rebuild from history wins.
- Rejected history rows do not appear in ordinary reads.
- Projection repair after rejection removes rejected visible state.
- Projection repair after late acceptance can make previously hidden state
  visible.
- Local current projection may include local optimistic mergeable writes.
- Remote pending history cannot displace a durable accepted/global current row.
- Remote pending history may materialize only when no durable row version exists
  for that row and branch.
- Durable/global ordering uses `(global_epoch, tie_breaker)`, not global epoch
  alone.
- Cross-node concurrent same-row pending writes are conflicts unless merge
  strategy resolves them.
- Incidental SQLite row order never decides visible conflict winners.

### D.4 Visibility And Snapshot Invariants

- Current projection reads and historical snapshot reads have distinct semantics.
- Global epoch snapshots include only accepted transactions at or below the
  requested global epoch.
- Rejected and pending transactions are excluded from global epoch snapshots.
- Full vector snapshots include global base, explicit local bases, and explicit
  dots.
- Full vector snapshots have no excludes in v0.
- Remote local bases are valid only when explicitly named in the snapshot.
- Remote pending history does not imply remote local-base visibility.
- Vector canonicalization does not change visible rows.
- Learning `tx_id -> global_epoch` never changes public transaction identity.
- Global epoch order is authority order, not full causality.
- Causality-sensitive validation uses observed/read facts and write facts.

### D.5 Branch Invariants

- Branches are visibility views over shared history, not copied databases.
- Branch creation creates both backing row and engine branch metadata.
- A branch handle cannot be used when the backing row is not visible under
  policy.
- Branch access checks both backing-row permission and row/version permission
  through the branch view.
- A branch-local transaction may be globally accepted while invisible to main.
- Main visibility does not automatically include branch-local history.
- Branch reads use source precedence, not incidental storage order.
- Branch source reachability is transitive and acyclic.
- Branch source depth is precedence: nearer sources shadow deeper sources,
  while same-depth candidates remain conflicts.
- Ordinary branch writes over unresolved same-depth candidates fail as
  ambiguous; explicit conflict resolution creates a branch-local base row.
- Branch-local writes use the same logical row ids as main by default.
- Branch source/provenance changes are ordinary authorized metadata
  transactions.
- Branch sync includes branch metadata as well as visible row history.
- Branch metadata includes base global epoch and source branch ids; row
  `branch_id` alone is insufficient for branch reproduction.
- Base-only rows needed for branch query results are included in branch sync.
- Branch-local tombstones over pinned-base rows prevent base rows from
  reappearing in the branch view.
- Rejected branch overlays fall back to the pinned base when a base candidate
  exists.
- Pinned branch reads use branch overlay plus base snapshot, not latest main.
- Pinned branch write-policy validation uses branch overlay plus base snapshot
  for referenced policy rows.
- Pinned branch policy read-set recording records base-snapshot dependencies
  when no branch overlay exists.
- Edge validation of untrusted branch writes reproduces the same pinned-base
  policy decision from synced branch/base history.
- Branch query-scope repair is scoped by branch id.
- A branch delete of a pinned-base row exports a branch tombstone sufficient to
  repair peer recursive reads.

### D.6 Query And Observed-Fact Invariants

- One-shot queries and subscriptions share query semantics.
- Queries return semantic rows and observed facts.
- Required includes filter out the parent when missing or unauthorized.
- Optional includes preserve the parent and return null/absent when missing or
  unauthorized.
- Optional missing includes produce absence facts.
- Policy dependencies are observed facts distinct from result dependencies.
- Rows needed only for policy do not automatically appear in semantic results.
- Predicate, range, absence, page-boundary, branch/source, and catalogue facts
  are represented when needed for correctness.
- Observed facts can carry multiple reasons for the same concrete row.
- Bundle locators dedupe concrete rows/transactions even when facts repeat.
- Normalized predicates/ranges compare deterministically for supported planner
  forms.
- Query-scope refresh repairs rows that leave a predicate through an update.
- Query-scope refresh repairs rows that leave a predicate through a delete by
  sending tombstone history.
- Query-scope export includes predicate observed facts with table, field, value,
  and branch context for supported predicates.
- Query-scope repair rows may be included even when they are no longer semantic
  result rows.
- Query-scope export dedupes concrete history records included for several
  reasons.
- Recursive query-scope export includes deleted descendant subtrees, not only
  direct deleted children.

### D.7 Sync Invariants

- Sync is query-scoped, not table replication.
- Bundles use public ids.
- Applying the same bundle twice is idempotent.
- Bundle application hydrates public ids before touching hot tables.
- Bundles are not authoritative result snapshots.
- Receivers apply history/outcomes/receipts/facts and rerun queries locally.
- A receiver lacking required catalogue state waits or fails closed.
- Out-of-order history and outcome delivery eventually converges after all
  required facts arrive.
- Duplicate, delayed, and reordered bundles do not create duplicate history.
- Reconnect replays desired subscriptions.
- Reconnect uses replay-window recovery before full scope/frontier fallback.
- Scope contraction removes or invalidates stale rows.
- Scope contraction is represented with enough ordinary history/facts for the
  receiver to rerun locally; bundles are not authoritative result snapshots.
- Rows that leave scope because of update, delete, policy change, branch source
  change, outcome change, or catalogue/lens change eventually disappear from
  local query results after relevant repair data arrives.

### D.8 Subscription Invariants

- Subscription first delivery equals the corresponding one-shot query at the
  same tier.
- Subscription updates are semantic row diffs.
- Dependency-only changes can update parent semantic rows.
- Every subscription update is tier-gated.
- Rows may arrive before query settlement without being published.
- Missing sync/catalogue state leaves a query unsettled until timeout or
  irrecoverable failure.
- Rejections that change visible results produce semantic diffs.
- Rejected unawaited writes surface through the global rejection/error callback.
- Ordered-page invalidation considers old and new order keys, not only row ids.

### D.9 Policy Invariants

- Policy sees the same session context across local, worker, edge, and global
  evaluation.
- Non-admin sessions fail closed when policy metadata is missing.
- Admin sessions bypass row policy but remain auditable sessions.
- Trusted peers may read applied policy-scoped facts without an end-user
  principal when acting as infrastructure.
- Read policy affects query results and sync delivery.
- Insert/update/delete policy affects transaction acceptance.
- Delete may fall back to update semantics where explicit delete rules are not
  yet available.
- Policy failures do not reveal whether a hidden row exists to ordinary clients.
- Trusted peer and authority logs may contain more detail than client errors.
- Edge policy may be stale for mergeable transactions only within the accepted
  product tradeoff.
- Exclusive transactions are validated by global authority against
  authority-visible history and policy facts.
- Recursive policies over acyclic ref chains are SQL-lowerable.
- Direct and indirect recursive policy cycles are rejected.
- Write policies record transitive policy read facts, not only direct parent
  rows.
- Policy evaluation and policy read-set recording use the same read context.
- Historical snapshot policy evaluates referenced parents at the same snapshot
  epoch recursively, not through current projection.
- Branch-local parent rows override base parents for branch policy checks.
- `write_if_created_by_principal` allows self-authored inserts and preserves
  original `created_by` on updates.
- Updates and deletes record the previously visible row as a read dependency.
- Partial updates preserve omitted fields when constructing the proposed row for
  write-policy validation, including omitted refs used by policy checks.
- Ref-retarget updates validate the proposed row against policy dependencies
  reached through the new ref target, and a denied retarget leaves the previous
  visible ref intact.
- A policy-denied local delete records the rejection and repairs current,
  query, and subscription-visible state back to the previously authorized row.
- Multi-row transactions reject atomically when any row mutation fails local
  write-policy validation, while preserving write-set history for the rejected
  transaction.
- Trusted/admin writes may bypass user row policies while preserving explicit
  author/provenance attribution.

### D.10 Catalogue And Lens Invariants

- `permissions.ts` is required even when empty.
- Missing permission bundles do not imply permissive behavior.
- Catalogue publication is admin/core controlled.
- Catalogue sync is a separate lane from ordinary query-scoped row sync.
- Runtime work references a catalogue revision.
- Explicit indexes and merge strategies are part of the schema hash.
- Index-only and merge-strategy-only schema changes derive automatic lens
  compatibility.
- Lenses live in `migrations/`.
- Lenses used by v0 are SQL-lowerable.
- Writes through an old schema view append current-schema history.

### D.11 Authority Validation Invariants

- Authority validation uses authority-visible history, not optimistic current
  projections polluted by proposals.
- Row reads still observe the same visible version at validation time.
- Absence reads remain absent at validation time.
- Range reads remain valid at validation time.
- Policy dependencies still authorize the operation at validation time.
- Exclusive write conflict items are logical rows.
- Two exclusive writes to different columns of the same row are not
  automatically safe.
- Column masks are auxiliary metadata for merge, UI, invalidation, explanation,
  and semantic diffs.
- Updates and deletes record the previously visible row version as write base.
- Read/write sets replace explicit parent pointers for v0 causality and
  validation.

### D.12 Conflict Invariants

- Current projection exposes a resolved value plus conflict metadata.
- Empty conflict metadata is represented explicitly enough for rebuild.
- Non-empty conflict metadata identifies candidate transactions.
- Conflict resolution is an ordinary transaction.
- Conflict resolution records resolved candidates and clears/updates conflict
  metadata.
- Mergeable transactions may use per-column merge metadata.
- Exclusive transactions remain row-granular for correctness.
- Encrypted conflicting values are represented as opaque conflicting blobs, not
  plaintext candidate values.

### D.13 Error And Diagnostic Invariants

- Write promise rejection and global rejection callback use the same error shape
  for the same transaction outcome.
- Errors carry stable machine codes plus human-readable messages.
- Transport/quota/upload capacity failures are transport/API errors.
- Semantic database failures are transaction/query errors or rejections.
- Recoverable catalogue/sync gaps are unsettled state before timeout, not
  immediate errors.
- Developer diagnostics can be richer and less stable than public errors.

### D.14 Storage And Lowering Invariants

- Hot paths use local integer surrogates for repeated public ids.
- Hot enum fields use integer discriminants.
- Runtime can install and use schemas that are not the todo fixture; fixture
  helpers do not define core semantics.
- Composite-key hot tables use `WITHOUT ROWID` unless benchmarks show a
  regression.
- Generated indexes come from schema/query intent.
- Generated indexes declare confidentiality leakage.
- Physical application row columns use `j_` engine names.
- Pure system tables do not need `j_` prefixes.
- User columns colliding with physical prefixes are escaped by the codec.
- SQL fragments and bind parameters travel together in implementation plans.
- The identity codec is centralized.

### D.15 File/Blob Invariants

- Blob metadata is ordinary policy-controlled relational data.
- Blob bytes do not bypass Jazz session or policy checks.
- Blob durability may gate transaction publication at a tier.
- File content is immutable in v0.
- Replacing a file creates a new content version.
- Immutable chunks may be shared by digest across branches.
- File serving re-checks session and policy.
- Deletes or permission changes on owning rows may cascade to blob access.

### D.16 Privacy Invariants

- Server-readable fields may participate in server-side policy, indexes,
  predicates, ordering, sync scope, and validation.
- Client-decrypted fields cannot be used by untrusted servers/edges for
  plaintext filtering, sorting, indexing, or policy.
- Sync facts can leak information and must be policy-aware.
- File content digests are privacy-sensitive.
- Generated indexes require server-readable or explicitly indexable-encrypted
  columns.

### D.17 Harness Invariants

- Multi-runtime tests can run against SQLite only; memory-only nodes use
  in-memory SQLite rather than a fake store.
- Multi-runtime tests can mix in-memory SQLite nodes and durable SQLite-file
  nodes.
- Tests can model local, edge, and global roles.
- Tests can delay, duplicate, drop, and reorder messages.
- Tests can inspect public events without relying on physical ids.
- Tests can rebuild projections and compare semantic state.
- Tests can assert query settled vs row-received distinctions.
- Deterministic clocks/epochs make failures reproducible.
- Durable nodes survive close/reopen with transaction records, history,
  projections, observed facts needed for recovery, catalogue state, and sync
  frontier state intact.
- In-memory nodes lose local non-synced state on restart unless that state has
  been synced to a durable peer.
- Browser-like main-thread in-memory nodes can reconcile from a durable
  worker/tab-broker node after restart.
- Durable worker/tab-broker nodes can reconcile with edge/global after
  disconnect.
- Edge nodes can reconcile with global after disconnect and preserve replayable
  mergeable transaction receipts.
- Global authority restart preserves global epochs, transaction outcomes,
  catalogue publication state, and validation history needed for correctness.
- After crash/reopen, projections are either intact or rebuildable from history
  and transaction outcomes/receipts.
- After disconnect/reconnect, subscriptions replay desired state and republish
  only settled semantic results.
- Message replay after reconnect is idempotent across durable and in-memory
  receivers.
- Crash at any explicit embedded transaction boundary leaves the SQLite database
  in a valid state.
- Crash after local write before upstream sync preserves durable local writes on
  durable nodes and drops them on purely in-memory nodes.
- Crash after receiving history before receiving outcome/receipt leaves queries
  unsettled or correctly pending, not incorrectly visible.
- Crash after outcome/receipt before projection repair repairs or rebuilds
  projection on reopen.
- Policy and lens state survive durable restart through catalogue state, not
  ambient process memory.

## Appendix E: Attempt 3 Test Traceability

This appendix maps the current `crates/mini-jazz-sqlite/tests/whole_system`
suite to the invariant groups in Appendix D. It is intentionally coarser than a
formal coverage database: one test may exercise several invariants, and one
invariant may require several tests before it is convincing.

Coverage labels:

- **covered**: at least one whole-system test directly exercises the invariant
- **partial**: tests exercise a narrow prototype shape, but not the full product
  invariant
- **untested**: no obvious Attempt 3 test covers it yet

### E.1 Coverage Summary By Invariant Group

| Group                      | Current status        | Notes                                                                                                                                                                                                                                              |
| -------------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| D.1 Identity               | covered for prototype | Public row ids, physical id locality, replica-local physical ids, and one principal writing from multiple nodes are tested.                                                                                                                        |
| D.2 Transactions           | partial               | Sealing, explicit transactions, edge/global receipts, rejection, idempotence, non-unique global epochs, and monotonic fate are tested. Awaiting-dependencies semantics and audit-grade receipt history are not.                                    |
| D.3 History/projection     | covered for prototype | Append-only deletes, rebuild, rejection repair, global ordering, remote pending constraints, and broad repair are tested. Full merge/conflict projection semantics remain partial.                                                                 |
| D.4 Visibility/snapshots   | partial               | Global epoch and pinned branch snapshot behavior is tested. Full vector snapshots are not implemented/tested.                                                                                                                                      |
| D.5 Branches               | covered for prototype | Branch overlay/base reads, branch tombstones, rejected overlay fallback, provenance sync, multi-source conflict candidates, and branch policy contexts are tested. Full product branch backing rows and merge commits are not.                     |
| D.6 Queries/observed facts | partial               | Equality, contains, IN, not-equal, null-present, selected system fields, ordered pages, absence facts, recursive query scopes, policy dependencies, query-scope repair, and predicate serialization are tested. Range and catalogue facts are not. |
| D.7 Sync                   | partial               | Query-scoped sync, table-vs-query scope, idempotence, public id hydration, reordered fate, scope contraction, active query refresh, and reconnect-shaped repair are tested. Compact reconnect summaries are not.                                   |
| D.8 Subscriptions          | partial               | Rerun-and-diff, policy dependency diffs, branch checkout diffs, pinned branch stability, pagination, and reconnect-shaped observed subscription recovery are tested. Tier gating and settled state are not.                                        |
| D.9 Policies               | covered for prototype | Read/write policies, ref-readable policies, recursive acyclic policies, cycle rejection, branch/pinned-base contexts, trusted bypass, and transitive policy read sets are tested. Full policy language and diagnostics are not.                    |
| D.10 Catalogue/lenses      | partial               | Narrow storage-name rename lenses, ref lenses, system prefix escaping, and index-only compatibility are tested. Catalogue revision graph, migrations directory semantics, inverse lenses, and copy-forward are not.                                |
| D.11 Authority validation  | partial               | Untrusted bundle rejection, atomic rejection, delete/update validation, branch-context validation, stale row/absence/policy/source read-set checks, exclusive same-row conflict, and repair are tested. Predicate/range validation is not.         |
| D.12 Conflicts             | partial               | Side APIs expose multi-base and pinned-base conflict candidates and policy-filtered candidates; conflict-aware row reads and resolution transactions are tested. Product metadata shape is not.                                                    |
| D.13 Errors/diagnostics    | partial               | Rejection codes, transaction info, rejection lists, rejection subscriptions, and detail enrichment are tested. Public error object shape, redaction, and diagnostics are not.                                                                      |
| D.14 Storage/lowering      | partial               | SQLite current/history tables, physical ids, system prefix escaping, integer-like enum behavior, and rebuild are exercised. `WITHOUT ROWID`, generated indexes, and query plans are not asserted.                                                  |
| D.15 Files/blobs           | untested              | No file/blob implementation in Attempt 3.                                                                                                                                                                                                          |
| D.16 Privacy/encryption    | untested              | No E2EE/encrypted-index implementation in Attempt 3.                                                                                                                                                                                               |
| D.17 Harness               | partial               | In-memory SQLite, file-backed durable nodes, multi-runtime local/edge/global tests, duplicate/reordered bundles, and durable reopen are tested. Drop/delay/reconnect protocol and deterministic clock APIs are not.                                |

### E.2 Test Module Mapping

#### `storage_projection.rs`

- `memory_runtime_writes_through_sqlite_current_projection`: D.3, D.14, D.17
- `durable_nodes_survive_reopen_but_memory_nodes_start_empty`: D.17, D.3
- `rebuild_current_projection_from_history_matches_current_reads`: D.3, D.14
- `delete_is_history_not_removal`: D.3

#### `transactions.rs`

- `explicit_transaction_seals_multiple_mutations_atomically`: D.2
- `generic_transaction_seals_multiple_rows_atomically`: D.2
- `generic_transaction_can_seal_updates_atomically`: D.2, D.3
- `generic_update_records_previous_row_read_set`: D.9, D.11
- `generic_transaction_can_seal_delete_with_other_mutations`: D.2, D.3
- `exclusive_transaction_requires_global_epoch_and_commits_accepted`: D.2,
  D.11
- `exclusive_transaction_mode_survives_sync`: D.2, D.7
- `authority_acceptance_enriches_existing_transaction`: D.2
- `generic_transaction_delete_records_previous_row_read_set`: D.9, D.11
- `exclusive_transaction_rejects_same_row_conflict`: D.11, D.12
- `generic_transaction_delete_shadows_pinned_base_row`: D.5, D.3
- `global_epoch_can_accept_multiple_transactions`: D.2, D.3

#### `sync_fate.rs`

- `query_scoped_sync_converges_memory_and_durable_nodes`: D.7, D.17
- `rejected_transaction_remains_history_but_is_hidden_from_current`: D.2, D.3
- `rejected_fate_update_repairs_peer_current_projection`: D.2, D.3, D.7
- `durable_worker_reconciles_rejected_fate_after_restart`: D.17, D.2, D.3
- `rejecting_generic_transaction_repairs_schema_driven_projection`: D.3, D.7
- `table_scope_sync_exports_delete_so_peer_removes_row`: D.3, D.7
- `same_bundle_twice_is_idempotent`: D.7
- `replicas_may_use_different_physical_ids_for_same_public_ids`: D.1
- `query_scope_is_not_table_replication`: D.7, D.6
- `query_scope_excludes_rows_outside_current_result_set`: D.7, D.6
- `accepted_global_fate_update_reaches_peer_transaction_info`: D.2, D.7
- `stale_pending_bundle_does_not_downgrade_accepted_fate`: D.2, D.7
- `out_of_order_global_epochs_do_not_regress_current_projection`: D.3, D.7
- `rebuild_uses_global_epoch_order_not_local_tx_order`: D.3
- `direct_global_acceptance_repairs_current_projection_order`: D.3
- `remote_pending_update_does_not_override_global_current_on_peer`: D.3
- `accepted_remote_pending_update_repairs_peer_current_projection`: D.3, D.7
- `accepted_bundle_does_not_resurrect_rejected_fate`: D.2, D.7
- `direct_accept_after_reject_preserves_rejected_outcome_with_global_metadata`:
  D.2
- `direct_reject_after_accept_removes_current_but_preserves_global_metadata`:
  D.2, D.3

#### `branches.rs`

- `branch_local_write_is_invisible_on_main`: D.5
- `branch_scoped_export_excludes_unrelated_branch_rows`: D.5, D.7
- `branch_scoped_export_excludes_unrelated_deleted_rows`: D.5, D.7
- `branch_reads_main_base_with_sparse_overlay`: D.5, D.4
- `fixture_open_todos_reads_pinned_base_with_sparse_overlay`: D.5, D.4
- `branch_base_is_pinned_to_global_epoch`: D.5, D.4
- `branch_base_snapshot_chooses_latest_row_version_within_same_global_epoch`:
  D.4, D.5
- `branch_delete_shadows_pinned_base_row`: D.5, D.3
- `rejected_branch_update_reveals_pinned_base_row_again`: D.5, D.3
- `rejected_branch_delete_reveals_pinned_base_row_again`: D.5, D.3
- `branch_export_includes_pinned_main_base_rows_for_receiver_view`: D.5, D.7
- `branch_base_snapshot_respects_deletes_and_excludes_pending_main`: D.4, D.5
- `branch_base_snapshot_applies_row_policy`: D.4, D.5, D.9
- `branch_base_snapshot_ref_policy_uses_parent_at_base_epoch`: D.4, D.5, D.9
- `branch_ref_policy_uses_branch_local_parent_visibility`: D.5, D.9
- `branch_equality_query_uses_effective_branch_policy`: D.5, D.6, D.9
- `branch_base_export_preserves_ref_policy_at_base_epoch`: D.5, D.7, D.9
- `branch_multi_base_conflicts_expose_multiple_candidates`: D.5, D.12
- `branch_conflict_candidates_include_pinned_base_candidate`: D.5, D.12
- `branch_source_metadata_survives_sync`: D.5, D.7
- `branch_conflict_candidates_respect_effective_row_policy`: D.5, D.9, D.12
- `branch_conflict_candidates_survive_durable_sync_and_rejected_fate`: D.5,
  D.12, D.17
- `branch_sync_preserves_branch_provenance`: D.5, D.7
- `branch_transitive_conflict_resolution_survives_sync`: D.5, D.7, D.12
- `durable_reopen_preserves_branch_sync_and_dedupes_replay`: D.5, D.7, D.17

#### `generic_schema.rs`

- `runtime_can_install_and_write_a_non_todo_schema`: D.14
- `generic_schema_rows_rebuild_and_sync_by_public_ids`: D.1, D.3, D.7
- `generic_equality_query_scope_exports_matching_rows_and_policy_dependencies`:
  D.6, D.7, D.9
- `equality_query_scope_resync_removes_row_that_left_predicate`: D.6, D.7
- `equality_query_scope_resync_removes_deleted_matching_row`: D.6, D.7
- `branch_equality_query_scope_records_branch_predicate_read`: D.5, D.6
- `branch_equality_query_scope_resync_repairs_row_that_left_predicate`: D.5,
  D.6, D.7
- `query_predicate_reads_survive_bundle_serialization`: D.6, D.7
- `generic_equality_query_lowers_public_ref_ids_to_physical_row_ids`: D.1,
  D.6, D.14
- `generic_update_records_update_op_and_syncs_current_value`: D.2, D.3, D.7

#### `policies.rs`

- `policy_filters_reads_through_required_parent_ref`: D.6, D.9
- `policy_scoped_sync_includes_required_parent_rows_only`: D.6, D.7, D.9
- `trusted_peer_can_read_applied_policy_scoped_facts_without_user_principal`:
  D.7, D.9
- `trusted_peer_generic_transaction_bypasses_user_write_policy`: D.9
- `trusted_edge_accepts_mergeable_tx_then_untrusted_peers_enforce_policy`:
  D.2, D.7, D.9
- `trusted_edge_acceptance_syncs_without_global_epoch`: D.2, D.7
- `edge_accepted_transaction_can_upgrade_to_global_epoch`: D.2
- `trusted_edge_rejects_policy_violating_tx_and_syncs_reason`: D.2, D.9,
  D.13
- `trusted_edge_authoritatively_rejects_untrusted_policy_violation_on_apply`:
  D.9, D.11
- `trusted_edge_rejects_untrusted_transaction_atomically`: D.2, D.9, D.11
- `trusted_edge_rejects_untrusted_update_to_unreadable_ref`: D.9, D.11
- `branch_write_policy_does_not_use_parent_from_different_branch`: D.5, D.9
- `branch_write_policy_uses_parent_visible_from_pinned_base`: D.5, D.9
- `branch_recursive_write_policy_uses_parent_state_from_pinned_base`: D.5,
  D.9
- `trusted_edge_validates_branch_recursive_write_policy_against_pinned_base`:
  D.5, D.9, D.11
- `trusted_edge_rejects_untrusted_delete_policy_violation`: D.9, D.11
- `created_by_write_policy_allows_self_create_but_rejects_other_writer`: D.9
- `untrusted_validation_error_does_not_leave_invalid_current_row_visible`:
  D.3, D.9, D.11
- `durable_edge_rejects_after_restart_and_repairs_memory_client`: D.9, D.17
- `policy_denied_write_is_rejected_history_not_current_state`: D.2, D.3, D.9
- `write_policy_parent_check_records_policy_read_set`: D.9, D.11
- `patch_update_uses_preserved_ref_for_write_policy_validation`: D.9
- `ref_retarget_update_validates_new_parent_policy`: D.9
- `policy_denied_delete_restores_previous_visible_row_and_subscription`: D.8,
  D.9
- `multi_row_transaction_rejects_atomically_when_one_policy_check_fails`: D.2,
  D.9
- `trusted_admin_write_bypasses_policy_but_preserves_author_provenance`: D.1,
  D.9
- `recursive_write_policy_records_transitive_policy_read_set`: D.9, D.11
- `policy_read_set_survives_sync`: D.7, D.9
- `bundle_read_sets_are_scoped_to_exported_history_transactions`: D.7, D.9

#### `recursive_queries.rs`

- `recursive_policy_filters_reads_through_grandparent_ref`: D.6, D.9
- `long_acyclic_ref_policy_chain_reads_visible_leaf`: D.9
- `schema_rejects_direct_recursive_policy_cycle`: D.9
- `schema_rejects_indirect_recursive_policy_cycle`: D.9
- `long_acyclic_recursive_policy_chain_is_sql_lowerable`: D.9, D.14
- `recursive_policy_scoped_sync_includes_transitive_parent_rows`: D.7, D.9
- `recursive_query_reads_policy_filtered_tree`: D.6, D.9
- `recursive_query_scope_sync_recreates_policy_filtered_tree`: D.6, D.7, D.9
- `recursive_query_scope_sync_exports_deleted_descendant_tombstone`: D.6, D.7
- `recursive_query_scope_sync_exports_deleted_descendant_subtree_tombstones`:
  D.6, D.7
- `recursive_query_scope_sync_includes_recursive_policy_ancestors`: D.6, D.7,
  D.9
- `recursive_query_reads_branch_base_and_sparse_overlay`: D.5, D.6
- `recursive_query_scope_sync_preserves_branch_base_and_overlay`: D.5, D.6,
  D.7
- `recursive_branch_query_export_includes_tombstone_for_deleted_base_descendant`:
  D.5, D.6, D.7
- `recursive_branch_query_export_includes_snapshot_policy_ancestors`: D.5,
  D.6, D.7, D.9

#### `schema_lenses.rs`

- `rename_lens_reads_old_storage_column_as_new_field_name`: D.10
- `rename_lens_writes_export_current_semantic_field_name`: D.10, D.7
- `renamed_ref_lens_participates_in_read_policy`: D.9, D.10
- `renamed_ref_lens_participates_in_untrusted_write_policy_validation`: D.9,
  D.10, D.11
- `user_columns_with_system_prefix_are_escaped_physically`: D.14
- `index_only_schema_changes_are_semantically_compatible`: D.10

#### `subscriptions.rs`

- `subscription_initial_snapshot_matches_query_then_diffs_semantic_rows`: D.8
- `subscription_removes_child_when_parent_policy_dependency_changes`: D.8,
  D.9
- `subscription_diffs_when_active_branch_changes`: D.5, D.8
- `subscription_on_pinned_branch_ignores_later_main_updates_until_overlay_changes`:
  D.5, D.8

### E.3 Tests That Added Or Sharpened Invariants

The following behaviors are now represented in Appendix D because the tests made
them concrete:

- edge-accepted mergeables are accepted/visible without global epochs
- global epochs are not unique per transaction
- remote pending history cannot override durable current rows
- branch metadata must include base epoch/source ids, not only row branch ids
- branch-local tombstones over pinned-base rows are required
- rejected branch overlays fall back to pinned base
- query-scope repair must handle rows leaving predicates by update and delete
- query-scope export must dedupe history included for several reasons
- recursive query-scope export must include deleted descendant subtrees
- recursive write-policy read sets are transitive
- historical and branch policy evaluation must use the correct read context
- `write_if_created_by_principal` has distinct create and update ownership
  semantics
- generic schema installation must not be defined by the todo fixture
- trusted infrastructure peers may read applied policy-scoped facts without a
  user principal
- transaction-info APIs must propagate receipts, global epochs, and rejection
  details consistently after sync

### E.4 Largest Untested Gaps

The largest gaps between Appendix D and Attempt 3 tests are:

- full vector snapshots and compact dotted-vector encoding
- exact one-simple-write transaction count, sealed transaction immutability, and
  rejection detail storage outside the hot transaction row
- explicit wait behavior for exclusive transactions at local, edge, and global
  tiers
- awaiting-dependencies state for edges that need missing policy facts before
  deciding or forwarding transactions
- compact reconnect summaries and active query-descriptor replay protocol
- range and catalogue observed facts
- cache eviction policy for retained out-of-scope rows
- tier-gated query/subscription settlement semantics
- missing catalogue and missing permission fail-closed behavior
- admin-controlled catalogue publication and separate catalogue sync lane
- full authority predicate/range read-set validation beyond current row,
  absence, policy, and branch-source cases
- final product conflict metadata shape and resolved-candidate provenance
- production catalogue revision graph, migration files, inverse/cross-schema
  lenses, and copy-forward writes
- files/blobs, encryption/privacy, and encrypted indexes
- generated index/query-plan assertions and `WITHOUT ROWID` layout checks
- staged untrusted authority apply before publication
- public error object shape, global rejection callback, and redaction policy
- deterministic clock/message harness for drop/delay/reconnect scenarios

This v2 spec is serious but still pre-implementation. The next attempt should
be allowed to falsify parts of it. When it does, the result should be a sharper
spec, not just another patch to a prototype.
