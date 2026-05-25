# Jazz Relational Core On An Embedded Database

Status: Draft.

Date: 2026-05-25.

Audience: database engineers and systems engineers who do not know existing
Jazz internals.

## Overview

This document describes how to implement Jazz as a relational local-first
database on top of a simple embedded database. SQLite is the recommended first
implementation target, and SQL examples use SQLite syntax, but the design is not
meant to make SQLite part of Jazz's public semantics.

Jazz is Jazz because it combines four ideas in one database model:

1. **Local-first sync.** Every replica can write locally, keep durable local
   state, subscribe to relational queries, and exchange only the history and
   metadata needed for those queries.
2. **Branching history everywhere.** Rows are not just overwritten. Writes
   produce durable history, branches are visibility/source metadata over that
   history, and historical snapshots are ordinary read modes.
3. **Policy-first relational access.** Query execution and sync scope are
   authorization-aware. Row-level security is part of planning, validation, and
   delivery, not an afterthought outside the database.
4. **Multi-version schemas with lenses.** Multiple schema versions can coexist.
   Lenses describe how older stored data is read through newer schema views and
   how writes are moved forward.

The embedded database provides local ACID transactions, durable storage,
B-tree indexes, and a relational query planner. Jazz provides the distributed
semantics above it: public identities, transaction fate, append-only row
history, current projections, scopes, sync bundles, branch/source visibility,
conflict metadata, policies, and schema-version lenses.

The design deliberately separates:

- the public data model, which uses stable public identifiers and application
  schema concepts
- the distributed semantics, which are expressed as transactions, fate,
  visibility relations, scopes, and deterministic projections over history
- the embedded-database lowering, which uses generated tables, generated
  indexes, compact local ids, and database-specific query plans

This is not a standards-track spec and it is not a product API proposal. It is an
architecture specification for the semantic spine that a future product API can
lower into.

## 1. How To Read This Document

The document uses "must", "should", and "may" in their ordinary engineering
sense. They mark design intent, not an external compliance standard.

The first half describes Jazz behavior independent of a particular storage
layout. The later lowering sections describe how that behavior should be mapped
onto an embedded relational database, with SQLite as the concrete target.

Open issue paragraphs identify underspecified parts of the design. A prototype
may pass through an open issue, but should record the decision before treating
the behavior as stable.

## 2. Goals

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

The core is explicitly intended to replace a hand-built local database engine
with a design that delegates ordinary database mechanics to an embedded
relational database.

## 3. Non-Goals

This spec does not specify:

- a final TypeScript DSL
- a final wire encoding
- a final authentication system
- a production policy language
- production schema-lens completeness
- browser/WASM packaging
- networking transports
- custom SQLite VFS behavior
- page compression
- garbage collection of history
- final UI behavior for conflict resolution

The design must leave room for those features, but it does not require them for
the next implementation attempt.

## 4. Jazz Model

A Jazz database is a relational database with application tables, durable row
history, sync metadata, policy metadata, branch metadata, and schema metadata.

Application tables are relational tables declared by schema. They are not stored
as simple mutable tables in the semantic model. Writes create transactions and
history. Fast current tables are derived serving state.

The central rule is:

```text
append-only history is truth;
current projections are rebuildable serving indexes.
```

The system should be able to rebuild a current projection from history plus
transaction fate and obtain the same visible rows, system columns, and conflict
metadata. If current projection and history disagree, history plus fate wins.

## 5. Product API And DSL

Jazz should preserve its high-level product APIs almost exactly.

Application developers should continue to work through:

- `schema.ts`
- `permissions.ts`
- typed table handles produced by `defineApp`
- one-shot reads such as `db.all` and `db.one`
- simple writes such as `db.insert`, `db.update`, and `db.delete`
- live query subscriptions such as `db.subscribeAll`
- explicit transaction handles through one parameterized transaction
  constructor

The product API is intentionally table-first. A table handle is both a typed
query root and the write target for that table. Query builders describe
relational intent. Write calls describe row mutations. Subscription APIs
describe long-lived query interest.

The relational core lowers that product model into schema metadata, policy
metadata, query plans, write transactions, visibility plans, scope collection,
sync bundles, current projection effects, and generated indexes.

The embedded-database core should not leak physical ids, generated table names,
integer enum discriminants, transaction metadata blobs, visibility temp tables,
or generated SQL into the application API.

`schema.ts` defines structural schema, relation metadata, merge metadata, and
explicit index intent. Explicit indexes should live in `schema.ts` in an
opt-out `indexOnly(...)` style: the engine may generate automatic indexes, but
developers can declare index-only support when they want specific query shapes
to be fast or allowed.

`permissions.ts` defines policy metadata separately from structural schema.
Every runtime should have an explicit policy posture. A local-only admin session
with no upstream peers can provide the development/testing use case that a
structural-schema-only permissive runtime used to cover.

Application-visible rows keep ordinary `id` fields plus selected magic/system
fields that roughly match the current product shape. Physical system columns
and internal ids are lowering details.

Simple writes are convenience paths over the same unified write machinery as
explicit transactions. Simple writes create mergeable transactions by default.
The new product model should use transaction terminology throughout.

Transactions are parameterized by mode:

- mergeable/eventually consistent transactions can be accepted independently and
  later reconciled
- exclusive/globally consistent transactions are validated by an authority
  before global acceptance

An explicit transaction constructor should select the mode rather than exposing
separate grouped-write concepts.

Product code should mostly care about visible results, not transaction
visibility internals. Rejection still matters at the product boundary:

- write promises should reject when their transaction is rejected
- runtimes should expose a global error/rejection callback for rejected writes
  that are not otherwise awaited or handled

Open issues:

- exact v2 syntax for `indexOnly(...)`
- exact selected magic fields beyond `id`
- how much explicit transaction/durability state remains visible on typed `Db`
  handles

## 6. Auth, Principals, Sessions, And Roles

Auth and session state are part of Jazz database semantics. They are not only
transport middleware.

Every query, write, and incoming sync application is evaluated under a session.
The session feeds policy evaluation, query scope, sync delivery, authority
validation, write provenance, and rejection/error reporting.

A principal is the actor the database believes is acting. Principals include
users, admins, service actors, and trusted runtime peers. In hosted auth flows,
the principal is derived from the authenticated token, typically a JWT. The app
or hosted auth configuration defines which claim is used as the principal id.
Local anonymous users may still have durable local principals, but this spec
does not require account-linking semantics where later credentials are attached
to the same anonymous principal.

A session is the execution context for a query, write, or sync connection. It
carries principal, trust role, auth mode, loaded policy context, and
runtime/connection context when relevant.

Trust role and durability tier are separate concepts.

Trust role answers: "Is this connection allowed to say this?"

- untrusted client
- admin
- trusted peer

Durability tier answers: "How far has this transaction or query answer settled?"

- local
- edge
- global

Admin sessions bypass row policy entirely. They are still represented as
sessions for audit, provenance, and catalogue/operational checks, but ordinary
row-level policies do not constrain admin writes.

Trusted peers are allowed to send peer-only protocol messages. Untrusted clients
must not be able to forge authority-only facts such as global acceptance,
rejection, or durability observations.

Policy evaluation should see the same session context whether work is evaluated
in a local client, a browser worker, an edge server, or the global authority.
Non-admin sessions fail closed when required policy metadata is missing.

Write provenance should record the acting principal. Application-visible rows
should expose selected magic fields for provenance, including:

- `$createdBy`
- `$updatedBy`

Physical storage may encode this provenance however it wants, but the semantic
fields should survive sync, replay, projection rebuild, and schema/lens reads.

Open issues:

- exact session wire shape
- which JWT/auth claims are valid principal sources and how that choice is
  configured per app
- whether and how anonymous local principals can be linked or migrated to hosted
  auth principals
- whether service actors and admins share one principal namespace with users
- which provenance fields are visible by default under policy

## 7. Terminology

### 7.1 Public Id

A public id is a stable identifier visible at API and sync boundaries. Public
ids identify rows, transactions, nodes, branches, schemas, and other externally
meaningful objects.

Public ids must not be replaced when local transactions become globally
accepted. Authority acceptance enriches existing public transaction identities.

### 7.2 Physical Id

A physical id is a SQLite-local integer surrogate used in hot tables and indexes.
Physical ids are not part of the public API or wire semantics.

The embedded-database lowering provides a codec that maps public ids to physical
ids on sync-apply/read/write boundaries and maps physical ids back to public ids
on export and debugging boundaries.

### 7.3 Node

A node is a local writer identity such as a device, process, or authority
participant. Nodes assign local epochs for transactions they create.

### 7.4 Transaction

A transaction is the unified write and fate unit. A simple write call creates
one sealed transaction. Explicit grouped write APIs may create one transaction
that contains multiple row mutations.

Mergeable transactions may be accepted without serial validation against every
concurrent same-row writer. Their concurrent effects are represented as conflict
candidates or resolved by schema-declared merge rules.

Exclusive transactions require authority validation before global acceptance.
Their same-row write conflicts are row-granular, even when the changed columns
are disjoint.

### 7.5 Fate

Fate is the authority-observed outcome of a transaction. In this spec, fate is
stored as mutable state on the transaction row for the first implementation:

- local pending
- edge durable, if an edge tier exists
- global durable accepted
- rejected

A rejected transaction remains in history but is not visible to ordinary reads.

### 7.6 Authority

An authority is a node that is allowed to assign global epochs and accepted or
rejected fate for exclusive transactions. This spec assumes one global authority
state machine. Future sharding is implicit in epoch assignment and is not part
of the snapshot coordinate.

### 7.7 Logical Row

A logical row is the application row identity visible to users and sync peers.
It may have many history row versions.

Logical row public ids are globally unique across the database. Physical row ids
are local integer surrogates.

### 7.8 History Row

A history row is one logical row version written by one transaction.

### 7.9 Current Projection

A current projection is a derived SQLite table containing visible row state for
a materialized branch/source context. This spec requires a current projection for
main. Other branch/source projections are optional serving indexes.

### 7.10 Visibility Relation

A visibility relation is the SQL-usable set of transactions and sources that
define what a read can see.

### 7.11 Scope

A scope is the semantic closure of a query: result rows, dependency rows,
predicate/range/absence facts, policy dependencies, page boundary facts, and
branch/source context needed to reproduce, validate, or sync the query.

### 7.12 Bundle

A bundle is a sync payload derived from scope. Bundles carry history,
transactions, fate, branch/source metadata, predicate facts, and catalogue data
when needed. Bundles must not be mere result payloads.

### 7.13 Catalogue

The catalogue is the engine metadata namespace for schemas, lenses, permission
bundles, and other metadata that is not ordinary application row history.

### 7.14 Permission Bundle

A permission bundle is catalogue metadata that defines authorization rules for a
schema/object context. A permission head selects the active permission bundle for
an authorization context.

### 7.15 Lens

A lens is a SQL-lowerable translation between schema versions. Lenses allow a
runtime to read old stored data through a newer schema and write updated values
into the current schema version.

### 7.16 Semantic Row

A semantic row is the decoded application-facing row, including requested
includes and user-facing system fields. Subscription diffs compare semantic rows,
not only base-table row versions.

### 7.17 Incoming Sync Application

Incoming sync application is the process of applying a received bundle to local
storage: hydrate public ids, record transactions and fate, append missing
history, repair derived projections, and emit semantic effects.

This document avoids the word "import" for that path because it can sound like a
bulk storage load. The operation is sync handling with database semantics.

### 7.18 Projection-Diff Effect

A projection-diff effect is an engine event derived by comparing visible
projection state before and after a write, incoming sync application, or fate
change.

### 7.19 Principal

A principal is an authenticated or local identity used by policies and write
metadata. Principals include users, service/admin actors, and trusted peers.

### 7.20 Session

A session is the runtime context under which queries and writes are evaluated.
It carries the principal, role, auth mode, and policy context.

### 7.21 Durability Tier

A durability tier is the product-level delivery target for writes and queries.
The known tiers are local, edge, and global. Transaction fate records what has
actually been observed; query delivery uses durability tiers to decide when a
result is safe to publish.

### 7.22 Query Settled Signal

A query settled signal says that a query has reached a settled answer at a
requested durability tier. It is separate from row delivery. Rows may arrive
before the result is publishable for a tier.

### 7.23 Storage Driver

A storage driver is the embedded persistence backend used by a runtime. SQLite
is the selected design target here, but the product also has browser memory,
OPFS-backed, native, and server storage concerns.

## 8. Transactions

### 8.1 Transaction Semantics And Stored Facts

Every transaction must have:

- public transaction id
- physical transaction id
- writer node
- local epoch assigned by that node
- optional global epoch assigned by the authority
- kind
- fate/status
- creation time
- rejection reason, if rejected
- metadata containing read/write sets and other transaction-scoped facts

Embedded-database lowering sketch:

```sql
CREATE TABLE jazz_tx (
  tx_num INTEGER PRIMARY KEY,
  tx_id TEXT NOT NULL UNIQUE,
  node_num INTEGER NOT NULL,
  local_epoch INTEGER NOT NULL,
  global_epoch INTEGER,
  kind INTEGER NOT NULL,
  status INTEGER NOT NULL,
  rejection_reason_blob BLOB,
  created_at INTEGER NOT NULL,
  metadata_blob BLOB NOT NULL,
  UNIQUE (node_num, local_epoch),
  UNIQUE (global_epoch)
);
```

The following status discriminants are the baseline:

```text
1 local_pending
2 edge_durable
3 global_durable_accepted
4 rejected
```

The following transaction-kind discriminants are the baseline:

```text
1 data
2 branch_metadata
3 schema_metadata
4 permission_metadata
```

Open issue: edge-tier fate semantics remain underspecified. The table reserves a
state for edge durability, but the first implementation may take the direct path
from local pending to global durable accepted.

### 8.2 Transaction Lifecycle

Each simple write call creates one sealed transaction. Explicit grouped write
APIs may create one sealed transaction containing multiple row mutations.

Simple writes create mergeable transactions by default.

The product API should expose one parameterized explicit transaction
constructor, for example:

```ts
db.transaction({ mode: "mergeable" });
db.transaction({ mode: "exclusive" });
```

The exact syntax is not specified, but the product model has one transaction
constructor with a mode parameter.

Both transaction modes may stage multiple row mutations and may be rolled back
before sealing. Commit seals the transaction.

Mode semantics:

- mergeable/eventually consistent transactions may become locally visible
  optimistically, may be accepted independently, and may later be reconciled
  through schema merge rules or conflict candidates
- exclusive/globally consistent transactions require authority validation
  before global acceptance

Waiting semantics:

- waiting on a mergeable transaction may target local, edge, or global
  durability
- waiting on an exclusive transaction with any tier other than global is a
  runtime error
- waiting on an exclusive transaction at global resolves only after global
  acceptance or rejects if the authority rejects the transaction

The local write path must:

1. allocate a transaction id and local epoch
2. begin an embedded database transaction
3. insert the `jazz_tx` row
4. append all history rows
5. record read/write sets
6. update or repair affected current projections
7. commit the embedded database transaction
8. publish local effects

Authority acceptance must update the existing transaction with a global epoch
and accepted fate. It must not create a new public transaction id.

Authority rejection must keep the transaction and history rows. Visibility
rules and projection repair must make rejected versions disappear from ordinary
reads.

Fate is mutable on `jazz_tx` in this spec. Append-only fate receipts may be added
later for audit, handoff, or replay diagnostics.

## 9. Row History

For each structural schema version of each application table, the engine must
create a history table.

Embedded-database lowering sketch:

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
```

The row operation discriminants are:

```text
1 insert
2 update
3 delete
```

History rows must be append-only with respect to logical row versions. The
implementation must not mutate a prior history row to change application state.

History rows must contain enough data to rebuild current projections
deterministically, including:

- operation/delete state
- application column values needed by the schema version
- immutable creation metadata
- update metadata
- conflict metadata or an explicit empty conflict state
- engine edit metadata needed for sync and API semantics

Delete is a history row version, not physical removal from history.

## 10. Current Projection

The main branch must have a current projection for fast ordinary reads.

Embedded-database lowering sketch:

```sql
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

Current projection rows are derived state. They may be updated incrementally on
local writes. They must be rebuildable from history and transaction fate.

This spec requires materialized current projection only for the main branch.
Branch and historical reads use query-time visibility relations unless an
implementation explicitly creates a hot-branch projection as an optimization.

Open issue: hot branch projections may be needed for heavily used branches. The
initial design should not require one projection table per possible branch.

Projection rebuild must use a deterministic reducer:

1. ignore rejected transactions
2. select candidate history rows visible to the projection's source context
3. group candidates by logical row
4. apply source precedence for branch/source contexts
5. apply transaction ordering for linear histories
6. preserve multiple candidates when concurrency cannot be reduced by the active
   merge semantics
7. apply delete semantics to the resolved visible state

For main current projection, local pending transactions from the local node may
appear optimistically. Accepted global transactions are ordered by global epoch.
Local pending transactions are ordered by `(node_num, local_epoch)` only within
their node; cross-node pending same-row writes are conflict candidates unless a
merge rule resolves them.

If a delete and an update are both visible concurrent candidates for the same
row, the reducer must either apply a specified merge/delete rule or preserve the
conflict candidates. It must not silently choose one by incidental SQLite row
order.

## 11. Indexes

Indexes are part of the logical lowering contract.

The schema/query planner should generate:

- point lookup indexes for row identity
- covering indexes for common current queries
- covering history indexes for snapshot and branch reads
- partial indexes for common predicates when selectivity justifies them
- authority-validation indexes when read-set validation becomes hot

Example:

```sql
CREATE INDEX todos_v1_current_open_created
  ON todos_v1_current(branch_num, done, j_created_at DESC, row_num);

CREATE INDEX todos_v1_history_row_tx
  ON todos_v1_history(branch_num, row_num, tx_num);
```

The implementation should retain `EXPLAIN QUERY PLAN` output for risky query
lowerings in performance tests.

Open issue: generated indexes must remain compatible with lenses. A covering
index generated for one structural schema may not directly serve another schema
view.

## 12. Visibility Semantics

Reads must be defined by visibility, not by physical storage location.

This spec distinguishes three read modes:

### 12.1 Current Local Read

A current local read reads from a current projection. Main current projection is
required. Hot branch projections are optional. If no projection exists for a
branch/source context, the read is a branch/source visibility read rather than a
current projection read.

Current local reads may include optimistic local pending transactions.

### 12.2 Global Epoch Snapshot

A global epoch snapshot reads accepted history where:

```text
tx.status = global_durable_accepted
tx.global_epoch <= requested_epoch
```

Rejected and pending transactions are not visible.

### 12.3 Full Vector Snapshot

A full vector snapshot reads history through a closed additive snapshot vector.
The vector contains:

- global base epoch
- node-local bases
- explicitly included transaction dots

There are no excludes in v0.

Node-local bases are local durability coordinates for transactions that are not
yet represented by the global base. After a transaction is accepted at a global
epoch covered by the global base, its node-local coordinate is redundant.

Snapshot vectors should be canonicalized by removing explicit transaction dots
and node-local base coverage that are already implied by the global base. This
canonicalization is a compactness rule; it must not change visibility.

When a local transaction becomes globally accepted, replicas learn the mapping:

```text
tx_id -> global_epoch
```

Receivers must preserve the public transaction id and may compact future vectors
by replacing that transaction's local/dot coordinate with global-base coverage
once the global base includes it.

An informative visibility predicate:

```text
visible(tx, snapshot) =
  tx.status IN (local_pending, edge_durable, global_durable_accepted)
  AND (
    tx.status = global_durable_accepted
      AND tx.global_epoch <= snapshot.global_base
    OR tx.node IN snapshot.local_base
      AND tx.local_epoch <= snapshot.local_base[tx.node]
    OR tx.tx_id IN snapshot.includes
  )
```

Local bases for remote nodes are valid only when the snapshot explicitly names
that remote node coordinate. They are not inferred from the presence of remote
pending history.

The exact encoding of snapshot vectors is open. This spec selects transaction
ids for explicit includes in the baseline because public transaction ids are
stable across local-to-global acceptance.

Global epoch order is authority order. It is not a complete causality relation.
Causality for validation and merge decisions comes from read/write sets and
snapshot bases.

## 13. Visibility Lowering

The query planner should lower non-current reads by first constructing a
SQL-usable visibility relation and then joining history against it.

The visibility relation may be represented as:

- a generated CTE
- a temporary table
- a generated predicate with bindings

SQL fragments and bind parameters must travel together as one plan artifact.
The implementation must not assemble SQL text and bind arrays independently in
ways that make parameter ordering implicit or fragile.

For a snapshot read, the general shape is:

```text
visible_tx(tx_num)
  -> history rows joined to visible_tx
  -> candidate row versions grouped by logical row
  -> delete filtering
  -> query predicates/order/limit/includes
  -> result rows plus scope
```

When two concurrent visible versions of the same logical row cannot be ordered
by the active merge semantics, the result is a conflict candidate set, not an
implicit last-writer-wins winner.

Open issue: the precise SQL shape for multi-source branch reads with conflict
candidates remains to be proven.

## 14. Branches

Branches are both product-visible objects and engine visibility contexts.
Branches are not database copies.

Applications declare branch-backing tables explicitly in schema. A branch has a
product identity represented by an ordinary app-visible row in such a table, and
engine metadata that records source/provenance state.

Branch creation should use a dedicated `createBranch`-style API. That operation
creates both:

- the branch backing row
- the engine branch/source metadata

`db.branch(branchId)` returns a branch-scoped database handle. It should fail as
early as possible if the branch backing row is not visible to the session under
ordinary row policy.

Once a session is authorized to use a branch, ordinary row policies still apply
to visible row versions in that branch context. Branch access therefore has two
policy layers:

- can this session see/use/change the branch backing row?
- can this session see or mutate this row/version through that branch?

Branch source/provenance changes are authorized through ordinary update
permissions on the branch backing row. The engine then lowers accepted changes
to branch/source metadata.

A branch-local transaction may be globally accepted while remaining invisible to
main. Global history alone does not imply branch visibility.

A branch must record:

- public branch id
- physical branch id
- precise provenance for UI/debug/rebuild
- flattened effective source list for query execution

The normative branch storage model has two layers:

- an append-only branch metadata history, written by branch metadata
  transactions
- a derived flattened source relation used by queries

The branch metadata history preserves exact provenance. The flattened source
relation is rebuildable serving state.

Example physical shape:

```sql
CREATE TABLE jazz_branch (
  branch_num INTEGER PRIMARY KEY,
  branch_id TEXT NOT NULL UNIQUE,
  current_head_tx_num INTEGER
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

An implementation may encode provenance as a blob in v0, but query execution
must use a flattened relational source list or an equivalent SQL-usable relation.

Rebuilding branch source state from branch metadata history must be
deterministic.

The source list is a relation of:

```text
source branch
source snapshot or epoch/vector
precedence
```

Visible row selection for branch reads is:

```text
for each logical row:
  consider visible versions from the branch source relation
  apply source precedence
  expose conflict candidates where source/base conflicts require resolution
  filter deleted winners unless explicitly requested
```

The baseline branch feature set is:

- explicit branch-backing table declaration
- branch create from main at a pinned global epoch
- branch-local writes
- branch reads over branch overlay plus pinned main base
- branch sync including branch-local rows and base-only rows

The following branch features are out of the normative baseline:

- multi-base branches
- hot branch current projections
- metadata-only merge commits

Branch-local writes use the same logical row ids as main by default. A branch
overlay writes new history versions for the same logical rows in a different
branch/source context.

Branch sync scope must include:

- branch backing row policy dependencies
- branch source/provenance metadata
- row history needed for query results
- predicate/range/absence facts in the branch context

Branch merge should preferably be a metadata transaction that changes
source/provenance rather than copying rows. Multi-base branch conflicts should
remain candidates rather than being silently resolved.

Open issue: conflicts between multiple branch bases should likely be represented
as multiple visible candidates. This is a design hypothesis to be tried, not yet
a settled rule.

## 15. Queries

Queries are relational plans that produce both application rows and scope.

A query plan must contain:

- SQL text
- bindings
- row decoder
- include decoder
- visibility/source plan
- scope collector
- expected index information when relevant

Application-facing query results include semantic rows. Engine-facing query
results include result locators, dependency locators, predicate/range facts,
policy facts, branch/source context, and page boundary facts.

Includes follow ordinary relational semantics:

- required includes lower to inner joins
- optional includes lower to left joins

If a required include is missing or unauthorized, the parent row is filtered out.
If an optional include is missing or unauthorized, the parent row remains and the
include is null/absent.

Optional missing includes must produce absence scope. A receiver cannot
reproduce an optional null result from row locators alone.

Open issue: relation inference from schema metadata should remove the need for
callers to pass foreign-key column names directly.

## 16. Scope

Every query execution that may participate in sync, subscription invalidation,
or authority validation must produce scope.

Scope categories:

- result row scope
- include/dependency row scope
- predicate scope
- range scope
- absence scope
- policy dependency scope
- page boundary scope
- branch/source scope
- schema/lens scope, when needed

Scope entries should distinguish why a fact was observed. A row needed for the
visible result and the same row needed for policy enforcement are the same row
for bundle deduplication but different reasons for explanation and invalidation.

An implementation may over-approximate predicate/range scope, but it must not
omit facts needed for correctness.

Scope has two layers:

- observed facts, which may repeat with different reasons
- bundle locators, which are deduplicated concrete export requirements

Scope fact identity must be stable across replicas. At minimum, each fact has:

- kind
- table/schema identity
- branch/source context
- locator or predicate/range expression
- observed visible transaction/version, when applicable
- reason

Scope fact ordering is not semantically meaningful. Encoders should canonicalize
ordering for deterministic tests and compact hashes.

Predicate, range, and absence facts must compare by normalized expression,
normalized bound values, table/schema identity, and branch/source context. The
normal form is not specified here; until it is, only the predicate forms
implemented by the planner are stable.

## 17. Sync Bundles

Sync is query-scoped. It is not table replication.

Given a query scope, the sender must export enough data for a receiver with the
same schema context and policy context to reproduce the query result locally.

Bundles contain:

- transaction records and fate
- branch/source metadata
- history rows
- file metadata and file bytes when scoped rows reference files
- predicate/range/absence facts
- schema/lens catalogue entries when needed
- permission catalogue entries when needed

Bundles must deduplicate concrete transactions and history rows even when scope
contains repeated reasons.

Bundles must use public ids on the wire. Incoming sync application hydrates those
public ids into local physical ids before touching hot tables.

Bundles must not be treated as authoritative result snapshots. The receiver
applies history/fate/scope, repairs projections, and runs queries locally.

Open issue: compact reconnect summaries and predicate/range closure encoding
are not specified here.

## 18. Files, Images, And Binary Data

Files are product-level data with relational metadata, access control, sync
scope, and lifecycle semantics. File bytes may use specialized chunk/blob
storage, but they must not bypass Jazz policy or transaction semantics.

Applications declare file metadata and file part tables explicitly according to
Jazz conventions. They are not automatically present in every app, but the
conventional shape should allow product tooling and serving infrastructure to
recognize them.

The model has three layers:

1. file metadata
2. chunk/part index
3. blob bytes

File metadata is ordinary relational data. It has an `id`, file name/content
type/size/hash metadata, ownership/ref metadata, history, policy, branch
behavior, and schema/lens behavior.

The chunk/part index maps file ids to byte chunks. It records part number, byte
range, digest, and storage locator. It may be represented as ordinary rows,
system side tables, or a hybrid lowering.

Blob bytes may live in SQLite blobs, OPFS/blob storage, object storage,
filesystem storage, or another specialized byte store. Access to those bytes is
authorized through the same session and policy model as row reads.

File content is immutable in v0. Replacing a file creates a new file/content
version rather than mutating bytes in place.

Upload completion is part of transaction fate. A transaction that creates or
replaces file metadata should not be considered accepted/durable at a requested
tier unless the required file bytes are durable at that tier too.

Branches share immutable file chunks by digest across branch versions. Branch
metadata rows can diverge like ordinary rows, but identical content chunks do
not need to be copied.

For now, query-scoped sync should include file bytes when scoped rows reference
files and the receiving session is authorized to receive them. Future protocols
may replace inline bytes with authorized fetch handles or separate blob transfer,
but that is not the baseline.

Deletes or permission changes on owning rows may cascade to file access
according to declared relation semantics. File serving must re-check session and
policy rather than treating stored bytes as public once uploaded.

Open issues:

- exact conventional schema for file and file part tables
- upload limit and validation policy
- partial/resumable upload protocol
- mutable file/chunk strategy
- whether file chunks participate in history tables or specialized byte stores

## 19. Incoming Sync Application

Incoming sync application is semantic, not insert-only.

Incoming sync application should:

1. hydrate public ids to physical ids
2. upsert transaction records and fate
3. upsert branch/source metadata
4. insert missing history rows
5. insert or update catalogue state when present
6. repair affected projections
7. diff affected projections
8. emit listener effects from projection deltas

Raw history insertion and semantic listener effects are different facts. A
received history row may be old, rejected, hidden by branch visibility, or not
projection-changing. Receiving it does not necessarily emit an application row
change.

Duplicate incoming sync application must be idempotent.

Open issue: affected-row discovery should become narrower than broad projection
rebuild, but broad repair is acceptable as a correctness baseline.

## 20. Runtime Topology, Transport, And Reconnect

The same Jazz semantics should hold across browser main thread, worker, native,
edge, and global runtimes. Runtime topology changes where storage lives and
where queries settle. It should not change the meaning of queries, writes,
policies, branches, or sync.

### 20.1 Runtime Topology

The product supports several runtime shapes:

- browser main-thread memory runtime
- durable browser worker runtime
- SharedWorker or tab broker runtime
- native/NAPI runtime
- React Native runtime
- server runtime
- edge runtime connected to the global authority
- global authority runtime

A browser main-thread runtime is the app-facing facade. It may execute queries
itself when no durability tier is requested. When a query or write requires
durability beyond the main-thread cache, it forwards work to a durable worker or
another runtime tier.

A durable browser worker runtime owns durable browser storage, incoming sync
application, upstream sync, and durable query settlement. In browser persistent
mode, the worker is a real runtime tier, not only a storage helper.

A SharedWorker or tab broker is the recommended topology for multi-writer local
browser environments. It centralizes durable storage, upstream sync, and query
replay across tabs so multiple tabs do not contend for the same local database
independently. This is the most complex local topology, but it is the one the
browser product needs.

Native, NAPI, React Native, server, edge, and global authority runtimes should
share the same relational semantics while using storage and scheduling choices
appropriate for the platform.

An edge runtime is a trusted peer relative to clients. It enforces policy for
connected clients, may accept mergeable transactions at edge tier, and forwards
exclusive/global work upstream.

Edges may permanently reject mergeable transactions when schema validation,
policy evaluation, quotas, or other receive-time checks fail. Edge policy
evaluation may be slightly stale with respect to rows that influence
permissions; that staleness is an accepted product tradeoff for mergeable
transactions. Exclusive transactions still require global authority validation.

The global authority assigns global acceptance or rejection for exclusive
transactions and owns global durability and catalogue authority.

There is one logical global authority per app in this spec. Future sharding may
exist internally, but it must preserve one app-level authority state machine for
global epochs, exclusive transaction acceptance/rejection, and catalogue
publication.

### 20.2 Hosted Cloud And Operations

The hosted product wraps the runtime topology in operational app boundaries.

An app has:

- app id
- hosted sync URL
- global authority placement
- optional edge placement
- schema, permission, and migration catalogue heads
- hosted auth configuration
- quotas, rate limits, and upload limits
- observability namespace

Hosted auth integrations authenticate sessions and produce principals according
to app configuration. For JWT-based auth, the app configuration chooses the
claim used as the Jazz principal id. Policies and provenance refer to that
principal id rather than to an implicit account-linking object.

Quotas, rate limits, upload limits, and transport-level capacity failures should
primarily surface as transport/API errors, not as accepted transactions with
rejected fate. Transaction rejection is reserved for semantic database outcomes
such as policy denial, constraint failure, invalid schema, or authority
conflict.

Operational dashboards, billing, backups, retention, shard placement,
observability, and hosted admin tooling are product requirements but are not
fully specified here.

Open issues:

- how an edge discovers the policy-influencing rows required for each
  app/schema/permission head
- whether edge policy readiness is derived from static policy analysis,
  catalogue-declared policy scopes, runtime dependency subscriptions, or a
  combination
- how edges keep policy-influencing rows fresh enough for mergeable transaction
  acceptance
- what "fresh enough" means for edge acceptance and rejection
- how edge replay or audit records which policy snapshot/context was used when
  accepting or rejecting a mergeable transaction
- exact hosted auth configuration syntax for principal-claim selection

### 20.3 Transport

Transport should stay thin. It carries typed sync messages and catalogue
payloads; it does not implement a second query engine.

The route surface may remain small:

- `GET /apps/:app_id/ws`
- `GET /apps/:app_id/schemas`
- `GET /apps/:app_id/schema/:hash`
- `GET /apps/:app_id/admin/schemas`
- `GET /apps/:app_id/admin/migrations`
- `GET /apps/:app_id/admin/schema-connectivity`
- `GET /apps/:app_id/admin/permissions/head`
- `GET /apps/:app_id/admin/permissions`
- `GET /health`

The WebSocket channel carries framed sync messages such as:

- query subscriptions and unsubscriptions
- transaction records
- transaction fate
- row history
- branch/source metadata
- catalogue metadata
- query settled signals
- errors and warnings

Transport authentication establishes the session/trust role. Runtime policy and
query machinery still decide row-level visibility.

Trust boundaries:

- untrusted clients cannot send authority fate or durability observations
- admin sessions can publish catalogue/admin state and bypass row policy
- trusted peers can send peer-only sync messages
- edge/global placement is a durability and authority concern, separate from
  client trust role

Mergeable transactions accepted by an edge produce replayable fate at the edge
tier. This is different from a non-replayable local observation.

### 20.4 Reconnect

Active subscriptions are desired state.

Reconnect should use two stages:

1. replay-window recovery
2. full scope/frontier snapshot fallback

During replay-window recovery, the reconnecting runtime asks for missed
transaction, fate, scope, catalogue, branch/source, and query-settled messages
since its last known stream position.

If the replay window is unavailable or insufficient, the peer recomputes active
query scope and sends a current scope/frontier snapshot with the history,
fate, branch/source metadata, catalogue metadata, and policy facts needed to
reproduce it.

Pending local transactions are reconciled during reconnect. Missing transactions
may need retransmission; rejected transactions must repair local projections and
surface through write handles or global rejection callbacks.

Query settled signals gate publication again after reconnect. If scope
contracts, stale rows must be removed or invalidated.

Open issues:

- exact stream position and replay-window encoding
- compact reconnect summaries
- how much catalogue digesting remains separate from query scope
- SharedWorker/tab broker ownership handoff
- SQLite WASM startup and binary-size constraints
- OPFS/locality behavior
- React Native and native packaging constraints

## 21. Subscriptions

One-shot queries and live subscriptions share the same query semantics.

A one-shot query is a short-lived query interest that settles once, returns its
semantic rows, and tears down. A subscription is a long-lived query interest
that keeps previous rows and scope so later changes can be delivered as semantic
diffs.

Subscriptions are live queries. The baseline implementation should rerun the
query and diff full semantic rows.

Subscription state must include:

- query plan or query AST
- previous ordered semantic rows
- dependency payloads for included rows
- previous scope
- invalidation metadata

Dependency payloads are required. If a joined dependency changes, the parent
semantic result row may change even when the parent row's own visible transaction
does not.

Subscription callbacks expose semantic diffs:

- all
- added
- updated
- removed

These diffs are over semantic rows, including requested includes and user-facing
system fields.

Tiered delivery:

- `tier: "local"` may publish from local durable state plus local optimistic
  mergeable transactions from the local runtime
- `tier: "edge"` waits until the query is settled at the connected edge for
  contributing visible transactions
- `tier: "global"` waits until the query is settled globally for contributing
  visible transactions

One-shot queries with a requested tier wait for the same query settled condition
as the first delivery of a subscription at that tier.

Every subscription update is tier-gated, not only the first result.

Pending exclusive transactions are not included in any tiered query result until
globally accepted. Local optimistic mergeable transactions appear only in the
originating local runtime's pending overlay. Once an edge accepts a mergeable
transaction based on policy evaluation, it may become visible to other runtimes
through edge-scoped delivery even if the permissions it relied on are later
stale. This is intentional for mergeable transactions.

Rejected transactions repair projections and produce semantic diffs when they
change visible results. Rejections should also be surfaced through the global
error/rejection callback when they affect a subscription and were not otherwise
awaited.

A query settled signal means: for this query, source context, schema context,
policy context, and requested durability tier, the runtime has applied the row
history, transaction fate, branch/source metadata, catalogue metadata, and
policy state required to publish the current semantic result at that tier.

Query settled is separate from row delivery. A runtime may have received rows
but still lack fate, policy context, catalogue state, or completeness.

Active subscriptions are desired state. On reconnect, they should be replayed;
the server or peer recomputes scope, resends missing history/fate/scope, and the
local runtime republishes only when query settled conditions are met. If scope
contracts, stale rows must be removed or invalidated.

Invalidation may start coarse. It must be correct.

Useful invalidation facts include:

- result/dependency row overlap
- predicate/range overlap
- branch/source changes
- fate changes
- schema/lens activation changes
- policy dependency changes
- old and new order keys for ordered pages
- column masks for projection/predicate precision

Row-id cursors alone are insufficient for ordered-page invalidation because a
row outside the page may move inside the page after its order key changes.

## 22. Authority Validation

Exclusive globally consistent transactions must be validated by an authority
before global acceptance.

The authority must validate against authority-visible history, not against a
current projection polluted by unaccepted proposals.

Validation checks include:

- row reads still observe the same visible version
- absence reads are still absent
- range reads remain valid
- policy dependencies still authorize the operation
- declared constraints remain true

If validation fails, the authority rejects the transaction with a structured
machine-readable reason.

The authority conflict item for exclusive writes is the logical row. Two
exclusive transactions that write different columns of the same row are not
automatically safe merely because their column masks are disjoint.

Column masks are still useful as auxiliary metadata for:

- mergeable transactions
- conflict UI
- subscription invalidation
- policy/error explanation
- semantic diffs

Open issue: predicate/range read-set encoding must be precise enough for
correctness without making every validation path require huge side tables.

## 23. Read And Write Sets

Read/write sets must be typed in memory.

Durable encoding should begin inline on transaction metadata. Hot side tables may
be added when quantitative measurements justify them.

Read-set entry kinds:

```text
row
absence
range
policy
page_boundary
```

A read-set entry records:

- table/schema identity
- branch/source context
- row id or predicate/range expression
- visible transaction/version observed
- reason

A write-set entry records:

- table/schema identity
- row id
- operation
- column mask metadata

For updates and deletes, the write path must record the previously visible row
version as the write base.

Read/write sets replace explicit parent pointers as the first-order causality
and validation mechanism. Merge operations may need to walk read/write sets and
history; slow merge walks are acceptable in the initial design.

## 24. Conflict Candidates And Resolution

Current projection rows expose:

- the resolved value
- conflict metadata, empty when there is no visible conflict

Conflict metadata may contain:

- candidate transaction ids
- candidate values
- changed column masks
- base/read-set information
- resolution metadata

At minimum, durable non-empty conflict metadata must identify the candidate
transactions and whether the stored visible value is resolved or unresolved.
When a prior conflict is cleared, the history row must carry an explicit empty
or cleared conflict state so projection rebuild does not resurrect old conflict
metadata.

Mergeable transactions may use per-column or per-field metadata to merge
automatically. Exclusive transactions must remain row-granular for correctness.

Conflict resolution is an ordinary transaction. It reads the conflicted row,
writes the chosen value, records the resolved candidates, and clears or updates
conflict metadata.

Open issue: candidate ordering, multi-base branch conflicts, and per-column UI
shape remain underspecified.

## 25. Policies

Policies are part of the database model. They shape reads, writes,
subscriptions, sync scope, and authority validation.

The policy language should preserve the current `permissions.ts` product shape
almost exactly. It may be internally redesigned, but app authors should not have
to learn a new policy vocabulary just because the core is rebuilt.

Policies must always stay SQL-lowerable. This includes ordinary row policies,
inherited/relational policies, branch policies, and recursive policies.

Structural schema plus permission bundle plus active permission head produce an
authorization schema. Non-admin sessions use that authorization schema for
query planning, write validation, sync delivery, and authority checks. If a
non-admin session lacks required policy metadata, it fails closed.

Admin sessions bypass row policy entirely.

Policy evaluation is part of query planning and authority validation. It is not
a post-filter outside the relational engine.

Policy operations:

- read policies shape row visibility and sync delivery
- insert policies check proposed row values plus session context
- update policies check the old visible row, proposed row values, and session
  context
- delete policies prefer explicit delete rules, but may fall back to update
  policy semantics
- branch policies are ordinary row policies on branch backing rows that then
  influence downstream permissions over row versions visible in that branch
- catalogue publication is admin/core-controlled rather than ordinary row policy

Policies may depend on rows other than the result row. Access can follow refs,
recursive relationships, branch backing rows, and other policy graph edges.

Policy dependencies must be represented in scope separately from ordinary result
dependencies. A row may be included only for policy enforcement.

For v0 sync, policy dependency scope may send full dependency rows and facts.
Future protocols may replace some policy dependencies with opaque proofs or
summaries, but this is not the baseline.

Policy failures should not distinguish "not visible" from "visible but denied"
at the application API level, because that leaks information. Transaction
rejections can still carry structured machine-readable reasons appropriate for
the actor allowed to see them.

Permission bundles and permission heads belong to the catalogue. They are not
ordinary user table rows.

Recursive queries and recursive permission policies are a major lowering risk.
They should be de-risked early with recursive CTE experiments rather than
treated as an afterthought.

Open issues:

- exact SQL-lowerable IR for recursive policies
- how to bound recursive policy evaluation
- how branch backing rows are declared and related to branch source metadata
- which structured denial/rejection reasons are safe to expose to which actors

## 26. Schemas, Catalogue, Migrations, And Lenses

Schema, permissions, lenses, and migrations are first-class catalogue data.
Developers edit them as files; runtimes consume them as versioned catalogue
state.

The developer-facing project shape is:

```text
schema.ts
permissions.ts
migrations/
```

`permissions.ts` is required, even when it declares an empty explicit permission
bundle. A runtime must not infer a permissive policy merely because a permission
file or bundle is absent.

`schema.ts` defines:

- structural schema
- relations
- merge strategies
- explicit `indexOnly(...)` declarations
- branch-backing table declarations
- file table conventions
- future confidentiality metadata

`permissions.ts` defines authorization rules. It is separate from structural
schema, preserves the current permissions product shape almost exactly, and
compiles against the structural schema. Permission bundles are keyed by app id
plus head version. The permission head selects the active bundle for an app
head; the spec does not currently require an additional object-id key.

`migrations/` contains reviewed migration/lens modules between schema hashes.
Lenses belong in migrations; there is no separate top-level lens concept in the
developer workflow.

Schema identity is content-addressed. Explicit indexes and merge strategies
are part of the schema hash. If two schema versions differ only by index
declarations or merge strategy declarations, the system should derive automatic
lens compatibility because no row-value shape translation is needed. Index-only
changes may require physical index work, but they should not require a data lens.
Merge-strategy-only changes may change future conflict resolution behavior, but
they should not require a row-shape lens.

Each structural schema version has its own physical table layout for history and
current projection tables.

The catalogue must carry:

- structural schema definitions
- migration/lens edges from `migrations/`
- permission bundles
- permission heads
- migration metadata when needed

Permission catalogue state is keyed by app id plus a head version. The exact
shape of permission heads remains open, but the active head selects the
permission bundle used with a structural schema to produce an authorization
schema.

Catalogue publication is admin/core controlled. Edge runtimes learn catalogue
state from the global authority through a separate catalogue sync lane. Catalogue
sync is not ordinary query-scoped row sync. Query-scoped sync may depend on
catalogue state, but it should not be the mechanism that discovers the app's
schema, permission, and migration graph.

Runtimes should not guess missing catalogue state. If required structural schema,
lens, migration, permission bundle, or permission head state is missing, the
runtime should fail or hold affected work until catalogue state catches up.

Lenses must be SQL-lowerable in v0.

An implementation may initially support only a narrow rename/project lens. Full
schema evolution is outside this spec's baseline.

Writes through an old schema view should be copy-on-write into the current
schema branch:

1. read old data through lenses into the current schema view
2. apply the write in the current schema
3. append a new history row in the current schema layout

Open issue: cross-schema conflict candidates and serving indexes over lens
unions are not specified.

Developer workflow:

- `jazz-tools validate` validates schema and permissions together
- validation emits explicit-policy diagnostics
- migration creation compares stored schema hashes and emits reviewed stubs
- migration push publishes reviewed migration/lens edges
- catalogue push publishes app-id/head-version permission bundles and heads
- dev tooling should inspect schema/lens connectivity, permission heads,
  generated indexes, and storage layout

Open issues:

- exact permission head shape
- exact representation of app heads and their relation to schema and permission
  heads
- automatic lens compatibility rules for non-row-shape schema changes beyond
  indexes and merge strategies
- generated index inspection workflow
- schema/lens compatibility across branches

## 27. System Column Semantics

User-facing system fields may be exposed with `$` names such as:

```text
$rowId
$txId
$createdAt
$updatedAt
```

Physical SQLite columns should use `j_` names in application row tables.

Pure system tables do not need the `j_` prefix because all their columns are
engine-owned.

`$createdAt` and `$updatedAt` are system fields. Queries must be able to filter
and sort over both user columns and user-facing system fields.

The mapping from user-facing system names to physical names must live in the
layout/query codec.

## 28. Wire/Public Boundary

The wire protocol and public APIs must use public ids.

The physical database may store public ids in boundary tables for debugging,
export, and hydration, but hot history/current/index paths should use integer
surrogates.

On export:

```text
physical ids -> public ids -> bundle
```

On incoming sync application:

```text
bundle public ids -> physical ids -> embedded database writes
```

Physical ids must not leak into public API equality, ordering, persistence, or
sync semantics.

## 29. Embedded Database Lowering

The previous sections define Jazz behavior. This section describes the selected
lowering strategy for an embedded relational database, using SQLite syntax for
examples.

The lowering should preserve a hard boundary:

- public ids, schema names, and user-facing system fields belong to Jazz
  semantics
- physical integer ids, generated table names, integer enum discriminants, and
  index choices belong to the embedded-database implementation

### 29.1 Physical Storage Requirements

Hot storage uses local integer surrogates for repeated public ids.

This applies at least to:

- nodes
- transactions
- rows
- branches
- tables, schemas, and columns when they appear repeatedly in hot metadata

Hot enum fields use stable integer discriminants, not string labels.

Composite-primary-key system tables should use `WITHOUT ROWID` in SQLite unless
benchmarks show a regression for the relevant workload.

System columns in physical row tables should use the `j_` prefix. User columns
whose names collide with the reserved physical prefix are escaped by the layout
codec. User-facing system fields may still use `$` syntax in DSLs and query
examples; `$` is not the physical SQLite column prefix.

Fixed-width binary public ids remain a possible future optimization, but this
spec selects integer surrogates as the baseline physical representation.

### 29.2 Identity Mapping

The physical database contains mappings from public ids to physical ids.

The exact split between embedded public-id columns and dedicated mapping tables
is an implementation choice, but the lowering needs these logical mappings:

```sql
CREATE TABLE jazz_node (
  node_num INTEGER PRIMARY KEY,
  node_id TEXT NOT NULL UNIQUE
);

CREATE TABLE jazz_tx_id (
  tx_num INTEGER PRIMARY KEY,
  tx_id TEXT NOT NULL UNIQUE
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

The implementation may combine `jazz_tx_id` with the transaction table if doing
so keeps the public/physical boundary clear.

The identity codec should be centralized. SQL-generating subsystems should not
invent ad hoc public-id/physical-id conversions.

`row_id` is unique across all logical tables. `table_num` is retained for
debugging, validation, and table-specific decoding, not to make row identity
table-scoped.

### 29.3 Generated Tables And Indexes

For each structural schema version, the lowering creates history tables and the
required current projection tables. Generated table names are implementation
details. Generated indexes are part of the lowering plan and should be derived
from schema/query intent, not handwritten per feature.

## 30. Undefined And Underspecified Areas

The following areas are intentionally not fully specified:

- canonical encoding for transaction metadata, read/write sets, and conflict
  metadata
- dotted version vector binary/wire representation
- local-to-global vector upgrade broadcast format
- compact predicate/range scope closure
- efficient authority validation over very large read sets
- multi-base branch conflict semantics
- precise branch provenance encoding
- policy language
- full schema lens semantics
- reconnect summaries
- durable subscription resume
- hot branch projection heuristics
- audit-grade fate receipts
- garbage collection and compaction

These are not small details. They are the remaining places where the design may
change once made executable.

## 31. Security And Privacy Considerations

Query-scoped sync can leak information if scope is over-approximated across
authorization boundaries. Enforcing runtimes must evaluate policy before sending
bundles to untrusted clients.

Policy rows and predicate/range facts may reveal why a row was visible or absent.
The protocol must eventually decide whether such facts are sent directly,
summarized, or represented by opaque proofs.

Rejected transactions and history rows remain stored. Implementations must
consider whether rejection reasons or rejected row values are safe to sync to a
given recipient.

## 32. Encryption And Privacy

Per-column end-to-end encryption is the long-term encryption model. Table-level
or row-level E2EE are not the primary design target.

E2EE is not in scope for the next implementation slice, but the relational core
must not assume that every user column is server-readable.

Columns and file content may eventually belong to different confidentiality
classes:

- server-readable
- client-decrypted
- encrypted but indexable
- opaque blob

Server-readable values can participate in server-side policy, indexes,
predicates, ordering, sync scope, and authority validation.

Client-decrypted values are stored and synced as opaque encrypted bytes. They
can be queried after local decryption, but an untrusted server or edge cannot
filter, sort, index, or enforce policy over their plaintext.

Encrypted-but-indexable values are a future special case. They might use
deterministic tokens, hashes, search indexes, or other mechanisms with explicit
leakage. This spec does not define those mechanisms.

Opaque blobs include file chunks or column values that servers store and sync
without understanding. Policy can only use metadata or proofs, not blob
plaintext.

Server-enforced policies must not depend on client-only encrypted fields. A
schema/policy compiler should eventually reject or warn about such policies
unless the field is explicitly marked as server-readable or has a defined
index/proof mechanism.

Sync scope itself can leak information. Predicate/range/absence facts, policy
dependencies, rejection reasons, and conflict metadata may reveal information
even when row values are encrypted. Future protocols may need opaque or
summarized facts; v0 may send full facts where policy allows.

File content digests should be treated as privacy-sensitive because they can
leak equality across branches, users, or sync sessions. Digest sharing across
branches is useful, but its leakage model must be explicit before E2EE ships.

Conflict metadata for encrypted fields should not expose plaintext candidate
values. The baseline conflict representation for encrypted fields is two or more
opaque encrypted blobs marked as conflicting, with enough metadata to resolve or
replace them on an authorized client.

Generated indexes must declare what they leak. They should require columns to be
server-readable or explicitly indexable-encrypted.

Open issues:

- confidentiality metadata syntax in `schema.ts`
- key management and sharing
- encrypted index/proof mechanisms
- policy compiler diagnostics for encrypted fields
- encrypted file digest strategy

## 33. Performance Considerations

The physical layout is chosen for SQLite:

- local integer surrogates for hot keys
- integer enum discriminants
- composite primary keys with `WITHOUT ROWID` where useful
- generated covering and partial indexes
- current projection for hot main reads
- query-time visibility for historical and branch correctness baselines

Performance risks:

- mapping tables add insert and boundary lookup cost
- inline transaction metadata may become expensive for authority validation
- broad projection repair may be too slow after incoming sync application/rejection
- rerun-and-diff subscriptions may be too coarse for large result sets
- predicate/range scope may become too large
- generated indexes may overfit query shapes and inflate writes

Fixed-width binary public ids remain a future revisit if public Jazz ids gain a
canonical compact binary form. They are not the baseline of this spec.

SQLite page compression is not in scope for this spec. The selected first-order
storage optimization is compact logical layout.

## 34. Data Export And External Sync

Export, ingest, and external connectors are primarily userland patterns, not
core database semantics.

Ordinary user export should be expressible as normal policy-filtered queries,
optionally with userland code that expands includes, files, or historical data.
The core does not need a built-in export subsystem for v0.

Restore is an administrative operation. Non-admin restore is out of scope.
Admin backup and restore should likely be expressed in terms of embedded
database snapshotting/restoring, with catalogue and file/blob storage included
by the operational backup system rather than replayed through ordinary user
transactions.

External connectors should be built above the core as application or service
code. They may write Jazz transactions using service/admin sessions, source
branches, or application tables, but the core does not prescribe connector
semantics in this spec.

Open issues:

- operational backup format for SQLite/native and browser storage
- whether hosted product exposes convenience export APIs built from normal
  queries

## 35. Platform Bindings And Packaging

Rust is the semantic source of truth for query execution, transactions, sync,
subscriptions, policy evaluation, catalogue application, conflict metadata, and
tiered delivery. TypeScript and framework packages provide schema/query DSLs,
generated types, tooling integration, and idiomatic UI bindings over those
semantics.

Bindings must agree on:

- row and result semantics
- transaction modes and fate
- subscription diff semantics
- tiered query delivery semantics
- policy/session semantics
- branch/source selection
- schema/catalogue/lens interpretation
- conflict metadata shape
- error/rejection shape

The browser main thread may run queries directly against an in-memory core. In a
durable browser topology, that main-thread runtime talks to the worker or tab
broker as a trusted upstream peer. The worker owns durable storage and upstream
sync; the main thread still gets low-latency local query execution.

Memory-only runtimes are first-class for tests, demos, and the full distributed
system harness. The important property is controllable topology and
in-memory-ness, not that the harness runs inside a real browser runtime.

Framework integrations should be thin adapters over the same reactive Jazz
client. Jazz's reactive machinery lives in the core/client runtime; React,
Solid, Svelte, Vue/Nuxt, and other bindings should integrate idiomatically
without creating independent cache/subscription semantics.

Platform storage choices remain binding-specific:

- browser durable mode: SQLite WASM plus browser storage such as OPFS where
  available
- Node/NAPI and server runtimes: native SQLite through Rust
- React Native/native mobile: native SQLite integration
- edge/global authority runtimes: native Rust SQLite or another embedded
  database behind the same lowering contract

Package boundaries are implementation guidance, not product semantics. The
current Jazz package model is a reasonable starting point; future package names
should be decided in the implementation plan rather than fixed here.

Open issues:

- SQLite WASM binary size and startup budget
- OPFS availability and fallback behavior, especially across browsers
- SharedWorker/tab-broker support and ownership handoff
- React Native SQLite packaging and storage behavior
- NAPI/native distribution across platforms
- keeping generated TypeScript types and Rust catalogue codecs in lockstep

## 36. Errors And Explanations

Errors should be structured, discriminable, and usable from both promise-based
write APIs and global runtime callbacks.

Application-facing error surfaces include:

- write promise rejection
- transaction fate rejection
- global rejection/error callback
- subscription error callback
- query failure
- sync connection error

Promise rejection and the global rejection/error callback should receive the
same error object shape for the same transaction outcome.

Errors should carry stable machine codes plus human-readable messages. Human
messages may evolve; machine codes are the compatibility surface.

Likely machine-code families include:

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

Transaction rejection reasons should be durable but should not live as a wide
field on the hot transaction row. The baseline layout should use a rejection
detail side table keyed by transaction id, keeping `jazz_tx` fast to query while
preserving replay/debug information.

Policy denial and validation explanations should be as detailed as is safe
without leaking privileged information. Ordinary clients must not be able to
distinguish hidden rows from nonexistent rows through error detail. Trusted-peer
and authority-side debug logs should preserve as much detail as possible for
operators and developers.

Recoverable subscription gaps, such as missing catalogue state or sync state
that may still arrive, should appear as "not settled yet" rather than immediate
errors. They become errors only after an explicit timeout, cancellation, or
irrecoverable protocol/storage failure.

Developer diagnostics may be richer and less stable than application errors.
Useful diagnostics include SQL lowering traces, policy lowering traces, missing
index advice, recursive policy unsupported-shape reports, schema/lens graph
errors, generated physical layout explanations, and subscription invalidation
explanations.

Open issues:

- exact stable error-code taxonomy
- public error object shape
- timeout defaults for unsettled subscriptions and tiered one-shot queries
- redaction rules for policy and validation explanations

## Appendix A: Implementation Strategy For Attempt 3

Attempt 3 should implement this spec as a cohesive mini-system, not as a direct
continuation of Attempt 2 code.

The implementation should start fresh and copy only deliberate helpers, tests,
and learnings.

It should be organized around data artifacts and verbs rather than manager
objects.

Core artifacts:

- `SchemaDef`
- `PhysicalLayout`
- `IdCodec`
- `EnumCodec`
- `TablePlan`
- `ProjectionPlan`
- `VisibilityPlan`
- `QueryPlan`
- `ScopePlan`
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
- `diff_projection`
- `poll_subscription`

Suggested implementation slices:

1. physical layout, id codec, enum codec, and DDL
2. local write/query/current projection
3. deterministic projection rebuild and projection diff
4. query scope
5. subscriptions
6. sync apply/export
7. authority validation
8. branch visibility
9. historical snapshots
10. conflict candidates
11. one narrow policy or lens slice

Tests should be product-shaped integration tests using realistic fixtures such
as projects and todos, and actors such as Alice, Bob, and the core authority.

Performance tests should remain runnable examples or benches until stabilized.
They should measure layout overhead, id representation, enum representation,
read/write-set storage, query plan shape, and memory representation.

## Appendix B: Rationale And Rejected Alternatives

Append-only history plus rebuildable projections was selected because prior
prototypes showed it handles rejection repair, restart/rebuild, sync replay, and
historical reads with one source of truth.

Mutable fate on `jazz_tx` was selected for v0 because it preserves stable
transaction identity and gives simple visibility predicates. Append-only fate
receipts are deferred.

Local integer surrogates were selected over text ids in hot tables because
physical layout experiments showed repeated text ids dominate disk and memory
overhead. Fixed-width binary public ids remain a future revisit.

Integer enum discriminants were selected over text enum labels because enum
labels are expensive in hot rows and discriminants are stable durable format
facts.

Query-scoped sync was selected over table replication because clients should
receive the history/fate/scope needed for active queries, not unrelated table
state.

Rerun-and-diff subscriptions were selected as the correctness baseline because
they align one-shot query semantics and live query semantics while leaving room
for finer invalidation.

Broad manager-object architecture was rejected for the next attempt because it
hid important database facts in orchestration objects. The implementation should
prefer explicit plans, codecs, and verbs.

Prior prototypes did not settle full conflict semantics, canonical vector
encoding, multi-base branch provenance, policy language, schema lenses, or final
performance.

## Appendix C: Possible Future Revisits

### C.1 Fixed-Width Binary Ids

If public Jazz ids gain a canonical compact binary representation, fixed-width
BLOB ids may be compared against local integer surrogates.

The comparison should measure:

- disk size
- insert speed
- query speed
- sync sync-apply/export cost
- memory representation
- mapping-table overhead avoided or introduced

Until then, local integer surrogates are the selected physical baseline.

### C.2 Append-Only Fate Receipts

Mutable fate on `jazz_tx` is the selected v0 design. Append-only fate receipts
may later be added for auditability, replication handoff, or debugging.

### C.3 Hot Branch Projections

Pure-query branch reads are the correctness baseline. Hot branches may justify
projection tables, but the base design should not require projection tables for
all branches.

### C.4 Indexed Read/Write-Set Side Tables

Inline transaction metadata is the selected v0 design. Side tables may be added
if authority validation measurements show that joins are cheaper than decoding
metadata blobs.

## Appendix D: spec Caveat

This is a serious design document, but it is still pre-implementation.

The document is intentionally more detached than the attempt logs. That is the
point: it forces the design to say what the database means without relying on
shared Jazz vocabulary or prototype context.

The next attempt should be allowed to falsify parts of this spec. When it does,
the result should be a sharper spec, not just another patch to the prototype.
