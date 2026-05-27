# Terminology

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
writer identities; users are authorization identities.

### 5.4 User And Session

A user is the authorization identity the database believes is acting. The
identity may represent a human user, a service user, or an admin user. A trusted
runtime peer is not itself a user merely because it is trusted; it executes work
under an explicit session user or under an admin session.

A session is the execution context for a query, write, or sync connection. It
carries user, trust role, auth mode, policy context, and runtime context.

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

Committing an explicit transaction with no staged mutations is a no-op and must
not allocate a public transaction id. Within a non-empty explicit transaction,
multiple staged mutations to the same row are normalized to one final semantic
row mutation before sealing. The normalized transaction still records the
observed facts needed to justify that final mutation.

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
