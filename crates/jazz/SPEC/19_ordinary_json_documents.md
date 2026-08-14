# jazz — Specification · 19. Ordinary-row JSON documents

## Overview

Queryable JSON documents are a library-level data model built exclusively from
ordinary Jazz tables, references, transactions, queries, permissions, history,
and branches. They are not a column class, merge strategy, wire payload, storage
layer, or sync protocol feature.

This chapter defines the initial representation and public API contract. Its
purpose is to keep the core boundary explicit while providing a first-class
document abstraction whose current and historical values do not require replay
of parent Jazz row versions.

Invariant digest:

- `INV-JDOC-1`: A JSON document commit MUST be one ordinary Jazz transaction containing the new immutable document nodes, the document-root update, and every affected declared-path projection update.
- `INV-JDOC-2`: Every committed document root MUST name a complete immutable logical snapshot; reconstructing that snapshot MUST traverse ordinary rows reachable from that root and MUST NOT scan or replay parent Jazz row versions.
- `INV-JDOC-3`: JSON document rows, nodes, and projections MUST use ordinary application tables and MUST NOT introduce a special value, operation, protocol frame, storage table, authorization path, fate path, or sync path in core.
- `INV-JDOC-4`: Declared-path projection rows MUST be derived from the exact root committed in the same transaction; a query observing that root MUST NOT observe projection values from another root.
- `INV-JDOC-5`: Historical roots and their reachable immutable nodes MUST remain readable for as long as the ordinary Jazz rows that reference them remain retained and authorized.
- `INV-JDOC-6`: Localized JSON edits SHOULD create a bounded root-to-leaf path of immutable nodes plus affected projection versions rather than rewriting the full logical document.

## Details

### 19.1 Boundary

The feature is an ordinary-schema library. The core sees only inserts and
updates of scalar/array/reference columns. Therefore existing transaction,
authorization, branching, history, lens, sync, fate, and storage rules apply
without document-specific branches.

The library MAY provide schema builders, codecs, navigation helpers, mutation
helpers, and query helpers. It MUST NOT depend on core-private storage access.

### 19.2 Logical tables

One document collection uses three roles. Applications MAY choose their table
names, policies, and declared query paths.

```text
documents
  root_id        UUID reference to an immutable root node
  ...application columns

document_nodes
  document_id    UUID reference to the owning document
  kind           object | array | scalar | internal
  child ids / persistent-sequence metadata
  scalar payload columns

document_paths
  document_id
  path           canonical declared JSON path
  typed scalar projection
```

Nodes are immutable after insertion. A document's mutable `root_id` is the
versioned identity of its complete current logical value. Unchanged subtrees are
shared between roots. An edit inserts the changed leaf and replacement ancestors
then advances `root_id` in the same transaction (`INV-JDOC-1`, `INV-JDOC-6`).

The first vertical slice MAY use a flat immutable root containing the complete
ordered leaf-reference sequence. That is a correct independently readable
snapshot but has O(number of leaves) root metadata per edit. It is a bootstrap
representation, not the scale target. The scale target is a bounded-fanout
persistent tree with O(log n) replacement nodes for localized edits.

### 19.3 Snapshot reconstruction

The root is data, not a hint into Jazz history. Reading a current or retained
historical root traverses immutable current rows reachable from that exact root.
It never asks which edits occurred since a parent document row version and never
replays the document's Jazz row-version ancestry (`INV-JDOC-2`).

Jazz time travel still determines which `documents.root_id` version is visible
at a historical cut. Once that root is selected, document reconstruction is
independent of the depth of the document row's history.

### 19.4 Query projections

Cross-document filters over declared scalar paths use ordinary projection rows.
A declaration associates a canonical JSON path with a typed projection. A
localized edit reports changed paths, and the same transaction that advances the
root updates only affected projections (`INV-JDOC-4`).

Projection rows are ordinary replicated data, not node-local hidden indexes.
Consequently their authorization, offline writes, history, branch overlays,
subscriptions, and conflict behavior are inspectable through the normal Jazz
model. A future general secondary-index facility may optimize the physical
query without changing this logical representation.

For each declared `(document_id, path)`, the mutation API requires exactly one
current projection row. Before advancing a root it verifies that row names the
observed old root and contains the scalar represented by that root. Missing,
duplicate, stale-root, or stale-value projection rows MUST fail closed before a
transaction opens; selecting an arbitrary duplicate and advancing only that row
would leave cross-document filters inconsistent with the document root.

Ad-hoc arbitrary-path queryability is not promised by the first API. An
application can declare more paths or explicitly opt into an all-scalar-path
projection, accepting its row and write amplification.

### 19.5 Permissions and lifecycle

Applications attach ordinary policies to all three tables. A deployment MUST
ensure that reading a node or projection cannot grant access beyond its owning
document. The library does not invent a content-address-as-authority rule.

Creating nodes and advancing the authorized document root happen atomically.
The general authorization model must permit transaction-created child rows when
the same transaction establishes their authorized ownership path; until that
contract is available to untrusted clients, the library must surface the policy
requirement rather than silently weaken child insertion policy.

Immutable nodes reachable from retained historical roots remain live
(`INV-JDOC-5`). Garbage collection is therefore ordinary reachability/retention
work and MUST NOT collect content that a retained authorized history root still
names.

### 19.6 Merge semantics

The initial library provides deterministic snapshot editing and ordinary Jazz
branch/history behavior. It does not claim automatic concurrent structural JSON
merge. Concurrent updates to one document root follow the root column's ordinary
merge strategy. A later userland merge library may compare complete roots and
emit a new complete merged root as one ordinary transaction.

### 19.7 Performance contract

Receipts report at least:

- logical payload size and scalar leaf count;
- rows inserted for initial creation;
- new rows and row versions per localized edit and array insertion/deletion;
- current and retained-root reconstruction latency;
- declared-path filter latency at 100, 1,000, and 10,000 documents where feasible;
- subscription delivery fanout for one leaf/path edit;
- logical, wire, and persisted bytes when those counters are available.

Timing receipts MUST identify build profile, storage, topology, and durability.
In-memory debug timings are directional feasibility evidence, not production
performance claims.

## Open questions

1. What bounded-fanout node schema gives good recursive hydration without a
   document-specific core query primitive?
2. Should internal immutable node identity be random, content-derived, or allow
   both modes? Content-derived identity must not become authorization.
3. Which general ordinary-row permission pattern authorizes transaction-created
   descendants through a same-transaction parent/root reference?
4. Should declared-path projections be one typed table per declaration, a
   column family on the document table, or a shared typed projection table?
5. What ordinary retention signal makes immutable-node garbage collection safe
   across history and branches?
