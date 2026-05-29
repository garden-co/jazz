# Visibility, Snapshots, And Branches

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

The core should support two uses of global epoch snapshots:

- full-system snapshot export/backup, where the output is complete authority
  state as of that epoch rather than a user-policy-filtered query result
- policy-filtered historical query evaluation, useful for previews such as
  branch creation from a historical base

Applications often want wall-clock history rather than explicit authority
epochs. The product should therefore include an `as-of time` query/export
placeholder that resolves a timestamp to the appropriate authority snapshot.
The exact timestamp-to-epoch mapping, clock authority, and behavior around
transactions sharing one epoch remain open.

Future discussion: `as-of time` is likely the user-facing historical query API,
but it needs a careful clock-authority and epoch-mapping design before it can be
specified precisely.

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
