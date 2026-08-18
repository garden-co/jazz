# jazz — Specification · 11. Partitioned history, overlay views & time travel

## Overview

Jazz does not own a branch object, branch lifecycle, branch identifier, or
branch-routing protocol. Applications model drafts, branches, environments,
scenarios, and similar concepts as ordinary rows. The core owns only the
relational mechanism those products need: schema-declared **partition
dimensions**, partition-qualified history and winner selection, and a read view
that overlays one current partition over an optional live or frozen base.

A table binds zero or more ordinary application columns to globally named
partition dimensions. The columns remain visible to queries, references, and
policies, while their encoded values also form an immutable storage coordinate.
Global object identity remains `RowUuid`; one object may have a distinct
incarnation in every partition tuple.

For each row and independent content/deletion layer, an overlay read chooses the
head-partition winner when one exists and otherwise chooses the base-partition
winner. This reduction happens before visibility, predicates, joins, policy,
aggregation, windows, or index-result publication. An application resolves any
branch row and supplies the concrete head and base selectors; Jazz does not
traverse or validate an application branch graph.

Invariant digest:

- `INV-TIME-1`: A historical read at `GlobalSeq` position MUST consider only globally settled transactions with `global_seq <= position` and MUST choose row/layer winners using the ordinary current-state rules over that subset.
- `INV-TIME-2`: A historical read MUST evaluate read policy over the historical state at the requested cut, not over current state.
- `INV-TIME-3`: `at_time(time)` MUST resolve to the latest settled global position whose transaction time is `<= time`, returning `GlobalSeq(0)` when none exists.
- `INV-TIME-4`: A local historical read MUST refuse to answer from incomplete local history.
- `INV-TIME-5`: A history-complete node at a sufficient watermark MUST answer an exact-position historical read locally.
- `INV-PART-1`: Partition dimensions MUST have schema-lineage-stable identities, names, types, canonical encodings, and deterministic order; tables MAY bind any subset through renameable application columns.
- `INV-PART-2`: The logical incarnation key MUST be `(PhysicalTableId, PartitionTuple, RowUuid)` while application object identity remains `RowUuid`.
- `INV-PART-3`: Every content or deletion version on a partitioned table MUST carry a complete canonical partition tuple; its version parents MUST have the same tuple.
- `INV-PART-4`: Partition bindings MUST be non-null and immutable for one incarnation. Moving an object between tuples requires explicit writes to both incarnations, which MAY share one atomic transaction.
- `INV-PART-5`: Content and deletion histories and current winners MUST be selected independently per `(PhysicalTableId, PartitionTuple, RowUuid, Layer)`.
- `INV-PART-6`: Secondary and unique indices MUST be physically prefixed by the exact partition tuple; uniqueness is per exact tuple, not per composed overlay view.
- `INV-PART-7`: A table with no bound dimensions MUST behave as shared data in every overlay view.
- `INV-PART-8`: A read selector MUST use globally named dimension values and each table MUST project that selector onto its declared subset; equal projected head/base tuples collapse to one source.
- `INV-PART-9`: An overlay MUST select head winners before base winners independently for content and deletion layers, and MUST perform that masking before predicates or relational operators.
- `INV-PART-10`: A content write MUST NOT imply restoration; an inherited or head-partition `Deleted` winner remains effective until an explicit `Restored` winner supersedes it.
- `INV-PART-11`: A base source MUST be either live current state or the exact state of the selected partition at a supplied `SnapshotRef`; the cut applies consistently to every table and policy dependency in the read.
- `INV-PART-12`: The canonical read/subscription identity MUST include normalized head and base partition sources, including any snapshot cut.
- `INV-PART-13`: Normal references MUST resolve a `RowUuid` through the current effective overlay view; exact-incarnation references are a separate, unsupported capability.
- `INV-PART-14`: A view-relative mutation of an inherited row MUST copy-on-write into the head partition. Exact-incarnation mutation MUST name its partition tuple explicitly.
- `INV-PART-15`: Effective rows MUST distinguish the requested head tuple from the physical partition that supplied each selected layer; ordinary partition columns project to the head values while hidden provenance retains supplying tuples.
- `INV-PART-16`: Transactions MAY atomically contain versions in multiple partition tuples, but admission, fate, persistence, and rejection remain all-or-nothing.
- `INV-PART-17`: Trusted replication MAY carry complete cross-partition commit units; untrusted selected delivery MUST NOT reveal unauthorized sibling versions, tables, tuples, payloads, or counts merely because they share a transaction.
- `INV-PART-18`: Read and write policy MUST use ordinary partition columns and the same effective view as the operation; missing reference/policy evidence fails closed, and Jazz MUST NOT impose a built-in partition-row existence or lifecycle gate.
- `INV-PART-19`: Schema evolution MAY only add a partition dimension with an immutable typed migration default. Historical versions and old-schema selectors MUST normalize to that default; removal, identity rename, type/encoding/default change, and tuple collapse are forbidden.
- `INV-MERGE-1`: Cross-partition merge calculation MUST remain a local, authorized helper that emits one ordinary atomic mergeable transaction; receivers MUST NOT require source history to admit its result.
- `INV-MERGE-2`: Merge provenance MUST identify source and target contributions by stable partition coordinates and exact field-grained contribution dots.
- `INV-MERGE-3`: A merge calculator MUST recursively subtract source contributions already represented in the target and MUST NOT echo target-originated effects back to their origin.
- `INV-MERGE-4`: Emitted version parents MUST be only the target incarnation's observed row/layer heads; source-partition versions are never causal parents.
- `INV-MERGE-5`: Merge calculation MUST fail atomically when exact contribution history, strategy capabilities, current-schema projection, source-read authority, or target-write authority is unavailable.
- `INV-MERGE-6`: Prior provenance visible in the target prevents observed duplicate transfer, but Jazz MUST NOT claim globally coordinated exactly-once behavior for concurrent offline calculations.

## Details

### 11.1 Historical reads

A historical read exposes globally settled state at a `GlobalSeq` cut. It
includes only transactions with `global_seq <= position`, chooses independent
content and deletion winners over that subset, derives visibility, and evaluates
the complete query and read policy against that historical state
(`INV-TIME-1`, `INV-TIME-2`).

`at_time(time)` is a convenience that resolves the latest settled position whose
transaction time is `<= time`, or `GlobalSeq(0)` if no transaction qualifies.
Clock skew means this is a best-effort lookup, not wall-clock truth
(`INV-TIME-3`).

A node may answer locally only when it can prove history completeness for the
requested shape and cut. Otherwise it returns a historical-read capability
error or routes to a history-complete authority; it never fabricates a partial
answer (`INV-TIME-4`). Historical reads are immutable one-shots rather than live
subscriptions.

### 11.2 Partition dimensions

A schema lineage declares globally named partition dimensions. Each dimension
has a stable internal `PartitionDimensionId`, a stable semantic name, one
non-null key-encodable type, one canonical wire/storage encoding, and a stable
ordering position. A table binds an application column to any subset of those
dimensions:

```text
dimensions:
  workspace: Uuid
  branch: Uuid

todos:
  workspace_id: Uuid  -> dimension workspace
  draft_id: Uuid      -> dimension branch
  partitionBy: [workspace_id, draft_id]

memberships:
  workspace_id: Uuid  -> dimension workspace
  partitionBy: [workspace_id]

users:
  partitionBy: []
```

Application columns may later be renamed while retaining the same dimension
binding. Canonical partition identity uses stable dimension ids and typed bytes,
never current column names or declaration order (`INV-PART-1`). Two tables that
bind the same named dimension must use the same type and encoding.

The empty tuple denotes shared data. It is not a privileged root partition and
has no lifecycle semantics (`INV-PART-7`).

#### Incarnation identity

`RowUuid` remains the stable application object identity. The same `RowUuid` may
have content and deletion histories in many partition tuples. The physical
incarnation key is:

```text
(PhysicalTableId, PartitionTuple, RowUuid)
```

and each layer winner is keyed by that incarnation plus `Content` or `Deletion`
(`INV-PART-2`, `INV-PART-5`). A raw fetch of a partitioned object therefore
requires a read view or an exact partition tuple.

Partition columns are ordinary values for query projection, reference traversal,
and policy, but key-like coordinates for mutation. Every version must carry the
complete canonical tuple. A patch cannot omit, inherit ambiguously, or change
that coordinate, and a version parent must belong to the same tuple
(`INV-PART-3`, `INV-PART-4`). An application move is an explicit atomic write to
the source and destination incarnations, not a cross-partition parent edge.

#### Storage and indices

Content and deletion history use the same partition coordinate. Sparse deletion
records are keyed by:

```text
(PhysicalTableId, PartitionTuple, RowUuid, TxId)
```

and never affect another tuple with the same `RowUuid`. Current caches preserve
independent content/deletion winner identities so they remain rebuildable from
history.

Every physical secondary or unique index implicitly prefixes its user key with
the partition tuple:

```text
(PhysicalTableId, PartitionTuple, UserIndexKey..., RowUuid)
```

The application declares only its user columns. Unique constraints are enforced
within one exact tuple. Composing a head over a base does not create a new
constraint domain and may expose equal indexed values from different row ids
even when both partitions are independently valid (`INV-PART-6`).

### 11.3 Overlay read views

The canonical request uses named dimension values even when an ergonomic facade
accepts schema-ordered arrays:

```text
head = Current({ workspace: W, branch: Draft })

base = Current({ workspace: W, branch: Main })
    or Snapshot({ workspace: W, branch: Main }, SnapshotRef)
    or absent
```

Only the head may be current in the initial surface. A snapshot head, multiple
bases, and dynamic base traversal are later capabilities. The application reads
whatever ordinary row represents its branch and supplies the resolved selector;
Jazz neither knows nor validates that representation.

Each participating table projects the selector onto its declared dimension
subset (`INV-PART-8`). With the example schema above:

```text
todos:       head=(W,Draft), base=(W,Main)
memberships: head=(W),       base=(W)       -> one shared source
users:       head=(),        base=()        -> one global shared source
```

A missing required named dimension is a validation error. An unrecognized extra
dimension is rejected rather than silently ignored. Positional syntax is only
facade sugar and is canonicalized immediately.

#### Independent layer reduction

For table `T`, row `R`, projected head tuple `H`, and optional base tuple `B`:

```text
effective_content(R) = content_winner(H,R) ?? content_winner(B,R)
effective_deletion(R) = deletion_winner(H,R) ?? deletion_winner(B,R)
visible(R) = effective_content exists
             and effective_deletion != Deleted
```

For a snapshot base, both base winners are chosen at the supplied cut. The cut
applies to every table, join, reference, and policy dependency in the read
(`INV-PART-11`).

Layer fallback is intentionally independent. Head content does not restore an
inherited deletion; a head `Restored` winner may reveal inherited content; a
head deletion hides either head or inherited content (`INV-PART-9`,
`INV-PART-10`).

Masking precedes predicates. If base row A has `status="open"` and head row A
has `status="closed"`, a query for open rows must not return base A. Index plans
must therefore anti-join base matches against all head-touched row ids for the
relevant layers, not merely against head rows that also match the predicate.

All downstream relational behavior consumes the effective sources: filters,
joins, includes, recursive reachability, aggregates, windows, policy joins, and
replacement witnesses. The overlay mechanism is source resolution, never a
late result filter.

#### Effective values and source provenance

An effective row distinguishes its requested view from its supplying history.
Ordinary bound partition columns project to the head selector even when content
or deletion fell back to the base. Hidden typed provenance records the exact
partition and version that supplied each selected layer (`INV-PART-15`). This
gives application code a coherent draft-shaped row while preserving exact
history for authorization diagnostics, synchronization, and merge calculation.

Normal `RowUuid` references resolve the visible target incarnation through the
same effective view. An exact-incarnation reference containing a partition tuple
is a distinct future capability (`INV-PART-13`). Policy reference traversal uses
the same rule.

### 11.4 Mutations and authorization

An exact mutation names `(table, PartitionTuple, RowUuid)`. A view-relative
mutation names the head view and `RowUuid`; if the visible row is inherited, the
mutation performs copy-on-write into the head tuple (`INV-PART-14`). The helper
may materialize inherited values needed by a merge strategy, but the first head
version has no cross-partition causal parent. Source derivation may be retained
only as typed non-causal provenance.

Content writes do not implicitly author `Restored`. An ergonomic update helper
may deliberately emit content and restoration together in one transaction, but
the transaction payload must make both effects explicit.

Jazz imposes no built-in branch row, creator, open/closed state, parent, or
existence check. Applications express those rules with ordinary tables and
policies, for example by traversing `todo.branch_id -> branches.id`. A missing
referenced branch or membership row is missing policy evidence and fails closed.
Read policy evaluates over the effective overlay view; write policy evaluates
the exact target tuple and the operation's candidate view (`INV-PART-18`).

#### Cross-partition atomicity

One transaction may write several partition tuples and shared tables. Every
version routes by its own canonical tuple, while the transaction retains one
identity, permission subject, fate, limit budget, durability state, and atomic
admission decision (`INV-PART-16`). Any malformed tuple or denied write rejects
the complete unit.

Version-parent edges remain tuple-local. Cross-partition atomic grouping does
not make one incarnation causally descend from another.

Trusted history replication may carry complete commit units. Client-facing
selected delivery must not reveal unauthorized sibling versions or even hidden
table/tuple/write-count structure merely because an authorized version shares
their `TxId`. Selected view facts may retain the transaction witness needed for
settlement without reconstructing hidden payload (`INV-PART-17`, ch. 8).

### 11.5 Maintained subscriptions

The normalized head/base sources are part of canonical `ReadViewKey`,
`SubscriptionKey`, binding-view identity, known state, coverage, settlement,
and cache reuse (`INV-PART-12`). Equal shapes and bindings over different tuples
must never share results, policy facts, replacement witnesses, receipts, or
unsubscribe cleanup.

A live base and current head are both maintained inputs. A frozen base is an
immutable historical input and only the head is live. The maintained graph
tracks content and deletion winners independently so head add, replace, delete,
and restore events produce the same result as a one-shot read without full
refresh. A base change affects a live-base view only when the corresponding
layer is not masked by a head winner.

Policy dependency tables project the same named selectors as user-query tables.
Tables whose projected head and base tuples are equal have one maintained source,
preventing duplicate shared evidence.

### 11.6 Cross-partition merge helper

Cross-partition merge is a high-level local calculation, not an admission or
replication primitive. Given explicit source and target read views, an initiating
identity, and complete authorized history, the helper calculates novel source
contributions and emits one ordinary atomic mergeable transaction whose versions
carry target partition tuples (`INV-MERGE-1`). A receiver applies that transaction
without source history.

#### Contribution identity

A contribution dot is field-grained and partition-qualified:

```text
PartitionCoordinate = sorted [(PartitionDimensionId, EncodedValue)]

ContributionDot = {
  partition,
  tx_id,
  table,
  row_uuid,
  layer,
  column-or-operation,
}
```

Merge provenance defines non-causal substitutions from each emitted target
coordinate to the exact source dots it represents (`INV-MERGE-2`). Transaction
identity alone never marks another row, layer, column, or operation as known.

The calculator recursively expands valid prior substitutions and subtracts every
source dot already represented by the target. This prevents root-originated
counter deltas or scalar writes from echoing home through chains such as
`A -> B -> C -> A` (`INV-MERGE-3`).

The output version names only the target incarnation's current row/layer heads
as causal parents. Source versions and frozen-base versions are never target
parents (`INV-MERGE-4`). Each merge strategy must expose exact native
contribution extraction and target-relative encoding. Authored presence,
deletion/restore operations, schema projection, and losing concurrent dots are
all preserved in the contribution calculation.

The initiator must be able to read every included source effect and pass every
ordinary target write check. Missing history, incomplete projection, unreadable
content, or an unsupported strategy fails the whole local calculation before a
transaction is minted (`INV-MERGE-5`).

Prior provenance visible in the target suppresses an observed retry. Independent
offline calculators that do not observe each other may still emit concurrent
duplicates; Jazz provides no global merge cursor or exactly-once import claim
(`INV-MERGE-6`).

### 11.7 Schema evolution

Partition evolution is monotone. A schema may add a globally named dimension and
bind it to tables only when the publication supplies one immutable, typed,
deterministic migration default. Old history and selectors authored under an
older schema normalize the missing dimension to that default (`INV-PART-19`).

For example:

```text
v1 tuple: (branch=A)
v2 tuple: (workspace=DEFAULT, branch=A)
```

All old incarnations enter the reserved default bucket. New-schema writes must
explicitly provide the new dimension. Only old-schema queries receive automatic
default completion; omission in a new-schema query is an error. A cross-schema
version parent is valid only when both effective normalized tuples are equal.

The migration default is schema-lineage metadata, not an ordinary insert
default. It must never change. Adding a dimension rekeys/rebuilds current and
secondary-index state and normalizes deletion history and contribution
coordinates consistently. Prefer reserved schema-minted UUIDs or stable enum
identities over user-reachable sentinel values.

The initial contract forbids removing a dimension, renaming its stable semantic
identity, changing its type or encoding, changing its default, splitting or
collapsing dimensions, and nullable partition bindings. Application columns may
be renamed because their binding retains the stable dimension id.

## Open Questions

- **Selected delivery for cross-partition transactions.** Specify the minimal
  transaction witness an untrusted receiver needs for atomic settlement without
  exposing hidden sibling count or structure.
- **Exclusive transactions.** Define predicate-read coordinates and conflict
  validation for overlay views before allowing view-relative exclusive writes.
- **Dimension type surface.** Start with UUIDs, stable enums, and fixed-width
  integers; decide whether strings or composite values can ever provide stable
  canonical coordinates.
- **Multiple bases.** An ordered fallback stack is a natural extension, but the
  initial implementation supports at most one base.
- **Exact-incarnation references.** Add only if product use cases cannot be
  expressed by view-relative `RowUuid` references.
- **Dynamic base resolution.** Applications resolve branch rows initially;
  relation-driven base selection, cycle handling, and live base-pointer changes
  are future query composition work.
