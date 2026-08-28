# jazz — Specification · 11. Branch views & time travel

## Overview

Jazz does not own a branch object, branch lifecycle, branch identifier, or
branch-routing protocol. Applications model drafts, branches, environments,
scenarios, and similar concepts as ordinary rows. The core owns only the
relational mechanism those products need: schema-declared **branch columns**,
branch-key-qualified history and winner selection, and a read view
that overlays one current branch key over an optional live or frozen base.

A table names zero or more ordinary application columns in `branchBy`. The
columns remain visible to queries, references, and policies, while their encoded
values also form an immutable storage coordinate. There is no stored branch-column
identity, binding, declaration, or branch object.
Global object identity remains `RowUuid`; one object may have a distinct
branch-local row in every branch key.

User-facing prose calls that coordinate-specific object a **branch-local row**.
“Branch version” is intentionally avoided: throughout Jazz, a row version is
one immutable history entry, and one branch-local row can have many such
versions.

For each row and independent content/deletion layer, a branch-view read chooses the
head-key winner when one exists and otherwise chooses the base-key
winner. This reduction happens before visibility, predicates, joins, policy,
aggregation, windows, or index-result publication. An application resolves any
branch row and supplies the concrete head and base selectors; Jazz does not
traverse or validate an application branch graph.

Invariant digest:

- `INV-TIME-1`: A historical read at `GlobalTime` position MUST consider only globally settled transactions with `global_time <= position` and MUST choose row/layer winners using the ordinary current-state rules over that subset.
- `INV-TIME-2`: A historical read MUST evaluate read policy over the historical state at the requested cut, not over current state.
- `INV-TIME-3`: `at_time(time)` MUST resolve to the latest settled global position whose transaction time is `<= time`, returning `GlobalTime(0)` when none exists.
- `INV-TIME-4`: A local historical read MUST refuse to answer from incomplete local history.
- `INV-TIME-5`: A history-complete node at a sufficient watermark MUST answer an exact-position historical read locally.
- `INV-BVIEW-1`: Every `branchBy` entry MUST name a non-null, key-encodable ordinary column; same-named branch columns across tables MUST have the same type and canonical encoding.
- `INV-BVIEW-2`: The logical branch-local row key MUST be `(PhysicalTableId, BranchKey, RowUuid)` while application object identity remains `RowUuid`.
- `INV-BVIEW-3`: Every content or deletion version on a branch-keyed table MUST carry a complete canonical branch key; its version parents MUST have the same branch key.
- `INV-BVIEW-4`: Branch columns MUST be immutable after insertion. Moving an object between branch keys requires explicit writes to both branch-local rows, which MAY share one atomic transaction.
- `INV-BVIEW-5`: Content and deletion histories and current winners MUST be selected independently per `(PhysicalTableId, BranchKey, RowUuid, Layer)`.
- `INV-BVIEW-6`: Secondary indices MUST be physically prefixed by the exact branch key; a composed branch view MUST apply head/base masking before consulting or publishing index results.
- `INV-BVIEW-7`: A table with no branch columns MUST behave as shared data in every branch view.
- `INV-BVIEW-8`: A read selector MUST use branch-column names and each table MUST project that selector onto its declared subset; equal projected head/base branch keys collapse to one source.
- `INV-BVIEW-9`: An overlay MUST select head winners before base winners independently for content and deletion layers, and MUST perform that masking before predicates or relational operators.
- `INV-BVIEW-10`: A content write MUST NOT imply restoration; an inherited or head-key `Deleted` winner remains effective until an explicit `Restored` winner supersedes it.
- `INV-BVIEW-11`: A base source MUST be either live current state or the exact state of the selected branch key at a supplied `SnapshotRef`; the cut applies consistently to every table and policy dependency in the read.
- `INV-BVIEW-12`: The canonical read/subscription identity MUST include normalized head and base branch sources, including any snapshot cut.
- `INV-BVIEW-13`: Normal references MUST resolve a `RowUuid` through the current effective branch view; branch-qualified row references are a separate, unsupported capability.
- `INV-BVIEW-14`: A view-relative mutation of an inherited row MUST copy-on-write into the head branch key. An exact mutation MUST name its branch key explicitly.
- `INV-BVIEW-15`: Effective rows MUST distinguish the requested head branch key from the physical branch key that supplied each selected layer; ordinary branch columns project to the head values while hidden provenance retains supplying branch keys.
- `INV-BVIEW-16`: Transactions MAY atomically contain versions in multiple branch keys, but admission, fate, persistence, and rejection remain all-or-nothing.
- `INV-BVIEW-17`: Trusted replication MAY carry complete cross-branch-key commit units; untrusted selected delivery MUST NOT reveal unauthorized sibling versions, tables, branch keys, payloads, or counts merely because they share a transaction.
- `INV-BVIEW-18`: Read and write policy MUST use ordinary branch columns and the same effective view as the operation; missing reference/policy evidence fails closed, and Jazz MUST NOT impose a built-in branch-row existence or lifecycle gate.
- `INV-BVIEW-19`: Schema evolution MAY only add a `branchBy` column with an immutable typed default. Historical versions and old-schema selectors MUST normalize to that default; removal and type/encoding/default change are forbidden. Ordinary schema lineage may rename the column while preserving its physical column identity.
- `INV-MERGE-1`: Cross-branch-key merge calculation MUST remain a local, authorized helper that emits one ordinary atomic mergeable transaction; receivers MUST NOT require source history to admit its result.
- `INV-MERGE-2`: Merge provenance MUST identify source and target contributions by stable branch coordinates and exact field-grained contribution dots.
- `INV-MERGE-3`: A merge calculator MUST recursively subtract source contributions already represented in the target and MUST NOT echo target-originated effects back to their origin.
- `INV-MERGE-4`: Emitted version parents MUST be only the target branch-local row's observed row/layer heads; source-key versions are never causal parents.
- `INV-MERGE-5`: Merge calculation MUST fail atomically when exact contribution history, strategy capabilities, current-schema projection, source-read authority, or target-write authority is unavailable.
- `INV-MERGE-6`: Prior provenance visible in the target prevents observed duplicate transfer, but Jazz MUST NOT claim globally coordinated exactly-once behavior for concurrent offline calculations.

## Details

### 11.1 Historical reads

A historical read exposes globally settled state at a `GlobalTime` cut. It
includes only transactions with `global_time <= position`, chooses independent
content and deletion winners over that subset, derives visibility, and evaluates
the complete query and read policy against that historical state
(`INV-TIME-1`, `INV-TIME-2`).

`at_time(time)` is a convenience that resolves the latest settled position whose
transaction time is `<= time`, or `GlobalTime(0)` if no transaction qualifies.
Clock skew means this is a best-effort lookup, not wall-clock truth
(`INV-TIME-3`).

A node may answer locally only when it can prove history completeness for the
requested shape and cut. Otherwise it returns a historical-read capability
error or routes to a history-complete authority; it never fabricates a partial
answer (`INV-TIME-4`). Historical reads are immutable one-shots rather than live
subscriptions.

### 11.2 Branch columns

Each table names a subset of its ordinary columns in `branchBy`:

```text
todos:
  workspace_id: Uuid
  branch_id: Uuid
  branchBy: [workspace_id, branch_id]

memberships:
  workspace_id: Uuid
  branchBy: [workspace_id]

users:
  branchBy: []
```

The durable public schema stores only the ordinary column definitions and their
names in `branchBy`:

```json
{
  "tables": {
    "todos": {
      "columns": [{ "name": "workspace_id", "column_type": { "type": "Uuid" }, "nullable": false }],
      "branchBy": ["workspace_id"]
    }
  }
}
```

Entries naming missing, nullable, duplicate, or non-key-encodable columns fail
schema admission. A branch column is immutable after insertion. Across all
tables in a schema, branch columns with the same name must have the same type
and canonical encoding (`INV-BVIEW-1`, `INV-BVIEW-4`). Tables may use different
subsets; a table with no `branchBy` columns is shared.

Mutation admission enforces that immutability directly. Jazz derives the
canonical branch-column cells from the mutation's branch selector and rejects
an explicitly authored value when it disagrees with the selector. Parent
versions must use the same canonical branch key, and replicated versions are
revalidated against both their stored key and branch-column cells before
admission. Moving an object therefore means writing its `RowUuid` in another
branch view, not updating the branch column of an existing branch-qualified row
version.

Column names provide the uniform selector vocabulary, not durable identities.
Ordinary schema lineage may rename a branch column because the migration lens
retains the column's existing physical identity. Historical keys authored with
the old name are projected through that lineage. Jazz stores no separate
branch-specific declaration or binding abstraction.

The empty branch key denotes shared data. It is not a privileged root branch key and
has no lifecycle semantics (`INV-BVIEW-7`).

#### Canonical branch-coordinate codec

Branch coordinates use an engine-owned binary codec, not the serde layout of
`groove::Value`, `BranchColumnValue`, or `BranchKey`. A branch-column envelope is:

```text
codec_version:u8 = 1
scalar_tag:u8
groove_value_bytes:remaining bytes
```

The scalar tags are permanently assigned as `0=U8`, `1=U16`, `2=U32`,
`3=U64`, `4=I32`, `5=I64`, `6=String`, `7=Uuid`, and `8=EnumTag`.
The payload is Groove's canonical single-field encoding under the schema-declared
column type. Selector construction may infer a scalar type before table projection;
projection MUST decode that value and re-encode it under the selected table's
declared type. In particular, a string selector for a stable enum becomes the
declared enum discriminant rather than retaining a Rust value-enum tag.

An exact branch key is:

```text
codec_version:u8 = 1
entry_count:u32 little-endian
repeated entry_count times:
  name_byte_length:u32 little-endian
  name_utf8:name_byte_length bytes
  value_byte_length:u32 little-endian
  branch_column_envelope:value_byte_length bytes
```

Entries MUST be strictly ordered by column name, with no duplicates or trailing
bytes. Decoders reject unknown versions, scalar tags, non-canonical Groove
payloads, invalid UTF-8 names, invalid lengths, and non-increasing names. A new
codec version is a storage-format migration boundary; legacy serde/postcard bytes
are not guessed from shape.

#### Branch-local row identity

`RowUuid` remains the stable application object identity. The same `RowUuid` may
have content and deletion histories in many branch keys. The physical
branch-local identity key is:

```text
(PhysicalTableId, BranchKey, RowUuid)
```

and each layer winner is keyed by that branch-local row plus `Content` or `Deletion`
(`INV-BVIEW-2`, `INV-BVIEW-5`). A raw fetch of a branch-keyed object therefore
requires a read view or an exact branch key.

Branch columns are ordinary values for query projection, reference traversal,
and policy, but key-like coordinates for mutation. Every version must carry the
complete canonical branch key. A patch cannot omit, inherit ambiguously, or change
that coordinate, and a version parent must belong to the same branch key,
physical table, row, and layer (`INV-BVIEW-3`, `INV-BVIEW-4`, `INV-HIST-18`). An application move is an explicit atomic write to
the source and destination branch-local rows, not a cross-branch-key parent edge.

Validation addresses an explicit version parent by its `(PhysicalTableId,
RowUuid, TxId)` across both content and deletion history before comparing its
`BranchKey`. Schema aliases resolve to the same physical table identity, and a
missing parent remains a pending history prerequisite. A cached parent lookup must
be row-addressable: unrelated rows from the same transaction, including
same-table siblings, must not be materialized merely to validate a parent.

#### Storage and indices

Content and deletion history use the same branch coordinate. Sparse deletion
records are keyed by:

```text
(PhysicalTableId, BranchKey, RowUuid, TxId)
```

and never affect another branch key with the same `RowUuid`. Current caches preserve
independent content/deletion winner identities so they remain rebuildable from
history.

Every physical secondary index implicitly prefixes its user key with the
branch key:

```text
(PhysicalTableId, BranchKey, UserIndexKey..., RowUuid)
```

That order is part of the durable coordinate, not a planner convention. For
example, a `title="open"` probe in branch `Draft` uses
`(PhysicalTableId, DraftKey, "open")`; it must neither probe
`(PhysicalTableId, "open")` nor share a `MainKey` probe for the same title and
`RowUuid`. Rebuilding a current/index table derives exactly those prefixes
from immutable history coordinates.

An index-coordinate layout change uses a new physical index identity; it does
not reinterpret or replace an existing index name. On open, Jazz registers and
backfills the new derived index from persisted current candidates before serving
queries, while the older derived index may remain unused. Candidate rows still
reduce to the canonical layer winner before predicates or publication, so an
older indexed candidate cannot reappear after rebuild.

The application declares only its user columns. Composing a head over a base
does not create a new physical index domain: winner masking precedes predicate
and index-result publication (`INV-BVIEW-6`). This chapter does not add a
distributed uniqueness guarantee; the open question below records the required
design work rather than treating Groove's local unique-index rejection as a
replicated conflict-resolution protocol.

Current branch-view sources do not scan every branch and discard non-matching
rows. They open one physical prefix range for each exact stored spelling of the
selected key (normally one; more only after monotone branch-column additions), for
both content and deletion winners. A head-over-base view therefore reads at
most the head and base key ranges before masking. Secondary-index probes use
the same branch-key prefix.

### 11.3 Branch views

The canonical request uses named branch-column values even when an ergonomic facade
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

Each participating table projects the selector onto its declared branch-column
subset (`INV-BVIEW-8`). With the example schema above:

```text
todos:       head=(W,Draft), base=(W,Main)
memberships: head=(W),       base=(W)       -> one shared source
users:       head=(),        base=()        -> one global shared source
```

A missing required named branch-column is a validation error. An unrecognized extra
branch-column value is rejected rather than silently ignored. Positional syntax is only
facade sugar and is canonicalized immediately.

#### Independent layer reduction

For table `T`, row `R`, projected head branch key `H`, and optional base branch key `B`:

```text
effective_content(R) = content_winner(H,R) ?? content_winner(B,R)
effective_deletion(R) = deletion_winner(H,R) ?? deletion_winner(B,R)
visible(R) = effective_content exists
             and effective_deletion != Deleted
```

For a snapshot base, both base winners are chosen at the supplied cut. The cut
applies to every table, join, reference, and policy dependency in the read
(`INV-BVIEW-11`).

The initial frozen-base implementation scans history only inside the selected
branch-key prefixes and reduces each source once at the cut; it does not walk
the complete table history separately for every row. Its cost is nevertheless
proportional to history depth within those selected branches. A future
snapshot-current materialization or checkpoint index could make this bounded
by current rows, but that optimization is not part of the initial contract.

Layer fallback is intentionally independent. Head content does not restore an
inherited deletion; a head `Restored` winner may reveal inherited content; a
head deletion hides either head or inherited content (`INV-BVIEW-9`,
`INV-BVIEW-10`).

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
Ordinary bound branch columns project to the head selector even when content
or deletion fell back to the base. Hidden typed provenance records the exact
branch key and version that supplied each selected layer (`INV-BVIEW-15`). This
gives application code a coherent draft-shaped row while preserving exact
history for authorization diagnostics, synchronization, and merge calculation.

Normal `RowUuid` references resolve the visible target branch-local row through the
same effective view. An exact branch-local row reference containing a branch key
is a distinct future capability (`INV-BVIEW-13`). Policy reference traversal uses
the same rule.

### 11.4 Mutations and authorization

An exact mutation names `(table, BranchKey, RowUuid)`. A view-relative
mutation names the head view and `RowUuid`; if the visible row is inherited, the
mutation performs copy-on-write into the head branch key (`INV-BVIEW-14`). The helper
may materialize inherited values needed by a merge strategy, but the first head
version has no cross-branch-key causal parent. Source derivation may be retained
only as typed non-causal provenance.

For example, when `R` is inherited from `Main` and an update is requested in
`Draft` over `Main`, the new version is addressed by `(T, DraftKey, R)` with
no `Main` parent. A subsequent `Draft` read projects `Draft` branch-column
cells. If `R` remains inherited, its hidden supplying coordinate records
`MainKey`; once copied, that coordinate records `DraftKey`. Neither case may
alias another branch merely because the `RowUuid` is the same.

Content writes do not implicitly author `Restored`. An ergonomic update helper
may deliberately emit content and restoration together in one transaction, but
the transaction payload must make both effects explicit.

Jazz imposes no built-in branch row, creator, open/closed state, parent, or
existence check. Applications express those rules with ordinary tables and
policies, for example by traversing `todo.branch_id -> branches.id`. A missing
referenced branch or membership row is missing policy evidence and fails closed.
Read policy evaluates over the effective branch view; write policy evaluates
the exact target branch key and the operation's candidate view (`INV-BVIEW-18`).

#### Cross-branch-key atomicity

One transaction may write several branch keys and shared tables. Every
version routes by its own canonical branch key, while the transaction retains one
identity, permission subject, fate, limit budget, durability state, and atomic
admission decision (`INV-BVIEW-16`). Any malformed branch key or denied write rejects
the complete unit.

Version-parent edges remain branch-key-local. Cross-branch-key atomic grouping does
not make one branch-local row causally descend from another.

Trusted history replication may carry complete commit units. Client-facing
selected delivery must not reveal unauthorized sibling versions or even hidden
table/branch key/write-count structure merely because an authorized version shares
their `TxId`. Selected view facts may retain the transaction witness needed for
settlement without reconstructing hidden payload. Such bundles are explicitly
`ViewScoped` and redact their transaction write count to the delivered version
count; only trusted complete replication uses `CompleteTransaction` with the
authored count (`INV-BVIEW-17`, ch. 8).

### 11.5 Maintained subscriptions

The normalized head/base sources are part of canonical `ReadViewKey`,
`SubscriptionKey`, binding-view identity, known state, coverage, settlement,
and cache reuse (`INV-BVIEW-12`). Equal shapes and bindings over different branch keys
must never share results, policy facts, replacement witnesses, receipts, or
unsubscribe cleanup.

A live base and current head are both maintained inputs. A frozen base is an
immutable historical input and only the head is live. The maintained graph
tracks content and deletion winners independently so head add, replace, delete,
and restore events produce the same result as a one-shot read without full
refresh. A base change affects a live-base view only when the corresponding
layer is not masked by a head winner.

Policy dependency tables project the same named selectors as user-query tables.
Tables whose projected head and base branch keys are equal have one maintained source,
preventing duplicate shared evidence.

### 11.6 Cross-branch-key merge helper

Cross-branch-key merge is a high-level local calculation, not an admission or
replication primitive. Given explicit source and target read views, an initiating
identity, and complete authorized history, the helper calculates novel source
contributions and emits one ordinary atomic mergeable transaction whose versions
carry target branch keys (`INV-MERGE-1`). A receiver applies that transaction
without source history.

#### Contribution identity

A contribution dot is field-grained and branch-key-qualified:

```text
BranchCoordinate = sorted [(BranchColumnName, EncodedValue)]

ContributionDot = {
  branch_key,
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

The output version names only the target branch-local row's current row/layer heads
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

Branch-key evolution is monotone. A schema may add an ordinary column to
`branchBy` only when that column has an immutable, typed, deterministic default.
Old history and selectors authored under an older schema normalize the missing
column to that default (`INV-BVIEW-19`).

For example:

```text
v1 branch key: (branch=A)
v2 branch key: (workspace=DEFAULT, branch=A)
```

All old branch-local rows enter the reserved default bucket. New-schema writes
must explicitly provide the new column. Only old-schema queries receive automatic
default completion; omission in a new-schema query is an error. A cross-schema
version parent is valid only when both effective normalized branch keys are equal.

The ordinary column default is also the migration default and must never change
after the column joins `branchBy`. Adding a branch column rekeys/rebuilds current and
secondary-index state and normalizes deletion history and contribution
coordinates consistently. Prefer reserved schema-minted UUIDs or stable enum
identities over user-reachable sentinel values.

The initial contract forbids removing a branch column from `branchBy`, changing
its type or encoding, changing its default, splitting or collapsing branch
columns, and nullable branch columns. An ordinary column rename is allowed
because schema lineage retains its physical column identity; no branch-specific
identity is stored.

### Current limitation: distributed uniqueness

Jazz has no convergent distributed uniqueness mechanism today. Groove can
reject a conflicting write to one local unique index, but offline replicas can
independently accept distinct `RowUuid`s for the same value; arrival order is
not a replicated winner rule. Branch-aware uniqueness therefore remains
unavailable until its replicated claim identity, deterministic arbitration,
authorization, selected delivery, and recovery semantics are specified.

## Open Questions

- 🔶 [#1780](https://github.com/garden-co/jazz/issues/1780) — Branch-view semantics beyond v1.
