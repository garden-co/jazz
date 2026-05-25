# Proposal: Authority HLC Row Stamps

Add one nullable global batch stamp, copied into each row-batch member, while
keeping the existing client timestamp exactly what it is today: human-facing
provenance.

The point is not to change conflict resolution now. The point is to make future
deterministic global snapshots possible.

## The Problem

Today, row batches carry wall-clock timestamps from the writer:

```text
row batch
  updated_at = client wall clock
```

That timestamp is useful for people:

```text
$updatedAt
audit display
app-level timestamp comparisons
```

But a client wall clock is not a good global sequencing source. Different
devices can be ahead, behind, offline, or manually changed.

For deterministic global snapshots, we need a timeline produced by one global
authority.

## The Proposal

Keep the current timestamp, and add a second field:

```text
row batch
  updated_at     = client wall clock, for people and current behavior
  authority_hlc  = global authority stamp, for future global snapshots
```

The two fields answer different questions:

```text
updated_at:
  "When did the writer say this happened?"

authority_hlc:
  "Where did the global authority place this batch
   in the durable global sequence?"
```

## What A Row Looks Like

Before:

```text
StoredRowBatch
  row_id
  branch
  batch_id
  parents
  updated_at
  created_at
  data
```

After:

```text
StoredRowBatch
  row_id
  branch
  batch_id
  parents
  updated_at        unchanged
  created_at        unchanged
  authority_hlc     new, nullable copy of the batch stamp
  data
```

`authority_hlc = null` is normal. It means the row exists locally or at the
edge, but the global authority has not stamped its batch yet.

## Write Flow

```text
Client or edge writes

  updated_at    = client time
  authority_hlc = null

          |
          | sync upstream
          v

Global authority records the batch

  updated_at    = unchanged
  authority_hlc = next global HLC for the batch

          |
          | sync downstream
          v

Clients and edges store the stamp

  current reads behave the same as before
```

Only the global authority can create the stamp. Clients and edges can store and
relay a stamp they received, but they do not invent one.

## Multi-Row Batches

The stamp is per batch, but stored inside every row member:

```text
batch A -> authority_hlc = HLC 100

  row 1 stores authority_hlc = HLC 100
  row 2 stores authority_hlc = HLC 100
  row 3 stores authority_hlc = HLC 100
```

This gives the batch one place in the global sequence while still keeping
future row scans simple. A snapshot scan can inspect row history directly
instead of joining through batch metadata.

## What Does Not Change

This proposal does not change current visible behavior:

```text
current LWW merge ordering       unchanged
current visible-row selection    unchanged
$createdAt / $updatedAt          unchanged
local-first optimistic reads     unchanged
batch fate                       unchanged
```

That is the main safety property of the proposal. The new field prepares a
future capability without changing what applications see today.

## Future Snapshot Shape

Later, a deterministic global snapshot can use the authority timeline:

```text
snapshot at HLC 500

include row-batch members where:
  authority_hlc is not null
  authority_hlc <= HLC 500

ignore by default:
  local-only rows
  edge-only rows
  rows not yet observed by the global authority
```

The snapshot then has one global ordering source, independent from client wall
clocks.

## Why This Is The Smallest Useful Step

```text
client timestamp remains human time
authority_hlc becomes global sequence time
current reads keep their behavior
future global snapshots get a direct scan key
```

The proposal separates concepts that are currently easy to conflate, without
forcing the engine to switch conflict resolution or public timestamp APIs at the
same time.
