# Authority HLC Row Stamps Design

## Status

Approved design for an additive row-history metadata change.

This design keeps existing wall-clock provenance timestamps and adds a separate
global-authority Hybrid Logical Clock stamp per row-batch member. The first
implementation is greenfield: storage and sync formats may change without
supporting old persisted rows or mixed-version peers.

## Context

Jazz currently stores row provenance with `created_at` and `updated_at` `u64`
timestamps. These values are client-provided wall-clock microseconds and are
also exposed through public provenance surfaces such as `$createdAt` and
`$updatedAt`.

Those timestamps currently participate in row-history ordering and visible-row
merge decisions. This design does not change that behavior in the first phase.
Instead, it introduces a distinct authority stamp for future deterministic
global snapshots.

## Goals

- Persist the writer's wall-clock provenance separately from global sequencing.
- Add a nullable, immutable `authority_hlc` to each concrete row-batch member.
- Allow only the single global authority to assign `authority_hlc`.
- Keep local-first writes visible without waiting for a global stamp.
- Preserve current query visibility, LWW ordering, merge behavior, and public
  timestamp APIs.
- Make future row scans for deterministic global snapshots simple by storing the
  stamp directly on each row-batch member.

## Non-Goals

- Do not use `authority_hlc` for current conflict resolution.
- Do not replace `$createdAt` or `$updatedAt` with HLC values.
- Do not support mixed-version sync peers in the first implementation.
- Do not store the stamp only in batch fate or batch-level metadata.
- Do not assign provisional HLCs on clients or edges.

## Core Decisions

### 1. Keep Two Separate Time Concepts

`created_at` and `updated_at` remain wall-clock provenance:

- They are still client-provided epoch microseconds.
- They continue to power `$createdAt` and `$updatedAt`.
- The TypeScript public API continues to expose them as `Date` values.
- Current read and merge behavior continues to use them where it already does.

`authority_hlc` is global sequencing metadata:

- It is nullable.
- It is stamped only by the global authority.
- It is scoped to one concrete `(row_id, branch_name, batch_id)` row-batch
  member.
- It is intended for future deterministic global snapshots.

### 2. Stamp Per Row-Batch Member

Each row-batch member gets its own `authority_hlc`. A multi-row batch therefore
receives one HLC value per touched row.

This is intentionally row-local. Future snapshot scans can read, filter, and
order row history without joining through batch fate or another side table.

### 3. Leave Optimistic Rows Unstamped

Local and edge runtimes do not create provisional HLCs. A local write persists
and syncs with `authority_hlc = None` until the global authority stamps it.

Rows without `authority_hlc` may remain visible in today's local-first query
model. Future deterministic global snapshots must ignore unstamped rows unless
they explicitly choose to include local, non-global data.

### 4. Current Visible Behavior Is Unchanged

The first implementation must not consult `authority_hlc` for:

- visible-row selection
- LWW column merge choice
- branch frontier resolution
- durability-tier preview choice
- `$createdAt` / `$updatedAt`
- current subscription delivery

This keeps the change additive and isolates global snapshot semantics for a
future feature.

## HLC Format

Introduce a dedicated fixed-width `AuthorityHlc` value rather than packing the
stamp into the existing `u64` timestamp type.

Recommended binary layout:

```text
AuthorityHlc:
  physical_micros: u64
  logical_counter: u32
  authority_epoch: u32
```

The encoded form is 16 bytes. If it is used in ordered keys or raw byte
comparisons, encode each component in big-endian order so byte ordering matches
`(physical_micros, logical_counter, authority_epoch)`. Since there is a single
global authority, `authority_epoch` can start at zero. It is reserved to make
future authority key rotation or epoching explicit without changing the row
format again.

The global authority clock advances as follows:

- Read current wall-clock microseconds.
- If wall-clock time is greater than the last stamped physical component, stamp
  `(now_micros, 0, authority_epoch)`.
- Otherwise stamp `(last_physical_micros, last_logical_counter + 1,
authority_epoch)`.
- If the logical counter overflows within one physical microsecond, wait until
  wall-clock time advances or return an explicit authority clock error.

Because only the global authority stamps rows, non-authority runtimes never
merge remote HLCs into their local clocks.

## Data Model

Add nullable `authority_hlc` fields to the row-history runtime shapes:

- `StoredRowBatch`
- `QueryRowBatch`
- visible-row metadata carried through `VisibleRowEntry.current_row`

Add a reserved nullable system column to flat row storage:

- history rows: `_jazz_authority_hlc`
- visible rows: `_jazz_authority_hlc`

The visible-row copy is metadata for the current materialized row only. It does
not affect how the visible row is chosen. For synthetic merged visible rows, it
is copied from the coarse metadata row already chosen for `batch_id`,
`updated_at`, and `updated_by`; it is not per-column provenance.

`authority_hlc` should also participate in row identity freshness where needed:

- Sync replay equality must compare it.
- Row digests should include it if the digest is used to detect exact row-batch
  content equality.
- Storage conformance helpers should preserve it when rows are encoded,
  decoded, replayed, patched, or materialized.

## Stamping Flow

1. A client or edge creates a row-batch member with `authority_hlc = None`.
2. The row syncs upstream through the existing row-batch payload path.
3. The global authority accepts or durably records the row-batch member.
4. The global authority stamps that member with the next `AuthorityHlc`.
5. The stamped row is persisted in history and the visible-row copy is refreshed
   if the row remains current.
6. Normal row sync carries the stamped row back to edges and clients.

The stamp is immutable once assigned. Restamping the same row-batch member with
a different value is protocol corruption.

## Sync Semantics

`StoredRowBatch` already travels through:

- `RowBatchCreated`
- `RowBatchNeeded`

Adding `authority_hlc` to `StoredRowBatch` is enough for basic row sync. Batch
fate remains batch-scoped and does not own HLC values.

Sync replay rules:

- Receiving an unstamped row stores and relays it normally.
- Receiving a stamped row stores and relays the stamp.
- Receiving the same row-batch member with the same stamp is idempotent.
- Receiving the same row-batch member with a different stamp is an error.
- Non-authority runtimes must not assign missing stamps while relaying.

If the global authority receives a row from a non-authority peer with an
already-populated `authority_hlc`, it should accept it only when it matches the
authority's existing record for that row-batch member. If the authority has no
record of assigning that stamp, or the stamp differs, reject the payload rather
than silently overwrite it. This keeps legitimate replay idempotent while making
authority-boundary bugs and malicious clients visible.

## Storage API Shape

The global authority needs a narrow patch path for stamping an existing
row-batch member:

```text
stamp_row_batch_authority_hlc(table, branch, row_id, batch_id, authority_hlc)
```

The operation must:

- load the exact row-batch member
- fail if the row already has a different stamp
- no-op if the row already has the same stamp
- write only the stamp metadata, not app data or wall-clock provenance
- rebuild the visible-row entry when the stamped row is the current visible row
- update any digest or sync freshness bookkeeping that includes the stamp

This can be implemented as a storage-level patch or as an existing row mutation
path with a stamp-only operation, but the public behavior should stay narrow.

## Error Handling

Use explicit failures for protocol violations:

- Non-authority payload includes an unrecognized or conflicting
  `authority_hlc`: reject at the authority.
- Same row-batch member receives conflicting stamps: reject the incoming row and
  emit diagnostics.
- Authority clock logical counter overflows: return an authority clock error and
  retry after physical time advances.
- Malformed encoded HLC: treat as row-format corruption and fail closed.

Missing `authority_hlc` is not an error. It is the normal state for local,
edge-only, unstamped, and not-yet-globally-observed writes.

## Testing

Prefer black-box integration coverage where possible:

- Local write persists with no `authority_hlc`.
- Edge or client syncs an unstamped write to the global authority.
- Global authority stamps each row-batch member separately.
- Stamped rows sync back to clients and survive storage reload.
- Multi-row batches receive per-member stamps.
- Current query ordering and `$updatedAt` behavior remain unchanged after rows
  are stamped.
- A conflicting stamp for the same row-batch member is rejected.
- An unrecognized non-authority-supplied stamp is rejected by the global
  authority.

Focused codec/storage conformance tests should cover the fixed-width HLC
encoding, nullable storage, visible-row copy, and digest/replay equality.

## Future Work

Future deterministic global snapshots can use `authority_hlc` as the global
scan boundary:

- include only rows with `authority_hlc <= snapshot_hlc`
- ignore unstamped rows by default
- order row-history entries by `authority_hlc` for deterministic replay

That future work can decide whether to add snapshot query APIs, historical
indices, or global-tier-only read modes. This design only persists the metadata
needed to make that work straightforward.
