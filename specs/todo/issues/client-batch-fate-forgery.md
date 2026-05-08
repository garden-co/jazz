# Client BatchFate forgery

## What

`process_from_client` accepts `SyncPayload::BatchFate` from any role and persists it into authoritative storage, letting any authenticated user forge whole-batch durability/rejection outcomes for arbitrary `BatchId`s and have the lie fanned out to every other subscribed client.

## Priority

high

## Notes

### Where

- `crates/jazz-tools/src/sync_manager/inbox.rs` — `process_from_client` `BatchFate` arm (currently calls `retain_client_batch_fate` unconditionally).
- `crates/jazz-tools/src/sync_manager/inbox.rs` — `retain_client_batch_fate` (`Ok(None) if matches!(fate, BatchFate::DurableDirect { .. }) => LocalBatchRecord::new(...)`) synthesizes a phantom local batch record for unknown ids.
- `crates/jazz-tools/src/storage/storage_trait.rs` — `upsert_local_batch_record` writes `latest_fate` into authoritative storage via `upsert_authoritative_batch_fate`.
- `crates/jazz-tools/src/sync_manager/types.rs` — `ClientRole` defines `User`/`Backend`/`Admin`/`Peer`; only the first three may be untrusted.

### Attack

A `User`-role client with a session sends:

```rust
SyncPayload::BatchFate {
    fate: BatchFate::DurableDirect {
        batch_id: <freshly minted UUIDv7>,
        confirmed_tier: <highest>,
    },
}
```

The server:

1. Calls `retain_client_batch_fate`, which manufactures a fresh `LocalBatchRecord` because the batch is unknown.
2. Calls `record.apply_fate(fate.clone())` and `storage.upsert_local_batch_record(&record)` — the latter persists the forged fate into the authoritative-fate table.
3. Pushes the forged fate onto `pending_batch_fates`, which the broadcast logic later relays to every other subscribed client.

Variants: forge `DurableDirect` for a `batch_id` the server already `Rejected` (stomping the legitimate decision); pre-empt a transactional batch in flight with a forged `AcceptedTransaction`; launder writes by claiming durability for batches the user does not own.

### Provenance

The settlements→fate refactor merged to `main` via PR #820 (`c739040a`, "Merge pull request #820 from garden-co/feat/batch-rollback"). That refactor added the `retain_client_batch_fate(storage, fate)` call inside the `process_from_client` arm; before it, the equivalent arm just pushed onto `pending_batch_settlements` and did not touch storage. The fan-out leak therefore pre-dates the refactor; the authoritative-storage forge and phantom-record synthesis are post-refactor regressions and are now live on `main`.

### Failing test (drop into `crates/jazz-tools/src/sync_manager/tests/permissions.rs`)

```rust
#[test]
fn batch_fate_from_user_client_must_not_forge_authoritative_fate() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let forged_batch_id = BatchId::new();

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::User);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("mallory"),
    );
    sm.take_outbox();

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::BatchFate {
            fate: BatchFate::DurableDirect {
                batch_id: forged_batch_id,
                confirmed_tier: DurabilityTier::Local,
            },
        },
    });
    sm.process_inbox(&mut io);

    assert!(io.load_authoritative_batch_fate(forged_batch_id).unwrap().is_none());
    assert!(io.load_local_batch_record(forged_batch_id).unwrap().is_none());
    assert!(sm.take_pending_batch_fates().is_empty());
    assert!(!sm.take_outbox().iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            payload: SyncPayload::BatchFate {
                fate: BatchFate::DurableDirect { batch_id, .. },
            },
            ..
        } if *batch_id == forged_batch_id
    )));
}
```

Lives on branch `fix/client-batch-fate-forgery` (off `main`). Fails at the first assertion today; the others would surface in turn once the storage write is gated.

### Same hole, sibling arms

While role-gating `BatchFate`, audit the other unguarded `process_from_client` arms — they were skipped by the same precedent that gated `CatalogueEntryUpdated`/`RowBatchCreated` and explicitly logged-and-ignored `SchemaWarning`/`ConnectionSchemaDiagnostics`:

- `SyncPayload::SealBatch { .. }` — calls `apply_payload_from_client` with no role check.
- `SyncPayload::BatchFateNeeded { batch_ids }` — server responds with current authoritative fate; less dangerous (read), but worth confirming.
- `SyncPayload::QuerySettled { .. }` — relayed from "downstream" with no role check.

### Direction

- Role-gate the `BatchFate` arm in `process_from_client`. My read of the protocol is that fate is server-authority output and should only flow inbound via `process_from_server`; check `Peer`-role routing before locking that in (peer-as-client may exist in some topologies).
- While in the file, sweep `SealBatch`, `BatchFateNeeded`, `QuerySettled` for the same gap.
- Stretch: also revisit whether the storage write should ever happen from the client edge regardless of role — the legitimate "client tells us a fate it learned downstream" flow is already covered by `process_from_server` at line 1190.
