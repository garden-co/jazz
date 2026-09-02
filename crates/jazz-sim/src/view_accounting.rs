//! Shared simulation accounting for sync-message row delivery payloads.

use jazz::protocol::{
    ResultMemberEntry, SyncMessage, VersionBundleRef, VersionCarrier, VersionRecord,
    ViewUpdatePayload,
};
use jazz::tx::Transaction;

/// Estimate row-delivery bytes carried by a sync message.
pub fn view_update_bytes(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate(ViewUpdatePayload {
            version_carriers,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            ..
        }) => {
            version_carriers_bytes(version_carriers)
                + (peer_payload_inventory.complete_tx_payloads.len() as u64 * tx_id_wire_bytes())
                + result_rows_bytes(result_member_adds)
                + result_rows_bytes(result_member_removes)
        }
        SyncMessage::CommitUnit { tx, versions } => {
            transaction_wire_bytes(tx) + versions.iter().map(version_record_bytes).sum::<u64>()
        }
        SyncMessage::FateUpdate { .. } => tx_id_wire_bytes() + 16,
        // An authority scope view carries an ordinary settlement-bearing view
        // update. Its row payload is part of the simulated delivery cost.
        SyncMessage::AuthorizationScopeView { view, .. } => scope_view_update_bytes(view),
        SyncMessage::RegisterShape { .. }
        | SyncMessage::ChunkRequestBatch(_)
        | SyncMessage::ChunkResponseBatch(_)
        | SyncMessage::ChunkUploadStart(_)
        | SyncMessage::ChunkUploadNodes(_)
        | SyncMessage::ChunkUploadResult(_)
        | SyncMessage::Subscribe(_)
        | SyncMessage::PublishSchema { .. }
        | SyncMessage::PublishSchemaWithLens { .. }
        | SyncMessage::PublishLens { .. }
        | SyncMessage::SetCurrentWriteSchema { .. }
        | SyncMessage::CatalogueAck(_)
        | SyncMessage::SessionClaims { .. }
        | SyncMessage::SubscribeRejected { .. }
        | SyncMessage::Unsubscribe { .. }
        | SyncMessage::FetchRowVersions { .. }
        | SyncMessage::RowVersionPayloads { .. }
        | SyncMessage::CatalogueSnapshot(_)
        | SyncMessage::PermissionAdviceRequest { .. }
        | SyncMessage::PermissionAdviceResponse { .. }
        | SyncMessage::AuthorizationScopeSubscribe { .. }
        | SyncMessage::AuthorizationScopeReceipt { .. }
        | SyncMessage::AuthorizationScopeIntent { .. }
        | SyncMessage::AuthorizationScopeAggregateReceipt { .. }
        | SyncMessage::AuthorizationScopeUnavailable { .. }
        | SyncMessage::AuthorizationScopeDecision { .. } => 0,
    }
}

/// Estimate the irreducible row-version payload bytes carried by a sync message.
pub fn bytes_floor(update: &SyncMessage) -> u64 {
    match update {
        SyncMessage::ViewUpdate(ViewUpdatePayload {
            version_carriers, ..
        }) => version_carriers_bytes_floor(version_carriers),
        SyncMessage::AuthorizationScopeView { view, .. } => scope_view_bytes_floor(view),
        _ => 0,
    }
}

fn scope_view_update_bytes(view: &ViewUpdatePayload) -> u64 {
    version_carriers_bytes(&view.version_carriers)
        + (view.peer_payload_inventory.complete_tx_payloads.len() as u64 * tx_id_wire_bytes())
        + result_rows_bytes(&view.result_member_adds)
        + result_rows_bytes(&view.result_member_removes)
}

fn scope_view_bytes_floor(view: &ViewUpdatePayload) -> u64 {
    version_carriers_bytes_floor(&view.version_carriers)
}

/// Borrow the logical singleton bundles represented by a carrier stream.
pub fn version_bundle_refs(
    carriers: &[VersionCarrier],
) -> impl Iterator<Item = VersionBundleRef<'_>> {
    carriers.iter().flat_map(|carrier| {
        carrier
            .bundle_refs()
            .expect("simulation accounting requires valid version carriers")
    })
}

fn version_carriers_bytes(carriers: &[VersionCarrier]) -> u64 {
    version_bundle_refs(carriers)
        .map(version_bundle_bytes)
        .sum()
}

fn version_carriers_bytes_floor(carriers: &[VersionCarrier]) -> u64 {
    version_bundle_refs(carriers)
        .flat_map(|bundle| bundle.versions)
        .map(version_record_bytes)
        .sum()
}

fn version_bundle_bytes(bundle: VersionBundleRef<'_>) -> u64 {
    transaction_wire_bytes(bundle.tx)
        + bundle
            .versions
            .iter()
            .map(version_record_bytes)
            .sum::<u64>()
        + 16
}

fn version_record_bytes(version: &VersionRecord) -> u64 {
    version.table().len() as u64 + version.record().raw().len() as u64
}

fn transaction_wire_bytes(tx: &Transaction) -> u64 {
    tx_id_wire_bytes()
        + 4
        + 16
        + tx.user_metadata_json
            .as_ref()
            .map_or(0, |metadata| metadata.len() as u64)
}

fn result_rows_bytes(rows: &[ResultMemberEntry]) -> u64 {
    rows.iter()
        .filter_map(|entry| entry.as_row())
        .map(|(table, _, _)| table.len() as u64 + 16 + tx_id_wire_bytes())
        .sum()
}

fn tx_id_wire_bytes() -> u64 {
    8 + 16
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use jazz::groove::records::Value;
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::protocol::{
        AuthorizationSupportScopeKey, PeerPayloadInventory, PermissionAdviceRequestId, ReadViewKey,
        SubscriptionKey, VersionBundle,
    };
    use jazz::query::{BindingId, ShapeId};
    use jazz::time::{GlobalTime, TxTime};
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    use jazz::tx::{DurabilityTier, Fate, Transaction, TxId, TxKind};

    use super::*;

    // This is necessarily internal: accounting helpers measure simulation
    // instrumentation, not public database behavior.
    #[test]
    fn authorization_scope_view_accounts_for_nested_payload_and_floor() {
        let tx_id = TxId::new(TxTime::new(1, 0), NodeUuid(uuid::Uuid::nil()));
        let schema = crate::public_schema_fixture::compile_public_schema(
            SchemaBuilder::new()
                .table(TableSchemaBuilder::new("items").column("name", ColumnType::Text))
                .build(),
        );
        let version = VersionRecord::from_cells(
            &schema.tables()[0],
            schema.version_id(),
            RowUuid(uuid::Uuid::nil()),
            Vec::new(),
            AuthorSubject::for_test_bytes([0; 16]),
            tx_id.time.physical_ms(),
            AuthorSubject::for_test_bytes([0; 16]),
            tx_id.time.physical_ms(),
            &BTreeMap::<String, Value>::from([("name".to_owned(), Value::String("value".into()))]),
            None,
        )
        .expect("valid test wire record");
        let nested = SyncMessage::ViewUpdate(ViewUpdatePayload {
            subscription: SubscriptionKey {
                shape_id: ShapeId(uuid::Uuid::nil()),
                binding_id: BindingId(uuid::Uuid::nil()),
                read_view: ReadViewKey::default(),
            },
            settled_through: GlobalTime::default(),
            reset_result_set: false,
            version_carriers: vec![jazz::protocol::VersionCarrier::Bundle(VersionBundle {
                tx: Transaction {
                    tx_id,
                    kind: TxKind::Mergeable,
                    n_total_writes: 1,
                    made_by: AuthorSubject::for_test_bytes([0; 16]),
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: None,
                    contribution_merge: None,
                },
                versions: vec![version],
                scope: jazz::protocol::VersionBundleScope::ViewScoped,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(1)),
                durability: DurabilityTier::Global,
            })],
            peer_payload_inventory: PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        });
        let nested_bytes = view_update_bytes(&nested);
        let nested_floor = bytes_floor(&nested);
        assert!(nested_bytes > 0);
        assert!(nested_floor > 0);

        let wrapped = SyncMessage::AuthorizationScopeView {
            request_id: PermissionAdviceRequestId([0; 16]),
            key: AuthorizationSupportScopeKey {
                support_shape_digest: [0; 32],
                subject: AuthorSubject::for_test_bytes([0; 16]),
                claims_digest: [0; 32],
                policy_digest: [0; 32],
            },
            clause_index: 0,
            clause_count: 1,
            view: jazz::protocol::ViewUpdatePayload::from_view_update(nested)
                .expect("fixture is a view update"),
        };

        assert_eq!(view_update_bytes(&wrapped), nested_bytes);
        assert_eq!(bytes_floor(&wrapped), nested_floor);
    }
}
