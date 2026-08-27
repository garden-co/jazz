use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

mod schema_fixture;
mod support;

use support::BenchFutureExt as _;

use jazz::groove::records::Value;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::{
    KnownStateDeclaration, RowVersionRef, SubscriptionKey, SyncMessage, expand_version_carriers,
};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{Fate, TxId};
use jazz::wire::encode_sync_message;
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use serde_json::json;
use sha2::{Digest, Sha256};
use support::{csv_usizes, emit_json_line, env_usize, phase_fields};

const TABLE: &str = "documents";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let rows = env_usize("JAZZ_KNOWN_STATE_ROWS", 1_000).max(1);
    let coverage_percentages = csv_usizes("JAZZ_KNOWN_STATE_COVERAGE", "0,25,50,75,100");
    assert!(
        coverage_percentages.iter().all(|coverage| *coverage <= 100),
        "known-state coverage percentages must be between 0 and 100"
    );

    let seed_started = Instant::now();
    let mut fixture = Fixture::new();
    let versions = fixture.seed(rows);
    let seed_us = seed_started.elapsed().as_micros();
    let subscription = fixture.subscription_key();
    let mut expected_digest = None;

    for coverage_percent in coverage_percentages {
        let known_count = rows * coverage_percent / 100;
        let known_versions = versions[..known_count]
            .iter()
            .map(|(row_uuid, tx_id)| RowVersionRef::new(TABLE, *row_uuid, *tx_id))
            .collect::<Vec<_>>();
        let declaration = (known_count > 0).then_some(KnownStateDeclaration::ExactVersionSet {
            versions: known_versions,
        });
        let declaration_bytes = postcard::to_allocvec(&declaration)
            .expect("encode known-state declaration payload")
            .len();

        let mut peer = PeerState::new();
        peer.declare_known_state(subscription, declaration);
        fixture.core.reset_storage_read_metrics();
        let serve_started = Instant::now();
        let update = peer
            .current_rows_update(&mut fixture.core, TABLE)
            .expect("serve current rows under known-state declaration");
        let serve_us = serve_started.elapsed().as_micros();
        let encoded_bytes = encode_sync_message(&update)
            .expect("encode known-state view update")
            .len();
        let metrics = fixture.core.storage_read_metrics();

        let SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
            version_carriers,
            version_bundles,
            peer_payload_inventory,
            result_member_adds,
            result_member_removes,
            program_fact_adds,
            program_fact_removes,
            ..
        }) = &update
        else {
            panic!("expected one view update");
        };
        let mut expanded_bundles = version_bundles.clone();
        expanded_bundles.extend(
            expand_version_carriers(version_carriers).expect("expand benchmark version carriers"),
        );
        let emitted_bundles = expanded_bundles.len();
        let expected_bundles = rows - known_count;
        assert_eq!(emitted_bundles, expected_bundles);
        assert_eq!(result_member_adds.len(), rows);
        assert!(result_member_removes.is_empty());
        assert!(program_fact_adds.is_empty());
        assert!(program_fact_removes.is_empty());
        let expected_versions = versions
            .iter()
            .map(|(row_uuid, tx_id)| RowVersionRef::new(TABLE, *row_uuid, *tx_id))
            .collect::<BTreeSet<_>>();
        let covered_versions = versions[..known_count]
            .iter()
            .map(|(row_uuid, tx_id)| RowVersionRef::new(TABLE, *row_uuid, *tx_id))
            .chain(expanded_bundles.iter().flat_map(|bundle| {
                bundle.versions.iter().map(|version| {
                    RowVersionRef::new(version.table(), version.row_uuid(), bundle.tx.tx_id)
                })
            }))
            .collect::<BTreeSet<_>>();
        assert_eq!(covered_versions, expected_versions);

        let digest = membership_digest(result_member_adds);
        match &expected_digest {
            Some(expected) => assert_eq!(&digest, expected, "known-state changed membership"),
            None => expected_digest = Some(digest.clone()),
        }

        let mut fields = phase_fields("known_state_rehydrate", serve_us);
        fields.insert("rows".to_owned(), json!(rows));
        fields.insert("known_rows".to_owned(), json!(known_count));
        fields.insert("known_percent".to_owned(), json!(coverage_percent));
        fields.insert("seed_us".to_owned(), json!(seed_us));
        fields.insert("encoded_bytes".to_owned(), json!(encoded_bytes));
        fields.insert("declaration_bytes".to_owned(), json!(declaration_bytes));
        fields.insert(
            "variable_exchange_bytes".to_owned(),
            json!(encoded_bytes + declaration_bytes),
        );
        fields.insert("version_bundles".to_owned(), json!(emitted_bundles));
        fields.insert(
            "complete_tx_payload_refs".to_owned(),
            json!(peer_payload_inventory.complete_tx_payloads.len()),
        );
        fields.insert("result_adds".to_owned(), json!(result_member_adds.len()));
        fields.insert(
            "result_removes".to_owned(),
            json!(result_member_removes.len()),
        );
        fields.insert("membership_digest".to_owned(), json!(digest));
        fields.insert("storage_reads".to_owned(), json!(metrics.total.reads));
        fields.insert("storage_ranges".to_owned(), json!(metrics.total.ranges));
        emit_json_line("known_state_scaling", fields);
    }
}

struct Fixture {
    writer: NodeState<RocksDbStorage>,
    core: NodeState<RocksDbStorage>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Fixture {
    fn new() -> Self {
        let schema = schema();
        let (writer_dir, writer) = open_node(node(1), schema.clone());
        let (core_dir, core) = open_node(node(2), schema);
        Self {
            writer,
            core,
            _dirs: vec![writer_dir, core_dir],
        }
    }

    fn seed(&mut self, rows: usize) -> Vec<(RowUuid, TxId)> {
        (0..rows)
            .map(|index| {
                let row_uuid = row(index);
                let (publication, unit) = self
                    .writer
                    .commit_mergeable_unit(
                        MergeableCommit::new(TABLE, row_uuid, 1_000 + index as u64)
                            .cells(cells(index)),
                    )
                    .expect("create fixture commit");
                let tx_id = support::settle_transaction(&mut self.writer, publication);
                let fate = core_ingest(&mut self.core, &unit, u64::MAX - SKEW_TOLERANCE_MS);
                assert!(matches!(
                    fate,
                    SyncMessage::FateUpdate {
                        fate: Fate::Accepted,
                        ..
                    }
                ));
                (row_uuid, tx_id)
            })
            .collect()
    }

    fn subscription_key(&mut self) -> SubscriptionKey {
        let mut peer = PeerState::new();
        let update = peer
            .current_rows_update(&mut self.core, TABLE)
            .expect("discover whole-table subscription key");
        match update {
            SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload { subscription, .. }) => {
                subscription
            }
            _ => panic!("expected one view update"),
        }
    }
}

fn membership_digest<T: serde::Serialize>(members: &T) -> String {
    let bytes = postcard::to_allocvec(members).expect("encode membership digest input");
    hex::encode(Sha256::digest(bytes))
}

fn core_ingest(
    core: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
    now_ms: u64,
) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let outcome = core
        .ingest_commit_unit(tx.clone(), versions.clone(), now_ms)
        .expect("core ingest");
    let [fate] = support::settle_outcome(core, outcome)
        .try_into()
        .expect("one fate update");
    fate
}

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new(TABLE).column("title", ColumnType::Text)),
    )
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .expect("open RocksDB");
    let node = NodeState::new(node_uuid, schema, storage).expect("open node");
    (temp_dir, node)
}

fn cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "title".to_owned(),
        Value::String(format!("document-{index}")),
    )])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(index: usize) -> RowUuid {
    let mut bytes = [7; 16];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    RowUuid::from_bytes(bytes)
}
