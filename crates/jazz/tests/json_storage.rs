use std::collections::BTreeMap;

use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState};
use jazz::protocol::{
    PeerPayloadInventory, RegisterShapeOptions, ShapeAst, Subscribe, SubscriptionKey, SyncMessage,
    VersionBundle, VersionRecord,
};
use jazz::query::Query;
use jazz::schema::{ColumnSchema, ColumnType, JazzSchema, Policy, TableSchema};
use jazz::time::{GlobalSeq, TxTime};
use jazz::tx::{DurabilityTier, Fate, Transaction, TxId, TxKind};
use serde_json::{Value as JsonValue, json};

fn documents_schema(payload_schema: Option<JsonValue>) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "documents",
        [ColumnSchema::json("payload", payload_schema)],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn schema_requiring_string_name() -> JazzSchema {
    documents_schema(Some(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })))
}

fn open_db(schema: JazzSchema) -> Db<MemoryStorage> {
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    jazz::db::block_on(Db::open(DbConfig {
        schema,
        storage: MemoryStorage::new(&column_family_refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: None,
        large_value_checkpoint_op_interval: 1024,
    }))
    .expect("open db")
}

fn open_node(schema: JazzSchema) -> NodeState<MemoryStorage> {
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    NodeState::new(
        NodeUuid::from_bytes([0x22; 16]),
        schema,
        MemoryStorage::new(&column_family_refs),
    )
    .expect("open node")
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn payload(raw: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("payload".to_owned(), Value::String(raw.to_owned()))])
}

/// Emits a wire commit unit carrying `raw` in `documents.payload`, as an
/// untrusted peer would, and hands it to the receiving node's ingest boundary.
fn remote_payload_unit(
    schema: &JazzSchema,
    document_id: RowUuid,
    raw: &str,
) -> (Transaction, VersionRecord) {
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "documents")
        .expect("documents table");
    let peer = NodeUuid::from_bytes([0x33; 16]);
    let author = AuthorId::from_bytes([0xb2; 16]);
    let tx_time = TxTime(500);
    let tx_id = TxId::new(tx_time, peer);

    let version = VersionRecord::from_cells(
        table,
        schema.version_id(),
        document_id,
        Vec::new(),
        author,
        tx_time,
        author,
        tx_time,
        &BTreeMap::from([("payload".to_owned(), Value::String(raw.to_owned()))]),
        None,
    )
    .expect("peer encodes a wire record without validating it");

    let tx = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 1,
        made_by: author,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        source_branch: None,
        merge_strategy: None,
    };

    (tx, version)
}

/// Emits a wire commit unit carrying `raw` in `documents.payload`, as an
/// untrusted peer would, and hands it to the receiving node's ingest boundary.
fn ingest_remote_payload(
    node: &mut NodeState<MemoryStorage>,
    schema: &JazzSchema,
    document_id: RowUuid,
    raw: &str,
) -> Vec<SyncMessage> {
    let (tx, version) = remote_payload_unit(schema, document_id, raw);
    node.ingest_commit_unit(tx, vec![version], 500)
        .expect("ingest reports rejection through fate, not a transport error")
}

/// Asserts the peer's row never became visible and that the node reported the
/// transaction as rejected rather than silently dropping it.
fn assert_row_absent(
    node: &mut NodeState<MemoryStorage>,
    document_id: RowUuid,
    messages: &[SyncMessage],
) {
    assert_eq!(
        node.visible_current_cells("documents", document_id)
            .expect("query local row"),
        None,
        "invalid JSON from a peer must not reach storage"
    );

    let rejected = messages.iter().any(|message| {
        matches!(
            message,
            SyncMessage::FateUpdate {
                fate: Fate::Rejected(_),
                ..
            }
        )
    });
    assert!(
        rejected,
        "peer must be told the commit unit was rejected: {messages:?}"
    );
}

fn query_documents(db: &Db<MemoryStorage>, schema: &JazzSchema) -> Vec<(RowUuid, Vec<Value>)> {
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "documents")
        .expect("documents table");
    let prepared = db
        .prepare_query(&db.table("documents"))
        .expect("prepare documents query");

    db.read(&prepared)
        .expect("query documents")
        .into_iter()
        .map(|row| {
            let row_id = row.row_uuid();
            let payload = row.cell(table, "payload").expect("stored document payload");
            (row_id, vec![payload])
        })
        .collect()
}

/// Malformed JSON Schemas fail when a schema enters the node rather than when
/// the first row using that column is written.
#[test]
fn node_open_rejects_malformed_json_schema() {
    let schema = JazzSchema {
        tables: vec![TableSchema::new(
            "documents",
            [ColumnSchema::new(
                "payload",
                ColumnType::Json {
                    schema: Some("{\"type\":7}".to_owned()),
                },
            )],
        )],
        branch_read_policy: None,
        branch_write_policy: None,
    };
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let error = match NodeState::new(
        NodeUuid::from_bytes([0x44; 16]),
        schema,
        MemoryStorage::new(&column_family_refs),
    ) {
        Ok(_) => panic!("malformed JSON Schema must fail node admission"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("invalid JSON schema for column `documents.payload`"),
        "unexpected error: {error:?}"
    );
}

/// Verifies that a JSON column stores the exact text the user inserted rather
/// than normalizing or reserializing it.
///
/// Actor: alice inserts a formatted JSON string into `documents.payload` and
/// reads the same text back through the public database API.
#[test]
fn insert_json_preserves_original_text() {
    let schema = documents_schema(None);
    let db = open_db(schema.clone());

    let raw = "{\n  \"name\": \"Ada\",\n  \"active\": true\n}";
    let document_id = row(1);
    db.insert_with_id("documents", document_id, payload(raw))
        .expect("insert valid json");

    let rows = query_documents(&db, &schema);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, document_id);
    assert_eq!(rows[0].1, vec![Value::String(raw.to_owned())]);
}

/// Verifies that writes to a JSON column reject syntactically invalid JSON
/// before the row is accepted.
///
/// Actor: alice attempts to insert malformed JSON into `documents.payload`.
#[test]
fn insert_rejects_invalid_json_text() {
    let db = open_db(documents_schema(None));

    let error = match db.insert_with_id("documents", row(1), payload("{\"name\":true")) {
        Ok(_) => panic!("invalid JSON must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(
        error
            .to_string()
            .contains("invalid JSON for column `payload`"),
        "unexpected error: {error:?}"
    );
}

/// Verifies that the public node-core write API applies logical JSON validation
/// before it persists a local mergeable commit.
///
/// Actor: a Rust caller bypasses the `Db` facade and submits malformed JSON
/// directly to `NodeState`.
#[test]
fn node_commit_mergeable_rejects_invalid_json_before_local_persistence() {
    let mut node = open_node(documents_schema(None));
    let document_id = row(2);

    let error = node
        .commit_mergeable(MergeableCommit::new("documents", document_id, 10).cells(payload("{")))
        .expect_err("invalid JSON must be rejected by the node core");

    assert!(
        error
            .to_string()
            .contains("invalid JSON for column `payload`"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        node.visible_current_cells("documents", document_id)
            .expect("query local row"),
        None
    );
}

/// Verifies that a JSON column with an attached JSON Schema rejects inserts
/// whose payload parses as JSON but does not satisfy the schema.
///
/// Actor: alice inserts a document whose `name` field is not a string.
#[test]
fn insert_rejects_json_schema_violation() {
    let db = open_db(schema_requiring_string_name());

    let error = match db.insert_with_id("documents", row(1), payload("{\"name\":123}")) {
        Ok(_) => panic!("schema-invalid JSON must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(
        error
            .to_string()
            .contains("JSON schema validation failed for column `payload`"),
        "unexpected error: {error:?}"
    );
}

/// Verifies that a JSON Schema violation during update is rejected and the
/// previous valid JSON payload remains visible.
///
/// Actor: alice inserts a valid document, attempts an invalid update, then
/// queries the row and sees the original payload.
#[test]
fn update_rejects_json_schema_violation_and_preserves_existing_payload() {
    let schema = schema_requiring_string_name();
    let db = open_db(schema.clone());
    let document_id = row(1);

    db.insert_with_id("documents", document_id, payload("{\"name\":\"ok\"}"))
        .expect("insert valid row first");

    let error = match db.update("documents", document_id, payload("{\"name\":42}")) {
        Ok(_) => panic!("invalid update payload must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(
        error
            .to_string()
            .contains("JSON schema validation failed for column `payload`"),
        "unexpected error: {error:?}"
    );

    let rows = query_documents(&db, &schema);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, document_id);
    assert_eq!(
        rows[0].1,
        vec![Value::String("{\"name\":\"ok\"}".to_owned())]
    );
}

/// Verifies that JSON values arriving from a remote peer are validated before
/// they are persisted, not merely on the local write path.
///
/// Actor: a peer node emits a wire commit unit whose `documents.payload` cell
/// is syntactically malformed, and the receiving node must not persist it.
///
/// Constructing the `VersionRecord` directly is deliberate rather than a
/// shortcut: the local write API rejects this value, so the only way an
/// invalid cell reaches storage is from a peer that did not validate it. That
/// untrusted-peer wire record *is* the subject under test.
#[test]
fn remote_ingest_rejects_syntactically_invalid_json() {
    let schema = documents_schema(None);
    let mut node = open_node(schema.clone());
    let document_id = row(3);

    let messages = ingest_remote_payload(&mut node, &schema, document_id, "{");

    assert_row_absent(&mut node, document_id, &messages);
}

/// Verifies that a remote peer cannot persist JSON that parses but violates the
/// column's attached JSON Schema.
///
/// Actor: a peer node emits a wire commit unit whose `documents.payload` holds
/// `{"name":123}` for a column requiring `name` to be a string.
///
/// See `remote_ingest_rejects_syntactically_invalid_json` for why the wire
/// record is constructed directly.
#[test]
fn remote_ingest_rejects_json_schema_violation() {
    let schema = schema_requiring_string_name();
    let mut node = open_node(schema.clone());
    let document_id = row(4);

    let messages = ingest_remote_payload(&mut node, &schema, document_id, "{\"name\":123}");

    assert_row_absent(&mut node, document_id, &messages);
}

/// Relay admission is a storage boundary rather than a fate authority, so an
/// invalid wire value is observable as an ingest error. Constructing the wire
/// record directly is necessary because every local write API rejects it first.
#[test]
fn relay_ingest_rejects_invalid_json_before_storage() {
    let schema = schema_requiring_string_name();
    let mut node = open_node(schema.clone());
    let document_id = row(5);
    let (tx, version) = remote_payload_unit(&schema, document_id, "{\"name\":123}");

    let error = node
        .ingest_relay_commit_unit(tx, vec![version])
        .expect_err("relay must reject schema-invalid JSON");

    assert!(
        error
            .to_string()
            .contains("JSON schema validation failed for column `payload`"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        node.visible_current_cells("documents", document_id)
            .expect("query local row"),
        None,
        "invalid relayed JSON must not reach storage"
    );
}

/// View updates require direct wire construction because a conforming serving
/// node cannot produce an invalid JSON cell. This exercises both live message
/// variants through `apply_sync_message`, including reset/bulk ingestion.
#[test]
fn view_update_wire_variants_reject_invalid_json_before_storage() {
    let schema = schema_requiring_string_name();

    for chunked in [false, true] {
        let mut node = open_node(schema.clone());
        let document_id = if chunked { row(7) } else { row(6) };
        let query = Query::from("documents");
        let shape = query.validate(&schema).expect("validate documents query");
        let binding = shape.bind(BTreeMap::new()).expect("bind documents query");
        let opts = RegisterShapeOptions::default();
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        node.apply_sync_message(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
        })
        .expect("register documents shape");
        node.apply_sync_message(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .expect("subscribe to documents");

        let (tx, version) = remote_payload_unit(&schema, document_id, "{\"name\":123}");
        let tx_id = tx.tx_id;
        let bundle = VersionBundle {
            tx,
            versions: vec![version],
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(1)),
            durability: DurabilityTier::Global,
        };
        let result_member_adds = vec![("documents".to_owned().into(), document_id, tx_id).into()];
        let message = if chunked {
            SyncMessage::ViewUpdateChunk {
                subscription,
                settled_through: GlobalSeq(1),
                reset_result_set: true,
                final_chunk: true,
                version_carriers: Vec::new(),
                version_bundles: vec![bundle],
                peer_payload_inventory: PeerPayloadInventory::default(),
                result_member_adds,
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            }
        } else {
            SyncMessage::ViewUpdate {
                subscription,
                settled_through: GlobalSeq(1),
                reset_result_set: true,
                version_carriers: Vec::new(),
                version_bundles: vec![bundle],
                peer_payload_inventory: PeerPayloadInventory::default(),
                result_member_adds,
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            }
        };

        let error = node
            .apply_sync_message(message)
            .expect_err("view update must reject schema-invalid JSON");
        assert!(
            error
                .to_string()
                .contains("JSON schema validation failed for column `payload`"),
            "unexpected error for chunked={chunked}: {error:?}"
        );
        assert_eq!(
            node.visible_current_cells("documents", document_id)
                .expect("query local row"),
            None,
            "invalid view-update JSON must not reach storage"
        );
    }
}
