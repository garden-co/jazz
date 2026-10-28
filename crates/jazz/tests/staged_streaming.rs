//! Completed byte streams attach once to a serializable transaction; the row
//! and companion metadata publish together through ordinary permissions and sync.

use crate::common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, LargeValueStagingPolicy, ReadOpts,
    ReadTier, StagedStreamingValue, StreamingMutationKind, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn schema() -> JazzSchema {
    schema_with_note(false)
}

fn schema_with_note(note: bool) -> JazzSchema {
    let policy = PolicyExpr::eq_session("owner", vec!["claims".to_owned(), "account".to_owned()]);
    let files = TableSchemaBuilder::new("files")
        .column("owner", ColumnType::Text)
        .column("payload", ColumnType::Bytea)
        .policies(
            jazz::tools::TablePolicies::new()
                .with_select(policy.clone())
                .with_insert(policy.clone())
                .with_update(Some(policy.clone()), policy)
                .with_delete(PolicyExpr::True),
        );
    let files = if note {
        files.nullable_column("note", ColumnType::Text)
    } else {
        files
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(files)
            .table(
                TableSchemaBuilder::new("manifests")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: user(node),
        },
    )
}

fn open(node: u8) -> Db {
    let db = block_on(Db::open(config(node))).unwrap();
    claims(&db, user(node), "alice");
    db
}

fn claims(db: &Db, author: AuthorSubject, account: &str) {
    db.set_identity_claims(
        author,
        BTreeMap::from([(
            jazz::query::provider_claim_key("account"),
            Value::String(account.to_owned()),
        )]),
    );
}

fn payload() -> Vec<u8> {
    // Multiple chunks, including non-UTF8 bytes; a descriptor cannot masquerade
    // as inline text or lose a tail while moving through the transaction overlay.
    (0..350_000)
        .map(|i| ((i * 37 + i / 251) % 256) as u8)
        .collect()
}

fn stage(
    db: &Db,
    id: u8,
    identity: WriteIdentity,
    mutation: StreamingMutationKind,
) -> StagedStreamingValue {
    stage_bytes(db, id, identity, mutation, &payload())
}

fn stage_bytes(
    db: &Db,
    id: u8,
    identity: WriteIdentity,
    mutation: StreamingMutationKind,
    bytes: &[u8],
) -> StagedStreamingValue {
    let cells = BTreeMap::from([("owner".to_owned(), Value::String("alice".to_owned()))]);
    let mut upload = db
        .begin_streaming_value_upload("files", &cells, "payload")
        .unwrap();
    for piece in bytes.chunks(4096) {
        block_on(db.push_streaming_value_upload(&mut upload, piece)).unwrap();
    }
    block_on(db.stage_streaming_value_upload(
        upload,
        mutation,
        "files",
        row(id),
        cells,
        "payload",
        identity,
        None,
        None,
        None,
    ))
    .unwrap()
}

fn rows(db: &Db, table: &str) -> Vec<jazz::db::CurrentRow> {
    let query = db.prepare_query(&Query::from(table)).unwrap();
    block_on(db.all(
        &query,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..Default::default()
        },
    ))
    .unwrap()
}

fn assert_bytes(db: &Db, id: u8) {
    assert_eq!(
        block_on(db.read_value_range("files", row(id), "payload", 0..350_000)).unwrap(),
        payload()
    );
}

fn expire(db: &Db) -> usize {
    db.set_large_value_staging_policy(LargeValueStagingPolicy {
        max_age_ms: 0,
        ..Default::default()
    });
    block_on(db.evict_expired_staged_large_values()).unwrap()
}

fn manifest(db: &Db, tx: OpenTransactionId, id: u8) {
    block_on(db.tx_ref(tx).insert(
        "manifests",
        BTreeMap::from([("label".to_owned(), Value::String("attachment".to_owned()))]),
        InsertOptions {
            row_id: Some(row(id)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Alice stages payload, then commits it together with its manifest. Other
/// readers see neither until commit; cleanup cannot reclaim the published bytes.
/// Alice --stage--> capability --attach--> batch --commit--> file + manifest
#[test]
fn staged_payload_and_manifest_publish_atomically() {
    let db = open(1);
    let mut staged = stage(
        &db,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    assert!(rows(&db, "files").is_empty());
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    block_on(db.attach_staged_streaming_value(tx, &mut staged)).unwrap();
    manifest(&db, tx, 1);
    assert!(rows(&db, "files").is_empty());
    assert!(rows(&db, "manifests").is_empty());
    assert!(
        block_on(db.tx_ref(tx).read("files", row(1)))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        expire(&db),
        0,
        "an attached capability is pinned by its batch"
    );
    block_on(db.tick()).unwrap();
    block_on(db.commit_serializable_handle(tx)).unwrap();
    assert_eq!(rows(&db, "files")[0].seq(), rows(&db, "manifests")[0].seq());
    assert!(!block_on(db.abort_staged_streaming_value(&mut staged)).unwrap());
    assert_eq!(expire(&db), 0);
    block_on(db.tick()).unwrap();
    assert_bytes(&db, 1);
}

/// Alice can update and upsert through the same capability path, retaining one
/// row and publishing the replacement only at the surrounding commit.
#[test]
fn staged_update_and_upsert_preserve_mutation_semantics() {
    let db = open(1);
    for mutation in [
        StreamingMutationKind::Upsert,
        StreamingMutationKind::Update,
        StreamingMutationKind::Upsert,
    ] {
        let mut staged = stage(&db, 1, WriteIdentity::Database, mutation);
        let tx = OpenTransactionId::new();
        block_on(db.begin_serializable(tx)).unwrap();
        block_on(db.attach_staged_streaming_value(tx, &mut staged)).unwrap();
        block_on(db.commit_serializable_handle(tx)).unwrap();
        assert_eq!(rows(&db, "files").len(), 1);
        assert_bytes(&db, 1);
    }
    let mut duplicate = stage(
        &db,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    assert!(block_on(db.attach_staged_streaming_value(tx, &mut duplicate)).is_err());
    db.abandon_transaction_handle(tx).unwrap();
    assert_eq!(expire(&db), 0);
    assert_bytes(&db, 1);
}

/// Alice's dropped, aborted, expired and abandoned capabilities publish no row.
/// Failed attachment consumes its capability, and a consumed handle cannot retry.
#[test]
fn capability_lifetimes_release_their_leases() {
    let db = open(1);
    drop(stage(
        &db,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    ));
    assert_eq!(expire(&db), 0, "Drop queued cleanup before the expiry scan");
    let mut aborted = stage(
        &db,
        2,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    assert!(block_on(db.abort_staged_streaming_value(&mut aborted)).unwrap());
    assert!(!block_on(db.abort_staged_streaming_value(&mut aborted)).unwrap());
    let mut expired = stage(
        &db,
        3,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    assert_eq!(expire(&db), 1);
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    let error = block_on(db.attach_staged_streaming_value(tx, &mut expired)).unwrap_err();
    assert!(error.to_string().contains("expired"));
    assert!(
        block_on(db.attach_staged_streaming_value(tx, &mut expired))
            .unwrap_err()
            .to_string()
            .contains("closed")
    );
    let mut abandoned = stage(
        &db,
        4,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    block_on(db.attach_staged_streaming_value(tx, &mut abandoned)).unwrap();
    db.abandon_transaction_handle(tx).unwrap();
    assert_eq!(expire(&db), 0);
    block_on(db.tick()).unwrap();
    assert!(rows(&db, "files").is_empty());
}

/// Mallory cannot transfer Alice's upload to another owner, schema view,
/// identity or mergeable transaction. Each rejected attachment consumes it.
#[test]
fn capabilities_are_bound_to_owner_schema_identity_and_serializable_kind() {
    let alice = open(1);
    let mallory = open(2);
    let tx = OpenTransactionId::new();
    block_on(alice.begin_serializable(tx)).unwrap();
    block_on(mallory.begin_serializable(tx)).unwrap();
    let mut staged = stage(
        &alice,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    assert!(
        block_on(mallory.attach_staged_streaming_value(tx, &mut staged))
            .unwrap_err()
            .to_string()
            .contains("another database or schema")
    );
    assert!(block_on(alice.attach_staged_streaming_value(tx, &mut staged)).is_err());
    let mut staged = stage(
        &alice,
        1,
        WriteIdentity::Session(user(1)),
        StreamingMutationKind::Insert,
    );
    assert!(
        block_on(alice.attach_staged_streaming_value(tx, &mut staged))
            .unwrap_err()
            .to_string()
            .contains("identity")
    );
    let mergeable = OpenTransactionId::new();
    block_on(alice.begin_mergeable(mergeable)).unwrap();
    let mut staged = stage(
        &alice,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    assert!(
        block_on(alice.attach_staged_streaming_value(mergeable, &mut staged))
            .unwrap_err()
            .to_string()
            .contains("mergeable")
    );
    let another_view = block_on(alice.register_schema_view(schema_with_note(true))).unwrap();
    let mut staged = stage(
        &alice,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    assert!(
        block_on(another_view.attach_staged_streaming_value(tx, &mut staged))
            .unwrap_err()
            .to_string()
            .contains("another database or schema")
    );
    let mut staged = stage(
        &alice,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    block_on(alice.attach_staged_streaming_value(tx, &mut staged)).unwrap();
    assert!(
        block_on(alice.commit_mergeable_handle(tx))
            .unwrap_err()
            .to_string()
            .contains("not mergeable")
    );
    assert_eq!(expire(&alice), 0);
    assert!(rows(&alice, "files").is_empty());
    assert!(rows(&mallory, "files").is_empty());
}

/// Alice's permission is checked again at commit. Revocation after attachment
/// rolls back the companion manifest too; Mallory's session reads hide her rows.
#[test]
fn session_batches_preserve_permissions_on_reads_and_commit() {
    let db = open(1);
    claims(&db, user(2), "mallory");
    let mut staged = stage(
        &db,
        1,
        WriteIdentity::Session(user(1)),
        StreamingMutationKind::Insert,
    );
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(tx, user(1))).unwrap();
    manifest(&db, tx, 1);
    block_on(db.attach_staged_streaming_value(tx, &mut staged)).unwrap();
    claims(&db, user(1), "revoked");
    assert!(block_on(db.commit_serializable_handle(tx)).is_err());
    assert!(rows(&db, "files").is_empty());
    assert!(rows(&db, "manifests").is_empty());
    assert_eq!(expire(&db), 0);
    claims(&db, user(1), "alice");
    let mut staged = stage(
        &db,
        2,
        WriteIdentity::Session(user(1)),
        StreamingMutationKind::Insert,
    );
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(tx, user(1))).unwrap();
    block_on(db.attach_staged_streaming_value(tx, &mut staged)).unwrap();
    block_on(db.commit_serializable_handle(tx)).unwrap();
    let mallory = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(mallory, user(2))).unwrap();
    assert!(
        block_on(db.tx_ref(mallory).read("files", row(2)))
            .unwrap()
            .is_none()
    );
    // Partial edits must read as the batch's session too, before deriving bytes.
    let error = block_on(db.tx_ref(mallory).update_with_large_value_mutations(
        "files",
        row(2),
        BTreeMap::new(),
        vec![jazz::db::LargeValueUpdate::Splice {
            column: "payload".to_owned(),
            within: jazz::db::LargeValueUpdatePage::Bytes { from: 0, to: 1 },
            splices: vec![jazz::db::LargeValueUpdateSplice {
                at: 0,
                delete: 1,
                insert: vec![0],
            }],
        }],
        Default::default(),
    ))
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    let mut staged = stage(
        &db,
        3,
        WriteIdentity::Session(user(2)),
        StreamingMutationKind::Insert,
    );
    assert!(block_on(db.attach_staged_streaming_value(mallory, &mut staged)).is_err());
    db.abandon_transaction_handle(mallory).unwrap();
    assert_eq!(rows(&db, "files").len(), 1);
}

/// Alice's concurrent manifest write invalidates the earlier snapshot. Neither
/// staged payload nor the batch's companion manifest may escape the conflict.
#[test]
fn serializable_conflict_rolls_back_every_staged_row() {
    let db = open(1);
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    let query = db.prepare_query(&Query::from("manifests")).unwrap();
    assert!(
        block_on(db.tx_ref(tx).all_prepared(&query))
            .unwrap()
            .is_empty()
    );
    let mut staged = stage(
        &db,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    block_on(db.attach_staged_streaming_value(tx, &mut staged)).unwrap();
    manifest(&db, tx, 1);
    block_on(db.insert(
        "manifests",
        BTreeMap::from([("label".to_owned(), Value::String("concurrent".to_owned()))]),
        InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(
        block_on(db.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
    assert!(rows(&db, "files").is_empty());
    assert_eq!(rows(&db, "manifests").len(), 1);
    assert_eq!(expire(&db), 0);
}

/// Alice's client pushes chunks through ordinary sync before its atomic commit.
/// The authority authenticates the session and accepts the bytes and manifest.
#[test]
fn staged_payload_uses_normal_chunk_sync_and_authority_permissions() {
    let server = open(0);
    let alice = open(1);
    let mallory = open(2);
    claims(&server, user(1), "alice");
    claims(&server, user(2), "mallory");
    for client in [&alice, &mallory] {
        let (up, down) = duplex_transport::duplex();
        let _link = block_on(client.connect_upstream(up));
        server.accept_subscriber(
            down,
            if std::ptr::eq(client, &alice) {
                user(1)
            } else {
                user(2)
            },
        );
    }
    for (client, id) in [(&alice, 1), (&mallory, 2)] {
        let mut staged = stage(
            client,
            id,
            WriteIdentity::Database,
            StreamingMutationKind::Insert,
        );
        let tx = OpenTransactionId::new();
        block_on(client.begin_serializable(tx)).unwrap();
        manifest(client, tx, id);
        block_on(client.attach_staged_streaming_value(tx, &mut staged)).unwrap();
        block_on(client.commit_serializable_handle(tx)).unwrap();
    }
    for _ in 0..40 {
        block_on(alice.tick()).unwrap();
        block_on(mallory.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert_eq!(
        rows(&server, "files").len(),
        1,
        "Mallory's denied commit publishes no payload"
    );
    assert_eq!(
        rows(&server, "manifests").len(),
        1,
        "Mallory's companion manifest rolls back too"
    );
    assert!(
        rows(&mallory, "files").is_empty(),
        "the denied optimistic row retracts"
    );
    assert_bytes(&server, 1);
}

/// Alice reopens a file store after a committed upload and an abandoned staged
/// one. Maintenance releases orphan receipts and preserves published payload.
#[test]
fn reopening_keeps_committed_payload_and_expires_orphan_uploads() {
    let dir = std::env::temp_dir().join(format!("jazz-staged-stream-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("store.sqlite");
    let db = block_on(Db::open_at(config(1), &path, Default::default())).unwrap();
    claims(&db, user(1), "alice");
    let mut staged = stage(
        &db,
        1,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    block_on(db.attach_staged_streaming_value(tx, &mut staged)).unwrap();
    block_on(db.commit_serializable_handle(tx)).unwrap();
    block_on(db.tick()).unwrap();
    let orphan = stage(
        &db,
        2,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    drop(db);
    drop(orphan);
    let reopened = block_on(Db::open_at(config(1), &path, Default::default())).unwrap();
    claims(&reopened, user(1), "alice");
    assert_eq!(expire(&reopened), 1);
    block_on(reopened.tick()).unwrap();
    assert_eq!(rows(&reopened, "files").len(), 1);
    assert_bytes(&reopened, 1);
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

/// Alice's empty, inline and chunked uploads all publish exact bytes and settle
/// through the ordinary authority path. Expiry cannot reclaim committed bytes.
#[test]
fn payload_size_boundaries_survive_commit_cleanup_and_sync() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    let alice = open(1);
    let server = open(0);
    claims(&server, user(1), "alice");
    let (up, down) = duplex_transport::duplex();
    block_on(alice.connect_upstream(up));
    server.accept_subscriber(down, user(1));
    let inline = jazz::large_values::INLINE_VALUE_MAX_BYTES;
    for (index, size) in [0, 1, inline - 1, inline, inline + 1]
        .into_iter()
        .enumerate()
    {
        let id = index as u8 + 1;
        let bytes: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let mut staged = stage_bytes(
            &alice,
            id,
            WriteIdentity::Database,
            StreamingMutationKind::Insert,
            &bytes,
        );
        let tx = OpenTransactionId::new();
        block_on(alice.begin_serializable(tx)).unwrap();
        block_on(alice.attach_staged_streaming_value(tx, &mut staged)).unwrap();
        let receipt = block_on(alice.commit_serializable_handle(tx)).unwrap();
        assert_eq!(expire(&alice), 0);
        let mut wait =
            pin!(alice.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global));
        let mut settled = false;
        for _ in 0..100 {
            match wait.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(result) => {
                    result.unwrap();
                    settled = true;
                    break;
                }
                Poll::Pending => {
                    block_on(alice.tick()).unwrap();
                    block_on(server.tick()).unwrap();
                }
            }
        }
        assert!(settled, "upload of {size} bytes did not settle");
        assert_eq!(
            block_on(server.read_value_range("files", row(id), "payload", 0..size as u64)).unwrap(),
            bytes
        );
        let published = rows(&alice, "files");
        assert_eq!(
            published
                .iter()
                .find(|r| r.row_uuid() == row(id))
                .unwrap()
                .cell("payload"),
            Some(Value::Bytes(bytes))
        );
    }
}

/// Closing Alice's owner refuses both completing a stream and attaching a
/// completed capability. Cleanup and abandonment still release their leases.
#[test]
fn shutdown_fence_refuses_staging_and_attachment_without_leaking_uploads() {
    let alice = open(1);
    let cells = BTreeMap::from([("owner".into(), Value::String("alice".into()))]);
    let mut upload = alice
        .begin_streaming_value_upload("files", &cells, "payload")
        .unwrap();
    block_on(alice.push_streaming_value_upload(&mut upload, &payload())).unwrap();
    let mut staged = stage(
        &alice,
        2,
        WriteIdentity::Database,
        StreamingMutationKind::Insert,
    );
    let tx = OpenTransactionId::new();
    block_on(alice.begin_serializable(tx)).unwrap();
    alice.stop_mutation_admission();
    let error = block_on(alice.stage_streaming_value_upload(
        upload,
        StreamingMutationKind::Insert,
        "files",
        row(1),
        cells,
        "payload",
        WriteIdentity::Database,
        None,
        None,
        None,
    ))
    .err()
    .expect("closed owner");
    assert_eq!(error.code, ErrorCode::Storage);
    let error = block_on(alice.attach_staged_streaming_value(tx, &mut staged)).unwrap_err();
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(!block_on(alice.abort_staged_streaming_value(&mut staged)).unwrap());
    alice.abandon_transaction_handle(tx).unwrap();
    assert_eq!(expire(&alice), 0);
    block_on(alice.tick()).unwrap();
    assert!(rows(&alice, "files").is_empty());
}
