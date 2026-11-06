//! Submission metadata is inspected through SQLite because it deliberately
//! conveys no public observation capability until a genuine receipt arrives.
use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::protocol::{LensOp, MigrationLens, TableLens};
use jazz::query::Query;
use jazz::records::{OwnedRecord, RecordDescriptor, Value};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use rusqlite::Connection;
use std::{collections::BTreeMap, path::Path};

fn schema(table: &str, column: &str) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column(column, ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}
fn open(path: &Path, schema: JazzSchema) -> Db {
    let db = block_on(Db::open_at(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([7; 16]),
                author: AuthorSubject::SYSTEM,
            },
        ),
        path,
        Default::default(),
    ))
    .unwrap();
    db.become_replica();
    db
}
fn body(column: &str, text: &str) -> BTreeMap<String, Value> {
    [(column.into(), Value::String(text.into()))].into()
}
fn submissions(conn: &Connection) -> Vec<(i64, Vec<u8>, Vec<u8>)> {
    conn.prepare("SELECT op,schema,scope FROM _jazz_observation_pending_v1 ORDER BY tx,op")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Alice's non-system database normalizes Session(SYSTEM) like Database;
/// Bob's real session remains distinct. Frozen metadata follows the actual
/// operation identity at staging and survives reopening without relabelling.
#[test]
fn system_session_uses_the_database_submission_scope() {
    use jazz::db::{InsertOptions, WriteIdentity};
    let alice = AuthorSubject::for_test_bytes([81; 16]);
    let bob = AuthorSubject::for_test_bytes([82; 16]);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("normalized-identity.sqlite");
    let schema = schema("notes", "body");
    let config = || {
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([83; 16]),
                author: alice,
            },
        )
    };
    let db = block_on(Db::open_at(config(), &path, Default::default())).unwrap();
    db.become_replica();
    for (author, name) in [
        (alice, "alice"),
        (bob, "bob"),
        (AuthorSubject::SYSTEM, "system"),
    ] {
        db.set_identity_claims(author, [("name".into(), Value::String(name.into()))].into());
    }
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for (identity, name) in [
        (WriteIdentity::Database, "database"),
        (
            WriteIdentity::Session(AuthorSubject::SYSTEM),
            "system-session",
        ),
        (WriteIdentity::Session(alice), "alice-session"),
        (WriteIdentity::Session(bob), "bob-session"),
    ] {
        block_on(db.tx_ref(tx).insert(
            "notes",
            body("body", name),
            InsertOptions {
                identity,
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(db.read(&query).unwrap().len(), 4);
    let conn = Connection::open(&path).unwrap();
    let held = submissions(&conn);
    assert_eq!(held.len(), 4);
    assert_eq!(
        held[0].2, held[1].2,
        "SYSTEM session is local database identity"
    );
    assert_eq!(
        held[0].2, held[2].2,
        "actual Alice session has Alice operands"
    );
    assert_ne!(
        held[0].2, held[3].2,
        "Bob's actual session remains distinct"
    );
    block_on(db.close()).unwrap();
    drop(db);
    let reopened = block_on(Db::open_at(config(), &path, Default::default())).unwrap();
    assert_eq!(submissions(&conn), held);
    let query = reopened.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(reopened.read(&query).unwrap().len(), 4);
}

/// Alice's submission keeps the operation's schema/scope after claims change
/// and the owner reopens. Pending provenance allocates no evidence watermark.
#[test]
fn submission_survives_reopen_without_receipt_time_scope_inference() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let schema = schema("notes", "body");
    let db = open(&path, schema.clone());
    block_on(db.insert("notes", body("body", "first"), Default::default())).unwrap();
    let conn = Connection::open(&path).unwrap();
    let held = submissions(&conn);
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].1, schema.version_id().as_bytes());
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/observation_scope_v1.json")).unwrap();
    let mut scope = hex::decode(fixtures["empty"]["sha256_hex"].as_str().unwrap()).unwrap();
    scope.push(0); // Actual local CurrentOwner, not an inferred upstream pin.
    scope.extend_from_slice(schema.version_id().as_bytes());
    scope.push(0); // UNKNOWN upstream governing context on a replica.
    let expected =
        jazz_types::sha256::derive_key("jazz.sqlite.observation-submission-scope.v1", &scope);
    assert_eq!(held[0].2, expected);
    db.set_identity_claims(
        AuthorSubject::SYSTEM,
        [("new".into(), Value::Bool(true))].into(),
    );
    block_on(db.close()).unwrap();
    drop(db);
    let reopened = open(&path, schema);
    assert_eq!(submissions(&conn), held);
    let evidence: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='_jazz_observation_evidence_v1')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!evidence);
    assert_eq!(
        reopened
            .read(&reopened.prepare_query(&Query::from("notes")).unwrap())
            .unwrap()
            .len(),
        1
    );
}

/// Alice writes through two schema versions in one ordinary batch. Each op
/// retains its own original version even though both lower to stored names.
#[test]
fn ordinary_batch_preserves_per_operation_schema_versions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let old = schema("notes", "body");
    let new = schema("tasks", "title");
    let db = open(&path, old.clone());
    let old_view = block_on(db.register_schema_view(old.clone())).unwrap();
    let lens = MigrationLens::new(
        old.version_id(),
        new.version_id(),
        vec![TableLens {
            source_table: "notes".into(),
            target_table: "tasks".into(),
            ops: vec![LensOp::RenameColumn {
                from: "body".into(),
                to: "title".into(),
            }],
        }],
    )
    .unwrap();
    block_on(db.activate_schema_with_lenses(new.clone(), &[lens])).unwrap();
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(
        old_view
            .tx_ref(tx)
            .insert("notes", body("body", "old"), Default::default()),
    )
    .unwrap();
    block_on(
        db.tx_ref(tx)
            .insert("tasks", body("title", "new"), Default::default()),
    )
    .unwrap();
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    let held = submissions(&Connection::open(path).unwrap());
    assert_eq!(held.len(), 2);
    assert_eq!(held[0].0, 0);
    assert_eq!(held[1].0, 1);
    assert_eq!(held[0].1, old.version_id().as_bytes());
    assert_eq!(held[1].1, new.version_id().as_bytes());
    assert_eq!(
        db.read(&db.prepare_query(&Query::from("tasks")).unwrap())
            .unwrap()
            .len(),
        2
    );
}

/// Alice's provenance storage failure rolls back the pending write and body
/// together. A later valid write still works with the same owner.
#[test]
fn submission_storage_failure_does_not_publish_a_partial_write() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let db = open(&path, schema("notes", "body"));
    block_on(db.insert("notes", body("body", "first"), Default::default())).unwrap();
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_submission BEFORE INSERT ON _jazz_observation_pending_v1 BEGIN SELECT RAISE(ABORT,'injected provenance failure'); END;").unwrap();
    assert!(
        block_on(db.insert("notes", body("body", "must roll back"), Default::default())).is_err()
    );
    assert_eq!(submissions(&conn).len(), 1);
    assert_eq!(
        conn.query_row("SELECT count(*) FROM _jazz_pending", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    let rows = db.read(&query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("body"), Some(Value::String("first".into())));
    conn.execute_batch("DROP TRIGGER reject_submission")
        .unwrap();
    block_on(db.insert("notes", body("body", "after failure"), Default::default())).unwrap();
    assert_eq!(submissions(&conn).len(), 2);
}

/// Alice's known ordinary write still commits when its engine-only claims
/// cannot be represented by observation scope v1. No guessed record appears.
#[test]
fn unsupported_submission_scope_preserves_ordinary_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("alice.sqlite");
    let db = open(&path, schema("notes", "body"));
    db.set_identity_claims(
        AuthorSubject::SYSTEM,
        [(
            "unsupported".into(),
            Value::Record(OwnedRecord::new(
                Vec::new(),
                RecordDescriptor::new(Vec::<(String, jazz::records::ValueType)>::new()),
            )),
        )]
        .into(),
    );
    block_on(db.insert("notes", body("body", "ordinary"), Default::default())).unwrap();
    assert_eq!(
        db.read(&db.prepare_query(&Query::from("notes")).unwrap())
            .unwrap()
            .len(),
        1
    );
    let conn = Connection::open(path).unwrap();
    assert!(!conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='_jazz_observation_pending_v1')",[],|r|r.get::<_,bool>(0)).unwrap());
}

// The local companion is deliberately not on this real byte-frame transport.
// Only the explicit Rust attachment below can supply producer provenance.
struct LocalFrames {
    outbound: std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<Vec<u8>>>>,
    inbound: std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<Vec<u8>>>>,
    sent: std::rc::Rc<std::cell::RefCell<Vec<Vec<u8>>>>,
}
impl jazz::db::WireTransport for LocalFrames {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), jazz::db::TransportError> {
        self.sent.borrow_mut().push(frame.clone());
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}
type FrameLog = std::rc::Rc<std::cell::RefCell<Vec<Vec<u8>>>>;
fn connect_local(receiver: &Db, producer: &Db) -> (jazz::db::SubscriberId, FrameLog) {
    let up = Default::default();
    let down = Default::default();
    let sent = FrameLog::default();
    let producer_wire = LocalFrames {
        outbound: std::rc::Rc::clone(&up),
        inbound: std::rc::Rc::clone(&down),
        sent: sent.clone(),
    };
    let receiver_wire = LocalFrames {
        outbound: down,
        inbound: up,
        sent: Default::default(),
    };
    block_on(
        producer.connect_upstream(Box::new(jazz::db::WireTransportAdapter::current(
            producer_wire,
        ))),
    );
    let subscriber = receiver
        .accept_subscriber(
            Box::new(jazz::db::WireTransportAdapter::current(receiver_wire)),
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    (subscriber, sent)
}
fn drive_local(owners: &[&Db]) {
    for _ in 0..12 {
        for db in owners {
            block_on(db.tick()).unwrap();
        }
    }
}
fn observation_config(schema: JazzSchema, node: u8) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}
fn roots_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("roots")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("refs")
                    .column("root", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

/// Alice's foreground emits ordinary lowered byte frames; her privileged
/// local host attaches only core-generated provenance to the durable owner.
/// foreground -> durable owner -> authority -> Global -> disk reopen/offline
/// Both root/ref observations share one cut and exclude a new pending prefix.
/// This is a native SQLite persistence test, not a browser/worker claim.
#[test]
fn trusted_local_root_ref_global_reopen_uses_one_accepted_cut() {
    use jazz::db::{LocalEvidenceProvenance, TxReceipt};
    let schema = roots_schema();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("durable.sqlite");
    let authority = block_on(Db::open_history_complete(observation_config(
        schema.clone(),
        1,
    )))
    .unwrap();
    let durable = block_on(Db::open_at(
        observation_config(schema.clone(), 7),
        &path,
        Default::default(),
    ))
    .unwrap();
    let foreground = block_on(Db::open(observation_config(schema.clone(), 9))).unwrap();
    connect_local(&authority, &durable);
    let (session, frames) = connect_local(&durable, &foreground);
    drive_local(&[&authority, &durable, &foreground]);
    // A retained-name view makes lowering observable: its public names differ
    // from the already-admitted physical names in ordinary Commit frames.
    let renamed = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("root_records")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("ref_records")
                    .column("root_id", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let lens = MigrationLens::new(
        schema.version_id(),
        renamed.version_id(),
        vec![
            TableLens {
                source_table: "roots".into(),
                target_table: "root_records".into(),
                ops: vec![LensOp::RenameColumn {
                    from: "label".into(),
                    to: "title".into(),
                }],
            },
            TableLens {
                source_table: "refs".into(),
                target_table: "ref_records".into(),
                ops: vec![LensOp::RenameColumn {
                    from: "root".into(),
                    to: "root_id".into(),
                }],
            },
        ],
    )
    .unwrap();
    block_on(authority.activate_schema_with_lenses(renamed.clone(), &[lens])).unwrap();
    drive_local(&[&authority, &durable, &foreground]);
    let foreground_view = block_on(foreground.register_schema_view(renamed.clone())).unwrap();
    let durable_view = block_on(durable.register_schema_view(renamed.clone())).unwrap();
    let route = durable
        .trusted_local_submission_route(&foreground, session)
        .unwrap();
    let tx = OpenTransactionId::new();
    block_on(foreground.begin_serializable(tx)).unwrap();
    let root = block_on(foreground_view.tx_ref(tx).insert(
        "root_records",
        body("title", "accepted"),
        Default::default(),
    ))
    .unwrap();
    block_on(foreground_view.tx_ref(tx).insert(
        "ref_records",
        [("root_id".into(), Value::Uuid(root.row_uuid().0))].into(),
        Default::default(),
    ))
    .unwrap();
    let receipt = block_on(foreground.commit_serializable_handle(tx)).unwrap();
    let export = foreground
        .export_local_submission(&route, receipt)
        .unwrap()
        .unwrap();
    assert!(durable.attach_local_submission(&route, export).unwrap());
    // Local durability includes both pending content and its original schemas.
    block_on(foreground.tick()).unwrap();
    block_on(durable.tick()).unwrap();
    let disk = Connection::open(&path).unwrap();
    let pending_provenance = submissions(&disk);
    assert_eq!(pending_provenance.len(), 2);
    assert!(
        pending_provenance
            .iter()
            .all(|(_, version, _)| version == renamed.version_id().as_bytes())
    );
    let pending: i64 = disk
        .query_row("SELECT count(*) FROM _jazz_pending", [], |r| r.get(0))
        .unwrap();
    assert_eq!(pending, 1);
    let commits: Vec<_> = frames
        .borrow()
        .iter()
        .filter_map(|frame| jazz::db::decode_message(frame).ok())
        .filter_map(|message| match message {
            jazz::db::SyncMessage::Commit { ops, .. } => Some(ops),
            _ => None,
        })
        .collect();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].len(), 2);
    assert!(
        commits[0]
            .iter()
            .all(|op| op.table() != "root_records" && op.table() != "ref_records")
    );
    drive_local(&[&authority, &durable, &foreground]);
    block_on(foreground.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global)).unwrap();
    assert!(submissions(&disk).is_empty());
    assert_eq!(
        disk.query_row(
            "SELECT count(*) FROM _jazz_observation_evidence_v1",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    let prepared = durable_view
        .prepare_query(&Query::from("root_records"))
        .unwrap();
    let retired_cut = durable_view.local_observation_cut(&prepared, None).unwrap();
    block_on(durable.close()).unwrap();
    drop(durable_view);
    drop(durable);
    let reopened_owner = open(&path, schema);
    let reopened = block_on(reopened_owner.register_schema_view(renamed)).unwrap();
    let roots = reopened
        .prepare_query(&Query::from("root_records"))
        .unwrap();
    let refs = reopened.prepare_query(&Query::from("ref_records")).unwrap();
    assert!(reopened.observe_local(&retired_cut, &roots, false).is_err());
    let cut = reopened.local_observation_cut(&roots, None).unwrap();
    block_on(reopened.insert("root_records", body("title", "pending"), Default::default()))
        .unwrap();
    block_on(reopened.update(
        "root_records",
        root.row_uuid(),
        body("title", "pending replacement"),
        Default::default(),
    ))
    .unwrap();
    let observed = reopened.observe_local(&cut, &roots, false).unwrap();
    assert_eq!(observed.rows.len(), 1);
    assert_eq!(
        observed.rows[0].cell("title"),
        Some(Value::String("accepted".into()))
    );
    assert_eq!(observed.evidence.len(), 1);
    assert_eq!(observed.evidence[0].state, observed.evidence[0].content);
    assert_eq!(
        observed.provenance,
        vec![LocalEvidenceProvenance::LocalSubmission]
    );
    let observed_refs = reopened.observe_local(&cut, &refs, false).unwrap();
    assert_eq!(observed_refs.rows.len(), 1);
    assert_eq!(
        observed_refs.rows[0].cell("root_id"),
        Some(Value::Uuid(root.row_uuid().0))
    );
    assert_eq!(observed.through, observed_refs.through);
    assert_eq!(
        reopened.read(&roots).unwrap().len(),
        2,
        "ordinary reads retain their pending prefix"
    );
    assert!(
        foreground
            .export_local_submission(&route, TxReceipt(receipt.0))
            .unwrap()
            .is_none(),
        "retired receiver invalidates export"
    );
}

/// Alice's receipt retains its local captured scope; Bob cannot substitute a
/// replacement owner/session. Refusing the companion does not refuse her
/// otherwise valid ordinary wire Commit or invent observation evidence.
#[test]
fn retired_local_route_leaves_ordinary_commit_valid_and_unobserved() {
    let schema = schema("notes", "body");
    let authority = block_on(Db::open_history_complete(observation_config(
        schema.clone(),
        1,
    )))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("durable.sqlite");
    let durable = open(&path, schema.clone());
    let foreground = block_on(Db::open(observation_config(schema, 9))).unwrap();
    connect_local(&authority, &durable);
    let (session, _) = connect_local(&durable, &foreground);
    drive_local(&[&authority, &durable, &foreground]);
    let route = durable
        .trusted_local_submission_route(&foreground, session)
        .unwrap();
    let write =
        block_on(foreground.insert("notes", body("body", "valid"), Default::default())).unwrap();
    let export = foreground
        .export_local_submission(&route, jazz::db::TxReceipt(write.mergeable_tx_id()))
        .unwrap()
        .unwrap();
    foreground.retire_local_observation_capabilities();
    assert!(!durable.attach_local_submission(&route, export).unwrap());
    drive_local(&[&authority, &durable, &foreground]);
    block_on(
        foreground.wait_for_transaction(write.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
    )
    .unwrap();
    let query = durable.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(durable.read(&query).unwrap().len(), 1);
    let cut = durable.local_observation_cut(&query, None).unwrap();
    assert_eq!(
        durable.observe_local(&cut, &query, false).unwrap_err().code,
        jazz::db::ErrorCode::NotObserved
    );
}

/// Alice's accepted receipt and evidence allocation are in the same SQLite
/// transaction as normal baseline replay. An injected accepted-body failure
/// rolls back the evidence, watermark and pending cleanup; reopening resends
/// the original commit and can then observe the genuine accepted body.
#[test]
fn accepted_body_rebase_failure_cannot_publish_observation_evidence() {
    let schema = schema("notes", "body");
    let authority = block_on(Db::open_history_complete(observation_config(
        schema.clone(),
        1,
    )))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("durable.sqlite");
    let local = open(&path, schema.clone());
    connect_local(&authority, &local);
    drive_local(&[&authority, &local]);
    let write = block_on(local.insert(
        "notes",
        body("body", "accepted after retry"),
        Default::default(),
    ))
    .unwrap();
    let disk = Connection::open(&path).unwrap();
    let captured = submissions(&disk);
    assert_eq!(captured.len(), 1);
    disk.execute_batch("CREATE TRIGGER fail_accepted_body BEFORE INSERT ON notes WHEN length(NEW._writer)=0 BEGIN SELECT RAISE(ABORT,'accepted body persistence failure'); END;").unwrap();
    block_on(local.tick()).unwrap();
    block_on(authority.tick()).unwrap();
    let error = block_on(local.tick()).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Storage);
    assert_eq!(
        submissions(&disk),
        captured,
        "pending provenance survived rollback"
    );
    assert_eq!(
        disk.query_row("SELECT count(*) FROM _jazz_pending", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let evidence_exists: bool = disk
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='_jazz_observation_evidence_v1')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    if evidence_exists {
        assert_eq!(
            disk.query_row(
                "SELECT count(*) FROM _jazz_observation_evidence_v1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        assert_eq!(disk.query_row("SELECT coalesce(MAX(seq),0) FROM sqlite_sequence WHERE name='_jazz_observation_evidence_v1'", [], |r|r.get::<_,i64>(0)).unwrap(), 0);
    }
    let query = local.prepare_query(&Query::from("notes")).unwrap();
    let cut = local.local_observation_cut(&query, None).unwrap();
    match local.observe_local(&cut, &query, false) {
        Ok(observation) => assert!(observation.rows.is_empty()),
        Err(error) => assert_eq!(error.code, jazz::db::ErrorCode::NotObserved),
    }
    assert_eq!(
        authority
            .read(&authority.prepare_query(&Query::from("notes")).unwrap())
            .unwrap()
            .len(),
        1
    );
    disk.execute_batch("DROP TRIGGER fail_accepted_body")
        .unwrap();
    drop(local);
    let reopened = open(&path, schema);
    connect_local(&authority, &reopened);
    drive_local(&[&authority, &reopened]);
    block_on(
        reopened.wait_for_transaction(write.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
    )
    .unwrap();
    assert!(submissions(&disk).is_empty());
    let query = reopened.prepare_query(&Query::from("notes")).unwrap();
    let cut = reopened.local_observation_cut(&query, None).unwrap();
    let observed = reopened.observe_local(&cut, &query, false).unwrap();
    assert_eq!(observed.rows.len(), 1);
    assert_eq!(
        observed.rows[0].cell("body"),
        Some(Value::String("accepted after retry".into()))
    );
    assert_eq!(
        observed.provenance,
        vec![jazz::db::LocalEvidenceProvenance::LocalSubmission]
    );
}

/// Alice's callback transaction lacks per-operation original-schema capture in
/// the existing callback buffer. Its ordinary commit still succeeds; C does not
/// substitute the current schema for the missing original provenance.
#[test]
fn unsupported_callback_capture_keeps_ordinary_commit_without_guessed_scope() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("callback.sqlite");
    let local = open(&path, schema("notes", "body"));
    block_on(local.transaction(async |tx| {
        tx.insert(
            "notes",
            body("body", "ordinary callback"),
            Default::default(),
        )
        .await?;
        Ok(())
    }))
    .unwrap();
    let disk = Connection::open(&path).unwrap();
    assert_eq!(
        disk.query_row("SELECT count(*) FROM _jazz_pending", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(!disk.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='_jazz_observation_pending_v1')", [], |r|r.get::<_,bool>(0)).unwrap());
    assert_eq!(
        local
            .read(&local.prepare_query(&Query::from("notes")).unwrap())
            .unwrap()
            .len(),
        1
    );
}

/// Alice's opaque companion belongs to two actual owners and one admitted
/// session generation. Bob's other owner and a reused slot cannot accept it;
/// the reconnected ordinary Commit still reaches Global without evidence.
#[test]
fn wrong_owner_and_reused_session_cannot_attach_local_provenance() {
    let schema = schema("notes", "body");
    let authority = block_on(Db::open_history_complete(observation_config(
        schema.clone(),
        1,
    )))
    .unwrap();
    let durable = block_on(Db::open(observation_config(schema.clone(), 7))).unwrap();
    let foreground = block_on(Db::open(observation_config(schema, 9))).unwrap();
    connect_local(&authority, &durable);
    let (session, _) = connect_local(&durable, &foreground);
    drive_local(&[&authority, &durable, &foreground]);
    let route = durable
        .trusted_local_submission_route(&foreground, session)
        .unwrap();
    let write = block_on(foreground.insert(
        "notes",
        body("body", "ordinary after reconnect"),
        Default::default(),
    ))
    .unwrap();
    let export = foreground
        .export_local_submission(&route, jazz::db::TxReceipt(write.mergeable_tx_id()))
        .unwrap()
        .unwrap();
    assert!(
        !authority
            .attach_local_submission(&route, export.clone())
            .unwrap()
    );
    durable.disconnect_subscriber(session);
    let (replacement, _) = connect_local(&durable, &foreground);
    assert_ne!(
        session, replacement,
        "an admitted slot's generation changes"
    );
    assert!(!durable.attach_local_submission(&route, export).unwrap());
    drive_local(&[&authority, &durable, &foreground]);
    block_on(
        foreground.wait_for_transaction(write.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
    )
    .unwrap();
    let query = durable.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        durable.read(&query).unwrap()[0].cell("body"),
        Some(Value::String("ordinary after reconnect".into()))
    );
    let cut = durable.local_observation_cut(&query, None).unwrap();
    assert_eq!(
        durable.observe_local(&cut, &query, false).unwrap_err().code,
        jazz::db::ErrorCode::NotObserved
    );
}

/// Alice attaches the second operation's companion before the first ordinary
/// frame. Exact frame/transaction matching rejects it without disturbing either
/// valid write; neither commit gains fabricated receipt-local provenance.
#[test]
fn mismatched_local_companion_preserves_both_ordinary_commits() {
    let schema = schema("notes", "body");
    let authority = block_on(Db::open_history_complete(observation_config(
        schema.clone(),
        1,
    )))
    .unwrap();
    let durable = block_on(Db::open(observation_config(schema.clone(), 7))).unwrap();
    let foreground = block_on(Db::open(observation_config(schema, 9))).unwrap();
    connect_local(&authority, &durable);
    let (session, _) = connect_local(&durable, &foreground);
    drive_local(&[&authority, &durable, &foreground]);
    let route = durable
        .trusted_local_submission_route(&foreground, session)
        .unwrap();
    let first =
        block_on(foreground.insert("notes", body("body", "first"), Default::default())).unwrap();
    let second =
        block_on(foreground.insert("notes", body("body", "second"), Default::default())).unwrap();
    let export = foreground
        .export_local_submission(&route, jazz::db::TxReceipt(second.mergeable_tx_id()))
        .unwrap()
        .unwrap();
    assert!(durable.attach_local_submission(&route, export).unwrap());
    drive_local(&[&authority, &durable, &foreground]);
    for write in [first, second] {
        block_on(
            foreground
                .wait_for_transaction(write.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
        )
        .unwrap();
    }
    let query = durable.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(durable.read(&query).unwrap().len(), 2);
    let cut = durable.local_observation_cut(&query, None).unwrap();
    assert_eq!(
        durable.observe_local(&cut, &query, false).unwrap_err().code,
        jazz::db::ErrorCode::NotObserved
    );
}

/// Alice's optional local capture is malformed on disk, while her ordinary
/// pending Commit is valid. No guessed scope is exported, and Bob's durable
/// receiver accepts the normal write without observation evidence.
#[test]
fn malformed_optional_capture_keeps_the_normal_commit_valid() {
    let schema = schema("notes", "body");
    let authority = block_on(Db::open_history_complete(observation_config(
        schema.clone(),
        1,
    )))
    .unwrap();
    let durable = block_on(Db::open(observation_config(schema.clone(), 8))).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("foreground.sqlite");
    let foreground = open(&path, schema);
    connect_local(&authority, &durable);
    let (session, _) = connect_local(&durable, &foreground);
    drive_local(&[&authority, &durable, &foreground]);
    let route = durable
        .trusted_local_submission_route(&foreground, session)
        .unwrap();
    let write =
        block_on(foreground.insert("notes", body("body", "ordinary"), Default::default())).unwrap();
    let disk = Connection::open(path).unwrap();
    disk.execute_batch("PRAGMA ignore_check_constraints=ON; UPDATE _jazz_observation_pending_v1 SET schema=zeroblob(15); PRAGMA ignore_check_constraints=OFF;").unwrap();
    assert!(
        foreground
            .export_local_submission(&route, jazz::db::TxReceipt(write.mergeable_tx_id()))
            .unwrap()
            .is_none()
    );
    drive_local(&[&authority, &durable, &foreground]);
    block_on(
        foreground.wait_for_transaction(write.mergeable_tx_id(), jazz::tx::DurabilityTier::Global),
    )
    .unwrap();
    assert!(submissions(&disk).is_empty());
    let query = durable.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(durable.read(&query).unwrap().len(), 1);
    let cut = durable.local_observation_cut(&query, None).unwrap();
    assert_eq!(
        durable.observe_local(&cut, &query, false).unwrap_err().code,
        jazz::db::ErrorCode::NotObserved
    );
}

/// Alice stages two ordinary writes under scopes A/B, then changes to C before
/// commit. Global receipt and reopen preserve A/B, never the commit-time scope.
/// stage A -> stage B -> registry C -> commit -> Global -> offline reopen
#[test]
fn staged_scopes_survive_commit_registry_changes_global_and_reopen() {
    use jazz::db::{ErrorCode, LocalEvidenceProvenance};
    use jazz::query::{col, eq, lit};

    let schema = schema("notes", "body");
    let authority = block_on(Db::open_history_complete(observation_config(
        schema.clone(),
        1,
    )))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("staged-scopes.sqlite");
    let local = open(&path, schema.clone());
    connect_local(&authority, &local);
    drive_local(&[&authority, &local]);
    let claims = |phase: &str| [("phase".into(), Value::String(phase.into()))].into();
    // Derive expected bytes from the independent empty-scope fixture and the
    // named codec's single string-claim encoding, not the capture helper.
    let expected_scope = |phase: u8| {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/observation_scope_v1.json")).unwrap();
        let mut bytes = hex::decode(fixtures["empty"]["preimage_hex"].as_str().unwrap()).unwrap();
        assert_eq!(&bytes[bytes.len() - 4..], &[0; 4]);
        bytes.truncate(bytes.len() - 4);
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&5_u32.to_le_bytes());
        bytes.extend_from_slice(b"phase");
        bytes.push(9); // String claim.
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.push(phase);
        let mut context =
            jazz_types::sha256::derive_key("jazz.sqlite.observation-scope.v1", &bytes).to_vec();
        context.push(0); // Actual local CurrentOwner.
        context.extend_from_slice(schema.version_id().as_bytes());
        context.push(0); // UNKNOWN upstream governing context.
        jazz_types::sha256::derive_key("jazz.sqlite.observation-submission-scope.v1", &context)
            .to_vec()
    };
    let tx = OpenTransactionId::new();
    block_on(local.begin_mergeable(tx)).unwrap();
    local.set_identity_claims(AuthorSubject::SYSTEM, claims("A"));
    block_on(
        local
            .tx_ref(tx)
            .insert("notes", body("body", "A"), Default::default()),
    )
    .unwrap();
    local.set_identity_claims(AuthorSubject::SYSTEM, claims("B"));
    block_on(
        local
            .tx_ref(tx)
            .insert("notes", body("body", "B"), Default::default()),
    )
    .unwrap();
    local.set_identity_claims(AuthorSubject::SYSTEM, claims("C"));
    let receipt = block_on(local.commit_mergeable_handle(tx)).unwrap();
    let disk = Connection::open(&path).unwrap();
    let pending = submissions(&disk);
    assert_eq!(pending.len(), 2);
    for (index, phase) in [b'A', b'B'].into_iter().enumerate() {
        assert_eq!(pending[index].0, index as i64);
        assert_eq!(pending[index].1, schema.version_id().as_bytes());
        assert_eq!(pending[index].2, expected_scope(phase));
        assert_ne!(pending[index].2, expected_scope(b'C'));
    }
    drive_local(&[&authority, &local]);
    block_on(local.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global)).unwrap();
    assert!(submissions(&disk).is_empty());
    assert_eq!(
        disk.query_row(
            "SELECT count(*) FROM _jazz_observation_evidence_v1",
            [],
            |r| { r.get::<_, i64>(0) }
        )
        .unwrap(),
        2
    );
    block_on(local.close()).unwrap();
    drop(local);
    let reopened = open(&path, schema.clone());
    for phase in ["A", "B", "C"] {
        reopened.set_identity_claims(AuthorSubject::SYSTEM, claims(phase));
        for original in ["A", "B"] {
            let query = reopened
                .prepare_query(
                    &Query::from("notes")
                        .filter(eq(col("body"), lit(Value::String(original.into())))),
                )
                .unwrap();
            assert_eq!(reopened.read(&query).unwrap().len(), 1);
            let cut = reopened.local_observation_cut(&query, None).unwrap();
            let result = reopened.observe_local(&cut, &query, false);
            if phase == original {
                let observed = result.unwrap();
                assert_eq!(observed.rows.len(), 1);
                assert_eq!(
                    observed.rows[0].cell("body"),
                    Some(Value::String(original.into()))
                );
                assert_eq!(
                    observed.provenance,
                    vec![LocalEvidenceProvenance::LocalSubmission]
                );
            } else {
                assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
            }
        }
    }
}

/// Alice rolls back an authority evidence admission inside a write callback.
/// Its cut must refuse capture before Bob's later admission reuses the number.
/// retain A + attempt cut -> callback rollback -> retain B -> committed cut
#[test]
fn callback_rollback_cannot_export_a_cut_for_a_reused_admission() {
    use jazz::db::{Error, ErrorCode};
    use jazz::query::{col, eq, lit};
    use std::cell::RefCell;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("callback-cut.sqlite");
    let db = block_on(Db::open_at(
        observation_config(schema("notes", "body"), 1),
        &path,
        Default::default(),
    ))
    .unwrap();
    db.set_authority();
    for value in ["A", "B"] {
        block_on(db.insert("notes", body("body", value), Default::default())).unwrap();
    }
    let query = |value: &str| {
        db.prepare_query(
            &Query::from("notes").filter(eq(col("body"), lit(Value::String(value.into())))),
        )
        .unwrap()
    };
    let a = query("A");
    let b = query("B");
    // Persist the authority namespace before the failing callback, so its
    // rollback cannot mask admission reuse by invalidating the cut's identity.
    let stable_identity = db.table_identity("notes").unwrap();
    assert!(stable_identity.is_some());
    let capture = RefCell::new(None);
    let failed = block_on(db.transaction(async |tx| {
        tx.insert("notes", body("body", "rolled back"), Default::default())
            .await?;
        let retained = db.retain_local_authority_read(&a, false, None)?;
        assert_eq!(retained.rows.len(), 1);
        capture.replace(Some(db.local_observation_cut(&a, None)));
        Err::<(), _>(Error::new(ErrorCode::Query, "abort callback after capture"))
    }));
    assert_eq!(failed.unwrap_err().message, "abort callback after capture");
    assert!(db.read(&query("rolled back")).unwrap().is_empty());
    assert_eq!(db.read(&a).unwrap().len(), 1);
    assert_eq!(db.read(&b).unwrap().len(), 1);
    assert_eq!(db.table_identity("notes").unwrap(), stable_identity);
    // The failed callback created the table and its first admission inside
    // its savepoint. Both must roll back; B then reuses admission number 1.
    let disk = Connection::open(&path).unwrap();
    assert!(!disk.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='_jazz_observation_evidence_v1')",
        [],
        |r| r.get::<_, bool>(0),
    ).unwrap());
    db.retain_local_authority_read(&b, false, None).unwrap();
    assert_eq!(db.table_identity("notes").unwrap(), stable_identity);
    assert_eq!(
        disk.query_row(
            "SELECT admission FROM _jazz_observation_evidence_v1",
            [],
            |r| { r.get::<_, i64>(0) }
        )
        .unwrap(),
        1
    );
    // Delay this assertion until after the real rollback and number reuse.
    // On the broken implementation an escaped cut can now observe B.
    match capture.into_inner().expect("callback attempted capture") {
        Err(error) => assert_eq!(error.code, ErrorCode::NotObserved),
        Ok(escaped) => {
            assert_eq!(
                db.observe_local(&escaped, &b, false)
                    .expect_err("escaped cut admitted evidence created after rollback")
                    .code,
                ErrorCode::NotObserved
            );
            panic!("capture inside a callback must explicitly return NotObserved");
        }
    }
    let committed = db.local_observation_cut(&b, None).unwrap();
    let observed = db.observe_local(&committed, &b, false).unwrap();
    assert_eq!(observed.rows.len(), 1);
    assert_eq!(
        observed.rows[0].cell("body"),
        Some(Value::String("B".into()))
    );
    assert_eq!(
        db.observe_local(&committed, &a, false).unwrap_err().code,
        ErrorCode::NotObserved
    );
}

/// Alice may commit an ordinary callback successfully, but cannot export its
/// still-uncommitted evidence watermark. Capture succeeds after that commit.
#[test]
fn successful_callback_refuses_cut_until_evidence_is_committed() {
    use jazz::db::ErrorCode;
    use jazz::query::{col, eq, lit};

    let db = block_on(Db::open_history_complete(observation_config(
        schema("notes", "body"),
        1,
    )))
    .unwrap();
    block_on(db.insert("notes", body("body", "accepted"), Default::default())).unwrap();
    let query = db
        .prepare_query(
            &Query::from("notes").filter(eq(col("body"), lit(Value::String("accepted".into())))),
        )
        .unwrap();
    block_on(db.transaction(async |tx| {
        tx.insert("notes", body("body", "committed"), Default::default())
            .await?;
        db.retain_local_authority_read(&query, false, None)?;
        let error = db
            .local_observation_cut(&query, None)
            .err()
            .expect("an active successful callback must refuse capture");
        assert_eq!(error.code, ErrorCode::NotObserved);
        Ok(())
    }))
    .unwrap();
    assert_eq!(
        db.read(&db.prepare_query(&Query::from("notes")).unwrap())
            .unwrap()
            .len(),
        2
    );
    let cut = db.local_observation_cut(&query, None).unwrap();
    assert_eq!(db.observe_local(&cut, &query, false).unwrap().rows.len(), 1);
}

/// Alice's unsupported stage scope remains unavailable after valid claims are
/// registered before commit. Her ordinary write is still admitted normally.
#[test]
fn unsupported_staged_scope_is_not_reconstructed_at_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unsupported-stage.sqlite");
    let db = open(&path, schema("notes", "body"));
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    db.set_identity_claims(
        AuthorSubject::SYSTEM,
        [(
            "unsupported".into(),
            Value::Record(OwnedRecord::new(
                Vec::new(),
                RecordDescriptor::new(Vec::<(String, jazz::records::ValueType)>::new()),
            )),
        )]
        .into(),
    );
    block_on(
        db.tx_ref(tx)
            .insert("notes", body("body", "ordinary"), Default::default()),
    )
    .unwrap();
    db.set_identity_claims(AuthorSubject::SYSTEM, Default::default());
    block_on(db.commit_mergeable_handle(tx)).unwrap();
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(db.read(&query).unwrap().len(), 1);
    let disk = Connection::open(&path).unwrap();
    assert_eq!(
        disk.query_row("SELECT count(*) FROM _jazz_pending", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert!(!disk.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='_jazz_observation_pending_v1')", [], |r| r.get::<_, bool>(0)).unwrap());
}
