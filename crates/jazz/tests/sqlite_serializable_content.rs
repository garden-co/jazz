//! Serializable union reads retain semantic occurrences through local and wire validation.
use crate::common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier,
    RemoteLinkHint, SerializedReadResult, SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{
    Query, RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinCondition, RelationJoinKind,
    RelationPredicate, RelationProjectColumn, RelationProjectExpr, RelationQuery, RelationRowIdRef,
    RelationUnionArm, RelationValueRef,
};
use jazz::records::Value;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchema, Value as Literal};
use jazz::tx::DurabilityTier;
use std::{cell::Cell, collections::BTreeMap, rc::Rc};
fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://union.example", "alice").unwrap()
}
fn config(node: u8) -> DbConfig {
    DbConfig::new(
        common::compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchema::builder("counters")
                        .column("value", ColumnType::Integer)
                        .policies(common::allow_all_policies()),
                )
                .table(
                    TableSchema::builder("flags")
                        .column("counter", ColumnType::Uuid)
                        .column("enabled", ColumnType::Boolean)
                        .index_only(["counter"])
                        .policies(common::allow_all_policies()),
                )
                .table(
                    TableSchema::builder("checks")
                        .column("value", ColumnType::Integer)
                        .policies(common::allow_all_policies()),
                )
                .build(),
        ),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )
}
fn authority() -> Db {
    let db = block_on(Db::open_history_complete(config(90))).unwrap();
    db.set_authority();
    db.initialize_authority_catalogue().unwrap();
    db
}
fn counter() -> RowUuid {
    RowUuid::from_bytes([7; 16])
}
fn flag() -> RowUuid {
    RowUuid::from_bytes([8; 16])
}
fn seed(db: &Db) {
    block_on(db.insert(
        "counters",
        BTreeMap::from([("value".into(), Value::I32(2))]),
        InsertOptions {
            row_id: Some(counter()),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(db.insert(
        "flags",
        BTreeMap::from([
            ("counter".into(), Value::Uuid(counter().0)),
            ("enabled".into(), Value::Bool(false)),
        ]),
        InsertOptions {
            row_id: Some(flag()),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn column(scope: &str, name: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(scope.into()),
        column: name.into(),
    }
}
fn arm(gate: Option<bool>) -> RelationExpr {
    let mut input = RelationExpr::TableScan {
        table: "counters".into(),
        alias: None,
    };
    if let Some(enabled) = gate {
        input = RelationExpr::Join {
            left: Box::new(input),
            right: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "flags".into(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: column("flags", "enabled"),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::literal(Literal::Boolean(enabled)),
                },
            }),
            on: vec![RelationJoinCondition {
                left: column("counters", "id"),
                right: column("flags", "counter"),
            }],
            join_kind: RelationJoinKind::Inner,
        };
    }
    RelationExpr::Project {
        input: Box::new(input),
        columns: vec![
            RelationProjectColumn {
                alias: "id".into(),
                expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
            },
            RelationProjectColumn {
                alias: "value".into(),
                expr: RelationProjectExpr::Column(column("counters", "value")),
            },
        ],
    }
}
fn union(gated: bool) -> Query {
    let mut query = Query::from("counters");
    query.relation = Some(RelationQuery {
        rel: RelationExpr::Union {
            inputs: vec![
                RelationUnionArm {
                    label: "a".into(),
                    input: arm(gated.then_some(false)),
                },
                RelationUnionArm {
                    label: "b".into(),
                    input: arm(gated.then_some(true)),
                },
            ],
        },
    });
    query
}
fn begin(db: &Db) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    tx
}
fn read(db: &Db, tx: OpenTransactionId, query: &Query) -> Vec<CurrentRow> {
    let result = block_on(db.all_serialized_query(
        &postcard::to_allocvec(query).unwrap(),
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        Some(tx),
        None,
        None,
    ))
    .unwrap();
    let SerializedReadResult::Rows(rows) = result else {
        panic!("flat union rows")
    };
    rows
}
fn switch_arm(db: &Db) {
    block_on(db.update(
        "flags",
        flag(),
        BTreeMap::from([("enabled".into(), Value::Bool(true))]),
        Default::default(),
    ))
    .unwrap();
}
fn stage_check(db: &Db, tx: OpenTransactionId) {
    block_on(db.tx_ref(tx).insert(
        "checks",
        BTreeMap::from([("value".into(), Value::I32(1))]),
        Default::default(),
    ))
    .unwrap();
}
fn count_checks(db: &Db) -> usize {
    block_on(db.all(
        &db.prepare_query(&Query::from("checks")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap()
    .len()
}
fn drive(dbs: &[&Db]) {
    for _ in 0..24 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}
fn settle(nodes: &[&Db], client: &Db, tx: u64) -> Result<u64, jazz::db::Error> {
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };
    let mut wait = pin!(client.wait_for_transaction(tx, DurabilityTier::Global));
    for _ in 0..200 {
        if let Poll::Ready(result) = wait.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            return result;
        }
        for node in nodes {
            block_on(node.tick()).unwrap();
        }
    }
    panic!("write did not settle");
}
fn connect(client: &Db, server: &Db) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    server.accept_subscriber(b, alice());
    client.set_remote_link_hint(RemoteLinkHint::Live);
}
fn hydrated(server: &Db) -> Db {
    let client = block_on(Db::open(config(1))).unwrap();
    connect(&client, server);
    let streams = ["counters", "flags"].map(|table| {
        block_on(client.subscribe(
            &client.prepare_query(&Query::from(table)).unwrap(),
            ReadOpts::default(),
        ))
        .unwrap()
    });
    drive(&[&client, server]);
    drop(streams);
    client.set_remote_link_hint(RemoteLinkHint::Failed);
    client
}
/// Alice reads duplicate occurrences and commits normally. Bob then switches
/// only their supporting flag: unchanged root IDs, cells and versions must
/// still conflict when the surviving occurrence moves from arm a to arm b.
#[test]
fn local_commits_compare_occurrences_and_keep_ordinary_reads_unchanged() {
    let db = authority();
    seed(&db);
    let tx = begin(&db);
    assert_eq!(read(&db, tx, &union(false)).len(), 2);
    stage_check(&db, tx);
    block_on(db.commit_serializable_handle(tx)).unwrap();
    assert_eq!(count_checks(&db), 1);
    let tx = begin(&db);
    let before = read(&db, tx, &union(true));
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].union_arm(), Some("a"));
    stage_check(&db, tx);
    switch_arm(&db);
    let after = block_on(db.all(
        &db.prepare_query(&union(true)).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    assert_eq!(after[0].row_uuid(), before[0].row_uuid());
    assert_eq!(after[0].seq(), before[0].seq());
    assert_eq!(after[0].cell_at(0), before[0].cell_at(0));
    assert_eq!(after[0].union_arm(), Some("b"));
    assert_eq!(
        block_on(db.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
    assert_eq!(count_checks(&db), 1);
}
/// Alice observes an empty union. Bob inserts a matching root before commit;
/// the query is re-evaluated so an empty read also protects against phantoms.
#[test]
fn empty_union_reads_detect_phantoms() {
    let db = authority();
    let tx = begin(&db);
    assert!(read(&db, tx, &union(false)).is_empty());
    stage_check(&db, tx);
    seed(&db);
    assert_eq!(
        block_on(db.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
    assert_eq!(count_checks(&db), 0);
}
/// Alice's offline replica commits a union read, then sends it to Bob. An
/// unchanged authority accepts; a flag-only occurrence change rejects the
/// entire transaction even though every root version and cell still matches.
#[test]
fn authority_validates_offline_union_occurrences_atomically() {
    for changed in [false, true] {
        let server = authority();
        seed(&server);
        let client = hydrated(&server);
        let tx = begin(&client);
        assert_eq!(read(&client, tx, &union(true))[0].union_arm(), Some("a"));
        stage_check(&client, tx);
        let receipt = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        if changed {
            switch_arm(&server);
        }
        client.set_remote_link_hint(RemoteLinkHint::Live);
        drive(&[&client, &server]);
        let fate = settle(&[&client, &server], &client, receipt);
        if changed {
            common::assert_serialization_conflict(&fate.unwrap_err());
            assert_eq!(count_checks(&server), 0);
        } else {
            fate.unwrap();
            assert_eq!(count_checks(&server), 1);
        }
    }
}

struct Tamper {
    link: Box<dyn Transport>,
    mode: &'static str,
    changed: Rc<Cell<usize>>,
}
impl Transport for Tamper {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let mut json = serde_json::to_value(message).unwrap();
        if let Some(kind) = json
            .get_mut("Commit")
            .and_then(|commit| commit.get_mut("kind"))
            .and_then(|kind| kind.as_object_mut())
            && let Some(mut body) = kind.remove("SerializableContentV1")
        {
            self.changed.set(self.changed.get() + 1);
            let role = match self.mode {
                "missing query" => {
                    body["reads"].as_array_mut().unwrap().clear();
                    "SerializableContentV1"
                }
                "missing read" => {
                    body["content"].as_array_mut().unwrap().clear();
                    "SerializableContentV1"
                }
                "missing row" => {
                    body["content"][0].as_array_mut().unwrap().pop();
                    "SerializableContentV1"
                }
                "alias arm" => {
                    body["content"][0][1] = body["content"][0][0].clone();
                    "SerializableContentV1"
                }
                "legacy" => {
                    body.as_object_mut().unwrap().remove("content");
                    assert!(body["own"].as_array().unwrap().is_empty());
                    "Serializable"
                }
                _ => panic!("unknown tamper mode"),
            };
            kind.insert(role.into(), body);
        }
        self.link.send(serde_json::from_value(json).unwrap())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.link.try_recv()
    }
}
/// Mallory strips an occurrence, aliases two duplicate roots' evidence, or
/// downgrades Alice's real upload to an older kind. Bob rejects all of them;
/// no staged companion write survives. Wire mutation is needed to test admission.
#[test]
fn malformed_or_downgraded_union_evidence_is_rejected() {
    for mode in [
        "missing query",
        "missing read",
        "missing row",
        "alias arm",
        "legacy",
    ] {
        let server = authority();
        seed(&server);
        let client = hydrated(&server);
        let tx = begin(&client);
        assert_eq!(read(&client, tx, &union(false)).len(), 2);
        stage_check(&client, tx);
        let receipt = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        let changed = Rc::new(Cell::new(0));
        let (a, b) = duplex_transport::duplex();
        block_on(client.connect_upstream(Box::new(Tamper {
            link: a,
            mode,
            changed: changed.clone(),
        })));
        server.accept_subscriber(b, alice());
        client.set_remote_link_hint(RemoteLinkHint::Live);
        drive(&[&client, &server]);
        assert!(changed.get() > 0, "{mode}: no real union upload inspected");
        assert!(
            settle(&[&client, &server], &client, receipt).is_err(),
            "{mode}"
        );
        assert_eq!(count_checks(&server), 0, "{mode}");
    }
}
/// Alice queues a union transaction offline and closes her persistent replica.
/// Reopening preserves both occurrence evidence and original dependencies; Bob
/// still distinguishes the unchanged case from a flag-only arm change.
#[test]
fn pending_union_read_evidence_survives_reopen() {
    for changed in [false, true] {
        let server = authority();
        seed(&server);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.db");
        let receipt = {
            let client = block_on(Db::open_at(config(1), &path, Default::default())).unwrap();
            connect(&client, &server);
            let streams = ["counters", "flags"].map(|table| {
                block_on(client.subscribe(
                    &client.prepare_query(&Query::from(table)).unwrap(),
                    ReadOpts::default(),
                ))
                .unwrap()
            });
            drive(&[&client, &server]);
            drop(streams);
            client.set_remote_link_hint(RemoteLinkHint::Failed);
            let tx = begin(&client);
            assert_eq!(read(&client, tx, &union(true)).len(), 1);
            stage_check(&client, tx);
            block_on(client.commit_serializable_handle(tx)).unwrap().0
        };
        if changed {
            switch_arm(&server);
        }
        let client = block_on(Db::open_at(config(1), &path, Default::default())).unwrap();
        connect(&client, &server);
        drive(&[&client, &server]);
        let fate = settle(&[&client, &server], &client, receipt);
        if changed {
            common::assert_serialization_conflict(&fate.unwrap_err());
            assert_eq!(count_checks(&server), 0);
        } else {
            fate.unwrap();
            assert_eq!(count_checks(&server), 1);
        }
    }
}
/// Alice's union window sees her staged writes and stays at its begin snapshot.
/// Bob's later insertion leaves that snapshot readable but conflicts at commit;
/// without Bob's insertion, Alice's companion write and staged update commit.
#[test]
fn union_windows_read_staged_prefixes_and_pin_the_snapshot() {
    use jazz::query::{OrderDirection, RelationOrderBy};
    for changed in [false, true] {
        let db = authority();
        seed(&db);
        block_on(db.insert(
            "counters",
            BTreeMap::from([("value".into(), Value::I32(3))]),
            Default::default(),
        ))
        .unwrap();
        let mut query = union(false);
        let rel = query.relation.take().unwrap().rel;
        query.relation = Some(RelationQuery {
            rel: RelationExpr::Limit {
                input: Box::new(RelationExpr::Offset {
                    input: Box::new(RelationExpr::OrderBy {
                        input: Box::new(rel),
                        terms: vec![RelationOrderBy {
                            column: column("counters", "value"),
                            direction: OrderDirection::Asc,
                        }],
                    }),
                    offset: 1,
                }),
                limit: 2,
            },
        });
        let tx = begin(&db);
        block_on(db.tx_ref(tx).update(
            "counters",
            counter(),
            BTreeMap::from([("value".into(), Value::I32(5))]),
            Default::default(),
        ))
        .unwrap();
        let rows = read(&db, tx, &query);
        assert_eq!(
            rows.iter().map(|row| row.cell_at(0)).collect::<Vec<_>>(),
            [Some(Value::I32(3)), Some(Value::I32(5))]
        );
        if changed {
            block_on(db.insert(
                "counters",
                BTreeMap::from([("value".into(), Value::I32(0))]),
                Default::default(),
            ))
            .unwrap();
        }
        // Replaying the staged prefix can allocate a different temporary
        // publication alias; logical rows and occurrences stay at the snapshot.
        let visible = |rows: Vec<CurrentRow>| {
            rows.into_iter()
                .map(|row| {
                    (
                        row.row_uuid(),
                        row.cell_at(0),
                        row.union_arm().map(str::to_owned),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(visible(read(&db, tx, &query)), visible(rows));
        stage_check(&db, tx);
        let result = block_on(db.commit_serializable_handle(tx));
        if changed {
            assert_eq!(result.unwrap_err().code, ErrorCode::TransactionConflict);
            assert_eq!(count_checks(&db), 0);
        } else {
            result.unwrap();
            assert_eq!(count_checks(&db), 1);
        }
    }
}

/// Alice's real upload retains the independently pinned plain and semantic
/// UNION content preimages. A separate field-built frame pins the new kind,
/// including its nested content vector, without using serializer output.
#[test]
fn content_and_wire_kind_match_independent_fixtures() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite_serializable_content_v1.json"
    ))
    .unwrap();
    let client = block_on(Db::open(config(1))).unwrap();
    client.set_remote_link_hint(RemoteLinkHint::Failed);
    seed(&client);
    let tx = begin(&client);
    assert_eq!(read(&client, tx, &Query::from("counters")).len(), 1);
    assert_eq!(read(&client, tx, &union(false)).len(), 2);
    block_on(client.commit_serializable_handle(tx)).unwrap();
    let (a, mut peer) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    block_on(client.tick()).unwrap();
    let content = std::iter::from_fn(|| peer.try_recv())
        .find_map(|message| match message {
            SyncMessage::Commit {
                kind: jazz::db::WireKind::SerializableContentV1 { content, .. },
                ..
            } => Some(content),
            _ => None,
        })
        .expect("real content-bearing commit");
    let plain = blake3::derive_key(
        fixture["plainRole"].as_str().unwrap(),
        &hex::decode(fixture["plainPreimageHex"].as_str().unwrap()).unwrap(),
    );
    let union: Vec<_> = fixture["unionPreimagesHex"]
        .as_array()
        .unwrap()
        .iter()
        .map(|preimage| {
            blake3::derive_key(
                fixture["unionRole"].as_str().unwrap(),
                &hex::decode(preimage.as_str().unwrap()).unwrap(),
            )
        })
        .collect();
    assert_ne!(union[0], union[1]);
    assert_eq!(content, [vec![plain], union]);
    let message: SyncMessage = serde_json::from_value(serde_json::json!({"Commit": {
        "node": vec![0x22u8;16], "tx": 128,
        "kind": {"SerializableContentV1": {
            "snapshot": 127,
            "reads": [{"query": Query::from("notes"), "binding": {}, "include_deleted": false, "rows": [[vec![0x33u8;16],128]]}],
            "own": [jazz::db::LOCAL_BASE + 1], "content": [[vec![0x55u8;32]]]
        }},
        "ops": [], "author": null, "session": null, "expect": [], "bases": [], "origin": null
    }})).unwrap();
    let bytes = hex::decode(fixture["commitHex"].as_str().unwrap()).unwrap();
    assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
    assert_eq!(
        serde_json::to_value(jazz::db::decode_message(&bytes).unwrap()).unwrap(),
        serde_json::to_value(message).unwrap()
    );
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(jazz::db::decode_message(&trailing).is_err());
    for end in 0..bytes.len() {
        assert!(
            jazz::db::decode_message(&bytes[..end]).is_err(),
            "truncated at {end}"
        );
    }
}

fn counter_config(node: u8) -> DbConfig {
    use jazz::tools::{ColumnDescriptor, ColumnMergeStrategy, RowDescriptor, Schema, TableName};
    let table = TableSchema::with_policies(
        RowDescriptor::new(vec![
            ColumnDescriptor::new("value", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ]),
        common::allow_all_policies(),
    );
    let checks = TableSchema::builder("checks")
        .column("value", ColumnType::Integer)
        .policies(common::allow_all_policies())
        .build();
    DbConfig::new(
        common::compile_schema(&Schema::from([
            (TableName::new("counters"), table),
            (TableName::new("checks"), checks),
        ])),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )
}
fn delta(db: &Db, amount: i32) {
    block_on(db.update(
        "counters",
        counter(),
        BTreeMap::from([("value".into(), Value::I32(amount))]),
        Default::default(),
    ))
    .unwrap();
}
fn counter_row(db: &Db) -> CurrentRow {
    db.read(&db.prepare_query(&Query::from("counters")).unwrap())
        .unwrap()
        .remove(0)
}
fn hydrate_counter(client: &Db, server: &Db) {
    connect(client, server);
    let stream = block_on(client.subscribe(
        &client.prepare_query(&Query::from("counters")).unwrap(),
        ReadOpts::for_read_tier(ReadTier::Remote),
    ))
    .unwrap();
    drive(&[client, server]);
    assert_eq!(counter_row(client).cell_at(0), Some(Value::I32(0)));
    drop(stream);
    client.set_remote_link_hint(RemoteLinkHint::Failed);
}
fn seed_counter(server: &Db) {
    block_on(server.insert(
        "counters",
        BTreeMap::from([("value".into(), Value::I32(0))]),
        InsertOptions {
            row_id: Some(counter()),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Bob's new accepted base arrives beneath Alice's pending counter delta. Its
/// numeric pending version stays the same, but the changed value invalidates
/// her earlier read and prevents a companion write from committing locally.
#[test]
fn changed_counter_replay_conflicts_at_the_same_pending_version() {
    let server = block_on(Db::open(counter_config(90))).unwrap();
    server.set_authority();
    seed_counter(&server);
    let client = block_on(Db::open(counter_config(1))).unwrap();
    hydrate_counter(&client, &server);
    delta(&client, 2);
    let tx = begin(&client);
    assert_eq!(
        read(&client, tx, &Query::from("counters"))[0].cell_at(0),
        Some(Value::I32(2))
    );
    let before = counter_row(&client).seq();
    stage_check(&client, tx);
    delta(&server, 3);
    client.set_remote_link_hint(RemoteLinkHint::Live);
    let _stream = block_on(client.subscribe(
        &client.prepare_query(&Query::from("counters")).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap();
    // Deliver the changed base before the pending upload is sent.
    block_on(server.tick()).unwrap();
    block_on(client.tick()).unwrap();
    assert_eq!(counter_row(&client).cell_at(0), Some(Value::I32(5)));
    assert_eq!(counter_row(&client).seq(), before);
    assert_eq!(
        block_on(client.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
    assert_eq!(count_checks(&client), 0);
}

/// Alice's offline counter delta keeps its identity when replayed over Bob's
/// newer base. Direct and relayed authorities compare its changed logical
/// value, rejecting the subsequent read-only decision; unchanged replay passes.
#[test]
fn authority_checks_counter_content_through_direct_and_relay_uploads() {
    for relayed in [false, true] {
        for changed in [false, true] {
            let server = block_on(Db::open(counter_config(90))).unwrap();
            server.set_authority();
            seed_counter(&server);
            let relay = block_on(Db::open(counter_config(2))).unwrap();
            let client = block_on(Db::open(counter_config(1))).unwrap();
            if relayed {
                hydrate_counter(&relay, &server);
                relay.set_remote_link_hint(RemoteLinkHint::Live);
                connect(&client, &relay);
                let _stream = block_on(client.subscribe(
                    &client.prepare_query(&Query::from("counters")).unwrap(),
                    ReadOpts::for_read_tier(ReadTier::Remote),
                ))
                .unwrap();
                drive(&[&client, &relay, &server]);
                assert_eq!(counter_row(&client).cell_at(0), Some(Value::I32(0)));
                client.set_remote_link_hint(RemoteLinkHint::Failed);
            } else {
                hydrate_counter(&client, &server);
            }
            delta(&client, 2);
            let tx = begin(&client);
            assert_eq!(
                read(&client, tx, &Query::from("counters"))[0].cell_at(0),
                Some(Value::I32(2))
            );
            if changed {
                delta(&server, 3);
            }
            let receipt = block_on(client.commit_serializable_handle(tx)).unwrap().0;
            client.set_remote_link_hint(RemoteLinkHint::Live);
            let result = settle(&[&client, &relay, &server], &client, receipt);
            assert_eq!(
                counter_row(&server).cell_at(0),
                Some(Value::I32(if changed { 5 } else { 2 }))
            );
            if changed {
                common::assert_serialization_conflict(&result.unwrap_err());
            } else {
                result.unwrap();
            }
        }
    }
}

/// Alice closes her ordinary SQLite replica with both a pending delta and its
/// read-only decision. Reopening preserves exact content evidence; Bob's later
/// base change still conflicts, while an unchanged base reaches Global.
#[test]
fn counter_content_survives_pending_upload_reopen() {
    for changed in [false, true] {
        let server = block_on(Db::open(counter_config(90))).unwrap();
        server.set_authority();
        seed_counter(&server);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("alice.sqlite");
        let receipt = {
            let client =
                block_on(Db::open_at(counter_config(1), &path, Default::default())).unwrap();
            hydrate_counter(&client, &server);
            delta(&client, 2);
            let tx = begin(&client);
            assert_eq!(
                read(&client, tx, &Query::from("counters"))[0].cell_at(0),
                Some(Value::I32(2))
            );
            block_on(client.commit_serializable_handle(tx)).unwrap().0
        };
        if changed {
            delta(&server, 3);
        }
        let client = block_on(Db::open_at(counter_config(1), &path, Default::default())).unwrap();
        connect(&client, &server);
        let result = settle(&[&client, &server], &client, receipt);
        if changed {
            common::assert_serialization_conflict(&result.unwrap_err());
        } else {
            result.unwrap();
        }
    }
}

/// Alice sends a retained UNION through an ordinary worker. Bob's flag change
/// swaps its arm without changing its root version; the worker must carry the
/// original digest unchanged so the authority rejects the companion write.
#[test]
fn relay_preserves_original_union_content_evidence() {
    for changed in [false, true] {
        let server = authority();
        seed(&server);
        let relay = block_on(Db::open(config(2))).unwrap();
        connect(&relay, &server);
        let client = block_on(Db::open(config(1))).unwrap();
        connect(&client, &relay);
        let _streams = ["counters", "flags"].map(|table| {
            block_on(client.subscribe(
                &client.prepare_query(&Query::from(table)).unwrap(),
                ReadOpts::default(),
            ))
            .unwrap()
        });
        drive(&[&client, &relay, &server]);
        client.set_remote_link_hint(RemoteLinkHint::Failed);
        let tx = begin(&client);
        assert_eq!(read(&client, tx, &union(true))[0].union_arm(), Some("a"));
        stage_check(&client, tx);
        let receipt = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        if changed {
            switch_arm(&server);
        }
        client.set_remote_link_hint(RemoteLinkHint::Live);
        let result = settle(&[&client, &relay, &server], &client, receipt);
        if changed {
            common::assert_serialization_conflict(&result.unwrap_err());
            assert_eq!(count_checks(&server), 0);
        } else {
            result.unwrap();
            assert_eq!(count_checks(&server), 1);
        }
    }
}

/// Mallory removes Alice's query/content entries or downgrades the kind before
/// it reaches the worker. The relay rejects the malformed evidence before
/// persisting it, and no companion write reaches the authority.
/// Wire mutation is needed to test validation at the intermediate host.
#[test]
fn relays_refuse_malformed_evidence_before_forwarding() {
    for mode in ["missing query", "missing read", "legacy"] {
        let server = authority();
        seed(&server);
        let relay = block_on(Db::open(config(2))).unwrap();
        connect(&relay, &server);
        let client = hydrated(&server);
        let tx = begin(&client);
        assert_eq!(read(&client, tx, &union(false)).len(), 2);
        stage_check(&client, tx);
        let receipt = block_on(client.commit_serializable_handle(tx)).unwrap().0;
        let changed = Rc::new(Cell::new(0));
        let (a, b) = duplex_transport::duplex();
        block_on(client.connect_upstream(Box::new(Tamper {
            link: a,
            mode,
            changed: changed.clone(),
        })));
        relay.accept_subscriber(b, alice());
        client.set_remote_link_hint(RemoteLinkHint::Live);
        assert!(
            settle(&[&client, &relay, &server], &client, receipt).is_err(),
            "{mode}"
        );
        assert!(changed.get() > 0);
        assert_eq!(
            count_checks(&relay),
            0,
            "{mode}: relay staged malformed write"
        );
        assert_eq!(
            count_checks(&server),
            0,
            "{mode}: authority applied malformed write"
        );
    }
}
