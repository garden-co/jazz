//! Serializable union reads retain semantic occurrences through local and wire validation.
mod common;
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
use jazz::tx::Fate;
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
        false,
        || false,
        |_| {},
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
    block_on(db.serializable_tx_ref(tx).insert(
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
        let fate = client.write_state(receipt).unwrap().fate;
        if changed {
            assert!(matches!(fate, Fate::Rejected(_)));
            assert_eq!(count_checks(&server), 0);
        } else {
            assert_eq!(fate, Fate::Accepted);
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
            && let Some(mut body) = kind.remove("SerializableUnionObservedV1")
        {
            self.changed.set(self.changed.get() + 1);
            let role = match self.mode {
                "missing read" => {
                    body["content"].as_array_mut().unwrap().clear();
                    "SerializableUnionObservedV1"
                }
                "missing row" => {
                    body["content"][0].as_array_mut().unwrap().pop();
                    "SerializableUnionObservedV1"
                }
                "alias arm" => {
                    body["content"][0][1] = body["content"][0][0].clone();
                    "SerializableUnionObservedV1"
                }
                "observed v1" => "SerializableObservedV1",
                "origins v1" => {
                    body.as_object_mut().unwrap().remove("content");
                    "SerializableOriginsV1"
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
        "missing read",
        "missing row",
        "alias arm",
        "observed v1",
        "origins v1",
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
            matches!(client.write_state(receipt).unwrap().fate, Fate::Rejected(_)),
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
        let fate = client.write_state(receipt).unwrap().fate;
        if changed {
            assert!(matches!(fate, Fate::Rejected(_)));
            assert_eq!(count_checks(&server), 0);
        } else {
            assert_eq!(fate, Fate::Accepted);
            assert_eq!(count_checks(&server), 1);
        }
    }
}
/// Alice's real union upload uses independent exact preimage fixtures for both
/// arms. Low-level byte inspection is required to pin the new persisted wire
/// kind and prove that the existing ordinary-read encoding is not repurposed.
#[test]
fn union_read_content_and_wire_kind_match_versioned_fixtures() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v2/union-read-content.json"
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
    let commit = std::iter::from_fn(|| peer.try_recv())
        .map(|m| serde_json::to_value(m).unwrap())
        .find(|m| {
            m["Commit"]["kind"]
                .get("SerializableUnionObservedV1")
                .is_some()
        })
        .unwrap();
    assert_eq!(
        fixture["wireRole"],
        "jazz.sqlite.serializable-union-observed.v1"
    );
    assert_eq!(
        fixture["contentRole"],
        "jazz.sqlite.serializable-union-read-content.v1"
    );
    let expected: Vec<Vec<u8>> = fixture["preimagesHex"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            blake3::derive_key(
                fixture["contentRole"].as_str().unwrap(),
                &hex::decode(h.as_str().unwrap()).unwrap(),
            )
            .to_vec()
        })
        .collect();
    assert_ne!(expected[0], expected[1]);
    let ordinary: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite_owner_read_content_v1.json"
    ))
    .unwrap();
    let ordinary_digest = blake3::derive_key(
        ordinary["contentRole"].as_str().unwrap(),
        &hex::decode(ordinary["preimageHex"].as_str().unwrap()).unwrap(),
    );
    assert_eq!(
        commit["Commit"]["kind"]["SerializableUnionObservedV1"]["content"],
        serde_json::json!([[ordinary_digest.to_vec()], expected])
    );
    let message:SyncMessage=serde_json::from_value(serde_json::json!({"Commit":{"node":vec![0x22u8;16],"tx":128,"kind":{"SerializableUnionObservedV1":{"snapshot":127,"reads":[],"own":[[(1u64<<52)+1,vec![0x11u8;16],300]],"content":[]}},"ops":[],"author":null,"session":null,"expect":[]}})).unwrap();
    let bytes = hex::decode(fixture["commitHex"].as_str().unwrap()).unwrap();
    assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
    let decoded = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(message).unwrap()
    );
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
        block_on(db.serializable_tx_ref(tx).update(
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
