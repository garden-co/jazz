//! Accepted UNION pages pair semantic occurrences with authority content evidence.
use jazz::db::{
    BindingSettledRead, Db, DbConfig, DbIdentity, Error, ErrorCode, InsertOptions, ReadOpts,
    ReadTier, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{
    OrderDirection, Query, RelationColumnRef, RelationExpr, RelationJoinCondition,
    RelationJoinKind, RelationOrderBy, RelationProjectColumn, RelationProjectExpr, RelationQuery,
    RelationRowIdRef, RelationUnionArm, provider_claim_key,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://accepted-union.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn claims(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("code"), Value::String(code.into()))])
}
fn open(n: u8, authority: bool, ordinary: bool) -> Db {
    let scoped = PolicyExpr::eq_session("code", vec!["claims".into(), "code".into()]);
    let policies = |read| {
        TablePolicies::new()
            .with_select(read)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    };
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("code", ColumnType::Text)
                    .policies(policies(scoped.clone())),
            )
            .table(
                TableSchemaBuilder::new("links")
                    .fk_column("doc", "docs")
                    .column("code", ColumnType::Text)
                    .policies(policies(scoped)),
            )
            .table(
                TableSchemaBuilder::new("checks")
                    .column("title", ColumnType::Text)
                    .policies(policies(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap();
    let cfg = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([n; 16]),
            author: if ordinary {
                alice()
            } else {
                AuthorSubject::SYSTEM
            },
        },
    );
    let db = if authority {
        block_on(Db::open_history_complete(cfg))
    } else {
        block_on(Db::open(cfg))
    }
    .unwrap();
    db.set_identity_claims(alice(), claims("a"));
    if authority {
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
    }
    db
}
fn insert(db: &Db, table: &str, n: u8, fields: &[(&str, Value)]) {
    block_on(
        db.insert(
            table,
            fields
                .iter()
                .map(|(k, v)| ((*k).into(), v.clone()))
                .collect(),
            InsertOptions {
                row_id: Some(id(n)),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}
fn seed(db: &Db) {
    for (n, title, code) in [
        (9, "aardvark", "a"),
        (10, "alpha", "a"),
        (11, "beta", "b"),
        (12, "gamma", "a"),
    ] {
        insert(
            db,
            "docs",
            n,
            &[
                ("title", Value::String(title.into())),
                ("code", Value::String(code.into())),
            ],
        );
    }
    for (n, code) in [(10, "a"), (11, "b")] {
        insert(
            db,
            "links",
            n + 10,
            &[
                ("doc", Value::Uuid(id(n).0)),
                ("code", Value::String(code.into())),
            ],
        );
    }
}
fn column(name: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some("docs".into()),
        column: name.into(),
    }
}
fn query(window: bool, alias: &str) -> Query {
    let arm = |linked| {
        let scan = RelationExpr::TableScan {
            table: "docs".into(),
            alias: None,
        };
        let input = if linked {
            RelationExpr::Join {
                left: Box::new(scan),
                right: Box::new(RelationExpr::TableScan {
                    table: "links".into(),
                    alias: None,
                }),
                on: vec![RelationJoinCondition {
                    left: column("id"),
                    right: RelationColumnRef {
                        scope: Some("links".into()),
                        column: "doc".into(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }
        } else {
            scan
        };
        RelationExpr::Project {
            input: Box::new(input),
            columns: vec![
                RelationProjectColumn {
                    alias: "id".into(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                RelationProjectColumn {
                    alias: alias.into(),
                    expr: RelationProjectExpr::Column(column("title")),
                },
            ],
        }
    };
    let mut rel = RelationExpr::OrderBy {
        input: Box::new(RelationExpr::Union {
            inputs: vec![
                RelationUnionArm {
                    label: "all".into(),
                    input: arm(false),
                },
                RelationUnionArm {
                    label: "linked".into(),
                    input: arm(true),
                },
            ],
        }),
        terms: vec![RelationOrderBy {
            column: column(alias),
            direction: OrderDirection::Asc,
        }],
    };
    if window {
        rel = RelationExpr::Limit {
            input: Box::new(RelationExpr::Offset {
                input: Box::new(rel),
                offset: 1,
            }),
            limit: 2,
        };
    }
    let mut q = Query::from("docs");
    q.relation = Some(RelationQuery { rel });
    q
}
fn connect(client: &Db, server: &Db, ordinary: bool) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber_with_claims(
        down,
        if ordinary {
            alice()
        } else {
            AuthorSubject::SYSTEM
        },
        ordinary.then(|| claims("a")),
    );
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
    let mut future = pin!(future);
    for _ in 0..400 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        for node in nodes {
            block_on(node.tick())?;
        }
    }
    panic!("accepted UNION did not settle");
}
fn begin(db: &Db, scoped: bool) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    if scoped {
        block_on(db.begin_serializable_for_identity(tx, alice())).unwrap();
    } else {
        block_on(db.begin_serializable(tx)).unwrap();
    }
    tx
}
async fn paired(
    db: &Db,
    tx: OpenTransactionId,
    q: &Query,
    scoped: bool,
    mode: u8,
) -> Result<BindingSettledRead, Error> {
    let mut prepared = db.prepare_query(q)?;
    if scoped {
        prepared = prepared.with_identity_claims(alice(), claims("a"));
    }
    let opts = ReadOpts::for_read_tier(ReadTier::Remote);
    if mode == 2 {
        db.all_settled_at_authority_begin(prepared, opts, tx, scoped.then(alice))?
            .await
    } else {
        db.all_settled(prepared, opts, tx, mode == 1, scoped.then(alice))?
            .await
    }
}
fn expect_page(read: &BindingSettledRead) {
    assert_eq!(
        read.rows
            .iter()
            .map(|row| (row.row_uuid(), row.union_arm().unwrap()))
            .collect::<Vec<_>>(),
        [(id(10), "all"), (id(10), "linked")]
    );
    assert_eq!(read.settlements.len(), 2);
    assert_eq!(read.settlements[0], read.settlements[1]);
}
fn commit(db: &Db, tx: OpenTransactionId, nodes: &[&Db]) -> Result<(), Error> {
    let receipt = block_on(db.commit_serializable_handle(tx))?;
    drive(
        nodes,
        db.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .map(drop)
}

/// Alice's cold pages retain duplicate root occurrences and Bob's real content
/// settlements through ordinary sessions and trusted backends, directly or relayed.
#[test]
fn cold_accepted_union_pages_keep_content_and_occurrences() {
    for (ordinary, scoped) in [(true, true), (false, true), (false, false)] {
        for relayed in [false, true] {
            let server = open(90, true, false);
            seed(&server);
            let q = query(true, "label");
            let expected =
                block_on(paired(&server, begin(&server, scoped), &q, scoped, 0)).unwrap();
            expect_page(&expected);
            let relay = open(2, false, ordinary);
            let client = open(1, false, ordinary);
            if relayed {
                connect(&relay, &server, ordinary);
                connect(&client, &relay, ordinary);
            } else {
                connect(&client, &server, ordinary);
            }
            let nodes = [&client, &relay, &server];
            let tx = begin(&client, scoped);
            let got = drive(&nodes, paired(&client, tx, &q, scoped, 0)).unwrap_or_else(|error| {
                panic!("ordinary={ordinary}, scoped={scoped}, relayed={relayed}: {error:?}")
            });
            assert_eq!(got, expected);
            assert_eq!(
                drive(&nodes, paired(&client, tx, &q, scoped, 0)).unwrap(),
                got
            );
            commit(&client, tx, &nodes).unwrap();
        }
    }
}

/// Alice's staged insertion changes the authority's cold page. A trusted backend
/// must reject it without publishing the simulated row or permitting commit.
#[test]
fn staged_union_phantom_is_rejected_without_publication() {
    for relayed in [false, true] {
        let server = open(90, true, false);
        seed(&server);
        let relay = open(2, false, false);
        let client = open(1, false, false);
        if relayed {
            connect(&relay, &server, false);
            connect(&client, &relay, false);
        } else {
            connect(&client, &server, false);
        }
        let nodes = [&client, &relay, &server];
        let tx = begin(&client, true);
        block_on(client.tx_ref(tx).insert(
            "docs",
            BTreeMap::from([
                ("title".into(), Value::String("aa".into())),
                ("code".into(), Value::String("a".into())),
            ]),
            InsertOptions {
                row_id: Some(id(40)),
                ..Default::default()
            },
        ))
        .unwrap();
        let error = drive(&nodes, paired(&client, tx, &query(true, "title"), true, 0)).unwrap_err();
        assert_eq!(error.code, ErrorCode::NotObserved, "{error:?}");
        assert!(block_on(client.commit_serializable_handle(tx)).is_err());
        let rows = block_on(server.all(
            &server.prepare_query(&Query::from("docs")).unwrap(),
            ReadOpts::default(),
        ))
        .unwrap();
        assert_eq!(rows.len(), 4);
    }
}

/// Bob revokes a join witness after Alice reads a page. The root content did not
/// change, but authority commit validation must reject the changed occurrence set.
#[test]
fn accepted_union_commit_rechecks_the_original_full_query() {
    let server = open(90, true, false);
    seed(&server);
    let client = open(1, false, false);
    connect(&client, &server, false);
    let nodes = [&client, &server];
    let tx = begin(&client, true);
    expect_page(&drive(&nodes, paired(&client, tx, &query(true, "title"), true, 0)).unwrap());
    block_on(client.tx_ref(tx).insert(
        "checks",
        BTreeMap::from([("title".into(), Value::String("must not publish".into()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(server.delete("links", id(20), Default::default())).unwrap();
    assert!(commit(&client, tx, &nodes).is_err());
    assert!(
        block_on(server.all(
            &server.prepare_query(&Query::from("checks")).unwrap(),
            ReadOpts::default()
        ))
        .unwrap()
        .is_empty()
    );
}

/// Alice pins the known authority coordinate before Bob edits the page. An
/// authority-at-begin read returns the old content, while commit still detects change.
#[test]
fn accepted_union_authority_begin_keeps_the_old_page() {
    let server = open(90, true, false);
    seed(&server);
    let client = open(1, false, false);
    connect(&client, &server, false);
    let nodes = [&client, &server];
    let q = query(true, "title");
    let warm = begin(&client, true);
    let expected = drive(&nodes, paired(&client, warm, &q, true, 0)).unwrap();
    commit(&client, warm, &nodes).unwrap();
    let tx = begin(&client, true);
    block_on(server.update(
        "docs",
        id(10),
        BTreeMap::from([("title".into(), Value::String("omega".into()))]),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        drive(&nodes, paired(&client, tx, &q, true, 2)).unwrap(),
        expected
    );
    assert!(commit(&client, tx, &nodes).is_err());
}

struct Mutated {
    link: Box<dyn Transport>,
    mode: &'static str,
    queued: VecDeque<SyncMessage>,
    receipt: Option<SyncMessage>,
}
impl Transport for Mutated {
    fn send(&mut self, m: SyncMessage) -> Result<(), TransportError> {
        self.link.send(m)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if let Some(message) = self.queued.pop_front() {
            return Some(message);
        }
        loop {
            let mut m = self.link.try_recv()?;
            match (&mut m, self.mode) {
                (SyncMessage::AcceptedRowsV1 { .. }, "missing_content") => continue,
                (SyncMessage::UnionAnswerV1 { .. }, "missing_occurrences") => continue,
                (SyncMessage::UnionAnswerV1 { occurrences, .. }, "wrong_arm") => {
                    occurrences[0].arm = "mallory".into()
                }
                (SyncMessage::UnionAnswerV1 { .. }, "reversed") => {
                    self.receipt = Some(m);
                    continue;
                }
                (SyncMessage::AcceptedScopedEvidenceAnswerV1 { through, .. }, "wrong_cut") => {
                    *through += 1
                }
                (SyncMessage::AcceptedScopedEvidenceAnswerV1 { rows, .. }, "wrong_rows") => {
                    rows.clear()
                }
                (
                    SyncMessage::AcceptedScopedEvidenceAnswerV1 {
                        id,
                        through,
                        rows,
                        evidence,
                    },
                    "downgrade",
                ) => {
                    return Some(SyncMessage::ScopedEvidenceAnswerV1 {
                        id: *id,
                        through: *through,
                        rows: rows.clone(),
                        evidence: evidence.clone(),
                    });
                }
                (SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. }, "reversed") => {
                    self.queued.push_back(self.receipt.take().unwrap());
                }
                _ => {}
            }
            return Some(m);
        }
    }
}

/// Mallory drops or changes Alice's accepted qualification or content proof.
/// Raw interception is needed to send messages a real authority never emits.
#[test]
fn accepted_union_refuses_missing_or_mismatched_qualification() {
    for mode in [
        "missing_content",
        "missing_occurrences",
        "wrong_arm",
        "wrong_cut",
        "wrong_rows",
        "downgrade",
        "reversed",
    ] {
        let server = open(90, true, false);
        seed(&server);
        let client = open(1, false, false);
        let (up, down) = duplex();
        block_on(client.connect_upstream(Box::new(Mutated {
            link: up,
            mode,
            queued: VecDeque::new(),
            receipt: None,
        })));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let tx = begin(&client, true);
        assert!(
            drive(
                &[&client, &server],
                paired(&client, tx, &query(true, "title"), true, 0)
            )
            .is_err(),
            "{mode}"
        );
        assert!(
            block_on(client.commit_serializable_handle(tx)).is_err(),
            "{mode}"
        );
    }
}

// Delay only the acceptance qualifier: the real authority still sends the
// root cells, content receipts and UNION occurrences through normal sync.
struct HeldQualification {
    link: Box<dyn Transport>,
    release: Rc<Cell<bool>>,
    held: Rc<RefCell<Option<SyncMessage>>>,
}
impl Transport for HeldQualification {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.held.borrow().is_some() {
            return if self.release.get() {
                self.held.borrow_mut().take()
            } else {
                None
            };
        }
        match self.link.try_recv() {
            Some(
                message @ (SyncMessage::AcceptedAnswerV1 { .. }
                | SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. }),
            ) if !self.release.get() => {
                *self.held.borrow_mut() = Some(message);
                None
            }
            message => message,
        }
    }
}

/// Alice's accepted page stays pending after its rows and occurrence receipt
/// arrive. Releasing the qualifier completes it; cancellation or reconnect
/// cannot lend the old qualifier to a new transaction. Transport interception
/// is needed to split the two real authority frames across drive turns.
#[test]
fn accepted_union_pairs_wait_and_retire_with_their_read() {
    for scoped in [false, true] {
        for action in ["release", "cancel", "reconnect"] {
            let server = open(90, true, false);
            seed(&server);
            let client = open(1, false, false);
            let (up, down) = duplex();
            let release = Rc::new(Cell::new(false));
            let held = Rc::new(RefCell::new(None));
            block_on(client.connect_upstream(Box::new(HeldQualification {
                link: up,
                release: release.clone(),
                held: held.clone(),
            })));
            server.accept_subscriber(down, AuthorSubject::SYSTEM);
            let nodes = [&client, &server];
            let q = query(true, "title");
            let tx = begin(&client, scoped);
            let mut pending = Box::pin(paired(&client, tx, &q, scoped, 0));
            for _ in 0..40 {
                assert!(
                    pending
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                for node in nodes {
                    block_on(node.tick()).unwrap();
                }
            }
            assert!(held.borrow().is_some());
            if action == "release" {
                release.set(true);
                expect_page(&drive(&nodes, pending).unwrap());
                commit(&client, tx, &nodes).unwrap();
            } else {
                if action == "reconnect" {
                    connect(&client, &server, false);
                    assert_eq!(
                        drive(&nodes, pending.as_mut()).unwrap_err().code,
                        ErrorCode::NotObserved
                    );
                }
                drop(pending);
                assert!(block_on(client.commit_serializable_handle(tx)).is_err());
                release.set(true);
                let next = begin(&client, scoped);
                expect_page(&drive(&nodes, paired(&client, next, &q, scoped, 0)).unwrap());
                commit(&client, next, &nodes).unwrap();
            }
        }
    }
}

struct InvalidAcceptedBatch {
    link: Box<dyn Transport>,
    inject: bool,
    done: bool,
}
impl Transport for InvalidAcceptedBatch {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if std::mem::take(&mut self.inject) {
            self.done = true;
            return Some(SyncMessage::Unsubscribe { id: 0 });
        }
        let message = self.link.try_recv()?;
        if !self.done
            && matches!(
                message,
                SyncMessage::AcceptedAnswerV1 { .. }
                    | SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. }
            )
        {
            self.inject = true;
        }
        Some(message)
    }
}

/// Mallory appends an invalid frame to Alice's real accepted reply. Neither
/// half may publish when that SQLite sync batch rolls back. A reconnect must
/// restore rolled-back catalogue metadata before a new read can succeed.
/// Raw interception is necessary to exercise the sync savepoint boundary.
#[test]
fn rolled_back_batch_cannot_publish_accepted_union_coverage() {
    for scoped in [false, true] {
        let server = open(90, true, false);
        seed(&server);
        let client = open(1, false, false);
        let (up, down) = duplex();
        block_on(client.connect_upstream(Box::new(InvalidAcceptedBatch {
            link: up,
            inject: false,
            done: false,
        })));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let q = query(true, "title");
        let tx = begin(&client, scoped);
        let mut pending = Box::pin(paired(&client, tx, &q, scoped, 0));
        let mut rolled_back = false;
        for _ in 0..100 {
            assert!(
                pending
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            block_on(server.tick()).unwrap();
            if let Err(error) = block_on(client.tick()) {
                assert_eq!(error.code, ErrorCode::Query);
                assert!(error.message.contains("client received"), "{error:?}");
                rolled_back = true;
                break;
            }
        }
        assert!(rolled_back);
        assert!(!matches!(
            pending
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(_))
        ));
        drop(pending);
        assert!(block_on(client.commit_serializable_handle(tx)).is_err());
        connect(&client, &server, false);
        let next = begin(&client, scoped);
        expect_page(&drive(&[&client, &server], paired(&client, next, &q, scoped, 0)).unwrap());
        commit(&client, next, &[&client, &server]).unwrap();
    }
}

/// Alice reads an exact empty accepted UNION. Bob then inserts a matching
/// row: the empty result must still register a phantom read and reject commit.
#[test]
fn empty_accepted_union_records_its_full_query() {
    for (ordinary, scoped) in [(true, true), (false, true), (false, false)] {
        let server = open(90, true, false);
        let client = open(1, false, ordinary);
        connect(&client, &server, ordinary);
        let nodes = [&client, &server];
        let tx = begin(&client, scoped);
        let read = drive(
            &nodes,
            paired(&client, tx, &query(false, "title"), scoped, 0),
        )
        .unwrap();
        assert!(read.rows.is_empty());
        assert!(read.settlements.is_empty());
        insert(
            &server,
            "docs",
            10,
            &[
                ("title", Value::String("alpha".into())),
                ("code", Value::String("a".into())),
            ],
        );
        assert!(commit(&client, tx, &nodes).is_err());
    }
}

/// Alice needs content already observed at transaction begin. Cold late-arriving
/// proofs cannot qualify, while a complete prior accepted observation can.
#[test]
fn accepted_union_observed_begin_refuses_late_proof() {
    for scoped in [false, true] {
        let server = open(90, true, false);
        seed(&server);
        let client = open(1, false, false);
        connect(&client, &server, false);
        let nodes = [&client, &server];
        let q = query(false, "title");
        let cold = begin(&client, scoped);
        assert_eq!(
            drive(&nodes, paired(&client, cold, &q, scoped, 1))
                .unwrap_err()
                .code,
            ErrorCode::NotObserved
        );
        assert!(block_on(client.commit_serializable_handle(cold)).is_err());
        let warm = begin(&client, scoped);
        let expected = drive(&nodes, paired(&client, warm, &q, scoped, 0)).unwrap();
        commit(&client, warm, &nodes).unwrap();
        let next = begin(&client, scoped);
        assert_eq!(
            drive(&nodes, paired(&client, next, &q, scoped, 1)).unwrap(),
            expected
        );
        commit(&client, next, &nodes).unwrap();
    }
}

/// Independent literal bytes pin accepted UNION qualifiers for populated and
/// empty pages. Codec checks protect the named wire sequence; the public query
/// tests above establish Alice's acceptance and permission semantics.
#[test]
fn accepted_union_coverage_sequence_has_fixed_bytes() {
    use jazz::db::{ScopedReadRowV1, UnionOccurrenceV1};
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v2/accepted-union.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["role"],
        "jazz.sqlite.sync.accepted-union-coverage.v2"
    );
    for scoped in [false, true] {
        for (key, id, through, populated) in
            [("populated", 128, 300, true), ("empty", 129, 301, false)]
        {
            let occurrences = if populated {
                ["all", "linked"]
                    .into_iter()
                    .map(|arm| UnionOccurrenceV1 {
                        row: [17; 16],
                        arm: arm.into(),
                    })
                    .collect()
            } else {
                vec![]
            };
            let rows = occurrences
                .iter()
                .map(|occurrence| occurrence.row)
                .collect();
            let qualifier = if scoped {
                let evidence = if populated {
                    [("docs", 17, 300), ("links", 34, 257), ("access", 51, 299)]
                        .into_iter()
                        .map(|(table, row, seq)| ScopedReadRowV1 {
                            table: table.into(),
                            id: [row; 16],
                            key: vec![],
                            seq,
                        })
                        .collect()
                } else {
                    vec![]
                };
                SyncMessage::AcceptedScopedEvidenceAnswerV1 {
                    id,
                    through,
                    rows,
                    evidence,
                }
            } else {
                SyncMessage::AcceptedAnswerV1 { id, through, rows }
            };
            let mode = if scoped { "scoped" } else { "ordinary" };
            for (message, encoded) in [
                SyncMessage::UnionAnswerV1 {
                    id,
                    through,
                    occurrences,
                },
                qualifier,
            ]
            .into_iter()
            .zip(fixture[mode][key].as_array().unwrap())
            {
                let bytes = hex::decode(encoded.as_str().unwrap()).unwrap();
                assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
                assert_eq!(
                    serde_json::to_value(jazz::sync_codec::decode_sync_message(&bytes).unwrap())
                        .unwrap(),
                    serde_json::to_value(message).unwrap()
                );
                let mut trailing = bytes.clone();
                trailing.push(0);
                assert!(jazz::sync_codec::decode_sync_message(&trailing).is_err());
                assert!(jazz::sync_codec::decode_sync_message(&bytes[..bytes.len() - 1]).is_err());
            }
        }
    }
}

/// Alice stages a new document after the selected cold page. The trusted
/// backend can validate the unchanged full query, but the authority must not
/// publish the simulated write until the ordinary transaction commits.
#[test]
fn trusted_union_overlay_outside_page_waits_for_commit() {
    for relayed in [false, true] {
        let server = open(90, true, false);
        seed(&server);
        let client = open(1, false, false);
        let relay = open(2, false, false);
        if relayed {
            connect(&relay, &server, false);
            connect(&client, &relay, false);
        } else {
            connect(&client, &server, false);
        }
        let nodes = [&client, &relay, &server];
        let tx = begin(&client, true);
        block_on(client.tx_ref(tx).insert(
            "docs",
            BTreeMap::from([
                ("title".into(), Value::String("zzzz".into())),
                ("code".into(), Value::String("a".into())),
            ]),
            InsertOptions {
                row_id: Some(id(40)),
                ..Default::default()
            },
        ))
        .unwrap();
        expect_page(&drive(&nodes, paired(&client, tx, &query(true, "title"), true, 0)).unwrap());
        let count = || {
            block_on(server.all(
                &server.prepare_query(&Query::from("docs")).unwrap(),
                ReadOpts::default(),
            ))
            .unwrap()
            .len()
        };
        assert_eq!(count(), 4);
        commit(&client, tx, &nodes).unwrap();
        assert_eq!(count(), 5);
    }
}

/// Alice orders a large title through an alias named after another stored
/// field. Cold relay hydration must preserve the exact page, full output cells
/// and content settlements after common chunk prefixes are compared.
#[test]
fn accepted_union_aliases_keep_large_values_and_content_receipts() {
    for (ordinary, scoped) in [(true, true), (false, true), (false, false)] {
        let server = open(90, true, false);
        seed(&server);
        let prefix = "prefix".repeat(24_000);
        for (row, suffix) in [(9, "c"), (10, "a"), (11, "d"), (12, "b")] {
            block_on(server.update(
                "docs",
                id(row),
                BTreeMap::from([("title".into(), Value::String(format!("{prefix}{suffix}")))]),
                Default::default(),
            ))
            .unwrap();
        }
        let q = query(true, "code");
        let expected = block_on(paired(&server, begin(&server, scoped), &q, scoped, 0)).unwrap();
        let client = open(1, false, ordinary);
        let relay = open(2, false, ordinary);
        connect(&relay, &server, ordinary);
        connect(&client, &relay, ordinary);
        let nodes = [&client, &relay, &server];
        let tx = begin(&client, scoped);
        let actual = drive(&nodes, paired(&client, tx, &q, scoped, 0)).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.rows.len(), 2);
        for row in &actual.rows {
            let cells = row.cells().collect::<Vec<_>>();
            assert_eq!(cells.len(), 1);
            assert_eq!(cells[0].0, "code");
            let Some(Value::String(value)) = cells[0].1 else {
                panic!("large title was not hydrated")
            };
            assert!(value.starts_with(&prefix));
        }
        commit(&client, tx, &nodes).unwrap();
    }
}
