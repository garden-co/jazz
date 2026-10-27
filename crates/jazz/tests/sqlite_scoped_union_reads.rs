//! Request-scoped UNION coverage keeps semantic occurrences and permission proof paired.
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, Error, ErrorCode, InsertOptions, ReadOpts, ReadTier,
    SerializedReadResult, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, RelationOrderBy, RelationQuery, provider_claim_key};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{
    RelationColumnRef, RelationExpr, RelationJoinCondition, RelationJoinKind,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationUnionArm,
};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://scoped-union.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn claims(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("code"), Value::String(code.into()))])
}
fn open(node: u8, authority: bool) -> Db {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("code", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                        "code",
                        vec!["claims".into(), "code".into()],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("links")
                    .fk_column("doc", "docs")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Exists {
                        table: "access".into(),
                        condition: Box::new(PolicyExpr::And(vec![
                            PolicyExpr::eq_session(
                                "link",
                                vec!["__jazz_outer_row".into(), "id".into()],
                            ),
                            PolicyExpr::eq_session("code", vec!["claims".into(), "code".into()]),
                        ])),
                    })),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("link", "links")
                    .column("code", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::False)),
            )
            .build(),
    )
    .unwrap();
    let config = DbConfig::new(
        if authority {
            schema
        } else {
            schema.without_permissions()
        },
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let db = if authority {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap();
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
fn seed(server: &Db) {
    for (n, title, code) in [
        (9, "aardvark", "a"),
        (10, "alpha", "a"),
        (11, "beta", "b"),
        (12, "gamma", "a"),
    ] {
        insert(
            server,
            "docs",
            n,
            &[
                ("title", Value::String(title.into())),
                ("code", Value::String(code.into())),
            ],
        );
    }
    for (n, code) in [(10, "a"), (11, "b")] {
        insert(server, "links", n + 10, &[("doc", Value::Uuid(id(n).0))]);
        insert(
            server,
            "access",
            n + 20,
            &[
                ("link", Value::Uuid(id(n + 10).0)),
                ("code", Value::String(code.into())),
            ],
        );
    }
    // Mallory's link cannot make Alice's gamma occurrence visible in the joined arm.
    insert(server, "links", 22, &[("doc", Value::Uuid(id(12).0))]);
    insert(
        server,
        "access",
        32,
        &[
            ("link", Value::Uuid(id(22).0)),
            ("code", Value::String("mallory".into())),
        ],
    );
}
fn column(table: &str, name: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(table.into()),
        column: name.into(),
    }
}
fn arm(joined: bool) -> RelationExpr {
    let mut input = RelationExpr::TableScan {
        table: "docs".into(),
        alias: None,
    };
    if joined {
        input = RelationExpr::Join {
            left: Box::new(input),
            right: Box::new(RelationExpr::TableScan {
                table: "links".into(),
                alias: None,
            }),
            on: vec![RelationJoinCondition {
                left: column("docs", "id"),
                right: column("links", "doc"),
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
                alias: "title".into(),
                expr: RelationProjectExpr::Column(column("docs", "title")),
            },
        ],
    }
}
fn query(window: bool) -> Query {
    let rel = RelationExpr::Union {
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
    };
    let mut rel = RelationExpr::OrderBy {
        input: Box::new(rel),
        terms: vec![RelationOrderBy {
            column: column("docs", "title"),
            direction: OrderDirection::Asc,
        }],
    };
    if window {
        rel = RelationExpr::Limit {
            input: Box::new(RelationExpr::Offset {
                input: Box::new(rel),
                offset: 2,
            }),
            limit: 2,
        };
    }
    let mut q = Query::from("docs");
    q.relation = Some(RelationQuery { rel });
    q
}
fn connect(client: &Db, server: &Db) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = Result<T, Error>>) -> Result<T, Error> {
    let mut future = pin!(future);
    for _ in 0..300 {
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
    panic!("scoped UNION did not answer");
}
async fn read(db: &Db, q: &Query, code: &str) -> Result<Vec<CurrentRow>, Error> {
    let bytes = postcard::to_allocvec(q).unwrap();
    let result = db
        .all_serialized_query_as(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            Some((alice(), claims(code))),
            Some(alice()),
            false,
            || false,
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("flat rows")
    };
    Ok(rows)
}
fn occurrences(rows: &[CurrentRow]) -> Vec<(RowUuid, String)> {
    rows.iter()
        .map(|row| (row.row_uuid(), row.union_arm().unwrap().into()))
        .collect()
}
fn expect_a_window(rows: &[CurrentRow]) {
    assert_eq!(
        occurrences(rows),
        [(id(10), "linked".into()), (id(12), "all".into())]
    );
}

/// Alice's cold request skips unseen predecessors and retains distinct UNION
/// arms. A trusted relay carries unreadable policy witnesses only as scoped
/// evidence; Mallory's unrelated grant and stale warm roots cannot fill the page.
#[test]
fn cold_scoped_windows_preserve_occurrences_through_a_relay() {
    for relayed in [false, true] {
        let server = open(90, true);
        seed(&server);
        let relay = open(2, false);
        let client = open(1, false);
        if relayed {
            connect(&relay, &server);
            connect(&client, &relay);
        } else {
            connect(&client, &server);
        }
        let nodes = [&client, &relay, &server];
        let rows = drive(&nodes, read(&client, &query(true), "a")).unwrap();
        expect_a_window(&rows);
        let cached = block_on(client.all(
            &client.prepare_query(&Query::from("docs")).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
        ))
        .unwrap();
        assert_eq!(
            cached
                .iter()
                .map(CurrentRow::row_uuid)
                .collect::<std::collections::BTreeSet<_>>(),
            [id(9), id(10), id(12)].into_iter().collect(),
            "the permitted prefix is cached; Bob's row remains excluded"
        );
        let b = drive(&nodes, read(&client, &query(false), "b")).unwrap();
        assert_eq!(
            occurrences(&b),
            [(id(11), "all".into()), (id(11), "linked".into())]
        );
        expect_a_window(&drive(&nodes, read(&client, &query(true), "a")).unwrap());
        block_on(server.delete("access", id(30), Default::default())).unwrap();
        let revoked = drive(&nodes, read(&client, &query(true), "a")).unwrap();
        assert_eq!(occurrences(&revoked), [(id(12), "all".into())]);
        assert!(
            drive(&nodes, read(&client, &query(false), "nobody"))
                .unwrap()
                .is_empty()
        );
    }
}

/// Two concurrent requests for Alice carry different claims through the same
/// backend and relay. Changing ambient claims cannot swap their exact results.
#[test]
fn concurrent_scoped_unions_keep_immutable_claims() {
    let server = open(90, true);
    seed(&server);
    let relay = open(2, false);
    let client = open(1, false);
    connect(&relay, &server);
    connect(&client, &relay);
    let q = query(false);
    let mut a = pin!(read(&client, &q, "a"));
    let mut b = pin!(read(&client, &q, "b"));
    let mut results = (None, None);
    for _ in 0..300 {
        if results.0.is_none()
            && let Poll::Ready(r) = a.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        {
            results.0 = Some(r.unwrap());
        }
        if results.1.is_none()
            && let Poll::Ready(r) = b.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        {
            results.1 = Some(r.unwrap());
        }
        if results.0.is_some() && results.1.is_some() {
            break;
        }
        client.set_identity_claims(alice(), claims("mallory"));
        for node in [&client, &relay, &server] {
            block_on(node.tick()).unwrap();
        }
    }
    assert_eq!(
        occurrences(&results.0.unwrap()),
        [
            (id(9), "all".into()),
            (id(10), "all".into()),
            (id(10), "linked".into()),
            (id(12), "all".into())
        ]
    );
    assert_eq!(
        occurrences(&results.1.unwrap()),
        [(id(11), "all".into()), (id(11), "linked".into())]
    );
}

struct ReplyEditor {
    link: Box<dyn Transport>,
    mode: Rc<Cell<&'static str>>,
    held: Rc<RefCell<VecDeque<SyncMessage>>>,
}
impl Transport for ReplyEditor {
    fn send(&mut self, mut message: SyncMessage) -> Result<(), TransportError> {
        let mode = self.mode.get();
        match &mut message {
            SyncMessage::UnionAnswerV1 {
                id, occurrences, ..
            } => match mode {
                "missing receipt" => return Ok(()),
                "wrong request" => *id += 1,
                "reverse pair" => {
                    self.held.borrow_mut().push_back(message);
                    return Ok(());
                }
                "unknown arm" => occurrences[0].arm = "forged".into(),
                "duplicate occurrence" => occurrences.push(occurrences[0].clone()),
                "duplicate receipt" => self.link.send(message.clone())?,
                _ => {}
            },
            SyncMessage::ScopedEvidenceAnswerV1 {
                id,
                through,
                rows,
                evidence,
            } => match mode {
                "hold evidence" => {
                    self.held.borrow_mut().push_back(message);
                    return Ok(());
                }
                "wrong cut" => *through += 1,
                "wrong roots" => rows.clear(),
                "missing evidence" => evidence.clear(),
                "ordinary answer" => {
                    message = SyncMessage::Answer {
                        id: *id,
                        through: *through,
                        rows: rows.clone(),
                    }
                }
                "accepted answer" => {
                    message = SyncMessage::AcceptedScopedEvidenceAnswerV1 {
                        id: *id,
                        through: *through,
                        rows: rows.clone(),
                        evidence: evidence.clone(),
                    }
                }
                "reverse pair" => {
                    self.link.send(message)?;
                    while let Some(receipt) = self.held.borrow_mut().pop_front() {
                        self.link.send(receipt)?;
                    }
                    return Ok(());
                }
                _ => {}
            },
            _ => {}
        }
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.mode.get() == "release" {
            while let Some(message) = self.held.borrow_mut().pop_front() {
                self.link.send(message).unwrap();
            }
        }
        self.link.try_recv()
    }
}
fn edited(
    client: &Db,
    server: &Db,
    mode: &'static str,
) -> (Rc<Cell<&'static str>>, Rc<RefCell<VecDeque<SyncMessage>>>) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    let mode = Rc::new(Cell::new(mode));
    let held = Rc::new(RefCell::new(VecDeque::new()));
    server.accept_subscriber(
        Box::new(ReplyEditor {
            link: down,
            mode: mode.clone(),
            held: held.clone(),
        }),
        AuthorSubject::SYSTEM,
    );
    (mode, held)
}

/// Mallory alters the real authority's reply. No ordinary, unpaired, mismatched
/// or forged occurrence/evidence sequence may publish Alice's scoped result.
/// Wire interception is needed to exercise protocol admission rather than queries.
#[test]
fn scoped_unions_reject_malformed_and_downgraded_coverage() {
    for mode in [
        "missing receipt",
        "wrong request",
        "reverse pair",
        "unknown arm",
        "duplicate occurrence",
        "duplicate receipt",
        "wrong cut",
        "wrong roots",
        "missing evidence",
        "ordinary answer",
        "accepted answer",
    ] {
        let server = open(90, true);
        seed(&server);
        let client = open(1, false);
        let _ = edited(&client, &server, mode);
        let error = drive(&[&client, &server], read(&client, &query(true), "a")).unwrap_err();
        assert!(
            matches!(error.code, ErrorCode::Protocol | ErrorCode::NotObserved),
            "{mode}: {error:?}"
        );
    }
}

/// Alice receives rows and occurrences first. A cold result stays pending until
/// its permission evidence arrives, even when all visible root cells are resident.
#[test]
fn scoped_union_waits_for_matching_permission_evidence() {
    let server = open(90, true);
    seed(&server);
    let client = open(1, false);
    let (mode, held) = edited(&client, &server, "hold evidence");
    let q = query(true);
    let mut future = pin!(read(&client, &q, "a"));
    for _ in 0..40 {
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        for node in [&client, &server] {
            block_on(node.tick()).unwrap();
        }
    }
    assert_eq!(held.borrow().len(), 1);
    mode.set("release");
    expect_a_window(&drive(&[&client, &server], future).unwrap());
}

/// An ordinary Alice session cannot turn a delegated scoped UNION request into
/// trusted backend admission, even when its requested principal is Alice too.
#[test]
fn ordinary_session_cannot_request_scoped_union_evidence() {
    let server = open(90, true);
    seed(&server);
    let client = open(1, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice());
    let error = drive(&[&client, &server], read(&client, &query(true), "a")).unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
}

/// Alice orders a renamed output whose name is also another stored text field.
/// Chunk-prefix ties must rank the title source, preserving the narrowed output
/// and duplicate arm identities through the trusted relay.
#[test]
fn scoped_union_aliases_sort_their_physical_large_value_source() {
    fn rename(expr: &mut RelationExpr) {
        match expr {
            RelationExpr::Union { inputs } => {
                for arm in inputs {
                    rename(&mut arm.input)
                }
            }
            RelationExpr::Project { columns, .. } => {
                for column in columns {
                    if column.alias == "title" {
                        column.alias = "code".into();
                    }
                }
            }
            RelationExpr::OrderBy { input, terms } => {
                for term in terms {
                    if term.column.column == "title" {
                        term.column.column = "code".into();
                    }
                }
                rename(input);
            }
            RelationExpr::Offset { input, .. } | RelationExpr::Limit { input, .. } => rename(input),
            _ => {}
        }
    }
    let server = open(90, true);
    seed(&server);
    let relay = open(2, false);
    let client = open(1, false);
    let prefix = "prefix".repeat(24_000);
    for (row, suffix) in [(9, "c"), (10, "a"), (12, "b")] {
        block_on(server.update(
            "docs",
            id(row),
            BTreeMap::from([("title".into(), Value::String(format!("{prefix}{suffix}")))]),
            Default::default(),
        ))
        .unwrap();
    }
    connect(&relay, &server);
    connect(&client, &relay);
    let mut q = query(true);
    rename(&mut q.relation.as_mut().unwrap().rel);
    let rows = drive(&[&client, &relay, &server], read(&client, &q, "a")).unwrap();
    assert_eq!(
        occurrences(&rows),
        [(id(12), "all".into()), (id(9), "all".into())]
    );
    for (row, suffix) in rows.iter().zip(["b", "c"]) {
        let cells = row
            .cells()
            .map(|(name, value)| (name.to_owned(), value.cloned()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            cells,
            BTreeMap::from([(
                "code".into(),
                Some(Value::String(format!("{prefix}{suffix}")))
            )])
        );
    }
}

/// Alice cancels or reconnects after receiving only an occurrence receipt.
/// The old permission reply cannot answer a later request or cross connections.
#[test]
fn scoped_union_partial_receipts_retire_with_the_request_and_connection() {
    for reconnect in [false, true] {
        let server = open(90, true);
        seed(&server);
        let client = open(1, false);
        let (mode, held) = edited(&client, &server, "hold evidence");
        let q = query(true);
        let mut future = Box::pin(read(&client, &q, "a"));
        for _ in 0..40 {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            for node in [&client, &server] {
                block_on(node.tick()).unwrap();
            }
        }
        assert_eq!(held.borrow().len(), 1);
        if reconnect {
            connect(&client, &server);
            let result = drive(&[&client, &server], future.as_mut());
            assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
        }
        drop(future);
        mode.set("release");
        expect_a_window(&drive(&[&client, &server], read(&client, &q, "a")).unwrap());
    }
}

/// Independent byte fixtures pin both frames, including duplicate roots and an
/// exact empty answer. Low-level encoding checks protect the protocol contract;
/// public query tests above establish the permission and occurrence semantics.
#[test]
fn scoped_union_coverage_sequence_has_fixed_bytes() {
    use jazz::db::{ScopedReadRowV1, UnionOccurrenceV1};
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v3/scoped-union.json")).unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.sync.scoped-union-coverage.v2");
    for (key, id, through, populated) in [("populated", 128, 300, true), ("empty", 129, 301, false)]
    {
        let occurrences = if populated {
            vec![
                UnionOccurrenceV1 {
                    row: [17; 16],
                    arm: "all".into(),
                },
                UnionOccurrenceV1 {
                    row: [17; 16],
                    arm: "linked".into(),
                },
            ]
        } else {
            vec![]
        };
        let rows = occurrences
            .iter()
            .map(|occurrence| occurrence.row)
            .collect();
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
        for (message, bytes) in [
            SyncMessage::UnionAnswerV1 {
                id,
                through,
                occurrences,
            },
            SyncMessage::ScopedEvidenceAnswerV1 {
                id,
                through,
                rows,
                evidence,
            },
        ]
        .into_iter()
        .zip(fixture[key].as_array().unwrap())
        {
            let bytes = hex::decode(bytes.as_str().unwrap()).unwrap();
            assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
            let decoded = jazz::sync_codec::decode_sync_message(&bytes).unwrap();
            assert_eq!(
                serde_json::to_value(decoded).unwrap(),
                serde_json::to_value(message).unwrap()
            );
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(jazz::sync_codec::decode_sync_message(&trailing).is_err());
            assert!(jazz::sync_codec::decode_sync_message(&bytes[..bytes.len() - 1]).is_err());
        }
    }
}

struct InvalidProofBatch {
    link: Box<dyn Transport>,
    inject: bool,
    done: bool,
}
impl Transport for InvalidProofBatch {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if std::mem::take(&mut self.inject) {
            self.done = true;
            return Some(SyncMessage::Unsubscribe { id: 0 });
        }
        let message = self.link.try_recv()?;
        if !self.done && matches!(message, SyncMessage::ScopedEvidenceAnswerV1 { .. }) {
            self.inject = true;
        }
        Some(message)
    }
}

/// Mallory appends an invalid frame after Alice's real scoped reply. A failed
/// SQLite batch must not publish either half of its coverage. Reconnecting
/// replays the rolled-back catalogue and requires a fresh valid response pair.
/// Wire interception tests the commit boundary.
#[test]
fn rolled_back_batch_cannot_publish_scoped_union_coverage() {
    let server = open(90, true);
    seed(&server);
    let client = open(1, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(Box::new(InvalidProofBatch {
        link: up,
        inject: false,
        done: false,
    })));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let q = query(true);
    let mut pending = Box::pin(read(&client, &q, "a"));
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
    connect(&client, &server);
    expect_a_window(&drive(&[&client, &server], read(&client, &q, "a")).unwrap());
}
