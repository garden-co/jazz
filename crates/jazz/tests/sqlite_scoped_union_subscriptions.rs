//! Live scoped UNION coverage retains immutable claims and exact permission proof.
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, Error, ErrorCode, InsertOptions, ReadOpts, ReadTier,
    SerializedReadResult, SubscriptionEvent, SubscriptionStream, SyncMessage, Transport,
    TransportError, block_on, duplex,
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
    AuthorSubject::authenticated("https://scoped-union.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn claims(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("code"), Value::String(code.into()))])
}
fn open(node: u8, authority: bool) -> Db {
    open_as(node, authority, AuthorSubject::SYSTEM)
}
fn open_as(node: u8, authority: bool, identity: AuthorSubject) -> Db {
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
            author: identity,
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
        .all_serialized_query(
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

fn subscribe(client: &Db, q: &Query, code: &str) -> SubscriptionStream {
    let prepared = client
        .prepare_query(q)
        .unwrap()
        .with_identity_claims(alice(), claims(code));
    block_on(client.subscribe_for_identity(
        &prepared,
        ReadOpts::for_read_tier(ReadTier::Remote),
        alice(),
    ))
    .unwrap()
}
fn opening(stream: &mut SubscriptionStream, nodes: &[&Db]) -> Vec<CurrentRow> {
    for _ in 0..100 {
        if let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    reset: true,
                    added,
                    settled: true,
                    tier: DurabilityTier::Global,
                    ..
                } => return added.into_iter().map(|row| row.row).collect(),
                event => panic!("unexpected scoped live opening {event:?}"),
            }
        }
        for node in nodes {
            block_on(node.tick()).unwrap();
        }
    }
    panic!("scoped UNION subscription did not open");
}
fn events(stream: &mut SubscriptionStream, nodes: &[&Db]) -> Vec<SubscriptionEvent> {
    let mut result = Vec::new();
    for _ in 0..40 {
        for node in nodes {
            block_on(node.tick()).unwrap();
        }
        while let Some(event) = stream.try_next_event() {
            result.push(event);
        }
    }
    result
}

/// Alice watches a cold page through a trusted backend, directly or relayed.
/// Bob's private permission witness cannot appear as a result; revoking Alice's
/// own witness removes only its UNION occurrence and keeps Global settlement.
#[test]
fn scoped_live_cold_pages_and_private_witness_revocation() {
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
        let mut stream = subscribe(&client, &query(true), "a");
        expect_a_window(&opening(&mut stream, &nodes));
        let cached = block_on(client.all(
            &client.prepare_query(&Query::from("docs")).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
        ))
        .unwrap();
        let mut cached_ids = cached.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
        cached_ids.sort();
        assert_eq!(
            cached_ids,
            [id(9), id(10), id(12)],
            "hydrate the permitted prefix and page, excluding Bob's private root"
        );
        block_on(server.delete("access", id(30), Default::default())).unwrap();
        let mut removed = Vec::new();
        for event in events(&mut stream, &nodes) {
            match event {
                SubscriptionEvent::Delta {
                    removed: rows,
                    added,
                    settled,
                    tier,
                    ..
                } => {
                    assert!(settled);
                    assert_eq!(tier, DurabilityTier::Global);
                    assert!(added.is_empty());
                    removed.extend(rows.into_iter().map(|row| (row.row_uuid, row.union_arm)));
                }
                event => panic!("unexpected revocation {event:?}"),
            }
        }
        assert_eq!(removed, [(id(10), Some("linked".into()))]);
        assert_eq!(
            occurrences(&drive(&nodes, read(&client, &query(true), "a")).unwrap()),
            [(id(12), "all".into())]
        );
    }
}

/// Alice's two tokens share a backend connection but retain different claims.
/// Mallory changes ambient claims and drops one same-query stream; neither may
/// alter or release the other's coverage. A later reconnect keeps its scope.
#[test]
fn scoped_live_claims_and_lifetimes_are_isolated() {
    let server = open(90, true);
    seed(&server);
    let relay = open(2, false);
    let client = open(1, false);
    connect(&relay, &server);
    connect(&client, &relay);
    let nodes = [&client, &relay, &server];
    let q = query(false);
    let mut a = subscribe(&client, &q, "a");
    let mut b = subscribe(&client, &q, "b");
    let mut empty = subscribe(&client, &q, "nobody");
    assert_eq!(
        occurrences(&opening(&mut a, &nodes)),
        [
            (id(9), "all".into()),
            (id(10), "all".into()),
            (id(10), "linked".into()),
            (id(12), "all".into())
        ]
    );
    assert_eq!(
        occurrences(&opening(&mut b, &nodes)),
        [(id(11), "all".into()), (id(11), "linked".into())]
    );
    assert!(opening(&mut empty, &nodes).is_empty());
    client.set_identity_claims(alice(), claims("mallory"));
    relay.set_identity_claims(alice(), claims("mallory"));
    server.set_identity_claims(alice(), claims("mallory"));
    drop(a);
    drop(empty);
    let _ = events(&mut b, &nodes);
    connect(&client, &relay);
    block_on(server.update(
        "docs",
        id(11),
        BTreeMap::from([("title".into(), Value::String("new beta".into()))]),
        Default::default(),
    ))
    .unwrap();
    let mut changed = Vec::new();
    for event in events(&mut b, &nodes) {
        match event {
            SubscriptionEvent::Delta {
                added,
                updated,
                settled,
                tier,
                ..
            } => {
                assert!(settled);
                assert_eq!(tier, DurabilityTier::Global);
                changed.extend(
                    added
                        .into_iter()
                        .chain(updated)
                        .map(|row| (row.row.row_uuid(), row.row.union_arm().unwrap().to_owned())),
                );
            }
            event => panic!("unexpected scoped change {event:?}"),
        }
    }
    changed.sort();
    changed.dedup();
    assert_eq!(changed, [(id(11), "all".into()), (id(11), "linked".into())]);
}

/// Mallory cannot turn an ordinary connection into a scoped backend stream,
/// even by requesting that connection's own identity. Alice's private owner
/// also rejects a different subject arriving over a trusted backend link.
/// Raw requests are needed to exercise invalid admission independently of APIs.
#[test]
fn scoped_live_requests_keep_connection_and_owner_boundaries() {
    for private in [false, true] {
        let server = if private {
            open_as(90, false, alice())
        } else {
            open(90, true)
        };
        if private {
            server.set_scope_isolated_storage_owner().unwrap();
        }
        let (mut peer, down) = duplex();
        server.accept_subscriber(
            down,
            if private {
                AuthorSubject::SYSTEM
            } else {
                alice()
            },
        );
        let requested = if private {
            AuthorSubject::authenticated("https://scoped-union.example", "mallory").unwrap()
        } else {
            alice()
        };
        peer.send(SyncMessage::ScopedUnionSubscribeV1 {
            id: 91,
            query: query(true),
            binding: BTreeMap::new(),
            author: requested.canonical().into(),
            claims: claims("a"),
            include_deleted: false,
        })
        .unwrap();
        let mut refused = false;
        for _ in 0..10 {
            block_on(server.tick()).unwrap();
            while let Some(message) = peer.try_recv() {
                match message {
                    SyncMessage::Refused {
                        id: 91,
                        code,
                        message,
                    } => {
                        assert_eq!(code, ErrorCode::WriteRejected);
                        assert!(
                            message.contains(if private {
                                "scope-isolated storage owner"
                            } else {
                                "trusted backend connection"
                            }),
                            "{message}"
                        );
                        refused = true;
                    }
                    SyncMessage::UnionAnswerV1 { id: 91, .. }
                    | SyncMessage::ScopedEvidenceAnswerV1 { id: 91, .. }
                    | SyncMessage::Settled { id: 91, .. } => panic!("forged stream answered"),
                    _ => {}
                }
            }
        }
        assert!(refused);
    }
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
                "duplicate evidence" => self.link.send(message.clone())?,
                "hold proof tail" => {
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
            SyncMessage::Settled {
                through, global, ..
            } => match mode {
                "hold proof tail" | "hold settlement" => {
                    self.held.borrow_mut().push_back(message);
                    return Ok(());
                }
                "wrong settlement cut" => *through += 1,
                "local settlement" => *global = false,
                "duplicate settlement" => self.link.send(message.clone())?,
                _ => {}
            },
            _ => {}
        }
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.mode.get() == "release evidence" {
            let mut messages = self.held.borrow_mut();
            if matches!(
                messages.front(),
                Some(SyncMessage::ScopedEvidenceAnswerV1 { .. })
            ) {
                self.link.send(messages.pop_front().unwrap()).unwrap();
            }
        }
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

/// Alice must receive the authority's complete receipt → private evidence →
/// Global settlement sequence before a scoped stream can open. Transport delay
/// is needed because public subscriptions deliberately hide these wire frames.
#[test]
fn scoped_live_opening_waits_for_evidence_and_settlement() {
    for relayed in [false, true] {
        for delay in ["hold proof tail", "hold settlement"] {
            let server = open(90, true);
            seed(&server);
            let relay = open(2, false);
            let client = open(1, false);
            let (mode, held) = if relayed {
                connect(&client, &relay);
                edited(&relay, &server, delay)
            } else {
                edited(&client, &server, delay)
            };
            let nodes = [&client, &relay, &server];
            let mut stream = subscribe(&client, &query(true), "a");
            assert!(events(&mut stream, &nodes).is_empty(), "{delay}");
            assert_eq!(
                held.borrow().len(),
                if delay == "hold proof tail" { 2 } else { 1 }
            );
            if delay == "hold proof tail" {
                mode.set("release evidence");
                assert!(events(&mut stream, &nodes).is_empty());
                assert_eq!(held.borrow().len(), 1);
            }
            mode.set("release");
            expect_a_window(&opening(&mut stream, &nodes));
            assert!(held.borrow().is_empty());
            assert!(events(&mut stream, &nodes).is_empty());
        }
    }
}

/// Mallory alters one part of a real authority response to Alice's backend.
/// Direct and relayed streams must refuse without publishing a result, even
/// when a later valid frame follows the invalid one in the same batch. Wire
/// interception exercises protocol admission independently of query execution.
#[test]
fn scoped_live_rejects_malformed_or_downgraded_sequences() {
    for relayed in [false, true] {
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
            "duplicate evidence",
            "wrong settlement cut",
            "local settlement",
            "duplicate settlement",
        ] {
            let server = open(90, true);
            seed(&server);
            let relay = open(2, false);
            let client = open(1, false);
            let _editor = if relayed {
                connect(&client, &relay);
                edited(&relay, &server, mode)
            } else {
                edited(&client, &server, mode)
            };
            let mut stream = subscribe(&client, &query(true), "a");
            let mut refused = false;
            for event in events(&mut stream, &[&client, &relay, &server]) {
                match event {
                    SubscriptionEvent::Rejected { .. } => refused = true,
                    SubscriptionEvent::Closed if refused => {}
                    event => panic!("{mode}, relayed={relayed}: {event:?}"),
                }
            }
            assert!(
                refused,
                "{mode}, relayed={relayed}: invalid sequence must refuse"
            );
        }
    }
}
