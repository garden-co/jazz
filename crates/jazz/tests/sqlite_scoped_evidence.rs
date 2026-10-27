//! Trusted backends re-evaluate delegated reads against exact authority evidence.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, InsertOptions, ReadOpts, ReadTier, ScopedReadRowV1,
    SerializedReadResult, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn schema() -> JazzSchema {
    let resource =
        || PolicyExpr::eq_session("resource", vec!["__jazz_outer_row".into(), "id".into()]);
    let grant = PolicyExpr::Exists {
        table: "grants".into(),
        condition: Box::new(PolicyExpr::And(vec![
            resource(),
            PolicyExpr::eq_session("account", vec!["user".into(), "account".into()]),
            PolicyExpr::eq_session("token", vec!["claims".into(), "token".into()]),
        ])),
    };
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(grant)),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .fk_column("resource", "docs")
                    .column("account", ColumnType::Uuid)
                    .column("token", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::False)),
            )
            .table(
                TableSchemaBuilder::new("bans")
                    .fk_column("resource", "docs")
                    .policies(TablePolicies::new().with_select(PolicyExpr::False)),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8, authority: bool) -> Db {
    let schema = if authority {
        schema()
    } else {
        schema().without_permissions()
    };
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
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
fn seed(db: &Db, token: &str) {
    insert(
        db,
        "docs",
        10,
        &[("body", Value::String("private document".into()))],
    );
    insert(
        db,
        "grants",
        20,
        &[
            ("resource", Value::Uuid(id(10).0)),
            ("account", Value::Uuid(alice().account_id().unwrap().0)),
            ("token", Value::String(token.into())),
        ],
    );
}
fn tick(nodes: &[&Db]) {
    for node in nodes {
        block_on(node.tick()).unwrap();
    }
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut pending = pin!(future);
    for _ in 0..200 {
        if let Poll::Ready(value) = pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        tick(nodes);
    }
    panic!("scoped read did not settle");
}
async fn read(db: &Db, table: &str, token: &str) -> Result<usize, Error> {
    let query = postcard::to_allocvec(&Query::from(table)).unwrap();
    let result = db
        .all_serialized_query_as(
            &query,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            Some((
                alice(),
                BTreeMap::from([(
                    jazz::query::provider_claim_key("token"),
                    Value::String(token.into()),
                )]),
            )),
            Some(alice()),
            false,
            || false,
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows")
    };
    Ok(rows.len())
}

/// Alice's trusted backend and relay may hold private policy rows. They still
/// deny their contents to Alice and isolate concurrent claims, including a
/// chunked token whose suffix differs after the stored sort prefix.
#[test]
fn private_evidence_preserves_request_claims_and_chunked_predicates() {
    for relayed in [false, true] {
        let server = open(80, true);
        let token = format!("{}-alice", "a".repeat(70 * 1024));
        seed(&server, &token);
        let backend = open(81, false);
        let relay = open(82, false);
        let (up, down) = duplex();
        block_on(backend.connect_upstream(up));
        if relayed {
            relay.accept_subscriber(down, AuthorSubject::SYSTEM);
            let (up, down) = duplex();
            block_on(relay.connect_upstream(up));
            server.accept_subscriber(down, AuthorSubject::SYSTEM);
        } else {
            server.accept_subscriber(down, AuthorSubject::SYSTEM);
        }
        let nodes = [&server, &relay, &backend];
        let wrong = format!("{}-mallory", "a".repeat(70 * 1024));
        let mut allowed = pin!(read(&backend, "docs", &token));
        let mut denied = pin!(read(&backend, "docs", &wrong));
        let (mut a, mut b) = (None, None);
        for _ in 0..200 {
            if a.is_none() {
                if let Poll::Ready(result) = allowed
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                {
                    a = Some(result);
                }
            }
            if b.is_none() {
                if let Poll::Ready(result) = denied
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                {
                    b = Some(result);
                }
            }
            if a.is_some() && b.is_some() {
                break;
            }
            tick(&nodes);
        }
        assert_eq!(a.unwrap().unwrap(), 1, "relayed={relayed}");
        assert_eq!(b.unwrap().unwrap(), 0, "claims leaked between requests");
        assert_eq!(drive(&nodes, read(&backend, "grants", &token)).unwrap(), 0);
    }
}

/// Alice first caches a grant in a trusted backend. After Bob removes or retargets
/// it, the authority's exact answer excludes that stale grant and protected row.
#[test]
fn scoped_permissions_exclude_stale_deleted_and_retargeted_grants() {
    for delete in [false, true] {
        let server = open(83, true);
        seed(&server, "alice");
        let backend = open(84, false);
        let (up, down) = duplex();
        block_on(backend.connect_upstream(up));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let nodes = [&server, &backend];
        let query = postcard::to_allocvec(&Query::from("grants")).unwrap();
        let cached = drive(
            &nodes,
            backend.all_serialized_query_as(
                &query,
                ReadOpts::for_read_tier(ReadTier::Remote),
                None,
                None,
                None,
                false,
                || false,
                |_| {},
            ),
        )
        .unwrap();
        let SerializedReadResult::Rows(cached) = cached else {
            panic!("rows")
        };
        assert_eq!(cached.len(), 1);
        assert_eq!(drive(&nodes, read(&backend, "docs", "alice")).unwrap(), 1);
        if delete {
            block_on(server.delete("grants", id(20), Default::default())).unwrap();
        } else {
            block_on(server.update(
                "grants",
                id(20),
                BTreeMap::from([("resource".into(), Value::Uuid(id(11).0))]),
                Default::default(),
            ))
            .unwrap();
        }
        assert_eq!(
            drive(&nodes, read(&backend, "docs", "alice")).unwrap(),
            0,
            "delete={delete}"
        );
    }
}

/// Mallory cannot obtain raw policy dependencies by sending a delegated fetch
/// on an ordinary session connection; the authority refuses it before rows.
#[test]
fn ordinary_sessions_cannot_request_private_permission_evidence() {
    let server = open(85, true);
    seed(&server, "alice");
    let (mut attacker, down) = duplex();
    server.accept_subscriber(down, alice());
    attacker
        .send(SyncMessage::ScopedFetchV1 {
            id: 77,
            query: Query::from("docs"),
            binding: BTreeMap::new(),
            author: alice().canonical().into(),
            claims: BTreeMap::from([(
                jazz::query::provider_claim_key("token"),
                Value::String("alice".into()),
            )]),
            include_deleted: false,
        })
        .unwrap();
    tick(&[&server]);
    let mut refused = false;
    while let Some(message) = attacker.try_recv() {
        match message {
            SyncMessage::Refused { id: 77, .. } => refused = true,
            SyncMessage::Rows { images } => assert!(images.is_empty(), "private data escaped"),
            SyncMessage::ScopedEvidenceAnswerV1 { .. } => {
                panic!("ordinary session received evidence")
            }
            _ => {}
        }
    }
    assert!(refused);
}

// Low-level fault injection is necessary to check the receipt and row batch
// commit together; public APIs cannot manufacture an invalid authority frame.
struct Poisoned {
    wire: Box<dyn Transport>,
    pending: VecDeque<SyncMessage>,
    receipt: Rc<RefCell<Option<SyncMessage>>>,
}
impl Transport for Poisoned {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.wire.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if let Some(message) = self.pending.pop_front() {
            return Some(message);
        }
        let message = self.wire.try_recv()?;
        if let SyncMessage::ScopedEvidenceAnswerV1 { id, .. } = &message {
            *self.receipt.borrow_mut() = Some(message.clone());
            self.pending.push_back(SyncMessage::ScopedEvidenceAnswerV1 {
                id: *id,
                through: 1 << 52,
                rows: vec![],
                evidence: vec![],
            });
        }
        Some(message)
    }
}

/// A malformed frame after Alice's valid receipt rolls back the entire incoming
/// batch. Her pending read cannot complete using that rolled-back receipt.
#[test]
fn failed_sync_batch_does_not_publish_its_scoped_receipt() {
    let server = open(86, true);
    seed(&server, "alice");
    let backend = open(87, false);
    let receipt = Rc::new(RefCell::new(None));
    let (up, down) = duplex();
    block_on(backend.connect_upstream(Box::new(Poisoned {
        wire: up,
        pending: VecDeque::new(),
        receipt: receipt.clone(),
    })));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let mut read = pin!(read(&backend, "docs", "alice"));
    let mut failed = false;
    for _ in 0..100 {
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        block_on(server.tick()).unwrap();
        if let Err(error) = block_on(backend.tick()) {
            assert!(
                error.to_string().contains("invalid scoped evidence"),
                "{error}"
            );
            failed = true;
            break;
        }
    }
    assert!(failed && receipt.borrow().is_some());
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "rolled-back receipt settled the request"
    );
}

/// Byte fixtures are necessary to pin the append-only wire tag and coordinate
/// order independently of serde. Alice's receipt is transient, not a new durable
/// row encoding; existing sync tags retain their original bytes.
#[test]
fn scoped_evidence_v1_has_frozen_wire_bytes() {
    let message = SyncMessage::ScopedEvidenceAnswerV1 {
        id: 17,
        through: 128,
        rows: vec![[0x22; 16]],
        evidence: vec![ScopedReadRowV1 {
            table: "docs".into(),
            id: [0x22; 16],
            key: vec![],
            seq: 127,
        }],
    };
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v3/scoped-evidence.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["role"],
        "jazz.sqlite.sync.scoped-evidence-answer.v1"
    );
    assert_eq!(fixture["version"], 3);
    let bytes = hex::decode(fixture["answer"].as_str().unwrap()).unwrap();
    assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
    let decoded = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(message).unwrap()
    );
}

// A transport gate creates the real rows-before-receipt ordering without
// changing storage or bypassing sync; only the receipt is held temporarily.
struct DelayedReceipt {
    wire: Box<dyn Transport>,
    held: Rc<RefCell<VecDeque<SyncMessage>>>,
    release: Rc<std::cell::Cell<bool>>,
}
impl Transport for DelayedReceipt {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.wire.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.release.get()
            && let Some(message) = self.held.borrow_mut().pop_front()
        {
            return Some(message);
        }
        while let Some(message) = self.wire.try_recv() {
            if !self.release.get() && matches!(message, SyncMessage::ScopedEvidenceAnswerV1 { .. })
            {
                self.held.borrow_mut().push_back(message);
            } else {
                return Some(message);
            }
        }
        None
    }
}

/// Alice's read is authorized before Bob changes the grant. The backend then
/// caches Bob's newer version before receiving Alice's receipt; rechecking uses
/// the exact older version while her next request observes revocation.
#[test]
fn delayed_scoped_receipt_uses_its_exact_historical_dependency() {
    let server = open(88, true);
    seed(&server, "alice");
    let backend = open(89, false);
    let held = Rc::new(RefCell::new(VecDeque::new()));
    let release = Rc::new(std::cell::Cell::new(false));
    let (up, down) = duplex();
    block_on(backend.connect_upstream(Box::new(DelayedReceipt {
        wire: up,
        held: held.clone(),
        release: release.clone(),
    })));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let nodes = [&server, &backend];
    let mut first = pin!(read(&backend, "docs", "alice"));
    for _ in 0..100 {
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        tick(&nodes);
        if !held.borrow().is_empty() {
            break;
        }
    }
    assert_eq!(held.borrow().len(), 1);
    block_on(server.update(
        "grants",
        id(20),
        BTreeMap::from([("token".into(), Value::String("revoked".into()))]),
        Default::default(),
    ))
    .unwrap();
    let query = postcard::to_allocvec(&Query::from("grants")).unwrap();
    let cached = drive(
        &nodes,
        backend.all_serialized_query_as(
            &query,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let SerializedReadResult::Rows(cached) = cached else {
        panic!("rows")
    };
    assert_eq!(cached.len(), 1);
    release.set(true);
    assert_eq!(drive(&nodes, first).unwrap(), 1);
    assert_eq!(drive(&nodes, read(&backend, "docs", "alice")).unwrap(), 0);
}

async fn scoped_ordered_ids(db: &Db, query: Query) -> Result<Vec<RowUuid>, Error> {
    let query = postcard::to_allocvec(&query).unwrap();
    let result = db
        .all_serialized_query_as(
            &query,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            Some((
                alice(),
                BTreeMap::from([(
                    jazz::query::provider_claim_key("token"),
                    Value::String("alice".into()),
                )]),
            )),
            Some(alice()),
            false,
            || false,
            |_| {},
        )
        .await?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows")
    };
    Ok(rows.into_iter().map(|row| row.row_uuid()).collect())
}
fn ordered_documents(server: &Db) {
    for (n, suffix) in [(11, "z"), (12, "a"), (13, "b"), (14, "a"), (15, "short")] {
        let body = if suffix == "short" {
            suffix.to_owned()
        } else {
            format!("{}{suffix}", "x".repeat(70 * 1024))
        };
        insert(server, "docs", n, &[("body", Value::String(body))]);
        insert(
            server,
            "grants",
            n + 30,
            &[
                ("resource", Value::Uuid(id(n).0)),
                ("account", Value::Uuid(alice().account_id().unwrap().0)),
                ("token", Value::String("alice".into())),
            ],
        );
    }
}

/// Alice's direct backend and trusted relay sort permitted documents by logical
/// text, including values with equal long prefixes and duplicate contents. An
/// unselected sort key still orders correctly, and the authority's window is
/// applied once in either direction.
#[test]
fn scoped_text_ordering_preserves_large_value_ties_and_windows() {
    use jazz::query::OrderDirection;
    for relayed in [false, true] {
        let server = open(90, true);
        ordered_documents(&server);
        let backend = open(91, false);
        let relay = open(92, false);
        let (up, down) = duplex();
        block_on(backend.connect_upstream(up));
        if relayed {
            relay.accept_subscriber(down, AuthorSubject::SYSTEM);
            let (up, down) = duplex();
            block_on(relay.connect_upstream(up));
            server.accept_subscriber(down, AuthorSubject::SYSTEM);
        } else {
            server.accept_subscriber(down, AuthorSubject::SYSTEM);
        }
        let nodes = [&server, &relay, &backend];
        for (direction, expected) in [
            (OrderDirection::Asc, vec![15, 12, 14, 13, 11]),
            (OrderDirection::Desc, vec![11, 13, 14, 12, 15]),
        ] {
            let query = Query::from("docs")
                .select(["id"])
                .order_by("body", direction);
            let all = drive(&nodes, scoped_ordered_ids(&backend, query.clone())).unwrap();
            assert_eq!(
                all,
                expected.iter().copied().map(id).collect::<Vec<_>>(),
                "relayed={relayed}, direction={direction:?}"
            );
            let page = drive(
                &nodes,
                scoped_ordered_ids(&backend, query.offset(1).limit(3)),
            )
            .unwrap();
            assert_eq!(
                page,
                expected[1..4].iter().copied().map(id).collect::<Vec<_>>()
            );
        }
    }
}

/// Alice's backend retains Bob's unrelated document with missing chunks. Her
/// scoped sort must not read or hydrate it. Raw SQLite only injects that fault;
/// setup, synchronization and assertions use public APIs and isolated storage.
#[test]
fn scoped_text_ordering_does_not_hydrate_unrelated_cached_values() {
    use jazz::query::{OrderDirection, col, eq, lit};
    let server = open(93, true);
    insert(
        &server,
        "docs",
        99,
        &[(
            "body",
            Value::String(format!(
                "{}{}",
                "x".repeat(70 * 1024),
                "unrelated".repeat(10_000)
            )),
        )],
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("backend.sqlite");
    let config = || {
        DbConfig::new(
            schema().without_permissions(),
            DbIdentity {
                node: NodeUuid::from_bytes([94; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    {
        let backend = block_on(Db::open_at(config(), &path, Default::default())).unwrap();
        let (up, down) = duplex();
        block_on(backend.connect_upstream(up));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let query = postcard::to_allocvec(&Query::from("docs")).unwrap();
        let cached = drive(
            &[&server, &backend],
            backend.all_serialized_query_as(
                &query,
                ReadOpts::for_read_tier(ReadTier::Remote),
                None,
                None,
                None,
                false,
                || false,
                |_| {},
            ),
        )
        .unwrap();
        let SerializedReadResult::Rows(cached) = cached else {
            panic!("rows")
        };
        assert_eq!(cached.len(), 1);
    }
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert!(conn.execute("DELETE FROM _jazz_lv_node", []).unwrap() > 0);
    }
    ordered_documents(&server);
    let backend = block_on(Db::open_at(config(), &path, Default::default())).unwrap();
    let (up, down) = duplex();
    block_on(backend.connect_upstream(up));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let rows = drive(
        &[&server, &backend],
        scoped_ordered_ids(
            &backend,
            Query::from("docs")
                .select(["id"])
                .order_by("body", OrderDirection::Asc),
        ),
    )
    .unwrap();
    assert_eq!(rows, [15, 12, 14, 13, 11].map(id));
    let unrelated = backend
        .prepare_query(&Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(id(99).0)))))
        .unwrap();
    let error = block_on(backend.all_for_identity(
        &unrelated,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        AuthorSubject::SYSTEM,
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("chunk"),
        "unrelated damaged value was unexpectedly hydrated: {error}"
    );
}

fn accepted_scoped(
    db: &Db,
    tx: jazz::tools::OpenTransactionId,
) -> impl Future<Output = Result<jazz::db::BindingSettledRead, Error>> + 'static {
    let prepared = db
        .prepare_query(&Query::from("docs"))
        .unwrap()
        .with_identity_claims(
            alice(),
            BTreeMap::from([(
                jazz::query::provider_claim_key("token"),
                Value::String("alice".into()),
            )]),
        );
    db.all_settled(
        prepared,
        ReadOpts::for_read_tier(ReadTier::Remote),
        tx,
        false,
        Some(alice()),
    )
    .unwrap()
}
fn accepted_scoped_query(
    db: &Db,
    tx: jazz::tools::OpenTransactionId,
    query: Query,
    at_begin: bool,
) -> impl Future<Output = Result<jazz::db::BindingSettledRead, Error>> + 'static {
    let prepared = db.prepare_query(&query).unwrap().with_identity_claims(
        alice(),
        BTreeMap::from([(
            jazz::query::provider_claim_key("token"),
            Value::String("alice".into()),
        )]),
    );
    db.all_settled(
        prepared,
        ReadOpts::for_read_tier(ReadTier::Remote),
        tx,
        at_begin,
        Some(alice()),
    )
    .unwrap()
}
fn scoped_backend_connection(server: &Db, backend: &Db, relay: &Db, relayed: bool) {
    let (up, down) = duplex();
    block_on(backend.connect_upstream(up));
    if relayed {
        relay.accept_subscriber(down, AuthorSubject::SYSTEM);
        let (up, down) = duplex();
        block_on(relay.connect_upstream(up));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
    } else {
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
    }
}

/// Alice's cold trusted backend and relay obtain rows and accepted content
/// metadata under her private grant. A read-only transaction still sends the
/// original query to the authority for Global validation.
#[test]
fn accepted_scoped_reads_commit_through_private_permission_evidence() {
    for relayed in [false, true] {
        let server = open(100, true);
        seed(&server, "alice");
        let backend = open(101, false);
        let relay = open(102, false);
        scoped_backend_connection(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        let tx = jazz::tools::OpenTransactionId::new();
        block_on(backend.begin_serializable_for_identity(tx, alice())).unwrap();
        let read = drive(&nodes, accepted_scoped(&backend, tx)).unwrap();
        assert_eq!(read.rows.len(), 1, "relayed={relayed}");
        assert_eq!(read.settlements.len(), 1);
        let receipt = block_on(backend.commit_serializable_handle(tx)).unwrap();
        drive(
            &nodes,
            backend.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
        )
        .unwrap();
    }
}

/// Bob retargets Alice's grant after her first accepted read pinned a cut.
/// The next read uses its historical UUID index and returns the same content;
/// the eventual transaction cannot commit against the now-revoked grant.
#[test]
fn accepted_scoped_reads_keep_historical_grants_and_reject_stale_commit() {
    for relayed in [false, true] {
        let server = open(103, true);
        seed(&server, "alice");
        let backend = open(104, false);
        let relay = open(105, false);
        scoped_backend_connection(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        let tx = jazz::tools::OpenTransactionId::new();
        block_on(backend.begin_serializable_for_identity(tx, alice())).unwrap();
        let first = drive(&nodes, accepted_scoped(&backend, tx)).unwrap();
        block_on(server.update(
            "grants",
            id(20),
            BTreeMap::from([("resource".into(), Value::Uuid(id(11).0))]),
            Default::default(),
        ))
        .unwrap();
        block_on(server.update(
            "grants",
            id(20),
            BTreeMap::from([("token".into(), Value::String("revoked".into()))]),
            Default::default(),
        ))
        .unwrap();
        let second = drive(&nodes, accepted_scoped(&backend, tx)).unwrap();
        assert_eq!(first, second, "relayed={relayed}");
        match block_on(backend.commit_serializable_handle(tx)) {
            Ok(receipt) => assert!(
                drive(
                    &nodes,
                    backend.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global)
                )
                .is_err()
            ),
            Err(error) => assert_eq!(error.code, jazz::db::ErrorCode::TransactionConflict),
        }
        assert_eq!(drive(&nodes, read(&backend, "docs", "alice")).unwrap(), 0);
    }
}

/// Alice's backend never caches Bob's new matching document. Global commit
/// still detects that phantom from the original unfiltered query, rather than
/// validating only the receipt's cached root IDs.
#[test]
fn accepted_scoped_read_only_commit_detects_uncached_phantoms() {
    let server = open(106, true);
    seed(&server, "alice");
    let backend = open(107, false);
    let relay = open(108, false);
    scoped_backend_connection(&server, &backend, &relay, true);
    let nodes = [&server, &relay, &backend];
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(backend.begin_serializable_for_identity(tx, alice())).unwrap();
    assert_eq!(
        drive(&nodes, accepted_scoped(&backend, tx))
            .unwrap()
            .rows
            .len(),
        1
    );
    insert(
        &server,
        "docs",
        11,
        &[("body", Value::String("phantom".into()))],
    );
    insert(
        &server,
        "grants",
        21,
        &[
            ("resource", Value::Uuid(id(11).0)),
            ("account", Value::Uuid(alice().account_id().unwrap().0)),
            ("token", Value::String("alice".into())),
        ],
    );
    let receipt = block_on(backend.commit_serializable_handle(tx)).unwrap();
    let error = drive(
        &nodes,
        backend.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
    )
    .unwrap_err();
    assert!(error.is_serialization_conflict(), "{error}");
    assert!(error.is_conflict(), "authority conflicts remain retryable");
}

/// A byte fixture is necessary to distinguish Alice's accepted query coverage
/// from ordinary scoped evidence. It pins the append-only tag and field order.
#[test]
fn accepted_scoped_evidence_v1_has_frozen_wire_bytes() {
    let message = SyncMessage::AcceptedScopedEvidenceAnswerV1 {
        id: 17,
        through: 128,
        rows: vec![[0x22; 16]],
        evidence: vec![ScopedReadRowV1 {
            table: "docs".into(),
            id: [0x22; 16],
            key: vec![],
            seq: 127,
        }],
    };
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v3/accepted-scoped-evidence.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["role"],
        "jazz.sqlite.sync.accepted-scoped-evidence-answer.v1"
    );
    assert_eq!(fixture["version"], 3);
    let bytes = hex::decode(fixture["answer"].as_str().unwrap()).unwrap();
    assert_eq!(jazz::db::encode_message(&message).unwrap(), bytes);
    let decoded = jazz::db::decode_message(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        serde_json::to_value(message).unwrap()
    );
}

/// Alice reopens a store created before the derived history index existed. The
/// index is installed without rewriting Bob's history. Direct SQLite inspection
/// is necessary to pin the index name/DDL and verify unchanged physical rows.
#[test]
fn permission_history_index_v1_installs_on_reopen_without_rewriting_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let config = || {
        DbConfig::new(
            schema(),
            DbIdentity {
                node: NodeUuid::from_bytes([112; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    {
        let db = block_on(Db::open_at(config(), &path, Default::default())).unwrap();
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
        seed(&db, "alice");
        block_on(db.update(
            "grants",
            id(20),
            BTreeMap::from([("resource".into(), Value::Uuid(id(11).0))]),
            Default::default(),
        ))
        .unwrap();
    }
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite_permission_history_index_v1.json"
    ))
    .unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.permission-history-index.v1");
    assert_eq!(fixture["version"], 1);
    let name = fixture["name"].as_str().unwrap();
    let rows = |conn: &rusqlite::Connection| {
        let mut statement = conn
            .prepare("SELECT * FROM grants__h ORDER BY id,_b,_seq")
            .unwrap();
        let columns = statement.column_count();
        statement
            .query_map([], |row| {
                (0..columns)
                    .map(|column| row.get::<_, rusqlite::types::Value>(column))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    let before = {
        let conn = rusqlite::Connection::open(&path).unwrap();
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?1",
                [name],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sql, fixture["sql"].as_str().unwrap());
        let rows = rows(&conn);
        assert_eq!(rows.len(), 1);
        conn.execute_batch("DROP INDEX \"grants__h_permission_v1(resource)\"")
            .unwrap();
        rows
    };
    drop(block_on(Db::open_at(config(), &path, Default::default())).unwrap());
    let conn = rusqlite::Connection::open(&path).unwrap();
    let sql: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?1",
            [name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sql, fixture["sql"].as_str().unwrap());
    assert_eq!(rows(&conn), before);
}

fn writable_scoped_schema() -> JazzSchema {
    let grant = PolicyExpr::Exists {
        table: "grants".into(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::eq_session("resource", vec!["__jazz_outer_row".into(), "id".into()]),
            PolicyExpr::eq_session("account", vec!["user".into(), "account".into()]),
            PolicyExpr::eq_session("token", vec!["claims".into(), "token".into()]),
        ])),
    };
    let writes = |select| {
        TablePolicies::new()
            .with_select(select)
            .with_insert(PolicyExpr::True)
            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
            .with_delete(PolicyExpr::True)
    };
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("body", ColumnType::Text)
                    .policies(writes(grant)),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .fk_column("resource", "docs")
                    .column("account", ColumnType::Uuid)
                    .column("token", ColumnType::Text)
                    .policies(writes(PolicyExpr::eq_session(
                        "account",
                        vec!["user".into(), "account".into()],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("markers")
                    .column("body", ColumnType::Text)
                    .policies(writes(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap()
}

/// Alice can stage an unrelated marker before reading accepted documents. A
/// staged document or grant write instead requires authority overlay evidence:
/// the backend must refuse, not silently certify its provisional result.
#[test]
fn accepted_scoped_reads_distinguish_unrelated_and_dependency_writes() {
    for table in ["markers", "docs", "grants"] {
        let make = |node, authority| {
            let schema = writable_scoped_schema();
            let db = block_on(Db::open(DbConfig::new(
                if authority {
                    schema
                } else {
                    schema.without_permissions()
                },
                DbIdentity {
                    node: NodeUuid::from_bytes([node; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            )))
            .unwrap();
            if authority {
                db.set_authority();
                db.initialize_authority_catalogue().unwrap();
            }
            db
        };
        let server = make(113, true);
        seed(&server, "alice");
        let backend = make(114, false);
        let (up, down) = duplex();
        block_on(backend.connect_upstream(up));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let nodes = [&server, &backend];
        assert_eq!(drive(&nodes, read(&backend, "docs", "alice")).unwrap(), 1);
        let view =
            block_on(backend.register_schema_view(writable_scoped_schema().without_permissions()))
                .unwrap();
        view.set_identity_claims(
            alice(),
            BTreeMap::from([(
                jazz::query::provider_claim_key("token"),
                Value::String("alice".into()),
            )]),
        );
        let tx = jazz::tools::OpenTransactionId::new();
        block_on(view.begin_serializable_for_identity(tx, alice())).unwrap();
        if table == "markers" {
            block_on(view.tx_ref(tx).insert(
                "markers",
                BTreeMap::from([("body".into(), Value::String("unrelated".into()))]),
                Default::default(),
            ))
            .unwrap();
        } else {
            let (row, column) = if table == "docs" {
                (id(10), "body")
            } else {
                (id(20), "token")
            };
            block_on(view.tx_ref(tx).update(
                table,
                row,
                BTreeMap::from([(column.into(), Value::String("changed".into()))]),
                Default::default(),
            ))
            .unwrap();
        }
        let result = drive(&nodes, accepted_scoped(&view, tx));
        if table == "markers" {
            assert_eq!(result.unwrap().rows.len(), 1);
            let receipt = block_on(view.commit_serializable_handle(tx)).unwrap();
            drive(
                &nodes,
                view.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
            )
            .unwrap();
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
            assert!(
                error.to_string().contains("authority overlay evidence"),
                "{error}"
            );
            assert!(block_on(view.commit_serializable_handle(tx)).is_err());
        }
    }
}

// Raw transport fault injection is necessary: the public accepted-read API
// never labels an ordinary scoped receipt as accepted query coverage.
struct WrongAcceptedKind(Box<dyn Transport>);
impl Transport for WrongAcceptedKind {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.0.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.0.try_recv().map(|message| match message {
            SyncMessage::AcceptedScopedEvidenceAnswerV1 {
                id,
                through,
                rows,
                evidence,
            } => SyncMessage::ScopedEvidenceAnswerV1 {
                id,
                through,
                rows,
                evidence,
            },
            other => other,
        })
    }
}

/// Alice's pending accepted read cannot be completed by an ordinary scoped
/// receipt, even when its row IDs, sequence and permission inputs are identical.
#[test]
fn ordinary_scoped_evidence_cannot_satisfy_an_accepted_read() {
    let server = open(115, true);
    seed(&server, "alice");
    let backend = open(116, false);
    let (up, down) = duplex();
    block_on(backend.connect_upstream(Box::new(WrongAcceptedKind(up))));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(backend.begin_serializable_for_identity(tx, alice())).unwrap();
    {
        let mut future = pin!(accepted_scoped(&backend, tx));
        let mut rejected = false;
        for _ in 0..100 {
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            block_on(server.tick()).unwrap();
            if let Err(error) = block_on(backend.tick()) {
                assert_eq!(error.code, jazz::db::ErrorCode::Protocol);
                assert!(
                    error.to_string().contains("different request kind"),
                    "{error}"
                );
                rejected = true;
                break;
            }
        }
        assert!(rejected);
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert!(block_on(backend.commit_serializable_handle(tx)).is_err());
}

/// An ordinary Alice connection can fetch her authorized document with accepted
/// metadata. Only trusted backend connections may receive raw grant evidence.
#[test]
fn ordinary_accepted_connections_do_not_receive_private_dependency_rows() {
    let server = open(117, true);
    seed(&server, "alice");
    let claims = BTreeMap::from([(
        jazz::query::provider_claim_key("token"),
        Value::String("alice".into()),
    )]);
    let (mut peer, down) = duplex();
    server.accept_subscriber_with_claims(down, alice(), Some(claims.clone()));
    peer.send(SyncMessage::AcceptedFetchV1 {
        id: 77,
        query: Query::from("docs"),
        binding: BTreeMap::new(),
        at: None,
        include_deleted: false,
        scope: Some((alice().canonical().into(), claims)),
    })
    .unwrap();
    block_on(server.tick()).unwrap();
    let mut covered = false;
    let mut docs = 0;
    while let Some(message) = peer.try_recv() {
        match message {
            SyncMessage::AcceptedAnswerV1 { id: 77, rows, .. } => {
                assert_eq!(rows, vec![*id(10).as_bytes()]);
                covered = true;
            }
            SyncMessage::Rows { images } => {
                for image in images {
                    assert_eq!(image.table, "docs");
                    docs += 1;
                }
            }
            SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. }
            | SyncMessage::ScopedEvidenceAnswerV1 { .. } => {
                panic!("ordinary connection received private evidence")
            }
            _ => {}
        }
    }
    assert!(covered && docs == 1);
}

/// Alice's accepted window hydrates a bounded prefix through a cold backend or
/// relay. Chunked, unselected sort keys retain logical order at the pinned cut
/// after Bob changes one of those keys, without applying the offset twice.
#[test]
fn accepted_scoped_windows_keep_chunked_historical_order() {
    use jazz::query::OrderDirection;
    for relayed in [false, true] {
        let server = open(118, true);
        ordered_documents(&server);
        let backend = open(119, false);
        let relay = open(120, false);
        scoped_backend_connection(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        let tx = jazz::tools::OpenTransactionId::new();
        block_on(backend.begin_serializable_for_identity(tx, alice())).unwrap();
        let query = Query::from("docs")
            .select(["id"])
            .order_by("body", OrderDirection::Asc)
            .offset(1)
            .limit(3);
        let first = drive(
            &nodes,
            accepted_scoped_query(&backend, tx, query.clone(), false),
        )
        .unwrap();
        assert_eq!(
            first
                .rows
                .iter()
                .map(|row| row.row_uuid())
                .collect::<Vec<_>>(),
            [12, 14, 13].map(id)
        );
        assert_eq!(first.settlements.len(), 3);
        block_on(server.update(
            "docs",
            id(12),
            BTreeMap::from([("body".into(), Value::String("first now".into()))]),
            Default::default(),
        ))
        .unwrap();
        let second = drive(&nodes, accepted_scoped_query(&backend, tx, query, false)).unwrap();
        assert_eq!(first, second, "relayed={relayed}");
    }
}

/// Alice may require that a Remote result was already admitted at transaction
/// begin. New scoped receipts cannot backfill that observation; a new batch can
/// use the same evidence once it has arrived, including through a relay.
#[test]
fn accepted_scoped_begin_requirement_preserves_evidence_admission() {
    for relayed in [false, true] {
        let server = open(121, true);
        seed(&server, "alice");
        let backend = open(122, false);
        let relay = open(123, false);
        scoped_backend_connection(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        let cold = jazz::tools::OpenTransactionId::new();
        block_on(backend.begin_serializable_for_identity(cold, alice())).unwrap();
        let error = drive(
            &nodes,
            accepted_scoped_query(&backend, cold, Query::from("docs"), true),
        )
        .unwrap_err();
        assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
        assert!(block_on(backend.commit_serializable_handle(cold)).is_err());
        let warm = jazz::tools::OpenTransactionId::new();
        block_on(backend.begin_serializable_for_identity(warm, alice())).unwrap();
        assert_eq!(
            drive(
                &nodes,
                accepted_scoped_query(&backend, warm, Query::from("docs"), true)
            )
            .unwrap()
            .rows
            .len(),
            1
        );
        let receipt = block_on(backend.commit_serializable_handle(warm)).unwrap();
        drive(
            &nodes,
            backend.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
        )
        .unwrap();
    }
}

/// Alice connects as an ordinary session to a trusted relay. The relay needs
/// private grant evidence upstream but must strip it from Alice's downstream
/// accepted answer while preserving her authorised document and content proof.
#[test]
fn ordinary_accepted_relay_connection_strips_private_evidence() {
    let server = open(124, true);
    seed(&server, "alice");
    let relay = open(125, false);
    let (up, down) = duplex();
    block_on(relay.connect_upstream(up));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let claims = BTreeMap::from([(
        jazz::query::provider_claim_key("token"),
        Value::String("alice".into()),
    )]);
    let (mut peer, down) = duplex();
    relay.accept_subscriber_with_claims(down, alice(), Some(claims.clone()));
    peer.send(SyncMessage::AcceptedFetchV1 {
        id: 78,
        query: Query::from("docs"),
        binding: BTreeMap::new(),
        at: None,
        include_deleted: false,
        scope: Some((alice().canonical().into(), claims)),
    })
    .unwrap();
    let mut covered = false;
    let mut docs = 0;
    for _ in 0..100 {
        tick(&[&server, &relay]);
        while let Some(message) = peer.try_recv() {
            match message {
                SyncMessage::AcceptedAnswerV1 { id: 78, rows, .. } => {
                    assert_eq!(rows, vec![*id(10).as_bytes()]);
                    covered = true;
                }
                SyncMessage::Rows { images } => {
                    for image in images {
                        assert_eq!(image.table, "docs");
                        docs += 1;
                    }
                }
                SyncMessage::AcceptedRowsV1 { rows } => {
                    for row in rows {
                        assert_eq!(row.table, "docs");
                    }
                }
                SyncMessage::AcceptedScopedEvidenceAnswerV1 { .. }
                | SyncMessage::ScopedEvidenceAnswerV1 { .. } => {
                    panic!("ordinary relay session received private evidence")
                }
                SyncMessage::Refused { .. } => panic!("ordinary relay read failed"),
                _ => {}
            }
        }
        if covered {
            break;
        }
    }
    assert!(covered && docs == 1);
}
