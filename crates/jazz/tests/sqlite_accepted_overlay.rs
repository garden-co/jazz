//! Accepted scoped reads validate their staged projection at the authority.
use jazz::db::{
    BindingSettledRead, Db, DbConfig, DbIdentity, Error, ErrorCode, InsertOptions, ReadOpts,
    ReadTier, SerializedReadResult, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit, provider_claim_key};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::DurabilityTier;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn claims() -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("token"), Value::String("alice".into()))])
}
fn schema() -> JazzSchema {
    let own = PolicyExpr::eq_session("account", vec!["user".into(), "account".into()]);
    let grant = PolicyExpr::Exists {
        table: "grants".into(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::eq_session("resource", vec!["__jazz_outer_row".into(), "id".into()]),
            own.clone(),
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
                    .policies(
                        TablePolicies::new()
                            .with_select(own.clone())
                            .with_insert(PolicyExpr::True)
                            .with_update(
                                Some(own.clone()),
                                PolicyExpr::And(vec![
                                    own,
                                    PolicyExpr::eq_session(
                                        "token",
                                        vec!["claims".into(), "token".into()],
                                    ),
                                ]),
                            )
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8, authority: bool) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        if authority {
            schema()
        } else {
            schema().without_permissions()
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
}
fn tick(nodes: &[&Db]) {
    for node in nodes {
        block_on(node.tick()).unwrap();
    }
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..250 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        tick(nodes);
    }
    panic!("accepted overlay operation did not settle")
}
fn connect(server: &Db, backend: &Db, relay: &Db, relayed: bool) {
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
fn fields(body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".into(), Value::String(body.into()))])
}
fn grant(resource: u8) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("resource".into(), Value::Uuid(id(resource).0)),
        (
            "account".into(),
            Value::Uuid(alice().account_id().unwrap().0),
        ),
        ("token".into(), Value::String("alice".into())),
    ])
}
fn seed(server: &Db) {
    block_on(server.insert(
        "docs",
        fields("middle"),
        InsertOptions {
            row_id: Some(id(10)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(server.insert(
        "docs",
        fields("last"),
        InsertOptions {
            row_id: Some(id(11)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(server.insert(
        "grants",
        grant(10),
        InsertOptions {
            row_id: Some(id(20)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn paired(
    db: &Db,
    tx: OpenTransactionId,
    query: Query,
) -> impl Future<Output = Result<BindingSettledRead, Error>> + 'static {
    let prepared = db
        .prepare_query(&query)
        .unwrap()
        .with_identity_claims(alice(), claims());
    db.all_settled(
        prepared,
        ReadOpts::for_read_tier(ReadTier::Remote),
        tx,
        false,
        Some(alice()),
    )
    .unwrap()
}
async fn fetch(db: &Db, query: Query) {
    let bytes = postcard::to_allocvec(&query).unwrap();
    let result = db
        .all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            Some((alice(), claims())),
            Some(alice()),
            false,
            || false,
            |_| {},
        )
        .await
        .unwrap();
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows")
    };
    assert!(!rows.is_empty(), "empty fetched {}", query.table);
}
fn view(backend: &Db) -> Db {
    let view = block_on(backend.register_schema_view(schema().without_permissions())).unwrap();
    view.set_identity_claims(alice(), claims());
    view
}
fn begin(db: &Db) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(tx, alice())).unwrap();
    tx
}
fn ids(read: &BindingSettledRead) -> Vec<RowUuid> {
    read.rows.iter().map(|row| row.row_uuid()).collect()
}
fn server_rows(server: &Db, table: &str) -> Vec<jazz::db::CurrentRow> {
    block_on(server.all(
        &server.prepare_query(&Query::from(table)).unwrap(),
        ReadOpts::default(),
    ))
    .unwrap()
}
fn server_count(server: &Db, table: &str) -> usize {
    server_rows(server, table).len()
}

/// Alice stages an unchanged grant before reading her document. Both direct and
/// relayed authority checks succeed without publishing the write. Only the
/// enclosing transaction's normal Global commit publishes the new grant state.
#[test]
fn unchanged_staged_grant_preserves_accepted_read() {
    for relayed in [false, true] {
        let server = open(1, true);
        seed(&server);
        let backend = open(2, false);
        let relay = open(3, false);
        connect(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        let p = server
            .prepare_query(&Query::from("grants"))
            .unwrap()
            .with_identity_claims(alice(), claims());
        assert_eq!(
            block_on(server.all_for_identity(&p, ReadOpts::default(), alice()))
                .unwrap()
                .len(),
            1,
            "authority grant, relayed={relayed}"
        );
        drive(&nodes, fetch(&backend, Query::from("grants")));
        let before = server_rows(&server, "grants");
        let view = view(&backend);
        let tx = begin(&view);
        block_on(view.serializable_tx_ref(tx).update(
            "grants",
            id(20),
            BTreeMap::from([("token".into(), Value::String("alice".into()))]),
            Default::default(),
        ))
        .unwrap();
        assert_eq!(
            ids(&drive(&nodes, paired(&view, tx, Query::from("docs"))).unwrap()),
            [id(10)]
        );
        assert_eq!(server_rows(&server, "grants"), before);
        let receipt = block_on(view.commit_serializable_handle(tx)).unwrap();
        drive(
            &nodes,
            view.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
    }
}

/// Bob's second document is already cached and readable by Alice. Changing it
/// must not block an accepted read filtered to the first document, even though
/// the staged write and read share the same table.
#[test]
fn staged_other_root_can_commit_after_filtered_accepted_read() {
    for relayed in [false, true] {
        let server = open(4, true);
        seed(&server);
        block_on(server.insert(
            "grants",
            grant(11),
            InsertOptions {
                row_id: Some(id(21)),
                ..Default::default()
            },
        ))
        .unwrap();
        let backend = open(5, false);
        let relay = open(6, false);
        connect(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        drive(&nodes, fetch(&backend, Query::from("docs")));
        let view = view(&backend);
        let tx = begin(&view);
        block_on(view.serializable_tx_ref(tx).update(
            "docs",
            id(11),
            fields("changed elsewhere"),
            Default::default(),
        ))
        .unwrap();
        let query = Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(id(10).0))));
        assert_eq!(
            ids(&drive(&nodes, paired(&view, tx, query)).unwrap()),
            [id(10)]
        );
        let receipt = block_on(view.commit_serializable_handle(tx)).unwrap();
        drive(
            &nodes,
            view.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
    }
}

/// Alice's staged grant exposes Bob's document that her backend never cached.
/// The authority detects that phantom, refuses the paired read, and rolls back
/// the simulated write. Dropping or failing the read cannot permit commit.
#[test]
fn staged_grant_cannot_expose_an_uncached_root() {
    for relayed in [false, true] {
        let server = open(7, true);
        seed(&server);
        let backend = open(8, false);
        let relay = open(9, false);
        connect(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        drive(&nodes, fetch(&backend, Query::from("docs")));
        let view = view(&backend);
        let tx = begin(&view);
        block_on(view.serializable_tx_ref(tx).insert(
            "grants",
            grant(11),
            InsertOptions {
                row_id: Some(id(21)),
                ..Default::default()
            },
        ))
        .unwrap();
        let error = drive(&nodes, paired(&view, tx, Query::from("docs"))).unwrap_err();
        assert_eq!(error.code, ErrorCode::NotObserved);
        assert!(
            error.to_string().contains("authority overlay evidence"),
            "{error}"
        );
        assert_eq!(server_count(&server, "grants"), 1);
        assert!(block_on(view.commit_serializable_handle(tx)).is_err());
    }
}

/// Alice's cold backend and relay prepare a row-local policy query before their
/// bootstrap schema receives authenticated policies. Both ordinary and paired
/// reads must return her grant under that governor, including its cold FK.
#[test]
fn cold_row_local_scopes_use_authenticated_policies() {
    for relayed in [false, true] {
        let server = open(10, true);
        seed(&server);
        let backend = open(11, false);
        let relay = open(12, false);
        connect(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        let tx = begin(&backend);
        let read = drive(&nodes, paired(&backend, tx, Query::from("grants"))).unwrap();
        assert_eq!(ids(&read), [id(20)]);
        drive(&nodes, fetch(&backend, Query::from("grants")));
        let receipt = block_on(backend.commit_serializable_handle(tx)).unwrap();
        drive(
            &nodes,
            backend.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
    }
}

/// Alice changes a chunked value outside her filtered accepted result. The
/// authority receives only her explicitly staged logical value, simulates it,
/// and leaves both rows and chunks unpublished until her normal commit.
#[test]
fn staged_chunked_other_root_preserves_accepted_result() {
    for relayed in [false, true] {
        let server = open(13, true);
        seed(&server);
        block_on(server.insert(
            "grants",
            grant(11),
            InsertOptions {
                row_id: Some(id(21)),
                ..Default::default()
            },
        ))
        .unwrap();
        let before = server_rows(&server, "docs");
        let backend = open(14, false);
        let relay = open(15, false);
        connect(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        drive(&nodes, fetch(&backend, Query::from("docs")));
        let view = view(&backend);
        let tx = begin(&view);
        block_on(view.serializable_tx_ref(tx).update(
            "docs",
            id(11),
            fields(&"x".repeat(70 * 1024)),
            Default::default(),
        ))
        .unwrap();
        let query = Query::from("docs").filter(eq(col("id"), lit(Value::Uuid(id(10).0))));
        assert_eq!(
            ids(&drive(&nodes, paired(&view, tx, query)).unwrap()),
            [id(10)]
        );
        assert_eq!(server_rows(&server, "docs"), before);
        let receipt = block_on(view.commit_serializable_handle(tx)).unwrap();
        drive(
            &nodes,
            view.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
    }
}

/// Alice's new grant can leave a limited result unchanged or displace it with
/// a previously uncached document. The authority compares her original window,
/// not just the previously returned root IDs or an incomplete replica cache.
#[test]
fn staged_grants_obey_the_original_ordered_window() {
    for relayed in [false, true] {
        for enters in [false, true] {
            let server = open(16, true);
            seed(&server);
            block_on(server.update(
                "docs",
                id(11),
                fields(if enters { "aaa" } else { "zzz" }),
                Default::default(),
            ))
            .unwrap();
            let backend = open(17, false);
            let relay = open(18, false);
            connect(&server, &backend, &relay, relayed);
            let nodes = [&server, &relay, &backend];
            drive(&nodes, fetch(&backend, Query::from("docs")));
            let view = view(&backend);
            let tx = begin(&view);
            block_on(view.serializable_tx_ref(tx).insert(
                "grants",
                grant(11),
                InsertOptions {
                    row_id: Some(id(21)),
                    ..Default::default()
                },
            ))
            .unwrap();
            let query = Query::from("docs")
                .order_by("body", OrderDirection::Asc)
                .limit(1);
            let result = drive(&nodes, paired(&view, tx, query));
            assert_eq!(server_count(&server, "grants"), 1);
            if enters {
                assert_eq!(result.unwrap_err().code, ErrorCode::NotObserved);
                assert!(block_on(view.commit_serializable_handle(tx)).is_err());
            } else {
                assert_eq!(ids(&result.unwrap()), [id(10)]);
                let receipt = block_on(view.commit_serializable_handle(tx)).unwrap();
                drive(
                    &nodes,
                    view.wait_for_transaction(receipt.0, DurabilityTier::Global),
                )
                .unwrap();
            }
        }
    }
}

/// Bob changes Alice's staged write target after she pins a cut. The authority
/// refuses that stale write before simulating it; its later state is preserved.
#[test]
fn stale_overlay_write_target_is_a_transaction_conflict() {
    for relayed in [false, true] {
        let server = open(19, true);
        seed(&server);
        let backend = open(20, false);
        let relay = open(21, false);
        connect(&server, &backend, &relay, relayed);
        let nodes = [&server, &relay, &backend];
        drive(&nodes, fetch(&backend, Query::from("grants")));
        let view = view(&backend);
        let tx = begin(&view);
        assert_eq!(
            ids(&drive(&nodes, paired(&view, tx, Query::from("docs"))).unwrap()),
            [id(10)]
        );
        block_on(view.serializable_tx_ref(tx).update(
            "grants",
            id(20),
            BTreeMap::from([("token".into(), Value::String("alice".into()))]),
            Default::default(),
        ))
        .unwrap();
        block_on(server.update(
            "grants",
            id(20),
            BTreeMap::from([("token".into(), Value::String("alice".into()))]),
            Default::default(),
        ))
        .unwrap();
        let before = server_rows(&server, "grants");
        let error = drive(&nodes, paired(&view, tx, Query::from("docs"))).unwrap_err();
        assert_eq!(error.code, ErrorCode::TransactionConflict);
        assert_eq!(server_rows(&server, "grants"), before);
        assert!(block_on(backend.commit_serializable_handle(tx)).is_err());
    }
}

// Transport substitution is necessary: public APIs never label an ordinary
// read answer as a validation of staged writes at an accepted cut.
struct WrongKind(Box<dyn Transport>);
impl Transport for WrongKind {
    fn send(&mut self, m: SyncMessage) -> Result<(), TransportError> {
        self.0.send(m)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.0.try_recv().map(|m| match m {
            SyncMessage::AcceptedOverlayCheckedV1 { id, through } => SyncMessage::Answer {
                id,
                through,
                rows: vec![],
            },
            other => other,
        })
    }
}

/// Mallory substitutes ordinary coverage for Alice's staged-projection check.
/// It cannot complete the accepted read or let that transaction commit.
#[test]
fn ordinary_answer_cannot_validate_an_accepted_overlay() {
    let server = open(22, true);
    seed(&server);
    let backend = open(23, false);
    let (up, down) = duplex();
    block_on(backend.connect_upstream(Box::new(WrongKind(up))));
    server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let nodes = [&server, &backend];
    drive(&nodes, fetch(&backend, Query::from("grants")));
    let view = view(&backend);
    let tx = begin(&view);
    block_on(view.serializable_tx_ref(tx).update(
        "grants",
        id(20),
        BTreeMap::from([("token".into(), Value::String("alice".into()))]),
        Default::default(),
    ))
    .unwrap();
    let error = drive(&nodes, paired(&view, tx, Query::from("docs"))).unwrap_err();
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert!(error.to_string().contains("ordinary cache answer"));
    assert!(block_on(view.commit_serializable_handle(tx)).is_err());
}

/// Mallory cannot use an ordinary session to simulate Alice's writes. A trusted
/// backend still has to pass Alice's actual write policies and request claims.
/// Raw requests are necessary to exercise forged scopes and denied mutations.
#[test]
fn overlay_requests_keep_connection_and_write_permission_boundaries() {
    for trusted in [false, true] {
        let server = open(24, true);
        seed(&server);
        let before = server_rows(&server, "grants");
        let (mut peer, down) = duplex();
        server.accept_subscriber(
            down,
            if trusted {
                AuthorSubject::SYSTEM
            } else {
                alice()
            },
        );
        peer.send(SyncMessage::AcceptedOverlayCheckV1{id:91,query:Query::from("docs"),binding:BTreeMap::new(),through:3,include_deleted:false,author:alice().canonical().into(),claims:BTreeMap::new(),ops:serde_json::from_value(serde_json::json!([{ "Update": {"table":"grants","row":id(20).as_bytes(),"cells":{"token":{"String":"alice"}},"target":"Root"}}])).unwrap()}).unwrap();
        block_on(server.tick()).unwrap();
        let mut refused = false;
        while let Some(message) = peer.try_recv() {
            match message {
                SyncMessage::Refused { id: 91, code, .. } => {
                    assert_eq!(code, ErrorCode::WriteRejected);
                    refused = true;
                }
                SyncMessage::AcceptedOverlayCheckedV1 { .. } => {
                    panic!("forged or denied overlay was accepted")
                }
                _ => {}
            }
        }
        assert!(refused, "trusted={trusted}");
        assert_eq!(server_rows(&server, "grants"), before);
    }
}

// Holding a transport answer exposes the race between a read's registration,
// its authority check and a concurrent write or cancellation in Alice's batch.
struct DelayOverlay {
    inner: Box<dyn Transport>,
    held: std::rc::Rc<std::cell::RefCell<Option<SyncMessage>>>,
    release: std::rc::Rc<std::cell::Cell<bool>>,
}
impl Transport for DelayOverlay {
    fn send(&mut self, m: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(m)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.held.borrow().is_some() {
            return if self.release.get() {
                self.held.borrow_mut().take()
            } else {
                None
            };
        }
        match self.inner.try_recv() {
            Some(m @ SyncMessage::AcceptedOverlayCheckedV1 { .. }) if !self.release.get() => {
                *self.held.borrow_mut() = Some(m);
                None
            }
            m => m,
        }
    }
}

/// Alice's authority check describes exactly the staged prefix she sent. A
/// later grant cannot reuse it, and dropping the pending read cannot unlock
/// commit. Late replies do not complete a different transaction.
#[test]
fn pending_overlay_checks_cannot_outlive_their_staged_prefix_or_read() {
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    for cancel in [false, true] {
        let server = open(25, true);
        seed(&server);
        let backend = open(26, false);
        let held = Rc::new(RefCell::new(None));
        let release = Rc::new(Cell::new(false));
        let (up, down) = duplex();
        block_on(backend.connect_upstream(Box::new(DelayOverlay {
            inner: up,
            held: held.clone(),
            release: release.clone(),
        })));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let nodes = [&server, &backend];
        drive(&nodes, fetch(&backend, Query::from("grants")));
        let view = view(&backend);
        let tx = begin(&view);
        block_on(view.serializable_tx_ref(tx).update(
            "grants",
            id(20),
            BTreeMap::from([("token".into(), Value::String("alice".into()))]),
            Default::default(),
        ))
        .unwrap();
        {
            let mut future = pin!(paired(&view, tx, Query::from("docs")));
            for _ in 0..100 {
                assert!(
                    future
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                tick(&nodes);
                if held.borrow().is_some() {
                    break;
                }
            }
            assert!(held.borrow().is_some());
            if !cancel {
                block_on(view.serializable_tx_ref(tx).insert(
                    "grants",
                    grant(11),
                    InsertOptions {
                        row_id: Some(id(21)),
                        ..Default::default()
                    },
                ))
                .unwrap();
                release.set(true);
                let error = drive(&nodes, future).unwrap_err();
                assert_eq!(error.code, ErrorCode::NotObserved);
                assert!(
                    error.to_string().contains("staged writes changed"),
                    "{error}"
                );
            }
        }
        assert!(block_on(view.commit_serializable_handle(tx)).is_err());
        release.set(true);
        tick(&nodes);
        assert_eq!(server_count(&server, "grants"), 1);
        let fresh = begin(&view);
        assert_eq!(
            ids(&drive(&nodes, paired(&view, fresh, Query::from("docs"))).unwrap()),
            [id(10)]
        );
    }
}

/// The byte fixture distinguishes Alice's staged-projection check and its
/// response from ordinary and accepted row coverage. It pins append-only tags,
/// a multi-byte authority cut, immutable claims and a staged update payload.
#[test]
fn accepted_overlay_v1_has_frozen_wire_bytes() {
    let request=SyncMessage::AcceptedOverlayCheckV1{id:17,query:Query::from("docs").limit(2).offset(1),binding:BTreeMap::new(),through:128,include_deleted:false,author:alice().canonical().into(),claims:claims(),ops:serde_json::from_value(serde_json::json!([{ "Update": {"table":"grants","row":id(20).as_bytes(),"cells":{"token":{"String":"alice"}},"target":"Root"}}])).unwrap()};
    let answer = SyncMessage::AcceptedOverlayCheckedV1 {
        id: 17,
        through: 128,
    };
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/sqlite-sync-v2/accepted-overlay.json"
    ))
    .unwrap();
    assert_eq!(
        fixture["request_role"],
        "jazz.sqlite.sync.accepted-overlay-check.v1"
    );
    assert_eq!(
        fixture["answer_role"],
        "jazz.sqlite.sync.accepted-overlay-checked.v1"
    );
    assert_eq!(fixture["version"], 2);
    for (key, message) in [("request", request), ("answer", answer)] {
        let encoded = jazz::db::encode_message(&message).unwrap();
        let bytes = hex::decode(fixture[key].as_str().unwrap()).unwrap();
        assert_eq!(encoded, bytes);
        let decoded = jazz::db::decode_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
    }
}

/// A compromised trusted host cannot make Alice's bounded check execute an
/// oversized staged batch. Raw requests exercise both operation and byte limits;
/// the authority must refuse before changing any application row.
#[test]
fn oversized_overlay_requests_are_refused_without_writes() {
    for many in [false, true] {
        let server = open(27, true);
        seed(&server);
        let before = server_rows(&server, "grants");
        let (mut peer, down) = duplex();
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let op = serde_json::json!({"Update":{"table":"grants","row":id(20).as_bytes(),"cells":{"token":{"String":if many {"alice".into()} else {"x".repeat(4*1024*1024)}}},"target":"Root"}});
        let ops = serde_json::from_value(serde_json::Value::Array(vec![
            op;
            if many { 1025 } else { 1 }
        ]))
        .unwrap();
        peer.send(SyncMessage::AcceptedOverlayCheckV1 {
            id: 92,
            query: Query::from("docs"),
            binding: BTreeMap::new(),
            through: 3,
            include_deleted: false,
            author: alice().canonical().into(),
            claims: claims(),
            ops,
        })
        .unwrap();
        block_on(server.tick()).unwrap();
        let mut refused = false;
        while let Some(message) = peer.try_recv() {
            match message {
                SyncMessage::Refused {
                    id: 92,
                    code,
                    message,
                } => {
                    assert_eq!(code, ErrorCode::Query);
                    assert!(message.contains("1024 writes or 4 MiB"), "{message}");
                    refused = true;
                }
                SyncMessage::AcceptedOverlayCheckedV1 { .. } => {
                    panic!("oversized overlay was accepted")
                }
                _ => {}
            }
        }
        assert!(refused);
        assert_eq!(server_rows(&server, "grants"), before);
    }
}
