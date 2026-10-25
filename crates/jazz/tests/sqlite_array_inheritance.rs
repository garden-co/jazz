//! UUID-array permission inheritance through real authorities and trusted relays.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, InsertOptions, ReadOpts, ReadTier, SerializedReadResult,
    SyncMessage, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, provider_claim_key};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, Operation, PolicyExpr, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};
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
fn claims(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([(provider_claim_key("code"), Value::String(code.into()))])
}
fn schema() -> JazzSchema {
    let permission = PolicyExpr::eq_session("code", vec!["claims".into(), "code".into()]);
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("title", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::Exists {
                        table: "access".into(),
                        condition: Box::new(PolicyExpr::and(vec![
                            PolicyExpr::eq_session(
                                "parent",
                                vec!["__jazz_outer_row".into(), "id".into()],
                            ),
                            permission.clone(),
                        ])),
                    })),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("parent", "parents")
                    .column("code", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::False)),
            )
            .table(
                TableSchemaBuilder::new("bundles")
                    .array_fk_column("parents", "parents")
                    .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                        operation: Operation::Select,
                        via_column: "parents".into(),
                        max_depth: None,
                    })),
            )
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new().with_select(PolicyExpr::InheritsReferencing {
                            operation: Operation::Select,
                            source_table: "grants".into(),
                            via_column: "targets".into(),
                            max_depth: None,
                        }),
                    ),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .array_fk_column("targets", "docs")
                    .column("code", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(permission)),
            )
            .build(),
    )
    .unwrap()
}
fn config(node: u8, authority: bool) -> DbConfig {
    DbConfig::new(
        if authority {
            schema()
        } else {
            schema().without_permissions()
        },
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}
fn open(node: u8, authority: bool) -> Db {
    let db = block_on(Db::open(config(node, authority))).unwrap();
    if authority {
        db.set_authority();
        db.initialize_authority_catalogue().unwrap();
    }
    db
}
fn put(db: &Db, table: &str, n: u8, fields: Vec<(&str, Value)>) {
    block_on(db.insert(
        table,
        fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn array(ids: &[u8]) -> Value {
    Value::Array(ids.iter().map(|n| Value::Uuid(id(*n).0)).collect())
}
fn seed(db: &Db) {
    for (n, code) in [(10, "a"), (11, "b")] {
        put(
            db,
            "parents",
            n,
            vec![("title", Value::String(code.into()))],
        );
        put(
            db,
            "access",
            n + 20,
            vec![
                ("parent", Value::Uuid(id(n).0)),
                ("code", Value::String(code.into())),
            ],
        );
    }
    for (n, parents) in [
        (20, vec![11, 10, 10]),
        (21, vec![]),
        (22, vec![11]),
        (23, vec![99]),
    ] {
        put(db, "bundles", n, vec![("parents", array(&parents))]);
    }
    for n in [40, 41, 42] {
        put(
            db,
            "docs",
            n,
            vec![("title", Value::String(format!("doc-{n}")))],
        );
    }
    for (n, targets, code) in [
        (50, vec![40, 40], "a"),
        (51, vec![41], "b"),
        (52, vec![], "a"),
    ] {
        put(
            db,
            "grants",
            n,
            vec![
                ("targets", array(&targets)),
                ("code", Value::String(code.into())),
            ],
        );
    }
}
fn connect(server: &Db, backend: &Db, relay: Option<&Db>) {
    let (up, down) = duplex();
    block_on(backend.connect_upstream(up));
    if let Some(relay) = relay {
        relay.accept_subscriber(down, AuthorSubject::SYSTEM);
        let (up, down) = duplex();
        block_on(relay.connect_upstream(up));
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
    } else {
        server.accept_subscriber(down, AuthorSubject::SYSTEM);
    }
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..200 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        for db in nodes {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("array inheritance read did not settle")
}
async fn read(db: &Db, table: &str, code: &str) -> Result<Vec<RowUuid>, Error> {
    let query = postcard::to_allocvec(&Query::from(table)).unwrap();
    let result = db
        .all_serialized_query(
            &query,
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
        panic!("rows")
    };
    let mut result = rows.into_iter().map(|r| r.row_uuid()).collect::<Vec<_>>();
    result.sort();
    Ok(result)
}
fn accepted(
    db: &Db,
    tx: OpenTransactionId,
) -> impl Future<Output = Result<jazz::db::BindingSettledRead, Error>> + 'static {
    let query = db
        .prepare_query(&Query::from("docs"))
        .unwrap()
        .with_identity_claims(alice(), claims("a"));
    db.all_settled(
        query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        tx,
        false,
        Some(alice()),
    )
    .unwrap()
}

/// Alice's two claim snapshots and Bob's unmatched code stay isolated. Forward
/// and reverse arrays use existential membership, and private grants never
/// appear as application rows, directly or through a cold trusted relay.
#[test]
fn arrays_keep_membership_claims_and_private_evidence_isolated() {
    for relayed in [false, true] {
        let server = open(100, true);
        seed(&server);
        let backend = open(101, false);
        let relay = open(102, false);
        connect(&server, &backend, relayed.then_some(&relay));
        let nodes = [&server, &relay, &backend];
        for table in ["bundles", "docs"] {
            let (a, b, absent) = drive(&nodes, async {
                futures::join!(
                    read(&backend, table, "a"),
                    read(&backend, table, "b"),
                    read(&backend, table, "none")
                )
            });
            assert_eq!(
                a.unwrap(),
                if table == "bundles" {
                    vec![id(20)]
                } else {
                    vec![id(40)]
                }
            );
            assert_eq!(
                b.unwrap(),
                if table == "bundles" {
                    vec![id(20), id(22)]
                } else {
                    vec![id(41)]
                }
            );
            assert!(absent.unwrap().is_empty());
        }
        assert!(
            drive(&nodes, read(&backend, "access", "a"))
                .unwrap()
                .is_empty()
        );
        block_on(server.delete("parents", id(10), Default::default())).unwrap();
        assert!(
            drive(&nodes, read(&backend, "bundles", "a"))
                .unwrap()
                .is_empty()
        );
        block_on(server.delete("grants", id(50), Default::default())).unwrap();
        assert!(
            drive(&nodes, read(&backend, "docs", "a"))
                .unwrap()
                .is_empty()
        );
    }
}

/// Alice pins an accepted read before Bob retargets and revokes an array grant.
/// Her second read must reproduce the historical cut, but commit must reject.
/// Alice(read) -> Bob(retarget, revoke) -> Alice(same cut, rejected commit).
#[test]
fn historical_array_grants_keep_the_cut_and_reject_stale_commit() {
    for relayed in [false, true] {
        let server = open(103, true);
        seed(&server);
        let backend = open(104, false);
        let relay = open(105, false);
        connect(&server, &backend, relayed.then_some(&relay));
        let nodes = [&server, &relay, &backend];
        let tx = OpenTransactionId::new();
        block_on(backend.begin_serializable_for_identity(tx, alice())).unwrap();
        let first = drive(&nodes, accepted(&backend, tx)).unwrap();
        assert_eq!(first.rows.len(), 1);
        block_on(server.update(
            "grants",
            id(50),
            BTreeMap::from([("targets".into(), array(&[42]))]),
            Default::default(),
        ))
        .unwrap();
        block_on(server.update(
            "grants",
            id(50),
            BTreeMap::from([("code".into(), Value::String("revoked".into()))]),
            Default::default(),
        ))
        .unwrap();
        assert_eq!(drive(&nodes, accepted(&backend, tx)).unwrap(), first);
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
        assert!(
            drive(&nodes, read(&backend, "docs", "a"))
                .unwrap()
                .is_empty()
        );
    }
}

/// Mallory's ordinary session cannot turn array inheritance into delegated
/// authority access or receive Alice's private permission closure.
#[test]
fn ordinary_sessions_cannot_forge_scoped_array_reads() {
    let server = open(106, true);
    seed(&server);
    let (mut client, peer) = duplex();
    server.accept_subscriber(peer, AuthorSubject::for_test_bytes([9; 16]));
    client
        .send(SyncMessage::Schema {
            version: *schema().version_id().as_bytes(),
        })
        .unwrap();
    client
        .send(SyncMessage::ScopedFetchV1 {
            id: 900,
            query: Query::from("bundles"),
            binding: BTreeMap::new(),
            author: alice().canonical().into(),
            claims: claims("a"),
            include_deleted: false,
        })
        .unwrap();
    block_on(server.tick()).unwrap();
    let mut refused = false;
    while let Some(message) = client.try_recv() {
        match message {
            SyncMessage::Refused { id: 900, .. } => refused = true,
            SyncMessage::Rows { images } => assert!(images.is_empty()),
            SyncMessage::ScopedEvidenceAnswerV1 { id: 900, .. }
            | SyncMessage::Answer { id: 900, .. } => panic!("forged delegation answered"),
            _ => {}
        }
    }
    assert!(refused);
}

/// Bob reopens the authority after Alice's array grant was retargeted. Current
/// membership survives reopen; deletion and restoration maintain the same
/// index through public writes without granting the stale target.
#[test]
fn reopened_authority_keeps_retargeted_membership_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("authority.sqlite");
    let server = block_on(Db::open_at(config(107, true), &path, Default::default())).unwrap();
    server.set_authority();
    server.initialize_authority_catalogue().unwrap();
    seed(&server);
    block_on(server.update(
        "grants",
        id(50),
        BTreeMap::from([("targets".into(), array(&[42]))]),
        Default::default(),
    ))
    .unwrap();
    block_on(server.close()).unwrap();
    drop(server);
    let server = block_on(Db::open_at(config(107, true), &path, Default::default())).unwrap();
    server.set_authority();
    server.initialize_authority_catalogue().unwrap();
    let backend = open(108, false);
    connect(&server, &backend, None);
    let nodes = [&server, &backend];
    assert_eq!(
        drive(&nodes, read(&backend, "docs", "a")).unwrap(),
        vec![id(42)]
    );
    block_on(server.delete("grants", id(50), Default::default())).unwrap();
    assert!(
        drive(&nodes, read(&backend, "docs", "a"))
            .unwrap()
            .is_empty()
    );
    block_on(server.restore("grants", id(50), None, Default::default())).unwrap();
    assert_eq!(
        drive(&nodes, read(&backend, "docs", "a")).unwrap(),
        vec![id(42)]
    );
}
