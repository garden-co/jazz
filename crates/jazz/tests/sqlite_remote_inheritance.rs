//! Synced inheritance transports readable parent capabilities without exposing
//! parents whose SELECT policy denies the subscriber.
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ReadOpts, ReadTier, SerializedReadResult, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{InheritsOperation, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, Operation, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn uuid(n: u8) -> Value {
    Value::Uuid(id(n).0)
}
fn text(s: &str) -> Value {
    Value::String(s.into())
}
fn cells(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    values
        .iter()
        .map(|(k, v)| ((*k).into(), v.clone()))
        .collect()
}
fn own() -> PolicyExpr {
    PolicyExpr::eq_session(
        "owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    )
}
fn schema() -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("owner", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(own())
                            .with_update(Some(own()), PolicyExpr::False)
                            .with_insert(PolicyExpr::False)
                            .with_delete(PolicyExpr::False),
                    ),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent", "parents")
                    .column("owner", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(own())),
            )
            .table(
                TableSchemaBuilder::new("bundles")
                    .array_fk_column("parents", "parents")
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("folders")
                    .nullable_fk_column("parent", "folders")
                    .column("owner", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::or(vec![
                        own(),
                        PolicyExpr::Inherits {
                            operation: Operation::Select,
                            via_column: "parent".into(),
                            max_depth: None,
                        },
                    ]))),
            )
            .table(
                TableSchemaBuilder::new("checks")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_update(None, PolicyExpr::False),
                    ),
            )
            .table(
                TableSchemaBuilder::new("check_children")
                    .fk_column("parent", "checks")
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("secrets")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::False)
                            .with_update(None, PolicyExpr::True),
                    ),
            )
            .table(
                TableSchemaBuilder::new("secret_children")
                    .fk_column("parent", "secrets")
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
    .unwrap()
}
fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: if node == 0 {
                AuthorSubject::SYSTEM
            } else {
                alice()
            },
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}
fn put(db: &Db, table: &str, row: u8, values: &[(&str, Value)]) {
    block_on(db.insert(
        table,
        cells(values),
        jazz::db::InsertOptions {
            row_id: Some(id(row)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn connect(server: &Db, client: &Db) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice());
}
fn fixture() -> (Db, Db) {
    let server = open(0);
    for (row, owner) in [(1, "alice"), (2, "bob")] {
        put(&server, "parents", row, &[("owner", text(owner))]);
    }
    for (row, parent, owner) in [
        (10, 1, "alice"),
        (11, 2, "alice"),
        (12, 1, "bob"),
        (13, 99, "alice"),
    ] {
        put(
            &server,
            "children",
            row,
            &[("parent", uuid(parent)), ("owner", text(owner))],
        );
    }
    for (row, parents) in [(20, vec![1, 1, 2]), (21, vec![2]), (22, vec![])] {
        put(
            &server,
            "bundles",
            row,
            &[(
                "parents",
                Value::Array(parents.into_iter().map(uuid).collect()),
            )],
        );
    }
    for (row, parent, owner) in [
        (30, None, "alice"),
        (31, Some(30), "bob"),
        (32, Some(31), "bob"),
        (33, Some(34), "bob"),
        (34, Some(33), "bob"),
    ] {
        put(
            &server,
            "folders",
            row,
            &[
                ("owner", text(owner)),
                (
                    "parent",
                    parent.map_or(Value::Nullable(None), |n| {
                        Value::Nullable(Some(Box::new(uuid(n))))
                    }),
                ),
            ],
        );
    }
    put(&server, "checks", 40, &[("label", text("immutable"))]);
    put(&server, "check_children", 41, &[("parent", uuid(40))]);
    put(&server, "secrets", 50, &[("label", text("never disclose"))]);
    put(&server, "secret_children", 51, &[("parent", uuid(50))]);
    let client = open(1);
    connect(&server, &client);
    (server, client)
}
fn turns(nodes: &[&Db]) {
    for _ in 0..10 {
        for db in nodes {
            block_on(db.tick()).unwrap();
        }
    }
}
fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        turns(nodes);
    }
    panic!("read did not finish");
}
fn remote(nodes: &[&Db], client: &Db, query: &Query) -> Result<Vec<RowUuid>, Error> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let result = drive(
        nodes,
        client.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )?;
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows")
    };
    let mut ids: Vec<_> = rows.iter().map(|row| row.row_uuid()).collect();
    ids.sort();
    Ok(ids)
}
fn cached(client: &Db, table: &str) -> Vec<RowUuid> {
    let query = client.prepare_query(&Query::from(table)).unwrap();
    let mut rows: Vec<_> =
        block_on(client.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
            .unwrap()
            .iter()
            .map(|row| row.row_uuid())
            .collect();
    rows.sort();
    rows
}

/// Alice's live inherited read receives its readable parent, enforces the
/// child's independent read policy, and tracks Bob's parent grant changes.
#[test]
fn remote_inheritance_hydrates_parents_and_tracks_grants() {
    let (server, client) = fixture();
    let nodes = [&client, &server, &client];
    let query = Query::from("children").inherits("parent");
    let prepared = client.prepare_query(&query).unwrap();
    let _stream =
        block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&nodes);
    assert_eq!(remote(&nodes, &client, &query).unwrap(), vec![id(10)]);
    assert_eq!(cached(&client, "parents"), vec![id(1)]);
    block_on(server.update(
        "parents",
        id(1),
        cells(&[("owner", text("bob"))]),
        Default::default(),
    ))
    .unwrap();
    turns(&nodes);
    assert!(remote(&nodes, &client, &query).unwrap().is_empty());
    block_on(server.update(
        "parents",
        id(2),
        cells(&[("owner", text("alice"))]),
        Default::default(),
    ))
    .unwrap();
    turns(&nodes);
    assert_eq!(remote(&nodes, &client, &query).unwrap(), vec![id(11)]);
}

/// Alice's cold replica receives finite parent chains and UUID-array witnesses;
/// repeated references and ungrounded cycles cannot create duplicate grants.
#[test]
fn remote_inheritance_preserves_arrays_depth_and_cycles() {
    let (server, client) = fixture();
    let nodes = [&client, &server, &client];
    assert_eq!(
        remote(&nodes, &client, &Query::from("bundles").inherits("parents")).unwrap(),
        vec![id(20)]
    );
    assert_eq!(cached(&client, "parents"), vec![id(1)]);
    for (depth, expected) in [(0, vec![]), (1, vec![id(31)]), (2, vec![id(31), id(32)])] {
        let mut query = Query::from("folders").inherits("parent");
        query.inherits[0].max_depth = Some(depth);
        assert_eq!(remote(&nodes, &client, &query).unwrap(), expected);
    }
}

/// Alice's UPDATE capability follows USING rather than WITH CHECK, while an
/// unreadable parent cannot be disclosed merely because she may update it.
#[test]
fn remote_operation_inheritance_keeps_capabilities_and_parent_privacy() {
    let (server, client) = fixture();
    let nodes = [&client, &server, &client];
    for (table, expected) in [("children", vec![id(10)]), ("check_children", vec![id(41)])] {
        let mut query = Query::from(table).inherits("parent");
        query.inherits[0].operation = InheritsOperation::Update;
        assert_eq!(remote(&nodes, &client, &query).unwrap(), expected);
    }
    let mut query = Query::from("secret_children").inherits("parent");
    query.inherits[0].operation = InheritsOperation::Update;
    let error = remote(&nodes, &client, &query).unwrap_err();
    assert!(error.message.contains("readable"), "{error}");
    assert!(cached(&client, "secrets").is_empty());
    assert!(cached(&client, "secret_children").is_empty());
    assert_eq!(
        remote(&nodes, &client, &Query::from("check_children")).unwrap(),
        vec![id(41)]
    );
}

/// Alice's ordinary and private relays keep an array grant alive through a
/// second readable parent, then remove it when Bob revokes the final parent.
#[test]
fn inherited_array_paths_remain_coherent_through_relays() {
    for private in [false, true] {
        let (server, _) = fixture();
        let relay = open(2);
        if private {
            relay.set_scope_isolated_storage_owner().unwrap();
        }
        let client = open(3);
        connect(&server, &relay);
        connect(&relay, &client);
        let nodes = [&client, &relay, &server, &relay, &client];
        let query = Query::from("bundles").inherits("parents");
        let prepared = client.prepare_query(&query).unwrap();
        let _stream =
            block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap();
        turns(&nodes);
        assert_eq!(remote(&nodes, &client, &query).unwrap(), vec![id(20)]);
        block_on(server.update(
            "parents",
            id(2),
            cells(&[("owner", text("alice"))]),
            Default::default(),
        ))
        .unwrap();
        turns(&nodes);
        assert_eq!(cached(&client, "parents"), vec![id(1), id(2)]);
        block_on(server.update(
            "parents",
            id(1),
            cells(&[("owner", text("bob"))]),
            Default::default(),
        ))
        .unwrap();
        turns(&nodes);
        assert_eq!(
            remote(&nodes, &client, &query).unwrap(),
            vec![id(20), id(21)]
        );
        block_on(server.update(
            "parents",
            id(2),
            cells(&[("owner", text("bob"))]),
            Default::default(),
        ))
        .unwrap();
        turns(&nodes);
        assert!(
            block_on(client.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap()
                .is_empty(),
            "private={private}: a revoked array parent still grants the child"
        );
    }
}

/// Alice's cold serializable read hydrates parents at the transaction's pinned
/// cut. A later remote read sees Bob's changed grant instead.
#[test]
fn inherited_snapshot_uses_the_parent_at_the_pinned_cut() {
    let (server, client) = fixture();
    let nodes = [&client, &server, &client];
    remote(&nodes, &client, &Query::from("children").limit(0)).unwrap();
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(client.begin_serializable(tx)).unwrap();
    let empty = client
        .prepare_query(&Query::from("children").limit(0))
        .unwrap();
    drive(&nodes, client.serializable_tx_ref(tx).all_prepared(&empty)).unwrap();
    block_on(server.update(
        "parents",
        id(1),
        cells(&[("owner", text("bob"))]),
        Default::default(),
    ))
    .unwrap();
    let query = Query::from("children").inherits("parent");
    let prepared = client.prepare_query(&query).unwrap();
    let rows = drive(
        &nodes,
        client.serializable_tx_ref(tx).all_prepared(&prepared),
    )
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![id(10)]
    );
    assert!(remote(&nodes, &client, &query).unwrap().is_empty());
    client.abandon_transaction_handle(tx).unwrap();
}

/// Alice's oversized inherited subscription is refused before sending roots
/// or parents. A smaller query still works on the same connection.
#[test]
fn inherited_witness_budget_refuses_without_partial_rows() {
    let (server, client) = fixture();
    let nodes = [&client, &server, &client];
    for n in 0..17_000_u128 {
        block_on(server.insert(
            "children",
            cells(&[("parent", uuid(1)), ("owner", text("alice"))]),
            jazz::db::InsertOptions {
                row_id: Some(RowUuid::from_bytes((n + 256).to_be_bytes())),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    let query = Query::from("children").inherits("parent");
    let prepared = client.prepare_query(&query).unwrap();
    let mut stream =
        block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&nodes);
    assert!(
        matches!(stream.try_next_event(), Some(jazz::db::SubscriptionEvent::Rejected { reason })
        if reason.contains("limit") || reason.contains("budget"))
    );
    assert!(cached(&client, "children").is_empty());
    assert!(cached(&client, "parents").is_empty());
    assert_eq!(remote(&nodes, &client, &query.limit(1)).unwrap().len(), 1);
}
