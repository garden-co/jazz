//! Retained output aliases survive local, synced and serializable reads.
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, ReadOpts, ReadTier,
    SerializedReadResult, SubscriptionEvent, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{
    OrderDirection, Query, RelationColumnRef, RelationExpr, RelationJoinCondition,
    RelationJoinKind, RelationOrderBy, RelationProjectColumn, RelationProjectExpr, RelationQuery,
    RelationRowIdRef, relation_query_to_query,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://alias.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn config(node: u8) -> DbConfig {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session(
            "owner",
            vec!["user".into(), "identity".into(), "subject".into()],
        ))
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::BigInt)
                    .column("owner", ColumnType::Text)
                    .policies(policies.clone()),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("doc", "docs")
                    .column("owner", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
    .unwrap();
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )
}
fn open(node: u8) -> Db {
    let config = config(node);
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}
fn doc(db: &Db, n: u8, title: &str, owner: &str) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([
            ("title".into(), Value::String(title.into())),
            ("rank".into(), Value::I64(i64::from(n))),
            ("owner".into(), Value::String(owner.into())),
        ]),
        InsertOptions {
            row_id: Some(id(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}
fn seed(db: &Db) {
    doc(db, 1, "z", "alice");
    doc(db, 2, "a", "alice");
    doc(db, 3, "secret", "bob");
    for (n, root, owner) in [
        (11, 1, "alice"),
        (12, 1, "alice"),
        (13, 2, "bob"),
        (14, 3, "alice"),
    ] {
        block_on(db.insert(
            "access",
            BTreeMap::from([
                ("doc".into(), Value::Uuid(id(root).0)),
                ("owner".into(), Value::String(owner.into())),
            ]),
            InsertOptions {
                row_id: Some(id(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
}
fn column(scope: &str, name: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(scope.into()),
        column: name.into(),
    }
}
fn relation(join: bool, aliases: &[(&str, &str)], window: bool) -> RelationQuery {
    let mut input = RelationExpr::TableScan {
        table: "docs".into(),
        alias: None,
    };
    if join {
        input = RelationExpr::Join {
            left: Box::new(input),
            right: Box::new(RelationExpr::TableScan {
                table: "access".into(),
                alias: Some("grant".into()),
            }),
            on: vec![RelationJoinCondition {
                left: column("docs", "id"),
                right: column("grant", "doc"),
            }],
            join_kind: RelationJoinKind::Inner,
        };
    }
    let mut rel = RelationExpr::Project {
        input: Box::new(input),
        columns: aliases
            .iter()
            .map(|(source, alias)| RelationProjectColumn {
                alias: (*alias).into(),
                expr: if *source == "id" {
                    RelationProjectExpr::RowId(RelationRowIdRef::Current)
                } else {
                    RelationProjectExpr::Column(column("docs", source))
                },
            })
            .collect(),
    };
    if window {
        rel = RelationExpr::Limit {
            input: Box::new(RelationExpr::OrderBy {
                input: Box::new(rel),
                terms: vec![RelationOrderBy {
                    column: column("docs", "rank"),
                    direction: OrderDirection::Asc,
                }],
            }),
            limit: 1,
        };
    }
    RelationQuery { rel }
}
fn query(join: bool, aliases: &[(&str, &str)], window: bool) -> Query {
    relation_query_to_query(&relation(join, aliases, window)).unwrap()
}
fn cells(row: &CurrentRow) -> Vec<(String, Option<Value>)> {
    row.cells()
        .map(|(name, value)| (name.to_owned(), value.cloned()))
        .collect()
}
fn connect(client: &Db, server: &Db) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice());
}
fn turns(nodes: &[&Db]) {
    for _ in 0..20 {
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
fn fetch(db: &Db, nodes: &[&Db], query: &Query) -> Vec<CurrentRow> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let SerializedReadResult::Rows(rows) = drive(
        nodes,
        db.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
        ),
    )
    .unwrap() else {
        panic!("flat rows")
    };
    rows
}
fn begin(db: &Db) -> OpenTransactionId {
    let tx = OpenTransactionId::new();
    block_on(db.begin_serializable(tx)).unwrap();
    tx
}

/// Alice's bare and normalized queries preserve UUID identity, repeated source
/// columns, quoted aliases and exact big integers without leaking unselected cells.
#[test]
fn aliases_keep_types_identity_and_shape() {
    let db = open(0);
    seed(&db);
    let large = "chunked".repeat(15_000);
    block_on(db.update(
        "docs",
        id(1),
        BTreeMap::from([
            ("title".into(), Value::String(large.clone())),
            ("rank".into(), Value::I64((1_i64 << 54) + 1)),
        ]),
        Default::default(),
    ))
    .unwrap();
    let rel = relation(
        false,
        &[
            ("id", "key"),
            ("title", "label"),
            ("title", "a\"b"),
            ("rank", "number"),
        ],
        false,
    );
    let mut bare = Query::from("docs");
    bare.relation = Some(rel.clone());
    let bare = db.prepare_query(&bare).unwrap();
    let normal = db
        .prepare_query(&relation_query_to_query(&rel).unwrap())
        .unwrap();
    assert_eq!(bare.shape(), normal.shape());
    assert_eq!(bare.query(), normal.query());
    assert!(normal.query().relation.is_some());
    let rows = db.read(&normal).unwrap();
    let row = rows.iter().find(|row| row.row_uuid() == id(1)).unwrap();
    assert_eq!(
        cells(row),
        vec![
            ("key".into(), Some(Value::Uuid(id(1).0))),
            ("label".into(), Some(Value::String(large.clone()))),
            ("a\"b".into(), Some(Value::String(large))),
            ("number".into(), Some(Value::I64((1_i64 << 54) + 1))),
        ]
    );
    let renamed = db
        .prepare_query(&query(false, &[("title", "other")], false))
        .unwrap();
    assert_ne!(normal.shape(), renamed.shape());
}

/// Alice names title "rank", but her maintained window must still sort by
/// physical rank. Simultaneous subscriptions cannot exchange output names.
#[test]
fn maintained_aliases_do_not_replace_sort_keys_or_share_output_names() {
    let db = open(0);
    seed(&db);
    let first = db
        .prepare_query(&query(false, &[("title", "rank")], true))
        .unwrap();
    let second = db
        .prepare_query(&query(false, &[("title", "label")], true))
        .unwrap();
    let mut streams =
        [&first, &second].map(|query| block_on(db.subscribe(query, ReadOpts::default())).unwrap());
    for stream in &mut streams {
        while stream.try_next_event().is_some() {}
    }
    assert_eq!(db.read(&first).unwrap()[0].row_uuid(), id(1));
    block_on(db.update(
        "docs",
        id(2),
        BTreeMap::from([("rank".into(), Value::I64(0))]),
        Default::default(),
    ))
    .unwrap();
    for (stream, alias) in streams.iter_mut().zip(["rank", "label"]) {
        let mut added = vec![];
        let mut removed = vec![];
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta {
                added: a,
                removed: r,
                ..
            } = event
            {
                added.extend(
                    a.into_iter()
                        .map(|item| (item.row.row_uuid(), cells(&item.row))),
                );
                removed.extend(r.into_iter().map(|item| item.row_uuid));
            }
        }
        assert_eq!(
            added,
            vec![(id(2), vec![(alias.into(), Some(Value::String("a".into())))])]
        );
        assert_eq!(removed, vec![id(1)]);
    }
}

/// Alice cold-reads renamed joined output through direct and ordinary-relay
/// paths. Bob's roots/grants remain hidden and final revocation wins.
#[test]
fn remote_aliases_keep_join_permissions_and_revocation() {
    for mode in 0..2 {
        let server = open(0);
        seed(&server);
        let client = open(1);
        let relay = open(2);
        let nodes = if mode == 0 {
            connect(&client, &server);
            vec![&client, &server]
        } else {
            connect(&relay, &server);
            connect(&client, &relay);
            vec![&client, &relay, &server]
        };
        let q = query(true, &[("id", "key"), ("title", "label")], false);
        let rows = fetch(&client, &nodes, &q);
        assert_eq!(rows.len(), 1, "mode {mode}");
        assert_eq!(
            cells(&rows[0]),
            vec![
                ("key".into(), Some(Value::Uuid(id(1).0))),
                ("label".into(), Some(Value::String("z".into()))),
            ]
        );
        let prepared = client.prepare_query(&q).unwrap();
        let mut stream =
            block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap();
        turns(&nodes);
        while stream.try_next_event().is_some() {}
        for grant in [11, 12] {
            block_on(server.update(
                "access",
                id(grant),
                BTreeMap::from([("owner".into(), Value::String("bob".into()))]),
                Default::default(),
            ))
            .unwrap();
            turns(&nodes);
            let rows =
                block_on(client.all(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
            assert_eq!(rows.len(), usize::from(grant == 11), "mode {mode}");
        }
        let mut removed = vec![];
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta { removed: items, .. } = event {
                removed.extend(items.into_iter().map(|item| item.row_uuid));
            }
        }
        assert_eq!(removed, vec![id(1)]);
    }
}

/// Alice reads renamed cells at a pinned remote snapshot after Bob changes the
/// live title. The cached newer version then prevents committing the stale read.
#[test]
fn cold_alias_snapshot_is_pinned_and_conflicts_at_commit() {
    let server = open(0);
    seed(&server);
    let client = open(1);
    connect(&client, &server);
    let nodes = [&client, &server];
    let empty = client.prepare_query(&Query::from("docs").limit(0)).unwrap();
    let _stream =
        block_on(client.subscribe(&empty, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&nodes);
    let tx = begin(&client);
    drive(&nodes, client.tx_ref(tx).all_prepared(&empty)).unwrap();
    block_on(server.update(
        "docs",
        id(1),
        BTreeMap::from([("title".into(), Value::String("changed".into()))]),
        Default::default(),
    ))
    .unwrap();
    let q = client
        .prepare_query(&query(true, &[("title", "label")], false))
        .unwrap();
    let rows = drive(&nodes, client.tx_ref(tx).all_prepared(&q)).unwrap();
    assert_eq!(
        cells(&rows[0]),
        vec![("label".into(), Some(Value::String("z".into())))]
    );
    assert_eq!(
        block_on(client.commit_serializable_handle(tx))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}
