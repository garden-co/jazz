//! Retained relation envelopes normalize without losing their row selection.
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SerializedReadResult,
    SubscriptionEvent, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{
    OrderDirection, Query, RelationColumnRef, RelationExpr, RelationJoinCondition,
    RelationJoinKind, RelationOrderBy, RelationProjectColumn, RelationProjectExpr, RelationQuery,
    col, eq, lit, relation_query_to_query,
};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", "alice").unwrap()
}
fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}
fn own() -> PolicyExpr {
    PolicyExpr::eq_session(
        "owner",
        vec!["user".into(), "identity".into(), "subject".into()],
    )
}
fn policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(own())
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}
fn open(node: u8) -> Db {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .column("rank", ColumnType::BigInt)
                    .column("owner", ColumnType::Text)
                    .policies(policies()),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .fk_column("doc", "docs")
                    .column("owner", ColumnType::Text)
                    .policies(policies()),
            )
            .build(),
    )
    .unwrap();
    let config = DbConfig::new(
        schema,
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
fn seed(db: &Db) {
    for (n, owner) in [(1, "alice"), (2, "alice"), (3, "bob"), (4, "alice")] {
        db.seed_settled_mergeable_for_bootstrap(
            "docs",
            id(n),
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("title".into(), Value::String(format!("doc {n}"))),
                ("rank".into(), Value::I64(i64::from(n))),
                ("owner".into(), Value::String(owner.into())),
            ]),
        )
        .unwrap();
    }
    // Two witnesses for doc 1 must still produce one root occurrence. Bob's
    // witness for doc 2 and Bob's doc 3 independently test both read policies.
    for (n, doc, owner) in [
        (11, 1, "alice"),
        (12, 1, "alice"),
        (13, 2, "bob"),
        (14, 3, "alice"),
    ] {
        db.seed_settled_mergeable_for_bootstrap(
            "access",
            id(n),
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("doc".into(), Value::Uuid(id(doc).0)),
                ("owner".into(), Value::String(owner.into())),
            ]),
        )
        .unwrap();
    }
}
fn column(scope: &str, name: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some(scope.into()),
        column: name.into(),
    }
}
fn relation(join: bool, names: &[&str]) -> RelationQuery {
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
    RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(input),
            columns: names
                .iter()
                .map(|name| RelationProjectColumn {
                    alias: (*name).into(),
                    expr: RelationProjectExpr::Column(column("docs", name)),
                })
                .collect(),
        },
    }
}
fn bare(relation: RelationQuery) -> Query {
    let mut query = Query::from("docs");
    query.relation = Some(relation);
    query
}
fn shown(rows: &[CurrentRow]) -> Vec<(RowUuid, String)> {
    rows.iter()
        .map(|row| {
            let cells = row.cells().collect::<Vec<_>>();
            assert_eq!(cells.len(), 1, "narrow projection leaked cells: {cells:?}");
            assert_eq!(cells[0].0, "title");
            let Some(Value::String(title)) = cells[0].1 else {
                panic!("title")
            };
            (row.row_uuid(), title.clone())
        })
        .collect()
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

/// Alice's bare relation and the public normalizer's envelope share a shape,
/// preserve the existential join, and show only their projected title cell.
#[test]
fn normalized_and_bare_join_envelopes_have_the_same_projection() {
    let db = open(0);
    seed(&db);
    let rel = relation(true, &["id", "title"]);
    let normalized = db
        .prepare_query(&relation_query_to_query(&rel).unwrap())
        .unwrap();
    let bare = db.prepare_query(&bare(rel)).unwrap();
    assert_eq!(normalized.shape(), bare.shape());
    assert_eq!(normalized.query(), bare.query());
    assert!(normalized.query().relation.is_none());
    assert_eq!(normalized.query().joins.len(), 1);
    let expected = vec![
        (id(1), "doc 1".into()),
        (id(2), "doc 2".into()),
        (id(3), "doc 3".into()),
    ];
    assert_eq!(shown(&db.read(&normalized).unwrap()), expected);
    assert_eq!(shown(&db.read(&bare).unwrap()), expected);
}

/// Alice cannot accidentally widen a narrowed output or add ignored clauses;
/// reserved aliases are refused explicitly instead of replacing engine carriers.
#[test]
fn conflicting_envelopes_and_reserved_aliases_do_not_silently_change_results() {
    let db = open(0);
    let rel = relation(true, &["id", "title"]);
    let mut query = relation_query_to_query(&rel).unwrap();
    query
        .filters
        .push(eq(col("title"), lit(Value::String("ignored".into()))));
    assert!(
        db.prepare_query(&query)
            .unwrap_err()
            .to_string()
            .contains("must match")
    );
    query = relation_query_to_query(&rel).unwrap();
    query.joins.clear();
    query.limit = Some(1);
    assert!(
        db.prepare_query(&query)
            .unwrap_err()
            .to_string()
            .contains("must match")
    );
    query = bare(rel);
    query.select = Some(vec!["owner".into()]);
    assert!(
        db.prepare_query(&query)
            .unwrap_err()
            .to_string()
            .contains("not supported")
    );
    let mut renamed = relation(false, &["id", "title"]);
    let RelationExpr::Project { columns, .. } = &mut renamed.rel else {
        unreachable!()
    };
    columns[1].alias = "__internal".into();
    assert!(
        db.prepare_query(&bare(renamed))
            .unwrap_err()
            .to_string()
            .contains("reserved")
    );
}

/// Alice's projected subscription orders by an unshown rank and keeps its
/// first page correct when a rank changes or a shown title is updated.
#[test]
fn narrowed_live_windows_keep_unprojected_sort_keys() {
    let db = open(0);
    seed(&db);
    let rel = RelationQuery {
        rel: RelationExpr::Limit {
            input: Box::new(RelationExpr::OrderBy {
                input: Box::new(relation(false, &["id", "title"]).rel),
                terms: vec![RelationOrderBy {
                    column: column("docs", "rank"),
                    direction: OrderDirection::Asc,
                }],
            }),
            limit: 2,
        },
    };
    let prepared = db
        .prepare_query(&relation_query_to_query(&rel).unwrap())
        .unwrap();
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    while stream.try_next_event().is_some() {}
    block_on(db.update(
        "docs",
        id(4),
        BTreeMap::from([("rank".into(), Value::I64(0))]),
        Default::default(),
    ))
    .unwrap();
    let mut added = Vec::new();
    let mut removed = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta {
            added: a,
            removed: r,
            ..
        } = event
        {
            added.extend(a.into_iter().flat_map(|item| shown(&[item.row])));
            removed.extend(r.into_iter().map(|item| item.row_uuid));
        }
    }
    assert_eq!(added, vec![(id(4), "doc 4".into())]);
    assert_eq!(removed, vec![id(2)]);
    assert_eq!(
        shown(&db.read(&prepared).unwrap()),
        vec![(id(4), "doc 4".into()), (id(1), "doc 1".into())]
    );
    block_on(db.update(
        "docs",
        id(1),
        BTreeMap::from([("title".into(), Value::String("edited".into()))]),
        Default::default(),
    ))
    .unwrap();
    let mut updated = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { updated: items, .. } = event {
            updated.extend(items.into_iter().flat_map(|item| shown(&[item.row])));
        }
    }
    assert_eq!(updated, vec![(id(1), "edited".into())]);
}

/// Alice's cold remote relation read hydrates its junction rows, checks both
/// tables' permissions, and removes a root only after its last grant is gone.
#[test]
fn remote_projected_joins_hydrate_witnesses_and_track_revocation() {
    remote_projected_join(0);
}

/// Alice's ordinary relay cannot let cached revoked grants keep a projected
/// joined root alive after the authority removes its last readable witness.
#[test]
fn projected_join_revocation_crosses_an_ordinary_relay() {
    remote_projected_join(1);
}

/// Alice's private storage owner preserves the same joined projection and
/// revocation behavior as a direct connection to the authority.
#[test]
fn projected_join_revocation_crosses_a_private_owner() {
    remote_projected_join(2);
}

fn remote_projected_join(mode: u8) {
    let server = open(0);
    seed(&server);
    let client = open(1);
    let relay = (mode > 0).then(|| open(2));
    let connect = |server: &Db, client: &Db| {
        let (up, down) = duplex();
        block_on(client.connect_upstream(up));
        server.accept_subscriber(down, alice());
    };
    let nodes = if let Some(relay) = &relay {
        if mode == 2 {
            relay.set_scope_isolated_storage_owner().unwrap();
        }
        connect(&server, relay);
        connect(relay, &client);
        vec![&client, relay, &server, relay, &client]
    } else {
        connect(&server, &client);
        vec![&client, &server, &client]
    };
    let query = relation_query_to_query(&relation(true, &["id", "title"])).unwrap();
    let bytes = postcard::to_allocvec(&query).unwrap();
    let result = drive(
        &nodes,
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
    )
    .unwrap();
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows")
    };
    assert_eq!(shown(&rows), vec![(id(1), "doc 1".into())]);
    let prepared = client.prepare_query(&query).unwrap();
    let mut stream =
        block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
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
        if grant == 11 {
            assert_eq!(shown(&rows), vec![(id(1), "doc 1".into())]);
        } else {
            assert!(rows.is_empty());
        }
    }
    let mut removed = Vec::new();
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { removed: items, .. } = event {
            removed.extend(items.into_iter().map(|item| item.row_uuid));
        }
    }
    assert_eq!(removed, vec![id(1)]);
}

/// Alice's cold serializable relation read uses its pinned title and grant
/// versions, even when Bob changes the authority's current title afterwards.
#[test]
fn projected_join_snapshot_keeps_its_pinned_cells() {
    let server = open(0);
    seed(&server);
    let client = open(1);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice());
    let nodes = [&client, &server, &client];
    let empty = client.prepare_query(&Query::from("docs").limit(0)).unwrap();
    let _stream =
        block_on(client.subscribe(&empty, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    turns(&nodes);
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(client.begin_serializable(tx)).unwrap();
    drive(&nodes, client.serializable_tx_ref(tx).all_prepared(&empty)).unwrap();
    block_on(server.update(
        "docs",
        id(1),
        BTreeMap::from([("title".into(), Value::String("edited".into()))]),
        Default::default(),
    ))
    .unwrap();
    let query = relation_query_to_query(&relation(true, &["id", "title"])).unwrap();
    let prepared = client.prepare_query(&query).unwrap();
    let rows = drive(
        &nodes,
        client.serializable_tx_ref(tx).all_prepared(&prepared),
    )
    .unwrap();
    assert_eq!(shown(&rows), vec![(id(1), "doc 1".into())]);
    client.abandon_transaction_handle(tx).unwrap();
}
