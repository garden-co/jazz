//! Ordinary UNIONs hydrate bounded prefixes; scoped reads also pair permission evidence.
use crate::common;
use common::{compile_schema, read_and_allow_all_writes, session_eq};
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SerializedReadResult, block_on,
    duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, RelationOrderBy, RelationQuery};
use jazz::records::Value as Cell;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationUnionArm, RelationValueRef,
};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchema, Value};
use std::collections::BTreeMap;

fn open(node: u8) -> Db {
    open_with_permissions(node, true)
}
fn open_with_permissions(node: u8, permissions: bool) -> Db {
    open_as(
        node,
        permissions,
        if node == 0 {
            AuthorSubject::SYSTEM
        } else {
            alice()
        },
    )
}
fn open_as(node: u8, permissions: bool, author: AuthorSubject) -> Db {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .column("value", ColumnType::BigInt)
                .column("owner", ColumnType::Text)
                .column("_arm", ColumnType::BigInt)
                .policies(read_and_allow_all_writes(session_eq(
                    "owner",
                    &["user", "identity", "subject"],
                ))),
        )
        .table(
            TableSchema::builder("grants")
                .fk_column("root", "todos")
                .column("owner", ColumnType::Text)
                .policies(read_and_allow_all_writes(session_eq(
                    "owner",
                    &["user", "identity", "subject"],
                ))),
        )
        .build();
    let schema = compile_schema(&schema);
    let config = DbConfig::new(
        if permissions {
            schema
        } else {
            schema.without_permissions()
        },
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}
fn insert(db: &Db, title: &str, done: bool, value: i64, owner: &str) -> RowUuid {
    block_on(db.insert(
        "todos",
        BTreeMap::from([
            ("title".into(), Cell::String(title.into())),
            ("done".into(), Cell::Bool(done)),
            ("value".into(), Cell::I64(value)),
            ("owner".into(), Cell::String(owner.into())),
            ("_arm".into(), Cell::I64(65)),
        ]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid()
}
fn column(name: &str) -> RelationColumnRef {
    RelationColumnRef {
        scope: Some("todos".into()),
        column: name.into(),
    }
}
fn arm(filter: Option<(&str, Value)>) -> RelationExpr {
    let mut input = RelationExpr::TableScan {
        table: "todos".into(),
        alias: None,
    };
    if let Some((name, value)) = filter {
        input = RelationExpr::Filter {
            input: Box::new(input),
            predicate: RelationPredicate::Cmp {
                left: column(name),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::literal(value),
            },
        };
    }
    RelationExpr::Project {
        input: Box::new(input),
        columns: ["id", "title", "done", "value", "owner", "_arm"]
            .into_iter()
            .map(|name| RelationProjectColumn {
                alias: name.into(),
                expr: if name == "id" {
                    RelationProjectExpr::RowId(RelationRowIdRef::Current)
                } else {
                    RelationProjectExpr::Column(column(name))
                },
            })
            .collect(),
    }
}
fn union(inputs: Vec<(&str, RelationExpr)>) -> RelationExpr {
    RelationExpr::Union {
        inputs: inputs
            .into_iter()
            .map(|(label, input)| RelationUnionArm {
                label: label.into(),
                input,
            })
            .collect(),
    }
}
fn window(input: RelationExpr, offset: usize, limit: usize) -> RelationExpr {
    RelationExpr::Limit {
        input: Box::new(RelationExpr::Offset {
            input: Box::new(RelationExpr::OrderBy {
                input: Box::new(input),
                terms: vec![RelationOrderBy {
                    column: column("title"),
                    direction: OrderDirection::Asc,
                }],
            }),
            offset,
        }),
        limit,
    }
}
fn query(rel: RelationExpr) -> Query {
    let mut query = Query::from("todos");
    query.relation = Some(RelationQuery { rel });
    query
}
fn titles(rows: &[CurrentRow]) -> Vec<String> {
    rows.iter()
        .map(
            |row| match row.cells().find(|(name, _)| *name == "title").unwrap().1 {
                Some(Cell::String(title)) => title.clone(),
                other => panic!("unexpected title {other:?}"),
            },
        )
        .collect()
}
fn both() -> RelationExpr {
    union(vec![
        ("byDone", arm(Some(("done", Value::Boolean(false))))),
        ("byTitle", arm(Some(("title", Value::Text("alpha".into()))))),
    ])
}

fn alice() -> AuthorSubject {
    AuthorSubject::authenticated("https://union-read.example", "alice").unwrap()
}
fn connect(client: &Db, server: &Db) {
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice());
}
fn drive<T>(nodes: &[&Db], future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    for _ in 0..300 {
        if let std::task::Poll::Ready(value) = future
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        {
            return value;
        }
        for node in nodes {
            block_on(node.tick()).unwrap();
        }
    }
    panic!("read did not finish");
}
fn fetch(client: &Db, nodes: &[&Db], query: &Query) -> Vec<CurrentRow> {
    let bytes = postcard::to_allocvec(query).unwrap();
    let SerializedReadResult::Rows(rows) = drive(
        nodes,
        client.all_serialized_query(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
        ),
    )
    .unwrap() else {
        panic!("rows")
    };
    rows
}
fn visible(rows: &[CurrentRow]) -> Vec<(RowUuid, String, String)> {
    rows.iter()
        .zip(titles(rows))
        .map(|(row, title)| (row.row_uuid(), row.union_arm().unwrap().into(), title))
        .collect()
}
fn expected(server: &Db, query: &Query) -> Vec<(RowUuid, String, String)> {
    visible(
        &block_on(server.all_for_identity(
            &server.prepare_query(query).unwrap(),
            ReadOpts::default(),
            alice(),
        ))
        .unwrap(),
    )
}

/// Alice cold-reads a global UNION page containing one root in two arms. Bob's
/// hidden root is excluded independently; the replica holds only page outputs.
#[test]
fn cold_union_page_preserves_occurrences_without_loading_inputs() {
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    insert(&server, "beta", false, 1, "alice");
    insert(&server, "aardvark", false, 2, "bob");
    for n in 0..30 {
        insert(&server, &format!("tail {n:03}"), false, 3, "alice");
    }
    let client = open(1);
    connect(&client, &server);
    let q = query(window(both(), 0, 2));
    let rows = fetch(&client, &[&client, &server], &q);
    assert_eq!(visible(&rows), expected(&server, &q));
    assert_eq!(titles(&rows), ["alpha", "alpha"]);
    assert_ne!(rows[0].union_arm(), rows[1].union_arm());
    let cached = block_on(client.all(
        &client.prepare_query(&Query::from("todos")).unwrap(),
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
    ))
    .unwrap();
    assert_eq!(
        cached.len(),
        1,
        "a two-occurrence page must not load whole arm inputs"
    );
}

/// Alice reads offsets inside leaf and nested UNION windows on a cold replica.
/// Bounded predecessor inputs keep both inner and outer page boundaries stable.
#[test]
fn nested_and_leaf_windows_hydrate_their_cold_predecessors() {
    let server = open(0);
    for title in ["alpha", "beta", "gamma", "omega"] {
        insert(&server, title, false, 0, "alice");
    }
    let client = open(1);
    connect(&client, &server);
    let rel = window(
        union(vec![
            (
                "nested",
                window(
                    union(vec![
                        ("leaf", window(arm(None), 1, 2)),
                        ("again", window(arm(None), 2, 1)),
                    ]),
                    1,
                    2,
                ),
            ),
            ("other", window(arm(None), 3, 1)),
        ]),
        1,
        2,
    );
    let q = query(rel);
    let rows = fetch(&client, &[&client, &server], &q);
    assert_eq!(visible(&rows), expected(&server, &q));
    assert_eq!(rows.len(), 2);
}

/// Alice reads a nested page directly, and through an ordinary
/// relay. Only its bounded predecessors are cached; Bob's earlier row and
/// an unrelated tail never become page support. A later reorder stays correct.
#[test]
fn nested_union_prefixes_are_bounded_and_policy_checked_on_each_hop() {
    for path in ["direct", "relay"] {
        let server = open(0);
        insert(&server, "000 private", false, 0, "bob");
        insert(&server, "alpha", false, 0, "alice");
        let beta = insert(&server, "beta", false, 0, "alice");
        insert(&server, "gamma", false, 0, "alice");
        insert(&server, "omega", false, 0, "alice");
        for n in 0..40 {
            insert(&server, &format!("tail {n:03}"), false, n, "alice");
        }
        let owner = open(2);
        let client = open(1);
        if path == "direct" {
            connect(&client, &server);
        } else {
            connect(&owner, &server);
            connect(&client, &owner);
        }
        let nodes = [&client, &owner, &server];
        let q = query(window(
            union(vec![(
                "nested",
                window(union(vec![("leaf", window(arm(None), 1, 2))]), 1, 1),
            )]),
            0,
            1,
        ));
        let rows = fetch(&client, &nodes, &q);
        assert_eq!(visible(&rows), expected(&server, &q), "{path}");
        assert_eq!(titles(&rows), ["gamma"], "{path}");
        let cached = block_on(client.all(
            &client.prepare_query(&Query::from("todos")).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
        ))
        .unwrap();
        let mut cached = titles(&cached);
        cached.sort();
        assert_eq!(cached, ["alpha", "beta", "gamma"], "{path}");
        block_on(server.update(
            "todos",
            beta,
            BTreeMap::from([("title".into(), Cell::String("zeta".into()))]),
            Default::default(),
        ))
        .unwrap();
        assert_eq!(
            visible(&fetch(&client, &nodes, &q)),
            expected(&server, &q),
            "{path}"
        );
    }
}

/// Alice's ordinary remote read cannot turn an outer page into an unbounded
/// inner-prefix load. Both durable replicas and foregrounds receive an explicit
/// unsupported-pattern error, including when the authority has no rows.
#[test]
fn unbounded_inner_union_prefixes_refuse_instead_of_returning_empty_pages() {
    for foreground in [false, true] {
        let server = open(0);
        let client = open(1);
        if foreground {
            client.set_non_durable_client();
        }
        connect(&client, &server);
        let q = query(window(
            union(vec![(
                "leaf",
                RelationExpr::Offset {
                    input: Box::new(arm(None)),
                    offset: 1,
                },
            )]),
            0,
            1,
        ));
        let bytes = postcard::to_allocvec(&q).unwrap();
        let error = drive(
            &[&client, &server],
            client.all_serialized_query(
                &bytes,
                ReadOpts::for_read_tier(ReadTier::Remote),
                None,
                None,
                None,
            ),
        )
        .unwrap_err();
        assert!(
            error.message.contains("inner offset without a limit"),
            "{error}"
        );
    }
}

fn turns(nodes: &[&Db]) {
    for _ in 0..12 {
        for node in nodes {
            block_on(node.tick()).unwrap();
        }
    }
}

/// Alice's subscription replaces a hidden predecessor while its visible gamma
/// occurrence stays unchanged. Both relay hops must maintain that prefix, and
/// unsubscribing releases every hidden result group along with the outer read.
#[test]
fn live_prefix_replacement_keeps_the_page_and_releases_hidden_groups() {
    for relayed in [false, true] {
        let server = open(0);
        let alpha = insert(&server, "alpha", false, 0, "alice");
        insert(&server, "beta", false, 0, "alice");
        insert(&server, "gamma", false, 0, "alice");
        insert(&server, "omega", false, 0, "alice");
        let relay = open(2);
        let client = open(1);
        if relayed {
            connect(&relay, &server);
            connect(&client, &relay);
        } else {
            connect(&client, &server);
        }
        let nodes = [&client, &relay, &server];
        let q = query(window(
            union(vec![(
                "nested",
                window(union(vec![("leaf", window(arm(None), 1, 2))]), 1, 1),
            )]),
            0,
            1,
        ));
        let prepared = client.prepare_query(&q).unwrap();
        let mut stream =
            block_on(client.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote)))
                .unwrap();
        turns(&nodes);
        let mut opening = Vec::new();
        while let Some(event) = stream.try_next_event() {
            if let jazz::db::SubscriptionEvent::Delta { added, .. } = event {
                opening.extend(added.into_iter().map(|added| added.row));
            } else {
                panic!("unexpected subscription event: {event:?}");
            }
        }
        assert_eq!(titles(&opening), ["gamma"], "relayed={relayed}");
        assert!(server.fanout_stats().groups > 1);
        block_on(server.transaction(async |tx| {
            tx.delete("todos", alpha, Default::default()).await?;
            tx.insert(
                "todos",
                BTreeMap::from([
                    ("title".into(), Cell::String("aardvark".into())),
                    ("done".into(), Cell::Bool(false)),
                    ("value".into(), Cell::I64(0)),
                    ("owner".into(), Cell::String("alice".into())),
                    ("_arm".into(), Cell::I64(0)),
                ]),
                Default::default(),
            )
            .await?;
            Ok(())
        }))
        .unwrap();
        turns(&nodes);
        let visible =
            block_on(client.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap();
        assert_eq!(titles(&visible), ["gamma"], "relayed={relayed}");
        let cached = block_on(client.all(
            &client.prepare_query(&Query::from("todos")).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
        ))
        .unwrap();
        let mut names = titles(&cached);
        names.sort();
        assert_eq!(names, ["aardvark", "beta", "gamma"]);
        while let Some(event) = stream.try_next_event() {
            if let jazz::db::SubscriptionEvent::Delta { added, removed, .. } = event {
                assert!(
                    added.is_empty() && removed.is_empty(),
                    "unchanged page changed"
                );
            } else {
                panic!("unexpected subscription event: {event:?}");
            }
        }
        drop(stream);
        turns(&nodes);
        assert_eq!(
            server.fanout_stats().groups,
            0,
            "authority retained hidden groups"
        );
        assert_eq!(
            relay.fanout_stats().groups,
            0,
            "relay retained hidden groups"
        );
    }
}
