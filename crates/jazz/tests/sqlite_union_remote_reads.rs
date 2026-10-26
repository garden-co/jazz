//! Remote UNION receipts preserve semantic occurrences without loading arm prefixes.
mod common;
use common::{compile_schema, read_and_allow_all_writes, session_eq};
use jazz::db::{
    CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SerializedReadResult,
    SubscriptionEvent, SubscriptionStream, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, RelationOrderBy, RelationQuery};
use jazz::records::Value as Cell;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationUnionArm, RelationValueRef,
};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchema, Value};
use jazz::tx::DurabilityTier;
use std::collections::BTreeMap;

fn open(node: u8) -> Db {
    open_with_permissions(node, true)
}
fn open_with_permissions(node: u8, permissions: bool) -> Db {
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
            false,
            || false,
            |_| {},
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
/// The receipt names final occurrences, so absent predecessors do not shift pages.
#[test]
fn nested_and_leaf_windows_do_not_require_cold_predecessors() {
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

/// Alice's cold ordinary relay forwards the exact UNION occurrences, then a
/// second read observes Bob changing one arm without duplicating the root.
#[test]
fn ordinary_relay_keeps_union_occurrences_and_new_membership() {
    let server = open(0);
    let alpha = insert(&server, "alpha", false, 0, "alice");
    insert(&server, "beta", false, 1, "alice");
    let relay = open(2);
    let client = open(1);
    connect(&relay, &server);
    connect(&client, &relay);
    let q = query(window(both(), 0, 2));
    let nodes = [&client, &relay, &server];
    assert_eq!(visible(&fetch(&client, &nodes, &q)), expected(&server, &q));
    block_on(server.update(
        "todos",
        alpha,
        BTreeMap::from([("done".into(), Cell::Bool(true))]),
        Default::default(),
    ))
    .unwrap();
    let rows = fetch(&client, &nodes, &q);
    assert_eq!(visible(&rows), expected(&server, &q));
    assert_eq!(titles(&rows), ["alpha", "beta"]);
}

/// Alice's joined UNION arm needs readable grant rows even though they are not
/// output occurrences. Direct and private-owner reads honour grant revocation.
#[test]
fn joined_union_arms_ship_readable_witnesses_at_the_receipt_cut() {
    use jazz::query::{RelationJoinCondition, RelationJoinKind};
    for private in [false, true] {
        let server = open(0);
        let alpha = insert(&server, "alpha", false, 0, "alice");
        insert(&server, "beta", false, 1, "alice");
        let grant = block_on(server.insert(
            "grants",
            BTreeMap::from([
                ("root".into(), Cell::Uuid(alpha.0)),
                ("owner".into(), Cell::String("alice".into())),
            ]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid();
        let client = open(1);
        let relay = open(2);
        let nodes = if private {
            relay.set_scope_isolated_storage_owner().unwrap();
            connect(&relay, &server);
            connect(&client, &relay);
            vec![&client, &relay, &server]
        } else {
            connect(&client, &server);
            vec![&client, &server]
        };
        let mut joined = arm(None);
        let RelationExpr::Project { input, .. } = &mut joined else {
            unreachable!()
        };
        **input = RelationExpr::Join {
            left: input.clone(),
            right: Box::new(RelationExpr::TableScan {
                table: "grants".into(),
                alias: None,
            }),
            on: vec![RelationJoinCondition {
                left: column("id"),
                right: RelationColumnRef {
                    scope: Some("grants".into()),
                    column: "root".into(),
                },
            }],
            join_kind: RelationJoinKind::Inner,
        };
        let q = query(window(
            union(vec![
                ("grant", joined),
                ("title", arm(Some(("title", Value::Text("beta".into()))))),
            ]),
            0,
            3,
        ));
        assert_eq!(visible(&fetch(&client, &nodes, &q)), expected(&server, &q));
        block_on(server.update(
            "grants",
            grant,
            BTreeMap::from([("owner".into(), Cell::String("bob".into()))]),
            Default::default(),
        ))
        .unwrap();
        let rows = fetch(&client, &nodes, &q);
        assert_eq!(visible(&rows), expected(&server, &q));
        assert_eq!(titles(&rows), ["beta"]);
    }
}

/// Alice hydrates large cells for just the selected UNION occurrences. A warm
/// cache from a different query cannot introduce extra roots or arm occurrences.
#[test]
fn large_values_and_unrelated_cached_rows_keep_exact_union_outputs() {
    let server = open(0);
    let title = "alpha".repeat(16_000);
    insert(&server, &title, false, 0, "alice");
    insert(&server, "beta", false, 1, "alice");
    insert(&server, "gamma", false, 2, "alice");
    let client = open(1);
    connect(&client, &server);
    let large_query = query(window(
        union(vec![("a", arm(None)), ("b", arm(None))]),
        0,
        2,
    ));
    assert_eq!(
        titles(&fetch(&client, &[&client, &server], &large_query)),
        [title.clone(), title.clone()]
    );
    fetch(&client, &[&client, &server], &Query::from("todos"));
    let q = query(window(
        union(vec![
            ("first", window(arm(None), 1, 1)),
            ("last", window(arm(None), 2, 1)),
        ]),
        1,
        1,
    ));
    let rows = fetch(&client, &[&client, &server], &q);
    assert_eq!(visible(&rows), expected(&server, &q));
    assert_eq!(titles(&rows), ["gamma"]);
    let q = query(window(
        union(vec![("a", arm(None)), ("b", arm(None))]),
        0,
        2,
    ));
    let rows = fetch(&client, &[&client, &server], &q);
    assert_eq!(titles(&rows), [title.clone(), title]);
}

struct Tamper {
    link: Box<dyn jazz::db::Transport>,
    mode: &'static str,
}
impl jazz::db::Transport for Tamper {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        use jazz::db::SyncMessage;
        let message = self.link.try_recv()?;
        Some(match message {
            SyncMessage::UnionAnswerV1 {
                id,
                mut through,
                mut occurrences,
            } => {
                match self.mode {
                    "unknown" => occurrences[0].arm = "not an arm".into(),
                    "duplicate" => occurrences.push(occurrences[0].clone()),
                    "cut" => through += 1,
                    "missing" => return self.link.try_recv(),
                    "downgrade" => {
                        return Some(SyncMessage::Answer {
                            id,
                            through,
                            rows: occurrences.iter().map(|o| o.row).collect(),
                        });
                    }
                    _ => unreachable!(),
                }
                SyncMessage::UnionAnswerV1 {
                    id,
                    through,
                    occurrences,
                }
            }
            message => message,
        })
    }
}

/// Mallory substitutes unknown arms, duplicate occurrences or a legacy row-ID
/// reply. Transport mutation is needed to test receipt admission; none can turn
/// an incomplete or differently shaped answer into covered UNION output.
#[test]
fn malformed_or_downgraded_union_receipts_are_rejected() {
    for mode in ["unknown", "duplicate", "downgrade"] {
        let server = open(0);
        insert(&server, "alpha", false, 0, "alice");
        let client = open(1);
        let (up, down) = duplex();
        block_on(client.connect_upstream(Box::new(Tamper { link: up, mode })));
        server.accept_subscriber(down, alice());
        let bytes = postcard::to_allocvec(&query(both())).unwrap();
        let error = drive(
            &[&client, &server],
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
        .unwrap_err();
        assert!(
            matches!(
                error.code,
                jazz::db::ErrorCode::Protocol | jazz::db::ErrorCode::NotObserved
            ),
            "{mode}: {error:?}"
        );
    }
}

/// Alice's duplicate root occurrences have independent arm bytes. The fixture
/// pins tag 41 and field order; Mallory's suffix, truncation and noncanonical tag
/// must fail the production exact decoder. Byte-level inspection is required.
#[test]
fn union_answer_v1_has_fixed_complete_bytes() {
    use jazz::db::SyncMessage;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_union_answer_v1.json")).unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.sync.union-answer.v1");
    for (key, id, through, occurrences) in [
        (
            "answer",
            128,
            300,
            serde_json::json!([{"row":vec![17_u8;16],"arm":"a"},{"row":vec![17_u8;16],"arm":"b"}]),
        ),
        ("empty", 129, 301, serde_json::json!([])),
    ] {
        let message:SyncMessage=serde_json::from_value(serde_json::json!({"UnionAnswerV1":{"id":id,"through":through,"occurrences":occurrences}})).unwrap();
        let bytes = hex::decode(fixture[key].as_str().unwrap()).unwrap();
        assert_eq!(postcard::to_allocvec(&message).unwrap(), bytes);
        let decoded = jazz::sync_codec::decode_sync_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(message).unwrap()
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        let mut overlong = vec![bytes[0] | 128, 0];
        overlong.extend_from_slice(&bytes[1..]);
        for invalid in [trailing, overlong, bytes[..bytes.len() - 1].to_vec()] {
            assert_eq!(
                jazz::sync_codec::decode_sync_message(&invalid)
                    .unwrap_err()
                    .code,
                jazz::db::ErrorCode::Protocol
            );
        }
    }
}

/// Alice's exact empty answer excludes already cached roots and occurrences
/// from an earlier query; an empty receipt never means permission to use cache.
#[test]
fn empty_union_receipt_excludes_warm_cache_occurrences() {
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    let client = open(1);
    connect(&client, &server);
    let nodes = [&client, &server];
    assert_eq!(fetch(&client, &nodes, &query(both())).len(), 2);
    let q = query(union(vec![
        ("hidden", arm(Some(("owner", Value::Text("bob".into()))))),
        ("absent", arm(Some(("title", Value::Text("absent".into()))))),
    ]));
    assert!(fetch(&client, &nodes, &q).is_empty());
    assert_eq!(fetch(&client, &nodes, &query(both())).len(), 2);
}

/// Alice's raw subscription receives exact semantic occurrences followed by a
/// settlement at the same cut. This wire-level test is needed to pin ordering,
/// which the public stream deliberately hides from callers.
#[test]
fn raw_union_subscription_pairs_occurrences_with_settlement() {
    use jazz::db::SyncMessage;
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    let (mut client, down) = duplex();
    server.accept_subscriber(down, alice());
    client
        .send(SyncMessage::Subscribe {
            id: 7,
            query: query(both()),
            binding: BTreeMap::new(),
            since: None,
            include_deleted: false,
        })
        .unwrap();
    for _ in 0..4 {
        block_on(server.tick()).unwrap();
    }
    let messages = std::iter::from_fn(|| client.try_recv()).collect::<Vec<_>>();
    let receipt = messages
        .iter()
        .position(|m| matches!(m, SyncMessage::UnionAnswerV1 { id: 7, .. }))
        .unwrap();
    let settlement = messages
        .iter()
        .position(|m| {
            matches!(
                m,
                SyncMessage::Settled {
                    id: 7,
                    global: true,
                    ..
                }
            )
        })
        .unwrap();
    assert!(receipt < settlement);
    let SyncMessage::UnionAnswerV1 {
        through,
        occurrences,
        ..
    } = &messages[receipt]
    else {
        unreachable!()
    };
    assert_eq!(occurrences.len(), 2);
    assert_eq!(occurrences[0].row, occurrences[1].row);
    assert_ne!(occurrences[0].arm, occurrences[1].arm);
    assert!(
        matches!(&messages[settlement], SyncMessage::Settled { through: cut, .. } if cut == through)
    );
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
                } => {
                    return added.into_iter().map(|row| row.row).collect();
                }
                SubscriptionEvent::Rejected { reason } => panic!("subscription rejected: {reason}"),
                event => panic!("unexpected opening {event:?}"),
            }
        }
        for node in nodes {
            block_on(node.tick()).unwrap();
        }
    }
    panic!("subscription did not open");
}

/// Alice subscribes to a cold two-occurrence page directly, through an ordinary
/// relay, and through a private owner. Bob's row is hidden; changing one arm
/// removes only that occurrence and fills the page from the next readable root.
#[test]
fn live_union_cold_page_and_arm_change_follow_authority() {
    for path in 0..3 {
        let server = open(0);
        let alpha = insert(&server, "alpha", false, 0, "alice");
        let beta = insert(&server, "beta", false, 1, "alice");
        insert(&server, "aardvark", false, 2, "bob");
        let relay = open(2);
        let client = open(1);
        if path == 2 {
            relay.set_scope_isolated_storage_owner().unwrap();
        }
        let nodes = if path == 0 {
            connect(&client, &server);
            vec![&client, &server]
        } else {
            connect(&relay, &server);
            connect(&client, &relay);
            vec![&client, &relay, &server]
        };
        let q = query(window(both(), 0, 2));
        let mut stream = block_on(client.subscribe(
            &client.prepare_query(&q).unwrap(),
            ReadOpts::for_read_tier(ReadTier::Remote),
        ))
        .unwrap();
        let rows = opening(&mut stream, &nodes);
        assert_eq!(visible(&rows), expected(&server, &q), "path {path}");
        let cached = block_on(client.all(
            &client.prepare_query(&Query::from("todos")).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
        ))
        .unwrap();
        assert_eq!(cached.len(), 1, "path {path}: do not hydrate the whole arm");
        block_on(server.update(
            "todos",
            alpha,
            BTreeMap::from([("done".into(), Cell::Bool(true))]),
            Default::default(),
        ))
        .unwrap();
        let mut removed = Vec::new();
        let mut added = Vec::new();
        for _ in 0..30 {
            for node in &nodes {
                block_on(node.tick()).unwrap();
            }
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Delta {
                        reset,
                        removed: r,
                        added: a,
                        settled,
                        tier,
                        ..
                    } => {
                        assert!(!reset, "path {path}");
                        assert!(settled, "path {path}");
                        assert_eq!(tier, DurabilityTier::Global, "path {path}");
                        removed.extend(r.into_iter().map(|r| (r.row_uuid, r.union_arm)));
                        added
                            .extend(a.into_iter().map(|r| {
                                (r.row.row_uuid(), r.row.union_arm().unwrap().to_owned())
                            }));
                    }
                    event => panic!("path {path}: {event:?}"),
                }
            }
        }
        assert_eq!(removed, [(alpha, Some("byDone".into()))], "path {path}");
        assert_eq!(added, [(beta, "byDone".into())], "path {path}");
    }
}

/// Alice's cold foreground obtains nested UNION windows through its private
/// owner. Both nonempty and empty authoritative answers finish the normal
/// temporary-subscription read without fetching the missing window prefixes.
#[test]
fn foreground_union_reads_nested_and_empty_results() {
    for empty in [false, true] {
        let server = open(0);
        if !empty {
            for title in ["alpha", "beta", "gamma", "omega"] {
                insert(&server, title, false, 0, "alice");
            }
        }
        let owner = open(2);
        owner.set_scope_isolated_storage_owner().unwrap();
        connect(&owner, &server);
        let client = open(1);
        client.set_non_durable_client();
        connect(&client, &owner);
        let q = query(window(
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
        ));
        assert_eq!(
            visible(&fetch(&client, &[&client, &owner, &server], &q)),
            expected(&server, &q)
        );
    }
}

/// Alice opens a strict remote page and a local-first preview of the same
/// UNION. Her unsent insertion appears only in the preview; after authority
/// acceptance the strict stream receives it with global settlement.
#[test]
fn pending_union_preview_does_not_share_strict_remote_results() {
    for local_first in [false, true] {
        let server = open(0);
        insert(&server, "alpha", false, 0, "alice");
        let client = open(1);
        connect(&client, &server);
        let q = client.prepare_query(&query(window(both(), 0, 2))).unwrap();
        let preview = || block_on(client.subscribe(&q, ReadOpts::default())).unwrap();
        let strict =
            || block_on(client.subscribe(&q, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
        let (mut preview, mut strict) = if local_first {
            let p = preview();
            (p, strict())
        } else {
            let s = strict();
            (preview(), s)
        };
        opening(&mut strict, &[&client, &server]);
        while preview.try_next_event().is_some() {}
        let local = insert(&client, "aardvark", false, 0, "alice");
        assert!(strict.try_next_event().is_none());
        let events = std::iter::from_fn(|| preview.try_next_event()).collect::<Vec<_>>();
        assert!(events.iter().any(|event| matches!(event, SubscriptionEvent::Delta { added, settled: false, tier: DurabilityTier::Local, .. } if added.iter().any(|row|row.row_uuid() == local))), "{events:?}");
        let mut accepted = false;
        for _ in 0..40 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
            while let Some(event) = strict.try_next_event() {
                match event {
                    SubscriptionEvent::Delta {
                        added,
                        settled,
                        tier,
                        ..
                    } => {
                        assert!(settled);
                        assert_eq!(tier, DurabilityTier::Global);
                        accepted |= added.iter().any(|row| row.row_uuid() == local);
                    }
                    other => panic!("{other:?}"),
                }
            }
        }
        assert!(accepted, "local-first opened first: {local_first}");
    }
}

/// Alice reconnects after Bob changes the authority's window. Resumed live
/// UNION coverage must include the new occurrences, even when the cached root
/// in the other arm did not change during the outage.
#[test]
fn reconnect_union_page_replaces_old_occurrence_coverage() {
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    insert(&server, "beta", false, 1, "alice");
    let client = open(1);
    connect(&client, &server);
    let q = query(window(both(), 0, 2));
    let mut stream = block_on(client.subscribe(
        &client.prepare_query(&q).unwrap(),
        ReadOpts::for_read_tier(ReadTier::Remote),
    ))
    .unwrap();
    assert_eq!(
        titles(&opening(&mut stream, &[&client, &server])),
        ["alpha", "alpha"]
    );
    client.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
    assert!(stream.try_next_event().is_none());
    let first = insert(&server, "aardvark", false, 2, "alice");
    connect(&client, &server);
    client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    let mut added = Vec::new();
    for _ in 0..30 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    added: rows,
                    settled,
                    tier,
                    ..
                } => {
                    assert!(settled);
                    assert_eq!(tier, DurabilityTier::Global);
                    added.extend(rows.into_iter().map(|row| row.row_uuid()));
                }
                other => panic!("{other:?}"),
            }
        }
    }
    assert!(added.contains(&first));
}

/// Alice's durable local-first one-shot waits for the same exact UNION page
/// coverage as a foreground read, instead of applying its offset to a partial
/// local cache. The one-shot releases its temporary subscription afterwards.
#[test]
fn local_first_wait_materializes_exact_union_window() {
    let server = open(0);
    for title in ["alpha", "beta", "gamma"] {
        insert(&server, title, false, 0, "alice");
    }
    let client = open(1);
    connect(&client, &server);
    let q = query(window(
        union(vec![("a", arm(None)), ("b", arm(None))]),
        3,
        2,
    ));
    let bytes = postcard::to_allocvec(&q).unwrap();
    let opts = ReadOpts {
        first_load: jazz::db::FirstLoad::WaitForRemote { timeout_ms: 60_000 },
        ..ReadOpts::default()
    };
    let result = drive(
        &[&client, &server],
        client.all_serialized_query(&bytes, opts, None, None, None, false, || false, |_| {}),
    )
    .unwrap();
    let SerializedReadResult::Rows(rows) = result else {
        panic!("rows")
    };
    assert_eq!(visible(&rows), expected(&server, &q));
}

/// Alice's empty UNION still rejects an unindexed text correlation. Shape
/// admission happens before evaluation, so an empty table cannot hide an
/// unsupported witness plan that would scan unrelated rows once populated.
#[test]
fn empty_union_still_validates_indexed_witness_shape() {
    use jazz::query::{RelationJoinCondition, RelationJoinKind};
    let server = open(0);
    let client = open(1);
    connect(&client, &server);
    let mut joined = arm(None);
    let RelationExpr::Project { input, .. } = &mut joined else {
        unreachable!()
    };
    **input = RelationExpr::Join {
        left: input.clone(),
        right: Box::new(RelationExpr::TableScan {
            table: "grants".into(),
            alias: None,
        }),
        on: vec![RelationJoinCondition {
            left: column("owner"),
            right: RelationColumnRef {
                scope: Some("grants".into()),
                column: "owner".into(),
            },
        }],
        join_kind: RelationJoinKind::Inner,
    };
    let bytes = postcard::to_allocvec(&query(union(vec![
        ("unsupported", joined),
        ("ordinary", arm(None)),
    ])))
    .unwrap();
    let error = drive(
        &[&client, &server],
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
    .unwrap_err();
    assert!(
        error.message.contains("indexed scalar UUID correlation"),
        "{error:?}"
    );
}

/// Mallory alters live receipts without changing the authority's row images.
/// Unknown arms, duplicates, missing receipts and mismatched cuts must reject
/// instead of releasing any globally settled result. Wire mutation is required
/// because the public authority never produces these invalid combinations.
#[test]
fn live_union_refuses_malformed_and_unpaired_coverage() {
    for mode in ["unknown", "duplicate", "downgrade", "cut", "missing"] {
        let server = open(0);
        insert(&server, "alpha", false, 0, "alice");
        let client = open(1);
        let (up, down) = duplex();
        block_on(client.connect_upstream(Box::new(Tamper { link: up, mode })));
        server.accept_subscriber(down, alice());
        let mut stream = block_on(client.subscribe(
            &client.prepare_query(&query(both())).unwrap(),
            ReadOpts::for_read_tier(ReadTier::Remote),
        ))
        .unwrap();
        let mut refused = false;
        for _ in 0..12 {
            block_on(server.tick()).unwrap();
            block_on(client.tick()).unwrap();
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Rejected { .. } => refused = true,
                    other => panic!("{mode}: invalid coverage delivered {other:?}"),
                }
            }
        }
        assert!(refused, "{mode}");
    }
}

#[derive(Default)]
struct DeliveryControl {
    paused: bool,
    held: bool,
    sent: Vec<jazz::db::SyncMessage>,
}
struct DelayedDelivery {
    link: Box<dyn jazz::db::Transport>,
    control: std::rc::Rc<std::cell::RefCell<DeliveryControl>>,
    kind: &'static str,
    held: Option<jazz::db::SyncMessage>,
}
impl jazz::db::Transport for DelayedDelivery {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        self.control.borrow_mut().sent.push(message.clone());
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        use jazz::db::SyncMessage;
        if self.held.is_some() {
            return (!self.control.borrow().paused)
                .then(|| self.held.take())
                .flatten();
        }
        let message = self.link.try_recv()?;
        let boundary = match self.kind {
            "settled" => matches!(message, SyncMessage::Settled { .. }),
            "chunks" => matches!(message, SyncMessage::Chunks { .. }),
            _ => unreachable!(),
        };
        if boundary && self.control.borrow().paused {
            self.held = Some(message);
            self.control.borrow_mut().held = true;
            None
        } else {
            Some(message)
        }
    }
}
fn delayed_connect(
    client: &Db,
    server: &Db,
    kind: &'static str,
) -> std::rc::Rc<std::cell::RefCell<DeliveryControl>> {
    let (up, down) = duplex();
    let control = std::rc::Rc::new(std::cell::RefCell::new(DeliveryControl {
        paused: true,
        ..Default::default()
    }));
    block_on(client.connect_upstream(Box::new(DelayedDelivery {
        link: up,
        control: control.clone(),
        kind,
        held: None,
    })));
    server.accept_subscriber(down, alice());
    control
}

/// Alice receives row images and a UNION receipt before its settlement, or a
/// complete receipt pair before large-value chunks. Delaying the real transport
/// tests the publication boundary without adding runtime hooks: neither case
/// may release an incomplete opening, and both finish after the missing input.
#[test]
fn live_union_waits_for_paired_settlement_and_large_content() {
    for kind in ["settled", "chunks"] {
        let server = open(0);
        let title = if kind == "chunks" {
            "alpha".repeat(16_000)
        } else {
            "alpha".into()
        };
        insert(&server, &title, false, 0, "alice");
        let client = open(1);
        let control = delayed_connect(&client, &server, kind);
        let q = query(window(
            union(vec![("a", arm(None)), ("b", arm(None))]),
            0,
            2,
        ));
        let mut stream = block_on(client.subscribe(
            &client.prepare_query(&q).unwrap(),
            ReadOpts::for_read_tier(ReadTier::Remote),
        ))
        .unwrap();
        for _ in 0..20 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
            assert!(stream.try_next_event().is_none(), "{kind}");
        }
        assert!(
            control.borrow().held,
            "{kind}: transport did not reach boundary"
        );
        control.borrow_mut().paused = false;
        assert_eq!(
            titles(&opening(&mut stream, &[&client, &server])),
            [title.clone(), title]
        );
        assert!(stream.try_next_event().is_none());
    }
}

/// Alice cancels a foreground one-shot after its occurrence receipt but before
/// settlement. The real temporary subscription must unsubscribe; its late
/// settlement cannot satisfy the next read, which obtains fresh coverage.
#[test]
fn cancelled_union_foreground_read_releases_attachment() {
    use jazz::db::SyncMessage;
    use std::future::Future;
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    let client = open(1);
    client.set_non_durable_client();
    let control = delayed_connect(&client, &server, "settled");
    let q = query(window(both(), 0, 2));
    let bytes = postcard::to_allocvec(&q).unwrap();
    let mut read = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
        false,
        || false,
        |_| {},
    ));
    for _ in 0..15 {
        assert!(
            read.as_mut()
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    assert!(control.borrow().held);
    assert_eq!(client.runtime_stats_for_test().active_subscriptions, 1);
    drop(read);
    assert_eq!(client.runtime_stats_for_test().active_subscriptions, 0);
    block_on(client.tick()).unwrap();
    let id = control
        .borrow()
        .sent
        .iter()
        .find_map(|m| match m {
            SyncMessage::Subscribe { id, .. } => Some(*id),
            _ => None,
        })
        .unwrap();
    assert!(
        control
            .borrow()
            .sent
            .iter()
            .any(|m| matches!(m, SyncMessage::Unsubscribe { id: cancelled } if *cancelled == id))
    );
    control.borrow_mut().paused = false;
    assert_eq!(
        visible(&fetch(&client, &[&client, &server], &q)),
        expected(&server, &q)
    );
    assert_eq!(client.runtime_stats_for_test().active_subscriptions, 0);
}

fn joined_grant_arm() -> RelationExpr {
    let mut joined = arm(None);
    let RelationExpr::Project { input, .. } = &mut joined else {
        unreachable!()
    };
    **input = RelationExpr::Join {
        left: input.clone(),
        right: Box::new(RelationExpr::TableScan {
            table: "grants".into(),
            alias: None,
        }),
        on: vec![jazz::query::RelationJoinCondition {
            left: column("id"),
            right: RelationColumnRef {
                scope: Some("grants".into()),
                column: "root".into(),
            },
        }],
        join_kind: jazz::query::RelationJoinKind::Inner,
    };
    joined
}

/// Alice's live UNION depends on a readable grant rather than a changed output
/// row. Bob revokes that grant: direct and private-owner subscriptions remove
/// only the dependent occurrence, retaining the independent title arm.
#[test]
fn live_union_tracks_joined_witness_revocation() {
    for private in [false, true] {
        let server = open(0);
        let alpha = insert(&server, "alpha", false, 0, "alice");
        let grant = block_on(server.insert(
            "grants",
            BTreeMap::from([
                ("root".into(), Cell::Uuid(alpha.0)),
                ("owner".into(), Cell::String("alice".into())),
            ]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid();
        let client = open(1);
        let owner = open(2);
        let nodes = if private {
            owner.set_scope_isolated_storage_owner().unwrap();
            connect(&owner, &server);
            connect(&client, &owner);
            vec![&client, &owner, &server]
        } else {
            connect(&client, &server);
            vec![&client, &server]
        };
        let q = query(window(
            union(vec![("grant", joined_grant_arm()), ("title", arm(None))]),
            0,
            2,
        ));
        let mut stream = block_on(client.subscribe(
            &client.prepare_query(&q).unwrap(),
            ReadOpts::for_read_tier(ReadTier::Remote),
        ))
        .unwrap();
        assert_eq!(
            visible(&opening(&mut stream, &nodes)),
            expected(&server, &q)
        );
        block_on(server.update(
            "grants",
            grant,
            BTreeMap::from([("owner".into(), Cell::String("bob".into()))]),
            Default::default(),
        ))
        .unwrap();
        let mut removed = Vec::new();
        for _ in 0..30 {
            for node in &nodes {
                block_on(node.tick()).unwrap();
            }
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Delta {
                        removed: r,
                        settled,
                        tier,
                        ..
                    } => {
                        assert!(settled);
                        assert_eq!(tier, DurabilityTier::Global);
                        removed.extend(r.into_iter().map(|r| (r.row_uuid, r.union_arm)));
                    }
                    other => panic!("private={private}: {other:?}"),
                }
            }
        }
        assert_eq!(
            removed,
            [(alpha, Some("grant".into()))],
            "private={private}"
        );
    }
}

/// Alice's sibling sees two semantic occurrences of her pending write through
/// a cold policy-free private owner while offline. A strict remote read still
/// waits for an authority; Mallory cannot use the owner to read Alice's inputs.
#[test]
fn offline_private_owner_union_previews_preserve_admitted_identity() {
    let owner = open_with_permissions(2, false);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
    let writer = open_with_permissions(1, false);
    writer.set_non_durable_client();
    connect(&writer, &owner);
    for _ in 0..10 {
        block_on(writer.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    let id = insert(&writer, "alpha", false, 0, "alice");
    for _ in 0..10 {
        block_on(writer.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    let sibling = open_with_permissions(3, false);
    sibling.set_non_durable_client();
    sibling.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
    connect(&sibling, &owner);
    let q = query(window(both(), 0, 2));
    assert_eq!(
        block_on(owner.all_for_identity(
            &owner.prepare_query(&q).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            alice()
        ))
        .unwrap()
        .len(),
        2
    );
    let prepared = sibling.prepare_query(&q).unwrap();
    let mut preview = block_on(sibling.subscribe(&prepared, ReadOpts::default())).unwrap();
    let mut strict =
        block_on(sibling.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    let mut seen = Vec::new();
    for _ in 0..30 {
        block_on(sibling.tick()).unwrap();
        block_on(owner.tick()).unwrap();
        assert!(strict.try_next_event().is_none());
        while let Some(event) = preview.try_next_event() {
            match event {
                SubscriptionEvent::Delta { added, tier, .. } => {
                    assert_eq!(tier, DurabilityTier::Local);
                    seen.extend(
                        added
                            .into_iter()
                            .map(|row| (row.row_uuid(), row.union_arm().unwrap().to_owned())),
                    );
                }
                other => panic!("{other:?}"),
            }
        }
    }
    assert_eq!(seen, [(id, "byDone".into()), (id, "byTitle".into())],);
    let (mut peer, down) = duplex();
    owner.accept_subscriber(
        down,
        AuthorSubject::authenticated("https://union-read.example", "mallory").unwrap(),
    );
    peer.send(jazz::db::SyncMessage::Subscribe {
        id: 7,
        query: q,
        binding: BTreeMap::new(),
        since: None,
        include_deleted: false,
    })
    .unwrap();
    block_on(owner.tick()).unwrap();
    let replies = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
    assert!(
        replies
            .iter()
            .any(|m| matches!(m, jazz::db::SyncMessage::Refused { id: 7, .. }))
    );
    assert!(!replies.iter().any(|m| matches!(
        m,
        jazz::db::SyncMessage::OwnerInputsV1 { .. } | jazz::db::SyncMessage::UnionAnswerV1 { .. }
    )));
}

/// Alice can preview her own offline insertion before policies are known, even
/// when its owner field names Bob. Once authority acceptance reveals that only
/// Bob may read it, that preview must disappear and a fresh foreground must
/// receive an empty UNION result. Pending ownership cannot grant accepted reads.
#[test]
fn accepted_union_rows_do_not_inherit_offline_preview_permission() {
    let owner = open_with_permissions(2, false);
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
    let writer = open_with_permissions(1, false);
    writer.set_non_durable_client();
    connect(&writer, &owner);
    for _ in 0..10 {
        block_on(writer.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    insert(&writer, "alpha", false, 0, "bob");
    for _ in 0..10 {
        block_on(writer.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    let sibling = open_with_permissions(3, false);
    sibling.set_non_durable_client();
    sibling.set_remote_link_hint(jazz::db::RemoteLinkHint::Failed);
    connect(&sibling, &owner);
    let q = query(window(both(), 0, 2));
    let mut stream =
        block_on(sibling.subscribe(&sibling.prepare_query(&q).unwrap(), ReadOpts::default()))
            .unwrap();
    let mut shown = 0;
    for _ in 0..20 {
        block_on(sibling.tick()).unwrap();
        block_on(owner.tick()).unwrap();
        while let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta { added, tier, .. } => {
                    assert_eq!(tier, DurabilityTier::Local);
                    shown += added.len();
                }
                other => panic!("{other:?}"),
            }
        }
    }
    assert_eq!(shown, 2);
    let server = open(0);
    connect(&owner, &server);
    owner.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    let mut removed = Vec::new();
    for _ in 0..40 {
        for node in [&writer, &sibling, &owner, &server] {
            block_on(node.tick()).unwrap();
        }
        while let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta { removed: r, .. } => removed.extend(r),
                other => panic!("{other:?}"),
            }
        }
    }
    assert_eq!(removed.len(), 2);
    assert_eq!(
        block_on(server.all(&server.prepare_query(&q).unwrap(), ReadOpts::default()))
            .unwrap()
            .len(),
        2,
        "authority accepted the write"
    );
    assert!(
        expected(&server, &q).is_empty(),
        "Alice's accepted read is denied"
    );
    let fresh = open_with_permissions(4, false);
    fresh.set_non_durable_client();
    connect(&fresh, &owner);
    assert!(fetch(&fresh, &[&fresh, &owner, &server], &q).is_empty());
}

/// Mallory cannot borrow Alice's live relay admission. Neither another subject
/// nor a different claim map may trigger an upstream UNION subscription or
/// receive a receipt; Alice's unchanged admission still works afterwards.
#[test]
fn live_union_relay_refuses_identity_and_claim_substitution() {
    use jazz::db::SyncMessage;
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    let relay = open(2);
    let control = delayed_connect(&relay, &server, "settled");
    control.borrow_mut().paused = false;
    for (identity, claims) in [
        (
            AuthorSubject::authenticated("https://union-read.example", "mallory").unwrap(),
            BTreeMap::new(),
        ),
        (
            alice(),
            BTreeMap::from([("extra".into(), Cell::Bool(true))]),
        ),
    ] {
        let (mut peer, down) = duplex();
        relay.accept_subscriber_with_claims(down, identity, Some(claims));
        peer.send(SyncMessage::Subscribe {
            id: 7,
            query: query(both()),
            binding: BTreeMap::new(),
            since: None,
            include_deleted: false,
        })
        .unwrap();
        for _ in 0..6 {
            block_on(relay.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        let replies = std::iter::from_fn(|| peer.try_recv()).collect::<Vec<_>>();
        assert!(replies.iter().any(|m| matches!(
            m,
            SyncMessage::Refused {
                id: 7,
                code: jazz::db::ErrorCode::WriteRejected,
                ..
            }
        )));
        assert!(!replies.iter().any(|m| matches!(
            m,
            SyncMessage::UnionAnswerV1 { .. }
                | SyncMessage::Settled { .. }
                | SyncMessage::OwnerInputsV1 { .. }
        )));
        assert!(
            !replies
                .iter()
                .any(|m| matches!(m, SyncMessage::Rows { images } if !images.is_empty()))
        );
    }
    assert!(!control.borrow().sent.iter().any(|m| matches!(
        m,
        SyncMessage::Subscribe { .. } | SyncMessage::SubscribeReadablePolicyV1 { .. }
    )));
    let client = open(1);
    connect(&client, &relay);
    let q = query(both());
    let mut stream = block_on(client.subscribe(
        &client.prepare_query(&q).unwrap(),
        ReadOpts::for_read_tier(ReadTier::Remote),
    ))
    .unwrap();
    assert_eq!(
        visible(&opening(&mut stream, &[&client, &relay, &server])),
        expected(&server, &q)
    );
}

/// Alice's live coverage uses the existing v1 UNION receipt followed by the
/// existing Settled tag. Independently assembled bytes pin both the matching
/// cut and the local/global flag, including an empty local owner response.
/// This wire-level check protects the compatibility contract, not runtime internals.
#[test]
fn live_union_coverage_pairs_have_fixed_bytes() {
    use jazz::db::SyncMessage;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_union_answer_v1.json")).unwrap();
    assert_eq!(fixture["live_role"], "jazz.sqlite.sync.union-coverage.v1");
    for (key, id, through, global) in [
        ("global_pair", 128, 300, true),
        ("local_empty_pair", 129, 301, false),
    ] {
        let pair = fixture[key].as_array().unwrap();
        assert_eq!(pair.len(), 2);
        let receipt =
            jazz::sync_codec::decode_sync_message(&hex::decode(pair[0].as_str().unwrap()).unwrap())
                .unwrap();
        assert!(
            matches!(receipt, SyncMessage::UnionAnswerV1 { id: actual, through: cut, .. } if actual == id && cut == through)
        );
        let settlement = SyncMessage::Settled {
            id,
            through,
            window: None,
            global,
        };
        let bytes = hex::decode(pair[1].as_str().unwrap()).unwrap();
        assert_eq!(postcard::to_allocvec(&settlement).unwrap(), bytes);
        let decoded = jazz::sync_codec::decode_sync_message(&bytes).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(settlement).unwrap()
        );
        assert_eq!(pair[0], fixture[if global { "answer" } else { "empty" }]);
    }
}

struct InvalidSettlementTail {
    link: Box<dyn jazz::db::Transport>,
    inject: bool,
    injected: bool,
}
impl jazz::db::Transport for InvalidSettlementTail {
    fn send(&mut self, message: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
        self.link.send(message)
    }
    fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
        if std::mem::take(&mut self.inject) {
            self.injected = true;
            return Some(jazz::db::SyncMessage::Unsubscribe { id: 0 });
        }
        let message = self.link.try_recv()?;
        if !self.injected && matches!(message, jazz::db::SyncMessage::Settled { .. }) {
            self.inject = true;
        }
        Some(message)
    }
}

/// Mallory appends an invalid message after a valid receipt pair in the same
/// sync batch. Wire injection is necessary to force rollback after staging:
/// Alice must receive neither rows nor settlement from that failed batch, and
/// a fresh read must recover through an independent valid authority response.
#[test]
fn failed_union_sync_batch_cannot_publish_staged_coverage() {
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    let client = open(1);
    let (up, down) = duplex();
    block_on(client.connect_upstream(Box::new(InvalidSettlementTail {
        link: up,
        inject: false,
        injected: false,
    })));
    server.accept_subscriber(down, alice());
    let q = query(both());
    let mut stream = block_on(client.subscribe(
        &client.prepare_query(&q).unwrap(),
        ReadOpts::for_read_tier(ReadTier::Remote),
    ))
    .unwrap();
    let mut failed = false;
    for _ in 0..20 {
        block_on(server.tick()).unwrap();
        if let Err(error) = block_on(client.tick()) {
            assert!(error.message.contains("client received"), "{error:?}");
            failed = true;
            break;
        }
        assert!(stream.try_next_event().is_none());
    }
    assert!(failed);
    assert!(stream.try_next_event().is_none());
    assert!(
        block_on(client.all(
            &client.prepare_query(&Query::from("todos")).unwrap(),
            ReadOpts::for_read_tier(ReadTier::LocalOnly)
        ))
        .unwrap()
        .is_empty()
    );
    drop(stream);
    assert_eq!(
        visible(&fetch(&client, &[&client, &server], &q)),
        expected(&server, &q)
    );
}

/// Alice reconnects with an unmatched receipt left from the old transport.
/// A bare settlement on the replacement connection cannot reuse that receipt,
/// even when its request ID and accepted cut happen to match. A synthetic peer
/// is required to send this otherwise-invalid resumed sequence.
#[test]
fn reconnect_discards_an_unpaired_union_receipt() {
    use jazz::db::SyncMessage;
    let server = open(0);
    insert(&server, "alpha", false, 0, "alice");
    let client = open(1);
    let control = delayed_connect(&client, &server, "settled");
    let mut stream = block_on(client.subscribe(
        &client.prepare_query(&query(both())).unwrap(),
        ReadOpts::for_read_tier(ReadTier::Remote),
    ))
    .unwrap();
    for _ in 0..12 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
        assert!(stream.try_next_event().is_none());
    }
    assert!(control.borrow().held);
    let (up, mut peer) = duplex();
    block_on(client.connect_upstream(up));
    block_on(client.tick()).unwrap();
    let id = std::iter::from_fn(|| peer.try_recv())
        .find_map(|m| match m {
            SyncMessage::Subscribe { id, .. } => Some(id),
            _ => None,
        })
        .unwrap();
    peer.send(SyncMessage::Settled {
        id,
        through: server.seq_high_water(),
        window: None,
        global: true,
    })
    .unwrap();
    block_on(client.tick()).unwrap();
    assert!(matches!(
        stream.try_next_event(),
        Some(SubscriptionEvent::Rejected { .. })
    ));
    assert!(stream.try_next_event().is_none());
}
