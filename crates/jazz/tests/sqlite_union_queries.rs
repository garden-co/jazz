//! UNION ALL reads preserve arm identity, policies and global page boundaries.
use crate::common;
use common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};
use jazz::db::{CurrentRow, Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, RelationOrderBy, RelationQuery};
use jazz::records::Value as Cell;
use jazz::tools::public_schema::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
    RelationProjectExpr, RelationRowIdRef, RelationUnionArm, RelationValueRef,
};
use jazz::tools::{ColumnType, SchemaBuilder, TableSchema, Value};
use std::collections::BTreeMap;

fn open(policy: bool) -> Db {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .column("value", ColumnType::BigInt)
                .column("owner", ColumnType::Text)
                .column("_arm", ColumnType::BigInt)
                .policies(if policy {
                    read_and_allow_all_writes(session_eq("owner", &["user", "identity", "subject"]))
                } else {
                    allow_all_policies()
                }),
        )
        .build();
    block_on(Db::open_history_complete(DbConfig::new(
        compile_schema(&schema),
        DbIdentity {
            node: NodeUuid::from_bytes([0x65; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
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
/// Alice reads two occurrences of alpha, then edits one arm's membership.
/// SQLite applies the window after the union and deltas remove only that arm.
#[test]
fn duplicate_occurrences_survive_windows_and_subscription_removals() {
    let db = open(false);
    let alpha = insert(&db, "alpha", false, 0, "alice");
    insert(&db, "beta", false, 1, "alice");
    insert(&db, "gamma", true, 2, "alice");
    let prepared = db.prepare_query(&query(window(both(), 0, 2))).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(titles(&rows), ["alpha", "alpha"]);
    assert_ne!(rows[0].union_arm(), rows[1].union_arm());
    let offset = db.prepare_query(&query(window(both(), 1, 2))).unwrap();
    assert_eq!(
        titles(&block_on(db.all(&offset, ReadOpts::default())).unwrap()),
        ["alpha", "beta"]
    );
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    while stream.try_next_event().is_some() {}
    block_on(db.update(
        "todos",
        alpha,
        BTreeMap::from([("done".into(), Cell::Bool(true))]),
        Default::default(),
    ))
    .unwrap();
    let mut removed = vec![];
    let mut added = vec![];
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta {
            removed: r,
            added: a,
            ..
        } = event
        {
            removed.extend(r.into_iter().map(|r| (r.row_uuid, r.union_arm)));
            added.extend(a.into_iter().map(|r| titles(&[r.row])));
        }
    }
    assert_eq!(removed, [(alpha, Some("byDone".into()))]);
    assert_eq!(added.concat(), ["beta"]);
    assert_eq!(
        titles(&block_on(db.all(&prepared, ReadOpts::default())).unwrap()),
        ["alpha", "beta"]
    );
    insert(&db, "aardvark", false, 0, "alice");
    assert_eq!(
        titles(&block_on(db.all(&prepared, ReadOpts::default())).unwrap()),
        ["aardvark", "alpha"]
    );
}
/// Bob can read his own rows through either arm, but neither arm grants access
/// to Alice's row. Exact signed bigint predicates do not round through f64.
#[test]
fn every_arm_applies_read_policy_and_exact_bigint_predicates() {
    let db = open(true);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let owner = bob.principal_parts().1;
    let big = 9_007_199_254_740_993i64;
    insert(&db, "positive", false, big, &owner);
    insert(&db, "negative", false, -big, &owner);
    insert(&db, "nearby", false, big - 1, &owner);
    insert(&db, "hidden", false, big, "alice");
    let prepared = db
        .prepare_query(&query(window(
            union(vec![
                ("positive", arm(Some(("value", Value::BigInt(big))))),
                ("negative", arm(Some(("value", Value::BigInt(-big))))),
            ]),
            0,
            10,
        )))
        .unwrap();
    assert_eq!(
        titles(&block_on(db.all_for_identity(&prepared, ReadOpts::default(), bob)).unwrap()),
        ["negative", "positive"]
    );
    let prepared = db
        .prepare_query(&query(union(vec![
            ("first", arm(None)),
            ("second", arm(None)),
        ])))
        .unwrap();
    let rows = block_on(db.all_for_identity(&prepared, ReadOpts::default(), bob)).unwrap();
    assert_eq!(rows.len(), 6);
    assert!(!titles(&rows).iter().any(|title| title == "hidden"));
}
/// Alice nests a windowed union: its page is retained inside the outer union,
/// and length-prefixed arm paths distinguish repeated source rows.
#[test]
fn nested_union_windows_and_labels_remain_local_to_their_level() {
    let db = open(false);
    insert(&db, "alpha", false, 0, "alice");
    insert(&db, "beta", false, 0, "alice");
    let inner = window(union(vec![("a", arm(None)), ("b", arm(None))]), 1, 2);
    let prepared = db
        .prepare_query(&query(window(
            union(vec![("nested", inner), ("plain", arm(None))]),
            0,
            10,
        )))
        .unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(titles(&rows), ["alpha", "alpha", "beta", "beta"]);
    assert_eq!(
        rows.iter()
            .filter(|r| r.union_arm() == Some("6:nested1:b"))
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r.union_arm() == Some("plain"))
            .count(),
        2
    );
}

/// Alice's chunked titles share their prefix key. Both an arm-local window
/// and the final union window must use complete-value ranks, including UTF-8.
#[test]
fn chunked_titles_sort_logically_at_both_union_levels() {
    let db = open(false);
    let prefix = "é".repeat(40_000);
    let low = format!("{prefix}a");
    let high = format!("{prefix}z");
    insert(&db, &high, false, 0, "alice");
    insert(&db, &low, false, 0, "alice");
    let prepared = db
        .prepare_query(&query(window(
            union(vec![
                ("first", window(arm(None), 0, 1)),
                ("second", arm(None)),
            ]),
            1,
            2,
        )))
        .unwrap();
    assert_eq!(
        titles(&block_on(db.all(&prepared, ReadOpts::default())).unwrap()),
        [low, high]
    );
}

/// Alice's arms retain identity and tie order when declarations are reordered.
/// The binding key uses the existing result-key v2 bytes, with no new format.
#[test]
fn arm_keys_and_tie_order_are_stable_and_use_result_key_v2() {
    let db = open(false);
    let row = RowUuid::from_bytes([0x65; 16]);
    block_on(db.insert(
        "todos",
        BTreeMap::from([
            ("title".into(), Cell::String("alpha".into())),
            ("done".into(), Cell::Bool(false)),
            ("value".into(), Cell::I64(0)),
            ("owner".into(), Cell::String("alice".into())),
            ("_arm".into(), Cell::I64(65)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    let mut keys = Vec::new();
    for labels in [["a", "b"], ["b", "a"]] {
        let prepared = db
            .prepare_query(&query(union(
                labels.into_iter().map(|label| (label, arm(None))).collect(),
            )))
            .unwrap();
        let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
        assert_eq!(
            rows.iter().map(CurrentRow::union_arm).collect::<Vec<_>>(),
            [Some("a"), Some("b")]
        );
        keys.push(
            rows.iter()
                .map(|row| row.occurrence_key())
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(keys[0], keys[1]);
    assert_ne!(keys[0][0], keys[0][1]);
    // This low-level byte check pins the binding contract, rather than relying
    // on a round trip through a decoder that could share an encoder mistake.
    let wire: Vec<Vec<u8>> = keys[0]
        .iter()
        .map(|key| serde_json::from_value(serde_json::to_value(key).unwrap()).unwrap())
        .collect();
    let fixture: Vec<Vec<u8>> =
        serde_json::from_str(include_str!("../fixtures/sqlite_union_result_keys_v2.json")).unwrap();
    assert_eq!(wire, fixture);
}

/// Alice can attach a UNION while offline, but a Remote stream cannot claim
/// an authority result before coverage arrives. Local reads remain available.
#[test]
fn remote_union_reads_wait_for_authority_coverage() {
    // A fresh owner without authority mode is a replica once it attaches.
    let client = open(false);
    let (up, _down) = duplex_transport::duplex();
    block_on(client.connect_upstream(up));
    let prepared = client.prepare_query(&query(both())).unwrap();
    let mut preview = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    while let Some(event) = preview.try_next_event() {
        match event {
            SubscriptionEvent::Delta { settled, .. } => assert!(!settled),
            SubscriptionEvent::Rejected { reason } => panic!("offline attachment: {reason}"),
            SubscriptionEvent::Closed => panic!("offline attachment closed"),
        }
    }
    let mut remote = block_on(client.subscribe(
        &prepared,
        ReadOpts::for_read_tier(jazz::db::ReadTier::Remote),
    ))
    .unwrap();
    assert!(remote.try_next_event().is_none());
    assert!(
        block_on(client.all(
            &prepared,
            ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly)
        ))
        .is_ok()
    );
}

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

/// Alice renames title to owner, another stored text column. UNION ordering
/// must rank the title chunks and preserve only the requested output column.
#[test]
fn renamed_union_columns_sort_their_physical_large_value_source() {
    fn narrow(expr: &mut RelationExpr) {
        match expr {
            RelationExpr::Union { inputs } => {
                for arm in inputs {
                    narrow(&mut arm.input);
                }
            }
            RelationExpr::Project { columns, .. } => {
                columns.retain(|column| column.alias == "id" || column.alias == "title");
                for column in columns {
                    if column.alias == "title" {
                        column.alias = "owner".into();
                    }
                }
            }
            RelationExpr::OrderBy { input, terms } => {
                for term in terms {
                    term.column.column = "owner".into();
                }
                narrow(input);
            }
            RelationExpr::Offset { input, .. } | RelationExpr::Limit { input, .. } => narrow(input),
            _ => {}
        }
    }
    let db = open(false);
    let prefix = "é".repeat(40_000);
    let low = format!("{prefix}a");
    let high = format!("{prefix}z");
    insert(&db, &high, false, 0, "alice");
    insert(&db, &low, false, 0, "alice");
    let mut rel = window(union(vec![("a", arm(None)), ("b", arm(None))]), 1, 2);
    narrow(&mut rel);
    let prepared = db.prepare_query(&query(rel)).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 2);
    for (row, expected) in rows.iter().zip([low, high]) {
        let cells = row
            .cells()
            .map(|(name, value)| (name.to_owned(), value.cloned()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            cells,
            BTreeMap::from([("owner".into(), Some(Cell::String(expected)))])
        );
    }
    assert_eq!(
        rows.iter().map(CurrentRow::union_arm).collect::<Vec<_>>(),
        [Some("b"), Some("a")]
    );
}
