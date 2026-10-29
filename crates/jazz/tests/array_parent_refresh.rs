//! Direct optional reverse includes refresh the affected parent through the
//! ordinary identity-scoped subscription interface, as in BandChat #3978.

use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
#[path = "common/duplex_transport.rs"]
mod duplex_transport;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, OrderDirection, Query};
use jazz::records::Value;
use jazz::tools::policy_expr::allowed_to_read;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn insert(db: &Db, table: &str, n: u8, cells: &[(&str, Value)]) {
    block_on(
        db.insert(
            table,
            cells
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
            InsertOptions {
                row_id: Some(id(n)),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}

fn drain(stream: &mut SubscriptionStream) -> Vec<SubscriptionEvent> {
    std::iter::from_fn(|| stream.try_next_event()).collect()
}

fn setup() -> Db {
    setup_with_reaction_policy(allowed_to_read("message"))
}

fn setup_with_reaction_policy(reaction_policy: PolicyExpr) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("senders")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .nullable_fk_column("sender", "senders")
                    .column("rank", ColumnType::BigInt)
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![
                            outer_eq("room", "room"),
                            session_eq("member", &["user", "identity", "subject"]),
                        ],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("reactions")
                    .fk_column("message", "messages")
                    .column("emoji", ColumnType::Text)
                    .policies(read_and_allow_all_writes(reaction_policy)),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    insert(&db, "rooms", 1, &[("name", Value::String("room".into()))]);
    let alice = AuthorSubject::for_test_bytes([10; 16]);
    insert(
        &db,
        "memberships",
        2,
        &[
            ("room", Value::Uuid(id(1).0)),
            ("member", Value::String(alice.principal_parts().1)),
        ],
    );
    for n in 3..6 {
        insert(
            &db,
            "messages",
            n,
            &[
                ("room", Value::Uuid(id(1).0)),
                ("rank", Value::I64(n.into())),
            ],
        );
    }
    db
}

/// The authority writes a message, then a reaction: alice sees the bounded page and
/// its reaction change without re-running the whole result. mallory sees
/// neither the messages nor the reactions.
///
/// authority -> message/reaction commit -> alice's page -> affected parent only
///                               -> mallory's page -> no rows
#[test]
fn optional_reverse_includes_refresh_only_affected_parents() {
    let db = setup();
    let query = db
        .prepare_query(
            &Query::from("messages")
                .order_by("rank", OrderDirection::Desc)
                .limit(2)
                .array_subquery(ArraySubquery::new(
                    "reactions",
                    "reactions",
                    "message",
                    "id",
                )),
        )
        .unwrap();
    let mut alice = block_on(db.subscribe_for_identity(
        &query,
        ReadOpts::default(),
        AuthorSubject::for_test_bytes([10; 16]),
    ))
    .unwrap();
    let mut mallory = block_on(db.subscribe_for_identity(
        &query,
        ReadOpts::default(),
        AuthorSubject::for_test_bytes([11; 16]),
    ))
    .unwrap();
    drain(&mut alice);
    drain(&mut mallory);
    let before = db.fanout_stats();
    insert(
        &db,
        "messages",
        6,
        &[("room", Value::Uuid(id(1).0)), ("rank", Value::I64(6))],
    );
    let events = drain(&mut alice);
    let SubscriptionEvent::Delta { added, removed, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(
        added.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
        vec![id(6)]
    );
    assert_eq!(
        removed.iter().map(|r| r.row_uuid).collect::<Vec<_>>(),
        vec![id(4)]
    );
    assert!(matches!(added[0].cell("reactions"), Some(Value::Array(rows)) if rows.is_empty()));
    assert!(drain(&mut mallory).is_empty());
    assert_eq!(
        db.fanout_stats().reran,
        before.reran,
        "new message must recheck its parent, not rebuild the page"
    );

    insert(
        &db,
        "reactions",
        7,
        &[
            ("message", Value::Uuid(id(6).0)),
            ("emoji", Value::String("like".into())),
        ],
    );
    let events = drain(&mut alice);
    let SubscriptionEvent::Delta { updated, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(
        updated.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
        vec![id(6)]
    );
    assert!(matches!(updated[0].cell("reactions"), Some(Value::Array(rows)) if rows.len() == 1));
    assert!(drain(&mut mallory).is_empty());
    assert_eq!(
        db.fanout_stats().reran,
        before.reran,
        "reaction must refresh its parent, not rebuild the page"
    );
}

/// alice watches two rooms with their newest readable message. The authority moves
/// a message between them and deletes it: both old and new correlations
/// refresh, while removing alice's membership still rechecks permissions.
#[test]
fn room_list_refreshes_old_and_new_parents_and_rechecks_revocation() {
    let db = setup();
    insert(&db, "rooms", 8, &[("name", Value::String("other".into()))]);
    let alice = AuthorSubject::for_test_bytes([10; 16]);
    insert(
        &db,
        "memberships",
        9,
        &[
            ("room", Value::Uuid(id(8).0)),
            ("member", Value::String(alice.principal_parts().1)),
        ],
    );
    let query = db
        .prepare_query(
            &Query::from("rooms").array_subquery(
                ArraySubquery::new("newest", "messages", "room", "id")
                    .order_by("rank", OrderDirection::Desc)
                    .limit(1),
            ),
        )
        .unwrap();
    let mut stream =
        block_on(db.subscribe_for_identity(&query, ReadOpts::default(), alice)).unwrap();
    drain(&mut stream);
    let before = db.fanout_stats();
    block_on(db.update(
        "messages",
        id(5),
        [("room".to_owned(), Value::Uuid(id(8).0))].into(),
        Default::default(),
    ))
    .unwrap();
    let events = drain(&mut stream);
    let SubscriptionEvent::Delta { updated, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(updated.len(), 2);
    for parent in updated {
        let Some(Value::Array(children)) = parent.cell("newest") else {
            panic!("{parent:?}")
        };
        assert_eq!(children.len(), 1);
        let expected = if parent.row_uuid() == id(1) { 4 } else { 5 };
        let Value::Record(child) = &children[0] else {
            panic!("expected nested message, got {:?}", children[0]);
        };
        assert_eq!(child.to_values().unwrap()[0], Value::Uuid(id(expected).0));
    }
    assert_eq!(db.fanout_stats().reran, before.reran);
    block_on(db.delete("messages", id(5), Default::default())).unwrap();
    let events = drain(&mut stream);
    let SubscriptionEvent::Delta { updated, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row_uuid(), id(8));
    assert!(matches!(updated[0].cell("newest"), Some(Value::Array(rows)) if rows.is_empty()));
    assert_eq!(db.fanout_stats().reran, before.reran);
    block_on(db.delete("memberships", id(2), Default::default())).unwrap();
    let events = drain(&mut stream);
    let SubscriptionEvent::Delta { updated, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row_uuid(), id(1));
    assert!(matches!(updated[0].cell("newest"), Some(Value::Array(rows)) if rows.is_empty()));
    assert!(db.fanout_stats().reran > before.reran);
}

/// alice's full two-message page loses its newest row. The engine reruns
/// the bounded query to refill it, preserving the remaining nested arrays.
#[test]
fn deleting_a_visible_parent_refills_the_page() {
    let db = setup();
    let query = db
        .prepare_query(
            &Query::from("messages")
                .order_by("rank", OrderDirection::Desc)
                .limit(2)
                .array_subquery(ArraySubquery::new(
                    "reactions",
                    "reactions",
                    "message",
                    "id",
                )),
        )
        .unwrap();
    let mut stream = block_on(db.subscribe_for_identity(
        &query,
        ReadOpts::default(),
        AuthorSubject::for_test_bytes([10; 16]),
    ))
    .unwrap();
    drain(&mut stream);
    let before = db.fanout_stats();
    block_on(db.delete("messages", id(5), Default::default())).unwrap();
    let events = drain(&mut stream);
    let SubscriptionEvent::Delta { added, removed, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(
        added.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
        vec![id(3)]
    );
    assert_eq!(
        removed.iter().map(|r| r.row_uuid).collect::<Vec<_>>(),
        vec![id(5)]
    );
    assert!(matches!(added[0].cell("reactions"), Some(Value::Array(rows)) if rows.is_empty()));
    assert_eq!(db.fanout_stats().reran, before.reran + 1);
}

/// alice watches rooms that require a readable message. The authority's first message
/// makes another room enter the result, so this include must retain the full
/// membership evaluation rather than refreshing only already-visible roots.
#[test]
fn required_arrays_keep_full_membership_evaluation() {
    let db = setup();
    insert(&db, "rooms", 8, &[("name", Value::String("other".into()))]);
    let alice = AuthorSubject::for_test_bytes([10; 16]);
    insert(
        &db,
        "memberships",
        9,
        &[
            ("room", Value::Uuid(id(8).0)),
            ("member", Value::String(alice.principal_parts().1)),
        ],
    );
    let query = db
        .prepare_query(
            &Query::from("rooms").array_subquery(
                ArraySubquery::new("messages", "messages", "room", "id")
                    .requirement(ArraySubqueryRequirement::AtLeastOne),
            ),
        )
        .unwrap();
    let mut stream =
        block_on(db.subscribe_for_identity(&query, ReadOpts::default(), alice)).unwrap();
    drain(&mut stream);
    let before = db.fanout_stats();
    insert(
        &db,
        "messages",
        6,
        &[("room", Value::Uuid(id(8).0)), ("rank", Value::I64(6))],
    );
    let events = drain(&mut stream);
    let SubscriptionEvent::Delta { added, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(
        added.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
        vec![id(8)]
    );
    assert_eq!(db.fanout_stats().reran, before.reran + 1);
}

/// alice's messages depend on her own membership, but reactions depend on
/// bob's membership. Revoking bob must refresh alice's nested results even
/// though the root policy's membership watch is pinned to alice.
#[test]
fn child_policy_dependencies_do_not_reuse_the_roots_watch_key() {
    let bob = AuthorSubject::for_test_bytes([12; 16]);
    let db = setup_with_reaction_policy(exists(
        "memberships",
        vec![PolicyExpr::eq_literal(
            "member",
            jazz::tools::Value::Text(bob.principal_parts().1),
        )],
    ));
    insert(
        &db,
        "memberships",
        12,
        &[
            ("room", Value::Uuid(id(1).0)),
            ("member", Value::String(bob.principal_parts().1)),
        ],
    );
    insert(
        &db,
        "reactions",
        7,
        &[
            ("message", Value::Uuid(id(5).0)),
            ("emoji", Value::String("like".into())),
        ],
    );
    let query = db
        .prepare_query(&Query::from("messages").array_subquery(ArraySubquery::new(
            "reactions",
            "reactions",
            "message",
            "id",
        )))
        .unwrap();
    let mut stream = block_on(db.subscribe_for_identity(
        &query,
        ReadOpts::default(),
        AuthorSubject::for_test_bytes([10; 16]),
    ))
    .unwrap();
    drain(&mut stream);
    block_on(db.delete("memberships", id(12), Default::default())).unwrap();
    let events = drain(&mut stream);
    assert!(
        !events.is_empty(),
        "bob's revocation must refresh alice's reactions"
    );
    let SubscriptionEvent::Delta { updated, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row_uuid(), id(5));
    assert!(matches!(updated[0].cell("reactions"), Some(Value::Array(rows)) if rows.is_empty()));
}

/// alice includes a sender and reactions. Renaming the sender must retain
/// the ordinary to-one include notification even when the root's stored
/// cells and its reaction array have not changed.
#[test]
fn sender_changes_still_notify_a_page_with_array_includes() {
    let db = setup();
    insert(
        &db,
        "senders",
        20,
        &[("name", Value::String("alice".into()))],
    );
    block_on(
        db.update(
            "messages",
            id(5),
            [(
                "sender".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(id(20).0)))),
            )]
            .into(),
            Default::default(),
        ),
    )
    .unwrap();
    let query = db
        .prepare_query(&Query::from("messages").include("sender").array_subquery(
            ArraySubquery::new("reactions", "reactions", "message", "id"),
        ))
        .unwrap();
    let mut stream = block_on(db.subscribe_for_identity(
        &query,
        ReadOpts::default(),
        AuthorSubject::for_test_bytes([10; 16]),
    ))
    .unwrap();
    drain(&mut stream);
    block_on(db.update(
        "senders",
        id(20),
        [("name".to_owned(), Value::String("renamed".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    let events = drain(&mut stream);
    assert!(
        !events.is_empty(),
        "sender rename must notify its referring message"
    );
    let SubscriptionEvent::Delta { updated, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(
        updated.iter().map(|r| r.row_uuid()).collect::<Vec<_>>(),
        vec![id(5)]
    );
}

/// alice's replica pauses while the authority moves one message through
/// rooms A -> B -> C. On one catch-up tick, both the previously published
/// parent A and final parent C must receive the correct nested result.
#[test]
fn queued_child_moves_refresh_the_previously_published_parent() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let config = |node| {
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    let server = block_on(Db::open_history_complete(config(21))).unwrap();
    let client = block_on(Db::open(config(22))).unwrap();
    for n in 1..=3 {
        insert(
            &server,
            "rooms",
            n,
            &[("name", Value::String(format!("room {n}")))],
        );
    }
    insert(&server, "messages", 4, &[("room", Value::Uuid(id(1).0))]);
    let (upstream, downstream) = duplex_transport::duplex();
    let _upstream = block_on(client.connect_upstream(upstream));
    server.accept_subscriber(downstream, AuthorSubject::SYSTEM);
    let query = client
        .prepare_query(
            &Query::from("rooms")
                .array_subquery(ArraySubquery::new("messages", "messages", "room", "id")),
        )
        .unwrap();
    let mut stream = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
    for _ in 0..5 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
    drain(&mut stream);
    for n in 2..=3 {
        block_on(server.update(
            "messages",
            id(4),
            [("room".to_owned(), Value::Uuid(id(n).0))].into(),
            Default::default(),
        ))
        .unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
    let events = drain(&mut stream);
    let rows: Vec<&jazz::db::SubscriptionOutputRow> = events
        .iter()
        .flat_map(|event| match event {
            SubscriptionEvent::Delta { updated, .. } => updated.iter(),
            _ => panic!("{event:?}"),
        })
        .collect();
    for (parent, expected) in [(1, 0), (3, 1)] {
        let row = rows
            .iter()
            .rev()
            .find(|row| row.row_uuid() == id(parent))
            .unwrap_or_else(|| panic!("missing parent {parent}: {events:?}"));
        assert!(
            matches!(row.cell("messages"), Some(Value::Array(children)) if children.len() == expected)
        );
    }
}

/// alice watches rooms with messages that can reference several rooms.
/// Moving a child between array-valued correlations keeps the full refresh
/// until that correlation has a specialised parent-routing proof.
#[test]
fn array_valued_correlations_keep_the_full_refresh() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .array_fk_column("rooms", "rooms")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([23; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    for n in 1..=2 {
        insert(
            &db,
            "rooms",
            n,
            &[("name", Value::String(format!("room {n}")))],
        );
    }
    insert(
        &db,
        "messages",
        3,
        &[("rooms", Value::Array(vec![Value::Uuid(id(1).0)]))],
    );
    let query = db
        .prepare_query(
            &Query::from("rooms")
                .array_subquery(ArraySubquery::new("messages", "messages", "rooms", "id")),
        )
        .unwrap();
    let mut stream = block_on(db.subscribe(&query, ReadOpts::default())).unwrap();
    drain(&mut stream);
    let before = db.fanout_stats();
    block_on(db.update(
        "messages",
        id(3),
        [("rooms".to_owned(), Value::Array(vec![Value::Uuid(id(2).0)]))].into(),
        Default::default(),
    ))
    .unwrap();
    let events = drain(&mut stream);
    assert!(
        !events.is_empty(),
        "array-valued correlation must refresh both parents"
    );
    let SubscriptionEvent::Delta { updated, .. } = &events[0] else {
        panic!("{events:?}")
    };
    assert_eq!(updated.len(), 2);
    for row in updated {
        let expected = usize::from(row.row_uuid() == id(2));
        assert!(
            matches!(row.cell("messages"), Some(Value::Array(children)) if children.len() == expected)
        );
    }
    assert_eq!(db.fanout_stats().reran, before.reran + 1);
}
