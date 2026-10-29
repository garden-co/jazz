//! Direct optional reverse includes refresh the affected parent through the
//! ordinary identity-scoped subscription interface, as in BandChat #3978.

use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query};
use jazz::records::Value;
use jazz::tools::policy_expr::allowed_to_read;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

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
    let schema = compile_schema(
        &SchemaBuilder::new()
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
                    .policies(read_and_allow_all_writes(allowed_to_read("message"))),
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

/// bob sends a message, then reacts to it: alice sees the bounded page and
/// its reaction change without re-running the whole result. mallory sees
/// neither the messages nor the reactions.
///
/// bob -> message/reaction commit -> alice's page -> affected parent only
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
