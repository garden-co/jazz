//! Nesting a read's relations reuses, within that one read, each relation's
//! compiled SQL (rebinding only the parent value) and the rows of each
//! (relation, parent value). The result must be exactly what the path
//! without reuse returns: one-shot reads, result trees and live
//! subscriptions, through read policies, nested ordering on unselected
//! columns with offsets and limits, and forward relations over arrays of
//! references with duplicates.

use std::collections::BTreeMap;

use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, UpdateOptions,
    set_relation_reuse_for_test,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

const USERS: usize = 6;
const MESSAGES: usize = 40;

fn id(kind: u8, n: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn member(n: usize) -> AuthorSubject {
    let mut bytes = [0u8; 16];
    bytes[0] = 0x55;
    bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
    AuthorSubject::for_test_bytes(bytes)
}

fn uuid(row: RowUuid) -> Value {
    Value::Uuid(row.0)
}

fn user(n: usize) -> Value {
    uuid(id(1, n % USERS))
}

fn cells(pairs: Vec<(&str, Value)>) -> BTreeMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

/// Two rooms; member 0 belongs to room 0 only. Messages and their system
/// rows are readable to room members; system rows of room-0 messages are
/// sometimes filed under room 1, so the policy hides some children.
fn seeded() -> Db {
    let me = ["identity", "subject"];
    let member_of_room = || {
        exists(
            "memberships",
            vec![outer_eq("room", "room"), session_eq("member", &me)],
        )
    };
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .column("lastSeen", ColumnType::BigInt)
                    .policies(read_and_allow_all_writes(PolicyExpr::True)),
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
                    .policies(read_and_allow_all_writes(session_eq("member", &me))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .fk_column("author", "users")
                    .column(
                        "mentions",
                        ColumnType::Array {
                            element: Box::new(ColumnType::Uuid),
                        },
                    )
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .policies(read_and_allow_all_writes(member_of_room())),
            )
            .table(
                TableSchemaBuilder::new("message_system")
                    .fk_column("room", "rooms")
                    .fk_column("message", "messages")
                    .column("kind", ColumnType::Text)
                    .column("rank", ColumnType::Integer)
                    .fk_column("actorUser", "users")
                    .fk_column("targetUser", "users")
                    .fk_column("user", "users")
                    .policies(read_and_allow_all_writes(member_of_room())),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let insert = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        block_on(db.insert(
            table,
            values,
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .unwrap();
    };
    for u in 0..USERS {
        insert(
            "users",
            id(1, u),
            cells(vec![
                ("name", Value::String(format!("user {u}"))),
                ("rank", Value::I32((u * 7 % 5) as i32)),
                ("lastSeen", Value::I64(0)),
            ]),
        );
    }
    for r in 0..2 {
        insert(
            "rooms",
            id(2, r),
            cells(vec![("name", Value::String(format!("room {r}")))]),
        );
    }
    insert(
        "memberships",
        id(3, 0),
        cells(vec![
            ("room", uuid(id(2, 0))),
            ("member", Value::String(member(0).principal_parts().1)),
        ]),
    );
    let mut system = 0;
    for m in 0..MESSAGES {
        // Mentions repeat users, in an order that is not the id order.
        let mentions = (0..m % 4).map(|k| user(m * 3 + [0, 4, 0][k])).collect();
        insert(
            "messages",
            id(4, m),
            cells(vec![
                ("room", uuid(id(2, m % 2))),
                ("author", user(m / 2)),
                ("mentions", Value::Array(mentions)),
                ("body", Value::String(format!("message {m}"))),
                ("sent_at", Value::I64(m as i64)),
            ]),
        );
        for k in 0..(m / 2) % 4 {
            insert(
                "message_system",
                id(5, system),
                cells(vec![
                    // Every third system row is filed where member 0 cannot read it.
                    ("room", uuid(id(2, usize::from(system % 3 == 2)))),
                    ("message", uuid(id(4, m))),
                    ("kind", Value::String(format!("kind {k}"))),
                    ("rank", Value::I32(((m + k * 5) % 4) as i32)),
                    ("actorUser", user(m + k)),
                    ("targetUser", user(m + k + 1)),
                    ("user", user(m)),
                ]),
            );
            system += 1;
        }
    }
    block_on(db.tick()).unwrap();
    db
}

/// Forward relations by foreign key, a reverse relation ordered by an
/// unselected column with an offset and a limit, relations nested under it,
/// and a forward relation over an array of references.
fn queries() -> Vec<(&'static str, Query)> {
    let user_ref = |name: &str| ArraySubquery::new(name, "users", "id", name);
    let system = |direction, offset, limit: Option<usize>| {
        let mut system = ArraySubquery::new("message_system", "message_system", "message", "id")
            .select(["kind"])
            .order_by("rank", direction)
            .offset(offset)
            .nested(user_ref("actorUser"))
            .nested(
                user_ref("targetUser")
                    .select(["name"])
                    .order_by("rank", OrderDirection::Desc),
            )
            .nested(user_ref("user"));
        if let Some(limit) = limit {
            system = system.limit(limit);
        }
        system
    };
    let page = || {
        Query::from("messages")
            .filter(eq(col("room"), lit(uuid(id(2, 0)))))
            .order_by("sent_at", OrderDirection::Desc)
    };
    vec![
        (
            "chat page",
            page()
                .limit(15)
                .array_subquery(user_ref("author"))
                .array_subquery(system(OrderDirection::Asc, 0, None)),
        ),
        (
            "system rows by an unselected rank, paged",
            page().array_subquery(system(OrderDirection::Desc, 1, Some(1))),
        ),
        (
            "mentions with duplicates",
            Query::from("messages")
                .order_by("sent_at", OrderDirection::Asc)
                .array_subquery(ArraySubquery::new("mentions", "users", "id", "mentions"))
                .array_subquery(user_ref("author")),
        ),
    ]
}

/// `read` without reuse, then with: they must agree.
fn agree<T: PartialEq + std::fmt::Debug>(what: &str, read: impl Fn() -> T) -> T {
    set_relation_reuse_for_test(false);
    let plain = read();
    set_relation_reuse_for_test(true);
    let reused = read();
    assert_eq!(reused, plain, "{what}");
    reused
}

/// A one-shot read as member 0, relations nested.
fn as_member(db: &Db, query: &Query) -> Vec<jazz::db::CurrentRow> {
    let prepared = db.prepare_query(query).unwrap();
    let roots = block_on(db.all_for_identity(&prepared, ReadOpts::default(), member(0))).unwrap();
    db.nest_relations(&prepared, roots).unwrap()
}

#[test]
fn reused_relations_read_exactly_what_unreused_ones_do() {
    let db = seeded();
    for (what, query) in queries() {
        let rows = agree(what, || as_member(&db, &query));
        assert!(!rows.is_empty(), "{what}: the fixture reads rows");
        let prepared = db.prepare_query(&query).unwrap();
        agree(what, || {
            block_on(db.all_result_tree(&prepared, ReadOpts::default())).unwrap()
        });
    }
}

#[test]
fn an_array_of_references_keeps_its_order_and_duplicates() {
    let db = seeded();
    let (_, query) = queries().remove(2);
    let prepared = db.prepare_query(&query).unwrap();
    let tree = agree("mentions", || {
        block_on(db.all_result_tree(&prepared, ReadOpts::default())).unwrap()
    });
    // Message 3 mentions users 9, 13 and 9 (mod 6): 3, 1 and 3 again.
    let node = tree
        .roots
        .iter()
        .find(|node| node.row.row_uuid() == id(4, 3))
        .expect("message 3");
    let jazz::result_tree::ResultRelation::Array(mentions) = &node.relations["mentions"] else {
        panic!("expected an array relation");
    };
    let mentioned: Vec<RowUuid> = mentions.iter().map(|m| m.row.row_uuid()).collect();
    assert_eq!(mentioned, [id(1, 3), id(1, 1), id(1, 3)]);
}

/// What a live result shows: each row's id and cells, nested relations
/// included (positions differ between two stores).
type Shown = Vec<(RowUuid, Vec<(String, Option<Value>)>)>;

fn shown(rows: impl IntoIterator<Item = jazz::db::SubscriptionOutputRow>) -> Shown {
    let mut rows: Shown = rows
        .into_iter()
        .map(|row| {
            let cells = row
                .row
                .cells()
                .map(|(name, value)| (name.to_owned(), value.cloned()))
                .collect();
            (row.row.row_uuid(), cells)
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows
}

/// Every event of one tick: rows added, then rows updated.
fn drain(subscription: &mut jazz::db::SubscriptionStream) -> (Shown, Shown) {
    let (mut added, mut updated) = (Vec::new(), Vec::new());
    while let Some(event) = subscription.try_next_event() {
        if let SubscriptionEvent::Delta {
            added: a,
            updated: u,
            ..
        } = event
        {
            added.extend(a);
            updated.extend(u);
        }
    }
    (shown(added), shown(updated))
}

/// Two identical stores keep the same live result, one without reuse and
/// one with: their openings, and the rows each presence write updates,
/// agree.
#[test]
fn a_live_result_re_run_with_reuse_matches_one_without() {
    for (what, query) in queries() {
        let stores = [(false, seeded()), (true, seeded())];
        let mut subscriptions: Vec<_> = stores
            .iter()
            .map(|(reuse, db)| {
                set_relation_reuse_for_test(*reuse);
                let prepared = db.prepare_query(&query).unwrap();
                let mut subscription =
                    block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), member(0)))
                        .unwrap();
                block_on(db.tick()).unwrap();
                (drain(&mut subscription), subscription)
            })
            .collect();
        assert_eq!(subscriptions[0].0, subscriptions[1].0, "{what}: opening");
        assert!(
            !subscriptions[1].0.0.is_empty(),
            "{what}: the opening shows rows"
        );
        let mut updated = 0;
        for write in 0..4 {
            let mut deltas = Vec::new();
            for ((reuse, db), (_, subscription)) in stores.iter().zip(&mut subscriptions) {
                set_relation_reuse_for_test(*reuse);
                block_on(db.update(
                    "users",
                    id(1, write % USERS),
                    cells(vec![
                        ("lastSeen", Value::I64(write as i64 + 1)),
                        ("rank", Value::I32(4 - write as i32)),
                    ]),
                    UpdateOptions::default(),
                ))
                .unwrap();
                block_on(db.tick()).unwrap();
                deltas.push(drain(subscription));
            }
            assert_eq!(deltas[0], deltas[1], "{what}: write {write}");
            updated += deltas[1].1.len();
        }
        assert!(updated > 0, "{what}: the writes reached the live result");
    }
    set_relation_reuse_for_test(true);
}
