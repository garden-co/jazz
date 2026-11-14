//! Nesting a read's relations reuses, within that one read, each relation's
//! compiled SQL (rebinding only the parent value) and the rows of each
//! (relation, parent value). The result must be exactly what the path
//! without reuse returns: one-shot reads, result trees and live
//! subscriptions, through read policies, nested ordering on unselected
//! columns with offsets and limits, and forward relations over arrays of
//! references with duplicates.
//!
//! Nesting also reads a level's rows a relation at a time (one statement
//! for all of that level's parents, each parent's offset and limit applied
//! by a correlated subquery, or for an array of references by a window),
//! which must again read exactly what the per-parent path does, with about
//! as little work. Every comparison runs the three paths in [`MODES`].

use std::collections::BTreeMap;

use crate::common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, UpdateOptions,
    set_relation_batch_for_test, set_relation_reuse_for_test,
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

/// The nesting paths compared, as (reuse, batch): per parent without
/// reuse, per parent with reuse, and a level at a time (the default).
pub(crate) const MODES: [(bool, bool); 3] = [(false, false), (true, false), (true, true)];

/// Nest relations on this thread as `mode` does.
pub(crate) fn set_mode((reuse, batch): (bool, bool)) {
    set_relation_reuse_for_test(reuse);
    set_relation_batch_for_test(batch);
}

/// `read` in each of [`MODES`]: they must agree.
fn agree<T: PartialEq + std::fmt::Debug>(what: &str, read: impl Fn() -> T) -> T {
    set_mode(MODES[0]);
    let plain = read();
    for mode in &MODES[1..] {
        set_mode(*mode);
        assert_eq!(read(), plain, "{what}: {mode:?}");
    }
    set_mode((true, true));
    plain
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
        let stores = MODES.map(|mode| (mode, seeded()));
        let mut subscriptions: Vec<_> = stores
            .iter()
            .map(|(mode, db)| {
                set_mode(*mode);
                let prepared = db.prepare_query(&query).unwrap();
                let mut subscription =
                    block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), member(0)))
                        .unwrap();
                block_on(db.tick()).unwrap();
                (drain(&mut subscription), subscription)
            })
            .collect();
        for (mode, opened) in MODES.iter().zip(&subscriptions).skip(1) {
            assert_eq!(subscriptions[0].0, opened.0, "{what}: opening, {mode:?}");
        }
        assert!(
            !subscriptions[1].0.0.is_empty(),
            "{what}: the opening shows rows"
        );
        let mut updated = 0;
        for write in 0..4 {
            let mut deltas = Vec::new();
            for ((mode, db), (_, subscription)) in stores.iter().zip(&mut subscriptions) {
                set_mode(*mode);
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
            for (mode, delta) in MODES.iter().zip(&deltas).skip(1) {
                assert_eq!(&deltas[0], delta, "{what}: write {write}, {mode:?}");
            }
            updated += deltas[1].1.len();
        }
        assert!(updated > 0, "{what}: the writes reached the live result");
    }
    set_mode((true, true));
}

mod randomized {
    //! Small random stores: several readers with different memberships,
    //! foreign keys that are NULL or dangling, a relation keyed by a text
    //! column, arrays of references that are empty or repeat, and ties in
    //! the orders. Every read path agrees with and without reuse.

    use std::collections::BTreeMap;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::{MODES, set_mode};
    use crate::common::{compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq};
    use jazz::block_on;
    use jazz::db::{
        Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, SubscriptionStream,
        UpdateOptions,
    };
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::query::{ArraySubquery, OrderDirection, Query};
    use jazz::records::Value;
    use jazz::tools::{
        ColumnType, OpenTransactionId, PolicyExpr, SchemaBuilder, TableSchemaBuilder,
    };

    const SEEDS: u64 = 12;
    const READERS: usize = 3;
    const PEOPLE: usize = 6;
    const TEAMS: usize = 3;
    const ROOMS: usize = 3;
    const POSTS: usize = 24;
    const COMMENTS: usize = 36;
    const WRITES: usize = 6;

    fn id(kind: u8, n: usize) -> RowUuid {
        let mut bytes = [0u8; 16];
        bytes[0] = kind;
        bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
        RowUuid::from_bytes(bytes)
    }

    fn reader(n: usize) -> AuthorSubject {
        let mut bytes = [0u8; 16];
        bytes[0] = 0x66;
        bytes[8..].copy_from_slice(&(n as u64).to_be_bytes());
        AuthorSubject::for_test_bytes(bytes)
    }

    fn uuid(row: RowUuid) -> Value {
        Value::Uuid(row.0)
    }

    fn cells(pairs: Vec<(&str, Value)>) -> BTreeMap<String, Value> {
        pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
    }

    /// A person, NULL, or a person who does not exist.
    fn person_or_not(rng: &mut StdRng) -> Value {
        match rng.gen_range(0..10) {
            0 | 1 => Value::Nullable(None),
            2 => uuid(id(1, 99)),
            _ => uuid(id(1, rng.gen_range(0..PEOPLE))),
        }
    }

    fn store(seed: u64) -> Db {
        let mut rng = StdRng::seed_from_u64(seed);
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
                    TableSchemaBuilder::new("people")
                        .column("name", ColumnType::Text)
                        .column("rank", ColumnType::Integer)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("teams")
                        .column("code", ColumnType::Text)
                        .column("name", ColumnType::Text)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("rooms")
                        .column("name", ColumnType::Text)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("memberships")
                        .fk_column("room", "rooms")
                        .column("member", ColumnType::Text)
                        .policies(read_and_allow_all_writes(session_eq("member", &me))),
                )
                .table(
                    TableSchemaBuilder::new("posts")
                        .fk_column("room", "rooms")
                        .nullable_fk_column("author", "people")
                        .nullable_column("team", ColumnType::Text)
                        .column(
                            "mentions",
                            ColumnType::Array {
                                element: Box::new(ColumnType::Uuid),
                            },
                        )
                        .column("rank", ColumnType::Integer)
                        .policies(read_and_allow_all_writes(member_of_room())),
                )
                .table(
                    TableSchemaBuilder::new("comments")
                        .fk_column("room", "rooms")
                        .fk_column("post", "posts")
                        .nullable_fk_column("author", "people")
                        .column("rank", ColumnType::Integer)
                        .policies(read_and_allow_all_writes(member_of_room())),
                )
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x22; 16]),
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
        for p in 0..PEOPLE {
            insert(
                "people",
                id(1, p),
                cells(vec![
                    ("name", Value::String(format!("person {p}"))),
                    ("rank", Value::I32(rng.gen_range(0..3))),
                ]),
            );
        }
        for t in 0..TEAMS {
            insert(
                "teams",
                id(2, t),
                cells(vec![
                    ("code", Value::String(format!("t{t}"))),
                    ("name", Value::String(format!("team {t}"))),
                ]),
            );
        }
        for r in 0..ROOMS {
            insert(
                "rooms",
                id(3, r),
                cells(vec![("name", Value::String(format!("room {r}")))]),
            );
        }
        // Each reader belongs to a random set of rooms, possibly none.
        let mut membership = 0;
        for n in 0..READERS {
            for r in 0..ROOMS {
                if rng.gen_bool(0.5) {
                    insert(
                        "memberships",
                        id(4, membership),
                        cells(vec![
                            ("room", uuid(id(3, r))),
                            ("member", Value::String(reader(n).principal_parts().1)),
                        ]),
                    );
                    membership += 1;
                }
            }
        }
        for p in 0..POSTS {
            let mentions = (0..rng.gen_range(0..4))
                .map(|_| uuid(id(1, rng.gen_range(0..PEOPLE + 1))))
                .collect();
            let team = match rng.gen_range(0..5) {
                0 => Value::Nullable(None),
                1 => Value::String("missing".to_owned()),
                _ => Value::String(format!("t{}", rng.gen_range(0..TEAMS))),
            };
            insert(
                "posts",
                id(5, p),
                cells(vec![
                    ("room", uuid(id(3, rng.gen_range(0..ROOMS)))),
                    ("author", person_or_not(&mut rng)),
                    ("team", team),
                    ("mentions", Value::Array(mentions)),
                    ("rank", Value::I32(rng.gen_range(0..4))),
                ]),
            );
        }
        for c in 0..COMMENTS {
            insert(
                "comments",
                id(6, c),
                cells(vec![
                    ("room", uuid(id(3, rng.gen_range(0..ROOMS)))),
                    ("post", uuid(id(5, rng.gen_range(0..POSTS)))),
                    ("author", person_or_not(&mut rng)),
                    ("rank", Value::I32(rng.gen_range(0..4))),
                ]),
            );
        }
        block_on(db.tick()).unwrap();
        db
    }

    /// A random write a live result may show.
    fn write(db: &Db, rng: &mut StdRng) {
        let (table, row, values) = match rng.gen_range(0..4) {
            0 => (
                "people",
                id(1, rng.gen_range(0..PEOPLE)),
                cells(vec![("rank", Value::I32(rng.gen_range(0..3)))]),
            ),
            1 => (
                "posts",
                id(5, rng.gen_range(0..POSTS)),
                cells(vec![("author", person_or_not(rng))]),
            ),
            2 => (
                "comments",
                id(6, rng.gen_range(0..COMMENTS)),
                cells(vec![
                    ("post", uuid(id(5, rng.gen_range(0..POSTS)))),
                    ("rank", Value::I32(rng.gen_range(0..4))),
                ]),
            ),
            _ => (
                "teams",
                id(2, rng.gen_range(0..TEAMS)),
                cells(vec![(
                    "code",
                    Value::String(format!("t{}", rng.gen_range(0..TEAMS + 1))),
                )]),
            ),
        };
        block_on(db.update(table, row, values, UpdateOptions::default())).unwrap();
        block_on(db.tick()).unwrap();
    }

    /// Posts with their author (NULL or dangling foreign keys), their team
    /// by text code, their mentions (empty or repeating), and their comments
    /// ordered by an unselected rank and paged, each with its author and
    /// its post's author; and people with the posts they wrote.
    fn queries(rng: &mut StdRng) -> Vec<Query> {
        let person = |name: &str| ArraySubquery::new(name, "people", "id", name);
        // Pages that may be empty (a limit of 0, an offset past the end) or
        // unbounded, each applied per parent.
        let offset = rng.gen_range(0..3);
        let limit = match rng.gen_range(0..5) {
            0 => None,
            n => Some(n - 1),
        };
        let mut comments = ArraySubquery::new("comments", "comments", "post", "id")
            .order_by("rank", OrderDirection::Desc)
            .offset(offset);
        if let Some(limit) = limit {
            comments = comments.limit(limit);
        }
        // Ranks tie often: ties break by id, in the last key's direction.
        let ranked_mentions = ArraySubquery::new("ranked_mentions", "people", "id", "mentions")
            .order_by("rank", OrderDirection::Desc)
            .offset(rng.gen_range(0..2))
            .limit(rng.gen_range(1..3));
        let comments = comments
            .select(["author"])
            .nested(person("author"))
            .nested(ArraySubquery::new("post", "posts", "id", "post").nested(person("author")));
        vec![
            Query::from("posts")
                .order_by("rank", OrderDirection::Asc)
                .array_subquery(person("author"))
                .array_subquery(ArraySubquery::new("team", "teams", "code", "team"))
                .array_subquery(ArraySubquery::new("mentions", "people", "id", "mentions"))
                .array_subquery(ranked_mentions)
                .array_subquery(comments),
            Query::from("people").array_subquery(
                ArraySubquery::new("posts", "posts", "author", "id")
                    .order_by("rank", OrderDirection::Desc)
                    .nested(ArraySubquery::new("team", "teams", "code", "team")),
            ),
        ]
    }

    type Shown = Vec<(RowUuid, String)>;

    fn shown(rows: impl IntoIterator<Item = jazz::db::SubscriptionOutputRow>) -> Shown {
        let mut rows: Shown = rows
            .into_iter()
            .map(|row| {
                let cells: Vec<_> = row.row.cells().collect();
                (row.row.row_uuid(), format!("{cells:?}"))
            })
            .collect();
        rows.sort();
        rows
    }

    fn drain(subscription: &mut SubscriptionStream) -> (Shown, Shown, Shown) {
        let (mut added, mut updated, mut removed) = (Vec::new(), Vec::new(), Vec::new());
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta {
                added: a,
                updated: u,
                removed: r,
                ..
            } = event
            {
                added.extend(a);
                updated.extend(u);
                removed.extend(r.into_iter().map(|row| row.row_uuid));
            }
        }
        removed.sort();
        let removed = removed.into_iter().map(|id| (id, String::new())).collect();
        (shown(added), shown(updated), removed)
    }

    /// `read` in each of [`MODES`].
    fn agree<T: PartialEq + std::fmt::Debug>(what: &str, read: impl Fn() -> T) {
        super::agree(what, read);
    }

    /// Each reader's live results on two identical stores, one without
    /// reuse and one with, through random writes.
    #[test]
    fn live_results_agree_per_reader_through_random_writes() {
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed);
            let queries = queries(&mut rng);
            let stores = MODES.map(|mode| (mode, store(seed)));
            for (q, query) in queries.iter().enumerate() {
                for n in 0..READERS {
                    let what = format!("seed {seed}, query {q}, reader {n}");
                    let mut opened: Vec<_> = stores
                        .iter()
                        .map(|(mode, db)| {
                            set_mode(*mode);
                            let prepared = db.prepare_query(query).unwrap();
                            let mut subscription = block_on(db.subscribe_for_identity(
                                &prepared,
                                ReadOpts::default(),
                                reader(n),
                            ))
                            .unwrap();
                            block_on(db.tick()).unwrap();
                            (drain(&mut subscription), subscription)
                        })
                        .collect();
                    for (mode, other) in MODES.iter().zip(&opened).skip(1) {
                        assert_eq!(opened[0].0, other.0, "{what}: opening, {mode:?}");
                    }
                    let writes = StdRng::seed_from_u64(seed * 31 + n as u64);
                    let mut rngs = [writes.clone(), writes.clone(), writes];
                    for w in 0..WRITES {
                        let mut deltas = Vec::new();
                        for (((mode, db), (_, subscription)), rng) in
                            stores.iter().zip(&mut opened).zip(&mut rngs)
                        {
                            set_mode(*mode);
                            write(db, rng);
                            deltas.push(drain(subscription));
                        }
                        for (mode, delta) in MODES.iter().zip(&deltas).skip(1) {
                            assert_eq!(&deltas[0], delta, "{what}: write {w}, {mode:?}");
                        }
                    }
                }
            }
        }
        set_mode((true, true));
    }

    /// One-shot reads on one store: result trees, nested rows, a snapshot
    /// (cut) read taken before later writes, and a serializable
    /// transaction's read set (`read_versions`). History reads carry no
    /// relations ("not supported in history queries yet"), so they never
    /// nest any.
    #[test]
    fn one_shot_reads_agree() {
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed ^ 0x0de5);
            let db = store(seed);
            let snapshot = block_on(db.capture_snapshot()).unwrap();
            for _ in 0..WRITES {
                write(&db, &mut rng);
            }
            let at_snapshot = ReadOpts {
                read_view: jazz::protocol::ReadViewSpec {
                    source: jazz::protocol::ReadViewSourceSpec::Snapshot {
                        snapshot: snapshot.clone(),
                    },
                    include_deleted: false,
                },
                ..Default::default()
            };
            for (q, query) in queries(&mut rng).iter().enumerate() {
                let what = format!("seed {seed}, query {q}");
                let prepared = db.prepare_query(query).unwrap();
                agree(&format!("{what}: tree"), || {
                    block_on(db.all_result_tree(&prepared, ReadOpts::default())).unwrap()
                });
                agree(&format!("{what}: nested rows"), || {
                    let roots = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
                    db.nest_relations(&prepared, roots).unwrap()
                });
                agree(&format!("{what}: snapshot"), || {
                    block_on(db.all_result_tree(&prepared, at_snapshot.clone()))
                        .map_err(|error| error.to_string())
                });
                agree(&format!("{what}: read set"), || {
                    let open = OpenTransactionId::new();
                    block_on(db.begin_serializable(open)).unwrap();
                    block_on(db.tx_ref(open).all_prepared(&prepared)).unwrap();
                    let mut read = db.read_set_for_test(open);
                    db.abandon_transaction_handle(open).unwrap();
                    read.sort();
                    read
                });
            }
        }
        set_mode((true, true));
    }
}

mod batched {
    //! Reading a level's children in one statement: each parent's page is
    //! its own (offset and limit per parent, ties by id), parents that share
    //! a value share its children, and parents with no exact JSON form fall
    //! back to the per-parent path.

    use std::collections::BTreeMap;

    use super::{agree, cells, id, seeded};
    use jazz::block_on;
    use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts};
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::query::{ArraySubquery, OrderDirection, Part, Query};
    use jazz::records::Value;
    use jazz::result_tree::{ResultNode, ResultRelation, ResultTree};
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

    use crate::common::{compile_schema, read_and_allow_all_writes};

    fn relation<'a>(node: &'a ResultNode, name: &str) -> &'a [ResultNode] {
        match &node.relations[name] {
            ResultRelation::Array(nodes) => nodes,
            #[allow(unreachable_patterns)]
            _ => panic!("expected an array relation"),
        }
    }

    fn tree(db: &Db, query: &Query) -> ResultTree {
        let prepared = db.prepare_query(query).unwrap();
        agree("tree", || {
            block_on(db.all_result_tree(&prepared, ReadOpts::default())).unwrap()
        })
    }

    /// Each message's system rows, ordered by an unselected rank with many
    /// ties, paged per message: exactly the rows that message alone would
    /// page, computed here from all of them.
    #[test]
    fn each_parent_is_paged_by_itself_with_ties_by_id() {
        let db = seeded();
        let all = tree(&db, &Query::from("message_system"));
        let mut by_message: BTreeMap<RowUuid, Vec<(i64, RowUuid)>> = BTreeMap::new();
        for node in &all.roots {
            let Some(Value::Uuid(message)) = node.row.cell("message") else {
                panic!("a message");
            };
            let rank = match node.row.cell("rank") {
                Some(Value::I32(rank)) => rank as i64,
                other => panic!("a rank: {other:?}"),
            };
            by_message
                .entry(RowUuid(message))
                .or_default()
                .push((rank, node.row.row_uuid()));
        }
        assert!(
            by_message.values().any(|rows| rows.len() >= 3),
            "some message has several system rows"
        );
        for (direction, offset, limit) in [
            (OrderDirection::Asc, 0, Some(1)),
            (OrderDirection::Asc, 1, Some(2)),
            (OrderDirection::Desc, 1, None),
            (OrderDirection::Desc, 0, Some(0)),
            (OrderDirection::Desc, 5, Some(2)),
        ] {
            let mut system = ArraySubquery::new("system", "message_system", "message", "id")
                .select(["kind"])
                .order_by("rank", direction)
                .offset(offset);
            if let Some(limit) = limit {
                system = system.limit(limit);
            }
            let read = tree(&db, &Query::from("messages").array_subquery(system));
            for message in &read.roots {
                let mut expected = by_message
                    .get(&message.row.row_uuid())
                    .cloned()
                    .unwrap_or_default();
                expected.sort();
                if direction == OrderDirection::Desc {
                    expected.reverse();
                }
                let expected: Vec<RowUuid> = expected
                    .into_iter()
                    .skip(offset)
                    .take(limit.unwrap_or(usize::MAX))
                    .map(|(_, row)| row)
                    .collect();
                let got: Vec<RowUuid> = relation(message, "system")
                    .iter()
                    .map(|node| node.row.row_uuid())
                    .collect();
                assert_eq!(
                    got, expected,
                    "{direction:?} offset {offset} limit {limit:?}"
                );
            }
        }
    }

    /// Levels split into several statements (tiny limits here): parent
    /// cells per few rows, children per few parent values, and values
    /// too large to share a list (some arrays of references) read alone.
    /// Each parent still gets exactly its own children.
    #[test]
    fn a_level_split_into_chunks_reads_the_same() {
        let db = seeded();
        for limits in [(1, 40), (3, 120), (7, 300)] {
            jazz::db::set_relation_batch_limits_for_test(Some(limits));
            for (what, query) in super::queries() {
                let prepared = db.prepare_query(&query).unwrap();
                agree(&format!("{what}, limits {limits:?}"), || {
                    block_on(db.all_result_tree(&prepared, ReadOpts::default())).unwrap()
                });
                agree(&format!("{what}, limits {limits:?}, as member"), || {
                    super::as_member(&db, &query)
                });
            }
        }
        jazz::db::set_relation_batch_limits_for_test(None);
    }

    /// Children as wide as a statement may be read per parent: the batched
    /// statement's extra columns would pass Durable Object SQLite's limit.
    #[test]
    fn children_too_wide_to_batch_read_per_parent() {
        let mut wide = TableSchemaBuilder::new("wide")
            .column("owner", ColumnType::Uuid)
            .column("rank", ColumnType::Integer);
        for c in 0..WIDE {
            wide = wide.column(&format!("c{c}"), ColumnType::Text);
        }
        let schema = compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("owners")
                        .column("name", ColumnType::Text)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(wide.policies(read_and_allow_all_writes(PolicyExpr::True)))
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x44; 16]),
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
        for o in 0..3 {
            insert(
                "owners",
                id(9, o),
                cells(vec![("name", Value::String(format!("owner {o}")))]),
            );
        }
        for w in 0..9 {
            let mut values = vec![
                ("owner", Value::Uuid(id(9, w % 3).0)),
                ("rank", Value::I32((w % 2) as i32)),
            ];
            let names: Vec<String> = (0..WIDE).map(|c| format!("c{c}")).collect();
            for name in &names {
                values.push((name.as_str(), Value::String(format!("{name} {w}"))));
            }
            insert("wide", id(10, w), cells(values));
        }
        block_on(db.tick()).unwrap();
        // Every text column twice, and the rank.
        let mut select: Vec<String> = (0..2 * WIDE).map(|c| format!("c{}", c % WIDE)).collect();
        select.push("rank".to_owned());
        let paged = ArraySubquery::new("wide", "wide", "owner", "id")
            .select(select)
            .order_by("rank", OrderDirection::Asc)
            .offset(1)
            .limit(1);
        let read = tree(&db, &Query::from("owners").array_subquery(paged));
        assert!(read.roots.iter().all(|o| relation(o, "wide").len() == 1));
    }

    /// Text columns of [`children_too_wide_to_batch_read_per_parent`]'s
    /// child. A Durable Object table holds at most 100 columns, system
    /// ones included, so a projection this wide selects columns twice:
    /// with id, `_clock`, `_state` and `_writer`, the per-parent statement
    /// returns 99 columns, and the batched page would return 101.
    const WIDE: usize = 47;

    /// Parents keyed by a real number have no exact JSON form, so they are
    /// read per parent; a part of a text cell reads the same either way.
    #[test]
    fn reals_fall_back_and_parts_read_the_same() {
        let schema = compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("buckets")
                        .column("at", ColumnType::Double)
                        .column("name", ColumnType::Text)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("points")
                        .column("at", ColumnType::Double)
                        .column("body", ColumnType::Text)
                        .column("rank", ColumnType::Integer)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x33; 16]),
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
        for b in 0..4 {
            insert(
                "buckets",
                id(7, b),
                cells(vec![
                    ("at", Value::F64(b as f64 / 2.0)),
                    ("name", Value::String(format!("bucket {b}"))),
                ]),
            );
        }
        for p in 0..20 {
            insert(
                "points",
                id(8, p),
                cells(vec![
                    ("at", Value::F64((p % 5) as f64 / 2.0)),
                    (
                        "body",
                        Value::String(format!("point {p} {}", "x".repeat(p))),
                    ),
                    ("rank", Value::I32((p % 3) as i32)),
                ]),
            );
        }
        block_on(db.tick()).unwrap();
        let points = ArraySubquery::new("points", "points", "at", "at")
            .order_by("rank", OrderDirection::Asc)
            .offset(1)
            .limit(2);
        let read = tree(&db, &Query::from("buckets").array_subquery(points.clone()));
        assert!(read.roots.iter().all(|b| relation(b, "points").len() == 2));
        let parts = ArraySubquery::new("points", "points", "id", "id")
            .select(["rank"])
            .select_part("body", Part::TextUtf8 { from: 2, to: 6 });
        let read = tree(
            &db,
            &Query::from("points").array_subquery(parts.nested(points)),
        );
        assert_eq!(read.roots.len(), 20);
    }
}

mod branch_views {
    //! Nested reads through a branch view composed over a base, live or
    //! frozen: each branched table's source is its effective rows (head
    //! rows, tombstones masking, then base rows the head does not
    //! override), a `UNION ALL`. Rows the head overrides, deletes, adds or
    //! moves to another parent, a read policy on the children, ordering
    //! with ties and per-parent pages must read the same per parent and
    //! batched.

    use super::{MODES, agree, cells, id, set_mode};
    use jazz::block_on;
    use jazz::db::{
        Db, DbConfig, DbIdentity, DeleteOptions, ExactWriteTarget, InsertOptions, ReadOpts,
        SubscriptionEvent, UpdateOptions, WriteTarget,
    };
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::protocol::{BranchSelector, BranchViewBase};
    use jazz::query::{ArraySubquery, OrderDirection, Query};
    use jazz::records::Value;
    use jazz::tools::{
        CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TableSchemaBuilder,
    };

    use crate::common::{compile_schema, read_and_allow_all_writes};

    const PEOPLE: usize = 4;
    const POSTS: usize = 6;
    const COMMENTS: usize = 18;

    fn branch(byte: u8) -> BranchSelector {
        BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
    }

    fn base() -> BranchSelector {
        branch(0xb1)
    }

    fn head() -> BranchSelector {
        branch(0xb2)
    }

    fn reader() -> AuthorSubject {
        AuthorSubject::for_test_bytes([0x77; 16])
    }

    fn person(n: usize) -> Value {
        Value::Uuid(id(1, n % PEOPLE).0)
    }

    fn post(n: usize) -> Value {
        Value::Uuid(id(5, n % POSTS).0)
    }

    /// Posts and comments are branched; people are not. Comments are
    /// readable only where `visible`.
    fn store() -> Db {
        let visible = PolicyExpr::Cmp {
            column: "visible".to_owned(),
            op: CmpOp::Eq,
            value: PolicyValue::Literal(jazz::tools::Value::Boolean(true)),
        };
        let schema = compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("people")
                        .column("name", ColumnType::Text)
                        .column("rank", ColumnType::Integer)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("posts")
                        .column("branch_id", ColumnType::Uuid)
                        .nullable_fk_column("author", "people")
                        .column(
                            "mentions",
                            ColumnType::Array {
                                element: Box::new(ColumnType::Uuid),
                            },
                        )
                        .column("rank", ColumnType::Integer)
                        .branch_by("branch_id")
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("comments")
                        .column("branch_id", ColumnType::Uuid)
                        .fk_column("post", "posts")
                        .nullable_fk_column("author", "people")
                        .column("rank", ColumnType::Integer)
                        .column("visible", ColumnType::Boolean)
                        .branch_by("branch_id")
                        .policies(read_and_allow_all_writes(visible)),
                )
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x55; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let insert = |table: &str, row: RowUuid, values, target| {
            block_on(db.insert(
                table,
                values,
                InsertOptions {
                    row_id: Some(row),
                    target,
                    ..Default::default()
                },
            ))
            .unwrap();
        };
        for p in 0..PEOPLE {
            insert(
                "people",
                id(1, p),
                cells(vec![
                    ("name", Value::String(format!("person {p}"))),
                    ("rank", Value::I32((p % 2) as i32)),
                ]),
                ExactWriteTarget::default(),
            );
        }
        for p in 0..POSTS {
            let mentions = (0..p % 3).map(|k| person(p + 2 * k)).collect();
            insert(
                "posts",
                id(5, p),
                cells(vec![
                    ("author", person(p)),
                    ("mentions", Value::Array(mentions)),
                    ("rank", Value::I32((p % 3) as i32)),
                ]),
                ExactWriteTarget::Branch(base()),
            );
        }
        for c in 0..COMMENTS {
            insert(
                "comments",
                id(6, c),
                cells(vec![
                    ("post", post(c)),
                    ("author", person(c / 2)),
                    // Ranks tie often: ties break by id.
                    ("rank", Value::I32((c % 2) as i32)),
                    ("visible", Value::Bool(c % 5 != 4)),
                ]),
                ExactWriteTarget::Branch(base()),
            );
        }
        block_on(db.tick()).unwrap();
        db
    }

    fn in_head() -> WriteTarget {
        WriteTarget::BranchView {
            head: head(),
            base: Some(BranchViewBase::Current(base())),
        }
    }

    /// The head's own changes over the base: a post's author and
    /// mentions, a comment moved to another post, a comment re-ranked,
    /// one hidden, one deleted (its tombstone masks the base row), and
    /// one comment only the head has.
    fn edit_head(db: &Db) {
        let update = |table: &str, row: RowUuid, values| {
            block_on(db.update(
                table,
                row,
                values,
                UpdateOptions {
                    target: in_head(),
                    ..Default::default()
                },
            ))
            .unwrap();
        };
        update(
            "posts",
            id(5, 0),
            cells(vec![
                ("author", person(3)),
                (
                    "mentions",
                    Value::Array(vec![person(2), person(2), person(1)]),
                ),
            ]),
        );
        update("comments", id(6, 1), cells(vec![("post", post(0))]));
        update("comments", id(6, 2), cells(vec![("rank", Value::I32(7))]));
        update(
            "comments",
            id(6, 3),
            cells(vec![("visible", Value::Bool(false))]),
        );
        block_on(db.delete(
            "comments",
            id(6, 6),
            DeleteOptions {
                target: in_head(),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(db.insert(
            "comments",
            cells(vec![
                ("post", post(0)),
                ("author", person(1)),
                ("rank", Value::I32(0)),
                ("visible", Value::Bool(true)),
            ]),
            InsertOptions {
                row_id: Some(id(6, 99)),
                target: ExactWriteTarget::Branch(head()),
                ..Default::default()
            },
        ))
        .unwrap();
        block_on(db.tick()).unwrap();
    }

    /// Posts with their author, mentions and a page of their comments
    /// (each with its author and its post's author); people with their
    /// posts and those posts' comments.
    fn queries() -> Vec<(&'static str, Query)> {
        let person = |name: &str| ArraySubquery::new(name, "people", "id", name);
        let comments = |offset, limit: Option<usize>| {
            let mut comments = ArraySubquery::new("comments", "comments", "post", "id")
                .order_by("rank", OrderDirection::Desc)
                .offset(offset)
                .nested(person("author"))
                .nested(ArraySubquery::new("post", "posts", "id", "post").nested(person("author")));
            if let Some(limit) = limit {
                comments = comments.limit(limit);
            }
            comments
        };
        vec![
            (
                "posts, unpaged comments",
                Query::from("posts")
                    .order_by("rank", OrderDirection::Asc)
                    .array_subquery(person("author"))
                    .array_subquery(ArraySubquery::new("mentions", "people", "id", "mentions"))
                    .array_subquery(comments(0, None)),
            ),
            (
                "posts, paged comments",
                Query::from("posts").array_subquery(comments(1, Some(2))),
            ),
            (
                "people, their posts and comments",
                Query::from("people").array_subquery(
                    ArraySubquery::new("posts", "posts", "author", "id")
                        .order_by("rank", OrderDirection::Desc)
                        .nested(comments(0, Some(3))),
                ),
            ),
        ]
    }

    /// The head over a live base, the head over the base frozen before
    /// later base writes, the base alone and the head alone.
    fn views(db: &Db) -> Vec<(&'static str, ReadOpts)> {
        let frozen = block_on(db.capture_snapshot()).unwrap();
        // Base writes after the snapshot: the live base sees them, the
        // frozen one does not.
        for (c, rank) in [(0, 5), (7, 9)] {
            block_on(db.update(
                "comments",
                id(6, c),
                cells(vec![("rank", Value::I32(rank))]),
                UpdateOptions {
                    target: WriteTarget::BranchView {
                        head: base(),
                        base: None,
                    },
                    ..Default::default()
                },
            ))
            .unwrap();
        }
        block_on(db.tick()).unwrap();
        vec![
            (
                "head over live base",
                ReadOpts::default().branch_view(head(), Some(BranchViewBase::Current(base()))),
            ),
            (
                "head over frozen base",
                ReadOpts::default()
                    .branch_view(head(), Some(BranchViewBase::snapshot(base(), frozen))),
            ),
            ("base", ReadOpts::default().branch_view(base(), None)),
            ("head alone", ReadOpts::default().branch_view(head(), None)),
        ]
    }

    #[test]
    fn one_shot_reads_agree_through_branch_views() {
        let db = store();
        edit_head(&db);
        let views = views(&db);
        for (view, opts) in &views {
            for (what, query) in queries() {
                let prepared = db.prepare_query(&query).unwrap();
                let tree = agree(&format!("{view}, {what}"), || {
                    block_on(db.all_result_tree(&prepared, opts.clone())).unwrap()
                });
                if *view != "head alone" {
                    assert!(!tree.roots.is_empty(), "{view}, {what}: rows");
                }
            }
        }
    }

    /// The head's edits show through the nested reads: post 0's comments
    /// over the live base are the moved, added and inherited ones, ordered
    /// by rank (descending, ties by id descending), without the deleted or
    /// hidden ones.
    #[test]
    fn the_head_overrides_masks_and_moves_nested_rows() {
        let db = store();
        edit_head(&db);
        let opts = ReadOpts::default().branch_view(head(), Some(BranchViewBase::Current(base())));
        let query = Query::from("posts")
            .array_subquery(
                ArraySubquery::new("comments", "comments", "post", "id")
                    .order_by("rank", OrderDirection::Desc),
            )
            .array_subquery(ArraySubquery::new("mentions", "people", "id", "mentions"));
        let prepared = db.prepare_query(&query).unwrap();
        let tree = agree("post 0", || {
            block_on(db.all_result_tree(&prepared, opts.clone())).unwrap()
        });
        let post0 = tree
            .roots
            .iter()
            .find(|node| node.row.row_uuid() == id(5, 0))
            .expect("post 0");
        let ids = |name: &str| -> Vec<RowUuid> {
            let jazz::result_tree::ResultRelation::Array(nodes) = &post0.relations[name] else {
                panic!("an array relation");
            };
            nodes.iter().map(|node| node.row.row_uuid()).collect()
        };
        // Post 0's base comments are 0, 6 and 12; the head moves 1 here,
        // deletes 6 and adds 99. Ranks: 1 → 1, 0 → 0, 12 → 0, 99 → 0.
        assert_eq!(ids("comments"), [id(6, 1), id(6, 99), id(6, 12), id(6, 0)]);
        assert_eq!(ids("mentions"), [id(1, 2), id(1, 2), id(1, 1)]);
    }

    /// Live results through a head over its base, as a reader the
    /// comments' policy applies to, on identical stores, one per path,
    /// through edits in the head and in the base.
    #[test]
    fn live_results_agree_through_branch_views() {
        for (what, query) in queries() {
            let stores = MODES.map(|mode| (mode, store()));
            let opts =
                ReadOpts::default().branch_view(head(), Some(BranchViewBase::Current(base())));
            let mut opened: Vec<_> = stores
                .iter()
                .map(|(mode, db)| {
                    set_mode(*mode);
                    let prepared = db.prepare_query(&query).unwrap();
                    let mut subscription =
                        block_on(db.subscribe_for_identity(&prepared, opts.clone(), reader()))
                            .unwrap();
                    block_on(db.tick()).unwrap();
                    (drain(&mut subscription), subscription)
                })
                .collect();
            for (mode, other) in MODES.iter().zip(&opened).skip(1) {
                assert_eq!(opened[0].0, other.0, "{what}: opening, {mode:?}");
            }
            if what == "posts, unpaged comments" {
                // The comments' policy hides some from the reader: an
                // unrestricted read of the same view shows more.
                let (_, db) = &stores[0];
                let prepared = db.prepare_query(&query).unwrap();
                let mut all = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
                block_on(db.tick()).unwrap();
                assert_ne!(
                    drain(&mut all).0,
                    opened[0].0.0,
                    "{what}: the policy hides some"
                );
            }
            for step in 0..3 {
                let mut deltas = Vec::new();
                for ((mode, db), (_, subscription)) in stores.iter().zip(&mut opened) {
                    set_mode(*mode);
                    match step {
                        0 => edit_head(db),
                        1 => {
                            block_on(db.update(
                                "comments",
                                id(6, 12),
                                cells(vec![("rank", Value::I32(3))]),
                                UpdateOptions {
                                    target: WriteTarget::BranchView {
                                        head: base(),
                                        base: None,
                                    },
                                    ..Default::default()
                                },
                            ))
                            .unwrap();
                            block_on(db.tick()).unwrap();
                        }
                        _ => {
                            block_on(db.update(
                                "people",
                                id(1, 1),
                                cells(vec![("rank", Value::I32(5))]),
                                UpdateOptions::default(),
                            ))
                            .unwrap();
                            block_on(db.tick()).unwrap();
                        }
                    }
                    deltas.push(drain(subscription));
                }
                for (mode, delta) in MODES.iter().zip(&deltas).skip(1) {
                    assert_eq!(&deltas[0], delta, "{what}: step {step}, {mode:?}");
                }
            }
        }
        set_mode((true, true));
    }

    type Shown = Vec<(RowUuid, String)>;

    fn drain(subscription: &mut jazz::db::SubscriptionStream) -> (Shown, Shown, Vec<RowUuid>) {
        let shown = |rows: Vec<jazz::db::SubscriptionOutputRow>| -> Shown {
            let mut rows: Shown = rows
                .into_iter()
                .map(|row| {
                    let cells: Vec<_> = row.row.cells().collect();
                    (row.row.row_uuid(), format!("{cells:?}"))
                })
                .collect();
            rows.sort();
            rows
        };
        let (mut added, mut updated, mut removed) = (Vec::new(), Vec::new(), Vec::new());
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta {
                added: a,
                updated: u,
                removed: r,
                ..
            } = event
            {
                added.extend(a);
                updated.extend(u);
                removed.extend(r.into_iter().map(|row| row.row_uuid));
            }
        }
        removed.sort();
        (shown(added), shown(updated), removed)
    }

    /// A page of a reverse relation read through a branch view (or a
    /// snapshot) does about the work of the per-parent statements: the
    /// batch reads it per parent there, since a batched statement would
    /// read the view's `UNION ALL` whole for every parent.
    #[test]
    fn a_page_through_a_view_does_about_the_per_parent_work() {
        const POSTS: usize = 5;
        const PER_POST: usize = 600;
        let schema = compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("posts")
                        .column("branch_id", ColumnType::Uuid)
                        .column("rank", ColumnType::Integer)
                        .branch_by("branch_id")
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("comments")
                        .column("branch_id", ColumnType::Uuid)
                        .fk_column("post", "posts")
                        .column("rank", ColumnType::Integer)
                        .composite_index(["post", "rank"])
                        .branch_by("branch_id")
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .build(),
        );
        let db = block_on(Db::open(DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x56; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let insert = |table: &str, row: RowUuid, values, target| {
            block_on(db.insert(
                table,
                values,
                InsertOptions {
                    row_id: Some(row),
                    target,
                    ..Default::default()
                },
            ))
            .unwrap();
        };
        for p in 0..POSTS {
            insert(
                "posts",
                id(5, p),
                cells(vec![("rank", Value::I32(p as i32))]),
                ExactWriteTarget::Branch(base()),
            );
            for k in 0..PER_POST {
                // A third of the comments only the head has.
                let branch = if k % 3 == 0 { head() } else { base() };
                insert(
                    "comments",
                    id(6, p * PER_POST + k),
                    cells(vec![
                        ("post", post(p)),
                        ("rank", Value::I32((k / 2) as i32)),
                    ]),
                    ExactWriteTarget::Branch(branch),
                );
            }
        }
        block_on(db.tick()).unwrap();
        let frozen = block_on(db.capture_snapshot()).unwrap();
        let views = [
            (
                "head over live base",
                ReadOpts::default().branch_view(head(), Some(BranchViewBase::Current(base()))),
            ),
            (
                "head over frozen base",
                ReadOpts::default()
                    .branch_view(head(), Some(BranchViewBase::snapshot(base(), frozen))),
            ),
            ("base", ReadOpts::default().branch_view(base(), None)),
        ];
        for (view, opts) in &views {
            for (offset, limit) in [(0, Some(1)), (PER_POST - 3, None)] {
                let mut page = ArraySubquery::new("comments", "comments", "post", "id")
                    .order_by("rank", OrderDirection::Desc)
                    .offset(offset);
                if let Some(limit) = limit {
                    page = page.limit(limit);
                }
                let prepared = db
                    .prepare_query(&Query::from("posts").array_subquery(page))
                    .unwrap();
                let read = |mode| {
                    set_mode(mode);
                    jazz::db::take_vm_steps_for_test();
                    let tree = block_on(db.all_result_tree(&prepared, opts.clone())).unwrap();
                    (tree, jazz::db::take_vm_steps_for_test())
                };
                let (per_parent, per_parent_steps) = read((true, false));
                let (batched, batched_steps) = read((true, true));
                set_mode((true, true));
                assert_eq!(batched, per_parent, "{view}, offset {offset}");
                assert!(
                    batched_steps <= 2 * per_parent_steps + 1_000,
                    "{view}, offset {offset}, limit {limit:?}: batched read took \
                     {batched_steps} SQLite steps, per parent {per_parent_steps}"
                );
            }
        }
    }
}

/// A paged reverse relation, batched, reads each parent's page as that
/// parent's own statement does: from the relation's index, stopping at the
/// page's end. A room list with each room's newest message must not read
/// every message of every room (alpha.62 numbered them all in a window and
/// kept one per room: the BandChat inbox went from ~15 ms to ~1.2 s).
#[test]
fn a_paged_relation_reads_each_parents_page_not_all_its_children() {
    const BUSY_ROOMS: usize = 4;
    const PER_ROOM: usize = 1_500;
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
                    .column("body", ColumnType::Text)
                    .column("sent_at", ColumnType::BigInt)
                    .composite_index(["room", "sent_at"])
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0x12; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    // Busy rooms, a room with two messages and an empty one.
    let rooms = BUSY_ROOMS + 2;
    block_on(db.transaction(async |tx| {
        for r in 0..rooms {
            tx.insert(
                "rooms",
                cells(vec![("name", Value::String(format!("room {r}")))]),
                InsertOptions {
                    row_id: Some(id(2, r)),
                    ..Default::default()
                },
            )
            .await?;
            let count = match r {
                r if r < BUSY_ROOMS => PER_ROOM,
                r if r == BUSY_ROOMS => 2,
                _ => 0,
            };
            for k in 0..count {
                // Equal send times in pairs: ties are broken by id.
                tx.insert(
                    "messages",
                    cells(vec![
                        ("room", uuid(id(2, r))),
                        ("body", Value::String(format!("room {r} message {k}"))),
                        ("sent_at", Value::I64((k / 2) as i64)),
                    ]),
                    InsertOptions {
                        row_id: Some(id(4, r * PER_ROOM + k)),
                        ..Default::default()
                    },
                )
                .await?;
            }
        }
        Ok(())
    }))
    .unwrap();
    block_on(db.tick()).unwrap();

    let pages = [
        (0, Some(1)),
        (1, Some(2)),
        (3, Some(3)),
        (PER_ROOM - 2, None),
    ];
    for (offset, limit) in pages {
        let mut newest = ArraySubquery::new("messagesViaRoom", "messages", "room", "id")
            .order_by("sent_at", OrderDirection::Desc)
            .offset(offset);
        if let Some(limit) = limit {
            newest = newest.limit(limit);
        }
        let query = Query::from("rooms").array_subquery(newest);
        let prepared = db.prepare_query(&query).unwrap();
        let read = |mode| {
            set_mode(mode);
            let roots = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
            jazz::db::take_vm_steps_for_test();
            let rows = db.nest_relations(&prepared, roots).unwrap();
            (rows, jazz::db::take_vm_steps_for_test())
        };
        let (per_parent, per_parent_steps) = read((true, false));
        let (batched, batched_steps) = read((true, true));
        set_mode((true, true));
        assert_eq!(batched, per_parent, "offset {offset}, limit {limit:?}");
        assert_eq!(batched.len(), rooms);
        // The per-parent statements seek each room's index range; the
        // batched one must do about as little, nowhere near the 6,000
        // messages the busy rooms hold.
        assert!(
            batched_steps <= 2 * per_parent_steps + 1_000,
            "offset {offset}, limit {limit:?}: batched read took {batched_steps} SQLite steps, \
             per parent {per_parent_steps}"
        );
    }
}
