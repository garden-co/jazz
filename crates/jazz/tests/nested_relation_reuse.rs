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

mod randomized {
    //! Small random stores: several readers with different memberships,
    //! foreign keys that are NULL or dangling, a relation keyed by a text
    //! column, arrays of references that are empty or repeat, and ties in
    //! the orders. Every read path agrees with and without reuse.

    use std::collections::BTreeMap;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use crate::common::{compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq};
    use jazz::block_on;
    use jazz::db::{
        Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SubscriptionEvent, SubscriptionStream,
        UpdateOptions, set_relation_reuse_for_test,
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
        let (offset, limit) = (rng.gen_range(0..2), rng.gen_range(1..4));
        let comments = ArraySubquery::new("comments", "comments", "post", "id")
            .order_by("rank", OrderDirection::Desc)
            .offset(offset)
            .limit(limit)
            .select(["author"])
            .nested(person("author"))
            .nested(ArraySubquery::new("post", "posts", "id", "post").nested(person("author")));
        vec![
            Query::from("posts")
                .order_by("rank", OrderDirection::Asc)
                .array_subquery(person("author"))
                .array_subquery(ArraySubquery::new("team", "teams", "code", "team"))
                .array_subquery(ArraySubquery::new("mentions", "people", "id", "mentions"))
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

    /// `read` without reuse, then with.
    fn agree<T: PartialEq + std::fmt::Debug>(what: &str, read: impl Fn() -> T) {
        set_relation_reuse_for_test(false);
        let plain = read();
        set_relation_reuse_for_test(true);
        let reused = read();
        assert_eq!(reused, plain, "{what}");
    }

    /// Each reader's live results on two identical stores, one without
    /// reuse and one with, through random writes.
    #[test]
    fn live_results_agree_per_reader_through_random_writes() {
        for seed in 0..SEEDS {
            let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed);
            let queries = queries(&mut rng);
            let stores = [(false, store(seed)), (true, store(seed))];
            for (q, query) in queries.iter().enumerate() {
                for n in 0..READERS {
                    let what = format!("seed {seed}, query {q}, reader {n}");
                    let mut opened: Vec<_> = stores
                        .iter()
                        .map(|(reuse, db)| {
                            set_relation_reuse_for_test(*reuse);
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
                    assert_eq!(opened[0].0, opened[1].0, "{what}: opening");
                    let writes = StdRng::seed_from_u64(seed * 31 + n as u64);
                    let mut rngs = [writes.clone(), writes];
                    for w in 0..WRITES {
                        let mut deltas = Vec::new();
                        for (((reuse, db), (_, subscription)), rng) in
                            stores.iter().zip(&mut opened).zip(&mut rngs)
                        {
                            set_relation_reuse_for_test(*reuse);
                            write(db, rng);
                            deltas.push(drain(subscription));
                        }
                        assert_eq!(deltas[0], deltas[1], "{what}: write {w}");
                    }
                }
            }
        }
        set_relation_reuse_for_test(true);
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
        set_relation_reuse_for_test(true);
    }
}
