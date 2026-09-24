//! Terminal edits for a windowed `TopBy` must identify rows, not their first
//! output field. Here field 0 is the partition column `user`, shared by every
//! row of one feed (#3290). Root keys are the TopBy row identity (group plus
//! tie fields); root indices are positions within the row's own group.

use std::collections::BTreeMap;

use groove::db::{Database, GraphBuilder};
use groove::ivm::runtime::{RoutedMultisinkTerminal, TerminalEdit, TerminalOperation};
use groove::ivm::{TopByLimit, TopByOrder};
use groove::records::{BorrowedRecord, RecordDescriptor, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

const WINDOW: usize = 2;
const USERS: u64 = 3;

/// Ordered visible roots per group (`user`), each `(key, [user, id, created])`.
type Groups = BTreeMap<u64, Vec<(Vec<u8>, Vec<Value>)>>;

fn user_of(values: &[Value]) -> u64 {
    let Value::U64(user) = values[0] else {
        panic!("unexpected user {values:?}");
    };
    user
}

fn locate(groups: &Groups, key: &[u8]) -> (u64, usize) {
    groups
        .iter()
        .find_map(|(user, rows)| {
            rows.iter()
                .position(|(candidate, _)| candidate == key)
                .map(|index| (*user, index))
        })
        .unwrap_or_else(|| panic!("terminal key {key:?} is not visible"))
}

/// Apply edits sequentially, as a consumer would. An insert carries the root's
/// final index, clamped to the current length; trailing moves settle order.
fn apply(groups: &mut Groups, operations: &[TerminalOperation]) {
    for operation in operations {
        assert!(operation.path.is_empty());
        let decode = |bytes: &[u8]| {
            let record = BorrowedRecord::new(bytes, &operation.root_descriptor);
            (0..3)
                .map(|field| record.get_idx(field).unwrap())
                .collect::<Vec<_>>()
        };
        match &operation.edit {
            TerminalEdit::Insert { key, index, value } => {
                assert!(
                    !groups.values().flatten().any(|(k, _)| k == key),
                    "insert of an already visible terminal key {key:?}: {operations:?}"
                );
                let values = decode(value);
                let rows = groups.entry(user_of(&values)).or_default();
                rows.insert((*index).min(rows.len()), (key.clone(), values));
            }
            TerminalEdit::Remove { key } => {
                let (user, index) = locate(groups, key);
                groups.get_mut(&user).unwrap().remove(index);
            }
            TerminalEdit::Move { key, index } => {
                let (user, from) = locate(groups, key);
                let rows = groups.get_mut(&user).unwrap();
                let row = rows.remove(from);
                rows.insert((*index).min(rows.len()), row);
            }
            TerminalEdit::Update { key, value } => {
                let (user, index) = locate(groups, key);
                let values = decode(value);
                assert_eq!(user_of(&values), user, "an update cannot change the group");
                groups.get_mut(&user).unwrap()[index].1 = values;
            }
        }
    }
    groups.retain(|_, rows| !rows.is_empty());
}

/// Per user, the `WINDOW` newest posts (ties by id), in window order.
fn expected(oracle: &BTreeMap<u64, (u64, u64)>) -> BTreeMap<u64, Vec<Vec<Value>>> {
    let mut by_user = BTreeMap::<u64, Vec<(u64, u64)>>::new();
    for (id, (user, created)) in oracle {
        by_user.entry(*user).or_default().push((*created, *id));
    }
    by_user
        .into_iter()
        .map(|(user, mut posts)| {
            posts.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
            let window = posts
                .into_iter()
                .take(WINDOW)
                .map(|(created, id)| vec![Value::U64(user), Value::U64(id), Value::U64(created)])
                .collect();
            (user, window)
        })
        .collect()
}

fn visible(groups: &Groups) -> BTreeMap<u64, Vec<Vec<Value>>> {
    groups
        .iter()
        .map(|(user, rows)| {
            (
                *user,
                rows.iter().map(|(_, values)| values.clone()).collect(),
            )
        })
        .collect()
}

async fn database() -> Database {
    let schema = DatabaseSchema::new([TableSchema::new(
        "posts",
        [
            ColumnSchema::new("user", ColumnType::U64),
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("created", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    Database::new(schema, MemoryStorage::new(&["posts"]).unwrap())
        .await
        .unwrap()
}

fn feed() -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::table("posts"),
        ["user"],
        [TopByOrder::desc("created")],
        ["id"],
        0,
        TopByLimit::Finite(WINDOW as u64),
    )
}

async fn commit(db: &mut Database, rows: impl IntoIterator<Item = (u64, u64, u64)>, update: bool) {
    let mut batch = db.open_batch();
    for (id, user, created) in rows {
        let values = vec![Value::U64(user), Value::U64(id), Value::U64(created)];
        if update {
            batch.update("posts", values);
        } else {
            batch.insert("posts", values);
        }
    }
    let persistence = db.apply_batch(batch).await.unwrap().persist().await;
    db.finish_persistence(persistence).unwrap();
}

/// Three users, three posts each: every window holds two rows sharing field 0.
const SEED: [(u64, u64, u64); 9] = [
    (1, 0, 10),
    (2, 0, 20),
    (3, 0, 30),
    (4, 1, 15),
    (5, 1, 25),
    (6, 1, 5),
    (7, 2, 40),
    (8, 2, 12),
    (9, 2, 22),
];

/// A deterministic post to bump: it becomes its user's newest.
fn bump(random: &mut u64) -> u64 {
    *random ^= *random << 13;
    *random ^= *random >> 7;
    *random ^= *random << 17;
    1 + *random % SEED.len() as u64
}

#[futures_test::test]
async fn partitioned_top_by_terminal_edits_track_rows_sharing_field_zero() {
    let mut db = database().await;
    let subscription = db.subscribe([("feed", feed())]).unwrap();
    subscription.try_recv().unwrap();
    let mut groups = Groups::new();
    let mut oracle = BTreeMap::new();
    commit(&mut db, SEED, false).await;
    for (id, user, created) in SEED {
        oracle.insert(id, (user, created));
    }
    apply(
        &mut groups,
        &subscription.try_recv().unwrap().terminal_sinks["feed"].operations,
    );
    assert_eq!(visible(&groups), expected(&oracle));

    let mut random = 0x3290_u64;
    for step in 0..60 {
        let id = bump(&mut random);
        let (user, _) = oracle[&id];
        commit(&mut db, [(id, user, 100 + step)], true).await;
        oracle.insert(id, (user, 100 + step));
        if let Ok(tick) = subscription.try_recv() {
            apply(&mut groups, &tick.terminal_sinks["feed"].operations);
        }
        assert_eq!(visible(&groups), expected(&oracle), "step {step}");
    }
}

/// The production shape: one prepared TopBy, a binding per user, each
/// subscription seeing exactly its own ordered window.
#[futures_test::test]
async fn routed_top_by_bindings_see_their_exact_ordered_window() {
    let mut db = database().await;
    let shape = db
        .prepare(
            [RoutedMultisinkTerminal::new(
                "feed",
                feed(),
                ["user"],
                ["user", "id", "created"],
            )],
            "user",
            RecordDescriptor::new([("user", ColumnType::U64)]),
        )
        .await
        .unwrap();
    let mut bindings = Vec::new();
    for user in 0..USERS {
        let subscription = db
            .bind_shape(shape.id(), &[Value::U64(user)])
            .await
            .unwrap();
        bindings.push((user, subscription, Groups::new()));
    }
    db.drive_progress().await.unwrap();
    for (_, subscription, _) in &bindings {
        while subscription.try_recv().is_ok() {}
    }
    // Every row arrives through terminal edits, so the model holds real keys.
    commit(&mut db, SEED, false).await;
    let mut oracle = BTreeMap::new();
    for (id, user, created) in SEED {
        oracle.insert(id, (user, created));
    }
    db.drive_progress().await.unwrap();
    let opening = expected(&oracle);
    for (user, subscription, groups) in &mut bindings {
        while let Ok(tick) = subscription.try_recv() {
            if let Some(terminal) = tick.terminal_sinks.get("feed") {
                apply(groups, &terminal.operations);
            }
        }
        assert_eq!(
            visible(groups).get(user),
            opening.get(user),
            "user {user} opening"
        );
    }

    let mut random = 0x3291_u64;
    for step in 0..60 {
        let id = bump(&mut random);
        let (user, _) = oracle[&id];
        commit(&mut db, [(id, user, 100 + step)], true).await;
        oracle.insert(id, (user, 100 + step));
        db.drive_progress().await.unwrap();
        let expected = expected(&oracle);
        for (user, subscription, groups) in &mut bindings {
            while let Ok(tick) = subscription.try_recv() {
                if let Some(terminal) = tick.terminal_sinks.get("feed") {
                    apply(groups, &terminal.operations);
                }
            }
            assert_eq!(
                visible(groups).get(user),
                expected.get(user),
                "user {user} at step {step}"
            );
            assert!(
                groups.keys().all(|group| group == user),
                "user {user}: {groups:?}"
            );
        }
    }
}

/// Randomized oracle over window 3 (#3309 review): inserts, deletes,
/// demotions, ties on `created` and rows moving between users, unbound and as
/// routed bindings. It covers a TopBy without tie fields, where rows sharing
/// an order value must still be distinct roots, and a Filter or projection
/// after the TopBy, whose indices must be positions in the filtered window.
mod randomized {
    use std::collections::{BTreeMap, BTreeSet};

    use groove::db::{Database, GraphBuilder, PrimaryKeyValue};
    use groove::ivm::runtime::{RoutedMultisinkTerminal, TerminalEdit, TerminalOperation};
    use groove::ivm::{PredicateExpr, TopByLimit, TopByOrder};
    use groove::records::{BorrowedRecord, RecordDescriptor, Value};
    use groove::schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
    };
    use groove::storage::MemoryStorage;

    const WINDOW: usize = 3;
    const USERS: u64 = 3;

    /// (user, id, created) per visible row, with its key, per group.
    type Row = (u64, u64, u64);
    type Groups = BTreeMap<u64, Vec<(Vec<u8>, Row)>>;

    fn u(v: Value) -> u64 {
        match v {
            Value::U64(x) => x,
            other => panic!("{other:?}"),
        }
    }

    fn locate(groups: &Groups, key: &[u8], ops: &[TerminalOperation]) -> (u64, usize) {
        groups
            .iter()
            .find_map(|(user, rows)| rows.iter().position(|(k, _)| k == key).map(|i| (*user, i)))
            .unwrap_or_else(|| panic!("key {key:?} not visible; ops {ops:#?}; groups {groups:?}"))
    }

    fn apply(groups: &mut Groups, ops: &[TerminalOperation]) {
        for op in ops {
            assert!(op.path.is_empty());
            let d = &op.root_descriptor;
            let (ui, ii, ci) = (
                d.field_index("user").unwrap(),
                d.field_index("id").unwrap(),
                d.field_index("created").unwrap(),
            );
            let decode = |bytes: &[u8]| {
                let r = BorrowedRecord::new(bytes, d);
                (
                    u(r.get_idx(ui).unwrap()),
                    u(r.get_idx(ii).unwrap()),
                    u(r.get_idx(ci).unwrap()),
                )
            };
            match &op.edit {
                TerminalEdit::Insert { key, index, value } => {
                    assert!(
                        !groups.values().flatten().any(|(k, _)| k == key),
                        "double insert {key:?}: {ops:#?}"
                    );
                    let row = decode(value);
                    let rows = groups.entry(row.0).or_default();
                    rows.insert((*index).min(rows.len()), (key.clone(), row));
                }
                TerminalEdit::Remove { key } => {
                    let (g, i) = locate(groups, key, ops);
                    groups.get_mut(&g).unwrap().remove(i);
                }
                TerminalEdit::Move { key, index } => {
                    let (g, i) = locate(groups, key, ops);
                    let rows = groups.get_mut(&g).unwrap();
                    let row = rows.remove(i);
                    rows.insert((*index).min(rows.len()), row);
                }
                TerminalEdit::Update { key, value } => {
                    let (g, i) = locate(groups, key, ops);
                    let row = decode(value);
                    assert_eq!(row.0, g, "update changes group: {ops:#?}");
                    groups.get_mut(&g).unwrap()[i].1 = row;
                }
            }
        }
        groups.retain(|_, rows| !rows.is_empty());
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Variant {
        Tie,
        NoTie,
        FilterAfter,
        ProjectAfter,
        /// String equality on `extra` after a TopBy with tie fields.
        StringFilterAfter,
        /// The same filter after a TopBy without tie fields, whose
        /// whole-record key includes the `tags` array column.
        NoTieStringFilterAfter,
        /// A projection dropping `extra` and `tags` after a TopBy without tie
        /// fields: roots are keyed by every output field.
        NoTieProjectAfter,
    }

    /// Every row carries a string and an array column derived from its id and
    /// order value, so an update can flip whether the string filter keeps it.
    fn extra(id: u64, created: u64) -> &'static str {
        if (id + created).is_multiple_of(3) {
            "drop"
        } else {
            "keep"
        }
    }

    fn no_tie(v: Variant) -> bool {
        matches!(
            v,
            Variant::NoTie | Variant::NoTieStringFilterAfter | Variant::NoTieProjectAfter
        )
    }

    fn builder(v: Variant) -> GraphBuilder {
        let ties: Vec<&str> = if no_tie(v) { vec![] } else { vec!["id"] };
        let top = GraphBuilder::top_by(
            GraphBuilder::table("posts"),
            ["user"],
            [TopByOrder::desc("created")],
            ties,
            0,
            TopByLimit::Finite(WINDOW as u64),
        );
        match v {
            Variant::FilterAfter => top.filter(PredicateExpr::gt("id", Value::U64(4))),
            Variant::ProjectAfter | Variant::NoTieProjectAfter => {
                top.project(["created", "id", "user"])
            }
            Variant::StringFilterAfter | Variant::NoTieStringFilterAfter => {
                top.filter(PredicateExpr::eq("extra", Value::String("keep".into())))
            }
            _ => top,
        }
    }

    type Oracle = BTreeMap<u64, (u64, u64)>; // id -> (user, created)

    fn expected(v: Variant, oracle: &Oracle) -> BTreeMap<u64, Vec<Row>> {
        let mut by_user = BTreeMap::<u64, Vec<(u64, u64)>>::new();
        for (id, (user, created)) in oracle {
            by_user.entry(*user).or_default().push((*created, *id));
        }
        by_user
            .into_iter()
            .map(|(user, mut posts)| {
                posts.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                let w: Vec<Row> = posts
                    .into_iter()
                    .take(WINDOW)
                    .map(|(c, id)| (user, id, c))
                    .filter(|r| v != Variant::FilterAfter || r.1 > 4)
                    .filter(|r| {
                        !matches!(
                            v,
                            Variant::StringFilterAfter | Variant::NoTieStringFilterAfter
                        ) || extra(r.1, r.2) == "keep"
                    })
                    .collect();
                (user, w)
            })
            .filter(|(_, w)| !w.is_empty())
            .collect()
    }

    fn visible(groups: &Groups) -> BTreeMap<u64, Vec<Row>> {
        groups
            .iter()
            .map(|(g, rows)| (*g, rows.iter().map(|(_, r)| *r).collect()))
            .collect()
    }

    /// For NoTie the tie order is unspecified: compare created sequence + id set.
    fn check(v: Variant, got: &BTreeMap<u64, Vec<Row>>, want: &BTreeMap<u64, Vec<Row>>, ctx: &str) {
        if no_tie(v) {
            let norm = |m: &BTreeMap<u64, Vec<Row>>| {
                m.iter()
                    .map(|(g, rows)| {
                        let created: Vec<u64> = rows.iter().map(|r| r.2).collect();
                        let mut ids: Vec<u64> = rows.iter().map(|r| r.1).collect();
                        ids.sort();
                        (*g, created, ids.len())
                    })
                    .collect::<Vec<_>>()
            };
            assert_eq!(norm(got), norm(want), "{ctx}: got {got:?} want {want:?}");
        } else {
            assert_eq!(got, want, "{ctx}");
        }
    }

    async fn database() -> Database {
        let schema = DatabaseSchema::new([TableSchema::new(
            "posts",
            [
                ColumnSchema::new("user", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("created", ColumnType::U64),
                ColumnSchema::new("extra", ColumnType::String),
                ColumnSchema::new("tags", ColumnType::Array(Box::new(ColumnType::U64))),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        Database::new(schema, MemoryStorage::new(&["posts"]).unwrap())
            .await
            .unwrap()
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    enum Op {
        Put(u64, u64, u64, bool),
        Del(u64),
    }

    fn random_ops(
        rng: &mut Rng,
        oracle: &mut Oracle,
        next_id: &mut u64,
        created_range: u64,
    ) -> Vec<Op> {
        let n = 1 + rng.next(3);
        let mut ops = Vec::new();
        let mut touched = BTreeSet::new();
        for _ in 0..n {
            let ids: Vec<u64> = oracle
                .keys()
                .copied()
                .filter(|i| !touched.contains(i))
                .collect();
            match rng.next(5) {
                0 | 1 if !ids.is_empty() => {
                    // update created
                    let id = ids[rng.next(ids.len() as u64) as usize];
                    let user = oracle[&id].0;
                    let c = rng.next(created_range);
                    touched.insert(id);
                    oracle.insert(id, (user, c));
                    ops.push(Op::Put(id, user, c, true));
                }
                2 if !ids.is_empty() => {
                    // change group (and maybe created)
                    let id = ids[rng.next(ids.len() as u64) as usize];
                    let user = rng.next(USERS);
                    let c = rng.next(created_range);
                    touched.insert(id);
                    oracle.insert(id, (user, c));
                    ops.push(Op::Put(id, user, c, true));
                }
                3 if ids.len() > 2 => {
                    let id = ids[rng.next(ids.len() as u64) as usize];
                    touched.insert(id);
                    oracle.remove(&id);
                    ops.push(Op::Del(id));
                }
                _ => {
                    let id = *next_id;
                    *next_id += 1;
                    let user = rng.next(USERS);
                    let c = rng.next(created_range);
                    touched.insert(id);
                    oracle.insert(id, (user, c));
                    ops.push(Op::Put(id, user, c, false));
                }
            }
        }
        ops
    }

    async fn commit(db: &mut Database, ops: Vec<Op>) {
        let mut batch = db.open_batch();
        for op in ops {
            match op {
                Op::Put(id, user, c, update) => {
                    let values = vec![
                        Value::U64(user),
                        Value::U64(id),
                        Value::U64(c),
                        Value::String(extra(id, c).into()),
                        Value::Array(vec![Value::U64(id % 2), Value::U64(c)]),
                    ];
                    if update {
                        batch.update("posts", values);
                    } else {
                        batch.insert("posts", values);
                    }
                }
                Op::Del(id) => batch.delete("posts", PrimaryKeyValue::U64(id)),
            }
        }
        let p = db.apply_batch(batch).await.unwrap().persist().await;
        db.finish_persistence(p).unwrap();
    }

    async fn run_unbound(v: Variant, seed: u64, steps: usize, created_range: u64) {
        let mut db = database().await;
        let sub = db.subscribe([("feed", builder(v))]).unwrap();
        let _ = sub.try_recv();
        let mut groups = Groups::new();
        let mut oracle = Oracle::new();
        let mut rng = Rng(seed);
        let mut next_id = 1;
        for step in 0..steps {
            let ops = random_ops(&mut rng, &mut oracle, &mut next_id, created_range);
            commit(&mut db, ops).await;
            while let Ok(tick) = sub.try_recv() {
                if let Some(t) = tick.terminal_sinks.get("feed") {
                    apply(&mut groups, &t.operations);
                }
            }
            check(
                v,
                &visible(&groups),
                &expected(v, &oracle),
                &format!("{v:?} seed {seed} step {step}"),
            );
        }
    }

    async fn run_routed(v: Variant, seed: u64, steps: usize, created_range: u64) {
        let mut db = database().await;
        let shape = db
            .prepare(
                [RoutedMultisinkTerminal::new(
                    "feed",
                    builder(v),
                    ["user"],
                    ["user", "id", "created"],
                )],
                "user",
                RecordDescriptor::new([("user", ColumnType::U64)]),
            )
            .await
            .unwrap();
        let mut bindings = Vec::new();
        for user in 0..USERS {
            let s = db
                .bind_shape(shape.id(), &[Value::U64(user)])
                .await
                .unwrap();
            bindings.push((user, s, Groups::new()));
        }
        db.drive_progress().await.unwrap();
        for (_, s, _) in &bindings {
            while s.try_recv().is_ok() {}
        }
        let mut oracle = Oracle::new();
        let mut rng = Rng(seed);
        let mut next_id = 1;
        for step in 0..steps {
            let ops = random_ops(&mut rng, &mut oracle, &mut next_id, created_range);
            commit(&mut db, ops).await;
            db.drive_progress().await.unwrap();
            let want = expected(v, &oracle);
            for (user, s, groups) in &mut bindings {
                while let Ok(tick) = s.try_recv() {
                    if let Some(t) = tick.terminal_sinks.get("feed") {
                        apply(groups, &t.operations);
                    }
                }
                let got: BTreeMap<_, _> = visible(groups).into_iter().collect();
                let mine: BTreeMap<_, _> = want
                    .get(user)
                    .map(|w| (*user, w.clone()))
                    .into_iter()
                    .collect();
                check(
                    v,
                    &got,
                    &mine,
                    &format!("routed {v:?} user {user} seed {seed} step {step}"),
                );
            }
        }
    }

    macro_rules! probe {
        ($name:ident, $runner:ident, $v:expr, $range:expr) => {
            #[futures_test::test]
            async fn $name() {
                for seed in 1..=20u64 {
                    $runner($v, 0x9e37_79b9_7f4a_7c15 ^ seed, 80, $range).await;
                }
            }
        };
    }

    probe!(unbound_tie_wide, run_unbound, Variant::Tie, 1000);
    probe!(unbound_tie_ties, run_unbound, Variant::Tie, 4);
    probe!(unbound_notie_ties, run_unbound, Variant::NoTie, 4);
    probe!(
        unbound_filter_after,
        run_unbound,
        Variant::FilterAfter,
        1000
    );
    probe!(
        unbound_filter_after_ties,
        run_unbound,
        Variant::FilterAfter,
        4
    );
    probe!(
        unbound_project_after,
        run_unbound,
        Variant::ProjectAfter,
        1000
    );
    probe!(
        unbound_project_after_ties,
        run_unbound,
        Variant::ProjectAfter,
        4
    );
    probe!(
        unbound_string_filter_after,
        run_unbound,
        Variant::StringFilterAfter,
        1000
    );
    probe!(
        unbound_notie_string_filter_after_ties,
        run_unbound,
        Variant::NoTieStringFilterAfter,
        4
    );
    probe!(
        unbound_notie_project_after_ties,
        run_unbound,
        Variant::NoTieProjectAfter,
        4
    );
    probe!(routed_tie_wide, run_routed, Variant::Tie, 1000);
    probe!(routed_tie_ties, run_routed, Variant::Tie, 4);
    probe!(routed_notie_ties, run_routed, Variant::NoTie, 4);
    probe!(routed_filter_after, run_routed, Variant::FilterAfter, 1000);
    probe!(
        routed_filter_after_ties,
        run_routed,
        Variant::FilterAfter,
        4
    );
    probe!(
        routed_string_filter_after,
        run_routed,
        Variant::StringFilterAfter,
        1000
    );
    probe!(
        routed_notie_string_filter_after_ties,
        run_routed,
        Variant::NoTieStringFilterAfter,
        4
    );
}

/// Root keys for column types the runtime primary-key encoder cannot key
/// (#3309 review). A TopBy without tie fields keys its roots by the whole
/// record, so an Array, an Enum or a spilled large string must neither fail
/// the write nor collapse rows. A filter or projection after the TopBy keeps
/// one distinct root per visible row.
mod whole_record_identity {
    use std::collections::BTreeSet;
    use std::rc::Rc;

    use groove::chunks::MemoryChunkStorage;
    use groove::db::MultisinkSubscription;
    use groove::db::{Database, GraphBuilder, PredicateExpr, PrimaryKeyValue};
    use groove::ivm::runtime::{TerminalEdit, TerminalOperation};
    use groove::ivm::{TopByLimit, TopByOrder};
    use groove::large_values::{INLINE_VALUE_MAX_BYTES, LargeValueKind, StagedLargeValueId};
    use groove::records::{
        BorrowedRecord, EnumCase, EnumSchema, EnumValue, RecordDescriptor, Value, ValueType,
    };
    use groove::schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
    };
    use groove::storage::MemoryStorage;

    /// One group's visible roots in order: `(key, record values)`.
    type Roots = Vec<(Vec<u8>, Vec<Value>)>;

    async fn database(extra: ColumnType) -> Database {
        let schema = DatabaseSchema::new([TableSchema::new(
            "posts",
            [
                ColumnSchema::new("user", ColumnType::U64),
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("created", ColumnType::U64),
                ColumnSchema::new("extra", extra),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
        let storage = MemoryStorage::new(&schema.column_families()).unwrap();
        let mut db = Database::new(schema, storage).await.unwrap();
        db.set_chunk_storage(Rc::new(MemoryChunkStorage::new()));
        db
    }

    fn top_by(ties: &[&str], limit: u64) -> GraphBuilder {
        GraphBuilder::top_by(
            GraphBuilder::table("posts"),
            ["user"],
            [TopByOrder::desc("created")],
            ties.to_vec(),
            0,
            TopByLimit::Finite(limit),
        )
    }

    enum Write {
        Insert(Vec<Value>),
        Update(Vec<Value>),
        Delete(u64),
    }

    async fn commit(db: &mut Database, writes: Vec<Write>, large: &[StagedLargeValueId]) {
        let mut batch = db.open_batch();
        for write in writes {
            match write {
                Write::Insert(values) => batch.insert("posts", values),
                Write::Update(values) => batch.update("posts", values),
                Write::Delete(id) => batch.delete("posts", PrimaryKeyValue::U64(id)),
            }
        }
        for id in large {
            batch.accept_large_value(*id);
        }
        let applied = db
            .apply_batch(batch)
            .await
            .expect("the write must not fail on the TopBy root key");
        let persistence = applied.persist().await;
        db.finish_persistence(persistence).unwrap();
    }

    fn row(id: u64, created: u64, extra: Value) -> Vec<Value> {
        vec![Value::U64(0), Value::U64(id), Value::U64(created), extra]
    }

    /// Apply edits in order, as a consumer would, refusing a second insert of
    /// a visible key and any edit of an unknown key.
    fn apply(roots: &mut Roots, operations: &[TerminalOperation]) {
        for operation in operations {
            assert!(operation.path.is_empty());
            let decode = |bytes: &[u8]| {
                BorrowedRecord::new(bytes, &operation.root_descriptor)
                    .to_values()
                    .unwrap()
            };
            let find = |roots: &Roots, key: &[u8]| {
                roots
                    .iter()
                    .position(|(candidate, _)| candidate == key)
                    .unwrap_or_else(|| panic!("key {key:?} is not visible: {operations:?}"))
            };
            match &operation.edit {
                TerminalEdit::Insert { key, index, value } => {
                    assert!(
                        roots.iter().all(|(candidate, _)| candidate != key),
                        "insert of an already visible key {key:?}: {operations:?}"
                    );
                    roots.insert((*index).min(roots.len()), (key.clone(), decode(value)));
                }
                TerminalEdit::Remove { key } => {
                    let index = find(roots, key);
                    roots.remove(index);
                }
                TerminalEdit::Move { key, index } => {
                    let from = find(roots, key);
                    let root = roots.remove(from);
                    roots.insert((*index).min(roots.len()), root);
                }
                TerminalEdit::Update { key, value } => {
                    let index = find(roots, key);
                    roots[index].1 = decode(value);
                }
            }
        }
    }

    fn drain(subscription: &MultisinkSubscription, roots: &mut Roots) {
        while let Ok(tick) = subscription.try_recv() {
            if let Some(terminal) = tick.terminal_sinks.get("feed") {
                apply(roots, &terminal.operations);
            }
        }
    }

    fn ids(roots: &Roots) -> Vec<u64> {
        roots
            .iter()
            .map(|(_, values)| match values[1] {
                Value::U64(id) => id,
                ref other => panic!("unexpected id {other:?}"),
            })
            .collect()
    }

    fn distinct_keys(roots: &Roots) -> usize {
        roots
            .iter()
            .map(|(key, _)| key)
            .collect::<BTreeSet<_>>()
            .len()
    }

    /// A window of three over four rows with distinct extras; two rows share
    /// `created`, so only the whole record tells them apart. Then a demotion,
    /// a promotion and a delete, checking the exact visible window each time.
    async fn no_tie_window_over(mut db: Database, extras: [Value; 4]) {
        let subscription = db.subscribe([("feed", top_by(&[], 3))]).unwrap();
        let mut roots = Roots::new();
        drain(&subscription, &mut roots);
        let [a, b, c, d] = extras;
        commit(
            &mut db,
            vec![
                Write::Insert(row(1, 50, a.clone())),
                Write::Insert(row(2, 50, b.clone())),
                Write::Insert(row(3, 40, c.clone())),
                Write::Insert(row(4, 10, d.clone())),
            ],
            &[],
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(distinct_keys(&roots), 3, "{roots:?}");
        let mut opening = ids(&roots);
        opening[..2].sort();
        assert_eq!(opening, vec![1, 2, 3]);
        assert_eq!(roots[2].1[3], c, "the extra column is published intact");

        // Demote row 1 below row 4: row 4 enters the window.
        commit(&mut db, vec![Write::Update(row(1, 5, a.clone()))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![2, 3, 4]);
        assert_eq!(distinct_keys(&roots), 3);

        // Promote row 1 to the top, then delete row 2.
        commit(&mut db, vec![Write::Update(row(1, 90, a.clone()))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![1, 2, 3]);
        commit(&mut db, vec![Write::Delete(2)], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![1, 3, 4]);
        assert_eq!(roots[0].1[3], a);
        assert_eq!(roots[2].1[3], d);
    }

    #[futures_test::test]
    async fn no_tie_top_by_keys_rows_with_an_array_column() {
        let extra = ColumnType::Array(Box::new(ColumnType::U64));
        let extras = [
            Value::Array(vec![Value::U64(1)]),
            Value::Array(vec![Value::U64(2), Value::U64(3)]),
            Value::Array(vec![]),
            Value::Array(vec![Value::U64(4)]),
        ];
        no_tie_window_over(database(extra).await, extras).await;
    }

    #[futures_test::test]
    async fn no_tie_top_by_keys_rows_with_an_enum_column() {
        let note = RecordDescriptor::new([("text", ValueType::String)]);
        let flag = RecordDescriptor::new([("on", ValueType::Bool)]);
        let schema = EnumSchema::new(
            "mark",
            [EnumCase::new("note", note), EnumCase::new("flag", flag)],
        )
        .unwrap();
        let extra = ColumnType::Enum(Box::new(schema));
        let note_of = |text: &str| {
            Value::Enum(EnumValue::create(0, note, &[Value::String(text.into())]).unwrap())
        };
        let flag_of =
            |on: bool| Value::Enum(EnumValue::create(1, flag, &[Value::Bool(on)]).unwrap());
        let extras = [note_of("a"), note_of("b"), flag_of(true), flag_of(false)];
        no_tie_window_over(database(extra).await, extras).await;
    }

    #[futures_test::test]
    async fn no_tie_top_by_keys_rows_with_a_spilled_string_column() {
        let db = database(ColumnType::String).await;
        let mut extras = Vec::new();
        let mut staged = Vec::new();
        for fill in [b'a', b'b', b'c', b'd'] {
            let text = String::from_utf8(vec![fill; INLINE_VALUE_MAX_BYTES + 1]).unwrap();
            let large = db
                .prepare_and_stage_large_value(LargeValueKind::String, text.as_bytes())
                .await
                .unwrap();
            staged.push(large.id);
            extras.push((text, Value::Large(Box::new(large.value_ref))));
        }
        // Rows are written with the indirect reference; the consumer sees the
        // materialized string.
        let written: [Value; 4] = std::array::from_fn(|i| extras[i].1.clone());
        let mut db = db;
        let subscription = db.subscribe([("feed", top_by(&[], 3))]).unwrap();
        let mut roots = Roots::new();
        drain(&subscription, &mut roots);
        commit(
            &mut db,
            (0..4)
                .map(|i| {
                    let created = [50, 50, 40, 10][i];
                    Write::Insert(row(i as u64 + 1, created, written[i].clone()))
                })
                .collect(),
            &staged,
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(distinct_keys(&roots), 3, "{} roots", roots.len());
        let mut opening = ids(&roots);
        opening[..2].sort();
        assert_eq!(opening, vec![1, 2, 3]);
        assert_eq!(roots[2].1[3], Value::String(extras[2].0.clone()));

        // Demote row 1 (same large value): row 4 enters.
        commit(
            &mut db,
            vec![Write::Update(row(1, 5, written[0].clone()))],
            &[],
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![2, 3, 4]);
        commit(&mut db, vec![Write::Delete(3)], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![2, 4, 1]);
        assert_eq!(distinct_keys(&roots), 3);
        assert_eq!(roots[2].1[3], Value::String(extras[0].0.clone()));
    }

    /// A string equality filter after a TopBy with tie fields: every visible
    /// row is its own root, and indices count only rows the filter keeps.
    #[futures_test::test]
    async fn string_filter_after_top_by_keeps_one_root_per_visible_row() {
        let mut db = database(ColumnType::String).await;
        let feed =
            top_by(&["id"], 4).filter(PredicateExpr::eq("extra", Value::String("keep".into())));
        let subscription = db.subscribe([("feed", feed)]).unwrap();
        let mut roots = Roots::new();
        drain(&subscription, &mut roots);
        let keep = || Value::String("keep".into());
        commit(
            &mut db,
            vec![
                Write::Insert(row(1, 10, keep())),
                Write::Insert(row(2, 20, Value::String("drop".into()))),
                Write::Insert(row(3, 30, keep())),
                Write::Insert(row(4, 40, keep())),
            ],
            &[],
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 3, 1]);
        assert_eq!(distinct_keys(&roots), 3, "three visible rows, three roots");

        // Promote row 1 above the dropped row 2 and row 3: it moves to index 1.
        commit(&mut db, vec![Write::Update(row(1, 35, keep()))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 1, 3]);
        // Row 2 starts passing the filter and enters between rows 1 and 3.
        commit(&mut db, vec![Write::Update(row(2, 32, keep()))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 1, 2, 3]);
        assert_eq!(distinct_keys(&roots), 4);
    }

    /// The same filter over a TopBy without tie fields, whose whole-record
    /// key includes the filtered string column.
    #[futures_test::test]
    async fn string_filter_after_no_tie_top_by_keeps_one_root_per_visible_row() {
        let mut db = database(ColumnType::String).await;
        let feed = top_by(&[], 4).filter(PredicateExpr::eq("extra", Value::String("keep".into())));
        let subscription = db.subscribe([("feed", feed)]).unwrap();
        let mut roots = Roots::new();
        drain(&subscription, &mut roots);
        let keep = || Value::String("keep".into());
        commit(
            &mut db,
            vec![
                Write::Insert(row(1, 10, keep())),
                Write::Insert(row(2, 20, Value::String("drop".into()))),
                Write::Insert(row(3, 30, keep())),
                Write::Insert(row(4, 40, keep())),
            ],
            &[],
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 3, 1]);
        assert_eq!(distinct_keys(&roots), 3);
        commit(&mut db, vec![Write::Update(row(1, 35, keep()))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 1, 3]);
        assert_eq!(distinct_keys(&roots), 3);
    }

    /// A compound filter reading a spilled string column after a TopBy
    /// without tie fields. The filter cannot be evaluated on a window row
    /// without loading the value, so those rows are placed by the output's own
    /// edits; the whole-record key of a spilled row is taken from the same
    /// unloaded form on both sides.
    #[futures_test::test]
    async fn filter_on_a_spilled_string_after_no_tie_top_by_keys_every_visible_row() {
        let mut db = database(ColumnType::String).await;
        let mut staged = Vec::new();
        let mut large = Vec::new();
        for fill in [b'x', b'y', b'z'] {
            let text = String::from_utf8(vec![fill; INLINE_VALUE_MAX_BYTES + 1]).unwrap();
            let value = db
                .prepare_and_stage_large_value(LargeValueKind::String, text.as_bytes())
                .await
                .unwrap();
            staged.push(value.id);
            large.push((text, Value::Large(Box::new(value.value_ref))));
        }
        let feed = top_by(&[], 5).filter(PredicateExpr::Or(vec![
            PredicateExpr::eq("extra", Value::String("keep".into())),
            PredicateExpr::gt("created", Value::U64(100)),
        ]));
        let subscription = db.subscribe([("feed", feed)]).unwrap();
        let mut roots = Roots::new();
        drain(&subscription, &mut roots);
        let text = |s: &str| Value::String(s.into());
        commit(
            &mut db,
            vec![
                Write::Insert(row(1, 10, text("keep"))),
                Write::Insert(row(2, 120, large[0].1.clone())),
                Write::Insert(row(3, 30, text("drop"))),
                Write::Insert(row(4, 140, large[1].1.clone())),
                // Spilled and not newer than 100: in the window, filtered out.
                Write::Insert(row(5, 50, large[2].1.clone())),
            ],
            &staged,
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 2, 1]);
        assert_eq!(distinct_keys(&roots), 3);
        assert_eq!(roots[0].1[3], Value::String(large[1].0.clone()));

        commit(&mut db, vec![Write::Update(row(1, 130, text("keep")))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 1, 2]);

        // The hidden spilled row passes the filter once it is newer than 100.
        commit(
            &mut db,
            vec![Write::Update(row(5, 125, large[2].1.clone()))],
            &[],
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 1, 5, 2]);
        assert_eq!(distinct_keys(&roots), 4);

        // A spilled row moves: it is edited by the key its first edit used.
        commit(
            &mut db,
            vec![Write::Update(row(2, 135, large[0].1.clone()))],
            &[],
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![4, 2, 1, 5]);
        commit(&mut db, vec![Write::Delete(4)], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![2, 1, 5]);
    }

    /// A projection that drops an order field of a TopBy without tie fields:
    /// the roots are keyed by every output field, not by field 0.
    #[futures_test::test]
    async fn projection_dropping_the_order_field_keys_roots_by_all_output_fields() {
        let mut db = database(ColumnType::String).await;
        let feed = top_by(&[], 3).project(["user", "id"]);
        let subscription = db.subscribe([("feed", feed)]).unwrap();
        let mut roots = Roots::new();
        drain(&subscription, &mut roots);
        let text = |s: &str| Value::String(s.into());
        commit(
            &mut db,
            vec![
                Write::Insert(row(1, 10, text("a"))),
                Write::Insert(row(2, 20, text("b"))),
                Write::Insert(row(3, 30, text("c"))),
                Write::Insert(row(4, 5, text("d"))),
            ],
            &[],
        )
        .await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![3, 2, 1]);
        assert_eq!(distinct_keys(&roots), 3, "{roots:?}");
        // Reorder within the window, then push row 3 out of it.
        commit(&mut db, vec![Write::Update(row(1, 40, text("a")))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![1, 3, 2]);
        commit(&mut db, vec![Write::Update(row(3, 1, text("c")))], &[]).await;
        drain(&subscription, &mut roots);
        assert_eq!(ids(&roots), vec![1, 2, 4]);
        assert_eq!(distinct_keys(&roots), 3);
    }
}
