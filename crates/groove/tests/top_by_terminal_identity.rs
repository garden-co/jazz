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
    }

    fn builder(v: Variant) -> GraphBuilder {
        let ties: Vec<&str> = if v == Variant::NoTie {
            vec![]
        } else {
            vec!["id"]
        };
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
            Variant::ProjectAfter => top.project(["created", "id", "user"]),
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
        if v == Variant::NoTie {
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
                    let values = vec![Value::U64(user), Value::U64(id), Value::U64(c)];
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
}
