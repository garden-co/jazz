//! Positions for unbounded ordered plain outputs are derived from the rows a
//! write changed, not from complete before/after windows (#3505). Replaying
//! every tick's terminal edits must reproduce the query order exactly, and a
//! write moves at most the rows it changed: unchanged rows keep their relative
//! order, so no edit ever addresses them.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use groove::db::{Database, GraphBuilder, PredicateExpr, PrimaryKeyValue};
use groove::ivm::runtime::{TerminalEdit, TerminalOperation};
use groove::ivm::{TopByLimit, TopByOrder};
use groove::records::{BorrowedRecord, Value};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Variant {
    /// One group: positions over the whole result.
    Plain,
    /// Grouped by `user`: positions within each user's rows.
    Grouped,
    /// A projection that keeps the identity after the TopBy.
    ProjectAfter,
    /// A filter after the TopBy drops some rows, so positions count only the
    /// rows that reach the output (complete-window path).
    FilterAfter,
}

fn builder(variant: Variant) -> GraphBuilder {
    let groups: Vec<&str> = if variant == Variant::Grouped {
        vec!["user"]
    } else {
        vec![]
    };
    let top = GraphBuilder::top_by(
        GraphBuilder::table("posts"),
        groups,
        [TopByOrder::asc("rank")],
        ["id"],
        0,
        TopByLimit::Unbounded,
    );
    match variant {
        Variant::ProjectAfter => top.project(["id", "rank", "user"]),
        Variant::FilterAfter => top.filter(PredicateExpr::gt("id", Value::U64(9))),
        Variant::Plain | Variant::Grouped => top,
    }
}

async fn database() -> Database {
    let schema = DatabaseSchema::new([TableSchema::new(
        "posts",
        [
            ColumnSchema::new("user", ColumnType::U64),
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("rank", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    Database::new(schema, MemoryStorage::new(&["posts"]).unwrap())
        .await
        .unwrap()
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// id -> (user, rank)
type Oracle = BTreeMap<u64, (u64, u64)>;

fn expected(variant: Variant, oracle: &Oracle) -> BTreeMap<u64, Vec<u64>> {
    let mut groups = BTreeMap::<u64, Vec<(u64, u64)>>::new();
    for (id, (user, rank)) in oracle {
        if variant == Variant::FilterAfter && *id <= 9 {
            continue;
        }
        let group = if variant == Variant::Grouped {
            *user
        } else {
            0
        };
        groups.entry(group).or_default().push((*rank, *id));
    }
    groups
        .into_iter()
        .map(|(group, mut rows)| {
            rows.sort();
            (group, rows.into_iter().map(|(_, id)| id).collect())
        })
        .collect()
}

/// The consumer: each group's roots in order, as (root key, id).
#[derive(Default)]
struct Replica {
    groups: BTreeMap<u64, Vec<(Vec<u8>, u64)>>,
    group_of: HashMap<Vec<u8>, u64>,
}

impl Replica {
    fn apply(&mut self, variant: Variant, operations: &[TerminalOperation]) {
        for operation in operations {
            match &operation.edit {
                TerminalEdit::Insert { key, index, value } => {
                    let record = BorrowedRecord::new(value, &operation.root_descriptor);
                    let field = |name: &str| {
                        let index = operation
                            .root_descriptor
                            .fields()
                            .iter()
                            .position(|field| field.name.as_deref() == Some(name))
                            .unwrap();
                        match record.get_idx(index).unwrap() {
                            Value::U64(value) => value,
                            other => panic!("unexpected {other:?}"),
                        }
                    };
                    let group = if variant == Variant::Grouped {
                        field("user")
                    } else {
                        0
                    };
                    self.group_of.insert(key.clone(), group);
                    self.groups
                        .entry(group)
                        .or_default()
                        .insert(*index, (key.clone(), field("id")));
                }
                TerminalEdit::Remove { key } => {
                    let group = self.group_of.remove(key).unwrap();
                    let rows = self.groups.get_mut(&group).unwrap();
                    rows.remove(rows.iter().position(|(k, _)| k == key).unwrap());
                }
                TerminalEdit::Move { key, index } => {
                    let rows = self.groups.get_mut(&self.group_of[key]).unwrap();
                    let row = rows.remove(rows.iter().position(|(k, _)| k == key).unwrap());
                    rows.insert(*index, row);
                }
                TerminalEdit::Update { .. } => {}
            }
        }
    }

    fn visible(&self) -> BTreeMap<u64, Vec<u64>> {
        self.groups
            .iter()
            .filter(|(_, rows)| !rows.is_empty())
            .map(|(group, rows)| (*group, rows.iter().map(|(_, id)| *id).collect()))
            .collect()
    }
}

async fn run(variant: Variant, seed: u64) {
    let mut db = database().await;
    let subscription = db.subscribe([("posts", builder(variant))]).unwrap();
    let _ = subscription.try_recv();
    let mut rng = Rng(seed);
    let mut oracle = Oracle::new();
    let mut replica = Replica::default();
    let mut next_id = 0u64;
    for step in 0..150 {
        let mut batch = db.open_batch();
        let mut touched = BTreeSet::new();
        for _ in 0..1 + rng.below(4) {
            let existing = oracle.keys().copied().collect::<Vec<_>>();
            let choice = rng.below(10);
            if existing.is_empty() || choice < 4 {
                let (id, user, rank) = (next_id, rng.below(3), rng.below(40));
                next_id += 1;
                batch.insert(
                    "posts",
                    vec![Value::U64(user), Value::U64(id), Value::U64(rank)],
                );
                oracle.insert(id, (user, rank));
                touched.insert(id);
                continue;
            }
            let id = existing[rng.below(existing.len() as u64) as usize];
            if !touched.insert(id) {
                continue;
            }
            if choice < 8 {
                let user = oracle[&id].0;
                let rank = rng.below(40);
                batch.update(
                    "posts",
                    vec![Value::U64(user), Value::U64(id), Value::U64(rank)],
                );
                oracle.insert(id, (user, rank));
            } else {
                batch.delete("posts", PrimaryKeyValue::U64(id));
                oracle.remove(&id);
            }
        }
        let persistence = db.apply_batch(batch).await.unwrap().persist().await;
        db.finish_persistence(persistence).unwrap();
        if let Ok(tick) = subscription.try_recv()
            && let Some(terminal) = tick.terminal_sinks.get("posts")
        {
            let moves = terminal
                .operations
                .iter()
                .filter(|operation| matches!(operation.edit, TerminalEdit::Move { .. }))
                .count();
            assert!(
                moves <= touched.len(),
                "{variant:?} seed {seed} step {step}: {moves} moves for {} changed rows",
                touched.len()
            );
            replica.apply(variant, &terminal.operations);
        }
        assert_eq!(
            replica.visible(),
            expected(variant, &oracle),
            "{variant:?} seed {seed} step {step}"
        );
    }
}

#[futures_test::test]
async fn replayed_unbounded_positions_match_the_query_order_and_move_only_changed_rows() {
    for variant in [
        Variant::Plain,
        Variant::Grouped,
        Variant::ProjectAfter,
        Variant::FilterAfter,
    ] {
        for seed in 1..=12u64 {
            run(variant, seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1).await;
        }
    }
}
