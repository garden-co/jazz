use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, MergeableTxOps, ReadOpts, SeededRowIdSource, SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, BranchDimensionId, NodeUuid, RowUuid};
use jazz::protocol::{
    BranchSelector, BranchViewBase, ReadViewSourceSpec, ReadViewSpec, SnapshotRef,
};
use jazz::schema::{BranchDimensionSchema, JazzSchema, TableSchema};
use jazz::time::GlobalSeq;

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn open_db() -> (Db<MemoryStorage>, JazzSchema) {
    let dimension = BranchDimensionId(uuid::Uuid::from_bytes([0x61; 16]));
    let schema = JazzSchema::new_with_branch_dimensions(
        [BranchDimensionSchema::new(
            dimension,
            "branch",
            ColumnType::Uuid,
            Value::Uuid(uuid::Uuid::nil()),
        )],
        [TableSchema::new(
            "todos",
            [
                ColumnSchema::new("branch_id", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_branch_dimension("branch_id", dimension)],
    );
    let families = schema.column_families();
    let storage = MemoryStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>());
    let db = block_on(Db::open(
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x62; 16]),
                author: AuthorId::from_bytes([0x63; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(1)),
    ))
    .unwrap();
    (db, schema)
}

#[test]
fn one_mergeable_transaction_can_atomically_write_multiple_branches() {
    let (db, schema) = open_db();
    let row = RowUuid::from_bytes([0x70; 16]);
    let left = selector(0x72);
    let right = selector(0x73);

    let tx = db.mergeable_tx().unwrap();
    tx.insert_with_id_in_branch(
        "todos",
        left.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("left".to_owned()))]),
    )
    .unwrap();
    tx.insert_with_id_in_branch(
        "todos",
        right.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("right".to_owned()))]),
    )
    .unwrap();
    tx.commit().unwrap();

    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let rows_in = |branch| {
        block_on(db.all(&prepared, ReadOpts::default().branch_view(branch, None))).unwrap()
    };
    let left_rows = rows_in(left);
    let right_rows = rows_in(right);
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "todos")
        .unwrap();
    assert_eq!(left_rows.len(), 1);
    assert_eq!(right_rows.len(), 1);
    assert_eq!(
        left_rows[0].cell(table, "title"),
        Some(Value::String("left".to_owned()))
    );
    assert_eq!(
        right_rows[0].cell(table, "title"),
        Some(Value::String("right".to_owned()))
    );
}

#[test]
fn db_exact_mutations_and_branch_view_reads_compose_head_over_base() {
    let (db, schema) = open_db();
    let row = RowUuid::from_bytes([0x64; 16]);
    let base = selector(0x65);
    let head = selector(0x66);
    db.insert_with_id_in_branch(
        "todos",
        base.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
    )
    .unwrap();

    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts {
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base)),
            },
            ..ReadViewSpec::default()
        },
        ..ReadOpts::default()
    };
    let rows = block_on(db.all(&prepared, opts.clone())).unwrap();
    assert_eq!(rows.len(), 1);
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "todos")
        .unwrap();
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("base".to_owned()))
    );
    assert_eq!(
        rows[0].cell(table, "branch_id"),
        Some(Value::Uuid(uuid::Uuid::from_bytes([0x66; 16])))
    );
    let mut subscription = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
    let opening = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        opening,
        SubscriptionEvent::Delta { reset: true, .. }
    ));

    db.update_in_branch(
        "todos",
        selector(0x65),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("base edited".to_owned()))]),
    )
    .unwrap();
    let base_changed = block_on(subscription.next_event()).unwrap();
    assert!(
        matches!(
            base_changed,
            SubscriptionEvent::Delta { reset: false, ref updated, .. } if updated.len() == 1
        ),
        "unexpected live-base subscription delta: {base_changed:?}"
    );

    db.update_in_branch_view(
        "todos",
        head.clone(),
        Some(BranchViewBase::Current(selector(0x65))),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("draft".to_owned()))]),
    )
    .unwrap();
    let changed = block_on(subscription.next_event()).unwrap();
    assert!(
        matches!(
            changed,
            SubscriptionEvent::Delta { reset: false, ref updated, .. } if updated.len() == 1
        ),
        "unexpected branch subscription delta: {changed:?}"
    );
    db.update_in_branch(
        "todos",
        head.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("edited".to_owned()))]),
    )
    .unwrap();
    let rows = block_on(db.all(&prepared, opts.clone())).unwrap();
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("edited".to_owned()))
    );

    db.delete_in_branch("todos", head, row).unwrap();
    assert!(block_on(db.all(&prepared, opts)).unwrap().is_empty());
}

#[test]
fn inherited_delete_is_a_head_register_and_can_be_restored() {
    let (db, schema) = open_db();
    let row = RowUuid::from_bytes([0x67; 16]);
    let base = selector(0x68);
    let head = selector(0x69);
    db.insert_with_id_in_branch(
        "todos",
        base.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
    )
    .unwrap();
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts {
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base)),
            },
            ..ReadViewSpec::default()
        },
        ..ReadOpts::default()
    };
    let mut subscription = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: true, .. }
    ));

    db.delete_in_branch_view(
        "todos",
        head.clone(),
        match &opts.read_view.source {
            ReadViewSourceSpec::BranchView { base, .. } => base.clone(),
            _ => unreachable!(),
        },
        row,
    )
    .unwrap();
    let deleted = block_on(subscription.next_event()).unwrap();
    assert!(
        matches!(
            deleted,
            SubscriptionEvent::Delta { reset: false, ref removed, .. } if removed.len() == 1
        ),
        "unexpected inherited-delete delta: {deleted:?}"
    );
    assert!(
        block_on(db.all(&prepared, opts.clone()))
            .unwrap()
            .is_empty()
    );

    db.restore_with_cells_in_branch(
        "todos",
        head.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("restored".to_owned()))]),
    )
    .unwrap();
    let restored = block_on(subscription.next_event()).unwrap();
    assert!(
        matches!(
            restored,
            SubscriptionEvent::Delta { reset: false, ref added, .. } if added.len() == 1
        ),
        "unexpected inherited-restore delta: {restored:?}"
    );
    let rows = block_on(db.all(&prepared, opts)).unwrap();
    let table = &schema.tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("restored".to_owned()))
    );
}

#[test]
fn frozen_base_subscription_keeps_the_base_fixed_and_the_head_live() {
    let (db, schema) = open_db();
    let row = RowUuid::from_bytes([0x74; 16]);
    let base = selector(0x75);
    let head = selector(0x76);
    let seeded = db
        .insert_with_id_in_branch(
            "todos",
            base.clone(),
            row,
            BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
        )
        .unwrap()
        .mergeable_tx_id();
    let frozen = SnapshotRef {
        owner: seeded.node,
        global_base: GlobalSeq(0),
        local_base: seeded.time,
        dots: Vec::new(),
    };
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts::default().branch_view(
        head.clone(),
        Some(BranchViewBase::Snapshot {
            branch: base.clone(),
            snapshot: frozen,
        }),
    );
    let mut subscription = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: true, .. }
    ));

    db.update_in_branch(
        "todos",
        base,
        row,
        BTreeMap::from([("title".to_owned(), Value::String("later base".to_owned()))]),
    )
    .unwrap();
    assert!(subscription.try_next_event().is_none());

    db.update_in_branch_view(
        "todos",
        head.clone(),
        match &opts.read_view.source {
            ReadViewSourceSpec::BranchView { base, .. } => base.clone(),
            _ => unreachable!(),
        },
        row,
        BTreeMap::from([("title".to_owned(), Value::String("live head".to_owned()))]),
    )
    .unwrap();
    let changed = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        changed,
        SubscriptionEvent::Delta { reset: false, ref updated, .. } if updated.len() == 1
    ));
    let rows = block_on(db.all(&prepared, opts)).unwrap();
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("live head".to_owned()))
    );

    db.delete_in_branch("todos", head.clone(), row).unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref removed, .. } if removed.len() == 1
    ));
    db.restore_with_cells_in_branch(
        "todos",
        head,
        row,
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("restored head".to_owned()),
        )]),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, .. } if added.len() == 1
    ));
}
