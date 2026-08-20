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
use jazz::node::ContributionMergeRow;
use jazz::protocol::{
    BranchSelector, BranchViewBase, ReadViewSourceSpec, ReadViewSpec, SnapshotRef,
};
use jazz::query::{OrderDirection, Query, claim, col, eq};
use jazz::schema::{BranchDimensionSchema, JazzSchema, Policy, TableSchema};
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
        .with_branch_dimension("branch_id", dimension)
        .with_indexed_column("title")],
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

fn open_history_complete_db() -> (Db<MemoryStorage>, JazzSchema) {
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
    let db = block_on(Db::open_history_complete(
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x90; 16]),
                author: AuthorId::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(9)),
    ))
    .unwrap();
    (db, schema)
}

#[test]
fn indexed_branch_view_masks_base_before_applying_the_predicate() {
    let (db, _schema) = open_db();
    let base = selector(0x7d);
    let head = selector(0x7e);
    let overridden = RowUuid::from_bytes([0x7f; 16]);
    let inherited = RowUuid::from_bytes([0x80; 16]);
    for row in [overridden, inherited] {
        db.insert_with_id_in_branch(
            "todos",
            base.clone(),
            row,
            BTreeMap::from([("title".to_owned(), Value::String("needle".to_owned()))]),
        )
        .unwrap();
    }
    db.update_in_branch_view(
        "todos",
        head.clone(),
        Some(BranchViewBase::Current(base.clone())),
        overridden,
        BTreeMap::from([("title".to_owned(), Value::String("hidden".to_owned()))]),
    )
    .unwrap();

    let query = db
        .prepare_query(&Query::from("todos").filter(eq(
            col("title"),
            jazz::query::lit(Value::String("needle".to_owned())),
        )))
        .unwrap();
    let base_rows =
        block_on(db.all(&query, ReadOpts::default().branch_view(base.clone(), None))).unwrap();
    assert_eq!(base_rows.len(), 2);

    let rows = block_on(db.all(
        &query,
        ReadOpts::default().branch_view(head, Some(BranchViewBase::Current(base))),
    ))
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![inherited],
        "the non-matching head incarnation must still mask the base index hit"
    );
}

#[test]
fn branch_view_reduction_precedes_aggregation_and_ordered_windows() {
    let (db, schema) = open_db();
    let base = selector(0x97);
    let head = selector(0x98);
    let replaced = RowUuid::from_bytes([0x99; 16]);
    let inherited = RowUuid::from_bytes([0x9a; 16]);
    let added = RowUuid::from_bytes([0x9b; 16]);
    for (row, title) in [(replaced, "alpha"), (inherited, "bravo")] {
        db.insert_with_id_in_branch(
            "todos",
            base.clone(),
            row,
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
        )
        .unwrap();
    }
    db.update_in_branch_view(
        "todos",
        head.clone(),
        Some(BranchViewBase::Current(base.clone())),
        replaced,
        BTreeMap::from([("title".to_owned(), Value::String("zulu".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id_in_branch(
        "todos",
        head.clone(),
        added,
        BTreeMap::from([("title".to_owned(), Value::String("charlie".to_owned()))]),
    )
    .unwrap();
    let opts = ReadOpts::default().branch_view(head, Some(BranchViewBase::Current(base)));

    let count = db.prepare_query(&Query::from("todos").count()).unwrap();
    let count_rows = block_on(db.all(&count, opts.clone())).unwrap();
    assert_eq!(count_rows[0].cell_at(0), Some(Value::U64(3)));

    let window = db
        .prepare_query(
            &Query::from("todos")
                .order_by("title", OrderDirection::Asc)
                .offset(1)
                .limit(1),
        )
        .unwrap();
    let rows = block_on(db.all(&window, opts)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("charlie".to_owned()))
    );
}

#[test]
fn branch_view_join_projects_dimension_subsets_and_shared_tables() {
    let workspace_dimension = BranchDimensionId(uuid::Uuid::from_bytes([0x81; 16]));
    let branch_dimension = BranchDimensionId(uuid::Uuid::from_bytes([0x82; 16]));
    let schema = JazzSchema::new_with_branch_dimensions(
        [
            BranchDimensionSchema::new(
                workspace_dimension,
                "workspace",
                ColumnType::Uuid,
                Value::Uuid(uuid::Uuid::nil()),
            ),
            BranchDimensionSchema::new(
                branch_dimension,
                "branch",
                ColumnType::Uuid,
                Value::Uuid(uuid::Uuid::nil()),
            ),
        ],
        [
            TableSchema::new(
                "workspaces",
                [ColumnSchema::new("name", ColumnType::String)],
            ),
            TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)]),
            TableSchema::new(
                "memberships",
                [
                    ColumnSchema::new("workspace_id", ColumnType::Uuid),
                    ColumnSchema::new("role", ColumnType::String),
                ],
            )
            .with_reference("workspace_id", "workspaces")
            .with_branch_dimension("workspace_id", workspace_dimension),
            TableSchema::new(
                "documents",
                [
                    ColumnSchema::new("workspace_id", ColumnType::Uuid),
                    ColumnSchema::new("branch_id", ColumnType::Uuid),
                    ColumnSchema::new("owner", ColumnType::Uuid),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            )
            .with_reference("workspace_id", "workspaces")
            .with_reference("owner", "users")
            .with_branch_dimension("workspace_id", workspace_dimension)
            .with_branch_dimension("branch_id", branch_dimension),
        ],
    );
    let families = schema.column_families();
    let db = block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>()),
            DbIdentity {
                node: NodeUuid::from_bytes([0x83; 16]),
                author: AuthorId::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(3)),
    ))
    .unwrap();
    let workspace = uuid::Uuid::from_bytes([0x84; 16]);
    let branch = uuid::Uuid::from_bytes([0x85; 16]);
    let owner = RowUuid::from_bytes([0x86; 16]);
    db.insert_with_id(
        "workspaces",
        RowUuid(workspace),
        BTreeMap::from([("name".to_owned(), Value::String("shared".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "users",
        owner,
        BTreeMap::from([("name".to_owned(), Value::String("shared".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id_in_branch(
        "memberships",
        BranchSelector::new([("workspace", Value::Uuid(workspace))]),
        RowUuid::from_bytes([0x87; 16]),
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    )
    .unwrap();
    let document = RowUuid::from_bytes([0x88; 16]);
    db.insert_with_id_in_branch(
        "documents",
        BranchSelector::new([
            ("workspace", Value::Uuid(workspace)),
            ("branch", Value::Uuid(branch)),
        ]),
        document,
        BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(owner.0)),
            ("title".to_owned(), Value::String("draft".to_owned())),
        ]),
    )
    .unwrap();

    let query = db
        .prepare_query(
            &Query::from("documents")
                .join_via_row_id("users", "owner", [])
                .join_via_column(
                    "memberships",
                    "workspace_id",
                    "workspace_id",
                    [eq(col("role"), jazz::query::lit("editor"))],
                ),
        )
        .unwrap();
    let rows = block_on(db.all(
        &query,
        ReadOpts::default().branch_view(
            BranchSelector::new([
                ("workspace", Value::Uuid(workspace)),
                ("branch", Value::Uuid(branch)),
            ]),
            None,
        ),
    ))
    .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![document]
    );
}

fn open_policy_db() -> (Db<MemoryStorage>, JazzSchema) {
    let dimension = BranchDimensionId(uuid::Uuid::from_bytes([0x74; 16]));
    let branch_policy = Policy::shape(Query::from("todos").join_via_row_id(
        "branches",
        "branch_id",
        [eq(col("owner"), claim("sub"))],
    ));
    let schema = JazzSchema::new_with_branch_dimensions(
        [BranchDimensionSchema::new(
            dimension,
            "branch",
            ColumnType::Uuid,
            Value::Uuid(uuid::Uuid::nil()),
        )],
        [
            TableSchema::new(
                "branches",
                [
                    ColumnSchema::new("name", ColumnType::String),
                    ColumnSchema::new("owner", ColumnType::Uuid),
                ],
            )
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
            TableSchema::new(
                "todos",
                [
                    ColumnSchema::new("branch_id", ColumnType::Uuid),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            )
            .with_reference("branch_id", "branches")
            .with_branch_dimension("branch_id", dimension)
            .with_read_policy(branch_policy.clone())
            .with_write_policy(branch_policy),
        ],
    );
    let families = schema.column_families();
    let storage = MemoryStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>());
    let db = block_on(Db::open(
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0x75; 16]),
                author: AuthorId::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(2)),
    ))
    .unwrap();
    (db, schema)
}

#[test]
fn branch_dimension_reference_policy_controls_effective_reads() {
    let (db, _schema) = open_policy_db();
    let owner = AuthorId::from_bytes([0x76; 16]);
    let outsider = AuthorId::from_bytes([0x77; 16]);
    let branch = RowUuid::from_bytes([0x78; 16]);
    let selector = BranchSelector::new([("branch", Value::Uuid(branch.0))]);
    db.insert_with_id(
        "branches",
        branch,
        BTreeMap::from([
            ("name".to_owned(), Value::String("draft".to_owned())),
            ("owner".to_owned(), Value::Uuid(owner.0)),
        ]),
    )
    .unwrap();
    let branches = db.prepare_query(&db.table("branches")).unwrap();
    assert_eq!(
        block_on(db.all_for_identity(&branches, ReadOpts::default(), owner))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        block_on(db.all_for_identity(&branches, ReadOpts::default(), outsider))
            .unwrap()
            .len(),
        1
    );

    let todo = RowUuid::from_bytes([0x79; 16]);
    db.insert_with_id_in_branch_for_identity(
        owner,
        "todos",
        selector.clone(),
        todo,
        BTreeMap::from([("title".to_owned(), Value::String("allowed".to_owned()))]),
    )
    .expect("the ordinary referenced branch row authorizes its owner");
    let joined = db
        .prepare_query(&Query::from("todos").join_via_row_id(
            "branches",
            "branch_id",
            [eq(col("owner"), jazz::query::lit(owner.0))],
        ))
        .unwrap();
    assert_eq!(
        block_on(db.all(
            &joined,
            ReadOpts::default().branch_view(selector.clone(), None),
        ))
        .unwrap()
        .len(),
        1,
        "ordinary reference traversal must see shared policy evidence from a branch view"
    );
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts::default().branch_view(selector, None);
    assert_eq!(
        block_on(db.all_for_identity(&prepared, opts.clone(), owner))
            .unwrap()
            .len(),
        1
    );
    assert!(
        block_on(db.all_for_identity(&prepared, opts, outsider))
            .unwrap()
            .is_empty()
    );

    let missing = BranchSelector::new([("branch", Value::Uuid(RowUuid::from_bytes([0x7b; 16]).0))]);
    assert!(
        block_on(db.all_for_identity(
            &prepared,
            ReadOpts::default().branch_view(missing, None),
            owner,
        ))
        .unwrap()
        .is_empty(),
        "a forged branch coordinate has no ordinary policy evidence"
    );
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
fn branch_move_is_explicit_source_delete_and_destination_restore() {
    let (db, schema) = open_db();
    let source = selector(0x94);
    let target = selector(0x95);
    let row = RowUuid::from_bytes([0x96; 16]);
    db.insert_with_id_in_branch(
        "todos",
        source.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("move me".to_owned()))]),
    )
    .unwrap();

    let tx = db.mergeable_tx().unwrap();
    tx.move_between_branches("todos", source.clone(), target.clone(), row)
        .unwrap();
    tx.commit().unwrap();

    let query = db.prepare_query(&db.table("todos")).unwrap();
    assert!(
        block_on(db.all(&query, ReadOpts::default().branch_view(source, None),))
            .unwrap()
            .is_empty()
    );
    let destination =
        block_on(db.all(&query, ReadOpts::default().branch_view(target, None))).unwrap();
    assert_eq!(destination.len(), 1);
    assert_eq!(
        destination[0].cell(&schema.tables[0], "title"),
        Some(Value::String("move me".to_owned()))
    );
}

#[test]
fn sibling_branch_view_subscriptions_isolate_first_writes() {
    let (db, _schema) = open_db();
    let left = selector(0x89);
    let right = selector(0x8a);
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let mut left_stream = block_on(db.subscribe(
        &prepared,
        ReadOpts::default().branch_view(left.clone(), None),
    ))
    .unwrap();
    let mut right_stream = block_on(db.subscribe(
        &prepared,
        ReadOpts::default().branch_view(right.clone(), None),
    ))
    .unwrap();
    assert!(matches!(
        block_on(left_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: true, ref added, .. } if added.is_empty()
    ));
    assert!(matches!(
        block_on(right_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: true, ref added, .. } if added.is_empty()
    ));

    let row = RowUuid::from_bytes([0x8b; 16]);
    db.insert_with_id_in_branch(
        "todos",
        left,
        row,
        BTreeMap::from([("title".to_owned(), Value::String("left".to_owned()))]),
    )
    .unwrap();
    assert!(matches!(
        block_on(left_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, .. }
            if added.iter().any(|candidate| candidate.row_uuid() == row)
    ));
    assert!(
        right_stream.try_next_event().is_none(),
        "a sibling branch key must not receive the first-write delta"
    );

    db.insert_with_id_in_branch(
        "todos",
        right,
        row,
        BTreeMap::from([("title".to_owned(), Value::String("right".to_owned()))]),
    )
    .unwrap();
    assert!(matches!(
        block_on(right_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, .. }
            if added.iter().any(|candidate| candidate.row_uuid() == row)
    ));
    assert!(left_stream.try_next_event().is_none());

    db.delete_in_branch("todos", selector(0x89), row).unwrap();
    assert!(matches!(
        block_on(left_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref removed, .. }
            if removed.iter().any(|candidate| candidate.row_uuid == row)
    ));
    assert!(right_stream.try_next_event().is_none());
    db.restore_in_branch("todos", selector(0x89), row).unwrap();
    assert!(matches!(
        block_on(left_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, .. }
            if added.iter().any(|candidate| candidate.row_uuid() == row)
    ));
    assert!(right_stream.try_next_event().is_none());
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

#[test]
fn db_contribution_merge_is_an_ordinary_retry_safe_transaction() {
    let (db, schema) = open_history_complete_db();
    let source = selector(0x91);
    let target = selector(0x92);
    let row = RowUuid::from_bytes([0x93; 16]);
    db.insert_with_id_in_branch(
        "todos",
        source.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("source".to_owned()))]),
    )
    .unwrap();

    let selected = || {
        [ContributionMergeRow {
            table: "todos".to_owned(),
            row_uuid: row,
        }]
    };
    let merged = db
        .merge_branch_contributions(source.clone(), target.clone(), selected())
        .unwrap()
        .expect("the first calculation emits an ordinary transaction");
    let _ordinary_write_state = db.write_state(merged).unwrap();
    assert!(
        db.merge_branch_contributions(source, target.clone(), selected())
            .unwrap()
            .is_none(),
        "observed contribution provenance suppresses a retry"
    );

    let query = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(&query, ReadOpts::default().branch_view(target, None))).unwrap();
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("source".to_owned()))
    );
}
