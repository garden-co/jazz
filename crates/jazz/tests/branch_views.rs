use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, MergeableTxOps, ReadOpts, SeededRowIdSource, SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::ContributionMergeRow;
use jazz::protocol::{
    BranchSelector, BranchViewBase, ReadViewSourceSpec, ReadViewSpec, SnapshotRef,
};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::time::GlobalTime;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz_storage_rocksdb::RocksDbStorage;

use common::{allow_all_policies, compile_schema};

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

trait TestResultFutureExt<T, E>: Future<Output = Result<T, E>> {
    fn unwrap(self) -> T
    where
        Self: Sized,
        E: std::fmt::Debug,
    {
        block_on(self).unwrap()
    }

    fn expect(self, message: &str) -> T
    where
        Self: Sized,
        E: std::fmt::Debug,
    {
        block_on(self).expect(message)
    }
}

impl<F, T, E> TestResultFutureExt<T, E> for F where F: Future<Output = Result<T, E>> {}

fn selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn branch_owner_policy() -> PolicyExpr {
    PolicyExpr::Exists {
        table: "branches".to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::Cmp {
                column: "branch_key".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec![
                    "__jazz_outer_row".to_owned(),
                    "branch_id".to_owned(),
                ]),
            },
            PolicyExpr::Cmp {
                column: "owner".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec!["user_id".to_owned()]),
            },
        ])),
    }
}

fn policy_with_all_writes(read: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(read.clone())
        .with_insert(read.clone())
        .with_update(Some(read.clone()), read.clone())
        .with_delete(read)
}

fn open_db() -> (Db<MemoryStorage>, JazzSchema) {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .index_only(["title"]),
            )
            .build(),
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
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id"),
            )
            .build(),
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

fn open_rocks_db(path: &std::path::Path, schema: &JazzSchema) -> Db<RocksDbStorage> {
    let families = schema.column_families();
    let storage = RocksDbStorage::open(
        path,
        &families.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .unwrap();
    block_on(Db::open(
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([0xa4; 16]),
                author: AuthorId::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(10)),
    ))
    .unwrap()
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
        "the non-matching head branch-local row must still mask the base index hit"
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
fn branch_view_join_projects_branch_column_subsets_and_shared_tables() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("workspaces").column("name", ColumnType::Text))
            .table(TableSchemaBuilder::new("users").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("workspace_id", "workspaces")
                    .column("role", ColumnType::Text)
                    .branch_by("workspace_id"),
            )
            .table(
                TableSchemaBuilder::new("documents")
                    .fk_column("workspace_id", "workspaces")
                    .column("branch_id", ColumnType::Uuid)
                    .fk_column("owner", "users")
                    .column("title", ColumnType::Text)
                    .branch_by("workspace_id")
                    .branch_by("branch_id"),
            )
            .build(),
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
        BranchSelector::new([("workspace_id", Value::Uuid(workspace))]),
        RowUuid::from_bytes([0x87; 16]),
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    )
    .unwrap();
    let document = RowUuid::from_bytes([0x88; 16]);
    db.insert_with_id_in_branch(
        "documents",
        BranchSelector::new([
            ("workspace_id", Value::Uuid(workspace)),
            ("branch_id", Value::Uuid(branch)),
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
                ("workspace_id", Value::Uuid(workspace)),
                ("branch_id", Value::Uuid(branch)),
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

#[test]
fn branch_view_reachability_consumes_effective_sources() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(TableSchemaBuilder::new("teams").column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new("documents")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id"),
            )
            .table(
                TableSchemaBuilder::new("access")
                    .column("branch_id", ColumnType::Uuid)
                    .fk_column("document", "documents")
                    .fk_column("team", "teams")
                    .branch_by("branch_id"),
            )
            .table(
                TableSchemaBuilder::new("team_edges")
                    .column("branch_id", ColumnType::Uuid)
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams")
                    .branch_by("branch_id"),
            )
            .build(),
    );
    let families = schema.column_families();
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>()),
        DbIdentity {
            node: NodeUuid::from_bytes([0x9d; 16]),
            author: AuthorId::SYSTEM,
        },
    )))
    .unwrap();
    let base = selector(0x9e);
    let head = selector(0x9f);
    let document = RowUuid::from_bytes([0xa0; 16]);
    let access = RowUuid::from_bytes([0xa1; 16]);
    let allowed_team = uuid::Uuid::from_bytes([0xa2; 16]);
    let denied_team = uuid::Uuid::from_bytes([0xa3; 16]);
    for (team, name) in [(allowed_team, "allowed"), (denied_team, "denied")] {
        db.insert_with_id(
            "teams",
            RowUuid(team),
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        )
        .unwrap();
    }
    db.insert_with_id_in_branch(
        "documents",
        base.clone(),
        document,
        BTreeMap::from([("title".to_owned(), Value::String("reachable".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id_in_branch(
        "access",
        base.clone(),
        access,
        BTreeMap::from([
            ("document".to_owned(), Value::Uuid(document.0)),
            ("team".to_owned(), Value::Uuid(allowed_team)),
        ]),
    )
    .unwrap();
    let query = db
        .prepare_query(&Query::from("documents").reachable_via(
            "access",
            "document",
            "team",
            lit(Value::Uuid(allowed_team)),
            "team_edges",
            "member",
            "parent",
            [],
        ))
        .unwrap();
    assert_eq!(
        block_on(db.all(&query, ReadOpts::default().branch_view(base.clone(), None),))
            .unwrap()
            .len(),
        1
    );

    db.update_in_branch_view(
        "access",
        head.clone(),
        Some(BranchViewBase::Current(base.clone())),
        access,
        BTreeMap::from([("team".to_owned(), Value::Uuid(denied_team))]),
    )
    .unwrap();
    assert!(
        block_on(db.all(
            &query,
            ReadOpts::default().branch_view(head, Some(BranchViewBase::Current(base))),
        ))
        .unwrap()
        .is_empty(),
        "the head access branch-local row must mask reachable base evidence"
    );
}

fn open_policy_db() -> (Db<MemoryStorage>, JazzSchema) {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("branches")
                    .fk_column("branch_key", "branches")
                    .column("name", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .fk_column("branch_id", "branches")
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(policy_with_all_writes(branch_owner_policy())),
            )
            .build(),
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
fn branch_column_reference_policy_controls_effective_reads() {
    let (db, _schema) = open_policy_db();
    let owner = AuthorId::from_bytes([0x76; 16]);
    let outsider = AuthorId::from_bytes([0x77; 16]);
    let branch = RowUuid::from_bytes([0x78; 16]);
    let selector = BranchSelector::new([("branch_id", Value::Uuid(branch.0))]);
    db.insert_with_id(
        "branches",
        branch,
        BTreeMap::from([
            ("branch_key".to_owned(), Value::Uuid(branch.0)),
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

    let missing =
        BranchSelector::new([("branch_id", Value::Uuid(RowUuid::from_bytes([0x7b; 16]).0))]);
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
fn frozen_base_applies_one_cut_to_policy_dependencies() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("branches")
                    .column("branch_id", ColumnType::Uuid)
                    .fk_column("branch_key", "branches")
                    .column("name", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("todos")
                    .fk_column("branch_id", "branches")
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(policy_with_all_writes(branch_owner_policy())),
            )
            .build(),
    );
    let families = schema.column_families();
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        MemoryStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>()),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa9; 16]),
            author: AuthorId::SYSTEM,
        },
    )))
    .unwrap();
    let before = AuthorId::from_bytes([0xad; 16]);
    let after = AuthorId::from_bytes([0xae; 16]);
    let base_branch = RowUuid::from_bytes([0xaa; 16]);
    let head_branch = RowUuid::from_bytes([0xab; 16]);
    let base = BranchSelector::new([("branch_id", Value::Uuid(base_branch.0))]);
    let head = BranchSelector::new([("branch_id", Value::Uuid(head_branch.0))]);
    for branch in [base_branch, head_branch] {
        db.insert_with_id_in_branch(
            "branches",
            base.clone(),
            branch,
            BTreeMap::from([
                ("branch_key".to_owned(), Value::Uuid(branch.0)),
                ("name".to_owned(), Value::String("branch".to_owned())),
                ("owner".to_owned(), Value::Uuid(before.0)),
            ]),
        )
        .unwrap();
    }
    let row = RowUuid::from_bytes([0xac; 16]);
    let authored = db
        .insert_with_id_in_branch_for_identity(
            before,
            "todos",
            base.clone(),
            row,
            BTreeMap::from([("title".to_owned(), Value::String("frozen".to_owned()))]),
        )
        .unwrap()
        .mergeable_tx_id();
    let cut = SnapshotRef {
        owner: authored.node,
        global_base: GlobalTime(0),
        local_base: authored.time,
        dots: Vec::new(),
    };
    db.update_in_branch(
        "branches",
        base.clone(),
        head_branch,
        BTreeMap::from([("owner".to_owned(), Value::Uuid(after.0))]),
    )
    .unwrap();

    let query = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts::default().branch_view(
        head,
        Some(BranchViewBase::Snapshot {
            branch: base,
            snapshot: cut,
        }),
    );
    let branches = db.prepare_query(&db.table("branches")).unwrap();
    let branch_rows = block_on(db.all(&branches, opts.clone())).unwrap();
    assert_eq!(branch_rows.len(), 2);
    let branch_table = schema
        .tables
        .iter()
        .find(|table| table.name == "branches")
        .unwrap();
    assert!(branch_rows.iter().all(|row| {
        row.cell(branch_table, "owner") == Some(Value::Uuid(before.0))
            && row.cell(branch_table, "branch_id") == Some(Value::Uuid(head_branch.0))
    }));
    let system_rows = block_on(db.all(&query, opts.clone())).unwrap();
    assert_eq!(system_rows.len(), 1);
    let live_opts = ReadOpts::default().branch_view(
        BranchSelector::new([("branch_id", Value::Uuid(head_branch.0))]),
        Some(BranchViewBase::Current(BranchSelector::new([(
            "branch_id",
            Value::Uuid(base_branch.0),
        )]))),
    );
    assert_eq!(
        block_on(db.all_for_identity(&query, live_opts, after))
            .unwrap()
            .len(),
        1
    );
    let before_rows = block_on(db.all_for_identity(&query, opts.clone(), before)).unwrap();
    let after_rows = block_on(db.all_for_identity(&query, opts, after)).unwrap();
    assert_eq!(
        before_rows.len(),
        1,
        "policy traversal must see the head branch row at the frozen cut; post-cut identity saw {} rows",
        after_rows.len()
    );
    assert!(
        after_rows.is_empty(),
        "a post-cut policy grant must not leak into a frozen branch view"
    );
}

#[test]
fn branch_view_subscription_tracks_reference_policy_revoke_and_grant() {
    let (db, _schema) = open_policy_db();
    let owner = AuthorId::from_bytes([0xb5; 16]);
    let outsider = AuthorId::from_bytes([0xb6; 16]);
    let branch = RowUuid::from_bytes([0xb7; 16]);
    let selector = BranchSelector::new([("branch_id", Value::Uuid(branch.0))]);
    db.insert_with_id(
        "branches",
        branch,
        BTreeMap::from([
            ("branch_key".to_owned(), Value::Uuid(branch.0)),
            ("name".to_owned(), Value::String("draft".to_owned())),
            ("owner".to_owned(), Value::Uuid(owner.0)),
        ]),
    )
    .unwrap();
    let row = RowUuid::from_bytes([0xb8; 16]);
    db.insert_with_id_in_branch_for_identity(
        owner,
        "todos",
        selector.clone(),
        row,
        BTreeMap::from([("title".to_owned(), Value::String("visible".to_owned()))]),
    )
    .unwrap();
    let query = db.prepare_query(&db.table("todos")).unwrap();
    let mut subscription = block_on(db.subscribe_for_identity(
        &query,
        ReadOpts::default().branch_view(selector, None),
        owner,
    ))
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: true, ref added, .. }
            if added.iter().any(|candidate| candidate.row_uuid() == row)
    ));

    db.update(
        "branches",
        branch,
        BTreeMap::from([("owner".to_owned(), Value::Uuid(outsider.0))]),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref removed, .. }
            if removed.iter().any(|candidate| candidate.row_uuid == row)
    ));

    db.update(
        "branches",
        branch,
        BTreeMap::from([("owner".to_owned(), Value::Uuid(owner.0))]),
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, .. }
            if added.iter().any(|candidate| candidate.row_uuid() == row)
    ));
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
fn cross_branch_transaction_and_independent_winners_survive_reopen() {
    let (_template, schema) = open_db();
    let directory = tempfile::tempdir().unwrap();
    let row = RowUuid::from_bytes([0xa5; 16]);
    let left = selector(0xa6);
    let right = selector(0xa7);
    {
        let db = open_rocks_db(directory.path(), &schema);
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
        db.delete_in_branch("todos", left.clone(), row).unwrap();
    }

    let db = open_rocks_db(directory.path(), &schema);
    let query = db.prepare_query(&db.table("todos")).unwrap();
    assert!(
        block_on(db.all(&query, ReadOpts::default().branch_view(left, None)))
            .unwrap()
            .is_empty(),
        "the left deletion register must recover independently"
    );
    let rows = block_on(db.all(&query, ReadOpts::default().branch_view(right, None))).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("right".to_owned()))
    );
}

#[test]
fn sibling_branch_view_subscriptions_isolate_first_writes() {
    let (db, _schema) = open_db();
    #[cfg(feature = "testing")]
    let baseline = db.runtime_stats_for_test().active_subscriptions;
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

    drop(left_stream);
    block_on(db.tick()).unwrap();
    #[cfg(feature = "testing")]
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 1,
        "dropping one branch view must preserve its sibling subscription"
    );
    drop(right_stream);
    block_on(db.tick()).unwrap();
    #[cfg(feature = "testing")]
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline,
        "dropping the final branch view must release its maintained source"
    );
}

#[test]
fn seeded_branch_view_subscription_matches_one_shot_reduction() {
    let (db, schema) = open_db();
    let table = &schema.tables[0];
    let base = selector(0xaf);
    let head = selector(0xb0);
    let rows = (0_u8..12)
        .map(|index| RowUuid::from_bytes([index.wrapping_add(0xb1); 16]))
        .collect::<Vec<_>>();
    for (index, row) in rows.iter().enumerate() {
        db.insert_with_id_in_branch(
            "todos",
            base.clone(),
            *row,
            BTreeMap::from([("title".to_owned(), Value::String(format!("seed-{index}")))]),
        )
        .unwrap();
    }
    let query = db.prepare_query(&db.table("todos")).unwrap();
    let opts =
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone())));
    let mut subscription = block_on(db.subscribe(&query, opts.clone())).unwrap();
    let mut maintained = BTreeMap::<RowUuid, String>::new();
    let apply = |maintained: &mut BTreeMap<RowUuid, String>, event: SubscriptionEvent| {
        let SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } = event
        else {
            panic!("branch-view subscription closed unexpectedly");
        };
        if reset {
            maintained.clear();
        }
        for removed in removed {
            maintained.remove(&removed.row_uuid);
        }
        for row in added.into_iter().chain(updated) {
            let Value::String(title) = row.cell(table, "title").unwrap() else {
                panic!("title must remain a string");
            };
            maintained.insert(row.row_uuid(), title);
        }
    };
    apply(
        &mut maintained,
        block_on(subscription.next_event()).unwrap(),
    );

    let mut seed = 0x5eed_cafe_u64;
    for step in 0..200_u64 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let row = rows[(seed as usize) % rows.len()];
        let result = match (seed >> 32) % 5 {
            0 => block_on(db.update_in_branch(
                "todos",
                base.clone(),
                row,
                BTreeMap::from([("title".to_owned(), Value::String(format!("base-{step}")))]),
            ))
            .map(|_| ()),
            1 => block_on(db.update_in_branch_view(
                "todos",
                head.clone(),
                Some(BranchViewBase::Current(base.clone())),
                row,
                BTreeMap::from([("title".to_owned(), Value::String(format!("head-{step}")))]),
            ))
            .map(|_| ()),
            2 => block_on(db.delete_in_branch_view(
                "todos",
                head.clone(),
                Some(BranchViewBase::Current(base.clone())),
                row,
            ))
            .map(|_| ()),
            3 => block_on(db.restore_with_cells_in_branch(
                "todos",
                head.clone(),
                row,
                BTreeMap::from([(
                    "title".to_owned(),
                    Value::String(format!("restored-{step}")),
                )]),
            ))
            .map(|_| ()),
            _ => block_on(db.update_in_branch(
                "todos",
                head.clone(),
                row,
                BTreeMap::from([("title".to_owned(), Value::String(format!("exact-{step}")))]),
            ))
            .map(|_| ()),
        };
        if result.is_err() {
            continue;
        }
        while let Some(event) = subscription.try_next_event() {
            apply(&mut maintained, event);
        }
        let one_shot = block_on(db.all(&query, opts.clone()))
            .unwrap()
            .into_iter()
            .map(|row| {
                let Value::String(title) = row.cell(table, "title").unwrap() else {
                    panic!("title must remain a string");
                };
                (row.row_uuid(), title)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(maintained, one_shot, "diverged after seeded step {step}");
    }
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
        global_base: GlobalTime(0),
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
mod common;
