use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use groove::schema::{ColumnSchema, ColumnType};
use groove::storage::{OrderedKvStorage, ReopenableStorage, RocksDbStorage};

use super::*;
use crate::ids::{AuthorId, NodeUuid};
use crate::protocol::{CatalogueAck, LensOp, TableLens};
use crate::query::{
    Include, JoinMode, all_of, any_of, col, contains, eq, gt, in_list, is_null, lit, lte, ne, not,
};
use crate::schema::{Policy, TableSchema};

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn opened_rows(event: SubscriptionEvent) -> Vec<CurrentRow> {
    match event {
        SubscriptionEvent::Opened { current, .. } | SubscriptionEvent::Reset { current, .. } => {
            current
        }
        other => panic!("expected subscription snapshot event, got {other:?}"),
    }
}

fn delta_rows(event: SubscriptionEvent) -> (Vec<CurrentRow>, Vec<CurrentRow>, Vec<RemovedRow>) {
    match event {
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => (added, updated, removed),
        other => panic!("expected subscription delta event, got {other:?}"),
    }
}

fn event_settled(event: &SubscriptionEvent) -> bool {
    match event {
        SubscriptionEvent::Opened { settled, .. }
        | SubscriptionEvent::Delta { settled, .. }
        | SubscriptionEvent::Reset { settled, .. } => *settled,
        SubscriptionEvent::Closed => false,
    }
}

fn global_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
    }
}

fn edge_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Edge,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
    }
}

fn prepared<S>(db: &Db<S>, query: &Query) -> PreparedQuery
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.prepare_query(query).unwrap()
}

fn prepared_read<S>(db: &Db<S>, query: &Query) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.read(&prepared).unwrap()
}

fn prepared_one<S>(db: &Db<S>, query: &Query) -> Option<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.one(&prepared).unwrap()
}

fn prepared_all<S>(db: &Db<S>, query: &Query, opts: ReadOpts) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    block_on(db.all(&prepared, opts)).unwrap()
}

fn prepared_subscribe<S>(
    db: &Db<S>,
    query: &Query,
    opts: ReadOpts,
) -> Result<SubscriptionStream, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    block_on(db.subscribe(&prepared, opts))
}

#[derive(Default)]
struct RecordingScheduler {
    calls: RefCell<Vec<TickUrgency>>,
}

impl TickScheduler for RecordingScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.calls.borrow_mut().push(urgency);
    }
}

impl RecordingScheduler {
    fn take(&self) -> Vec<TickUrgency> {
        std::mem::take(&mut self.calls.borrow_mut())
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn owner_read_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))
    .with_write_policy(Policy::public())])
}

fn owner_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}

fn owner_id_read_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "messages",
        [
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("messages").filter(eq(col("owner_id"), crate::query::claim("user_id"))),
    ))
    .with_write_policy(Policy::public())])
}

fn owner_id_public_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "messages",
        [
            ColumnSchema::new("body", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn evolved_owner_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn cells(title: &str, done: bool, owner: AuthorId) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn issue_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("projects", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "issues",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("state", ColumnType::String),
                ColumnSchema::new("assignee", ColumnType::Uuid),
                ColumnSchema::new("project", ColumnType::Uuid),
                ColumnSchema::new("priority", ColumnType::U64),
                ColumnSchema::new("labels", ColumnType::String.array_of()),
                ColumnSchema::new("snoozed_until", ColumnType::U64.nullable()),
            ],
        )
        .with_reference("project", "projects")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "issue_tags",
            [
                ColumnSchema::new("issue", ColumnType::Uuid),
                ColumnSchema::new("tag", ColumnType::String),
            ],
        )
        .with_reference("issue", "issues")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn issue_cells(
    title: &str,
    state: &str,
    assignee: AuthorId,
    project: RowUuid,
    priority: u64,
    labels: &[&str],
    snoozed_until: Option<u64>,
) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("state".to_owned(), Value::String(state.to_owned())),
        ("assignee".to_owned(), Value::Uuid(assignee.0)),
        ("project".to_owned(), Value::Uuid(project.0)),
        ("priority".to_owned(), Value::U64(priority)),
        (
            "labels".to_owned(),
            Value::Array(
                labels
                    .iter()
                    .map(|label| Value::String((*label).to_owned()))
                    .collect(),
            ),
        ),
        (
            "snoozed_until".to_owned(),
            Value::Nullable(snoozed_until.map(|value| Box::new(Value::U64(value)))),
        ),
    ])
}

#[test]
fn can_insert_dry_run_uses_current_identity_without_writing() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);

    assert!(
        owner_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap()
    );
    assert!(
        !other_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap()
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 0);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 0);
}

#[test]
fn can_read_dry_run_uses_current_local_winner() {
    let schema = owner_read_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let row = row(1);
    let write = core
        .insert_with_id("todos", row, cells("private", false, owner))
        .unwrap();

    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id());
    let SyncMessage::CommitUnit { tx, versions } = unit.unwrap() else {
        panic!("commit unit expected");
    };
    owner_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        })
        .unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit { tx, versions })
        .unwrap();

    assert!(owner_db.can_read("todos", row).unwrap());
    assert!(!other_db.can_read("todos", row).unwrap());
}

#[test]
fn can_delete_dry_run_is_gated_by_write_policy_without_mutating() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    let row = row(1);
    let write = owner_db
        .insert_with_id("todos", row, cells("owned", false, owner))
        .unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(
            owner_db
                .node
                .node
                .borrow_mut()
                .commit_unit_for(write.mergeable_tx_id())
                .unwrap(),
        )
        .unwrap();

    assert!(owner_db.can_delete("todos", row).unwrap());
    assert!(!other_db.can_delete("todos", row).unwrap());
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 1);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 1);
}

#[test]
fn core_attributed_insert_uses_core_identity_for_policy_and_user_for_made_by() {
    let schema = owner_write_schema();
    let backend = AuthorId::from_bytes([0xbe; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, backend, &schema);
    let write = core
        .insert_attributed(
            attributed_user,
            "todos",
            cells("attributed", false, backend),
        )
        .unwrap();

    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, attributed_user);
    assert_eq!(core.read(&core.table("todos")).unwrap().len(), 1);
}

#[test]
fn client_attributed_insert_to_different_user_is_rejected() {
    let schema = owner_write_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let err = match client.insert_attributed(
        attributed_user,
        "todos",
        cells("forged", false, client_author),
    ) {
        Ok(_) => panic!("client attribution should be rejected"),
        Err(err) => err,
    };

    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert_eq!(prepared_read(&client, &client.table("todos")).len(), 0);
}

#[test]
fn default_insert_keeps_subject_and_made_by_equal() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, owner, &schema);
    let write = db.insert("todos", cells("default", false, owner)).unwrap();
    let unit = db
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, owner);
    assert_eq!(prepared_read(&db, &db.table("todos")).len(), 1);
}

#[test]
fn db_facade_opens_writes_and_reads_todos_end_to_end() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("learn the db facade", false),
        )
        .unwrap();
    let todo = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let read_rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&read_rows), vec![todo]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let one_row = prepared_one(&db, &query).unwrap();
    assert_eq!(one_row.row_uuid(), todo);
    assert_eq!(
        one_row.cell(table, "title"),
        Some(Value::String("learn the db facade".to_owned()))
    );

    let all_rows = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(row_ids(&all_rows), vec![todo]);
    assert_eq!(all_rows[0].cell(table, "done"), Some(Value::Bool(false)));
}

#[test]
fn read_opts_default_and_effective_tier_preserve_local_update_contract() {
    let opts = ReadOpts::default();
    assert_eq!(opts.tier, DurabilityTier::Local);
    assert_eq!(opts.local_updates, LocalUpdates::Immediate);
    assert_eq!(opts.propagation, Propagation::Full);

    assert_eq!(
        effective_read_tier(ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
        }),
        DurabilityTier::Local
    );
    assert_eq!(
        effective_read_tier(ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
        }),
        DurabilityTier::Global
    );
    assert_eq!(
        effective_read_tier(ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
        }),
        DurabilityTier::None
    );
}

#[test]
fn edge_read_opts_and_wait_honor_edge_durability() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert("todos", doctest_support::todo_cells("edge observed", false))
        .unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    assert_eq!(
        effective_read_tier(ReadOpts {
            tier: DurabilityTier::Edge,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
        }),
        DurabilityTier::Edge
    );
    assert!(
        doctest_support::block_on(db.all(
            &prepared_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                include_deleted: false,
            },
        ))
        .unwrap()
        .is_empty()
    );
    let not_observed = doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap_err();
    assert_eq!(not_observed.code, ErrorCode::NotObserved);

    // E1: edge-accept produced directly; E2 wires the acceptance path.
    db.node
        .node
        .borrow_mut()
        .apply_fate_update(
            write.mergeable_tx_id(),
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();

    assert_eq!(
        doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(
        row_ids(
            &doctest_support::block_on(db.all(
                &prepared_query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    local_updates: LocalUpdates::Immediate,
                    propagation: Propagation::LocalOnly,
                    include_deleted: false,
                },
            ))
            .unwrap()
        ),
        vec![write.row_uuid()]
    );
}

#[test]
fn upsert_merges_existing_rows_but_writes_absent_rows_directly() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let existing = row(1);
    let absent = row(2);

    db.upsert(
        "todos",
        existing,
        doctest_support::todo_cells("draft", false),
    )
    .unwrap();
    db.upsert(
        "todos",
        existing,
        BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
    )
    .unwrap();
    db.upsert(
        "todos",
        absent,
        BTreeMap::from([("title".to_owned(), Value::String("created".to_owned()))]),
    )
    .unwrap();

    let rows = prepared_read(&db, &db.table("todos"))
        .into_iter()
        .map(|row| (row.row_uuid(), row))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows.get(&existing).unwrap().cell(table, "title"),
        Some(Value::String("renamed".to_owned()))
    );
    assert_eq!(
        rows.get(&existing).unwrap().cell(table, "done"),
        Some(Value::Bool(false))
    );
    assert_eq!(
        rows.get(&absent).unwrap().cell(table, "title"),
        Some(Value::String("created".to_owned()))
    );
    assert_eq!(rows.get(&absent).unwrap().cell(table, "done"), None);
}

#[test]
fn mergeable_tx_commits_multiple_writes_under_one_tx_id() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row_one = row(1);
    let row_two = row(2);
    let mut tx = db.mergeable_tx();

    tx.insert_with_id("todos", row_one, doctest_support::todo_cells("one", false))
        .unwrap();
    tx.insert_with_id("todos", row_two, doctest_support::todo_cells("two", true))
        .unwrap();
    let tx_id = tx.commit().unwrap();

    let rows = prepared_read(&db, &db.table("todos"))
        .into_iter()
        .map(|row| (row.row_uuid(), row))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows.get(&row_one).unwrap().cell(table, "title"),
        Some(Value::String("one".to_owned()))
    );
    assert_eq!(
        rows.get(&row_two).unwrap().cell(table, "title"),
        Some(Value::String("two".to_owned()))
    );
    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 2);
    assert_eq!(versions.len(), 2);
}

#[test]
fn mergeable_tx_coalesces_insert_then_update_for_same_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);
    let mut tx = db.mergeable_tx();

    tx.insert_with_id("todos", row, doctest_support::todo_cells("draft", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let tx_id = tx.commit().unwrap();

    let row_after = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("draft".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_coalesces_restore_then_update_for_same_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("archived", false))
        .unwrap();
    db.delete("todos", row).unwrap();
    assert!(prepared_read(&db, &db.table("todos")).is_empty());

    let mut tx = db.mergeable_tx();
    tx.restore("todos", row, doctest_support::todo_cells("restored", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let tx_id = tx.commit().unwrap();

    let row_after = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("restored".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 2);
    assert_eq!(versions.len(), 2);
    assert_eq!(
        versions
            .iter()
            .filter(|version| version.deletion().is_none())
            .count(),
        1
    );
    assert_eq!(
        versions
            .iter()
            .filter(|version| version.deletion() == Some(DeletionEvent::Restored))
            .count(),
        1
    );
}

#[test]
fn mergeable_tx_coalesces_repeated_same_row_updates() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);
    let mut tx = db.mergeable_tx();

    tx.insert_with_id("todos", row, doctest_support::todo_cells("first", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
    )
    .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let tx_id = tx.commit().unwrap();

    let row_after = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("second".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_coalesces_update_then_delete_for_same_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("base", false))
        .unwrap();
    let mut tx = db.mergeable_tx();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("ignored".to_owned()))]),
    )
    .unwrap();
    tx.delete("todos", row).unwrap();
    let tx_id = tx.commit().unwrap();

    assert!(prepared_read(&db, &db.table("todos")).is_empty());
    let unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn exclusive_tx_rejects_conflicting_concurrent_update() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let table = &schema.tables[0];
    let row = row(1);

    core.insert_with_id("todos", row, cells("base", false, owner))
        .unwrap();
    let first = core.exclusive_tx().unwrap();
    let second = core.exclusive_tx().unwrap();
    assert_eq!(
        second.read("todos", row).unwrap().unwrap().get("title"),
        Some(&Value::String("base".to_owned()))
    );

    first
        .insert_with_id("todos", row, cells("first", false, owner))
        .unwrap();
    first.commit().unwrap();
    second
        .update(
            "todos",
            row,
            BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        )
        .unwrap();

    let err = second.commit().unwrap_err();

    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(err.message.contains("ExclusiveConflict"));
    assert_eq!(
        core.one(&core.table("todos"))
            .unwrap()
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

#[test]
fn exclusive_tx_blind_writes_are_first_committer_wins() {
    // Two concurrent exclusive transactions overwrite the same existing row
    // WITHOUT reading it. With no read sets, only per-write first-committer-wins
    // (INV-TX-20) can catch the conflict — this is the exact case the earlier
    // broken validator let through (it short-circuited to "ok" on empty reads).
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let table = &schema.tables[0];
    let row = row(1);

    core.insert_with_id("todos", row, cells("base", false, owner))
        .unwrap();

    let first = core.exclusive_tx().unwrap();
    let second = core.exclusive_tx().unwrap();
    first
        .insert_with_id("todos", row, cells("first", false, owner))
        .unwrap();
    second
        .insert_with_id("todos", row, cells("second", false, owner))
        .unwrap();

    first.commit().unwrap();
    let err = second.commit().unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(err.message.contains("ExclusiveConflict"));
    assert_eq!(
        core.one(&core.table("todos"))
            .unwrap()
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

#[test]
fn db_facade_mutation_lifecycle_writes_reads_deletes_and_restores() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let write = db
        .insert("todos", doctest_support::todo_cells("draft todo", false))
        .unwrap();
    let todo = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let write = db
        .update(
            "todos",
            todo,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));

    let write = db.delete("todos", todo).unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert!(prepared_read(&db, &query).is_empty());

    let write = db
        .restore(
            "todos",
            todo,
            doctest_support::todo_cells("restored todo", true),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("restored todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn db_facade_subscription_reports_initial_and_changed_results() {
    let schema = doctest_support::schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let db = doctest_support::block_on(Db::open_history_complete(DbConfig {
        schema,
        storage: doctest_support::MemoryStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorId::from_bytes([0xa1; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];
    let prepared_query = prepared(&db, &query);
    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    let todo = RowUuid::from_bytes([0x44; 16]);
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        todo,
        db.identity.author,
        doctest_support::todo_cells("subscription makes a todo appear", true),
    )
    .unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_event()).unwrap());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&added), vec![todo]);
    assert_eq!(
        added[0].cell(table, "title"),
        Some(Value::String("subscription makes a todo appear".to_owned()))
    );
    assert_eq!(added[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn db_facade_subscription_refresh_preserves_read_tier() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    db.insert(
        "todos",
        doctest_support::todo_cells("pending local-only write", true),
    )
    .unwrap();

    assert_eq!(prepared_read(&db, &query).len(), 1);
}

#[test]
fn db_facade_subscription_accepts_local_tier_for_alpha_style_live_reads() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
    let opened = doctest_support::block_on(subscription.next_event()).unwrap();
    assert_eq!(opened_rows(opened), Vec::<CurrentRow>::new());

    db.insert(
        "todos",
        doctest_support::todo_cells("local callback", false),
    )
    .unwrap();
    let changed = doctest_support::block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta { added, tier, .. } = changed else {
        panic!("expected local subscription delta");
    };
    assert_eq!(tier, DurabilityTier::Local);
    assert_eq!(added.len(), 1);
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn local_write_is_readable_synchronously_without_running_tick() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    db.insert(
        "todos",
        doctest_support::todo_cells("read before tick", false),
    )
    .unwrap();

    let rows = db.read(&prepared_query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn local_write_notifies_subscription_synchronously_without_running_tick() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    db.insert(
        "todos",
        doctest_support::todo_cells("notify before tick", false),
    )
    .unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn db_facade_schedules_immediate_tick_for_propagated_query_coverage() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    db.propagate_query_with_opts(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
        },
    );

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn db_facade_local_only_subscription_does_not_register_upstream_coverage() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());
    assert_eq!(scheduler.take(), Vec::<TickUrgency>::new());
    assert!(db.node.upstream_subscriptions.borrow().is_empty());
}

#[test]
fn db_facade_schedules_immediate_tick_for_upstream_connection() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let (client_transport, _server_transport) = duplex();

    let _upstream = db.connect_upstream(client_transport);

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn upstream_inbound_application_schedules_immediate_tick() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x51, author, &schema);
    let client = open_db(0x52, author, &schema);
    let scheduler = Rc::new(RecordingScheduler::default());
    client.set_tick_scheduler(Some(scheduler.clone()));
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);
    scheduler.take();

    let query = client.table("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    scheduler.take();

    client.tick().unwrap();
    assert!(scheduler.take().is_empty());
    server.tick().unwrap();
    assert!(scheduler.take().is_empty());
    client.tick().unwrap();

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn mergeable_tx_emits_one_subscription_delta_for_many_writes() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert!(opened_rows(doctest_support::block_on(subscription.next_event()).unwrap()).is_empty());

    let mut tx = db.mergeable_tx();
    for index in 0..100u8 {
        tx.insert_with_id(
            "todos",
            RowUuid::from_bytes([index + 1; 16]),
            doctest_support::todo_cells(&format!("todo {index}"), false),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 100);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert!(subscription.try_next_event().is_none());
}

#[test]
fn db_facade_runs_saas_shaped_local_lane_end_to_end() {
    let schema = schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: owner,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x11))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();

    let query = Query::from("todos");
    let write = db
        .insert("todos", cells("ship facade", false, owner))
        .unwrap();
    let todo = write.row_uuid();
    let table = &schema.tables[0];
    let rows = prepared_read(&db, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("ship facade".to_owned()))
    );
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    db.update(
        "todos",
        todo,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    )
    .unwrap();
    let updated = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].cell(table, "done"), Some(Value::Bool(true)));
}

/// In-memory transport pair: each side's outbound queue is the other's
/// inbound queue, so a `send` lands directly in the peer's `try_recv`.
struct DuplexTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
        }),
    )
}

struct BackpressureOnceTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    failed: bool,
}

impl Transport for BackpressureOnceTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if !self.failed {
            self.failed = true;
            return Err(TransportError::Backpressure);
        }
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}

/// Byte transport pair: each side sends postcard-encoded frames to the
/// other's staged inbound queue.
struct ByteDuplexTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
}

impl WireTransport for ByteDuplexTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn byte_duplex_raw() -> (ByteDuplexTransport, ByteDuplexTransport) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        ByteDuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
        },
        ByteDuplexTransport {
            outbound: right,
            inbound: left,
        },
    )
}

fn byte_duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    (
        Box::new(WireTransportAdapter::current(left)),
        Box::new(WireTransportAdapter::current(right)),
    )
}

fn byte_duplex_with_session(
    identity: AuthorId,
    epoch: u64,
) -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    let session = WireSession {
        session_id: "test-session".to_owned(),
        epoch,
        identity: Some(identity),
    };
    (
        Box::new(WireTransportAdapter::new(
            left,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD
                | crate::wire::FEATURE_SESSION_FRAME
                | FEATURE_STRUCTURED_ERRORS,
            Some(session.clone()),
        )),
        Box::new(WireTransportAdapter::new(
            right,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD
                | crate::wire::FEATURE_SESSION_FRAME
                | FEATURE_STRUCTURED_ERRORS,
            Some(session),
        )),
    )
}

fn test_wire_session(identity: AuthorId, epoch: u64) -> WireSession {
    WireSession {
        session_id: "test-session".to_owned(),
        epoch,
        identity: Some(identity),
    }
}

fn test_catalogue_ack() -> SyncMessage {
    SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(1),
        schema: None,
        lens: None,
        applied: true,
    })
}

fn encode_test_message_frame(session: Option<WireSession>) -> Vec<u8> {
    let payload = encode_sync_message(&test_catalogue_ack()).unwrap();
    let mut envelope = WireEnvelope::new(
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        payload,
    );
    if let Some(session) = session {
        envelope = envelope.with_session(session);
    }
    encode_frame(&WireFrame::Message(envelope)).unwrap()
}

fn expect_auth_failed_frame(transport: &mut ByteDuplexTransport, retry: WireRetry, message: &str) {
    let error = transport.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    let WireFrame::Error(WireError {
        code,
        retry: actual_retry,
        message: actual_message,
    }) = frame
    else {
        panic!("expected error frame");
    };
    assert_eq!(code, WireErrorCode::AuthFailed);
    assert_eq!(actual_retry, retry);
    assert!(
        actual_message.contains(message),
        "expected {actual_message:?} to contain {message:?}"
    );
}

#[test]
fn wire_transport_adapter_reports_malformed_frames() {
    let (left, mut right) = byte_duplex_raw();
    left.inbound.borrow_mut().push_back(vec![0xff, 0x00, 0x01]);

    let mut adapter = WireTransportAdapter::current(left);
    assert!(adapter.try_recv().is_none());

    let error = right.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    assert!(matches!(
        frame,
        WireFrame::Error(WireError {
            code: WireErrorCode::MalformedFrame,
            retry: WireRetry::Never,
            ..
        })
    ));
}

#[test]
fn wire_transport_adapter_accepts_matching_session() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorId::from_bytes([0xa1; 16]);
    let session = test_wire_session(identity, 3);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(session.clone())));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(session),
    );

    assert_eq!(adapter.try_recv(), Some(test_catalogue_ack()));
    assert!(right.try_recv_frame().is_none());
}

#[test]
fn wire_transport_adapter_rejects_missing_session_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorId::from_bytes([0xa2; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(None));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "missing");
}

#[test]
fn wire_transport_adapter_rejects_wrong_identity_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let expected_identity = AuthorId::from_bytes([0xa3; 16]);
    let actual_identity = AuthorId::from_bytes([0xb3; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(test_wire_session(
            actual_identity,
            3,
        ))));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(expected_identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "identity");
}

#[test]
fn wire_transport_adapter_rejects_stale_epoch_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorId::from_bytes([0xa4; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(test_wire_session(
            identity, 2,
        ))));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterResume, "stale");
}

#[test]
fn wire_transport_adapter_preserves_message_order() {
    let (left, mut right) = byte_duplex_raw();
    let mut adapter = WireTransportAdapter::current(left);

    adapter
        .send(SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            schema: None,
            lens: None,
            applied: true,
        }))
        .unwrap();
    adapter
        .send(SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(2),
            schema: None,
            lens: None,
            applied: true,
        }))
        .unwrap();

    let first = right.try_recv_frame().unwrap();
    let second = right.try_recv_frame().unwrap();
    let first = match decode_frame(&first).unwrap() {
        WireFrame::Message(envelope) => decode_sync_message(&envelope.payload).unwrap(),
        other => panic!("expected message frame, got {other:?}"),
    };
    let second = match decode_frame(&second).unwrap() {
        WireFrame::Message(envelope) => decode_sync_message(&envelope.payload).unwrap(),
        other => panic!("expected message frame, got {other:?}"),
    };

    assert!(matches!(
        first,
        SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            ..
        })
    ));
    assert!(matches!(
        second,
        SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(2),
            ..
        })
    ));
}

fn rocks_storage(schema: &JazzSchema) -> RocksDbStorage {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.keep();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    RocksDbStorage::open(&path, &refs).unwrap()
}

fn open_db(node: u8, author: AuthorId, schema: &JazzSchema) -> Db<RocksDbStorage> {
    let storage = rocks_storage(schema);
    block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node as u64))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap()
}

fn joined_issue_query() -> Query {
    Query::from("issues").join_via("issue_tags", "issue", [eq(col("tag"), lit("prepared"))])
}

fn seed_issue_project(db: &Db<RocksDbStorage>, author: AuthorId) {
    db.seed_settled_mergeable_for_bootstrap(
        "projects",
        row(10),
        author,
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issues",
        row(1),
        author,
        issue_cells("Platform", "open", author, row(10), 5, &["api"], None),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issue_tags",
        row(20),
        author,
        BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(row(1).0)),
            ("tag".to_owned(), Value::String("prepared".to_owned())),
        ]),
    )
    .unwrap();
}

#[test]
fn prepared_current_write_query_installs_and_reads_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    assert!(prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(prepared.has_plan_for_tier(DurabilityTier::Global));
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "stored prepared plans should be used without replanning"
    );
}

#[test]
fn subscribe_uses_prepared_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa2, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let mut subscription = block_on(db.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
        },
    ))
    .unwrap();

    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_event()).unwrap())),
        vec![row(1)]
    );
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "initial subscribe read should consume the stored prepared plan"
    );
}

#[test]
fn simple_prepared_current_write_query_uses_lowered_plan() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa3, author, &schema);
    db.insert_with_id("todos", row(1), cells("simple", false, author))
        .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Global));

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "simple prepared current reads should stay on the direct lowered path without installing a shared plan"
    );
}

#[test]
fn filtered_root_prepared_query_still_reads_without_preinstalled_plan() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa4, author, &schema);
    db.insert_with_id("todos", row(1), cells("wanted", false, author))
        .unwrap();

    let prepared = db
        .prepare_query(&Query::from("todos").filter(eq(col("title"), lit("wanted"))))
        .unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert_eq!(
        db.read(&prepared)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row(1)]
    );
}

struct CoreDb {
    server: Node<RocksDbStorage>,
    schema: JazzSchema,
    author: AuthorId,
    next_now_ms: Cell<u64>,
    id_source: RefCell<SeededRowIdSource>,
}

fn open_core(node_byte: u8, author: AuthorId, schema: &JazzSchema) -> CoreDb {
    let storage = rocks_storage(schema);
    let node = NodeState::new_history_complete(
        NodeUuid::from_bytes([node_byte; 16]),
        schema.clone(),
        storage,
    )
    .unwrap();
    CoreDb {
        server: Node::new(node),
        schema: schema.clone(),
        author,
        next_now_ms: Cell::new(1),
        id_source: RefCell::new(SeededRowIdSource::new(node_byte as u64)),
    }
}

impl CoreDb {
    fn node(&self) -> Rc<RefCell<NodeState<RocksDbStorage>>> {
        self.server.node()
    }

    fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    fn read(&self, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.server
            .node()
            .borrow_mut()
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .map_err(Into::into)
    }

    fn one(&self, query: &Query) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(query)?.into_iter().next())
    }

    fn at(&self, position: GlobalSeq, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        self.server
            .node()
            .borrow_mut()
            .at(position)
            .read(&shape, &binding)
            .map_err(Into::into)
    }

    fn insert(&self, table: &str, cells: RowCells) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)
    }

    fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let node = self.server.node();
        let tx_id = node.borrow_mut().commit_mergeable(
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(self.author)
                .cells(cells),
        )?;
        node.borrow_mut().finalize_local_mergeable_commit(tx_id)?;
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    fn insert_attributed(
        &self,
        made_by: AuthorId,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        let node = self.server.node();
        let tx_id = node.borrow_mut().commit_mergeable(
            MergeableCommit::new(table, row, self.next_now_ms())
                .made_by(made_by)
                .permission_subject(self.author)
                .cells(cells),
        )?;
        node.borrow_mut().finalize_local_mergeable_commit(tx_id)?;
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
        })
    }

    fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let table_schema = self
            .schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))?;
        let mut cells = BTreeMap::new();
        if let Some(existing) = self
            .read(&Query::from(table))?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row)
        {
            for column in &table_schema.columns {
                if let Some(value) = existing.cell(&table_schema, &column.name) {
                    cells.insert(column.name.clone(), value);
                }
            }
        }
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server.accept_subscriber(transport, identity)
    }

    fn accept_subscriber_with_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        trust: CommitUnitTrust,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_trust(transport, identity, trust)
    }

    fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        claims: BTreeMap<String, Value>,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_claims(transport, identity, claims)
    }

    fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorId,
        cursor: ResumeCursor,
    ) -> Rc<RefCell<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    fn tick(&self) -> Result<(), Error> {
        self.server.tick().map(|_| ())
    }

    fn exclusive_tx(&self) -> Result<CoreExclusiveTx<'_>, Error> {
        let tx_id = self.server.node().borrow_mut().open_exclusive()?;
        Ok(CoreExclusiveTx {
            core: self,
            tx_id,
            has_reads: Cell::new(false),
        })
    }

    fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        self.server
            .node()
            .borrow_mut()
            .apply_sync_message(SyncMessage::PublishSchema {
                author: self.author,
                schema: Box::new(schema),
            })
            .map_err(Into::into)
    }

    fn publish_lens(&self, lens: MigrationLens) -> Result<Vec<SyncMessage>, Error> {
        self.server
            .node()
            .borrow_mut()
            .apply_sync_message(SyncMessage::PublishLens {
                author: self.author,
                lens,
            })
            .map_err(Into::into)
    }

    fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        self.server
            .node()
            .borrow_mut()
            .apply_sync_message(SyncMessage::SetCurrentWriteSchema {
                author: self.author,
                pointer,
            })
            .map_err(Into::into)
    }
}

struct CoreExclusiveTx<'a> {
    core: &'a CoreDb,
    tx_id: OpenTxId,
    has_reads: Cell<bool>,
}

impl CoreExclusiveTx<'_> {
    fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.has_reads.set(true);
        self.core
            .server
            .node()
            .borrow_mut()
            .tx_read(self.tx_id, table, row)
            .map_err(Into::into)
    }

    fn insert_with_id(&self, table: &str, row: RowUuid, cells: RowCells) -> Result<(), Error> {
        self.core
            .server
            .node()
            .borrow_mut()
            .tx_write(self.tx_id, table, row, cells, None)
            .map_err(Into::into)
    }

    fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    fn commit(self) -> Result<TxId, Error> {
        let node = self.core.server.node();
        if self.has_reads.get() && node.borrow().open_exclusive_snapshot_moved(self.tx_id)? {
            node.borrow_mut().abandon_tx(self.tx_id)?;
            return Err(write_rejected(RejectionReason::ExclusiveConflict));
        }
        let (tx_id, unit) = node.borrow_mut().commit_exclusive(
            self.tx_id,
            self.core.author,
            self.core.next_now_ms(),
        )?;
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            return Err(Error::new(
                ErrorCode::Protocol,
                "commit_exclusive must yield a CommitUnit",
            ));
        };
        let fate = node
            .borrow_mut()
            .finalize_local_exclusive_commit(tx, versions)?;
        if let Fate::Rejected(reason) = fate {
            return Err(write_rejected(reason));
        }
        Ok(tx_id)
    }
}

/// Commit a row on an authority node and confirm it reached Global, so the
/// serving path ships it.
fn seed(db: &CoreDb, table: &str, cells: RowCells) -> RowUuid {
    let write = db.insert(table, cells).unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();
    write.row_uuid()
}

#[test]
fn db_at_reads_historical_cut_and_partial_requires_server() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let partial = open_db(0xc1, author, &schema);
    let todo = row(0x42);

    core.insert_with_id("todos", todo, cells("draft", false, author))
        .unwrap();
    core.update(
        "todos",
        todo,
        BTreeMap::from([("title".to_owned(), Value::String("final".to_owned()))]),
    )
    .unwrap();

    let table = &schema.tables[0];
    let at_first = core.at(GlobalSeq(1), &Query::from("todos")).unwrap();
    assert_eq!(at_first.len(), 1);
    assert_eq!(
        at_first[0].cell(table, "title"),
        Some(Value::String("draft".to_owned()))
    );
    let at_second = core.at(GlobalSeq(2), &Query::from("todos")).unwrap();
    assert_eq!(
        at_second[0].cell(table, "title"),
        Some(Value::String("final".to_owned()))
    );

    let partial_todos = partial.prepare_query(&Query::from("todos")).unwrap();
    let err = partial.at(GlobalSeq(1), &partial_todos).unwrap_err();
    assert_eq!(err.code, ErrorCode::HistoricalReadRequiresServer);
    assert_eq!(err.message, "historical read requires server evaluation");
}

#[test]
fn db_catalogue_facade_publishes_schema_lens_and_current_write_schema() {
    let base = owner_write_schema();
    let evolved = evolved_owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &base);
    let client = open_db(0xc1, owner, &base);
    let schema_version = SchemaVersion::new(evolved.clone());

    let schema_ack = core.publish_schema(schema_version.clone()).unwrap();
    assert!(matches!(
        schema_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)] if ack.schema == Some(schema_version.id) && ack.applied
    ));

    let lens = MigrationLens::new(
        base.version_id(),
        schema_version.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    let lens_ack = core.publish_lens(lens.clone()).unwrap();
    assert!(matches!(
        lens_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)] if ack.lens == Some(lens.id) && ack.applied
    ));

    let pointer = CurrentWriteSchema {
        revision: 2,
        schema: schema_version.id,
    };
    let pointer_ack = core.set_current_write_schema(pointer).unwrap();
    assert!(matches!(
        pointer_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)] if ack.revision == Some(2) && ack.schema == Some(schema_version.id) && ack.applied
    ));

    let row = seed(&core, "todos", cells("under evolved schema", false, owner));
    let rows = core.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);

    let unauthorized = client.publish_schema(schema_version).unwrap_err();
    assert_eq!(unauthorized.code, ErrorCode::Protocol);
    assert!(
        unauthorized
            .message
            .contains("catalogue updates require a serving Node")
    );
}

#[test]
fn core_db_self_finalizes_own_writes_to_global() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);

    let write = core
        .insert("todos", cells("authority write", false, owner))
        .unwrap();
    // No upstream, no connection: a Core Db is the authority, so its own
    // write is immediately Accepted/Global.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(core.read(&Query::from("todos")).unwrap().len(), 1);
}

#[test]
fn db_sync_surface_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    // Wire the two Dbs together and subscribe on the client.
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_event()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    // Drive: client announces the shape -> server serves -> client applies.
    client.tick().unwrap(); // RegisterShape + BindingDelta upstream
    server.tick().unwrap(); // ViewUpdate downstream
    client.tick().unwrap(); // apply, push the subscription event

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    // A later server write propagates incrementally on the next round trip.
    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn subscription_emits_when_remote_coverage_settles_without_row_changes() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_event()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let settled = block_on(subscription.next_event()).unwrap();
    assert!(event_settled(&settled));
    let (added, updated, removed) = delta_rows(settled);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn one_shot_propagated_query_records_empty_remote_coverage() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    assert!(!client.query_is_covered(&prepared));

    client.propagate_query_with_opts(&prepared, global_subscribe_opts());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_is_covered(&prepared));
    assert!(prepared_read(&client, &query).is_empty());
}

#[test]
fn one_shot_propagated_query_rehydrates_already_covered_binding() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    client.propagate_query_with_opts(&prepared, global_subscribe_opts());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_is_covered(&prepared));
    assert_eq!(prepared_read(&client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    client.propagate_query_with_opts(&prepared, global_subscribe_opts());
    assert!(!client.query_is_covered(&prepared));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_is_covered(&prepared));
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn one_shot_edge_query_rehydrates_already_covered_binding() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    client.propagate_query_with_opts(&prepared, edge_subscribe_opts());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_is_covered(&prepared));
    assert_eq!(prepared_read(&client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    client.propagate_query_with_opts(&prepared, edge_subscribe_opts());
    assert!(!client.query_is_covered(&prepared));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_is_covered(&prepared));
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn one_shot_edge_query_rehydrates_claim_bound_already_covered_binding() {
    let schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("joinCode", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("chats").filter(any_of([])).policy_branch(
            crate::query::PolicyBranch::from_query(
                Query::from("chats").filter(eq(col("joinCode"), crate::query::claim("join_code"))),
            ),
        ),
    ))
    .with_write_policy(Policy::public())]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let reader = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, reader, &schema);
    let join_code = "invite-code-123";
    client.set_identity_claims(
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let first = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber_with_claims(
        server_transport,
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let query = Query::from("chats");
    let prepared = prepared(&client, &query);
    client.propagate_query_with_opts(&prepared, edge_subscribe_opts());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_is_covered(&prepared));
    assert_eq!(
        row_ids(&prepared_all(&client, &query, edge_subscribe_opts())),
        vec![first]
    );

    let second = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );
    client.propagate_query_with_opts(&prepared, edge_subscribe_opts());
    assert!(!client.query_is_covered(&prepared));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_is_covered(&prepared));
    assert_eq!(
        row_ids(&prepared_all(&client, &query, edge_subscribe_opts())),
        vec![first, second]
    );
}

#[test]
fn edge_subscription_with_claim_bound_policy_emits_later_matching_server_write() {
    let schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("joinCode", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("chats").filter(any_of([])).policy_branch(
            crate::query::PolicyBranch::from_query(
                Query::from("chats").filter(eq(col("joinCode"), crate::query::claim("join_code"))),
            ),
        ),
    ))
    .with_write_policy(Policy::public())]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let reader = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, reader, &schema);
    let join_code = "invite-code-123";
    let claims = BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]);
    client.set_identity_claims(reader, claims.clone());

    let first = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber_with_claims(server_transport, reader, claims);

    let query = Query::from("chats");
    let mut subscription = prepared_subscribe(&client, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        row_ids(&delta_rows(block_on(subscription.next_event()).unwrap()).0),
        vec![first]
    );

    let second = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );
    server.tick().unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(row_ids(&added), vec![second]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(
        row_ids(&prepared_all(&client, &query, edge_subscribe_opts())),
        vec![first, second]
    );
}

#[test]
fn write_state_waiter_resolves_on_remote_fate_update() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let write = client
        .insert("todos", cells("wait for fate", false, owner))
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    assert_eq!(
        client.write_state(tx_id).unwrap().durability,
        DurabilityTier::Local
    );

    let changed = client.next_write_state_change(tx_id);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(changed);

    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
}

#[test]
fn db_sync_surface_round_trips_blob_large_value_to_reader() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let writer_author = AuthorId::from_bytes([0xc1; 16]);
    let reader_author = AuthorId::from_bytes([0xc2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let writer = open_db(0xc1, writer_author, &schema);
    let reader = open_db(0xc2, reader_author, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = writer.connect_upstream(writer_transport);
    let _writer_subscriber = server.accept_subscriber(server_writer_transport, writer_author);
    let payload = b"synced blob bytes".to_vec();
    writer
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes(payload.clone()))]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = reader.connect_upstream(reader_transport);
    let _reader_subscriber = server.accept_subscriber(server_reader_transport, reader_author);
    let query = Query::from("files");
    let mut subscription = prepared_subscribe(&reader, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    reader.tick().unwrap();
    server.tick().unwrap();
    reader.tick().unwrap();

    let table = &schema.tables[0];
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(
        prepared_read(&reader, &query)[0].cell(table, "data"),
        Some(Value::Bytes(payload))
    );
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_private_table_query() {
    let schema = owner_id_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let writer = open_db(0xa1, alice, &schema);
    let reader = open_db(0xb2, bob, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = writer.connect_upstream(writer_transport);
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                ("owner_id".to_owned(), Value::String(alice.0.to_string())),
            ]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = reader.connect_upstream(reader_transport);
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([("user_id".to_owned(), Value::String(bob.0.to_string()))]),
    );
    let query = Query::from("messages");
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    reader.tick().unwrap();
    server.tick().unwrap();
    reader.tick().unwrap();

    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_after_runtime_schema_publish() {
    let public_schema = owner_id_public_schema();
    let permission_schema = owner_id_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &public_schema);
    let writer = open_db(0xa1, alice, &permission_schema);
    let reader = open_db(0xb2, bob, &permission_schema);

    let schema_version = SchemaVersion::new(permission_schema.clone());
    let schema_id = schema_version.id;
    let acks = server.publish_schema(schema_version).unwrap();
    assert!(acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));
    let current_acks = server
        .server
        .node()
        .borrow_mut()
        .apply_sync_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: schema_id,
            },
        })
        .unwrap();
    assert!(current_acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = writer.connect_upstream(writer_transport);
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                ("owner_id".to_owned(), Value::String(alice.0.to_string())),
            ]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = reader.connect_upstream(reader_transport);
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([("user_id".to_owned(), Value::String(bob.0.to_string()))]),
    );
    let query = Query::from("messages");
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    reader.tick().unwrap();
    server.tick().unwrap();
    reader.tick().unwrap();

    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn detached_subscriber_is_not_served_on_server_tick() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());
    client.tick().unwrap();

    assert!(server.server.detach_connection(&subscriber));
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(prepared_read(&client, &query).is_empty());
}

#[test]
fn byte_wire_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_bytes, server_bytes) = byte_duplex_raw();
    let server_inbound = Rc::clone(&server_bytes.inbound);
    let _upstream = client.connect_upstream(Box::new(WireTransportAdapter::current(client_bytes)));
    let _subscriber = server.accept_subscriber(
        Box::new(WireTransportAdapter::current(server_bytes)),
        client_author,
    );

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    {
        let queued = server_inbound.borrow();
        let first = queued.front().expect("register shape frame");
        let second = queued.get(1).expect("binding delta frame");
        let first = match decode_frame(first).unwrap() {
            WireFrame::Message(envelope) => decode_sync_message(&envelope.payload).unwrap(),
            other => panic!("expected message frame, got {other:?}"),
        };
        let second = match decode_frame(second).unwrap() {
            WireFrame::Message(envelope) => decode_sync_message(&envelope.payload).unwrap(),
            other => panic!("expected message frame, got {other:?}"),
        };
        let SyncMessage::RegisterShape { shape_id, .. } = first else {
            panic!("expected RegisterShape, got {first:?}");
        };
        let SyncMessage::BindingDelta(delta) = second else {
            panic!("expected BindingDelta, got {second:?}");
        };
        assert_eq!(delta.shape_id, shape_id);
    }
    server.tick().unwrap();
    client.tick().unwrap();

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    // The subscriber registers the whole-table query shape; explicit
    // current-row serving then sends the facade-level initial snapshot.
    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = duplex();
    let _resumed_upstream = client.connect_upstream(client_transport);
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert_ne!(resume_bytes, full_bytes);
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn byte_wire_subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 1);
    let _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 2);
    let _resumed_upstream = client.connect_upstream(client_transport);
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "byte-wire resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert_ne!(resume_bytes, full_bytes);
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn connect_upstream_announces_existing_subscriptions_on_first_tick() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _upstream = client.connect_upstream(client_transport);

    client.tick().unwrap();
    let first = upstream_transport.try_recv().unwrap();
    let second = upstream_transport.try_recv().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    let SyncMessage::RegisterShape { shape_id, .. } = first else {
        panic!("expected existing subscription shape to be registered upstream first");
    };
    let SyncMessage::BindingDelta(delta) = second else {
        panic!("expected existing subscription binding to be announced upstream second");
    };
    assert_eq!(delta.shape_id, shape_id);
    assert_eq!(delta.adds.len(), 1);
    assert!(delta.removes.is_empty());
}

#[test]
fn upload_is_not_marked_sent_after_one_shot_backpressure_and_retries() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let outbound = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let transport = BackpressureOnceTransport {
        outbound: Rc::clone(&outbound),
        failed: false,
    };
    let _upstream = client.connect_upstream(Box::new(transport));

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("retry", false, client_author)),
        )
        .unwrap();
    client
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    let error = client.tick().unwrap_err();
    assert_eq!(error.code, ErrorCode::Backpressure);
    assert!(outbound.borrow().is_empty());

    client.tick().unwrap();
    let sent = outbound.borrow_mut().pop_front().unwrap();
    let SyncMessage::CommitUnit { tx, .. } = sent else {
        panic!("expected retried commit upload");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert!(outbound.borrow_mut().pop_front().is_none());
}

#[test]
fn blob_commit_upload_sends_content_extents_before_commit_unit() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);

    let write = client
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes(b"blob bytes".to_vec()))]),
        )
        .unwrap();
    client.tick().unwrap();

    let first = upstream_transport.try_recv().unwrap();
    let second = upstream_transport.try_recv().unwrap();
    assert!(matches!(first, SyncMessage::ContentExtents { .. }));
    let SyncMessage::CommitUnit { tx, .. } = second else {
        panic!("expected commit unit after content extents");
    };
    assert_eq!(tx.tx_id, write.mergeable_tx_id());
}

#[test]
fn detach_connection_removes_connection_from_db_ticks() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let upstream = client.connect_upstream(client_transport);

    assert!(client.detach_connection(&upstream));
    assert!(!client.detach_connection(&upstream));

    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn accepted_subscriber_is_served_under_subscriber_author_identity() {
    let schema = owner_read_schema();
    let subscriber_author = AuthorId::from_bytes([0xc1; 16]);
    let server_author = AuthorId::from_bytes([0x5e; 16]);
    let other_author = AuthorId::from_bytes([0xd1; 16]);
    let server = open_core(0x5e, server_author, &schema);
    let client = open_db(0xc1, subscriber_author, &schema);

    let visible = seed(
        &server,
        "todos",
        cells("for subscriber", false, subscriber_author),
    );
    seed(&server, "todos", cells("for server", false, server_author));
    seed(
        &server,
        "todos",
        cells("for someone else", false, other_author),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, subscriber_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_event()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let (rows, updated, removed) = delta_rows(block_on(subscription.next_event()).unwrap());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&rows), vec![visible]);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("for subscriber".to_owned()))
    );
}

#[test]
fn db_sync_surface_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    // A local client write is Local and queued for upload.
    let write = client
        .insert("todos", cells("from client", false, author))
        .unwrap();
    let row = write.row_uuid();

    // Drive: client uploads the commit unit -> server (authority) accepts to
    // Global and sends the fate back -> client applies the fate.
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    // The client's own write reached Global once the authority fate landed.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    // The authority received and applied the uploaded row.
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn byte_wire_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = byte_duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert("todos", cells("from client", false, author))
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn db_sync_surface_uploads_client_exclusive_commit_for_global_fate() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe1);
    let exclusive = client.exclusive_tx().unwrap();
    exclusive
        .insert_with_id("todos", row, cells("exclusive", false, author))
        .unwrap();
    let tx_id = exclusive.commit().unwrap();

    assert_eq!(
        client.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        client.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn db_sync_surface_returns_exclusive_conflict_fate_to_client() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe2);
    let first = client.exclusive_tx().unwrap();
    let second = client.exclusive_tx().unwrap();
    first
        .insert_with_id("todos", row, cells("first", false, author))
        .unwrap();
    second
        .insert_with_id("todos", row, cells("second", false, author))
        .unwrap();
    let first_tx = first.commit().unwrap();
    let second_tx = second.commit().unwrap();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        client.write_state(first_tx).unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    assert_eq!(
        client.write_state(second_tx).unwrap(),
        WriteState {
            fate: Fate::Rejected(RejectionReason::ExclusiveConflict),
            durability: DurabilityTier::Local,
        }
    );

    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    let table = &schema.tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

#[test]
fn write_fate_and_durability_are_queryable_through_facade() {
    let schema = schema();
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert("todos", cells("facade state", false, author))
        .unwrap();
    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );
    assert_eq!(
        client.write_state(write.mergeable_tx_id()).unwrap(),
        write.write_state().unwrap()
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
}

#[test]
fn session_upload_rejects_forged_made_by_without_ingesting_rows() {
    let schema = owner_write_schema();
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let forged_author = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(forged_author)
                .cells(cells("forged", false, session_author)),
        )
        .unwrap();
    client
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let handle = WriteHandle {
        node: Rc::downgrade(&client.node.node),
        row_uuid: row(0xf1),
        tx_id,
        local_tier: DurabilityTier::Local,
    };
    let err = block_on(handle.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn session_upload_uses_connection_identity_for_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert("todos", cells("honest", false, session_author))
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
}

#[test]
fn session_delete_uses_current_row_for_owner_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let other_author = AuthorId::from_bytes([0xd1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert("todos", cells("owned", false, session_author))
        .unwrap();
    let row = write.row_uuid();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();

    let bad_delete = match client.delete_for_identity(other_author, "todos", row) {
        Ok(_) => panic!("foreign owner delete should be rejected locally"),
        Err(error) => error,
    };
    assert_eq!(bad_delete.code, ErrorCode::WriteRejected);

    let delete = client
        .delete_for_identity(session_author, "todos", row)
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn trusted_backend_upload_uses_backend_policy_and_stores_user_made_by() {
    let schema = owner_write_schema();
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let tx_id = backend
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xf2), backend.next_now_ms())
                .made_by(attributed_user)
                .permission_subject(backend_author)
                .cells(cells("attributed", false, backend_author)),
        )
        .unwrap();
    backend
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    let SyncMessage::CommitUnit { tx, .. } =
        server.node().borrow_mut().commit_unit_for(tx_id).unwrap()
    else {
        panic!("expected stored commit unit");
    };
    assert_eq!(tx.made_by, attributed_user);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0xf2));
}

#[test]
fn trusted_backend_delete_uses_permission_subject_parent_for_write_policy() {
    let schema = owner_write_schema();
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let insert = backend
        .insert_with_id_for_identity(
            attributed_user,
            "todos",
            row(0xf3),
            cells("attributed", false, attributed_user),
        )
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();
    block_on(insert.wait(DurabilityTier::Global)).unwrap();

    let delete = backend
        .delete_for_identity(attributed_user, "todos", row(0xf3))
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn db_large_text_values_round_trip_across_edit_chain() {
    let schema =
        JazzSchema::new([
            TableSchema::new("notes", [crate::schema::ColumnSchema::text("body")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x33; 16]),
            author: AuthorId::from_bytes([0x44; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x33))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];

    let write = db
        .insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::Bytes(b"hello".to_vec()))]),
        )
        .unwrap();
    let note = write.row_uuid();
    assert_eq!(
        prepared_one(&db, &Query::from("notes"))
            .unwrap()
            .cell(table, "body"),
        Some(Value::Bytes(b"hello".to_vec()))
    );

    for value in [
        "hello world".as_bytes().to_vec(),
        "hello brave world".as_bytes().to_vec(),
        "brave new world".as_bytes().to_vec(),
        "brave new world - ecriture 日本".as_bytes().to_vec(),
    ] {
        db.update(
            "notes",
            note,
            BTreeMap::from([("body".to_owned(), Value::Bytes(value.clone()))]),
        )
        .unwrap();
        assert_eq!(
            prepared_one(&db, &Query::from("notes"))
                .unwrap()
                .cell(table, "body"),
            Some(Value::Bytes(value))
        );
    }
}

#[test]
fn db_large_blob_values_round_trip_binary_from_empty_parent() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x55; 16]),
            author: AuthorId::from_bytes([0x66; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x55))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let first = vec![0, 1, 2, 3, 255, 0, 128];
    let second = vec![0, 1, 9, 3, 255, 64, 128, 200];

    let write = db
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes(first.clone()))]),
        )
        .unwrap();
    let file = write.row_uuid();
    assert_eq!(
        prepared_one(&db, &Query::from("files"))
            .unwrap()
            .cell(table, "data"),
        Some(Value::Bytes(first))
    );

    db.update(
        "files",
        file,
        BTreeMap::from([("data".to_owned(), Value::Bytes(second.clone()))]),
    )
    .unwrap();
    assert_eq!(
        prepared_one(&db, &Query::from("files"))
            .unwrap()
            .cell(table, "data"),
        Some(Value::Bytes(second))
    );
}

#[test]
fn db_text_edit_ops_materialize_expected_value() {
    let schema =
        JazzSchema::new([
            TableSchema::new("notes", [crate::schema::ColumnSchema::text("body")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x77; 16]),
            author: AuthorId::from_bytes([0x88; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x77))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let write = db
        .insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::Bytes(b"hello world".to_vec()))]),
        )
        .unwrap();

    db.edit_text(
        "notes",
        write.row_uuid(),
        "body",
        TextEdit::new().delete(5, 6).insert(5, b", ops".to_vec()),
    )
    .unwrap();

    assert_eq!(
        prepared_one(&db, &Query::from("notes"))
            .unwrap()
            .cell(table, "body"),
        Some(Value::Bytes(b"hello, ops".to_vec()))
    );
}

#[test]
fn db_text_dump_and_edit_paths_interleave() {
    let schema =
        JazzSchema::new([
            TableSchema::new("notes", [crate::schema::ColumnSchema::text("body")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x78; 16]),
            author: AuthorId::from_bytes([0x89; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x78))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let write = db
        .insert(
            "notes",
            BTreeMap::from([("body".to_owned(), Value::Bytes(b"start".to_vec()))]),
        )
        .unwrap();
    let row = write.row_uuid();

    db.update(
        "notes",
        row,
        BTreeMap::from([("body".to_owned(), Value::Bytes(b"start middle".to_vec()))]),
    )
    .unwrap();
    db.edit_text(
        "notes",
        row,
        "body",
        TextEdit::new().insert(12, b" end".to_vec()),
    )
    .unwrap();
    db.update(
        "notes",
        row,
        BTreeMap::from([(
            "body".to_owned(),
            Value::Bytes(b"BEGIN middle end".to_vec()),
        )]),
    )
    .unwrap();
    db.edit_text("notes", row, "body", TextEdit::new().delete(5, 7))
        .unwrap();

    assert_eq!(
        prepared_one(&db, &Query::from("notes"))
            .unwrap()
            .cell(table, "body"),
        Some(Value::Bytes(b"BEGIN end".to_vec()))
    );
}

#[test]
fn db_blob_edit_ops_handle_binary_and_multibyte_bytes() {
    let schema =
        JazzSchema::new([
            TableSchema::new("files", [crate::schema::ColumnSchema::blob("data")])
                .with_read_policy(Policy::public())
                .with_write_policy(Policy::public()),
        ]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x79; 16]),
            author: AuthorId::from_bytes([0x8a; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x79))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();
    let table = &schema.tables[0];
    let write = db
        .insert(
            "files",
            BTreeMap::from([("data".to_owned(), Value::Bytes("aé日z".as_bytes().to_vec()))]),
        )
        .unwrap();

    db.edit_text(
        "files",
        write.row_uuid(),
        "data",
        TextEdit::new()
            .delete(1, "é".len())
            .insert(6, vec![0, 255])
            .insert(7, "✓".as_bytes().to_vec()),
    )
    .unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(b"a");
    expected.extend_from_slice("日".as_bytes());
    expected.extend_from_slice(&[0, 255]);
    expected.extend_from_slice(b"z");
    expected.extend_from_slice("✓".as_bytes());
    assert_eq!(
        prepared_one(&db, &Query::from("files"))
            .unwrap()
            .cell(table, "data"),
        Some(Value::Bytes(expected))
    );
}

#[test]
fn db_query_builder_expresses_s1_shaped_filters_and_include_modes() {
    let schema = issue_schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x22; 16]),
            author: alice,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x22))),
        large_value_checkpoint_op_interval: crate::node::LARGE_VALUE_CHECKPOINT_OP_INTERVAL,
    }))
    .unwrap();

    db.insert_with_id(
        "projects",
        row(10),
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(1),
        issue_cells(
            "ship api query builder",
            "open",
            alice,
            row(10),
            5,
            &["api", "platform"],
            None,
        ),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(2),
        issue_cells("closed work", "done", alice, row(10), 3, &["api"], Some(99)),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(3),
        issue_cells("someone else", "open", bob, row(10), 8, &["platform"], None),
    )
    .unwrap();
    db.insert_with_id(
        "issues",
        row(4),
        issue_cells("missing project", "open", alice, row(99), 6, &["api"], None),
    )
    .unwrap();

    let s1_query = db
        .table("issues")
        .filter(all_of([
            eq(col("assignee"), lit(alice.0)),
            in_list(col("state"), [lit("open"), lit("blocked")]),
            not(ne(col("state"), lit("open"))),
            any_of([
                contains(col("title"), lit("api")),
                contains(col("labels"), lit("api")),
            ]),
            gt(col("priority"), lit(4_u64)),
            lte(col("priority"), lit(6_u64)),
            is_null(col("snoozed_until")),
        ]))
        .include("project")
        .select([
            "title", "state", "assignee", "project", "priority", "labels",
        ])
        .limit(10)
        .offset(0);

    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "issues")
        .unwrap();
    let read_rows = prepared_read(&db, &s1_query);
    assert_eq!(row_ids(&read_rows), vec![row(1)]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("ship api query builder".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "snoozed_until"), None);
    let all_rows = prepared_all(&db, &s1_query, ReadOpts::default());
    assert_eq!(row_ids(&all_rows), vec![row(1)]);

    let holes_query = db
        .table("issues")
        .filter(eq(col("assignee"), lit(alice.0)))
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes));
    assert_eq!(
        row_ids(&prepared_read(&db, &holes_query)),
        vec![row(1), row(4)]
    );

    let require_query = holes_query.clone().include_with(
        Include::new("project")
            .join_mode(JoinMode::Holes)
            .require_includes(),
    );
    assert_eq!(row_ids(&prepared_read(&db, &require_query)), vec![row(1)]);

    let paged = db
        .table("issues")
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes))
        .offset(1)
        .limit(1);
    assert_eq!(row_ids(&prepared_read(&db, &paged)), vec![row(3)]);
}

fn row_ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    rows.iter().map(CurrentRow::row_uuid).collect()
}
