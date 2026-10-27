use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{
    BranchSelector, BranchViewBase, ReadViewSourceSpec, ReadViewSpec, SnapshotRef,
};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};

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

fn captured_branch_snapshot(db: &Db) -> SnapshotRef {
    db.capture_snapshot().unwrap()
}

/// Alice captures her work before a later edit. Her frozen
/// draft keeps Original; the live draft sees Changed, and copy-on-write only
/// changes the draft. No authority or server acceptance is required.
#[test]
fn captured_snapshot_fences_later_writes_and_preserves_draft_overrides() {
    let (db, _) = open_db();
    let base = selector(0xe1);
    let head = selector(0xe2);
    let row = db
        .insert(
            "todos",
            BTreeMap::from([("title".into(), Value::String("Original".into()))]),
            jazz::db::InsertOptions {
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap()
        .row_uuid();
    let snapshot = captured_branch_snapshot(&db);
    db.update(
        "todos",
        row,
        BTreeMap::from([("title".into(), Value::String("Changed".into()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: base.clone(),
                base: None,
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        snapshot.dots.is_empty(),
        "a database without an upstream has nothing pending to name"
    );
    let snapshot = SnapshotRef::from_persisted(&snapshot.to_persisted()).unwrap();
    let frozen = ReadOpts::default().branch_view(
        head.clone(),
        Some(BranchViewBase::snapshot(base.clone(), snapshot.clone())),
    );
    let live =
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::current(base.clone())));
    let query = db.prepare_query(&db.table("todos")).unwrap();
    assert_eq!(
        db.all(&query, frozen.clone()).unwrap()[0].cell("title"),
        Some(Value::String("Original".into()))
    );
    assert_eq!(
        db.all(&query, live).unwrap()[0].cell("title"),
        Some(Value::String("Changed".into()))
    );
    db.update(
        "todos",
        row,
        BTreeMap::from([("title".into(), Value::String("Draft".into()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head,
                base: Some(BranchViewBase::snapshot(base.clone(), snapshot)),
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.all(&query, frozen).unwrap()[0].cell("title"),
        Some(Value::String("Draft".into()))
    );
    assert_eq!(
        db.all(&query, ReadOpts::default().branch_view(base, None))
            .unwrap()[0]
            .cell("title"),
        Some(Value::String("Changed".into()))
    );
}

/// Alice restores a captured reference with retained history after reopening
/// her persistent database. Bob's empty store must refuse the same reference.
#[test]
fn captured_snapshot_reopens_and_missing_history_is_explicit() {
    let (_, schema) = open_db();
    let dir = tempfile::tempdir().unwrap();
    let base = selector(0xe3);
    let head = selector(0xe4);
    let persisted = {
        let db = open_file_db(dir.path(), &schema);
        let row = db
            .insert(
                "todos",
                BTreeMap::from([("title".into(), Value::String("Original".into()))]),
                jazz::db::InsertOptions {
                    target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
            .row_uuid();
        let reference = captured_branch_snapshot(&db).to_persisted();
        db.update(
            "todos",
            row,
            BTreeMap::from([("title".into(), Value::String("Changed".into()))]),
            jazz::db::UpdateOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: base.clone(),
                    base: None,
                },
                ..Default::default()
            },
        )
        .unwrap();
        reference
    };
    let snapshot = SnapshotRef::from_persisted(&persisted).unwrap();
    let opts =
        ReadOpts::default().branch_view(head, Some(BranchViewBase::snapshot(base, snapshot)));
    {
        let db = open_file_db(dir.path(), &schema);
        let query = db.prepare_query(&db.table("todos")).unwrap();
        let frozen = db.all(&query, opts.clone()).unwrap();
        assert_eq!(frozen.len(), 1);
        assert_eq!(
            frozen[0].cell("title"),
            Some(Value::String("Original".into()))
        );
        assert_eq!(
            db.all(
                &query,
                ReadOpts::default().branch_view(selector(0xe3), None)
            )
            .unwrap()[0]
                .cell("title"),
            Some(Value::String("Changed".into()))
        );
    }
    let (bob, _) = open_db();
    let query = bob.prepare_query(&bob.table("todos")).unwrap();
    let error = block_on(bob.all(&query, opts)).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
    assert!(
        error
            .to_string()
            .contains("Snapshot history is unavailable"),
        "{error}"
    );
}

/// The application codec is a public compatibility contract: u64 extrema and
/// transaction dots survive both the string format and the established binary
/// serde layout. The fixture is authored independently of serde.
#[test]
fn snapshot_reference_codec_matches_pinned_bytes_and_preserves_wire_layout() {
    #[derive(serde::Serialize)]
    struct OldLayout<'a> {
        owner: NodeUuid,
        global_base: jazz::time::GlobalTime,
        local_base: jazz::time::TxTime,
        dots: &'a [jazz::tx::TxId],
    }
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/snapshot_ref_v1.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let token = case["reference"].as_str().unwrap();
        let snapshot = SnapshotRef::from_persisted(token).unwrap();
        assert_eq!(snapshot.to_persisted(), token);
        let expected: SnapshotRef = serde_json::from_value(case["fields"].clone()).unwrap();
        assert_eq!(snapshot, expected);
        let old = postcard::to_allocvec(&OldLayout {
            owner: snapshot.owner,
            global_base: snapshot.global_base,
            local_base: snapshot.local_base,
            dots: &snapshot.dots,
        })
        .unwrap();
        assert_eq!(postcard::to_allocvec(&snapshot).unwrap(), old);
        assert_eq!(postcard::from_bytes::<SnapshotRef>(&old).unwrap(), snapshot);
        assert_eq!(
            serde_json::from_value::<SnapshotRef>(serde_json::Value::String(token.to_owned()))
                .unwrap(),
            snapshot
        );
    }
    for invalid in fixture["invalid"].as_array().unwrap() {
        assert!(SnapshotRef::from_persisted(invalid.as_str().unwrap()).is_err());
    }
}

/// Alice restores a reference containing unsorted duplicate transaction dots.
/// Rust re-encoding canonicalises the string without changing its frontier.
#[test]
fn snapshot_reference_codec_canonicalises_unsorted_duplicate_dots() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/snapshot_ref_v1.json")).unwrap();
    for case in fixture["noncanonical"].as_array().unwrap() {
        let mut restored =
            SnapshotRef::from_persisted(case["reference"].as_str().unwrap()).unwrap();
        let canonical = case["canonical"].as_str().unwrap();
        let expected = SnapshotRef::from_persisted(canonical).unwrap();
        assert_ne!(restored.dots, expected.dots);
        assert_eq!(restored.to_persisted(), canonical);
        restored.dots.sort_unstable();
        restored.dots.dedup();
        assert_eq!(restored, expected);
        assert_eq!(expected.to_persisted(), canonical);
    }
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
                value: PolicyValue::SessionRef(vec![
                    "user".to_owned(),
                    "identity".to_owned(),
                    "subject".to_owned(),
                ]),
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

fn open_db() -> (Db, JazzSchema) {
    open_db_as(AuthorSubject::for_test_bytes([0x63; 16]))
}

fn open_db_as(author: AuthorSubject) -> (Db, JazzSchema) {
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
    let db = block_on(Db::open(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x62; 16]),
                author,
            },
        )
        .with_id_source(SeededRowIdSource::new(1)),
    ))
    .unwrap();
    (db, schema)
}

fn open_history_complete_db_with_schema(schema: JazzSchema) -> Db {
    block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x90; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(9)),
    ))
    .unwrap()
}

fn open_file_db(path: &std::path::Path, schema: &JazzSchema) -> Db {
    block_on(Db::open_at(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xa4; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(10)),
        &path.join("jazz.sqlite"),
        jazz::db::FileOptions::default(),
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
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("needle".to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.update(
        "todos",
        overridden,
        BTreeMap::from([("title".to_owned(), Value::String("hidden".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
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
fn branch_view_upsert_merges_head_copies_inherited_and_inserts_absent_rows() {
    let (db, _) = open_db();
    let base = selector(0xb1);
    let head = selector(0xb2);
    let head_row = RowUuid::from_bytes([0xb3; 16]);
    let inherited_row = RowUuid::from_bytes([0xb4; 16]);
    let absent_row = RowUuid::from_bytes([0xb5; 16]);
    for (row, title, branch) in [
        (head_row, "head before", head.clone()),
        (inherited_row, "base before", base.clone()),
    ] {
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(branch),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let target = jazz::db::WriteTarget::BranchView {
        head: head.clone(),
        base: Some(BranchViewBase::Current(base.clone())),
    };
    for (row, title) in [
        (head_row, "head after"),
        (inherited_row, "inherited after"),
        (absent_row, "absent after"),
    ] {
        db.upsert(
            "todos",
            row,
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            jazz::db::UpsertOptions {
                target: target.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let query = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(
        &query,
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone()))),
    ))
    .unwrap();
    let titles = rows
        .iter()
        .map(|row| (row.row_uuid(), row.cell("title").unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        titles,
        BTreeMap::from([
            (head_row, Value::String("head after".to_owned())),
            (inherited_row, Value::String("inherited after".to_owned())),
            (absent_row, Value::String("absent after".to_owned())),
        ])
    );

    let base_rows = block_on(db.all(&query, ReadOpts::default().branch_view(base, None))).unwrap();
    assert_eq!(base_rows.len(), 1);
    assert_eq!(base_rows[0].row_uuid(), inherited_row);
    assert_eq!(
        base_rows[0].cell("title"),
        Some(Value::String("base before".to_owned())),
        "copy-on-write must leave inherited base content unchanged"
    );
}

#[test]
fn mergeable_transaction_branch_view_upsert_handles_all_three_row_states() {
    let (db, _) = open_db();
    let base = selector(0xb6);
    let head = selector(0xb7);
    let head_row = RowUuid::from_bytes([0xb8; 16]);
    let inherited_row = RowUuid::from_bytes([0xb9; 16]);
    let absent_row = RowUuid::from_bytes([0xba; 16]);
    for (row, title, branch) in [
        (head_row, "head before", head.clone()),
        (inherited_row, "base before", base.clone()),
    ] {
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(branch),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let tx = db.mergeable_tx().unwrap();
    let target = jazz::db::WriteTarget::BranchView {
        head: head.clone(),
        base: Some(BranchViewBase::Current(base.clone())),
    };
    for (row, title) in [
        (head_row, "head transaction"),
        (inherited_row, "inherited transaction"),
        (absent_row, "absent transaction"),
    ] {
        tx.upsert(
            "todos",
            row,
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            jazz::db::UpsertOptions {
                target: target.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let query = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(
        &query,
        ReadOpts::default().branch_view(head, Some(BranchViewBase::Current(base))),
    ))
    .unwrap();
    let titles = rows
        .iter()
        .map(|row| (row.row_uuid(), row.cell("title").unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        titles,
        BTreeMap::from([
            (head_row, Value::String("head transaction".to_owned())),
            (
                inherited_row,
                Value::String("inherited transaction".to_owned())
            ),
            (absent_row, Value::String("absent transaction".to_owned())),
        ])
    );
}

/// A branch upsert later in the same mergeable transaction supersedes alice's
/// staged delete instead of committing content behind that tombstone.
///
/// alice ──delete head row──► tx overlay ──upsert row──► visible replacement
#[test]
fn branch_upsert_after_staged_delete_replaces_the_pending_tombstone() {
    let (db, _) = open_db();
    let head = selector(0xbb);
    let row = RowUuid::from_bytes([0xbc; 16]);
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("before".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(head.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    let target = jazz::db::WriteTarget::BranchView {
        head: head.clone(),
        base: None,
    };
    let tx = db.mergeable_tx().unwrap();
    tx.delete(
        "todos",
        row,
        jazz::db::DeleteOptions {
            target: target.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    tx.upsert(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("after".to_owned()))]),
        jazz::db::UpsertOptions {
            target,
            ..Default::default()
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let query = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(&query, ReadOpts::default().branch_view(head, None))).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("after".to_owned()))
    );
}

/// A session transaction can patch branch rows it inserted or upserted earlier
/// in its own overlay; policy visibility is resolved from staged content, not
/// only the committed branch view.
///
/// alice ──insert/upsert rows──► tx overlay ──upsert same rows──► merged content
#[test]
fn session_transaction_can_upsert_its_staged_branch_rows_again() {
    let (db, _) = open_db_as(AuthorSubject::SYSTEM);
    let alice = AuthorSubject::for_test_bytes([0xbd; 16]);
    let head = selector(0xbe);
    let inserted_row = RowUuid::from_bytes([0xbf; 16]);
    let upserted_row = RowUuid::from_bytes([0xc0; 16]);
    let target = jazz::db::WriteTarget::BranchView {
        head: head.clone(),
        base: None,
    };
    let tx = db.mergeable_tx_for_identity(alice).unwrap();
    tx.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("inserted".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(inserted_row),
            target: jazz::db::ExactWriteTarget::Branch(head.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    tx.upsert(
        "todos",
        inserted_row,
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("insert then upsert".to_owned()),
        )]),
        jazz::db::UpsertOptions {
            target: target.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    tx.upsert(
        "todos",
        upserted_row,
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        jazz::db::UpsertOptions {
            target: target.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    tx.upsert(
        "todos",
        upserted_row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        jazz::db::UpsertOptions {
            target,
            ..Default::default()
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let query = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(&query, ReadOpts::default().branch_view(head, None))).unwrap();
    let titles = rows
        .iter()
        .map(|row| (row.row_uuid(), row.cell("title").unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        titles,
        BTreeMap::from([
            (inserted_row, Value::String("insert then upsert".to_owned())),
            (upserted_row, Value::String("second".to_owned())),
        ])
    );
}

#[test]
fn branch_view_reduction_precedes_aggregation_and_ordered_windows() {
    let (db, _) = open_db();
    let base = selector(0x97);
    let head = selector(0x98);
    let replaced = RowUuid::from_bytes([0x99; 16]);
    let inherited = RowUuid::from_bytes([0x9a; 16]);
    let added = RowUuid::from_bytes([0x9b; 16]);
    for (row, title) in [(replaced, "alpha"), (inherited, "bravo")] {
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.update(
        "todos",
        replaced,
        BTreeMap::from([("title".to_owned(), Value::String("zulu".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("charlie".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(added),
            target: jazz::db::ExactWriteTarget::Branch(head.clone()),
            ..Default::default()
        },
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
        rows[0].cell("title"),
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
    let db = block_on(Db::open(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x83; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(3)),
    ))
    .unwrap();
    let workspace = uuid::Uuid::from_bytes([0x84; 16]);
    let branch = uuid::Uuid::from_bytes([0x85; 16]);
    let owner = RowUuid::from_bytes([0x86; 16]);
    db.insert(
        "workspaces",
        BTreeMap::from([("name".to_owned(), Value::String("shared".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid(workspace)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("shared".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(owner),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "memberships",
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(RowUuid::from_bytes([0x87; 16])),
            target: jazz::db::ExactWriteTarget::Branch(BranchSelector::new([(
                "workspace_id",
                Value::Uuid(workspace),
            )])),
            ..Default::default()
        },
    )
    .unwrap();
    let document = RowUuid::from_bytes([0x88; 16]);
    db.insert(
        "documents",
        BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(owner.0)),
            ("title".to_owned(), Value::String("draft".to_owned())),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(document),
            target: jazz::db::ExactWriteTarget::Branch(BranchSelector::new([
                ("workspace_id", Value::Uuid(workspace)),
                ("branch_id", Value::Uuid(branch)),
            ])),
            ..Default::default()
        },
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

/// Alice's branch view counts its reduced rows; Bob's grouped window observes
/// explicit aggregate roles, deterministic group identity, and empty counts.
#[test]
fn review_branch_aggregate_empty_grouped_window_and_roles() {
    let (db, _) = open_db();
    let base = selector(0xd1);
    let head = selector(0xd2);
    let opts =
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone())));
    let count = db.prepare_query(&Query::from("todos").count()).unwrap();
    let empty = block_on(db.all(&count, opts.clone())).unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0].cell_at(0), Some(Value::U64(0)));
    let grouped = db
        .prepare_query(&Query::from("todos").count().group_by("title"))
        .unwrap();
    assert!(block_on(db.all(&grouped, opts.clone())).unwrap().is_empty());

    for (index, title) in ["alpha", "bravo", "bravo", "charlie", "charlie", "charlie"]
        .iter()
        .enumerate()
    {
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String((*title).to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(RowUuid::from_bytes([0xd3 + index as u8; 16])),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    // Effective branch reduction moves one row between groups before counting.
    db.update(
        "todos",
        RowUuid::from_bytes([0xd6; 16]),
        BTreeMap::from([("title".to_owned(), Value::String("bravo".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head,
                base: Some(BranchViewBase::Current(base)),
            },
            ..Default::default()
        },
    )
    .unwrap();
    let window = db
        .prepare_query(
            &Query::from("todos")
                .count()
                .group_by("title")
                .order_by("count", OrderDirection::Desc)
                .order_by("title", OrderDirection::Asc)
                .offset(1)
                .limit(1),
        )
        .unwrap();
    let rows = block_on(db.all(&window, opts.clone())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell_at(0),
        Some(Value::String("charlie".to_owned()))
    );
    assert_eq!(rows[0].cell_at(1), Some(Value::U64(2)));
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("charlie".to_owned()))
    );
    assert_eq!(
        rows[0].cells().map(|(name, _)| name).collect::<Vec<_>>(),
        vec!["title", "count"],
        "the group key, then the count under its aggregate role"
    );
    let again = block_on(db.all(&window, opts.clone())).unwrap();
    assert_eq!(again[0].row_uuid(), rows[0].row_uuid());
}

#[test]
fn aggregate_window_subscription_reconciles_live_page_membership() {
    let (db, _schema) = open_db();
    let base = selector(0xe1);
    let head = selector(0xe2);
    let opts =
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone())));
    for (index, title) in ["alpha", "bravo", "bravo", "bravo", "charlie", "charlie"]
        .iter()
        .enumerate()
    {
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String((*title).to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(RowUuid::from_bytes([0xe3 + index as u8; 16])),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = db
        .prepare_query(
            &Query::from("todos")
                .count()
                .group_by("title")
                .order_by("count", OrderDirection::Desc)
                .order_by("title", OrderDirection::Asc)
                .offset(1)
                .limit(1),
        )
        .unwrap();
    let mut subscription = block_on(db.subscribe(&query, opts.clone())).unwrap();
    let opening = block_on(subscription.next_event()).unwrap();
    let visible = match opening {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            assert_eq!(added.len(), 1);
            assert_eq!(
                added[0].cell_at(0),
                Some(Value::String("charlie".to_owned()))
            );
            added[0].row_uuid()
        }
        other => panic!("expected charlie aggregate-window opening reset: {other:?}"),
    };

    db.update(
        "todos",
        RowUuid::from_bytes([0xe3; 16]),
        BTreeMap::from([("title".to_owned(), Value::String("bravo".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
    )
    .unwrap();
    let expected = block_on(db.all(&query, opts.clone())).unwrap();
    assert_eq!(expected.len(), 1);
    assert_eq!(
        expected[0].cell_at(0),
        Some(Value::String("charlie".to_owned()))
    );
    assert!(
        subscription.try_next_event().is_none(),
        "an aggregate change outside the retained page must not leak an add"
    );

    db.update(
        "todos",
        RowUuid::from_bytes([0xe7; 16]),
        BTreeMap::from([("title".to_owned(), Value::String("alpha".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head,
                base: Some(BranchViewBase::Current(base)),
            },
            ..Default::default()
        },
    )
    .unwrap();
    let expected = block_on(db.all(&query, opts)).unwrap();
    assert_eq!(expected.len(), 1);
    assert_eq!(
        expected[0].cell_at(0),
        Some(Value::String("alpha".to_owned()))
    );
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, ref updated, ref removed, .. }
            if added.len() == 1
                && added[0].cell_at(0) == Some(Value::String("alpha".to_owned()))
                && updated.is_empty()
                && removed.len() == 1
                && removed[0].row_uuid == visible
    ));
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
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x9d; 16]),
            author: AuthorSubject::SYSTEM,
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
        db.insert(
            "teams",
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(RowUuid(team)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.insert(
        "documents",
        BTreeMap::from([("title".to_owned(), Value::String("reachable".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(document),
            target: jazz::db::ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "access",
        BTreeMap::from([
            ("document".to_owned(), Value::Uuid(document.0)),
            ("team".to_owned(), Value::Uuid(allowed_team)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(access),
            target: jazz::db::ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
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

    db.update(
        "access",
        access,
        BTreeMap::from([("team".to_owned(), Value::Uuid(denied_team))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
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

fn open_policy_db() -> (Db, JazzSchema) {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("branches")
                    .fk_column("branch_key", "branches")
                    .column("name", ColumnType::Text)
                    .column("owner", ColumnType::Text)
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
    let db = block_on(Db::open(
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0x75; 16]),
                author: AuthorSubject::SYSTEM,
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
    let owner = AuthorSubject::for_test_bytes([0x76; 16]);
    let outsider = AuthorSubject::for_test_bytes([0x77; 16]);
    let branch = RowUuid::from_bytes([0x78; 16]);
    let selector = BranchSelector::new([("branch_id", Value::Uuid(branch.0))]);
    db.insert(
        "branches",
        BTreeMap::from([
            ("branch_key".to_owned(), Value::Uuid(branch.0)),
            ("name".to_owned(), Value::String("draft".to_owned())),
            ("owner".to_owned(), Value::String(owner.principal_parts().1)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(branch),
            ..Default::default()
        },
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
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("allowed".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(todo),
            identity: jazz::db::WriteIdentity::Session(owner),
            target: jazz::db::ExactWriteTarget::Branch(selector.clone()),
            ..Default::default()
        },
    )
    .expect("the ordinary referenced branch row authorizes its owner");
    let joined = db
        .prepare_query(&Query::from("todos").join_via_row_id(
            "branches",
            "branch_id",
            [eq(
                col("owner"),
                jazz::query::lit(owner.principal_parts().1),
            )],
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
                    .column("owner", ColumnType::Text)
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
    let db = block_on(Db::open(DbConfig::new(
        schema.clone(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let before = AuthorSubject::for_test_bytes([0xad; 16]);
    let after = AuthorSubject::for_test_bytes([0xae; 16]);
    let base_branch = RowUuid::from_bytes([0xaa; 16]);
    let head_branch = RowUuid::from_bytes([0xab; 16]);
    let base = BranchSelector::new([("branch_id", Value::Uuid(base_branch.0))]);
    let head = BranchSelector::new([("branch_id", Value::Uuid(head_branch.0))]);
    for branch in [base_branch, head_branch] {
        db.insert(
            "branches",
            BTreeMap::from([
                ("branch_key".to_owned(), Value::Uuid(branch.0)),
                ("name".to_owned(), Value::String("branch".to_owned())),
                (
                    "owner".to_owned(),
                    Value::String(before.principal_parts().1),
                ),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(branch),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let row = RowUuid::from_bytes([0xac; 16]);
    let authored = db
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("frozen".to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                identity: jazz::db::WriteIdentity::Session(before),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap()
        .mergeable_tx_id();
    let cut = captured_branch_snapshot(&db);
    assert_eq!(cut.owner, NodeUuid::from_bytes([0xa9; 16]));
    assert!(cut.global_base.0 >= authored);
    db.update(
        "branches",
        head_branch,
        BTreeMap::from([("owner".to_owned(), Value::String(after.principal_parts().1))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: base.clone(),
                base: None,
            },
            ..Default::default()
        },
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
    assert!(branch_rows.iter().all(|row| {
        row.cell("owner") == Some(Value::String(before.principal_parts().1))
            && row.cell("branch_id") == Some(Value::Uuid(head_branch.0))
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
    let owner = AuthorSubject::for_test_bytes([0xb5; 16]);
    let outsider = AuthorSubject::for_test_bytes([0xb6; 16]);
    let branch = RowUuid::from_bytes([0xb7; 16]);
    let selector = BranchSelector::new([("branch_id", Value::Uuid(branch.0))]);
    db.insert(
        "branches",
        BTreeMap::from([
            ("branch_key".to_owned(), Value::Uuid(branch.0)),
            ("name".to_owned(), Value::String("draft".to_owned())),
            ("owner".to_owned(), Value::String(owner.principal_parts().1)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(branch),
            ..Default::default()
        },
    )
    .unwrap();
    let row = RowUuid::from_bytes([0xb8; 16]);
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("visible".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            identity: jazz::db::WriteIdentity::Session(owner),
            target: jazz::db::ExactWriteTarget::Branch(selector.clone()),
            ..Default::default()
        },
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
        BTreeMap::from([(
            "owner".to_owned(),
            Value::String(outsider.principal_parts().1),
        )]),
        Default::default(),
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
        BTreeMap::from([("owner".to_owned(), Value::String(owner.principal_parts().1))]),
        Default::default(),
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
    let (db, _) = open_db();
    let row = RowUuid::from_bytes([0x70; 16]);
    let left = selector(0x72);
    let right = selector(0x73);

    let tx = db.mergeable_tx().unwrap();
    tx.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("left".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(left.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    tx.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("right".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(right.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let rows_in = |branch| {
        block_on(db.all(&prepared, ReadOpts::default().branch_view(branch, None))).unwrap()
    };
    let left_rows = rows_in(left);
    let right_rows = rows_in(right);
    assert_eq!(left_rows.len(), 1);
    assert_eq!(right_rows.len(), 1);
    assert_eq!(
        left_rows[0].cell("title"),
        Some(Value::String("left".to_owned()))
    );
    assert_eq!(
        right_rows[0].cell("title"),
        Some(Value::String("right".to_owned()))
    );
}

#[test]
fn branch_move_is_explicit_source_delete_and_destination_restore() {
    let (db, _) = open_db();
    let source = selector(0x94);
    let target = selector(0x95);
    let row = RowUuid::from_bytes([0x96; 16]);
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("move me".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(source.clone()),
            ..Default::default()
        },
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
        destination[0].cell("title"),
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
        let db = open_file_db(directory.path(), &schema);
        let tx = db.mergeable_tx().unwrap();
        tx.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("left".to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(left.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        tx.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("right".to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(right.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        tx.commit().unwrap();
        db.delete(
            "todos",
            row,
            jazz::db::DeleteOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: left.clone(),
                    base: None,
                },
                ..Default::default()
            },
        )
        .unwrap();
    }

    let db = open_file_db(directory.path(), &schema);
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
        rows[0].cell("title"),
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
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("left".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(left),
            ..Default::default()
        },
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

    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("right".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(right),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        block_on(right_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, .. }
            if added.iter().any(|candidate| candidate.row_uuid() == row)
    ));
    assert!(left_stream.try_next_event().is_none());

    db.delete(
        "todos",
        row,
        jazz::db::DeleteOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: selector(0x89),
                base: None,
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        block_on(left_stream.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref removed, .. }
            if removed.iter().any(|candidate| candidate.row_uuid == row)
    ));
    assert!(right_stream.try_next_event().is_none());
    db.restore(
        "todos",
        row,
        None,
        jazz::db::RestoreOptions {
            target: jazz::db::ExactWriteTarget::Branch(selector(0x89)),
            ..Default::default()
        },
    )
    .unwrap();
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
    let (db, _) = open_db();
    let base = selector(0xaf);
    let head = selector(0xb0);
    let rows = (0_u8..12)
        .map(|index| RowUuid::from_bytes([index.wrapping_add(0xb1); 16]))
        .collect::<Vec<_>>();
    for (index, row) in rows.iter().enumerate() {
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String(format!("seed-{index}")))]),
            jazz::db::InsertOptions {
                row_id: Some(*row),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
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
            let Value::String(title) = row.cell("title").unwrap() else {
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
            0 => block_on(db.update(
                "todos",
                row,
                BTreeMap::from([("title".to_owned(), Value::String(format!("base-{step}")))]),
                jazz::db::UpdateOptions {
                    target: jazz::db::WriteTarget::BranchView {
                        head: base.clone(),
                        base: None,
                    },
                    ..Default::default()
                },
            ))
            .map(|_| ()),
            1 => block_on(db.update(
                "todos",
                row,
                BTreeMap::from([("title".to_owned(), Value::String(format!("head-{step}")))]),
                jazz::db::UpdateOptions {
                    target: jazz::db::WriteTarget::BranchView {
                        head: head.clone(),
                        base: Some(BranchViewBase::Current(base.clone())),
                    },
                    ..Default::default()
                },
            ))
            .map(|_| ()),
            2 => block_on(db.delete(
                "todos",
                row,
                jazz::db::DeleteOptions {
                    target: jazz::db::WriteTarget::BranchView {
                        head: head.clone(),
                        base: Some(BranchViewBase::Current(base.clone())),
                    },
                    ..Default::default()
                },
            ))
            .map(|_| ()),
            3 => block_on(db.restore(
                "todos",
                row,
                Some(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String(format!("restored-{step}")),
                )])),
                jazz::db::RestoreOptions {
                    target: jazz::db::ExactWriteTarget::Branch(head.clone()),
                    ..Default::default()
                },
            ))
            .map(|_| ()),
            _ => block_on(db.update(
                "todos",
                row,
                BTreeMap::from([("title".to_owned(), Value::String(format!("exact-{step}")))]),
                jazz::db::UpdateOptions {
                    target: jazz::db::WriteTarget::BranchView {
                        head: head.clone(),
                        base: None,
                    },
                    ..Default::default()
                },
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
                let Value::String(title) = row.cell("title").unwrap() else {
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
    let (db, _) = open_db();
    let row = RowUuid::from_bytes([0x64; 16]);
    let base = selector(0x65);
    let head = selector(0x66);
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts {
        read_view: ReadViewSpec {
            include_deleted: false,
            source: ReadViewSourceSpec::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(base)),
            },
        },
        ..ReadOpts::default()
    };
    let rows = block_on(db.all(&prepared, opts.clone())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("base".to_owned()))
    );
    assert_eq!(
        rows[0].cell("branch_id"),
        Some(Value::Uuid(uuid::Uuid::from_bytes([0x66; 16])))
    );
    let mut subscription = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
    let opening = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        opening,
        SubscriptionEvent::Delta { reset: true, .. }
    ));

    db.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("base edited".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: selector(0x65),
                base: None,
            },
            ..Default::default()
        },
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

    db.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("draft".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::Current(selector(0x65))),
            },
            ..Default::default()
        },
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
    db.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("edited".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: None,
            },
            ..Default::default()
        },
    )
    .unwrap();
    let rows = block_on(db.all(&prepared, opts.clone())).unwrap();
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("edited".to_owned()))
    );

    db.delete(
        "todos",
        row,
        jazz::db::DeleteOptions {
            target: jazz::db::WriteTarget::BranchView { head, base: None },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(block_on(db.all(&prepared, opts)).unwrap().is_empty());
}

#[test]
fn indexed_branch_view_copy_on_write_and_reopen_keep_branch_coordinates_distinct() {
    let (_template, schema) = open_db();
    let directory = tempfile::tempdir().unwrap();
    let base = selector(0xd1);
    let head = selector(0xd2);
    let copied = RowUuid::from_bytes([0xd3; 16]);
    let inherited = RowUuid::from_bytes([0xd4; 16]);
    let needle = Value::String("needle".to_owned());
    let branch_view =
        ReadOpts::default().branch_view(head.clone(), Some(BranchViewBase::Current(base.clone())));

    {
        let db = open_file_db(directory.path(), &schema);
        for row in [copied, inherited] {
            db.insert(
                "todos",
                BTreeMap::from([("title".to_owned(), needle.clone())]),
                jazz::db::InsertOptions {
                    row_id: Some(row),
                    target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        db.update(
            "todos",
            copied,
            BTreeMap::from([("title".to_owned(), Value::String("draft".to_owned()))]),
            jazz::db::UpdateOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: head.clone(),
                    base: Some(BranchViewBase::Current(base.clone())),
                },
                ..Default::default()
            },
        )
        .unwrap();

        assert_branch_view_copy_on_write_receipt(&db, &base, copied, inherited, &branch_view);
    }

    let db = open_file_db(directory.path(), &schema);
    assert_branch_view_copy_on_write_receipt(&db, &base, copied, inherited, &branch_view);
}

fn assert_branch_view_copy_on_write_receipt(
    db: &Db,
    base: &BranchSelector,
    copied: RowUuid,
    inherited: RowUuid,
    branch_view: &ReadOpts,
) {
    let needle_query = db
        .prepare_query(&Query::from("todos").filter(eq(
            col("title"),
            jazz::query::lit(Value::String("needle".to_owned())),
        )))
        .unwrap();
    let needle_rows = block_on(db.all(&needle_query, branch_view.clone())).unwrap();
    assert_eq!(
        needle_rows
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![inherited],
        "the head copy must mask the base index hit before the title predicate"
    );

    let all = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(&all, branch_view.clone())).unwrap();
    assert_eq!(rows.len(), 2);
    let by_row = rows
        .iter()
        .map(|row| (row.row_uuid(), row))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_row[&copied].cell("title"),
        Some(Value::String("draft".to_owned()))
    );
    for row in by_row.values() {
        assert_eq!(
            row.cell("branch_id"),
            Some(Value::Uuid(uuid::Uuid::from_bytes([0xd2; 16]))),
            "ordinary branch cells project the requested head, including inherited content"
        );
    }

    let base_rows = block_on(db.all(
        &needle_query,
        ReadOpts::default().branch_view(base.clone(), None),
    ))
    .unwrap();
    assert_eq!(
        base_rows
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![copied, inherited],
        "copy-on-write must not rewrite the inherited branch-local row"
    );
}

#[test]
fn inherited_delete_is_a_head_register_and_can_be_restored() {
    let (db, _) = open_db();
    let row = RowUuid::from_bytes([0x67; 16]);
    let base = selector(0x68);
    let head = selector(0x69);
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let opts = ReadOpts {
        read_view: ReadViewSpec {
            include_deleted: false,
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

    db.delete(
        "todos",
        row,
        jazz::db::DeleteOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: match &opts.read_view.source {
                    ReadViewSourceSpec::BranchView { base, .. } => base.clone(),
                    _ => unreachable!(),
                },
            },
            ..Default::default()
        },
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

    db.restore(
        "todos",
        row,
        Some(BTreeMap::from([(
            "title".to_owned(),
            Value::String("restored".to_owned()),
        )])),
        jazz::db::RestoreOptions {
            target: jazz::db::ExactWriteTarget::Branch(head.clone()),
            ..Default::default()
        },
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
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("restored".to_owned()))
    );
}

#[test]
fn frozen_base_subscription_keeps_the_base_fixed_and_the_head_live() {
    let (db, _) = open_db();
    let row = RowUuid::from_bytes([0x74; 16]);
    let base = selector(0x75);
    let head = selector(0x76);
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("base".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let frozen = captured_branch_snapshot(&db);
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

    db.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("later base".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: base,
                base: None,
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(subscription.try_next_event().is_none());

    db.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("live head".to_owned()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: match &opts.read_view.source {
                    ReadViewSourceSpec::BranchView { base, .. } => base.clone(),
                    _ => unreachable!(),
                },
            },
            ..Default::default()
        },
    )
    .unwrap();
    let changed = block_on(subscription.next_event()).unwrap();
    assert!(matches!(
        changed,
        SubscriptionEvent::Delta { reset: false, ref updated, .. } if updated.len() == 1
    ));
    let rows = block_on(db.all(&prepared, opts)).unwrap();
    assert_eq!(
        rows[0].cell("title"),
        Some(Value::String("live head".to_owned()))
    );

    db.delete(
        "todos",
        row,
        jazz::db::DeleteOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: head.clone(),
                base: None,
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref removed, .. } if removed.len() == 1
    ));
    db.restore(
        "todos",
        row,
        Some(BTreeMap::from([(
            "title".to_owned(),
            Value::String("restored head".to_owned()),
        )])),
        jazz::db::RestoreOptions {
            target: jazz::db::ExactWriteTarget::Branch(head),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_event()).unwrap(),
        SubscriptionEvent::Delta { reset: false, ref added, .. } if added.len() == 1
    ));
}

/// Bob owns a project in a branch, and a note in that branch is readable
/// through it (`allowedTo.read("project")`). His history read in the branch
/// must find the project in that same branch, as his current read does; an
/// outsider without the project sees neither.
#[test]
fn branch_history_checks_inherited_policy_evidence_in_the_branch() {
    let owner_policy = PolicyExpr::Cmp {
        column: "owner".to_owned(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(vec![
            "user".to_owned(),
            "identity".to_owned(),
            "subject".to_owned(),
        ]),
    };
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("projects")
                    .column("branch_id", ColumnType::Uuid)
                    .column("owner", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(
                        TablePolicies::new()
                            .with_select(owner_policy)
                            .with_insert(PolicyExpr::True),
                    ),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("branch_id", ColumnType::Uuid)
                    .fk_column("project", "projects")
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::inherits(Operation::Select, "project"))
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .build(),
    );
    let db = block_on(Db::open(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([0x81; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(3)),
    ))
    .unwrap();
    let bob = AuthorSubject::for_test_bytes([0x82; 16]);
    let outsider = AuthorSubject::for_test_bytes([0x83; 16]);
    let project = RowUuid::from_bytes([0x84; 16]);
    let note = RowUuid::from_bytes([0x85; 16]);
    let draft =
        BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0x86; 16])))]);
    db.insert(
        "projects",
        BTreeMap::from([("owner".to_owned(), Value::String(bob.principal_parts().1))]),
        jazz::db::InsertOptions {
            row_id: Some(project),
            target: jazz::db::ExactWriteTarget::Branch(draft.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "notes",
        BTreeMap::from([
            ("project".to_owned(), Value::Uuid(project.0)),
            ("title".to_owned(), Value::String("first".to_owned())),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(note),
            target: jazz::db::ExactWriteTarget::Branch(draft.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    db.update(
        "notes",
        note,
        BTreeMap::from([("title".into(), Value::String("second".into()))]),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: draft.clone(),
                base: None,
            },
            ..Default::default()
        },
    )
    .unwrap();

    let opts = ReadOpts {
        tier: jazz::db::ReadTier::Remote,
        ..Default::default()
    }
    .branch_view(draft, None);
    let current = db
        .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(note.0)))))
        .unwrap();
    assert_eq!(
        block_on(db.all_for_identity(&current, opts.clone(), bob))
            .unwrap()
            .len(),
        1,
        "bob reads the note's current version through his project in the branch"
    );
    let history = db
        .prepare_query(
            &Query::from("history.notes")
                .filter(eq(col("id"), lit(Value::Uuid(note.0))))
                .order_by("$acceptedAt", OrderDirection::Asc),
        )
        .unwrap();
    let versions = block_on(db.all_for_identity(&history, opts.clone(), bob)).unwrap();
    assert_eq!(
        versions
            .iter()
            .map(|version| {
                version
                    .cells()
                    .find(|(name, _)| *name == "title")
                    .and_then(|(_, value)| value.cloned())
            })
            .collect::<Vec<_>>(),
        ["first", "second"]
            .map(|title| Some(Value::String(title.into())))
            .to_vec(),
        "the history gate must find the project in the same branch"
    );
    assert!(
        block_on(db.all_for_identity(&history, opts, outsider))
            .unwrap()
            .is_empty()
    );
}

use crate::common;

/// One write in a scripted branch-view sequence.
type BranchEdit<'a> = Box<dyn Fn(&Db) + 'a>;

/// Alice subscribes to `title = needle` on her draft over main. Under the
/// title index each branch's deletion register is read only for the row ids
/// of the content candidates, and main is masked only by the draft rows
/// sharing a candidate's id, so every late change on either side must still
/// reach the view: deletes and restores on the draft and on main, an indexed
/// value moving into and out of the prefix, a new row, and deletes of rows
/// outside the prefix. After each write the live result equals the unindexed
/// read of the same view, filtered to `needle`.
///
/// ```text
/// main:  a=needle  b=needle  c=hay  d=needle  x=hay (deleted)
/// draft: (empty)
/// alice ──title = needle, draft over main──► {a, b, d}, then one write at a time
/// ```
#[test]
fn indexed_branch_view_subscription_follows_late_changes_on_both_branches() {
    let (db, _) = open_db();
    let main = selector(0x81);
    let draft = selector(0x82);
    let [a, b, c, d, x, e] =
        [0x83, 0x84, 0x85, 0x86, 0x87, 0x88].map(|byte| RowUuid::from_bytes([byte; 16]));
    let title =
        |title: &str| BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]);
    for (row, value) in [
        (a, "needle"),
        (b, "needle"),
        (c, "hay"),
        (d, "needle"),
        (x, "hay"),
    ] {
        db.insert(
            "todos",
            title(value),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(main.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let on_main = || jazz::db::WriteTarget::BranchView {
        head: main.clone(),
        base: None,
    };
    let on_draft = || jazz::db::WriteTarget::BranchView {
        head: draft.clone(),
        base: Some(BranchViewBase::Current(main.clone())),
    };
    let delete = |db: &Db, row, target| {
        db.delete(
            "todos",
            row,
            jazz::db::DeleteOptions {
                target,
                ..Default::default()
            },
        )
        .unwrap();
    };
    let update = |db: &Db, row, value: &str, target| {
        db.update(
            "todos",
            row,
            title(value),
            jazz::db::UpdateOptions {
                target,
                ..Default::default()
            },
        )
        .unwrap();
    };
    let restore = |db: &Db, row, value: &str, branch: &BranchSelector| {
        db.restore(
            "todos",
            row,
            Some(title(value)),
            jazz::db::RestoreOptions {
                target: jazz::db::ExactWriteTarget::Branch(branch.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    };
    delete(&db, x, on_main());

    let opts =
        ReadOpts::default().branch_view(draft.clone(), Some(BranchViewBase::Current(main.clone())));
    let needle = db
        .prepare_query(&Query::from("todos").filter(eq(col("title"), lit("needle"))))
        .unwrap();
    let all = db.prepare_query(&db.table("todos")).unwrap();
    let expected = |db: &Db| {
        let mut rows = block_on(db.all(&all, opts.clone()))
            .unwrap()
            .into_iter()
            .filter(|row| row.cell("title") == Some(Value::String("needle".to_owned())))
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };

    let mut subscription = block_on(db.subscribe(&needle, opts.clone())).unwrap();
    let mut live = BTreeMap::new();
    let apply = |subscription: &mut jazz::db::SubscriptionStream,
                 live: &mut BTreeMap<RowUuid, ()>| {
        while let Some(event) = subscription.try_next_event() {
            let SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } = event
            else {
                panic!("unexpected subscription event: {event:?}");
            };
            if reset {
                live.clear();
            }
            for row in removed {
                assert!(
                    live.remove(&row.row_uuid).is_some(),
                    "removed an absent row"
                );
            }
            for row in added {
                assert!(
                    live.insert(row.row_uuid(), ()).is_none(),
                    "added a row twice"
                );
            }
        }
    };
    let opening = block_on(subscription.next_event()).unwrap();
    let SubscriptionEvent::Delta {
        reset: true, added, ..
    } = opening
    else {
        panic!("unexpected opening event: {opening:?}");
    };
    live.extend(added.iter().map(|row| (row.row_uuid(), ())));
    assert_eq!(live.keys().copied().collect::<Vec<_>>(), vec![a, b, d]);

    let edits: Vec<(&str, BranchEdit)> = vec![
        (
            "draft deletes an inherited row",
            Box::new(|db| delete(db, a, on_draft())),
        ),
        (
            "main deletes a row",
            Box::new(|db| delete(db, b, on_main())),
        ),
        (
            "main restores it",
            Box::new(|db| restore(db, b, "needle", &main)),
        ),
        (
            "draft restores its delete",
            Box::new(|db| restore(db, a, "needle", &draft)),
        ),
        (
            "main moves a row into the prefix",
            Box::new(|db| update(db, c, "needle", on_main())),
        ),
        (
            "main deletes the moved row",
            Box::new(|db| delete(db, c, on_main())),
        ),
        (
            "draft moves a row out of the prefix",
            Box::new(|db| update(db, d, "hay", on_draft())),
        ),
        (
            "draft deletes that row",
            Box::new(|db| delete(db, d, on_draft())),
        ),
        (
            "main deletes the row draft deleted",
            Box::new(|db| delete(db, d, on_main())),
        ),
        (
            "main restores the outside row",
            Box::new(|db| restore(db, x, "needle", &main)),
        ),
        (
            "main inserts a row",
            Box::new(|db| {
                db.insert(
                    "todos",
                    title("needle"),
                    jazz::db::InsertOptions {
                        row_id: Some(e),
                        target: jazz::db::ExactWriteTarget::Branch(main.clone()),
                        ..Default::default()
                    },
                )
                .unwrap();
            }),
        ),
        (
            "draft deletes the new row",
            Box::new(|db| delete(db, e, on_draft())),
        ),
    ];
    for (step, edit) in edits {
        edit(&db);
        apply(&mut subscription, &mut live);
        assert_eq!(
            live.keys().copied().collect::<Vec<_>>(),
            expected(&db),
            "{step}"
        );
    }
    assert_eq!(live.keys().copied().collect::<Vec<_>>(), {
        let mut rows = vec![a, b, x];
        rows.sort();
        rows
    });
}

/// Branched todos whose `status` enum declares `variants`.
fn todos_with_status(variants: &[&str]) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .column(
                        "status",
                        ColumnType::ScalarEnum {
                            name: "Status".to_owned(),
                            variants: variants
                                .iter()
                                .map(|variant| (*variant).to_owned())
                                .collect(),
                        },
                    )
                    .branch_by("branch_id"),
            )
            .build(),
    )
}

/// The identity lens from `todos_with_status(["open"])` to
/// `todos_with_status(["open", "closed"])`: a newer version that adds an
/// enum variant older readers cannot decode.
fn status_widening_lens(
    v1: &JazzSchema,
    v2: &jazz::protocol::SchemaVersion,
) -> jazz::protocol::MigrationLens {
    jazz::protocol::MigrationLens::new(
        v1.version_id(),
        v2.id,
        vec![jazz::protocol::TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![jazz::protocol::LensOp::TransformColumn {
                column: "status".to_owned(),
                transform: "jazz.identity".to_owned(),
            }],
        }],
    )
    .unwrap()
}

/// Adding a variant to a stored enum column is not supported yet: its
/// `TransformColumn` lens is refused (and without one, so is changing the
/// stored column's type) with an explicit error, and the store keeps
/// writing the version it had.
#[test]
fn widening_a_stored_enum_column_is_not_supported_yet() {
    let v1 = todos_with_status(&["open"]);
    let db = open_history_complete_db_with_schema(v1.clone());
    let v2 = todos_with_status(&["open", "closed"]);
    let target = jazz::protocol::SchemaVersion::new(v2);
    let publication = db
        .author_schema_lineage_publication(target.clone(), status_widening_lens(&v1, &target))
        .unwrap();
    block_on(db.publish_schema_with_lens(publication)).unwrap();
    let error = block_on(db.activate_catalogue_schema_for_test(
        jazz::protocol::CurrentWriteSchema {
            revision: 1,
            schema: target.id,
        },
    ))
    .unwrap_err();
    assert!(
        error.to_string().contains("is not supported yet"),
        "{error}"
    );
    let error = block_on(db.activate_schema(target.schema.clone())).unwrap_err();
    assert!(
        error.to_string().contains("is not supported yet"),
        "{error}"
    );
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("still v1".to_owned())),
            ("status".to_owned(), Value::EnumTag(0)),
        ]),
        jazz::db::InsertOptions {
            target: jazz::db::ExactWriteTarget::Branch(selector(0x50)),
            ..Default::default()
        },
    )
    .expect("the store keeps writing its current version");
}

/// An older reader omits a head row it cannot decode, but that head row still
/// masks the base row underneath: the reader never sees the base body.
///
/// base/open ──v2 head/closed──► v1 reader of head-over-base sees neither
#[test]
#[ignore = "#4273: widening a stored enum column via a TransformColumn lens is not supported yet"]
fn branch_view_head_row_an_older_reader_omits_still_masks_the_base() {
    let schema = todos_with_status;
    let v1 = schema(&["open"]);
    let db = open_history_complete_db_with_schema(v1.clone());
    let old = block_on(db.register_schema_view(v1.clone())).unwrap();
    let base = selector(0x51);
    let head = selector(0x52);
    let incompatible = RowUuid::from_bytes([0x53; 16]);
    let inherited = RowUuid::from_bytes([0x54; 16]);
    for (row, title) in [
        (incompatible, "base incompatible"),
        (inherited, "base inherited"),
    ] {
        db.insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("status".to_owned(), Value::EnumTag(0)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let v2 = schema(&["open", "closed"]);
    let target = jazz::protocol::SchemaVersion::new(v2.clone());
    let publication = db
        .author_schema_lineage_publication(target.clone(), status_widening_lens(&v1, &target))
        .unwrap();
    block_on(db.publish_schema_with_lens(publication)).unwrap();
    block_on(
        db.activate_catalogue_schema_for_test(jazz::protocol::CurrentWriteSchema {
            revision: 1,
            schema: target.id,
        }),
    )
    .unwrap();
    let new = block_on(db.register_schema_view(v2.clone())).unwrap();
    new.insert(
        "todos",
        BTreeMap::from([
            (
                "title".to_owned(),
                Value::String("head incompatible".to_owned()),
            ),
            ("status".to_owned(), Value::EnumTag(1)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(incompatible),
            target: jazz::db::ExactWriteTarget::Branch(head.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let whole = old.prepare_query(&old.table("todos")).unwrap();
    let composed = ReadOpts {
        tier: jazz::db::ReadTier::LocalOnly,
        ..ReadOpts::default().branch_view(head, Some(BranchViewBase::Current(base)))
    };
    assert_eq!(
        block_on(old.all(&whole, composed))
            .unwrap()
            .into_iter()
            .map(|row| (row.row_uuid(), row.cell("title")))
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(inherited, Some(Value::String("base inherited".to_owned())))]),
        "the omitted head row masks the base body"
    );
}

/// An enum branch column keeps its enum type in branch-view reads, on both
/// the head and the inherited base rows.
///
/// base/open + head/closed ──read head-over-base──► branch cells stay enums
#[test]
fn branch_view_reads_keep_enum_branch_columns() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column(
                        "branch",
                        ColumnType::ScalarEnum {
                            name: "Branch".to_owned(),
                            variants: vec!["base".to_owned(), "head".to_owned()],
                        },
                    )
                    .column("title", ColumnType::Text)
                    .branch_by("branch"),
            )
            .build(),
    );
    let db = open_history_complete_db_with_schema(schema.clone());
    let base = BranchSelector::new([("branch", Value::EnumTag(0))]);
    let head = BranchSelector::new([("branch", Value::EnumTag(1))]);
    let inherited = RowUuid::from_bytes([0x7a; 16]);
    let own = RowUuid::from_bytes([0x7b; 16]);
    for (row, branch, title) in [
        (inherited, base.clone(), "inherited"),
        (own, head.clone(), "own"),
    ] {
        db.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(branch),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = db.prepare_query(&db.table("todos")).unwrap();
    let branches = |opts: ReadOpts| {
        block_on(db.all(&query, opts))
            .unwrap()
            .into_iter()
            .map(|row| (row.row_uuid(), row.cell("branch")))
            .collect::<BTreeMap<_, _>>()
    };
    let exact = ReadOpts {
        tier: jazz::db::ReadTier::LocalOnly,
        ..ReadOpts::default().branch_view(head.clone(), None)
    };
    assert_eq!(
        branches(exact.clone()),
        BTreeMap::from([(own, Some(Value::EnumTag(1)))])
    );
    assert_eq!(
        branches(exact.branch_view(head, Some(BranchViewBase::Current(base)))),
        BTreeMap::from([
            (inherited, Some(Value::EnumTag(1))),
            (own, Some(Value::EnumTag(1))),
        ])
    );
}

/// A row's branch columns are its branch: an update that changes one is a
/// schema error and leaves the row where it was; writing the same value is
/// fine.
#[test]
fn an_update_cannot_move_a_row_to_another_branch() {
    let (db, _) = open_db();
    let draft = selector(0xd1);
    let row = db
        .insert(
            "todos",
            BTreeMap::from([("title".into(), Value::String("Draft".into()))]),
            jazz::db::InsertOptions {
                target: jazz::db::ExactWriteTarget::Branch(draft.clone()),
                ..Default::default()
            },
        )
        .unwrap()
        .row_uuid();
    let update = |branch: u8| {
        block_on(db.update(
            "todos",
            row,
            BTreeMap::from([(
                "branch_id".into(),
                Value::Uuid(uuid::Uuid::from_bytes([branch; 16])),
            )]),
            jazz::db::UpdateOptions {
                target: jazz::db::WriteTarget::BranchView {
                    head: draft.clone(),
                    base: None,
                },
                ..Default::default()
            },
        ))
    };
    let error = update(0xd2).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Schema);
    assert!(
        error
            .message
            .contains("branch column does not match exact branch key"),
        "{error}"
    );
    update(0xd1).unwrap();
    let query = db.prepare_query(&db.table("todos")).unwrap();
    let on = |branch: u8| {
        db.all(
            &query,
            ReadOpts::default().branch_view(selector(branch), None),
        )
        .unwrap()
        .len()
    };
    assert_eq!((on(0xd1), on(0xd2)), (1, 0));
}

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

/// A tab over a worker with no server captures a snapshot of its own
/// writes, changes them, and edits a draft frozen at the snapshot: the
/// draft's base is what the tab captured, though the worker has settled
/// those writes since.
#[test]
fn a_tab_over_its_worker_edits_a_draft_frozen_at_its_capture() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node: u8| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::for_test_bytes([0x63; 16]),
            },
        )))
        .unwrap()
    };
    let worker = open(0x70);
    worker.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    let tab = open(0x71);
    tab.set_non_durable_client();
    // As its worker's: no server to wait for.
    tab.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    let (client_side, server_side) = duplex_transport::duplex();
    block_on(tab.connect_upstream(client_side));
    worker.accept_subscriber(server_side, AuthorSubject::for_test_bytes([0x63; 16]));
    let main = selector(0xe1);
    let draft = selector(0xe2);
    let title = |t: &str| BTreeMap::from([("title".into(), Value::String(t.into()))]);
    let row = block_on(tab.insert(
        "todos",
        title("Original"),
        jazz::db::InsertOptions {
            target: jazz::db::ExactWriteTarget::Branch(main.clone()),
            ..Default::default()
        },
    ))
    .unwrap()
    .row_uuid();
    let snapshot = {
        let mut capture = pin!(tab.capture_snapshot());
        loop {
            let mut context = Context::from_waker(Waker::noop());
            if let Poll::Ready(value) = capture.as_mut().poll(&mut context) {
                break value.unwrap();
            }
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
        }
    };
    let on = |head: &BranchSelector, base: Option<BranchViewBase>| jazz::db::UpdateOptions {
        target: jazz::db::WriteTarget::BranchView {
            head: head.clone(),
            base,
        },
        ..Default::default()
    };
    block_on(tab.update("todos", row, title("Changed"), on(&main, None))).unwrap();
    let frozen = Some(BranchViewBase::snapshot(main.clone(), snapshot.clone()));
    let query = tab.prepare_query(&tab.table("todos")).unwrap();
    let read = |opts: ReadOpts| {
        let mut all = pin!(tab.all(&query, opts));
        loop {
            let mut context = Context::from_waker(Waker::noop());
            if let Poll::Ready(rows) = all.as_mut().poll(&mut context) {
                break rows.unwrap();
            }
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
        }
    };
    let titles = |rows: Vec<jazz::db::CurrentRow>| {
        rows.iter().map(|row| row.cell("title")).collect::<Vec<_>>()
    };
    assert_eq!(
        titles(read(
            ReadOpts::default().branch_view(draft.clone(), frozen.clone())
        )),
        [Some(Value::String("Original".into()))]
    );
    assert_eq!(
        titles(read(ReadOpts::default().branch_view(
            draft.clone(),
            Some(BranchViewBase::current(main.clone()))
        ))),
        [Some(Value::String("Changed".into()))]
    );
    block_on(tab.update("todos", row, title("Draft"), on(&draft, frozen))).unwrap();
}

#[test]
fn a_tab_edits_a_draft_frozen_at_a_capture_it_awaits_after_updating_and_a_new_tab_restores_it() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node: u8| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author: AuthorSubject::for_test_bytes([0x63; 16]),
            },
        )))
        .unwrap()
    };
    let worker = open(0x70);
    worker.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    let tab = open(0x71);
    tab.set_non_durable_client();
    // As its worker's: no server to wait for.
    tab.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    let (client_side, server_side) = duplex_transport::duplex();
    block_on(tab.connect_upstream(client_side));
    worker.accept_subscriber(server_side, AuthorSubject::for_test_bytes([0x63; 16]));
    let pump = || {
        for _ in 0..8 {
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
        }
    };
    let main = selector(0xe1);
    let draft = selector(0xe2);
    let title = |t: &str| BTreeMap::from([("title".into(), Value::String(t.into()))]);
    let row = block_on(tab.insert(
        "todos",
        title("Original"),
        jazz::db::InsertOptions {
            target: jazz::db::ExactWriteTarget::Branch(main.clone()),
            ..Default::default()
        },
    ))
    .unwrap()
    .row_uuid();
    let on = |head: &BranchSelector, base: Option<BranchViewBase>| jazz::db::UpdateOptions {
        target: jazz::db::WriteTarget::BranchView {
            head: head.clone(),
            base,
        },
        ..Default::default()
    };
    let other = block_on(tab.insert(
        "todos",
        title("Inherited original"),
        jazz::db::InsertOptions {
            target: jazz::db::ExactWriteTarget::Branch(main.clone()),
            ..Default::default()
        },
    ))
    .unwrap()
    .row_uuid();
    // A browser's capture starts when called: the first poll captures.
    let mut capture = pin!(tab.capture_snapshot());
    let mut context = Context::from_waker(Waker::noop());
    let mut snapshot = match capture.as_mut().poll(&mut context) {
        Poll::Ready(value) => Some(value.unwrap()),
        Poll::Pending => None,
    };
    block_on(tab.update("todos", row, title("Changed"), on(&main, None))).unwrap();
    block_on(tab.update("todos", other, title("Inherited changed"), on(&main, None))).unwrap();
    let snapshot = loop {
        if let Some(snapshot) = snapshot.take() {
            break snapshot;
        }
        let mut context = Context::from_waker(Waker::noop());
        if let Poll::Ready(value) = capture.as_mut().poll(&mut context) {
            break value.unwrap();
        }
        block_on(tab.tick()).unwrap();
        block_on(worker.tick()).unwrap();
    };
    // The worker settles the tab's commits before it reads.
    pump();
    let frozen = Some(BranchViewBase::snapshot(main.clone(), snapshot.clone()));
    let query = tab.prepare_query(&tab.table("todos")).unwrap();
    let read = |opts: ReadOpts| {
        let mut all = pin!(tab.all(&query, opts));
        loop {
            let mut context = Context::from_waker(Waker::noop());
            if let Poll::Ready(rows) = all.as_mut().poll(&mut context) {
                pump();
                break rows.unwrap();
            }
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
        }
    };
    let titles = |rows: Vec<jazz::db::CurrentRow>| {
        rows.iter().map(|row| row.cell("title")).collect::<Vec<_>>()
    };
    assert_eq!(
        titles(read(
            ReadOpts::default().branch_view(draft.clone(), frozen.clone())
        )),
        [
            Some(Value::String("Original".into())),
            Some(Value::String("Inherited original".into()))
        ]
    );
    assert_eq!(
        titles(read(ReadOpts::default().branch_view(
            draft.clone(),
            Some(BranchViewBase::current(main.clone()))
        ))),
        [
            Some(Value::String("Changed".into())),
            Some(Value::String("Inherited changed".into()))
        ]
    );
    block_on(tab.update("todos", row, title("Draft"), on(&draft, frozen))).unwrap();
    pump();

    // A reopened tab is a new, empty foreground over the same worker: the
    // worker holds what the capture names, and reads it as of the capture.
    let tab = open(0x72);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    let (client_side, server_side) = duplex_transport::duplex();
    block_on(tab.connect_upstream(client_side));
    worker.accept_subscriber(server_side, AuthorSubject::for_test_bytes([0x63; 16]));
    // As the bindings read: the store is empty, so the read asks the worker.
    let table = postcard::to_allocvec(&tab.table("todos")).unwrap();
    let read_of = |query: &[u8], opts: ReadOpts| {
        let mut all = pin!(tab.all_serialized_query_as(
            query,
            opts,
            None,
            None,
            None,
            false,
            || false,
            |attachment| tab.detach_query(attachment),
        ));
        loop {
            let mut context = Context::from_waker(Waker::noop());
            if let Poll::Ready(rows) = all.as_mut().poll(&mut context) {
                match rows.unwrap() {
                    jazz::db::SerializedReadResult::Rows(rows) => break rows,
                    other => panic!("{other:?}"),
                }
            }
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
        }
    };
    let read = |opts: ReadOpts| read_of(&table, opts);
    let restored = Some(BranchViewBase::snapshot(main.clone(), snapshot.clone()));
    // The first read is of one row, as an app's `one` reads it.
    let one = Query::from("todos")
        .filter(eq(col("id"), lit(Value::Uuid(row.0))))
        .limit(1);
    assert_eq!(
        titles(read_of(
            &postcard::to_allocvec(&one).unwrap(),
            ReadOpts::default().branch_view(draft.clone(), restored.clone())
        )),
        [Some(Value::String("Draft".into()))]
    );
    assert_eq!(
        titles(read(
            ReadOpts::default().branch_view(draft.clone(), restored.clone())
        )),
        [
            Some(Value::String("Draft".into())),
            Some(Value::String("Inherited original".into()))
        ]
    );
    assert_eq!(
        titles(read(ReadOpts::default().branch_view(main.clone(), None))),
        [
            Some(Value::String("Changed".into())),
            Some(Value::String("Inherited changed".into()))
        ]
    );
    assert_eq!(
        titles(read(
            ReadOpts::default().branch_view(draft.clone(), restored.clone())
        )),
        [
            Some(Value::String("Draft".into())),
            Some(Value::String("Inherited original".into()))
        ]
    );
}

/// A head row deleted through a branch view (masking its inherited base
/// row) is restored on the exact head, leaving the base untouched.
#[test]
fn a_head_deletion_through_a_branch_view_restores_on_the_exact_head() {
    let (db, _) = open_db();
    let base = selector(0xc1);
    let head = selector(0xc2);
    let row = RowUuid::from_bytes([0xc3; 16]);
    let title = |text: &str| BTreeMap::from([("title".to_owned(), Value::String(text.to_owned()))]);
    db.insert(
        "todos",
        title("inherited"),
        jazz::db::InsertOptions {
            row_id: Some(row),
            target: jazz::db::ExactWriteTarget::Branch(base.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let view = jazz::db::WriteTarget::BranchView {
        head: head.clone(),
        base: Some(BranchViewBase::Current(base.clone())),
    };
    db.upsert(
        "todos",
        row,
        title("draft upsert"),
        jazz::db::UpsertOptions {
            target: view.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    db.delete(
        "todos",
        row,
        jazz::db::DeleteOptions {
            target: view,
            ..Default::default()
        },
    )
    .unwrap();
    db.restore(
        "todos",
        row,
        Some(title("restored draft")),
        jazz::db::RestoreOptions {
            target: jazz::db::ExactWriteTarget::Branch(head.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    let query = db.prepare_query(&db.table("todos")).unwrap();
    let read = |selector: BranchSelector| {
        block_on(db.all(&query, ReadOpts::default().branch_view(selector, None)))
            .unwrap()
            .iter()
            .map(|row| row.cell("title").unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(read(head), [Value::String("restored draft".to_owned())]);
    assert_eq!(read(base), [Value::String("inherited".to_owned())]);
}

/// The same head deletion and restore on a client connected to a server,
/// however the writes interleave with sync: the server's answers leave the
/// head tombstone on the replica, so the exact-head restore finds it.
#[test]
fn a_synced_head_deletion_through_a_branch_view_restores_on_the_exact_head() {
    for schedule in 0..27u32 {
        synced_head_deletion_restores(schedule);
    }
}

fn synced_head_deletion_restores(schedule: u32) {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node: u8, author: AuthorSubject| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let author = AuthorSubject::for_test_bytes([0x63; 16]);
    let client = open(0x02, author);
    let server = open(0x01, AuthorSubject::SYSTEM);
    let (client_side, server_side) = jazz::db::duplex();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, author);
    client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    // Step `n` of the schedule: 0 no sync, 1 one exchange, 2 a full pump.
    let sync = |step: u32| match schedule / 3u32.pow(step) % 3 {
        0 => {}
        1 => {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
            block_on(client.tick()).unwrap();
        }
        _ => {
            for _ in 0..5 {
                block_on(client.tick()).unwrap();
                block_on(server.tick()).unwrap();
            }
            block_on(client.tick()).unwrap();
        }
    };

    let base = selector(0xd1);
    let head = selector(0xd2);
    let row = RowUuid::from_bytes([0xd3; 16]);
    let title = |text: &str| BTreeMap::from([("title".to_owned(), Value::String(text.to_owned()))]);
    let view = jazz::db::WriteTarget::BranchView {
        head: head.clone(),
        base: Some(BranchViewBase::Current(base.clone())),
    };
    client
        .insert(
            "todos",
            title("inherited"),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    sync(0);
    client
        .upsert(
            "todos",
            row,
            title("draft upsert"),
            jazz::db::UpsertOptions {
                target: view.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    sync(1);
    client
        .delete(
            "todos",
            row,
            jazz::db::DeleteOptions {
                target: view,
                ..Default::default()
            },
        )
        .unwrap();
    sync(2);
    client
        .restore(
            "todos",
            row,
            Some(title("restored draft")),
            jazz::db::RestoreOptions {
                target: jazz::db::ExactWriteTarget::Branch(head.clone()),
                ..Default::default()
            },
        )
        .expect(&format!("schedule {schedule}"));
}

/// A Remote read through one branch view judges only that branch's cached
/// rows: rows on other branches, which its answer does not list, stay.
#[test]
fn a_remote_branch_read_keeps_the_rows_of_other_branches() {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node: u8, author: AuthorSubject| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let author = AuthorSubject::for_test_bytes([0x63; 16]);
    let server = open(0x01, AuthorSubject::SYSTEM);
    server.set_authority();
    // A tab over its storage owner, as in a browser.
    let worker = open(0x02, author);
    let (worker_side, server_side) = jazz::db::duplex();
    let _upstream = block_on(worker.connect_upstream(worker_side));
    server.accept_subscriber(server_side, author);
    worker.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    let client = open(0x03, author);
    client.set_non_durable_client();
    let (tab_side, owner_side) = jazz::db::duplex();
    let _owner = block_on(client.connect_upstream(tab_side));
    worker.accept_subscriber(owner_side, author);
    client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
    let tick = || {
        block_on(client.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(server.tick()).unwrap();
        block_on(worker.tick()).unwrap();
        block_on(client.tick()).unwrap();
    };
    let pump = || {
        for _ in 0..5 {
            tick();
        }
    };

    let (main, draft) = (selector(0xe1), selector(0xe2));
    for (byte, branch) in [(0xe3, &main), (0xe4, &draft)] {
        client
            .insert(
                "todos",
                BTreeMap::from([("title".to_owned(), Value::String(format!("{byte:x}")))]),
                jazz::db::InsertOptions {
                    row_id: Some(RowUuid::from_bytes([byte; 16])),
                    target: jazz::db::ExactWriteTarget::Branch(branch.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
    }
    pump();

    // The host read path, as a binding reads.
    let query = postcard::to_allocvec(&client.table("todos")).unwrap();
    let read = |branch: &BranchSelector, tier: jazz::db::ReadTier| {
        let mut opts = ReadOpts::default().branch_view(branch.clone(), None);
        opts.tier = tier;
        let mut read = pin!(client.all_serialized_query(&query, opts, None, None, None));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..50 {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return match result.unwrap() {
                    jazz::db::SerializedReadResult::Rows(rows) => rows.len(),
                    other => panic!("{other:?}"),
                };
            }
            tick();
        }
        panic!("the read did not complete");
    };
    assert_eq!(read(&draft, jazz::db::ReadTier::Remote), 1);
    assert_eq!(
        read(&main, jazz::db::ReadTier::LocalOnly),
        1,
        "the main row stays cached"
    );
    assert_eq!(read(&main, jazz::db::ReadTier::Remote), 1);
    assert_eq!(
        read(&draft, jazz::db::ReadTier::LocalOnly),
        1,
        "the draft row stays cached"
    );
}
