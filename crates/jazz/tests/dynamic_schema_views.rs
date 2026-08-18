use jazz::block_on;
use jazz::db::BlockingResultFutureExt;
use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, col, eq, gt, lit};
use jazz::schema::{ColumnSchema, JazzSchema, MergeStrategy, Policy, TableSchema};
use jazz::tools::OpenBatchId;

fn schema(default: &str) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [ColumnSchema::new("label", ColumnType::String)
            .with_default(Value::String(default.to_owned()))],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn schema_with_note(default: &str) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("label", ColumnType::String)
                .with_default(Value::String(default.to_owned())),
            ColumnSchema::new("note", ColumnType::String)
                .with_default(Value::String(String::new())),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn owner_only_schema(default: &str) -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "items",
        [ColumnSchema::new("label", ColumnType::String)
            .with_default(Value::String(default.to_owned()))],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::shape(
        jazz::query::Query::from("items").filter(eq(lit(1_i64), lit(2_i64))),
    ))])
}

fn metadata_schema(reference: Option<&str>, counter: bool, indexed: bool) -> JazzSchema {
    let mut table = TableSchema::new(
        "items",
        [ColumnSchema::new("value", ColumnType::I64).with_default(Value::I64(0))],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public());
    if let Some(target) = reference {
        table = table.with_reference("value", target);
    }
    if counter {
        table = table.with_column_merge_strategy("value", MergeStrategy::Counter);
    }
    if indexed {
        table = table.with_indexed_column("value");
    }
    JazzSchema::new([table])
}

fn open_owner(schema: JazzSchema) -> Db {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    futures::executor::block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x31; 16]),
                author: AuthorId::from_bytes([0xa1; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(7)),
    ))
    .unwrap()
}

/// One database owner registers two typed schema views and both handles stage
/// writes into the same owner-wide open batch.
///
/// alice(owner) ──begin──► open batch
/// old view ──insert──────► │
/// new view ──insert──────► │ ──single commit──► two rows
#[test]
fn registered_schema_views_share_one_open_batch() {
    let old_schema = schema("old-default");
    let new_schema = schema("new-default");
    let mut owner = open_owner(old_schema.clone());
    let old_view = owner.register_schema_view(old_schema).unwrap();
    let new_view = owner.register_schema_view(new_schema).unwrap();
    assert_ne!(old_view.schema_view_id(), new_view.schema_view_id());

    let batch = OpenBatchId::new();
    owner.begin_mergeable_with_id(batch).unwrap();
    owner
        .view(&old_view)
        .unwrap()
        .mergeable_insert(
            batch,
            "items",
            RowUuid::from_bytes([1; 16]),
            Default::default(),
            None,
        )
        .unwrap();
    owner
        .view(&new_view)
        .unwrap()
        .mergeable_insert(
            batch,
            "items",
            RowUuid::from_bytes([2; 16]),
            Default::default(),
            None,
        )
        .unwrap();
    owner.commit_mergeable(batch).unwrap();

    let query = owner.prepare_query(&owner.table("items")).unwrap();
    let rows = owner.read(&query).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].cell(&schema("old-default").tables[0], "label"),
        Some(Value::String("old-default".to_owned()))
    );
    assert_eq!(
        rows[1].cell(&schema("new-default").tables[0], "label"),
        Some(Value::String("new-default".to_owned()))
    );
}

/// Registration is idempotent by canonical schema identity and lookup never
/// manufactures an unregistered view.
#[test]
fn schema_view_registration_is_idempotent_and_explicit() {
    let mut owner = open_owner(schema("default"));
    let first = owner.register_schema_view(schema("other")).unwrap();
    let second = owner.register_schema_view(schema("other")).unwrap();
    assert_eq!(first.schema_view_id(), second.schema_view_id());
    assert_eq!(
        owner
            .schema_view(first.schema_view_id())
            .unwrap()
            .schema_view_id(),
        first.schema_view_id()
    );
}

/// Exact view policy is an authorization boundary even when its physical
/// structure shares one SchemaVersionId with a public catalogue schema.
#[test]
fn exact_schema_view_policy_cannot_inherit_public_structural_policy() {
    let mut owner = open_owner(schema("public"));
    assert_eq!(
        schema("public").version_id(),
        owner_only_schema("not-the-author").version_id()
    );
    let error = match block_on(owner.register_schema_view(owner_only_schema("not-the-author"))) {
        Ok(_) => panic!("conflicting policy view unexpectedly registered"),
        Err(error) => error,
    };
    assert_eq!(error.code, jazz::db::ErrorCode::Schema);
    assert!(error.message.contains("policy metadata conflicts"));
}

#[test]
fn automatic_schema_view_admission_rejects_non_lens_metadata_changes() {
    for (label, target) in [
        ("references", metadata_schema(Some("other"), false, false)),
        ("merge strategies", metadata_schema(None, true, false)),
    ] {
        let mut owner = open_owner(metadata_schema(None, false, false));
        let error = match block_on(owner.register_schema_view(target)) {
            Ok(_) => panic!("{label} change unexpectedly auto-admitted"),
            Err(error) => error,
        };
        assert!(error.message.contains(label));
        assert!(error.message.contains("explicit lens"));
    }

    let base = metadata_schema(None, false, false);
    let mut owner = open_owner(base.clone());
    let indexed = metadata_schema(None, false, true);
    assert_eq!(base.version_id(), indexed.version_id());
    let error = match block_on(owner.register_schema_view(indexed)) {
        Ok(_) => panic!("index change unexpectedly registered without physical admission"),
        Err(error) => error,
    };
    assert!(error.message.contains("index metadata conflicts"));
}

/// A runtime owner may exist before any typed application schema is known;
/// registering the first typed view must still permit local-first staging.
#[test]
fn empty_owner_accepts_first_typed_schema_view() {
    let mut owner = open_owner(JazzSchema::new([]));
    let batch = OpenBatchId::new();
    owner.begin_mergeable_with_id(batch).unwrap();
    let view = owner.register_schema_view(schema("first")).unwrap();
    owner
        .view(&view)
        .unwrap()
        .mergeable_insert(
            batch,
            "items",
            RowUuid::from_bytes([3; 16]),
            Default::default(),
            None,
        )
        .unwrap();
    let prepared = owner
        .prepare_query_in_view(&view, &owner.table("items"))
        .unwrap();
    let rows = owner
        .view(&view)
        .unwrap()
        .transaction_all(batch, &prepared, Default::default())
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), RowUuid::from_bytes([3; 16]));
    owner.commit_mergeable(batch).unwrap();
}

/// Structurally distinct schema views may stage rows in one owner-wide batch;
/// each pending write retains the schema version selected by its view.
#[test]
fn one_batch_accepts_writes_from_structurally_distinct_views() {
    let mut owner = open_owner(JazzSchema::new([]));
    let batch = OpenBatchId::new();
    owner.begin_mergeable_with_id(batch).unwrap();
    let first = owner.register_schema_view(schema("same")).unwrap();
    owner
        .view(&first)
        .unwrap()
        .mergeable_insert(
            batch,
            "items",
            RowUuid::from_bytes([4; 16]),
            Default::default(),
            None,
        )
        .unwrap();
    let second = owner
        .register_schema_view(schema_with_note("same"))
        .unwrap();
    owner
        .view(&second)
        .unwrap()
        .mergeable_insert(
            batch,
            "items",
            RowUuid::from_bytes([5; 16]),
            Default::default(),
            None,
        )
        .unwrap();

    let prepared = owner
        .prepare_query_in_view(&second, &owner.table("items"))
        .unwrap();
    let rows = owner
        .view(&second)
        .unwrap()
        .transaction_all(batch, &prepared, Default::default())
        .unwrap();
    assert_eq!(rows.len(), 2);
    owner.commit_mergeable(batch).unwrap();
}

/// A batch opened by an empty owner must resolve its fixed preexisting snapshot
/// through the attached typed view, not through the owner's genesis schema.
#[test]
fn typed_view_reads_and_updates_preexisting_snapshot_rows() {
    let mut owner = open_owner(JazzSchema::new([]));
    let view = owner.register_schema_view(schema("initial")).unwrap();
    let row = RowUuid::from_bytes([6; 16]);

    let seed = OpenBatchId::new();
    owner.begin_mergeable_with_id(seed).unwrap();
    owner
        .view(&view)
        .unwrap()
        .mergeable_insert(seed, "items", row, Default::default(), None)
        .unwrap();
    owner.commit_mergeable(seed).unwrap();

    let batch = OpenBatchId::new();
    owner.begin_mergeable_with_id(batch).unwrap();
    assert_eq!(
        owner
            .view(&view)
            .unwrap()
            .mergeable_read(batch, "items", row)
            .unwrap()
            .unwrap()["label"],
        Value::String("initial".to_owned())
    );
    let prepared = owner
        .prepare_query_in_view(&view, &owner.table("items"))
        .unwrap();
    assert_eq!(
        owner
            .view(&view)
            .unwrap()
            .transaction_all(batch, &prepared, Default::default())
            .unwrap()
            .len(),
        1
    );
    let ordered = owner
        .prepare_query_in_view(
            &view,
            &owner.table("items").order_by("label", OrderDirection::Asc),
        )
        .unwrap();
    assert_eq!(
        owner
            .view(&view)
            .unwrap()
            .transaction_all(batch, &ordered, Default::default())
            .unwrap()
            .len(),
        1
    );
    let windowed = owner
        .prepare_query_in_view(
            &view,
            &owner
                .table("items")
                .filter(gt(col("label"), lit("a")))
                .offset(0)
                .limit(1),
        )
        .unwrap();
    assert_eq!(
        owner
            .view(&view)
            .unwrap()
            .transaction_all(batch, &windowed, Default::default())
            .unwrap()
            .len(),
        1
    );
    owner
        .view(&view)
        .unwrap()
        .mergeable_update(
            batch,
            "items",
            row,
            [("label".to_owned(), Value::String("updated".to_owned()))].into(),
            None,
        )
        .unwrap();
    assert_eq!(
        owner
            .view(&view)
            .unwrap()
            .mergeable_read(batch, "items", row)
            .unwrap()
            .unwrap()["label"],
        Value::String("updated".to_owned())
    );
    owner.commit_mergeable(batch).unwrap();
}

/// An owner-wide snapshot and an ordinary typed-view write are independent:
/// the direct write commits, while the already-open snapshot remains stable.
#[test]
fn ordinary_view_write_does_not_enter_open_owner_snapshot() {
    let mut owner = open_owner(JazzSchema::new([]));
    let batch = OpenBatchId::new();
    owner.begin_exclusive_with_id(batch).unwrap();
    let view = owner.register_schema_view(schema("direct")).unwrap();
    let row = RowUuid::from_bytes([7; 16]);
    owner
        .view(&view)
        .unwrap()
        .insert_with_id("items", row, Default::default(), None, None)
        .unwrap();

    let prepared = owner
        .prepare_query_in_view(&view, &owner.table("items"))
        .unwrap();
    assert!(
        owner
            .view(&view)
            .unwrap()
            .transaction_all(batch, &prepared, Default::default())
            .unwrap()
            .is_empty()
    );
    owner.abandon_exclusive(batch).unwrap();
    assert_eq!(
        owner
            .view(&view)
            .unwrap()
            .all(&prepared, Default::default())
            .unwrap()
            .len(),
        1
    );
}

/// Local-only runtimes enforce the same exclusive parent invariant before a
/// commit can resolve at Local durability; no remote authority is required.
#[test]
fn exclusive_view_commit_rejects_concurrent_local_row_change() {
    let mut owner = open_owner(JazzSchema::new([]));
    let view = owner.register_schema_view(schema("base")).unwrap();
    let row = RowUuid::from_bytes([8; 16]);
    owner
        .view(&view)
        .unwrap()
        .insert_with_id("items", row, Default::default(), None, None)
        .unwrap();

    let batch = OpenBatchId::new();
    owner.begin_exclusive_with_id(batch).unwrap();
    assert!(
        owner
            .view(&view)
            .unwrap()
            .exclusive_read(batch, "items", row)
            .unwrap()
            .is_some()
    );
    owner
        .view(&view)
        .unwrap()
        .update(
            "items",
            row,
            [("label".to_owned(), Value::String("alice".to_owned()))].into(),
            None,
            None,
        )
        .unwrap();
    owner
        .view(&view)
        .unwrap()
        .exclusive_update(
            batch,
            "items",
            row,
            [("label".to_owned(), Value::String("bob".to_owned()))].into(),
            None,
        )
        .unwrap();

    let error = owner.commit_exclusive(batch).unwrap_err();
    assert_eq!(
        error.to_string(),
        "(transaction_conflict): row visible parent changed since transaction write was staged"
    );
}
