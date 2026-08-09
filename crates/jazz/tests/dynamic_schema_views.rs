use jazz::db::{Db, DbConfig, DbIdentity, MergeableTxOps, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, col, gt, lit};
use jazz::schema::{ColumnSchema, JazzSchema, Policy, TableSchema};
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

fn open_owner(schema: JazzSchema) -> Db<MemoryStorage> {
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
    let owner = open_owner(old_schema.clone());
    let old_view = owner.register_schema_view(old_schema).unwrap();
    let new_view = owner.register_schema_view(new_schema).unwrap();
    assert_ne!(old_view.schema_view_id(), new_view.schema_view_id());

    let batch = OpenBatchId::new();
    owner.begin_mergeable(batch).unwrap();
    old_view
        .mergeable_tx_ref(batch)
        .insert_with_id("items", RowUuid::from_bytes([1; 16]), Default::default())
        .unwrap();
    new_view
        .mergeable_tx_ref(batch)
        .insert_with_id("items", RowUuid::from_bytes([2; 16]), Default::default())
        .unwrap();
    owner.commit_mergeable_handle(batch).unwrap();

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
    let owner = open_owner(schema("default"));
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

/// A runtime owner may exist before any typed application schema is known;
/// registering the first typed view must still permit local-first staging.
#[test]
fn empty_owner_accepts_first_typed_schema_view() {
    let owner = open_owner(JazzSchema::new([]));
    let batch = OpenBatchId::new();
    owner.begin_mergeable(batch).unwrap();
    let view = owner.register_schema_view(schema("first")).unwrap();
    view.mergeable_tx_ref(batch)
        .insert_with_id("items", RowUuid::from_bytes([3; 16]), Default::default())
        .unwrap();
    let prepared = view.prepare_query(&view.table("items")).unwrap();
    let rows = view
        .mergeable_tx_ref(batch)
        .all_prepared(&prepared)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), RowUuid::from_bytes([3; 16]));
    owner.commit_mergeable_handle(batch).unwrap();
}

/// Structurally distinct schema views may stage rows in one owner-wide batch;
/// each pending write retains the schema version selected by its view.
#[test]
fn one_batch_accepts_writes_from_structurally_distinct_views() {
    let owner = open_owner(JazzSchema::new([]));
    let batch = OpenBatchId::new();
    owner.begin_mergeable(batch).unwrap();
    let first = owner.register_schema_view(schema("same")).unwrap();
    first
        .mergeable_tx_ref(batch)
        .insert_with_id("items", RowUuid::from_bytes([4; 16]), Default::default())
        .unwrap();
    let second = owner
        .register_schema_view(schema_with_note("same"))
        .unwrap();
    second
        .mergeable_tx_ref(batch)
        .insert_with_id("items", RowUuid::from_bytes([5; 16]), Default::default())
        .unwrap();

    let prepared = second.prepare_query(&second.table("items")).unwrap();
    let rows = second
        .mergeable_tx_ref(batch)
        .all_prepared(&prepared)
        .unwrap();
    assert_eq!(rows.len(), 2);
    owner.commit_mergeable_handle(batch).unwrap();
}

/// A batch opened by an empty owner must resolve its fixed preexisting snapshot
/// through the attached typed view, not through the owner's genesis schema.
#[test]
fn typed_view_reads_and_updates_preexisting_snapshot_rows() {
    let owner = open_owner(JazzSchema::new([]));
    let view = owner.register_schema_view(schema("initial")).unwrap();
    let row = RowUuid::from_bytes([6; 16]);

    let seed = OpenBatchId::new();
    owner.begin_mergeable(seed).unwrap();
    view.mergeable_tx_ref(seed)
        .insert_with_id("items", row, Default::default())
        .unwrap();
    owner.commit_mergeable_handle(seed).unwrap();

    let batch = OpenBatchId::new();
    owner.begin_mergeable(batch).unwrap();
    let tx = view.mergeable_tx_ref(batch);
    assert_eq!(
        tx.read("items", row).unwrap().unwrap()["label"],
        Value::String("initial".to_owned())
    );
    let prepared = view.prepare_query(&view.table("items")).unwrap();
    assert_eq!(tx.all_prepared(&prepared).unwrap().len(), 1);
    let ordered = view
        .prepare_query(&view.table("items").order_by("label", OrderDirection::Asc))
        .unwrap();
    assert_eq!(tx.all_prepared(&ordered).unwrap().len(), 1);
    let windowed = view
        .prepare_query(
            &view
                .table("items")
                .filter(gt(col("label"), lit("a")))
                .offset(0)
                .limit(1),
        )
        .unwrap();
    assert_eq!(tx.all_prepared(&windowed).unwrap().len(), 1);
    tx.update(
        "items",
        row,
        [("label".to_owned(), Value::String("updated".to_owned()))].into(),
    )
    .unwrap();
    assert_eq!(
        tx.read("items", row).unwrap().unwrap()["label"],
        Value::String("updated".to_owned())
    );
    owner.commit_mergeable_handle(batch).unwrap();
}
