use jazz::db::{Db, DbConfig, DbIdentity, MergeableTxOps, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
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
    let view = owner.register_schema_view(schema("first")).unwrap();
    let batch = OpenBatchId::new();
    owner.begin_mergeable(batch).unwrap();
    view.mergeable_tx_ref(batch)
        .insert_with_id("items", RowUuid::from_bytes([3; 16]), Default::default())
        .unwrap();
    owner.commit_mergeable_handle(batch).unwrap();
}
