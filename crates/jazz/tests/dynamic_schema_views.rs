use jazz::db::{Db, DbConfig, DbIdentity, ExclusiveTxOps, MergeableTxOps, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::schema::ColumnType;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, col, eq, gt, lit};
use jazz::schema::{ColumnSchema, JazzSchema, MergeStrategy, Policy, TableSchema};
use jazz::tools::OpenTransactionId;

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

async fn open_owner(schema: JazzSchema) -> Db<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x31; 16]),
                author: AuthorId::from_bytes([0xa1; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(7)),
    )
    .await
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
    futures::executor::block_on(async {
        let old_schema = schema("old-default");
        let new_schema = schema("new-default");
        let owner = open_owner(old_schema.clone()).await;
        let old_view = owner.register_schema_view(old_schema).await.unwrap();
        let new_view = owner.register_schema_view(new_schema).await.unwrap();
        assert_ne!(old_view.schema_view_id(), new_view.schema_view_id());

        let batch = OpenTransactionId::new();
        owner.begin_mergeable(batch).await.unwrap();
        old_view
            .mergeable_tx_ref(batch)
            .insert_with_id("items", RowUuid::from_bytes([1; 16]), Default::default())
            .await
            .unwrap();
        new_view
            .mergeable_tx_ref(batch)
            .insert_with_id("items", RowUuid::from_bytes([2; 16]), Default::default())
            .await
            .unwrap();
        owner.commit_mergeable_handle(batch).await.unwrap();

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
    });
}

/// Registration is idempotent by canonical schema identity and lookup never
/// manufactures an unregistered view.
#[test]
fn schema_view_registration_is_idempotent_and_explicit() {
    futures::executor::block_on(async {
        let owner = open_owner(schema("default")).await;
        let first = owner.register_schema_view(schema("other")).await.unwrap();
        let second = owner.register_schema_view(schema("other")).await.unwrap();
        assert_eq!(first.schema_view_id(), second.schema_view_id());
        assert_eq!(
            owner
                .schema_view(first.schema_view_id())
                .await
                .unwrap()
                .schema_view_id(),
            first.schema_view_id()
        );
    });
}

/// Exact view policy is an authorization boundary even when its physical
/// structure shares one SchemaVersionId with a public catalogue schema.
#[test]
fn exact_schema_view_policy_cannot_inherit_public_structural_policy() {
    futures::executor::block_on(async {
        let owner = open_owner(schema("public")).await;
        assert_eq!(
            schema("public").version_id(),
            owner_only_schema("not-the-author").version_id()
        );
        let error = match owner
            .register_schema_view(owner_only_schema("not-the-author"))
            .await
        {
            Ok(_) => panic!("conflicting policy view unexpectedly registered"),
            Err(error) => error,
        };
        assert_eq!(error.code, jazz::db::ErrorCode::Schema);
        assert!(error.message.contains("policy metadata conflicts"));
    });
}

#[test]
fn automatic_schema_view_admission_rejects_non_lens_metadata_changes() {
    futures::executor::block_on(async {
        for (label, target) in [
            ("references", metadata_schema(Some("other"), false, false)),
            ("merge strategies", metadata_schema(None, true, false)),
        ] {
            let owner = open_owner(metadata_schema(None, false, false)).await;
            let error = match owner.register_schema_view(target).await {
                Ok(_) => panic!("{label} change unexpectedly auto-admitted"),
                Err(error) => error,
            };
            assert!(error.message.contains(label));
            assert!(error.message.contains("explicit lens"));
        }

        let base = metadata_schema(None, false, false);
        let owner = open_owner(base.clone()).await;
        let indexed = metadata_schema(None, false, true);
        assert_eq!(base.version_id(), indexed.version_id());
        let error = match owner.register_schema_view(indexed).await {
            Ok(_) => panic!("index change unexpectedly registered without physical admission"),
            Err(error) => error,
        };
        assert!(error.message.contains("index metadata conflicts"));
    });
}

/// A runtime owner may exist before any typed application schema is known;
/// registering the first typed view must still permit local-first staging.
#[test]
fn empty_owner_accepts_first_typed_schema_view() {
    futures::executor::block_on(async {
        let owner = open_owner(JazzSchema::new([])).await;
        let batch = OpenTransactionId::new();
        owner.begin_mergeable(batch).await.unwrap();
        let view = owner.register_schema_view(schema("first")).await.unwrap();
        view.mergeable_tx_ref(batch)
            .insert_with_id("items", RowUuid::from_bytes([3; 16]), Default::default())
            .await
            .unwrap();
        let prepared = view.prepare_query(&view.table("items")).unwrap();
        let rows = view
            .mergeable_tx_ref(batch)
            .all_prepared(&prepared)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), RowUuid::from_bytes([3; 16]));
        owner.commit_mergeable_handle(batch).await.unwrap();
    });
}

/// Structurally distinct schema views may stage rows in one owner-wide batch;
/// each pending write retains the schema version selected by its view.
#[test]
fn one_batch_accepts_writes_from_structurally_distinct_views() {
    futures::executor::block_on(async {
        let owner = open_owner(JazzSchema::new([])).await;
        let batch = OpenTransactionId::new();
        owner.begin_mergeable(batch).await.unwrap();
        let first = owner.register_schema_view(schema("same")).await.unwrap();
        first
            .mergeable_tx_ref(batch)
            .insert_with_id("items", RowUuid::from_bytes([4; 16]), Default::default())
            .await
            .unwrap();
        let second = owner
            .register_schema_view(schema_with_note("same"))
            .await
            .unwrap();
        second
            .mergeable_tx_ref(batch)
            .insert_with_id("items", RowUuid::from_bytes([5; 16]), Default::default())
            .await
            .unwrap();

        let prepared = second.prepare_query(&second.table("items")).unwrap();
        let rows = second
            .mergeable_tx_ref(batch)
            .all_prepared(&prepared)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        owner.commit_mergeable_handle(batch).await.unwrap();
    });
}

/// A batch opened by an empty owner must resolve its fixed preexisting snapshot
/// through the attached typed view, not through the owner's genesis schema.
#[test]
fn typed_view_reads_and_updates_preexisting_snapshot_rows() {
    futures::executor::block_on(async {
        let owner = open_owner(JazzSchema::new([])).await;
        let view = owner.register_schema_view(schema("initial")).await.unwrap();
        let row = RowUuid::from_bytes([6; 16]);

        let seed = OpenTransactionId::new();
        owner.begin_mergeable(seed).await.unwrap();
        view.mergeable_tx_ref(seed)
            .insert_with_id("items", row, Default::default())
            .await
            .unwrap();
        owner.commit_mergeable_handle(seed).await.unwrap();

        let batch = OpenTransactionId::new();
        owner.begin_mergeable(batch).await.unwrap();
        let tx = view.mergeable_tx_ref(batch);
        assert_eq!(
            tx.read("items", row).await.unwrap().unwrap()["label"],
            Value::String("initial".to_owned())
        );
        let prepared = view.prepare_query(&view.table("items")).unwrap();
        assert_eq!(tx.all_prepared(&prepared).await.unwrap().len(), 1);
        let ordered = view
            .prepare_query(&view.table("items").order_by("label", OrderDirection::Asc))
            .unwrap();
        assert_eq!(tx.all_prepared(&ordered).await.unwrap().len(), 1);
        let windowed = view
            .prepare_query(
                &view
                    .table("items")
                    .filter(gt(col("label"), lit("a")))
                    .offset(0)
                    .limit(1),
            )
            .unwrap();
        assert_eq!(tx.all_prepared(&windowed).await.unwrap().len(), 1);
        tx.update(
            "items",
            row,
            [("label".to_owned(), Value::String("updated".to_owned()))].into(),
        )
        .await
        .unwrap();
        assert_eq!(
            tx.read("items", row).await.unwrap().unwrap()["label"],
            Value::String("updated".to_owned())
        );
        owner.commit_mergeable_handle(batch).await.unwrap();
    });
}

/// An owner-wide snapshot and an ordinary typed-view write are independent:
/// the direct write commits, while the already-open snapshot remains stable.
#[test]
fn ordinary_view_write_does_not_enter_open_owner_snapshot() {
    futures::executor::block_on(async {
        let owner = open_owner(JazzSchema::new([])).await;
        let batch = OpenTransactionId::new();
        owner.begin_exclusive(batch).await.unwrap();
        let view = owner.register_schema_view(schema("direct")).await.unwrap();
        let row = RowUuid::from_bytes([7; 16]);
        view.insert_with_id("items", row, Default::default())
            .await
            .unwrap();

        let prepared = view.prepare_query(&view.table("items")).unwrap();
        assert!(
            view.exclusive_tx_ref(batch)
                .all_prepared(&prepared)
                .await
                .unwrap()
                .is_empty()
        );
        owner.abandon_exclusive_handle(batch).unwrap();
        assert_eq!(view.read(&prepared).unwrap().len(), 1);
    });
}

/// Local-only runtimes enforce the same exclusive parent invariant before a
/// commit can resolve at Local durability; no remote authority is required.
#[test]
fn exclusive_view_commit_rejects_concurrent_local_row_change() {
    futures::executor::block_on(async {
        let owner = open_owner(JazzSchema::new([])).await;
        let view = owner.register_schema_view(schema("base")).await.unwrap();
        let row = RowUuid::from_bytes([8; 16]);
        view.insert_with_id("items", row, Default::default())
            .await
            .unwrap();

        let batch = OpenTransactionId::new();
        owner.begin_exclusive(batch).await.unwrap();
        let tx = view.exclusive_tx_ref(batch);
        assert!(tx.read("items", row).await.unwrap().is_some());
        view.update(
            "items",
            row,
            [("label".to_owned(), Value::String("alice".to_owned()))].into(),
        )
        .await
        .unwrap();
        tx.update(
            "items",
            row,
            [("label".to_owned(), Value::String("bob".to_owned()))].into(),
        )
        .await
        .unwrap();

        let error = owner.commit_exclusive_handle(batch).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "(transaction_conflict): row visible parent changed since transaction write was staged"
        );
    });
}
