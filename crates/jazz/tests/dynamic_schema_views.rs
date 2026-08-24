mod common;

use jazz::db::{Db, DbConfig, DbIdentity, ExclusiveTxOps, MergeableTxOps, SeededRowIdSource};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, col, gt, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, ObjectId, OpenTransactionId, PolicyExpr,
    RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema, TableSchemaBuilder,
    Value as PublicValue,
};

use common::{allow_all_policies, compile_schema};

fn schema(default: &str) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column_with_default(
                        "label",
                        ColumnType::Text,
                        PublicValue::Text(default.to_owned()),
                    )
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn schema_with_note(default: &str) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column_with_default(
                        "label",
                        ColumnType::Text,
                        PublicValue::Text(default.to_owned()),
                    )
                    .column_with_default("note", ColumnType::Text, PublicValue::Text(String::new()))
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn owner_only_schema(default: &str) -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column_with_default(
                        "label",
                        ColumnType::Text,
                        PublicValue::Text(default.to_owned()),
                    )
                    .policies(
                        jazz::tools::TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False)
                            .with_update(Some(PolicyExpr::False), PolicyExpr::False)
                            .with_delete(PolicyExpr::False),
                    ),
            )
            .build(),
    )
}

fn metadata_schema(
    column: ColumnDescriptor,
    indexed_columns: Option<Vec<&str>>,
    include_reference_target: bool,
) -> JazzSchema {
    let mut source = Schema::from([(
        TableName::new("items"),
        TableSchema {
            columns: RowDescriptor::new(vec![column]),
            indexed_columns: indexed_columns
                .map(|columns| columns.into_iter().map(Into::into).collect()),
            policies: allow_all_policies(),
            branch_by: Vec::new(),
        },
    )]);
    if include_reference_target {
        source.extend(
            SchemaBuilder::new()
                .table(TableSchemaBuilder::new("other"))
                .build(),
        );
    }
    compile_schema(&source)
}

fn reference_metadata_schema(reference: bool) -> JazzSchema {
    let column = ColumnDescriptor::new("value", ColumnType::Uuid)
        .default(PublicValue::Uuid(ObjectId::from_uuid(uuid::Uuid::nil())));
    let column = if reference {
        column.references("other")
    } else {
        column
    };
    metadata_schema(column, None, true)
}

fn counter_metadata_schema(counter: bool) -> JazzSchema {
    let column =
        ColumnDescriptor::new("value", ColumnType::Integer).default(PublicValue::Integer(0));
    let column = if counter {
        column.merge_strategy(ColumnMergeStrategy::Counter)
    } else {
        column
    };
    metadata_schema(column, None, false)
}

fn indexed_metadata_schema(indexed: bool) -> JazzSchema {
    metadata_schema(
        ColumnDescriptor::new("value", ColumnType::BigInt).default(PublicValue::BigInt(0)),
        indexed.then_some(vec!["value"]),
        false,
    )
}

fn empty_schema() -> JazzSchema {
    compile_schema(&SchemaBuilder::new().build())
}

async fn open_owner(schema: JazzSchema) -> Db<TestStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([0x31; 16]),
                author: AuthorSubject::for_test_bytes([0xa1; 16]),
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
            .insert(
                "items",
                Default::default(),
                jazz::db::InsertOptions {
                    row_id: Some(RowUuid::from_bytes([1; 16])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        new_view
            .mergeable_tx_ref(batch)
            .insert(
                "items",
                Default::default(),
                jazz::db::InsertOptions {
                    row_id: Some(RowUuid::from_bytes([2; 16])),
                    ..Default::default()
                },
            )
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
        for (label, base, target) in [
            (
                "references",
                reference_metadata_schema(false),
                reference_metadata_schema(true),
            ),
            (
                "merge strategies",
                counter_metadata_schema(false),
                counter_metadata_schema(true),
            ),
        ] {
            let owner = open_owner(base).await;
            let error = match owner.register_schema_view(target).await {
                Ok(_) => panic!("{label} change unexpectedly auto-admitted"),
                Err(error) => error,
            };
            assert!(error.message.contains(label));
            assert!(error.message.contains("explicit lens"));
        }

        let base = indexed_metadata_schema(false);
        let owner = open_owner(base.clone()).await;
        let indexed = indexed_metadata_schema(true);
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
        let owner = open_owner(empty_schema()).await;
        let batch = OpenTransactionId::new();
        owner.begin_mergeable(batch).await.unwrap();
        let view = owner.register_schema_view(schema("first")).await.unwrap();
        view.mergeable_tx_ref(batch)
            .insert(
                "items",
                Default::default(),
                jazz::db::InsertOptions {
                    row_id: Some(RowUuid::from_bytes([3; 16])),
                    ..Default::default()
                },
            )
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
        let owner = open_owner(empty_schema()).await;
        let batch = OpenTransactionId::new();
        owner.begin_mergeable(batch).await.unwrap();
        let first = owner.register_schema_view(schema("same")).await.unwrap();
        first
            .mergeable_tx_ref(batch)
            .insert(
                "items",
                Default::default(),
                jazz::db::InsertOptions {
                    row_id: Some(RowUuid::from_bytes([4; 16])),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let second = owner
            .register_schema_view(schema_with_note("same"))
            .await
            .unwrap();
        second
            .mergeable_tx_ref(batch)
            .insert(
                "items",
                Default::default(),
                jazz::db::InsertOptions {
                    row_id: Some(RowUuid::from_bytes([5; 16])),
                    ..Default::default()
                },
            )
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
        let owner = open_owner(empty_schema()).await;
        let view = owner.register_schema_view(schema("initial")).await.unwrap();
        let row = RowUuid::from_bytes([6; 16]);

        let seed = OpenTransactionId::new();
        owner.begin_mergeable(seed).await.unwrap();
        view.mergeable_tx_ref(seed)
            .insert(
                "items",
                Default::default(),
                jazz::db::InsertOptions {
                    row_id: Some(row),
                    ..Default::default()
                },
            )
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
            Default::default(),
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
        let owner = open_owner(empty_schema()).await;
        let batch = OpenTransactionId::new();
        owner.begin_exclusive(batch).await.unwrap();
        let view = owner.register_schema_view(schema("direct")).await.unwrap();
        let row = RowUuid::from_bytes([7; 16]);
        view.insert(
            "items",
            Default::default(),
            jazz::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
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
        let owner = open_owner(empty_schema()).await;
        let view = owner.register_schema_view(schema("base")).await.unwrap();
        let row = RowUuid::from_bytes([8; 16]);
        view.insert(
            "items",
            Default::default(),
            jazz::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
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
            Default::default(),
        )
        .await
        .unwrap();
        tx.update(
            "items",
            row,
            [("label".to_owned(), Value::String("bob".to_owned()))].into(),
            Default::default(),
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
