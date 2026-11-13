use crate::common;

use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, col, gt, lit};
use jazz::records::Value;
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
            composite_indexes: Vec::new(),
            policies: allow_all_policies(),
            branch_by: Vec::new(),
            history: Default::default(),
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

async fn open_owner(schema: JazzSchema) -> Db {
    Db::open(
        DbConfig::new(
            schema,
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
            .tx_ref(batch)
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
            .tx_ref(batch)
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
            rows[0].cell("label"),
            Some(Value::String("old-default".to_owned()))
        );
        assert_eq!(
            rows[1].cell("label"),
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
/// A UUID reference must name a declared table, but schema admission does not
/// require a referenced row to exist yet.
#[test]
fn public_schema_rejects_uuid_reference_to_absent_table() {
    let schema = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("roots").fk_column("target_id", "targets"))
        .build();

    let error = JazzSchema::new(&schema)
        .expect_err("a UUID reference to an absent table must fail schema admission");
    let diagnostic = error.to_string();
    assert!(
        diagnostic.contains("roots") && diagnostic.contains("target_id"),
        "diagnostic must identify the source table and column: {diagnostic}"
    );
    assert!(
        diagnostic.contains("targets"),
        "diagnostic must identify the absent referenced table: {diagnostic}"
    );
}

/// A declared reference is only valid on UUID-shaped source columns; the
/// conversion diagnostic must identify the particular malformed source.
#[test]
fn public_schema_rejects_references_on_non_uuid_columns() {
    for (column_name, column_type) in [
        ("text_target", ColumnType::Text),
        ("integer_target", ColumnType::Integer),
    ] {
        let mut schema = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("roots"))
            .table(TableSchemaBuilder::new("targets"))
            .build();
        schema
            .get_mut(&TableName::new("roots"))
            .expect("roots table is declared")
            .columns
            .columns
            .push(ColumnDescriptor::new(column_name, column_type).references("targets"));

        let error = JazzSchema::new(&schema)
            .expect_err("non-UUID reference metadata must fail schema admission");
        let diagnostic = error.to_string();
        assert!(
            diagnostic.contains("roots") && diagnostic.contains(column_name),
            "diagnostic must identify the malformed source column {column_name:?}: {diagnostic}"
        );
        assert!(
            diagnostic.to_ascii_lowercase().contains("reference")
                || diagnostic.to_ascii_lowercase().contains("uuid"),
            "diagnostic must identify the UUID/reference mismatch: {diagnostic}"
        );
    }
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
        view.tx_ref(batch)
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
        let rows = view.tx_ref(batch).all_prepared(&prepared).await.unwrap();
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
            .tx_ref(batch)
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
            .tx_ref(batch)
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
        let rows = second.tx_ref(batch).all_prepared(&prepared).await.unwrap();
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
        view.tx_ref(seed)
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
        let tx = view.tx_ref(batch);
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
        owner.begin_serializable(batch).await.unwrap();
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
            view.tx_ref(batch)
                .all_prepared(&prepared)
                .await
                .unwrap()
                .is_empty()
        );
        owner.abandon_transaction_handle(batch).unwrap();
        assert_eq!(view.read(&prepared).unwrap().len(), 1);
    });
}

/// Local-only runtimes enforce the same serializable parent invariant before a
/// commit can resolve at Local durability; no remote authority is required.
#[test]
fn serializable_view_commit_rejects_concurrent_local_row_change() {
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
        owner.begin_serializable(batch).await.unwrap();
        let tx = view.tx_ref(batch);
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

        let error = owner.commit_serializable_handle(batch).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "(transaction_conflict): row visible parent changed since transaction write was staged"
        );
    });
}

mod executing_policy_context {
    use crate::common::{compile_schema, read_and_allow_all_writes};
    use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on};
    use jazz::ids::{AuthorSubject, NodeUuid};
    use jazz::query::Query;
    use jazz::records::Value;
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

    fn schema(read: bool) -> jazz::tools::public_schema::Schema {
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(read_and_allow_all_writes(if read {
                        PolicyExpr::True
                    } else {
                        PolicyExpr::False
                    })),
            )
            .build()
    }
    fn owner() -> Db {
        let db = block_on(Db::open(DbConfig::new(
            compile_schema(&schema(true)),
            DbIdentity {
                node: NodeUuid::from_bytes([19; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        block_on(db.insert(
            "notes",
            [("body".into(), Value::String("private note".into()))].into(),
            Default::default(),
        ))
        .unwrap();
        db
    }
    fn read(db: &Db, query: &jazz::db::PreparedQuery) -> usize {
        block_on(db.all_for_identity(
            query,
            ReadOpts {
                tier: ReadTier::LocalOnly,
                ..Default::default()
            },
            AuthorSubject::for_test_bytes([1; 16]),
        ))
        .unwrap()
        .len()
    }
    fn pinned(db: &Db, allow: bool) -> Db {
        let bundle = block_on(db.publish_bundle(&schema(allow))).unwrap();
        db.pinned_view(db.current_schema().version_id(), Some(&bundle))
            .unwrap()
            .unwrap()
    }

    /// Alice retains a governed (not pinned) view while Bob closes the current
    /// schema's SELECT policy. The documented current-policy view must follow it.
    #[test]
    fn retained_governed_view_observes_current_policy_revocation() {
        let db = owner();
        let version = db.current_schema().version_id();
        let retained = db.governed_view(version).unwrap();
        assert_eq!(
            read(
                &retained,
                &retained.prepare_query(&Query::from("notes")).unwrap()
            ),
            1
        );
        block_on(db.activate_schema(compile_schema(&schema(false)))).unwrap();
        let fresh = db.governed_view(version).unwrap();
        assert_eq!(
            read(&fresh, &fresh.prepare_query(&Query::from("notes")).unwrap()),
            0
        );
        // Prepare after activation, isolating a retained-handle issue from stale queries.
        assert_eq!(
            read(
                &retained,
                &retained.prepare_query(&Query::from("notes")).unwrap()
            ),
            0,
            "retained governed view kept revoked SELECT permission"
        );
    }

    /// Alice explicitly pins an open bundle; Bob changing the current owner policy
    /// must leave that bundle's documented permission contract intact.
    #[test]
    fn pinned_view_keeps_its_bundle_after_owner_policy_revocation() {
        let db = owner();
        let pinned = pinned(&db, true);
        block_on(db.activate_schema(compile_schema(&schema(false)))).unwrap();
        let query = pinned.prepare_query(&Query::from("notes")).unwrap();
        assert_eq!(read(&pinned, &query), 1);
    }

    /// Alice's retained governed handle cannot insert after Bob revokes INSERT;
    /// a separately pinned open bundle keeps its explicitly selected policy.
    #[test]
    fn retained_governed_write_uses_current_policy_but_pin_stays_fixed() {
        use jazz::db::{InsertOptions, WriteIdentity};
        let db = owner();
        let governed = db.governed_view(db.current_schema().version_id()).unwrap();
        let pinned = pinned(&db, true);
        let closed = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(
                        jazz::tools::TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::False),
                    ),
            )
            .build();
        block_on(db.activate_schema(compile_schema(&closed))).unwrap();
        let opts = InsertOptions {
            identity: WriteIdentity::Session(AuthorSubject::for_test_bytes([1; 16])),
            ..Default::default()
        };
        let cells = [("body".into(), Value::String("new note".into()))].into();
        assert!(block_on(governed.insert("notes", cells, opts.clone())).is_err());
        let cells = [("body".into(), Value::String("pinned note".into()))].into();
        block_on(pinned.insert("notes", cells, opts)).unwrap();
    }

    fn serialized_count(result: jazz::db::SerializedReadResult) -> usize {
        match result {
            jazz::db::SerializedReadResult::Rows(rows) => rows.len(),
            jazz::db::SerializedReadResult::Relation(_) => panic!("plain notes query"),
        }
    }

    /// Alice reads through a retained governed handle using the ordinary binding
    /// route after Bob revokes SELECT. Preparation must not retain old authority.
    #[test]
    fn serialized_identity_read_observes_retained_view_revocation() {
        let db = owner();
        let retained = db.governed_view(db.current_schema().version_id()).unwrap();
        let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
        block_on(db.activate_schema(compile_schema(&schema(false)))).unwrap();
        let result = block_on(retained.all_serialized_query(
            &query,
            ReadOpts::for_read_tier(ReadTier::LocalOnly),
            None,
            Some((AuthorSubject::for_test_bytes([1; 16]), Default::default())),
            None,
        ))
        .unwrap();
        assert_eq!(
            serialized_count(result),
            0,
            "serialized read retained revoked SELECT"
        );
    }

    /// Alice's serialized query waits for Bob's remote coverage. Her local
    /// governed view loses SELECT while pending, so evaluation must use the new
    /// policy when the ordinary response finally arrives.
    ///
    /// Alice --Read--> Bob (not driven) ; revoke ; Bob --Rows/Settled--> Alice
    #[test]
    fn serialized_identity_read_rechecks_policy_after_remote_wait() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};
        let server = owner();
        server.set_authority();
        let alice = AuthorSubject::for_test_bytes([1; 16]);
        let client = block_on(Db::open(DbConfig::new(
            compile_schema(&schema(true)),
            DbIdentity {
                node: NodeUuid::from_bytes([21; 16]),
                author: alice,
            },
        )))
        .unwrap();
        let (up, down) = jazz::db::duplex();
        block_on(client.connect_upstream(up));
        server.accept_subscriber(down, alice).unwrap();
        client.set_remote_link_hint(jazz::db::RemoteLinkHint::Live);
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        let retained = client
            .governed_view(client.current_schema().version_id())
            .unwrap();
        let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
        let mut read = Box::pin(retained.all_serialized_query(
            &query,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            Some((alice, Default::default())),
            None,
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(read.as_mut().poll(&mut cx).is_pending());
        block_on(client.tick()).unwrap();
        assert!(
            read.as_mut().poll(&mut cx).is_pending(),
            "read must actually wait for Bob"
        );
        block_on(client.activate_schema(compile_schema(&schema(false)))).unwrap();
        for _ in 0..16 {
            block_on(server.tick()).unwrap();
            block_on(client.tick()).unwrap();
            if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
                assert_eq!(
                    serialized_count(result.unwrap()),
                    0,
                    "serialized read kept policy from before remote wait"
                );
                return;
            }
        }
        panic!("ordinary remote read did not complete");
    }

    /// Alice's explicit bundle and exact registered view stay fixed; an
    /// unfiltered ordinary cache read stays unfiltered when Bob changes owner
    /// policies. These are distinct from a current-governed identity read.
    #[test]
    fn serialized_reads_preserve_pins_exact_views_and_unfiltered_semantics() {
        let db = owner();
        let pinned = pinned(&db, true);
        let exact = block_on(db.register_schema_view(compile_schema(&schema(true)))).unwrap();
        let governed = db.governed_view(db.current_schema().version_id()).unwrap();
        let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
        block_on(db.activate_schema(compile_schema(&schema(false)))).unwrap();
        for (view, scope) in [
            (
                &pinned,
                Some((AuthorSubject::for_test_bytes([1; 16]), Default::default())),
            ),
            (
                &exact,
                Some((AuthorSubject::for_test_bytes([1; 16]), Default::default())),
            ),
            (&governed, None),
        ] {
            let result = block_on(view.all_serialized_query(
                &query,
                ReadOpts::for_read_tier(ReadTier::LocalOnly),
                None,
                scope,
                None,
            ))
            .unwrap();
            assert_eq!(serialized_count(result), 1);
        }
    }
}
