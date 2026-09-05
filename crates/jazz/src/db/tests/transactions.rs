//! Mergeable and exclusive transaction staging, coalescing, provenance, and conflicts.

use super::*;

#[test]
fn reopened_seeded_row_ids_do_not_claim_freshness() {
    let schema = doctest_support::schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let dir = tempfile::tempdir().unwrap();
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0x92; 16]),
        author: AuthorSubject::for_test_bytes([0xa2; 16]),
    };

    let open = |storage| {
        block_on(Db::open(DbConfig {
            schema: schema.clone(),
            storage,
            identity,
            id_source: Some(Box::new(SeededRowIdSource::new(0x9292))),
        }))
        .unwrap()
    };

    let db = open(RocksDbStorage::open(dir.path(), &refs).unwrap());
    let first_tx = OpenTransactionId::new();
    db.begin_mergeable(first_tx).unwrap();
    let first_row = db
        .mergeable_tx_ref(first_tx)
        .insert(
            "todos",
            doctest_support::todo_cells("first", false),
            InsertOptions {
                updated_at_ms: Some(100),
                ..Default::default()
            },
        )
        .unwrap();
    block_on(db.commit_mergeable_handle(first_tx)).unwrap();
    let first_provenance = prepared_one(&db, &db.table("todos"))
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    block_on(db.close()).unwrap();
    drop(db);

    let reopened = open(RocksDbStorage::open(dir.path(), &refs).unwrap());
    let repeated_tx = OpenTransactionId::new();
    reopened.begin_mergeable(repeated_tx).unwrap();
    let repeated_row = reopened
        .mergeable_tx_ref(repeated_tx)
        .insert(
            "todos",
            doctest_support::todo_cells("must conflict", true),
            InsertOptions {
                updated_at_ms: Some(200),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(repeated_row, first_row);
    block_on(reopened.commit_mergeable_handle(repeated_tx)).unwrap();
    let rows = prepared_all(&reopened, &reopened.table("todos"), ReadOpts::default());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String("must conflict".to_owned()))
    );
    let repeated_provenance = rows[0].provenance().unwrap().unwrap();
    assert_eq!(repeated_provenance.created_at, first_provenance.created_at);
    assert_eq!(repeated_provenance.created_by, first_provenance.created_by);
    assert!(repeated_provenance.updated_at > repeated_provenance.created_at);
}

#[test]
fn exclusive_transactions_lower_oversized_scalars_before_publication() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let title = "x".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 91);
    let row = row(0x4e);
    let tx = db.exclusive_tx().unwrap();
    tx.insert(
        "todos",
        doctest_support::todo_cells(&title, false),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let physical = block_on(async {
        db.node
            .node
            .lock()
            .await
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
    });
    assert!(matches!(physical, Value::Large(_)));
    let result = db
        .read(&db.prepare_query(&db.table("todos")).unwrap())
        .unwrap();
    assert_eq!(
        result[0].cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String(title.clone()))
    );

    let update = db.exclusive_tx().unwrap();
    assert_eq!(
        update.read("todos", row).unwrap().unwrap().get("title"),
        Some(&Value::String(title.clone())),
        "public transaction reads must not expose the physical descriptor"
    );
    assert_eq!(
        update.all("todos").unwrap()[0].cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String(title.clone()))
    );
    update
        .update(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    update.commit().unwrap();
    let after_update = block_on(async {
        db.node
            .node
            .lock()
            .await
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
    });
    assert_eq!(physical, after_update, "unchanged locators must be stable");

    let upsert = db.exclusive_tx().unwrap();
    upsert
        .upsert(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
            Default::default(),
        )
        .unwrap();
    upsert.commit().unwrap();
    let after_upsert = block_on(async {
        db.node
            .node
            .lock()
            .await
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
    });
    assert_eq!(
        physical, after_upsert,
        "upserting unrelated cells must preserve unchanged locators"
    );

    let mergeable = db.mergeable_tx().unwrap();
    assert_eq!(
        mergeable.read("todos", row).unwrap().unwrap().get("title"),
        Some(&Value::String(title.clone()))
    );
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    assert_eq!(
        mergeable.all_prepared(&prepared).unwrap()[0]
            .cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String(title.clone()))
    );

    let forged = db.exclusive_tx().unwrap();
    forged
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), physical),
                ("done".to_owned(), Value::Bool(false)),
            ]),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([0x4f; 16])),
                ..Default::default()
            },
        )
        .unwrap();
    let error = block_on(forged.commit()).unwrap_err();
    assert!(error.message.contains("unverified large-value descriptor"));
}

/// A branch-view update starts a physical overlay with every visible base
/// cell. Its untouched large descriptor is engine-derived, while a descriptor
/// from another source remains untrusted even if both have the same shape.
#[test]
fn mergeable_branch_view_tx_retains_only_exact_inherited_large_values() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("branch", PublicColumnType::Text)
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text)
                .branch_by("branch"),
        ),
    );
    let db = open_db(0x4f, AuthorSubject::SYSTEM, &schema);
    block_on(async {
        db.node
            .node
            .lock()
            .await
            .set_chunk_storage(std::rc::Rc::new(groove::chunks::MemoryChunkStorage::new()));
    });
    let main = BranchSelector::new([("branch", Value::String("main".to_owned()))]);
    let draft = BranchSelector::new([("branch", Value::String("draft".to_owned()))]);
    let target_row = row(0x4f);
    let body = "x".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 1);
    let seeded = db
        .insert(
            "documents",
            BTreeMap::from([
                ("branch".to_owned(), Value::String("main".to_owned())),
                ("title".to_owned(), Value::String("base".to_owned())),
                ("body".to_owned(), Value::String(body)),
            ]),
            InsertOptions {
                row_id: Some(target_row),
                target: ExactWriteTarget::Branch(main.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    block_on(seeded.wait(DurabilityTier::Local)).unwrap();
    let other = row(0x50);
    let other_seeded = db
        .insert(
            "documents",
            BTreeMap::from([
                ("branch".to_owned(), Value::String("main".to_owned())),
                ("title".to_owned(), Value::String("other".to_owned())),
                (
                    "body".to_owned(),
                    Value::String("y".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 2)),
                ),
            ]),
            InsertOptions {
                row_id: Some(other),
                target: ExactWriteTarget::Branch(main.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    block_on(other_seeded.wait(DurabilityTier::Local)).unwrap();
    let inherited = block_on(async {
        db.node
            .node
            .lock()
            .await
            .visible_current_physical_cells_in_branch_schema(
                db.schema_version_id,
                "documents",
                &main,
                target_row,
            )
            .await
            .unwrap()
            .unwrap()
    });

    let tx = db.mergeable_tx().unwrap();
    tx.update(
        "documents",
        target_row,
        BTreeMap::from([("title".to_owned(), Value::String("draft".to_owned()))]),
        UpdateOptions {
            target: WriteTarget::BranchView {
                head: draft.clone(),
                base: Some(BranchViewBase::Current(main.clone())),
            },
            ..Default::default()
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let draft_cells = block_on(async {
        db.node
            .node
            .lock()
            .await
            .visible_current_physical_cells_in_branch_schema(
                db.schema_version_id,
                "documents",
                &draft,
                target_row,
            )
            .await
            .unwrap()
            .unwrap()
    });
    assert_eq!(draft_cells.get("body"), inherited.get("body"));

    let unrelated = block_on(async {
        db.node
            .node
            .lock()
            .await
            .visible_current_physical_cells_in_branch_schema(
                db.schema_version_id,
                "documents",
                &main,
                other,
            )
            .await
            .unwrap()
            .unwrap()
            .remove("body")
            .unwrap()
    });
    assert!(matches!(unrelated, Value::Large(_)));
    let forged = MergeableCommit::new("documents", target_row, 1)
        .branch(draft)
        .cells(BTreeMap::from([("body".to_owned(), unrelated)]))
        .verified_inherited_large_cells(&inherited);
    let result = block_on(async {
        db.node
            .node
            .lock()
            .await
            .seal_inherited_large_values(forged, db.schema_version_id, true)
            .await
    });
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("cross-source descriptor must remain unverified"),
    };
    assert!(matches!(
        error,
        crate::node::Error::InvalidMergeableCommit(
            "row update contains an unverified large-value descriptor"
        )
    ));
}

/// Standalone and mergeable-transaction branch-view upserts may copy an
/// inherited row whose untouched large value is represented by a descriptor.
/// The descriptor remains trusted only because it came from that exact base
/// preimage.
#[test]
fn branch_view_upserts_preserve_verified_inherited_large_values() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("branch", PublicColumnType::Text)
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text)
                .branch_by("branch"),
        ),
    );
    let db = open_db(0x51, AuthorSubject::SYSTEM, &schema);
    block_on(async {
        db.node
            .node
            .lock()
            .await
            .set_chunk_storage(std::rc::Rc::new(groove::chunks::MemoryChunkStorage::new()));
    });
    let base = BranchSelector::new([("branch", Value::String("base".to_owned()))]);
    let standalone_head = BranchSelector::new([("branch", Value::String("standalone".to_owned()))]);
    let transaction_head =
        BranchSelector::new([("branch", Value::String("transaction".to_owned()))]);
    let standalone_row = row(0x51);
    let transaction_row = row(0x52);
    let standalone_body = "s".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 1);
    let transaction_body = "t".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 2);

    for (row_id, body) in [
        (standalone_row, standalone_body.clone()),
        (transaction_row, transaction_body.clone()),
    ] {
        let seeded = db
            .insert(
                "documents",
                BTreeMap::from([
                    ("branch".to_owned(), Value::String("base".to_owned())),
                    ("title".to_owned(), Value::String("base".to_owned())),
                    ("body".to_owned(), Value::String(body)),
                ]),
                InsertOptions {
                    row_id: Some(row_id),
                    target: ExactWriteTarget::Branch(base.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
        block_on(seeded.wait(DurabilityTier::Local)).unwrap();
    }

    let standalone = db
        .upsert(
            "documents",
            standalone_row,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("standalone overlay".to_owned()),
            )]),
            UpsertOptions {
                target: WriteTarget::BranchView {
                    head: standalone_head.clone(),
                    base: Some(BranchViewBase::Current(base.clone())),
                },
                ..Default::default()
            },
        )
        .unwrap();
    block_on(standalone.wait(DurabilityTier::Local)).unwrap();

    let tx = db.mergeable_tx().unwrap();
    tx.upsert(
        "documents",
        transaction_row,
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("transaction overlay".to_owned()),
        )]),
        UpsertOptions {
            target: WriteTarget::BranchView {
                head: transaction_head.clone(),
                base: Some(BranchViewBase::Current(base.clone())),
            },
            ..Default::default()
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let query = db.table("documents");
    for (head, row_id, expected_body) in [
        (standalone_head, standalone_row, standalone_body),
        (transaction_head, transaction_row, transaction_body),
    ] {
        let rows = prepared_all(
            &db,
            &query,
            ReadOpts::default().branch_view(head, Some(BranchViewBase::Current(base.clone()))),
        );
        let visible = rows
            .iter()
            .find(|candidate| candidate.row_uuid() == row_id)
            .expect("upserted inherited row remains visible");
        assert_eq!(
            visible.cell(&schema.tables[0], "body"),
            Some(Value::String(expected_body))
        );
    }
}

#[test]
fn attached_schema_mergeable_batch_is_queryable_after_owner_commit() {
    let empty = build_public_db_test_schema(PublicSchemaBuilder::new());
    let refs = empty.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let owner = block_on(Db::open_history_complete(DbConfig {
        schema: empty,
        storage: doctest_support::MemoryStorage::new(&refs).expect("valid memory storage families"),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x91; 16]),
            author: AuthorSubject::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(91))),
    }))
    .unwrap();
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean),
        ),
    );
    let view = owner.register_schema_view(schema.clone()).unwrap();
    let open = OpenTransactionId::new();
    owner.begin_mergeable(open).unwrap();
    let inserted = row(0x91);
    view.mergeable_tx_ref(open)
        .insert(
            "todos",
            doctest_support::todo_cells("attached", false),
            crate::db::InsertOptions {
                row_id: Some(inserted),
                updated_at_ms: Some(1_704_067_200_123),
                ..Default::default()
            },
        )
        .unwrap();
    owner.commit_mergeable_handle(open).unwrap();

    // Advance the owner's canonical schema after the query view was registered.
    // The historical view still calls this column `title`; resolving projection
    // against the canonical schema would silently omit it after the rename.
    let renamed_schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("done", PublicColumnType::Boolean)
                .column("summary", PublicColumnType::Text),
        ),
    );
    let renamed = SchemaVersion::new(renamed_schema);
    let publication = owner
        .author_schema_lineage_publication(
            renamed.clone(),
            MigrationLens::new(
                schema.version_id(),
                renamed.id,
                vec![TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: vec![LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "summary".to_owned(),
                    }],
                }],
            )
            .expect("valid migration lens"),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    owner.publish_schema_with_lens(2, publication).unwrap();
    owner
        .set_current_write_schema(CurrentWriteSchema {
            revision: 2,
            schema: renamed.id,
        })
        .unwrap();

    let overlay_open = OpenTransactionId::new();
    owner.begin_mergeable(overlay_open).unwrap();
    let overlay_inserted = row(0x93);
    let overlay_tx = view.mergeable_tx_ref(overlay_open);
    overlay_tx
        .insert(
            "todos",
            doctest_support::todo_cells("overlay", true),
            crate::db::InsertOptions {
                row_id: Some(overlay_inserted),
                updated_at_ms: Some(1_704_067_200_456),
                ..Default::default()
            },
        )
        .unwrap();
    let prepared = view
        .prepare_query(
            &view
                .table("todos")
                .select(["done", "title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();
    let overlay_rows = overlay_tx.all_prepared(&prepared).unwrap();
    let overlay_row = overlay_rows
        .iter()
        .find(|row| row.row_uuid() == overlay_inserted)
        .expect("staged historical-view row is visible");
    assert!(
        overlay_row
            .encoded_record()
            .0
            .field_index("user_title")
            .is_some()
    );
    assert!(
        overlay_row
            .encoded_record()
            .0
            .field_index("user_done")
            .is_some()
    );
    assert_eq!(
        overlay_row.cell_at(0),
        Some(Value::String("overlay".to_owned()))
    );
    assert_eq!(overlay_row.cell_at(1), Some(Value::Bool(true)));
    let overlay_provenance = overlay_row.provenance().unwrap().unwrap();
    assert_eq!(overlay_provenance.created_at, 1_704_067_200_456);
    assert_eq!(overlay_provenance.updated_at, 1_704_067_200_456);
    owner.abandon_transaction_handle(overlay_open).unwrap();

    let rows = block_on(view.all(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), inserted);
    assert!(
        rows[0]
            .encoded_record()
            .0
            .field_index("user_title")
            .is_some()
    );
    assert_eq!(
        rows[0].cell_at(0),
        Some(Value::String("attached".to_owned()))
    );
}

/// Internal because prompt, nonblocking RAII cleanup is observable only in the
/// node's open-transaction owner, before any later database operation runs.
#[test]
fn dropping_owned_transactions_abandons_both_kinds_immediately_when_uncontended() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let mergeable = db.mergeable_tx().unwrap();
    let mergeable_id = mergeable.tx_id;
    let exclusive = db.exclusive_tx().unwrap();
    let exclusive_id = exclusive.tx_id;

    drop(mergeable);
    drop(exclusive);

    assert!(db.node.pending_transaction_abandonments.borrow().is_empty());
    for tx_id in [mergeable_id, exclusive_id] {
        let error = db.abandon_transaction_handle(tx_id).unwrap_err();
        assert!(error.message.contains("missing open transaction"));
    }
}

/// Internal because contention and queue ownership are below the public
/// transaction API; both RAII handle kinds must take the same nonblocking path.
#[test]
fn dropping_owned_transactions_while_node_is_locked_queues_both_abandonments() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let mergeable = db.mergeable_tx().unwrap();
    let mergeable_id = mergeable.tx_id;
    let exclusive = db.exclusive_tx().unwrap();
    let exclusive_id = exclusive.tx_id;

    block_on(async {
        let guard = db.node.node.lock().await;
        drop(mergeable);
        drop(exclusive);
        assert_eq!(db.node.pending_transaction_abandonments.borrow().len(), 2);
        drop(guard);

        db.tick().await.unwrap();
    });

    assert!(db.node.pending_transaction_abandonments.borrow().is_empty());
    for tx_id in [mergeable_id, exclusive_id] {
        let error = db.abandon_transaction_handle(tx_id).unwrap_err();
        assert!(error.message.contains("missing open transaction"));
    }
}

/// A waiter already queued on the node mutex must observe a handle's synchronous
/// tombstone before it can commit; the maintenance tick is deliberately never
/// driven in this receipt.
#[test]
fn dropped_handles_beat_commit_waiters_already_ahead_of_tick() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let mergeable = db.mergeable_tx().await.unwrap();
        let mergeable_id = mergeable.tx_id;
        mergeable
            .insert(
                "todos",
                doctest_support::todo_cells("mergeable abandoned", false),
                InsertOptions {
                    row_id: Some(row(0xd1)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let guard = db.node.node.lock().await;
        let mut mergeable_commit = Box::pin(db.commit_mergeable_handle(mergeable_id));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            mergeable_commit.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(mergeable);
        drop(guard);

        let mergeable_error = mergeable_commit.await.unwrap_err();
        assert_eq!(mergeable_error.code, ErrorCode::Protocol);
        assert!(mergeable_error.message.contains("was abandoned"));
        assert!(db.node.pending_transaction_abandonments.borrow().is_empty());

        let exclusive = db.exclusive_tx().await.unwrap();
        let exclusive_id = exclusive.tx_id;
        exclusive
            .insert(
                "todos",
                doctest_support::todo_cells("exclusive abandoned", false),
                InsertOptions {
                    row_id: Some(row(0xd2)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let guard = db.node.node.lock().await;
        let mut exclusive_commit = Box::pin(db.commit_exclusive_handle(exclusive_id));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            exclusive_commit.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(exclusive);
        drop(guard);

        let exclusive_error = exclusive_commit.await.unwrap_err();
        assert_eq!(exclusive_error.code, ErrorCode::Protocol);
        assert!(exclusive_error.message.contains("was abandoned"));
        assert!(db.node.pending_transaction_abandonments.borrow().is_empty());
    });
}

/// Internal because queued binding commits and RAII drops meet below the
/// public facade. The public receipt is the queued write handle's terminal
/// error after one owner tick.
#[test]
fn queued_commits_reject_handles_abandoned_while_the_node_is_locked() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let mergeable = db.mergeable_tx().await.unwrap();
        let mergeable_id = mergeable.tx_id;
        mergeable
            .insert(
                "todos",
                doctest_support::todo_cells("queued mergeable abandoned", false),
                InsertOptions {
                    row_id: Some(row(0xd3)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let node_owner = db.node.node.lock().await;
        let mergeable_commit = db.enqueue_commit_mergeable_handle(mergeable_id).unwrap();
        drop(mergeable);
        drop(node_owner);
        db.tick().await.unwrap();
        let mergeable_error = mergeable_commit
            .wait(DurabilityTier::Local)
            .await
            .unwrap_err();
        assert_eq!(mergeable_error.code, ErrorCode::Protocol);
        assert!(mergeable_error.message.contains("was abandoned"));
        // Settle the failed mergeable commit's idempotent cleanup before
        // placing the independent exclusive receipt on the FIFO owner.
        db.tick().await.unwrap();

        let exclusive = db.exclusive_tx().await.unwrap();
        let exclusive_id = exclusive.tx_id;
        exclusive
            .insert(
                "todos",
                doctest_support::todo_cells("queued exclusive abandoned", false),
                InsertOptions {
                    row_id: Some(row(0xd4)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let node_owner = db.node.node.lock().await;
        let exclusive_commit = db.enqueue_commit_exclusive_handle(exclusive_id).unwrap();
        drop(exclusive);
        drop(node_owner);
        db.tick().await.unwrap();
        let exclusive_error = exclusive_commit
            .wait(DurabilityTier::Local)
            .await
            .unwrap_err();
        assert_eq!(exclusive_error.code, ErrorCode::Protocol);
        assert!(exclusive_error.message.contains("was abandoned"));
    });
}

/// Internal because stale maintenance ids are deliberately absent from the
/// public API. A stale id at the head must not fail the tick or strand a later
/// live transaction.
#[test]
fn stale_transaction_abandonments_do_not_discard_later_queue_entries() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let already_terminal = db.mergeable_tx().unwrap();
    let already_terminal_id = already_terminal.tx_id;
    drop(already_terminal);
    let never_opened_id = OpenTransactionId::new();
    let live = db.exclusive_tx().unwrap();
    let live_id = live.tx_id;

    db.node
        .pending_transaction_abandonments
        .borrow_mut()
        .extend([already_terminal_id, never_opened_id, live_id]);

    block_on(db.tick()).unwrap();

    assert!(db.node.pending_transaction_abandonments.borrow().is_empty());
    let error = db.abandon_transaction_handle(live_id).unwrap_err();
    assert!(error.message.contains("missing open transaction"));
    drop(live);
}

/// Internal because this drives `close` to its first lock wait to make the
/// admission boundary deterministic. Queued drops before the boundary and
/// live handles dropped after it must all be terminal when close completes.
#[test]
fn close_gates_transaction_admission_and_terminalizes_close_races() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let queued_mergeable = db.mergeable_tx().unwrap();
    let queued_mergeable_id = queued_mergeable.tx_id;
    let queued_exclusive = db.exclusive_tx().unwrap();
    let queued_exclusive_id = queued_exclusive.tx_id;
    let late_mergeable = db.mergeable_tx().unwrap();
    let late_mergeable_id = late_mergeable.tx_id;
    let late_exclusive = db.exclusive_tx().unwrap();
    let late_exclusive_id = late_exclusive.tx_id;

    block_on(async {
        let guard = db.node.node.lock().await;
        drop(queued_mergeable);
        drop(queued_exclusive);
        assert_eq!(db.node.pending_transaction_abandonments.borrow().len(), 2);

        let mut closing = pin!(db.close());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(closing.as_mut().poll(&mut context), Poll::Pending));

        let mergeable_error = match db.mergeable_tx().await {
            Ok(_) => panic!("close must reject mergeable transaction admission"),
            Err(error) => error,
        };
        assert_eq!(mergeable_error.code, ErrorCode::WriteRejected);
        let exclusive_error = match db.exclusive_tx().await {
            Ok(_) => panic!("close must reject exclusive transaction admission"),
            Err(error) => error,
        };
        assert_eq!(exclusive_error.code, ErrorCode::Protocol);

        drop(late_mergeable);
        drop(late_exclusive);
        assert_eq!(
            db.node.pending_transaction_abandonments.borrow().len(),
            2,
            "late drops belong to close's terminal sweep, not the closed queue"
        );

        drop(guard);
        closing.await.unwrap();
    });

    assert!(db.node.pending_transaction_abandonments.borrow().is_empty());
    for tx_id in [
        queued_mergeable_id,
        queued_exclusive_id,
        late_mergeable_id,
        late_exclusive_id,
    ] {
        let error = db.abandon_transaction_handle(tx_id).unwrap_err();
        assert!(error.message.contains("missing open transaction"));
    }
}

#[derive(Clone, Copy, Debug)]
enum CloseCancellationWait {
    CloseOwner,
    QueuedMutationDrain,
    TransactionWaitObserverDrain,
    DeferredRejectionDiscard,
    TransactionSweep,
}

fn assert_cancelled_close_sweeps_transactions_at(wait: CloseCancellationWait) {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let mergeable = db.mergeable_tx().await.unwrap();
        let mergeable_id = mergeable.tx_id;
        let exclusive = db.exclusive_tx().await.unwrap();
        let exclusive_id = exclusive.tx_id;

        let mut observer_release = None;
        let mut queued_mutation_release = None;
        let mut node_guard = None;
        let mut close_owner_guard = None;
        match wait {
            CloseCancellationWait::CloseOwner => {
                close_owner_guard = Some(db.node.lock_close_owner().await);
            }
            CloseCancellationWait::QueuedMutationDrain => {
                let (release, blocked) = futures::channel::oneshot::channel();
                db.node
                    .enqueue_transaction_operation(
                        mergeable_id,
                        Box::pin(async {
                            blocked
                                .await
                                .expect("the cancelled close retains the queued mutation");
                            Ok(())
                        }),
                    )
                    .unwrap();
                queued_mutation_release = Some(release);
            }
            CloseCancellationWait::TransactionWaitObserverDrain => {
                let (release, blocked) = futures::channel::oneshot::channel();
                db.node
                    .enqueue_transaction_wait_observer_for_test(Box::pin(async move {
                        let _ = blocked.await;
                    }));
                observer_release = Some(release);
            }
            CloseCancellationWait::DeferredRejectionDiscard => {
                db.node.defer_rejection_discard_for_test(TxId::new(
                    TxTime::new(7_001, 9),
                    NodeUuid::from_bytes([0xd7; 16]),
                ));
                node_guard = Some(db.node.node.lock().await);
            }
            CloseCancellationWait::TransactionSweep => {
                node_guard = Some(db.node.node.lock().await);
            }
        }

        let mut closing = Box::pin(db.close());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(
            matches!(closing.as_mut().poll(&mut context), Poll::Pending),
            "close must suspend at {wait:?}",
        );

        let error = match db.mergeable_tx().await {
            Ok(_) => panic!("close must reject transaction admission at {wait:?}"),
            Err(error) => error,
        };
        assert_eq!(error.code, ErrorCode::WriteRejected);

        drop(closing);
        if let Some(release) = observer_release {
            release
                .send(())
                .expect("the pending observer remains owned by node maintenance");
        }
        if let Some(release) = queued_mutation_release {
            // Cancelling while close owns this FIFO lease intentionally leaves
            // the final sweep pending. Release the retained operation, then
            // let an ordinary maintenance turn drain it before asserting the
            // sweep. This preserves the pre-sweep cancellation coverage
            // without treating an active lease as quiescence.
            release
                .send(())
                .expect("the pending mutation remains owned by node maintenance");
        }
        drop(node_guard);
        drop(close_owner_guard);

        db.tick().await.unwrap();
        assert!(db.node.pending_transaction_abandonments.borrow().is_empty());
        for tx_id in [mergeable_id, exclusive_id] {
            let error = db.abandon_transaction_handle(tx_id).unwrap_err();
            assert!(
                error.message.contains("missing open transaction"),
                "{wait:?} cancellation left transaction {tx_id} open: {error}",
            );
        }

        drop(mergeable);
        drop(exclusive);
    });
}

/// Internal because deterministic suspension at each node-owner wait and the
/// resulting open-transaction owner state are not exposed by the public API.
/// Admission closes and node maintenance owns the sweep before any of these
/// boundaries can suspend.
#[test]
fn cancelled_close_sweeps_transactions_from_every_pre_sweep_wait() {
    for wait in [
        CloseCancellationWait::CloseOwner,
        CloseCancellationWait::QueuedMutationDrain,
        CloseCancellationWait::TransactionWaitObserverDrain,
        CloseCancellationWait::DeferredRejectionDiscard,
        CloseCancellationWait::TransactionSweep,
    ] {
        assert_cancelled_close_sweeps_transactions_at(wait);
    }
}

/// Internal because only the owner queue exposes the exact cancellation point
/// between dequeue and a cold operation's completion. Accepted FIFO work must
/// remain retained when an in-progress close future is dropped.
#[test]
fn cancelled_close_retains_the_cold_queued_owner_operation() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let (release, blocked) = futures::channel::oneshot::channel();
        let completed = Rc::new(Cell::new(false));
        let operation_completed = Rc::clone(&completed);
        db.node
            .enqueue_transaction_operation(
                OpenTransactionId::new(),
                Box::pin(async move {
                    blocked
                        .await
                        .expect("cancelled close must retain the queued operation");
                    operation_completed.set(true);
                    Ok(())
                }),
            )
            .unwrap();

        let mut closing = Box::pin(db.close());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(closing.as_mut().poll(&mut context), Poll::Pending));
        drop(closing);

        release
            .send(())
            .expect("close cancellation must not drop the queued operation");
        db.drive_queued_mutation_once();
        assert!(completed.get());
    });
}

/// Internal because cancellation must strand retained FIFO work between close
/// owners; the public API does not expose that handoff boundary.
#[test]
fn cancelled_close_handoff_is_coherent_with_concurrent_and_repeated_close() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let transaction = db.mergeable_tx().await.unwrap();
        let transaction_id = transaction.tx_id;
        let (release_retained_operation, retained_operation) = futures::channel::oneshot::channel();
        db.node
            .enqueue_transaction_operation(
                transaction_id,
                Box::pin(async move {
                    retained_operation
                        .await
                        .expect("the cancelled close retains accepted FIFO work");
                    Ok(())
                }),
            )
            .unwrap();

        let queued_transaction_id = OpenTransactionId::new();
        db.enqueue_begin_mergeable(queued_transaction_id, None, None)
            .unwrap();
        db.enqueue_transaction_insert(
            queued_transaction_id,
            false,
            "todos".to_owned(),
            doctest_support::todo_cells("accepted before close", false),
            InsertOptions {
                row_id: Some(row(0xd8)),
                ..Default::default()
            },
        )
        .unwrap();
        let queued_commit = db
            .enqueue_commit_mergeable_handle(queued_transaction_id)
            .unwrap();

        let mut cancelled = Box::pin(db.close());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(cancelled);

        // Cancellation retains the cold head; release it before asking a new
        // close owner to finish the same FIFO sequence.
        release_retained_operation
            .send(())
            .expect("retained owner operation remains live after cancellation");

        let (first, second) = futures::future::join(db.close(), db.close()).await;
        first.unwrap();
        second.unwrap();
        db.close().await.unwrap();
        assert_eq!(
            queued_commit.wait(DurabilityTier::Local).await.unwrap(),
            queued_commit.mergeable_tx_id(),
            "concurrent close owners must drain FIFO work accepted before shutdown",
        );

        let error = db.abandon_transaction_handle(transaction_id).unwrap_err();
        assert!(error.message.contains("missing open transaction"));
        drop(transaction);
    });
}

/// Internal because this fixes the exact owner-queue interleaving that public
/// bindings cannot hold deterministically: close is cancelled while a cold
/// predecessor is retained, then an ordinary tick runs before the accepted
/// stage/commit sequence has acquired the node lock.
#[test]
fn cancelled_close_tick_does_not_tombstone_a_later_admitted_mergeable_commit() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let (release_head, cold_head) = futures::channel::oneshot::channel();
        db.node
            .enqueue_transaction_operation(
                OpenTransactionId::new(),
                Box::pin(async move {
                    cold_head
                        .await
                        .expect("cancelled close retains its cold FIFO predecessor");
                    Ok(())
                }),
            )
            .unwrap();

        let transaction = db.mergeable_tx().await.unwrap();
        let tx_id = transaction.tx_id;
        db.enqueue_transaction_insert(
            tx_id,
            false,
            "todos".to_owned(),
            doctest_support::todo_cells("accepted before cancelled close", false),
            InsertOptions {
                row_id: Some(row(0xd9)),
                ..Default::default()
            },
        )
        .unwrap();
        let commit = db.enqueue_commit_mergeable_handle(tx_id).unwrap();

        let mut cancelled = Box::pin(db.close());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(cancelled);

        // An ordinary tick while the cold predecessor is still retained must
        // not consume shutdown's final transaction sweep.
        db.tick().await.unwrap();
        release_head
            .send(())
            .expect("the cold predecessor remains retained after cancellation");

        // Complete the predecessor, then run the queued insert. The remaining
        // commit proves that the already-open transaction was not swept between
        // those FIFO entries.
        db.tick().await.unwrap();
        db.tick().await.unwrap();
        db.close().await.unwrap();
        assert_eq!(
            commit.wait(DurabilityTier::Local).await.unwrap(),
            commit.mergeable_tx_id(),
            "shutdown must drain the accepted mergeable sequence rather than tombstoning it"
        );
        drop(transaction);
    });
}

/// Internal because the accepted begin is intentionally queued behind a cold
/// predecessor. If an intervening tick consumes the shutdown sweep early, a
/// later close has no way to retire the transaction that this begin opens.
#[test]
fn completed_close_sweeps_begin_admitted_before_cancelled_close() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let (release_head, cold_head) = futures::channel::oneshot::channel();
        db.node
            .enqueue_transaction_operation(
                OpenTransactionId::new(),
                Box::pin(async move {
                    cold_head
                        .await
                        .expect("cancelled close retains its cold FIFO predecessor");
                    Ok(())
                }),
            )
            .unwrap();
        let queued_tx = OpenTransactionId::new();
        db.enqueue_begin_mergeable(queued_tx, None, None).unwrap();

        let mut cancelled = Box::pin(db.close());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            cancelled.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(cancelled);

        db.tick().await.unwrap();
        release_head.send(()).unwrap();
        db.tick().await.unwrap();
        db.tick().await.unwrap();

        db.close().await.unwrap();
        let error = db
            .abandon_transaction_handle(queued_tx)
            .expect_err("completed close must retire the queued begin it admitted");
        assert!(error.message.contains("missing open transaction"));
    });
}

/// A live close owns the cold queue head in a [`QueuedMutationLease`], so the
/// queue itself is temporarily empty. A concurrent maintenance turn must not
/// mistake that gap for shutdown quiescence: the admitted begin still has to
/// run before the final open-transaction sweep.
#[test]
fn live_close_does_not_sweep_a_cold_admitted_begin_before_its_lease_drops() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();

    block_on(async {
        let queued_tx = OpenTransactionId::new();
        let queued_db = db.clone_for_owner_operation();
        let (release_begin, cold_begin) = futures::channel::oneshot::channel();
        db.node
            .enqueue_transaction_operation(
                queued_tx,
                Box::pin(async move {
                    cold_begin
                        .await
                        .expect("live close retains the cold admitted begin");
                    queued_db.begin_mergeable(queued_tx).await
                }),
            )
            .unwrap();

        let mut closing = Box::pin(db.close());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(closing.as_mut().poll(&mut context), Poll::Pending));

        // This is the concurrent tick/maintenance interleaving. Before the
        // active-lease guard, queue emptiness alone consumed shutdown here.
        db.node
            .finish_transaction_abandonment_shutdown()
            .await
            .unwrap();
        assert!(db.node.transaction_abandonment_shutdown_is_pending());

        release_begin.send(()).unwrap();
        closing.await.unwrap();

        let error = db
            .abandon_transaction_handle(queued_tx)
            .expect_err("the resumed close must sweep the begin it admitted");
        assert!(error.message.contains("missing open transaction"));
    });
}

#[test]
fn mergeable_overlay_uses_staged_provenance_and_preserves_it_at_commit() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let existing = row(0xa1);
    db.insert(
        "todos",
        doctest_support::todo_cells("existing", false),
        crate::db::InsertOptions {
            row_id: Some(existing),
            updated_at_ms: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    let inserted = row(0xa2);
    let tx = db.mergeable_tx().unwrap();
    tx.insert(
        "todos",
        doctest_support::todo_cells("inserted", false),
        crate::db::InsertOptions {
            row_id: Some(inserted),
            updated_at_ms: Some(200),
            ..Default::default()
        },
    )
    .unwrap();
    tx.update(
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        crate::db::UpdateOptions {
            updated_at_ms: Some(300),
            ..Default::default()
        },
    )
    .unwrap();
    let query = db
        .prepare_query(
            &db.table("todos")
                .select(["title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();

    let overlay = tx.all_prepared(&query).unwrap();
    let repeated = tx.all_prepared(&query).unwrap();
    assert_eq!(overlay, repeated, "transaction provenance must be stable");
    let inserted_overlay = overlay
        .iter()
        .find(|row| row.row_uuid() == inserted)
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    assert_eq!(inserted_overlay.created_at, 200);
    assert_eq!(inserted_overlay.updated_at, 200);
    assert_eq!(inserted_overlay.created_by, db.identity.author);
    let updated_overlay = overlay
        .iter()
        .find(|row| row.row_uuid() == existing)
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    assert_eq!(updated_overlay.created_at, 100);
    assert_eq!(updated_overlay.updated_at, 300);
    assert_eq!(updated_overlay.updated_by, db.identity.author);

    tx.commit().unwrap();
    let committed = db.read(&query).unwrap();
    for (row_id, staged) in [(inserted, inserted_overlay), (existing, updated_overlay)] {
        let committed = committed
            .iter()
            .find(|row| row.row_uuid() == row_id)
            .unwrap()
            .provenance()
            .unwrap()
            .unwrap();
        assert_eq!(committed.created_by, staged.created_by);
        assert_eq!(committed.created_at, staged.created_at);
        assert_eq!(committed.updated_by, staged.updated_by);
        assert_eq!(committed.updated_at, staged.updated_at);
    }
}

#[test]
fn exclusive_overlay_reserves_stable_provenance_for_insert_and_update() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let existing = row(0xb1);
    db.insert(
        "todos",
        doctest_support::todo_cells("existing", false),
        crate::db::InsertOptions {
            row_id: Some(existing),
            updated_at_ms: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    let inserted = row(0xb2);
    let tx = db.exclusive_tx().unwrap();
    tx.insert(
        "todos",
        doctest_support::todo_cells("inserted", false),
        crate::db::InsertOptions {
            row_id: Some(inserted),
            ..Default::default()
        },
    )
    .unwrap();
    tx.update(
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )
    .unwrap();
    let query = db
        .prepare_query(
            &db.table("todos")
                .select(["title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();
    let overlay = tx.all_prepared(&query).unwrap();
    let repeated = tx.all_prepared(&query).unwrap();
    assert_eq!(overlay, repeated, "exclusive provenance must be stable");
    let provenance = |rows: &[CurrentRow], id| {
        rows.iter()
            .find(|row| row.row_uuid() == id)
            .unwrap()
            .provenance()
            .unwrap()
            .unwrap()
    };
    let inserted_overlay = provenance(&overlay, inserted);
    let updated_overlay = provenance(&overlay, existing);
    assert_ne!(inserted_overlay.created_at, 0);
    assert_eq!(inserted_overlay.created_at, inserted_overlay.updated_at);
    assert_eq!(updated_overlay.created_at, 100);
    assert_ne!(updated_overlay.updated_at, 0);

    tx.commit().unwrap();
    let committed = db.read(&query).unwrap();
    assert_eq!(provenance(&committed, inserted), inserted_overlay);
    assert_eq!(provenance(&committed, existing), updated_overlay);
}

#[test]
fn exclusive_crud_preserves_explicit_updated_at() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let inserted = row(0xc1);
    let upserted = row(0xc2);
    let deleted = row(0xc3);
    let restored = row(0xc4);

    for (row, title, updated_at_ms) in [
        (upserted, "upsert base", 10),
        (deleted, "delete base", 20),
        (restored, "restore base", 30),
    ] {
        db.insert(
            "todos",
            doctest_support::todo_cells(title, false),
            InsertOptions {
                row_id: Some(row),
                updated_at_ms: Some(updated_at_ms),
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.delete(
        "todos",
        restored,
        DeleteOptions {
            updated_at_ms: Some(40),
            ..Default::default()
        },
    )
    .unwrap();

    let tx = db.exclusive_tx().unwrap();
    tx.insert(
        "todos",
        doctest_support::todo_cells("inserted", false),
        InsertOptions {
            row_id: Some(inserted),
            updated_at_ms: Some(100),
            ..Default::default()
        },
    )
    .unwrap();
    tx.upsert(
        "todos",
        upserted,
        BTreeMap::from([("title".to_owned(), Value::String("upserted".to_owned()))]),
        UpsertOptions {
            updated_at_ms: Some(200),
            ..Default::default()
        },
    )
    .unwrap();
    tx.delete(
        "todos",
        deleted,
        DeleteOptions {
            updated_at_ms: Some(300),
            ..Default::default()
        },
    )
    .unwrap();
    tx.restore(
        "todos",
        restored,
        Some(doctest_support::todo_cells("restored", true)),
        RestoreOptions {
            updated_at_ms: Some(400),
            ..Default::default()
        },
    )
    .unwrap();
    tx.commit().unwrap();

    let query = db
        .prepare_query(
            &db.table("todos")
                .select(["title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();
    let rows = block_on(db.all(
        &query,
        ReadOpts {
            include_deleted: true,
            ..Default::default()
        },
    ))
    .unwrap();
    let updated_at = |row_id| {
        rows.iter()
            .find(|row| row.row_uuid() == row_id)
            .unwrap()
            .provenance()
            .unwrap()
            .unwrap()
            .updated_at
    };

    assert_eq!(updated_at(inserted), 100);
    assert_eq!(updated_at(upserted), 200);
    assert_eq!(updated_at(deleted), 300);
    assert_eq!(updated_at(restored), 400);
}

#[test]
fn out_of_range_explicit_timestamp_is_rejected_before_mutating() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let row_id = row(0xc5);
    let result = block_on(db.insert(
        "todos",
        doctest_support::todo_cells("must not be written", false),
        InsertOptions {
            row_id: Some(row_id),
            updated_at_ms: Some(crate::time::HLC_MAX_PHYSICAL_MS + 1),
            ..Default::default()
        },
    ));
    let error = match result {
        Ok(_) => panic!("a timestamp outside the HLC physical range must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::WriteRejected);

    let query = db.prepare_query(&db.table("todos")).unwrap();
    assert!(
        db.read(&query).unwrap().is_empty(),
        "rejected timestamp input must not leave a visible row"
    );
}

/// This stays internal because transaction overlays are not sync-visible. The
/// point, table, and prepared-query paths are separate overlay consumers, so
/// exercise all three with the same row UUID present in two logical tables.
#[test]
fn exclusive_tx_overlay_scopes_same_row_uuid_by_table() {
    fn table_schema(name: &str) -> PublicTableSchemaBuilder {
        PublicTableSchemaBuilder::new(name)
            .column("status", PublicColumnType::Text)
            .column("value", PublicColumnType::Text)
    }

    fn cells(status: &str, value: &str) -> RowCells {
        BTreeMap::from([
            ("status".to_owned(), Value::String(status.to_owned())),
            ("value".to_owned(), Value::String(value.to_owned())),
        ])
    }

    fn visible_cells(
        rows: Vec<CurrentRow>,
        table_name: &str,
        table: &TableSchema,
    ) -> BTreeMap<RowUuid, RowCells> {
        rows.into_iter()
            .map(|row| {
                assert_eq!(row.table(), table_name);
                (
                    row.row_uuid(),
                    BTreeMap::from([
                        (
                            "status".to_owned(),
                            row.cell(table, "status").expect("status cell"),
                        ),
                        (
                            "value".to_owned(),
                            row.cell(table, "value").expect("value cell"),
                        ),
                    ]),
                )
            })
            .collect()
    }

    fn assert_reads<T>(
        tx: &T,
        table_name: &str,
        table: &TableSchema,
        filtered: &PreparedQuery,
        shared_row: RowUuid,
        expected_shared: &RowCells,
        expected_all: BTreeMap<RowUuid, RowCells>,
    ) where
        T: ExclusiveTxOps<RocksDbStorage>,
    {
        assert_eq!(
            tx.read(table_name, shared_row).unwrap().as_ref(),
            Some(expected_shared)
        );
        assert_eq!(
            visible_cells(tx.all(table_name).unwrap(), table_name, table),
            expected_all
        );
        assert_eq!(
            visible_cells(tx.all_prepared(filtered).unwrap(), table_name, table),
            BTreeMap::from([(shared_row, expected_shared.clone())])
        );
    }

    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(table_schema("table_a"))
            .table(table_schema("table_b")),
    );
    let db = open_db(0x5e, AuthorSubject::SYSTEM, &schema);
    let table_a = schema
        .tables
        .iter()
        .find(|table| table.name == "table_a")
        .unwrap();
    let table_b = schema
        .tables
        .iter()
        .find(|table| table.name == "table_b")
        .unwrap();
    let shared_row = row(0x44);
    let other_a = row(0xa1);
    let other_b = row(0xb1);
    let current_a = cells("selected", "table A current");
    let current_b = cells("selected", "table B current");
    let nonmatching_a = cells("ignored", "table A nonmatching");
    let nonmatching_b = cells("ignored", "table B nonmatching");

    for (table, row_uuid, row_cells) in [
        ("table_a", shared_row, current_a.clone()),
        ("table_a", other_a, nonmatching_a.clone()),
        ("table_b", shared_row, current_b.clone()),
        ("table_b", other_b, nonmatching_b.clone()),
    ] {
        let write = db
            .insert(
                table,
                row_cells,
                crate::db::InsertOptions {
                    row_id: Some(row_uuid),
                    ..Default::default()
                },
            )
            .unwrap();
        block_on(write.wait(DurabilityTier::Local)).unwrap();
    }

    let filtered_a = db
        .prepare_query(
            &db.table("table_a")
                .filter(eq(col("status"), lit("selected"))),
        )
        .unwrap();
    let filtered_b = db
        .prepare_query(
            &db.table("table_b")
                .filter(eq(col("status"), lit("selected"))),
        )
        .unwrap();
    let tx = db.exclusive_tx().unwrap();
    let pending_a = cells("selected", "table A pending");
    tx.insert(
        "table_a",
        pending_a.clone(),
        crate::db::InsertOptions {
            row_id: Some(shared_row),
            ..Default::default()
        },
    )
    .unwrap();

    assert_reads(
        &tx,
        "table_b",
        table_b,
        &filtered_b,
        shared_row,
        &current_b,
        BTreeMap::from([
            (shared_row, current_b.clone()),
            (other_b, nonmatching_b.clone()),
        ]),
    );

    let pending_b = cells("selected", "table B pending");
    tx.insert(
        "table_b",
        pending_b.clone(),
        crate::db::InsertOptions {
            row_id: Some(shared_row),
            ..Default::default()
        },
    )
    .unwrap();
    assert_reads(
        &tx,
        "table_a",
        table_a,
        &filtered_a,
        shared_row,
        &pending_a,
        BTreeMap::from([(shared_row, pending_a.clone()), (other_a, nonmatching_a)]),
    );
    assert_reads(
        &tx,
        "table_b",
        table_b,
        &filtered_b,
        shared_row,
        &pending_b,
        BTreeMap::from([(shared_row, pending_b.clone()), (other_b, nonmatching_b)]),
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
        Default::default(),
    )
    .unwrap();
    db.upsert(
        "todos",
        existing,
        BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    db.upsert(
        "todos",
        absent,
        BTreeMap::from([("title".to_owned(), Value::String("created".to_owned()))]),
        Default::default(),
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
    let tx = db.mergeable_tx().unwrap();

    tx.insert(
        "todos",
        doctest_support::todo_cells("one", false),
        crate::db::InsertOptions {
            row_id: Some(row_one),
            ..Default::default()
        },
    )
    .unwrap();
    tx.insert(
        "todos",
        doctest_support::todo_cells("two", true),
        crate::db::InsertOptions {
            row_id: Some(row_two),
            ..Default::default()
        },
    )
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
    let tx = db.mergeable_tx().unwrap();

    tx.insert(
        "todos",
        doctest_support::todo_cells("draft", false),
        crate::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
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
fn mergeable_tx_rejects_update_of_committed_deleted_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let row = row(1);
    db.insert(
        "todos",
        doctest_support::todo_cells("archived", false),
        crate::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();
    db.delete("todos", row, Default::default()).unwrap();

    let tx = db.mergeable_tx().unwrap();
    let error = tx
        .update(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap_err();
    assert_eq!(error.code, crate::db::ErrorCode::WriteRejected);
    assert!(prepared_read(&db, &db.table("todos")).is_empty());
}

#[test]
fn mergeable_tx_coalesces_restore_then_update_for_same_row() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);

    db.insert(
        "todos",
        doctest_support::todo_cells("archived", false),
        crate::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();
    db.delete("todos", row, Default::default()).unwrap();
    assert!(prepared_read(&db, &db.table("todos")).is_empty());

    let tx = db.mergeable_tx().unwrap();
    tx.restore(
        "todos",
        row,
        Some(doctest_support::todo_cells("restored", false)),
        Default::default(),
    )
    .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
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
    let tx = db.mergeable_tx().unwrap();

    tx.insert(
        "todos",
        doctest_support::todo_cells("first", false),
        crate::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
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

    db.insert(
        "todos",
        doctest_support::todo_cells("base", false),
        crate::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();
    let tx = db.mergeable_tx().unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("ignored".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    tx.delete("todos", row, Default::default()).unwrap();
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
fn mergeable_tx_and_ref_have_identical_restore_and_reinsert_results() {
    let builder = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let handle = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let restored = row(1);
    let reinserted = row(2);

    for db in [&builder, &handle] {
        db.insert(
            "todos",
            doctest_support::todo_cells("archived", false),
            crate::db::InsertOptions {
                row_id: Some(restored),
                ..Default::default()
            },
        )
        .unwrap();
        db.delete("todos", restored, Default::default()).unwrap();
        db.insert(
            "todos",
            doctest_support::todo_cells("original", false),
            crate::db::InsertOptions {
                row_id: Some(reinserted),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let builder_tx = builder.mergeable_tx().unwrap();
    builder_tx
        .restore(
            "todos",
            restored,
            Some(doctest_support::todo_cells("restored", false)),
            Default::default(),
        )
        .unwrap();
    builder_tx
        .update(
            "todos",
            restored,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    builder_tx
        .delete("todos", reinserted, Default::default())
        .unwrap();
    builder_tx
        .insert(
            "todos",
            doctest_support::todo_cells("reinserted", true),
            crate::db::InsertOptions {
                row_id: Some(reinserted),
                ..Default::default()
            },
        )
        .unwrap();
    builder_tx.commit().unwrap();

    let open_tx = OpenTransactionId::new();
    handle.begin_mergeable(open_tx).unwrap();
    {
        let tx = handle.mergeable_tx_ref(open_tx);
        tx.restore(
            "todos",
            restored,
            Some(doctest_support::todo_cells("restored", false)),
            Default::default(),
        )
        .unwrap();
        tx.update(
            "todos",
            restored,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
        tx.delete("todos", reinserted, Default::default()).unwrap();
        tx.insert(
            "todos",
            doctest_support::todo_cells("reinserted", true),
            crate::db::InsertOptions {
                row_id: Some(reinserted),
                ..Default::default()
            },
        )
        .unwrap();
    }
    handle.commit_mergeable_handle(open_tx).unwrap();

    let read_state = |db: &Db<_>| {
        let query = db.prepare_query(&db.table("todos")).unwrap();
        doctest_support::block_on(db.all(
            &query,
            ReadOpts {
                include_deleted: true,
                ..ReadOpts::default()
            },
        ))
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.row_uuid(),
                (
                    row.is_deleted(),
                    row.cell(table, "title"),
                    row.cell(table, "done"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>()
    };

    let builder_state = read_state(&builder);
    let handle_state = read_state(&handle);
    assert_eq!(builder_state, handle_state);
    assert_eq!(
        builder_state.get(&restored),
        Some(&(
            false,
            Some(Value::String("restored".to_owned())),
            Some(Value::Bool(true)),
        ))
    );
    assert_eq!(
        builder_state.get(&reinserted),
        Some(&(
            true,
            Some(Value::String("reinserted".to_owned())),
            Some(Value::Bool(true)),
        ))
    );
}

#[test]
fn mergeable_tx_read_observes_its_staged_restore() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let row = row(1);

    db.insert(
        "todos",
        doctest_support::todo_cells("archived", false),
        crate::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();
    db.delete("todos", row, Default::default()).unwrap();

    let tx = db.mergeable_tx().unwrap();
    tx.restore(
        "todos",
        row,
        Some(doctest_support::todo_cells("restored", false)),
        Default::default(),
    )
    .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )
    .unwrap();

    assert_eq!(
        tx.read("todos", row).unwrap(),
        Some(doctest_support::todo_cells("restored", true))
    );
}

#[test]
fn exclusive_tx_ref_survives_handle_reconstruction_until_explicit_commit() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let row = row(1);

    db.insert(
        "todos",
        doctest_support::todo_cells("base", false),
        crate::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();

    let open_tx = OpenTransactionId::new();
    db.begin_exclusive(open_tx).unwrap();
    {
        let tx = db.exclusive_tx_ref(open_tx);
        assert_eq!(
            tx.read("todos", row).unwrap(),
            Some(doctest_support::todo_cells("base", false))
        );
        tx.update(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    }
    db.commit_exclusive_handle(open_tx).unwrap();

    let current = prepared_one(&db, &db.table("todos")).unwrap();
    assert_eq!(
        current.cell(table, "title"),
        Some(Value::String("base".to_owned()))
    );
    assert_eq!(current.cell(table, "done"), Some(Value::Bool(true)));
}

/// An exclusive transaction binds alice at begin: its staged read cannot be
/// re-authorized as bob, while the handle commit consumes that bound identity.
#[test]
fn identity_bound_exclusive_transaction_rejects_cross_identity_reads_and_commits_as_bound_author() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let alice = AuthorSubject::for_test_bytes([0xc1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let open = OpenTransactionId::new();
    let row = row(0xa1);
    assert_ne!(alice, db.identity.author);
    let prepared = db
        .prepare_query(
            &db.table("todos")
                .select(["title", "$createdBy", "$updatedBy"]),
        )
        .unwrap();

    db.begin_exclusive_for_identity(open, alice).unwrap();
    db.exclusive_tx_ref(open)
        .insert(
            "todos",
            doctest_support::todo_cells("alice", false),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();

    // Planted positive: the bound identity can read the transaction overlay.
    let staged = db
        .exclusive_tx_ref(open)
        .all_prepared_for_identity(&prepared, alice)
        .unwrap();
    assert_eq!(staged.len(), 1);
    let staged_provenance = staged[0].provenance().unwrap().unwrap();
    assert_eq!(staged_provenance.created_by, alice);
    assert_eq!(staged_provenance.updated_by, alice);
    assert!(matches!(
        doctest_support::block_on(
            db.exclusive_tx_ref(open)
                .all_prepared_for_identity(&prepared, bob),
        ),
        Err(error) if error.code == ErrorCode::Protocol
    ));

    db.commit_exclusive_handle(open).unwrap();
    let committed = prepared_one(
        &db,
        &db.table("todos")
            .select(["title", "$createdBy", "$updatedBy"]),
    )
    .unwrap();
    assert_eq!(committed.row_uuid(), row);
    let committed_provenance = committed.provenance().unwrap().unwrap();
    assert_eq!(committed_provenance.created_by, alice);
    assert_eq!(committed_provenance.updated_by, alice);
}

fn exclusive_read_for_write_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("owner", &["claims", "sub"]))
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True),
                ),
        ),
    )
}

fn private_grant_read_for_write_schema() -> JazzSchema {
    let grant = PublicPolicyExpr::Exists {
        table: "grants".to_owned(),
        condition: Box::new(PublicPolicyExpr::and(vec![
            public_outer_eq("doc_id", "id"),
            public_session_eq("subject", &["claims", "sub"]),
        ])),
    };
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(grant)
                            .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("grants")
                    .fk_column("doc_id", "docs")
                    .column("subject", PublicColumnType::Uuid),
            ),
    )
}

/// A replica may have a complete document preimage but not the private grant
/// that proves its reader identity. It must still stage a mergeable UPDATE;
/// only the connected fate authority decides the read-for-write rule. This is
/// deliberately an end-to-end transport test rather than a local policy unit:
/// the observable contract is pending locally, then Accepted or
/// AuthorizationDenied without exposing the grant.
///
/// ```text
/// client (doc, no grants) ──stage UPDATE──► authority (doc, private grant)
///                                           ├── alice grant: Accepted
///                                           └── bob no grant: AuthorizationDenied
/// ```
#[test]
fn mergeable_read_for_write_is_decided_only_by_the_authority() {
    let schema = private_grant_read_for_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xd8; 16]);
    let bob = AuthorSubject::for_test_bytes([0xd9; 16]);
    let target = row(0xca);
    let server = open_core(0xda, AuthorSubject::SYSTEM, &schema);
    server
        .insert_with_id(
            "docs",
            target,
            BTreeMap::from([("title".to_owned(), Value::String("original".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "grants",
            row(0xcb),
            BTreeMap::from([
                ("doc_id".to_owned(), Value::Uuid(target.0)),
                ("subject".to_owned(), Value::Uuid(alice.test_uuid())),
            ]),
        )
        .unwrap();

    let alice_client = open_db(0xdb, alice, &schema);
    let bob_client = open_db(0xdc, bob, &schema);
    alice_client.set_test_provider_claims(alice, test_provider_claims(alice));
    bob_client.set_test_provider_claims(bob, test_provider_claims(bob));

    // Seed only the complete target preimage on each client. Neither client
    // receives or creates the authority's private `grants` support row.
    for client in [&alice_client, &bob_client] {
        client
            .insert(
                "docs",
                BTreeMap::from([("title".to_owned(), Value::String("original".to_owned()))]),
                InsertOptions {
                    row_id: Some(target),
                    identity: WriteIdentity::Session(AuthorSubject::SYSTEM),
                    ..Default::default()
                },
            )
            .unwrap();
    }

    let (alice_transport, alice_server_transport) = duplex();
    let _alice_upstream = block_on(alice_client.connect_upstream(alice_transport));
    let _alice_subscriber = server.accept_subscriber_with_claims(
        alice_server_transport,
        alice,
        test_provider_claims(alice),
    );
    let (bob_transport, bob_server_transport) = duplex();
    let _bob_upstream = block_on(bob_client.connect_upstream(bob_transport));
    let _bob_subscriber =
        server.accept_subscriber_with_claims(bob_server_transport, bob, test_provider_claims(bob));

    // Settle the SYSTEM setup writes before the session cases. The grants
    // remain authority-only because docs policy narrows ordinary delivery.
    for client in [&alice_client, &bob_client] {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
    }

    let stage_update = |client: &Db<RocksDbStorage>, author, use_upsert| {
        block_on(client.transaction_for_identity(author, async |tx| {
            let cells = BTreeMap::from([("title".to_owned(), Value::String("edited".to_owned()))]);
            if use_upsert {
                tx.upsert("docs", target, cells, Default::default()).await
            } else {
                tx.update("docs", target, cells, Default::default()).await
            }
        }))
        .expect("client staging uses the retained preimage, not local policy support")
        .1
    };

    let alice_tx = stage_update(&alice_client, alice, false);
    assert!(matches!(
        alice_client.write_state(alice_tx).unwrap(),
        WriteState {
            fate: Fate::Pending,
            ..
        }
    ));
    alice_client.tick().unwrap();
    server.tick().unwrap();
    alice_client.tick().unwrap();
    assert!(matches!(
        alice_client.write_state(alice_tx).unwrap(),
        WriteState {
            fate: Fate::Accepted,
            ..
        }
    ));

    let bob_tx = stage_update(&bob_client, bob, true);
    assert!(matches!(
        bob_client.write_state(bob_tx).unwrap(),
        WriteState {
            fate: Fate::Pending,
            ..
        }
    ));
    bob_client.tick().unwrap();
    server.tick().unwrap();
    bob_client.tick().unwrap();
    assert!(matches!(
        bob_client.write_state(bob_tx).unwrap(),
        WriteState {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));
    let docs = server.read(&Query::from("docs")).unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(
        docs[0].cell(&schema.tables[0], "title"),
        Some(Value::String("edited".to_owned())),
        "the denied update must not overwrite the authority's accepted result"
    );
}

/// Alice's exclusive transaction cannot update or upsert Bob's read-hidden
/// snapshot row. Full and partial updates return the same non-disclosing error.
///
/// alice tx ──full/partial UPDATE or UPSERT──► bob row ──► denied
#[test]
fn exclusive_session_mutations_deny_hidden_existing_targets_without_disclosure() {
    let schema = exclusive_read_for_write_schema();
    let db = open_db(0xd4, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa4; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb4; 16]);
    let target = row(0xc4);
    db.set_test_provider_claims(alice, test_provider_claims(alice));
    db.insert(
        "todos",
        cells("bob secret", false, bob),
        InsertOptions {
            row_id: Some(target),
            ..Default::default()
        },
    )
    .unwrap();
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    assert!(
        block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice))
            .unwrap()
            .is_empty(),
        "the planted target must be read-hidden from Alice"
    );

    for (label, patch) in [
        ("full", cells("replacement", true, alice)),
        (
            "partial",
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        ),
    ] {
        let open = OpenTransactionId::new();
        db.begin_exclusive_for_identity(open, alice).unwrap();
        let error = db
            .exclusive_tx_ref(open)
            .update("todos", target, patch, Default::default())
            .unwrap_err();
        assert_eq!(error.code, ErrorCode::WriteRejected, "{label} update");
        assert_eq!(
            error.message,
            "read policy denied UPDATE on table todos: the operation requires read permission on the target row"
        );
        db.abandon_exclusive_handle(open).unwrap();
    }

    let open = OpenTransactionId::new();
    db.begin_exclusive_for_identity(open, alice).unwrap();
    let error = db
        .exclusive_tx_ref(open)
        .upsert(
            "todos",
            target,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert_eq!(
        error.message,
        "read policy denied UPSERT on table todos: the operation requires read permission on the target row"
    );
    db.abandon_exclusive_handle(open).unwrap();
}

/// Exclusive upsert distinguishes hidden-existing from absent internally: an
/// absent row is inserted, while an intervening insert conflicts with the
/// recorded absence rather than silently overwriting it.
///
/// alice tx ──upsert(absent)──► overlay; concurrent insert ──► commit conflict
#[test]
fn exclusive_session_absent_upsert_records_absence_and_observes_its_overlay() {
    let schema = exclusive_read_for_write_schema();
    let db = open_db(0xd5, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa5; 16]);
    db.set_test_provider_claims(alice, test_provider_claims(alice));

    let successful = row(0xc5);
    let success_open = OpenTransactionId::new();
    db.begin_exclusive_for_identity(success_open, alice)
        .unwrap();
    let success = db.exclusive_tx_ref(success_open);
    success
        .upsert(
            "todos",
            successful,
            cells("new", false, alice),
            Default::default(),
        )
        .unwrap();
    success
        .update(
            "todos",
            successful,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .expect("the session can read and update its visible overlay row");
    db.commit_exclusive_handle(success_open).unwrap();

    let conflicted = row(0xc6);
    let conflict_open = OpenTransactionId::new();
    db.begin_exclusive_for_identity(conflict_open, alice)
        .unwrap();
    db.exclusive_tx_ref(conflict_open)
        .upsert(
            "todos",
            conflicted,
            cells("pending", false, alice),
            Default::default(),
        )
        .unwrap();
    db.insert(
        "todos",
        cells("concurrent", false, alice),
        InsertOptions {
            row_id: Some(conflicted),
            ..Default::default()
        },
    )
    .unwrap();
    let error = db.commit_exclusive_handle(conflict_open).unwrap_err();
    assert_eq!(error.code, ErrorCode::TransactionConflict);
}

/// Read-for-update authorization uses the exclusive transaction's fixed
/// snapshot. A concurrent owner change cannot retroactively hide the snapshot
/// row, but the recorded row/predicate reads make commit fail.
///
/// alice tx snapshot(readable) ──concurrent owner change──► stage ✓, commit ✗
#[test]
fn exclusive_session_update_authorizes_snapshot_then_conflicts_on_toctou_change() {
    let schema = exclusive_read_for_write_schema();
    let db = open_db(0xd6, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa6; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb6; 16]);
    let target = row(0xc7);
    db.set_test_provider_claims(alice, test_provider_claims(alice));
    db.insert(
        "todos",
        cells("snapshot", false, alice),
        InsertOptions {
            row_id: Some(target),
            ..Default::default()
        },
    )
    .unwrap();

    let open = OpenTransactionId::new();
    db.begin_exclusive_for_identity(open, alice).unwrap();
    db.update(
        "todos",
        target,
        BTreeMap::from([("owner".to_owned(), Value::Uuid(bob.test_uuid()))]),
        Default::default(),
    )
    .unwrap();
    db.exclusive_tx_ref(open)
        .update(
            "todos",
            target,
            BTreeMap::from([("title".to_owned(), Value::String("staged".to_owned()))]),
            Default::default(),
        )
        .expect("the fixed snapshot still exposes Alice's target");
    let error = db.commit_exclusive_handle(open).unwrap_err();
    assert_eq!(error.code, ErrorCode::TransactionConflict);
}

/// A session-authored mergeable transaction stages later mutations from its
/// fixed overlay, not only from the pre-transaction current state. Policy
/// authorization is deferred to the fate authority.
///
/// alice tx: INSERT visible row ──UPDATE/UPSERT──► same staged row
#[test]
fn mergeable_session_mutations_observe_visible_rows_in_their_overlay() {
    let schema = exclusive_read_for_write_schema();
    let db = open_db(0xd7, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa7; 16]);
    db.set_test_provider_claims(alice, test_provider_claims(alice));
    let target = row(0xc8);
    let open = OpenTransactionId::new();
    db.begin_mergeable_for_identity(open, alice).unwrap();
    let tx = db.mergeable_tx_ref(open);

    tx.insert(
        "todos",
        cells("draft", false, alice),
        InsertOptions {
            row_id: Some(target),
            ..Default::default()
        },
    )
    .unwrap();
    tx.update(
        "todos",
        target,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )
    .expect("the staged insert is visible to its session author");
    tx.upsert(
        "todos",
        target,
        BTreeMap::from([("title".to_owned(), Value::String("ready".to_owned()))]),
        Default::default(),
    )
    .expect("upsert observes and merges the visible staged row");

    db.commit_mergeable_handle(open).unwrap();
    let committed = db.local_current_row("todos", target).unwrap().unwrap();
    let table = &schema.tables[0];
    assert_eq!(committed.cell(table, "done"), Some(Value::Bool(true)));
    assert_eq!(
        committed.cell(table, "title"),
        Some(Value::String("ready".to_owned()))
    );
}

/// A mergeable transaction opened for alice is an identity capability: its
/// serving reads cannot be re-authorized as bob.
#[test]
fn identity_bound_mergeable_transaction_rejects_cross_identity_reads() {
    let schema = owner_read_schema();
    let db = open_db(0xd3, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa3; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb3; 16]);
    let open = OpenTransactionId::new();
    let prepared = db
        .prepare_query(&db.table("todos").filter(eq(
            col("owner"),
            claim(crate::query::provider_claim_key("sub")),
        )))
        .unwrap();
    db.set_test_provider_claims(alice, test_provider_claims(alice));
    db.set_test_provider_claims(bob, test_provider_claims(bob));
    let alice_row = row(0xa3);
    let bob_row = row(0xb3);
    db.insert(
        "todos",
        cells("alice", false, alice),
        InsertOptions {
            row_id: Some(alice_row),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        cells("bob", false, bob),
        InsertOptions {
            row_id: Some(bob_row),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(
        block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice))
            .unwrap()
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![alice_row]
    );

    db.begin_mergeable_for_identity(open, alice).unwrap();
    let alice_rows = doctest_support::block_on(
        db.mergeable_tx_ref(open)
            .all_prepared_for_identity(&prepared, alice),
    )
    .unwrap();
    assert_eq!(
        alice_rows
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![alice_row]
    );
    assert!(matches!(
        doctest_support::block_on(
            db.mergeable_tx_ref(open)
                .all_prepared_for_identity(&prepared, bob),
        ),
        Err(error) if error.code == ErrorCode::Protocol
    ));
    db.abandon_transaction_handle(open).unwrap();
}

#[test]
fn exclusive_tx_rejects_conflicting_concurrent_update() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
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
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
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
    assert_eq!(err.code, ErrorCode::TransactionConflict);
    assert!(err.message.contains("visible parent changed"));
    assert_eq!(
        core.one(&core.table("todos"))
            .unwrap()
            .unwrap()
            .cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

#[test]
fn mergeable_tx_emits_one_subscription_delta_for_many_writes() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert!(opened_rows(doctest_support::block_on(subscription.next_raw()).unwrap()).is_empty());

    let tx = db.mergeable_tx().unwrap();
    for index in 0..100u8 {
        tx.insert(
            "todos",
            doctest_support::todo_cells(&format!("todo {index}"), false),
            crate::db::InsertOptions {
                row_id: Some(RowUuid::from_bytes([index + 1; 16])),
                ..Default::default()
            },
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 100);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert!(subscription.try_next_event().is_none());
}
