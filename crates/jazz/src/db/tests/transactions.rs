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
    owner
        .publish_schema_with_lens(
            2,
            SchemaLineagePublication::new(
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
                ),
                Vec::<String>::new(),
                Vec::<String>::new(),
            ),
        )
        .unwrap();
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

/// A session-authored mergeable transaction authorizes later mutations from
/// its fixed overlay, not only from the pre-transaction current state.
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

    // A different visible row must not satisfy the targeted policy proof for
    // a staged row that Alice cannot read.
    let hidden = row(0xc9);
    let hidden_open = OpenTransactionId::new();
    db.begin_mergeable_for_identity(hidden_open, alice).unwrap();
    let hidden_tx = db.mergeable_tx_ref(hidden_open);
    hidden_tx
        .insert(
            "todos",
            cells("hidden", false, AuthorSubject::for_test_bytes([0xb7; 16])),
            InsertOptions {
                row_id: Some(hidden),
                ..Default::default()
            },
        )
        .unwrap();
    for (label, error) in [
        (
            "update",
            hidden_tx
                .update(
                    "todos",
                    hidden,
                    BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
                    Default::default(),
                )
                .expect_err("hidden overlay UPDATE must require visibility"),
        ),
        (
            "upsert",
            hidden_tx
                .upsert(
                    "todos",
                    hidden,
                    BTreeMap::from([("title".to_owned(), Value::String("nope".to_owned()))]),
                    Default::default(),
                )
                .expect_err("hidden overlay UPSERT must require visibility"),
        ),
    ] {
        assert_eq!(error.code, ErrorCode::WriteRejected, "{label}");
        assert_eq!(
            error.message,
            format!(
                "read policy denied {} on table todos: the operation requires read permission on the target row",
                label.to_ascii_uppercase()
            )
        );
    }
    db.commit_mergeable_handle(hidden_open).unwrap();
}

/// Mergeable serving reads retain their existing per-call identity semantics.
#[test]
fn mergeable_transaction_identity_reads_are_not_forced_to_begin_author() {
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
    let bob_rows = doctest_support::block_on(
        db.mergeable_tx_ref(open)
            .all_prepared_for_identity(&prepared, bob),
    )
    .unwrap();
    assert_eq!(
        alice_rows
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![alice_row]
    );
    assert_eq!(
        bob_rows
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![bob_row]
    );
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
