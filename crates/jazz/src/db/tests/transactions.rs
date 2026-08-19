//! Mergeable and exclusive transaction staging, coalescing, provenance, and conflicts.

use super::*;

#[test]
fn attached_schema_mergeable_batch_is_queryable_after_owner_commit() {
    let empty = JazzSchema::new([]);
    let refs = empty.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let owner = block_on(Db::open_history_complete(DbConfig {
        schema: empty,
        storage: doctest_support::MemoryStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x91; 16]),
            author: AuthorId::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(91))),
    }))
    .unwrap();
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )]);
    let view = owner.register_schema_view(schema.clone()).unwrap();
    let open = OpenTransactionId::new();
    owner.begin_mergeable(open).unwrap();
    let inserted = row(0x91);
    view.mergeable_tx_ref(open)
        .insert_with_id_at_ms(
            "todos",
            inserted,
            doctest_support::todo_cells("attached", false),
            1_704_067_200_123,
        )
        .unwrap();
    owner.commit_mergeable_handle(open).unwrap();

    // Advance the owner's canonical schema after the query view was registered.
    // The historical view still calls this column `title`; resolving projection
    // against the canonical schema would silently omit it after the rename.
    let renamed_schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("summary", ColumnType::String),
        ],
    )]);
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
        .insert_with_id_at_ms(
            "todos",
            overlay_inserted,
            doctest_support::todo_cells("overlay", true),
            1_704_067_200_456,
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
    assert_eq!(overlay_provenance.created_at, TxTime(1_704_067_200_456));
    assert_eq!(overlay_provenance.updated_at, TxTime(1_704_067_200_456));
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
    db.insert_with_id_at_ms(
        "todos",
        existing,
        doctest_support::todo_cells("existing", false),
        100,
    )
    .unwrap();
    let inserted = row(0xa2);
    let tx = db.mergeable_tx().unwrap();
    tx.insert_with_id_at_ms(
        "todos",
        inserted,
        doctest_support::todo_cells("inserted", false),
        200,
    )
    .unwrap();
    tx.update_at_ms(
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        300,
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
    assert_eq!(inserted_overlay.created_at, TxTime(200));
    assert_eq!(inserted_overlay.updated_at, TxTime(200));
    assert_eq!(inserted_overlay.created_by, db.identity.author);
    let updated_overlay = overlay
        .iter()
        .find(|row| row.row_uuid() == existing)
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    assert_eq!(updated_overlay.created_at, TxTime(100));
    assert_eq!(updated_overlay.updated_at, TxTime(300));
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
    db.insert_with_id_at_ms(
        "todos",
        existing,
        doctest_support::todo_cells("existing", false),
        100,
    )
    .unwrap();
    let inserted = row(0xb2);
    let tx = db.exclusive_tx().unwrap();
    tx.insert_with_id(
        "todos",
        inserted,
        doctest_support::todo_cells("inserted", false),
    )
    .unwrap();
    tx.update(
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
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
    assert_ne!(inserted_overlay.created_at, TxTime(0));
    assert_eq!(inserted_overlay.created_at, inserted_overlay.updated_at);
    assert_eq!(updated_overlay.created_at, TxTime(100));
    assert_ne!(updated_overlay.updated_at, TxTime(0));

    tx.commit().unwrap();
    let committed = db.read(&query).unwrap();
    assert_eq!(provenance(&committed, inserted), inserted_overlay);
    assert_eq!(provenance(&committed, existing), updated_overlay);
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
    let tx = db.mergeable_tx().unwrap();

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
    let tx = db.mergeable_tx().unwrap();

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

    let tx = db.mergeable_tx().unwrap();
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
    let tx = db.mergeable_tx().unwrap();

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
    let tx = db.mergeable_tx().unwrap();
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
fn mergeable_tx_and_ref_have_identical_restore_and_reinsert_results() {
    let builder = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let handle = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let table = &doctest_support::schema().tables[0];
    let restored = row(1);
    let reinserted = row(2);

    for db in [&builder, &handle] {
        db.insert_with_id(
            "todos",
            restored,
            doctest_support::todo_cells("archived", false),
        )
        .unwrap();
        db.delete("todos", restored).unwrap();
        db.insert_with_id(
            "todos",
            reinserted,
            doctest_support::todo_cells("original", false),
        )
        .unwrap();
    }

    let builder_tx = builder.mergeable_tx().unwrap();
    builder_tx
        .restore(
            "todos",
            restored,
            doctest_support::todo_cells("restored", false),
        )
        .unwrap();
    builder_tx
        .update(
            "todos",
            restored,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    builder_tx.delete("todos", reinserted).unwrap();
    builder_tx
        .insert_with_id(
            "todos",
            reinserted,
            doctest_support::todo_cells("reinserted", true),
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
            doctest_support::todo_cells("restored", false),
        )
        .unwrap();
        tx.update(
            "todos",
            restored,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
        tx.delete("todos", reinserted).unwrap();
        tx.insert_with_id(
            "todos",
            reinserted,
            doctest_support::todo_cells("reinserted", true),
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

    db.insert_with_id("todos", row, doctest_support::todo_cells("archived", false))
        .unwrap();
    db.delete("todos", row).unwrap();

    let tx = db.mergeable_tx().unwrap();
    tx.restore("todos", row, doctest_support::todo_cells("restored", false))
        .unwrap();
    tx.update(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
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

    db.insert_with_id("todos", row, doctest_support::todo_cells("base", false))
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
        tx.insert_with_id(
            "todos",
            RowUuid::from_bytes([index + 1; 16]),
            doctest_support::todo_cells(&format!("todo {index}"), false),
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
