//! Mergeable and exclusive transaction staging, coalescing, provenance, and conflicts.

use super::*;

#[test]
fn attached_schema_mergeable_batch_is_queryable_after_owner_commit() {
    let mut empty = JazzSchema::new([]);
    let mut refs = empty.column_families();
    let mut refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let mut owner = block_on(Db::open_history_complete(DbConfig {
        schema: empty,
        storage: doctest_support::MemoryStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x91; 16]),
            author: AuthorId::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(91))),
    }))
    .unwrap();
    let mut schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )]);
    let mut view = block_on(owner.register_schema_view(schema.clone())).unwrap();
    let mut open = OpenBatchId::new();
    {
        let mut attached = owner.view(&view).unwrap();
        block_on(attached.begin_mergeable(open, None)).unwrap();
        let mut inserted = row(0x91);
        block_on(attached.mergeable_insert(
            open,
            "todos",
            inserted,
            doctest_support::todo_cells("attached", false),
            Some(1_704_067_200_123),
        ))
        .unwrap();
    }
    let mut inserted = row(0x91);
    block_on(owner.commit_mergeable(open)).unwrap();

    // Advance the owner's canonical schema after the query view was registered.
    // The historical view still calls this column `title`; resolving projection
    // against the canonical schema would silently omit it after the rename.
    let mut renamed_schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("summary", ColumnType::String),
        ],
    )]);
    let mut renamed = SchemaVersion::new(renamed_schema);
    block_on(owner.publish_schema_with_lens(
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
    ))
    .unwrap();
    block_on(owner.set_current_write_schema(CurrentWriteSchema {
        revision: 2,
        schema: renamed.id,
    }))
    .unwrap();

    let mut overlay_open = OpenBatchId::new();
    let mut overlay_inserted = row(0x93);
    let (prepared, overlay_rows) = {
        let mut attached = owner.view(&view).unwrap();
        block_on(attached.begin_mergeable(overlay_open, None)).unwrap();
        block_on(attached.mergeable_insert(
            overlay_open,
            "todos",
            overlay_inserted,
            doctest_support::todo_cells("overlay", true),
            Some(1_704_067_200_456),
        ))
        .unwrap();
        let mut prepared = attached
            .prepare_query(&attached.table("todos").select([
                "done",
                "title",
                "$createdAt",
                "$updatedAt",
            ]))
            .unwrap();
        let mut rows =
            block_on(attached.transaction_all(overlay_open, &prepared, ReadOpts::default()))
                .unwrap();
        (prepared, rows)
    };
    let mut overlay_row = overlay_rows
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
    let mut overlay_provenance = overlay_row.provenance().unwrap().unwrap();
    assert_eq!(overlay_provenance.created_at, TxTime(1_704_067_200_456));
    assert_eq!(overlay_provenance.updated_at, TxTime(1_704_067_200_456));
    owner.abandon_mergeable(overlay_open).unwrap();

    let mut rows = {
        let mut attached = owner.view(&view).unwrap();
        block_on(attached.all(&prepared, ReadOpts::default())).unwrap()
    };
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
    let mut db = block_on(doctest_support::open_todos_db()).unwrap();
    let mut existing = row(0xa1);
    db.insert_with_id_at_ms(
        "todos",
        existing,
        doctest_support::todo_cells("existing", false),
        100,
    )
    .unwrap();
    let mut inserted = row(0xa2);
    let mut author = db.identity.author;
    let mut tx = block_on(db.begin_mergeable()).unwrap();
    let mut view_token = db.default_view();
    let mut view = db.view(&view_token).unwrap();
    block_on(view.mergeable_insert(
        tx,
        "todos",
        inserted,
        doctest_support::todo_cells("inserted", false),
        Some(200),
    ))
    .unwrap();
    block_on(view.mergeable_update(
        tx,
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Some(300),
    ))
    .unwrap();
    let mut query = view
        .prepare_query(
            &view
                .table("todos")
                .select(["title", "$createdAt", "$updatedAt"]),
        )
        .unwrap();

    let mut overlay = block_on(view.transaction_all(tx, &query, ReadOpts::default())).unwrap();
    let mut repeated = block_on(view.transaction_all(tx, &query, ReadOpts::default())).unwrap();
    assert_eq!(overlay, repeated, "transaction provenance must be stable");
    let mut inserted_overlay = overlay
        .iter()
        .find(|row| row.row_uuid() == inserted)
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    assert_eq!(inserted_overlay.created_at, TxTime(200));
    assert_eq!(inserted_overlay.updated_at, TxTime(200));
    assert_eq!(inserted_overlay.created_by, author);
    let mut updated_overlay = overlay
        .iter()
        .find(|row| row.row_uuid() == existing)
        .unwrap()
        .provenance()
        .unwrap()
        .unwrap();
    assert_eq!(updated_overlay.created_at, TxTime(100));
    assert_eq!(updated_overlay.updated_at, TxTime(300));
    assert_eq!(updated_overlay.updated_by, author);

    drop(view);
    block_on(db.commit_mergeable(tx)).unwrap();
    let mut committed = block_on(db.read(&query)).unwrap();
    for (row_id, staged) in [(inserted, inserted_overlay), (existing, updated_overlay)] {
        let mut committed = committed
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
    let mut db = block_on(doctest_support::open_todos_db()).unwrap();
    let mut existing = row(0xb1);
    db.insert_with_id_at_ms(
        "todos",
        existing,
        doctest_support::todo_cells("existing", false),
        100,
    )
    .unwrap();
    let mut inserted = row(0xb2);
    let mut tx = block_on(db.begin_exclusive()).unwrap();
    block_on(db.exclusive_insert(
        tx,
        "todos",
        inserted,
        doctest_support::todo_cells("inserted", false),
    ))
    .unwrap();
    block_on(db.exclusive_update(
        tx,
        "todos",
        existing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();
    let mut query = db
        .prepare_query(&Query::from("todos").select(["title", "$createdAt", "$updatedAt"]))
        .unwrap();
    let mut overlay = block_on(db.transaction_all(tx, &query, ReadOpts::default())).unwrap();
    let mut repeated = block_on(db.transaction_all(tx, &query, ReadOpts::default())).unwrap();
    assert_eq!(overlay, repeated, "exclusive provenance must be stable");
    let mut provenance = |rows: &[CurrentRow], id| {
        rows.iter()
            .find(|row| row.row_uuid() == id)
            .unwrap()
            .provenance()
            .unwrap()
            .unwrap()
    };
    let mut inserted_overlay = provenance(&overlay, inserted);
    let mut updated_overlay = provenance(&overlay, existing);
    assert_ne!(inserted_overlay.created_at, TxTime(0));
    assert_eq!(inserted_overlay.created_at, inserted_overlay.updated_at);
    assert_eq!(updated_overlay.created_at, TxTime(100));
    assert_ne!(updated_overlay.updated_at, TxTime(0));

    block_on(db.commit_exclusive(tx)).unwrap();
    let mut committed = block_on(db.read(&query)).unwrap();
    assert_eq!(provenance(&committed, inserted), inserted_overlay);
    assert_eq!(provenance(&committed, existing), updated_overlay);
}

#[test]
fn upsert_merges_existing_rows_but_writes_absent_rows_directly() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut table = &doctest_support::schema().tables[0];
    let mut existing = row(1);
    let mut absent = row(2);

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

    let mut query = db.table("todos");
    let mut rows = prepared_read(&mut db, &query)
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
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut table = &doctest_support::schema().tables[0];
    let mut row_one = row(1);
    let mut row_two = row(2);
    let tx = block_on(db.begin_mergeable()).unwrap();
    block_on(db.mergeable_insert(
        tx,
        "todos",
        row_one,
        doctest_support::todo_cells("one", false),
    ))
    .unwrap();
    block_on(db.mergeable_insert(
        tx,
        "todos",
        row_two,
        doctest_support::todo_cells("two", true),
    ))
    .unwrap();
    let tx_id = block_on(db.commit_mergeable(tx)).unwrap();

    let mut rows = prepared_read(&mut db, &Query::from("todos"))
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
    let mut unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 2);
    assert_eq!(versions.len(), 2);
}

#[test]
fn mergeable_tx_coalesces_insert_then_update_for_same_row() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut table = &doctest_support::schema().tables[0];
    let mut row = row(1);
    let tx = block_on(db.begin_mergeable()).unwrap();
    block_on(db.mergeable_insert(
        tx,
        "todos",
        row,
        doctest_support::todo_cells("draft", false),
    ))
    .unwrap();
    block_on(db.mergeable_update(
        tx,
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();
    let tx_id = block_on(db.commit_mergeable(tx)).unwrap();

    let mut row_after = prepared_one(&mut db, &Query::from("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("draft".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let mut unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_coalesces_restore_then_update_for_same_row() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut table = &doctest_support::schema().tables[0];
    let mut row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("archived", false))
        .unwrap();
    db.delete("todos", row).unwrap();
    assert!(prepared_read(&mut db, &Query::from("todos")).is_empty());

    let tx = block_on(db.begin_mergeable()).unwrap();
    block_on(db.mergeable_restore(
        tx,
        "todos",
        row,
        doctest_support::todo_cells("restored", false),
    ))
    .unwrap();
    block_on(db.mergeable_update(
        tx,
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();
    let tx_id = block_on(db.commit_mergeable(tx)).unwrap();

    let mut row_after = prepared_one(&mut db, &Query::from("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("restored".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let mut unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
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
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut table = &doctest_support::schema().tables[0];
    let mut row = row(1);
    let tx = block_on(db.begin_mergeable()).unwrap();
    block_on(db.mergeable_insert(
        tx,
        "todos",
        row,
        doctest_support::todo_cells("first", false),
    ))
    .unwrap();
    block_on(db.mergeable_update(
        tx,
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
    ))
    .unwrap();
    block_on(db.mergeable_update(
        tx,
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();
    let tx_id = block_on(db.commit_mergeable(tx)).unwrap();

    let mut row_after = prepared_one(&mut db, &Query::from("todos")).unwrap();
    assert_eq!(row_after.row_uuid(), row);
    assert_eq!(
        row_after.cell(table, "title"),
        Some(Value::String("second".to_owned()))
    );
    assert_eq!(row_after.cell(table, "done"), Some(Value::Bool(true)));

    let mut unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_coalesces_update_then_delete_for_same_row() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("base", false))
        .unwrap();
    let tx = block_on(db.begin_mergeable()).unwrap();
    block_on(db.mergeable_update(
        tx,
        "todos",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("ignored".to_owned()))]),
    ))
    .unwrap();
    block_on(db.mergeable_delete(tx, "todos", row)).unwrap();
    let tx_id = block_on(db.commit_mergeable(tx)).unwrap();

    assert!(prepared_read(&mut db, &Query::from("todos")).is_empty());
    let mut unit = db.node.node.borrow_mut().commit_unit_for(tx_id).unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert_eq!(tx.n_total_writes, 1);
    assert_eq!(versions.len(), 1);
}

#[test]
fn mergeable_tx_and_ref_have_identical_restore_and_reinsert_results() {
    let mut builder = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut handle = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut table = &doctest_support::schema().tables[0];
    let mut restored = row(1);
    let mut reinserted = row(2);

    for db in [&mut builder, &mut handle] {
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

    let builder_tx = block_on(builder.begin_mergeable()).unwrap();
    block_on(builder.mergeable_restore(
        builder_tx,
        "todos",
        restored,
        doctest_support::todo_cells("restored", false),
    ))
    .unwrap();
    block_on(builder.mergeable_update(
        builder_tx,
        "todos",
        restored,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();
    block_on(builder.mergeable_delete(builder_tx, "todos", reinserted)).unwrap();
    block_on(builder.mergeable_insert(
        builder_tx,
        "todos",
        reinserted,
        doctest_support::todo_cells("reinserted", true),
    ))
    .unwrap();
    block_on(builder.commit_mergeable(builder_tx)).unwrap();

    let mut open_tx = OpenBatchId::new();
    block_on(handle.begin_mergeable_with_id(open_tx)).unwrap();
    block_on(handle.mergeable_restore(
        open_tx,
        "todos",
        restored,
        doctest_support::todo_cells("restored", false),
    ))
    .unwrap();
    block_on(handle.mergeable_update(
        open_tx,
        "todos",
        restored,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();
    block_on(handle.mergeable_delete(open_tx, "todos", reinserted)).unwrap();
    block_on(handle.mergeable_insert(
        open_tx,
        "todos",
        reinserted,
        doctest_support::todo_cells("reinserted", true),
    ))
    .unwrap();
    block_on(handle.commit_mergeable(open_tx)).unwrap();

    let mut read_state = |db: &mut Db| {
        let mut query = db.prepare_query(&Query::from("todos")).unwrap();
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

    let mut builder_state = read_state(&mut builder);
    let mut handle_state = read_state(&mut handle);
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
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("archived", false))
        .unwrap();
    db.delete("todos", row).unwrap();

    let tx = block_on(db.begin_mergeable()).unwrap();
    block_on(db.mergeable_restore(
        tx,
        "todos",
        row,
        doctest_support::todo_cells("restored", false),
    ))
    .unwrap();
    block_on(db.mergeable_update(
        tx,
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    let rows = block_on(db.transaction_all(tx, &prepared, ReadOpts::default())).unwrap();
    let restored = rows.into_iter().next().unwrap();
    let table = &doctest_support::schema().tables[0];
    assert_eq!(
        restored.cell(table, "title"),
        Some(Value::String("restored".to_owned()))
    );
    assert_eq!(restored.cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn exclusive_tx_ref_survives_handle_reconstruction_until_explicit_commit() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut table = &doctest_support::schema().tables[0];
    let mut row = row(1);

    db.insert_with_id("todos", row, doctest_support::todo_cells("base", false))
        .unwrap();

    let open_tx = OpenBatchId::new();
    block_on(db.begin_exclusive_with_id(open_tx)).unwrap();
    assert_eq!(
        block_on(db.exclusive_read(open_tx, "todos", row)).unwrap(),
        Some(doctest_support::todo_cells("base", false))
    );
    block_on(db.exclusive_update(
        open_tx,
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    ))
    .unwrap();
    block_on(db.commit_exclusive(open_tx)).unwrap();

    let mut current = prepared_one(&mut db, &Query::from("todos")).unwrap();
    assert_eq!(
        current.cell(table, "title"),
        Some(Value::String("base".to_owned()))
    );
    assert_eq!(current.cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn exclusive_tx_rejects_conflicting_concurrent_update() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut table = &schema.tables[0];
    let mut row = row(1);

    core.insert_with_id("todos", row, cells("base", false, owner))
        .unwrap();
    let mut first = core.exclusive_tx().unwrap();
    let mut second = core.exclusive_tx().unwrap();
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

    let mut err = second.commit().unwrap_err();

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
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut core = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut table = &schema.tables[0];
    let mut row = row(1);

    core.insert_with_id("todos", row, cells("base", false, owner))
        .unwrap();

    let mut first = core.exclusive_tx().unwrap();
    let mut second = core.exclusive_tx().unwrap();
    first
        .insert_with_id("todos", row, cells("first", false, owner))
        .unwrap();
    second
        .insert_with_id("todos", row, cells("second", false, owner))
        .unwrap();

    first.commit().unwrap();
    let mut err = second.commit().unwrap_err();
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
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut query = db.table("todos");
    let mut prepared_query = prepared(&mut db, &query);
    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert!(opened_rows(doctest_support::block_on(subscription.next_raw()).unwrap()).is_empty());

    let tx = block_on(db.begin_mergeable()).unwrap();
    for index in 0..100u8 {
        block_on(db.mergeable_insert(
            tx,
            "todos",
            RowUuid::from_bytes([index + 1; 16]),
            doctest_support::todo_cells(&format!("todo {index}"), false),
        ))
        .unwrap();
    }
    block_on(db.commit_mergeable(tx)).unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 100);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert!(subscription.try_next_event().is_none());
}
