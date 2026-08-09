fn mergeable_open_test_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("note", ColumnType::String),
        ],
    )])
}

fn mergeable_open_cells(
    title: impl Into<String>,
    note: impl Into<String>,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("note".to_owned(), Value::String(note.into())),
    ])
}

#[test]
fn mergeable_open_commit_matches_replayed_mergeable_batch_with_intervening_writes() {
    // This proof is internal by necessity: public reads expose the resulting row
    // state, but not exact batch ids, version ordering, parent vectors,
    // per-version provenance times, or permission subjects.
    let node_uuid = node(0x61);
    let schema = mergeable_open_test_schema();
    let (_actual_dir, mut actual) = open_node_with_schema(node_uuid, schema.clone());
    let (_expected_dir, mut expected) = open_node_with_schema(node_uuid, schema);
    let updated = row(1);
    let deleted = row(2);
    let restored = row(3);
    let inserted = row(4);
    let inserted_then_deleted = row(5);

    for core in [&mut actual, &mut expected] {
        core.commit_mergeable(
            MergeableCommit::new("todos", updated, 10)
                .cells(mergeable_open_cells("base", "base-note")),
        )
        .unwrap();
        core.commit_mergeable(
            MergeableCommit::new("todos", deleted, 11)
                .cells(mergeable_open_cells("delete-me", "delete-note")),
        )
        .unwrap();
        core.commit_mergeable(
            MergeableCommit::new("todos", restored, 12)
                .cells(mergeable_open_cells("archived", "archive-note")),
        )
        .unwrap();
        core.commit_mergeable(
            MergeableCommit::new("todos", restored, 13).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    }

    let author = user(0x71);
    let open_tx = actual.open_mergeable(author, Some(author)).unwrap();
    actual
        .tx_write_mergeable(
            open_tx,
            "todos",
            inserted,
            mergeable_open_cells("inserted", "initial-note"),
            None,
            Vec::new(),
            Some(101),
            false,
        )
        .unwrap();
    actual
        .tx_patch_mergeable(
            open_tx,
            "todos",
            inserted,
            BTreeMap::from([("note".to_owned(), Value::String("updated-note".to_owned()))]),
            None,
        )
        .unwrap();
    actual
        .tx_patch_mergeable(
            open_tx,
            "todos",
            updated,
            BTreeMap::from([("title".to_owned(), Value::String("pending".to_owned()))]),
            Some(103),
        )
        .unwrap();
    actual
        .tx_write_mergeable(
            open_tx,
            "todos",
            deleted,
            BTreeMap::new(),
            Some(DeletionEvent::Deleted),
            Vec::new(),
            Some(104),
            false,
        )
        .unwrap();

    let staged_content_parents = actual
        .local_content_winner_tx_id("todos", restored)
        .unwrap()
        .into_iter()
        .collect();
    let staged_deletion_parents = actual
        .local_deletion_winner_tx_id("todos", restored)
        .unwrap()
        .into_iter()
        .collect();
    actual
        .tx_write_mergeable(
            open_tx,
            "todos",
            restored,
            mergeable_open_cells("restored", "restored-note"),
            None,
            staged_content_parents,
            Some(105),
            true,
        )
        .unwrap();
    actual
        .tx_write_mergeable(
            open_tx,
            "todos",
            restored,
            BTreeMap::new(),
            Some(DeletionEvent::Restored),
            staged_deletion_parents,
            Some(105),
            true,
        )
        .unwrap();
    actual
        .tx_write_mergeable(
            open_tx,
            "todos",
            inserted_then_deleted,
            mergeable_open_cells("doomed", "doomed-note"),
            None,
            Vec::new(),
            Some(106),
            false,
        )
        .unwrap();
    actual
        .tx_write_mergeable(
            open_tx,
            "todos",
            inserted_then_deleted,
            BTreeMap::new(),
            Some(DeletionEvent::Deleted),
            Vec::new(),
            None,
            false,
        )
        .unwrap();

    assert_eq!(
        actual.tx_read(open_tx, "todos", inserted).unwrap(),
        Some(mergeable_open_cells("inserted", "updated-note"))
    );
    assert_eq!(
        actual.tx_read(open_tx, "todos", updated).unwrap(),
        Some(mergeable_open_cells("pending", "base-note"))
    );
    assert_eq!(
        actual.tx_read(open_tx, "todos", restored).unwrap(),
        Some(mergeable_open_cells("restored", "restored-note"))
    );
    assert_eq!(actual.tx_read(open_tx, "todos", deleted).unwrap(), None);
    assert_eq!(
        actual
            .tx_read(open_tx, "todos", inserted_then_deleted)
            .unwrap(),
        None
    );

    let mut intervening_content = None;
    let mut intervening_deletion = None;
    for core in [&mut actual, &mut expected] {
        core.commit_mergeable(
            MergeableCommit::new("todos", updated, 110)
                .cells(mergeable_open_cells("external", "external-note")),
        )
        .unwrap();
        let content = core
            .commit_mergeable(
                MergeableCommit::new("todos", restored, 111)
                    .cells(mergeable_open_cells("external-archive", "external-archive-note")),
            )
            .unwrap();
        let deletion_tx = core
            .commit_mergeable(
                MergeableCommit::new("todos", restored, 112)
                    .deletion(DeletionEvent::Deleted),
            )
            .unwrap();
        intervening_content = Some(content);
        intervening_deletion = Some(deletion_tx);
    }
    let intervening_content = intervening_content.unwrap();
    let intervening_deletion = intervening_deletion.unwrap();

    let expected_commits = vec![
        MergeableCommit::new("todos", inserted, 200)
            .made_by(author)
            .permission_subject(author)
            .cells(mergeable_open_cells("inserted", "updated-note")),
        MergeableCommit::new("todos", updated, 103)
            .made_by(author)
            .permission_subject(author)
            .authored_columns(BTreeSet::from(["title".to_owned()]))
            .cells(mergeable_open_cells("pending", "external-note")),
        MergeableCommit::new("todos", deleted, 104)
            .made_by(author)
            .permission_subject(author)
            .deletion(DeletionEvent::Deleted),
        MergeableCommit::new("todos", restored, 105)
            .made_by(author)
            .permission_subject(author)
            .parents(vec![intervening_content])
            .cells(mergeable_open_cells("restored", "restored-note")),
        MergeableCommit::new("todos", restored, 105)
            .made_by(author)
            .permission_subject(author)
            .parents(vec![intervening_deletion])
            .deletion(DeletionEvent::Restored),
        MergeableCommit::new("todos", inserted_then_deleted, 201)
            .made_by(author)
            .permission_subject(author)
            .deletion(DeletionEvent::Deleted),
    ];
    let expected_tx = expected.commit_mergeable_many(expected_commits).unwrap();
    let mut fallback_now_ms = 200;
    let actual_tx = actual
        .commit_mergeable_open(open_tx, || {
            let now_ms = fallback_now_ms;
            fallback_now_ms += 1;
            now_ms
        })
        .unwrap();

    assert_eq!(actual_tx, expected_tx, "batch ids must match");
    assert_eq!(
        actual.commit_unit_for(actual_tx).unwrap(),
        expected.commit_unit_for(expected_tx).unwrap(),
        "mergeable-open lowering must match the replayed commit batch exactly"
    );
}

#[test]
fn abandoning_mergeable_open_transaction_discards_its_only_staged_representation() {
    let (_temp_dir, mut core) = open_node();
    let staged = row(0x31);
    let open_tx = core.open_mergeable(AuthorId::SYSTEM, None).unwrap();
    core.tx_write_mergeable(
        open_tx,
        "todos",
        staged,
        title_cells("staged"),
        None,
        Vec::new(),
        Some(50),
        false,
    )
    .unwrap();

    assert_eq!(
        core.tx_read(open_tx, "todos", staged).unwrap(),
        Some(title_cells("staged"))
    );
    core.abandon_tx(open_tx).unwrap();

    assert!(
        core.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        core.commit_mergeable_open(open_tx, || 51).unwrap_err(),
        Error::MissingOpenTx(missing) if missing == open_tx
    ));
}
