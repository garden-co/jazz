//! Local deltas and branch opening, isolation, reconnect, and teardown.

use super::*;

#[test]
fn local_subscription_emits_removed_row_for_fire_and_forget_delete() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0x31; 16]);
    let db = open_db(0x31, owner, &schema);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let row_id = row(0x31);
    db.insert(
        "todos",
        cells("delete me", false, owner),
        crate::db::InsertOptions {
            row_id: Some(row_id),
            ..Default::default()
        },
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![row_id]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete("todos", row_id, Default::default()).unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![row_id]
    );
}

#[test]
fn one_shot_and_subscription_rows_keep_identical_record_descriptors() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0x32; 16]);
    let db = open_db(0x32, owner, &schema);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();

    let row_id = row(0x32);
    db.insert(
        "todos",
        BTreeMap::from([
            (
                "title".to_owned(),
                Value::String("descriptor parity".to_owned()),
            ),
            ("done".to_owned(), Value::Bool(false)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row_id),
            ..Default::default()
        },
    )
    .unwrap();
    let (added, _, _) = delta_rows(block_on(subscription.next_raw()).unwrap());
    let one_shot = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(added.len(), 1);
    assert_eq!(one_shot.len(), 1);
    let table = &schema.tables[0];
    assert_eq!(
        added[0].cell(&table, "title"),
        Some(Value::String("descriptor parity".to_owned()))
    );
    assert_eq!(added[0].cell(&table, "done"), Some(Value::Bool(false)));
    assert_eq!(added[0].encoded_record(), one_shot[0].encoded_record());
}

#[test]
fn session_scoped_subscription_emits_removed_row_for_owned_delete() {
    let schema = owner_id_public_schema();
    let author = AuthorSubject::for_test_bytes([0x32; 16]);
    let db = open_db(0x32, AuthorSubject::SYSTEM, &schema);
    let user_id = "local-first-user";
    db.set_identity_claims(
        author,
        BTreeMap::from([("user_id".to_owned(), Value::String(user_id.to_owned()))]),
    );
    let query = Query::from("messages");
    let prepared = prepared(&db, &query);
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), author)).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let row_id = row(0x32);
    db.insert(
        "messages",
        BTreeMap::from([
            ("body".to_owned(), Value::String("delete me".to_owned())),
            ("owner_id".to_owned(), Value::String(user_id.to_owned())),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row_id),
            identity: crate::db::WriteIdentity::Session(author),
            ..Default::default()
        },
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![row_id]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete(
        "messages",
        row_id,
        crate::db::DeleteOptions {
            identity: crate::db::WriteIdentity::Session(author),
            ..Default::default()
        },
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![row_id]
    );
}

#[test]
fn subscription_retains_a_plan_from_its_selected_authorization_mode() {
    let schema = owner_id_public_schema();
    let author = AuthorSubject::for_test_bytes([0x33; 16]);
    let db = open_db(0x33, author, &schema);
    db.set_identity_claims(
        author,
        BTreeMap::from([("user_id".to_owned(), Value::String("alice".to_owned()))]),
    );
    let prepared = prepared(
        &db,
        &Query::from("messages").filter(eq(col("owner_id"), claim("user_id"))),
    );

    let client = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(
        client.retained_plan_authorization_mode(),
        Some(QueryAuthorizationMode::ClientLocal)
    );

    let trusted =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), author)).unwrap();
    assert_eq!(
        trusted.retained_plan_authorization_mode(),
        Some(QueryAuthorizationMode::TrustedServing)
    );
}

#[test]
fn include_deleted_fails_closed_on_live_subscription_apis() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let opts = ReadOpts {
        include_deleted: true,
        ..ReadOpts::default()
    };

    assert_unsupported_subscription_include_deleted(expect_error(doctest_support::block_on(
        db.subscribe(&prepared_query, opts.clone()),
    )));
    assert_unsupported_subscription_include_deleted(expect_error(doctest_support::block_on(
        db.subscribe_for_identity(&prepared_query, opts.clone(), db.identity.author),
    )));

    let rows = doctest_support::block_on(db.all(&prepared_query, opts)).unwrap();
    assert!(rows.is_empty());
}
