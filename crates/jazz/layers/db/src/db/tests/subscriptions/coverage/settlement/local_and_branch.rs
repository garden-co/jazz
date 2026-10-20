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
        cells("descriptor parity", false, owner),
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
fn subscription_executes_its_selected_authorization_mode() {
    // This stays internal solely to inspect provenance captured from the actual
    // compiled program. The exact row assertions below exercise public Db APIs.
    // Client-local reads see local inputs; serving reads must enforce policy.
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("messages")
                .column("body", PublicColumnType::Text)
                .column("owner_id", PublicColumnType::Text)
                .policies(PublicTablePolicies::new().with_select(public_literal_eq(
                    "body",
                    PublicValue::Text("visible".into()),
                ))),
        ),
    );
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

    for (id, body, owner_id) in [
        (row(0x34), "visible", "alice"),
        (row(0x35), "hidden", "alice"),
        (row(0x36), "visible", "bob"),
    ] {
        // The public row-input macro has facade values; this text-only fixture
        // converts them explicitly for the direct Db boundary under test.
        let input = crate::row_input!("body" => body, "owner_id" => owner_id)
            .into_iter()
            .map(|(name, value)| {
                let crate::model::public_schema::Value::Text(value) = value else {
                    panic!("text fixture")
                };
                (name, Value::String(value))
            })
            .collect();
        db.insert(
            "messages",
            input,
            InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let opts = ReadOpts {
        tier: crate::db::ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut client = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
    assert_eq!(
        client.compiled_authorization_mode(),
        Some(QueryAuthorizationMode::ClientLocal)
    );
    assert_eq!(
        row_ids(&opened_rows(block_on(client.next_raw()).unwrap()))
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0x34), row(0x35)]),
        "client execution binds the claim without reapplying serving policy"
    );

    let mut trusted = block_on(db.subscribe_for_identity(&prepared, opts, author)).unwrap();
    assert_eq!(
        trusted.compiled_authorization_mode(),
        Some(QueryAuthorizationMode::TrustedServing)
    );
    assert_eq!(
        row_ids(&opened_rows(block_on(trusted.next_raw()).unwrap())),
        vec![row(0x34)],
        "serving execution binds the claim AND enforces the read policy"
    );
}

/// Alice's deleted todo remains readable through both client-local and trusted
/// include-deleted subscriptions, while ordinary reads exclude it.
#[test]
fn include_deleted_subscriptions_return_deleted_rows_on_both_public_apis() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0x36; 16]);
    let db = open_db(0x36, alice, &schema);
    let row_id = row(0x36);
    db.insert(
        "todos",
        cells("deleted todo", false, alice),
        crate::db::InsertOptions {
            row_id: Some(row_id),
            ..Default::default()
        },
    )
    .unwrap();
    db.delete("todos", row_id, Default::default()).unwrap();
    let prepared_query = prepared(&db, &Query::from("todos"));
    assert!(
        block_on(db.all(&prepared_query, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
    let opts = ReadOpts {
        read_view: crate::protocol::ReadViewSpec {
            include_deleted: true,
            ..Default::default()
        },
        ..ReadOpts::default()
    };
    let expected = block_on(db.all(&prepared_query, opts.clone())).unwrap();
    assert_eq!(row_ids(&expected), vec![row_id]);
    assert!(expected[0].is_deleted());

    let mut client = block_on(db.subscribe(&prepared_query, opts.clone())).unwrap();
    let mut trusted = block_on(db.subscribe_for_identity(&prepared_query, opts, alice)).unwrap();
    for subscription in [&mut client, &mut trusted] {
        let rows = opened_rows(block_on(subscription.next_raw()).unwrap());
        assert_eq!(row_ids(&rows), vec![row_id]);
        assert_eq!(
            rows[0].cell(&schema.tables()[0], "title"),
            Some(Value::String("deleted todo".to_owned()))
        );
    }
}
