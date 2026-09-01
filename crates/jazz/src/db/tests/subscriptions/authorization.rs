//! Claim-bound, seeded, inherited-policy, and identity-isolation subscriptions.

use super::*;

struct TrustedBackendRelayTransport {
    inner: Box<dyn crate::db::Transport>,
}

impl crate::db::Transport for TrustedBackendRelayTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), crate::wire::TransportError> {
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }

    fn connection_session_context(&self) -> Option<crate::db::ConnectionSessionContext> {
        self.inner.connection_session_context()
    }

    fn permits_delegated_sessions(&self) -> bool {
        true
    }
}

#[test]
fn maintained_physical_point_subscriptions_keep_policy_scopes_live() {
    let schema = owner_read_schema();
    let db = open_db(0xa0, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    for identity in [alice, bob] {
        db.node
            .node
            .borrow_mut()
            .set_session_claims(identity, test_provider_claims(identity));
    }

    let target = row(0x71);
    let other = row(0x72);
    for (row_id, title, owner) in [(target, "target", alice), (other, "other", bob)] {
        db.insert(
            "todos",
            cells(title, false, owner),
            crate::db::InsertOptions {
                row_id: Some(row_id),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let target_query = db
        .prepare_query(&Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(target.0)))))
        .unwrap();
    let other_query = db
        .prepare_query(&Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(other.0)))))
        .unwrap();
    let opts = ReadOpts::default();
    let mut alice_target =
        block_on(db.subscribe_for_identity(&target_query, opts.clone(), alice)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(alice_target.next_raw()).unwrap())),
        vec![target],
        "the target's owner receives the maintained physical-point seed"
    );
    let mut bob_target =
        block_on(db.subscribe_for_identity(&target_query, opts.clone(), bob)).unwrap();
    assert!(
        opened_rows(block_on(bob_target.next_raw()).unwrap()).is_empty(),
        "a second identity must not inherit the first identity's point-policy result"
    );
    let mut bob_other = block_on(db.subscribe_for_identity(&other_query, opts, bob)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(bob_other.next_raw()).unwrap())),
        vec![other],
        "independent point subscriptions retain their own physical target"
    );

    db.update(
        "todos",
        other,
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("other changed".to_owned()),
        )]),
        Default::default(),
    )
    .unwrap();
    assert!(alice_target.try_next_event().is_none());
    assert!(bob_target.try_next_event().is_none());
    let (_, updated, removed) = delta_rows(block_on(bob_other.next_raw()).unwrap());
    assert!(updated.iter().any(|row| row.row_uuid() == other));
    assert!(removed.is_empty());

    db.update(
        "todos",
        target,
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("target changed".to_owned()),
        )]),
        Default::default(),
    )
    .unwrap();
    assert!(bob_target.try_next_event().is_none());
    let (_, updated, removed) = delta_rows(block_on(alice_target.next_raw()).unwrap());
    assert!(updated.iter().any(|row| row.row_uuid() == target));
    assert!(removed.is_empty());

    db.update(
        "todos",
        target,
        BTreeMap::from([("owner".to_owned(), Value::Uuid(bob.test_uuid()))]),
        Default::default(),
    )
    .unwrap();
    let (_, updated, removed) = delta_rows(block_on(alice_target.next_raw()).unwrap());
    assert!(updated.is_empty());
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        vec![target]
    );
    let (added, updated, removed) = delta_rows(block_on(bob_target.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![target]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete("todos", target, Default::default()).unwrap();
    let (_, updated, removed) = delta_rows(block_on(bob_target.next_raw()).unwrap());
    assert!(updated.is_empty());
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        vec![target]
    );

    db.restore(
        "todos",
        target,
        Some(cells("target restored", false, bob)),
        Default::default(),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(bob_target.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![target]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn maintained_subscription_emits_created_by_scoped_insert_after_empty_seed() {
    let schema = created_by_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa1, alice, &schema);
    let query = Query::from("todos");
    let prepared = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();

    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let write = db
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("created by alice".to_owned()),
                ),
                ("done".to_owned(), Value::Bool(false)),
            ]),
            Default::default(),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);

    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn prepared_one_shot_releases_local_groove_subscription_immediately() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = Query::from("todos").filter(eq(col("title"), param("title")));
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("title".to_owned(), Value::String("missing".to_owned()))]),
        )
        .unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;

    for _ in 0..4 {
        assert!(
            block_on(db.all(&prepared, ReadOpts::default()))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            db.runtime_stats_for_test().active_subscriptions,
            baseline,
            "completed one-shot reads must not retain Groove outputs"
        );
    }
}

#[test]
fn dropping_local_stream_releases_groove_subscription_without_a_write() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = Query::from("todos").filter(eq(col("title"), param("title")));
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("title".to_owned(), Value::String("missing".to_owned()))]),
        )
        .unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;
    let opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };

    let mut subscription = block_on(db.subscribe(&prepared, opts)).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 1
    );

    drop(subscription);
    // Drop is deliberately non-blocking now that the node may be suspended
    // on storage. Its terminal cleanup runs on the next ordinary owner turn,
    // without requiring a data write or a Groove notification.
    block_on(db.tick()).unwrap();
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline,
        "the next owner turn must retire a dropped local stream without a write"
    );
}

#[test]
fn dropping_one_local_stream_preserves_a_sibling_on_the_same_binding() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = Query::from("todos").filter(eq(col("title"), param("title")));
    let prepared = db
        .prepare_query_bound(
            &query,
            BTreeMap::from([("title".to_owned(), Value::String("match".to_owned()))]),
        )
        .unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;
    let opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };
    let mut first = block_on(db.subscribe(&prepared, opts.clone())).unwrap();
    let mut survivor = block_on(db.subscribe(&prepared, opts)).unwrap();
    assert!(opened_rows(block_on(first.next_raw()).unwrap()).is_empty());
    assert!(opened_rows(block_on(survivor.next_raw()).unwrap()).is_empty());
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 2
    );

    drop(first);
    block_on(db.tick()).unwrap();
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 1
    );

    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("match", false),
            Default::default(),
        )
        .unwrap();
    let (added, updated, removed) = delta_rows(block_on(survivor.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn maintained_subscription_emits_created_by_scoped_insert_for_explicit_identity() {
    let schema = created_by_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa1, alice, &schema);
    let query = Query::from("todos");
    let prepared = prepared(&db, &query);
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), alice)).unwrap();

    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let write = db
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("created by alice".to_owned()),
                ),
                ("done".to_owned(), Value::Bool(false)),
            ]),
            Default::default(),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = block_on(db.all_for_identity(&prepared, ReadOpts::default(), alice)).unwrap();
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);

    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn local_propagating_subscription_emits_created_by_scoped_insert_after_empty_seed() {
    let schema = created_by_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa1, alice, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, alice);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();

    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let mut snapshot = RelationSnapshot::default();
    while let Some(event) = subscription.try_next_event() {
        apply_subscription_event(&mut snapshot, event);
        assert!(
            snapshot.rows.is_empty(),
            "pre-insert coverage events must stay empty"
        );
    }

    let write = client
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("created by alice".to_owned()),
                ),
                ("done".to_owned(), Value::Bool(false)),
            ]),
            Default::default(),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = prepared_all(&client, &query, ReadOpts::default());
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);

    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn local_propagating_subscription_coerces_user_id_claim_for_created_by() {
    let schema = created_by_read_schema_for_claim("user_id");
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa1, alice, &schema);
    let claims = BTreeMap::from([(
        "user_id".to_owned(),
        Value::String(alice.test_uuid().to_string()),
    )]);
    client.set_test_provider_claims(alice, claims.clone());
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(server_transport, alice, claims);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();

    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    while let Some(event) = subscription.try_next_event() {
        assert!(opened_rows(event).is_empty());
    }

    let write = client
        .insert(
            "todos",
            doctest_support::todo_cells("created by alice", false),
            Default::default(),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let one_shot = prepared_all(&client, &query, ReadOpts::default());
    assert_eq!(row_ids(&one_shot), vec![write.row_uuid()]);
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![write.row_uuid()]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

fn resource_test_cells(title: &str) -> RowCells {
    resource_test_cells_with_group(title, row(0x11))
}

fn resource_test_cells_with_group(title: &str, group: RowUuid) -> RowCells {
    BTreeMap::from([
        ("org_id".to_owned(), Value::Uuid(row(0x01).0)),
        ("created_by".to_owned(), Value::Uuid(group.0)),
        ("updated_by".to_owned(), Value::Uuid(group.0)),
        ("archived".to_owned(), Value::Bool(false)),
        ("label".to_owned(), Value::String(title.to_owned())),
        ("date_created".to_owned(), Value::U64(1)),
        ("date_updated".to_owned(), Value::U64(2)),
        ("col_text_a".to_owned(), Value::Nullable(None)),
        ("col_text_b".to_owned(), Value::Nullable(None)),
        ("col_float".to_owned(), Value::Nullable(None)),
        ("col_int".to_owned(), Value::Nullable(None)),
        ("col_json".to_owned(), Value::Nullable(None)),
        ("col_tags".to_owned(), Value::Nullable(None)),
    ])
}

fn resource_access_test_cells(resource: RowUuid, team: RowUuid, administrator: bool) -> RowCells {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("team".to_owned(), Value::Uuid(team.0)),
        ("grant_role".to_owned(), Value::String("viewer".to_owned())),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ])
}

fn group_access_test_cells(group: RowUuid, user: AuthorSubject) -> RowCells {
    BTreeMap::from([
        ("group_id".to_owned(), Value::Uuid(group.0)),
        ("user_id".to_owned(), Value::Uuid(user.test_uuid())),
        ("role".to_owned(), Value::String("viewer".to_owned())),
    ])
}

fn uuid_string_grant_role_schema(role: uuid::Uuid) -> JazzSchema {
    let resource_policy = public_recursive_access_policy(
        "doc_access_edges",
        "resource_id",
        "team_id",
        &[],
        &[(
            "grant_role",
            vec![PublicValue::Uuid(PublicObjectId::from_uuid(role))],
        )],
        "teams",
        "team_entry",
        "member_id",
        "target_id",
        &[],
        "teams",
        "identity_key",
        &["claims", "sub"],
        "id",
    );
    let access_policy = public_recursive_access_policy(
        "doc_access_edges",
        "id",
        "team_id",
        &[],
        &[],
        "teams",
        "team_entry",
        "member_id",
        "target_id",
        &[],
        "teams",
        "identity_key",
        &["claims", "sub"],
        "id",
    );
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("teams")
                    .column("name", PublicColumnType::Text)
                    .column("identity_key", PublicColumnType::Uuid),
            )
            .table(
                PublicTableSchemaBuilder::new("team_entry")
                    .fk_column("member_id", "teams")
                    .fk_column("target_id", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(resource_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("doc_access_edges")
                    .fk_column("resource_id", "docs")
                    .fk_column("team_id", "teams")
                    .column("grant_role", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(access_policy)),
            ),
    )
}

#[test]
fn string_grant_role_access_filter_matches_uuid_literal_in_list() {
    let role = uuid::Uuid::parse_str("0cae56e7-0f54-421c-ba8b-54fcbfec8dd2").unwrap();
    let schema = uuid_string_grant_role_schema(role);
    let server = open_core(0x6d, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x6e; 16]);
    let member_team = row(0x61);
    let resource_team = row(0x62);
    let doc = row(0x63);

    server
        .insert_with_id(
            "teams",
            member_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("member".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(member.test_uuid())),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "teams",
            resource_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("resource".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(row(0x64).0)),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "team_entry",
            row(0x65),
            BTreeMap::from([
                ("member_id".to_owned(), Value::Uuid(member_team.0)),
                ("target_id".to_owned(), Value::Uuid(resource_team.0)),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "docs",
            doc,
            BTreeMap::from([("title".to_owned(), Value::String("visible".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "doc_access_edges",
            row(0x66),
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(doc.0)),
                ("team_id".to_owned(), Value::Uuid(resource_team.0)),
                ("grant_role".to_owned(), Value::String(role.to_string())),
            ]),
        )
        .unwrap();

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "docs"),
        vec![doc]
    );

    let db = block_on(Db::open_history_complete(DbConfig {
        schema: schema.clone(),
        storage: rocks_storage(&schema),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x6f; 16]),
            author: AuthorSubject::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x6f))),
    }))
    .unwrap();
    for (table, row_id, cells) in [
        (
            "teams",
            member_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("member".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(member.test_uuid())),
            ]),
        ),
        (
            "teams",
            resource_team,
            BTreeMap::from([
                ("name".to_owned(), Value::String("resource".to_owned())),
                ("identity_key".to_owned(), Value::Uuid(row(0x64).0)),
            ]),
        ),
        (
            "team_entry",
            row(0x65),
            BTreeMap::from([
                ("member_id".to_owned(), Value::Uuid(member_team.0)),
                ("target_id".to_owned(), Value::Uuid(resource_team.0)),
            ]),
        ),
        (
            "docs",
            doc,
            BTreeMap::from([("title".to_owned(), Value::String("visible".to_owned()))]),
        ),
        (
            "doc_access_edges",
            row(0x66),
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(doc.0)),
                ("team_id".to_owned(), Value::Uuid(resource_team.0)),
                ("grant_role".to_owned(), Value::String(role.to_string())),
            ]),
        ),
    ] {
        db.seed_settled_mergeable_for_bootstrap(table, row_id, AuthorSubject::SYSTEM, cells)
            .unwrap();
    }
    db.node
        .node
        .borrow_mut()
        .set_test_provider_claims(member, test_provider_claims(member));
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    let one_shot = block_on(db.all_for_identity(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        member,
    ))
    .unwrap();
    assert_eq!(row_ids(&one_shot), vec![doc]);

    let access = db.prepare_query(&Query::from("doc_access_edges")).unwrap();
    let access_rows = block_on(db.all_for_identity(
        &access,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        member,
    ))
    .unwrap();
    assert_eq!(row_ids(&access_rows), vec![row(0x66)]);
}

#[test]
fn customer_resource_access_edge_policy_requires_group_access_seed() {
    let schema = customer_resource_policy_minimal_schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x11; 16]);
    let group = row(0x22);
    let resource = row(0xd1);

    server
        .insert_with_id(
            "org",
            row(0x01),
            BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id("group", group, team_cells("member-group"))
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa1),
            group_access_test_cells(group, member),
        )
        .unwrap();
    server
        .insert_with_id("res_i", resource, resource_test_cells("visible"))
        .unwrap();
    server
        .insert_with_id(
            "res_i_access_edges",
            row(0xb1),
            resource_access_test_cells(resource, group, false),
        )
        .unwrap();
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i"),
        vec![resource]
    );
}

#[test]
fn seeded_membership_resource_policy_allows_direct_and_transitive_groups() {
    let schema = customer_resource_policy_minimal_schema();
    let server = open_core(0x5f, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x12; 16]);
    let other = AuthorSubject::for_test_bytes([0x13; 16]);
    let (direct, transitive, hidden) =
        seed_seeded_membership_resource_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i"),
        vec![direct, transitive]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "res_i"),
        vec![hidden]
    );
    assert!(
        served_subscription_rows_for_author(
            &schema,
            &server,
            AuthorSubject::for_test_bytes([0x99; 16]),
            "res_i"
        )
        .is_empty()
    );
}

#[test]
fn direct_multi_identity_subscribe_reuses_shared_seeded_fragments_without_leaking() {
    let schema = customer_resource_policy_minimal_schema();
    let db = open_db(0x69, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x12; 16]);
    let other = AuthorSubject::for_test_bytes([0x13; 16]);
    let spy = AuthorSubject::for_test_bytes([0x99; 16]);
    for identity in [member, other, spy] {
        db.node
            .node
            .borrow_mut()
            .set_test_provider_claims(identity, test_provider_claims(identity));
    }
    db.insert(
        "org",
        BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0x01)),
            ..Default::default()
        },
    )
    .unwrap();
    let direct_group = row(0x31);
    let transitive_group = row(0x32);
    let hidden_group = row(0x33);
    let direct = row(0xd1);
    let transitive = row(0xd2);
    let hidden = row(0xd3);
    for (group, name) in [
        (direct_group, "direct"),
        (transitive_group, "transitive"),
        (hidden_group, "hidden"),
    ] {
        db.insert(
            "group",
            team_cells(name),
            crate::db::InsertOptions {
                row_id: Some(group),
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.insert(
        "group_access_edges",
        group_access_test_cells(direct_group, member),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "group_access_edges",
        group_access_test_cells(hidden_group, other),
        crate::db::InsertOptions {
            row_id: Some(row(0xa2)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "group_entry",
        group_entry_test_cells(direct_group, transitive_group, false),
        crate::db::InsertOptions {
            row_id: Some(row(0xc1)),
            ..Default::default()
        },
    )
    .unwrap();
    for (resource, title) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        db.insert(
            "res_i",
            resource_test_cells(title),
            crate::db::InsertOptions {
                row_id: Some(resource),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb1), direct, direct_group),
        (row(0xb2), transitive, transitive_group),
        (row(0xb3), hidden, hidden_group),
    ] {
        db.insert(
            "res_i_access_edges",
            resource_access_test_cells(resource, group, false),
            crate::db::InsertOptions {
                row_id: Some(edge),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let prepared = db.prepare_query(&Query::from("res_i")).unwrap();
    let opts = ReadOpts::default();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut member_subscription =
        block_on(db.subscribe_for_identity(&prepared, opts.clone(), member)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(
            block_on(member_subscription.next_raw()).unwrap()
        )),
        vec![direct, transitive]
    );
    let member_reads = db.node.node.borrow().take_storage_read_metrics();
    assert!(
        member_reads.total.reads > 0,
        "first identity should hydrate the shared seeded fragments"
    );

    db.node.node.borrow().reset_storage_read_metrics();
    let mut other_subscription =
        block_on(db.subscribe_for_identity(&prepared, opts.clone(), other)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(
            block_on(other_subscription.next_raw()).unwrap()
        )),
        vec![hidden]
    );
    let other_reads = db.node.node.borrow().take_storage_read_metrics();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut spy_subscription = block_on(db.subscribe_for_identity(&prepared, opts, spy)).unwrap();
    assert!(opened_rows(block_on(spy_subscription.next_raw()).unwrap()).is_empty());
    let spy_reads = db.node.node.borrow().take_storage_read_metrics();

    assert!(
        other_reads.total.reads < member_reads.total.reads,
        "second identity should probe shared hydrated fragments, not rescan them: first={:?}, second={:?}",
        member_reads,
        other_reads
    );
    assert!(
        spy_reads.total.reads < member_reads.total.reads,
        "zero-grant identity should also reuse shared canonical fragments without seeing rows: first={:?}, spy={:?}",
        member_reads,
        spy_reads
    );
}

#[test]
fn direct_same_identity_subscribe_reuses_shared_seeded_fragments_across_shapes() {
    let schema = customer_two_resource_policy_minimal_schema();
    let db = open_db(0x6a, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x12; 16]);
    db.node
        .node
        .borrow_mut()
        .set_test_provider_claims(member, test_provider_claims(member));
    db.insert(
        "org",
        BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0x01)),
            ..Default::default()
        },
    )
    .unwrap();
    let direct_group = row(0x31);
    let transitive_group = row(0x32);
    for (group, name) in [(direct_group, "direct"), (transitive_group, "transitive")] {
        db.insert(
            "group",
            team_cells(name),
            crate::db::InsertOptions {
                row_id: Some(group),
                ..Default::default()
            },
        )
        .unwrap();
    }
    db.insert(
        "group_access_edges",
        group_access_test_cells(direct_group, member),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "group_entry",
        group_entry_test_cells(direct_group, transitive_group, false),
        crate::db::InsertOptions {
            row_id: Some(row(0xc1)),
            ..Default::default()
        },
    )
    .unwrap();

    let res_i_direct = row(0xd1);
    let res_i_transitive = row(0xd2);
    let res_j_direct = row(0xe1);
    let res_j_transitive = row(0xe2);
    for (table, resource, title) in [
        ("res_i", res_i_direct, "i-direct"),
        ("res_i", res_i_transitive, "i-transitive"),
        ("res_j", res_j_direct, "j-direct"),
        ("res_j", res_j_transitive, "j-transitive"),
    ] {
        db.insert(
            table,
            resource_test_cells(title),
            crate::db::InsertOptions {
                row_id: Some(resource),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (table, edge, resource, group) in [
        ("res_i_access_edges", row(0xb1), res_i_direct, direct_group),
        (
            "res_i_access_edges",
            row(0xb2),
            res_i_transitive,
            transitive_group,
        ),
        ("res_j_access_edges", row(0xb3), res_j_direct, direct_group),
        (
            "res_j_access_edges",
            row(0xb4),
            res_j_transitive,
            transitive_group,
        ),
    ] {
        db.insert(
            table,
            resource_access_test_cells(resource, group, false),
            crate::db::InsertOptions {
                row_id: Some(edge),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let res_i = db.prepare_query(&Query::from("res_i")).unwrap();
    let res_j = db.prepare_query(&Query::from("res_j")).unwrap();
    let opts = ReadOpts::default();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut first = block_on(db.subscribe_for_identity(&res_i, opts.clone(), member)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(first.next_raw()).unwrap())),
        vec![res_i_direct, res_i_transitive]
    );
    let first_reads = db.node.node.borrow().take_storage_read_metrics();

    db.node.node.borrow().reset_storage_read_metrics();
    let mut second = block_on(db.subscribe_for_identity(&res_j, opts, member)).unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(second.next_raw()).unwrap())),
        vec![res_j_direct, res_j_transitive]
    );
    let second_reads = db.node.node.borrow().take_storage_read_metrics();

    assert!(
        second_reads.total.reads < first_reads.total.reads,
        "second shape should probe shared hydrated fragments, not rescan them: first={:?}, second={:?}",
        first_reads,
        second_reads
    );
}

#[test]
fn seeded_membership_grant_and_revoke_propagate_incrementally() {
    let schema = customer_resource_policy_minimal_schema();
    let server = open_core(0x60, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x14; 16]);
    let group = row(0x41);
    let resource = row(0xd4);
    let access = row(0xb4);

    seed_customer_resource_base(&server);
    server
        .insert_with_id("group", group, team_cells("direct"))
        .unwrap();
    server
        .insert_with_id("res_i", resource, resource_test_cells("resource"))
        .unwrap();
    server
        .insert_with_id(
            "res_i_access_edges",
            access,
            resource_access_test_cells(resource, group, false),
        )
        .unwrap();

    let client = open_db(0x61, member, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, member);
    let mut subscription =
        prepared_subscribe(&client, &Query::from("res_i"), ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    while let Some(event) = subscription.try_next_event() {
        if let SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } = event
        {
            assert!(added.is_empty());
            assert!(updated.is_empty());
            assert!(removed.is_empty());
        }
    }

    server
        .insert_with_id(
            "group_access_edges",
            row(0xa4),
            group_access_test_cells(group, member),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![resource]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    server
        .update(
            "res_i_access_edges",
            access,
            BTreeMap::from([("administrator".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![resource]
    );
}

#[test]
fn same_table_seeded_membership_allows_direct_and_transitive_groups() {
    let schema = same_table_seeded_resource_policy_schema();
    let server = open_core(0x66, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x21; 16]);
    let other = AuthorSubject::for_test_bytes([0x22; 16]);
    let (direct, transitive, hidden) =
        seed_same_table_seeded_resource_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "resources"),
        vec![direct, transitive]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "resources"),
        vec![hidden]
    );
    assert!(
        served_subscription_rows_for_author(
            &schema,
            &server,
            AuthorSubject::for_test_bytes([0x99; 16]),
            "resources"
        )
        .is_empty()
    );
}

#[test]
fn same_table_string_seeded_membership_allows_direct_and_transitive_groups() {
    let schema = same_table_string_seeded_resource_policy_schema();
    let server = open_core(0x86, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x21; 16]);
    let other = AuthorSubject::for_test_bytes([0x22; 16]);
    let (direct, transitive, hidden) =
        seed_same_table_string_seeded_resource_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "resources"),
        vec![direct, transitive]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "resources"),
        vec![hidden]
    );
    assert!(
        served_subscription_rows_for_author(
            &schema,
            &server,
            AuthorSubject::for_test_bytes([0x99; 16]),
            "resources"
        )
        .is_empty()
    );
}

#[test]
fn same_table_seeded_membership_identity_key_update_propagates_incrementally() {
    let schema = same_table_seeded_resource_policy_schema();
    let server = open_core(0x67, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x23; 16]);
    let other = AuthorSubject::for_test_bytes([0x24; 16]);
    let direct_group = row(0x71);
    let transitive_group = row(0x72);
    let resource = row(0xe7);

    for (group, identity, label) in [
        (direct_group, other, "direct"),
        (transitive_group, other, "transitive"),
    ] {
        server
            .insert_with_id("teams", group, same_table_team_cells(label, identity))
            .unwrap();
    }
    server
        .insert_with_id(
            "team_entries",
            row(0xc7),
            same_table_team_entry_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    server
        .insert_with_id("resources", resource, same_table_resource_cells("resource"))
        .unwrap();
    server
        .insert_with_id(
            "resource_access",
            row(0xb7),
            same_table_resource_access_cells(resource, transitive_group, false),
        )
        .unwrap();

    let client = open_db(0x68, member, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, member);
    let mut subscription =
        prepared_subscribe(&client, &Query::from("resources"), ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    while let Some(event) = subscription.try_next_event() {
        let (added, updated, removed) = match event {
            SubscriptionEvent::Delta {
                added,
                updated,
                removed,
                ..
            } => (added, updated, removed),
            SubscriptionEvent::Rejected { reason } => {
                panic!("unexpected subscription rejection: {reason:?}")
            }
            SubscriptionEvent::Closed => (Vec::new(), Vec::new(), Vec::new()),
        };
        assert!(added.is_empty());
        assert!(updated.is_empty());
        assert!(removed.is_empty());
    }

    server
        .update(
            "teams",
            direct_group,
            BTreeMap::from([("identity_key".to_owned(), Value::Uuid(member.test_uuid()))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(
        subscription
            .try_next_event()
            .expect("identity-key grant must publish during the completed tick cycle"),
    );
    assert_eq!(row_ids(&added), vec![resource]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    let mut late_subscription =
        prepared_subscribe(&client, &Query::from("resources"), ReadOpts::default()).unwrap();
    assert_eq!(
        row_ids(&opened_rows(
            late_subscription
                .try_next_event()
                .expect("late subscription must open synchronously"),
        )),
        vec![resource]
    );

    server
        .update(
            "teams",
            direct_group,
            BTreeMap::from([("identity_key".to_owned(), Value::Uuid(other.test_uuid()))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(
        subscription
            .try_next_event()
            .expect("identity-key revoke must publish during the completed tick cycle"),
    );
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![resource]
    );
    let (added, updated, removed) = delta_rows(
        late_subscription
            .try_next_event()
            .expect("late subscription must publish the identity-key revoke"),
    );
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![resource]
    );
}

#[test]
fn inherited_child_policy_allows_two_and_three_level_chains_per_identity() {
    let schema = customer_inherited_child_policy_schema();
    let server = open_core(0x62, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x15; 16]);
    let other = AuthorSubject::for_test_bytes([0x16; 16]);
    let (member_child, member_grandchild, other_child, other_grandchild) =
        seed_inherited_child_fixture(&server, member, other);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i_child"),
        vec![member_child]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i_grandchild"),
        vec![member_grandchild]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "res_i_child"),
        vec![other_child]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, other, "res_i_grandchild"),
        vec![other_grandchild]
    );
    let spy = AuthorSubject::for_test_bytes([0x99; 16]);
    assert!(served_subscription_rows_for_author(&schema, &server, spy, "res_i_child").is_empty());
    assert!(
        served_subscription_rows_for_author(&schema, &server, spy, "res_i_grandchild").is_empty()
    );
}

#[test]
fn inherited_child_policy_parent_revocation_propagates_incrementally() {
    let schema = customer_inherited_child_policy_schema();
    let server = open_core(0x63, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x17; 16]);
    let other = AuthorSubject::for_test_bytes([0x18; 16]);
    let (child, _grandchild, _other_child, _other_grandchild) =
        seed_inherited_child_fixture(&server, member, other);

    let client = open_db(0x64, member, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, member);
    let mut subscription =
        prepared_subscribe(&client, &Query::from("res_i_child"), ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![child]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    server
        .update(
            "res_i_access_edges",
            row(0xbb),
            BTreeMap::from([("administrator".to_owned(), Value::Bool(true))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![child]
    );
}

#[test]
fn inherited_child_policy_composes_with_local_predicates() {
    let schema = customer_inherited_child_policy_schema();
    let server = open_core(0x65, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x19; 16]);
    let other = AuthorSubject::for_test_bytes([0x1a; 16]);
    let (open_child, _grandchild, _other_child, _other_grandchild) =
        seed_inherited_child_fixture(&server, member, other);
    let closed_child = row(0xee);
    server
        .insert_with_id(
            "res_i_child",
            closed_child,
            child_cells(row(0xdd), "closed", "closed child"),
        )
        .unwrap();

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_i_child"),
        vec![open_child]
    );
}

#[test]
fn inherited_child_insert_uses_parent_update_where_old_only() {
    let schema = inherited_insert_policy_schema();
    let member = AuthorSubject::for_test_bytes([0x21; 16]);
    let other = AuthorSubject::for_test_bytes([0x22; 16]);
    let server = open_core(0x65, AuthorSubject::SYSTEM, &schema);
    let member_db = open_db(0x66, member, &schema);
    let parent = row(0xf1);
    server
        .insert_with_id(
            "parents",
            parent,
            BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(member.test_uuid())),
                ("locked".to_owned(), Value::Bool(true)),
            ]),
        )
        .unwrap();

    let (member_transport, server_member_transport) = duplex();
    let _member_upstream = crate::db::block_on(member_db.connect_upstream(member_transport));
    let _member_subscriber = server.accept_subscriber(server_member_transport, member);
    let allowed = member_db
        .insert(
            "children",
            child_insert_cells(parent, "allowed"),
            crate::db::InsertOptions {
                row_id: Some(row(0xf2)),
                ..Default::default()
            },
        )
        .unwrap();
    member_db.tick().unwrap();
    server.tick().unwrap();
    member_db.tick().unwrap();
    assert_eq!(
        block_on(allowed.wait(DurabilityTier::Global)).unwrap(),
        allowed.mergeable_tx_id()
    );
    assert_eq!(
        prepared_read(&member_db, &Query::from("children"))[0].row_uuid(),
        allowed.row_uuid()
    );

    let other_db = open_db(0x67, other, &schema);
    let (other_transport, server_other_transport) = duplex();
    let _other_upstream = crate::db::block_on(other_db.connect_upstream(other_transport));
    let _other_subscriber = server.accept_subscriber(server_other_transport, other);
    let denied = other_db
        .insert(
            "children",
            child_insert_cells(parent, "denied"),
            crate::db::InsertOptions {
                row_id: Some(row(0xf3)),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&other_db, &server, &denied);
    let other_rows = prepared_read(&other_db, &Query::from("children"));
    assert!(
        other_rows.is_empty(),
        "the rejected child must roll back locally"
    );
    let rows = server.read(&Query::from("children")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), allowed.row_uuid());
}

fn seed_customer_resource_base(server: &CoreDb) {
    server
        .insert_with_id(
            "org",
            row(0x01),
            BTreeMap::from([("label".to_owned(), Value::String("org".to_owned()))]),
        )
        .unwrap();
}

fn seed_seeded_membership_resource_fixture(
    server: &CoreDb,
    member: AuthorSubject,
    other: AuthorSubject,
) -> (RowUuid, RowUuid, RowUuid) {
    seed_customer_resource_base(server);
    let direct_group = row(0x31);
    let transitive_group = row(0x32);
    let hidden_group = row(0x33);
    let direct = row(0xd1);
    let transitive = row(0xd2);
    let hidden = row(0xd3);

    for (group, name) in [
        (direct_group, "direct"),
        (transitive_group, "transitive"),
        (hidden_group, "hidden"),
    ] {
        server
            .insert_with_id("group", group, team_cells(name))
            .unwrap();
    }
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa1),
            group_access_test_cells(direct_group, member),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa2),
            group_access_test_cells(hidden_group, other),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_entry",
            row(0xc1),
            group_entry_test_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    for (resource, title) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        server
            .insert_with_id("res_i", resource, resource_test_cells(title))
            .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb1), direct, direct_group),
        (row(0xb2), transitive, transitive_group),
        (row(0xb3), hidden, hidden_group),
    ] {
        server
            .insert_with_id(
                "res_i_access_edges",
                edge,
                resource_access_test_cells(resource, group, false),
            )
            .unwrap();
    }
    (direct, transitive, hidden)
}

fn seed_same_table_seeded_resource_fixture(
    server: &CoreDb,
    member: AuthorSubject,
    other: AuthorSubject,
) -> (RowUuid, RowUuid, RowUuid) {
    let direct_group = row(0x61);
    let transitive_group = row(0x62);
    let hidden_group = row(0x63);
    let direct = row(0xf1);
    let transitive = row(0xf2);
    let hidden = row(0xf3);

    for (group, identity, label) in [
        (direct_group, member, "direct"),
        (
            transitive_group,
            AuthorSubject::for_test_bytes([0x88; 16]),
            "transitive",
        ),
        (hidden_group, other, "hidden"),
    ] {
        server
            .insert_with_id("teams", group, same_table_team_cells(label, identity))
            .unwrap();
    }
    server
        .insert_with_id(
            "team_entries",
            row(0xc6),
            same_table_team_entry_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    for (resource, label) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        server
            .insert_with_id("resources", resource, same_table_resource_cells(label))
            .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb6), direct, direct_group),
        (row(0xb7), transitive, transitive_group),
        (row(0xb8), hidden, hidden_group),
    ] {
        server
            .insert_with_id(
                "resource_access",
                edge,
                same_table_resource_access_cells(resource, group, false),
            )
            .unwrap();
    }
    (direct, transitive, hidden)
}

fn seed_same_table_string_seeded_resource_fixture(
    server: &CoreDb,
    member: AuthorSubject,
    other: AuthorSubject,
) -> (RowUuid, RowUuid, RowUuid) {
    let direct_group = row(0x61);
    let transitive_group = row(0x62);
    let hidden_group = row(0x63);
    let direct = row(0xf1);
    let transitive = row(0xf2);
    let hidden = row(0xf3);

    for (group, identity, label) in [
        (direct_group, member.test_uuid().to_string(), "direct"),
        (transitive_group, "not-the-member".to_owned(), "transitive"),
        (hidden_group, other.test_uuid().to_string(), "hidden"),
    ] {
        server
            .insert_with_id(
                "teams",
                group,
                same_table_team_string_cells(label, &identity),
            )
            .unwrap();
    }
    server
        .insert_with_id(
            "team_entries",
            row(0xc6),
            same_table_team_entry_cells(direct_group, transitive_group, false),
        )
        .unwrap();
    for (resource, label) in [
        (direct, "direct"),
        (transitive, "transitive"),
        (hidden, "hidden"),
    ] {
        server
            .insert_with_id("resources", resource, same_table_resource_cells(label))
            .unwrap();
    }
    for (edge, resource, group) in [
        (row(0xb6), direct, direct_group),
        (row(0xb7), transitive, transitive_group),
        (row(0xb8), hidden, hidden_group),
    ] {
        server
            .insert_with_id(
                "resource_access",
                edge,
                same_table_resource_access_cells(resource, group, false),
            )
            .unwrap();
    }
    (direct, transitive, hidden)
}

fn seed_inherited_child_fixture(
    server: &CoreDb,
    member: AuthorSubject,
    other: AuthorSubject,
) -> (RowUuid, RowUuid, RowUuid, RowUuid) {
    seed_customer_resource_base(server);
    let member_group = row(0xd1);
    let other_group = row(0xd2);
    let member_resource = row(0xdd);
    let other_resource = row(0xde);
    let member_child = row(0xe1);
    let other_child = row(0xe2);
    let member_grandchild = row(0xe3);
    let other_grandchild = row(0xe4);

    for (group, label) in [(member_group, "member"), (other_group, "other")] {
        server
            .insert_with_id("group", group, team_cells(label))
            .unwrap();
    }
    server
        .insert_with_id(
            "group_access_edges",
            row(0xaa),
            group_access_test_cells(member_group, member),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xab),
            group_access_test_cells(other_group, other),
        )
        .unwrap();
    for (resource, group, label) in [
        (member_resource, member_group, "member-resource"),
        (other_resource, other_group, "other-resource"),
    ] {
        server
            .insert_with_id(
                "res_i",
                resource,
                resource_test_cells_with_group(label, group),
            )
            .unwrap();
    }
    server
        .insert_with_id(
            "res_i_access_edges",
            row(0xbb),
            resource_access_test_cells(member_resource, member_group, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_access_edges",
            row(0xbc),
            resource_access_test_cells(other_resource, other_group, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_child",
            member_child,
            child_cells(member_resource, "open", "member-child"),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_child",
            other_child,
            child_cells(other_resource, "open", "other-child"),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_grandchild",
            member_grandchild,
            grandchild_cells(member_child, "member-grandchild"),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_i_grandchild",
            other_grandchild,
            grandchild_cells(other_child, "other-grandchild"),
        )
        .unwrap();
    (
        member_child,
        member_grandchild,
        other_child,
        other_grandchild,
    )
}

fn team_cells(name: &str) -> RowCells {
    BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))])
}

fn same_table_team_cells(name: &str, identity: AuthorSubject) -> RowCells {
    BTreeMap::from([
        ("name".to_owned(), Value::String(name.to_owned())),
        ("identity_key".to_owned(), Value::Uuid(identity.test_uuid())),
    ])
}

fn same_table_team_string_cells(name: &str, identity: &str) -> RowCells {
    BTreeMap::from([
        ("name".to_owned(), Value::String(name.to_owned())),
        (
            "identity_key".to_owned(),
            Value::String(identity.to_owned()),
        ),
    ])
}

fn group_entry_test_cells(member: RowUuid, target: RowUuid, administrator: bool) -> RowCells {
    BTreeMap::from([
        ("member_id".to_owned(), Value::Uuid(member.0)),
        ("target_id".to_owned(), Value::Uuid(target.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
        ("date_added".to_owned(), Value::U64(1)),
    ])
}

fn same_table_team_entry_cells(member: RowUuid, target: RowUuid, administrator: bool) -> RowCells {
    BTreeMap::from([
        ("member_id".to_owned(), Value::Uuid(member.0)),
        ("target_id".to_owned(), Value::Uuid(target.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ])
}

fn same_table_resource_cells(label: &str) -> RowCells {
    BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))])
}

fn same_table_resource_access_cells(
    resource: RowUuid,
    group: RowUuid,
    administrator: bool,
) -> RowCells {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("team".to_owned(), Value::Uuid(group.0)),
        ("administrator".to_owned(), Value::Bool(administrator)),
    ])
}

fn child_cells(resource: RowUuid, status: &str, label: &str) -> RowCells {
    BTreeMap::from([
        ("resource".to_owned(), Value::Uuid(resource.0)),
        ("status".to_owned(), Value::String(status.to_owned())),
        ("label".to_owned(), Value::String(label.to_owned())),
    ])
}

fn grandchild_cells(child: RowUuid, label: &str) -> RowCells {
    BTreeMap::from([
        ("child".to_owned(), Value::Uuid(child.0)),
        ("label".to_owned(), Value::String(label.to_owned())),
    ])
}

fn child_insert_cells(parent: RowUuid, label: &str) -> RowCells {
    BTreeMap::from([
        ("parent_id".to_owned(), Value::Uuid(parent.0)),
        ("label".to_owned(), Value::String(label.to_owned())),
    ])
}

fn seed_recursive_reachable_read_fixture(
    server: &CoreDb,
    member: AuthorSubject,
) -> (RowUuid, RowUuid) {
    let direct_doc = row(0xd1);
    let inherited_doc = row(0xd2);
    let hidden_doc = row(0xd3);
    let member_team = RowUuid(member.test_uuid());
    let parent_team = row(0xa1);
    let hidden_team = row(0xa2);

    for (team, name) in [
        (member_team, "member"),
        (parent_team, "parent"),
        (hidden_team, "hidden"),
    ] {
        server
            .insert_with_id("group", team, team_cells(name))
            .unwrap();
    }

    for (doc, title) in [
        (direct_doc, "direct"),
        (inherited_doc, "inherited"),
        (hidden_doc, "hidden"),
    ] {
        server
            .insert_with_id("res_a", doc, resource_test_cells(title))
            .unwrap();
    }

    server
        .insert_with_id(
            "res_a_access_edges",
            row(0xb1),
            resource_access_test_cells(direct_doc, member_team, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "group_access_edges",
            row(0xa1),
            group_access_test_cells(member_team, member),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_a_access_edges",
            row(0xb2),
            resource_access_test_cells(inherited_doc, parent_team, false),
        )
        .unwrap();
    server
        .insert_with_id(
            "res_a_access_edges",
            row(0xb3),
            resource_access_test_cells(hidden_doc, hidden_team, false),
        )
        .unwrap();
    for i in 0..42 {
        let member = if i == 0 { member_team } else { parent_team };
        let target = parent_team;
        server
            .insert_with_id(
                "group_entry",
                row(0xc1 + i),
                group_entry_test_cells(member, target, false),
            )
            .unwrap();
    }

    (direct_doc, inherited_doc)
}

fn served_subscription_rows_for_author(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorSubject,
    table: &str,
) -> Vec<RowUuid> {
    served_subscription_rows_for_author_with_claims(
        schema,
        server,
        author,
        table,
        test_provider_claims(author),
    )
}

fn served_subscription_rows_for_author_with_claims(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorSubject,
    table: &str,
    claims: BTreeMap<String, Value>,
) -> Vec<RowUuid> {
    let client_node = match author {
        AuthorSubject::System => 0x5d,
        AuthorSubject::Authenticated(_) => author.test_uuid().as_bytes()[0],
    };
    let client = open_db(client_node, author, schema);
    client
        .node
        .node
        .borrow_mut()
        .set_test_provider_claims(author, claims.clone());
    server
        .node()
        .borrow_mut()
        .set_test_provider_claims(author, claims.clone());
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    server
        .node()
        .borrow_mut()
        .set_test_provider_claims(author, claims);
    let query = Query::from(table);
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    let mut rows = BTreeSet::new();

    for _ in 0..8 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            {
                if reset {
                    rows.clear();
                }
                for row in removed {
                    rows.remove(&row.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    rows.insert(row.row_uuid());
                }
            }
        }
    }
    rows.into_iter().collect()
}

fn served_many_subscription_rows_for_author(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorSubject,
    tables: &[&str],
) -> BTreeMap<String, Vec<RowUuid>> {
    let client = open_db(
        author.test_uuid().as_bytes()[0].wrapping_add(0x40),
        author,
        schema,
    );
    client
        .node
        .node
        .borrow_mut()
        .set_test_provider_claims(author, test_provider_claims(author));
    server
        .node()
        .borrow_mut()
        .set_test_provider_claims(author, test_provider_claims(author));
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    let mut subscriptions = Vec::new();
    for table in tables {
        let query = Query::from(*table);
        let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
        assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
        subscriptions.push(((*table).to_owned(), subscription));
    }

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    subscriptions
        .into_iter()
        .map(|(table, mut subscription)| {
            let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            (table, row_ids(&added))
        })
        .collect()
}

fn served_table_rows_via_relay(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorSubject,
    table: &str,
) -> (Vec<RowUuid>, usize, usize) {
    let relay = open_db(0x71, AuthorSubject::SYSTEM, schema);
    let client = open_db(0x72, author, schema);
    let mut downstream_claims = test_provider_claims(author);
    // This extra admitted claim is irrelevant to the policy itself. It proves
    // the relay forwards this connection's immutable snapshot rather than
    // reconstructing claims from the client node's author-keyed cache.
    downstream_claims.insert(
        crate::query::provider_claim_key("relay_fixture"),
        Value::String("delegated".to_owned()),
    );
    let (relay_transport, core_transport) = duplex();
    let _relay_upstream = crate::db::block_on(relay.connect_upstream(Box::new(
        TrustedBackendRelayTransport {
            inner: relay_transport,
        },
    )));
    // A relay sends its downstream session's immutable policy binding to its
    // upstream. Model the same scope-isolated admission that the serving
    // boundary performs in production: a generic SYSTEM/trusted link cannot
    // self-assert this subject's claims.
    let core_subscriber = server.server.accept_scope_isolated_relay_subscriber(
        core_transport,
        author,
        downstream_claims.clone(),
        1,
    );
    let (client_transport, relay_sub_transport) = duplex();
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    // The relay is the downstream client's authentication boundary.  Its
    // receiving connection therefore gets the actual session claims instead
    // of the empty claims used by the generic test transport helper.
    let _relay_subscriber =
        relay.accept_subscriber_with_claims(relay_sub_transport, author, downstream_claims.clone());

    let query = Query::from(table);
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    let mut rows = BTreeSet::new();
    for _ in 0..20 {
        server.server.tick().unwrap();
        relay.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            {
                if reset {
                    rows.clear();
                }
                for row in removed {
                    rows.remove(&row.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    rows.insert(row.row_uuid());
                }
            }
        }
    }
    let client_query = client.prepare_query(&Query::from(table)).unwrap();
    let client_one_shot = block_on(client.all(&client_query, ReadOpts::default()))
        .unwrap()
        .len();
    let relay_query = relay.prepare_query(&Query::from(table)).unwrap();
    let relay_one_shot = block_on(relay.all(&relay_query, ReadOpts::default()))
        .unwrap()
        .len();
    let expected_delegated_binding = (author, downstream_claims);
    let connection = core_subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        panic!("relay's core-facing connection must be a subscriber link");
    };
    assert!(
        state
            .coverage_groups
            .values()
            .any(|group| group.policy_binding == expected_delegated_binding),
        "the core must retain the downstream session's delegated policy binding"
    );
    (rows.into_iter().collect(), client_one_shot, relay_one_shot)
}

/// A browser worker is a relay, but not a trusted backend: its upstream
/// session is the same authenticated session as its foreground client.  It
/// must therefore receive the authority's direct policy binding without
/// trying to forward either delegated-session or SessionClaims frames.
fn served_table_rows_via_ordinary_browser_worker(
    schema: &JazzSchema,
    server: &CoreDb,
    author: AuthorSubject,
    table: &str,
) -> Vec<RowUuid> {
    let worker = open_db(0x73, author, schema);
    let client = open_db(0x74, author, schema);
    let claims = test_provider_claims(author);
    let (worker_transport, core_transport) = duplex();
    let _worker_upstream = crate::db::block_on(worker.connect_upstream(worker_transport));
    let core_subscriber =
        server.accept_subscriber_with_claims(core_transport, author, claims.clone());
    let (client_transport, worker_sub_transport) = duplex();
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _worker_subscriber =
        worker.accept_subscriber_with_claims(worker_sub_transport, author, claims.clone());

    let query = Query::from(table);
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    let mut rows = BTreeSet::new();
    for _ in 0..20 {
        client.tick().unwrap();
        worker.tick().unwrap();
        server.server.tick().unwrap();
        worker.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            if let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            {
                if reset {
                    rows.clear();
                }
                for row in removed {
                    rows.remove(&row.row_uuid);
                }
                for row in added.into_iter().chain(updated) {
                    rows.insert(row.row_uuid());
                }
            }
        }
    }
    let connection = core_subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        panic!("browser worker's core-facing connection must be a subscriber link");
    };
    assert!(
        state
            .coverage_groups
            .values()
            .any(|group| group.policy_binding == (author, claims.clone())),
        "the core must use the worker upstream's authenticated session binding"
    );
    rows.into_iter().collect()
}

#[test]
fn db_surface_recursive_reachable_claim_policy_subscription_routes_per_identity() {
    let schema = benchmark_shaped_recursive_reachable_read_schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let member = AuthorSubject::for_test_bytes([0x11; 16]);
    let admin = AuthorSubject::SYSTEM;
    let spy = AuthorSubject::for_test_bytes([0x33; 16]);
    let (direct_doc, inherited_doc) = seed_recursive_reachable_read_fixture(&server, member);

    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "res_a"),
        vec![direct_doc, inherited_doc]
    );
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, admin, "res_a"),
        vec![direct_doc, inherited_doc, row(0xd3)]
    );
    assert!(served_subscription_rows_for_author(&schema, &server, spy, "res_a").is_empty());
    assert_eq!(
        served_subscription_rows_for_author(&schema, &server, member, "group_entry"),
        (0..42).map(|i| row(0xc1 + i)).collect::<Vec<_>>()
    );
    let rows = served_many_subscription_rows_for_author(
        &schema,
        &server,
        member,
        &["group", "res_a_access_edges", "res_a", "group_entry"],
    );
    assert_eq!(
        rows["group_entry"],
        (0..42).map(|i| row(0xc1 + i)).collect::<Vec<_>>()
    );
    let (relay_rows, client_one_shot, relay_one_shot) =
        served_table_rows_via_relay(&schema, &server, member, "group_entry");
    assert_eq!(relay_one_shot, 42);
    assert_eq!(client_one_shot, 42);
    assert_eq!(
        relay_rows,
        (0..42).map(|i| row(0xc1 + i)).collect::<Vec<_>>()
    );
    let (spy_relay_rows, spy_client_one_shot, spy_relay_one_shot) =
        served_table_rows_via_relay(&schema, &server, spy, "res_a");
    assert_eq!(spy_relay_one_shot, 0);
    assert_eq!(spy_client_one_shot, 0);
    assert!(spy_relay_rows.is_empty());

    assert_eq!(
        served_table_rows_via_ordinary_browser_worker(&schema, &server, member, "group_entry"),
        (0..42).map(|i| row(0xc1 + i)).collect::<Vec<_>>(),
        "an ordinary browser-worker relay receives its authenticated session's authority view"
    );
}
