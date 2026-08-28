// Identity-scoped reads, peer delivery, Edge rehydration, and deletion visibility.

fn private_message_membership_schema() -> JazzSchema {
    let member_exists = |outer_column: &str| {
        public_outer_exists(
            "chatMembers",
            "chatId",
            outer_column,
            [public_claim_eq("userId", "user_id")],
        )
    };
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_claim_eq("userId", "user_id"),
                        member_exists("chatId"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("isPublic", PublicColumnType::Boolean)
                    .column("createdBy", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", PublicColumnType::Text)
                    .column("createdAt", PublicColumnType::Timestamp)
                    .policies(public_all_policies().with_select(member_exists("chatId"))),
            ),
    )
}

#[test]
fn message_read_policy_allows_public_chat_or_membership_join() {
    let member = user(0xa1);
    let other = user(0xb2);
    let public_chat = row(0x18);
    let private_chat = row(0x19);
    let public_message = row(0x28);
    let private_message = row(0x29);
    let membership = row(0x1a);
    let member_exists = |outer_column: &str| {
        public_outer_exists(
            "chat_members",
            "chat_id",
            outer_column,
            [public_claim_eq("user_id", "user_id")],
        )
    };
    let public_chat_exists = |outer_column: &str| {
        public_outer_exists(
            "chats",
            "id",
            outer_column,
            [public_literal_eq(
                "visibility",
                PublicValue::Text("public".to_owned()),
            )],
        )
    };
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .column("visibility", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_literal_eq(
                            "visibility",
                            PublicValue::Text("public".to_owned()),
                        ),
                        member_exists("id"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("chat_members")
                    .fk_column("chat_id", "chats")
                    .column("user_id", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_claim_eq("user_id", "user_id"),
                        member_exists("chat_id"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chat_id", "chats")
                    .column("text", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_chat_exists("chat_id"),
                        member_exists("chat_id"),
                    ]))),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    core.set_test_provider_claims(
        member,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(member.test_uuid().to_string()),
        )]),
    );
    core.set_test_provider_claims(
        other,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(other.test_uuid().to_string()),
        )]),
    );

    accept_global(
        &mut core,
        MergeableCommit::new("chats", public_chat, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("public".to_owned())),
            ("visibility".to_owned(), Value::String("public".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chats", private_chat, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("private".to_owned())),
            ("visibility".to_owned(), Value::String("private".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("messages", public_message, 12).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(public_chat.0)),
            ("text".to_owned(), Value::String("public message".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("messages", private_message, 13).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("text".to_owned(), Value::String("private message".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chat_members", membership, 14).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("user_id".to_owned(), Value::String(member.test_uuid().to_string())),
        ])),
    );

    let public_shape = Query::from("messages")
        .join_via_row_id("chats", "chat_id", [eq(col("visibility"), lit("public"))])
        .validate(&core.catalogue.schema)
        .unwrap();
    let public_binding = public_shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows(&public_shape, &public_binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );
    assert_eq!(
        core.query_rows(&public_shape, &public_binding, DurabilityTier::Edge)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );

    let shape = Query::from("messages")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message, private_message])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message, private_message])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, other)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );
}

#[test]
fn read_policy_compares_indirect_text_by_its_logical_value() {
    let reader = user(0xa1);
    let allowed = "policy-visible/".repeat(6_000);
    let denied = format!("{}x", &allowed[..allowed.len() - 1]);
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("documents")
            .column("classification", PublicColumnType::Text)
            .policies(
                public_all_policies().with_select(public_literal_eq(
                    "classification",
                    PublicValue::Text(allowed.clone()),
                )),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(0x6c), schema);
    let visible = row(0x6c);
    let hidden = row(0x6d);
    accept_global(
        &mut core,
        MergeableCommit::new("documents", visible, 10).cells(BTreeMap::from([(
            "classification".to_owned(),
            Value::String(allowed.clone()),
        )])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("documents", hidden, 11).cells(BTreeMap::from([(
            "classification".to_owned(),
            Value::String(denied),
        )])),
    );

    let table = core.table("documents").unwrap().clone();
    let physical = core.query_table_versions("documents").unwrap();
    assert!(matches!(physical[0].cell(&table, "classification"), Ok(Some(Value::Large(_)))));
    assert_eq!(
        core.current_rows("documents", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.row_uuid() == visible)
            .and_then(|candidate| candidate.cell(&table, "classification")),
        Some(Value::String(allowed.clone()))
    );
    let direct = Query::from("documents")
        .filter(eq(col("classification"), lit(allowed.clone())))
        .validate(&core.catalogue.schema)
        .unwrap();
    let direct_binding = direct.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_for_link(
            &direct,
            &direct_binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
        )
        .unwrap()
        .len(),
        1
    );

    assert!(
        core.dry_run_read_current_allows("documents", visible, reader)
            .unwrap()
    );
    assert!(
        !core
            .dry_run_read_current_allows("documents", hidden, reader)
            .unwrap()
    );
}

#[test]
fn camel_case_message_read_policy_incrementally_adds_member_message() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let chat = row(0x18);
    let alice_profile = row(0x38);
    let bob_profile = row(0x39);
    let alice_message = row(0x28);
    let bob_message = row(0x29);
    let alice_membership = row(0x1a);
    let bob_membership = row(0x1b);
    let member_exists = |outer_column: &str| {
        public_outer_exists(
            "chatMembers",
            "chatId",
            outer_column,
            [public_claim_eq("userId", "user_id")],
        )
    };
    let public_chat_exists = |outer_column: &str| {
        public_outer_exists(
            "chats",
            "id",
            outer_column,
            [public_literal_eq("isPublic", PublicValue::Boolean(true))],
        )
    };
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("isPublic", PublicColumnType::Boolean)
                    .column("createdBy", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_literal_eq("isPublic", PublicValue::Boolean(true)),
                        member_exists("id"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_claim_eq("userId", "user_id"),
                        member_exists("chatId"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chatId", "chats")
                    .column("text", PublicColumnType::Text)
                    .fk_column("senderId", "profiles")
                    .column("createdAt", PublicColumnType::Timestamp)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_chat_exists("chatId"),
                        member_exists("chatId"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("profiles")
                    .column("userId", PublicColumnType::Text)
                    .column("name", PublicColumnType::Text)
                    .policies(public_all_policies()),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    accept_global(
        &mut core,
        MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
            ("isPublic".to_owned(), Value::Bool(true)),
            ("createdBy".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", alice_membership, 11).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("messages", alice_message, 12).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("text".to_owned(), Value::String("hello".to_owned())),
            ("senderId".to_owned(), Value::Uuid(alice_profile.0)),
            ("createdAt".to_owned(), Value::U64(12)),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("profiles", alice_profile, 15).cells(BTreeMap::from([
            ("userId".to_owned(), Value::String(alice.test_uuid().to_string())),
            ("name".to_owned(), Value::String("Alice".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("profiles", bob_profile, 16).cells(BTreeMap::from([
            ("userId".to_owned(), Value::String(bob.test_uuid().to_string())),
            ("name".to_owned(), Value::String("Bob".to_owned())),
        ])),
    );

    let shape = Query::from("messages")
        .include("senderId")
        .order_by("createdAt", OrderDirection::Desc)
        .limit(21)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut alice_peer = PeerState::client_link(alice);
    alice_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();

    let bob_membership_tx = accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", bob_membership, 13).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(bob.test_uuid().to_string())),
        ])),
    );
    let bob_message_tx = accept_global(
        &mut core,
        MergeableCommit::new("messages", bob_message, 14).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("text".to_owned(), Value::String("from bob".to_owned())),
            ("senderId".to_owned(), Value::Uuid(bob_profile.0)),
            ("createdAt".to_owned(), Value::U64(14)),
        ])),
    );

    let update = alice_peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([bob_message, bob_profile]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([bob_message, bob_profile]));
    assert!(matches!(
        update,
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                result_member_adds: ref adds,
                ..
        }) if adds.iter().any(|entry| entry == &("messages".to_owned().into(), bob_message, bob_message_tx))
    ));
    let _ = bob_membership_tx;
}

#[test]
fn edge_read_policy_joins_use_edge_visible_dependency_rows() {
    let member = user(0xa1);
    let other = user(0xb2);
    let bob = user(0xc3);
    let public_chat = row(0x18);
    let private_chat = row(0x19);
    let public_message = row(0x28);
    let private_message = row(0x29);
    let bob_private_message = row(0x2a);
    let membership = row(0x1a);
    let bob_membership = row(0x1b);
    let member_exists = |outer_column: &str| {
        public_outer_exists(
            "chat_members",
            "chat_id",
            outer_column,
            [public_claim_eq("user_id", "user_id")],
        )
    };
    let public_chat_exists = |outer_column: &str| {
        public_outer_exists(
            "chats",
            "id",
            outer_column,
            [public_literal_eq(
                "visibility",
                PublicValue::Text("public".to_owned()),
            )],
        )
    };
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chat_members")
                    .fk_column("chat_id", "chats")
                    .column("user_id", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .column("visibility", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_literal_eq(
                            "visibility",
                            PublicValue::Text("public".to_owned()),
                        ),
                        member_exists("id"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chat_id", "chats")
                    .column("text", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_chat_exists("chat_id"),
                        member_exists("chat_id"),
                    ]))),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for identity in [member, other, bob] {
        core.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                "user_id".to_owned(),
                Value::String(identity.test_uuid().to_string()),
            )]),
        );
    }
    for commit in [
        MergeableCommit::new("chats", public_chat, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("public".to_owned())),
            ("visibility".to_owned(), Value::String("public".to_owned())),
        ])),
        MergeableCommit::new("chats", private_chat, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("private".to_owned())),
            ("visibility".to_owned(), Value::String("private".to_owned())),
        ])),
        MergeableCommit::new("messages", public_message, 12).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(public_chat.0)),
            ("text".to_owned(), Value::String("public message".to_owned())),
        ])),
        MergeableCommit::new("messages", private_message, 13).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("text".to_owned(), Value::String("private message".to_owned())),
        ])),
        MergeableCommit::new("chat_members", membership, 14).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("user_id".to_owned(), Value::String(member.test_uuid().to_string())),
        ])),
        MergeableCommit::new("chat_members", bob_membership, 15).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("user_id".to_owned(), Value::String(bob.test_uuid().to_string())),
        ])),
        MergeableCommit::new("messages", bob_private_message, 16).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("text".to_owned(), Value::String("bob private message".to_owned())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many_settled(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }

    let shape = Query::from("messages")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.query.settled_result_sets.insert(
        crate::protocol::BindingViewKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        read_view: Default::default(),
},
        BTreeSet::new(),
    );
    assert!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, member)
            .unwrap()
            .is_empty(),
        "global policy reads must not be authorized by edge-only dependency rows",
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message, private_message, bob_private_message])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, other)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );

    let mut other_peer = PeerState::edge_client(other);
    let update = other_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([public_chat, public_message]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([public_chat, public_message]));

    let mut member_peer = PeerState::edge_client(member);
    let update = member_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(
        &update,
        BTreeSet::from([
            public_chat,
            private_chat,
            public_message,
            private_message,
            bob_private_message,
        ]),
    );
    assert_view_update_only_ships_rows(
        &update,
        BTreeSet::from([
            public_chat,
            private_chat,
            public_message,
            private_message,
            bob_private_message,
        ]),
    );
}

#[test]
fn edge_membership_insert_updates_previously_empty_private_message_query() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let chat = row(0x18);
    let seed_message = row(0x28);
    let alice_membership = row(0x1a);
    let bob_membership = row(0x1b);
    let schema = private_message_membership_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for identity in [alice, bob] {
        core.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                "user_id".to_owned(),
                Value::String(identity.test_uuid().to_string()),
            )]),
        );
    }
    for commit in [
        MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
            ("isPublic".to_owned(), Value::Bool(false)),
            ("createdBy".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
        MergeableCommit::new("chatMembers", alice_membership, 11).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many_settled(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }
    let seed_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("messages", seed_message, 12).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("text".to_owned(), Value::String("invite-only seed".to_owned())),
                ("createdAt".to_owned(), Value::U64(12)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(seed_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chatId")))
        .order_by("createdAt", OrderDirection::Asc)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("chatId".to_owned(), Value::Uuid(chat.0))]))
        .unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };
    let mut bob_peer = PeerState::edge_client(bob);
    let initial = bob_peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts)
        .unwrap();
    assert_view_update_only_references_rows(&initial, BTreeSet::new());

    let bob_membership_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("chatMembers", bob_membership, 13).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("userId".to_owned(), Value::String(bob.test_uuid().to_string())),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(
        bob_membership_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();

    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, bob)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([seed_message])
    );

    let update = bob_peer
        .query_update_for_subscription(&mut core, subscription, &shape, &binding)
        .unwrap();
    assert!(matches!(
        update,
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                result_member_adds: ref adds,
                ..
        }) if adds.iter().any(|entry| entry == &("messages".to_owned().into(), seed_message, seed_tx))
    ));
}

#[test]
fn edge_rehydrate_refreshes_previously_covered_private_message_query() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let chat = row(0x18);
    let seed_message = row(0x28);
    let bob_message = row(0x29);
    let alice_membership = row(0x1a);
    let bob_membership = row(0x1b);
    let schema = private_message_membership_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for identity in [alice, bob] {
        core.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                "user_id".to_owned(),
                Value::String(identity.test_uuid().to_string()),
            )]),
        );
    }
    for commit in [
        MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
            ("isPublic".to_owned(), Value::Bool(false)),
            ("createdBy".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
        MergeableCommit::new("chatMembers", alice_membership, 11).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many_settled(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }
    let seed_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("messages", seed_message, 12).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("text".to_owned(), Value::String("invite-only seed".to_owned())),
                ("createdAt".to_owned(), Value::U64(12)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(seed_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chatId")))
        .order_by("createdAt", OrderDirection::Desc)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("chatId".to_owned(), Value::Uuid(chat.0))]))
        .unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    let mut alice_peer = PeerState::edge_client(alice);
    let initial = alice_peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts.clone())
        .unwrap();
    assert!(matches!(
        initial,
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                result_member_adds: ref adds,
                reset_result_set: true,
                ..
        }) if adds.iter().any(|entry| entry == &("messages".to_owned().into(), seed_message, seed_tx))
    ));

    let bob_membership_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("chatMembers", bob_membership, 13).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("userId".to_owned(), Value::String(bob.test_uuid().to_string())),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(
        bob_membership_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    let bob_message_tx = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("messages", bob_message, 14).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("text".to_owned(), Value::String("bob accepted invite".to_owned())),
                ("createdAt".to_owned(), Value::U64(14)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(bob_message_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let rehydrated = alice_peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts)
        .unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        reset_result_set,
        ..
    }) = rehydrated
    else {
        panic!("expected rehydrate view update");
    };
    assert!(reset_result_set);
    assert_eq!(
        result_member_adds
            .into_iter()
            .filter_map(crate::protocol::ResultMemberEntry::into_row)
            .filter(|(table, _, _)| table.as_str() == "messages")
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ("messages".to_owned().into(), seed_message, seed_tx),
            ("messages".to_owned().into(), bob_message, bob_message_tx),
        ])
    );
}

#[test]
fn edge_public_or_owner_claim_policy_rehydrates_empty_result_set() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let private_chat = row(0x18);
    let public_chat = row(0x19);
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("chats")
            .column("title", PublicColumnType::Text)
            .column("visibility", PublicColumnType::Text)
            .column("owner_id", PublicColumnType::Text)
            .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                public_literal_eq(
                    "visibility",
                    PublicValue::Text("public".to_owned()),
                ),
                public_claim_eq("owner_id", "user_id"),
            ]))),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for commit in [
        MergeableCommit::new("chats", private_chat, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("private".to_owned())),
            ("visibility".to_owned(), Value::String("private".to_owned())),
            ("owner_id".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
        MergeableCommit::new("chats", public_chat, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("public".to_owned())),
            ("visibility".to_owned(), Value::String("public".to_owned())),
            ("owner_id".to_owned(), Value::String(alice.test_uuid().to_string())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many_settled(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::edge_client(bob);
    let update = bob_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([public_chat]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([public_chat]));
}

#[test]
fn composed_read_policy_grants_and_revokes_incrementally() {
    let invited = user(0xa1);
    let spy = user(0xb2);
    let canvas_row = row(8);
    let shape_row = row(10);
    let invite_row = row(9);
    let shape_policy = public_outer_exists(
        "canvasInvites",
        "canvas",
        "canvas",
        [public_claim_eq("userID", "sub")],
    );
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("canvases")
                    .column("title", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("shapes")
                    .fk_column("canvas", "canvases")
                    .column("title", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(shape_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("canvasInvites")
                    .fk_column("canvas", "canvases")
                    .column("userID", PublicColumnType::Uuid),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, invited);
    install_test_uuid_sub_claim(&mut core, spy);
    let shape = Query::from("shapes")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = crate::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    read_view: Default::default(),
};

    let canvas_tx =
        core.commit_mergeable_settled(MergeableCommit::new("canvases", canvas_row, 10).cells(
            BTreeMap::from([("title".to_owned(), Value::String("policy-row".to_owned()))]),
        ))
        .unwrap();
    core.apply_fate_update(
        canvas_tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let shape_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("shapes", shape_row, 11).cells(BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas_row.0)),
                ("title".to_owned(), Value::String("policy-row".to_owned())),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        shape_tx,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let mut invited_link = PeerState::client_link(invited);
    let mut spy_link = PeerState::client_link(spy);
    let invited_initial = invited_link
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let spy_initial = spy_link
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    assert!(matches!(
        invited_initial,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            result_member_adds: ref adds,
            ..
        }) if adds.is_empty()
    ));
    assert!(matches!(
        spy_initial,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            result_member_adds: ref adds,
            ..
        }) if adds.is_empty()
    ));
    assert_eq!(
        core.query
            .query_shape_cache
            .keys()
            .filter(|(_, tier, _)| *tier == DurabilityTier::Global)
            .count(),
        1,
        "identities with the same shape and policy should share one prepared graph"
    );

    let invite_tx = core
        .commit_mergeable_settled(MergeableCommit::new("canvasInvites", invite_row, 12).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas_row.0)),
                ("userID".to_owned(), Value::Uuid(invited.test_uuid())),
            ]),
        ))
        .unwrap();
    core.apply_fate_update(
        invite_tx,
        Fate::Accepted,
        Some(GlobalTime(3)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let grant_update = invited_link
        .query_update(&mut core, &shape, &binding)
        .unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = grant_update
    else {
        panic!("expected grant update");
    };
    assert_eq!(
        result_member_adds,
        vec![
            ("canvases".to_owned().into(), canvas_row, canvas_tx),
            ("shapes".to_owned().into(), shape_row, shape_tx),
        ]
    );
    assert!(result_member_removes.is_empty());
    assert_eq!(invited_link.metrics.view_updates_out, 2);

    let spy_update = spy_link.query_update(&mut core, &shape, &binding).unwrap();
    assert!(matches!(
        spy_update,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            result_member_adds: ref adds,
            result_member_removes: ref removes,
            ..
        }) if adds.is_empty() && removes.is_empty()
    ));
    assert_eq!(spy_link.metrics.result_adds_out, 0);
    assert_eq!(spy_link.metrics.version_bundles_out, 0);

    let revoke_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("canvasInvites", invite_row, 13).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        revoke_tx,
        Fate::Accepted,
        Some(GlobalTime(4)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let revoke_update = invited_link
        .query_update(&mut core, &shape, &binding)
        .unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = revoke_update
    else {
        panic!("expected revoke update");
    };
    assert!(result_member_adds.is_empty());
    assert_eq!(
        result_member_removes,
        vec![
            ("canvases".to_owned().into(), canvas_row, canvas_tx),
            ("shapes".to_owned().into(), shape_row, shape_tx),
        ]
    );
    assert_eq!(invited_link.metrics.view_updates_out, 3);
    assert_eq!(
        invited_link.subscription_result_sets(subscription),
        Some(BTreeSet::new())
    );
}
#[test]
fn system_identity_read_policy_sees_everything() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(3), schema);
    commit_core_owner_fixture(&mut core, row(1), user(0xa1), "a row", 10);
    commit_core_owner_fixture(&mut core, row(2), user(0xb2), "b row", 11);
    let mut peer = PeerState::new();

    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([row(1), row(2)]));
    reader.apply_sync_message_settled(update).unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(1), row(2)])
    );
}

#[test]
fn relay_and_edge_peer_identities_drive_policy_composed_reads() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let owner = user(0xa1);
    let other = user(0xb2);
    commit_core_owner_fixture(&mut core, row(1), owner, "owned", 10);

    let mut relay = PeerState::relay();
    assert_eq!(relay.identity(), AuthorSubject::SYSTEM);
    assert_view_update_only_references_rows(
        &relay.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(1)]),
    );

    let mut edge_owner = PeerState::edge_client(owner);
    assert_eq!(edge_owner.identity(), owner);
    assert_view_update_only_references_rows(
        &edge_owner.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(1)]),
    );

    let mut edge_other = PeerState::edge_client(other);
    assert_eq!(edge_other.identity(), other);
    assert_view_update_only_references_rows(
        &edge_other.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::new(),
    );
}

#[test]
fn edge_query_rehydrate_applies_session_user_id_read_policy() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .column("visibility", PublicColumnType::Text)
                    .column("owner_id", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_literal_eq(
                            "visibility",
                            PublicValue::Text("public".to_owned()),
                        ),
                        public_claim_eq("owner_id", "user_id"),
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .column("chat_id", PublicColumnType::Uuid)
                    .column("body", PublicColumnType::Text)
                    .column("author_id", PublicColumnType::Text)
                    .column("owner_id", PublicColumnType::Text)
                    .policies(
                        public_all_policies()
                            .with_select(public_claim_eq("owner_id", "user_id")),
                    ),
            ),
    );
    let (_alice_dir, mut alice) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice_id = user(0xa1);
    let bob_id = user(0xb2);
    let alice_user_id = alice_id.test_uuid().to_string();
    let bob_user_id = bob_id.test_uuid().to_string();
    core.set_test_provider_claims(
        bob_id,
        BTreeMap::from([("user_id".to_owned(), v(bob_user_id.clone()))]),
    );
    let alice_private_chat_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("chats", row(0x10), 10)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("title".to_owned(), v("alice private")),
                ("visibility".to_owned(), v("private")),
                ("owner_id".to_owned(), v(alice_user_id.clone())),
            ])),
    );
    core.apply_fate_update(
        alice_private_chat_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    let public_chat_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("chats", row(0x11), 11)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("title".to_owned(), v("public")),
                ("visibility".to_owned(), v("public")),
                ("owner_id".to_owned(), v(alice_user_id.clone())),
            ])),
    );
    core.apply_fate_update(public_chat_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();
    let alice_private_message_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("messages", row(0x20), 12)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("chat_id".to_owned(), Value::Uuid(row(0x10).0)),
                ("body".to_owned(), v("alice private message")),
                ("author_id".to_owned(), v(alice_user_id)),
                ("owner_id".to_owned(), v(alice_id.test_uuid().to_string())),
            ])),
    );
    core.apply_fate_update(
        alice_private_message_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    let bob_message_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("messages", row(0x21), 13)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("chat_id".to_owned(), Value::Uuid(row(0x11).0)),
                ("body".to_owned(), v("bob message")),
                ("author_id".to_owned(), v(bob_user_id.clone())),
                ("owner_id".to_owned(), v(bob_user_id)),
            ])),
    );
    core.apply_fate_update(bob_message_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let mut bob = PeerState::edge_client(bob_id);
    let chat_shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let chat_binding = chat_shape.bind(BTreeMap::new()).unwrap();
    let message_shape = Query::from("messages")
        .validate(&core.catalogue.schema)
        .unwrap();
    let message_binding = message_shape.bind(BTreeMap::new()).unwrap();

    let chat_update = bob
        .rehydrate_query_with_opts(
            &mut core,
            &chat_shape,
            &chat_binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&chat_update, BTreeSet::from([row(0x11)]));
    assert_view_update_only_ships_rows(&chat_update, BTreeSet::from([row(0x11)]));

    let message_update = bob
        .rehydrate_query_with_opts(
            &mut core,
            &message_shape,
            &message_binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&message_update, BTreeSet::from([row(0x21)]));
    assert_view_update_only_ships_rows(&message_update, BTreeSet::from([row(0x21)]));
}

#[test]
fn edge_query_rehydrate_ships_public_chat_from_chat_policy_schema() {
    let member_exists = public_outer_exists(
        "chat_members",
        "chat_id",
        "id",
        [public_claim_eq("user_id", "user_id")],
    );
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .column("visibility", PublicColumnType::Text)
                    .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                        public_literal_eq(
                            "visibility",
                            PublicValue::Text("public".to_owned()),
                        ),
                        member_exists,
                    ]))),
            )
            .table(
                PublicTableSchemaBuilder::new("chat_members")
                    .fk_column("chat_id", "chats")
                    .column("user_id", PublicColumnType::Text)
                    .policies(public_all_policies()),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    let public_chat = row(0x11);
    let chat_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("chats", public_chat, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("public")),
                    ("visibility".to_owned(), v("public")),
                ])),
        )
        .unwrap();
    core.apply_fate_update(chat_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::edge_client(bob);

    let update = bob_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();

    assert_view_update_only_references_rows(&update, BTreeSet::from([public_chat]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([public_chat]));
}

/// A source-harness regression for row-scoped read policy plus query output
/// projection. Two fresh edge-client links ask for the same readable chat via
/// different public projections; both wire payloads must be the identical
/// complete canonical row version. `select` shapes terminal output only.
#[test]
fn public_chat_projections_ship_identical_complete_row_versions() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("chats")
            .column("title", PublicColumnType::Text)
            .column("visibility", PublicColumnType::Text)
            .policies(public_all_policies().with_select(public_literal_eq(
                "visibility",
                PublicValue::Text("public".to_owned()),
            ))),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(0x62), schema.clone());
    let reader = user(0x63);
    let public_chat = row(0x64);
    let private_chat = row(0x65);
    let public_tx = accept_global(
        &mut core,
        MergeableCommit::new("chats", public_chat, 10).cells(BTreeMap::from([
            ("title".to_owned(), v("public title")),
            ("visibility".to_owned(), v("public")),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chats", private_chat, 11).cells(BTreeMap::from([
            ("title".to_owned(), v("private title")),
            ("visibility".to_owned(), v("private")),
        ])),
    );

    let full_shape = Query::from("chats").validate(&schema).unwrap();
    let title_shape = Query::from("chats")
        .select(["title"])
        .validate(&schema)
        .unwrap();
    let mut full_link = PeerState::edge_client(reader);
    let full_update = full_link
        .rehydrate_query(&mut core, &full_shape, &full_shape.bind(BTreeMap::new()).unwrap())
        .unwrap();
    let mut title_link = PeerState::edge_client(reader);
    let title_update = title_link
        .rehydrate_query(
            &mut core,
            &title_shape,
            &title_shape.bind(BTreeMap::new()).unwrap(),
        )
        .unwrap();

    for update in [&full_update, &title_update] {
        assert_view_update_only_references_rows(update, BTreeSet::from([public_chat]));
        assert_view_update_only_ships_rows(update, BTreeSet::from([public_chat]));
    }
    let full_version = version_bundles_for_update(&full_update)
        .into_iter()
        .flat_map(|bundle| bundle.versions)
        .find(|version| version.table() == "chats" && version.row_uuid() == public_chat)
        .expect("full query must ship public chat payload");
    let title_version = version_bundles_for_update(&title_update)
        .into_iter()
        .flat_map(|bundle| bundle.versions)
        .find(|version| version.table() == "chats" && version.row_uuid() == public_chat)
        .expect("title query must ship public chat payload");
    let canonical = core
        .query_versions_for_tx(public_tx)
        .unwrap()
        .into_iter()
        .find(|version| version.table() == "chats" && version.row_uuid() == public_chat)
        .map(|version| core.version_record_from_row(&version).unwrap())
        .expect("public chat must have canonical history payload");
    assert_eq!(full_version, canonical);
    assert_eq!(title_version, canonical);

    let title_rows = core
        .query_rows_for_link(
            &title_shape,
            &title_shape.bind(BTreeMap::new()).unwrap(),
            DurabilityTier::Global,
            reader,
        )
        .unwrap();
    assert_eq!(title_rows.len(), 1);
    assert_eq!(title_rows[0].cell(&schema.tables[0], "title"), Some(v("public title")));
    assert_eq!(
        title_rows[0].cell(&schema.tables[0], "visibility"),
        None,
        "select projection belongs to terminal output, not VersionRecord payloads"
    );
}

#[test]
fn nullable_join_code_claim_branch_allows_edge_chat_read() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("chats")
            .column("title", PublicColumnType::Text)
            .nullable_column("joinCode", PublicColumnType::Text)
            .policies(
                public_all_policies().with_select(public_claim_eq("joinCode", "join_code")),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let reader = user(0xb2);
    let chat = row(0x31);
    let join_code = "jazz-join-123";
    let tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("chats", chat, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("private by join code")),
                    (
                        "joinCode".to_owned(),
                        Value::Nullable(Some(Box::new(v(join_code)))),
                    ),
                ])),
        )
        .unwrap();
    core.apply_fate_update(tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();
    core.set_test_provider_claims(
        reader,
        BTreeMap::from([("join_code".to_owned(), v(join_code))]),
    );

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, reader)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([chat])
    );

    let mut reader_peer = PeerState::edge_client(reader);
    let update = reader_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();

    assert_view_update_only_references_rows(&update, BTreeSet::from([chat]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([chat]));
}

#[test]
fn edge_query_rehydrate_resets_empty_result_for_denied_private_chat() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("chats")
            .column("title", PublicColumnType::Text)
            .column("visibility", PublicColumnType::Text)
            .column("owner_id", PublicColumnType::Text)
            .policies(public_all_policies().with_select(PublicPolicyExpr::Or(vec![
                public_literal_eq(
                    "visibility",
                    PublicValue::Text("public".to_owned()),
                ),
                public_claim_eq("owner_id", "user_id"),
            ]))),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    let private_chat = row(0x12);
    let tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("chats", private_chat, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("private")),
                    ("visibility".to_owned(), v("private")),
                    ("owner_id".to_owned(), v(alice.test_uuid().to_string())),
                ])),
        )
        .unwrap();
    core.apply_fate_update(tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::edge_client(bob);

    let update = bob_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();

    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        result_member_adds,
        version_carriers,
        ..
    }) = update
    else {
        panic!("expected view update");
    };
    assert!(reset_result_set);
    assert!(result_member_adds.is_empty());
    assert!(version_carriers.is_empty());
}

#[test]
fn deletion_read_policy_requires_visible_global_content_winner() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid)
            .policies(
                PublicTablePolicies::new().with_select(public_claim_eq("owner", "sub")),
            ),
    ));
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner = user(0xa1);
    let other = user(0xb2);
    install_test_uuid_sub_claim(&mut core, owner);
    install_test_uuid_sub_claim(&mut core, other);
    let row_uuid = row(0x81);
    let content = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(owner_cells(owner, "visible")),
        )
        .unwrap();
    core.apply_fate_update(
        content,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let deletion = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 11).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        deletion,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let owner_rows = core
        .query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            owner,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    assert_eq!(owner_rows.len(), 1);
    assert_eq!(owner_rows[0].row_uuid(), row_uuid);
    assert!(owner_rows[0].is_deleted());
    assert!(
        core.query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            other,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap()
        .is_empty()
    );

    let orphan_row = row(0x82);
    let orphan_deletion = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", orphan_row, 12).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        orphan_deletion,
        Fate::Accepted,
        Some(GlobalTime(3)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    assert!(
        core.query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            owner,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap()
        .into_iter()
        .all(|row| row.row_uuid() != orphan_row)
    );
}
