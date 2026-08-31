//! authorization query-evaluation tests.

use super::*;
use crate::node::query_eval::authorization::permission_scope_claim_values;

#[test]
fn permission_advice_scope_preserves_provider_sub_and_injects_canonical_user() {
    let author = AuthorSubject::authenticated("https://issuer.example", "opaque-subject").unwrap();
    let claims = BTreeMap::from([("sub".to_owned(), Value::String("spoofed".to_owned()))]);

    let values = permission_scope_claim_values(author, Some(&claims));

    assert_eq!(
        values.get("sub"),
        Some(&Value::String("spoofed".to_owned()))
    );
    assert_eq!(
        values.get("user"),
        Some(&Value::String(author.canonical().to_owned()))
    );
}

#[test]
fn negated_null_membership_policy_does_not_authorize_null_rows() {
    let mut schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .nullable_column("classification", PublicColumnType::Text)
                .nullable_column("null_option", PublicColumnType::Text),
        ),
    );
    schema.runtime_mut_for_testing().tables[0].read_policy = Some(Query::from("documents").filter(
        crate::query::not(in_list(col("classification"), [col("null_option")])),
    ));
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0x98; 16]), schema.clone());
    for (id, timestamp, classification) in [
        (row(1), 1_001, Value::Nullable(None)),
        (
            row(2),
            1_002,
            Value::Nullable(Some(Box::new(Value::String("public".to_owned())))),
        ),
    ] {
        node.commit_mergeable_unit_settled(
            MergeableCommit::new("documents", id, timestamp)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([
                    ("classification".to_owned(), classification),
                    ("null_option".to_owned(), Value::Nullable(None)),
                ])),
        )
        .unwrap();
    }

    let reader = author(9);
    let shape = Query::from("documents").validate_runtime(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let visible = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Local, reader)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(visible, BTreeSet::from([row(2)]));
}

#[test]
fn nested_read_policy_claim_slots_do_not_cross_validated_types() {
    let schema = RuntimeSchema::new([
        TableSchema::new(
            "uuid_owners",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        ),
        TableSchema::new(
            "other_string_owners",
            [ColumnSchema::new("owner", ColumnType::String)],
        ),
        TableSchema::new(
            "nullable_string_owners",
            [ColumnSchema::new("owner", ColumnType::String.nullable())],
        ),
    ]);
    let provider_sub = crate::query::provider_claim_key("sub");
    let provider_path = ClaimPath(vec!["claims".to_owned(), "sub".to_owned()]);
    let plain_name = claim_param_field(&provider_path);
    let outer_slots = BTreeMap::from([(
        plain_name.clone(),
        ProgramClaimParam {
            path: provider_path,
            ty: ColumnType::String,
        },
    )]);
    let mut query =
        Query::from("uuid_owners").filter(eq(col("owner"), claim(provider_sub.clone())));
    let mut binding_values = BTreeMap::new();
    bind_scope_claim_operands(
        &mut query,
        &BTreeMap::from([(provider_sub, Value::String("not-a-uuid".to_owned()))]),
        &mut binding_values,
    );
    let slots = disambiguate_policy_claim_params_with_outer_slots(
        &mut query,
        &schema,
        &mut binding_values,
        &outer_slots,
    )
    .expect("the policy shape is valid before binding values");

    let uuid_slot = typed_claim_param_alias(&plain_name, &ColumnType::Uuid);
    assert!(slots.contains_key(&uuid_slot));
    assert!(
        !slots.contains_key(&plain_name),
        "a String outer claim slot must never be reused for a UUID policy operand"
    );
    assert!(
        query
            .validate_runtime(&schema)
            .unwrap()
            .params()
            .contains_key(&uuid_slot),
        "the policy query itself must reference the UUID-specific slot"
    );

    let other_path = ClaimPath(vec!["account_id".to_owned()]);
    let other_name = claim_param_field(&other_path);
    let mut other_path_query =
        Query::from("other_string_owners").filter(eq(col("owner"), claim("account_id")));
    let mut other_path_values = BTreeMap::new();
    bind_scope_claim_operands(
        &mut other_path_query,
        &BTreeMap::from([(
            "account_id".to_owned(),
            Value::String("same-type-different-path".to_owned()),
        )]),
        &mut other_path_values,
    );
    let other_path_slots = disambiguate_policy_claim_params_with_outer_slots(
        &mut other_path_query,
        &schema,
        &mut other_path_values,
        &outer_slots,
    )
    .expect("same-type claim at another path validates");
    let other_path_alias = typed_claim_param_alias(&other_name, &ColumnType::String);
    assert!(other_path_slots.contains_key(&other_path_alias));
    assert!(
        !other_path_slots.contains_key(&plain_name),
        "an outer user_id slot must not be reused by account_id merely because both are String"
    );

    let mut nullable_query = Query::from("nullable_string_owners").filter(eq(
        col("owner"),
        claim(crate::query::provider_claim_key("sub")),
    ));
    let mut nullable_values = BTreeMap::new();
    bind_scope_claim_operands(
        &mut nullable_query,
        &BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String("nullable-boundary".to_owned()),
        )]),
        &mut nullable_values,
    );
    let nullable_slots = disambiguate_policy_claim_params_with_outer_slots(
        &mut nullable_query,
        &schema,
        &mut nullable_values,
        &outer_slots,
    )
    .expect("nullable policy claim validates");
    let nullable_alias = typed_claim_param_alias(&plain_name, &ColumnType::String.nullable());
    assert!(nullable_slots.contains_key(&nullable_alias));
    assert!(
        !nullable_slots.contains_key(&plain_name),
        "a non-nullable String outer slot must not be reused for a nullable String operand"
    );
}

#[test]
fn prepared_nested_policy_claim_routes_keep_outer_descriptor_slots() {
    // This intentionally exercises the compiler/prepare boundary rather
    // than a public transport: a JS invite subscription previously failed
    // while Groove prepared its shared binding descriptor, before any
    // observable query could run. Keeping the reproducer here makes the
    // descriptor contract cheap to validate without NAPI or a browser.
    let chat_member = |outer_column: &str| {
        public_outer_exists(
            "chatMembers",
            "chatId",
            outer_column,
            [public_claim_eq("userId", "user_id")],
        )
    };
    let schema =
        public_query_eval_schema(
            PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("chats")
                        .nullable_column("name", PublicColumnType::Text)
                        .column("isPublic", PublicColumnType::Boolean)
                        .column("createdBy", PublicColumnType::Text)
                        .nullable_column("joinCode", PublicColumnType::Text)
                        .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::Or(
                            vec![
                                PublicPolicyExpr::eq_literal(
                                    "isPublic",
                                    crate::tools::Value::Boolean(true),
                                ),
                                public_claim_eq("joinCode", "join_code"),
                                chat_member("id"),
                            ],
                        ))),
                )
                .table(
                    PublicTableSchemaBuilder::new("chatMembers")
                        .fk_column("chatId", "chats")
                        .column("userId", PublicColumnType::Text)
                        .nullable_column("joinCode", PublicColumnType::Text)
                        .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::Or(
                            vec![public_claim_eq("userId", "user_id"), chat_member("chatId")],
                        ))),
                )
                .table(
                    PublicTableSchemaBuilder::new("profiles")
                        .column("userId", PublicColumnType::Text)
                        .column("name", PublicColumnType::Text)
                        .nullable_column("avatar", PublicColumnType::Text)
                        .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
                )
                .table(
                    PublicTableSchemaBuilder::new("messages")
                        .fk_column("chatId", "chats")
                        .fk_column("senderId", "profiles")
                        .column("text", PublicColumnType::Text)
                        .column("createdAt", PublicColumnType::Timestamp)
                        .policies(PublicTablePolicies::new().with_select(chat_member("chatId"))),
                ),
        );
    let identity = author(0xa9);
    let (_client_dir, mut client) =
        open_node_with_uuid(NodeUuid::from_bytes([0xa7; 16]), schema.clone());
    client.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("join_code"),
            Value::String("invite-123".to_owned()),
        )]),
    );
    let client_shape = Query::from("chats")
        .filter(eq(col("id"), param("id")))
        .validate_runtime(&schema)
        .unwrap();
    let client_binding = client_shape
        .bind(BTreeMap::from([(
            "id".to_owned(),
            Value::Uuid(row(0xaa).0),
        )]))
        .unwrap();
    let (shape, binding, _client_plan) = client
        .prepare_query_binding_for_link(
            &client_shape,
            &client_binding,
            DurabilityTier::Edge,
            identity,
        )
        .expect("prepare retained invite binding on the client before server coverage");
    register_query_shape(
        &mut client,
        &shape,
        RegisterShapeOptions {
            tier: DurabilityTier::Edge,
            ..RegisterShapeOptions::default()
        },
    );
    subscribe_query_binding(&mut client, &shape, &binding);

    let (_server_dir, mut node) =
        open_node_with_uuid(NodeUuid::from_bytes([0xa8; 16]), schema.clone());
    node.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("join_code"),
            Value::String("invite-123".to_owned()),
        )]),
    );
    let chat = row(0xaa);
    let chat_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
                ("name".to_owned(), Value::Nullable(None)),
                ("isPublic".to_owned(), Value::Bool(false)),
                (
                    "createdBy".to_owned(),
                    Value::String(identity.test_uuid().to_string()),
                ),
                (
                    "joinCode".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("invite-123".to_owned())))),
                ),
            ])),
        )
        .unwrap();
    node.apply_fate_update(
        chat_tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let profile = row(0xac);
    let profile_tx = node
        .commit_mergeable_settled(MergeableCommit::new("profiles", profile, 11).cells(
            BTreeMap::from([
                (
                    "userId".to_owned(),
                    Value::String(identity.test_uuid().to_string()),
                ),
                ("name".to_owned(), Value::String("Alice".to_owned())),
                ("avatar".to_owned(), Value::Nullable(None)),
            ]),
        ))
        .unwrap();
    node.apply_fate_update(
        profile_tx,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let message = row(0xad);
    let message_tx = node
        .commit_mergeable_settled(MergeableCommit::new("messages", message, 12).cells(
            BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("senderId".to_owned(), Value::Uuid(profile.0)),
                (
                    "text".to_owned(),
                    Value::String("invite-only seed".to_owned()),
                ),
                ("createdAt".to_owned(), Value::U64(1)),
            ]),
        ))
        .unwrap();
    node.apply_fate_update(
        message_tx,
        Fate::Accepted,
        Some(GlobalTime(3)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    // Mirror the wire receiver: the server reconstructs the client
    // binding from RegisterShape + Subscribe before it prepares the
    // maintained graph under the invite-authenticated identity.
    register_query_shape(
        &mut node,
        &shape,
        RegisterShapeOptions {
            tier: DurabilityTier::Edge,
            ..RegisterShapeOptions::default()
        },
    );
    subscribe_query_binding(&mut node, &shape, &binding);
    let registered_values = node
        .query
        .registered_bindings
        .get(&shape.shape_id())
        .and_then(|bindings| {
            bindings.get(&(
                binding.binding_id(),
                RegisterShapeOptions::default().read_view_key(),
            ))
        })
        .map(|registered| registered.values.clone())
        .expect("server reconstructed the subscribed invite binding");
    let server_binding = shape
        .bind(
            shape
                .params()
                .keys()
                .cloned()
                .zip(registered_values)
                .collect(),
        )
        .expect("registered wire values reconstruct the invite binding");
    let program = node
        .compile_current_query_program_for_read_view(
            &shape,
            &server_binding,
            DurabilityTier::Edge,
            identity,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
        )
        .expect("compile invite policy topology");
    let typed_join_code = typed_claim_param_alias(
        &claim_param_field(&ClaimPath(vec![
            "claims".to_owned(),
            "join_code".to_owned(),
        ])),
        &ColumnType::String.nullable(),
    );
    assert!(
        program
            .request
            .input
            .binding
            .values
            .contains_key(&typed_join_code),
        "the prepared invite binding must retain its nullable typed join-code slot"
    );
    let members = node.table("chatMembers").unwrap().clone();
    let members_policy = node
        .table_read_policy_authorization_request(
            shape.schema_version(),
            "chatMembers",
            identity,
            ParamBindingMode::RetainAllParams,
            DurabilityTier::Edge,
            program.request.input.binding.source_shape.clone(),
            program.request.input.binding.extra_user_params.clone(),
            program.request.input.binding.claim_params.clone(),
        )
        .expect("compile nested chat-members policy against the invite binding");
    node.policy_authorization_row_id_graph(members_policy.clone())
        .expect("prepare nested chat-members policy dependency");
    let prepared_policy_graphs = node.query_engine_read_metrics.policy_authorization_graphs;
    let members_authorized = node
        .compose_policy_filtered_current_source_graph(
            Ok(members_policy),
            node.maintained_view_content_current_with_version(&members, DurabilityTier::Edge)
                .expect("compile chat-members storage source"),
            &global_current_storage_fields(&members, true, true),
        )
        .expect("route nested chat-members policy through the invite binding");
    assert_eq!(
        node.query_engine_read_metrics.policy_authorization_graphs, prepared_policy_graphs,
        "source composition must only consume the prepared dependency"
    );
    assert!(
        members_authorized.route_fields.contains(&typed_join_code),
        "a nested policy source must carry the outer invite slot even when its own policy only consumes user_id"
    );
    let member_fields =
        crate::node::query_engine::graph_declared_output_fields(&members_authorized.graph)
            .expect("nested policy graph has a declared descriptor");
    assert!(
        member_fields.contains(&typed_join_code),
        "a membership CommitUnit must reach the live invite subscription with its outer claim route"
    );
    let app_program = node
        .compile_current_query_program_for_read_view(
            &shape,
            &server_binding,
            DurabilityTier::Edge,
            identity,
            CurrentQueryProgramOutput::AppRows,
            &ReadViewSpec::default(),
        )
        .expect("compile invite app-row topology");
    let system_program = node
        .compile_current_query_program_for_read_view(
            &shape,
            &server_binding,
            DurabilityTier::Edge,
            AuthorSubject::SYSTEM,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
        )
        .expect("System/asBackend reads must not require invite claim values");
    assert!(
        system_program.request.input.binding.claim_params.is_empty(),
        "System/asBackend prepared descriptors cannot retain session claim slots"
    );
    assert!(
        system_program
            .request
            .input
            .shape
            .nodes
            .keys()
            .all(|node| !node.0.starts_with("policy_branch:")),
        "System/asBackend reads must remove linked policy branches before normalization, not merely clear their claim bindings"
    );
    // The ordinary current-query path above strips System claim slots before
    // it constructs the binding. Exercise the nested authorization builders
    // too: a policy claim route must not leave an identity-scoped descriptor
    // behind when the served identity is System.
    let system_members_policy = node
        .table_read_policy_authorization_request(
            shape.schema_version(),
            "chatMembers",
            AuthorSubject::SYSTEM,
            ParamBindingMode::RetainAllParams,
            DurabilityTier::Edge,
            program.request.input.binding.source_shape.clone(),
            program.request.input.binding.extra_user_params.clone(),
            program.request.input.binding.claim_params.clone(),
        )
        .expect("System nested policy compilation must not retain session claim slots");
    assert!(
        system_members_policy.input.binding.claim_params.is_empty(),
        "System nested policy descriptors must not retain outer session claim slots"
    );
    assert_eq!(
        system_members_policy.input.binding.source_shape,
        query_binding_source_shape_for_parts_if_needed(
            &system_members_policy.input.binding.param_types,
            &BTreeMap::new(),
        ),
        "System nested policy descriptors must be keyed without session claim slots"
    );
    node.policy_authorization_row_id_graph(system_members_policy)
        .expect("System nested policy graph must bind against its claim-free descriptor");
    let system_members_policy_at = node
        .table_read_policy_authorization_request_at(
            shape.schema_version(),
            "chatMembers",
            AuthorSubject::SYSTEM,
            ParamBindingMode::RetainAllParams,
            GlobalTime(0),
            program.request.input.binding.source_shape.clone(),
            program.request.input.binding.extra_user_params.clone(),
            program.request.input.binding.claim_params.clone(),
        )
        .expect("System historical nested policy compilation must not retain session claim slots");
    assert!(
        system_members_policy_at
            .input
            .binding
            .claim_params
            .is_empty(),
        "System historical policy descriptors must not retain outer session claim slots"
    );
    assert!(
        binding_claim_params_for_shape(
            &system_members_policy_at.input.shape,
            &system_members_policy_at.input.binding.param_types,
        )
        .is_empty(),
        "System historical policy inputs must not retain policy claim operands"
    );
    for terminal in &program.lowered.terminals {
        let expected_routes = match &terminal.output {
            OutputTerminalSchema::Fact(fact) => output_routing_fields_for_query_eval(fact),
            OutputTerminalSchema::AppRows(_) => BTreeSet::new(),
        };
        let declared = crate::node::query_engine::graph_declared_output_fields(&terminal.graph)
            .expect("the invite terminal has a statically declared output descriptor");
        assert!(
            expected_routes.is_subset(&declared),
            "every advertised invite route must be produced by its terminal; expected {expected_routes:?}, declared {declared:?}"
        );
    }
    let mut descriptors_by_shape = BTreeMap::new();
    let mut projected_binding_fields = BTreeMap::new();
    for terminal in program
        .lowered
        .terminals
        .iter()
        .chain(app_program.lowered.terminals.iter())
    {
        collect_binding_source_descriptor_fields(&terminal.graph, &mut descriptors_by_shape);
        collect_binding_source_projected_fields(&terminal.graph, &mut projected_binding_fields);
    }
    assert!(
        projected_binding_fields
            .values()
            .flatten()
            .all(|fields| fields.contains(&typed_join_code)),
        "every nested policy binding projection must preserve the outer nullable invite slot; {projected_binding_fields:?}"
    );
    assert!(
        descriptors_by_shape
            .values()
            .all(|descriptors| descriptors.len() == 1),
        "every binding-source shape must retain one shared descriptor; {descriptors_by_shape:?}"
    );

    node.open_seeded_maintained_subscription_view(
        &shape,
        &server_binding,
        identity,
        DurabilityTier::Edge,
        &ReadViewSpec::default(),
    )
    .expect("nested policy claim routes must prepare and bind against the root binding descriptor");
    node.query_rows_with_prepared_plan_for_identity(
        &shape,
        &server_binding,
        DurabilityTier::Edge,
        None,
        identity,
    )
    .expect("one-shot nested policy claim routes must bind against the root descriptor");

    let mut edge = PeerState::edge_client(identity);
    let client_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let update = edge
        .rehydrate_query_for_subscription_with_opts(
            &mut node,
            client_subscription,
            &shape,
            &server_binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .expect("the serving maintained view must retain the invite claim route")
        .expect("the invite subscription has an initial update");
    client
        .apply_sync_message_settled(update)
        .expect("the client must materialize the invited chat update");

    // The browser failure occurred only after the invite subscription was
    // live and accepting membership was committed. This must wake the
    // maintained graph without dropping its outer invite claim route.
    let member_tx = node
        .commit_mergeable_settled(MergeableCommit::new("chatMembers", row(0xab), 11).cells(
            BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                (
                    "userId".to_owned(),
                    Value::String(identity.test_uuid().to_string()),
                ),
                (
                    "joinCode".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("invite-123".to_owned())))),
                ),
            ]),
        ))
        .unwrap();
    node.apply_fate_update(
        member_tx,
        Fate::Accepted,
        Some(GlobalTime(4)),
        Some(DurabilityTier::Global),
    )
    .expect("a live invite subscription must tolerate its membership CommitUnit");
    edge.query_update(&mut node, &shape, &server_binding)
        .expect(
            "flushing the live invite subscription after membership must preserve its claim route",
        );

    // The invite has now become ordinary membership. A later normal
    // session must materialize an already-existing private message through
    // its sender include and timestamp order, not merely discover chat
    // membership itself.
    node.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("user_id"),
            Value::String(identity.test_uuid().to_string()),
        )]),
    );
    let message_shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat_id")))
        .array_subquery(ArraySubquery::new("sender", "profiles", "id", "senderId"))
        .order_by("createdAt", OrderDirection::Asc)
        .validate_runtime(&schema)
        .expect("validate normal-member message query");
    let message_binding = message_shape
        .bind(BTreeMap::from([(
            "chat_id".to_owned(),
            Value::Uuid(chat.0),
        )]))
        .expect("bind normal-member message query");
    let message_rows = node
        .query_rows_with_prepared_plan_for_identity(
            &message_shape,
            &message_binding,
            DurabilityTier::Edge,
            None,
            identity,
        )
        .expect("materialize private seed message with sender include and timestamp order");
    assert_eq!(
        message_rows
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![message],
        "normal membership reads the seed message after invite acceptance"
    );
    node.open_seeded_maintained_subscription_view(
        &message_shape,
        &message_binding,
        identity,
        DurabilityTier::Edge,
        &ReadViewSpec::default(),
    )
    .expect("prepare and hydrate normal-member message include/order subscription");
    let (_normal_client_dir, mut normal_client) =
        open_node_with_uuid(NodeUuid::from_bytes([0xae; 16]), schema.clone());
    normal_client.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("user_id"),
            Value::String(identity.test_uuid().to_string()),
        )]),
    );
    let mut normal_membership_peer = PeerState::edge_client(identity);
    normal_client
        .apply_sync_message_settled(
            normal_membership_peer
                .current_rows_update(&mut node, "chatMembers")
                .expect("serve the accepted membership to the normal client"),
        )
        .expect("normal client applies its accepted membership before querying messages");
    let simple_message_shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chat_id")))
        .validate_runtime(&schema)
        .expect("validate normal-member message query without include");
    let simple_message_binding = simple_message_shape
        .bind(BTreeMap::from([(
            "chat_id".to_owned(),
            Value::Uuid(chat.0),
        )]))
        .expect("bind normal-member message query without include");
    register_query_shape(
        &mut normal_client,
        &simple_message_shape,
        RegisterShapeOptions {
            tier: DurabilityTier::Edge,
            ..RegisterShapeOptions::default()
        },
    );
    subscribe_query_binding(
        &mut normal_client,
        &simple_message_shape,
        &simple_message_binding,
    );
    let mut normal_simple_peer = PeerState::edge_client(identity);
    let normal_simple_subscription = SubscriptionKey {
        shape_id: simple_message_shape.shape_id(),
        binding_id: simple_message_binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    normal_client
        .apply_sync_message_settled(
            normal_simple_peer
                .rehydrate_query_for_subscription_with_opts(
                    &mut node,
                    normal_simple_subscription,
                    &simple_message_shape,
                    &simple_message_binding,
                    RegisterShapeOptions {
                        tier: DurabilityTier::Edge,
                        ..RegisterShapeOptions::default()
                    },
                )
                .expect("serve normal-member message snapshot without include")
                .expect("normal-member message snapshot without include is ready"),
        )
        .expect("client applies normal-member message snapshot without include");
    assert_eq!(
        normal_client
            .query_rows_for_client(
                &simple_message_shape,
                &simple_message_binding,
                DurabilityTier::Edge,
                identity,
            )
            .expect("client materializes the private seed message without include")
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![message],
        "the normal client must first materialize the private seed message without include"
    );
    register_query_shape(
        &mut normal_client,
        &message_shape,
        RegisterShapeOptions {
            tier: DurabilityTier::Edge,
            ..RegisterShapeOptions::default()
        },
    );
    subscribe_query_binding(&mut normal_client, &message_shape, &message_binding);
    let mut normal_peer = PeerState::edge_client(identity);
    let normal_subscription = SubscriptionKey {
        shape_id: message_shape.shape_id(),
        binding_id: message_binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let normal_update = normal_peer
        .rehydrate_query_for_subscription_with_opts(
            &mut node,
            normal_subscription,
            &message_shape,
            &message_binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .expect("serve normal-member message include/order snapshot")
        .expect("normal-member message include/order snapshot is ready");
    if let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        version_carriers, ..
    }) = &normal_update
    {
        let version_bundles = crate::protocol::expand_version_carriers(version_carriers)
            .expect("expand normal-member message include/order payloads");
        let (profile_bundle, profile_version) = version_bundles
            .iter()
            .find_map(|bundle| {
                bundle
                    .versions
                    .iter()
                    .find(|version| version.table() == "profiles" && version.row_uuid() == profile)
                    .map(|version| (bundle, version))
            })
            .expect("the relation snapshot ships the sender version");
        assert_eq!(
            profile_bundle.tx.tx_id, profile_tx,
            "the sender witness must retain the profile version identity rather than borrow the message anchor"
        );
        assert_eq!(
            profile_version
                .record()
                .borrowed()
                .get_idx(7)
                .expect("decode sender wire userId"),
            Value::Nullable(Some(Box::new(Value::String(
                identity.test_uuid().to_string()
            )))),
            "the relation sender version ships userId content"
        );
    } else {
        panic!("expected normal-member view update")
    }
    let missing = normal_client
        .missing_known_state_row_version_refs(&normal_update)
        .expect("inspect normal-member message include/order repair requirements");
    assert!(
        missing.is_empty(),
        "the server snapshot already carries every visible row-version payload; missing {missing:?}"
    );
    if !missing.is_empty() {
        let messages = normal_peer
            .handle_row_versions_fetch(
                &mut node,
                SyncMessage::FetchRowVersions {
                    requests: missing.clone(),
                    delegated_session: None,
                },
            )
            .expect("serve normal-member message include/order repair payloads");
        let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice() else {
            panic!("expected row-version repair payloads")
        };
        normal_client
            .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
            .expect("apply normal-member message include/order repair payloads");
    }
    normal_client
        .apply_sync_message_settled(normal_update)
        .expect("client applies normal-member message include/order snapshot");
    assert!(
        normal_client
            .current_rows("profiles", DurabilityTier::Local)
            .expect("inspect locally materialized sender rows")
            .iter()
            .any(|row| row.row_uuid() == profile),
        "the include snapshot must deliver the sender row before local query evaluation"
    );
    assert_eq!(
        normal_client
            .current_rows("profiles", DurabilityTier::Local)
            .expect("inspect the local sender table")
            .iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([profile]),
        "the sender table must not receive the message row through relation delivery"
    );
    assert_eq!(
        normal_client
            .current_rows("profiles", DurabilityTier::Local)
            .expect("inspect the local sender payload")
            .into_iter()
            .next()
            .and_then(|row| row.cell(normal_client.table("profiles").unwrap(), "userId")),
        Some(Value::String(identity.test_uuid().to_string())),
        "the delivered sender version retains its required userId"
    );
    let (local_shape, local_binding, local_plan) = normal_client
        .prepare_query_binding_for_link_in_authorization_mode(
            &message_shape,
            &message_binding,
            DurabilityTier::Edge,
            identity,
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("prepare the same client-local maintained relation subscription as the browser");
    let (_local_subscription, local_snapshot) = normal_client
        .open_maintained_view_subscription_in_authorization_mode(
            &local_shape,
            &local_binding,
            identity,
            DurabilityTier::Edge,
            &ReadViewSpec::default(),
            Some(local_plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("open the client-local maintained relation subscription");
    assert_eq!(
        local_snapshot.root_count, 1,
        "the maintained client-local relation subscription retains the seed message"
    );
    let local_one_shot = normal_client
        .query_relation_snapshot_for_client(
            &message_shape,
            &message_binding,
            DurabilityTier::Edge,
            identity,
            &ReadViewSpec::default(),
        )
        .expect("materialize the client-local relation snapshot API used by WASM");
    assert_eq!(
        local_one_shot.root_count, 1,
        "the client-local relation snapshot API retains the seed message"
    );
    assert_eq!(
        normal_client
            .query_rows_for_client(
                &message_shape,
                &message_binding,
                DurabilityTier::Edge,
                identity,
            )
            .expect("client materializes normal-member message include/order snapshot")
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![message],
        "the normal client must retain the seed message when the sender include is added"
    );
}

#[test]
fn missing_policy_relation_seed_claim_fails_closed_without_breaking_prepared_bindings() {
    // This reproduces the server-side shape of a SessionRef policy graph:
    // the outer query is prepared first, then the protected source builds
    // a nested authorization subplan whose reachable seed needs a custom
    // session claim. An absent claim is a denied proof, not malformed
    // stored state or an unavailable source.
    let schema = missing_session_seed_policy_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xc1; 16]), schema.clone());
    let reader = author(0xc2);
    let team = row(0xc3);
    let resource = row(0xc4);
    commit_global_cells(
        &mut node,
        "resources",
        resource,
        BTreeMap::from([("name".to_owned(), Value::String("secret".to_owned()))]),
        1,
        1,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(0xc5),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource.0)),
            ("team".to_owned(), Value::Uuid(team.0)),
        ]),
        2,
        2,
    );
    commit_global_cells(
        &mut node,
        "teamSeeds",
        row(0xc6),
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("user".to_owned(), Value::Uuid(reader.test_uuid())),
        ]),
        3,
        3,
    );

    let shape = Query::from("resources").validate_runtime(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let missing_rows = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
        .expect("an absent policy seed claim must compile to a denied proof")
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert!(
        missing_rows.is_empty(),
        "missing custom claim must deny access"
    );

    let ordinary_error = node
        .program_binding_for_shape_and_policy(
            &shape,
            &binding,
            None,
            BTreeMap::new(),
            BTreeMap::from([(
                claim_param_field(&ClaimPath(vec!["session_id".to_owned()])),
                ProgramClaimParam {
                    path: ClaimPath(vec!["session_id".to_owned()]),
                    ty: ColumnType::Uuid,
                },
            )]),
            &node.query_program_policy_context(reader),
        )
        .expect_err("ordinary prepared bindings must still reject missing claims");
    assert!(matches!(ordinary_error, Error::InvalidStoredValue(_)));

    node.set_test_provider_claims(
        reader,
        BTreeMap::from([(
            crate::query::provider_claim_key("session_id"),
            Value::Uuid(reader.test_uuid()),
        )]),
    );
    let allowed_rows = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Global, reader)
        .expect("bound policy seed claim must compile")
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(allowed_rows, BTreeSet::from([resource]));
}

#[test]
fn declared_id_point_read_prepares_claim_policy_bindings() {
    // This deliberately exercises the internal point-read helper because it
    // is the authorization-support path used by PermissionAdvice and repair,
    // rather than a public client query. A declared `id` forces that helper to
    // select by physical row UUID while the read policy requires a claim
    // binding. The owner and denied reader must see the policy result, not a
    // Groove binding-source execution error; changing ownership must reverse
    // those results.
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("id", PublicColumnType::Uuid)
                .column("owner", PublicColumnType::Uuid)
                .column("title", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new().with_select(public_claim_eq("owner", "user_id")),
                ),
        ),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xd1; 16]), schema);
    let alice = author(0xd2);
    let bob = author(0xd3);
    let document = row(0xd4);

    node.set_test_provider_claims(
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("user_id"),
            Value::Uuid(alice.test_uuid()),
        )]),
    );
    node.set_test_provider_claims(
        bob,
        BTreeMap::from([(
            crate::query::provider_claim_key("user_id"),
            Value::Uuid(bob.test_uuid()),
        )]),
    );
    commit_global_cells(
        &mut node,
        "documents",
        document,
        BTreeMap::from([
            ("id".to_owned(), Value::Uuid(row(0xd5).0)),
            ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
            ("title".to_owned(), Value::String("private".to_owned())),
        ]),
        1,
        1,
    );
    let prepared_shape_baseline = node.runtime_stats_for_test().active_prepared_shapes;

    assert!(
        node.dry_run_read_current_allows("documents", document, alice)
            .expect("owner point read must bind policy claims")
    );
    assert_eq!(
        node.runtime_stats_for_test().active_prepared_shapes,
        prepared_shape_baseline,
        "the one-shot owner point read must retire its prepared graph"
    );
    assert!(
        !node
            .dry_run_read_current_allows("documents", document, bob)
            .expect("denied point read must be an empty result, not a binding error")
    );
    assert_eq!(
        node.runtime_stats_for_test().active_prepared_shapes,
        prepared_shape_baseline,
        "the one-shot denied point read must retire its prepared graph"
    );

    commit_global_cells(
        &mut node,
        "documents",
        document,
        BTreeMap::from([("owner".to_owned(), Value::Uuid(bob.test_uuid()))]),
        2,
        2,
    );

    assert!(
        !node
            .dry_run_read_current_allows("documents", document, alice)
            .expect("former owner must lose access after ownership changes")
    );
    assert_eq!(
        node.runtime_stats_for_test().active_prepared_shapes,
        prepared_shape_baseline,
        "ownership changes must not retain point-read graphs"
    );
    assert!(
        node.dry_run_read_current_allows("documents", document, bob)
            .expect("new owner must gain access after ownership changes")
    );
    assert_eq!(
        node.runtime_stats_for_test().active_prepared_shapes,
        prepared_shape_baseline,
        "repeated point reads must retain no prepared graph"
    );
}

#[test]
fn missing_policy_seed_claim_denies_authorization_support_rehydration() {
    // Terminal CommitUnit admission rehydrates a compiled read-policy
    // support subscription. This is distinct from the one-shot policy
    // read above: the support shape itself has no user parameter, while
    // its protected source carries the seed claim as a prepared route.
    let schema = missing_session_seed_policy_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xc7; 16]), schema.clone());
    let writer = author(0xc8);
    let team = row(0xc9);
    let resource = row(0xca);
    commit_global_cells(
        &mut node,
        "resources",
        resource,
        BTreeMap::from([("name".to_owned(), Value::String("secret".to_owned()))]),
        1,
        1,
    );
    commit_global_cells(
        &mut node,
        "resourceAccess",
        row(0xcb),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource.0)),
            ("team".to_owned(), Value::Uuid(team.0)),
        ]),
        2,
        2,
    );
    commit_global_cells(
        &mut node,
        "teamSeeds",
        row(0xcc),
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("user".to_owned(), Value::Uuid(writer.test_uuid())),
        ]),
        3,
        3,
    );

    let scope = node
        .authorization_support_scope(
            writer,
            &PermissionAdviceAction::Read {
                table: "resources".to_owned(),
                row: resource,
            },
        )
        .expect("missing policy claim is represented by a denied support shape");
    let options = scope.options.clone();
    let (shape, binding) = scope
        .subscriptions
        .into_iter()
        .next()
        .expect("read policy requires one support subscription");
    let mut peer = PeerState::client_link(writer);
    let ordinary_error = peer
        .rehydrate_query(&mut node, &shape, &binding)
        .expect_err("ordinary prepared subscription hydration must retain missing-claim errors");
    assert!(matches!(ordinary_error, Error::InvalidStoredValue(_)));

    let update = peer
        .rehydrate_authorization_support_query_for_identity(
            &mut node,
            writer,
            SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: options.read_view_key(),
            },
            &shape,
            &binding,
            options,
        )
        .expect("missing policy seed claim must hydrate as an empty authorization proof");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("authorization support must return a settled view update");
    };
    assert!(result_member_adds.is_empty());
    assert!(result_member_removes.is_empty());
}

#[test]
fn policy_claim_array_string_ids_bind_as_uuid_array() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("issues")
                    .column("title", PublicColumnType::Text)
                    .column("state", PublicColumnType::Text)
                    .fk_column("assignee", "users")
                    .column("priority", PublicColumnType::Timestamp)
                    .policies(
                        PublicTablePolicies::new().with_select(PublicPolicyExpr::In {
                            column: "assignee".to_owned(),
                            session_path: vec!["claims".to_owned(), "team_ids".to_owned()],
                        }),
                    ),
            ),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([8; 16]), schema.clone());
    let alice = author(1);
    let bob = author(2);
    commit_issue(&mut node, 1, "open", alice);
    commit_issue(&mut node, 2, "open", bob);

    let reader = author(9);
    node.set_test_provider_claims(
        reader,
        BTreeMap::from([(
            crate::query::provider_claim_key("team_ids"),
            Value::Array(vec![Value::String(alice.test_uuid().to_string())]),
        )]),
    );
    let shape = Query::from("issues").validate_runtime(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let visible = node
        .query_rows_for_link(&shape, &binding, DurabilityTier::Local, reader)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(visible, BTreeSet::from([row(1)]));
}

#[test]
fn prepared_policy_plan_is_recompiled_after_same_identity_claim_revision_changes() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("issues")
                .column("title", PublicColumnType::Text)
                .column("state", PublicColumnType::Text)
                .column("assignee", PublicColumnType::Uuid)
                .column("priority", PublicColumnType::Timestamp)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_claim_eq("assignee", "selected_assignee")),
                ),
        ),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0x81; 16]), schema.clone());
    let alice = author(0x82);
    let bob = author(0x83);
    commit_issue(&mut node, 1, "open", alice);
    commit_issue(&mut node, 2, "open", bob);

    let identity = author(0x84);
    let shape = Query::from("issues").validate_runtime(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let visible_for = |node: &mut NodeState<RocksDbStorage>| {
        node.query_rows_for_link(&shape, &binding, DurabilityTier::Local, identity)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>()
    };

    node.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("selected_assignee"),
            Value::Uuid(alice.test_uuid()),
        )]),
    );
    assert_eq!(visible_for(&mut node), BTreeSet::from([row(1)]));

    node.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("selected_assignee"),
            Value::Uuid(bob.test_uuid()),
        )]),
    );
    assert_eq!(
        visible_for(&mut node),
        BTreeSet::from([row(2)]),
        "the same prepared shape and identity must not reuse the plan compiled for prior claims",
    );
}

#[test]
fn production_policy_union_labels_survive_reorder_and_unrelated_insertion() {
    fn branch(state: &str) -> crate::query::PolicyBranch {
        crate::query::PolicyBranch {
            filters: vec![eq(col("state"), lit(state))],
            joins: Vec::new(),
            reachable: Vec::new(),
            inherits: Vec::new(),
        }
    }
    fn labels(node: &NodeState<RocksDbStorage>, branches: &[&str]) -> BTreeSet<String> {
        let mut query = Query::from("issues");
        query.policy_branches = branches.iter().map(|state| branch(state)).collect();
        let shape = query.validate_runtime(&schema()).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();
        normalized
            .nodes
            .values()
            .find_map(|node| match node {
                RowSetExpr::Union { inputs } => Some(
                    inputs
                        .iter()
                        .map(|input| input.label.clone())
                        .collect::<BTreeSet<_>>(),
                ),
                _ => None,
            })
            .expect("policy alternatives normalize through Union")
    }

    let (_dir, node) = open_node();
    let original = labels(&node, &["open", "done"]);
    let reordered_with_insert = labels(&node, &["done", "blocked", "open"]);
    assert!(original.is_subset(&reordered_with_insert));
    assert_eq!(reordered_with_insert.len(), original.len() + 1);
    assert_ne!(labels(&node, &["open"]), labels(&node, &["changed"]));
}
