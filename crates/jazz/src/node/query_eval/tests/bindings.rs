//! bindings query-evaluation tests.

use super::*;

#[test]
fn prepared_integer_bindings_coerce_only_when_representable() {
    let cases = [
        (Value::I64(7), ColumnType::U8, Value::U8(7)),
        (
            Value::U32(u8::MAX as u32),
            ColumnType::U8,
            Value::U8(u8::MAX),
        ),
        (
            Value::U32(u16::MAX as u32),
            ColumnType::U16,
            Value::U16(u16::MAX),
        ),
        (
            Value::U64(u32::MAX as u64),
            ColumnType::U32,
            Value::U32(u32::MAX),
        ),
        (Value::I32(7), ColumnType::U64, Value::U64(7)),
        (
            Value::I64(i32::MIN as i64),
            ColumnType::I32,
            Value::I32(i32::MIN),
        ),
        (
            Value::U64(i64::MAX as u64),
            ColumnType::I64,
            Value::I64(i64::MAX),
        ),
    ];

    for (value, column_type, expected) in cases {
        assert_eq!(coerce_prepared_binding_value(value, &column_type), expected);
    }
}

#[test]
fn prepared_integer_bindings_do_not_wrap_out_of_range_values() {
    let cases = [
        (Value::I64(-1), ColumnType::U8),
        (Value::U16(u8::MAX as u16 + 1), ColumnType::U8),
        (Value::U32(u16::MAX as u32 + 1), ColumnType::U16),
        (Value::U64(u32::MAX as u64 + 1), ColumnType::U32),
        (Value::I64(-1), ColumnType::U64),
        (Value::I64(i32::MIN as i64 - 1), ColumnType::I32),
        (Value::U64(i64::MAX as u64 + 1), ColumnType::I64),
    ];

    for (value, column_type) in cases {
        assert_eq!(
            coerce_prepared_binding_value(value.clone(), &column_type),
            value,
            "unrepresentable values must fail closed rather than wrap"
        );
    }
}

#[test]
fn prepared_nullable_integer_bindings_normalize_exactly_once() {
    let nullable_u8 = ColumnType::Nullable(Box::new(ColumnType::U8));
    let some_i64 = Value::Nullable(Some(Box::new(Value::I64(7))));
    let none = Value::Nullable(None);

    let cases = [
        (
            Value::I64(7),
            ColumnType::U8,
            Value::U8(7),
            "nonnullable source to nonnullable target",
        ),
        (
            Value::I64(7),
            nullable_u8.clone(),
            Value::Nullable(Some(Box::new(Value::U8(7)))),
            "nonnullable source to nullable target",
        ),
        (
            some_i64.clone(),
            ColumnType::U8,
            Value::Nullable(Some(Box::new(Value::U8(7)))),
            "nullable source to nonnullable target",
        ),
        (
            some_i64,
            nullable_u8.clone(),
            Value::Nullable(Some(Box::new(Value::U8(7)))),
            "nullable source to nullable target must not double-wrap",
        ),
        (
            none.clone(),
            ColumnType::U8,
            none.clone(),
            "nullable None to nonnullable target",
        ),
        (
            none.clone(),
            nullable_u8.clone(),
            none,
            "nullable None to nullable target",
        ),
    ];

    for (value, column_type, expected, case) in cases {
        assert_eq!(
            coerce_prepared_binding_value(value, &column_type),
            expected,
            "{case}"
        );
    }

    let out_of_range = Value::Nullable(Some(Box::new(Value::I64(256))));
    for column_type in [ColumnType::U8, nullable_u8] {
        assert_eq!(
            coerce_prepared_binding_value(out_of_range.clone(), &column_type),
            out_of_range,
            "out-of-range nullable integers must not wrap or narrow"
        );
    }
}

#[test]
fn prepared_claim_descriptor_uses_validated_param_type_for_both_equality_orders() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("text_owners")
                    .column("owner", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("nullable_owners")
                    .nullable_column("owner", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("uuid_owners")
                    .column("owner", PublicColumnType::Uuid),
            ),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([0xb4; 16]), schema.clone());
    let claim_param = claim_param_field(&ClaimPath(vec!["user".to_owned()]));
    let cases = [
        (
            Query::from("text_owners").filter(eq(col("owner"), param(&claim_param))),
            ColumnType::String,
            Value::String("alice".to_owned()),
        ),
        (
            Query::from("nullable_owners").filter(eq(param(&claim_param), col("owner"))),
            ColumnType::String.nullable(),
            Value::Nullable(Some(Box::new(Value::String("alice".to_owned())))),
        ),
        (
            Query::from("uuid_owners").filter(eq(col("owner"), param(&claim_param))),
            ColumnType::Uuid,
            Value::Uuid(uuid::Uuid::from_bytes([0xb5; 16])),
        ),
    ];

    for (query, expected_type, value) in cases {
        let shape = query.validate(&schema).unwrap();
        let binding = shape
            .bind(BTreeMap::from([(claim_param.clone(), value)]))
            .unwrap();
        let normalized = node.normalized_row_set_shape(&shape, &binding).unwrap();
        let claims = binding_claim_params_for_shape(&normalized, shape.params());
        assert_eq!(
            claims.get(&claim_param).map(|claim| &claim.ty),
            Some(&expected_type),
            "prepared descriptor must retain the validator's paired-column type",
        );
    }
}

#[test]
fn binding_source_shape_is_descriptor_and_claim_path_identity() {
    let mut params = BTreeMap::new();
    params.insert("route".to_owned(), ColumnType::String);
    let claims = BTreeMap::from([(
        claim_param_field(&ClaimPath(vec!["sub".to_owned()])),
        ProgramClaimParam {
            path: ClaimPath(vec!["sub".to_owned()]),
            ty: ColumnType::Uuid,
        },
    )]);

    let first = query_binding_source_shape_for_parts(&params, &claims);
    let second = query_binding_source_shape_for_parts(&params, &claims);
    assert_eq!(first, second);
    assert!(!first.contains("jazz-query:"));

    let mut different_params = params.clone();
    different_params.insert("route".to_owned(), ColumnType::Uuid);
    assert_ne!(
        first,
        query_binding_source_shape_for_parts(&different_params, &claims)
    );

    let different_claims = BTreeMap::from([(
        claim_param_field(&ClaimPath(vec!["team".to_owned(), "id".to_owned()])),
        ProgramClaimParam {
            path: ClaimPath(vec!["team".to_owned(), "id".to_owned()]),
            ty: ColumnType::Uuid,
        },
    )]);
    assert_ne!(
        first,
        query_binding_source_shape_for_parts(&params, &different_claims)
    );
}

#[test]
fn nested_read_policies_reuse_an_outer_equivalent_claim_slot() {
    // A maintained outer query owns this prepared source. Its first
    // nested policy uses the legacy claim field name; a later protected
    // source validates the same claim as Text and must reuse that slot
    // rather than add a redundant typed alias under the already-active
    // source name.
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("public_profiles")
                    .column("name", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            )
            .table(
                PublicTableSchemaBuilder::new("private_chats")
                    .column("owner", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new().with_select(public_claim_eq("owner", "user_id")),
                    ),
            ),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xf4; 16]), schema.clone());
    let identity = author(0xf5);
    node.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("user_id"),
            Value::String(identity.test_uuid().to_string()),
        )]),
    );

    let claim_name = claim_param_field(&ClaimPath(vec!["claims".to_owned(), "user_id".to_owned()]));
    let outer_claims = BTreeMap::from([(
        claim_name.clone(),
        ProgramClaimParam {
            path: ClaimPath(vec!["claims".to_owned(), "user_id".to_owned()]),
            ty: ColumnType::String,
        },
    )]);
    let source_shape = query_binding_source_shape_for_parts(&BTreeMap::new(), &outer_claims);
    let binding = Query::from("public_profiles")
        .validate(&schema)
        .unwrap()
        .bind(BTreeMap::new())
        .unwrap();

    for table in ["public_profiles", "private_chats"] {
        let request = node
            .table_read_policy_authorization_request(
                node.catalogue.current_schema_version_id,
                table,
                identity,
                ParamBindingMode::RetainAllParams,
                DurabilityTier::Edge,
                Some(source_shape.clone()),
                BTreeMap::new(),
                outer_claims.clone(),
            )
            .unwrap();
        let program = node.compile_query_program_request(request).unwrap();
        node.subscribe_lowered_program(
            program,
            &binding,
            source_shape.clone(),
            PreparedClaimBindingMode::Strict,
        )
        .unwrap_or_else(|error| {
            panic!(
                "{table} must reuse the outer String claim slot instead of registering a divergent binding descriptor: {error:?}"
            )
        });
    }
}

#[test]
fn one_claim_path_has_distinct_prepared_slots_per_numeric_width() {
    let field = claim_param_field(&ClaimPath(vec!["access_level".to_owned()]));
    let i32_alias = typed_claim_param_alias(&field, &ColumnType::I32);
    let i64_alias = typed_claim_param_alias(&field, &ColumnType::I64);

    assert_ne!(i32_alias, i64_alias);
    assert_eq!(
        claim_path_from_param_field(&i32_alias),
        Some(ClaimPath(vec!["access_level".to_owned()]))
    );
    let slots = BTreeMap::from([
        (
            i32_alias,
            ProgramClaimParam {
                path: ClaimPath(vec!["access_level".to_owned()]),
                ty: ColumnType::I32,
            },
        ),
        (
            i64_alias,
            ProgramClaimParam {
                path: ClaimPath(vec!["access_level".to_owned()]),
                ty: ColumnType::I64,
            },
        ),
    ]);
    assert_eq!(slots.len(), 2);
    assert_eq!(
        slots
            .values()
            .map(|slot| slot.path.clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([ClaimPath(vec!["access_level".to_owned()])])
    );
}

#[test]
fn lowered_groove_graph_is_shared_for_distinct_identity_claims() {
    let schema = owner_policy_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xa1; 16]), schema.clone());
    let shape = Query::from("issues").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let alice_graph = lowered_current_app_rows_graph(
        &mut node,
        &shape,
        &binding,
        author(0xa1),
        &ReadViewSpec::default(),
    );
    let bob_graph = lowered_current_app_rows_graph(
        &mut node,
        &shape,
        &binding,
        author(0xb2),
        &ReadViewSpec::default(),
    );

    assert_eq!(
        alice_graph, bob_graph,
        "identity values belong in prepared bindings so hash-equal policy graphs share work"
    );
}

#[test]
fn lowered_groove_graph_differs_for_distinct_session_claim_values() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("issues")
                .column("title", PublicColumnType::Text)
                .column("requiresAdmin", PublicColumnType::Boolean),
        ),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xa2; 16]), schema.clone());
    let identity = author(0xa3);
    let shape = Query::from("issues")
        .filter(eq(
            col("requiresAdmin"),
            claim(crate::query::provider_claim_key("isAdmin")),
        ))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    node.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("isAdmin"),
            Value::Bool(true),
        )]),
    );
    let admin_graph = lowered_current_app_rows_graph(
        &mut node,
        &shape,
        &binding,
        identity,
        &ReadViewSpec::default(),
    );

    node.set_test_provider_claims(
        identity,
        BTreeMap::from([(
            crate::query::provider_claim_key("isAdmin"),
            Value::Bool(false),
        )]),
    );
    let non_admin_graph = lowered_current_app_rows_graph(
        &mut node,
        &shape,
        &binding,
        identity,
        &ReadViewSpec::default(),
    );

    assert_ne!(
        admin_graph, non_admin_graph,
        "session claim values must be encoded in the lowered Groove descriptor graph"
    );

    node.set_session_claims(
        identity,
        BTreeMap::from([("isAdmin".to_owned(), Value::Bool(true))]),
    );
    let legacy_flat_graph = lowered_current_app_rows_graph(
        &mut node,
        &shape,
        &binding,
        identity,
        &ReadViewSpec::default(),
    );
    assert_ne!(
        legacy_flat_graph, admin_graph,
        "an unprefixed provider claim must not satisfy a nested session.claims binding"
    );
}

#[test]
fn prepared_query_lowering_matches_expected_sets() {
    for seed in 0..12_u64 {
        let (_dir, mut prepared_node) = open_node();
        let alice = author(1);
        let bob = author(2);
        let user = if seed & 1 == 0 { alice } else { bob };
        let mut filtered_expected = BTreeSet::new();
        let mut joined_expected = BTreeSet::new();
        for idx in 0..36 {
            let mixed = seed.wrapping_add(idx as u64 * 17);
            let state = if mixed % 4 == 0 { "done" } else { "open" };
            let assignee = if mixed & 1 == 0 { alice } else { bob };
            commit_issue(&mut prepared_node, idx, state, assignee);
            if state == "open" && assignee == user {
                filtered_expected.insert(row(idx));
            }
            if mixed % 3 == 0 {
                let member_user = if mixed & 2 == 0 { alice } else { bob };
                commit_member(&mut prepared_node, idx, row(idx), member_user);
                if member_user == user {
                    joined_expected.insert(row(idx));
                }
            }
        }

        let shapes = [
            (
                Query::from("issues")
                    .filter(eq(col("state"), lit("open")))
                    .filter(eq(col("assignee"), param("user")))
                    .validate(&schema())
                    .unwrap(),
                filtered_expected,
            ),
            (
                Query::from("issues")
                    .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
                    .validate(&schema())
                    .unwrap(),
                joined_expected,
            ),
        ];
        for (shape, expected) in shapes {
            let binding = shape
                .bind(BTreeMap::from([(
                    "user".to_owned(),
                    Value::Uuid(user.test_uuid()),
                )]))
                .unwrap();
            let prepared = prepared_node
                .query_rows(&shape, &binding, DurabilityTier::Local)
                .unwrap()
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>();
            assert_eq!(prepared, expected, "seed {seed}");
        }
    }
}
