// The pre-removal differential matrix is retained as a lowered-program
// regression matrix. Clause-level evaluation is not separately observable
// through the public client API, so these cases use the same candidate-query
// primitive as production write admission.

#[derive(Clone, Copy)]
enum WritePolicyOperation {
    Insert,
    UpdateUsing,
    UpdateCheck,
    Delete,
}

fn assert_lowered_write_policy_case(
    core: &mut NodeState<RocksDbStorage>,
    label: &str,
    operation: WritePolicyOperation,
    table: &TableSchema,
    policy: &Query,
    row_uuid: RowUuid,
    candidate: Option<&BTreeMap<String, Value>>,
    old_row: Option<&CurrentRow>,
    identity: AuthorSubject,
    expected: bool,
) {
    // These fixture policies intentionally exercise an application-owned `sub`
    // claim.  It is no longer derived from authorship, so each simulated
    // session supplies the raw claim explicitly.
    if identity != AuthorSubject::SYSTEM {
        let mut claims = core.session_claims.get(&identity).cloned().unwrap_or_default();
        claims
            .entry("sub".to_owned())
            .or_insert_with(|| Value::Uuid(identity.test_uuid()));
        core.set_test_provider_claims(identity, claims);
    }
    let (cells, insert_candidate) = match operation {
        WritePolicyOperation::Insert => (
            candidate
                .unwrap_or_else(|| panic!("{label}: insert requires a candidate"))
                .clone(),
            true,
        ),
        WritePolicyOperation::UpdateCheck => {
            let mut effective_cells = old_row
                .into_iter()
                .flat_map(|row| {
                    table.columns.iter().filter_map(|column| {
                        row.cell(table, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                })
                .collect::<BTreeMap<_, _>>();
            effective_cells.extend(
                candidate
                    .unwrap_or_else(|| panic!("{label}: update check requires a candidate"))
                    .clone(),
            );
            (effective_cells, false)
        }
        WritePolicyOperation::UpdateUsing | WritePolicyOperation::Delete => {
            let row = old_row.unwrap_or_else(|| panic!("{label}: operation requires an old row"));
            (
                table
                    .columns
                    .iter()
                    .filter_map(|column| {
                        row.cell(table, &column.name)
                            .map(|value| (column.name.clone(), value))
                    })
                    .collect(),
                false,
            )
        }
    };
    let actual = core
        .write_policy_query_allows_candidate(
            table,
            policy,
            row_uuid,
            &cells,
            identity,
            insert_candidate,
        )
        .unwrap_or_else(|error| panic!("{label}: lowered evaluation failed: {error}"));
    assert_eq!(actual, expected, "{label}: lowered verdict");
}

fn write_policy_child_cells(
    owner: AuthorSubject,
    parent: RowUuid,
    access: RowUuid,
    marker: &str,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ("parent_id".to_owned(), Value::Uuid(parent.0)),
        ("access_id".to_owned(), Value::Uuid(access.0)),
        ("marker".to_owned(), Value::String(marker.to_owned())),
    ])
}

#[test]
fn lowered_write_policies_normalize_integer_widths_for_equality_in_and_contains() {
    let identity = user(0x71);
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("numbers")
            .column("number", PublicColumnType::Timestamp)
            .column(
                "allowed",
                PublicColumnType::Array {
                    element: Box::new(PublicColumnType::Integer),
                },
            )
            .column("floating", PublicColumnType::Double),
    ));
    let table = schema.tables[0].clone();
    let (_dir, mut core) = open_node_with_schema(node(0x72), schema);
    core.set_test_provider_claims(
        identity,
        BTreeMap::from([
            ("signed_seven".to_owned(), Value::I64(7)),
            ("large".to_owned(), Value::U64(i64::MAX as u64 + 1)),
        ]),
    );
    let candidate = BTreeMap::from([
        ("number".to_owned(), Value::U64(7)),
        ("allowed".to_owned(), Value::Array(vec![Value::I32(7)])),
        ("floating".to_owned(), Value::F64(7.0)),
    ]);

    for (label, policy, expected) in [
        (
            "equality matches I64 claim against U64 candidate",
            Query::from("numbers").filter(eq(col("number"), claim(crate::query::provider_claim_key("signed_seven")))),
            true,
        ),
        (
            "IN matches I64 claim against U64 candidate",
            Query::from("numbers").filter(crate::query::Predicate::In(
                col("number"),
                vec![claim(crate::query::provider_claim_key("signed_seven"))],
            )),
            true,
        ),
        (
            "contains matches I64 claim against I32 array member",
            Query::from("numbers").filter(crate::query::contains(
                col("allowed"),
                claim(crate::query::provider_claim_key("signed_seven")),
            )),
            true,
        ),
        (
            "float and integer remain type-exact",
            Query::from("numbers").filter(eq(col("floating"), claim(crate::query::provider_claim_key("signed_seven")))),
            false,
        ),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            WritePolicyOperation::Insert,
            &table,
            &policy,
            row(0x73),
            Some(&candidate),
            None,
            identity,
            expected,
        );
    }

    let large_candidate = BTreeMap::from([
        ("number".to_owned(), Value::U64(i64::MAX as u64 + 1)),
        ("allowed".to_owned(), Value::Array(Vec::new())),
        ("floating".to_owned(), Value::F64(0.0)),
    ]);
    assert_lowered_write_policy_case(
        &mut core,
        "U64 above i64::MAX remains exact",
        WritePolicyOperation::Insert,
        &table,
        &Query::from("numbers").filter(eq(col("number"), claim(crate::query::provider_claim_key("large")))),
        row(0x74),
        Some(&large_candidate),
        None,
        identity,
        true,
    );
}

#[test]
fn lowered_write_policy_operation_matrix() {
    let owner = user(0xa1);
    let other = user(0xb2);
    let editor = user(0xc3);
    let parent_write_policy = public_claim_eq("editor", "sub");
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("grandparents")
                    .column("owner", PublicColumnType::Uuid)
                    .policies(
                        public_all_policies().with_select(public_claim_eq("owner", "sub")),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .fk_column("grandparent_id", "grandparents")
                    .column("owner", PublicColumnType::Uuid)
                    .column("editor", PublicColumnType::Uuid)
                    .policies(
                        public_write_policies(parent_write_policy).with_select(
                            PublicPolicyExpr::inherits(
                                PublicOperation::Select,
                                "grandparent_id",
                            ),
                        ),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .column("owner", PublicColumnType::Uuid)
                    .nullable_column("optional_owner", PublicColumnType::Uuid)
                    .fk_column("parent_id", "parents")
                    .fk_column("access_id", "access")
                    .column("marker", PublicColumnType::Text)
                    .policies(public_write_policies(PublicPolicyExpr::True)),
            )
            .table(
                PublicTableSchemaBuilder::new("access")
                    .column("child_marker", PublicColumnType::Text)
                    .column("member", PublicColumnType::Uuid)
                    .policies(public_write_policies(PublicPolicyExpr::True)),
            ),
    );
    let children = schema
        .tables
        .iter()
        .find(|table| table.name == "children")
        .unwrap()
        .clone();
    let (_dir, mut core) = open_node_with_schema(node(0xd1), schema);
    let grandparent = row(0xd2);
    let parent = row(0xd3);
    let old_child = row(0xd4);
    let access = row(0xd5);
    core.commit_mergeable_settled(
        MergeableCommit::new("grandparents", grandparent, 1)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([("owner".to_owned(), Value::Uuid(owner.test_uuid()))])),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("parents", parent, 2)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([
                ("grandparent_id".to_owned(), Value::Uuid(grandparent.0)),
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ("editor".to_owned(), Value::Uuid(editor.test_uuid())),
            ])),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("access", access, 3)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([
                ("child_marker".to_owned(), Value::String("open".to_owned())),
                ("member".to_owned(), Value::Uuid(owner.test_uuid())),
            ])),
    )
    .unwrap();
    let mut old_cells = write_policy_child_cells(owner, parent, access, "open");
    old_cells.insert(
        "optional_owner".to_owned(),
        Value::Nullable(Some(Box::new(Value::Uuid(owner.test_uuid())))),
    );
    core.commit_mergeable_settled(
        MergeableCommit::new("children", old_child, 4)
            .made_by(AuthorSubject::SYSTEM)
            .cells(old_cells.clone()),
    )
    .unwrap();
    let old_row = core
        .current_rows("children", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .find(|row| row.row_uuid() == old_child)
        .unwrap();

    let allowed = write_policy_child_cells(owner, parent, access, "open");
    let denied = write_policy_child_cells(other, parent, access, "open");
    let missing_owner_candidate = BTreeMap::from([(
        "marker".to_owned(),
        Value::String("changed".to_owned()),
    )]);
    let owner_policy = Query::from("children").filter(eq(col("owner"), claim(crate::query::provider_claim_key("sub"))));
    for (label, operation, candidate, old_row, identity, expected) in [
        (
            "filter insert allowed",
            WritePolicyOperation::Insert,
            Some(&allowed),
            None,
            owner,
            true,
        ),
        (
            "filter insert denied identity mismatch",
            WritePolicyOperation::Insert,
            Some(&denied),
            None,
            owner,
            false,
        ),
        (
            "filter update check allowed",
            WritePolicyOperation::UpdateCheck,
            Some(&allowed),
            None,
            owner,
            true,
        ),
        (
            "filter update check denied",
            WritePolicyOperation::UpdateCheck,
            Some(&denied),
            None,
            owner,
            false,
        ),
        (
            "update check uses old row for a missing candidate column",
            WritePolicyOperation::UpdateCheck,
            Some(&missing_owner_candidate),
            Some(&old_row),
            owner,
            true,
        ),
        (
            "filter update using allowed",
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
            owner,
            true,
        ),
        (
            "filter update using denied identity mismatch",
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
            other,
            false,
        ),
        (
            "filter delete allowed",
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
            owner,
            true,
        ),
        (
            "filter delete denied identity mismatch",
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
            other,
            false,
        ),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            operation,
            &children,
            &owner_policy,
            old_child,
            candidate,
            old_row,
            identity,
            expected,
        );
    }

    let optional_owner_policy =
        Query::from("children").filter(eq(col("optional_owner"), claim(crate::query::provider_claim_key("sub"))));
    let mut optional_owner = allowed.clone();
    optional_owner.insert(
        "optional_owner".to_owned(),
        Value::Nullable(Some(Box::new(Value::Uuid(owner.test_uuid())))),
    );
    let mut null_optional_owner = allowed.clone();
    null_optional_owner.insert("optional_owner".to_owned(), Value::Nullable(None));
    for (label, candidate, expected) in [
        ("nullable filter value allowed", &optional_owner, true),
        ("nullable filter explicit null denied", &null_optional_owner, false),
        ("nullable filter missing value denied", &allowed, false),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            WritePolicyOperation::Insert,
            &children,
            &optional_owner_policy,
            old_child,
            Some(candidate),
            None,
            owner,
            expected,
        );
    }

    core.set_test_provider_claims(
        owner,
        BTreeMap::from([("role".to_owned(), Value::String("writer".to_owned()))]),
    );
    core.set_test_provider_claims(
        other,
        BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]),
    );
    let session_policy = Query::from("children").filter(eq(col("marker"), claim(crate::query::provider_claim_key("role"))));
    let session_candidate = write_policy_child_cells(owner, parent, access, "writer");
    for (label, identity, expected) in [
        ("session claim allowed", owner, true),
        ("session claim denied", other, false),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            WritePolicyOperation::Insert,
            &children,
            &session_policy,
            old_child,
            Some(&session_candidate),
            None,
            identity,
            expected,
        );
    }

    let join_policy = Query::from("children").join_via_column(
        "access",
        "id",
        "access_id",
        [eq(col("member"), claim(crate::query::provider_claim_key("sub")))],
    );
    for (label, identity, expected) in [
        ("join allowed", owner, true),
        ("join denied identity mismatch", other, false),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            WritePolicyOperation::Insert,
            &children,
            &join_policy,
            old_child,
            Some(&allowed),
            None,
            identity,
            expected,
        );
    }

    let branch_policy = Query::from("children")
        .filter(crate::query::Predicate::Any(Vec::new()))
        .policy_branch(crate::query::PolicyBranch::single_alternative_from_query(
            Query::from("children").filter(eq(col("owner"), claim(crate::query::provider_claim_key("sub")))),
        ));
    for (label, candidate, expected) in [
        ("policy branch allowed", &allowed, true),
        ("policy branch denied", &denied, false),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            WritePolicyOperation::Insert,
            &children,
            &branch_policy,
            old_child,
            Some(candidate),
            None,
            owner,
            expected,
        );
    }
    for (label, operation, candidate, old_row, identity, expected) in [
        (
            "policy branch update check allowed",
            WritePolicyOperation::UpdateCheck,
            Some(&allowed),
            None,
            owner,
            true,
        ),
        (
            "policy branch update check denied",
            WritePolicyOperation::UpdateCheck,
            Some(&denied),
            None,
            owner,
            false,
        ),
        (
            "policy branch update using allowed",
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
            owner,
            true,
        ),
        (
            "policy branch update using denied",
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
            other,
            false,
        ),
        (
            "policy branch delete allowed",
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
            owner,
            true,
        ),
        (
            "policy branch delete denied",
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
            other,
            false,
        ),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            operation,
            &children,
            &branch_policy,
            old_child,
            candidate,
            old_row,
            identity,
            expected,
        );
    }

    let inherited_select = Query::from("children").inherits("parent_id");
    for (label, operation, candidate, old_row, identity, expected) in [
        (
            "inherited select insert uses parent update-using policy",
            WritePolicyOperation::Insert,
            Some(&allowed),
            None,
            owner,
            false,
        ),
        (
            "inherited select insert allows the parent editor",
            WritePolicyOperation::Insert,
            Some(&allowed),
            None,
            editor,
            true,
        ),
        (
            "inherited select existing-row chain uses parent read policy",
            WritePolicyOperation::UpdateCheck,
            Some(&allowed),
            None,
            owner,
            true,
        ),
        (
            "inherited select existing-row chain denies a non-reader",
            WritePolicyOperation::UpdateCheck,
            Some(&allowed),
            None,
            editor,
            false,
        ),
    ] {
        assert_lowered_write_policy_case(
            &mut core,
            label,
            operation,
            &children,
            &inherited_select,
            old_child,
            candidate,
            old_row,
            identity,
            expected,
        );
    }

    for (
        label,
        inherited_operation,
        operation,
        candidate,
        old_row,
        identity,
        expected,
    ) in [
        (
            "inherits insert denies a parent reader who fails parent insert policy",
            crate::query::InheritsOperation::Insert,
            WritePolicyOperation::Insert,
            Some(&allowed),
            None,
            owner,
            false,
        ),
        (
            "inherits insert allows the parent writer",
            crate::query::InheritsOperation::Insert,
            WritePolicyOperation::Insert,
            Some(&allowed),
            None,
            editor,
            true,
        ),
        (
            "inherits update denies a parent reader who fails parent update policy",
            crate::query::InheritsOperation::Update,
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
            owner,
            false,
        ),
        (
            "inherits update allows the parent writer",
            crate::query::InheritsOperation::Update,
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
            editor,
            true,
        ),
        (
            "inherits delete denies a parent reader who fails parent delete policy",
            crate::query::InheritsOperation::Delete,
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
            owner,
            false,
        ),
        (
            "inherits delete allows the parent writer",
            crate::query::InheritsOperation::Delete,
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
            editor,
            true,
        ),
    ] {
        let policy = Query::from("children").inherits_operation("parent_id", inherited_operation);
        assert_lowered_write_policy_case(
            &mut core,
            label,
            operation,
            &children,
            &policy,
            old_child,
            candidate,
            old_row,
            identity,
            expected,
        );
        let branch_policy = Query::from("children")
            .filter(crate::query::Predicate::Any(Vec::new()))
            .policy_branch(crate::query::PolicyBranch::single_alternative_from_query(
                policy,
            ));
        assert_lowered_write_policy_case(
            &mut core,
            &format!("policy alternative: {label}"),
            operation,
            &children,
            &branch_policy,
            old_child,
            candidate,
            old_row,
            identity,
            expected,
        );
    }
}

#[test]
fn lowered_write_policy_covers_deep_inherited_write_chains() {
    let owner = user(0xf1);
    let other = user(0xf2);
    let grandparent_policy = public_claim_eq("owner", "sub");
    let parent_insert = PublicPolicyExpr::inherits(PublicOperation::Insert, "grandparent_id");
    let parent_update = PublicPolicyExpr::inherits(PublicOperation::Update, "grandparent_id");
    let parent_delete = PublicPolicyExpr::inherits(PublicOperation::Delete, "grandparent_id");
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("grandparents")
                    .column("owner", PublicColumnType::Uuid)
                    .policies(
                        public_write_policies(grandparent_policy)
                            .with_select(PublicPolicyExpr::False),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .fk_column("grandparent_id", "grandparents")
                    .policies(
                        PublicTablePolicies::new()
                            .with_insert(parent_insert)
                            .with_update(Some(parent_update.clone()), parent_update)
                            .with_delete(parent_delete),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("parent_id", "parents")
                    .policies(public_write_policies(PublicPolicyExpr::True)),
            ),
    );
    let children = schema
        .tables
        .iter()
        .find(|table| table.name == "children")
        .unwrap()
        .clone();
    let (_dir, mut core) = open_node_with_schema(node(0xf3), schema);
    let grandparent = row(0xf4);
    let parent = row(0xf5);
    let child = row(0xf6);
    core.commit_mergeable_settled(
        MergeableCommit::new("grandparents", grandparent, 1)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([(
                "owner".to_owned(),
                Value::Uuid(owner.test_uuid()),
            )])),
    )
    .unwrap();
    core.commit_mergeable_settled(
        MergeableCommit::new("parents", parent, 2)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([(
                "grandparent_id".to_owned(),
                Value::Uuid(grandparent.0),
            )])),
    )
    .unwrap();
    let cells = BTreeMap::from([("parent_id".to_owned(), Value::Uuid(parent.0))]);
    core.commit_mergeable_settled(
        MergeableCommit::new("children", child, 3)
            .made_by(AuthorSubject::SYSTEM)
            .cells(cells.clone()),
    )
    .unwrap();
    let old_row = core
        .current_rows("children", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .find(|row| row.row_uuid() == child)
        .unwrap();

    for (label, inherited_operation, operation, candidate, old_row) in [
        (
            "deep inherited insert",
            crate::query::InheritsOperation::Insert,
            WritePolicyOperation::Insert,
            Some(&cells),
            None,
        ),
        (
            "deep inherited update check",
            crate::query::InheritsOperation::Update,
            WritePolicyOperation::UpdateCheck,
            Some(&cells),
            None,
        ),
        (
            "deep inherited update using",
            crate::query::InheritsOperation::Update,
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
        ),
        (
            "deep inherited delete",
            crate::query::InheritsOperation::Delete,
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
        ),
    ] {
        let policy = Query::from("children")
            .inherits_operation("parent_id", inherited_operation);
        for (identity, expected) in [(owner, true), (other, false)] {
            assert_lowered_write_policy_case(
                &mut core,
                &format!("{label} for {identity:?}"),
                operation,
                &children,
                &policy,
                child,
                candidate,
                old_row,
                identity,
                expected,
            );
        }
    }

}
#[test]
fn lowered_write_policy_keeps_v1_policy_pinned_after_table_rename() {
    let owner = user(0xe1);
    let v1 = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("owner", PublicColumnType::Uuid)
            .policies(public_write_policies(public_claim_eq("owner", "sub"))),
    ));
    let v2 = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("tasks").column("owner", PublicColumnType::Uuid),
        ),
    );
    let v2_payload = SchemaVersion::new(v2.clone());
    let (_dir, mut core) = open_node_with_schema(node(0xe2), v1.clone());
    publish_schema_lineage(
        &mut core,
        v2_payload.clone(),
        MigrationLens::new(
            v1.version_id(),
            v2_payload.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![LensOp::RenameTable {
                    from: "todos".to_owned(),
                    to: "tasks".to_owned(),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: v2_payload.id,
        },
    })
    .unwrap();

    let candidate = BTreeMap::from([("owner".to_owned(), Value::Uuid(owner.test_uuid()))]);
    let v1_table = &v1.tables[0];
    let policy = v1_table.write_policies.insert_check.as_ref().unwrap();
    let old_row = current_row_from_cells(v1_table, row(0xe3), &candidate).unwrap();
    for (label, operation, candidate, old_row) in [
        (
            "v1 insert policy evaluated for a v2 candidate after table rename",
            WritePolicyOperation::Insert,
            Some(&candidate),
            None,
        ),
        (
            "v1 update-check policy evaluated for a v2 candidate after table rename",
            WritePolicyOperation::UpdateCheck,
            Some(&candidate),
            Some(&old_row),
        ),
        (
            "v1 update-using policy evaluated for a v2 old row after table rename",
            WritePolicyOperation::UpdateUsing,
            None,
            Some(&old_row),
        ),
        (
            "v1 delete policy evaluated for a v2 old row after table rename",
            WritePolicyOperation::Delete,
            None,
            Some(&old_row),
        ),
    ] {
        for (identity, expected) in [(owner, true), (user(0xe4), false)] {
            assert_lowered_write_policy_case(
                &mut core,
                &format!("{label} for {identity:?}"),
                operation,
                v1_table,
                policy,
                row(0xe3),
                candidate,
                old_row,
                identity,
                expected,
            );
        }
    }
    assert!(
        core.dry_run_insert_allows(
            MergeableCommit::new("tasks", row(0xe3), 1)
                .made_by(owner)
                .cells(candidate.clone()),
        )
        .unwrap(),
        "the actual v2 write must use that same pinned v1 policy"
    );
    assert!(
        !core
            .dry_run_insert_allows(
                MergeableCommit::new("tasks", row(0xe3), 1)
                    .made_by(user(0xe4))
                    .cells(candidate.clone()),
            )
            .unwrap(),
        "the actual v2 insert must deny an identity rejected by the pinned v1 policy"
    );

    let existing = row(0xe5);
    let existing_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", existing, 2)
                .made_by(AuthorSubject::SYSTEM)
                .cells(candidate.clone()),
        )
        .unwrap();
    let update = MergeableCommit::new("tasks", existing, 3)
        .cells(candidate.clone())
        .parents(vec![existing_tx]);
    assert!(
        core.advisory_mergeable_write_allows(update.clone().made_by(owner))
            .unwrap(),
        "the actual v2 update must use the pinned v1 update clauses"
    );
    assert!(
        !core
            .advisory_mergeable_write_allows(update.made_by(user(0xe4)))
            .unwrap(),
        "the actual v2 update must deny an identity rejected by the pinned v1 update clauses"
    );
    let delete = MergeableCommit::new("tasks", existing, 4)
        .cells(candidate)
        .parents(vec![existing_tx])
        .deletion(DeletionEvent::Deleted);
    assert!(
        core.advisory_mergeable_write_allows(delete.clone().made_by(owner))
            .unwrap(),
        "the actual v2 delete must use the pinned v1 delete clause"
    );
    assert!(
        !core
            .advisory_mergeable_write_allows(delete.made_by(user(0xe4)))
            .unwrap(),
        "the actual v2 delete must deny an identity rejected by the pinned v1 delete clause"
    );
}
