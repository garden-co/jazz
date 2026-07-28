// The interpreter and query-program authorization paths are internal details, so
// this matrix uses the test-only NodeState differential seam rather than trying
// to infer the two individual verdicts through the public client API.

#[derive(Clone, Copy)]
enum DifferentialExpectation {
    Agreement(bool),
    KnownDivergence {
        interpreter: bool,
        lowered: bool,
        correct: &'static str,
    },
}

fn assert_write_policy_differential_case(
    core: &mut NodeState<RocksDbStorage>,
    label: &str,
    operation: WritePolicyDifferentialOperation,
    table: &TableSchema,
    policy: &Query,
    row_uuid: RowUuid,
    candidate: Option<&BTreeMap<String, Value>>,
    old_row: Option<&CurrentRow>,
    identity: AuthorId,
    expectation: DifferentialExpectation,
) {
    let verdicts = core
        .evaluate_write_policy_differential_for_test(
            operation, table, policy, row_uuid, candidate, old_row, identity,
        )
        .unwrap_or_else(|error| panic!("{label}: differential evaluation failed: {error}"));
    match expectation {
        DifferentialExpectation::Agreement(expected) => {
            assert_eq!(
                verdicts.interpreter, expected,
                "{label}: interpreter verdict"
            );
            assert_eq!(verdicts.lowered, expected, "{label}: lowered verdict");
        }
        DifferentialExpectation::KnownDivergence {
            interpreter,
            lowered,
            correct,
        } => {
            assert_eq!(
                verdicts.interpreter, interpreter,
                "{label}: interpreter verdict"
            );
            assert_eq!(verdicts.lowered, lowered, "{label}: lowered verdict");
            assert_ne!(
                verdicts.interpreter, verdicts.lowered,
                "{label}: this case must continue to expose the live fork"
            );
            eprintln!(
                "write-policy differential disagreement [{label}]: interpreter={interpreter}, lowered={lowered}; correct={correct}"
            );
        }
    }
}

fn differential_child_cells(
    owner: AuthorId,
    parent: RowUuid,
    access: RowUuid,
    marker: &str,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("owner".to_owned(), Value::Uuid(owner.0)),
        ("parent_id".to_owned(), Value::Uuid(parent.0)),
        ("access_id".to_owned(), Value::Uuid(access.0)),
        ("marker".to_owned(), Value::String(marker.to_owned())),
    ])
}

#[test]
fn write_policy_interpreter_and_lowered_program_matrix() {
    let owner = user(0xa1);
    let other = user(0xb2);
    let editor = user(0xc3);
    let parent_write_policy = Query::from("parents").filter(eq(col("editor"), claim("sub")));
    let schema = JazzSchema::new([
        TableSchema::new(
            "grandparents",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_read_policy(Policy::owner_only("grandparents", "owner"))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "parents",
            [
                ColumnSchema::new("grandparent_id", ColumnType::Uuid),
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("editor", ColumnType::Uuid),
            ],
        )
        .with_reference("grandparent_id", "grandparents")
        .with_read_policy(Policy::shape(Query::from("parents").inherits("grandparent_id")))
        .with_write_policies(crate::schema::WritePolicies {
            insert_check: Some(parent_write_policy.clone()),
            update_using: Some(parent_write_policy.clone()),
            update_check: Some(parent_write_policy.clone()),
            delete_using: Some(parent_write_policy),
        }),
        TableSchema::new(
            "children",
            [
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("optional_owner", ColumnType::Uuid.nullable()),
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("access_id", ColumnType::Uuid),
                ColumnSchema::new("marker", ColumnType::String),
            ],
        )
        .with_reference("parent_id", "parents")
        .with_reference("access_id", "access")
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "access",
            [
                ColumnSchema::new("child_marker", ColumnType::String),
                ColumnSchema::new("member", ColumnType::Uuid),
            ],
        )
        .with_write_policy(Policy::public()),
    ]);
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
    core.commit_mergeable(
        MergeableCommit::new("grandparents", grandparent, 1)
            .made_by(AuthorId::SYSTEM)
            .cells(BTreeMap::from([("owner".to_owned(), Value::Uuid(owner.0))])),
    )
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("parents", parent, 2)
            .made_by(AuthorId::SYSTEM)
            .cells(BTreeMap::from([
                ("grandparent_id".to_owned(), Value::Uuid(grandparent.0)),
                ("owner".to_owned(), Value::Uuid(owner.0)),
                ("editor".to_owned(), Value::Uuid(editor.0)),
            ])),
    )
    .unwrap();
    core.commit_mergeable(
        MergeableCommit::new("access", access, 3)
            .made_by(AuthorId::SYSTEM)
            .cells(BTreeMap::from([
                ("child_marker".to_owned(), Value::String("open".to_owned())),
                ("member".to_owned(), Value::Uuid(owner.0)),
            ])),
    )
    .unwrap();
    let mut old_cells = differential_child_cells(owner, parent, access, "open");
    old_cells.insert(
        "optional_owner".to_owned(),
        Value::Nullable(Some(Box::new(Value::Uuid(owner.0)))),
    );
    core.commit_mergeable(
        MergeableCommit::new("children", old_child, 4)
            .made_by(AuthorId::SYSTEM)
            .cells(old_cells.clone()),
    )
    .unwrap();
    let old_row = core
        .current_rows("children", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .find(|row| row.row_uuid() == old_child)
        .unwrap();

    let allowed = differential_child_cells(owner, parent, access, "open");
    let denied = differential_child_cells(other, parent, access, "open");
    let missing_owner_candidate = BTreeMap::from([(
        "marker".to_owned(),
        Value::String("changed".to_owned()),
    )]);
    let owner_policy = Query::from("children").filter(eq(col("owner"), claim("sub")));
    for (label, operation, candidate, old_row, identity, expected) in [
        (
            "filter insert allowed",
            WritePolicyDifferentialOperation::Insert,
            Some(&allowed),
            None,
            owner,
            true,
        ),
        (
            "filter insert denied identity mismatch",
            WritePolicyDifferentialOperation::Insert,
            Some(&denied),
            None,
            owner,
            false,
        ),
        (
            "filter update check allowed",
            WritePolicyDifferentialOperation::UpdateCheck,
            Some(&allowed),
            None,
            owner,
            true,
        ),
        (
            "filter update check denied",
            WritePolicyDifferentialOperation::UpdateCheck,
            Some(&denied),
            None,
            owner,
            false,
        ),
        (
            "update check uses old row for a missing candidate column",
            WritePolicyDifferentialOperation::UpdateCheck,
            Some(&missing_owner_candidate),
            Some(&old_row),
            owner,
            true,
        ),
        (
            "filter update using allowed",
            WritePolicyDifferentialOperation::UpdateUsing,
            None,
            Some(&old_row),
            owner,
            true,
        ),
        (
            "filter update using denied identity mismatch",
            WritePolicyDifferentialOperation::UpdateUsing,
            None,
            Some(&old_row),
            other,
            false,
        ),
        (
            "filter delete allowed",
            WritePolicyDifferentialOperation::Delete,
            None,
            Some(&old_row),
            owner,
            true,
        ),
        (
            "filter delete denied identity mismatch",
            WritePolicyDifferentialOperation::Delete,
            None,
            Some(&old_row),
            other,
            false,
        ),
    ] {
        assert_write_policy_differential_case(
            &mut core,
            label,
            operation,
            &children,
            &owner_policy,
            old_child,
            candidate,
            old_row,
            identity,
            DifferentialExpectation::Agreement(expected),
        );
    }

    let optional_owner_policy =
        Query::from("children").filter(eq(col("optional_owner"), claim("sub")));
    let mut optional_owner = allowed.clone();
    optional_owner.insert(
        "optional_owner".to_owned(),
        Value::Nullable(Some(Box::new(Value::Uuid(owner.0)))),
    );
    let mut null_optional_owner = allowed.clone();
    null_optional_owner.insert("optional_owner".to_owned(), Value::Nullable(None));
    for (label, candidate, expected) in [
        ("nullable filter value allowed", &optional_owner, true),
        ("nullable filter explicit null denied", &null_optional_owner, false),
        ("nullable filter missing value denied", &allowed, false),
    ] {
        assert_write_policy_differential_case(
            &mut core,
            label,
            WritePolicyDifferentialOperation::Insert,
            &children,
            &optional_owner_policy,
            old_child,
            Some(candidate),
            None,
            owner,
            DifferentialExpectation::Agreement(expected),
        );
    }

    core.set_session_claims(
        owner,
        BTreeMap::from([("role".to_owned(), Value::String("writer".to_owned()))]),
    );
    core.set_session_claims(
        other,
        BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]),
    );
    let session_policy = Query::from("children").filter(eq(col("marker"), claim("role")));
    let session_candidate = differential_child_cells(owner, parent, access, "writer");
    for (label, identity, expected) in [
        ("session claim allowed", owner, true),
        ("session claim denied", other, false),
    ] {
        assert_write_policy_differential_case(
            &mut core,
            label,
            WritePolicyDifferentialOperation::Insert,
            &children,
            &session_policy,
            old_child,
            Some(&session_candidate),
            None,
            identity,
            DifferentialExpectation::Agreement(expected),
        );
    }

    let join_policy = Query::from("children").join_via_column(
        "access",
        "id",
        "access_id",
        [eq(col("member"), claim("sub"))],
    );
    for (label, identity, expected) in [
        ("join allowed", owner, true),
        ("join denied identity mismatch", other, false),
    ] {
        assert_write_policy_differential_case(
            &mut core,
            label,
            WritePolicyDifferentialOperation::Insert,
            &children,
            &join_policy,
            old_child,
            Some(&allowed),
            None,
            identity,
            DifferentialExpectation::Agreement(expected),
        );
    }

    let branch_policy = Query::from("children")
        .filter(crate::query::Predicate::Any(Vec::new()))
        .policy_branch(crate::query::PolicyBranch::single_alternative_from_query(
            Query::from("children").filter(eq(col("owner"), claim("sub"))),
        ));
    for (label, candidate, expected) in [
        ("policy branch allowed", &allowed, true),
        ("policy branch denied", &denied, false),
    ] {
        assert_write_policy_differential_case(
            &mut core,
            label,
            WritePolicyDifferentialOperation::Insert,
            &children,
            &branch_policy,
            old_child,
            Some(candidate),
            None,
            owner,
            DifferentialExpectation::Agreement(expected),
        );
    }

    let inherited_select = Query::from("children").inherits("parent_id");
    assert_write_policy_differential_case(
        &mut core,
        "inherited select chain more than one level deep",
        WritePolicyDifferentialOperation::UpdateCheck,
        &children,
        &inherited_select,
        old_child,
        Some(&allowed),
        None,
        owner,
        DifferentialExpectation::Agreement(true),
    );

    for (label, inherited_operation, operation, candidate, old_row) in [
        (
            "inherits insert uses parent insert policy",
            crate::query::InheritsOperation::Insert,
            WritePolicyDifferentialOperation::Insert,
            Some(&allowed),
            None,
        ),
        (
            "inherits update uses parent update policy",
            crate::query::InheritsOperation::Update,
            WritePolicyDifferentialOperation::UpdateUsing,
            None,
            Some(&old_row),
        ),
        (
            "inherits delete uses parent delete policy",
            crate::query::InheritsOperation::Delete,
            WritePolicyDifferentialOperation::Delete,
            None,
            Some(&old_row),
        ),
    ] {
        let policy = Query::from("children").inherits_operation("parent_id", inherited_operation);
        assert_write_policy_differential_case(
            &mut core,
            label,
            operation,
            &children,
            &policy,
            old_child,
            candidate,
            old_row,
            owner,
            DifferentialExpectation::KnownDivergence {
                interpreter: false,
                lowered: true,
                correct: "the interpreter: InheritsOperation explicitly selects the parent's matching write-policy clause, while lowering currently always applies the parent read policy",
            },
        );
    }
}

#[test]
fn write_policy_differential_keeps_v1_policy_pinned_after_table_rename() {
    let owner = user(0xe1);
    let v1 = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("owner", ColumnType::Uuid)],
    )
    .with_write_policy(Policy::owner_only("todos", "owner"))]);
    let v2 = JazzSchema::new([TableSchema::new(
        "tasks",
        [ColumnSchema::new("owner", ColumnType::Uuid)],
    )]);
    let v2_payload = SchemaVersion::new(v2.clone());
    let (_dir, mut core) = open_node_with_schema(node(0xe2), v1.clone());
    core.apply_sync_message(SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(v2_payload.clone()),
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::PublishLens {
        author: AuthorId::SYSTEM,
        lens: MigrationLens::new(
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
    })
    .unwrap();
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: v2_payload.id,
        },
    })
    .unwrap();

    let candidate = BTreeMap::from([("owner".to_owned(), Value::Uuid(owner.0))]);
    let v1_table = &v1.tables[0];
    let policy = v1_table.write_policies.insert_check.as_ref().unwrap();
    assert_write_policy_differential_case(
        &mut core,
        "v1 policy evaluated for a v2 candidate after table rename",
        WritePolicyDifferentialOperation::Insert,
        v1_table,
        policy,
        row(0xe3),
        Some(&candidate),
        None,
        owner,
        DifferentialExpectation::Agreement(true),
    );
    assert!(
        core.dry_run_insert_allows(
            MergeableCommit::new("tasks", row(0xe3), 1)
                .made_by(owner)
                .cells(candidate),
        )
        .unwrap(),
        "the actual v2 write must use that same pinned v1 policy"
    );
}
