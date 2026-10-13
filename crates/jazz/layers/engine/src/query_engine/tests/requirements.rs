//! Source-field closure and compilation requirement contracts.

use super::*;

#[test]
fn closure_requirements_merge_sparse_root_and_every_alias_hop_key() {
    let root = source("roots", SourceRole::Root);
    let project = source("projects", SourceRole::Alias("include:0:0".to_owned()));
    let org = source("orgs", SourceRole::Alias("include:0:1".to_owned()));
    let backup = source("projects", SourceRole::Alias("include:1:0".to_owned()));
    let backup_org = source("orgs", SourceRole::Alias("include:1:1".to_owned()));
    let member = source("profiles", SourceRole::Alias("include:2:0".to_owned()));
    let owner = source("users", SourceRole::Alias("reference:owner".to_owned()));
    let mut input = row_set_input(0x2a);
    input.shape.result = ResultId::RealRow {
        table: "roots".to_owned(),
        row: ResultRowRef::Source(root.clone()),
    };
    input.shape.nodes = BTreeMap::from([(
        input.shape.root.clone(),
        RowSetExpr::Source {
            source: root.clone(),
            visibility: RowVisibility::Visible,
        },
    )]);
    input.shape.auxiliary_sources = BTreeSet::from([
        project.clone(),
        org.clone(),
        backup.clone(),
        backup_org.clone(),
        member.clone(),
        owner.clone(),
    ]);
    input.shape.closure_paths = vec![
        ClosurePath::ExplicitInclude {
            id: "include:0:project.org".to_owned(),
            segments: vec![
                ClosurePathSegment {
                    parent: root.clone(),
                    target: project.clone(),
                    source_field: "project".to_owned(),
                },
                ClosurePathSegment {
                    parent: project.clone(),
                    target: org.clone(),
                    source_field: "org".to_owned(),
                },
            ],
            root_gate: Some(ClosureRootGate::Inner),
        },
        ClosurePath::ExplicitInclude {
            id: "include:1:backup.org".to_owned(),
            segments: vec![
                ClosurePathSegment {
                    parent: root.clone(),
                    target: backup.clone(),
                    source_field: "backup".to_owned(),
                },
                ClosurePathSegment {
                    parent: backup.clone(),
                    target: backup_org.clone(),
                    source_field: "org".to_owned(),
                },
            ],
            root_gate: Some(ClosureRootGate::Inner),
        },
        ClosurePath::ExplicitInclude {
            id: "include:2:members".to_owned(),
            segments: vec![ClosurePathSegment {
                parent: root.clone(),
                target: member.clone(),
                source_field: "members".to_owned(),
            }],
            root_gate: Some(ClosureRootGate::Required),
        },
        ClosurePath::ImplicitRootReference {
            id: "reference:owner".to_owned(),
            segment: ClosurePathSegment {
                parent: root.clone(),
                target: owner.clone(),
                source_field: "owner".to_owned(),
            },
        },
    ];
    let mut output = row_set_output(BTreeSet::new());
    output.app_rows.as_mut().expect("app rows").projection =
        PayloadProjection::Tree(AppProjectionTree {
            fields: FieldProjection::Fields(BTreeSet::from(["title".to_owned()])),
            paths: Vec::new(),
        });
    let request = QueryProgramRequest {
        authorization_mode: QueryAuthorizationMode::TrustedServing,
        reads: QueryReadSet::primary(ReadView {
            read_schema: schema(0x10),
            policy_schema: schema(0x11),
            sources: BTreeMap::from([
                (
                    root.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    project.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    org.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    backup.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    backup_org.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    member.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
                (
                    owner.clone(),
                    requested_current_source(DurabilityTier::Global),
                ),
            ]),
        }),
        policy: system_policy_context(),
        input,
        output,
    };

    let requirements = source_requirements_for_test(&request).expect("collect closure fields");

    let expected_root = BTreeSet::from([
        "title".to_owned(),
        "project".to_owned(),
        "backup".to_owned(),
        "members".to_owned(),
        "owner".to_owned(),
    ]);
    assert!(matches!(
        requirements.get(&root).map(|requirements| &requirements.app_fields),
        Some(FieldRequirement::Fields(fields)) if *fields == expected_root
    ));
    for source in [&project, &backup] {
        assert!(matches!(
            requirements
                .get(source)
                .map(|requirements| &requirements.app_fields),
            Some(FieldRequirement::Fields(fields)) if *fields == BTreeSet::from(["org".to_owned()])
        ));
    }
}

#[test]
fn read_frontier_facts_are_outputs_not_delivery_profiles() {
    let key = ProgramSharingKey {
        shape_id: shape(0x55),
        reads: QueryReadSet::primary(ResolvedReadKey {
            read_schema: schema(0x55),
            policy_schema: schema(0x55),
            sources: BTreeMap::from([(
                source("todos", SourceRole::Root),
                ResolvedSourceExpr::VisibleCurrent {
                    projection: resolved_projection(0x55),
                    data: DataSource::Current,
                    tier: DurabilityTier::Global,
                },
            )]),
        }),
        policy: PolicySharingKey::System,
    };
    let local_output = row_set_output(BTreeSet::from([ProgramFactKey::ResultMembership]));
    let covered_output = row_set_output(BTreeSet::from([
        ProgramFactKey::ResultMembership,
        ProgramFactKey::ReadFrontierSettled(program_frontier()),
    ]));
    let local_output_key = ProgramOutputKey {
        fingerprint: vec![0x01],
    };
    let covered_output_key = ProgramOutputKey {
        fingerprint: vec![0x02],
    };

    assert_eq!(key, key.clone());
    assert_ne!(local_output, covered_output);
    assert_ne!(local_output_key, covered_output_key);
}
