use std::collections::HashMap;

use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentError, DeploymentRequest, prepare_deployment,
};
use jazz::tools::{
    ColumnType, Lens, LensOp, LensTransform, PolicyExpr, Schema, SchemaBuilder, SchemaHash,
    TableName, TablePolicies, TableSchema, Value,
};

fn schema(columns: &[&str]) -> Schema {
    let mut table = TableSchema::builder("notes").column("title", ColumnType::Text);
    for column in columns {
        table = table.column(*column, ColumnType::Text);
    }
    SchemaBuilder::new().table(table).build()
}

fn migration(from: &Schema, to: &Schema, ops: Vec<LensOp>) -> Lens {
    Lens::new(
        SchemaHash::compute(from),
        SchemaHash::compute(to),
        LensTransform::with_ops(ops),
    )
}

fn add(column: &str) -> LensOp {
    LensOp::AddColumn {
        table: "notes".into(),
        column: column.into(),
        column_type: ColumnType::Text,
        default: Value::Text(format!("default-{column}")),
    }
}

fn rename(from: &str, to: &str) -> LensOp {
    LensOp::RenameColumn {
        table: "notes".into(),
        old_name: from.into(),
        new_name: to.into(),
    }
}

fn request(target: &Schema, schemas: &[Schema], migrations: Vec<Lens>) -> DeploymentRequest {
    DeploymentRequest {
        target_schema_hash: SchemaHash::compute(target),
        schemas: schemas
            .iter()
            .map(|schema| (SchemaHash::compute(schema), schema.clone()))
            .collect(),
        migrations,
        permissions: HashMap::new(),
    }
}

#[test]
fn initial_and_permission_only_deployments_replace_embedded_permissions() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::True)),
        )
        .build();
    let prepared = prepare_deployment(
        &DeploymentCatalogue::default(),
        request(&schema, std::slice::from_ref(&schema), vec![]),
    )
    .unwrap();
    assert_eq!(
        prepared.active_schema().public_schema()[&TableName::from("notes")].policies,
        TablePolicies::default()
    );
    assert_eq!(prepared.schemas().len(), 1);
    assert!(prepared.migrations().is_empty());
    let stored = DeploymentCatalogue {
        schemas: vec![schema.clone()],
        active_schema_hash: Some(SchemaHash::compute(&schema)),
        ..Default::default()
    };
    let mut update = request(&schema, &[], vec![]);
    let grants = TablePolicies::new().with_insert(PolicyExpr::True);
    update.permissions.insert("notes".into(), grants.clone());
    let prepared = prepare_deployment(&stored, update).unwrap();
    assert_eq!(
        prepared.active_schema().public_schema()[&TableName::from("notes")].policies,
        grants
    );
    assert_eq!(
        stored.schemas[0], schema,
        "preparation must not mutate the stored catalogue"
    );
}

#[test]
fn equivalent_diamond_combines_stored_and_submitted_history_in_any_order() {
    let base = schema(&[]);
    let left = schema(&["a"]);
    let right = schema(&["b"]);
    let merged = schema(&["a", "b"]);
    let first = migration(&base, &left, vec![add("a")]);
    let stored = DeploymentCatalogue {
        schemas: vec![base.clone(), left.clone()],
        migrations: vec![first.clone()],
        active_schema_hash: Some(SchemaHash::compute(&left)),
    };
    let mut update = request(
        &merged,
        &[merged.clone(), right.clone()],
        vec![
            migration(&right, &merged, vec![add("a")]),
            first,
            migration(&left, &merged, vec![add("b")]),
            migration(&base, &right, vec![add("b")]),
        ],
    );
    let expected = prepare_deployment(
        &stored,
        request(
            &merged,
            &update
                .schemas
                .iter()
                .map(|(_, schema)| schema.clone())
                .collect::<Vec<_>>(),
            update.migrations.clone(),
        ),
    )
    .unwrap();
    update.schemas.reverse();
    update.migrations.reverse();
    let actual = prepare_deployment(&stored, update).unwrap();
    let hashes = |schemas: &[Schema]| schemas.iter().map(SchemaHash::compute).collect::<Vec<_>>();
    assert_eq!(hashes(expected.schemas()), hashes(actual.schemas()));
    assert_eq!(actual.schemas().len(), 4);
    assert_eq!(actual.migrations().len(), 4);
    assert_eq!(
        SchemaHash::compute(actual.schemas().last().unwrap()),
        SchemaHash::compute(&merged)
    );
    assert_eq!(actual.active_schema().public_schema(), &merged);
}

#[test]
fn sibling_path_cannot_deploy_without_forward_convergence_from_the_authored_branch() {
    // Same graph as merged_schema_via_sibling_path_defaults_reintroduced_column_without_erasing_source:
    // left -> base -> right -> merged is a read path, not an eligible deployment.
    let base = schema(&[]);
    let left = schema(&["a"]);
    let right = schema(&["b"]);
    let merged = schema(&["a", "b"]);
    let stored = DeploymentCatalogue {
        schemas: vec![base.clone(), left.clone()],
        migrations: vec![migration(&base, &left, vec![add("a")])],
        active_schema_hash: Some(SchemaHash::compute(&left)),
    };
    let error = prepare_deployment(
        &stored,
        request(
            &merged,
            &[right.clone(), merged.clone()],
            vec![
                migration(&base, &right, vec![add("b")]),
                migration(&right, &merged, vec![add("a")]),
            ],
        ),
    )
    .unwrap_err();
    assert_eq!(
        error,
        DeploymentError::UnreachableTarget {
            target: SchemaHash::compute(&merged),
            active: SchemaHash::compute(&left),
        }
    );
}

#[test]
fn parallel_paths_must_preserve_column_identity_and_defaults() {
    let base = schema(&["left", "right"]);
    let left = schema(&["x", "right"]);
    let right = schema(&["left", "y"]);
    let merged = schema(&["x", "y"]);
    let edges = |last| {
        vec![
            migration(&base, &left, vec![rename("left", "x")]),
            migration(&base, &right, vec![rename("right", "y")]),
            migration(&left, &merged, vec![rename("right", "y")]),
            migration(&right, &merged, last),
        ]
    };
    let schemas = [base.clone(), left.clone(), right.clone(), merged.clone()];
    prepare_deployment(
        &DeploymentCatalogue::default(),
        request(&merged, &schemas, edges(vec![rename("left", "x")])),
    )
    .unwrap();
    for conflicting in [
        vec![
            rename("left", "temporary"),
            rename("y", "x"),
            rename("temporary", "y"),
        ],
        vec![
            LensOp::RemoveColumn {
                table: "notes".into(),
                column: "left".into(),
                column_type: ColumnType::Text,
                default: Value::Text("default-left".into()),
            },
            add("x"),
        ],
    ] {
        let error = prepare_deployment(
            &DeploymentCatalogue::default(),
            request(&merged, &schemas, edges(conflicting)),
        )
        .unwrap_err();
        let DeploymentError::ConflictingPaths { first, second } = error else {
            panic!("{error}")
        };
        assert_eq!(first.first(), Some(&SchemaHash::compute(&base)));
        assert_eq!(first.last(), Some(&SchemaHash::compute(&merged)));
        assert_eq!(first.first(), second.first());
        assert_eq!(first.last(), second.last());
        assert_ne!(first, second);
    }
    let base = schema(&[]);
    let left = schema(&["a"]);
    let right = schema(&["b"]);
    let merged = schema(&["a", "b"]);
    let different_default = LensOp::AddColumn {
        table: "notes".into(),
        column: "a".into(),
        column_type: ColumnType::Text,
        default: Value::Text("different".into()),
    };
    let result = prepare_deployment(
        &DeploymentCatalogue::default(),
        request(
            &merged,
            &[base.clone(), left.clone(), right.clone(), merged.clone()],
            vec![
                migration(&base, &left, vec![add("a")]),
                migration(&base, &right, vec![add("b")]),
                migration(&left, &merged, vec![add("b")]),
                migration(&right, &merged, vec![different_default]),
            ],
        ),
    );
    assert!(matches!(
        result,
        Err(DeploymentError::ConflictingPaths { .. })
    ));
}

#[test]
fn allows_backward_activation_but_rejects_cycles_and_disconnected_stored_schemas() {
    let base = schema(&[]);
    let next = schema(&["a"]);
    let edge = migration(&base, &next, vec![add("a")]);
    let stored = DeploymentCatalogue {
        schemas: vec![base.clone(), next.clone()],
        migrations: vec![edge.clone()],
        active_schema_hash: Some(SchemaHash::compute(&next)),
    };
    prepare_deployment(&stored, request(&base, &[], vec![])).unwrap();
    let reverse = Lens::new(edge.target_hash, edge.source_hash, edge.backward.clone());
    assert!(matches!(
        prepare_deployment(&stored, request(&next, &[], vec![reverse])),
        Err(DeploymentError::Cycle { .. })
    ));
    let isolated = schema(&["isolated"]);
    assert!(matches!(
        prepare_deployment(&stored, request(&next, &[isolated], vec![])),
        Err(DeploymentError::DisconnectedGraph { .. })
    ));
    assert!(matches!(
        prepare_deployment(
            &stored,
            request(&next, &[], vec![migration(&next, &next, vec![])])
        ),
        Err(DeploymentError::Cycle { .. })
    ));
}

#[test]
fn rejects_missing_artifacts_bad_hashes_and_conflicting_migrations() {
    let base = schema(&[]);
    let next = schema(&["a"]);
    let edge = migration(&base, &next, vec![add("a")]);
    let stored = DeploymentCatalogue {
        schemas: vec![base.clone(), next.clone()],
        migrations: vec![edge.clone()],
        active_schema_hash: Some(SchemaHash::compute(&base)),
    };
    assert!(matches!(
        prepare_deployment(
            &DeploymentCatalogue::default(),
            request(&next, std::slice::from_ref(&next), vec![edge.clone()])
        ),
        Err(DeploymentError::MissingSchema { .. })
    ));
    let mut bad = request(&next, std::slice::from_ref(&next), vec![]);
    bad.schemas[0].0 = SchemaHash::compute(&base);
    assert!(matches!(
        prepare_deployment(&stored, bad),
        Err(DeploymentError::SchemaHashMismatch { .. })
    ));
    assert!(matches!(
        prepare_deployment(
            &stored,
            request(&next, &[], vec![migration(&base, &next, vec![])])
        ),
        Err(DeploymentError::ConflictingMigration { .. })
    ));
    assert!(matches!(
        prepare_deployment(&DeploymentCatalogue::default(), request(&next, &[], vec![])),
        Err(DeploymentError::MissingSchema { .. })
    ));
}

#[test]
fn rejects_invalid_lenses_and_policy_schema_pairs() {
    let base = schema(&[]);
    let next = schema(&["a"]);
    for ops in [
        vec![],
        vec![LensOp::AddColumn {
            table: "notes".into(),
            column: "a".into(),
            column_type: ColumnType::Text,
            default: Value::Integer(1),
        }],
        vec![LensOp::AddColumn {
            table: "notes".into(),
            column: "a".into(),
            column_type: ColumnType::Integer,
            default: Value::Integer(1),
        }],
    ] {
        assert!(matches!(
            prepare_deployment(
                &DeploymentCatalogue::default(),
                request(
                    &next,
                    &[base.clone(), next.clone()],
                    vec![migration(&base, &next, ops)]
                )
            ),
            Err(DeploymentError::InvalidMigration { .. })
        ));
    }
    let mut update = request(&base, std::slice::from_ref(&base), vec![]);
    update.permissions.insert(
        "missing".into(),
        TablePolicies::new().with_select(PolicyExpr::True),
    );
    assert!(matches!(
        prepare_deployment(&DeploymentCatalogue::default(), update),
        Err(DeploymentError::InvalidPermissions { .. })
    ));
    let mut update = request(&base, std::slice::from_ref(&base), vec![]);
    update.permissions.insert(
        "notes".into(),
        TablePolicies::new().with_select(jazz::tools::policy_expr::eq("missing", "x")),
    );
    assert!(matches!(
        prepare_deployment(&DeploymentCatalogue::default(), update),
        Err(DeploymentError::InvalidPermissions { .. })
    ));
    let invalid = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("title", ColumnType::Integer),
        )
        .build();
    assert!(matches!(
        prepare_deployment(
            &DeploymentCatalogue::default(),
            request(&invalid, std::slice::from_ref(&invalid), vec![])
        ),
        Err(DeploymentError::InvalidSchema { .. })
    ));
}

#[test]
fn parallel_paths_must_preserve_table_identity() {
    let schema = |names: &[&str]| {
        let mut builder = SchemaBuilder::new();
        for name in names {
            builder = builder.table(TableSchema::builder(name).column("title", ColumnType::Text));
        }
        builder.build()
    };
    let rename = |from: &str, to: &str| LensOp::RenameTable {
        old_name: from.into(),
        new_name: to.into(),
    };
    let base = schema(&["left", "right"]);
    let left = schema(&["x", "right"]);
    let right = schema(&["left", "y"]);
    let merged = schema(&["x", "y"]);
    let schemas = [base.clone(), left.clone(), right.clone(), merged.clone()];
    let edges = |last| {
        vec![
            migration(&base, &left, vec![rename("left", "x")]),
            migration(&base, &right, vec![rename("right", "y")]),
            migration(&left, &merged, vec![rename("right", "y")]),
            migration(&right, &merged, last),
        ]
    };
    prepare_deployment(
        &DeploymentCatalogue::default(),
        request(&merged, &schemas, edges(vec![rename("left", "x")])),
    )
    .unwrap();
    let error = prepare_deployment(
        &DeploymentCatalogue::default(),
        request(
            &merged,
            &schemas,
            edges(vec![
                LensOp::RemoveTable {
                    table: "left".into(),
                    schema: right[&TableName::from("left")].clone(),
                },
                LensOp::AddTable {
                    table: "x".into(),
                    schema: merged[&TableName::from("x")].clone(),
                },
            ]),
        ),
    )
    .unwrap_err();
    assert!(matches!(error, DeploymentError::ConflictingPaths { .. }));
    assert!(error.to_string().contains(" -> "));
}
