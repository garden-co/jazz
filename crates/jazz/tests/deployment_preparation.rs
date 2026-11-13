use std::collections::HashMap;

use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentError, DeploymentRequest, prepare_deployment,
};
use jazz::tools::{
    ColumnType, HistoryRetention, Lens, LensOp, LensTransform, PolicyExpr, Schema, SchemaBuilder,
    SchemaHash, TableName, TablePolicies, TableSchema, Value,
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
        allow_dropping_history: false,
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
fn retention_only_changes_deploy_without_a_migration_in_both_directions() {
    let full = schema(&[]);
    let mut latest = full.clone();
    latest.get_mut(&TableName::from("notes")).unwrap().history = HistoryRetention::Latest;
    assert_ne!(SchemaHash::compute(&full), SchemaHash::compute(&latest));

    for (from, to) in [(&full, &latest), (&latest, &full)] {
        let stored = DeploymentCatalogue {
            schemas: vec![from.clone()],
            admitted_schema_hashes: vec![SchemaHash::compute(from)],
            ..Default::default()
        };
        let prepared = prepare_deployment(
            &stored,
            DeploymentRequest {
                allow_dropping_history: true,
                ..request(to, std::slice::from_ref(to), vec![])
            },
        )
        .expect("a retention change needs no migration");
        assert_eq!(
            prepared.active_schema().tables()[0].history,
            to[&TableName::from("notes")].history
        );
        assert!(
            prepared
                .migrations()
                .iter()
                .all(|lens| lens.forward.ops.is_empty()),
            "schemas differing only in retention are joined by an empty lens"
        );
    }
}

/// Indexes change how a node finds rows, not the rows: adding or dropping a
/// composite index or narrowing the indexed columns deploys without a
/// migration, in both directions, as `migrations create` promises.
#[test]
fn index_only_changes_deploy_without_a_migration_in_both_directions() {
    let plain = schema(&["state", "priority"]);
    let mut composite = plain.clone();
    composite
        .get_mut(&TableName::from("notes"))
        .unwrap()
        .composite_indexes = vec![vec!["state".into(), "priority".into()]];
    let mut index_only = plain.clone();
    index_only
        .get_mut(&TableName::from("notes"))
        .unwrap()
        .indexed_columns = Some(vec!["state".into()]);
    for changed in [&composite, &index_only] {
        assert_ne!(SchemaHash::compute(&plain), SchemaHash::compute(changed));
        for (from, to) in [(&plain, changed), (changed, &plain)] {
            let stored = DeploymentCatalogue {
                schemas: vec![from.clone()],
                admitted_schema_hashes: vec![SchemaHash::compute(from)],
                ..Default::default()
            };
            let prepared =
                prepare_deployment(&stored, request(to, std::slice::from_ref(to), vec![]))
                    .expect("an index change needs no migration");
            assert!(
                prepared
                    .migrations()
                    .iter()
                    .all(|lens| lens.forward.ops.is_empty()),
                "schemas differing only in indexes are joined by an empty lens"
            );
        }
    }
}

/// Switching a table the store keeps in full to latest-only drops its past
/// versions for good, so the deploy is refused until it says so; a new
/// latest-only table, or switching back to full history, needs no flag.
#[test]
fn switching_a_kept_table_to_latest_only_needs_an_explicit_flag() {
    let full = schema(&[]);
    let mut latest = full.clone();
    latest.get_mut(&TableName::from("notes")).unwrap().history = HistoryRetention::Latest;
    let deployed = |schema: &Schema| DeploymentCatalogue {
        schemas: vec![schema.clone()],
        admitted_schema_hashes: vec![SchemaHash::compute(schema)],
        ..Default::default()
    };

    let error = prepare_deployment(
        &deployed(&full),
        request(&latest, std::slice::from_ref(&latest), vec![]),
    )
    .expect_err("an unconfirmed switch drops history");
    assert_eq!(
        error,
        DeploymentError::DropsHistory {
            target: SchemaHash::compute(&latest),
            tables: vec![TableName::from("notes")],
        }
    );
    assert!(
        error.to_string().contains(
            "switches table `notes` to latest-only history, which drops each row's past versions"
        ),
        "{error}"
    );
    assert!(
        error.to_string().contains("allowDroppingHistory: true"),
        "{error}"
    );

    prepare_deployment(
        &deployed(&full),
        DeploymentRequest {
            allow_dropping_history: true,
            ..request(&latest, std::slice::from_ref(&latest), vec![])
        },
    )
    .expect("a confirmed switch deploys");
    prepare_deployment(
        &DeploymentCatalogue::default(),
        request(&latest, std::slice::from_ref(&latest), vec![]),
    )
    .expect("a first deploy has no history to drop");
    prepare_deployment(
        &deployed(&latest),
        request(&full, std::slice::from_ref(&full), vec![]),
    )
    .expect("switching back to full history drops nothing");

    // Only the first version to make the table latest-only needs the
    // flag: once one is admitted, republishing it or rolling back and
    // forth does not.
    let published = |admitted: [&Schema; 2]| DeploymentCatalogue {
        schemas: vec![full.clone(), latest.clone()],
        admitted_schema_hashes: admitted.iter().map(|s| SchemaHash::compute(s)).collect(),
        ..Default::default()
    };
    prepare_deployment(&published([&full, &latest]), request(&latest, &[], vec![]))
        .expect("a latest-only version is already admitted");
    prepare_deployment(&published([&latest, &full]), request(&latest, &[], vec![]))
        .expect("returning to latest-only after a rollback is no first switch");
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
        ..Default::default()
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
        let DeploymentError::ConflictingPaths { first, second, .. } = error else {
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
    let error = result.unwrap_err();
    assert!(matches!(error, DeploymentError::ConflictingPaths { .. }));
    // The refusal names the column and how the paths differ, and the
    // versions as their snapshots are named.
    let message = error.to_string();
    for part in [
        "for `notes.a`, one path adds it with default",
        r#""default-a""#,
        r#""different""#,
    ] {
        assert!(message.contains(part), "{message}");
    }
    assert!(
        message.contains(&SchemaHash::compute(&base).to_hex()[..12]),
        "{message}"
    );
    assert!(
        !message.contains(&SchemaHash::compute(&base).to_hex()),
        "{message}"
    );
}

#[test]
fn allows_backward_activation_but_rejects_cycles_and_disconnected_stored_schemas() {
    let base = schema(&[]);
    let next = schema(&["a"]);
    let edge = migration(&base, &next, vec![add("a")]);
    let stored = DeploymentCatalogue {
        schemas: vec![base.clone(), next.clone()],
        migrations: vec![edge.clone()],
        ..Default::default()
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
        ..Default::default()
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

fn tasks(status: &[&str], labels: &[&str]) -> Schema {
    let enumeration = |variants: &[&str]| ColumnType::Enum {
        variants: variants.iter().map(|v| (*v).to_owned()).collect(),
    };
    SchemaBuilder::new()
        .table(
            TableSchema::builder("tasks")
                .column("title", ColumnType::Text)
                .column("status", enumeration(status))
                .column(
                    "labels",
                    ColumnType::Array {
                        element: Box::new(enumeration(labels)),
                    },
                ),
        )
        .build()
}

/// A migration with no steps may add enum variants to a kept column, also
/// an array of enums, declared anywhere in the list: the rows it already
/// holds pass unchanged. One that drops a variant (a removal, or a rename
/// without a value mapping) is refused, naming the variant.
///
/// ```
/// v1 status(todo, done)          ──empty lens──► v2 status(blocked, todo, done) ✓
///    labels(bug)[]                                  labels(bug, ui)[]
/// v1 status(todo, done)          ──empty lens──► status(todo, finished)        ✗ "done"
/// ```
#[test]
fn an_empty_migration_may_add_enum_variants_but_not_drop_them() {
    let v1 = tasks(&["todo", "done"], &["bug"]);
    for added in [
        tasks(&["blocked", "todo", "done"], &["bug"]),
        tasks(&["todo", "done"], &["bug", "ui"]),
        tasks(&["done", "blocked", "todo"], &["ui", "bug"]),
    ] {
        let prepared = prepare_deployment(
            &DeploymentCatalogue::default(),
            request(
                &added,
                &[v1.clone(), added.clone()],
                vec![migration(&v1, &added, vec![])],
            ),
        )
        .expect("adding enum variants needs no migration steps");
        assert_eq!(prepared.migrations().len(), 1);
    }

    for (dropped, variant) in [
        (tasks(&["todo"], &["bug"]), "\"done\""),
        (tasks(&["todo", "finished"], &["bug"]), "\"done\""),
        (tasks(&["todo", "done"], &["ui"]), "\"bug\""),
    ] {
        let refused = prepare_deployment(
            &DeploymentCatalogue::default(),
            request(
                &dropped,
                &[v1.clone(), dropped.clone()],
                vec![migration(&v1, &dropped, vec![])],
            ),
        )
        .unwrap_err()
        .to_string();
        assert!(
            refused.contains(&format!("no longer declares enum variant {variant}"))
                && refused.contains("renaming or removing them is not yet"),
            "{refused}"
        );
    }
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

mod client_bundle {
    use super::*;
    use jazz::tools::deployment::published::{
        ClientMigrations, PublishLensOp, PublishMigrationRequest, PublishTableLens,
        PublishedSchema, check_client_migrations,
    };

    fn published(schema: &Schema) -> PublishedSchema {
        PublishedSchema {
            hash: SchemaHash::compute(schema).to_hex(),
            schema: schema.clone(),
        }
    }

    fn adds(from: &Schema, to: &Schema, column: &str, default: &str) -> PublishMigrationRequest {
        PublishMigrationRequest {
            from_hash: SchemaHash::compute(from).to_hex(),
            to_hash: SchemaHash::compute(to).to_hex(),
            forward: vec![PublishTableLens {
                table: "notes".into(),
                added: false,
                removed: false,
                renamed_from: None,
                operations: vec![PublishLensOp::Introduce {
                    column: column.into(),
                    column_type: ColumnType::Text,
                    value: Value::Text(default.into()),
                }],
            }],
        }
    }

    fn check(
        target: &Schema,
        schemas: &[&Schema],
        migrations: Vec<PublishMigrationRequest>,
    ) -> Result<(), String> {
        let bundle = ClientMigrations {
            schemas: schemas.iter().map(|schema| published(schema)).collect(),
            migrations,
        };
        check_client_migrations(
            &serde_json::to_vec(&bundle).unwrap(),
            &SchemaHash::compute(target).to_hex(),
        )
    }

    /// Validate checks what a deploy checks of the migrations alone: each
    /// lens, the paths between two versions, and the bundle's own hashes.
    /// Whether the history joins into one is left to the deploy.
    #[test]
    fn checks_the_migrations_as_a_deploy_does_apart_from_the_history_shape() {
        let (base, left, right, merged) = (
            schema(&[]),
            schema(&["a"]),
            schema(&["b"]),
            schema(&["a", "b"]),
        );
        let all = [&base, &left, &right, &merged];
        let diamond = |default: &str| {
            vec![
                adds(&base, &left, "a", "x"),
                adds(&base, &right, "b", "y"),
                adds(&left, &merged, "b", "y"),
                adds(&right, &merged, "a", default),
            ]
        };
        check(&merged, &all, diamond("x")).expect("both paths agree");
        let refused = check(&merged, &all, diamond("other")).unwrap_err();
        for part in [
            "for `notes.a`, one path adds it with default",
            r#""x""#,
            r#""other""#,
        ] {
            assert!(refused.contains(part), "{refused}");
        }

        // A snapshot no migration reaches is no refusal here: the store may
        // already run it.
        let unrelated = schema(&["z"]);
        check(
            &left,
            &[&base, &left, &unrelated],
            vec![adds(&base, &left, "a", "x")],
        )
        .expect("history shape is the deploy's");

        // Two different migrations between the same versions are refused.
        let refused = check(
            &left,
            &[&base, &left],
            vec![adds(&base, &left, "a", "x"), adds(&base, &left, "a", "y")],
        )
        .unwrap_err();
        assert!(refused.contains("conflicting migrations"), "{refused}");

        // A schema that isn't the one its hash names is refused.
        let mut bundle = ClientMigrations {
            schemas: vec![published(&base), published(&left)],
            migrations: vec![adds(&base, &left, "a", "x")],
        };
        bundle.schemas[1].schema = right.clone();
        let refused = check_client_migrations(
            &serde_json::to_vec(&bundle).unwrap(),
            &SchemaHash::compute(&left).to_hex(),
        )
        .unwrap_err();
        assert!(refused.contains("hash"), "{refused}");
    }
}
