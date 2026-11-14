use super::*;
use crate::tools::deployment::{DeploymentCatalogue, prepare_deployment};
use crate::tools::public_schema::{EnumCaseDescriptor, SchemaBuilder};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/app-artifact");

fn uuid_fk(name: &str, table: &str) -> ColumnDescriptor {
    let mut column = ColumnDescriptor::new(name, ColumnType::Uuid);
    column.nullable = true;
    column.references = Some(TableName::from(table));
    column
}

fn nullable(name: &str, column_type: ColumnType) -> ColumnDescriptor {
    let mut column = ColumnDescriptor::new(name, column_type);
    column.nullable = true;
    column
}

fn table(columns: Vec<ColumnDescriptor>) -> TableSchema {
    let mut schema = TableSchema::builder("unused").build();
    schema.columns = RowDescriptor { columns };
    schema
}

fn schema(tables: Vec<(&str, TableSchema)>) -> Schema {
    let mut schema = SchemaBuilder::new().build();
    for (name, table) in tables {
        schema.insert(TableName::from(name), table);
    }
    schema
}

fn always() -> TablePolicies {
    TablePolicies {
        select: OperationPolicy::using(PolicyExpr::True),
        insert: OperationPolicy::with_check(PolicyExpr::True),
        update: OperationPolicy::using_and_check(PolicyExpr::True, PolicyExpr::True),
        delete: OperationPolicy::using(PolicyExpr::True),
    }
}

fn forward(table: &str, column: &str) -> CodegenRelation {
    CodegenRelation::Forward {
        table: table.into(),
        column: column.into(),
    }
}

fn reverse(table: &str, relation: &str) -> CodegenRelation {
    CodegenRelation::Reverse {
        table: table.into(),
        relation: relation.into(),
    }
}

/// `examples/todo-server-rs`: one version, open permissions.
fn todo_server() -> ArtifactInput {
    let current = schema(vec![
        (
            "projects",
            table(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
        ),
        (
            "todos",
            table(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("done", ColumnType::Boolean),
                nullable("description", ColumnType::Text),
                uuid_fk("parent", "todos"),
                uuid_fk("project", "projects"),
            ]),
        ),
    ]);
    let codegen = Codegen {
        tables: BTreeMap::from([
            (
                "projects".into(),
                CodegenTable {
                    relations: BTreeMap::from([(
                        "todosViaProject".into(),
                        reverse("todos", "projectRelation"),
                    )]),
                    ts_only: vec![],
                },
            ),
            (
                "todos".into(),
                CodegenTable {
                    relations: BTreeMap::from([
                        ("parentRelation".into(), forward("todos", "parent")),
                        ("todosViaParent".into(), reverse("todos", "parentRelation")),
                        ("projectRelation".into(), forward("projects", "project")),
                    ]),
                    ts_only: vec![],
                },
            ),
        ]),
        slices: BTreeMap::new(),
    };
    ArtifactInput {
        current: SchemaHash::compute(&current).to_string(),
        schemas: vec![current],
        migrations: vec![],
        permissions: Permissions::from([
            (
                TableName::from("projects"),
                TablePolicies {
                    select: OperationPolicy::using(PolicyExpr::True),
                    insert: OperationPolicy::with_check(PolicyExpr::True),
                    ..Default::default()
                },
            ),
            (TableName::from("todos"), always()),
        ]),
        codegen,
    }
}

/// Two versions with a migration, and most of what a schema, a policy and
/// codegen metadata can say.
fn shared_todos() -> ArtifactInput {
    let v1 = schema(vec![
        (
            "todos",
            table(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                nullable("owner", ColumnType::Uuid),
            ]),
        ),
        (
            "shares",
            table(vec![
                uuid_fk("todo", "todos"),
                ColumnDescriptor::new("user", ColumnType::Uuid),
            ]),
        ),
    ]);
    let mut v2 = schema(vec![
        (
            "todos",
            table(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                nullable("owner", ColumnType::Uuid),
                ColumnDescriptor::new(
                    "tags",
                    ColumnType::Array {
                        element: Box::new(ColumnType::Text),
                    },
                ),
                nullable(
                    "status",
                    ColumnType::EnumPayload {
                        cases: vec![
                            EnumCaseDescriptor {
                                name: "open".into(),
                                fields: vec![],
                            },
                            EnumCaseDescriptor {
                                name: "done".into(),
                                fields: vec![ColumnDescriptor::new("at", ColumnType::Timestamp)],
                            },
                        ],
                    },
                ),
                nullable(
                    "meta",
                    ColumnType::Json {
                        schema: Some(serde_json::json!({"type": "object"})),
                    },
                ),
            ]),
        ),
        (
            "shares",
            table(vec![
                uuid_fk("todo", "todos"),
                ColumnDescriptor::new("user", ColumnType::Uuid),
            ]),
        ),
    ]);
    v2.get_mut(&TableName::from("todos"))
        .unwrap()
        .indexed_columns = Some(vec!["owner".into()]);
    v2.get_mut(&TableName::from("shares")).unwrap().history = HistoryRetention::Latest;
    let (h1, h2) = (SchemaHash::compute(&v1), SchemaHash::compute(&v2));
    let migration: PublishMigrationRequest = serde_json::from_value(serde_json::json!({
        "fromHash": h1.to_string(),
        "toHash": h2.to_string(),
        "forward": [{
            "table": "todos",
            "operations": [
                {"type": "rename", "column": "title", "value": "name"},
                {"type": "introduce", "column": "tags",
                 "column_type": {"type": "Array", "element": {"type": "Text"}},
                 "value": {"type": "Array", "value": []}},
                {"type": "introduce", "column": "status",
                 "column_type": {"type": "EnumPayload", "cases": [
                     {"name": "open", "fields": []},
                     {"name": "done", "fields": [{"name": "at", "column_type": {"type": "Timestamp"}, "nullable": false}]}
                 ]},
                 "value": {"type": "Null"}},
                {"type": "introduce", "column": "meta",
                 "column_type": {"type": "Json", "schema": {"type": "object"}},
                 "value": {"type": "Null"}}
            ]
        }]
    }))
    .unwrap();
    let user = || vec!["user".to_string()];
    let shared_with_me = PolicyExpr::ExistsRel {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "shares".into(),
                alias: None,
            }),
            predicate: RelationPredicate::And(vec![
                RelationPredicate::Cmp {
                    left: RelationColumnRef::unscoped("todo"),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::RowId(RelationRowIdRef::Outer),
                },
                RelationPredicate::Cmp {
                    left: RelationColumnRef::unscoped("user"),
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::SessionRef(user()),
                },
            ]),
        },
    };
    let mine = PolicyExpr::Cmp {
        column: "owner".into(),
        op: CmpOp::Eq,
        value: PolicyValue::SessionRef(user()),
    };
    let permissions = Permissions::from([
        (
            TableName::from("todos"),
            TablePolicies {
                select: OperationPolicy::using(PolicyExpr::Or(vec![mine.clone(), shared_with_me])),
                insert: OperationPolicy::with_check(mine.clone()),
                update: OperationPolicy::using_and_check(mine.clone(), mine.clone()),
                delete: OperationPolicy::using(PolicyExpr::And(vec![
                    mine,
                    PolicyExpr::Not(Box::new(PolicyExpr::IsNull {
                        column: "owner".into(),
                    })),
                ])),
            },
        ),
        (
            TableName::from("shares"),
            TablePolicies {
                select: OperationPolicy::using(PolicyExpr::Inherits {
                    operation: Operation::Select,
                    via_column: "todo".into(),
                    max_depth: None,
                }),
                ..Default::default()
            },
        ),
    ]);
    let codegen = Codegen {
        tables: BTreeMap::from([
            (
                "todos".into(),
                CodegenTable {
                    relations: BTreeMap::from([("shares".into(), reverse("shares", "todo"))]),
                    ts_only: vec![
                        TsOnlyColumn::Computed {
                            column: "shareCount".into(),
                        },
                        TsOnlyColumn::Transform {
                            column: "meta".into(),
                        },
                    ],
                },
            ),
            (
                "shares".into(),
                CodegenTable {
                    relations: BTreeMap::from([("todo".into(), forward("todos", "todo"))]),
                    ts_only: vec![],
                },
            ),
        ]),
        slices: BTreeMap::from([("sharing".into(), vec!["shares".into()])]),
    };
    ArtifactInput {
        current: h2.to_string(),
        schemas: vec![v1, v2],
        migrations: vec![migration],
        permissions,
        codegen,
    }
}

fn fixture_path(name: &str) -> String {
    format!("{FIXTURES}/{name}.json")
}

/// The fixtures every language's codegen tests read. `JAZZ_UPDATE_FIXTURES=1`
/// rewrites them.
#[test]
fn the_writer_reproduces_each_fixture_byte_for_byte() {
    for (name, input) in [
        ("todo-server", todo_server()),
        ("shared-todos", shared_todos()),
    ] {
        let text = write(input).unwrap_or_else(|error| panic!("{name}: {error}"));
        let path = fixture_path(name);
        if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
            std::fs::create_dir_all(FIXTURES).unwrap();
            std::fs::write(&path, &text).unwrap();
        }
        let committed = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("{path} is missing; run with JAZZ_UPDATE_FIXTURES=1"));
        assert_eq!(text, committed, "{name} differs from its fixture");
    }
}

#[test]
fn a_written_file_loads_and_renders_back_to_itself() {
    for input in [todo_server(), shared_todos()] {
        let text = write(input).unwrap();
        let loaded = load(text.as_bytes()).unwrap();
        let reparsed: AppArtifact = serde_json::from_str(&text).unwrap();
        assert_eq!(render(&reparsed), text);
        assert_eq!(loaded.bundle().to_hex(), reparsed.bundle);
    }
}

#[test]
fn the_bundle_is_the_one_a_store_publishes_for_the_same_schema() {
    let input = todo_server();
    let mut target = input.schemas[0].clone();
    for (name, policies) in &input.permissions {
        target.get_mut(name).unwrap().policies = policies.clone();
    }
    let loaded = load(write(input).unwrap().as_bytes()).unwrap();
    let config = crate::db::DbConfig::new(
        JazzSchema::new(&target).unwrap(),
        crate::db::DbIdentity {
            node: crate::ids::NodeUuid::from_bytes([1; 16]),
            author: crate::ids::AuthorSubject::SYSTEM,
        },
    );
    let db = crate::db::block_on(crate::db::Db::open(config)).unwrap();
    let published = crate::db::block_on(db.publish_bundle(&target)).unwrap();
    assert_eq!(loaded.bundle(), published);
}

#[test]
fn policies_and_relation_expressions_survive_the_file_form() {
    let input = shared_todos();
    let permissions = input.permissions.clone();
    let loaded = load(write(input).unwrap().as_bytes()).unwrap();
    assert_eq!(loaded.permissions(), &permissions);
}

#[test]
fn a_loaded_file_feeds_clients_and_a_deployment() {
    let loaded = load(write(shared_todos()).unwrap().as_bytes()).unwrap();
    let migrations = loaded.client_migrations();
    assert_eq!(migrations.schemas.len(), 2);
    assert_eq!(migrations.lenses.len(), 1);
    let prepared = prepare_deployment(
        &DeploymentCatalogue::default(),
        loaded.deployment_request(false),
    )
    .unwrap();
    assert_eq!(
        SchemaHash::compute(&structural(
            prepared.active_schema().public_schema().clone()
        )),
        loaded.current_hash()
    );
}

#[test]
fn the_file_uses_one_tagging_convention() {
    let text = write(shared_todos()).unwrap();
    // Relation IR inside a policy is internally tagged, like everything else.
    assert!(text.contains("\"type\": \"ExistsRel\""));
    assert!(text.contains("\"type\": \"Filter\""));
    assert!(!text.contains("\"Filter\": {"));
    // snake_case fields, PascalCase tags.
    for camel in [
        "fromHash",
        "toHash",
        "renamedFrom",
        "branchBy",
        "columnType",
        "tsOnly",
    ] {
        assert!(!text.contains(camel), "{camel} in the file");
    }
    assert!(text.contains("\"type\": \"Rename\""));
    assert!(text.contains("\"history\": \"Latest\""));
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

fn edited(edit: impl FnOnce(&mut serde_json::Value)) -> Result<LoadedApp, ArtifactError> {
    let mut value: serde_json::Value =
        serde_json::from_str(&write(shared_todos()).unwrap()).unwrap();
    edit(&mut value);
    load(&serde_json::to_vec(&value).unwrap())
}

fn refused(edit: impl FnOnce(&mut serde_json::Value)) -> String {
    edited(edit)
        .expect_err("the edit must be refused")
        .message()
        .to_string()
}

#[test]
fn an_unknown_format_is_refused_before_anything_else() {
    let error = refused(|v| {
        v["format"] = 2.into();
        v["something_new"] = true.into();
    });
    assert!(error.contains("format 2 is not supported"), "{error}");
    assert!(
        refused(|v| v
            .as_object_mut()
            .unwrap()
            .remove("format")
            .map(drop)
            .unwrap())
        .contains("no `format`")
    );
}

#[test]
fn unknown_fields_are_refused_at_every_level() {
    for edit in [
        (|v: &mut serde_json::Value| v["extra"] = true.into()) as fn(&mut serde_json::Value),
        |v| v["schemas"][0]["tables"]["todos"]["policies"] = serde_json::json!({}),
        |v| v["schemas"][0]["tables"]["todos"]["columns"][0]["sparse"] = true.into(),
        |v| v["codegen"]["tables"]["todos"]["relations"]["shares"]["kind"] = "reverse".into(),
        |v| v["permissions"]["todos"]["select"]["using"]["exprs"][1]["rel"]["extra"] = 1.into(),
    ] {
        let error = refused(edit);
        assert!(error.contains("unknown field"), "{error}");
    }
}

#[test]
fn a_schema_must_match_its_hash() {
    let error = refused(|v| v["schemas"][0]["tables"]["todos"]["columns"][0]["name"] = "x".into());
    assert!(error.contains("schemas[0].hash is"), "{error}");
    assert!(error.contains("jazz-tools validate"), "{error}");
}

#[test]
fn current_and_migrations_must_name_shipped_schemas() {
    let missing = "0".repeat(64);
    let error = refused(|v| v["current"] = missing.clone().into());
    assert!(error.contains("is not among `schemas`"), "{error}");
    let error = refused(|v| v["migrations"][0]["from"] = missing.clone().into());
    assert!(error.contains("migrations[0].from names schema"), "{error}");
}

#[test]
fn a_lens_that_does_not_reproduce_its_target_is_refused() {
    let error = refused(|v| {
        let ops = v["migrations"][0]["forward"][0]["operations"]
            .as_array_mut()
            .unwrap();
        ops.retain(|op| op["type"] != "Rename");
    });
    assert!(error.starts_with("migrations:"), "{error}");
}

#[test]
fn the_bundle_must_match_the_current_permissions() {
    let error = refused(|v| {
        v["permissions"]["shares"]["select"]["using"] = serde_json::json!({"type": "False"})
    });
    assert!(error.contains("`bundle` is"), "{error}");
}

#[test]
fn permissions_must_compile_against_the_current_schema() {
    let error = refused(|v| v["permissions"]["gone"] = serde_json::json!({}));
    assert!(error.contains("permissions.gone"), "{error}");
    let error =
        refused(|v| v["permissions"]["todos"]["insert"]["with_check"]["column"] = "nope".into());
    assert!(error.starts_with("permissions:"), "{error}");
}

#[test]
fn codegen_must_name_what_the_current_schema_has() {
    let error = refused(|v| v["codegen"]["tables"]["nope"] = serde_json::json!({}));
    assert!(error.contains("codegen.tables.nope"), "{error}");
    let error = refused(|v| {
        v["codegen"]["tables"]["shares"]["relations"]["todo"]["column"] = "nope".into()
    });
    assert!(error.contains("has no column nope"), "{error}");
    let error = refused(|v| {
        v["codegen"]["tables"]["todos"]["relations"]["shares"]["relation"] = "nope".into()
    });
    assert!(error.contains("no forward relation nope"), "{error}");
    let error =
        refused(|v| v["codegen"]["tables"]["todos"]["ts_only"][1]["column"] = "nope".into());
    assert!(error.contains("ts_only[1]"), "{error}");
    let error = refused(|v| v["codegen"]["slices"]["sharing"] = serde_json::json!(["nope"]));
    assert!(error.contains("codegen.slices.sharing"), "{error}");
}

#[test]
fn the_writer_refuses_what_a_reader_would() {
    let mut input = todo_server();
    input.current = "0".repeat(64);
    assert!(
        write(input)
            .unwrap_err()
            .message()
            .contains("not among `schemas`")
    );
    let mut input = todo_server();
    input.schemas[0]
        .get_mut(&TableName::from("todos"))
        .unwrap()
        .columns
        .columns[0]
        .column_type = ColumnType::ScalarEnum {
        name: "e".into(),
        variants: vec!["a".into()],
    };
    assert!(
        write(input)
            .unwrap_err()
            .message()
            .contains("not supported in jazz.app.json yet")
    );
}

#[test]
fn embedded_schema_policies_are_dropped_as_a_deployment_drops_them() {
    let mut input = todo_server();
    let plain = write(todo_server()).unwrap();
    input.schemas[0]
        .get_mut(&TableName::from("todos"))
        .unwrap()
        .policies = TablePolicies {
        select: OperationPolicy::using(PolicyExpr::False),
        ..Default::default()
    };
    assert_eq!(write(input).unwrap(), plain);
}
