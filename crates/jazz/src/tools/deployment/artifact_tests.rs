use super::*;
use crate::query::{
    OrderDirection, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationOrderBy, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationUnionArm,
    RelationValueRef,
};
use crate::tools::deployment::{DeploymentCatalogue, prepare_deployment};
use crate::tools::public_schema::Value;
use crate::tools::public_schema::{
    CmpOp, ColumnDescriptor, ColumnType, EnumCaseDescriptor, HistoryRetention, Operation,
    OperationPolicy, PolicyExpr, PolicyValue, RowDescriptor, SchemaBuilder, TableName,
};
use std::collections::BTreeSet;

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
    // As TS compiles `policy.shares.exists.where({ todo: todo.id, user: session.user })`.
    let shared_with_me = PolicyExpr::Exists {
        table: "shares".into(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::Cmp {
                column: "todo".into(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), "id".into()]),
            },
            PolicyExpr::Cmp {
                column: "user".into(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(user()),
            },
        ])),
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

/// `file` of the fixture app `name`: each app's files sit in their own
/// directory, as they sit beside an app's schema.ts.
fn fixture_path(name: &str, file: &str) -> String {
    format!("{FIXTURES}/{name}/{file}")
}

/// `input`'s two files, loaded as a backend loads them.
fn loaded(input: ArtifactInput) -> LoadedApp {
    let written = write(input).unwrap();
    load(written.app.as_bytes(), written.permissions.as_bytes()).unwrap()
}

/// The fixtures every language's codegen tests read. `JAZZ_UPDATE_FIXTURES=1`
/// rewrites them.
#[test]
fn the_writer_reproduces_each_fixture_byte_for_byte() {
    for (name, input) in [
        ("todo-server", todo_server()),
        ("shared-todos", shared_todos()),
    ] {
        let written = write(input).unwrap_or_else(|error| panic!("{name}: {error}"));
        for (file, text) in [
            (APP_FILE, &written.app),
            (PERMISSIONS_FILE, &written.permissions),
        ] {
            let path = fixture_path(name, file);
            if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
                std::fs::create_dir_all(format!("{FIXTURES}/{name}")).unwrap();
                std::fs::write(&path, text).unwrap();
            }
            let committed = std::fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("{path} is missing; run with JAZZ_UPDATE_FIXTURES=1"));
            assert_eq!(*text, committed, "{name}/{file} differs from its fixture");
        }
    }
}

#[test]
fn written_files_read_back_to_the_engine_input() {
    for input in [todo_server(), shared_todos()] {
        let hashes: Vec<SchemaHash> = input
            .schemas
            .iter()
            .map(|s| SchemaHash::compute(&structural(s.clone())))
            .collect();
        let current = input.current.clone();
        let permissions = input.permissions.clone();
        let mut target = input
            .schemas
            .iter()
            .map(|s| structural(s.clone()))
            .find(|s| SchemaHash::compute(s).to_string() == current)
            .unwrap();
        let loaded = loaded(input);
        assert_eq!(loaded.current_hash().to_string(), current);
        assert_eq!(
            loaded.schemas().iter().map(|(h, _)| *h).collect::<Vec<_>>(),
            hashes
        );
        assert_eq!(loaded.permissions(), &permissions);
        for (name, policies) in &permissions {
            target.get_mut(name).unwrap().policies = policies.clone();
        }
        // The bundle is the one a store computes for the same permissions.
        let config = crate::db::DbConfig::new(
            JazzSchema::new(&target).unwrap(),
            crate::db::DbIdentity {
                node: crate::ids::NodeUuid::from_bytes([1; 16]),
                author: crate::ids::AuthorSubject::SYSTEM,
            },
        );
        let db = crate::db::block_on(crate::db::Db::open(config)).unwrap();
        assert_eq!(
            loaded.bundle(),
            crate::db::block_on(db.publish_bundle(&target)).unwrap()
        );
    }
}

#[test]
fn a_loaded_app_feeds_clients_and_a_deployment() {
    let loaded = loaded(shared_todos());
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
fn the_files_read_like_the_dsl() {
    let written = write(shared_todos()).unwrap();
    // No hashes, no bundle ids, no permissions in the clients' file.
    for absent in [
        "bundle",
        "\"hash\"",
        "permissions",
        "using",
        "with_check",
        "Select",
    ] {
        assert!(
            !written.app.contains(absent),
            "{absent} in jazz.app.json:\n{}",
            written.app
        );
    }
    assert!(!written.permissions.contains("bundle"));
    for present in [
        "\"owner\": \"uuid?\"",
        "\"tags\": { \"array\": \"string\" }",
        "\"todo\": { \"rel\": \"todos\", \"column\": \"todo\" }",
        "\"shares\": { \"reverse\": \"shares\", \"relation\": \"todo\" }",
        "\"history\": \"latest\"",
        "\"renameFrom\": \"title\"",
    ] {
        assert!(
            written.app.contains(present),
            "{present} not in:\n{}",
            written.app
        );
    }
    for present in [
        "\"anyOf\": [\"owner = session.user\", \"exists shares(todo = this.id, user = session.user)\"]",
        "\"read\": \"allowedTo.read(todo)\"",
        "\"delete\": { \"allOf\": [\"owner = session.user\", \"not owner = null\"] }",
    ] {
        assert!(
            written.permissions.contains(present),
            "{present} not in:\n{}",
            written.permissions
        );
    }
}

// ---------------------------------------------------------------------------
// The grammar
// ---------------------------------------------------------------------------

/// Compile `source` as `table`'s read policy, in `shared-todos`' current
/// schema, with `defs`.
fn read_policy(table: &str, source: &str, defs: serde_json::Value) -> Result<PolicyExpr, String> {
    let written = write(shared_todos()).unwrap();
    // `allowedTo` needs the table it inherits from to have the policy.
    let other = if table == "shares" { "todos" } else { "shares" };
    let file = serde_json::json!({
        "format": 1, "defs": defs,
        "tables": { table: { "read": source }, other: { "read": "true", "update": "true" } },
    });
    let loaded =
        load(written.app.as_bytes(), file.to_string().as_bytes()).map_err(|e| e.to_string())?;
    Ok(loaded.permissions()[&TableName::from(table)]
        .select
        .using
        .clone()
        .unwrap())
}

/// Print `expr` as `table`'s policy text in `shared-todos`.
fn printed(table: &str, expr: &PolicyExpr) -> String {
    let codegen = shared_todos().codegen;
    let relations = codegen.relations();
    let loaded = loaded(shared_todos());
    let defs = expr::Defs::new();
    let context = expr::Context {
        schema: loaded.current_schema(),
        relations: &relations,
        defs: &defs,
    };
    expr::print(expr, &context, table).unwrap()
}

#[test]
fn every_construct_reads_and_prints_back() {
    for (table, source) in [
        ("todos", "true"),
        ("todos", "false"),
        ("todos", "owner = session.user"),
        ("todos", "owner != null"),
        ("todos", "owner = null"),
        ("todos", "name != 'it''s'"),
        ("todos", "name in ['a', 'b']"),
        ("todos", "owner in session.claims.people"),
        ("todos", "tags contains 'urgent'"),
        ("todos", "session.claims.role = 'admin'"),
        ("todos", "session.claims.role in ['admin', 'editor']"),
        ("todos", "session.claims.team = null"),
        ("todos", "session.claims.labels contains 'x'"),
        ("todos", "$createdBy.account = session.user"),
        (
            "todos",
            "exists shares(todo = this.id, user = session.user)",
        ),
        ("todos", "exists shares()"),
        ("todos", "allowedTo.read(shares)"),
        ("shares", "allowedTo.read(todo)"),
        ("shares", "allowedTo.update(todo, maxDepth: 3)"),
        ("todos", "not owner = session.user"),
        ("todos", "owner = session.user or name = 'x'"),
        (
            "todos",
            "(owner = session.user or name = 'x') and tags contains 'y'",
        ),
        ("todos", "not (owner = null or name = 'x')"),
    ] {
        let compiled = read_policy(table, source, serde_json::json!({}))
            .unwrap_or_else(|e| panic!("{source}: {e}"));
        assert_eq!(printed(table, &compiled), source, "{source} printed as");
    }
}

#[test]
fn equivalent_spellings_compile_alike() {
    let same = |a: &str, b: &str| {
        assert_eq!(
            read_policy("todos", a, serde_json::json!({})).unwrap(),
            read_policy("todos", b, serde_json::json!({})).unwrap(),
            "{a} vs {b}"
        );
    };
    same("session.user = owner", "owner = session.user");
    same("this.owner = session.user", "owner = session.user");
    same("owner=session.user", "owner  =  session.user");
    same("((owner = session.user))", "owner = session.user");
}

#[test]
fn defs_expand_where_they_are_called() {
    let defs = serde_json::json!({
        "sharedWith($todo)": "exists shares(todo = $todo, user = session.user)",
        "mineOrShared($todo)": { "anyOf": ["owner = session.user", "sharedWith($todo)"] },
    });
    assert_eq!(
        read_policy("todos", "mineOrShared(this.id)", defs).unwrap(),
        read_policy(
            "todos",
            "owner = session.user or exists shares(todo = this.id, user = session.user)",
            serde_json::json!({})
        )
        .unwrap()
    );
}

#[test]
fn recursive_defs_are_refused() {
    let defs = serde_json::json!({
        "a($x)": "b($x)",
        "b($x)": "a($x) or owner = session.user",
    });
    let error = read_policy("todos", "a(this.id)", defs).unwrap_err();
    assert!(
        error.contains("definitions may not be recursive"),
        "{error}"
    );
    assert!(error.contains("a → b"), "{error}");
}

#[test]
fn expression_errors_point_at_the_spot_and_suggest() {
    let error = read_policy(
        "todos",
        "exists shares(todo = this.id, usr = session.user)",
        serde_json::json!({}),
    )
    .unwrap_err();
    assert!(
        error.starts_with("jazz.permissions.json: tables.todos.read:"),
        "{error}"
    );
    assert!(
        error.contains("unknown column `usr` in shares; did you mean `user`?"),
        "{error}"
    );
    assert!(
        error.contains("\n                                ^^^\n"),
        "{error}"
    );
    assert!(error.contains(expr::GRAMMAR_URL), "{error}");

    for (source, message) in [
        ("ownr = session.user", "did you mean `owner`?"),
        (
            "exists share(todo = this.id)",
            "unknown table `share`; did you mean `shares`?",
        ),
        (
            "allowedTo.read(share)",
            "unknown relation `share` on todos; did you mean `shares`?",
        ),
        ("allowedTo.raed(shares)", "did you mean `read`?"),
        ("owner == session.user", "comparisons use `=`, not `==`"),
        ("name = \"x\"", "strings use single quotes"),
        (
            "owner = session.usr",
            "a session value is `session.user` or `session.claims.<name>`",
        ),
        (
            "owner = session.user name = 'x'",
            "join conditions with `and` or `or`",
        ),
        ("unknownDef(this.id)", "unknown definition `unknownDef`"),
        ("owner < null", "only `=` and `!=` compare with null"),
        ("owner = 'not-a-uuid'", "is not a UUID"),
        (
            "owner = name",
            "two columns of the same row can't be compared",
        ),
        (
            "exists shares(todo = this.id).todoo(name = 'x')",
            "unknown relation `todoo` on shares; did you mean `todo`?",
        ),
        (
            "outer.owner = session.user",
            "`outer.` names the row enclosing an `exists`",
        ),
        ("name = int('x')", "`int(…)` takes a 32-bit integer"),
        ("exists (from shares where)", "expected a column"),
        ("", "an empty expression"),
    ] {
        let error = read_policy("todos", source, serde_json::json!({})).unwrap_err();
        assert!(error.contains(message), "{source}: {error}");
    }
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

fn edited_app(edit: impl FnOnce(&mut serde_json::Value)) -> String {
    let written = write(shared_todos()).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&written.app).unwrap();
    edit(&mut value);
    load(value.to_string().as_bytes(), written.permissions.as_bytes())
        .expect_err("the edit must be refused")
        .to_string()
}

fn edited_permissions(edit: impl FnOnce(&mut serde_json::Value)) -> String {
    let written = write(shared_todos()).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&written.permissions).unwrap();
    edit(&mut value);
    load(written.app.as_bytes(), value.to_string().as_bytes())
        .expect_err("the edit must be refused")
        .to_string()
}

#[test]
fn unknown_keys_are_refused_at_every_level_with_the_nearest() {
    let error = edited_app(|v| v["tabels"] = serde_json::json!({}));
    assert!(
        error.contains("unknown key `tabels`; did you mean `tables`?"),
        "{error}"
    );
    let error = edited_app(|v| v["tables"]["todos"]["colums"] = serde_json::json!({}));
    assert!(
        error.contains("tables.todos: unknown key `colums`; did you mean `columns`?"),
        "{error}"
    );
    let error = edited_app(|v| {
        v["tables"]["todos"]["columns"]["owner"] =
            serde_json::json!({"type": "uuid", "nullable": true})
    });
    assert!(error.contains("unknown key `nullable`"), "{error}");
    let error = edited_app(|v| v["tables"]["todos"]["columns"]["owner"] = serde_json::json!("uid"));
    assert!(
        error.contains("unknown type `uid`; did you mean `uuid`?"),
        "{error}"
    );
    let error = edited_permissions(|v| v["tables"]["todos"]["select"] = serde_json::json!("true"));
    assert!(error.contains("unknown operation `select`"), "{error}");
    let error = edited_permissions(|v| v["tables"]["todo"] = serde_json::json!({}));
    assert!(
        error.contains("jazz.app.json has no table `todo`; did you mean `todos`?"),
        "{error}"
    );
    // `$comment` is allowed everywhere.
    let written = write(shared_todos()).unwrap();
    let mut app: serde_json::Value = serde_json::from_str(&written.app).unwrap();
    app["tables"]["todos"]["$comment"] = "A to-do".into();
    let mut permissions: serde_json::Value = serde_json::from_str(&written.permissions).unwrap();
    permissions["tables"]["shares"]["read"] =
        serde_json::json!({"$comment": "Inherited", "where": "allowedTo.read(todo)"});
    load(
        app.to_string().as_bytes(),
        permissions.to_string().as_bytes(),
    )
    .unwrap();
}

#[test]
fn an_unknown_format_is_refused_before_anything_else() {
    let error = edited_app(|v| {
        v["format"] = 2.into();
        v["something_new"] = true.into();
    });
    assert!(error.contains("format 2 is not supported"), "{error}");
    let error = edited_permissions(|v| v["format"] = "1".into());
    assert!(error.contains("`format` must be the integer 1"), "{error}");
    let error = edited_app(|v| {
        v.as_object_mut().unwrap().remove("format");
    });
    assert!(error.contains("no `format`"), "{error}");
}

#[test]
fn migrations_must_name_versions_and_pass_their_lens_checks() {
    let error = edited_app(|v| v["migrations"][0]["from"] = "nope".into());
    assert!(error.contains("no version `nope` in `history`"), "{error}");
    let error = edited_app(|v| {
        v["migrations"][0]["migrate"]["todos"]
            .as_object_mut()
            .unwrap()
            .remove("name");
    });
    assert!(error.contains("migrations:"), "{error}");
}

#[test]
fn relations_must_name_real_columns() {
    let error =
        edited_app(|v| v["tables"]["shares"]["relations"]["todo"]["column"] = "nope".into());
    assert!(
        error.contains("names column `nope`, which shares doesn't have"),
        "{error}"
    );
}

#[test]
fn what_the_files_cant_say_is_refused_by_name() {
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
    input.current = SchemaHash::compute(&structural(input.schemas[0].clone())).to_string();
    let error = write(input).unwrap_err().to_string();
    assert!(
        error.contains("not supported in jazz.permissions.json yet")
            || error.contains("not supported"),
        "{error}"
    );
    assert!(error.contains("catalogue-native enum"), "{error}");
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

#[test]
fn malformed_files_are_refused_not_panicked_on() {
    let written = write(shared_todos()).unwrap();
    let (app, permissions) = (&written.app, &written.permissions);
    let mut deep = String::new();
    for _ in 0..10_000 {
        deep.push('[');
    }
    for input in [
        b"".as_slice(),
        b"not json",
        &app.as_bytes()[..app.len() / 2],
        b"[]",
        b"{\"format\": 1}",
        deep.as_bytes(),
    ] {
        assert!(load(input, permissions.as_bytes()).is_err());
        assert!(load(app.as_bytes(), input).is_err() || input == b"{\"format\": 1}");
        assert!(load_client(input).is_err());
    }
    let mut long = String::from("true");
    for _ in 0..2_000 {
        long = format!("not ({long})");
    }
    let _ = read_policy("todos", &long, serde_json::json!({}));
}

// ---------------------------------------------------------------------------
// The client reader
// ---------------------------------------------------------------------------

#[test]
fn a_file_both_readers_accept_gives_clients_the_same_migrations() {
    for input in [todo_server(), shared_todos()] {
        let written = write(input).unwrap();
        let full = load(written.app.as_bytes(), written.permissions.as_bytes())
            .unwrap()
            .client_migrations();
        let client = load_client(written.app.as_bytes()).unwrap();
        assert_eq!(client.schemas, full.schemas);
        assert_eq!(client.lenses, full.lenses);
    }
}

// ---------------------------------------------------------------------------
// Parity: every policy the engine holds round-trips through the file
// ---------------------------------------------------------------------------

fn every_policy_variant() -> PolicyExpr {
    let path = || vec!["claims".to_string(), "team".to_string()];
    let column = |name: &str| RelationColumnRef {
        scope: Some("t".into()),
        column: name.into(),
    };
    let scan = |table: &str| RelationExpr::TableScan {
        table: table.into(),
        alias: Some("t".into()),
    };
    let predicate = RelationPredicate::Or(vec![
        RelationPredicate::Cmp {
            left: column("a"),
            op: RelationCmpOp::Ge,
            right: RelationValueRef::Literal(serde_json::json!({"type": "Integer", "value": 3})),
        },
        RelationPredicate::IsNull {
            column: column("b"),
        },
        RelationPredicate::IsNotNull {
            column: column("c"),
        },
        RelationPredicate::In {
            left: column("d"),
            values: vec![
                RelationValueRef::Param("p".into()),
                RelationValueRef::SessionRef(path()),
                RelationValueRef::OuterColumn(column("e")),
                RelationValueRef::FrontierColumn(column("f")),
                RelationValueRef::RowId(RelationRowIdRef::Current),
                RelationValueRef::RowId(RelationRowIdRef::Outer),
                RelationValueRef::RowId(RelationRowIdRef::Frontier),
                RelationValueRef::Literal(serde_json::json!([1, {"a": "it's"}])),
                RelationValueRef::Literal(serde_json::json!(null)),
                RelationValueRef::Literal(serde_json::json!(1.5e300)),
                RelationValueRef::Literal(serde_json::json!(-7)),
                RelationValueRef::Literal(serde_json::json!({"type": "Null"})),
            ],
        },
        RelationPredicate::Contains {
            left: column("g"),
            right: RelationValueRef::Literal(serde_json::json!("bare")),
        },
        RelationPredicate::EnumMatch {
            column: column("h"),
            case: "done".into(),
            payload: Box::new(RelationPredicate::True),
        },
        RelationPredicate::And(vec![RelationPredicate::Not(Box::new(
            RelationPredicate::False,
        ))]),
    ]);
    let key = RelationKeyRef::Column(column("id"));
    let rel = RelationExpr::Limit {
        input: Box::new(RelationExpr::Offset {
            input: Box::new(RelationExpr::OrderBy {
                input: Box::new(RelationExpr::Distinct {
                    input: Box::new(RelationExpr::Project {
                        input: Box::new(RelationExpr::Join {
                            left: Box::new(RelationExpr::Filter {
                                input: Box::new(scan("a")),
                                predicate,
                            }),
                            right: Box::new(RelationExpr::Union {
                                inputs: vec![
                                    RelationUnionArm {
                                        label: "arm".into(),
                                        input: RelationExpr::Gather {
                                            seed: Box::new(scan("b")),
                                            step: Box::new(scan("c")),
                                            frontier_key: RelationKeyRef::RowId(
                                                RelationRowIdRef::Frontier,
                                            ),
                                            bound: RecursionBound::MaxDepth(4),
                                            dedupe_key: vec![key.clone()],
                                        },
                                    },
                                    RelationUnionArm {
                                        label: "fixpoint".into(),
                                        input: RelationExpr::Gather {
                                            seed: Box::new(scan("b")),
                                            step: Box::new(scan("c")),
                                            frontier_key: key.clone(),
                                            bound: RecursionBound::Fixpoint,
                                            dedupe_key: vec![],
                                        },
                                    },
                                ],
                            }),
                            on: vec![RelationJoinCondition {
                                left: column("x"),
                                right: column("y"),
                            }],
                            join_kind: RelationJoinKind::Left,
                        }),
                        columns: vec![
                            RelationProjectColumn {
                                alias: "c".into(),
                                expr: RelationProjectExpr::Column(column("c")),
                            },
                            RelationProjectColumn {
                                alias: "id".into(),
                                expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                            },
                        ],
                    }),
                    key: vec![key],
                }),
                terms: vec![
                    RelationOrderBy {
                        column: column("c"),
                        direction: OrderDirection::Desc,
                    },
                    RelationOrderBy {
                        column: column("id"),
                        direction: OrderDirection::Asc,
                    },
                ],
            }),
            offset: 2,
        }),
        limit: 10,
    };
    let inner_join = PolicyExpr::ExistsRel {
        rel: RelationExpr::Join {
            left: Box::new(scan("a")),
            right: Box::new(scan("b")),
            on: vec![],
            join_kind: RelationJoinKind::Inner,
        },
    };
    let uuid = || {
        crate::tools::ObjectId::from_uuid(uuid::Uuid::from_u128(
            0x1234_5678_9abc_def0_1234_5678_9abc_def0,
        ))
    };
    let outer =
        |column: &str| PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), column.into()]);
    PolicyExpr::Or(vec![
        // `name <= 3`: an integer against a text column, so `int(3)`.
        PolicyExpr::Cmp {
            column: "name".into(),
            op: CmpOp::Le,
            value: PolicyValue::Literal(Value::Integer(3)),
        },
        PolicyExpr::Cmp {
            column: "name".into(),
            op: CmpOp::Eq,
            value: PolicyValue::Literal(Value::Null),
        },
        PolicyExpr::Cmp {
            column: "owner".into(),
            op: CmpOp::Eq,
            value: PolicyValue::Literal(Value::Uuid(uuid())),
        },
        // An enclosing-row reference outside `exists`: only the raw path.
        PolicyExpr::Cmp {
            column: "owner".into(),
            op: CmpOp::Eq,
            value: outer("owner"),
        },
        PolicyExpr::SessionCmp {
            path: path(),
            op: CmpOp::Ne,
            value: Value::Text("x".into()),
        },
        PolicyExpr::SessionCmp {
            path: vec!["claims".into(), "odd path".into(), "and".into()],
            op: CmpOp::Gt,
            value: Value::Double(f64::NAN),
        },
        PolicyExpr::IsNull {
            column: "owner".into(),
        },
        PolicyExpr::SessionIsNull { path: path() },
        PolicyExpr::IsNotNull {
            column: "meta".into(),
        },
        PolicyExpr::SessionIsNotNull { path: path() },
        PolicyExpr::Contains {
            column: "tags".into(),
            value: PolicyValue::SessionRef(path()),
        },
        PolicyExpr::SessionContains {
            path: path(),
            value: Value::Text("y".into()),
        },
        PolicyExpr::In {
            column: "owner".into(),
            session_path: path(),
        },
        PolicyExpr::InList {
            column: "name".into(),
            values: vec![
                PolicyValue::Literal(Value::BigInt(1 << 40)),
                PolicyValue::SessionRef(path()),
                PolicyValue::Literal(Value::Text("it's".into())),
            ],
        },
        PolicyExpr::SessionInList {
            path: path(),
            values: vec![
                Value::Boolean(true),
                Value::Null,
                Value::Double(-0.5),
                Value::Timestamp(1_700_000_000_000),
                Value::Bytea(vec![0, 255]),
                Value::TransactionId([7; 16]),
                Value::Array(vec![Value::Integer(1), Value::Null]),
                Value::Row {
                    id: Some(uuid()),
                    values: vec![Value::Text("r".into())],
                },
                Value::Row {
                    id: None,
                    values: vec![],
                },
                Value::Enum {
                    case: "done".into(),
                    values: vec![Value::Timestamp(5)],
                },
            ],
        },
        PolicyExpr::Exists {
            table: "shares".into(),
            condition: Box::new(PolicyExpr::True),
        },
        // `exists` in `exists`, each level reaching the row around it.
        PolicyExpr::Exists {
            table: "shares".into(),
            condition: Box::new(PolicyExpr::And(vec![
                PolicyExpr::Cmp {
                    column: "todo".into(),
                    op: CmpOp::Eq,
                    value: outer("id"),
                },
                PolicyExpr::Exists {
                    table: "todos".into(),
                    condition: Box::new(PolicyExpr::And(vec![
                        PolicyExpr::Cmp {
                            column: "id".into(),
                            op: CmpOp::Eq,
                            value: outer("todo"),
                        },
                        PolicyExpr::Exists {
                            table: "shares".into(),
                            condition: Box::new(PolicyExpr::Cmp {
                                column: "user".into(),
                                op: CmpOp::Eq,
                                value: outer("owner"),
                            }),
                        },
                    ])),
                },
            ])),
        },
        PolicyExpr::ExistsRel { rel },
        inner_join,
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "owner".into(),
            max_depth: Some(3),
        },
        PolicyExpr::InheritsReferencing {
            operation: Operation::Delete,
            source_table: "shares".into(),
            via_column: "todo".into(),
            max_depth: None,
        },
        PolicyExpr::InheritsReferencing {
            operation: Operation::Select,
            source_table: "shares".into(),
            via_column: "user".into(),
            max_depth: Some(1),
        },
        PolicyExpr::And(vec![PolicyExpr::Not(Box::new(PolicyExpr::False))]),
        PolicyExpr::Or(vec![]),
        PolicyExpr::Not(Box::new(PolicyExpr::And(vec![
            PolicyExpr::Or(vec![PolicyExpr::True, PolicyExpr::False]),
            PolicyExpr::Not(Box::new(PolicyExpr::Not(Box::new(PolicyExpr::True)))),
        ]))),
    ])
}

/// Names of the variants `expr` uses, collected exhaustively.
fn policy_variants(expr: &PolicyExpr, seen: &mut BTreeSet<&'static str>) {
    fn rel(expr: &RelationExpr, seen: &mut BTreeSet<&'static str>) {
        let name = match expr {
            RelationExpr::TableScan { .. } => "TableScan",
            RelationExpr::Filter { input, predicate } => {
                rel(input, seen);
                pred(predicate, seen);
                "Filter"
            }
            RelationExpr::Union { inputs } => {
                inputs.iter().for_each(|arm| rel(&arm.input, seen));
                "Union"
            }
            RelationExpr::Join {
                left,
                right,
                join_kind,
                ..
            } => {
                rel(left, seen);
                rel(right, seen);
                match join_kind {
                    RelationJoinKind::Inner => seen.insert("Join.Inner"),
                    RelationJoinKind::Left => seen.insert("Join.Left"),
                };
                "Join"
            }
            RelationExpr::Project { input, columns } => {
                rel(input, seen);
                for column in columns {
                    match column.expr {
                        RelationProjectExpr::Column(_) => seen.insert("Project.Column"),
                        RelationProjectExpr::RowId(_) => seen.insert("Project.RowId"),
                    };
                }
                "Project"
            }
            RelationExpr::Gather {
                seed, step, bound, ..
            } => {
                rel(seed, seen);
                rel(step, seen);
                match bound {
                    RecursionBound::Fixpoint => seen.insert("Gather.Fixpoint"),
                    RecursionBound::MaxDepth(_) => seen.insert("Gather.MaxDepth"),
                };
                "Gather"
            }
            RelationExpr::Distinct { input, .. } => {
                rel(input, seen);
                "Distinct"
            }
            RelationExpr::OrderBy { input, .. } => {
                rel(input, seen);
                "OrderBy"
            }
            RelationExpr::Offset { input, .. } => {
                rel(input, seen);
                "Offset"
            }
            RelationExpr::Limit { input, .. } => {
                rel(input, seen);
                "Limit"
            }
        };
        seen.insert(name);
    }
    fn value(value: &RelationValueRef, seen: &mut BTreeSet<&'static str>) {
        let name = match value {
            RelationValueRef::Literal(_) => "Value.Literal",
            RelationValueRef::Param(_) => "Value.Param",
            RelationValueRef::SessionRef(_) => "Value.SessionRef",
            RelationValueRef::OuterColumn(_) => "Value.OuterColumn",
            RelationValueRef::FrontierColumn(_) => "Value.FrontierColumn",
            RelationValueRef::RowId(row) => match row {
                RelationRowIdRef::Current => "Value.RowId.Current",
                RelationRowIdRef::Outer => "Value.RowId.Outer",
                RelationRowIdRef::Frontier => "Value.RowId.Frontier",
            },
        };
        seen.insert(name);
    }
    fn pred(predicate: &RelationPredicate, seen: &mut BTreeSet<&'static str>) {
        let name = match predicate {
            RelationPredicate::Cmp { right, .. } => {
                value(right, seen);
                "Pred.Cmp"
            }
            RelationPredicate::IsNull { .. } => "Pred.IsNull",
            RelationPredicate::IsNotNull { .. } => "Pred.IsNotNull",
            RelationPredicate::In { values, .. } => {
                values.iter().for_each(|v| value(v, seen));
                "Pred.In"
            }
            RelationPredicate::Contains { right, .. } => {
                value(right, seen);
                "Pred.Contains"
            }
            RelationPredicate::EnumMatch { payload, .. } => {
                pred(payload, seen);
                "Pred.EnumMatch"
            }
            RelationPredicate::And(all) => {
                all.iter().for_each(|p| pred(p, seen));
                "Pred.And"
            }
            RelationPredicate::Or(all) => {
                all.iter().for_each(|p| pred(p, seen));
                "Pred.Or"
            }
            RelationPredicate::Not(inner) => {
                pred(inner, seen);
                "Pred.Not"
            }
            RelationPredicate::True => "Pred.True",
            RelationPredicate::False => "Pred.False",
        };
        seen.insert(name);
    }
    let name = match expr {
        PolicyExpr::Cmp { .. } => "Cmp",
        PolicyExpr::SessionCmp { .. } => "SessionCmp",
        PolicyExpr::IsNull { .. } => "IsNull",
        PolicyExpr::SessionIsNull { .. } => "SessionIsNull",
        PolicyExpr::IsNotNull { .. } => "IsNotNull",
        PolicyExpr::SessionIsNotNull { .. } => "SessionIsNotNull",
        PolicyExpr::Contains { .. } => "Contains",
        PolicyExpr::SessionContains { .. } => "SessionContains",
        PolicyExpr::In { .. } => "In",
        PolicyExpr::InList { .. } => "InList",
        PolicyExpr::SessionInList { .. } => "SessionInList",
        PolicyExpr::Exists { condition, .. } => {
            policy_variants(condition, seen);
            "Exists"
        }
        PolicyExpr::ExistsRel { rel: r } => {
            rel(r, seen);
            "ExistsRel"
        }
        PolicyExpr::Inherits { .. } => "Inherits",
        PolicyExpr::InheritsReferencing { .. } => "InheritsReferencing",
        PolicyExpr::And(all) => {
            all.iter().for_each(|e| policy_variants(e, seen));
            "And"
        }
        PolicyExpr::Or(all) => {
            all.iter().for_each(|e| policy_variants(e, seen));
            "Or"
        }
        PolicyExpr::Not(inner) => {
            policy_variants(inner, seen);
            "Not"
        }
        PolicyExpr::True => "True",
        PolicyExpr::False => "False",
    };
    seen.insert(name);
}

/// `expr` as `todos`' policy on every operation, through
/// jazz.permissions.json's text and back.
fn through_the_permissions_file(expr: &PolicyExpr) -> TablePolicies {
    let input = shared_todos();
    let current = loaded(shared_todos()).current_schema().clone();
    let relations = input.codegen.relations();
    let mut permissions = crate::catalogue::Permissions::new();
    let policies = TablePolicies {
        select: OperationPolicy::using(expr.clone()),
        insert: OperationPolicy::with_check(expr.clone()),
        update: OperationPolicy::using_and_check(PolicyExpr::True, expr.clone()),
        delete: OperationPolicy {
            using: Some(expr.clone()),
            with_check: Some(PolicyExpr::False),
        },
    };
    permissions.insert(TableName::from("todos"), policies.clone());
    let file = permissions_file::write(&permissions, &current, &relations).unwrap();
    let text = json::print(&file);
    only_strings(&file, &text);
    let reread = json::parse(text.as_bytes()).unwrap();
    let read = permissions_file::read(&reread, &current, &relations)
        .unwrap_or_else(|e| panic!("{e:?}\n{text}"));
    assert_eq!(read[&TableName::from("todos")], policies, "{text}");
    policies
}

/// Every policy in the file is a string, or anyOf / allOf / `$comment` /
/// `{ using, withCheck }` / `{ old, new }` around strings: no structured leaf.
fn only_strings(file: &json::Json, text: &str) {
    fn policy(value: &json::Json, text: &str) {
        match value {
            json::Json::String(_) => {}
            json::Json::Array(items) => items.iter().for_each(|i| policy(i, text)),
            json::Json::Object(entries) => {
                for (key, value) in entries {
                    assert!(
                        matches!(
                            key.as_str(),
                            "using" | "withCheck" | "old" | "new" | "anyOf" | "allOf" | "where"
                        ),
                        "a structured leaf (`{key}`) in:\n{text}"
                    );
                    policy(value, text);
                }
            }
            other => panic!("{other:?} as a policy in:\n{text}"),
        }
    }
    let json::Json::Object(top) = file else {
        panic!("{text}")
    };
    for (key, tables) in top {
        if key != "tables" {
            continue;
        }
        let json::Json::Object(tables) = tables else {
            panic!("{text}")
        };
        for (_, ops) in tables {
            let json::Json::Object(ops) = ops else {
                panic!("{text}")
            };
            ops.iter().for_each(|(_, p)| policy(p, text));
        }
    }
}

#[test]
fn every_policy_and_relation_variant_round_trips_through_the_file() {
    let expr = every_policy_variant();
    let mut seen = BTreeSet::new();
    policy_variants(&expr, &mut seen);
    // 20 policy variants, 10 relation variants (+2 join kinds, 2 projections,
    // 2 bounds), 11 predicates, 8 value refs.
    assert_eq!(seen.len(), 20 + 10 + 6 + 11 + 8, "{seen:?}");
    through_the_permissions_file(&expr);
    // And each top-level alternative on its own, so a leaf the grammar spells
    // is checked as a string too.
    if let PolicyExpr::And(items) | PolicyExpr::Or(items) = &expr {
        for item in items {
            through_the_permissions_file(item);
        }
    }
}

#[test]
fn relation_hops_and_nested_exists_round_trip() {
    use crate::query::{RelationExpr, RelationPredicate, RelationValueRef};
    let col = |scope: &str, column: &str| RelationColumnRef {
        scope: Some(scope.into()),
        column: column.into(),
    };
    // `policy.shares.where({ todo: todo.id }).hopTo("todo").where({ owner: me })`,
    // as TS (epic-drop's `files … hopTo("folder")`) builds it.
    let hop = PolicyExpr::ExistsRel {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::TableScan {
                        table: "shares".into(),
                        alias: None,
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: "todos".into(),
                        alias: Some("__hop_0".into()),
                    }),
                    on: vec![RelationJoinCondition {
                        left: col("shares", "todo"),
                        right: col("__hop_0", "id"),
                    }],
                    join_kind: RelationJoinKind::Inner,
                }),
                predicate: RelationPredicate::And(vec![
                    RelationPredicate::Cmp {
                        left: col("shares", "todo"),
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::OuterColumn(RelationColumnRef {
                            scope: None,
                            column: "id".into(),
                        }),
                    },
                    RelationPredicate::Cmp {
                        left: col("__hop_0", "owner"),
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::SessionRef(vec!["user".into()]),
                    },
                ]),
            }),
            columns: vec![RelationProjectColumn {
                alias: "id".into(),
                expr: RelationProjectExpr::Column(col("__hop_0", "id")),
            }],
        },
    };
    let policies = through_the_permissions_file(&hop);
    assert_eq!(policies.select.using, Some(hop.clone()));
    let text = printed("todos", &hop);
    assert_eq!(
        text,
        "exists (from shares join todos as __hop_0 on shares.todo = __hop_0.id where shares.todo = outer.id and __hop_0.owner = session.user select __hop_0.id as id)"
    );
    // The hop sugar compiles to the same relation.
    assert_eq!(
        read_policy(
            "todos",
            "exists shares(todo = this.id).todo(owner = session.user)",
            serde_json::json!({})
        )
        .unwrap(),
        hop
    );

    // Nested `exists` with aliases: a nested level names its own row.
    let nested = read_policy(
        "todos",
        "exists shares as s(s.todo = this.id, exists todos as t(t.owner = session.user))",
        serde_json::json!({}),
    )
    .unwrap();
    assert_eq!(
        nested,
        PolicyExpr::Exists {
            table: "shares".into(),
            condition: Box::new(PolicyExpr::And(vec![
                PolicyExpr::Cmp {
                    column: "todo".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), "id".into()]),
                },
                PolicyExpr::Exists {
                    table: "todos".into(),
                    condition: Box::new(PolicyExpr::Cmp {
                        column: "owner".into(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec!["user".into()]),
                    }),
                },
            ])),
        }
    );
    assert_eq!(
        printed("todos", &nested),
        "exists shares(todo = this.id, exists todos(owner = session.user))"
    );
    through_the_permissions_file(&nested);
    // Which row an outer column is, deeper in, the engine reads two ways:
    // the file only names it by its raw path.
    let error = read_policy(
        "todos",
        "exists shares as s(exists todos(id = s.todo))",
        serde_json::json!({}),
    )
    .unwrap_err();
    assert!(
        error.contains("names only its own row's columns"),
        "{error}"
    );
    let deep = PolicyExpr::Exists {
        table: "shares".into(),
        condition: Box::new(PolicyExpr::Exists {
            table: "todos".into(),
            condition: Box::new(PolicyExpr::Cmp {
                column: "id".into(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), "todo".into()]),
            }),
        }),
    };
    assert_eq!(
        printed("todos", &deep),
        "exists shares(exists todos(id = session.__jazz_outer_row.todo))"
    );
}

#[test]
fn readable_leaves_stay_strings() {
    // A policy the grammar spells exactly is written as a string, not structured.
    let input = shared_todos();
    let written = write(input).unwrap();
    assert!(
        !written.permissions.contains("\"type\": \"Cmp\""),
        "{}",
        written.permissions
    );
}
