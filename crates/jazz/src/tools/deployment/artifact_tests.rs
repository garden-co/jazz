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
    CmpOp, ColumnDescriptor, ColumnType, HistoryRetention, Operation, OperationPolicy, PolicyExpr,
    PolicyValue, RowDescriptor, SchemaBuilder, TableName,
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
                    ..Default::default()
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
                    ..Default::default()
                },
            ),
        ]),
        ..Default::default()
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
                    ColumnType::Enum {
                        variants: vec!["open".into(), "done".into()],
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
                 "column_type": {"type": "Enum", "variants": ["open", "done"]},
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
                    relations: BTreeMap::from([("shares".into(), reverse("shares", "todoRow"))]),
                    computed: vec![ComputedField {
                        name: "shareCount".into(),
                        field_type: FieldType {
                            column_type: ColumnType::Integer,
                            nullable: false,
                        },
                        from: vec!["id".into()],
                    }],
                    transforms: BTreeMap::from([("meta".into(), "jsonText".into())]),
                    ..Default::default()
                },
            ),
            (
                "shares".into(),
                CodegenTable {
                    relations: BTreeMap::from([("todoRow".into(), forward("todos", "todo"))]),
                    ts_only: vec![],
                    ..Default::default()
                },
            ),
        ]),
        transforms: BTreeMap::from([(
            "jsonText".into(),
            TransformDecl {
                stored: ColumnType::Json {
                    schema: Some(serde_json::json!({ "type": "object" })),
                },
                view: ColumnType::Text,
            },
        )]),
        ..Default::default()
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

/// A hop condition or relation filter with a plain literal (`name = 'x'`,
/// as the DSL's `.hop("rel", {name: "x"})` writes it) compiles to a bare
/// JSON literal. It validates and deploys alike: the compile reads it with
/// the column's type, and the catalogue keeps it as JSON (#5931).
#[test]
fn relation_literals_written_as_text_validate_and_deploy() {
    let written = write(shared_todos()).unwrap();
    for read in [
        "exists shares(todo = this.id).todoRow(name = 'x')",
        "exists (from todos where todos.name = 'x')",
    ] {
        let file = serde_json::json!({
            "format": 1,
            "tables": { "todos": { "read": read }, "shares": { "read": "true" } },
        });
        let loaded = load(written.app.as_bytes(), file.to_string().as_bytes())
            .unwrap_or_else(|error| panic!("{read}: {error}"));
        let request = loaded.deployment_request(false);
        let target = &request
            .schemas
            .iter()
            .find(|(hash, _)| *hash == request.target_schema_hash)
            .unwrap()
            .1;
        let mut with_policies = target.clone();
        for (name, policies) in &request.permissions {
            with_policies.get_mut(name).unwrap().policies = policies.clone();
        }
        crate::tools::public_schema_convert::check_deployable_policies(&with_policies)
            .unwrap_or_else(|error| panic!("validate {read}: {error}"));
        prepare_deployment(&DeploymentCatalogue::default(), request)
            .unwrap_or_else(|error| panic!("deploy {read}: {error}"));
    }
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
        "\"todoRow\": { \"rel\": \"todos\", \"column\": \"todo\" }",
        "\"shares\": { \"reverse\": \"shares\", \"relation\": \"todoRow\" }",
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
        "\"read\": \"allowedTo.read(todoRow)\"",
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
/// schema.
fn read_policy(table: &str, source: &str) -> Result<PolicyExpr, String> {
    let written = write(shared_todos()).unwrap();
    // `allowedTo` needs the table it inherits from to have the policy.
    let other = if table == "shares" { "todos" } else { "shares" };
    let file = serde_json::json!({
        "format": 1,
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
    let context = expr::Context {
        schema: loaded.current_schema(),
        relations: &relations,
        computed: &expr::ComputedNames::new(),
    };
    expr::print(expr, &context, table).unwrap()
}

/// Every construct of the grammar, as `(table, source)`: the writer prints
/// each back as it is.
const CONSTRUCTS: &[(&str, &str)] = &[
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
    ("shares", "allowedTo.read(todoRow)"),
    ("shares", "allowedTo.update(todoRow, maxDepth: 3)"),
    ("todos", "not owner = session.user"),
    ("todos", "owner = session.user or name = 'x'"),
    (
        "todos",
        "(owner = session.user or name = 'x') and tags contains 'y'",
    ),
    ("todos", "not (owner = null or name = 'x')"),
];

#[test]
fn every_construct_reads_and_prints_back() {
    for &(table, source) in CONSTRUCTS {
        let compiled = read_policy(table, source).unwrap_or_else(|e| panic!("{source}: {e}"));
        assert_eq!(printed(table, &compiled), source, "{source} printed as");
    }
}

#[test]
fn equivalent_spellings_compile_alike() {
    let same = |a: &str, b: &str| {
        assert_eq!(
            read_policy("todos", a).unwrap(),
            read_policy("todos", b).unwrap(),
            "{a} vs {b}"
        );
    };
    same("session.user = owner", "owner = session.user");
    same("this.owner = session.user", "owner = session.user");
    same("owner=session.user", "owner  =  session.user");
    same("((owner = session.user))", "owner = session.user");
}

#[test]
fn defs_and_calls_are_refused() {
    let written = write(shared_todos()).unwrap();
    let file = serde_json::json!({
        "format": 1,
        "defs": { "mine($todo)": "owner = session.user" },
        "tables": { "todos": { "read": "true" } },
    });
    let error = load(written.app.as_bytes(), file.to_string().as_bytes())
        .unwrap_err()
        .to_string();
    assert!(error.contains("`defs` was removed in alpha.66"), "{error}");
    assert!(error.contains("inline each definition"), "{error}");
    let error = read_policy("todos", "mine(this.id)").unwrap_err();
    assert!(error.contains("unknown function `mine`"), "{error}");
    let error = read_policy("todos", "owner = $todo").unwrap_err();
    assert!(error.contains("unknown column `$todo`"), "{error}");
}

#[test]
fn expression_errors_point_at_the_spot_and_suggest() {
    let error =
        read_policy("todos", "exists shares(todo = this.id, usr = session.user)").unwrap_err();
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
        ("unknownDef(this.id)", "unknown function `unknownDef`"),
        ("owner < null", "only `=` and `!=` compare with null"),
        ("owner = 'not-a-uuid'", "is not a UUID"),
        (
            "owner = name",
            "two columns of the same row can't be compared",
        ),
        (
            "exists shares(todo = this.id).todoRw(name = 'x')",
            "unknown relation `todoRw` on shares; did you mean `todoRow`?",
        ),
        (
            "outer.owner = session.user",
            "`outer.` names the row enclosing an `exists`",
        ),
        ("name = int('x')", "`int(…)` takes a 32-bit integer"),
        ("exists (from shares where)", "expected a column"),
        ("", "an empty expression"),
    ] {
        let error = read_policy("todos", source).unwrap_err();
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
        serde_json::json!({"$comment": "Inherited", "where": "allowedTo.read(todoRow)"});
    load(
        app.to_string().as_bytes(),
        permissions.to_string().as_bytes(),
    )
    .unwrap();
}

/// `__e2ee_` names Jazz's own end-to-end encryption tables, which schema
/// validation treats differently: an application may not define one in its
/// current schema, nor set policies on one, even when the schema it reads
/// the permissions against holds one (as Jazz's own will).
#[test]
fn an_unknown_version_is_refused_before_anything_else() {
    // `$schema` carries the version.
    let error = edited_app(|v| {
        v["$schema"] = "https://jazz.tools/schemas/jazz.app.v2.json".into();
        v["something_new"] = true.into();
    });
    assert!(
        error.contains("version 2 was written by a newer Jazz"),
        "{error}"
    );
    let error = edited_permissions(|v| {
        v["$schema"] = "https://example.com/jazz.permissions.v1.json".into();
    });
    assert!(error.contains("`$schema` must be"), "{error}");
    let error = edited_app(|v| {
        v.as_object_mut().unwrap().remove("$schema");
    });
    assert!(error.contains("no `$schema`"), "{error}");
    assert!(error.contains("jazz-tools"), "{error}");
    // A legacy `format` from a newer Jazz.
    let error = edited_app(|v| {
        v.as_object_mut().unwrap().remove("$schema");
        v["format"] = 2.into();
    });
    assert!(
        error.contains("version 2 was written by a newer Jazz"),
        "{error}"
    );
    let error = edited_permissions(|v| v["format"] = "1".into());
    assert!(
        error.contains("legacy `format` must be the integer 1"),
        "{error}"
    );
}

#[test]
fn legacy_files_load_as_version_1() {
    let written = write(shared_todos()).unwrap();
    let legacy = |text: &str, url: &str, legacy_url: Option<&str>, format: bool| {
        let mut v: serde_json::Value = serde_json::from_str(text).unwrap();
        let o = v.as_object_mut().unwrap();
        assert_eq!(o["$schema"], url);
        o.remove("$schema");
        let mut out = serde_json::Map::new();
        if let Some(legacy_url) = legacy_url {
            out.insert("$schema".into(), legacy_url.into());
        }
        if format {
            out.insert("format".into(), 1.into());
        }
        out.extend(o.clone());
        serde_json::Value::Object(out).to_string()
    };
    for (legacy_url, format) in [
        (true, true),  // what Jazz before alpha.66 wrote
        (false, true), // `format` alone
        (true, false), // the old URL alone
    ] {
        let app = legacy(
            &written.app,
            APP_SCHEMA_URL,
            legacy_url.then_some(LEGACY_APP_SCHEMA_URL),
            format,
        );
        let permissions = legacy(
            &written.permissions,
            PERMISSIONS_SCHEMA_URL,
            legacy_url.then_some(LEGACY_PERMISSIONS_SCHEMA_URL),
            format,
        );
        load(app.as_bytes(), permissions.as_bytes())
            .unwrap_or_else(|e| panic!("{legacy_url} {format}: {e}"));
    }
}

#[test]
fn a_legacy_format_takes_any_schema() {
    let written = write(shared_todos()).unwrap();
    let versioned = format!("\"$schema\": \"{APP_SCHEMA_URL}\",\n");
    for schema in [
        "\"./node_modules/jazz-tools/jazz.app.schema.json\"",
        "\"http://jazz.tools/schemas/jazz.app.schema.json\"",
        "null",
    ] {
        let legacy = written.app.replacen(
            &versioned,
            &format!("\"$schema\": {schema},\n  \"format\": 1,\n"),
            1,
        );
        load(legacy.as_bytes(), written.permissions.as_bytes())
            .unwrap_or_else(|e| panic!("{schema}: {e}"));
    }
    // Without `format`, `$schema` is the version and must be Jazz's.
    let error = edited_app(|v| v["$schema"] = "./jazz.app.schema.json".into());
    assert!(error.contains("`$schema` must be"), "{error}");
    let error = edited_app(|v| {
        v.as_object_mut().unwrap().remove("$schema");
        v["format"] = 0.into();
    });
    assert!(error.contains("version 0 is not supported"), "{error}");
    let error = edited_app(|v| v["$schema"] = "https://jazz.tools/schemas/jazz.app.v0.json".into());
    assert!(error.contains("version 0 is not supported"), "{error}");
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
        edited_app(|v| v["tables"]["shares"]["relations"]["todoRow"]["column"] = "nope".into());
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
    let error = write(input).unwrap_err();
    assert_eq!(error.kind(), ArtifactErrorKind::Unsupported, "{error}");
    assert!(error.is_unsupported());
    let error = error.to_string();
    assert!(error.contains(expr::UNSUPPORTED), "{error}");
    assert!(error.contains("catalogue-native enum"), "{error}");
    // A malformed file is wrong input, not something the files can't say.
    let refused = load(b"{", b"{}").unwrap_err();
    assert_eq!(refused.kind(), ArtifactErrorKind::Refused, "{refused}");
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
    let _ = read_policy("todos", &long);
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
        RelationPredicate::And(vec![RelationPredicate::Not(Box::new(
            RelationPredicate::False,
        ))]),
        RelationPredicate::Not(Box::new(RelationPredicate::True)),
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
            ],
        },
        PolicyExpr::Exists {
            table: "shares".into(),
            condition: Box::new(PolicyExpr::True),
        },
        // `exists` in `exists`. The one shape left out: an outer-row
        // reference in the nested `exists` (or outside any), which the
        // engine refuses at validation and deploy, as the writer does
        // (`the_writer_refuses_outer_rows_the_engine_refuses`).
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
                            column: "name".into(),
                            op: CmpOp::Eq,
                            value: PolicyValue::Literal(Value::Text("n".into())),
                        },
                        PolicyExpr::Exists {
                            table: "shares".into(),
                            condition: Box::new(PolicyExpr::Cmp {
                                column: "user".into(),
                                op: CmpOp::Eq,
                                value: PolicyValue::SessionRef(vec!["user".into()]),
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
        // No `max_depth`: the engine drops one on reverse inheritance, so the
        // file refuses it (`policies_the_dsl_and_the_engine_refused_are_refused`).
        PolicyExpr::InheritsReferencing {
            operation: Operation::Select,
            source_table: "shares".into(),
            via_column: "user".into(),
            max_depth: None,
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
            RelationPredicate::Removed(removed) => match *removed {},
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
            // Queries only: the policy language has no prefix/suffix test.
            RelationPredicate::StartsWith { .. } | RelationPredicate::EndsWith { .. } => {
                unreachable!("not a policy predicate")
            }
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
        // Jazz derives these for its system tables; the file has no
        // spelling for them, so they are not among the variants above.
        PolicyExpr::InheritsFrom { .. } => "InheritsFrom",
        PolicyExpr::InsertedByThisTransaction => "InsertedByThisTransaction",
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
    let read = permissions_file::read(&reread, &current, &relations, &Default::default())
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

/// `table_id('T')` reads as table `T`'s catalogue identity, compared with
/// a `uuid` column, and the file writes it back so; a table the schema lacks,
/// or a column that is no `uuid`, is refused where it is written.
#[test]
fn a_table_identity_reads_and_writes_as_table_id() {
    let expr = read_policy("todos", "owner = table_id('shares')").unwrap();
    assert_eq!(
        expr,
        PolicyExpr::Cmp {
            column: "owner".into(),
            op: CmpOp::Eq,
            value: PolicyValue::TableIdentity {
                table: "shares".into(),
            },
        }
    );
    assert_eq!(printed("todos", &expr), "owner = table_id('shares')");
    through_the_permissions_file(&expr);

    let error = read_policy("todos", "owner = table_id('notes')").unwrap_err();
    assert!(
        error.contains("`table_id('notes')` names no table"),
        "{error}"
    );
    let error = read_policy("todos", "name = table_id('shares')").unwrap_err();
    assert!(error.contains("a table identity is a uuid"), "{error}");
}

/// The nodes Jazz derives for its system tables have no spelling in the
/// file: writing one is refused, naming it, rather than written as text that
/// would read back as something else.
#[test]
fn derived_policy_nodes_are_not_written_to_the_file() {
    let codegen = shared_todos().codegen;
    let relations = codegen.relations();
    let loaded = loaded(shared_todos());
    let context = expr::Context {
        schema: loaded.current_schema(),
        relations: &relations,
        computed: &expr::ComputedNames::new(),
    };
    for (expr, name) in [
        (
            PolicyExpr::InheritsFrom {
                operation: Operation::Select,
                table_column: "owner".into(),
                id_column: "owner".into(),
                tables: vec!["shares".into()],
            },
            "InheritsFrom",
        ),
        (
            PolicyExpr::Exists {
                table: "shares".into(),
                condition: Box::new(PolicyExpr::InsertedByThisTransaction),
            },
            "InsertedByThisTransaction",
        ),
    ] {
        let error = expr::print(&expr, &context, "todos").unwrap_err();
        assert!(error.contains(&format!("`{name}`")), "{error}");
    }
}

#[test]
fn every_policy_and_relation_variant_round_trips_through_the_file() {
    let expr = every_policy_variant();
    let mut seen = BTreeSet::new();
    policy_variants(&expr, &mut seen);
    // 20 policy variants, 10 relation variants (+2 join kinds, 2 projections,
    // 2 bounds), 10 predicates, 8 value refs.
    assert_eq!(seen.len(), 20 + 10 + 6 + 10 + 8, "{seen:?}");
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
        "exists shares(todo = this.id).todoRow(owner = session.user)"
    );
    // The hop sugar compiles to the same relation.
    assert_eq!(
        read_policy(
            "todos",
            "exists shares(todo = this.id).todoRow(owner = session.user)"
        )
        .unwrap(),
        hop
    );

    // Nested `exists` with aliases: a nested level names its own row.
    let nested = read_policy(
        "todos",
        "exists shares as s(s.todo = this.id, exists todos as t(t.owner = session.user))",
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
    // Deeper in, the engine refuses an outer-row reference; so does the reader.
    let error = read_policy("todos", "exists shares as s(exists todos(id = s.todo))").unwrap_err();
    assert!(
        error.contains("names only its own row's columns"),
        "{error}"
    );
}

#[test]
fn the_writer_refuses_outer_rows_the_engine_refuses() {
    let outer =
        |column: &str| PolicyValue::SessionRef(vec!["__jazz_outer_row".into(), column.into()]);
    let current = loaded(shared_todos()).current_schema().clone();
    let relations = shared_todos().codegen.relations();
    let write_read = |expr: PolicyExpr| {
        let mut permissions = crate::catalogue::Permissions::new();
        permissions.insert(
            TableName::from("todos"),
            TablePolicies {
                select: OperationPolicy::using(expr),
                ..TablePolicies::default()
            },
        );
        permissions_file::write(&permissions, &current, &relations).unwrap_err()
    };
    // In an `exists` inside another `exists`.
    let nested = PolicyExpr::Exists {
        table: "shares".into(),
        condition: Box::new(PolicyExpr::Exists {
            table: "todos".into(),
            condition: Box::new(PolicyExpr::Cmp {
                column: "id".into(),
                op: CmpOp::Eq,
                value: outer("todo"),
            }),
        }),
    };
    let error = write_read(nested);
    assert!(
        error.contains("tables.todos.read: ") && error.contains(expr::NESTED_OUTER_ROW),
        "{error}"
    );
    assert_eq!(
        expr::NESTED_OUTER_ROW,
        "not supported in policies yet: an `exists` inside another `exists` can't refer to the checked row's columns"
    );
    // Outside any `exists`.
    let error = write_read(PolicyExpr::Cmp {
        column: "owner".into(),
        op: CmpOp::Eq,
        value: outer("owner"),
    });
    assert!(
        error.contains("tables.todos.read: ") && error.contains(expr::OUTSIDE_OUTER_ROW),
        "{error}"
    );
    // And the reader refuses the raw path, at its span.
    let error = read_policy(
        "todos",
        "exists shares(exists todos(id = session.__jazz_outer_row.todo))",
    )
    .unwrap_err();
    assert!(
        error.contains("`session.__jazz_outer_row` is not a session value"),
        "{error}"
    );
    assert!(error.contains("^^^^"), "{error}");
}

#[test]
fn epic_drops_hop_prints_as_a_hop() {
    let schema = schema(vec![
        (
            "folders",
            table(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Uuid),
                uuid_fk("parent_id", "folders"),
            ]),
        ),
        (
            "files",
            table(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                uuid_fk("folder_id", "folders"),
                ColumnDescriptor::new("owner_id", ColumnType::Uuid),
            ]),
        ),
    ]);
    let relations: BTreeMap<String, BTreeMap<String, CodegenRelation>> = [
        (
            "files".to_string(),
            [(
                "folder".to_string(),
                CodegenRelation::Forward {
                    table: "folders".into(),
                    column: "folder_id".into(),
                },
            )]
            .into(),
        ),
        (
            "folders".to_string(),
            [
                (
                    "parent".to_string(),
                    CodegenRelation::Forward {
                        table: "folders".into(),
                        column: "parent_id".into(),
                    },
                ),
                (
                    "filesViaFolder".to_string(),
                    CodegenRelation::Reverse {
                        table: "files".into(),
                        relation: "folder".into(),
                    },
                ),
            ]
            .into(),
        ),
    ]
    .into();
    // The IR TS's `relationToIr` builds for it.
    let rel: RelationExpr = serde_json::from_value(serde_json::json!({
        "Project": {
            "input": { "Filter": {
                "input": { "Join": {
                    "left": { "TableScan": { "table": "files" } },
                    "right": { "TableScan": { "table": "folders", "alias": "__hop_0" } },
                    "on": [{
                        "left": { "scope": "files", "column": "folder_id" },
                        "right": { "scope": "__hop_0", "column": "id" }
                    }],
                    "join_kind": "Inner"
                } },
                "predicate": { "And": [
                    { "Cmp": {
                        "left": { "scope": "files", "column": "id" },
                        "op": "Eq",
                        "right": { "OuterColumn": { "column": "id" } }
                    } },
                    { "Cmp": {
                        "left": { "scope": "__hop_0", "column": "owner_id" },
                        "op": "Eq",
                        "right": { "SessionRef": ["user"] }
                    } }
                ] }
            } },
            "columns": [{
                "alias": "id",
                "expr": { "Column": { "scope": "__hop_0", "column": "id" } }
            }]
        }
    }))
    .unwrap();
    let hop = PolicyExpr::ExistsRel { rel };
    let context = expr::Context {
        schema: &schema,
        relations: &relations,
        computed: &expr::ComputedNames::new(),
    };
    let text = expr::print(&hop, &context, "files").unwrap();
    assert_eq!(
        text,
        "exists files(id = this.id).folder(owner_id = session.user)"
    );
    assert!(!text.contains("__hop"), "{text}");
    let back = expr::compile(&expr::parse(&text).unwrap(), &context, "files").unwrap();
    assert_eq!(back, hop);

    // And through the file, inside the update check as epic-drop has it.
    let policy = PolicyExpr::And(vec![
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "folder_id".into(),
            max_depth: None,
        },
        PolicyExpr::Or(vec![
            PolicyExpr::Cmp {
                column: "owner_id".into(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec!["user".into()]),
            },
            hop.clone(),
        ]),
    ]);
    let mut permissions = crate::catalogue::Permissions::new();
    let policies = TablePolicies {
        update: OperationPolicy::using_and_check(PolicyExpr::True, policy),
        ..TablePolicies::default()
    };
    permissions.insert(TableName::from("files"), policies.clone());
    let file = permissions_file::write(&permissions, &schema, &relations).unwrap();
    let text = json::print(&file);
    assert!(
        text.contains("exists files(id = this.id).folder(owner_id = session.user)"),
        "{text}"
    );
    assert!(!text.contains("__hop"), "{text}");
    let read = permissions_file::read(
        &json::parse(text.as_bytes()).unwrap(),
        &schema,
        &relations,
        &Default::default(),
    )
    .unwrap_or_else(|e| panic!("{e:?}\n{text}"));
    assert_eq!(read[&TableName::from("files")], policies, "{text}");
}

/// Deep nesting of every recursive construct is refused with an error, never
/// a stack overflow.
#[test]
fn deep_nesting_is_refused_not_overflowed() {
    const N: usize = 5000;
    let nest = |open: &str, inner: &str, close: &str| {
        format!("{}{inner}{}", open.repeat(N), close.repeat(N))
    };
    let deep_sources = [
        (
            "typed literals",
            nest("array(", "", ")").replacen("array(", "owner = array(", 1),
        ),
        ("parentheses", nest("(", "true", ")")),
        ("not", nest("not ", "true", "")),
        ("anyOf", nest("anyOf(", "true", ")")),
        ("exists", nest("exists shares(", "", ")")),
        (
            "from",
            format!("exists ({})", nest("from (", "from shares", ")")),
        ),
        (
            "join",
            format!(
                "exists ({})",
                nest("from shares join (", "from shares", ")")
            ),
        ),
        (
            "union",
            format!("exists ({})", nest("from union(a: ", "from shares", ")")),
        ),
        (
            "recursive",
            format!(
                "exists ({})",
                nest(
                    "from recursive(seed: ",
                    "from shares",
                    ", step: from shares, frontier: id(row), dedupe: (), maxDepth: 1)"
                )
            ),
        ),
        (
            "where not",
            format!("exists (from shares where {})", nest("not ", "true", "")),
        ),
        (
            "where parentheses",
            format!("exists (from shares where {})", nest("(", "true", ")")),
        ),
        (
            "relation typed literals",
            format!(
                "exists (from shares where shares.user = {})",
                nest("array(", "", ")")
            ),
        ),
    ];
    for (what, source) in deep_sources {
        let error = read_policy("todos", &source).unwrap_err();
        assert!(error.contains("nests too deeply"), "{what}: {error}");
    }

    // The files' JSON.
    let written = write(shared_todos()).unwrap();
    for (what, deep) in [
        ("arrays", nest("[", "1", "]")),
        ("objects", nest("{\"a\": ", "1", "}")),
    ] {
        let file = format!("{{\"format\": 1, \"tables\": {{}}, \"x\": {deep}}}");
        let error = load(written.app.as_bytes(), file.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(error.contains("nesting too deep"), "{what}: {error}");
        let error = load(file.as_bytes(), written.permissions.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(error.contains("nesting too deep"), "{what}: {error}");
    }

    // The writer, given an engine policy nested deeper than the grammar reads.
    let current = loaded(shared_todos()).current_schema().clone();
    let relations = shared_todos().codegen.relations();
    let mut not = PolicyExpr::True;
    let mut array = Value::Null;
    let mut rel = RelationExpr::TableScan {
        table: "shares".into(),
        alias: None,
    };
    for _ in 0..N {
        not = PolicyExpr::Not(Box::new(not));
        array = Value::Array(vec![array]);
        rel = RelationExpr::Limit {
            input: Box::new(rel),
            limit: 1,
        };
    }
    for deep in [
        not,
        PolicyExpr::SessionCmp {
            path: vec!["user".into()],
            op: CmpOp::Eq,
            value: array,
        },
        PolicyExpr::ExistsRel { rel },
    ] {
        let mut permissions = crate::catalogue::Permissions::new();
        permissions.insert(
            TableName::from("todos"),
            TablePolicies {
                select: OperationPolicy::using(deep),
                ..TablePolicies::default()
            },
        );
        let error = permissions_file::write(&permissions, &current, &relations).unwrap_err();
        assert!(error.contains("nested more than 64 levels deep"), "{error}");
    }
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

// ---------------------------------------------------------------------------
// Computed fields, transforms, TODOs and .63 leftovers
// ---------------------------------------------------------------------------

fn edited_app_loads(edit: impl FnOnce(&mut serde_json::Value)) -> AppDefinition {
    let written = write(shared_todos()).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&written.app).unwrap();
    edit(&mut value);
    load_app(value.to_string().as_bytes()).unwrap_or_else(|error| panic!("{error}"))
}

#[test]
fn computed_fields_and_transforms_load_into_the_codegen_metadata() {
    let app = edited_app_loads(|_| {});
    let todos = &app.codegen().tables["todos"];
    assert_eq!(todos.computed.len(), 1);
    assert_eq!(todos.computed[0].name, "shareCount");
    assert_eq!(todos.computed[0].from, vec!["id".to_string()]);
    assert_eq!(todos.transforms["meta"], "jsonText");
    assert_eq!(app.codegen().transforms["jsonText"].view, ColumnType::Text);
    // A column's transform doesn't change the stored schema.
    let plain = edited_app_loads(|v| {
        v["tables"]["todos"]["columns"]["meta"]
            .as_object_mut()
            .unwrap()
            .remove("transform");
        v["tables"]["todos"]
            .as_object_mut()
            .unwrap()
            .remove("computed");
    });
    assert_eq!(app.current_hash(), plain.current_hash());
}

#[test]
fn computed_fields_and_transforms_must_name_what_exists() {
    let error = edited_app(|v| {
        v["tables"]["todos"]["computed"]["shareCount"]["from"] = serde_json::json!(["nmae"])
    });
    assert!(
        error.contains("tables.todos.computed.shareCount.from[0]: table todos has no column nmae; did you mean `name`?"),
        "{error}"
    );
    let error = edited_app(|v| {
        v["tables"]["todos"]["computed"]["name"] = serde_json::json!({"type": "string", "from": []})
    });
    assert!(
        error.contains("tables.todos.computed.name: table todos already has a column name"),
        "{error}"
    );
    let error = edited_app(|v| {
        v["tables"]["todos"]["computed"]["shares"] = serde_json::json!({"type": "int", "from": []})
    });
    assert!(error.contains("already has a relation shares"), "{error}");
    let error = edited_app(|v| {
        v["tables"]["todos"]["computed"]["twice"] =
            serde_json::json!({"type": "int", "from": ["shareCount"]})
    });
    assert!(
        error.contains("`shareCount` is a computed field"),
        "{error}"
    );
    let error =
        edited_app(|v| v["tables"]["todos"]["computed"]["x"] = serde_json::json!({"from": []}));
    assert!(error.contains("a computed field needs `type`"), "{error}");
    let error = edited_app(|v| {
        v["tables"]["todos"]["computed"]["x"] =
            serde_json::json!({"type": {"type": "int", "default": 1}, "from": []})
    });
    assert!(
        error.contains("computed.x.type.default: `default` is for stored columns"),
        "{error}"
    );
    let error =
        edited_app(|v| v["tables"]["todos"]["columns"]["meta"]["transform"] = "jsonTxt".into());
    assert!(
        error.contains("tables.todos.columns.meta.transform: no transform `jsonTxt` in the top-level `transforms`; did you mean `jsonText`?"),
        "{error}"
    );
    let error = edited_app(|v| {
        v["tables"]["todos"]["columns"]["name"] =
            serde_json::json!({"type": "string", "transform": "jsonText"})
    });
    assert!(
        error.contains("transform `jsonText` stores { \"json\": { \"type\": \"object\" } }, but column name is \"string\""),
        "{error}"
    );
    let error = edited_app(|v| {
        v["transforms"]["bad"] = serde_json::json!({"stored": "string?", "view": "string"})
    });
    assert!(
        error.contains("transforms.bad.stored: a transform's types carry no `?`"),
        "{error}"
    );
    let error = edited_app(|v| v["transforms"]["bad"] = serde_json::json!({"stored": "string"}));
    assert!(error.contains("a transform needs `view`"), "{error}");
    let error = edited_app(|v| {
        v["history"]["00304e9bce65"]["tables"]["todos"]["computed"] = serde_json::json!({})
    });
    assert!(
        error.contains("only the current `tables` declare computed fields"),
        "{error}"
    );
}

#[test]
fn unresolved_todos_are_refused_with_every_path() {
    let error = edited_app(|v| {
        v["migrations"][0]["migrate"]["todos"]["tags"]["default"] =
            "TODO: a value for existing rows".into();
        v["migrations"][0]["migrate"]["todos"]["meta"] = "TODO: rename or add?".into();
    });
    assert!(error.contains("2 unresolved TODOs"), "{error}");
    assert!(
        error.contains(
            "  migrations[0].migrate.todos.tags.default: TODO: a value for existing rows"
        ),
        "{error}"
    );
    assert!(
        error.contains("  migrations[0].migrate.todos.meta: TODO: rename or add?"),
        "{error}"
    );
    // A comment may say TODO.
    edited_app_loads(|v| v["$comment"] = "TODO: tidy up".into());
    // The client loader refuses them too.
    let written = write(shared_todos()).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&written.app).unwrap();
    value["tables"]["todos"]["computed"]["shareCount"]["type"] = "TODO: the type".into();
    assert!(load_client(value.to_string().as_bytes()).is_err());
}

#[test]
fn slices_and_ts_only_from_alpha63_load_with_a_warning() {
    let app = edited_app_loads(|v| {
        v["slices"] = serde_json::json!({ "sharing": ["shares"] });
        v["tables"]["shares"]["tsOnly"] = serde_json::json!({ "user": "transform" });
    });
    assert_eq!(app.warnings().len(), 2, "{:?}", app.warnings());
    assert!(
        app.warnings()
            .iter()
            .any(|w| w.contains("slices: schema slicing is gone in alpha.66"))
    );
    assert!(app.warnings().iter().any(|w| {
        w.contains("tables.shares.tsOnly: `tsOnly` (written before alpha.66) is ignored")
    }));
    assert!(edited_app_loads(|_| {}).warnings().is_empty());
}

#[test]
fn migrations_take_a_name_and_comments() {
    let app = edited_app_loads(|v| {
        v["migrations"][0]["name"] = "rename-title".into();
        v["migrations"][0]["migrate"]["$comment"] = "why".into();
        v["migrations"][0]["migrate"]["todos"]["$comment"] = "title is now name".into();
    });
    assert_eq!(app.migrations().len(), 1);
    let error = edited_app(|v| v["migrations"][0]["name"] = 3.into());
    assert!(
        error.contains("migrations[0].name: must be a string"),
        "{error}"
    );
}

#[test]
fn the_source_hash_ignores_formatting_but_not_content() {
    let canonical = source_hash(b"{\n  \"format\": 1,\n  \"tables\": {}\n}\n");
    for same in [
        &b"{\r\n  \"format\": 1,\r\n  \"tables\": {}\r\n}\r\n"[..],
        b"{\"format\":1,\"tables\":{}}",
        b"\xef\xbb\xbf{\n\t\"format\": 1, \"tables\": {  }\n}",
    ] {
        assert_eq!(
            source_hash(same),
            canonical,
            "{}",
            String::from_utf8_lossy(same)
        );
    }
    assert_ne!(source_hash(b"{\"format\": 2, \"tables\": {}}"), canonical);
    // Key order is content: columns are in declared order.
    assert_ne!(source_hash(b"{\"tables\": {}, \"format\": 1}"), canonical);
    assert!(
        canonical.starts_with("sha256:") && canonical.len() == 7 + 64,
        "{canonical}"
    );
}

#[test]
fn a_version_whose_rows_pass_unchanged_needs_no_written_migration() {
    // A .64 file: `migrations create` wrote a snapshot, but no migration,
    // for a change of indexes alone.
    let file = r#"{ "format": 1,
        "tables": { "todos": { "columns": { "title": "string", "done": "boolean" }, "indexes": ["title"] } },
        "history": { "a1b2c3d4e5f6": { "tables": { "todos": { "columns": { "title": "string", "done": "boolean" } } } } } }"#;
    let app = load_app(file.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(app.migrations().len(), 1, "the implied identity step");
    assert!(
        app.migrations()[0]
            .forward
            .iter()
            .all(|lens| lens.operations.is_empty())
    );
    assert_eq!(load_client(file.as_bytes()).unwrap().schemas.len(), 2);
    assert!(app.stranded().is_empty());
    // A version whose rows would need transforming still needs its migration.
    let structural = file.replace(
        r#""done": "boolean" } } } } }"#,
        r#""note": "string?" } } } } }"#,
    );
    let app = load_app(structural.as_bytes()).unwrap();
    assert!(
        app.stranded()[0].contains("history.a1b2c3d4e5f6: no migration leads from it"),
        "{:?}",
        app.stranded()
    );
}

#[test]
fn todo_is_only_a_placeholder_where_the_tools_write_one() {
    let file = |extra: &str| {
        format!(
            r#"{{ "format": 1, "tables": {{ "notes": {{ "columns": {{
                "title": {{ "type": "string", "default": "TODO: later" }},
                "kind": {{ "enum": ["TODO:x", "done"] }},
                "meta": {{ "json": {{ "type": "object", "description": "TODO: describe" }}, "transform": "metaText" }}
            }} }} }},
            "transforms": {{ "metaText": {{ "stored": {{ "json": {{ "type": "object", "description": "TODO: describe" }} }}, "view": {extra} }} }} }}"#
        )
    };
    load_app(file("\"string\"").as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let error = load_app(file("\"TODO: the view type\"").as_bytes())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("transforms.metaText.view: TODO: the view type"),
        "{error}"
    );
    let error = load_app(file("{ \"json\": \"TODO: a schema\" }").as_bytes())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("transforms.metaText.view.json: TODO: a schema"),
        "{error}"
    );
}

#[test]
fn load_app_checks_what_load_checks_of_the_app_file() {
    let written = write(shared_todos()).unwrap();
    let app = load_app(written.app.as_bytes()).unwrap();
    let full = load(written.app.as_bytes(), written.permissions.as_bytes()).unwrap();
    assert_eq!(app.current_hash(), full.current_hash());
    assert_eq!(app.schemas().len(), full.schemas().len());
    assert_eq!(app.codegen(), full.codegen());
}

#[test]
fn version_names_javascript_would_reorder_are_refused() {
    let error = edited_app(|v| {
        let history = v["history"].as_object_mut().unwrap();
        let version = history.remove("00304e9bce65").unwrap();
        history.insert("12".into(), version);
        v["migrations"][0]["from"] = "12".into();
    });
    assert!(
        error.contains("history.12: `12` can't name a version"),
        "{error}"
    );
    // Twelve digits are never an array index.
    for name in ["123456789012", "000000000001", "4294967295"] {
        edited_app_loads(|v| {
            let history = v["history"].as_object_mut().unwrap();
            let version = history.remove("00304e9bce65").unwrap();
            history.insert(name.into(), version);
            v["migrations"][0]["from"] = name.into();
        });
    }
}

#[test]
fn the_exact_login_is_session_identity() {
    let expr = read_policy("todos", "$createdBy.identity = session.identity").unwrap();
    assert_eq!(
        printed("todos", &expr),
        "$createdBy.identity = session.identity"
    );
    let account = read_policy("todos", "$createdBy = session.user").unwrap();
    assert_ne!(expr, account);
    let error = read_policy("todos", "$createdBy.identity = session.identty").unwrap_err();
    assert!(error.contains("`session.identity`"), "{error}");
}

#[test]
fn a_version_no_migration_leads_from_loads_with_a_warning() {
    // As `jazz-tools validate` wrote files before alpha.66 while a
    // migration was still to be written; `generate` refuses them.
    let written = write(shared_todos()).unwrap();
    let mut value: serde_json::Value = serde_json::from_str(&written.app).unwrap();
    value.as_object_mut().unwrap().remove("migrations");
    let app = load_app(value.to_string().as_bytes()).unwrap();
    assert_eq!(app.stranded().len(), 1);
    assert!(
        app.stranded()[0]
            .contains("history.00304e9bce65: no migration leads from it to the current `tables`"),
        "{:?}",
        app.stranded()
    );
    assert!(app.warnings().contains(&app.stranded()[0]));
    load_client(value.to_string().as_bytes()).unwrap();
    let loaded = load(value.to_string().as_bytes(), written.permissions.as_bytes()).unwrap();
    assert_eq!(loaded.warnings().len(), 1);
}

/// `policy.<t>.gather(…)` policies from the TS DSL: a query inside
/// `recursive(…)` ends at the next `, <field>:`, so a `select` or `order by`
/// list doesn't run on into it. The writer prints them and reads them back.
#[test]
fn recursive_relations_end_their_queries_at_the_next_field() {
    // permissions.repro.test.ts's "explicit gather seeds through a
    // same-table double-ref edge".
    let app = r#"{ "format": 1, "tables": {
        "teams": { "columns": { "name": "string" }, "relations": {
            "team_entryViaTeam": { "reverse": "team_entry", "relation": "teamRelation" },
            "team_entryViaTarget": { "reverse": "team_entry", "relation": "target" },
            "dropdowns_access_edgesViaTeam": { "reverse": "dropdowns_access_edges", "relation": "teamRelation" } } },
        "team_entry": { "columns": { "team_id": "uuid", "target_id": "uuid", "user_id": "uuid", "administrator": "boolean" },
            "relations": { "teamRelation": { "rel": "teams", "column": "team_id" }, "target": { "rel": "teams", "column": "target_id" } } },
        "dropdowns": { "columns": { "name": "string" }, "relations": {
            "dropdowns_access_edgesViaResource": { "reverse": "dropdowns_access_edges", "relation": "resource" } } },
        "dropdowns_access_edges": { "columns": { "resource_id": "uuid", "team_id": "uuid", "grant_role": "string", "administrator": "boolean" },
            "relations": { "resource": { "rel": "dropdowns", "column": "resource_id" }, "teamRelation": { "rel": "teams", "column": "team_id" } } }
    } }"#;
    let dropdowns = "exists (from recursive(seed: from team_entry join teams as __hop_0 on team_entry.target_id = __hop_0.id where team_entry.user_id = session.user select __hop_0.id as id, step: from team_entry where team_entry.administrator = bool(false) and team_entry.team_id = id(frontier) select team_entry.target_id as id, frontier: id(row), dedupe: (id(row)), maxDepth: 8) join dropdowns_access_edges as __recursive_join_0 on teams.id = __recursive_join_0.team_id where __recursive_join_0.resource_id = outer.id and __recursive_join_0.grant_role in [text('viewer')] and __recursive_join_0.administrator = bool(false) select __recursive_join_0.id as id)";
    let ordered = "exists (from recursive(seed: from teams select teams.id as id, teams.name as name, step: from teams where teams.id = id(frontier) order by teams.name, teams.id select teams.id as id, teams.name as name, frontier: id(row), dedupe: (id(row)), maxDepth: fixpoint) where teams.id = outer.id)";
    for text in [dropdowns, ordered] {
        expr::parse(text).unwrap_or_else(|e| panic!("{}", e.render(text)));
    }
    let permissions = serde_json::json!({
        "format": 1,
        "tables": { "dropdowns": { "read": dropdowns }, "teams": { "read": "true" } },
    });
    let loaded =
        load(app.as_bytes(), permissions.to_string().as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let codegen = load_app(app.as_bytes()).unwrap().codegen().clone();
    let written = write(ArtifactInput {
        current: loaded.current_hash().to_string(),
        schemas: vec![loaded.current_schema().clone()],
        migrations: vec![],
        permissions: loaded.permissions().clone(),
        codegen,
    })
    .unwrap_or_else(|e| panic!("{e}"));
    let reloaded = load(written.app.as_bytes(), written.permissions.as_bytes()).unwrap();
    assert_eq!(reloaded.permissions(), loaded.permissions());
    assert_eq!(reloaded.bundle(), loaded.bundle());
}

// ---------------------------------------------------------------------------
// The JSON Schemas editors read (docs/public/schemas/)
// ---------------------------------------------------------------------------

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn json_schema(file: &str) -> serde_json::Value {
    let path = repo_root().join("docs/public/schemas").join(file);
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

/// Every app file in the repository: fixtures, examples, codegen test apps.
fn committed(file: &str) -> Vec<std::path::PathBuf> {
    let out = std::process::Command::new("git")
        .args(["ls-files", &format!("*{file}")])
        .current_dir(repo_root())
        .output()
        .expect("git");
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|line| repo_root().join(line))
        .collect()
}

#[test]
fn every_committed_app_file_fits_its_json_schema() {
    for (file, schema, legacy_schema) in [
        (APP_FILE, "jazz.app.v1.json", "jazz.app.schema.json"),
        (
            PERMISSIONS_FILE,
            "jazz.permissions.v1.json",
            "jazz.permissions.schema.json",
        ),
    ] {
        let validator = jsonschema::validator_for(&json_schema(schema)).unwrap();
        // Legacy files (`format` and/or the unversioned URL) still load, and
        // the old JSON Schema still describes them.
        let legacy_validator = jsonschema::validator_for(&json_schema(legacy_schema)).unwrap();
        let files = committed(file);
        assert!(files.len() > 10, "{file}: {files:?}");
        for path in files {
            let text = std::fs::read_to_string(&path).unwrap();
            let instance: serde_json::Value = serde_json::from_str(&text).unwrap();
            let validator = if instance.get("format").is_some()
                || !instance["$schema"]
                    .as_str()
                    .is_some_and(|s| s.ends_with(schema))
            {
                &legacy_validator
            } else {
                &validator
            };
            let errors: Vec<String> = validator
                .iter_errors(&instance)
                .map(|e| format!("{} at {}", e, e.instance_path()))
                .collect();
            assert!(
                errors.is_empty(),
                "{}:\n{}",
                path.display(),
                errors.join("\n")
            );
        }
    }
    // And what the writer writes.
    let written = write(shared_todos()).unwrap();
    for (text, schema) in [
        (&written.app, "jazz.app.v1.json"),
        (&written.permissions, "jazz.permissions.v1.json"),
    ] {
        let validator = jsonschema::validator_for(&json_schema(schema)).unwrap();
        let instance: serde_json::Value = serde_json::from_str(text).unwrap();
        assert!(validator.is_valid(&instance), "{schema}");
    }
}

fn properties(value: &serde_json::Value) -> BTreeSet<String> {
    value["properties"]
        .as_object()
        .unwrap_or_else(|| panic!("no properties in {value}"))
        .keys()
        .filter(|k| *k != "$comment")
        .cloned()
        .collect()
}

fn keys(list: &[&str]) -> BTreeSet<String> {
    // `slices` and `tsOnly` load (with a warning) but aren't written any
    // more: the JSON Schema flags them.
    // `format` is legacy: `$schema` carries the version.
    list.iter()
        .filter(|k| !matches!(**k, "slices" | "tsOnly" | "format"))
        .map(|k| k.to_string())
        .collect()
}

#[test]
fn the_json_schemas_take_the_keys_the_readers_take() {
    let app = json_schema("jazz.app.v1.json");
    let defs = &app["$defs"];
    for (what, schema, reader) in [
        ("the top level", properties(&app), keys(app_file::TOP_KEYS)),
        (
            "a table",
            properties(&defs["table"]),
            keys(app_file::TABLE_KEYS),
        ),
        (
            "a column",
            properties(&defs["column"]["anyOf"][1]),
            keys(app_file::COLUMN_KEYS),
        ),
        (
            "a migration",
            properties(&defs["migration"]),
            keys(app_file::MIGRATION_KEYS),
        ),
        (
            "a version",
            properties(&app["properties"]["history"]["additionalProperties"]),
            keys(app_file::VERSION_KEYS),
        ),
        (
            "a transform",
            properties(&app["properties"]["transforms"]["additionalProperties"]),
            keys(app_file::TRANSFORM_KEYS),
        ),
        (
            "a computed field",
            properties(&defs["table"]["properties"]["computed"]["additionalProperties"]),
            keys(app_file::COMPUTED_KEYS),
        ),
    ] {
        assert_eq!(schema, reader, "{what}");
    }
    let permissions = json_schema("jazz.permissions.v1.json");
    assert_eq!(properties(&permissions), keys(permissions_file::TOP_KEYS));
}

#[test]
fn the_permissions_json_schema_names_every_grammar_construct() {
    let text =
        std::fs::read_to_string(repo_root().join("docs/public/schemas/jazz.permissions.v1.json"))
            .unwrap();
    let words: BTreeSet<&str> = text
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .collect();
    let missing: Vec<&str> = expr::KEYWORDS
        .iter()
        .copied()
        .filter(|keyword| !words.contains(keyword))
        .collect();
    assert!(
        missing.is_empty(),
        "keywords the description doesn't name: {missing:?}"
    );
    for construct in [
        "session.user",
        "session.identity",
        "session.identity.subject",
        "session.identity.issuer",
        "session.authMode",
        "session.claims",
        "$createdBy.identity",
        "$deletedBy",
        "$deletedAt",
        "allowedTo.update",
        "maxDepth",
        "recursive(",
        "union(",
        "distinct on",
        "order by",
        "left join",
        "id(row)",
        "id(outer)",
        "id(frontier)",
        "param.",
        "row(",
        "json(",
    ] {
        assert!(
            text.contains(construct),
            "the description doesn't name `{construct}`"
        );
    }
}

/// A multi-column `select` in a recursive relation's seed and step prints
/// and reads back as the same policy (the printer refuses a spelling that
/// doesn't read back).
#[test]
fn recursive_relations_with_several_selected_columns_print_and_read_back() {
    let codegen = shared_todos().codegen;
    let relations = codegen.relations();
    let loaded = loaded(shared_todos());
    let context = expr::Context {
        schema: loaded.current_schema(),
        relations: &relations,
        computed: &expr::ComputedNames::new(),
    };
    let text = "exists (from recursive(seed: from todos where todos.owner = session.user select todos.id as id, todos.name as name, step: from todos where todos.id = id(frontier) order by todos.name, todos.id select todos.id as id, todos.name as name, frontier: id(row), dedupe: (id(row)), maxDepth: 3) where todos.id = outer.id)";
    let ast = expr::parse(text).unwrap_or_else(|e| panic!("{}", e.render(text)));
    let policy =
        expr::compile(&ast, &context, "shares").unwrap_or_else(|e| panic!("{}", e.render(text)));
    let printed = expr::print(&policy, &context, "shares").unwrap_or_else(|e| panic!("{e}"));
    let again = expr::compile(&expr::parse(&printed).unwrap(), &context, "shares").unwrap();
    assert_eq!(again, policy, "{printed}");
}

/// A migration can't make a kept column optional: the deploy's lens check
/// refuses it, as `migrations create` does. So an app whose old reference
/// column is required can't loosen it before switching to a new reference
/// column (the encryption guide's copy recipe relies on this).
#[test]
fn a_migration_cannot_make_a_kept_required_reference_optional() {
    let file = r#"{"$schema":"https://jazz.tools/schemas/jazz.app.v1.json",
        "tables":{"tasks":{"columns":{"title":"string"}},
                  "comments":{"columns":{"body":"string","taskId":{"type":"uuid","references":"tasks","optional":true}}}},
        "history":{"v1":{"tables":{"tasks":{"columns":{"title":"string"}},
                  "comments":{"columns":{"body":"string","taskId":{"type":"uuid","references":"tasks"}}}}}},
        "migrations":[{"from":"v1","migrate":{}}]}"#;
    let permissions =
        br#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": {} }"#;
    let error = load(file.as_bytes(), permissions)
        .err()
        .expect("refused")
        .to_string();
    assert!(
        error.contains("\"taskId\" comes out required but is optional")
            && error.contains("must keep its type and optionality"),
        "{error}"
    );
}

#[test]
fn a_migration_may_not_move_a_reference_to_another_table() {
    let file = r#"{ "format": 1,
        "tables": {
            "a": { "columns": { "x": "string" } }, "b": { "columns": { "x": "string" } },
            "notes": { "columns": { "owner": { "type": "uuid", "references": "b" }, "body": "string" } } },
        "history": { "v1": { "tables": {
            "a": { "columns": { "x": "string" } }, "b": { "columns": { "x": "string" } },
            "notes": { "columns": { "owner": { "type": "uuid", "references": "a" } } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": { "body": { "add": "string", "default": "" } } } }] }"#;
    let error = load_app(file.as_bytes()).unwrap_err().to_string();
    assert!(
        error.contains("migrations[0]: column notes.owner moves its reference from a to b, which is not supported yet"),
        "{error}"
    );
    // Gaining a reference is fine.
    let gains = file.replace(r#"{ "type": "uuid", "references": "a" }"#, r#""uuid""#);
    load_app(gains.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    // Nor is such a version bridged by an identity step.
    let bridged = file
        .replace(r#", "body": "string""#, "")
        .replace(r#",
        "migrations": [{ "from": "v1", "migrate": { "notes": { "body": { "add": "string", "default": "" } } } }]"#, "");
    assert_eq!(load_app(bridged.as_bytes()).unwrap().stranded().len(), 1);
}

#[test]
fn a_computed_field_cant_be_indexed() {
    let error = edited_app(|v| v["tables"]["todos"]["indexes"] = serde_json::json!(["shareCount"]));
    assert!(
        error.contains(
            "tables.todos.indexes[0]: `shareCount` is a computed field, which isn't stored"
        ),
        "{error}"
    );
}

#[test]
fn the_schema_hash_is_the_engines_without_policies() {
    let written = write(shared_todos()).unwrap();
    let app = load_app(written.app.as_bytes()).unwrap();
    let json = serde_json::to_vec(app.current_schema()).unwrap();
    assert_eq!(schema_hash(&json).unwrap(), app.current_hash());
    assert!(schema_hash(b"{\"tables\": 1}").is_err());
}

#[test]
fn a_reference_follows_the_table_a_migration_renames() {
    let file = r#"{ "format": 1,
        "tables": {
            "people": { "columns": { "name": "string" } },
            "todos": { "columns": { "owner": { "type": "uuid", "references": "people" } } } },
        "history": { "v1": { "tables": {
            "users": { "columns": { "name": "string" } },
            "todos": { "columns": { "owner": { "type": "uuid", "references": "users" } } } } } },
        "migrations": [{ "from": "v1", "renameTables": { "people": "users" }, "migrate": { "todos": {} } }] }"#;
    let app = load_app(file.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    assert!(app.stranded().is_empty());
}

#[test]
fn an_index_names_a_stored_column() {
    let error = edited_app(|v| v["tables"]["todos"]["indexes"] = serde_json::json!(["nmae"]));
    assert!(
        error.contains(
            "tables.todos.indexes[0]: table todos has no column `nmae`; did you mean `name`?"
        ),
        "{error}"
    );
    edited_app_loads(|v| {
        v["tables"]["todos"]["indexes"] = serde_json::json!(["owner", ["owner", "$createdAt"]])
    });
}

/// What the TS permissions compiler (and the engine behind it) refused, the
/// loader refuses too, naming the spot.
#[test]
fn policies_the_dsl_and_the_engine_refused_are_refused() {
    let refused = |table: &str, source: &str, expected: &str| {
        let error = read_policy(table, source).expect_err(source).to_string();
        assert!(error.contains(expected), "{source}:\n{error}");
        error
    };
    // `maxDepth` on a reverse relation was dropped by the engine; the TS
    // DSL refused it. Both spellings, pointing at the bound.
    for source in [
        "allowedTo.read(shares, maxDepth: 3)",
        "allowedTo.read(via shares.todo, maxDepth: 3)",
    ] {
        let error = refused(
            "todos",
            source,
            "`allowedTo` along a reverse relation takes no `maxDepth`; omit it",
        );
        let caret = error
            .lines()
            .find(|l| l.trim_start().starts_with('^'))
            .unwrap();
        assert_eq!(caret.trim(), "^", "{error}");
        assert_eq!(
            caret.find('^'),
            error
                .lines()
                .find(|l| l.contains(source))
                .unwrap()
                .find('3')
        );
    }
    read_policy("todos", "allowedTo.read(shares)").unwrap();
    read_policy("shares", "allowedTo.read(todoRow, maxDepth: 3)").unwrap();
    // A bad `maxDepth`.
    refused(
        "shares",
        "allowedTo.read(todoRow, maxDepth: -1)",
        "`maxDepth` takes a whole number",
    );
    refused(
        "shares",
        "allowedTo.read(todoRow, maxDepth: 1.5)",
        "`maxDepth` takes a whole number",
    );
    refused(
        "shares",
        "allowedTo.read(todoRow, maxDepth: x)",
        "expected `maxDepth: <number>`",
    );
    refused(
        "todos",
        "exists (from recursive(seed: from todos where todos.owner = session.user, step: from todos where todos.id = id(frontier) select todos.id as id, frontier: id(row), dedupe: (id(row)), maxDepth: 65) where todos.id = outer.id)",
        "`maxDepth` 65 exceeds the cap of 64 for a recursive relation",
    );
    // Inheriting through a column that references no table.
    refused(
        "shares",
        "allowedTo.read(via user)",
        "INHERITS via_column 'user' has no FK reference",
    );
    // Session values compare for equality only.
    for op in [">", "<", ">=", "<="] {
        refused(
            "todos",
            &format!("session.claims.level {op} 3"),
            "core schema policies only support Eq/Ne SessionCmp operators yet",
        );
    }
    // Inheritance inside `exists`: see
    // `allowed_to_inside_exists_is_refused_by_the_reader`.
    // Non-finite floats.
    for literal in ["'NaN'", "'inf'", "'-inf'"] {
        refused(
            "todos",
            &format!("session.claims.x = double({literal})"),
            "floating-point literals must be finite numbers",
        );
    }
    // The writer refuses an engine policy the file can't say rather than
    // drop its bound.
    let mut input = shared_todos();
    input.permissions.insert(
        TableName::from("todos"),
        TablePolicies {
            select: OperationPolicy::using(PolicyExpr::InheritsReferencing {
                operation: Operation::Select,
                source_table: "shares".into(),
                via_column: "todo".into(),
                max_depth: Some(3),
            }),
            ..Default::default()
        },
    );
    let error = write(input).unwrap_err();
    assert!(error.is_unsupported(), "{error}");
    assert!(error.to_string().contains("takes no `maxDepth`"), "{error}");
}

/// `allowedTo` inside an `exists` (its conditions, a hop's, a relation's
/// predicate) is refused where the file is read, naming the rule and
/// pointing at the `allowedTo`, rather than by the engine when the schema
/// compiles: the engine checks inheritance only on the row a rule is for.
#[test]
fn allowed_to_inside_exists_is_refused_by_the_reader() {
    let message = "`allowedTo.read(…)` inside `exists` is not supported yet: repeat the \
                   other table's read rule there instead (e.g. share it as a helper function)";
    for (source, pointed) in [
        (
            "exists shares(todo = this.id, allowedTo.read(todoRow))",
            "allowedTo.read(todoRow)",
        ),
        (
            "exists shares(todo = this.id, anyOf(user = session.user, not allowedTo.read(via todo)))",
            "allowedTo.read(via todo)",
        ),
        (
            "exists shares(user = session.user).todoRow(allowedTo.read(shares))",
            "allowedTo.read(shares)",
        ),
        (
            "exists (from shares where allowedTo.read(todoRow))",
            "allowedTo.read(todoRow)",
        ),
    ] {
        let error = read_policy("todos", source).expect_err(source);
        assert!(
            error.starts_with("jazz.permissions.json: tables.todos.read: "),
            "{source}:\n{error}"
        );
        assert!(error.contains(message), "{source}:\n{error}");
        // The caret spans the whole `allowedTo.…(…)`.
        let lines: Vec<&str> = error.lines().collect();
        let at = lines.iter().position(|l| l.trim() == source).unwrap();
        let caret = lines[at + 1];
        let start = lines[at].find(pointed).unwrap();
        assert_eq!(caret.find('^'), Some(start), "{error}");
        assert_eq!(caret.trim().len(), pointed.len(), "{error}");
    }
    let error = read_policy(
        "todos",
        "exists shares(todo = this.id, allowedTo.delete(todoRow))",
    )
    .unwrap_err();
    assert!(
        error.contains("`allowedTo.delete(…)` inside `exists` is not supported yet"),
        "{error}"
    );
    // At a rule's top it is what inheritance is for.
    read_policy("shares", "allowedTo.read(todoRow)").unwrap();
}

/// `not allowedTo…` (directly or through `anyOf` / `allOf` / `not`) is
/// refused where the file is read, naming the rule and pointing at the
/// `allowedTo`: the engine checks inheritance only as a grant.
#[test]
fn negated_allowed_to_is_refused_by_the_reader() {
    let message = "`not allowedTo.read(…)` is not supported yet: write the rule without \
                   negating inheritance (`not` works on this row's columns and session values)";
    for source in [
        "not allowedTo.read(todoRow)",
        "anyOf(user = session.user, not allowedTo.read(todoRow))",
        "not anyOf(user = session.user, allOf(user != null, allowedTo.read(todoRow)))",
        "not not allowedTo.read(via todo)",
    ] {
        let error = read_policy("shares", source).expect_err(source);
        assert!(
            error.starts_with("jazz.permissions.json: tables.shares.read: "),
            "{source}:\n{error}"
        );
        assert!(error.contains(message), "{source}:\n{error}");
        let lines: Vec<&str> = error.lines().collect();
        let at = lines.iter().position(|l| l.trim() == source).unwrap();
        let caret = lines[at + 1];
        let start = lines[at].find("allowedTo").unwrap();
        assert_eq!(caret.find('^'), Some(start), "{error}");
        assert_eq!(
            caret.trim().len(),
            lines[at][start..].find(')').unwrap() + 1,
            "{error}"
        );
    }
    // An update, both forms.
    let written = write(shared_todos()).unwrap();
    for (update, rule) in [
        (
            serde_json::json!("not allowedTo.update(todoRow)"),
            "tables.shares.update",
        ),
        (
            serde_json::json!({ "old": "true", "new": "anyOf(user = session.user, not allowedTo.update(todoRow))" }),
            "tables.shares.update.new",
        ),
    ] {
        let file = serde_json::json!({
            "format": 1,
            "tables": { "shares": { "update": update }, "todos": { "update": "true" } },
        });
        let error = load(written.app.as_bytes(), file.to_string().as_bytes())
            .unwrap_err()
            .to_string();
        assert!(
            error.starts_with(&format!("jazz.permissions.json: {rule}: ")),
            "{error}"
        );
        assert!(
            error.contains("`not allowedTo.update(…)` is not supported yet"),
            "{error}"
        );
    }
    // `not` on the row's own columns and session values still compiles.
    read_policy("shares", "not user = session.user").unwrap();
    read_policy(
        "shares",
        "anyOf(not user = session.user, allowedTo.read(todoRow))",
    )
    .unwrap();
}

/// A policy the engine refuses when the schema compiles is named by the
/// rule it is in (table and operation, as the file names them), not by the
/// table an `exists` in it reads.
#[test]
fn an_engine_refusal_names_the_rule_it_is_in() {
    let written = write(shared_todos()).unwrap();
    let load_with = |tables: serde_json::Value| {
        let file = serde_json::json!({ "format": 1, "tables": tables });
        load(written.app.as_bytes(), file.to_string().as_bytes())
            .unwrap_err()
            .to_string()
    };
    let refusal = "core schema policies only support Eq/Ne SessionCmp operators yet";
    let inside = "exists shares(todo = this.id, session.claims.level > 3)";
    for (tables, rule) in [
        (
            serde_json::json!({ "todos": { "read": inside } }),
            "todos.read",
        ),
        (
            serde_json::json!({ "todos": { "update": { "old": "true", "new": inside } } }),
            "todos.update.new",
        ),
        (
            serde_json::json!({ "todos": { "insert": inside } }),
            "todos.insert",
        ),
    ] {
        let error = load_with(tables);
        assert_eq!(
            error,
            format!("jazz.permissions.json: tables.{rule}: {refusal}"),
        );
    }
}

/// The relation checks `load` runs on the current tables.
#[test]
fn relations_must_lead_where_they_say() {
    let error =
        edited_app(|v| v["tables"]["todos"]["relations"]["shares"]["reverse"] = "sharez".into());
    assert!(
        error.contains("tables.todos.relations.shares: the current schema has no table sharez; did you mean `shares`?"),
        "{error}"
    );
    let error =
        edited_app(|v| v["tables"]["todos"]["relations"]["shares"]["relation"] = "todoRw".into());
    assert!(
        error.contains(
            "tables.todos.relations.shares: shares has no forward relation todoRw to todos"
        ),
        "{error}"
    );
    // A reverse relation whose forward one leads to another table.
    let error = edited_app(|v| {
        v["tables"]["shares"]["relations"]["owner"] =
            serde_json::json!({ "rel": "shares", "column": "user" });
        v["tables"]["todos"]["relations"]["shares"]["relation"] = "owner".into();
    });
    assert!(
        error.contains(
            "tables.todos.relations.shares: shares has no forward relation owner to todos"
        ),
        "{error}"
    );
    let error =
        edited_app(|v| v["tables"]["shares"]["relations"]["todoRow"]["rel"] = "todoz".into());
    assert!(error.contains("todoz"), "{error}");
}

#[test]
fn a_relation_leads_where_its_column_references() {
    // `shares.todo` references todos; a relation over it can't lead elsewhere.
    let error = edited_app(|v| {
        v["tables"]["shares"]["columns"]["todo"] =
            serde_json::json!({ "type": "uuid?", "references": "todos" });
        v["tables"]["shares"]["relations"]["todoRow"]["rel"] = "shares".into();
    });
    assert!(
        error.contains(
            "tables.shares.relations.todoRow: column todo references todos, so a relation over it leads to todos, not shares"
        ),
        "{error}"
    );
}

#[test]
fn a_policy_reading_a_computed_field_is_told_it_is_computed() {
    let error = edited_permissions(|v| {
        v["tables"]["todos"]["read"] = "shareCount = 1".into();
    });
    assert!(
        error.contains(
            "unknown column `shareCount` in todos: `shareCount` is a computed field, which isn't \
             stored, so a permission can't read it; write the policy over the columns it's \
             computed from"
        ),
        "{error}"
    );
    // A plain unknown column keeps the nearest-name hint.
    let error = edited_permissions(|v| {
        v["tables"]["todos"]["read"] = "nmae = 'x'".into();
    });
    assert!(
        error.contains("unknown column `nmae` in todos; did you mean `name`?"),
        "{error}"
    );
}

#[test]
fn a_relation_policy_reading_a_computed_outer_field_is_told_it_is_computed() {
    let error = edited_permissions(|v| {
        v["tables"]["todos"]["read"] = "exists (from shares where todo = outer.shareCount)".into();
    });
    assert!(
        error.contains(
            "unknown column todos.shareCount: `shareCount` is a computed field, which isn't \
             stored, so a permission can't read it; write the policy over the columns it's \
             computed from"
        ),
        "{error}"
    );
}

/// An enum whose cases carry fields (accepted until 2.0.0-alpha.66) loads
/// in an earlier version of `history` and in a migration dropping it, so
/// such an app can migrate away; the current tables may not declare one.
#[test]
fn enum_cases_with_fields_load_in_history_and_are_refused_in_the_current_tables() {
    let status = r#"{ "enum": { "open": {}, "done": { "at": "timestamp" } }, "optional": true }"#;
    let app = format!(
        r#"{{
  "format": 1,
  "tables": {{ "todos": {{ "columns": {{ "title": "string" }} }} }},
  "history": {{
    "v1": {{ "tables": {{ "todos": {{ "columns": {{ "title": "string", "status": {status} }} }} }} }}
  }},
  "migrations": [
    {{ "from": "v1", "migrate": {{ "todos": {{ "status": {{ "drop": {{ "enum": {{ "open": {{}}, "done": {{ "at": "timestamp" }} }} }} }} }} }} }}
  ]
}}"#
    );
    let loaded = load(app.as_bytes(), br#"{ "format": 1 }"#).unwrap_or_else(|e| panic!("{e}"));
    assert!(!loaded.deployment_body(false).is_null());

    let current = format!(
        r#"{{ "format": 1, "tables": {{ "todos": {{ "columns": {{ "title": "string", "status": {status} }} }} }} }}"#
    );
    let error = load(current.as_bytes(), br#"{ "format": 1 }"#)
        .unwrap_err()
        .to_string();
    assert!(error.contains("tables.todos.columns.status"), "{error}");
    assert!(
        error.contains("enum cases with fields are not supported"),
        "{error}"
    );
}

/// The policy corpus the TS printer's parity test reads
/// (`packages/jazz-tools/src/printer/parity.test.ts`): shared-todos' app and,
/// per case, a permissions file holding one policy as `table`'s read, as the
/// writer prints it. `JAZZ_UPDATE_FIXTURES=1` rewrites it.
#[test]
fn the_policy_corpus_matches_its_fixture() {
    let written = write(shared_todos()).unwrap();
    let current = loaded(shared_todos()).current_schema().clone();
    let relations = shared_todos().codegen.relations();
    let mut cases = Vec::new();
    let file_for = |table: &str, read: serde_json::Value| {
        let other = if table == "shares" { "todos" } else { "shares" };
        serde_json::json!({
            "format": 1,
            "tables": { table: { "read": read }, other: { "read": "true", "update": "true" } },
        })
    };
    // Magic columns in both spellings: each is its own DSL key.
    const MAGIC: &[&str] = &[
        "$createdBy = session.user",
        "$createdBy.identity = session.identity",
        "$updatedBy = session.user",
        "$updatedBy.account = session.user",
        "$updatedBy.identity = session.identity",
        "$deletedBy = session.user",
        "$deletedBy.account = session.user",
        "$deletedBy.identity = session.identity",
    ];
    let magic: Vec<(&str, &str)> = MAGIC.iter().map(|&source| ("todos", source)).collect();
    for &(table, source) in CONSTRUCTS.iter().chain(magic.iter()) {
        cases.push(
            serde_json::json!({ "name": source, "permissions": file_for(table, source.into()) }),
        );
    }
    // `updateTo`'s `old`: one condition object compares several columns with
    // the old row's, in one self-`exists`.
    cases.push(serde_json::json!({
        "name": "update.new comparing several columns with the old row",
        "permissions": {
            "tables": {
                "todos": {
                    "read": "true",
                    "update": {
                        "old": "owner = session.user",
                        "new": "exists todos(id = this.id, name = this.name, owner = this.owner)",
                    },
                },
                "shares": { "read": "true", "update": "true" },
            },
        },
    }));
    let variant = every_policy_variant();
    let mut exprs = vec![("every policy variant".to_string(), variant.clone())];
    if let PolicyExpr::And(items) | PolicyExpr::Or(items) = &variant {
        for (i, item) in items.iter().enumerate() {
            exprs.push((format!("every policy variant, item {i}"), item.clone()));
        }
    }
    for (name, expr) in exprs {
        let mut permissions = crate::catalogue::Permissions::new();
        permissions.insert(
            TableName::from("todos"),
            TablePolicies {
                select: OperationPolicy::using(expr),
                ..Default::default()
            },
        );
        permissions.insert(
            TableName::from("shares"),
            TablePolicies {
                select: OperationPolicy::using(PolicyExpr::True),
                update: OperationPolicy::using_and_check(PolicyExpr::True, PolicyExpr::True),
                ..Default::default()
            },
        );
        let file = permissions_file::write(&permissions, &current, &relations).unwrap();
        let text = json::print(&file);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        cases.push(serde_json::json!({ "name": name, "permissions": value }));
    }
    let corpus = serde_json::json!({ "app": written.app, "cases": cases });
    let text = format!("{}\n", serde_json::to_string_pretty(&corpus).unwrap());
    let path = format!("{FIXTURES}/policy-corpus.json");
    if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
        std::fs::write(&path, &text).unwrap();
    }
    let committed = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("{path} is missing; run with JAZZ_UPDATE_FIXTURES=1"));
    assert_eq!(
        text, committed,
        "policy-corpus.json differs from its fixture"
    );
}

// ---------------------------------------------------------------------------
// `$generated`
// ---------------------------------------------------------------------------

#[test]
fn generated_files_carry_a_marker_after_schema_that_loaders_ignore() {
    let plain = write(shared_todos()).unwrap();
    let marked = write_generated(shared_todos(), GENERATED_BY_TS).unwrap();
    for (plain, marked) in [
        (&plain.app, &marked.app),
        (&plain.permissions, &marked.permissions),
    ] {
        let root = json::parse(marked.as_bytes()).unwrap();
        let keys: Vec<&str> = root
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, _)| k.as_str())
            .take(2)
            .collect();
        assert_eq!(keys, ["$schema", GENERATED_KEY]);
        let digest = {
            use sha2::{Digest, Sha256};
            hex::encode(Sha256::digest(plain.as_bytes()))
        };
        assert_eq!(
            root.get(GENERATED_KEY),
            Some(&Json::object(vec![
                ("by", Json::string(GENERATED_BY_TS)),
                ("sha256", Json::string(digest.clone())),
            ]))
        );
        assert_eq!(
            verify_generated(marked.as_bytes()).unwrap(),
            GeneratedCheck::Matches(GeneratedMarker {
                by: GENERATED_BY_TS.into(),
                sha256: digest,
            })
        );
        assert_eq!(
            verify_generated(plain.as_bytes()).unwrap(),
            GeneratedCheck::Unmarked
        );
        // Marking again changes nothing.
        assert_eq!(
            &mark_generated(marked.as_bytes(), GENERATED_BY_TS).unwrap(),
            marked
        );
    }
    let loaded_plain = load(plain.app.as_bytes(), plain.permissions.as_bytes()).unwrap();
    let loaded_marked = load(marked.app.as_bytes(), marked.permissions.as_bytes()).unwrap();
    assert_eq!(loaded_plain.bundle(), loaded_marked.bundle());
    assert_eq!(loaded_plain.current_hash(), loaded_marked.current_hash());
    load_client(marked.app.as_bytes()).unwrap();
    for (text, schema) in [
        (&marked.app, "jazz.app.v1.json"),
        (&marked.permissions, "jazz.permissions.v1.json"),
        (&marked.app, "jazz.app.schema.json"),
        (&marked.permissions, "jazz.permissions.schema.json"),
    ] {
        let validator = jsonschema::validator_for(&json_schema(schema)).unwrap();
        let instance: serde_json::Value = serde_json::from_str(text).unwrap();
        if schema.ends_with("v1.json") {
            let errors: Vec<String> = validator
                .iter_errors(&instance)
                .map(|e| e.to_string())
                .collect();
            assert!(errors.is_empty(), "{schema}: {errors:?}");
        } else {
            // The unversioned schemas describe `$generated` too.
            let marker = serde_json::json!({ "$generated": instance[GENERATED_KEY].clone() });
            let generated = &json_schema(schema)["$defs"]["generated"];
            let validator = jsonschema::validator_for(generated).unwrap();
            assert!(validator.is_valid(&marker[GENERATED_KEY]), "{schema}");
        }
    }
}

#[test]
fn an_edited_generated_file_mismatches_but_reformatting_does_not() {
    let marked = write_generated(shared_todos(), GENERATED_BY_TS).unwrap();
    let reformatted = marked.app.replace('\n', "\r\n").replace("  ", "    ");
    assert!(matches!(
        verify_generated(reformatted.as_bytes()).unwrap(),
        GeneratedCheck::Matches(_)
    ));
    let mut edited = serde_json::from_str::<serde_json::Value>(&marked.permissions).unwrap();
    edited["tables"]["todos"]["read"] = "true".into();
    let edited = serde_json::to_string(&edited).unwrap();
    let GeneratedCheck::Mismatch { marker, actual } = verify_generated(edited.as_bytes()).unwrap()
    else {
        panic!("an edit should mismatch");
    };
    assert_ne!(marker.sha256, actual);
}

#[test]
fn a_malformed_marker_is_refused() {
    let written = write(shared_todos()).unwrap();
    for marker in [
        serde_json::json!("x"),
        serde_json::json!({ "by": "me" }),
        serde_json::json!({ "by": "me", "sha256": "ABC" }),
        serde_json::json!({ "by": 1, "sha256": "0".repeat(64) }),
        serde_json::json!({ "by": "me", "sha256": "0".repeat(64), "at": "now" }),
    ] {
        let mut app = serde_json::from_str::<serde_json::Value>(&written.app).unwrap();
        app[GENERATED_KEY] = marker.clone();
        let app = app.to_string();
        let error = load(app.as_bytes(), written.permissions.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(error.contains("$generated"), "{marker}: {error}");
        assert!(verify_generated(app.as_bytes()).is_err(), "{marker}");
    }
}

// ---------------------------------------------------------------------------
// Rewriting readable files
// ---------------------------------------------------------------------------

/// Readable files as an author (or the TS tooling) might write them: loose
/// formatting, a migration name, comments, and policies spelled the long
/// way round.
fn readable_files() -> (String, String) {
    let written = write(shared_todos()).unwrap();
    let mut app: serde_json::Value = serde_json::from_str(&written.app).unwrap();
    app["migrations"][0]
        .as_object_mut()
        .unwrap()
        .insert("name".into(), "rename title".into());
    let permissions = serde_json::json!({
        "format": 1,
        "$comment": "hand-written",
        "tables": {
            "todos": {
                "read": { "anyOf": [
                    "session.user = owner",
                    { "allOf": ["name in ['a', 'b']", "tags contains 'x'"] },
                    "exists shares(todo = this.id, user = session.user)"
                ] },
                "insert": "owner = session.user and session.claims.role in ['admin', 'editor']",
                "update": { "old": "owner = session.user", "new": "((owner = session.user))" },
                "delete": { "allOf": ["owner = session.user", "not owner = null"] }
            },
            "shares": { "read": "allowedTo.read(todoRow)", "update": "true" }
        }
    });
    (
        serde_json::to_string(&app).unwrap(),
        serde_json::to_string(&permissions).unwrap(),
    )
}

#[test]
fn rewriting_readable_files_gives_the_canonical_text() {
    let (app, permissions) = readable_files();
    let rewritten = rewrite(app.as_bytes(), permissions.as_bytes(), None).unwrap();
    assert_eq!(
        rewritten.permissions,
        r#"{
  "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
  "tables": {
    "shares": { "read": "allowedTo.read(todoRow)", "update": "true" },
    "todos": {
      "read": {
        "anyOf": [
          "owner = session.user",
          { "allOf": ["name in ['a', 'b']", "tags contains 'x'"] },
          "exists shares(todo = this.id, user = session.user)"
        ]
      },
      "insert": { "allOf": ["owner = session.user", "session.claims.role in ['admin', 'editor']"] },
      "update": "owner = session.user",
      "delete": { "allOf": ["owner = session.user", "not owner = null"] }
    }
  }
}
"#
    );
    let root = json::parse(rewritten.app.as_bytes()).unwrap();
    assert_eq!(
        root.get("migrations")
            .and_then(|m| match m {
                Json::Array(items) => items.first(),
                _ => None,
            })
            .and_then(|m| m.as_object())
            .map(|m| m[0].clone()),
        Some(("name".to_string(), Json::string("rename title")))
    );
    // The same app as the files given.
    let given = load(app.as_bytes(), permissions.as_bytes()).unwrap();
    let again = load(rewritten.app.as_bytes(), rewritten.permissions.as_bytes()).unwrap();
    assert_eq!(given.bundle(), again.bundle());
    assert_eq!(given.permissions(), again.permissions());
}

#[test]
fn rewriting_is_idempotent_with_and_without_the_marker() {
    let (app, permissions) = readable_files();
    for by in [None, Some(GENERATED_BY_TS)] {
        let once = rewrite(app.as_bytes(), permissions.as_bytes(), by).unwrap();
        let twice = rewrite(once.app.as_bytes(), once.permissions.as_bytes(), by).unwrap();
        assert_eq!(once, twice, "{by:?}");
        if by.is_some() {
            for text in [&once.app, &once.permissions] {
                assert!(matches!(
                    verify_generated(text.as_bytes()).unwrap(),
                    GeneratedCheck::Matches(_)
                ));
            }
        }
    }
    // The writer's own output is already canonical.
    let written = write(shared_todos()).unwrap();
    assert_eq!(
        rewrite(written.app.as_bytes(), written.permissions.as_bytes(), None).unwrap(),
        written
    );
}

#[test]
fn rewriting_refuses_what_load_refuses() {
    let (app, _) = readable_files();
    let error = rewrite(
        app.as_bytes(),
        br#"{ "format": 1, "tables": { "todos": { "read": "ownr = session.user" } } }"#,
        None,
    )
    .unwrap_err();
    assert_eq!(error.file(), PERMISSIONS_FILE);
    assert!(error.message().contains("did you mean `owner`"), "{error}");
}

/// Device accounts were `"local-first"` in `session.authMode` before the
/// rename. A file still comparing with it is refused where the literal is,
/// naming the new value: kept, `session.authMode != 'local-first'` would let
/// every device account in.
#[test]
fn the_removed_local_first_auth_mode_is_refused_by_name() {
    for source in [
        "session.authMode = 'local-first'",
        "session.authMode != 'local-first'",
        "session.authMode in ['external', 'local-first']",
        "not (session.authMode = 'local-first')",
        "owner = session.user and session.authMode != 'local-first'",
    ] {
        let error = read_policy("todos", source).unwrap_err();
        assert!(
            error.starts_with("jazz.permissions.json: tables.todos.read:"),
            "{source}: {error}"
        );
        assert!(
            error.contains("never \"local-first\" any more")
                && error.contains("compare with \"device-account\""),
            "{source}: {error}"
        );
    }
    assert_eq!(
        read_policy("todos", "session.authMode = 'device-account'").unwrap(),
        PolicyExpr::SessionCmp {
            path: vec!["authMode".into()],
            op: crate::tools::CmpOp::Eq,
            value: Value::Text("device-account".into()),
        }
    );
    // A claim may still hold the text: only `session.authMode` changed.
    read_policy("todos", "session.claims.mode = 'local-first'").unwrap();
}

/// An enum whose cases carry fields may stay in a `history` version, plain
/// or encrypted, so that a migration can drop it: the JSON Schema takes
/// such a file as the reader does, and refuses the same enum in the current
/// tables as the reader does.
#[test]
fn the_json_schema_takes_enum_cases_with_fields_in_history_only() {
    let validator = jsonschema::validator_for(&json_schema("jazz.app.v1.json")).unwrap();
    let permissions = br#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let encrypted = r#"{"$schema":"https://jazz.tools/schemas/jazz.app.v1.json",
        "tables":{"notes":{"columns":{"summary":"bytes"},"encryption":{"space":"id","scope":"notes","columns":{"summary":"string?"}}}},
        "history":{"old":{"tables":{"notes":{"columns":{"detail":"bytes"},"encryption":{"space":"id","scope":"notes","columns":{"detail":{"enum":{"pending":{"reason":"string"}}}}}}}}},
        "migrations":[{"from":"old","migrate":{"notes":{"detail":{"drop":"bytes","backwardsDefault":""},"summary":{"add":"bytes","default":""}}}}]}"#;
    let plain = r#"{"$schema":"https://jazz.tools/schemas/jazz.app.v1.json",
        "tables":{"todos":{"columns":{"title":"string"}}},
        "history":{"v1":{"tables":{"todos":{"columns":{"title":"string","status":{"enum":{"open":{},"done":{"at":"timestamp"}},"optional":true}}}}}},
        "migrations":[{"from":"v1","migrate":{"todos":{"status":{"drop":{"enum":{"open":{},"done":{"at":"timestamp"}}}}}}}]}"#;
    let plain_permissions =
        br#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": {} }"#;
    let current = r#"{"$schema":"https://jazz.tools/schemas/jazz.app.v1.json",
        "tables":{"todos":{"columns":{"title":"string","status":{"enum":{"open":{},"done":{"at":"timestamp"}},"optional":true}}}}}"#;
    // A case's field takes what a column's type takes (a type string with
    // `?`, or a type object), not an empty object.
    let field = |spec: &str| {
        plain.replace(
            r#""done":{"at":"timestamp"}"#,
            &format!(r#""done":{{"at":{spec}}}"#),
        )
    };
    let (optional_field, empty_field) = (field(r#"{"type":"string?"}"#), field("{}"));
    for (text, permissions, expected) in [
        (encrypted, &permissions[..], true),
        (plain, &plain_permissions[..], true),
        (current, &plain_permissions[..], false),
        (optional_field.as_str(), &plain_permissions[..], true),
        (empty_field.as_str(), &plain_permissions[..], false),
    ] {
        let instance: serde_json::Value = serde_json::from_str(text).unwrap();
        let by_schema = validator.is_valid(&instance);
        let by_reader = load(text.as_bytes(), permissions);
        assert_eq!(
            (by_schema, by_reader.is_ok()),
            (expected, expected),
            "{text}: schema {by_schema}, reader {:?}",
            by_reader.err().map(|e| e.to_string())
        );
    }
}

/// The app file's JSON Schema takes an encrypted column's declaration
/// exactly when the reader does (`load`, what `loadAppArtifact` calls): each
/// declaration below sits in an otherwise valid encrypted app, and the 2020-12
/// validator and the reader must both accept it or both refuse it.
#[test]
fn the_json_schema_takes_exactly_the_encrypted_columns_the_reader_takes() {
    let validator = jsonschema::validator_for(&json_schema("jazz.app.v1.json")).unwrap();
    let permissions = br#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let app = |declaration: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {{ "columns": {{ "title": "bytes" }},
                "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": {declaration} }} }} }} }} }}"#
        )
    };
    let accepted = [
        r#""string""#,
        r#""string?""#,
        r#"{ "type": "string" }"#,
        r#"{ "type": "string", "default": "untitled" }"#,
        r#"{ "type": "string", "optional": true }"#,
        r#"{ "enum": ["low", "high"] }"#,
        r#"{ "enum": ["low", "high"], "default": "low" }"#,
        r#"{ "json": true }"#,
        r#"{ "array": "string" }"#,
        r#"{ "array": { "enum": ["low", "high"] } }"#,
        r#"{ "type": { "enum": ["low", "high"] } }"#,
        r#"{ "type": { "enum": ["low", "high"] }, "default": "low" }"#,
        r#"{ "type": { "enum": ["low", "high", "top"] }, "sealedType": { "enum": ["low", "high"] } }"#,
        r#"{ "type": "string", "$comment": "a note" }"#,
        r#"{ "array": { "type": "string", "$comment": "a note" } }"#,
        r#"{ "type": { "array": { "enum": ["low"], "$comment": "a note" } } }"#,
        r#"{ "json": { "type": "integer", "minimum": 1, "maximum": 10 } }"#,
        r#"{ "array": { "json": { "enum": [1, "two", null, true] } } }"#,
        r#"{ "json": { "minimum": -9223372036854775808, "maximum": 18446744073709551615 } }"#,
    ];
    let refused = [
        r#"42"#,
        r#"{}"#,
        r#""strng""#,
        r#"{ "type": "string", "from": "etc" }"#,
        r#"{ "type": "string", "anything": 1 }"#,
        r#"{ "type": "string", "enum": ["low"] }"#,
        r#"{ "type": "string", "merge": "counter" }"#,
        r#"{ "type": "string", "references": "notes" }"#,
        r#"{ "type": "string", "transform": "upper" }"#,
        r#"{ "sealedType": "string" }"#,
        r#"{ "type": "string", "sealedType": {} }"#,
        r#"{ "type": "string", "sealedType": { "enum": ["low"], "anything": 1 } }"#,
        r#"{ "type": "string", "sealedType": { "type": "string", "enum": ["low"] } }"#,
        r#"{ "type": { "enum": ["low"] }, "optional": true }"#,
        r#"{ "type": { "enum": ["low"], "default": "low" }, "sealedType": { "enum": ["low"] } }"#,
        r#"{ "type": { "enum": ["low"] }, "sealedType": { "enum": ["low"] }, "from": "etc" }"#,
        r#"{ "array": { "type": "string", "from": "etc" } }"#,
        r#"{ "type": { "array": { "type": "string", "reverse": "etc" } } }"#,
        r#"{ "type": { "enum": ["low"], "from": "etc" } }"#,
        r#"{ "array": { "type": "string", "$comment": 1 } }"#,
        // The current version may not hold what a client can't seal.
        r#"{ "json": { "minimum": 0.5 } }"#,
        r#"{ "type": { "array": { "json": { "multipleOf": 0.25 } } } }"#,
        r#"{ "enum": { "pending": { "reason": "string" } } }"#,
        r#"{ "array": { "enum": { "pending": { "reason": "string" } } } }"#,
        // Just past the largest unsigned 64-bit integer: this validator
        // compares it exactly (a JavaScript editor reads it as the nearest
        // double, which is the bound itself).
        r#"{ "json": { "maximum": 18446744073709551616 } }"#,
        r#"{ "json": { "maximum": 100000000000000000000000000000 } }"#,
        r#"{ "json": { "minimum": -100000000000000000000000000000 } }"#,
    ];
    // The known differences, where the JSON Schema takes what the reader
    // refuses:
    // - JSON Schema compares numbers by value, so it takes `1.0` (or `1e2`)
    //   as an integer; the reader goes by how the number is written and
    //   refuses it, as a client could not encode it.
    // - A number just below the smallest signed 64-bit integer is read as the
    //   nearest double, which is that bound, so it passes the JSON Schema's
    //   `minimum`; the reader refuses it.
    let schema_only = [
        r#"{ "json": { "minimum": 1.0 } }"#,
        r#"{ "json": { "minimum": -9223372036854775809 } }"#,
    ];
    for declaration in schema_only {
        let text = app(declaration);
        let instance: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert!(validator.is_valid(&instance), "{declaration}");
        assert!(load(text.as_bytes(), permissions).is_err(), "{declaration}");
    }
    for (declarations, expected) in [(&accepted[..], true), (&refused[..], false)] {
        for declaration in declarations {
            let text = app(declaration);
            let instance: serde_json::Value = serde_json::from_str(&text).unwrap();
            let by_schema = validator.is_valid(&instance);
            let by_reader = load(text.as_bytes(), permissions);
            assert_eq!(
                (by_schema, by_reader.is_ok()),
                (expected, expected),
                "{declaration}: schema {by_schema}, reader {:?}",
                by_reader.err().map(|e| e.to_string())
            );
        }
    }
}

/// A space table missing from a version is refused in words that fit
/// whose entries name it: the version's own (a backstop behind its entry
/// checks) or the nearest earlier ones it follows.
#[test]
fn a_missing_space_table_is_worded_for_own_or_inherited_entries() {
    let schema = SchemaBuilder::new().build();
    let entries: encryption::Entries = [(
        "messages".to_owned(),
        crate::tools::public_schema::EncryptionEntry {
            space: "roomId".to_owned(),
            scope: "rooms".to_owned(),
            columns: Default::default(),
            equality: Vec::new(),
        },
    )]
    .into();
    let own = encryption::check_space_tables(&schema, &entries, false).unwrap_err();
    assert_eq!(
        own,
        "space table rooms is not in this version: its encryption entries name rooms as a \
         space table. Keep rooms, or declare its entries with a space table this version has."
    );
    let inherited = encryption::check_space_tables(&schema, &entries, true).unwrap_err();
    assert!(
        inherited.starts_with(
            "space table rooms is not in this version (dropped or renamed). This version \
             declares no encryption entries"
        ),
        "{inherited}"
    );
    assert!(encryption::check_space_tables(&schema, &encryption::Entries::new(), false).is_ok());
}

/// A retyped encrypted column is refused naming both types as the app
/// file writes them, the sealed one first, never their Rust debug form.
#[test]
fn a_retyped_encrypted_column_names_both_types_as_the_file_writes_them() {
    use crate::tools::public_schema::{EncryptedColumn, PlainType};
    let column = |column_type, nullable| EncryptedColumn {
        plain: PlainType {
            column_type,
            nullable,
            default: None,
        },
        sealed_type: None,
    };
    let refusal = |from: &EncryptedColumn, to: &EncryptedColumn| {
        encryption::check_stored_column_type("messages.text", from, to, false).unwrap_err()
    };
    assert_eq!(
        refusal(
            &column(ColumnType::Text, false),
            &column(ColumnType::Integer, false)
        ),
        "messages.text: the encrypted column's type changes from \"string\", which its cells \
         were sealed under, to \"int\"; only append-only enum widening can pin the old type \
         with sealedType; to encrypt existing data, copy it into a new encrypted table \
         (docs/e2ee/encrypting-existing-data)"
    );
    let retyped = refusal(
        &column(ColumnType::Text, true),
        &column(
            ColumnType::Enum {
                variants: vec!["a".to_owned(), "b".to_owned()],
            },
            true,
        ),
    );
    assert!(
        retyped.starts_with(
            "messages.text: the encrypted column's type changes from \"string?\", which its \
             cells were sealed under, to { \"enum\": [\"a\", \"b\"] } (nullable);"
        ),
        "{retyped}"
    );
    assert!(!retyped.contains("PlainType"), "{retyped}");
}
