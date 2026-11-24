use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::deployment::artifact::{load_app, system_tables::MEMBERS};
use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentRequest, prepare_encrypted_deployment,
};
use jazz::tools::{SchemaHash, TableName};
use serde_json::json;

const LEGACY: &[u8] = include_bytes!("../../jazz-codegen/tests/apps/encrypted/jazz.app.json");

fn upgraded_file() -> String {
    use jazz::tools::deployment::artifact::{declared_history_version, json};
    let old = load_app(LEGACY).unwrap();
    let entries = old
        .encryption()
        .entries(&old.current_hash().to_string())
        .unwrap();
    let history = declared_history_version(old.current_schema(), entries).unwrap();
    let json::Json::Object(mut fields) = json::parse(LEGACY).unwrap() else {
        unreachable!()
    };
    fields.extend([
        ("systemTablesVersion".into(), json::Json::Number("2".into())),
        (
            "history".into(),
            json::Json::object(vec![("legacy", history)]),
        ),
        (
            "migrations".into(),
            json::parse(br#"[{ "from": "legacy" }]"#).unwrap(),
        ),
    ]);
    json::print(&json::Json::Object(fields))
}

/// Alice's generated v1 bundle re-renders byte for byte with its shipped schema hash.
#[test]
fn the_shipped_encrypted_app_file_round_trips_under_the_new_loader() {
    use jazz::tools::deployment::artifact::{GENERATED_BY_TS, rewrite};
    const APP: &[u8] =
        include_bytes!("../../../examples/band-chat/apps/react-hono-betterauth-e2ee/jazz.app.json");
    const PERMS: &[u8] = include_bytes!(
        "../../../examples/band-chat/apps/react-hono-betterauth-e2ee/jazz.permissions.json"
    );
    let loaded = load_app(APP).unwrap();
    assert_eq!(
        loaded.current_hash().to_string(),
        "15d53d22acfd0ba8e973039c13ef98b5e60cebe5e582bb408cffae1f5275a09a"
    );
    let written = rewrite(APP, PERMS, Some(GENERATED_BY_TS)).unwrap();
    assert_eq!(written.app.as_bytes(), APP);
}

/// Alice's membership row written under the shipped v1 file is still the
/// same row in the v2 view, with a null value for the added optional column.
#[test]
fn alice_reads_a_v1_membership_after_the_system_table_upgrade() {
    let old = load_app(LEGACY).unwrap();
    let new = load_app(upgraded_file().as_bytes()).unwrap();
    assert_eq!(new.schemas()[0].0, old.current_hash());
    let db = block_on(Db::open_history_complete(DbConfig::new(
        JazzSchema::new(old.current_schema()).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let scope = block_on(db.table_identity("projects")).unwrap().unwrap().0;
    let account = uuid::Uuid::from_bytes([2; 16]);
    let project = uuid::Uuid::from_bytes([3; 16]);
    let member = RowUuid::from_bytes([4; 16]);
    block_on(
        db.insert(
            MEMBERS,
            [
                ("scopeId".into(), Value::Uuid(scope)),
                ("identifier".into(), Value::Uuid(project)),
                ("accountId".into(), Value::Uuid(account)),
            ]
            .into(),
            InsertOptions {
                row_id: Some(member),
                ..Default::default()
            },
        ),
    )
    .unwrap();

    let schema = JazzSchema::new(new.current_schema()).unwrap();
    block_on(db.admit_schema_with_lenses(schema.clone(), &new.client_migrations().lenses)).unwrap();
    let view = block_on(db.register_schema_view(schema)).unwrap();
    let query = view.prepare_query(&view.table(MEMBERS)).unwrap();
    let rows = block_on(view.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), member);
    assert_eq!(rows[0].cell("accountId"), Some(Value::Uuid(account)));
    assert_eq!(rows[0].cell("identifier"), Some(Value::Uuid(project)));
    assert_eq!(rows[0].cell("scopeId"), Some(Value::Uuid(scope)));
    assert_eq!(rows[0].cell("label"), Some(Value::Nullable(None)));
}

/// Alice's source migration cannot supply a system step, even one with the
/// same effect as Jazz's upgrade.
#[test]
fn an_authored_system_table_lens_is_refused() {
    let mut file: serde_json::Value = serde_json::from_str(&upgraded_file()).unwrap();
    file["migrations"][0]["migrate"] =
        json!({ "__e2ee_members": { "label": { "add": "string" } } });
    let error = load_app(file.to_string().as_bytes()).unwrap_err();
    assert!(
        error.to_string().contains("may not change __e2ee_members"),
        "{error}"
    );
}

/// Alice cannot ship a migration from a newer system layout back to the old one.
#[test]
fn a_source_migration_cannot_downgrade_the_system_tables() {
    let mut file: serde_json::Value = serde_json::from_str(&upgraded_file()).unwrap();
    file["history"]["legacy"]["systemTablesVersion"] = json!(2);
    file["systemTablesVersion"] = json!(1);
    let error = load_app(file.to_string().as_bytes()).unwrap_err();
    assert!(error.to_string().contains("cannot downgrade"), "{error}");
}

/// Bob's server identifies an unknown system schema from the published
/// tables, even when the submitted hash really matches those tables.
#[test]
fn an_unknown_fragment_requires_a_server_upgrade() {
    let mut schema = load_app(LEGACY).unwrap().current_schema().clone();
    schema
        .get_mut(&TableName::from(MEMBERS))
        .unwrap()
        .columns
        .columns
        .pop();
    let hash = SchemaHash::compute(&schema);
    let error = prepare_encrypted_deployment(
        &DeploymentCatalogue::default(),
        DeploymentRequest {
            schemas: vec![(hash, schema)],
            target_schema_hash: hash,
            migrations: Vec::new(),
            permissions: Default::default(),
            allow_dropping_history: false,
        },
        &Default::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("upgrade the server"), "{error}");
}

/// Alice's database rejects raw system-table lenses that differ from the
/// declared addition, including the same column with another type or default.
#[test]
fn raw_system_table_lenses_require_the_exact_declared_column() {
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    use jazz::tools::public_schema::{ColumnDescriptor, ColumnType};
    let old = load_app(LEGACY).unwrap();
    let source = JazzSchema::new(old.current_schema()).unwrap();
    let add = |name: &str, default| LensOp::AddColumn {
        column: name.into(),
        default,
    };
    let label = || ColumnDescriptor::new("label", ColumnType::Text).nullable();
    let null = Value::Nullable(None);
    let cases = [
        (
            MEMBERS,
            ColumnDescriptor::new("other", ColumnType::Text).nullable(),
            add("other", null.clone()),
        ),
        (
            MEMBERS,
            ColumnDescriptor::new("label", ColumnType::Boolean).nullable(),
            add("label", null.clone()),
        ),
        (
            MEMBERS,
            ColumnDescriptor::new("label", ColumnType::Text),
            add("label", null.clone()),
        ),
        (
            MEMBERS,
            label().default(jazz::tools::Value::Text("x".into())),
            add("label", null.clone()),
        ),
        (MEMBERS, label(), add("label", Value::String("x".into()))),
        ("__e2ee_spaces", label(), add("label", null.clone())),
        (
            MEMBERS,
            label(),
            LensOp::DropColumn {
                column: "accountId".into(),
                backwards_default: null,
            },
        ),
    ];
    for (table, descriptor, op) in cases {
        let mut target = old.current_schema().clone();
        target
            .get_mut(&TableName::from(table))
            .unwrap()
            .columns
            .columns
            .push(descriptor);
        let target = JazzSchema::new(&target).unwrap();
        let db = block_on(Db::open_history_complete(DbConfig::new(
            source.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([1; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let lens = MigrationLens::new(
            source.version_id(),
            target.version_id(),
            vec![TableLens {
                source_table: table.into(),
                target_table: table.into(),
                ops: vec![op.clone()],
            }],
        )
        .unwrap();
        let error = block_on(db.admit_schema_with_lenses(target, &[lens])).unwrap_err();
        assert!(
            error.message.contains("carried between versions unchanged"),
            "{op:?}: {error}"
        );
    }
}
