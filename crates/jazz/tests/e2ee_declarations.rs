//! An app's encryption declarations in its files (E2EE protocol §1.5,
//! migrations §3.1–§3.2): the `encryption` entry of each table and the
//! top-level settings, read and written by the app-file reader and writer,
//! recorded per schema version, returned by the loaders, and checked the way
//! the deploy and `generate` check them.
//!
//! ```text
//! jazz.app.json ──load──► schema + Jazz's system tables ──► one schema hash
//!      │                         │
//!      │                         └─ derived system policies ─► bundle id
//!      └─ encryption entries (outside the hash) ─► client declarations
//! ```

use jazz::tools::AppId;
use jazz::tools::deployment::artifact::system_tables::{MEMBERS, SPACES};
use jazz::tools::deployment::artifact::{
    ArtifactError, ArtifactInput, load, load_app, load_client_current, load_client_encrypted,
    rewrite, write,
};
use jazz::tools::public_schema::{ColumnType, PlainType, TableName};

/// A project-management app: projects are spaces, a task's title and notes
/// are encrypted in its project's space.
fn app(tasks_encryption: &str) -> String {
    format!(
        r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": {{
            "projects": {{ "columns": {{ "name": "bytes", "ownerId": "uuid" }},
                "encryption": {{ "space": "id", "scope": "projects", "columns": {{ "name": "string" }} }} }},
            "tasks": {{
                "columns": {{ "projectId": "uuid", "title": "bytes", "notes": "bytes", "__e2ee_eq_title": "bytes", "done": "boolean" }},
                "relations": {{ "project": {{ "rel": "projects", "column": "projectId" }} }},
                "encryption": {tasks_encryption}
            }}
        }},
        "encryption": {{ "allowAccountReset": true }} }}"#
    )
}

fn tasks_entry() -> &'static str {
    r#"{ "space": "projectId", "scope": "projects",
        "columns": { "title": "string", "notes": "string?" }, "equality": ["title"] }"#
}

const MEMBERS_RULE: &str = "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)";

fn permissions(project_read: &str) -> String {
    format!(
        r#"{{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": {{
            "projects": {{ "read": "{project_read}", "insert": "$createdBy.account = session.user", "update": "ownerId = session.user" }},
            "tasks": {{ "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" }}
        }} }}"#,
        project_read = project_read.replace('"', "\\\"")
    )
}

fn refusal(result: Result<impl std::fmt::Debug, ArtifactError>) -> String {
    result.expect_err("refused").to_string()
}

/// An app that declares encryption loads with Jazz's system tables in its
/// schema, its declarations returned beside it, and its files rewrite to
/// the same text, the system tables left out.
#[test]
fn an_encrypted_app_loads_with_its_system_tables_and_declarations() {
    let app = app(tasks_entry());
    let loaded = load(app.as_bytes(), permissions(MEMBERS_RULE).as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let schema = loaded.current_schema();
    for table in [MEMBERS, SPACES, "projects", "tasks"] {
        assert!(schema.contains_key(&TableName::from(table)), "{table}");
    }
    let declared = loaded.encryption();
    assert!(declared.settings.allow_account_reset);
    let entries = declared
        .entries(&loaded.current_hash().to_string())
        .expect("the current version's entries");
    assert_eq!(entries["tasks"].equality, ["title"]);
    assert_eq!(
        entries["tasks"].columns["notes"].plain,
        PlainType {
            column_type: ColumnType::Text,
            nullable: true,
            default: None,
        }
    );
    // The app's own permissions travel; Jazz's are derived on both sides.
    assert!(!loaded.permissions().contains_key(&TableName::from(MEMBERS)));
    let body = loaded.deployment_body(false);
    assert_eq!(body["encryption"]["settings"]["allowAccountReset"], true);

    let (client_schema, _, declarations) =
        load_client_encrypted(app.as_bytes(), AppId::from_name("tasks-app"), "production").unwrap();
    assert_eq!(client_schema, *schema);
    let declarations = declarations.expect("declared");
    assert_eq!(declarations.environment, "production");
    assert_eq!(declarations.tables, *entries);
    // The loader a client opens with refuses an encrypted app until clients
    // have hooks (`a_client_refuses_an_encrypted_app_until_it_has_hooks`).
    assert!(load_client_current(app.as_bytes()).is_err());

    let written = rewrite(app.as_bytes(), permissions(MEMBERS_RULE).as_bytes(), None)
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(!written.app.contains("__e2ee_members"));
    assert!(written.app.contains("\"equality\""));
    let again = rewrite(written.app.as_bytes(), written.permissions.as_bytes(), None).unwrap();
    assert_eq!(again, written, "rewriting is idempotent");
    assert_eq!(
        load(written.app.as_bytes(), written.permissions.as_bytes())
            .unwrap()
            .bundle(),
        loaded.bundle()
    );
}

/// An app without declarations gains no system tables, so its schema hash
/// is what it was.
#[test]
fn an_app_without_declarations_has_no_system_tables() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string" } } } }"#;
    let app = load_app(file.as_bytes()).unwrap();
    assert_eq!(app.current_schema().len(), 1);
    assert!(app.encryption().is_empty());
}

/// The reader refuses declarations the protocol rules out, naming where.
#[test]
fn malformed_declarations_are_refused() {
    let cases = [
        (
            r#"{ "space": "projectId", "scope": "nope", "columns": {} }"#,
            "no table nope",
        ),
        (
            r#"{ "space": "id", "scope": "projects", "columns": {} }"#,
            "only when the table is its own space table",
        ),
        (
            r#"{ "space": "done", "scope": "projects", "columns": {} }"#,
            "a `uuid` column referencing projects",
        ),
        (
            r#"{ "space": "projectId", "scope": "projects", "columns": { "done": "boolean" } }"#,
            "stored as required `bytes`",
        ),
        (
            r#"{ "space": "projectId", "scope": "projects", "columns": { "title": "string" }, "equality": ["notes"] }"#,
            "not an encrypted column",
        ),
        (
            r#"{ "space": "projectId", "scope": "projects", "columns": { "title": { "type": "int", "from": "done" } } }"#,
            "unknown key `from`",
        ),
        (
            r#"{ "space": "projectId", "scope": "projects", "columns": {}, "unknown": 1 }"#,
            "unknown key `unknown`",
        ),
    ];
    for (entry, expected) in cases {
        let error = refusal(load_app(app(entry).as_bytes()));
        assert!(
            error.contains(expected) && error.contains("tables.tasks.encryption"),
            "{entry}: {error}"
        );
    }
    let merged = app(tasks_entry()).replace(
        r#""notes": "bytes""#,
        r#""notes": { "type": "bytes", "merge": "counter" }"#,
    );
    assert!(refusal(load_app(merged.as_bytes())).contains("last-writer-wins"));
}

/// The app-file reader names an unknown key and the declaration containing it.
#[test]
fn unknown_encryption_keys_are_refused_in_the_app_file() {
    for (key, on_column) in [
        ("from", true),
        ("typo", true),
        ("backfill", false),
        ("typo", false),
    ] {
        let mut entry: serde_json::Value = serde_json::from_str(tasks_entry()).unwrap();
        let path = if on_column {
            entry["columns"]["notes"] = serde_json::json!({"type": "string?", key: null});
            "tables.tasks.encryption.columns.notes"
        } else {
            entry[key] = serde_json::Value::Null;
            "tables.tasks.encryption"
        };
        let error = refusal(load_app(app(&entry.to_string()).as_bytes()));
        assert!(
            error.contains(path)
                && error.contains(&format!("unknown key `{key}`"))
                && error.contains("this object takes")
                && error.contains(if on_column { "`type`" } else { "`scope`" })
                && !error.contains("docs/e2ee/encrypting-existing-data"),
            "{error}"
        );
    }
    let nested = app(&tasks_entry().replace(
        r#""notes": "string?""#,
        r#""notes": {"type":{"enum":["a"], "from":"done"}}"#,
    ));
    let error = refusal(load_app(nested.as_bytes()));
    assert!(
        error.contains("tables.tasks.encryption.columns.notes.type")
            && error.contains("unknown key `from`"),
        "{error}"
    );
}

/// Alice receives the nested location when an array element names an unknown field.
#[test]
fn nested_encrypted_type_objects_refuse_unknown_fields() {
    let plain = r#"{"$schema":"https://jazz.tools/schemas/jazz.app.v1.json",
        "tables":{"notes":{"columns":{"labels":{"array":{"type":"string"}}}}}}"#;
    let annotated = plain.replace(r#""type":"string""#, r#""type":"string","from":"oldTitle""#);
    assert_eq!(
        load_app(annotated.as_bytes()).unwrap().current_hash(),
        load_app(plain.as_bytes()).unwrap().current_hash()
    );
    let duplicate = r#"{"columnType":{"type":"Array","element":{"type":"Integer","type":"Text"}}}"#;
    assert!(
        serde_json::from_str::<PlainType>(duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate field")
    );
    let mut accepted = Vec::new();
    for (ty, path, key) in [
        (
            r#"{"array":{"type":"string","from":"oldTitle"}}"#,
            "array",
            "from",
        ),
        (
            r#"{"type":{"array":{"type":"string","reverse":"oldTitle"}}}"#,
            "type.array",
            "reverse",
        ),
        (
            r#"{"type":{"array":{"array":"string"}},"sealedType":{"array":{"array":{"type":"string","typo":0}}}}"#,
            "sealedType.array.array",
            "typo",
        ),
    ] {
        let file =
            app(&tasks_entry().replace(r#""notes": "string?""#, &format!(r#""notes": {ty}"#)));
        match load_app(file.as_bytes()) {
            Ok(_) => accepted.push(path),
            Err(e) => {
                let error = e.to_string();
                assert!(
                    error.contains(&format!("tables.tasks.encryption.columns.notes.{path}"))
                        && error.contains(&format!("unknown key `{key}`"))
                        && error.contains("this object takes")
                        && !error.contains("docs/e2ee/encrypting-existing-data"),
                    "{error}"
                );
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "accepted nested fields at {accepted:?}"
    );
}

/// Jazz's system tables are Jazz's: the app may not define one, reference
/// one, write its policies or reach one in a policy, except testing the
/// members table with `exists`.
#[test]
fn an_app_may_not_define_or_reach_system_tables() {
    let own = app(tasks_entry()).replace(
        r#""tasks": {"#,
        r#""__e2ee_notes": { "columns": { "x": "string" } }, "tasks": {"#,
    );
    assert!(refusal(load_app(own.as_bytes())).contains("reserved for Jazz's own"));
    let referencing = app(tasks_entry()).replace(
        r#""done": "boolean""#,
        r#""done": "boolean", "member": { "type": "uuid", "references": "__e2ee_members" }"#,
    );
    assert!(refusal(load_app(referencing.as_bytes())).contains("system tables"));

    let app = app(tasks_entry());
    let entry = permissions(MEMBERS_RULE).replace(
        r#""tables": {"#,
        r#""tables": { "__e2ee_members": { "read": "true" },"#,
    );
    assert!(refusal(load(app.as_bytes(), entry.as_bytes())).contains("reserved for Jazz's own"));
    let reach = format!("{MEMBERS_RULE} and exists __e2ee_spaces(identifier = this.id)");
    assert!(
        refusal(load(app.as_bytes(), permissions(&reach).as_bytes()))
            .contains("reaches __e2ee_spaces")
    );
}

/// A space table's read rule must come from the members table (R13):
/// `M(T)`, or an `allOf` with `M(T)` among its direct conjuncts.
#[test]
fn a_space_table_read_rule_must_come_from_the_members_table() {
    let app = app(tasks_entry());
    for accepted in [
        MEMBERS_RULE.to_owned(),
        format!("{MEMBERS_RULE} and ownerId = session.user"),
        "exists __e2ee_members(accountId = session.user, identifier = this.id, scopeId = table_id('projects'))".to_owned(),
    ] {
        load(app.as_bytes(), permissions(&accepted).as_bytes())
            .unwrap_or_else(|e| panic!("{accepted}: {e}"));
    }
    for refused in [
        "true".to_owned(),
        "ownerId = session.user".to_owned(),
        format!("{MEMBERS_RULE} or ownerId = session.user"),
        format!("not ({MEMBERS_RULE})"),
        "exists __e2ee_members(scopeId = table_id('tasks'), identifier = this.id, accountId = session.user)".to_owned(),
        "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id)".to_owned(),
    ] {
        let error = refusal(load(app.as_bytes(), permissions(&refused).as_bytes()));
        assert!(
            error.contains("space table projects: its read rule must come from the members table"),
            "{refused}: {error}"
        );
    }
}

/// The deploy warns of what it can see: a broad update rule on a space
/// table, and an encrypted table readable without its space row.
#[test]
fn the_deploy_warns_of_broad_rules_it_can_see() {
    let app = app(tasks_entry());
    let broad = permissions(MEMBERS_RULE)
        .replace("ownerId = session.user", "true")
        .replace("allowedTo.read(project)", "done = false");
    let loaded = load(app.as_bytes(), broad.as_bytes()).unwrap();
    let warnings = loaded.warnings().join("\n");
    assert!(
        warnings.contains("space table projects: its update rule is broad"),
        "{warnings}"
    );
    assert!(
        warnings.contains("table tasks: its read rule doesn't involve its space row"),
        "{warnings}"
    );
    let quiet = load(app.as_bytes(), permissions(MEMBERS_RULE).as_bytes()).unwrap();
    assert!(quiet.warnings().is_empty(), "{:?}", quiet.warnings());
}

/// Alice deploys an encrypted table that checks membership through its project.
/// Only the read rule with an additional owner check is narrower than the project's.
#[test]
fn the_deploy_compares_membership_through_the_declared_space_column() {
    let app = app(tasks_entry());
    let member = MEMBERS_RULE.replace("this.id", "this.projectId");
    let owner = "$createdBy.account = session.user";
    for (read, narrower) in [
        (member.clone(), false),
        (format!("{owner} and {member}"), true),
        (
            format!("{owner} and {}", member.replace("'projects'", "'tasks'")),
            false,
        ),
    ] {
        let permissions = permissions(MEMBERS_RULE).replace("allowedTo.read(project)", &read);
        let loaded = load(app.as_bytes(), permissions.as_bytes()).unwrap();
        let warnings = loaded.warnings();
        if narrower {
            assert!(
                matches!(warnings, [warning] if warning.contains(
                    "table tasks: its read rule is its space table projects's plus further conditions"
                )),
                "{read}: {warnings:?}"
            );
        } else {
            assert!(warnings.is_empty(), "{read}: {warnings:?}");
        }
    }
}

/// Alice sees which column her membership check uses when it differs from the
/// declared space, even when both columns reference projects.
#[test]
fn the_deploy_names_the_wrong_membership_column_and_the_declared_space() {
    let app = app(tasks_entry()).replace(
        r#""done": "boolean""#,
        r#""done": "boolean", "otherProjectId": { "type": "uuid", "references": "projects" }"#,
    );
    for column in ["id", "otherProjectId"] {
        let member = MEMBERS_RULE.replace("this.id", &format!("this.{column}"));
        let read = format!("{member} and $createdBy.account = session.user");
        let permissions = permissions(MEMBERS_RULE).replace("allowedTo.read(project)", &read);
        let loaded = load(app.as_bytes(), permissions.as_bytes()).unwrap();
        assert_eq!(
            loaded.warnings(),
            [format!(
                "jazz.permissions.json: table tasks: its read rule checks projects membership through {column}, but its space is projectId"
            )]
        );
    }
}

/// Every schema version records its own declarations, and a migration may
/// not change the encoding, space or scope of its stored ciphertext.
#[test]
fn migrations_are_checked_against_both_versions_declarations() {
    let with_history = |current_title: &str, history_title: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{
                "notes": {{ "columns": {{ "title": "bytes", "body": "bytes" }},
                    "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": {current_title} }} }} }} }},
            "history": {{ "v1": {{ "tables": {{
                "notes": {{ "columns": {{ "title": "bytes" }},
                    "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": {history_title} }} }} }} }} }} }},
            "migrations": [{{ "from": "v1", "migrate": {{ "notes": {{ "body": {{ "add": "bytes", "default": "" }} }} }} }}] }}"#
        )
    };
    let app = load_app(with_history("\"string\"", "\"string\"").as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(app.encryption().versions.len(), 2, "both versions declare");
    let error = refusal(load_app(with_history("\"int\"", "\"string\"").as_bytes()));
    assert!(
        error.contains("the encrypted column's type changes"),
        "{error}"
    );
    let error = refusal(load_app(
        with_history(
            r#"{ "type": "string?", "sealedType": "string" }"#,
            "\"string\"",
        )
        .as_bytes(),
    ));
    assert!(
        error.contains("copy it into a new encrypted table"),
        "{error}"
    );

    // Encrypting a column that was an optional plaintext column in place.
    let encrypt = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "bodyPlain": "string?", "body": { "type": "bytes", "default": "" } },
            "encryption": { "space": "id", "scope": "notes", "columns": { "body": { "type": "string", "from": "bodyPlain" } } } } },
        "history": { "v1": { "tables": { "notes": { "columns": { "body": "string?" } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": {
            "bodyPlain": { "renameFrom": "body" },
            "body": { "add": "bytes", "default": "" } } } }] }"#;
    let error = refusal(load_app(encrypt.as_bytes()));
    assert!(error.contains("unknown key `from`"), "{error}");

    // Declaring encryption for the first time adds the system tables along
    // the migration, so clients holding the earlier version open.
    let first = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string", "secret": { "type": "bytes", "default": "" } },
            "encryption": { "space": "id", "scope": "notes", "columns": { "secret": { "type": "string", "default": "" } } } } },
        "history": { "v1": { "tables": { "notes": { "columns": { "title": "string" } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": { "secret": { "add": "bytes", "default": "" } } } }] }"#;
    let app = load_app(first.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(app.encryption().versions.len(), 1);
    assert_eq!(app.client_migrations().schemas.len(), 2);
    let notes = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": { "notes": {
        "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let written =
        rewrite(first.as_bytes(), notes.as_bytes(), None).unwrap_or_else(|e| panic!("{e}"));
    assert!(!written.app.contains("__e2ee_members"), "{}", written.app);
    assert_eq!(
        load_app(written.app.as_bytes()).unwrap().current_hash(),
        app.current_hash()
    );
}

/// A policy may not read an encrypted column's value, which the server
/// sees only as ciphertext (migrations §4.1): not in its own table's
/// policy, an `exists`, a relation (`exists (…)`), or through `this.col`.
#[test]
fn a_policy_may_not_read_an_encrypted_value() {
    let app = app(tasks_entry());
    for (table_policy, reads) in [
        (r#""tasks": { "read": "title = null" }"#, "tasks.title"),
        (
            r#""tasks": { "read": "exists tasks(projectId = this.projectId, notes = null)" }"#,
            "tasks.notes",
        ),
        (
            r#""tasks": { "read": "exists projects(id = this.projectId, name = this.title)" }"#,
            "projects.name",
        ),
        (
            r#""tasks": { "read": "exists (from tasks as t where t.notes = null)" }"#,
            "tasks.notes",
        ),
        (
            r#""tasks": { "read": "exists (from projects join tasks as t on t.projectId = projects.id where projects.name = null)" }"#,
            "projects.name",
        ),
    ] {
        let permissions = permissions(MEMBERS_RULE).replace(
            r#""tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" }"#,
            table_policy,
        );
        let error = refusal(load(app.as_bytes(), permissions.as_bytes()));
        assert!(
            error.contains(&format!("reads {reads}, an encrypted column")),
            "{table_policy}: {error}"
        );
    }
    // A relation over plaintext columns stays allowed.
    let plaintext = permissions(MEMBERS_RULE).replace(
        r#""tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" }"#,
        r#""tasks": { "read": "exists (from tasks as t where t.done = false)" }"#,
    );
    load(app.as_bytes(), plaintext.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
}

/// The server keeps each schema version's declarations in a named,
/// versioned form, pinned here byte for byte; another form is refused.
#[test]
fn stored_declarations_are_pinned() {
    use jazz::tools::deployment::artifact::encryption::{decode_stored, encode_stored};
    let entries = load_app(app(tasks_entry()).as_bytes())
        .unwrap()
        .encryption()
        .versions
        .values()
        .next()
        .unwrap()
        .clone();
    let stored = encode_stored(&entries).unwrap();
    assert_eq!(
        String::from_utf8(stored.clone()).unwrap(),
        "jazz-e2ee-declarations-v1\n\
         {\"projects\":{\"columns\":{\"name\":{\"type\":{\"columnType\":{\"type\":\"Text\"},\"nullable\":false}}},\"scope\":\"projects\",\"space\":\"id\"},\
         \"tasks\":{\"columns\":{\"notes\":{\"type\":{\"columnType\":{\"type\":\"Text\"},\"nullable\":true}},\"title\":{\"type\":{\"columnType\":{\"type\":\"Text\"},\"nullable\":false}}},\"equality\":[\"title\"],\"scope\":\"projects\",\"space\":\"projectId\"}}"
    );
    assert_eq!(decode_stored(&stored).unwrap(), entries);
    let empty = Default::default();
    let stored_empty = b"jazz-e2ee-declarations-v1\n{}";
    assert_eq!(encode_stored(&empty).unwrap(), stored_empty);
    assert_eq!(decode_stored(stored_empty).unwrap(), empty);
    assert!(decode_stored(b"jazz-e2ee-declarations-v1\n{").is_err());
    assert!(decode_stored(b"\xff").is_err());
    assert!(decode_stored(b"jazz-e2ee-declarations-v2\n{}").is_err());
}

/// Without kept declarations (a server with no store), a migration's
/// source is judged by what the deployment says of it: a stored encrypted
/// version left out of the body, its declarations dropped, is refused
/// rather than read as plaintext.
///
/// ```text
/// stored: v1 (system tables, title encrypted)
/// body:   v2 + migration v1 → v2, declarations for v2 only   ✗
/// ```
#[test]
fn a_migrations_stored_source_without_declarations_is_refused() {
    use jazz::tools::deployment::{DeploymentCatalogue, prepare_encrypted_deployment};
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "bytes", "done": "boolean" },
            "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } },
        "history": { "v1": { "tables": { "notes": { "columns": { "title": "bytes" },
            "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": { "done": { "add": "boolean", "default": false } } } }] }"#;
    let notes = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": { "notes": {
        "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let app = load(file.as_bytes(), notes.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let (v1, v1_schema) = app
        .schemas()
        .iter()
        .find(|(hash, _)| *hash != app.current_hash())
        .cloned()
        .unwrap();
    let stored = DeploymentCatalogue {
        schemas: vec![v1_schema],
        ..Default::default()
    };
    let mut request = app.deployment_request(false);
    request.schemas.retain(|(hash, _)| *hash != v1);
    let mut encryption = app.encryption().clone();
    encryption.versions.remove(&v1.to_string());
    let error = prepare_encrypted_deployment(&stored, request, &encryption)
        .expect_err("the source's declarations are missing");
    assert!(
        error.to_string().contains("declares no encryption"),
        "{error}"
    );

    let request = app.deployment_request(false);
    prepare_encrypted_deployment(&stored, request, app.encryption())
        .unwrap_or_else(|e| panic!("with them it deploys: {e}"));
}

/// A direct caller whose migration names a schema the deployment lacks
/// gets the deployment's missing-schema error, not a panic.
#[test]
fn a_migration_to_an_unknown_schema_is_a_missing_schema() {
    use jazz::tools::deployment::{
        DeploymentCatalogue, DeploymentError, prepare_encrypted_deployment,
    };
    use jazz::tools::public_schema::SchemaHash;
    use jazz::tools::schema_lens::{Lens, LensTransform};
    let app = load(
        app(tasks_entry()).as_bytes(),
        permissions(MEMBERS_RULE).as_bytes(),
    )
    .unwrap();
    let unknown = SchemaHash([7; 32]);
    let mut request = app.deployment_request(false);
    request.migrations.push(Lens::new(
        unknown,
        app.current_hash(),
        LensTransform::with_ops(Vec::new()),
    ));
    let error =
        prepare_encrypted_deployment(&DeploymentCatalogue::default(), request, app.encryption())
            .expect_err("an unknown migration end");
    assert!(
        matches!(error, DeploymentError::MissingSchema { schema } if schema == unknown),
        "{error}"
    );
}

/// The writer leaves out Jazz's system tables only when they are exactly
/// Jazz's fragment, which an encrypted app's schema holds; a table with the
/// reserved prefix beside it, or an altered fragment, is refused before
/// anything is written.
///
/// ```text
/// schema + Jazz's fragment                 ──write──► files without the fragment ✓
/// schema + fragment, one column dropped    ──write──► ✗ reserved table name
/// schema + fragment + an `__e2ee_notes`    ──write──► ✗ reserved table name
/// ```
#[test]
fn the_writer_omits_only_jazzs_complete_fragment() {
    let app = app(tasks_entry());
    let loaded = load(app.as_bytes(), permissions(MEMBERS_RULE).as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let input = |mut schema: jazz::tools::public_schema::Schema| {
        // The writer hashes the structure: a deployment drops the policies.
        for table in schema.values_mut() {
            table.policies = Default::default();
        }
        ArtifactInput {
            current: jazz::tools::public_schema::SchemaHash::compute(&schema).to_string(),
            schemas: vec![schema],
            migrations: Vec::new(),
            permissions: loaded.permissions().clone(),
            codegen: loaded.codegen().clone(),
        }
    };
    let written = write(input(loaded.current_schema().clone())).unwrap_or_else(|e| panic!("{e}"));
    assert!(!written.app.contains("__e2ee_members"), "{}", written.app);
    assert!(written.app.contains("\"equality\""));

    let mut altered = loaded.current_schema().clone();
    let spaces = altered.get_mut(&TableName::from(SPACES)).unwrap();
    spaces.columns.columns.pop();
    let error = refusal(write(input(altered)));
    assert!(error.contains("are reserved for Jazz's own"), "{error}");

    let mut foreign = loaded.current_schema().clone();
    let copy = foreign[&TableName::from(SPACES)].clone();
    foreign.insert(TableName::from("__e2ee_notes"), copy);
    let error = refusal(write(input(foreign)));
    assert!(error.contains("`__e2ee_notes`"), "{error}");
}

/// No app was deployed with a table of the reserved prefix, so one is
/// refused in every version of the app file, an older one in `history`
/// included.
#[test]
fn an_app_table_with_the_reserved_prefix_is_refused_in_every_version() {
    let older = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string", "done": "boolean" } } },
        "history": { "v1": { "tables": {
            "notes": { "columns": { "title": "string" } },
            "__e2ee_notes": { "columns": { "x": "string" } }
        } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": { "done": { "add": "boolean", "default": false } } } }] }"#;
    let error = refusal(load_app(older.as_bytes()));
    assert!(error.contains("history"), "{error}");
    assert!(error.contains("are reserved for Jazz's own"), "{error}");
}

/// An encrypted column's declared default (what a cell never written reads
/// as, protocol §4.1) travels with its declarations: from the app file
/// through the client loader, the rewritten file, the deploy request and
/// the server's stored form. It is not part of the type the cells are
/// sealed under.
///
/// ```text
/// jazz.app.json { "type": "string", "default": "none yet" }
///   ├─► client declarations ─► runtime
///   ├─► rewritten app file
///   └─► deploy request ─► jazz-e2ee-declarations-v1 (stored) ─► back
/// ```
#[test]
fn an_encrypted_columns_default_travels_with_its_declarations() {
    use jazz::tools::deployment::artifact::encryption::{decode_stored, encode_stored};
    use jazz::tools::public_schema::Value;
    let entry = r#"{ "space": "projectId", "scope": "projects",
        "columns": { "title": "string", "notes": { "type": "string", "default": "none yet" } },
        "equality": ["title"] }"#;
    let file = app(entry);
    let expected = PlainType {
        column_type: ColumnType::Text,
        nullable: false,
        default: Some(Value::Text("none yet".to_owned())),
    };
    let notes = |entries: &jazz::tools::deployment::artifact::encryption::Entries| {
        entries["tasks"].columns["notes"].plain.clone()
    };

    let (_, _, declarations) =
        load_client_encrypted(file.as_bytes(), AppId::from_name("tasks-app"), "production")
            .unwrap();
    assert_eq!(notes(&declarations.unwrap().tables), expected);

    let written = rewrite(file.as_bytes(), permissions(MEMBERS_RULE).as_bytes(), None)
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(written.app.contains("\"none yet\""), "{}", written.app);
    let reloaded = load_app(written.app.as_bytes()).unwrap();
    let entries = reloaded.encryption().versions.values().next().unwrap();
    assert_eq!(notes(entries), expected);

    let loaded = load(file.as_bytes(), permissions(MEMBERS_RULE).as_bytes()).unwrap();
    // The deploy body carries the declarations as the server reads them.
    let body = loaded.deployment_body(false);
    let sent: jazz::tools::deployment::artifact::encryption::DeclaredEncryption =
        serde_json::from_value(body["encryption"].clone()).unwrap();
    let deployed = sent.versions.values().next().unwrap();
    assert_eq!(notes(deployed), expected);
    let stored = encode_stored(deployed).unwrap();
    assert!(
        String::from_utf8(stored.clone())
            .unwrap()
            .contains("\"default\"")
    );
    assert_eq!(notes(&decode_stored(&stored).unwrap()), expected);

    let pinned = file.replace(
        r#""default": "none yet""#,
        r#""sealedType": "string", "default": "none yet""#,
    );
    let written = rewrite(
        pinned.as_bytes(),
        permissions(MEMBERS_RULE).as_bytes(),
        None,
    )
    .unwrap();
    let json: serde_json::Value = serde_json::from_str(&written.app).unwrap();
    assert_eq!(
        json["tables"]["tasks"]["encryption"]["columns"]["notes"],
        serde_json::json!({
            "type": "string", "sealedType": "string", "default": "none yet"
        })
    );
    let reloaded = load_app(written.app.as_bytes()).unwrap();
    let entries = reloaded.encryption().versions.values().next().unwrap();
    assert_eq!(notes(entries), expected);
    assert!(entries["tasks"].columns["notes"].sealed_type.is_some());

    // A sealed type takes no default: it only names what cells are sealed
    // under.
    let sealed = app(r#"{ "space": "projectId", "scope": "projects",
        "columns": { "title": "string", "notes": { "type": "string?",
            "sealedType": { "type": "string", "default": "x" } } } }"#);
    let error = refusal(load_app(sealed.as_bytes()));
    assert!(error.contains("sealedType.default"), "{error}");
}

/// A migration may add an encrypted column with or without a declared
/// default, but a later version may not change a column's default: the
/// rows stored before the column read it, and clients of each version
/// would read them differently.
#[test]
fn an_encrypted_columns_default_may_not_change() {
    let with_body = |body: &str, history_body: Option<&str>| {
        let history_columns = match history_body {
            Some(_) => r#"{ "title": "bytes", "body": "bytes" }"#,
            None => r#"{ "title": "bytes" }"#,
        };
        let history_encrypted = match history_body {
            Some(spec) => format!(r#"{{ "title": "string", "body": {spec} }}"#),
            None => r#"{ "title": "string" }"#.to_owned(),
        };
        let migrate = match history_body {
            Some(_) => r#"{ "notes": { "done": { "add": "boolean", "default": false } } }"#,
            None => {
                r#"{ "notes": { "body": { "add": "bytes", "default": "" }, "done": { "add": "boolean", "default": false } } }"#
            }
        };
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{
                "notes": {{ "columns": {{ "title": "bytes", "body": "bytes", "done": "boolean" }},
                    "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": "string", "body": {body} }} }} }} }},
            "history": {{ "v1": {{ "tables": {{
                "notes": {{ "columns": {history_columns},
                    "encryption": {{ "space": "id", "scope": "notes", "columns": {history_encrypted} }} }} }} }} }},
            "migrations": [{{ "from": "v1", "migrate": {migrate} }}] }}"#
        )
    };
    for accepted in [
        r#"{ "type": "string", "default": "untitled" }"#,
        "\"string?\"",
    ] {
        load_app(with_body(accepted, None).as_bytes())
            .unwrap_or_else(|e| panic!("{accepted}: {e}"));
    }
    // A required column the migration adds needs its default declared: the
    // rows stored before it read as never written.
    let error = refusal(load_app(with_body("\"string\"", None).as_bytes()));
    assert!(error.contains("declare its default"), "{error}");
    let error = refusal(load_app(
        with_body(
            r#"{ "type": "string", "default": "new" }"#,
            Some(r#"{ "type": "string", "default": "old" }"#),
        )
        .as_bytes(),
    ));
    assert!(error.contains("default changes"), "{error}");
    let error = refusal(load_app(
        with_body(
            "\"string\"",
            Some(r#"{ "type": "string", "default": "old" }"#),
        )
        .as_bytes(),
    ));
    assert!(error.contains("default changes"), "{error}");
    load_app(
        with_body(
            r#"{ "type": "string", "default": "same" }"#,
            Some(r#"{ "type": "string", "default": "same" }"#),
        )
        .as_bytes(),
    )
    .unwrap_or_else(|e| panic!("keeping a default: {e}"));
}

/// An encrypted enum column's declared default must be one of its
/// variants: the reader refuses any other, as the runtime would.
#[test]
fn an_encrypted_enum_default_must_be_a_variant() {
    let with_default = |default: &str| {
        app(&format!(
            r#"{{ "space": "projectId", "scope": "projects",
            "columns": {{ "title": "string",
                "notes": {{ "enum": ["open", "done"], "default": "{default}" }} }} }}"#
        ))
    };
    load_app(with_default("done").as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let error = refusal(load_app(with_default("lost").as_bytes()));
    assert!(error.contains("one of the enum's variants"), "{error}");
}

/// The app-file reader refuses a source on either an existing or a fresh encrypted column.
#[test]
fn a_column_cannot_declare_a_conversion_source() {
    let version = |columns: &str, entry: &str, migrate: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {{ "columns": {columns},
                "encryption": {{ "space": "id", "scope": "notes", "columns": {entry} }} }} }},
            "history": {{ "v1": {{ "tables": {{ "notes": {{
                "columns": {{ "title": "bytes", "legacy": "string" }},
                "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": "string" }} }} }} }} }} }},
            "migrations": [{{ "from": "v1", "migrate": {{ "notes": {migrate} }} }}] }}"#
        )
    };
    let over = version(
        r#"{ "title": "bytes", "legacy": "string", "done": "boolean" }"#,
        r#"{ "title": { "type": "int", "from": "legacy", "convert": "parse" } }"#,
        r#"{ "done": { "add": "boolean", "default": false } }"#,
    );
    let error = refusal(load_app(over.as_bytes()));
    assert!(error.contains("unknown key `from`"), "{error}");

    let into_new = version(
        r#"{ "title": "bytes", "title2": { "type": "bytes", "default": "" } }"#,
        r#"{ "title2": { "type": "int", "from": "title", "convert": "parse" } }"#,
        r#"{ "title2": { "add": "bytes", "default": "" }, "legacy": { "drop": "string", "backwardsDefault": "" } }"#,
    );
    assert!(refusal(load_app(into_new.as_bytes())).contains("unknown key `from`"));
}

/// Existing tables cannot add equality. Removing equality drops the token column in the same step.
#[test]
fn equality_changes_across_versions_follow_the_migrations_spec() {
    let app = |tables: &str, history: &str, migrate: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {tables} }},
            "history": {{ "v1": {{ "tables": {{ "notes": {history} }} }} }},
            "migrations": [{{ "from": "v1", "migrate": {{ "notes": {migrate} }} }}] }}"#
        )
    };
    let entry = |extra: &str| {
        format!(
            r#"{{ "columns": {{ "title": "bytes", "__e2ee_eq_title": "bytes", "done": "boolean" }},
            "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": "string" }}{extra} }} }}"#
        )
    };
    let untagged = r#"{ "columns": { "title": "bytes" },
        "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } }"#;
    let add_tag = r#"{ "__e2ee_eq_title": { "add": "bytes", "default": "" },
        "done": { "add": "boolean", "default": false } }"#;
    let error = refusal(load_app(
        app(&entry(r#", "equality": ["title"]"#), untagged, add_tag).as_bytes(),
    ));
    assert!(error.contains("adding equality"), "{error}");
    assert!(
        refusal(load_app(
            app(
                &entry(r#", "equality": ["title"], "backfill": ["title"]"#),
                untagged,
                add_tag,
            )
            .as_bytes(),
        ))
        .contains("unknown key `backfill`")
    );

    let tagged = r#"{ "columns": { "title": "bytes", "__e2ee_eq_title": "bytes" },
        "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" },
            "equality": ["title"] } }"#;
    let add_done = r#"{ "done": { "add": "boolean", "default": false } }"#;
    let error = refusal(load_app(app(&entry(""), tagged, add_done).as_bytes()));
    assert!(error.contains("token column"), "{error}");
    let drop_tag = r#"{ "__e2ee_eq_title": { "drop": "bytes", "backwardsDefault": "" },
        "done": { "add": "boolean", "default": false } }"#;
    let dropped = r#"{ "columns": { "title": "bytes", "done": "boolean" },
        "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } }"#;
    load_app(app(dropped, tagged, drop_tag).as_bytes()).unwrap_or_else(|e| panic!("{e}"));
}

/// An equality token has an encoding for the scalar and enum types, JSON
/// and arrays of any of them, and an encrypted column's stored default is
/// the empty ciphertext: a stored default that is not empty is refused, and
/// the types the format can't encode are refused in
/// `equality_follows_the_types_the_format_encodes`.
#[test]
fn equality_and_stored_defaults_follow_their_formats() {
    let with_type = |ty: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {{ "columns": {{ "data": "bytes", "__e2ee_eq_data": "bytes" }},
                "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "data": {ty} }},
                    "equality": ["data"] }} }} }} }}"#
        )
    };
    for accepted in [
        r#"{ "json": true }"#,
        r#"{ "json": {} }"#,
        r#"{ "array": "string" }"#,
        r#"{ "array": { "json": true } }"#,
        r#"{ "array": { "array": "int" } }"#,
        r#""string""#,
    ] {
        load_app(with_type(accepted).as_bytes()).unwrap_or_else(|e| panic!("{accepted}: {e}"));
    }

    let stored_default = |default: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {{ "columns": {{ "data": {{ "type": "bytes", "default": "{default}" }} }},
                "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "data": "string" }} }} }} }} }}"#
        )
    };
    load_app(stored_default("").as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let error = refusal(load_app(stored_default("c2VjcmV0").as_bytes()));
    assert!(error.contains("stored default"), "{error}");
}

/// Alice can give an equality token an empty stored default; any other bytes are refused.
#[test]
fn an_equality_tokens_stored_default_must_be_empty() {
    let with_default = |encoded: &str| {
        app(tasks_entry()).replace(
            r#""__e2ee_eq_title": "bytes""#,
            &format!(r#""__e2ee_eq_title": {{ "type": "bytes", "default": "{encoded}" }}"#),
        )
    };
    load_app(with_default("").as_bytes()).unwrap();
    let error = refusal(load_app(with_default("AQ==").as_bytes()));
    assert!(
        error.contains(
            "the stored default of token column __e2ee_eq_title must be an empty byte string"
        ),
        "{error}"
    );
}

/// A client has no encryption hooks yet, so none of the ways a client
/// opens an app lets an encrypted one in: the files' client loader and the
/// bindings' database open both refuse it, while a backend can still read
/// the files to deploy them.
#[test]
fn a_client_refuses_an_encrypted_app_until_it_has_hooks() {
    use jazz::db::{DbConfig, DbIdentity, block_on};
    use jazz::ids::{AuthorSubject, NodeUuid};
    let app = app(tasks_entry());
    let error = refusal(load_client_current(app.as_bytes()));
    assert!(error.contains("declares end-to-end encryption"), "{error}");

    let loaded = load(app.as_bytes(), permissions(MEMBERS_RULE).as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let schema = jazz::schema::JazzSchema::new(loaded.current_schema()).unwrap();
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let opened = block_on(jazz::binding_host::open_memory(config));
    let error = opened.err().expect("refused");
    assert!(
        error.to_string().contains("declares end-to-end encryption"),
        "{error}"
    );
}

/// Equality follows the types the equality format encodes: JSON and arrays
/// of any searchable element type are accepted, while a row, a
/// transaction ID and an array of either are refused, naming what stops it.
#[test]
fn equality_follows_the_types_the_format_encodes() {
    use jazz::tools::deployment::artifact::encryption::check_entry;
    use jazz::tools::public_schema::{
        ColumnDescriptor, EncryptedColumn, EncryptionEntry, PlainType, RowDescriptor, Schema,
        TableSchema,
    };
    use std::collections::BTreeMap;
    let schema: Schema = Schema::from([(
        TableName::new("notes"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("data", ColumnType::Bytea),
            ColumnDescriptor::new("__e2ee_eq_data", ColumnType::Bytea),
        ])),
    )]);
    let entry = |ty: ColumnType| EncryptionEntry {
        space: "id".to_owned(),
        scope: "notes".to_owned(),
        columns: BTreeMap::from([(
            "data".to_owned(),
            EncryptedColumn {
                plain: PlainType::new(ty, false),
                sealed_type: None,
            },
        )]),
        equality: vec!["data".to_owned()],
    };
    let array = |element: ColumnType| ColumnType::Array {
        element: Box::new(element),
    };
    let row = ColumnType::Row {
        columns: Box::new(RowDescriptor::new(vec![ColumnDescriptor::new(
            "x",
            ColumnType::Text,
        )])),
    };
    for accepted in [
        ColumnType::Json { schema: None },
        array(ColumnType::Text),
        array(ColumnType::Json { schema: None }),
        array(array(ColumnType::Integer)),
    ] {
        check_entry("notes", &entry(accepted.clone()), &schema)
            .unwrap_or_else(|(at, e)| panic!("{accepted:?}: {at}: {e}"));
    }
    for (refused, named) in [
        (row.clone(), "row columns"),
        (ColumnType::TransactionId, "transaction-ID columns"),
        (array(row), "array of row columns"),
        (
            array(array(ColumnType::TransactionId)),
            "array of array of transaction-ID",
        ),
    ] {
        let (_, message) = check_entry("notes", &entry(refused.clone()), &schema)
            .expect_err("a type the format can't encode");
        assert!(message.contains(named), "{refused:?}: {message}");
    }
}

/// A Rust program that connects a `JazzClient` directly, without the files'
/// loader, is refused by `connect` itself: the schema it hands over holds
/// Jazz's system tables, and the client has no encryption hooks yet.
///
/// ```text
/// AppContext { encrypted schema } ──JazzClient::connect──► ✗ refused
/// AppContext { ordinary schema }  ──JazzClient::connect──► ✓
/// ```
#[test]
fn connect_refuses_an_encrypted_schema_by_itself() {
    use jazz::db::block_on;
    use jazz::tools::{AppContext, JazzClient};
    let app = app(tasks_entry());
    let loaded = load(app.as_bytes(), permissions(MEMBERS_RULE).as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let refused = block_on(JazzClient::connect(AppContext::test(
        loaded.current_schema().clone(),
    )));
    let error = refused.err().expect("an encrypted schema is refused");
    assert!(
        error.to_string().contains("declares end-to-end encryption"),
        "{error}"
    );
    let ordinary = load_app(
        r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": { "notes": { "columns": { "title": "string" } } } }"#
            .as_bytes(),
    )
    .unwrap();
    block_on(JazzClient::connect(AppContext::test(
        ordinary.current_schema().clone(),
    )))
    .unwrap_or_else(|e| panic!("{e}"));
}

/// An existing table may hold rows with no token for a newly added column.
#[test]
fn a_new_equality_column_in_an_existing_table_is_refused() {
    let app = |history_columns: &str,
               history_entry: &str,
               current_columns: &str,
               current_title: &str,
               backfill: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {{
                "columns": {current_columns},
                "encryption": {{ "space": "id", "scope": "notes",
                    "columns": {{ {current_title}"body": {{ "type": "string", "default": "" }} }},
                    "equality": ["body"]{backfill} }} }} }},
            "history": {{ "v1": {{ "tables": {{ "notes": {{
                "columns": {history_columns}{history_entry} }} }} }} }},
            "migrations": [{{ "from": "v1", "migrate": {{ "notes": {{
                "body": {{ "add": "bytes", "default": "" }},
                "__e2ee_eq_body": {{ "add": "bytes", "default": "" }} }} }} }}] }}"#
        )
    };
    // The table was encrypted before (title); body and its tokens are new.
    let encrypted_columns = r#"{ "title": "bytes", "body": "bytes", "__e2ee_eq_body": "bytes" }"#;
    let encrypted_entry = r#", "encryption": { "space": "id", "scope": "notes",
        "columns": { "title": "string" } }"#;
    let before = |backfill| {
        app(
            r#"{ "title": "bytes" }"#,
            encrypted_entry,
            encrypted_columns,
            r#""title": "string", "#,
            backfill,
        )
    };
    let error = refusal(load_app(before("").as_bytes()));
    assert!(error.contains("adding equality"), "{error}");
    assert!(
        refusal(load_app(before(r#", "backfill": ["body"]"#).as_bytes()))
            .contains("unknown key `backfill`")
    );

    // The table is encrypted for the first time: it held plaintext before.
    let plain_columns = r#"{ "title": "string", "body": "bytes", "__e2ee_eq_body": "bytes" }"#;
    let first = |backfill| app(r#"{ "title": "string" }"#, "", plain_columns, "", backfill);
    let error = refusal(load_app(first("").as_bytes()));
    assert!(error.contains("adding equality"), "{error}");
    assert!(
        refusal(load_app(first(r#", "backfill": ["body"]"#).as_bytes()))
            .contains("unknown key `backfill`")
    );
}

/// The `__e2ee_` column names are Jazz's: the token column of an equality
/// column (`__e2ee_eq_<column>`), or any other reserved name, can't be
/// declared as an encrypted column.
#[test]
fn reserved_column_names_cannot_be_encrypted_columns() {
    let with_columns = |declared: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {{
                "columns": {{ "title": "bytes", "__e2ee_eq_title": "bytes", "plain": "string?" }},
                "encryption": {{ "space": "id", "scope": "notes",
                    "columns": {{ {declared} }}, "equality": ["title"] }} }} }} }}"#
        )
    };
    load_app(with_columns(r#""title": "string""#).as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    for declared in [
        r#""title": "string", "__e2ee_eq_title": "string""#,
        r#""title": "string", "__e2ee_other": "string""#,
    ] {
        let error = refusal(load_app(with_columns(declared).as_bytes()));
        assert!(error.contains("`__e2ee_`"), "{declared}: {error}");
    }
}

/// Once a version carries Jazz's system tables, every later version does,
/// even one that declares no encryption: every loader appends the fragment,
/// so the client's and the server's schemas and hashes agree, the system
/// tables take the declarations of the nearest earlier version that has
/// them, and a client of the app is still an encrypted app.
///
/// ```text
/// v1: notes.title encrypted      (fragment)
/// v2: notes without encryption   (fragment kept, title dropped)
/// ```
#[test]
fn a_version_without_declarations_keeps_the_system_tables_after_an_encrypted_one() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "done": "boolean" } } },
        "history": { "v1": { "tables": { "notes": {
            "columns": { "title": "bytes", "done": "boolean" },
            "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": {
            "title": { "drop": "bytes", "backwardsDefault": "" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let loaded = load(file.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    // Both versions hold the fragment, though only v1 declares encryption.
    assert!(
        loaded
            .current_schema()
            .contains_key(&TableName::from(MEMBERS))
    );
    assert_eq!(loaded.schemas().len(), 2);
    assert!(
        loaded
            .schemas()
            .iter()
            .all(|(_, schema)| schema.contains_key(&TableName::from(MEMBERS)))
    );
    assert_eq!(loaded.encryption().versions.len(), 1, "only v1 declares");

    // A client computes the same schema and hash, with the declarations it
    // would build its hooks from, and still refuses to open for now.
    let (client_schema, _, declarations) =
        load_client_encrypted(file.as_bytes(), AppId::from_name("notes-app"), "production")
            .unwrap();
    assert_eq!(client_schema, *loaded.current_schema());
    assert_eq!(
        jazz::tools::public_schema::SchemaHash::compute(&client_schema),
        loaded.current_hash()
    );
    // Its own declarations: none, since it dropped the encrypted column (v1's
    // are not its own), though it is still an encrypted app.
    let declarations = declarations.expect("an encrypted app, with its system tables");
    assert!(declarations.tables.is_empty(), "{:?}", declarations.tables);
    // The system tables' derived policies still follow v1's declarations.
    assert_eq!(
        loaded.bundle().to_hex(),
        "ef6fae8b8d7e11c3bc684201d8ca9a6566e4a5581734e5a87d9786766f1f2c73"
    );
    let error = refusal(load_client_current(file.as_bytes()));
    assert!(error.contains("declares end-to-end encryption"), "{error}");

    // The files rewrite to the same app.
    let written =
        rewrite(file.as_bytes(), permissions.as_bytes(), None).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        load(written.app.as_bytes(), written.permissions.as_bytes())
            .unwrap()
            .bundle(),
        loaded.bundle()
    );
}

/// Alice cannot include a decrypt declaration in a historical version.
#[test]
fn historical_versions_refuse_unknown_encryption_keys() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "done": "boolean", "bodyText": "string?" } } },
        "history": {
            "v1": { "tables": { "notes": {
                "columns": { "body": "bytes", "done": "boolean" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "body": "string" } } } } },
            "v2": { "tables": { "notes": {
                "columns": { "body": "bytes", "bodyText": "string?", "done": "boolean" },
                "encryption": { "space": "id", "scope": "notes",
                    "columns": { "body": { "type": "string", "to": "bodyText" } } } } } } },
        "migrations": [
            { "from": "v1", "to": "v2", "migrate": { "notes": { "bodyText": { "add": "string?", "default": null } } } },
            { "from": "v2", "migrate": { "notes": { "body": { "drop": "bytes", "backwardsDefault": "" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let error = refusal(load(file.as_bytes(), permissions.as_bytes()));
    assert!(
        error.contains("history.v2.tables.notes.encryption.columns.body")
            && error.contains("unknown key `to`")
            && error.contains("this object takes"),
        "{error}"
    );

    // Keeping the ciphertext declared until its column is dropped preserves the bundle.
    let supported = file.replace(r#", "to": "bodyText""#, "");
    let loaded = load(supported.as_bytes(), permissions.as_bytes()).unwrap();
    assert_eq!(loaded.schemas().len(), 3);
    assert!(
        loaded
            .schemas()
            .iter()
            .all(|(_, schema)| schema.contains_key(&TableName::from(MEMBERS)))
    );
    assert_eq!(loaded.encryption().versions.len(), 2);
    let (_, _, declarations) = load_client_encrypted(
        supported.as_bytes(),
        AppId::from_name("notes-app"),
        "production",
    )
    .unwrap();
    assert!(
        declarations
            .expect("an encrypted app keeps its system tables")
            .tables
            .is_empty()
    );
    assert_eq!(
        loaded.bundle().to_hex(),
        "583f00e5a15a00b10f8dbcabaacd8f98fbd155788ff7d9a507c147156bf86172"
    );
}

/// The backend loader, which reads the app the generated bindings embed,
/// accepts a version that follows one that has Jazz's system tables and
/// keeps them without declaring encryption, as the file loaders compute it.
#[test]
fn the_backend_loader_accepts_a_version_that_keeps_the_system_tables() {
    use jazz::tools::deployment::artifact::load_backend;
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "done": "boolean" } } },
        "history": { "v1": { "tables": { "notes": {
            "columns": { "title": "bytes", "done": "boolean" },
            "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": {
            "title": { "drop": "bytes", "backwardsDefault": "" } } } }] }"#;
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    let loaded = load(file.as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    let body = loaded.deployment_body(false);
    let bound = |encryption: serde_json::Value| {
        serde_json::json!({
            "schema": {
                "current": loaded.current_hash().to_string(),
                "schemas": body["schemas"],
                "migrations": body["migrations"],
                "encryption": encryption,
            },
            "relations": {},
        })
        .to_string()
    };
    let backend = load_backend(
        bound(body["encryption"].clone()).as_bytes(),
        permissions.as_bytes(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(backend.current_hash(), loaded.current_hash());
    assert_eq!(
        backend.bundle(),
        loaded.bundle(),
        "the same bundle as the file loader"
    );
    // A version that has the tables but follows none that does is still
    // refused: without v1's declarations nothing explains them.
    let error = refusal(load_backend(
        bound(serde_json::json!({ "settings": {}, "versions": {} })).as_bytes(),
        permissions.as_bytes(),
    ));
    assert!(error.contains("system tables"), "{error}");
}

/// A version that keeps the system tables without declaring encryption
/// takes the declarations of the nearest version before it that has them;
/// two equally near versions that declare differently leave no nearest one,
/// and the loader refuses, as the deploy's check does.
///
/// ```text
/// v1 (declares) ──┐
///                 ├──► current (declares nothing)   ✗ they disagree
/// v2 (declares) ──┘
/// ```
#[test]
fn equally_near_versions_that_declare_differently_are_refused() {
    let file = |v2_type: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
            "tables": {{ "notes": {{ "columns": {{ "done": "boolean" }} }} }},
            "history": {{
                "v1": {{ "tables": {{ "notes": {{
                    "columns": {{ "title": "bytes", "done": "boolean" }},
                    "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": "string" }} }} }} }} }},
                "v2": {{ "tables": {{ "notes": {{
                    "columns": {{ "title": "bytes", "done": "boolean", "extra": "boolean?" }},
                    "encryption": {{ "space": "id", "scope": "notes", "columns": {{ "title": "{v2_type}" }} }} }} }} }} }},
            "migrations": [
                {{ "from": "v1", "migrate": {{ "notes": {{ "title": {{ "drop": "bytes", "backwardsDefault": "" }} }} }} }},
                {{ "from": "v2", "migrate": {{ "notes": {{
                    "title": {{ "drop": "bytes", "backwardsDefault": "" }},
                    "extra": {{ "drop": "boolean?", "backwardsDefault": null }} }} }} }}] }}"#
        )
    };
    let permissions = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": { "notes": { "read": "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)" } } }"#;
    // The same declarations: the nearest ones are unambiguous.
    load(file("string").as_bytes(), permissions.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    // Different ones: no nearest.
    let disagreeing = file("int");
    let error = refusal(load(disagreeing.as_bytes(), permissions.as_bytes()));
    assert!(error.contains("declare differently"), "{error}");
    // The client's declarations are the version's own, so the client loader
    // has no inherited choice to refuse.
    let (_, _, declarations) = load_client_encrypted(
        disagreeing.as_bytes(),
        AppId::from_name("notes-app"),
        "production",
    )
    .unwrap();
    assert!(declarations.expect("an encrypted app").tables.is_empty());
}

/// A policy of a version that keeps the system tables without declaring
/// encryption may read a column that is plaintext in it, though an earlier
/// version had a column of that name encrypted: only the columns the version
/// itself declares encrypted are refused, while the system tables' policies
/// still follow the earlier declarations.
///
/// ```text
/// v1: title encrypted
/// v2: adds plaintext titleText       (declares)
/// v3: drops title                         (declares nothing)
/// v4: titleText renamed title, plaintext  (declares nothing) ◄── may read title
/// ```
#[test]
fn a_policy_may_read_a_column_that_is_plaintext_in_this_version() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string?" } } },
        "history": {
            "v1": { "tables": { "notes": {
                "columns": { "title": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } },
            "v2": { "tables": { "notes": {
                "columns": { "title": "bytes", "titleText": "string?" },
                "encryption": { "space": "id", "scope": "notes",
                    "columns": { "title": "string" } } } } },
            "v3": { "tables": { "notes": { "columns": { "titleText": "string?" } } } } },
        "migrations": [
            { "from": "v1", "to": "v2", "migrate": { "notes": { "titleText": { "add": "string?", "default": null } } } },
            { "from": "v2", "to": "v3", "migrate": { "notes": { "title": { "drop": "bytes", "backwardsDefault": "" } } } },
            { "from": "v3", "migrate": { "notes": { "title": { "renameFrom": "titleText" } } } }] }"#;
    let members = "exists __e2ee_members(scopeId = table_id('notes'), identifier = this.id, accountId = session.user)";
    let permissions = |read: &str| {
        format!(
            r#"{{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
            "tables": {{ "notes": {{ "read": "{}" }} }} }}"#,
            read.replace('"', "\\\"")
        )
    };
    let reading_title = format!("{members} and title = 'public'");
    load(file.as_bytes(), permissions(&reading_title).as_bytes()).unwrap_or_else(|e| panic!("{e}"));
    // The space table's rule still has to come from the members table.
    let error = refusal(load(
        file.as_bytes(),
        permissions("title = 'public'").as_bytes(),
    ));
    assert!(error.contains("members table"), "{error}");
}

/// The system tables go only to versions a migration leads to from one that
/// has them, as the server follows the migrations, not to every version the
/// file lists after an encrypting one: a plain branch that merely comes
/// later in the file stays plain, so a purely plaintext current version
/// stays plain, and a server holding the tables refuses a plain sibling
/// instead of the loader adding them silently.
///
/// ```text
/// v1 ──► v3 ──► current          plain: no system tables
/// v2 (encrypts, leads nowhere)   the only version with them
/// ```
#[test]
fn the_system_tables_follow_the_migrations_not_the_order_of_the_file() {
    let file = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "done": "boolean", "extra": "boolean?", "more": "boolean?" } } },
        "history": {
            "v1": { "tables": { "notes": { "columns": { "done": "boolean" } } } },
            "v2": { "tables": { "notes": {
                "columns": { "done": "boolean", "title": "bytes" },
                "encryption": { "space": "id", "scope": "notes", "columns": { "title": "string" } } } } },
            "v3": { "tables": { "notes": { "columns": { "done": "boolean", "extra": "boolean?" } } } } },
        "migrations": [
            { "from": "v1", "to": "v3", "migrate": { "notes": { "extra": { "add": "boolean?", "default": null } } } },
            { "from": "v3", "migrate": { "notes": { "more": { "add": "boolean?", "default": null } } } }] }"#;
    let members = TableName::from(MEMBERS);
    let loaded = load(
        file.as_bytes(),
        br#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": {} }"#,
    )
    .unwrap_or_else(|e| panic!("{e}"));
    let carrying: Vec<bool> = loaded
        .schemas()
        .iter()
        .map(|(_, schema)| schema.contains_key(&members))
        .collect();
    assert_eq!(
        carrying.iter().filter(|carries| **carries).count(),
        1,
        "only v2, the version that encrypts: {carrying:?}"
    );
    assert!(
        !loaded.current_schema().contains_key(&members),
        "the current version is plain"
    );
    // A client of the plain current version opens it as a plain app.
    load_client_current(file.as_bytes()).unwrap_or_else(|e| panic!("{e}"));
}
