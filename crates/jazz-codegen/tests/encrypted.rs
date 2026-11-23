//! Bindings of an app that declares encryption (`tests/apps/encrypted`):
//! code reads and writes plaintext types, never ciphertext or equality
//! tokens, and a backend running from its bindings reaches the bundle the
//! files give, Jazz's system policies included.

use std::path::PathBuf;

use jazz::tools::deployment::artifact;
use jazz::tools::public_schema::ColumnType;
use jazz_codegen::{App, Lang};

fn app_json() -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/apps/encrypted/jazz.app.json"),
    )
    .unwrap()
}

const PERMISSIONS: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
    "tables": {
        "projects": {
            "read": "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
            "insert": "$createdBy.account = session.user",
            "update": "ownerId = session.user"
        },
        "tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" }
    } }"#;

/// Generated code sees plaintext column types and the members table.
/// Equality columns and the other system tables are hidden.
#[test]
fn bindings_expose_plaintext_and_hide_ciphertext() {
    let app = App::load(&app_json()).unwrap();
    let names: Vec<&str> = app.tables.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["__e2ee_members", "projects", "tasks"]);
    let tasks = app.table("tasks").unwrap();
    assert!(tasks.column("__e2ee_eq_title").is_none());
    let title = tasks.column("title").unwrap();
    assert_eq!(title.ty.ty, ColumnType::Text);
    assert!(!title.ty.nullable);
    assert!(tasks.column("notes").unwrap().ty.nullable);
    assert_eq!(
        app.table("tasks").unwrap().column("done").unwrap().ty.ty,
        ColumnType::Boolean
    );
}

/// Rust callers get typed members bindings alongside their app tables.
#[test]
fn encrypted_app_generates_its_committed_rust_byte_for_byte() {
    let generated = jazz_codegen::generate(Lang::Rust, &app_json()).unwrap();
    if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/apps/encrypted/src/jazz_app.rs");
        std::fs::write(path, &generated).unwrap();
        return;
    }
    assert_eq!(generated, include_str!("apps/encrypted/src/jazz_app.rs"));
}

/// The members table's fixed policy refuses every update, so no language's
/// bindings give it an update, an upsert or update advice: its row, insert,
/// reads and deletes only. App tables keep theirs.
#[test]
fn members_table_gets_no_update_surface() {
    let app = App::load(&app_json()).unwrap();
    assert!(!app.table("__e2ee_members").unwrap().updatable);
    assert!(app.table("projects").unwrap().updatable);
    let generate = |lang| jazz_codegen::generate(lang, &app_json()).unwrap();

    let rust = generate(Lang::Rust);
    assert!(rust.contains("type Update = ::jazz_app_support::NoUpdate;"));
    assert!(!rust.contains("E2eeMemberUpdate"));
    assert!(rust.contains("impl ::jazz_app_support::Insert for NewE2eeMember"));
    assert!(rust.contains("pub struct ProjectUpdate"));

    let swift = generate(Lang::Swift);
    assert!(swift.contains("public struct E2eeMember: Jazz.Row {"));
    assert!(!swift.contains("E2eeMemberPatch"));
    assert!(swift.contains("public struct NewE2eeMember: Jazz.Insert {"));
    assert!(swift.contains("public struct Project: Jazz.Row, Jazz.Updatable {"));
    assert!(swift.contains("public struct ProjectPatch: Jazz.Patch {"));

    let kotlin = generate(Lang::Kotlin);
    assert!(
        kotlin.contains(
            "public object E2eeMembers : tools.jazz.BaseTable<E2eeMember, NewE2eeMember>("
        )
    );
    assert!(!kotlin.contains("E2eeMemberPatch"));
    assert!(kotlin.contains("public data class NewE2eeMember("));
    assert!(
        kotlin.contains(
            "public object Projects : tools.jazz.Table<Project, NewProject, ProjectPatch>("
        )
    );

    let go = generate(Lang::Go);
    // A restore is checked against the update policy, so it goes too.
    for method in ["Update", "Upsert", "CanUpdate", "Restore"] {
        assert!(
            !go.contains(&format!("func (e2eeMembersTable) {method}(")),
            "{method}"
        );
        assert!(
            go.contains(&format!("func (projectsTable) {method}(")),
            "{method}"
        );
    }
    assert!(!go.contains("E2eeMemberPatch"));
    for method in [
        "Query",
        "Insert",
        "Delete",
        "CanRead",
        "CanInsert",
        "CanDelete",
    ] {
        assert!(
            go.contains(&format!("func (e2eeMembersTable) {method}(")),
            "{method}"
        );
    }
}

/// The TypeScript type map marks each encrypted column `encrypted: true`
/// (and no plain one), so a keyless backend's types can drop it; the row
/// and insert types stay the plaintext ones clients use.
#[test]
fn typescript_marks_encrypted_columns() {
    let ts = jazz_codegen::generate(jazz_codegen::Lang::TypeScript, &app_json()).unwrap();
    assert!(
        ts.contains(
            "title: { kind: \"string\"; nullable: false; value: string; encrypted: true };"
        )
    );
    assert!(ts.contains("done: { kind: \"boolean\"; nullable: false; value: boolean };"));
    assert!(ts.contains("  title: string;\n"));
}

/// A backend loading its permissions against the bindings' compiled app
/// computes the bundle the app's files give: the embedded declarations
/// let it derive the system tables' policies.
#[test]
fn a_backend_from_bindings_derives_the_same_bundle() {
    let bound = jazz_codegen::compile_app(&app_json()).unwrap();
    let from_bindings = artifact::load_backend(bound.as_bytes(), PERMISSIONS.as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let from_files = artifact::load(&app_json(), PERMISSIONS.as_bytes()).unwrap();
    assert_eq!(from_bindings.bundle(), from_files.bundle());
    assert_eq!(from_bindings.encryption(), from_files.encryption());
}

/// A client that has the bindings but not `jazz.app.json` reads the same
/// declarations the files give; an app that declares none gives `None`.
///
/// ```text
/// jazz.app.json ──compile_app──► bindings ──bound_declarations──► declarations
///        └──────────load_client_encrypted──────────────────────────► same
/// ```
#[test]
fn a_client_from_bindings_reads_the_same_declarations() {
    let app_id = jazz::tools::AppId::from_name("encrypted-app");
    let bound = jazz_codegen::compile_app(&app_json()).unwrap();
    let from_bindings = artifact::bound_declarations(bound.as_bytes(), app_id, "test")
        .unwrap()
        .expect("declared");
    let (_, _, from_files) = artifact::load_client_encrypted(&app_json(), app_id, "test").unwrap();
    assert_eq!(from_bindings, from_files.expect("declared"));

    let plain = br#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string" } } } }"#;
    let bound = jazz_codegen::compile_app(plain).unwrap();
    assert!(
        artifact::bound_declarations(bound.as_bytes(), app_id, "test")
            .unwrap()
            .is_none()
    );
}
