//! Bindings of an app that declares encryption (`tests/apps/encrypted`):
//! code reads and writes plaintext types, never ciphertext or equality
//! tokens, and a backend running from its bindings reaches the bundle the
//! files give, Jazz's system policies included.

use jazz::tools::deployment::artifact;
fn app_json() -> Vec<u8> {
    std::fs::read(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/apps/encrypted/jazz.app.json"),
    )
    .unwrap()
}

/// The members table's fixed policy refuses every update, so no language's
/// bindings give it an update, an upsert or update advice: its row, insert,
/// reads and deletes only. App tables keep theirs.
#[test]
fn members_table_gets_no_update_surface() {
    use jazz_codegen::{App, Lang};
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

/// A backend loading its permissions against the bindings' compiled app
/// keeps the encrypted app's declarations, so it computes the bundle the
/// app's files give: the declarations let it derive the system tables'
/// policies.
///
/// ```text
/// jazz.app.json ──compile_app──► bindings ──load_backend──► bundle, declarations
///        └──────────────────────load────────────────────► same
/// ```
#[test]
fn a_backend_from_bindings_keeps_the_encryption_declarations() {
    const PERMISSIONS: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json",
        "tables": {
            "projects": {
                "read": "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
                "insert": "$createdBy.account = session.user",
                "update": "ownerId = session.user"
            },
            "tasks": { "read": "allowedTo.read(project)", "insert": "allowedTo.update(project)" }
        } }"#;
    let bound = jazz_codegen::compile_app(&app_json()).unwrap();
    let from_bindings = artifact::load_backend(bound.as_bytes(), PERMISSIONS.as_bytes())
        .unwrap_or_else(|e| panic!("{e}"));
    let from_files = artifact::load(&app_json(), PERMISSIONS.as_bytes()).unwrap();
    assert_eq!(from_bindings.encryption(), from_files.encryption());
    assert!(
        from_bindings
            .encryption()
            .entries(&from_bindings.current_hash().to_string())
            .is_some_and(|entries| entries.contains_key("tasks")),
        "the bindings' backend lost the declarations"
    );
    assert_eq!(from_bindings.bundle(), from_files.bundle());
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
    let (current, from_bindings) =
        artifact::bound_declarations(bound.as_bytes(), app_id, "test", true).unwrap();
    assert_eq!(
        current,
        artifact::load_app(&app_json()).unwrap().current_hash()
    );
    let from_bindings = from_bindings.expect("declared");
    // Relay admission must verify the schema bytes before trusting this hash.
    let mut forged: serde_json::Value = serde_json::from_str(&bound).unwrap();
    let schema = forged["schema"]["schemas"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entry| entry["hash"] == current.to_string())
        .unwrap();
    let tables = schema["schema"]["tables"].as_object_mut().unwrap();
    tables.insert("invented".into(), tables.values().next().unwrap().clone());
    let forged = serde_json::to_vec(&forged).unwrap();
    assert!(
        artifact::bound_declarations(&forged, app_id, "test", true)
            .unwrap_err()
            .to_string()
            .contains("does not match its hash")
    );
    let (_, _, from_files) = artifact::load_client_encrypted(&app_json(), app_id, "test").unwrap();
    assert_eq!(from_bindings, from_files.expect("declared"));

    let plain = br#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string" } } } }"#;
    let bound = jazz_codegen::compile_app(plain).unwrap();
    assert!(
        artifact::bound_declarations(bound.as_bytes(), app_id, "test", false)
            .unwrap()
            .1
            .is_none()
    );
}
