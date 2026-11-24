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
