//! A plain app adding its first encrypted table: `migrations create` writes
//! a migration that loads, as the deploy loads it.

use jazz_codegen::migrations::create;
use jazz_codegen::{App, Lang, generate};

const PLAIN: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "todos": { "columns": { "title": "string" } }
  }
}
"#;

const WITH_NOTES: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "notes": {
      "columns": { "detail": "bytes" },
      "encryption": { "space": "id", "scope": "notes", "columns": { "detail": "string" } }
    },
    "todos": { "columns": { "title": "string" } }
  }
}
"#;

#[test]
fn a_plain_app_adds_its_first_encrypted_table() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        let committed = generate(lang, PLAIN.as_bytes()).unwrap();
        let created = create(lang, WITH_NOTES.as_bytes(), Some(&committed), "notes")
            .unwrap_or_else(|error| panic!("{lang:?}: {}", error.message()));
        assert!(
            created.changed && created.todos.is_empty(),
            "{lang:?}: {created:?}"
        );
        let app = App::load(created.app.as_bytes())
            .unwrap_or_else(|error| panic!("{lang:?}: {error}\n{}", created.app));
        assert_eq!(app.compiled.schemas.len(), 2, "{lang:?}");
        // The next version generates from it.
        generate(lang, created.app.as_bytes()).unwrap();
    }
}

/// Alice upgrades a shipped v1 app, then archives its v2 successor. Each
/// source version reconstructs the schema its generated bindings recorded.
#[test]
fn system_table_versions_survive_codegen_and_two_migrations() {
    let file = include_str!("apps/encrypted/jazz.app.json");
    let shipped = include_str!("apps/encrypted/jazz.generated.ts");
    let old_hash = "912a9aa6f088c330ad4cb16c7a4d746edb0014ec4c969a2bc1d55ebb49b34424";
    let old = App::load(file.as_bytes()).unwrap();
    assert_eq!(old.compiled.current.to_string(), old_hash);
    assert_eq!(
        generate(Lang::TypeScript, file.as_bytes()).unwrap(),
        shipped
    );

    let stable = create(
        Lang::TypeScript,
        file.as_bytes(),
        Some(shipped),
        "unchanged",
    )
    .unwrap();
    assert!(!stable.changed);
    assert_eq!(stable.app, file);
    let edit = file.replacen(
        r#""done": "boolean""#,
        r#""done": "boolean", "archived": "boolean?""#,
        1,
    );
    let new_version = create(Lang::TypeScript, edit.as_bytes(), Some(shipped), "archive").unwrap();
    let source: serde_json::Value = serde_json::from_str(&new_version.app).unwrap();
    assert_eq!(source["systemTablesVersion"], 2);
    assert!(new_version.todos.is_empty());

    let edited = file.replacen(r#""tables""#, r#""systemTablesVersion": 2, "tables""#, 1);
    let upgraded = create(
        Lang::TypeScript,
        edited.as_bytes(),
        Some(shipped),
        "upgrade",
    )
    .unwrap();
    let app = App::load(upgraded.app.as_bytes()).unwrap();
    assert_eq!(app.compiled.schemas[0].0.to_string(), old_hash);
    assert_ne!(app.compiled.current, old.compiled.current);
    let v2 = app.compiled.current;
    let bindings = generate(Lang::TypeScript, upgraded.app.as_bytes()).unwrap();

    let edited = upgraded.app.replacen(
        r#""done": "boolean""#,
        r#""done": "boolean", "doneAgain": "boolean?""#,
        1,
    );
    let next = create(
        Lang::TypeScript,
        edited.as_bytes(),
        Some(&bindings),
        "add_done",
    )
    .unwrap();
    let file: serde_json::Value = serde_json::from_str(&next.app).unwrap();
    assert!(
        file["history"][&old_hash[..12]]
            .get("systemTablesVersion")
            .is_none()
    );
    assert_eq!(
        file["history"][&v2.to_string()[..12]]["systemTablesVersion"],
        2
    );
    let app = App::load(next.app.as_bytes()).unwrap();
    assert_eq!(app.compiled.schemas[0].0.to_string(), old_hash);
    assert_eq!(app.compiled.schemas[1].0, v2);
    generate(Lang::TypeScript, next.app.as_bytes()).unwrap();
}
