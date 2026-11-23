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
