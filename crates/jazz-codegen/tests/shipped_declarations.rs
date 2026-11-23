//! An encrypted column is stored as bytes whatever its plaintext type, so
//! editing only its declaration keeps the schema's hash. The bindings record
//! the declarations of the version they were generated from; `generate`,
//! `validate` and `migrations create` refuse an app file that declares that
//! same version differently, rather than overwrite what shipped.

use jazz_codegen::bindings::{self, Staleness};
use jazz_codegen::migrations::create;
use jazz_codegen::{App, Lang, generate};

/// Two encrypted tables (`projects` is its own space) and a plain one,
/// with `tasks.notes` declared as `notes` and `boards` given `boards`.
fn app(notes: &str, boards: &str) -> String {
    format!(
        r#"{{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {{
    "boards": {{ "columns": {boards} }},
    "projects": {{
      "columns": {{ "name": "bytes", "ownerId": "uuid" }},
      "encryption": {{ "space": "id", "scope": "projects", "columns": {{ "name": "string" }} }}
    }},
    "tasks": {{
      "columns": {{ "projectId": "uuid", "title": "bytes", "notes": "bytes", "done": "boolean" }},
      "relations": {{ "project": {{ "rel": "projects", "column": "projectId" }} }},
      "encryption": {{
        "space": "projectId",
        "scope": "projects",
        "columns": {{ "title": "string", "notes": {notes} }}
      }}
    }}
  }}
}}
"#
    )
}

const BOARDS: &str = r#"{ "title": "string" }"#;
const BOARDS_WITH_PRIORITY: &str = r#"{ "title": "string", "priority": "int?" }"#;

fn assert_names_the_change(error: &str, what: &str) {
    assert!(
        error.contains("tasks.notes")
            && error.contains("docs/e2ee/encrypting-existing-data")
            && error.contains("can't change"),
        "{what}: {error}"
    );
}

/// The reviewer's case: `notes` goes from `string?` to `int?` and nothing
/// stored changes. `generate` and `validate` (which read staleness) and
/// `migrations create` refuse it, before and after an unrelated change, and
/// the recorded `string?` stays the baseline.
#[test]
fn a_recorded_versions_declarations_cannot_change() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        let shipped = app(r#""string?""#, BOARDS);
        let committed = generate(lang, shipped.as_bytes()).unwrap();
        let retyped = app(r#""int?""#, BOARDS);
        let loaded = App::load(retyped.as_bytes()).unwrap();
        let error = bindings::staleness(&loaded, lang, Some(&committed))
            .expect_err("generate and validate refuse a recorded version declared anew");
        assert_names_the_change(error.message(), &format!("{lang:?} staleness"));
        let error = create(lang, retyped.as_bytes(), Some(&committed), "retype")
            .expect_err("migrations create refuses it too");
        assert_names_the_change(error.message(), &format!("{lang:?} migrations create"));

        // With an unrelated change too, the earlier version keeps what the
        // bindings recorded, and the type change is refused against it.
        let both = app(r#""int?""#, BOARDS_WITH_PRIORITY);
        let error = create(lang, both.as_bytes(), Some(&committed), "retype-and-add")
            .expect_err("the type change is refused against the recorded version");
        assert!(
            error.message().contains("notes"),
            "{lang:?}: {}",
            error.message()
        );
    }
}

/// Declarations as recorded: `generate` goes ahead, and an unrelated change
/// moves the version into `history` with its own declarations.
#[test]
fn unchanged_declarations_still_generate() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        let shipped = app(r#""string?""#, BOARDS);
        let committed = generate(lang, shipped.as_bytes()).unwrap();
        let loaded = App::load(shipped.as_bytes()).unwrap();
        assert_eq!(
            bindings::staleness(&loaded, lang, Some(&committed)).unwrap(),
            None,
            "{lang:?}"
        );
        // The same declarations written another way still count as the same.
        let respelled = app(r#"{ "type": "string", "optional": true }"#, BOARDS);
        let loaded = App::load(respelled.as_bytes()).unwrap();
        assert!(
            matches!(
                bindings::staleness(&loaded, lang, Some(&committed)),
                Ok(None | Some(Staleness::OutOfDate))
            ),
            "{lang:?}"
        );

        let added = app(r#""string?""#, BOARDS_WITH_PRIORITY);
        let created = create(lang, added.as_bytes(), Some(&committed), "add").unwrap();
        assert!(
            created.changed && created.todos.is_empty(),
            "{lang:?}: {created:?}"
        );
        let written: serde_json::Value = serde_json::from_str(&created.app).unwrap();
        let earlier = written["history"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        assert_eq!(
            earlier["tables"]["tasks"]["encryption"]["columns"]["notes"], "string?",
            "{lang:?}: {}",
            created.app
        );
    }
}

/// The version moved into `history` keeps its own declarations, not the
/// current ones: `notes` is the enum `[a]` in v1, and v2 appends `b`
/// (pinning `[a]` as its sealed type) beside an unrelated column.
#[test]
fn history_keeps_the_recorded_declarations_not_the_edited_ones() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        let v1 = app(r#"{ "enum": ["a"] }"#, BOARDS);
        let committed = generate(lang, v1.as_bytes()).unwrap();
        let v2 = app(
            r#"{ "type": { "enum": ["a", "b"] }, "sealedType": { "enum": ["a"] } }"#,
            BOARDS_WITH_PRIORITY,
        );
        let created = create(lang, v2.as_bytes(), Some(&committed), "widen").unwrap();
        assert!(
            created.changed && created.todos.is_empty(),
            "{lang:?}: {created:?}"
        );
        let written: serde_json::Value = serde_json::from_str(&created.app).unwrap();
        let earlier = written["history"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        assert_eq!(
            earlier["tables"]["tasks"]["encryption"]["columns"]["notes"],
            serde_json::json!({ "enum": ["a"] }),
            "{lang:?}: {}",
            created.app
        );
        assert_eq!(
            written["tables"]["tasks"]["encryption"]["columns"]["notes"]["sealedType"],
            serde_json::json!({ "enum": ["a"] }),
            "{lang:?}"
        );
    }
}
