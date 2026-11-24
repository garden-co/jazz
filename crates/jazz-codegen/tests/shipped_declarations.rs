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

/// `text` with every `from` replaced by `to`, exactly once.
fn replaced_once(text: &str, from: &str, to: &str) -> String {
    assert_eq!(text.matches(from).count(), 1, "`{from}` in {text}");
    text.replace(from, to)
}

/// `text` with its current `tables` replaced by `tables`, history and
/// migrations kept.
fn with_tables(text: &str, tables: &str) -> String {
    use jazz::tools::deployment::artifact::json;
    let mut root = json::parse(text.as_bytes()).unwrap();
    let json::Json::Object(top) = &mut root else {
        panic!("not an object")
    };
    let at = top.iter().position(|(k, _)| k == "tables").unwrap();
    top[at].1 = json::parse(tables.as_bytes()).unwrap();
    json::print(&root)
}

/// The tasks table of [`app`] without its encrypted `notes` column.
fn app_without_notes(boards: &str) -> String {
    replaced_once(
        &replaced_once(&app(r#""string?""#, boards), r#", "notes": "bytes""#, ""),
        r#", "notes": "string?""#,
        "",
    )
}

/// The second version of [`app`], which drops the encrypted `tasks.notes`
/// with the empty ciphertext as its backwards default, as the author
/// resolves the stub's TODO.
fn second_version(lang: Lang, committed: &str) -> String {
    let created = create(
        lang,
        app_without_notes(BOARDS).as_bytes(),
        Some(committed),
        "drop-notes",
    )
    .unwrap();
    assert_eq!(
        created.todos,
        ["migrations[0].migrate.tasks.notes.backwardsDefault"],
        "{lang:?}: {created:?}"
    );
    let at = created.app.find(r#""backwardsDefault": "TODO:"#).unwrap();
    let value = at + r#""backwardsDefault": "#.len();
    let end = value + 1 + created.app[value + 1..].find('"').unwrap();
    let resolved = format!("{}\"\"{}", &created.app[..value], &created.app[end + 1..]);
    App::load(resolved.as_bytes()).unwrap();
    resolved
}

/// The second version drops the encrypted `tasks.notes`, and the migration
/// keeps the empty ciphertext as its backwards default. Editing only that
/// default leaves the tables as the bindings recorded them, so no new
/// migration is needed, and the file must load: the loader's error is
/// returned rather than a note that only indexes changed.
#[test]
fn an_invalid_shipped_migration_is_refused_with_the_loaders_error() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        let v1 = app(r#""string?""#, BOARDS);
        let committed = generate(lang, v1.as_bytes()).unwrap();
        let second = second_version(lang, &committed);
        let shipped = generate(lang, second.as_bytes()).unwrap();
        let edited = replaced_once(
            &second,
            r#""backwardsDefault": """#,
            r#""backwardsDefault": "AQ==""#,
        );
        let Err(loader) = App::load(edited.as_bytes()) else {
            panic!("{lang:?}: the loader refuses a non-empty ciphertext default")
        };
        let loader = loader.to_string();
        let error = create(lang, edited.as_bytes(), Some(&shipped), "again")
            .expect_err("migrations create refuses it too");
        assert_eq!(error.message(), loader, "{lang:?}");
    }
}

/// Removing the only encryption declaration while keeping the column's
/// bytes leaves the stored tables as they were once Jazz's system tables
/// are added back. That is still the bindings' version, so its
/// declarations can't change.
#[test]
fn removing_the_last_declaration_is_refused() {
    const ENCRYPTED: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "notes": {
      "columns": { "title": "string", "detail": "bytes" },
      "encryption": { "space": "id", "scope": "notes", "columns": { "detail": "string" } }
    }
  }
}
"#;
    const PLAIN: &str = r#"{
  "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
  "tables": {
    "notes": { "columns": { "title": "string", "detail": "bytes" } }
  }
}
"#;
    for lang in [Lang::TypeScript, Lang::Rust] {
        let committed = generate(lang, ENCRYPTED.as_bytes()).unwrap();
        let error = create(lang, PLAIN.as_bytes(), Some(&committed), "plain")
            .expect_err("the declaration can't be removed from the recorded version");
        assert!(
            error.message().contains("can't change") && error.message().contains("notes"),
            "{lang:?}: {}",
            error.message()
        );
    }
}

/// A third version (#6296): the second version's migration has no `to`, so
/// the edited file doesn't load until `migrations create` names it. The
/// recorded declarations are still checked: of the first version, which
/// retired the encrypted `tasks.notes`, as of the second.
#[test]
fn a_third_version_still_checks_the_recorded_declarations() {
    for lang in [Lang::TypeScript, Lang::Rust] {
        let v1 = app(r#""string?""#, BOARDS);
        let committed = generate(lang, v1.as_bytes()).unwrap();
        let second = second_version(lang, &committed);
        let shipped = generate(lang, second.as_bytes()).unwrap();
        let v3_tables = {
            let v3 = app_without_notes(BOARDS_WITH_PRIORITY);
            let root: serde_json::Value = serde_json::from_str(&v3).unwrap();
            root["tables"].to_string()
        };
        let third = with_tables(&second, &v3_tables);
        assert!(
            App::load(third.as_bytes()).is_err(),
            "{lang:?}: #6296's case"
        );

        // As recorded: the third version is written, and loads.
        let created = create(lang, third.as_bytes(), Some(&shipped), "priority").unwrap();
        assert!(
            created.changed && created.todos.is_empty(),
            "{lang:?}: {created:?}"
        );
        assert_eq!(
            App::load(created.app.as_bytes())
                .unwrap()
                .compiled
                .schemas
                .len(),
            3,
            "{lang:?}"
        );

        // The first version's retired column declared anew.
        let rewritten = replaced_once(&third, r#""notes": "string?""#, r#""notes": "int?""#);
        let error = create(lang, rewritten.as_bytes(), Some(&shipped), "priority")
            .expect_err("the first version's declarations can't change");
        assert_names_the_change(error.message(), &format!("{lang:?}"));
    }
}
