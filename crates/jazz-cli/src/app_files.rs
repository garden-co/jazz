//! `jazz-tools validate` and `jazz-tools migrations create` on an app's two
//! files and its committed bindings, without Node: what an app in any
//! language runs after editing `jazz.app.json`.

use std::path::{Path, PathBuf};

use jazz::tools::deployment::artifact::{self, APP_FILE, PERMISSIONS_FILE};
use jazz_codegen::bindings::{self, Staleness};
use jazz_codegen::{App, Lang};

/// What a command did, for the caller to print.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Lines for stdout.
    pub notes: Vec<String>,
    /// Lines for stderr (warnings).
    pub warnings: Vec<String>,
}

fn read(path: &Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))
}

fn read_optional(path: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}

fn bindings_path(dir: &Path, lang: Lang) -> PathBuf {
    dir.join(lang.file())
}

/// Check `dir`'s `jazz.app.json` (and `jazz.permissions.json`, when there
/// is one) and its bindings in `lang`. With `check`, refuse stale bindings;
/// otherwise regenerate them, unless `tables` changed without a migration
/// (the bindings hold the only copy of the version to migrate from).
pub fn validate(dir: &Path, lang: Lang, check: bool) -> Result<Outcome, String> {
    let mut outcome = Outcome::default();
    let app_json = read(&dir.join(APP_FILE))?;
    let app = App::load(&app_json).map_err(|error| error.to_string())?;
    outcome.warnings.extend(app.warnings.iter().cloned());
    if let Some(permissions) = read_optional(&dir.join(PERMISSIONS_FILE))? {
        artifact::load(&app_json, permissions.as_bytes()).map_err(|error| error.to_string())?;
    }
    let path = bindings_path(dir, lang);
    let committed = read_optional(&path)?;
    let staleness = bindings::staleness(&app, committed.as_deref()).map_err(|e| e.to_string())?;
    match staleness {
        None => {
            outcome
                .notes
                .push(format!("{} and {} are up to date", APP_FILE, lang.file()));
            Ok(outcome)
        }
        Some(Staleness::SchemaChanged) => Err(Staleness::SchemaChanged.message()),
        Some(stale) if check => Err(stale.message()),
        Some(_) => {
            let generated =
                jazz_codegen::generate_app(lang, &app).map_err(|error| error.to_string())?;
            std::fs::write(&path, generated)
                .map_err(|error| format!("{}: {error}", path.display()))?;
            outcome.notes.push(format!("wrote {}", path.display()));
            Ok(outcome)
        }
    }
}

/// Write the migration from the version `dir`'s bindings were generated
/// from to its edited `tables`, named `name`, and regenerate the bindings
/// once the migration has no TODOs left.
pub fn migrations_create(dir: &Path, lang: Lang, name: &str) -> Result<Outcome, String> {
    let mut outcome = Outcome::default();
    let app_path = dir.join(APP_FILE);
    let app_json = read(&app_path)?;
    let path = bindings_path(dir, lang);
    let committed = read_optional(&path)?;
    let created = jazz_codegen::migrations::create(&app_json, committed.as_deref(), name)
        .map_err(|error| error.to_string())?;
    if !created.changed {
        outcome.notes.push(match committed {
            None => format!(
                "no {}, so no earlier version to migrate from: run `jazz-tools validate`",
                lang.file()
            ),
            Some(_) => "no schema change to migrate".into(),
        });
        return Ok(outcome);
    }
    std::fs::write(&app_path, &created.app)
        .map_err(|error| format!("{}: {error}", app_path.display()))?;
    outcome.notes.push(format!(
        "wrote migration `{name}` in {}",
        app_path.display()
    ));
    if created.todos.is_empty() {
        outcome.notes.extend(validate(dir, lang, false)?.notes);
    } else {
        outcome.notes.push(format!(
            "resolve {} TODO{} in {}, then run `jazz-tools validate`:",
            created.todos.len(),
            if created.todos.len() == 1 { "" } else { "s" },
            APP_FILE
        ));
        outcome
            .notes
            .extend(created.todos.iter().map(|path| format!("  {path}")));
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: &str =
        r#"{ "format": 1, "tables": { "todos": { "columns": { "title": "string" } } } }"#;
    const V2: &str = r#"{ "format": 1, "tables": { "todos": { "columns": { "title": "string", "done": { "type": "boolean", "default": false } } } } }"#;

    fn dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "jazz-cli-app-files-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn validate_writes_checks_and_refuses_an_unmigrated_edit() {
        let dir = dir();
        std::fs::write(dir.join(APP_FILE), V1).unwrap();
        let ts = Lang::TypeScript;
        // No bindings: --check refuses, validate writes them.
        assert!(
            validate(&dir, ts, true)
                .unwrap_err()
                .contains("run `jazz-tools validate`")
        );
        validate(&dir, ts, false).unwrap();
        validate(&dir, ts, true).unwrap();
        // Reformatting the bindings doesn't make them stale.
        let path = dir.join(ts.file());
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(
            &path,
            text.replace("  readonly id: string;", "    readonly id: string;"),
        )
        .unwrap();
        validate(&dir, ts, true).unwrap();
        // Editing `tables` needs a migration first, and validate leaves the
        // bindings alone until there is one.
        std::fs::write(dir.join(APP_FILE), V2).unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        let error = validate(&dir, ts, false).unwrap_err();
        assert!(error.contains("migrations create --name"), "{error}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
        let outcome = migrations_create(&dir, ts, "done-flag").unwrap();
        assert!(
            outcome
                .notes
                .iter()
                .any(|n| n.contains("wrote migration `done-flag`")),
            "{outcome:?}"
        );
        validate(&dir, ts, true).unwrap();
        // A permissions file is checked too.
        std::fs::write(
            dir.join(PERMISSIONS_FILE),
            r#"{ "format": 1, "tables": { "todo": {} } }"#,
        )
        .unwrap();
        let error = validate(&dir, ts, true).unwrap_err();
        assert!(error.contains("did you mean `todos`?"), "{error}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn migrations_create_lists_the_todos_it_leaves() {
        let dir = dir();
        std::fs::write(dir.join(APP_FILE), V1).unwrap();
        validate(&dir, Lang::TypeScript, false).unwrap();
        std::fs::write(
            dir.join(APP_FILE),
            r#"{ "format": 1, "tables": { "todos": { "columns": { "title": "string", "owner": "uuid" } } } }"#,
        )
        .unwrap();
        let outcome = migrations_create(&dir, Lang::TypeScript, "owner").unwrap();
        assert!(
            outcome.notes.iter().any(|n| n.contains("resolve 1 TODO")),
            "{outcome:?}"
        );
        assert!(
            outcome
                .notes
                .iter()
                .any(|n| n == "  migrations[0].migrate.todos.owner.default"),
            "{outcome:?}"
        );
        let error = validate(&dir, Lang::TypeScript, false).unwrap_err();
        assert!(error.contains("1 unresolved TODO"), "{error}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
