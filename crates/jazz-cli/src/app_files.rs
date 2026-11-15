//! `jazz-tools generate` and `jazz-tools migrations create` on an app's two
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

/// A language bindings are generated in, and whether `jazz-tools` found it
/// by itself rather than being told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Selected {
    pub lang: Lang,
    /// Found by its marker alone (no `--lang`, no committed bindings):
    /// a language its back end can't handle the app in yet is skipped
    /// with a warning rather than refused.
    pub detected: bool,
}

impl From<Lang> for Selected {
    fn from(lang: Lang) -> Self {
        Self {
            lang,
            detected: false,
        }
    }
}

/// The languages `dir`'s app is written in: those given, or else each that
/// [`Lang::detected_in`] finds there (a `package.json`; a `Cargo.toml` of
/// a crate depending on `jazz-app`; a `go.mod` requiring `jazz-go`) or
/// whose bindings are committed there.
pub fn languages(dir: &Path, given: &[Lang]) -> Result<Vec<Selected>, String> {
    if !given.is_empty() {
        return Ok(given.iter().copied().map(Selected::from).collect());
    }
    let found: Vec<Selected> = Lang::ALL
        .iter()
        .copied()
        .filter_map(|lang| {
            if bindings_path(dir, lang).exists() {
                Some(Selected::from(lang))
            } else {
                lang.detected_in(dir).then_some(Selected {
                    lang,
                    detected: true,
                })
            }
        })
        .collect();
    if found.is_empty() {
        return Err(format!(
            "{}: nothing beside {APP_FILE} says which language to generate bindings in \
             (a package.json, the Cargo.toml of a crate depending on jazz-app, or a go.mod requiring jazz-go); pass --lang",
            dir.display(),
        ));
    }
    Ok(found)
}

/// Check `dir`'s `jazz.app.json` (and `jazz.permissions.json`, when there
/// is one) and its bindings in each of `langs`. With `check`, refuse stale
/// bindings; otherwise regenerate them, unless `tables` changed without a
/// migration (the bindings hold the only copy of the version to migrate
/// from), which is refused before any file is written.
pub fn generate(dir: &Path, langs: &[Selected], check: bool) -> Result<Outcome, String> {
    let mut outcome = Outcome::default();
    let app_json = read(&dir.join(APP_FILE))?;
    let app = App::load(&app_json).map_err(|error| error.to_string())?;
    outcome.warnings.extend(app.warnings.iter().cloned());
    if let Some(permissions) = read_optional(&dir.join(PERMISSIONS_FILE))? {
        artifact::load(&app_json, permissions.as_bytes()).map_err(|error| error.to_string())?;
    }
    let mut stale = Vec::new();
    let mut skipped = Vec::new();
    let mut current = 0;
    for &selected in langs {
        let lang = selected.lang;
        let path = bindings_path(dir, lang);
        let committed = read_optional(&path)?;
        match bindings::staleness(&app, lang, committed.as_deref()).map_err(|e| e.to_string())? {
            None => {
                current += 1;
                outcome
                    .notes
                    .push(format!("{} is up to date", path.display()));
            }
            Some(Staleness::SchemaChanged) => {
                return Err(format!(
                    "{}: {}",
                    path.display(),
                    Staleness::SchemaChanged.message(lang)
                ));
            }
            Some(Staleness::Unreadable(why)) if !check => {
                outcome.warnings.push(format!(
                    "{why}; regenerating it, so a change of `tables` since can't be caught this time"
                ));
                stale.push((selected, path, Staleness::Unreadable(why)));
            }
            Some(staleness) => stale.push((selected, path, staleness)),
        }
    }
    // Generate every language before writing any, so a refusal in one
    // leaves no file half-updated.
    let mut generated = Vec::new();
    for (selected, path, staleness) in stale {
        let text = match jazz_codegen::generate_app(selected.lang, &app) {
            Ok(text) => text,
            // Found only by its marker: not this app's language yet.
            Err(error) if selected.detected && error.is_unsupported() => {
                skipped.push(format!("{}: {error}", path.display()));
                continue;
            }
            Err(error) => return Err(format!("{}: {error}", path.display())),
        };
        if check {
            return Err(format!(
                "{}: {}",
                path.display(),
                staleness.message(selected.lang)
            ));
        }
        generated.push((path, text));
    }
    if !skipped.is_empty() {
        // Some other language must be the app's.
        if current == 0 && generated.is_empty() {
            return Err(skipped.join("\n"));
        }
        outcome.warnings.extend(
            skipped
                .into_iter()
                .map(|why| format!("skipped {why} (pass --lang to make this an error)")),
        );
    }
    for (path, text) in generated {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("{}: {error}", parent.display()))?;
        }
        std::fs::write(&path, text).map_err(|error| format!("{}: {error}", path.display()))?;
        outcome.notes.push(format!("wrote {}", path.display()));
    }
    Ok(outcome)
}

/// Write the migration from the version `dir`'s committed bindings were
/// generated from to its edited `tables`, named `name`, and regenerate the
/// bindings in each of `langs` once the migration has no TODOs left.
pub fn migrations_create(dir: &Path, langs: &[Selected], name: &str) -> Result<Outcome, String> {
    let mut outcome = Outcome::default();
    let app_path = dir.join(APP_FILE);
    let app_json = read(&app_path)?;
    // Every language's bindings record the same version; the first there is
    // tells it.
    let mut committed = None;
    for &Selected { lang, .. } in langs {
        if let Some(text) = read_optional(&bindings_path(dir, lang))? {
            committed = Some((lang, text));
            break;
        }
    }
    let Some((lang, text)) = committed else {
        outcome.notes.push(
            "no committed bindings, so no earlier version to migrate from: run `jazz-tools generate`"
                .into(),
        );
        return Ok(outcome);
    };
    let created = jazz_codegen::migrations::create(lang, &app_json, Some(&text), name)
        .map_err(|error| error.to_string())?;
    if !created.changed {
        outcome.notes.push(
            created
                .note
                .unwrap_or_else(|| "no schema change to migrate".into()),
        );
        return Ok(outcome);
    }
    std::fs::write(&app_path, &created.app)
        .map_err(|error| format!("{}: {error}", app_path.display()))?;
    outcome.notes.push(format!(
        "wrote migration `{name}` in {}",
        app_path.display()
    ));
    if created.todos.is_empty() {
        outcome.notes.extend(generate(dir, langs, false)?.notes);
    } else {
        outcome.notes.push(format!(
            "resolve {} TODO{} in {}, then run `jazz-tools generate`:",
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
    fn generate_writes_checks_and_refuses_an_unmigrated_edit() {
        let dir = dir();
        std::fs::write(dir.join(APP_FILE), V1).unwrap();
        let ts = Lang::TypeScript;
        // No bindings: --check refuses, generate writes them.
        assert!(
            generate(&dir, &[ts.into()], true)
                .unwrap_err()
                .contains("run `jazz-tools generate`")
        );
        generate(&dir, &[ts.into()], false).unwrap();
        generate(&dir, &[ts.into()], true).unwrap();
        // Reformatting the bindings doesn't make them stale.
        let path = dir.join(ts.file());
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(
            &path,
            text.replace("  readonly id: string;", "    readonly id: string;"),
        )
        .unwrap();
        generate(&dir, &[ts.into()], true).unwrap();
        // Editing `tables` needs a migration first, and generate leaves the
        // bindings alone until there is one.
        std::fs::write(dir.join(APP_FILE), V2).unwrap();
        let before = std::fs::read_to_string(&path).unwrap();
        let error = generate(&dir, &[ts.into()], false).unwrap_err();
        assert!(error.contains("migrations create --name"), "{error}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
        let outcome = migrations_create(&dir, &[ts.into()], "done-flag").unwrap();
        assert!(
            outcome
                .notes
                .iter()
                .any(|n| n.contains("wrote migration `done-flag`")),
            "{outcome:?}"
        );
        generate(&dir, &[ts.into()], true).unwrap();
        // A permissions file is checked too.
        std::fs::write(
            dir.join(PERMISSIONS_FILE),
            r#"{ "format": 1, "tables": { "todo": {} } }"#,
        )
        .unwrap();
        let error = generate(&dir, &[ts.into()], true).unwrap_err();
        assert!(error.contains("did you mean `todos`?"), "{error}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn migrations_create_lists_the_todos_it_leaves() {
        let dir = dir();
        std::fs::write(dir.join(APP_FILE), V1).unwrap();
        generate(&dir, &[Lang::TypeScript.into()], false).unwrap();
        std::fs::write(
            dir.join(APP_FILE),
            r#"{ "format": 1, "tables": { "todos": { "columns": { "title": "string", "owner": "uuid" } } } }"#,
        )
        .unwrap();
        let outcome = migrations_create(&dir, &[Lang::TypeScript.into()], "owner").unwrap();
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
        let error = generate(&dir, &[Lang::TypeScript.into()], false).unwrap_err();
        assert!(error.contains("1 unresolved TODO"), "{error}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn languages_come_from_the_files_beside_the_app() {
        let dir = dir();
        let error = languages(&dir, &[]).unwrap_err();
        assert!(error.contains("pass --lang"), "{error}");
        std::fs::write(dir.join("package.json"), "{}").unwrap();
        assert_eq!(
            languages(&dir, &[]).unwrap(),
            vec![Selected {
                lang: Lang::TypeScript,
                detected: true
            }]
        );
        // A workspace root's Cargo.toml beside a TS app is not a Rust app.
        std::fs::write(
            dir.join("Cargo.toml"),
            "[workspace]\nmembers = [\"crates/*\"]\n",
        )
        .unwrap();
        assert_eq!(languages(&dir, &[]).unwrap().len(), 1);
        std::fs::write(dir.join(APP_FILE), V1).unwrap();
        generate(&dir, &languages(&dir, &[]).unwrap(), false).unwrap();
        generate(&dir, &languages(&dir, &[]).unwrap(), true).unwrap();
        assert!(!dir.join(Lang::Rust.file()).exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    const RUST_APP: &str = "[package]\nname = \"todos\"\n\n[dependencies]\njazz-app = \"0.1\"\n";

    #[test]
    fn a_rust_app_generates_checks_and_migrates() {
        let dir = dir();
        std::fs::write(dir.join(APP_FILE), V1).unwrap();
        std::fs::write(dir.join("Cargo.toml"), RUST_APP).unwrap();
        let langs = languages(&dir, &[]).unwrap();
        assert_eq!(
            langs.iter().map(|s| s.lang).collect::<Vec<_>>(),
            [Lang::Rust]
        );
        assert!(
            generate(&dir, &langs, true)
                .unwrap_err()
                .contains("src/jazz_app.rs")
        );
        generate(&dir, &langs, false).unwrap();
        assert!(dir.join("src/jazz_app.rs").exists());
        // Committed bindings: no longer only detected.
        let langs = languages(&dir, &[]).unwrap();
        generate(&dir, &langs, true).unwrap();
        // The Rust bindings hold the version to migrate from.
        std::fs::write(dir.join(APP_FILE), V2).unwrap();
        assert!(
            generate(&dir, &langs, false)
                .unwrap_err()
                .contains("migrations create")
        );
        let outcome = migrations_create(&dir, &langs, "done-flag").unwrap();
        assert!(
            outcome
                .notes
                .iter()
                .any(|n| n.contains("wrote migration `done-flag`")),
            "{outcome:?}"
        );
        generate(&dir, &langs, true).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A payload enum, which Rust bindings can't express yet.
    const PAYLOAD_ENUM: &str = r#"{ "format": 1, "tables": { "todos": { "columns": { "status": { "enum": { "open": {}, "done": { "at": "timestamp" } } } } } } }"#;

    #[test]
    fn a_language_found_only_by_its_marker_is_skipped_where_unsupported() {
        let dir = dir();
        std::fs::write(dir.join(APP_FILE), PAYLOAD_ENUM).unwrap();
        std::fs::write(dir.join("package.json"), "{}").unwrap();
        std::fs::write(dir.join("Cargo.toml"), RUST_APP).unwrap();
        let langs = languages(&dir, &[]).unwrap();
        assert_eq!(langs.len(), 2);
        let outcome = generate(&dir, &langs, false).unwrap();
        assert!(
            outcome
                .warnings
                .iter()
                .any(|w| w.contains("skipped") && w.contains("enum-with-payload")),
            "{outcome:?}"
        );
        assert!(dir.join(Lang::TypeScript.file()).exists());
        assert!(!dir.join(Lang::Rust.file()).exists());
        generate(&dir, &languages(&dir, &[]).unwrap(), true).unwrap();
        // Asked for, it is an error, and nothing is written.
        std::fs::remove_file(dir.join(Lang::TypeScript.file())).unwrap();
        let error =
            generate(&dir, &[Lang::TypeScript.into(), Lang::Rust.into()], false).unwrap_err();
        assert!(error.contains("enum-with-payload"), "{error}");
        assert!(!dir.join(Lang::TypeScript.file()).exists());
        // The only language found can't be skipped.
        std::fs::remove_file(dir.join("package.json")).unwrap();
        let error = generate(&dir, &languages(&dir, &[]).unwrap(), false).unwrap_err();
        assert!(error.contains("enum-with-payload"), "{error}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
