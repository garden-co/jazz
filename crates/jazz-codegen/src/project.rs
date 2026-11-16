//! `jazz-tools generate [--check]` and `jazz-tools migrations create` on an
//! app's directory: its two files, the languages it's written in and their
//! committed bindings. The one implementation: the `jazz-tools` binary runs
//! it directly and the npm CLI through jazz-napi, so both pick the same
//! languages and write the same bytes.

use std::path::{Path, PathBuf};

use crate::bindings::{self, Staleness};
use crate::{App, Lang};
use jazz::tools::deployment::artifact::{self, APP_FILE, PERMISSIONS_FILE};

/// What a command did, for the caller to print.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Lines for stdout.
    pub notes: Vec<String>,
    /// Lines for stderr (warnings).
    pub warnings: Vec<String>,
    /// Bindings files written, relative to the directory.
    pub written: Vec<String>,
    /// Bindings files that were already up to date, relative to the directory.
    pub current: Vec<String>,
    /// `migrations create`: whether `jazz.app.json` changed.
    pub changed: bool,
    /// `migrations create`: JSON paths of the TODOs left to resolve.
    pub todos: Vec<String>,
}

fn read(path: &Path) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|error| format!("{}: {error}", shown(&path)))
}

fn read_optional(path: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("{}: {error}", shown(&path))),
    }
}

/// `path` as a message shows it: relative to the current directory when
/// it's below it, as both CLIs print paths.
fn shown<P: AsRef<Path> + ?Sized>(path: &P) -> String {
    let path = path.as_ref();
    let relative = std::env::current_dir()
        .ok()
        .and_then(|cwd| path.strip_prefix(cwd).ok().map(Path::to_path_buf));
    match relative {
        Some(relative) if relative.as_os_str().is_empty() => ".".into(),
        Some(relative) => relative.display().to_string(),
        None => path.display().to_string(),
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
    languages_or(dir, given, None)
}

/// Manifests of the ecosystems Jazz generates bindings for. A directory
/// with one of these belongs to that language even while its bindings are
/// switched off, so no fallback language is assumed there.
pub const LANGUAGE_MANIFESTS: &[&str] = &[
    "package.json",
    "Cargo.toml",
    "Package.swift",
    "build.gradle",
    "build.gradle.kts",
    "settings.gradle",
    "settings.gradle.kts",
    "go.mod",
];

/// [`languages`], and `fallback` when nothing is detected and no
/// [`LANGUAGE_MANIFESTS`] sits beside `jazz.app.json`: the npm CLI passes
/// TypeScript, as running it says the app is a Node one, but never where
/// another language's manifest says otherwise.
pub fn languages_or(
    dir: &Path,
    given: &[Lang],
    fallback: Option<Lang>,
) -> Result<Vec<Selected>, String> {
    if let Some(lang) = fallback
        && given.is_empty()
        && !Lang::ALL
            .iter()
            .any(|l| l.detected_in(dir) || bindings_path(dir, *l).exists())
        && !LANGUAGE_MANIFESTS.iter().any(|m| dir.join(m).exists())
    {
        return Ok(vec![Selected::from(lang)]);
    }
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
            shown(&dir),
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
                outcome.current.push(lang.file().to_string());
                outcome
                    .notes
                    .push(format!("{} is up to date", shown(&path)));
            }
            Some(Staleness::SchemaChanged) => {
                return Err(format!(
                    "{}: {}",
                    shown(&path),
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
        let text = match crate::generate_app(selected.lang, &app) {
            Ok(text) => text,
            // Found only by its marker: not this app's language yet.
            Err(error) if selected.detected && error.is_unsupported() => {
                skipped.push(format!("{}: {error}", shown(&path)));
                continue;
            }
            Err(error) => return Err(format!("{}: {error}", shown(&path))),
        };
        if check {
            return Err(match staleness {
                // "no <file>: …" names the file already.
                Staleness::Missing => Staleness::Missing.message(selected.lang),
                other => format!("{}: {}", shown(&path), other.message(selected.lang)),
            });
        }
        generated.push((selected.lang, path, text));
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
    for (lang, path, text) in generated {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("{}: {error}", shown(&parent)))?;
        }
        std::fs::write(&path, text).map_err(|error| format!("{}: {error}", shown(&path)))?;
        outcome.written.push(lang.file().to_string());
        outcome.notes.push(format!("wrote {}", shown(&path)));
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
    let created = crate::migrations::create(lang, &app_json, Some(&text), name)
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
        .map_err(|error| format!("{}: {error}", shown(&app_path)))?;
    outcome.changed = true;
    outcome.todos = created.todos.clone();
    outcome
        .notes
        .push(format!("wrote migration `{name}` in {}", shown(&app_path)));
    if created.todos.is_empty() {
        let generated = generate(dir, langs, false)?;
        outcome.notes.extend(generated.notes);
        outcome.warnings.extend(generated.warnings);
        outcome.written = generated.written;
        outcome.current = generated.current;
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

    /// Two enum values Rust names alike, which TS bindings express and Rust
    /// bindings refuse.
    const RUST_UNSUPPORTED: &str = r#"{ "format": 1, "tables": { "todos": { "columns": { "status": { "enum": ["in-progress", "in_progress"] } } } } }"#;

    #[test]
    fn a_language_found_only_by_its_marker_is_skipped_where_unsupported() {
        let dir = dir();
        std::fs::write(dir.join(APP_FILE), RUST_UNSUPPORTED).unwrap();
        std::fs::write(dir.join("package.json"), "{}").unwrap();
        std::fs::write(dir.join("Cargo.toml"), RUST_APP).unwrap();
        let langs = languages(&dir, &[]).unwrap();
        assert_eq!(langs.len(), 2);
        let outcome = generate(&dir, &langs, false).unwrap();
        assert!(
            outcome
                .warnings
                .iter()
                .any(|w| w.contains("skipped") && w.contains("Rust variant")),
            "{outcome:?}"
        );
        assert!(dir.join(Lang::TypeScript.file()).exists());
        assert!(!dir.join(Lang::Rust.file()).exists());
        generate(&dir, &languages(&dir, &[]).unwrap(), true).unwrap();
        // Asked for, it is an error, and nothing is written.
        std::fs::remove_file(dir.join(Lang::TypeScript.file())).unwrap();
        let error =
            generate(&dir, &[Lang::TypeScript.into(), Lang::Rust.into()], false).unwrap_err();
        assert!(error.contains("Rust variant"), "{error}");
        assert!(!dir.join(Lang::TypeScript.file()).exists());
        // The only language found can't be skipped.
        std::fs::remove_file(dir.join("package.json")).unwrap();
        let error = generate(&dir, &languages(&dir, &[]).unwrap(), false).unwrap_err();
        assert!(error.contains("Rust variant"), "{error}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;

    #[test]
    fn messages_show_paths_below_the_current_directory_relative_to_it() {
        let cwd = std::env::current_dir().unwrap();
        assert_eq!(
            shown(&cwd.join("app").join("jazz.generated.ts")),
            format!("app{}jazz.generated.ts", std::path::MAIN_SEPARATOR)
        );
        assert_eq!(shown(&cwd), ".");
        let outside = Path::new("/elsewhere/jazz.app.json");
        if !outside.starts_with(&cwd) {
            assert_eq!(shown(outside), "/elsewhere/jazz.app.json");
        }
    }

    /// A TS app's files may sit in `src/` or `src/lib/` of the package whose
    /// `package.json` says it's TS, where the npm CLI finds them.
    #[test]
    fn a_ts_app_is_found_from_its_package_above_src() {
        let root = std::env::temp_dir().join(format!("jazz-codegen-layout-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        for dir in ["src", "src/lib"] {
            let dir = root.join(dir);
            std::fs::create_dir_all(&dir).unwrap();
            assert!(languages(&dir, &[]).is_err(), "no package.json yet");
        }
        std::fs::write(root.join("package.json"), "{}").unwrap();
        for dir in ["", "src", "src/lib"] {
            assert_eq!(
                languages(&root.join(dir), &[]).unwrap(),
                vec![Selected {
                    lang: Lang::TypeScript,
                    detected: true
                }],
                "{dir}"
            );
        }
        // Only those: a package two levels up of another directory isn't.
        let other = root.join("src/other");
        std::fs::create_dir_all(&other).unwrap();
        assert!(languages(&other, &[]).is_err());
        // The npm CLI's fallback applies there, but not beside another
        // language's manifest.
        assert_eq!(
            languages_or(&other, &[], Some(Lang::TypeScript)).unwrap(),
            vec![Selected::from(Lang::TypeScript)]
        );
        for manifest in ["Package.swift", "build.gradle.kts", "go.mod"] {
            std::fs::write(other.join(manifest), "").unwrap();
            assert!(
                languages_or(&other, &[], Some(Lang::TypeScript)).is_err(),
                "{manifest}"
            );
            std::fs::remove_file(other.join(manifest)).unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
