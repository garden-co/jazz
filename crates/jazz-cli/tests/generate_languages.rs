//! `jazz-tools generate --check` picks an app's languages from the files
//! beside its `jazz.app.json`, as `npx jazz-tools generate` does: both run
//! jazz-codegen's `project` (jazz-tools' `src/dev/project.languages.test.ts`
//! drives the npm CLI on the same directories). Writing is refused: from
//! alpha.66 TypeScript is the only authoring source, so `npx jazz-tools
//! generate` writes the files and bindings from `schema.ts`.

use std::path::{Path, PathBuf};
use std::process::Command;

mod support;

const APP: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json", "tables": { "todos": { "columns": { "title": "string" } } } }"#;
const PERMISSIONS: &str = r#"{ "$schema": "https://jazz.tools/schemas/jazz.permissions.v1.json", "tables": { "todos": { "read": "true" } } }"#;

fn app_dir(name: &str) -> PathBuf {
    let root =
        std::env::temp_dir().join(format!("jazz-cli-languages-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let dir = root.join("app");
    std::fs::create_dir_all(&dir).unwrap();
    // A package.json two levels up is not this app's.
    std::fs::write(root.join("package.json"), "{}").unwrap();
    std::fs::write(dir.join("jazz.app.json"), APP).unwrap();
    std::fs::write(dir.join("jazz.permissions.json"), PERMISSIONS).unwrap();
    dir
}

fn check(dir: &Path) -> std::process::Output {
    Command::new(support::cargo_binary("jazz-tools"))
        .args(["generate", "--check", "--dir"])
        .arg(dir)
        .output()
        .unwrap()
}

#[test]
fn an_app_with_only_another_language_s_manifest_gets_no_ts_bindings() {
    for (marker, text) in [
        ("Package.swift", "// swift-tools-version:5.9\n"),
        ("build.gradle.kts", "plugins { kotlin(\"jvm\") }\n"),
        ("go.mod", "module example.com/app\n\ngo 1.22\n"),
    ] {
        let dir = app_dir(&marker.replace('.', "-"));
        std::fs::write(dir.join(marker), text).unwrap();
        let output = check(&dir);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{marker}: {stderr}");
        assert!(
            stderr.contains(
                "nothing beside jazz.app.json says which language to generate bindings in"
            ),
            "{marker}: {stderr}"
        );
        assert!(!dir.join("jazz.generated.ts").exists(), "{marker}");
    }
}

#[test]
fn a_ts_app_s_bindings_are_checked_against_exactly_the_bytes_the_shared_code_writes() {
    let dir = app_dir("ts");
    std::fs::write(dir.join("package.json"), "{}").unwrap();
    std::fs::write(dir.join("Package.swift"), "// swift-tools-version:5.9\n").unwrap();
    // No bindings yet: the check fails and writes nothing.
    let output = check(&dir);
    assert!(!output.status.success(), "missing bindings passed");
    assert!(!dir.join("jazz.generated.ts").exists());

    std::fs::write(
        dir.join("jazz.generated.ts"),
        jazz_codegen::generate(jazz_codegen::Lang::TypeScript, APP.as_bytes()).unwrap(),
    )
    .unwrap();
    let output = check(&dir);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn a_json_only_app_is_pointed_at_npx_jazz_tools_upgrade() {
    let dir = app_dir("json-only");
    std::fs::write(dir.join("package.json"), "{}").unwrap();
    for args in [
        &["generate"][..],
        &["migrations", "create", "--name", "x"][..],
    ] {
        let output = run(&dir, args);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{args:?}: {stderr}");
        assert!(
            stderr.contains(
                "TS is the source in alpha.66: this app has jazz.app.json but no schema.ts. \
                 Write schema.ts from it with `npx jazz-tools@2.0.0-alpha.70 upgrade`"
            ),
            "{args:?}: {stderr}"
        );
        assert!(!dir.join("jazz.generated.ts").exists(), "{args:?}");
        assert!(!dir.join("migrations").exists(), "{args:?}");
        assert_eq!(
            std::fs::read_to_string(dir.join("jazz.app.json")).unwrap(),
            APP
        );
    }
}

fn run(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(support::cargo_binary("jazz-tools"))
        .args(args)
        .arg("--dir")
        .arg(dir)
        .output()
        .unwrap()
}

#[test]
fn a_ts_authored_app_is_left_to_npx_jazz_tools() {
    for root in [".", "src", "src/lib"] {
        let dir = app_dir(&format!("ts-authored-{}", root.replace(['.', '/'], "_")));
        std::fs::write(dir.join("package.json"), "{}").unwrap();
        std::fs::create_dir_all(dir.join(root)).unwrap();
        std::fs::write(dir.join(root).join("schema.ts"), "export {};\n").unwrap();
        for args in [
            &["generate"][..],
            &["migrations", "create", "--name", "x"][..],
        ] {
            let output = run(&dir, args);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!output.status.success(), "{root} {args:?}: {stderr}");
            assert!(
                stderr.contains("schema.ts exists") && stderr.contains("npx jazz-tools"),
                "{root} {args:?}: {stderr}"
            );
            assert!(!dir.join("jazz.generated.ts").exists(), "{root}");
        }
    }
}

#[test]
fn a_ts_authored_app_s_bindings_are_checked_against_its_committed_files() {
    let dir = app_dir("ts-authored-check");
    std::fs::write(dir.join("package.json"), "{}").unwrap();
    // Committed files as `npx jazz-tools generate` writes them, and their bindings.
    for (file, text) in [
        ("jazz.app.json", APP),
        ("jazz.permissions.json", PERMISSIONS),
    ] {
        let marked = jazz::tools::deployment::artifact::mark_generated(
            text.as_bytes(),
            jazz::tools::deployment::artifact::GENERATED_BY_TS,
        )
        .unwrap();
        std::fs::write(dir.join(file), marked).unwrap();
    }
    let app = std::fs::read_to_string(dir.join("jazz.app.json")).unwrap();
    std::fs::write(
        dir.join("jazz.generated.ts"),
        jazz_codegen::generate(jazz_codegen::Lang::TypeScript, app.as_bytes()).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("schema.ts"), "export {};\n").unwrap();

    let output = run(&dir, &["generate", "--check"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("npx jazz-tools generate --check"),
        "{stdout}"
    );

    // Stale bindings fail the check.
    std::fs::write(dir.join("jazz.generated.ts"), "// stale\n").unwrap();
    let output = run(&dir, &["generate", "--check"]);
    assert!(!output.status.success(), "stale bindings passed");
    assert_eq!(
        std::fs::read_to_string(dir.join("jazz.generated.ts")).unwrap(),
        "// stale\n"
    );

    // So does a committed file edited by hand.
    std::fs::write(
        dir.join("jazz.generated.ts"),
        jazz_codegen::generate(jazz_codegen::Lang::TypeScript, app.as_bytes()).unwrap(),
    )
    .unwrap();
    let edited = app.replace("\"title\": \"string\"", "\"title\": \"string?\"");
    assert_ne!(edited, app);
    std::fs::write(dir.join("jazz.app.json"), edited).unwrap();
    let output = run(&dir, &["generate", "--check"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "a hand edit passed");
    assert!(stderr.contains("was edited since"), "{stderr}");
}

#[test]
fn removed_upgrade_refuses_with_a_pointer_and_validate_is_unknown() {
    let dir = app_dir("removed-upgrade");
    std::fs::write(dir.join("jazz.app.json"), APP).unwrap();
    for args in [&["upgrade"][..], &["upgrade", "--help"][..]] {
        let output = run(&dir, args);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{args:?}: {stderr}");
        assert!(
            stderr.contains(
                "`jazz-tools upgrade` was removed in 2.0.0-beta.0. To convert an alpha.65 (or \
                 older) app, run `npx jazz-tools@2.0.0-alpha.70 upgrade`, then follow the \
                 upgrade guide: \
                 https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha70-to-beta0"
            ),
            "{args:?}: {stderr}"
        );
        assert!(!dir.join("schema.ts").exists(), "{args:?}");
        assert_eq!(
            std::fs::read_to_string(dir.join("jazz.app.json")).unwrap(),
            APP
        );
    }
    let output = run(&dir, &["validate"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        !stderr.contains("renamed to `jazz-tools generate`"),
        "{stderr}"
    );
}
