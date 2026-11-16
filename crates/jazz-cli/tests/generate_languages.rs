//! `jazz-tools generate` picks an app's languages from the files beside its
//! `jazz.app.json`, as `npx jazz-tools generate` does: both run
//! jazz-codegen's `project` (jazz-tools' `src/dev/project.languages.test.ts`
//! drives the npm CLI on the same directories).

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

fn generate(dir: &Path) -> std::process::Output {
    Command::new(support::cargo_binary("jazz-tools"))
        .args(["generate", "--dir"])
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
        let output = generate(&dir);
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
fn a_ts_app_gets_exactly_the_bytes_the_shared_code_writes() {
    let dir = app_dir("ts");
    std::fs::write(dir.join("package.json"), "{}").unwrap();
    std::fs::write(dir.join("Package.swift"), "// swift-tools-version:5.9\n").unwrap();
    let output = generate(&dir);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let written = std::fs::read_to_string(dir.join("jazz.generated.ts")).unwrap();
    assert_eq!(
        written,
        jazz_codegen::generate(jazz_codegen::Lang::TypeScript, APP.as_bytes()).unwrap()
    );
}

#[test]
fn a_ts_authored_app_is_left_to_npx_jazz_tools() {
    for root in [".", "src", "src/lib"] {
        let dir = app_dir(&format!("ts-authored-{}", root.replace(['.', '/'], "_")));
        std::fs::write(dir.join("package.json"), "{}").unwrap();
        std::fs::create_dir_all(dir.join(root)).unwrap();
        std::fs::write(dir.join(root).join("schema.ts"), "export {};\n").unwrap();
        for args in [
            &["generate", "--dir"][..],
            &["migrations", "create", "--name", "x", "--dir"][..],
        ] {
            let output = Command::new(support::cargo_binary("jazz-tools"))
                .args(args)
                .arg(&dir)
                .output()
                .unwrap();
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
