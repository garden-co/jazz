//! The committed bindings the other tests use are what the Rust back end
//! of jazz-codegen generates for their fixtures. Regenerate them with
//! `JAZZ_UPDATE_BINDINGS=1 cargo test -p jazz-app --test bindings`.

use jazz_codegen::Lang;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
const BINDINGS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/bindings");

#[test]
fn committed_bindings_are_generated() {
    for app in ["todos", "tasks"] {
        let file = std::fs::read(format!("{FIXTURES}/{app}/jazz.app.json")).unwrap();
        let generated = jazz_codegen::generate(Lang::Rust, &file).unwrap();
        let path = format!("{BINDINGS}/{app}.rs");
        if std::env::var_os("JAZZ_UPDATE_BINDINGS").is_some() {
            std::fs::write(&path, &generated).unwrap();
            continue;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            committed == generated,
            "{path} is stale: run JAZZ_UPDATE_BINDINGS=1 cargo test -p jazz-app --test bindings"
        );
    }
}

#[test]
fn payload_enums_are_not_supported_yet() {
    let file = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../jazz/fixtures/app-artifact/shared-todos/jazz.app.json"
    ))
    .unwrap();
    let error = jazz_codegen::generate(Lang::Rust, &file).unwrap_err();
    assert!(error.is_unsupported(), "{error}");
    assert!(error.message().contains("enum-with-payload"), "{error}");
}
