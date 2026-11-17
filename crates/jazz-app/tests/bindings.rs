//! The committed bindings the other tests use are what the Rust back end
//! of jazz-codegen generates for their fixtures. Regenerate them with
//! `JAZZ_UPDATE_BINDINGS=1 cargo test -p jazz-app-support --test bindings`.

use jazz_codegen::Lang;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
const BINDINGS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/bindings");

#[test]
fn committed_bindings_are_generated() {
    for app in ["todos", "tasks", "guarded", "drafts", "clashes"] {
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
            "{path} is stale: run JAZZ_UPDATE_BINDINGS=1 cargo test -p jazz-app-support --test bindings"
        );
    }
}

/// An app declaring an enum whose cases carry fields is refused when it
/// loads, naming the column: Jazz has no such columns (jazz#5758).
#[test]
fn an_app_with_enum_cases_carrying_fields_is_refused() {
    let file = br#"{ "format": 1, "tables": { "todos": { "columns": { "status": { "enum": { "open": {}, "done": { "at": "timestamp" } } } } } } }"#;
    let error = jazz_codegen::generate(Lang::Rust, file)
        .unwrap_err()
        .to_string();
    assert!(error.contains("todos"), "{error}");
    assert!(error.contains("status"), "{error}");
    assert!(
        error.contains("enum cases with fields are not supported"),
        "{error}"
    );
}

/// `jazz-tools generate` reads committed Rust bindings back: what they
/// were generated from, for `--check` and `migrations create`.
#[test]
fn committed_bindings_read_back() {
    for app in ["todos", "tasks", "guarded", "drafts", "clashes"] {
        let file = std::fs::read(format!("{FIXTURES}/{app}/jazz.app.json")).unwrap();
        let committed = std::fs::read_to_string(format!("{BINDINGS}/{app}.rs")).unwrap();
        let loaded = jazz_codegen::App::load(&file).unwrap();
        let recorded = jazz_codegen::bindings::read(Lang::Rust, &committed).unwrap();
        assert_eq!(recorded.source, loaded.source);
        assert_eq!(recorded.format, Lang::Rust.format());
        assert_eq!(recorded.current, loaded.compiled.current);
        assert_eq!(
            jazz_codegen::bindings::staleness(&loaded, Lang::Rust, Some(&committed)).unwrap(),
            None
        );

        // Reformatting `APP` changes nothing; editing what it says does.
        let reformatted = committed.replacen("\"format\": 1", "\"format\":  1", 1);
        assert!(jazz_codegen::bindings::read(Lang::Rust, &reformatted).is_ok());
        let edited = committed.replacen(
            "\"tables\": {",
            "\"tables\": { \"extra\": { \"columns\": {} },",
            1,
        );
        let error = jazz_codegen::bindings::read(Lang::Rust, &edited).unwrap_err();
        assert!(
            error.message().contains("Restore it from version control"),
            "{error}"
        );
    }
    let foreign = "// some other file\n";
    assert!(jazz_codegen::bindings::read(Lang::Rust, foreign).is_err());
}
