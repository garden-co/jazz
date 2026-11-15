//! Each app under `tests/apps/` generates byte-stable bindings, committed
//! beside its `jazz.app.json`. `JAZZ_UPDATE_FIXTURES=1` rewrites them after
//! an intended change.

use std::path::PathBuf;

use jazz_codegen::{App, Lang, bindings, generate};

fn apps() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/apps");
    let mut apps: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.join("jazz.app.json").exists())
        .collect();
    apps.sort();
    apps
}

#[test]
fn every_app_generates_its_committed_typescript_byte_for_byte() {
    let apps = apps();
    assert!(apps.len() >= 3, "{apps:?}");
    for dir in apps {
        let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
        let generated = generate(Lang::TypeScript, &app_json)
            .unwrap_or_else(|error| panic!("{}: {error}", dir.display()));
        // Generating twice gives the same bytes.
        assert_eq!(generated, generate(Lang::TypeScript, &app_json).unwrap());
        let path = dir.join(jazz_codegen::ts::FILE);
        if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
            std::fs::write(&path, &generated).unwrap();
            continue;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!(
                "{} is missing; run with JAZZ_UPDATE_FIXTURES=1",
                path.display()
            )
        });
        assert_eq!(
            generated,
            committed,
            "{} differs from what the generator writes",
            path.display()
        );
    }
}

#[test]
fn committed_bindings_read_back_and_fit_their_app() {
    for dir in apps() {
        let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
        let app = App::load(&app_json).unwrap();
        let generated = generate(Lang::TypeScript, &app_json).unwrap();
        let recorded = bindings::read(Lang::TypeScript, &generated).unwrap();
        assert_eq!(recorded.source, app.source);
        assert_eq!(recorded.current, app.compiled.current);
        assert_eq!(
            bindings::staleness(&app, Lang::TypeScript, Some(&generated)).unwrap(),
            None
        );
    }
}

#[test]
fn the_embedded_schema_is_what_load_client_lowers() {
    use jazz::tools::deployment::artifact::{self, json};
    for dir in apps() {
        let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
        let options =
            json::parse(jazz_codegen::compile_app(&app_json).unwrap().as_bytes()).unwrap();
        let schema = json::print(options.get("schema").unwrap());
        let from_bindings =
            jazz::tools::deployment::published::lowered_client_migrations(Some(schema.as_bytes()))
                .unwrap();
        let from_file = artifact::load_client(&app_json).unwrap();
        assert_eq!(
            format!("{:?}", from_bindings.schemas),
            format!("{:?}", from_file.schemas),
            "{}",
            dir.display()
        );
        assert_eq!(
            format!("{:?}", from_bindings.lenses),
            format!("{:?}", from_file.lenses),
            "{}",
            dir.display()
        );
    }
}

#[test]
fn a_loaded_app_keeps_its_file_text_as_its_source_hashes_it() {
    let unix = b"{\n  \"format\": 1,\n  \"tables\": { \"t\": { \"columns\": { \"a\": \"string\" } } }\n}\n";
    let windows = String::from_utf8_lossy(unix).replace('\n', "\r\n");
    let (a, b) = (
        App::load(unix).unwrap(),
        App::load(windows.as_bytes()).unwrap(),
    );
    assert_eq!(a.file, String::from_utf8_lossy(unix));
    assert_eq!(a.file, b.file);
    assert_eq!(a.source, b.source);
}

#[test]
fn type_names_may_shadow_globals_the_module_never_uses() {
    let app = |table: &str| {
        format!(
            r#"{{ "format": 1, "tables": {{ "{table}": {{ "columns": {{ "at": "timestamp" }} }} }} }}"#
        )
    };
    let generated = generate(Lang::TypeScript, app("records").as_bytes()).unwrap();
    assert!(generated.contains("export type Record = {"), "{generated}");
    assert!(
        !generated.contains("Record<"),
        "the module must not use the global Record"
    );
    // `Date` is a type the module uses itself.
    let error = generate(Lang::TypeScript, app("dates").as_bytes()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("table `dates` would be the TypeScript type `Date`"),
        "{error}"
    );
}

#[test]
fn bindings_read_back_after_a_formatter_and_regenerate_when_unreadable() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/apps/tasks");
    let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
    let app = App::load(&app_json).unwrap();
    let generated = generate(Lang::TypeScript, &app_json).unwrap();
    // A BOM, CRLF, tabs and bare keys, as editors and formatters leave them.
    let reformatted = format!(
        "\u{feff}{}",
        generated
            .replace("  \"source\": ", "\tsource: ")
            .replace("  \"schema\": ", "\tschema:  ")
            .replace("  \"relations\": ", "\trelations: ")
            .replace('\n', "\r\n")
    );
    let recorded = bindings::read(Lang::TypeScript, &reformatted).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(recorded.source, app.source);
    assert_eq!(
        bindings::staleness(&app, Lang::TypeScript, Some(&reformatted)).unwrap(),
        None
    );
    // Bindings that can't be read are stale, not an error.
    let mangled = generated.replace("\"schema\": {", "\"schema\": {{");
    assert!(matches!(
        bindings::staleness(&app, Lang::TypeScript, Some(&mangled)).unwrap(),
        Some(bindings::Staleness::Unreadable(_))
    ));
}

#[test]
fn a_backend_with_only_the_bindings_loads_what_the_files_load() {
    use jazz::tools::deployment::artifact;
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/apps/chat-react");
    let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
    let permissions = std::fs::read(dir.join("jazz.permissions.json")).unwrap();
    let options = jazz_codegen::compile_app(&app_json).unwrap();
    let from_bindings =
        artifact::load_backend(options.as_bytes(), &permissions).unwrap_or_else(|e| panic!("{e}"));
    let from_files = artifact::load(&app_json, &permissions).unwrap();
    assert_eq!(from_bindings.bundle(), from_files.bundle());
    assert_eq!(from_bindings.permissions(), from_files.permissions());
    assert_eq!(
        from_bindings.deployment_body(false),
        from_files.deployment_body(false)
    );
}

/// Go bindings for each app Go can bind, committed as
/// `jazzapp/jazz_app.go` beside its `jazz.app.json`; the others are
/// refused as unsupported (computed fields, transforms).
#[test]
fn every_go_app_generates_its_committed_go_byte_for_byte() {
    let mut generated_any = 0;
    for dir in apps().into_iter().chain(go_apps()) {
        let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
        let path = dir.join(jazz_codegen::go::FILE);
        let generated = match generate(Lang::Go, &app_json) {
            Ok(generated) => generated,
            Err(error) => {
                assert!(error.is_unsupported(), "{}: {error}", dir.display());
                assert!(!path.exists(), "{}: refused, yet committed", dir.display());
                continue;
            }
        };
        generated_any += 1;
        assert_eq!(generated, generate(Lang::Go, &app_json).unwrap());
        let app = App::load(&app_json).unwrap();
        let recorded = bindings::read(Lang::Go, &generated).unwrap();
        assert_eq!(recorded.source, app.source);
        assert_eq!(recorded.current, app.compiled.current);
        assert_eq!(
            bindings::staleness(&app, Lang::Go, Some(&generated)).unwrap(),
            None
        );
        if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &generated).unwrap();
            continue;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!(
                "{} is missing; run with JAZZ_UPDATE_FIXTURES=1",
                path.display()
            )
        });
        assert_eq!(generated, committed, "{} differs", path.display());
    }
    assert!(generated_any >= 3);
}

#[test]
fn go_bindings_refuse_computed_fields_and_read_back_backquotes() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/apps/tasks");
    let error = generate(Lang::Go, &std::fs::read(dir.join("jazz.app.json")).unwrap()).unwrap_err();
    assert!(error.is_unsupported(), "{error}");
    assert!(
        error
            .to_string()
            .contains("not supported in Go bindings yet"),
        "{error}"
    );
    // A backquote in the file survives the raw string it is embedded in.
    let app_json = "{ \"format\": 1, \"tables\": { \"notes\": { \"columns\": { \"body\": { \"enum\": [\"a`b\", \"c\"] } } } } }\n";
    let generated = generate(Lang::Go, app_json.as_bytes()).unwrap();
    let app = App::load(app_json.as_bytes()).unwrap();
    assert_eq!(
        bindings::read(Lang::Go, &generated).unwrap().source,
        app.source
    );
    assert!(generated.contains("NoteBodyAB"), "{generated}");
    // An edited `App` is caught.
    let edited = generated.replace("\"c\"", "\"d\"");
    assert!(bindings::read(Lang::Go, &edited).is_err());
}

/// Go-only apps (what the other back ends refuse, such as tables named
/// `strings`), whose bindings CI also runs `go vet` on.
fn go_apps() -> Vec<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/go-apps");
    let mut apps: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.join("jazz.app.json").exists())
        .collect();
    apps.sort();
    apps
}

/// Names that look like the bindings' own (`source`, `app`, `open`) or a
/// Go builtin or import (`string`, `error`, `json`, `context`) are fields
/// and types of their own, and the bindings still read back.
#[test]
fn go_names_never_shadow_the_bindings_or_go() {
    let generated = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/go-apps/names/jazzapp/jazz_app.go"),
    )
    .unwrap();
    for expected in [
        "type String struct {",
        "\tSource string `json:\"source\"`",
        "\tApp    string `json:\"app\"`",
        "type Error struct {",
        "\tJSON    *json.RawMessage `json:\"json\"`",
        "\tErrorLevel1          ErrorLevel = \"1\"",
        "\tAt   jazz.Timestamp `json:\"at\"`",
        "\tBig  jazz.BigInt    `json:\"big\"`",
        "var Strings = stringsTable{",
        "var ActivityTable = activityTable{",
    ] {
        assert!(generated.contains(expected), "{expected:?} in\n{generated}");
    }
    let refused = |app_json: &str, name: &str| {
        let error = generate(Lang::Go, app_json.as_bytes()).unwrap_err();
        assert!(
            error.is_unsupported() && error.to_string().contains(name),
            "{error}"
        );
    };
    // A table whose row would be one of the bindings' own names.
    refused(
        r#"{ "format": 1, "tables": { "apps": { "columns": { "a": "string" } } } }"#,
        "`App`",
    );
    // A column whose Go name is a patch's or a table value's method.
    refused(
        r#"{ "format": 1, "tables": { "notes": { "columns": { "marshalJSON": "string" } } } }"#,
        "`MarshalJSON`",
    );
    refused(
        r#"{ "format": 1, "tables": { "notes": { "columns": { "query": "string" } } } }"#,
        "`Query`",
    );
}

#[test]
fn go_is_switched_off_in_the_cli() {
    let error = Lang::parse("go").unwrap_err();
    assert!(error.is_unsupported(), "{error}");
    assert!(error.to_string().contains("not available yet"), "{error}");
    assert!(!Lang::ALL.contains(&Lang::Go));
}

/// Each app's Swift bindings, committed beside it as `JazzApp.swift`; an
/// app the Swift back end refuses (computed fields, transforms) commits
/// none and is refused as unsupported.
#[test]
fn every_app_generates_its_committed_swift_byte_for_byte() {
    let mut generated_any = false;
    for dir in apps() {
        let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
        let path = dir.join(jazz_codegen::swift::FILE);
        let generated = match generate(Lang::Swift, &app_json) {
            Ok(generated) => generated,
            Err(error) => {
                assert!(error.is_unsupported(), "{}: {error}", dir.display());
                assert!(!path.exists(), "{} should not exist", path.display());
                continue;
            }
        };
        generated_any = true;
        assert_eq!(generated, generate(Lang::Swift, &app_json).unwrap());
        if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
            std::fs::write(&path, &generated).unwrap();
            continue;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_else(|_| {
            panic!(
                "{} is missing; run with JAZZ_UPDATE_FIXTURES=1",
                path.display()
            )
        });
        assert_eq!(
            generated,
            committed,
            "{} differs from what the generator writes",
            path.display()
        );
        let app = App::load(&app_json).unwrap();
        let recorded = bindings::read(Lang::Swift, &committed).unwrap();
        assert_eq!(recorded.source, app.source);
        assert_eq!(recorded.current, app.compiled.current);
        assert_eq!(recorded.format, Lang::Swift.format());
        assert_eq!(
            bindings::staleness(&app, Lang::Swift, Some(&committed)).unwrap(),
            None
        );
        // An edited embedded app is caught.
        let edited = committed.replacen(": \"string\"", ": \"boolean\"", 1);
        assert_ne!(edited, committed);
        assert!(bindings::read(Lang::Swift, &edited).is_err());
    }
    assert!(generated_any);
}

/// The StagePlan Swift example commits its bindings (`jazz-tools generate`
/// doesn't write Swift yet, so this test keeps them current).
/// `JAZZ_UPDATE_FIXTURES=1` rewrites them.
#[test]
fn the_stage_plan_swift_example_commits_its_bindings() {
    let dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/stage-plan/apps/swift-cli");
    let app_json = std::fs::read(dir.join("jazz.app.json")).unwrap();
    let generated = generate(Lang::Swift, &app_json).unwrap();
    let path = dir.join(jazz_codegen::swift::FILE);
    if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
        std::fs::write(&path, &generated).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        generated,
        committed,
        "{} is stale: run with JAZZ_UPDATE_FIXTURES=1",
        path.display()
    );
}

/// The Jazz Swift package's tests compile and run the bindings of
/// `tests/apps/computed` (computed fields and transforms) from a copy.
/// `JAZZ_UPDATE_FIXTURES=1` rewrites it.
#[test]
fn the_swift_package_tests_the_computed_bindings() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let app_json = std::fs::read(root.join("tests/apps/computed/jazz.app.json")).unwrap();
    let generated = generate(Lang::Swift, &app_json).unwrap();
    let path = root.join("../../packages/jazz-swift/Tests/JazzTests/ComputedApp.swift");
    if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
        if path.parent().unwrap().exists() {
            std::fs::write(&path, &generated).unwrap();
        }
        return;
    }
    // Absent where the package isn't in the checkout.
    if let Ok(committed) = std::fs::read_to_string(&path) {
        assert_eq!(
            generated,
            committed,
            "{} is stale: run with JAZZ_UPDATE_FIXTURES=1",
            path.display()
        );
    }
}
