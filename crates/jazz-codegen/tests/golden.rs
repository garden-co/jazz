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
        let recorded = bindings::read(&generated).unwrap();
        assert_eq!(recorded.source, app.source);
        assert_eq!(recorded.current, app.compiled.current);
        assert_eq!(bindings::staleness(&app, Some(&generated)).unwrap(), None);
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
