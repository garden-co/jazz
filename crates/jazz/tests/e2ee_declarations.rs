//! An app's files and Jazz's reserved `__e2ee_` table prefix: the reader
//! refuses an application table with it, whatever version of the app holds
//! it.

use jazz::tools::deployment::artifact::{ArtifactError, load_app};

fn refusal(result: Result<impl std::fmt::Debug, ArtifactError>) -> String {
    result.expect_err("refused").to_string()
}

/// No app was deployed with a table of the reserved prefix, so one is
/// refused in every version of the app file, an older one in `history`
/// included.
#[test]
fn an_app_table_with_the_reserved_prefix_is_refused_in_every_version() {
    let older = r#"{ "$schema": "https://jazz.tools/schemas/jazz.app.v1.json",
        "tables": { "notes": { "columns": { "title": "string", "done": "boolean" } } },
        "history": { "v1": { "tables": {
            "notes": { "columns": { "title": "string" } },
            "__e2ee_notes": { "columns": { "x": "string" } }
        } } },
        "migrations": [{ "from": "v1", "migrate": { "notes": { "done": { "add": "boolean", "default": false } } } }] }"#;
    let error = refusal(load_app(older.as_bytes()));
    assert!(error.contains("history"), "{error}");
    assert!(error.contains("are reserved for Jazz's own"), "{error}");
}
