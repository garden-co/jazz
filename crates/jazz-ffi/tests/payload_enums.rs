//! Enums with payloads: read as `{ "case", "fields" }`; writing them and
//! comparing them in a query are refused by name.

mod common;

use jazz_ffi::{JazzError, JazzHandle};
use serde_json::json;

const SHAPES_APP: &str = r#"{
  "format": 1,
  "tables": {
    "drawings": {
      "columns": {
        "label": "string",
        "shape": { "enum": { "circle": { "r": "float" }, "rect": { "w": "float", "h": "float" } } }
      }
    }
  }
}"#;

/// A filter on a payload-enum column would compare against nothing the
/// engine understands, so it is refused rather than matching wrongly; so is
/// writing one (the engine's client can't yet).
#[test]
fn payload_enums_are_refused_where_they_cannot_work() {
    let jazz = JazzHandle::open(json!({ "app": SHAPES_APP }).to_string()).unwrap();
    let not_supported = |result: Result<String, JazzError>| match result {
        Err(JazzError::Refused { reason }) if reason.starts_with("not supported yet") => reason,
        other => panic!("expected not supported yet, got {other:?}"),
    };
    let circle = json!({ "case": "circle", "fields": { "r": 2.5 } });
    assert!(
        not_supported(jazz.insert(
            "drawings".into(),
            json!({ "label": "dot", "shape": circle }).to_string()
        ))
        .contains("writing an enum with payloads")
    );
    assert!(not_supported(jazz.all(
        json!({ "table": "drawings", "where": [{ "column": "shape", "op": "eq", "value": circle }] })
            .to_string()
    ))
    .contains("comparing shape"));
    jazz.all(
        json!({ "table": "drawings", "where": [{ "column": "shape", "op": "isNotNull" }] })
            .to_string(),
    )
    .unwrap();
}
