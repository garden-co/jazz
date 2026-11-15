//! jazz-ffi v2: reading part of a large value (`select` parts).

mod common;

use common::*;
use jazz_ffi::{JazzError, JazzHandle};
use serde_json::json;

/// A query reads a text column by UTF-8 bytes (on code point boundaries)
/// or UTF-16 units, a bytes column by byte range, and a JSON column at a
/// pointer, alongside whole columns; parts inside includes are refused.
#[test]
fn select_parts_read_only_what_they_name() {
    let jazz = JazzHandle::open(json!({ "app": KITCHEN_APP }).to_string()).unwrap();
    jazz.insert(
        "things".into(),
        json!({ "label": "héllo world", "count": 1, "flag": true, "tags": [],
                "blob": "AQIDBAU=", "meta": { "a": [1, { "deep": true }] } })
        .to_string(),
    )
    .unwrap();
    let read = |select: serde_json::Value| {
        parse(
            &jazz
                .one(json!({ "table": "things", "select": select }).to_string())
                .unwrap()
                .unwrap(),
        )
    };
    let row = read(json!([
        "count",
        { "column": "label", "from": 0, "to": 3, "units": "utf8" },
        { "column": "blob", "from": 1, "to": 3, "units": "bytes" },
        { "column": "meta", "at": "/a/1" },
    ]));
    assert_eq!(row["count"], 1);
    assert_eq!(row["label"], "hé");
    assert_eq!(row["blob"], "AgM=");
    assert_eq!(row["meta"], json!({ "deep": true }));
    assert!(row.get("flag").is_none());

    let row = read(json!(["*", { "column": "label", "from": 1, "to": 5, "units": "utf16" }]));
    assert_eq!(row["label"], "éllo");
    assert_eq!(row["flag"], true);

    let refused = |select: serde_json::Value| match jazz
        .all(json!({ "table": "things", "select": select }).to_string())
    {
        Err(JazzError::Refused { reason }) => reason,
        other => panic!("expected Refused, got {other:?}"),
    };
    assert!(refused(json!([{ "column": "label", "at": "/x" }])).contains("json column"));
    assert!(
        refused(json!([{ "column": "label", "from": 0, "to": 1, "units": "words" }]))
            .contains("units")
    );
}
