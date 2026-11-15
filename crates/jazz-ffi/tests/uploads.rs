//! jazz-ffi v2: streaming uploads of large values.

mod common;

use base64::Engine as _;
use common::*;
use jazz_ffi::JazzHandle;
use serde_json::json;

/// A bytes value uploaded in pieces becomes one row's value; an update
/// upload replaces it; an aborted upload writes nothing.
#[test]
fn uploads_stream_a_value_in_pieces() {
    let jazz = JazzHandle::open(json!({ "app": KITCHEN_APP }).to_string()).unwrap();
    let row = json!({ "label": "big", "count": 1, "flag": true, "tags": [] }).to_string();
    let upload = jazz
        .clone()
        .begin_upload(
            "things".into(),
            "blob".into(),
            "insert".into(),
            None,
            Some(row),
        )
        .unwrap();
    let bytes: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    for piece in bytes.chunks(64 * 1024) {
        upload.push(piece.to_vec()).unwrap();
    }
    let stored = parse(&upload.finish().unwrap());
    let base64 = base64::engine::general_purpose::STANDARD;
    assert_eq!(
        base64.decode(stored["blob"].as_str().unwrap()).unwrap(),
        bytes
    );
    assert_eq!(stored["label"], "big");
    let id = stored["id"].as_str().unwrap().to_owned();

    let update = jazz
        .clone()
        .begin_upload(
            "things".into(),
            "blob".into(),
            "update".into(),
            Some(id.clone()),
            None,
        )
        .unwrap();
    update.push(b"small".to_vec()).unwrap();
    update.finish().unwrap();
    let read = parse(
        &jazz
            .one(json!({ "table": "things" }).to_string())
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        base64.decode(read["blob"].as_str().unwrap()).unwrap(),
        b"small"
    );

    let aborted = jazz
        .clone()
        .begin_upload(
            "things".into(),
            "blob".into(),
            "insert".into(),
            None,
            Some(json!({ "label": "never", "count": 2, "flag": false, "tags": [] }).to_string()),
        )
        .unwrap();
    aborted.push(vec![1, 2, 3]).unwrap();
    aborted.abort().unwrap();
    assert_eq!(
        parse(&jazz.all(json!({ "table": "things" }).to_string()).unwrap())
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(aborted.push(vec![4]).is_err());
}
