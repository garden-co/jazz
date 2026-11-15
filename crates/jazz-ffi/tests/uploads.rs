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

/// The checks and the C ABI: an upload's row naming an id, and an upload in
/// a transaction, are refused (it would escape the rollback); a dropped
/// UniFFI upload aborts; `uploadBegin`/`Push`/`Finish`/`Abort` through
/// `jazz_call`, and closing the app with an upload open.
#[test]
fn uploads_refuse_what_would_escape_and_work_through_the_c_abi() {
    use jazz_ffi::c_abi::{jazz_call, jazz_close, jazz_open, jazz_string_free};
    use std::ffi::{CStr, CString};
    let jazz = JazzHandle::open(json!({ "app": KITCHEN_APP }).to_string()).unwrap();
    let with_id = json!({ "id": "01a1e200-0000-7000-8000-000000000021", "label": "x", "count": 1, "flag": true, "tags": [] });
    assert!(
        jazz.clone()
            .begin_upload(
                "things".into(),
                "blob".into(),
                "insert".into(),
                None,
                Some(with_id.to_string())
            )
            .is_err()
    );
    {
        let dropped = jazz
            .clone()
            .begin_upload(
                "things".into(),
                "blob".into(),
                "insert".into(),
                None,
                Some(
                    json!({ "label": "dropped", "count": 1, "flag": true, "tags": [] }).to_string(),
                ),
            )
            .unwrap();
        dropped.push(vec![0; 1024]).unwrap();
    }
    assert_eq!(
        parse(&jazz.all(json!({ "table": "things" }).to_string()).unwrap()),
        json!([])
    );
    jazz.shutdown().unwrap();

    let config = CString::new(json!({ "app": KITCHEN_APP }).to_string()).unwrap();
    let mut err = std::ptr::null_mut();
    let h = unsafe { jazz_open(config.as_ptr(), &mut err) };
    let call =
        |method: &str, args: serde_json::Value| -> Result<serde_json::Value, serde_json::Value> {
            let (method, args) = (
                CString::new(method).unwrap(),
                CString::new(args.to_string()).unwrap(),
            );
            let mut err = std::ptr::null_mut();
            let out = unsafe { jazz_call(h, method.as_ptr(), args.as_ptr(), &mut err) };
            let take = |text: *mut std::ffi::c_char| {
                let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
                unsafe { jazz_string_free(text) };
                parse(&copy)
            };
            if out.is_null() {
                Err(take(err))
            } else {
                Ok(take(out))
            }
        };
    let row = json!({ "label": "c", "count": 1, "flag": true, "tags": [] });
    let tx = call("txBegin", json!({})).unwrap()["tx"].clone();
    assert!(
        call(
            "uploadBegin",
            json!({ "table": "things", "column": "blob", "row": row, "tx": tx })
        )
        .is_err()
    );
    call("txRollback", json!({ "tx": tx })).unwrap();
    let upload = call(
        "uploadBegin",
        json!({ "table": "things", "column": "blob", "row": row }),
    )
    .unwrap()["upload"]
        .clone();
    call("uploadPush", json!({ "upload": upload, "bytes": "AQID" })).unwrap();
    assert!(
        call(
            "uploadPush",
            json!({ "upload": upload, "bytes": "not base64!" })
        )
        .is_err()
    );
    let stored = call("uploadFinish", json!({ "upload": upload, "tier": "local" })).unwrap();
    assert_eq!(stored["blob"], "AQID");
    assert!(call("uploadPush", json!({ "upload": upload, "bytes": "AQID" })).is_err());
    let aborted = call(
        "uploadBegin",
        json!({ "table": "things", "column": "blob", "row": row }),
    )
    .unwrap()["upload"]
        .clone();
    call("uploadAbort", json!({ "upload": aborted })).unwrap();
    // Closing with an upload still open.
    let open = call(
        "uploadBegin",
        json!({ "table": "things", "column": "blob", "row": row }),
    )
    .unwrap()["upload"]
        .clone();
    call("uploadPush", json!({ "upload": open, "bytes": "AQID" })).unwrap();
    assert_eq!(
        call("all", json!({ "query": { "table": "things" } }))
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    unsafe { jazz_close(h) };
}
