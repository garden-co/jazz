//! jazz-ffi v2: upsert and restore.

mod common;

use common::*;
use jazz_ffi::JazzHandle;
use serde_json::json;

/// alice upserts a show under an id she chose (an insert), upserts it again
/// with a new venue (a whole row, as for an insert), deletes it and restores it as it was, then with new
/// content (a whole row); inside a transaction too.
#[test]
fn upsert_writes_by_id_and_restore_brings_rows_back() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let id = "01a1e200-0000-7000-8000-000000000001".to_owned();
    let row = parse(
        &jazz
            .upsert("shows".into(), id.clone(), show(nil_uuid()))
            .unwrap(),
    );
    assert_eq!(row["id"], id);
    assert_eq!(row["venue"], "Main Hall");
    let row = parse(
        &jazz
            .upsert(
                "shows".into(),
                id.clone(),
                json!({ "venue": "Annex" }).to_string(),
            )
            .unwrap(),
    );
    assert_eq!(row["venue"], "Annex");
    assert_eq!(row["name"], "Spring Gala");

    jazz.delete("shows".into(), id.clone()).unwrap();
    assert_eq!(
        jazz.one(json!({ "table": "shows" }).to_string()).unwrap(),
        None
    );
    let row = parse(&jazz.restore("shows".into(), id.clone(), None).unwrap());
    assert_eq!(row["venue"], "Annex");
    jazz.delete("shows".into(), id.clone()).unwrap();
    let tx = jazz.clone().begin().unwrap();
    let row = parse(
        &tx.restore(
            "shows".into(),
            id.clone(),
            Some({
                let mut row = parse(&show(nil_uuid()));
                row["name"] = json!("Autumn Gala");
                row.to_string()
            }),
        )
        .unwrap(),
    );
    assert_eq!(row["name"], "Autumn Gala");
    tx.commit(None).unwrap();
    assert_eq!(
        parse(
            &jazz
                .one(json!({ "table": "shows" }).to_string())
                .unwrap()
                .unwrap()
        )["name"],
        "Autumn Gala"
    );
}

/// The checks and the C ABI: an upsert whose row names another id, and a
/// restore whose patch does, are refused; a partial upsert updates an
/// existing row but can't create one; `upsert`/`restore` through
/// `jazz_call`, with a tier.
#[test]
fn upsert_and_restore_refuse_mismatched_ids_and_work_through_the_c_abi() {
    use jazz_ffi::c_abi::{jazz_call, jazz_close, jazz_open, jazz_string_free};
    use std::ffi::{CStr, CString};
    let jazz = JazzHandle::open(client_config()).unwrap();
    let (id, other) = (
        "01a1e200-0000-7000-8000-000000000011".to_owned(),
        "01a1e200-0000-7000-8000-000000000012".to_owned(),
    );
    let mut row = parse(&show(nil_uuid()));
    row["id"] = json!(other);
    let refused = |result: Result<String, jazz_ffi::JazzError>| match result {
        Err(jazz_ffi::JazzError::Refused { reason }) => reason,
        other => panic!("expected Refused, got {other:?}"),
    };
    assert!(refused(jazz.upsert("shows".into(), id.clone(), row.to_string())).contains("id"));
    let partial = json!({ "venue": "Annex" }).to_string();
    assert!(refused(jazz.upsert("shows".into(), id.clone(), partial.clone())).contains("required"));
    jazz.upsert("shows".into(), id.clone(), show(nil_uuid()))
        .unwrap();
    assert_eq!(
        parse(&jazz.upsert("shows".into(), id.clone(), partial).unwrap())["venue"],
        "Annex"
    );
    jazz.delete("shows".into(), id.clone()).unwrap();
    assert!(
        refused(jazz.restore("shows".into(), id.clone(), Some(row.to_string())))
            .contains("another row")
    );
    jazz.shutdown().unwrap();

    let config = CString::new(client_config()).unwrap();
    let mut err = std::ptr::null_mut();
    let h = unsafe { jazz_open(config.as_ptr(), &mut err) };
    let call = |method: &str, args: serde_json::Value| -> serde_json::Value {
        let (method, args) = (
            CString::new(method).unwrap(),
            CString::new(args.to_string()).unwrap(),
        );
        let mut err = std::ptr::null_mut();
        let out = unsafe { jazz_call(h, method.as_ptr(), args.as_ptr(), &mut err) };
        assert!(
            !out.is_null(),
            "{}",
            unsafe { CStr::from_ptr(err) }.to_str().unwrap()
        );
        let text = unsafe { CStr::from_ptr(out) }.to_str().unwrap().to_owned();
        unsafe { jazz_string_free(out) };
        parse(&text)
    };
    let row = call(
        "upsert",
        json!({ "table": "shows", "id": id, "row": parse(&show(nil_uuid())), "tier": "local" }),
    );
    assert_eq!(row["id"], id);
    call("delete", json!({ "table": "shows", "id": id }));
    let restored = call(
        "restore",
        json!({ "table": "shows", "id": id, "tier": "local" }),
    );
    assert_eq!(restored["venue"], "Main Hall");
    unsafe { jazz_close(h) };
}
