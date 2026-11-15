//! jazz-ffi v2: transactions (UniFFI `Transaction`, C ABI `tx*`) and tiers.

mod common;

use std::ffi::{CStr, CString, c_char};
use std::ptr;

use common::*;
use jazz_ffi::JazzError;
use jazz_ffi::JazzHandle as Handle;
use jazz_ffi::c_abi::*;
use serde_json::{Value, json};

fn refused(result: Result<impl std::fmt::Debug, JazzError>) -> String {
    match result {
        Err(JazzError::Refused { reason }) => reason,
        other => panic!("expected Refused, got {other:?}"),
    }
}

/// alice adds a show and its first task in one transaction: inside it, her
/// reads see her own staged writes; outside, nothing shows until commit. A
/// rolled-back transaction leaves nothing behind, and so does one dropped
/// without commit.
#[test]
fn a_transaction_commits_its_writes_together_or_not_at_all() {
    let jazz = Handle::open(client_config()).unwrap();
    let tx = jazz.clone().begin().unwrap();
    let show_id = parse(&tx.insert("shows".into(), show(nil_uuid())).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    tx.insert("tasks".into(), task(&show_id, "Rig", 1.0))
        .unwrap();
    assert_eq!(
        titles(&parse(&tx.all(tasks_of(&show_id)).unwrap())),
        ["Rig"]
    );
    assert_eq!(parse(&jazz.all(tasks_of(&show_id)).unwrap()), json!([]));
    tx.commit(Some("local".into())).unwrap();
    assert_eq!(
        titles(&parse(&jazz.all(tasks_of(&show_id)).unwrap())),
        ["Rig"]
    );
    assert!(refused(tx.commit(None)).contains("already ended"));

    let tx = jazz.clone().begin().unwrap();
    tx.insert("tasks".into(), task(&show_id, "Discarded", 2.0))
        .unwrap();
    tx.rollback().unwrap();
    {
        let dropped = jazz.clone().begin().unwrap();
        dropped
            .insert("tasks".into(), task(&show_id, "Dropped", 3.0))
            .unwrap();
    }
    assert_eq!(
        titles(&parse(&jazz.all(tasks_of(&show_id)).unwrap())),
        ["Rig"]
    );

    // A transaction that only read just ends.
    let tx = jazz.clone().begin().unwrap();
    tx.all(tasks_of(&show_id)).unwrap();
    tx.commit(None).unwrap();

    // Tiers: a local write waits for local durability; a serverless app
    // can't reach global, and says so.
    jazz.insert_at("tasks".into(), task(&show_id, "Doors", 4.0), "local".into())
        .unwrap();
    assert!(matches!(
        jazz.insert_at("tasks".into(), task(&show_id, "Far", 5.0), "global".into()),
        Err(JazzError::Refused { .. } | JazzError::Io { .. })
    ));
    assert!(
        refused(jazz.insert_at("tasks".into(), task(&show_id, "x", 6.0), "edge".into()))
            .starts_with("not supported yet")
    );
    assert_eq!(
        parse(&jazz.all_at(tasks_of(&show_id), "local".into()).unwrap())
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert!(refused(jazz.all_at(tasks_of(&show_id), "nearby".into())).contains("read tier"));
}

fn c(text: &str) -> CString {
    CString::new(text).unwrap()
}

fn call(
    handle: *mut jazz_ffi::c_abi::JazzHandle,
    method: &str,
    args: Value,
) -> Result<Value, Value> {
    let mut err = ptr::null_mut();
    let out = unsafe {
        jazz_call(
            handle,
            c(method).as_ptr(),
            c(&args.to_string()).as_ptr(),
            &mut err,
        )
    };
    let take = |text: *mut c_char| {
        let copy = unsafe { CStr::from_ptr(text) }.to_str().unwrap().to_owned();
        unsafe { jazz_string_free(text) };
        parse(&copy)
    };
    if out.is_null() {
        Err(take(err))
    } else {
        Ok(take(out))
    }
}

/// Go's path: `txBegin`, `"tx"` on reads and writes, `txCommit` with a tier,
/// `txRollback`; a closed transaction is refused.
#[test]
fn transactions_through_the_c_abi() {
    let mut err = ptr::null_mut();
    let config = json!({ "app": STAGE_PLAN_APP, "contract": 2 }).to_string();
    let h = unsafe { jazz_open(c(&config).as_ptr(), &mut err) };
    assert!(!h.is_null());

    let tx = call(h, "txBegin", json!({ "serializable": true })).unwrap()["tx"].clone();
    let created = call(
        h,
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())), "tx": tx }),
    )
    .unwrap();
    let show_id = created["id"].as_str().unwrap().to_owned();
    let inside = call(h, "all", json!({ "query": { "table": "shows" }, "tx": tx })).unwrap();
    assert_eq!(inside.as_array().unwrap().len(), 1);
    assert_eq!(
        call(h, "all", json!({ "query": { "table": "shows" } })).unwrap(),
        json!([])
    );
    assert_eq!(
        call(h, "txCommit", json!({ "tx": tx, "tier": "local" })).unwrap(),
        Value::Null
    );
    assert_eq!(
        call(h, "one", json!({ "query": { "table": "shows" } })).unwrap()["id"],
        show_id
    );
    assert_eq!(
        call(h, "txCommit", json!({ "tx": tx })).unwrap_err()["kind"],
        "refused"
    );

    let tx = call(h, "txBegin", json!({})).unwrap()["tx"].clone();
    call(
        h,
        "delete",
        json!({ "table": "shows", "id": show_id, "tx": tx }),
    )
    .unwrap();
    call(h, "txRollback", json!({ "tx": tx })).unwrap();
    assert_eq!(
        call(h, "one", json!({ "query": { "table": "shows" } })).unwrap()["id"],
        show_id
    );

    assert_eq!(
        call(h, "txBegin", json!({ "serializable": false })).unwrap_err()["kind"],
        "refused"
    );
    let tx = call(h, "txBegin", json!({})).unwrap()["tx"].clone();
    assert!(
        call(
            h,
            "insert",
            json!({ "table": "shows", "row": parse(&show(nil_uuid())), "tx": tx, "tier": "local" })
        )
        .unwrap_err()["message"]
            .as_str()
            .unwrap()
            .contains("txCommit")
    );
    call(
        h,
        "update",
        json!({ "table": "shows", "id": show_id, "patch": { "venue": "Annex" }, "tier": "local" }),
    )
    .unwrap();
    assert_eq!(
        call(
            h,
            "one",
            json!({ "query": { "table": "shows" }, "tier": "local" })
        )
        .unwrap()["venue"],
        "Annex"
    );
    unsafe { jazz_close(h) };

    let bad = json!({ "app": STAGE_PLAN_APP, "contract": 3 }).to_string();
    let mut err = ptr::null_mut();
    assert!(unsafe { jazz_open(c(&bad).as_ptr(), &mut err) }.is_null());
    unsafe { jazz_string_free(err) };
}
