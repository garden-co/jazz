//! jazz-ffi v2: partial edits — text-merge edits, splices, JSON set.

mod common;

use common::*;
use jazz_ffi::{JazzError, JazzHandle};
use serde_json::{Value, json};

fn notes(jazz: &JazzHandle, id: &str) -> Value {
    parse(
        &jazz
            .one(
                json!({ "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": id }] })
                    .to_string(),
            )
            .unwrap()
            .unwrap(),
    )["notes"]
        .clone()
}

/// On StagePlan's `tasks.notes` (`merge: text`): bob turns "hello world"
/// into "hello brave world"; alice, who saw "hello world", appends "!".
/// Her edit keeps bob's change: "hello brave world!". The same works inside
/// a transaction, and through the C ABI's `editText`.
#[test]
fn a_text_edit_merges_over_changes_its_writer_did_not_see() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let show_id = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let id = parse(
        &jazz
            .insert(
                "tasks".into(),
                json!({ "showId": show_id, "title": "Rig", "status": "todo", "rank": 1.0, "notes": "hello world" })
                    .to_string(),
            )
            .unwrap(),
    )["id"]
        .as_str()
        .unwrap()
        .to_owned();

    jazz.update(
        "tasks".into(),
        id.clone(),
        json!({ "notes": "hello brave world" }).to_string(),
    )
    .unwrap();
    jazz.edit_text(
        "tasks".into(),
        id.clone(),
        "notes".into(),
        "hello world".into(),
        "hello world!".into(),
    )
    .unwrap();
    assert_eq!(notes(&jazz, &id), "hello brave world!");

    let tx = jazz.clone().begin().unwrap();
    tx.edit_text(
        "tasks".into(),
        id.clone(),
        "notes".into(),
        "hello brave world!".into(),
        "hello brave new world!".into(),
    )
    .unwrap();
    tx.commit(None).unwrap();
    assert_eq!(notes(&jazz, &id), "hello brave new world!");

    // A splice in UTF-8 bytes, and a whole-column patch, in one write.
    jazz.edit(
        "tasks".into(),
        id.clone(),
        Some(json!({ "status": "doing" }).to_string()),
        json!([{ "kind": "splice", "column": "title", "within": { "units": "utf8", "from": 0, "to": 3 },
                 "splices": [{ "at": 3, "delete": 0, "insert": " lights" }] }])
        .to_string(),
    )
    .unwrap();
    let row = parse(
        &jazz
            .one(json!({ "table": "tasks" }).to_string())
            .unwrap()
            .unwrap(),
    );
    assert_eq!(row["title"], "Rig lights");
    assert_eq!(row["status"], "doing");

    let refused =
        |edits: Value| match jazz.edit("tasks".into(), id.clone(), None, edits.to_string()) {
            Err(JazzError::Refused { reason }) => reason,
            other => panic!("expected Refused, got {other:?}"),
        };
    assert!(
        refused(json!([{ "kind": "text", "column": "rank", "text": "x", "editedFrom": "y" }]))
            .contains("not a string column")
    );
    assert!(refused(json!([{ "kind": "poke", "column": "notes" }])).contains("kind must be"));
    assert!(
        refused(json!([{ "kind": "splice", "column": "title", "units": "words", "splices": [] }]))
            .contains("edits[0].units must be")
    );
    assert!(refused(json!([{ "kind": "splice", "column": "title", "splices": [{ "at": 0, "delete": "2" }] }]))
        .contains("delete"));
    let missing = jazz.edit_text(
        "tasks".into(),
        "01a1e200-0000-7000-8000-00000000dead".into(),
        "notes".into(),
        "a".into(),
        "b".into(),
    );
    assert!(
        matches!(&missing, Err(JazzError::Refused { reason }) if reason.contains("no tasks row")),
        "{missing:?}"
    );
    assert!(
        refused(json!([{ "kind": "text", "column": "nope", "text": "x", "editedFrom": "y" }]))
            .contains("no column nope")
    );
}

/// A JSON column edited in place by pointer.
#[test]
fn json_set_replaces_by_pointer() {
    let jazz = JazzHandle::open(json!({ "app": KITCHEN_APP }).to_string()).unwrap();
    let id = parse(
        &jazz
            .insert(
                "things".into(),
                json!({ "label": "x", "count": 1, "flag": true, "tags": [], "meta": { "a": [1, 2], "b": "keep" } })
                    .to_string(),
            )
            .unwrap(),
    )["id"]
        .as_str()
        .unwrap()
        .to_owned();
    jazz.edit(
        "things".into(),
        id,
        None,
        json!([{ "kind": "jsonSet", "column": "meta", "edits": [{ "at": "/a/1", "value": { "deep": true } }] }])
            .to_string(),
    )
    .unwrap();
    let row = parse(
        &jazz
            .one(json!({ "table": "things" }).to_string())
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        row["meta"],
        json!({ "a": [1, { "deep": true }], "b": "keep" })
    );
}

/// Go's `textEdit`: splices at positions over the whole current value (in
/// UTF-8 bytes, or UTF-16 units), with no page to give; and the same as an
/// `edit` splice without `within`.
#[test]
fn splices_over_the_whole_value() {
    use jazz_ffi::c_abi::*;
    use std::ffi::{CStr, CString};
    let mut err = std::ptr::null_mut();
    let config = CString::new(client_config()).unwrap();
    let h = unsafe { jazz_open(config.as_ptr(), &mut err) };
    let call = |method: &str, args: Value| -> Value {
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
    let show_id = call(
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
    )["id"]
        .clone();
    let id = call(
        "insert",
        json!({ "table": "tasks", "row": { "showId": show_id, "title": "Rig", "status": "todo", "rank": 1.0, "notes": "héllo world" } }),
    )["id"]
        .clone();
    // "héllo world": é is 2 UTF-8 bytes, so " world" starts at byte 6.
    call(
        "textEdit",
        json!({ "table": "tasks", "id": id, "column": "notes", "units": "utf8",
                "edits": [{ "pos": 6, "delete": 6, "insert": " there" }] }),
    );
    let read = |h_call: &dyn Fn(&str, Value) -> Value| {
        h_call("one", json!({ "query": { "table": "tasks" } }))["notes"].clone()
    };
    assert_eq!(read(&call), "héllo there");
    call(
        "edit",
        json!({ "table": "tasks", "id": id, "edits": [{ "kind": "splice", "column": "notes", "units": "utf16",
                "splices": [{ "at": 0, "delete": 5, "insert": "hi" }] }] }),
    );
    assert_eq!(read(&call), "hi there");
    unsafe { jazz_close(h) };
}
