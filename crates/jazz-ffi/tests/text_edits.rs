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
