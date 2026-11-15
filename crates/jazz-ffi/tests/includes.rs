//! jazz-ffi v2 queries: `include` (nested related rows) and `select`.

mod common;

use std::sync::{Arc, mpsc};
use std::time::Duration;

use common::*;
use jazz_ffi::{JazzError, JazzHandle, SubscriptionListener};
use serde_json::{Value, json};

fn stage(jazz: &JazzHandle) -> (String, String, String) {
    let show = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap());
    let show_id = show["id"].as_str().unwrap().to_owned();
    let crew = parse(
        &jazz
            .insert(
                "crew".into(),
                json!({ "account": nil_uuid(), "name": "Ada" }).to_string(),
            )
            .unwrap(),
    );
    let crew_id = crew["id"].as_str().unwrap().to_owned();
    jazz.insert(
        "tasks".into(),
        json!({ "showId": show_id, "title": "Rig", "status": "todo", "rank": 2.0, "assigneeId": crew_id })
            .to_string(),
    )
    .unwrap();
    jazz.insert("tasks".into(), task(&show_id, "Sound", 1.0))
        .unwrap();
    (show_id, crew_id, show["name"].as_str().unwrap().to_owned())
}

/// alice's StagePlan client reads a show with its tasks (a reverse
/// relation, ordered and selected), each task with its assignee (a forward
/// relation, nested), and a task with its show.
#[test]
fn includes_nest_related_rows_under_their_relation_names() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let (show_id, crew_id, name) = stage(&jazz);

    let shows = parse(
        &jazz
            .all(
                json!({
                    "table": "shows",
                    "select": ["name"],
                    "include": [{
                        "relation": "tasks",
                        "query": {
                            "orderBy": [{ "column": "rank" }],
                            "select": ["rank", "title"],
                            "include": [{ "relation": "assignee", "query": { "select": ["name"] } }],
                        },
                    }],
                })
                .to_string(),
            )
            .unwrap(),
    );
    assert_eq!(
        shows,
        json!([{
            "id": show_id,
            "name": name,
            "tasks": [
                { "id": shows[0]["tasks"][0]["id"], "rank": 1.0, "title": "Sound", "assignee": null },
                { "id": shows[0]["tasks"][1]["id"], "rank": 2.0, "title": "Rig",
                  "assignee": { "id": crew_id, "name": "Ada" } },
            ],
        }])
    );

    // A forward relation through a required column; every column without
    // `select`.
    let task = parse(
        &jazz
            .one(
                json!({ "table": "tasks", "where": [{ "column": "title", "op": "eq", "value": "Rig" }],
                        "include": [{ "relation": "show", "required": true }] })
                .to_string(),
            )
            .unwrap()
            .unwrap(),
    );
    assert_eq!(task["show"]["id"], show_id);
    assert_eq!(task["show"]["venue"], "Main Hall");
    assert_eq!(task["status"], "todo");

    // An include's own `limit` and `where`.
    let limited = parse(
        &jazz
            .all(
                json!({ "table": "shows", "include": [{ "relation": "tasks", "query": {
                    "where": [{ "column": "status", "op": "eq", "value": "todo" }],
                    "orderBy": [{ "column": "rank", "direction": "desc" }], "limit": 1 } }] })
                .to_string(),
            )
            .unwrap(),
    );
    assert_eq!(titles(&limited[0]["tasks"]), ["Rig"]);
}

/// Includes the contract can't express are refused by name.
#[test]
fn includes_refuse_what_they_cannot_do() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let refused = |query: Value| match jazz.all(query.to_string()) {
        Err(JazzError::Refused { reason }) => reason,
        other => panic!("expected Refused, got {other:?}"),
    };
    assert!(
        refused(json!({ "table": "shows", "include": [{ "relation": "nope" }] }))
            .contains("no relation nope")
    );
    assert!(
        refused(
            json!({ "table": "tasks", "include": [{ "relation": "assignee", "required": true }] })
        )
        .starts_with("not supported yet: \"required\" on assignee")
    );
    assert!(refused(json!({ "table": "shows", "include": [{ "relation": "tasks", "query": { "table": "x" } }] }))
        .contains("unknown key"));
    assert!(refused(json!({ "table": "shows", "select": ["nope"] })).contains("no column nope"));
    assert!(refused(json!({ "table": "shows", "include": [{ "relation": "tasks" }, { "relation": "tasks" }] }))
        .contains("included twice"));
}

struct Collect(mpsc::Sender<Value>);
impl SubscriptionListener for Collect {
    fn on_delta(&self, delta_json: String) {
        let _ = self.0.send(parse(&delta_json));
    }
    fn on_error(&self, _: String) {}
}

/// A subscription with an include delivers rows with their related rows,
/// and again when a related row changes.
#[test]
fn subscriptions_carry_includes() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let (show_id, _, _) = stage(&jazz);
    let (sender, deltas) = mpsc::channel();
    let _subscription = jazz
        .subscribe(
            json!({ "table": "shows", "include": [{ "relation": "tasks", "query": { "select": ["title"] } }] })
                .to_string(),
            Arc::new(Collect(sender)),
        )
        .unwrap();
    let reset = deltas.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(reset["reset"], true);
    assert_eq!(reset["added"][0]["tasks"].as_array().unwrap().len(), 2);

    jazz.insert("tasks".into(), task(&show_id, "Doors", 3.0))
        .unwrap();
    let mut seen = None;
    while let Ok(delta) = deltas.recv_timeout(Duration::from_secs(5)) {
        if let Some(row) = delta["updated"].as_array().and_then(|rows| rows.first()) {
            seen = Some(row.clone());
            break;
        }
    }
    let row = seen.expect("the show updates when a task is added");
    assert_eq!(row["tasks"].as_array().unwrap().len(), 3);
}

/// Two nested includes named out of alphabetical order land under their own
/// names (the engine returns nested relations in name order).
#[test]
fn nested_includes_in_any_order_decode_under_their_names() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let (show_id, crew_id, name) = stage(&jazz);
    let shows = parse(
        &jazz
            .all(
                json!({ "table": "shows", "include": [{ "relation": "tasks", "query": {
                    "where": [{ "column": "title", "op": "eq", "value": "Rig" }],
                    "include": [{ "relation": "show" }, { "relation": "assignee" }],
                } }] })
                .to_string(),
            )
            .unwrap(),
    );
    let rig = &shows[0]["tasks"][0];
    assert_eq!(rig["show"]["id"], show_id);
    assert_eq!(rig["show"]["name"], name);
    assert_eq!(rig["assignee"]["id"], crew_id);
    assert_eq!(rig["assignee"]["name"], "Ada");
}

const RELATIONS_APP: &str = include_str!("fixtures/relations/jazz.app.json");

/// A forward relation through a `uuid[]` column reads as an array of the
/// referenced rows, as in TS; `required` keeps only rows whose every
/// reference is readable. A reverse relation that reverses a forward one
/// pointing at another table is refused, not lowered.
#[test]
fn forward_relations_through_uuid_arrays_read_as_arrays() {
    let jazz = JazzHandle::open(json!({ "app": RELATIONS_APP }).to_string()).unwrap();
    let person = |name: &str| {
        parse(
            &jazz
                .insert("people".into(), json!({ "name": name }).to_string())
                .unwrap(),
        )["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let (ada, bo) = (person("Ada"), person("Bo"));
    jazz.insert(
        "teams".into(),
        json!({ "name": "Crew", "members": [ada, bo], "lead": ada }).to_string(),
    )
    .unwrap();
    let teams = parse(
        &jazz
            .all(
                json!({ "table": "teams", "include": [
                    { "relation": "memberRows", "required": true },
                    { "relation": "leadRow" },
                ] })
                .to_string(),
            )
            .unwrap(),
    );
    let mut members: Vec<_> = teams[0]["memberRows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap().to_owned())
        .collect();
    members.sort();
    assert_eq!(members, ["Ada", "Bo"]);
    assert_eq!(teams[0]["leadRow"]["name"], "Ada");

    let misdirected = jazz
        .all(json!({ "table": "notes", "include": [{ "relation": "misdirected" }] }).to_string());
    assert!(
        matches!(&misdirected, Err(JazzError::Refused { reason }) if reason.contains("no relation misdirected")),
        "{misdirected:?}"
    );
}

/// TS-compatible leniency: `offset: 0` in an include and `select: ["*"]`.
#[test]
fn offset_zero_and_select_star_are_accepted() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    stage(&jazz);
    let shows = parse(
        &jazz
            .all(
                json!({ "table": "shows", "select": ["*"],
                        "include": [{ "relation": "tasks", "query": { "offset": 0, "select": ["*"] } }] })
                .to_string(),
            )
            .unwrap(),
    );
    assert_eq!(shows[0]["venue"], "Main Hall");
    assert_eq!(shows[0]["tasks"][0]["status"], "todo");
}
