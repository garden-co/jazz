//! jazz-ffi v2: hopTo, gather, union, history and includeDeleted, lowered
//! through jazz-app's untyped builder.

mod common;

use common::*;
use jazz_ffi::{JazzError, JazzHandle};
use serde_json::{Value, json};

fn rows(jazz: &JazzHandle, query: Value) -> Vec<Value> {
    parse(&jazz.all(query.to_string()).unwrap())
        .as_array()
        .unwrap()
        .clone()
}

fn names(rows: &[Value], key: &str) -> Vec<String> {
    let mut names: Vec<_> = rows
        .iter()
        .map(|r| r[key].as_str().unwrap().to_owned())
        .collect();
    names.sort();
    names
}

/// On StagePlan: the shows of the blocked tasks (hop forward, filtered on
/// the source side), the tasks of a show by hop backwards with target-side
/// filters in `after`, a union of two shows' tasks, and a deleted task read
/// with `includeDeleted`.
#[test]
fn hops_unions_and_deleted_rows() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let insert_show = |name: &str| {
        let mut row = parse(&show(nil_uuid()));
        row["name"] = json!(name);
        parse(&jazz.insert("shows".into(), row.to_string()).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let (gala, fair) = (insert_show("Gala"), insert_show("Fair"));
    let rig = parse(
        &jazz
            .insert("tasks".into(), task(&gala, "Rig", 1.0))
            .unwrap(),
    )["id"]
        .as_str()
        .unwrap()
        .to_owned();
    jazz.insert("tasks".into(), task(&gala, "Doors", 2.0))
        .unwrap();
    jazz.insert("tasks".into(), task(&fair, "Tent", 1.0))
        .unwrap();
    jazz.update(
        "tasks".into(),
        rig.clone(),
        json!({ "status": "blocked" }).to_string(),
    )
    .unwrap();

    let shows = rows(
        &jazz,
        json!({ "table": "tasks", "where": [{ "column": "status", "op": "eq", "value": "blocked" }], "hopTo": "show" }),
    );
    assert_eq!(names(&shows, "name"), ["Gala"]);

    let tasks = rows(
        &jazz,
        json!({ "table": "shows", "where": [{ "column": "name", "op": "eq", "value": "Gala" }], "hopTo": ["tasks"],
                "after": { "where": [{ "column": "status", "op": "eq", "value": "todo" }],
                           "orderBy": [{ "column": "rank" }] } }),
    );
    assert_eq!(names(&tasks, "title"), ["Doors"]);

    let union = rows(
        &jazz,
        json!({ "union": [
            { "table": "tasks", "where": [{ "column": "showId", "op": "eq", "value": fair }] },
            { "table": "tasks", "where": [{ "column": "status", "op": "eq", "value": "blocked" }] },
        ], "orderBy": [{ "column": "title" }] }),
    );
    assert_eq!(names(&union, "title"), ["Rig", "Tent"]);
    // A row two arms match comes once.
    let overlapping = rows(
        &jazz,
        json!({ "union": [
            { "table": "tasks", "where": [{ "column": "showId", "op": "eq", "value": gala }] },
            { "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": rig }] },
        ] }),
    );
    assert_eq!(names(&overlapping, "title"), ["Doors", "Rig"]);

    jazz.delete("tasks".into(), rig.clone()).unwrap();
    assert!(
        rows(
            &jazz,
            json!({ "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": rig }] })
        )
        .is_empty()
    );
    let deleted = rows(
        &jazz,
        json!({ "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": rig }], "includeDeleted": true }),
    );
    assert_eq!(deleted.len(), 1);

    let refused = |query: Value| match jazz.all(query.to_string()) {
        Err(JazzError::Refused { reason }) => reason,
        other => panic!("expected Refused, got {other:?}"),
    };
    assert!(refused(json!({ "table": "tasks", "hopTo": "nope" })).contains("no relation nope"));
    assert!(
        refused(json!({ "union": [{ "table": "tasks" }, { "table": "shows" }] }))
            .contains("every arm")
    );
}

const TREE_APP: &str = r#"{
  "format": 1,
  "tables": {
    "nodes": {
      "columns": { "name": "string", "parent": "uuid?" },
      "relations": { "parentRow": { "rel": "nodes", "column": "parent" } }
    }
  }
}"#;

/// A leaf and every node above it, along a self relation, to a depth.
#[test]
fn gather_follows_a_self_relation() {
    let jazz = JazzHandle::open(json!({ "app": TREE_APP }).to_string()).unwrap();
    let mut parent = Value::Null;
    let mut leaf = String::new();
    for name in ["root", "branch", "twig", "leaf"] {
        let row = parse(
            &jazz
                .insert(
                    "nodes".into(),
                    json!({ "name": name, "parent": parent }).to_string(),
                )
                .unwrap(),
        );
        leaf = row["id"].as_str().unwrap().to_owned();
        parent = row["id"].clone();
    }
    let all = rows(
        &jazz,
        json!({ "table": "nodes", "where": [{ "column": "id", "op": "eq", "value": leaf }],
                "gather": { "relation": "parentRow" } }),
    );
    assert_eq!(names(&all, "name"), ["branch", "leaf", "root", "twig"]);
    let near = rows(
        &jazz,
        json!({ "table": "nodes", "where": [{ "column": "id", "op": "eq", "value": leaf }],
                "gather": { "relation": "parentRow", "maxDepth": 1 } }),
    );
    assert_eq!(names(&near, "name"), ["leaf", "twig"]);
}

/// History reads the versions the server accepted: alice's backend writes a
/// task, then moves it; read remotely, its history has both versions in
/// acceptance order, each with its `$history`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_reads_accepted_versions() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let config = backend_config(&server, &app_id.to_string(), "alice");
    let result = tokio::task::spawn_blocking(move || {
        let jazz = JazzHandle::open(config).unwrap();
        let me = jazz.account().unwrap().unwrap();
        let show_id = parse(&jazz.insert_at("shows".into(), show(&me), "global".into()).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let id = parse(&jazz.insert_at("tasks".into(), task(&show_id, "Rig", 1.0), "global".into()).unwrap())["id"]
            .as_str()
            .unwrap()
            .to_owned();
        jazz.update_at("tasks".into(), id.clone(), json!({ "status": "doing" }).to_string(), "global".into())
            .unwrap();
        let query = json!({ "table": "tasks", "history": true, "where": [{ "column": "id", "op": "eq", "value": id }],
                            "orderBy": [{ "column": "$acceptedAt" }] })
        .to_string();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let history = loop {
            let history = parse(&jazz.all_at(query.clone(), "remote".into()).unwrap());
            if history.as_array().unwrap().len() >= 2 || std::time::Instant::now() > deadline {
                break history;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        };
        assert_eq!(history.as_array().unwrap().len(), 2, "{history}");
        assert_eq!(history[0]["status"], "todo");
        assert_eq!(history[1]["status"], "doing");
        assert!(history[0]["$history"]["transactionId"].is_string(), "{history}");
        assert!(history[0]["$history"]["acceptedAt"].as_u64().unwrap() > 0);
        assert!(history[0]["$history"]["acceptedAt"].as_u64() <= history[1]["$history"]["acceptedAt"].as_u64());
        jazz.shutdown().unwrap();
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}
