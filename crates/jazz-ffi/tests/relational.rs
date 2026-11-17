//! jazz-ffi v2: hopTo, gather, union, history and includeDeleted, lowered
//! through jazz-app-support's untyped builder.

mod common;

use common::*;
use jazz_ffi::{JazzError, JazzHandle};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    // As TS (UNION ALL): a row two arms match comes once per arm, and the
    // window counts every occurrence.
    let overlapping = json!({ "union": [
        { "table": "tasks", "where": [{ "column": "showId", "op": "eq", "value": gala }] },
        { "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": rig }] },
    ], "orderBy": [{ "column": "title" }] });
    assert_eq!(
        names(&rows(&jazz, overlapping.clone()), "title"),
        ["Doors", "Rig", "Rig"]
    );
    let mut windowed = overlapping;
    windowed["offset"] = json!(1);
    windowed["limit"] = json!(2);
    assert_eq!(names(&rows(&jazz, windowed), "title"), ["Rig", "Rig"]);

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
    // An unlabelled arm is labelled by what it reads, so two identical ones
    // are refused; labelled, the same query reads twice.
    assert!(
        refused(json!({ "union": [{ "table": "tasks" }, { "table": "tasks" }] }))
            .contains("name the arms")
    );
    let twice = json!({ "union": [
        { "table": "tasks", "label": "one" },
        { "table": "tasks", "label": "two" },
    ] });
    assert_eq!(
        rows(&jazz, twice).len(),
        2 * rows(&jazz, json!({ "table": "tasks" })).len()
    );
    assert!(
        refused(json!({ "union": [{ "table": "tasks", "label": 1 }] }))
            .contains("must be a string")
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
    let app_id = jazz_app_support::AppId::random();
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

const TEAMS_APP: &str = include_str!("fixtures/relations/jazz.app.json");

/// A hop over a `uuid[]` reference reaches every row it names.
#[test]
fn hops_over_uuid_arrays_reach_every_named_row() {
    let jazz = JazzHandle::open(json!({ "app": TEAMS_APP }).to_string()).unwrap();
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
    let (ada, bo, cy) = (person("Ada"), person("Bo"), person("Cy"));
    jazz.insert(
        "teams".into(),
        json!({ "name": "Crew", "members": [ada, bo], "lead": ada }).to_string(),
    )
    .unwrap();
    let _ = cy;
    let members = rows(&jazz, json!({ "table": "teams", "hopTo": "memberRows" }));
    assert_eq!(names(&members, "name"), ["Ada", "Bo"]);
}

/// Review cases: `one` on hop, union and gather queries; a hop read inside a
/// transaction; a subscription to a hop; the C ABI path; a non-boolean
/// `history` refused by name.
#[test]
fn one_transactions_subscriptions_and_the_c_abi_on_relational_queries() {
    use std::sync::mpsc;
    let jazz = JazzHandle::open(client_config()).unwrap();
    let mut row = parse(&show(nil_uuid()));
    row["name"] = json!("Gala");
    let gala = parse(&jazz.insert("shows".into(), row.to_string()).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    jazz.insert("tasks".into(), task(&gala, "Rig", 1.0))
        .unwrap();
    jazz.insert("tasks".into(), task(&gala, "Doors", 2.0))
        .unwrap();

    let hop = json!({ "table": "tasks", "hopTo": "show" });
    let one = |query: &Value| parse(&jazz.one(query.to_string()).unwrap().expect("a row"));
    assert_eq!(one(&hop)["name"], "Gala");
    let union = json!({ "union": [{ "table": "tasks" }], "orderBy": [{ "column": "title" }] });
    assert_eq!(one(&union)["title"], "Doors");
    let tasks_of_gala = json!({ "table": "shows", "hopTo": ["tasks"], "after": { "orderBy": [{ "column": "rank" }], "limit": 5 } });
    assert_eq!(one(&tasks_of_gala)["title"], "Rig");

    let tx = jazz.clone().begin().unwrap();
    assert_eq!(
        names(
            parse(&tx.all(hop.to_string()).unwrap()).as_array().unwrap(),
            "name"
        ),
        ["Gala"]
    );
    tx.rollback().unwrap();

    struct Collect(Mutex<mpsc::Sender<Value>>);
    impl jazz_ffi::SubscriptionListener for Collect {
        fn on_delta(&self, delta: String) {
            let _ = self.0.lock().unwrap().send(parse(&delta));
        }
        fn on_error(&self, _: String, _: String) {}
    }
    let (sender, deltas) = mpsc::channel();
    let subscription = jazz
        .subscribe(
            tasks_of_gala.to_string(),
            Arc::new(Collect(Mutex::new(sender))),
        )
        .unwrap();
    let reset = deltas.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        names(reset["added"].as_array().unwrap(), "title"),
        ["Doors", "Rig"]
    );
    jazz.insert("tasks".into(), task(&gala, "Tent", 3.0))
        .unwrap();
    let added = deltas.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(names(added["added"].as_array().unwrap(), "title"), ["Tent"]);
    subscription.cancel();

    match jazz.all(json!({ "table": "tasks", "history": "yes" }).to_string()) {
        Err(JazzError::Refused { reason }) => assert!(reason.contains("history"), "{reason}"),
        other => panic!("expected Refused, got {other:?}"),
    }
    jazz.shutdown().unwrap();

    use jazz_ffi::c_abi::{jazz_call, jazz_close, jazz_open, jazz_string_free};
    use std::ffi::{CStr, CString};
    let config = CString::new(client_config()).unwrap();
    let mut err = std::ptr::null_mut();
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
    let id = call(
        "insert",
        json!({ "table": "shows", "row": parse(&show(nil_uuid())) }),
    )["id"]
        .clone();
    call(
        "insert",
        json!({ "table": "tasks", "row": parse(&task(id.as_str().unwrap(), "C", 1.0)) }),
    );
    assert_eq!(
        call(
            "one",
            json!({ "query": { "table": "tasks", "hopTo": "show" } })
        )["id"],
        id
    );
    assert_eq!(
        call(
            "all",
            json!({ "query": { "union": [{ "table": "tasks" }] } })
        )
        .as_array()
        .unwrap()
        .len(),
        1
    );
    unsafe { jazz_close(h) };
}
