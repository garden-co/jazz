//! The UniFFI surface Swift and Kotlin call, exercised from Rust.

mod common;

use std::sync::{Arc, mpsc};
use std::time::Duration;

use common::*;
use jazz_ffi::{JazzError, JazzHandle, SubscriptionListener};
use serde_json::{Value, json};

struct Collect(mpsc::Sender<Result<Value, String>>);

impl SubscriptionListener for Collect {
    fn on_delta(&self, delta_json: String) {
        let _ = self.0.send(Ok(parse(&delta_json)));
    }
    fn on_error(&self, kind: String, message: String) {
        let _ = self.0.send(Err(format!("{kind}: {message}")));
    }
}

fn next(deltas: &mpsc::Receiver<Result<Value, String>>) -> Value {
    deltas
        .recv_timeout(Duration::from_secs(5))
        .expect("a delta")
        .expect("not an error")
}

/// A delta's rows without their result keys (`$key`, checked where results
/// share an id: tests/result_keys.rs), to compare with written rows.
fn unkeyed(rows: &Value) -> Value {
    let mut rows = rows.clone();
    for row in rows.as_array_mut().unwrap() {
        assert!(row["$key"].is_string(), "{row}");
        row.as_object_mut().unwrap().remove("$key");
    }
    rows
}

fn refused(result: Result<impl std::fmt::Debug, JazzError>) -> String {
    match result {
        Err(JazzError::Refused { reason }) => reason,
        other => panic!("expected Refused, got {other:?}"),
    }
}

/// A local StagePlan client (no actors beyond the app) inserts a show and
/// its tasks, reads them back through every query op, moves a task between
/// stages, rewrites its text-merge notes, deletes it, and closes.
#[test]
fn stage_plan_reads_and_writes_locally() {
    let jazz = JazzHandle::open(client_config()).expect("open locally");
    assert_eq!(jazz.account().unwrap(), None);

    let created = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap());
    let show_id = created["id"].as_str().unwrap().to_owned();
    assert_eq!(created["venue"], "Main Hall");
    assert_eq!(created["chiefAccount"], nil_uuid());

    let rig = parse(
        &jazz
            .insert("tasks".into(), task(&show_id, "Rig lights", 2.5))
            .unwrap(),
    );
    // Floats, enums, optional uuids and the text-merge column round-trip.
    assert_eq!(rig["rank"], 2.5);
    assert_eq!(rig["status"], "todo");
    assert_eq!(rig["assigneeId"], Value::Null);
    assert_eq!(rig["notes"], Value::Null);
    let sound = parse(
        &jazz
            .insert(
                "tasks".into(),
                json!({ "showId": show_id, "title": "Sound check", "status": "doing", "rank": 1.25,
                        "notes": "bring the spare XLRs" })
                .to_string(),
            )
            .unwrap(),
    );
    assert_eq!(sound["notes"], "bring the spare XLRs");

    let ordered = parse(&jazz.all(tasks_of(&show_id)).unwrap());
    assert_eq!(titles(&ordered), ["Sound check", "Rig lights"]);
    assert_eq!(ordered[0], sound);

    // Move a task between stages and rewrite its (mergeable) notes.
    let rig_id = rig["id"].as_str().unwrap().to_owned();
    jazz.update(
        "tasks".into(),
        rig_id.clone(),
        json!({ "status": "done", "notes": "rigged", "rank": 0.5 }).to_string(),
    )
    .unwrap();
    let moved = parse(
        &jazz
            .one(json!({ "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": rig_id }] }).to_string())
            .unwrap()
            .expect("the task"),
    );
    assert_eq!(moved["status"], "done");
    assert_eq!(moved["notes"], "rigged");
    assert_eq!(
        titles(&parse(&jazz.all(tasks_of(&show_id)).unwrap())),
        ["Rig lights", "Sound check"]
    );

    // Every op of the query JSON.
    let count = |condition: Value| {
        parse(
            &jazz
                .all(json!({ "table": "tasks", "where": [condition] }).to_string())
                .unwrap(),
        )
        .as_array()
        .unwrap()
        .len()
    };
    assert_eq!(
        count(json!({ "column": "status", "op": "ne", "value": "done" })),
        1
    );
    assert_eq!(
        count(json!({ "column": "rank", "op": "lt", "value": 1.25 })),
        1
    );
    assert_eq!(
        count(json!({ "column": "rank", "op": "lte", "value": 1.25 })),
        2
    );
    assert_eq!(
        count(json!({ "column": "rank", "op": "gt", "value": 0.5 })),
        1
    );
    assert_eq!(
        count(json!({ "column": "rank", "op": "gte", "value": 0.5 })),
        2
    );
    assert_eq!(
        count(json!({ "column": "title", "op": "contains", "value": "check" })),
        1
    );
    assert_eq!(
        count(json!({ "column": "status", "op": "in", "value": ["todo", "doing"] })),
        1
    );
    assert_eq!(count(json!({ "column": "assigneeId", "op": "isNull" })), 2);
    assert_eq!(count(json!({ "column": "notes", "op": "isNotNull" })), 2);
    let page = parse(
        &jazz
            .all(json!({ "table": "tasks", "orderBy": [{ "column": "rank", "direction": "desc" }], "limit": 1, "offset": 1 }).to_string())
            .unwrap(),
    );
    assert_eq!(titles(&page), ["Rig lights"]);

    jazz.delete("tasks".into(), rig_id.clone()).unwrap();
    assert_eq!(
        jazz.one(
            json!({ "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": rig_id }] })
                .to_string()
        )
        .unwrap(),
        None
    );

    jazz.shutdown().unwrap();
    jazz.shutdown().unwrap();
    assert_eq!(jazz.all(tasks_of(&show_id)), Err(JazzError::Closed));
}

/// Every column kind round-trips as the contract's plain JSON: bigint as a
/// decimal string, bytes as base64, json as nested JSON, enum defaults filled.
#[test]
fn every_value_kind_maps_to_plain_json() {
    let jazz = JazzHandle::open(json!({ "app": KITCHEN_APP }).to_string()).unwrap();
    let row = json!({
        "label": "all of them",
        "count": -7,
        "big": "9007199254740993",
        "ratio": 0.25,
        "flag": true,
        "at": 1_763_236_800_000u64,
        "ref": "6f1c2a8e-3f7b-4d2a-9c1e-0a1b2c3d4e5f",
        "blob": "AAEC/w==",
        "meta": { "nested": [1, "two", null] },
        "tags": ["a", "b"],
    });
    let stored = parse(&jazz.insert("things".into(), row.to_string()).unwrap());
    let mut expected = row.clone();
    expected["id"] = stored["id"].clone();
    // The enum's schema default fills the omitted column.
    expected["level"] = json!("low");
    assert_eq!(stored, expected);
    let read = parse(
        &jazz
            .one(json!({ "table": "things" }).to_string())
            .unwrap()
            .unwrap(),
    );
    assert_eq!(read, expected);

    let id = stored["id"].as_str().unwrap().to_owned();
    jazz.update(
        "things".into(),
        id.clone(),
        json!({ "big": null, "meta": null, "level": "high" }).to_string(),
    )
    .unwrap();
    let read = parse(
        &jazz
            .one(json!({ "table": "things" }).to_string())
            .unwrap()
            .unwrap(),
    );
    assert_eq!(read["big"], Value::Null);
    assert_eq!(read["meta"], Value::Null);
    assert_eq!(read["level"], "high");

    let query = |condition: Value| {
        parse(
            &jazz
                .all(json!({ "table": "things", "where": [condition] }).to_string())
                .unwrap(),
        )
        .as_array()
        .unwrap()
        .len()
    };
    assert_eq!(
        query(json!({ "column": "tags", "op": "contains", "value": "b" })),
        1
    );
    assert_eq!(query(json!({ "column": "at", "op": "gt", "value": 0 })), 1);
    assert_eq!(
        query(json!({ "column": "$createdAt", "op": "gt", "value": 0 })),
        1
    );
    assert_eq!(
        query(json!({ "column": "flag", "op": "eq", "value": false })),
        0
    );
}

/// A subscriber gets a reset delta with the full result, then added, updated
/// and removed deltas, and nothing after cancel. A listener may call back
/// into the handle from its delivery thread.
#[test]
fn subscriptions_deliver_deltas_until_cancelled() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let show_id = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let first = parse(
        &jazz
            .insert("tasks".into(), task(&show_id, "first", 1.0))
            .unwrap(),
    );

    let (sender, deltas) = mpsc::channel();
    let subscription = jazz
        .subscribe(tasks_of(&show_id), Arc::new(Collect(sender)))
        .unwrap();
    let reset = next(&deltas);
    assert_eq!(reset["reset"], true);
    assert_eq!(reset["coalesced"], false);
    assert_eq!(unkeyed(&reset["added"]), json!([first]));

    let second = parse(
        &jazz
            .insert("tasks".into(), task(&show_id, "second", 2.0))
            .unwrap(),
    );
    let added = next(&deltas);
    assert_eq!(added["reset"], false);
    assert_eq!(unkeyed(&added["added"]), json!([second]));

    let first_id = first["id"].as_str().unwrap().to_owned();
    jazz.update(
        "tasks".into(),
        first_id.clone(),
        json!({ "status": "blocked" }).to_string(),
    )
    .unwrap();
    let updated = next(&deltas);
    assert_eq!(updated["updated"][0]["status"], "blocked");

    jazz.delete("tasks".into(), first_id.clone()).unwrap();
    let removed = next(&deltas);
    assert_eq!(removed["removed"], json!([first_id]));

    subscription.cancel();
    subscription.cancel();
    jazz.insert("tasks".into(), task(&show_id, "unseen", 3.0))
        .unwrap();
    assert!(deltas.recv_timeout(Duration::from_millis(300)).is_err());

    // A listener may call back into the handle.
    struct Reentrant(Arc<JazzHandle>, mpsc::Sender<usize>);
    impl SubscriptionListener for Reentrant {
        fn on_delta(&self, _: String) {
            let rows = parse(&self.0.all(json!({ "table": "tasks" }).to_string()).unwrap());
            let _ = self.1.send(rows.as_array().unwrap().len());
        }
        fn on_error(&self, _: String, _: String) {}
    }
    let (sender, counts) = mpsc::channel();
    let _reentrant = jazz
        .subscribe(
            json!({ "table": "tasks" }).to_string(),
            Arc::new(Reentrant(jazz.clone(), sender)),
        )
        .unwrap();
    assert_eq!(counts.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
}

/// What jazz-app cannot do (offsets in includes, foreign session issuers),
/// misconfigured servers and invalid input are refused with `Refused`, naming the pattern.
#[test]
fn refuses_what_it_cannot_do_by_name() {
    assert!(refused(JazzHandle::open("not json".into())).contains("not valid JSON"));
    let message = refused(JazzHandle::open(
        json!({ "app": STAGE_PLAN_APP, "serverUrl": "http://localhost:1" }).to_string(),
    ));
    // A server needs a backend's permissions or an end user's `auth`.
    assert!(message.contains("\"auth\""), "{message}");
    let message = refused(JazzHandle::open(
        json!({ "app": STAGE_PLAN_APP, "jwt": "x" }).to_string(),
    ));
    assert!(message.contains("token provider"), "{message}");
    let message = refused(JazzHandle::open(
        json!({ "app": STAGE_PLAN_APP, "permissions": STAGE_PLAN_PERMISSIONS, "appId": nil_uuid(),
                "serverUrl": "http://localhost:1", "backendSecret": "b", "adminSecret": "a",
                "session": { "issuer": "https://auth.example", "subject": "alice" } })
        .to_string(),
    ));
    assert!(
        message.starts_with("not supported yet: a session issuer"),
        "{message}"
    );
    let message = refused(JazzHandle::open(
        json!({ "app": STAGE_PLAN_APP, "permissions": STAGE_PLAN_PERMISSIONS, "appId": nil_uuid(),
                "serverUrl": "http://localhost:1", "backendSecret": "b", "session": { "subject": "alice" } })
        .to_string(),
    ));
    assert!(message.contains("adminSecret"), "{message}");
    assert!(
        refused(JazzHandle::open(
            json!({ "app": "{\"format\": 1}" }).to_string()
        ))
        .starts_with("app files")
    );

    let message = refused(JazzHandle::open(
        json!({ "app": STAGE_PLAN_APP, "permissions": STAGE_PLAN_PERMISSIONS, "appId": nil_uuid(),
                "backendSecret": "b", "adminSecret": "a", "session": { "subject": "alice" } })
        .to_string(),
    ));
    assert!(message.contains("serverUrl"), "{message}");

    let jazz = JazzHandle::open(client_config()).unwrap();
    // `one` validates the query as given before limiting it to one row.
    assert!(
        refused(jazz.one(json!({ "table": "shows", "limit": -1 }).to_string())).contains("limit")
    );
    let message =
        refused(jazz.all(json!({ "table": "shows", "include": [{ "relation": "tasks", "query": { "offset": 1 } }] }).to_string()));
    assert!(
        message.starts_with("not supported yet: \"offset\" in an include"),
        "{message}"
    );
    assert!(refused(jazz.all(json!({ "table": "nope" }).to_string())).contains("no table nope"));
    assert!(refused(jazz.all(json!({ "table": "tasks", "where": [{ "column": "nope", "op": "eq", "value": 1 }] }).to_string()))
        .contains("no column nope"));
    assert!(refused(jazz.all(json!({ "table": "tasks", "where": [{ "column": "rank", "op": "like", "value": 1 }] }).to_string()))
        .contains("unknown op"));
    let show_id = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let message = refused(jazz.insert(
        "tasks".into(),
        json!({ "showId": show_id, "title": "x", "status": "someday", "rank": 1 }).to_string(),
    ));
    assert!(
        message.contains("must be one of todo, doing, done, blocked"),
        "{message}"
    );
    let message = refused(jazz.insert(
        "tasks".into(),
        json!({ "showId": show_id, "title": 3 }).to_string(),
    ));
    assert!(
        message.contains("tasks.title must be a string"),
        "{message}"
    );
    assert!(
        refused(jazz.update("tasks".into(), "not-a-uuid".into(), "{}".into())).contains("UUID")
    );
}

/// As StagePlan's backend: alice's backend creates a show she chiefs
/// (`chiefAccount = session.user`) and adds a task; a second backend acting
/// as alice receives both through the server, and bob (neither chief nor
/// crew) reads none of it.
///
/// ```
/// alice ──insert show, task──► server ──read policy──► alice (again)
///                                 └──────────✗──────► bob
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_backend_acts_as_its_session_user_under_permissions() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let app_id = app_id.to_string();
    let open = |subject: &'static str| {
        let config = backend_config(&server, &app_id, subject);
        tokio::task::spawn_blocking(move || JazzHandle::open(config))
    };
    let alice = open("alice").await.unwrap().expect("open as alice");
    let bob = open("bob").await.unwrap().expect("open as bob");
    let again_config = backend_config(&server, &app_id, "alice");

    let result = tokio::task::spawn_blocking(move || {
        let account = alice.account().unwrap().expect("alice's account");
        assert_ne!(Some(account.clone()), bob.account().unwrap());
        let created = parse(&alice.insert("shows".into(), show(&account)).unwrap());
        let show_id = created["id"].as_str().unwrap().to_owned();
        alice
            .insert("tasks".into(), task(&show_id, "Rig lights", 1.0))
            .unwrap();
        let tasks = parse(&alice.all(tasks_of(&show_id)).unwrap());
        assert_eq!(titles(&tasks), ["Rig lights"]);

        // The server accepted both writes: a second backend acting as Alice
        // receives them through it, under the read policies.
        let again = JazzHandle::open(again_config).unwrap();
        assert_eq!(again.account().unwrap(), Some(account));
        let (sender, deltas) = mpsc::channel();
        let _alices = again
            .subscribe(tasks_of(&show_id), Arc::new(Collect(sender)))
            .unwrap();
        let mut seen = Vec::new();
        while seen.is_empty() {
            seen.extend(titles(&next(&deltas)["added"]));
        }
        assert_eq!(seen, ["Rig lights"]);

        // Bob is neither chief nor crew of the show: he reads none of it.
        let (sender, deltas) = mpsc::channel();
        let _bobs = bob
            .subscribe(tasks_of(&show_id), Arc::new(Collect(sender)))
            .unwrap();
        let mut bob_sees = Vec::new();
        while let Ok(delta) = deltas.recv_timeout(Duration::from_millis(500)) {
            bob_sees.extend(titles(&delta.unwrap()["added"]));
        }
        assert!(bob_sees.is_empty(), "{bob_sees:?}");
        again.shutdown().unwrap();
        alice.shutdown().unwrap();
        bob.shutdown().unwrap();
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}

/// A subscription ends with a last `on_error` when the app shuts down, so a
/// Swift `AsyncStream` or Kotlin `Flow` built on it finishes instead of
/// waiting forever.
#[test]
fn shutdown_ends_live_subscriptions_with_on_error() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let (sender, events) = mpsc::channel();
    let _subscription = jazz
        .subscribe(
            json!({ "table": "shows" }).to_string(),
            Arc::new(Collect(sender)),
        )
        .unwrap();
    assert_eq!(next(&events)["reset"], true);
    jazz.shutdown().unwrap();
    let end = events
        .recv_timeout(Duration::from_secs(5))
        .expect("a last event");
    assert_eq!(end, Err("closed: the app is closed".to_owned()));
    assert!(events.recv_timeout(Duration::from_millis(200)).is_err());
}
