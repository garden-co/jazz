//! Subscription deltas keyed by result (`$key`, `removedKeys`): a live
//! union whose arms both match a row, and a history subscription, whose
//! results share a row's id.

mod common;

use std::collections::HashSet;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use common::*;
use jazz_ffi::{JazzHandle, SubscriptionListener};
use serde_json::{Value, json};

struct Collect(Mutex<mpsc::Sender<Value>>);
impl SubscriptionListener for Collect {
    fn on_delta(&self, delta: String) {
        let _ = self.0.lock().unwrap().send(parse(&delta));
    }
    fn on_error(&self, kind: String, message: String) {
        panic!("{kind}: {message}");
    }
}

fn keys(rows: &Value) -> Vec<String> {
    rows.as_array()
        .unwrap()
        .iter()
        .map(|row| row["$key"].as_str().expect("a $key").to_owned())
        .collect()
}

/// A row both arms match comes once per arm, each with its own key; an
/// update changes both occurrences and a delete removes both, by key.
#[test]
fn a_union_s_occurrences_are_applied_by_key() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let gala = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    jazz.insert("tasks".into(), task(&gala, "Doors", 1.0))
        .unwrap();
    let rig = parse(
        &jazz
            .insert("tasks".into(), task(&gala, "Rig", 2.0))
            .unwrap(),
    )["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let overlapping = json!({ "union": [
        { "table": "tasks", "where": [{ "column": "showId", "op": "eq", "value": gala }] },
        { "table": "tasks", "where": [{ "column": "id", "op": "eq", "value": rig }] },
    ] });
    let (sender, deltas) = mpsc::channel();
    let subscription = jazz
        .subscribe(
            overlapping.to_string(),
            Arc::new(Collect(Mutex::new(sender))),
        )
        .unwrap();
    let reset = deltas.recv_timeout(Duration::from_secs(5)).unwrap();
    let added = reset["added"].as_array().unwrap();
    assert_eq!(added.len(), 3, "{reset}");
    assert_eq!(
        keys(&reset["added"])
            .into_iter()
            .collect::<HashSet<_>>()
            .len(),
        3
    );
    let rig_keys: HashSet<String> = added
        .iter()
        .filter(|row| row["id"] == rig.as_str())
        .map(|row| row["$key"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(rig_keys.len(), 2, "one key per arm: {reset}");

    jazz.update(
        "tasks".into(),
        rig.clone(),
        json!({ "title": "Rig lights" }).to_string(),
    )
    .unwrap();
    let mut updated = HashSet::new();
    while updated.len() < 2 {
        let delta = deltas.recv_timeout(Duration::from_secs(5)).unwrap();
        for row in delta["updated"].as_array().unwrap() {
            assert_eq!(row["title"], "Rig lights", "{delta}");
            updated.insert(row["$key"].as_str().unwrap().to_owned());
        }
    }
    assert_eq!(updated, rig_keys);

    jazz.delete("tasks".into(), rig.clone()).unwrap();
    let mut removed = HashSet::new();
    while removed.len() < 2 {
        let delta = deltas.recv_timeout(Duration::from_secs(5)).unwrap();
        let ids = delta["removed"].as_array().unwrap();
        let removed_keys = delta["removedKeys"].as_array().unwrap();
        assert_eq!(ids.len(), removed_keys.len(), "{delta}");
        for (id, key) in ids.iter().zip(removed_keys) {
            assert_eq!(id, &json!(rig));
            removed.insert(key.as_str().unwrap().to_owned());
        }
    }
    assert_eq!(removed, rig_keys);
    subscription.cancel();
    jazz.shutdown().unwrap();
}

/// A history subscription: each accepted version arrives as its own
/// result, with its `$history` and a key of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_subscriptions_follow_accepted_versions() {
    let app_id = jazz_app_support::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let config = backend_config(&server, &app_id.to_string(), "alice");
    tokio::task::spawn_blocking(move || {
        let jazz = JazzHandle::open(config).unwrap();
        let me = jazz.account().unwrap().unwrap();
        let show_id = parse(
            &jazz
                .insert_at("shows".into(), show(&me), "global".into())
                .unwrap(),
        )["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let id = parse(
            &jazz
                .insert_at("tasks".into(), task(&show_id, "Rig", 1.0), "global".into())
                .unwrap(),
        )["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let query = json!({ "table": "tasks", "history": true,
            "where": [{ "column": "id", "op": "eq", "value": id }] });
        let (sender, deltas) = mpsc::channel();
        let subscription = jazz
            .subscribe(query.to_string(), Arc::new(Collect(Mutex::new(sender))))
            .expect("history subscriptions are supported");
        jazz.update_at(
            "tasks".into(),
            id.clone(),
            json!({ "status": "doing" }).to_string(),
            "global".into(),
        )
        .unwrap();
        // Versions arrive as they're accepted, each its own result.
        let mut versions: Vec<Value> = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while versions.len() < 2 && std::time::Instant::now() < deadline {
            if let Ok(delta) = deltas.recv_timeout(Duration::from_millis(200)) {
                versions.extend(delta["added"].as_array().unwrap().iter().cloned());
            }
        }
        assert_eq!(versions.len(), 2, "{versions:?}");
        assert!(versions.iter().all(|v| v["id"] == id.as_str()));
        assert!(
            versions
                .iter()
                .all(|v| v["$history"]["transactionId"].is_string()),
            "{versions:?}"
        );
        let statuses: HashSet<_> = versions.iter().map(|v| v["status"].clone()).collect();
        assert_eq!(statuses, HashSet::from([json!("todo"), json!("doing")]));
        assert_ne!(versions[0]["$key"], versions[1]["$key"]);
        subscription.cancel();
        jazz.shutdown().unwrap();
    })
    .await
    .unwrap();
}

/// An unlabelled union arm is labelled by what it reads, not by where it
/// stands: the same arms in the other order give each row the same keys
/// (`jazz.union-arm-label.v2`, as every client labels it).
#[test]
fn a_union_s_keys_do_not_depend_on_arm_order() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let gala = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    jazz.insert("tasks".into(), task(&gala, "Doors", 1.0))
        .unwrap();
    jazz.insert("tasks".into(), task(&gala, "Rig", 2.0))
        .unwrap();
    let doors =
        json!({ "table": "tasks", "where": [{ "column": "title", "op": "eq", "value": "Doors" }] });
    let all =
        json!({ "table": "tasks", "where": [{ "column": "showId", "op": "eq", "value": gala }] });
    let keys_of = |arms: Value| {
        let (sender, deltas) = mpsc::channel();
        let subscription = jazz
            .subscribe(
                json!({ "union": arms }).to_string(),
                Arc::new(Collect(Mutex::new(sender))),
            )
            .unwrap();
        let reset = deltas.recv_timeout(Duration::from_secs(5)).unwrap();
        subscription.cancel();
        keys(&reset["added"]).into_iter().collect::<HashSet<_>>()
    };
    let forward = keys_of(json!([doors, all]));
    assert_eq!(forward.len(), 3);
    assert_eq!(keys_of(json!([all, doors])), forward);
    jazz.shutdown().unwrap();
}
