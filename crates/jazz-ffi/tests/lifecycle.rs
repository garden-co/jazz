//! jazz-ffi lifecycle, as TS's: mergeable transactions, `disconnect` /
//! `reconnect`, `deleteClientStorage`, and the refused writes nothing
//! waited on (`onMutationError`, a `mutationErrors` watch).

mod common;

use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use common::*;
use jazz_ffi::{JazzError, JazzHandle, StateListener};
use serde_json::{Value, json};

struct States(Mutex<mpsc::Sender<Value>>);
impl StateListener for States {
    fn on_change(&self, state_json: String) {
        let _ = self.0.lock().unwrap().send(parse(&state_json));
    }
}

/// A mergeable transaction's writes commit together and merge with a
/// concurrent write: where a serializable one would conflict, it commits.
#[test]
fn a_mergeable_transaction_commits_beside_concurrent_writes() {
    let jazz =
        JazzHandle::open(json!({ "app": STAGE_PLAN_APP, "contract": 2 }).to_string()).unwrap();
    let show_id = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap())["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let task_id = parse(
        &jazz
            .insert("tasks".into(), task(&show_id, "Rig", 1.0))
            .unwrap(),
    )["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mergeable = jazz.clone().begin_mergeable().unwrap();
    mergeable.all(tasks_of(&show_id)).unwrap();
    // Changed outside it after it read.
    jazz.update(
        "tasks".into(),
        task_id.clone(),
        json!({ "title": "Rig lights" }).to_string(),
    )
    .unwrap();
    mergeable
        .insert("tasks".into(), task(&show_id, "Focus", 2.0))
        .unwrap();
    mergeable
        .update(
            "tasks".into(),
            task_id.clone(),
            json!({ "rank": 3.0 }).to_string(),
        )
        .unwrap();
    mergeable
        .commit(None)
        .expect("a mergeable commit isn't a conflict");
    let rows = parse(&jazz.all(tasks_of(&show_id)).unwrap());
    let rig = rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == task_id.as_str())
        .unwrap();
    assert_eq!(
        (rig["title"].as_str(), rig["rank"].as_f64()),
        (Some("Rig lights"), Some(3.0))
    );
    assert_eq!(rows.as_array().unwrap().len(), 2);

    // Through the C ABI's call: "serializable": false.
    use std::ffi::{CStr, CString};
    use std::ptr;
    unsafe {
        let mut err = ptr::null_mut();
        let config = CString::new(client_config()).unwrap();
        let h = jazz_ffi::c_abi::jazz_open(config.as_ptr(), &mut err);
        assert!(!h.is_null());
        let call = |method: &str, args: Value| {
            let mut err = ptr::null_mut();
            let (method, args) = (
                CString::new(method).unwrap(),
                CString::new(args.to_string()).unwrap(),
            );
            let out = jazz_ffi::c_abi::jazz_call(h, method.as_ptr(), args.as_ptr(), &mut err);
            assert!(!out.is_null(), "{}", CStr::from_ptr(err).to_str().unwrap());
            let value = parse(CStr::from_ptr(out).to_str().unwrap());
            jazz_ffi::c_abi::jazz_string_free(out);
            value
        };
        let tx = call("txBegin", json!({ "serializable": false }))["tx"].clone();
        call(
            "insert",
            json!({ "table": "shows", "row": parse(&show(nil_uuid())), "tx": tx }),
        );
        call("txCommit", json!({ "tx": tx }));
        assert_eq!(
            call("all", json!({ "query": { "table": "shows" } }))
                .as_array()
                .unwrap()
                .len(),
            1
        );
        jazz_ffi::c_abi::jazz_close(h);
    }
}

/// Disconnected, writes stay local; reconnected, they reach the server.
/// A write the server refuses that nothing waited on reaches the
/// `mutationErrors` watch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_reconnect_and_refused_writes() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let mut config = parse(&backend_config(&server, &app_id.to_string(), "stage-plan"));
    config["contract"] = json!(2);
    let config = config.to_string();
    tokio::task::spawn_blocking(move || {
        let jazz = JazzHandle::open(config).unwrap();
        let account = jazz.account().unwrap().expect("the backend's user");
        jazz.disconnect().unwrap();
        jazz.insert("shows".into(), show(&account)).unwrap();
        // It waits here while the app is disconnected.
        std::thread::sleep(Duration::from_millis(500));
        let status = parse(&jazz.sync_status().unwrap());
        assert_eq!(status["pendingUploads"], 1, "{status}");
        assert_ne!(status["link"]["state"], "connected", "{status}");
        jazz.reconnect().unwrap();
        jazz.wait_for_sync("global".into())
            .expect("after reconnecting, the write reaches the server");

        // A show chiefed by someone else: the server refuses it, and no
        // durable wait asked, so the watch reports it.
        let (sender, errors) = mpsc::channel();
        let watch = jazz
            .watch_mutation_errors(Arc::new(States(Mutex::new(sender))))
            .unwrap();
        let written = parse(&jazz.insert("shows".into(), show(nil_uuid())).unwrap());
        let error = errors
            .recv_timeout(Duration::from_secs(10))
            .expect("the refusal");
        assert_eq!(error["denied"], true, "{error}");
        assert_eq!(error["serializable"], false, "{error}");
        assert!(error["transaction"].as_str().is_some(), "{error}");
        assert!(written["id"].is_string());
        watch.cancel();
        jazz.shutdown().unwrap();
    })
    .await
    .unwrap();
}

/// `deleteClientStorage` closes the app and empties its store: the next
/// open starts empty.
#[test]
fn delete_client_storage_empties_the_store() {
    let dir = std::env::temp_dir().join(format!("jazz-ffi-delete-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = json!({ "app": STAGE_PLAN_APP, "storage": "persistent",
        "dataDir": dir.to_str().unwrap(), "appId": jazz_app::AppId::random().to_string(),
        "contract": 2 })
    .to_string();
    let jazz = JazzHandle::open(config.clone()).unwrap();
    jazz.insert("shows".into(), show(nil_uuid())).unwrap();
    jazz.delete_client_storage().unwrap();
    assert!(matches!(
        jazz.all(json!({ "table": "shows" }).to_string()),
        Err(JazzError::Closed)
    ));
    let reopened = JazzHandle::open(config).unwrap();
    assert_eq!(
        parse(
            &reopened
                .all(json!({ "table": "shows" }).to_string())
                .unwrap()
        ),
        json!([])
    );
    reopened.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
