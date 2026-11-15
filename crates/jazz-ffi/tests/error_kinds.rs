//! jazz-ffi contract 2 error kinds: `permission`, `notSupported`,
//! `conflict`; contract 1 hosts still see `refused`.

mod common;

use common::*;
use jazz_ffi::{JazzError, JazzHandle};
use serde_json::json;

fn v2_client() -> std::sync::Arc<JazzHandle> {
    JazzHandle::open(json!({ "app": STAGE_PLAN_APP, "contract": 2 }).to_string()).unwrap()
}

/// What isn't supported yet is `NotSupported` for a contract 2 host and
/// `Refused` for a contract 1 host, with the same reason.
#[test]
fn not_supported_is_its_own_kind_from_contract_2() {
    let query =
        json!({ "table": "shows", "include": [{ "relation": "tasks", "query": { "offset": 1 } }] })
            .to_string();
    let v2 = v2_client().all(query.clone());
    assert!(
        matches!(&v2, Err(JazzError::NotSupported { reason }) if reason.starts_with("not supported yet")),
        "{v2:?}"
    );
    let v1 = JazzHandle::open(client_config()).unwrap().all(query);
    assert!(
        matches!(&v1, Err(JazzError::Refused { reason }) if reason.starts_with("not supported yet")),
        "{v1:?}"
    );
    let opened =
        JazzHandle::open(json!({ "app": STAGE_PLAN_APP, "jwt": "x", "contract": 2 }).to_string());
    assert!(
        matches!(opened, Err(JazzError::NotSupported { .. })),
        "{opened:?}"
    );
}

/// Two transactions read the same task; the first to commit its update
/// wins, and the second's commit is a `Conflict` the host may retry.
#[test]
fn a_serializable_conflict_is_its_own_kind() {
    let jazz = v2_client();
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
    let first = jazz.clone().begin().unwrap();
    let second = jazz.clone().begin().unwrap();
    first.all(tasks_of(&show_id)).unwrap();
    second.all(tasks_of(&show_id)).unwrap();
    first
        .update(
            "tasks".into(),
            task_id.clone(),
            json!({ "status": "doing" }).to_string(),
        )
        .unwrap();
    second
        .update(
            "tasks".into(),
            task_id.clone(),
            json!({ "status": "blocked" }).to_string(),
        )
        .unwrap();
    first.commit(None).unwrap();
    let lost = second.commit(None);
    assert!(matches!(lost, Err(JazzError::Conflict { .. })), "{lost:?}");
}

/// bob's backend, neither chief nor crew of alice's show, adds a task to it
/// and waits for global durability: the authority refuses it, and bob's
/// contract 2 host sees `Permission`.
///
/// ```
/// bob ──insert task (tier global)──► server ──policy ✗──► Permission
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_the_permissions_refuse_is_permission() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let with_contract = |subject: &str| {
        let mut config = parse(&backend_config(&server, &app_id.to_string(), subject));
        config["contract"] = json!(2);
        config.to_string()
    };
    let (alice, bob) = (with_contract("alice"), with_contract("bob"));
    let result = tokio::task::spawn_blocking(move || {
        let alice = JazzHandle::open(alice).unwrap();
        let bob = JazzHandle::open(bob).unwrap();
        let account = alice.account().unwrap().unwrap();
        let show_id = parse(
            &alice
                .insert_at("shows".into(), show(&account), "global".into())
                .unwrap(),
        )["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let denied = bob.insert_at(
            "tasks".into(),
            task(&show_id, "Sneaky", 1.0),
            "global".into(),
        );
        assert!(
            matches!(denied, Err(JazzError::Permission { .. })),
            "{denied:?}"
        );
        alice.shutdown().unwrap();
        bob.shutdown().unwrap();
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}
