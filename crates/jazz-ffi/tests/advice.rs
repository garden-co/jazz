//! jazz-ffi v2: permission advice (`can`).

mod common;

use common::*;
use jazz_ffi::JazzHandle;
use serde_json::json;

/// alice may read and update the show she chiefs; bob may do neither, and
/// may not insert a task into it. With no server there's no advice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_answers_as_the_server_would() {
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let (alice, bob) = (
        backend_config(&server, &app_id.to_string(), "alice"),
        backend_config(&server, &app_id.to_string(), "bob"),
    );
    let result = tokio::task::spawn_blocking(move || {
        let alice = JazzHandle::open(alice).unwrap();
        let bob = JazzHandle::open(bob).unwrap();
        let account = alice.account().unwrap().unwrap();
        assert!(alice.bundle().unwrap().is_some());
        let show_id = parse(
            &alice
                .insert_at("shows".into(), show(&account), "global".into())
                .unwrap(),
        )["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let id = json!({ "id": show_id }).to_string();
        assert_eq!(
            alice
                .can("shows".into(), "read".into(), id.clone())
                .unwrap(),
            Some(true)
        );
        assert_eq!(
            alice
                .can(
                    "shows".into(),
                    "update".into(),
                    json!({ "id": show_id, "row": { "venue": "Annex" } }).to_string()
                )
                .unwrap(),
            Some(true)
        );
        assert_eq!(
            bob.can("shows".into(), "read".into(), id).unwrap(),
            Some(false)
        );
        assert_eq!(
            bob.can(
                "tasks".into(),
                "insert".into(),
                json!({ "row": parse(&task(&show_id, "Sneaky", 1.0)) }).to_string()
            )
            .unwrap(),
            Some(false)
        );
        assert!(bob.can("shows".into(), "poke".into(), "{}".into()).is_err());
        alice.shutdown().unwrap();
        bob.shutdown().unwrap();

        let local = JazzHandle::open(client_config()).unwrap();
        assert_eq!(
            local
                .can(
                    "shows".into(),
                    "read".into(),
                    json!({ "id": nil_uuid() }).to_string()
                )
                .unwrap(),
            None
        );
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}
