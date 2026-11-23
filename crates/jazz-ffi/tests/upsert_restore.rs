//! jazz-ffi v2: upsert and restore.

mod common;

use common::*;
use jazz_ffi::{JazzError, JazzHandle};
use serde_json::json;

/// alice upserts a show under an id she chose (an insert), upserts it again
/// with a new venue (a whole row, as for an insert), deletes it and restores it as it was, then with new
/// content (a whole row); inside a transaction too.
#[test]
fn upsert_writes_by_id_and_restore_brings_rows_back() {
    let jazz = JazzHandle::open(client_config()).unwrap();
    let id = "01a1e200-0000-7000-8000-000000000001".to_owned();
    let row = parse(
        &jazz
            .upsert("shows".into(), id.clone(), show(nil_uuid()))
            .unwrap(),
    );
    assert_eq!(row["id"], id);
    assert_eq!(row["venue"], "Main Hall");
    let row = parse(
        &jazz
            .upsert(
                "shows".into(),
                id.clone(),
                json!({ "venue": "Annex" }).to_string(),
            )
            .unwrap(),
    );
    assert_eq!(row["venue"], "Annex");
    assert_eq!(row["name"], "Spring Gala");

    jazz.delete("shows".into(), id.clone()).unwrap();
    assert_eq!(
        jazz.one(json!({ "table": "shows" }).to_string()).unwrap(),
        None
    );
    let row = parse(&jazz.restore("shows".into(), id.clone(), None).unwrap());
    assert_eq!(row["venue"], "Annex");
    jazz.delete("shows".into(), id.clone()).unwrap();
    let tx = jazz.clone().begin().unwrap();
    let row = parse(
        &tx.restore(
            "shows".into(),
            id.clone(),
            Some({
                let mut row = parse(&show(nil_uuid()));
                row["name"] = json!("Autumn Gala");
                row.to_string()
            }),
        )
        .unwrap(),
    );
    assert_eq!(row["name"], "Autumn Gala");
    tx.commit(None).unwrap();
    assert_eq!(
        parse(
            &jazz
                .one(json!({ "table": "shows" }).to_string())
                .unwrap()
                .unwrap()
        )["name"],
        "Autumn Gala"
    );
}

/// The checks and the C ABI: an upsert whose row names another id, and a
/// restore whose patch does, are refused; a partial upsert updates an
/// existing row but can't create one; `upsert`/`restore` through
/// `jazz_call`, with a tier.
#[test]
fn upsert_and_restore_refuse_mismatched_ids_and_work_through_the_c_abi() {
    use jazz_ffi::c_abi::{jazz_call, jazz_close, jazz_open, jazz_string_free};
    use std::ffi::{CStr, CString};
    let jazz = JazzHandle::open(client_config()).unwrap();
    let (id, other) = (
        "01a1e200-0000-7000-8000-000000000011".to_owned(),
        "01a1e200-0000-7000-8000-000000000012".to_owned(),
    );
    let mut row = parse(&show(nil_uuid()));
    row["id"] = json!(other);
    let refused = |result: Result<String, jazz_ffi::JazzError>| match result {
        Err(jazz_ffi::JazzError::Refused { reason }) => reason,
        other => panic!("expected Refused, got {other:?}"),
    };
    assert!(refused(jazz.upsert("shows".into(), id.clone(), row.to_string())).contains("id"));
    let partial = json!({ "venue": "Annex" }).to_string();
    assert!(refused(jazz.upsert("shows".into(), id.clone(), partial.clone())).contains("required"));
    jazz.upsert("shows".into(), id.clone(), show(nil_uuid()))
        .unwrap();
    assert_eq!(
        parse(&jazz.upsert("shows".into(), id.clone(), partial).unwrap())["venue"],
        "Annex"
    );
    jazz.delete("shows".into(), id.clone()).unwrap();
    assert!(
        refused(jazz.restore("shows".into(), id.clone(), Some(row.to_string())))
            .contains("another row")
    );
    jazz.shutdown().unwrap();

    let config = CString::new(client_config()).unwrap();
    let mut err = std::ptr::null_mut();
    let h = unsafe { jazz_open(config.as_ptr(), &mut err) };
    let call = |method: &str, args: serde_json::Value| -> serde_json::Value {
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
    let row = call(
        "upsert",
        json!({ "table": "shows", "id": id, "row": parse(&show(nil_uuid())), "tier": "local" }),
    );
    assert_eq!(row["id"], id);
    call("delete", json!({ "table": "shows", "id": id }));
    let restored = call(
        "restore",
        json!({ "table": "shows", "id": id, "tier": "local" }),
    );
    assert_eq!(restored["venue"], "Main Hall");
    unsafe { jazz_close(h) };
}

/// alice upserts at `global` while offline and, before that can finish,
/// deletes the row from the same app (another screen, say). Back online the
/// server accepts both: the upsert answers with what it wrote, rather than
/// calling an accepted write refused because the row is gone by then.
///
/// ```
/// disconnect──upsert venue=Annex (global, waits)──delete──reconnect──► Annex (accepted)
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_upsert_answers_with_the_row_it_wrote() {
    let app_id = jazz_app_support::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let config = backend_config(&server, &app_id.to_string(), "alice");
    let result = tokio::task::spawn_blocking(move || {
        let alice = JazzHandle::open(config).unwrap();
        let account = alice.account().unwrap().unwrap();
        let id = "01a1e200-0000-7000-8000-000000000021".to_owned();
        let whole = alice
            .upsert_at("shows".into(), id.clone(), show(&account), "global".into())
            .unwrap();
        assert_eq!(parse(&whole)["venue"], "Main Hall");

        // Offline, so the upsert is certainly still waiting when the delete runs.
        alice.disconnect().unwrap();
        let upserting = {
            let (alice, id) = (alice.clone(), id.clone());
            std::thread::spawn(move || {
                alice.upsert_at(
                    "shows".into(),
                    id,
                    json!({ "venue": "Annex" }).to_string(),
                    "global".into(),
                )
            })
        };
        let query = json!({ "table": "shows" }).to_string();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while parse(&alice.all_at(query.clone(), "local".into()).unwrap())[0]["venue"] != "Annex" {
            assert!(
                std::time::Instant::now() < deadline,
                "the upsert never applied locally"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        alice.delete("shows".into(), id.clone()).unwrap();
        alice.reconnect().unwrap();
        let written = upserting.join().unwrap().expect("an accepted upsert");
        assert_eq!(parse(&written)["venue"], "Annex");
        alice.wait_for_sync("global".into()).unwrap();
        assert_eq!(
            parse(&alice.all_at(query, "remote".into()).unwrap()),
            json!([])
        );
        alice.shutdown().unwrap();
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}

/// bob, neither chief nor crew of alice's show, upserts a task into it at
/// `global`: the server's refusal is the answer (`Permission`), and the task
/// is nowhere afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_global_upsert_the_server_refuses_is_permission() {
    let app_id = jazz_app_support::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let as_subject = |subject: &str| {
        let mut config = parse(&backend_config(&server, &app_id.to_string(), subject));
        config["contract"] = json!(2);
        config.to_string()
    };
    let (alice, bob) = (as_subject("alice"), as_subject("bob"));
    let result = tokio::task::spawn_blocking(move || {
        let alice = JazzHandle::open(alice).unwrap();
        let bob = JazzHandle::open(bob).unwrap();
        let account = alice.account().unwrap().unwrap();
        let show_id = "01a1e200-0000-7000-8000-000000000031".to_owned();
        alice
            .upsert_at(
                "shows".into(),
                show_id.clone(),
                show(&account),
                "global".into(),
            )
            .unwrap();
        let denied = bob.upsert_at(
            "tasks".into(),
            "01a1e200-0000-7000-8000-000000000032".into(),
            task(&show_id, "Sneaky", 1.0),
            "global".into(),
        );
        assert!(
            matches!(denied, Err(JazzError::Permission { .. })),
            "{denied:?}"
        );
        let tasks = alice.all_at(tasks_of(&show_id), "remote".into()).unwrap();
        assert_eq!(parse(&tasks), json!([]));
        alice.shutdown().unwrap();
        bob.shutdown().unwrap();
    })
    .await;
    server.shutdown().await;
    result.unwrap();
}

/// What can't be waited for is refused before anything is written: `global`
/// without a server, a tier that isn't one, and, through the C ABI, a tier
/// on a transaction's upsert (the commit takes it).
#[test]
fn an_upsert_tier_that_cant_be_waited_for_writes_nothing() {
    use jazz_ffi::c_abi::{jazz_call, jazz_close, jazz_open, jazz_string_free};
    use std::ffi::{CStr, CString};
    let jazz = JazzHandle::open(client_config()).unwrap();
    let id = "01a1e200-0000-7000-8000-000000000041".to_owned();
    for tier in ["global", "edge"] {
        let refused = jazz.upsert_at("shows".into(), id.clone(), show(nil_uuid()), tier.into());
        assert!(
            matches!(refused, Err(JazzError::Refused { .. })),
            "{tier}: {refused:?}"
        );
    }
    let query = json!({ "table": "shows" }).to_string();
    assert_eq!(jazz.one(query).unwrap(), None);
    jazz.shutdown().unwrap();

    let config = CString::new(client_config()).unwrap();
    let mut err = std::ptr::null_mut();
    let h = unsafe { jazz_open(config.as_ptr(), &mut err) };
    let call = |method: &str, args: serde_json::Value| {
        let (method, args) = (
            CString::new(method).unwrap(),
            CString::new(args.to_string()).unwrap(),
        );
        let mut err = std::ptr::null_mut();
        let out = unsafe { jazz_call(h, method.as_ptr(), args.as_ptr(), &mut err) };
        let text = if out.is_null() { err } else { out };
        let json = parse(unsafe { CStr::from_ptr(text) }.to_str().unwrap());
        unsafe { jazz_string_free(text) };
        if out.is_null() { Err(json) } else { Ok(json) }
    };
    let tx = call("txBegin", json!({})).unwrap()["tx"].clone();
    let refused = call(
        "upsert",
        json!({ "table": "shows", "id": id, "row": parse(&show(nil_uuid())), "tx": tx, "tier": "local" }),
    )
    .unwrap_err();
    assert!(
        refused["message"].as_str().unwrap().contains("txCommit"),
        "{refused}"
    );
    let rows = call("all", json!({ "query": { "table": "shows" }, "tx": tx })).unwrap();
    assert_eq!(rows, json!([]));
    call("txRollback", json!({ "tx": tx })).unwrap();
    unsafe { jazz_close(h) };
}
