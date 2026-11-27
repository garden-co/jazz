mod common;

use std::cell::RefCell;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures::channel::oneshot;
use jazz::db::block_on;
use jazz_e2ee_client::host::bridge::{JsStore, serve};
use jazz_e2ee_client::host::runtime::{Built, Encryption};
use jazz_e2ee_client::{ClientError, MemoryStore, PrivateStore};
use serde_json::Value;

use common::*;

/// Alice signs out with a key save in flight. Hold the client and its DB
/// alive so the test can observe the keys the host would otherwise drop.
///
/// save starts ──► shutdown waits ──► store replies ──► memory clears
#[test]
fn shutdown_drains_the_private_store_and_clears_held_keys() {
    let (device_user, identity) = device_account(7);
    assert_shutdown(account(0xa1), None);
    assert_shutdown(device_user, Some(identity));
}

fn assert_shutdown(
    who: jazz::ids::AuthorSubject,
    identity: Option<jazz_e2ee::protocol::derive::RecoverySecret>,
) {
    let has_identity = identity.is_some();
    let memory = Rc::new(MemoryStore::new());
    let write_barrier = Rc::new(RefCell::new(None::<oneshot::Receiver<()>>));
    let store = Rc::new(JsStore::served_by(Rc::new({
        let answer = serve(memory.clone());
        let write_barrier = write_barrier.clone();
        move |request| {
            let op: Value = serde_json::from_str(&request).unwrap();
            let wait = if op["op"] == "save" {
                write_barrier.borrow_mut().take()
            } else {
                None
            };
            let answer = answer.clone();
            Box::pin(async move {
                if let Some(wait) = wait {
                    wait.await.unwrap();
                }
                answer(request).await
            })
        }
    })));
    let settings = jazz::e2ee::EncryptionSettings::default();
    let client = Rc::new(open_account(store.clone(), who, settings.clone(), identity));
    let server = secure_authority_with(&settings, None);
    let (db, _link) = session(&client, &server, who, &settings, 1, None);
    drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    let recovery = jazz_e2ee::protocol::derive::RecoverySecret::from_bytes([11; 32]);
    drive(&server, &[&db], client.register_recovery(&db, &recovery)).unwrap();
    drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    let root = client
        .with_account(|a| a.state.recovery_roots[0].row.id.clone())
        .unwrap();
    drive(&server, &[&db], client.retire_recovery_root(&db, &root)).unwrap();
    assert_eq!(
        client.with_account(|a| a.epoch_keys.epoch_ids().count()),
        Some(2)
    );
    assert_eq!(client.has_device_account_identity(), has_identity);
    let first = hold_new_space(&client, &db, project(&db, "first"));
    drop(open_account(store.clone(), who, settings.clone(), None));
    assert_eq!(stored_verified_depth(&memory, &first), Some(0));
    let late = new_space_key(
        &client.device().unwrap(),
        &address(&db, project(&db, "late")),
    );
    let late_root = late.root_id().to_owned();
    let later = new_space_key(
        &client.device().unwrap(),
        &address(&db, project(&db, "after shutdown")),
    );
    let runtime = Encryption::new(
        Built {
            client: client.clone(),
            spaces: Value::Null,
            reports: Default::default(),
        },
        store,
        Rc::new(db),
    );

    let (release_save, wait_for_save) = oneshot::channel();
    *write_barrier.borrow_mut() = Some(wait_for_save);
    let mut context = Context::from_waker(Waker::noop());
    let mut saving = pin!(client.keep(late));
    assert!(saving.as_mut().poll(&mut context).is_pending());
    assert!(
        write_barrier.borrow().is_none(),
        "the save reached the store"
    );

    client.note_refused_creation(
        "00000000-0000-4000-8000-0000000000aa",
        jazz::db::Error::new(jazz::db::ErrorCode::WriteRejected, "refused"),
    );
    assert_eq!(client.refused_creations(), 1);

    let mut shutdown = runtime.call("shutdown".into(), Value::Null);
    assert!(shutdown.as_mut().poll(&mut context).is_pending());
    assert!(client.with_held(|keys| keys.space(&first).is_some()));
    release_save.send(()).unwrap();
    assert!(matches!(
        saving.as_mut().poll(&mut context),
        Poll::Ready(Ok(()) | Err(ClientError::Closed))
    ));
    assert_eq!(
        shutdown.as_mut().poll(&mut context),
        Poll::Ready(Ok(Value::Null))
    );

    assert!(
        client
            .with_account(|a| a.epoch_keys.epoch_ids().count())
            .is_none()
    );
    assert!(!client.has_device_account_identity());
    assert_eq!(client.refused_creations(), 0);
    assert!(client.with_held(|keys| keys.spaces().is_empty()));
    assert!(matches!(client.device(), Err(ClientError::Closed)));
    assert!(matches!(
        block_on(client.keep(later)),
        Err(ClientError::Closed)
    ));
    let scopes = memory.scopes();
    let [scope] = scopes.as_slice() else {
        panic!("shutdown should leave one scope on disk, found {scopes:?}");
    };
    let documents = block_on(memory.list(scope)).unwrap();
    assert!(documents.contains(&format!("held-{first}")));
    assert!(documents.contains(&format!("held-{late_root}")));
    assert_eq!(stored_verified_depth(&memory, &first), Some(0));

    // An opted-in sign-out deletes this scope after closing the runtime.
    block_on(memory.delete(scope)).unwrap();
    let fresh = open_account(memory.clone(), who, settings, None);
    assert!(fresh.with_held(|held| held.spaces().is_empty()));
    assert_eq!(stored_verified_depth(&memory, &first), None);
}

/// Alice stops her host after challenging a waiting device and queueing a
/// delivery pass. The challenge secret drops and the caller wakes immediately.
#[test]
fn stopping_drops_a_pending_approval_and_cancels_a_queued_command() {
    let alice = account(0xa1);
    let settings = jazz::e2ee::EncryptionSettings::default();
    let server = secure_authority_with(&settings, None);
    let store = Rc::new(JsStore::served_by(serve(Rc::new(MemoryStore::new()))));
    let client = Rc::new(open_account(store.clone(), alice, settings.clone(), None));
    let (db, _link) = session(&client, &server, alice, &settings, 1, None);
    let db = Rc::new(db);
    drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    let other = open_account(Rc::new(MemoryStore::new()), alice, settings.clone(), None);
    let (other_db, _other_link) = session(&other, &server, alice, &settings, 2, None);
    assert_eq!(
        drive(&server, &[&other_db], other.ensure_account(&other_db)).unwrap(),
        jazz_e2ee_client::AccountStatus::AwaitingApproval
    );
    let runtime = Encryption::new(
        Built {
            client,
            spaces: Value::Null,
            reports: Default::default(),
        },
        store,
        db.clone(),
    );
    let challenge = runtime.call(
        "challenge".into(),
        serde_json::json!({ "deviceId": other.device().unwrap().id }),
    );
    drive(&server, &[&db], challenge).unwrap();
    assert_eq!(runtime.pending_approval_count(), 1);
    let mut deliver = runtime.call("keysDeliver".into(), Value::Null);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(deliver.as_mut().poll(&mut cx).is_pending());
    runtime.stop();
    assert_eq!(runtime.pending_approval_count(), 0);
    assert!(
        matches!(deliver.as_mut().poll(&mut cx), Poll::Ready(Err(message)) if message.starts_with("closed:"))
    );
}
