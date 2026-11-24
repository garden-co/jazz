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

use common::{address, authority, hold_new_space, new_space_key, online, open_client, project};

/// Alice signs out with a key save in flight. Hold the client and its DB
/// alive so the test can observe the keys the host would otherwise drop.
///
/// save starts ──► shutdown waits ──► store replies ──► memory clears
#[test]
fn shutdown_drains_the_private_store_and_clears_held_keys() {
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
    let client = Rc::new(open_client(store.clone()));
    let server = authority();
    let (db, _link, _) = online(&client, &server, None);
    let first = hold_new_space(&client, &db, project(&db, "first"));
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
}
