//! Durability against a real server whose permissions refuse some writes:
//! a durable write waits for the server's verdict and fails on a refusal,
//! and `wait_for_sync` surfaces a refusal of an earlier plain write.

#[path = "bindings/guarded.rs"]
mod bindings;

use bindings::{NewNote, notes};
use jazz_app::{AppId, Backend, Config, DurabilityTier, Jazz, ObjectId};
use jazz_server::JazzServer;
use std::path::Path;
use std::time::Duration;

async fn open(server: &JazzServer, app_id: AppId) -> Jazz {
    let permissions = include_bytes!("fixtures/guarded/jazz.permissions.json");
    bindings::open(
        Config::new(app_id).server(server.base_url()).backend(
            Backend::new(server.backend_secret(), server.admin_secret(), "alice")
                .permissions(permissions.as_slice()),
        ),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_writes_wait_for_the_servers_verdict() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let jazz = open(&server, app_id).await;
    let me = ObjectId::from_uuid(jazz.account().unwrap().0);
    let someone_else = ObjectId::new();
    let durable = jazz.durable(DurabilityTier::GlobalServer);

    // Allowed: returns once the server accepted it.
    let mine = durable.insert(NewNote::new(me, "mine")).await.unwrap();
    assert_eq!(mine.owner, me);

    // Refused: the durable write fails with the server's verdict.
    let refused = durable.insert(NewNote::new(someone_else, "not mine")).await;
    assert!(refused.is_err(), "{refused:?}");

    // A plain write returns at once; wait_for_sync reports its refusal.
    jazz.insert(NewNote::new(someone_else, "also not mine"))
        .await
        .unwrap();
    let synced = jazz.wait_for_sync().await;
    assert!(synced.is_err(), "{synced:?}");

    // A durable transaction fails the same way, and leaves nothing behind.
    let refused = durable
        .transaction(move |tx| async move {
            tx.insert(NewNote::new(someone_else, "in a transaction"))?;
            Ok(())
        })
        .await;
    assert!(refused.is_err(), "{refused:?}");

    // What the server accepted is all that stays.
    let remote = jazz
        .all_with(notes::query(), jazz_app::ReadTier::Remote)
        .await
        .unwrap();
    assert_eq!(remote, vec![mine]);

    jazz.shutdown().await.unwrap();
    server.shutdown().await;
}

/// A transaction body that panics rolls back and reports it.
#[tokio::test]
async fn a_panicking_transaction_rolls_back() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let id = ObjectId::new();
    let panicked = jazz
        .transaction(move |tx| async move {
            tx.insert(NewNote::new(id, "half done"))?;
            let fail = true;
            if fail {
                panic!("boom");
            }
            Ok(())
        })
        .await;
    assert!(
        matches!(&panicked, Err(jazz_app::Error::Panicked(message)) if message == "boom"),
        "{panicked:?}"
    );
    assert!(jazz.all(notes::query()).await.unwrap().is_empty());
    // The client runs on, and the next transaction commits.
    jazz.transaction(move |tx| async move {
        tx.insert(NewNote::new(id, "done"))?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(jazz.all(notes::query()).await.unwrap().len(), 1);
}

/// A durable write that can't reach its tier is refused before writing.
#[tokio::test]
async fn an_unreachable_tier_writes_nothing() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    let refused = jazz
        .durable(DurabilityTier::GlobalServer)
        .insert(NewNote::new(ObjectId::new(), "nowhere"))
        .await;
    assert!(
        matches!(refused, Err(jazz_app::Error::Config(_))),
        "{refused:?}"
    );
    assert!(jazz.all(notes::query()).await.unwrap().is_empty());
}

fn open_dir(server: &JazzServer, app_id: AppId, dir: &Path) -> Config {
    let permissions = include_bytes!("fixtures/guarded/jazz.permissions.json");
    Config::new(app_id)
        .server(server.base_url())
        .storage(jazz_app::Storage::Dir(dir.to_path_buf()))
        .backend(
            Backend::new(server.backend_secret(), server.admin_secret(), "alice")
                .permissions(permissions.as_slice()),
        )
}

async fn next_error(errors: &mut jazz_app::MutationErrors) -> jazz_app::MutationError {
    use futures::StreamExt;
    tokio::time::timeout(Duration::from_secs(5), errors.next())
        .await
        .expect("a mutation error")
        .expect("the stream is open")
}

/// A plain write the server refuses is a mutation error, delivered even
/// when it arrived before anyone listened; a durable write's refusal is
/// its own wait's, not a mutation error. A reported refusal's payload is
/// gone from the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_errors_report_unwaited_refusals() {
    use futures::StreamExt;
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let data = tempfile::TempDir::new().unwrap();
    let jazz = bindings::open(open_dir(&server, app_id, data.path()))
        .await
        .unwrap();
    let someone_else = ObjectId::new();

    // Two refused before anyone listens, and a durable one, whose wait
    // reports it.
    jazz.insert(NewNote::new(someone_else, "early 1"))
        .await
        .unwrap();
    jazz.insert(NewNote::new(someone_else, "early 2"))
        .await
        .unwrap();
    assert!(jazz.wait_for_sync().await.is_err());
    assert!(
        jazz.durable(DurabilityTier::GlobalServer)
            .insert(NewNote::new(someone_else, "waited"))
            .await
            .is_err()
    );
    // Well past the reporter's poll: the early ones are buffered, not
    // still in the client, when the listener comes.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut errors = jazz.mutation_errors().await.unwrap();
    let first = next_error(&mut errors).await;
    let second = next_error(&mut errors).await;
    assert!(first.denied && second.denied, "{first:?} {second:?}");
    assert_ne!(first.transaction, second.transaction);
    assert_eq!(
        serde_json::to_value(&first).unwrap(),
        serde_json::json!({
            "transaction": first.transaction.to_string(),
            "message": first.message,
            "denied": true,
            "serializable": false,
        })
    );
    // A later one, live.
    jazz.insert(NewNote::new(someone_else, "late"))
        .await
        .unwrap();
    let late = next_error(&mut errors).await;
    assert!(![first.transaction, second.transaction].contains(&late.transaction));
    // Nothing else: the durable refusal isn't there.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), errors.next())
            .await
            .is_err()
    );

    jazz.shutdown().await.unwrap();
    // Reported refusals keep no payload in the store.
    let store = rusqlite::Connection::open(data.path().join("jazz.sqlite")).unwrap();
    let kept: i64 = store
        .query_row("SELECT count(*) FROM _jazz_rejected", [], |row| row.get(0))
        .unwrap();
    assert_eq!(kept, 0, "refused payloads left in the store");
    server.shutdown().await;
}

/// Disconnecting keeps local work going and stops sync: another client
/// doesn't see the write until reconnecting syncs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_stops_sync_until_reconnect() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let data = tempfile::TempDir::new().unwrap();
    let jazz = bindings::open(open_dir(&server, app_id, data.path()))
        .await
        .unwrap();
    let observer = open(&server, app_id).await;
    let me = ObjectId::from_uuid(jazz.account().unwrap().0);

    jazz.disconnect().await.unwrap();
    let offline = jazz
        .insert(NewNote::new(me, "written offline"))
        .await
        .unwrap();
    assert_eq!(
        jazz.all(notes::query()).await.unwrap(),
        vec![offline.clone()]
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        observer
            .all_with(notes::query(), jazz_app::ReadTier::Remote)
            .await
            .unwrap()
            .is_empty(),
        "a disconnected client's write reached the server"
    );

    jazz.reconnect().await.unwrap();
    jazz.wait_for_sync().await.unwrap();
    assert_eq!(
        observer
            .all_with(notes::query(), jazz_app::ReadTier::Remote)
            .await
            .unwrap(),
        vec![offline]
    );
    jazz.shutdown().await.unwrap();
    observer.shutdown().await.unwrap();
    server.shutdown().await;
}

/// Without a server, disconnect and reconnect do nothing, and return.
#[tokio::test]
async fn disconnect_and_reconnect_without_a_server() {
    let jazz = bindings::open(Config::new(AppId::random())).await.unwrap();
    jazz.disconnect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), jazz.reconnect())
        .await
        .expect("reconnect returns without a server")
        .unwrap();
    let note = jazz
        .insert(NewNote::new(ObjectId::new(), "local"))
        .await
        .unwrap();
    assert_eq!(jazz.all(notes::query()).await.unwrap(), vec![note]);
}

/// Deleting the client storage removes every store file: the next open is
/// empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_client_storage_removes_every_file() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let data = tempfile::TempDir::new().unwrap();
    let jazz = bindings::open(open_dir(&server, app_id, data.path()))
        .await
        .unwrap();
    let me = ObjectId::from_uuid(jazz.account().unwrap().0);
    jazz.insert(NewNote::new(me, "kept")).await.unwrap();
    jazz.wait_for_sync().await.unwrap();
    jazz.shutdown().await.unwrap();
    // What a crash leaves next to the store.
    let files = [
        "jazz.sqlite",
        "jazz.sqlite-wal",
        "jazz.sqlite-shm",
        "client_id",
    ];
    for file in &files[1..3] {
        std::fs::write(data.path().join(file), b"").unwrap();
    }
    for file in files {
        assert!(data.path().join(file).exists(), "{file} before deleting");
    }

    jazz.delete_client_storage().await.unwrap();
    for file in files {
        assert!(!data.path().join(file).exists(), "{file} left behind");
    }
    let reopened = bindings::open(open_dir(&server, app_id, data.path()))
        .await
        .unwrap();
    assert!(
        reopened.all(notes::query()).await.unwrap().is_empty(),
        "the local store starts empty"
    );
    reopened.shutdown().await.unwrap();
    server.shutdown().await;
}

/// A store another app has open, in this process or another, is not
/// deleted: the other app keeps it, and its writes.
#[tokio::test]
async fn delete_client_storage_refuses_a_held_store() {
    let app_id = AppId::random();
    let data = tempfile::TempDir::new().unwrap();
    let config = || Config::new(app_id).storage(jazz_app::Storage::Dir(data.path().into()));
    let a = bindings::open(config()).await.unwrap();
    let b = bindings::open(config()).await.unwrap();

    let refused = a.delete_client_storage().await;
    assert!(
        matches!(refused, Err(jazz_app::Error::StoreHeld(_))),
        "{refused:?}"
    );
    // Both run on, on the same store.
    let note = b
        .insert(NewNote::new(ObjectId::new(), "b's"))
        .await
        .unwrap();
    assert!(data.path().join("jazz.sqlite").exists());
    assert!(a.all(notes::query()).await.is_ok());
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    let reopened = bindings::open(config()).await.unwrap();
    assert_eq!(reopened.all(notes::query()).await.unwrap(), vec![note]);

    // Another process holds the store's lock while it has it open.
    let other = std::fs::File::open(data.path().join("jazz.lock")).unwrap();
    other.lock_shared().unwrap();
    let refused = reopened.delete_client_storage().await;
    assert!(
        matches!(refused, Err(jazz_app::Error::StoreHeld(_))),
        "{refused:?}"
    );
    assert!(data.path().join("jazz.sqlite").exists());
    drop(other);
    reopened.delete_client_storage().await.unwrap();
    assert!(!data.path().join("jazz.sqlite").exists());
}

/// Text anchors find a position again after edits around it.
#[test]
fn text_anchors() {
    use jazz_app::text::{Anchor, Side};
    let anchor = Anchor::capture("hello world", None, 6, Side::Before);
    let resolved = anchor.resolve("well, hello world", None);
    assert_eq!(resolved.position, 12);
}
