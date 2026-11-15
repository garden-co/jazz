//! Durability against a real server whose permissions refuse some writes:
//! a durable write waits for the server's verdict and fails on a refusal,
//! and `wait_for_sync` surfaces a refusal of an earlier plain write.

#[path = "bindings/guarded.rs"]
mod bindings;

use bindings::{NewNote, notes};
use jazz_app::{AppId, Backend, Config, DurabilityTier, Jazz, ObjectId};
use jazz_server::JazzServer;

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

/// A plain write the server refuses is a mutation error, delivered even
/// when it arrived before anyone listened; a durable write's refusal is
/// its own wait's, not a mutation error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutation_errors_report_unwaited_refusals() {
    use futures::StreamExt;
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let jazz = open(&server, app_id).await;
    let someone_else = ObjectId::new();

    // Refused before anyone listens.
    jazz.insert(NewNote::new(someone_else, "early"))
        .await
        .unwrap();
    assert!(jazz.wait_for_sync().await.is_err());
    // A durable write's refusal is its wait's.
    assert!(
        jazz.durable(DurabilityTier::GlobalServer)
            .insert(NewNote::new(someone_else, "waited"))
            .await
            .is_err()
    );
    let mut errors = jazz.mutation_errors().await.unwrap();
    let early = tokio::time::timeout(std::time::Duration::from_secs(5), errors.next())
        .await
        .unwrap()
        .unwrap();
    assert!(early.denied, "{early:?}");
    // A later one, live.
    jazz.insert(NewNote::new(someone_else, "late"))
        .await
        .unwrap();
    let late = tokio::time::timeout(std::time::Duration::from_secs(5), errors.next())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(late.transaction, early.transaction);
    // Nothing else: the durable refusal isn't there.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), errors.next())
            .await
            .is_err()
    );

    jazz.shutdown().await.unwrap();
    server.shutdown().await;
}

/// Disconnecting keeps local work going; reconnecting syncs it. Deleting
/// the client storage leaves the next open empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_reconnect_and_delete_storage() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let data = tempfile::TempDir::new().unwrap();
    let permissions = include_bytes!("fixtures/guarded/jazz.permissions.json");
    let open_dir = || {
        bindings::open(
            Config::new(app_id)
                .server(server.base_url())
                .storage(jazz_app::Storage::Dir(data.path().to_path_buf()))
                .backend(
                    Backend::new(server.backend_secret(), server.admin_secret(), "alice")
                        .permissions(permissions.as_slice()),
                ),
        )
    };
    let jazz = open_dir().await.unwrap();
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
    jazz.reconnect().await.unwrap();
    jazz.wait_for_sync().await.unwrap();
    let remote = jazz
        .all_with(notes::query(), jazz_app::ReadTier::Remote)
        .await
        .unwrap();
    assert_eq!(remote, vec![offline]);

    jazz.delete_client_storage().await.unwrap();
    assert!(!data.path().join("jazz.sqlite").exists());
    let reopened = open_dir().await.unwrap();
    assert!(
        reopened.all(notes::query()).await.unwrap().is_empty(),
        "the local store starts empty"
    );
    reopened.shutdown().await.unwrap();
    server.shutdown().await;
}

/// Text anchors find a position again after edits around it.
#[test]
fn text_anchors() {
    use jazz_app::text::{Anchor, Side};
    let anchor = Anchor::capture("hello world", None, 6, Side::Before);
    let resolved = anchor.resolve("well, hello world", None);
    assert_eq!(resolved.position, 12);
}
