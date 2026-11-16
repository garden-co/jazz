//! A trusted backend client acting for users of an app whose permissions
//! scope rows to their owner: what each user's handle reads and writes.

#[path = "bindings/guarded.rs"]
mod guarded;
#[allow(dead_code)]
mod relay;

use std::time::Duration;

use guarded::{NewNote, notes};
use jazz_app::{
    AppFiles, AppId, Auth, Backend, Config, Error, Jazz, ObjectId, ReadTier, TokenReason,
};
use jazz_server::JazzServer;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/guarded");

fn files() -> AppFiles {
    AppFiles::read(FIXTURE).unwrap()
}

fn token(token: &'static str) -> impl jazz_app::TokenProvider + 'static {
    move |_: TokenReason| async move { Ok(Some(token.to_owned())) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trusted_client_reads_as_each_user_by_the_apps_permissions() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .unwrap();
    let backend = Jazz::open(
        files(),
        Config::new(app_id)
            .server(server.base_url())
            .backend(Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                "notes-service",
            )),
    )
    .await
    .unwrap();
    let relay = relay::start(
        &server.base_url(),
        app_id,
        server.backend_secret(),
        server.admin_secret(),
        backend.bundle().unwrap(),
    )
    .await;
    let endpoint = relay.endpoint.clone();
    let user = |bearer: &'static str| {
        Jazz::open(
            files(),
            Config::new(app_id)
                .server(&endpoint)
                .user(Auth::bearer(token(bearer))),
        )
    };
    // A and B sign in once, so each has an account to be acted for.
    let a = user("token-a").await.unwrap();
    let b = user("token-b").await.unwrap();
    let owner_a = ObjectId::from_uuid(a.account().unwrap().0);

    let job = Jazz::open(
        files(),
        Config::new(app_id)
            .server(&endpoint)
            .credential(token("machine-trusted")),
    )
    .await
    .unwrap();
    let as_a = job.for_user("a", serde_json::Value::Null).await.unwrap();
    let as_b = job.for_user("b", serde_json::Value::Null).await.unwrap();

    let note = as_a
        .insert(NewNote::new(owner_a, "A's note"))
        .await
        .unwrap();
    // A reads their own row back, locally and from the server.
    assert_eq!(as_a.all(notes::query()).await.unwrap(), vec![note.clone()]);
    let remote = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = as_a
                .all_with(notes::query(), ReadTier::Remote)
                .await
                .unwrap();
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("A's row from the server");
    assert_eq!(remote, vec![note.clone()]);
    // B sees none of it, and may not write as A.
    assert_eq!(as_b.all(notes::query()).await.unwrap(), vec![]);
    let refused = as_b
        .durable(jazz_app::DurabilityTier::GlobalServer)
        .insert(NewNote::new(owner_a, "Not B's"))
        .await;
    assert!(refused.is_err(), "{refused:?}");
    // A's own client sees what the job wrote as them.
    let mine = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = a.all_with(notes::query(), ReadTier::Remote).await.unwrap();
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(mine, vec![note]);

    job.close(jazz_app::CLOSE_TIMEOUT).await.unwrap();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    backend.shutdown().await.unwrap();
    server.shutdown().await;
}

#[tokio::test]
async fn a_backend_client_needs_the_apps_permissions() {
    let schema_only = AppFiles::new(
        std::fs::read(format!("{FIXTURE}/jazz.app.json")).unwrap(),
        None,
    );
    let opened = Jazz::open(
        schema_only,
        Config::new(AppId::random())
            .server("http://localhost:1/jazz")
            .credential(token("machine-trusted")),
    )
    .await;
    assert!(matches!(opened, Err(Error::App(_))), "{opened:?}");
}
