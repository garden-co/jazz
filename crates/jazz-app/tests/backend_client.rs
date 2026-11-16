//! A trusted backend client acting for users of an app whose permissions
//! scope rows to their owner: what each user's handle reads and writes.

#[path = "bindings/guarded.rs"]
mod guarded;
#[allow(dead_code)]
mod relay;

use jazz_app::{AppFiles, AppId, Auth, Backend, Config, Error, Jazz, TokenReason};
use jazz_server::JazzServer;

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/guarded");

fn files() -> AppFiles {
    AppFiles::read(FIXTURE).unwrap()
}

fn token(token: &'static str) -> impl jazz_app::TokenProvider + 'static {
    move |_: TokenReason| async move { Ok(Some(token.to_owned())) }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trusted_client_does_not_act_for_users_yet() {
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

    let job = Jazz::open(
        files(),
        Config::new(app_id)
            .server(&endpoint)
            .credential(token("machine-trusted")),
    )
    .await
    .unwrap();
    // Acting for a user is not supported yet.
    let unsupported = job.for_user("a", serde_json::Value::Null).await;
    assert!(
        matches!(unsupported, Err(Error::Unsupported(_))),
        "{unsupported:?}"
    );

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
