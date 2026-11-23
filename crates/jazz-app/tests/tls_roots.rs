//! Which CAs a client's HTTP requests trust: the OS's, so an app behind a
//! private CA signs in, links and acts for users, beside the webpki roots.

#[path = "bindings/todos.rs"]
mod bindings;
#[allow(dead_code)]
mod relay;
#[path = "../../jazz-native-transport/tests/tls/mod.rs"]
mod tls;

use std::time::Duration;

use bindings::{NewTodo, todos};
use jazz_app_support::{
    AppFiles, AppId, Auth, Backend, Config, DeviceKey, Error, Jazz, ReadTier, UserKind,
};
use jazz_server::JazzServer;
use tls::{Store, Tls, with_os_store};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/todos");

fn backend(server: &JazzServer) -> Backend {
    Backend::new(
        server.backend_secret(),
        server.admin_secret(),
        "todo-service",
    )
    .permissions(std::fs::read(format!("{FIXTURES}/jazz.permissions.json")).unwrap())
}

fn client_files() -> AppFiles {
    let app = std::fs::read(format!("{FIXTURES}/jazz.app.json")).unwrap();
    AppFiles::new(app, None)
}

fn bearer(token: &'static str) -> Auth {
    Auth::bearer(move |_| async move { Ok(Some(token.to_owned())) })
}

/// A server, the app's backend and the stand-in for its endpoint, all plain.
struct App {
    app_id: AppId,
    server: JazzServer,
    backend: Jazz,
    relay: relay::Relay,
}

impl App {
    async fn start() -> Self {
        let app_id = AppId::random();
        let server = JazzServer::builder()
            .with_app_id(app_id)
            .start()
            .await
            .expect("start a Jazz server");
        let backend = bindings::open(
            Config::new(app_id)
                .server(server.base_url())
                .backend(backend(&server)),
        )
        .await
        .expect("open the app's backend");
        let relay = relay::start(
            &server.base_url(),
            app_id,
            server.backend_secret(),
            server.admin_secret(),
            backend.bundle().unwrap(),
        )
        .await;
        Self {
            app_id,
            server,
            backend,
            relay,
        }
    }

    async fn open(&self, endpoint: &str, auth: Auth) -> jazz_app_support::Result<Jazz> {
        Jazz::open(
            client_files(),
            Config::new(self.app_id).server(endpoint).user(auth),
        )
        .await
    }

    async fn stop(self) {
        self.backend.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

/// `jazz` writes a todo and reads it back from the server.
async fn syncs(jazz: &Jazz) {
    let todo = jazz
        .insert(NewTodo::new("Over TLS", false))
        .await
        .expect("insert");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let rows = jazz
                .all_with(todos::query(), ReadTier::Remote)
                .await
                .expect("a remote read");
            if rows.contains(&todo) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the write reaches the server");
}

/// Alice signs in and syncs at an app whose certificate is from a CA only
/// her OS trusts, also when that store holds a certificate that doesn't
/// parse.
#[test]
fn signing_in_trusts_a_ca_the_os_trusts() {
    with_os_store(
        "signing_in_trusts_a_ca_the_os_trusts",
        &[Store::PrivateCa, Store::PrivateCaAmongInvalid],
        |tls: Tls| async move {
            let app = App::start().await;
            let https = tls.proxy(&app.relay.endpoint).await;
            let alice = app
                .open(&https.url, bearer("token-alice"))
                .await
                .expect("sign in over HTTPS");
            syncs(&alice).await;
            alice.shutdown().await.unwrap();
            app.stop().await;
        },
    );
}

/// Carol links her device's account to her sign-in at such an app.
#[test]
fn linking_trusts_a_ca_the_os_trusts() {
    with_os_store(
        "linking_trusts_a_ca_the_os_trusts",
        &[Store::PrivateCa],
        |tls: Tls| async move {
            let app = App::start().await;
            let https = tls.proxy(&app.relay.endpoint).await;
            let key = DeviceKey::generate().unwrap();
            let device = app
                .open(&https.url, Auth::Device(key.clone()))
                .await
                .expect("open with a device account");
            syncs(&device).await;
            let carol = device
                .session()
                .unwrap()
                .link("token-carol")
                .await
                .expect("link over HTTPS");
            assert_eq!(carol.account, key.account(app.app_id));
            assert_eq!(carol.kind, UserKind::Backend);
            device.shutdown().await.unwrap();
            app.stop().await;
        },
    );
}

/// A trusted job is admitted at such an app and acts for alice.
#[test]
fn for_user_trusts_a_ca_the_os_trusts() {
    with_os_store(
        "for_user_trusts_a_ca_the_os_trusts",
        &[Store::PrivateCa],
        |tls: Tls| async move {
            let app = App::start().await;
            let https = tls.proxy(&app.relay.endpoint).await;
            // Alice's account, made without TLS in the way.
            let alice = app
                .open(&app.relay.endpoint, bearer("token-alice"))
                .await
                .unwrap();
            let job = Jazz::open(
                AppFiles::read(FIXTURES).unwrap(),
                Config::new(app.app_id)
                    .server(&https.url)
                    .credential(|_| async { Ok(Some("machine-trusted".to_owned())) }),
            )
            .await
            .expect("admitted over HTTPS");
            let as_alice = job
                .for_user("alice", serde_json::Value::Null)
                .await
                .expect("act for alice over HTTPS");
            syncs(&as_alice).await;
            job.shutdown().await.unwrap();
            alice.shutdown().await.unwrap();
            app.stop().await;
        },
    );
}

/// The app's backend publishes to and syncs with a server behind such a CA.
#[test]
fn a_backend_trusts_a_ca_the_os_trusts() {
    with_os_store(
        "a_backend_trusts_a_ca_the_os_trusts",
        &[Store::PrivateCa],
        |tls: Tls| async move {
            let app_id = AppId::random();
            let server = JazzServer::builder()
                .with_app_id(app_id)
                .start()
                .await
                .expect("start a Jazz server");
            let https = tls.proxy(&server.base_url()).await;
            let jazz = bindings::open(
                Config::new(app_id)
                    .server(&https.url)
                    .backend(backend(&server)),
            )
            .await
            .expect("publish and enrol over HTTPS");
            syncs(&jazz).await;
            jazz.shutdown().await.unwrap();
            server.shutdown().await;
        },
    );
}

/// The app's endpoint reaches a server behind such a CA to sign alice in.
#[cfg(feature = "unstable-endpoint")]
#[test]
fn an_endpoint_trusts_a_ca_the_os_trusts() {
    use jazz_app_support::endpoint::{Answer, AuthUser, Endpoint, Request};
    with_os_store(
        "an_endpoint_trusts_a_ca_the_os_trusts",
        &[Store::PrivateCa],
        |tls: Tls| async move {
            let app_id = AppId::random();
            let server = JazzServer::builder()
                .with_app_id(app_id)
                .start()
                .await
                .expect("start a Jazz server");
            let https = tls.proxy(&server.base_url()).await;
            let jazz = bindings::open(
                Config::new(app_id)
                    .server(&https.url)
                    .backend(backend(&server)),
            )
            .await
            .expect("open the app's backend over HTTPS");
            let endpoint = Endpoint::new(&jazz, |_: Request| async {
                Ok(Some(Answer::User(AuthUser::new("alice", u64::MAX))))
            })
            .unwrap();
            let response = endpoint
                .handle(
                    Request::new(
                        "POST",
                        format!("/jazz/apps/{app_id}/accounts/backend-sign-in"),
                    )
                    .header("authorization", "Bearer token-alice"),
                )
                .await;
            assert_eq!(response.status, 200, "{}", response.text_body());
            endpoint.shutdown().await.unwrap();
            jazz.shutdown().await.unwrap();
            server.shutdown().await;
        },
    );
}

/// Mallory's CA is in no store: alice's client never sends it her bearer.
#[test]
fn signing_in_refuses_a_ca_nobody_trusts() {
    with_os_store(
        "signing_in_refuses_a_ca_nobody_trusts",
        &[Store::AnotherCa, Store::Empty, Store::AllInvalid],
        |tls: Tls| async move {
            let app = App::start().await;
            let https = tls.proxy(&app.relay.endpoint).await;
            let refused = app.open(&https.url, bearer("token-alice")).await;
            assert!(matches!(refused, Err(Error::Server(_))), "{refused:?}");
            assert_eq!(https.handshakes(), 0);
            app.stop().await;
        },
    );
}

/// An OS store with nothing usable in it costs alice nothing: her client
/// opens, and signs in where no private CA is involved.
#[test]
fn a_bad_os_store_does_not_stop_a_client() {
    with_os_store(
        "a_bad_os_store_does_not_stop_a_client",
        &[
            Store::Empty,
            Store::Missing,
            Store::Unreadable,
            Store::AllInvalid,
        ],
        |_: Tls| async move {
            let app = App::start().await;
            let alice = app
                .open(&app.relay.endpoint, bearer("token-alice"))
                .await
                .expect("open and sign in");
            syncs(&alice).await;
            alice.shutdown().await.unwrap();
            app.stop().await;
        },
    );
}
