//! The `jazz-app-test-relay` bin, as a binding's test runs it: a core, the
//! bin printing its endpoint, and an end user's device-key client syncing
//! through it. The bundle comes from a backend the test opens (`--bundle`),
//! or the bin publishes it itself (`--app`/`--permissions`), for
//! client-only bindings. Needs `--features test-relay`.

#[path = "bindings/todos.rs"]
mod bindings;

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bindings::{NewTodo, Todo, todos};
use futures::StreamExt;
use jazz_app::{AppFiles, AppId, Auth, Backend, Config, DeviceKey, Jazz, Link, ReadTier};
use jazz_server::JazzServer;

const APP: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/todos/jazz.app.json"
);
const PERMISSIONS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/todos/jazz.permissions.json"
);

/// The built bin: Cargo's `CARGO_BIN_EXE_*` when it sets it (it doesn't
/// here, as with jazz-cli's tests), else the bin beside this test's `deps/`.
fn relay_bin() -> std::path::PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_jazz-app-test-relay") {
        return path.into();
    }
    let test = std::env::current_exe().unwrap();
    let profile = test.parent().and_then(|deps| deps.parent()).unwrap();
    let bin = profile.join(format!(
        "jazz-app-test-relay{}",
        std::env::consts::EXE_SUFFIX
    ));
    assert!(
        bin.is_file(),
        "build it: cargo build -p jazz-app --features test-relay ({})",
        bin.display()
    );
    bin
}

/// The running bin, and the endpoint it printed. Killed if a test fails
/// before it stops it.
struct Bin {
    child: Option<Child>,
    endpoint: String,
}

impl Bin {
    /// Runs the bin against `server`, with the secrets from env and `args`
    /// naming the bundle.
    async fn start(server: &JazzServer, app_id: AppId, args: &[&str]) -> Self {
        let mut child = Command::new(relay_bin())
            .args(["--core", &server.base_url()])
            .args(["--app-id", &app_id.to_string()])
            .args(args)
            .env("JAZZ_BACKEND_SECRET", server.backend_secret())
            .env("JAZZ_ADMIN_SECRET", server.admin_secret())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn jazz-app-test-relay");
        let stdout = child.stdout.take().unwrap();
        let endpoint = tokio::task::spawn_blocking(move || {
            let mut line = String::new();
            BufReader::new(stdout).read_line(&mut line).unwrap();
            line.trim().to_owned()
        })
        .await
        .unwrap();
        assert!(
            endpoint.starts_with("http://") && endpoint.ends_with("/jazz"),
            "{endpoint:?}"
        );
        Self {
            child: Some(child),
            endpoint,
        }
    }

    /// SIGTERM stops it cleanly.
    async fn terminate(mut self) {
        let mut child = self.child.take().unwrap();
        let pid = child.id().to_string();
        let killed = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
        assert!(killed.success());
        let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
            .await
            .unwrap();
        assert!(status.success(), "{status:?}");
    }
}

impl Drop for Bin {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn start_server() -> (AppId, JazzServer) {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    (app_id, server)
}

/// A device account's client through the bin, once connected.
async fn device(app_id: AppId, endpoint: &str, key: &DeviceKey) -> Jazz {
    let files = AppFiles::new(std::fs::read(APP).unwrap(), None);
    let jazz = Jazz::open(
        files,
        Config::new(app_id)
            .server(endpoint)
            .user(Auth::Device(key.clone())),
    )
    .await
    .expect("open as a device account through the bin");
    assert_eq!(jazz.account(), Some(key.account(app_id)));
    let mut changes = jazz.sync_changes();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(status) = changes.next().await {
            if status.link == Link::Connected {
                return;
            }
        }
    })
    .await
    .expect("the client connects through the bin");
    jazz
}

/// Every todo `jazz` reads from the server, once there is one.
async fn remote_todos(jazz: &Jazz) -> Vec<Todo> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = jazz
                .all_with(todos::query(), ReadTier::Remote)
                .await
                .unwrap();
            if !rows.is_empty() {
                break rows;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the row reaches the server")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_syncs_through_the_bin() {
    let (app_id, server) = start_server().await;
    let backend = bindings::open(
        Config::new(app_id).server(server.base_url()).backend(
            Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                "todo-service",
            )
            .permissions(std::fs::read(PERMISSIONS).unwrap()),
        ),
    )
    .await
    .expect("open the app's backend");
    let bin = Bin::start(&server, app_id, &["--bundle", backend.bundle().unwrap()]).await;

    let phone = device(app_id, &bin.endpoint, &DeviceKey::generate().unwrap()).await;
    let written = phone
        .insert(NewTodo::new("Through the bin", false))
        .await
        .unwrap();
    assert_eq!(remote_todos(&backend).await, vec![written]);

    bin.terminate().await;
    phone.shutdown().await.unwrap();
    backend.shutdown().await.unwrap();
    server.shutdown().await;
}

/// A client-only binding's test: no backend of its own, the bin publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_bin_publishes_the_bundle_itself() {
    let (app_id, server) = start_server().await;
    let bin = Bin::start(
        &server,
        app_id,
        &["--app", APP, "--permissions", PERMISSIONS],
    )
    .await;

    let key = DeviceKey::generate().unwrap();
    let phone = device(app_id, &bin.endpoint, &key).await;
    let written = phone
        .insert(NewTodo::new("Published by the bin", false))
        .await
        .unwrap();
    // Another device of the same account reads it back through the bin.
    let laptop = device(app_id, &bin.endpoint, &key).await;
    assert_eq!(remote_todos(&laptop).await, vec![written]);

    bin.terminate().await;
    phone.shutdown().await.unwrap();
    laptop.shutdown().await.unwrap();
    server.shutdown().await;
}
