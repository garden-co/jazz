//! The `jazz-app-test-relay` bin, as a binding's test runs it: a core, a
//! backend for the bundle, the bin printing its endpoint, and an end user's
//! device-key client syncing through it. Needs `--features test-relay`.

#[path = "bindings/todos.rs"]
mod bindings;

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use bindings::{NewTodo, todos};
use futures::StreamExt;
use jazz_app::{AppFiles, AppId, Auth, Backend, Config, DeviceKey, Jazz, Link, ReadTier};
use jazz_server::JazzServer;

/// Kills the bin if the test fails before it stops it.
struct Bin(Child);

impl Drop for Bin {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_device_account_syncs_through_the_bin() {
    let app_id = AppId::random();
    let server = JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start a Jazz server");
    let permissions = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/todos/jazz.permissions.json"
    ))
    .unwrap();
    let backend = bindings::open(
        Config::new(app_id).server(server.base_url()).backend(
            Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                "todo-service",
            )
            .permissions(permissions),
        ),
    )
    .await
    .expect("open the app's backend");

    let mut bin = Bin(Command::new(relay_bin())
        .args(["--core", &server.base_url()])
        .args(["--app-id", &app_id.to_string()])
        .args(["--backend-secret", server.backend_secret()])
        .args(["--admin-secret", server.admin_secret()])
        .args(["--bundle", backend.bundle().unwrap()])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn jazz-app-test-relay"));
    let stdout = bin.0.stdout.take().unwrap();
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

    let files = AppFiles::new(
        std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/todos/jazz.app.json"
        ))
        .unwrap(),
        None,
    );
    let key = DeviceKey::generate().unwrap();
    let phone = Jazz::open(
        files,
        Config::new(app_id)
            .server(&endpoint)
            .user(Auth::Device(key.clone())),
    )
    .await
    .expect("open as a device account through the bin");
    assert_eq!(phone.account(), Some(key.account(app_id)));
    let mut changes = phone.sync_changes();
    tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(status) = changes.next().await {
            if status.link == Link::Connected {
                return;
            }
        }
    })
    .await
    .expect("the client connects through the bin");

    let written = phone
        .insert(NewTodo::new("Through the bin", false))
        .await
        .unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = backend
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
    .expect("the row reaches the backend");
    assert_eq!(seen, vec![written]);

    // SIGTERM stops it cleanly.
    let pid = bin.0.id().to_string();
    let killed = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
    assert!(killed.success());
    let status = tokio::task::spawn_blocking(move || bin.0.wait().unwrap())
        .await
        .unwrap();
    assert!(status.success(), "{status:?}");

    phone.shutdown().await.unwrap();
    backend.shutdown().await.unwrap();
    server.shutdown().await;
}
