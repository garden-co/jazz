//! TEST HARNESS, not an example backend to copy: what client bindings'
//! end-to-end tests (Kotlin's `examples/stage-plan/apps/kotlin-cli`) run
//! against. In one process: a Jazz server, the app's backend publishing its
//! permissions bundle, and jazz-app's test stand-in for a backend's Jazz
//! endpoint (`tests/relay`, as `jazz-app-test-relay` serves it), whose
//! `authenticate` takes `Authorization: Bearer token-<user>` as user
//! `<user>`. A real app's backend is `createJazz` (TypeScript), a Rust or a
//! Go backend.
//!
//! ```text
//! cargo run -p jazz-app --example test_app_backend -- <jazz.app.json> <jazz.permissions.json>
//! ```
//!
//! prints `{"appId": …, "endpoint": "http://127.0.0.1:<port>/jazz"}` on one
//! line, then serves until its stdin closes.

#[allow(dead_code)]
#[path = "../tests/relay/mod.rs"]
mod relay;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [app, permissions] = args.as_slice() else {
        eprintln!("usage: test_app_backend <jazz.app.json> <jazz.permissions.json>");
        std::process::exit(2);
    };
    let app_id = jazz_app::AppId::random();
    let server = jazz_server::JazzServer::builder()
        .with_app_id(app_id)
        .start()
        .await
        .expect("start the Jazz server");
    let files = jazz_app::AppFiles::new(std::fs::read(app).expect("read jazz.app.json"), None)
        .with_permissions(std::fs::read(permissions).expect("read jazz.permissions.json"));
    let backend = jazz_app::Jazz::open(
        files,
        jazz_app::Config::new(app_id)
            .server(server.base_url())
            .backend(jazz_app::Backend::new(
                server.backend_secret(),
                server.admin_secret(),
                "test-app-backend",
            )),
    )
    .await
    .expect("open the app's backend");
    let relay = relay::start(
        &server.base_url(),
        app_id,
        server.backend_secret(),
        server.admin_secret(),
        backend.bundle().expect("the backend's bundle"),
    )
    .await;
    println!(
        "{}",
        serde_json::json!({ "appId": app_id.to_string(), "endpoint": relay.endpoint })
    );
    // Serve until stdin closes (the test that started it ends).
    tokio::task::spawn_blocking(|| {
        use std::io::Read;
        let mut buffer = [0u8; 64];
        while std::io::stdin().read(&mut buffer).is_ok_and(|n| n > 0) {}
    })
    .await
    .ok();
    drop(relay);
    drop(backend);
    drop(server);
}
