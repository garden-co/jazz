//! `jazz-app-test-relay`: a stand-in for an app backend's Jazz endpoint
//! (`createJazz`'s handler), so bindings in other languages (Swift, Go) can
//! test end users against a running endpoint. It serves `tests/relay/mod.rs`,
//! the same relay jazz-app's and jazz-ffi's end-user tests use.
//!
//! ```text
//! cargo build -p jazz-app --features test-relay --bin jazz-app-test-relay
//! jazz-app-test-relay --core <url> --app-id <uuid> \
//!     --backend-secret <s> --admin-secret <s> --bundle <id>
//! ```
//!
//! It prints the endpoint URL (`http://127.0.0.1:<port>/jazz`, a client's
//! `server`) as one line on stdout, then serves until killed; it exits
//! cleanly on SIGTERM or SIGINT.
//!
//! - `--core`: the core's base URL (a running `jazz-server`).
//! - `--backend-secret`, `--admin-secret`: the core's secrets for the app.
//! - `--bundle`: the bundle the relay admits end users' sockets to. Get it by
//!   opening a backend (jazz-ffi, or jazz-app's `Backend`) on the core with
//!   the app's permissions (e.g. StagePlan's `jazz.permissions.json`) and
//!   calling `bundle()` on it.
//!
//! Auth: a device key signs in on its own; a bearer `Authorization: Bearer
//! token-<user>` is authenticated as user `<user>`.

#[path = "../../tests/relay/mod.rs"]
#[allow(dead_code)]
mod relay;

use std::io::Write;
use std::process::ExitCode;

use jazz_app::AppId;

const USAGE: &str = "usage: jazz-app-test-relay --core <url> --app-id <uuid> \
    --backend-secret <s> --admin-secret <s> --bundle <id>";

struct Args {
    core: String,
    app_id: AppId,
    backend_secret: String,
    admin_secret: String,
    bundle: String,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let (mut core, mut app_id, mut backend_secret, mut admin_secret, mut bundle) =
        (None, None, None, None, None);
    while let Some(flag) = args.next() {
        if flag == "-h" || flag == "--help" {
            return Err(USAGE.into());
        }
        let slot = match flag.as_str() {
            "--core" => &mut core,
            "--app-id" => &mut app_id,
            "--backend-secret" => &mut backend_secret,
            "--admin-secret" => &mut admin_secret,
            "--bundle" => &mut bundle,
            _ => return Err(format!("unknown argument {flag:?}\n{USAGE}")),
        };
        *slot = Some(args.next().ok_or_else(|| format!("{flag} needs a value"))?);
    }
    let need =
        |value: Option<String>, flag: &str| value.ok_or_else(|| format!("missing {flag}\n{USAGE}"));
    let app_id = need(app_id, "--app-id")?;
    Ok(Args {
        core: need(core, "--core")?,
        app_id: AppId::from_string(&app_id)
            .map_err(|error| format!("--app-id {app_id:?}: {error}"))?,
        backend_secret: need(backend_secret, "--backend-secret")?,
        admin_secret: need(admin_secret, "--admin-secret")?,
        bundle: need(bundle, "--bundle")?,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let relay = relay::start(
        &args.core,
        args.app_id,
        &args.backend_secret,
        &args.admin_secret,
        &args.bundle,
    )
    .await;
    let mut stdout = std::io::stdout().lock();
    if writeln!(stdout, "{}", relay.endpoint)
        .and_then(|()| stdout.flush())
        .is_err()
    {
        return ExitCode::FAILURE;
    }
    drop(stdout);

    shutdown().await;
    drop(relay);
    ExitCode::SUCCESS
}

/// SIGTERM or SIGINT.
async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate()).expect("listen for SIGTERM");
        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
