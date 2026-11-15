//! `jazz-app-test-relay`: a stand-in for an app backend's Jazz endpoint
//! (`createJazz`'s handler), so bindings in other languages (Swift, Go,
//! Kotlin) can test end users against a running endpoint. It serves
//! `tests/relay/mod.rs`, the same relay jazz-app's and jazz-ffi's end-user
//! tests use.
//!
//! ```text
//! cargo build -p jazz-app --features test-relay --bin jazz-app-test-relay
//! JAZZ_BACKEND_SECRET=<s> JAZZ_ADMIN_SECRET=<s> \
//!     jazz-app-test-relay --core <url> --app-id <uuid> \
//!     --app <jazz.app.json> --permissions <jazz.permissions.json>
//! # or, with a bundle the caller published itself:
//! JAZZ_BACKEND_SECRET=<s> JAZZ_ADMIN_SECRET=<s> \
//!     jazz-app-test-relay --core <url> --app-id <uuid> --bundle <id>
//! ```
//!
//! It prints the endpoint URL (`http://127.0.0.1:<port>/jazz`, a client's
//! `server`) as one line on stdout, once it is ready, then serves until
//! killed; it exits cleanly on SIGTERM or SIGINT.
//!
//! - `--core`: the core's base URL (a running `jazz-server`).
//! - `JAZZ_BACKEND_SECRET`, `JAZZ_ADMIN_SECRET`: the core's secrets for the
//!   app, as `jazz-tools server` reads them. `--backend-secret <s>` and
//!   `--admin-secret <s>` work too, but put them in argv, where `ps` shows
//!   them.
//! - The bundle the relay admits end users' sockets to, one of:
//!   - `--app <path> --permissions <path>`: the relay opens a jazz-app
//!     backend on the core with the app's `jazz.app.json` and
//!     `jazz.permissions.json` (e.g. StagePlan's), which publishes them, and
//!     serves the bundle that backend reports. The backend stays open while
//!     the relay serves. For client-only bindings, which open no backend.
//!   - `--bundle <id>`: a bundle the caller published, e.g. by opening a
//!     backend (jazz-ffi, or jazz-app's `Backend`) with the app's permissions
//!     and calling `bundle()` on it.
//!
//! Auth: a device key signs in on its own; a bearer `Authorization: Bearer
//! token-<user>` is authenticated as user `<user>`.

#[path = "../../tests/relay/mod.rs"]
#[allow(dead_code)]
mod relay;

use std::io::Write;
use std::process::ExitCode;

use jazz_app::{AppFiles, AppId, Backend, Config, Jazz};

const USAGE: &str = "usage: JAZZ_BACKEND_SECRET=<s> JAZZ_ADMIN_SECRET=<s> \
    jazz-app-test-relay --core <url> --app-id <uuid> \
    (--app <jazz.app.json> --permissions <jazz.permissions.json> | --bundle <id>) \
    [--backend-secret <s>] [--admin-secret <s>]";

/// The backend's user, when the relay publishes the bundle itself.
const BACKEND_USER: &str = "jazz-app-test-relay";

enum Bundle {
    Given(String),
    Publish { app: String, permissions: String },
}

struct Args {
    core: String,
    app_id: AppId,
    backend_secret: String,
    admin_secret: String,
    bundle: Bundle,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let (mut core, mut app_id, mut backend_secret, mut admin_secret) = (None, None, None, None);
    let (mut bundle, mut app, mut permissions) = (None, None, None);
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
            "--app" => &mut app,
            "--permissions" => &mut permissions,
            _ => return Err(format!("unknown argument {flag:?}\n{USAGE}")),
        };
        *slot = Some(args.next().ok_or_else(|| format!("{flag} needs a value"))?);
    }
    let need =
        |value: Option<String>, flag: &str| value.ok_or_else(|| format!("missing {flag}\n{USAGE}"));
    let secret = |value: Option<String>, flag: &str, env: &str| {
        value
            .or_else(|| std::env::var(env).ok())
            .ok_or_else(|| format!("missing {env} (or {flag})\n{USAGE}"))
    };
    let bundle = match (bundle, app, permissions) {
        (Some(bundle), None, None) => Bundle::Given(bundle),
        (None, Some(app), Some(permissions)) => Bundle::Publish { app, permissions },
        (None, None, None) => {
            return Err(format!(
                "missing --app and --permissions, or --bundle\n{USAGE}"
            ));
        }
        (Some(_), _, _) => {
            return Err(format!(
                "--bundle or --app/--permissions, not both\n{USAGE}"
            ));
        }
        (None, _, _) => return Err(format!("--app and --permissions go together\n{USAGE}")),
    };
    let app_id = need(app_id, "--app-id")?;
    Ok(Args {
        core: need(core, "--core")?,
        app_id: AppId::from_string(&app_id)
            .map_err(|error| format!("--app-id {app_id:?}: {error}"))?,
        backend_secret: secret(backend_secret, "--backend-secret", "JAZZ_BACKEND_SECRET")?,
        admin_secret: secret(admin_secret, "--admin-secret", "JAZZ_ADMIN_SECRET")?,
        bundle,
    })
}

/// Opens a backend with the app's files, which publishes them: the backend,
/// to keep open while serving, and its bundle.
async fn publish(args: &Args, app: &str, permissions: &str) -> Result<(Jazz, String), String> {
    let read = |path: &str| std::fs::read(path).map_err(|error| format!("read {path}: {error}"));
    let files = AppFiles::new(read(app)?, None);
    let config = Config::new(args.app_id).server(&args.core).backend(
        Backend::new(&args.backend_secret, &args.admin_secret, BACKEND_USER)
            .permissions(read(permissions)?),
    );
    let backend = Jazz::open(files, config)
        .await
        .map_err(|error| format!("open a backend to publish {app}: {error}"))?;
    let bundle = backend
        .bundle()
        .ok_or("the backend reports no bundle")?
        .to_owned();
    Ok((backend, bundle))
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
    let (backend, bundle) = match &args.bundle {
        Bundle::Given(bundle) => (None, bundle.clone()),
        Bundle::Publish { app, permissions } => match publish(&args, app, permissions).await {
            Ok((backend, bundle)) => (Some(backend), bundle),
            Err(message) => {
                eprintln!("{message}");
                return ExitCode::FAILURE;
            }
        },
    };
    let relay = relay::start(
        &args.core,
        args.app_id,
        &args.backend_secret,
        &args.admin_secret,
        &bundle,
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
    if let Some(backend) = backend {
        let _ = backend.shutdown().await;
    }
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
