//! Probes jazz-app's backend-client contract against a TypeScript main
//! backend (`createJazz` with `trustWorkload`), for
//! `tests/backend-client.test.ts`: what the stage-plan gate does not cover.
//!
//! ```text
//! rust-backend-client-probe <trusted|scoped|untrusted>
//! ```
//!
//! Environment: `JAZZ_SERVER_URL`, `JAZZ_APP_ID`, `JAZZ_TOKEN`, `JAZZ_APP_DIR`
//! (the app's `jazz.app.json` and `jazz.permissions.json`); for `trusted`,
//! `PROBE_USER` (has an account), `PROBE_NO_ACCOUNT_USER` (has none) and
//! `PROBE_REVOKED_USER` (whose identity the core refuses).
//!
//! Prints one JSON line per step, `{"step", "ok", "detail"}`, and exits
//! non-zero if any step failed.

use std::process::ExitCode;
use std::time::Duration;

use jazz_app::{
    AppFiles, AppId, BackendClientKind, CLOSE_TIMEOUT, Config, Error, Jazz, TokenReason,
};
use serde_json::{Value, json};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

struct Steps {
    failed: bool,
}

impl Steps {
    fn report(&mut self, step: &str, ok: bool, detail: impl std::fmt::Display) {
        self.failed |= !ok;
        println!(
            "{}",
            json!({ "step": step, "ok": ok, "detail": detail.to_string() })
        );
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let token = env("JAZZ_TOKEN");
    let config = Config::new(AppId::from_string(&env("JAZZ_APP_ID")).expect("JAZZ_APP_ID"))
        .server(env("JAZZ_SERVER_URL"))
        .credential(move |_: TokenReason| {
            let token = token.clone();
            async move { Ok(Some(token)) }
        });
    let files = AppFiles::read(env("JAZZ_APP_DIR")).expect("JAZZ_APP_DIR");
    let mut steps = Steps { failed: false };
    let opened = Jazz::open(files, config).await;
    match mode.as_str() {
        "untrusted" => {
            let refused = matches!(opened, Err(Error::Refused(_)));
            steps.report("open_refused", refused, format!("{:?}", opened.err()));
        }
        "scoped" => match opened {
            Ok(jazz) => {
                let kind = jazz.backend_client();
                steps.report(
                    "kind",
                    kind == Some(BackendClientKind::Scoped),
                    format!("{kind:?}"),
                );
                let for_user = jazz.for_user("anyone", Value::Null).await;
                steps.report(
                    "for_user_refused",
                    matches!(for_user, Err(Error::Config(_))),
                    format!("{:?}", for_user.err()),
                );
                let closed = jazz.close(CLOSE_TIMEOUT).await;
                steps.report("close", closed.is_ok(), format!("{closed:?}"));
            }
            Err(error) => steps.report("open", false, error),
        },
        "trusted" => match opened {
            Ok(jazz) => trusted(&jazz, &mut steps).await,
            Err(error) => steps.report("open", false, error),
        },
        other => steps.report("mode", false, format!("unknown mode {other:?}")),
    }
    if steps.failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

async fn trusted(jazz: &Jazz, steps: &mut Steps) {
    let kind = jazz.backend_client();
    steps.report(
        "kind",
        kind == Some(BackendClientKind::Trusted),
        format!("{kind:?}"),
    );

    let user = env("PROBE_USER");
    let acting = jazz.for_user(&user, json!({ "role": "probe" })).await;
    steps.report(
        "for_user",
        acting.as_ref().is_ok_and(Jazz::acts_for_user),
        format!("{:?}", acting.as_ref().err()),
    );

    let missing = env("PROBE_NO_ACCOUNT_USER");
    let none = jazz.for_user(&missing, Value::Null).await;
    steps.report(
        "for_user_no_account",
        matches!(&none, Err(Error::NoAccount(user)) if *user == missing),
        format!("{:?}", none.err()),
    );

    let revoked = env("PROBE_REVOKED_USER");
    let refused = jazz.for_user(&revoked, Value::Null).await;
    steps.report(
        "for_user_revoked",
        matches!(&refused, Err(Error::Refused(message)) if message.contains("identity_not_authorized")),
        format!("{:?}", refused.err()),
    );

    let closed = tokio::time::timeout(Duration::from_secs(30), jazz.close(CLOSE_TIMEOUT)).await;
    steps.report("close", matches!(closed, Ok(Ok(()))), format!("{closed:?}"));
    let after = jazz.for_user(&user, Value::Null).await;
    steps.report(
        "closed",
        matches!(after, Err(Error::Stopped)),
        format!("{:?}", after.err()),
    );
}
