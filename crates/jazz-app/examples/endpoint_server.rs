//! A Rust backend serving an app's Jazz endpoint at `/jazz`, over Axum: what
//! a TS backend's `createJazz` handler does, for the app's browser and
//! native clients.
//!
//! ```sh
//! # The core it fronts (in memory):
//! cargo run -p jazz-cli --bin jazz-tools -- server "$JAZZ_APP_ID" --in-memory \
//!   --backend-secret "$JAZZ_BACKEND_SECRET" --admin-secret "$JAZZ_ADMIN_SECRET"
//! # The backend, serving examples/stage-plan/apps/rust-cli's files:
//! cargo run -p jazz-app --features axum --example endpoint_server -- examples/stage-plan/apps/rust-cli
//! ```
//!
//! Environment:
//! - `JAZZ_APP_ID` (a UUID), `JAZZ_CORE_URL` (default `http://localhost:1625`),
//!   `JAZZ_BACKEND_SECRET`, `JAZZ_ADMIN_SECRET`: the core;
//! - `JAZZ_JWT_SECRET`, `JAZZ_JWT_ISSUER`: the app's own sign-in, HS256 JWTs
//!   in `Authorization: Bearer`. The user is `sub`, the claims are the
//!   token's `claims` object, the answer holds until `exp`;
//! - `JAZZ_ACCEPT_DEVICE_ACCOUNTS=1`: let device accounts sync too;
//! - `PORT` (default 4000).
//!
//! Clients connect to `http://localhost:$PORT/jazz`.

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use hmac::Mac as _;
use jazz_app::endpoint::{Answer, AuthUser, Endpoint, Request};
use jazz_app::{AppFiles, AppId, Backend, Config, Jazz};
use serde_json::Value;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

/// The app's check of its own JWT: HS256 signature, issuer and expiry.
fn verify(token: &str, secret: &[u8], issuer: &str) -> Option<AuthUser> {
    let decode = |part: &str| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(part)
            .ok()
    };
    let mut parts = token.split('.');
    let (head, claims, signature) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let header: Value = serde_json::from_slice(&decode(head)?).ok()?;
    if header["alg"] != "HS256" {
        return None;
    }
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).ok()?;
    mac.update(format!("{head}.{claims}").as_bytes());
    mac.verify_slice(&decode(signature)?).ok()?;
    let claims: Value = serde_json::from_slice(&decode(claims)?).ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let exp = claims["exp"].as_u64()?;
    if claims["iss"] != issuer || exp <= now {
        return None;
    }
    let user = AuthUser::new(claims["sub"].as_str()?, exp);
    Some(match &claims["claims"] {
        Value::Object(extra) => user.claims(extra.clone()),
        _ => user,
    })
}

#[tokio::main]
async fn main() -> jazz_app::Result<()> {
    let dir = std::env::args().nth(1).unwrap_or_else(|| ".".into());
    let app_id = AppId::from_string(&env("JAZZ_APP_ID")).expect("JAZZ_APP_ID is a UUID");
    let core = std::env::var("JAZZ_CORE_URL").unwrap_or_else(|_| "http://localhost:1625".into());
    let backend = Jazz::open(
        AppFiles::read(&dir)?,
        Config::new(app_id).server(core).backend(Backend::new(
            env("JAZZ_BACKEND_SECRET"),
            env("JAZZ_ADMIN_SECRET"),
            "endpoint-server",
        )),
    )
    .await?;
    let (secret, issuer) = (env("JAZZ_JWT_SECRET"), env("JAZZ_JWT_ISSUER"));
    let devices = std::env::var("JAZZ_ACCEPT_DEVICE_ACCOUNTS").is_ok_and(|v| v == "1");
    let endpoint = Endpoint::new(&backend, move |request: Request| {
        let answer = match request.bearer() {
            Some(token) => verify(token, secret.as_bytes(), &issuer).map(Answer::User),
            None => devices.then_some(Answer::DeviceAccount),
        };
        std::future::ready(Ok(answer))
    })?;
    let port = std::env::var("PORT").unwrap_or_else(|_| "4000".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("bind the port");
    println!("serving http://localhost:{port}/jazz for app {app_id}");
    axum::serve(listener, endpoint.router())
        .await
        .expect("serve");
    Ok(())
}
