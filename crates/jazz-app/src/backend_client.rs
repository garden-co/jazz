//! A backend client: another trusted machine (a CI job, a worker, a
//! partner's service) that syncs with the app through its main backend's
//! `/jazz`, proving itself with a machine credential (an OIDC workload
//! token), as TypeScript's `createJazzClient({ serverUrl, credential })`
//! (`backend/create-jazz-session.ts`, `docs/setup/backend-client.mdx`).
//!
//! The main backend decides what the credential is trusted as
//! (`backendClients` / `trustWorkload` in `createJazz`):
//!
//! - **trusted** (`as: "backend"`): the client acts as the system, with the
//!   main backend's authority, and can act for any user
//!   ([`crate::Jazz::for_user`]);
//! - **scoped** (`as: () => ({ userId })`): it acts as that one user, with
//!   their permissions, as if `authenticate` had named them.

use std::time::Duration;

use jazz::account_registry::{AccountId, SYSTEM_ACCOUNT_ID};
use jazz::tools::{AppContext, AppId, JazzClient, Session};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::auth::{Bearer, TokenReason};
use crate::error::{Error, Result};
use crate::handle::Jazz;
use crate::session::{Credential, User, UserKind};

/// What a trusted backend client states as its secret: its credential, on
/// the upgrade, states its access, and the main backend replaces this with
/// its own (TypeScript's `BACKEND_CLIENT_STAND_IN_SECRET`).
const STAND_IN_SECRET: &str = "stated-by-backend-credential";

/// The issuer of the system identity a trusted client acts as.
const SYSTEM_ISSUER: &str = "urn:jazz:system";

/// What the main backend trusts a backend client's credential as.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BackendClientKind {
    /// It acts as the system, and can act for any user.
    Trusted,
    /// It acts as one user, with their permissions.
    Scoped,
}

/// A backend client's way back to its main backend, for
/// [`Jazz::for_user`].
#[derive(Clone)]
pub(crate) struct BackendClientLink {
    pub kind: BackendClientKind,
    endpoint: String,
    app: AppId,
    credential: Bearer,
    /// The bundle the main backend serves, and this client's own, when
    /// they differ: a trusted client then acts for no user, since it
    /// evaluates their reads by its own permissions (jazz#5808).
    bundle_mismatch: Option<(String, String)>,
}

/// The header a main backend names the permissions bundle it serves by,
/// on its `backend/admit` answer.
const BUNDLE_HEADER: &str = "x-jazz-bundle";

fn http() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| Error::Server(error.to_string()))
}

fn app_url(endpoint: &str, app: AppId, path: &str) -> String {
    format!("{}/apps/{app}/{path}", endpoint.trim_end_matches('/'))
}

async fn token(credential: &Bearer, reason: TokenReason) -> Result<String> {
    credential.token(reason).await?.ok_or_else(|| {
        Error::Refused("the backend client's credential provider gave no token".into())
    })
}

/// Whether the main backend admits `token` as a trusted machine
/// (`backend/admit`): `None` when it does not (it may trust it as a user),
/// otherwise the bundle it serves, when it says.
async fn admits_as_trusted(
    endpoint: &str,
    app: AppId,
    token: &str,
) -> Result<Option<Option<String>>> {
    let response = http()?
        .post(app_url(endpoint, app, "backend/admit"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|error| Error::Server(format!("admitting the backend client: {error}")))?;
    let bundle = response
        .headers()
        .get(BUNDLE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_ascii_lowercase());
    match response.status().as_u16() {
        204 | 200 => Ok(Some(bundle)),
        401 | 404 => Ok(None),
        status => Err(Error::Server(format!(
            "admitting the backend client: the main backend answered {status}"
        ))),
    }
}

/// Connects a backend client: admitted as trusted, it syncs as the system
/// with the stand-in secret and its credential on the upgrade; otherwise it
/// signs in as the user the main backend trusts the credential as.
pub(crate) async fn connect(
    mut context: AppContext,
    connector: std::sync::Arc<jazz_native_transport::HttpStreamFallbackConnector>,
    credential: Bearer,
    endpoint: &str,
    own_bundle: Option<String>,
) -> Result<(JazzClient, User, Credential, BackendClientLink)> {
    let app = context.app_id;
    let first = token(&credential, TokenReason::Connect).await?;
    let link = |kind, bundle_mismatch| BackendClientLink {
        kind,
        endpoint: endpoint.to_owned(),
        app,
        credential: credential.clone(),
        bundle_mismatch,
    };
    if let Some(served) = admits_as_trusted(endpoint, app, &first).await? {
        let bundle_mismatch = match (served, own_bundle) {
            (Some(served), Some(own)) if served != own.to_ascii_lowercase() => Some((served, own)),
            _ => None,
        };
        context.backend_secret = Some(STAND_IN_SECRET.to_owned());
        let client =
            JazzClient::connect_with_native_transport_bearer(context, connector, first.clone())
                .await?;
        let user = User {
            account: SYSTEM_ACCOUNT_ID,
            issuer: SYSTEM_ISSUER.to_owned(),
            subject: "system".to_owned(),
            claims: json!({}),
            kind: UserKind::System,
        };
        let link = link(BackendClientKind::Trusted, bundle_mismatch);
        let credential = Credential::Bearer {
            provider: credential,
            token: first,
        };
        return Ok((client, user, credential, link));
    }
    let Some(signed_in) = crate::backend::sign_in(endpoint, app, &first).await? else {
        return Err(Error::Refused(
            "the main backend trusts this backend client's credential neither as a trusted \
             machine nor as a user (see trustWorkload in its backendClients)"
                .into(),
        ));
    };
    let mut session = Session::new(&signed_in.identity.issuer, &signed_in.identity.subject)
        .with_claims(signed_in.claims.clone());
    session.account_id = Some(signed_in.account);
    context.account_id = Some(signed_in.account);
    context.backend_session = Some(session);
    let client =
        JazzClient::connect_with_native_transport_bearer(context, connector, first.clone()).await?;
    let user = User {
        account: signed_in.account,
        issuer: signed_in.identity.issuer,
        subject: signed_in.identity.subject,
        claims: signed_in.claims,
        kind: UserKind::Backend,
    };
    let link = link(BackendClientKind::Scoped, None);
    let credential = Credential::Bearer {
        provider: credential,
        token: first,
    };
    Ok((client, user, credential, link))
}

/// The session `user_id` is acted for with (`backend/user-session`): the
/// account the main backend's `forReqUser` would act as. Never creates one.
async fn user_session(link: &BackendClientLink, user_id: &str, claims: Value) -> Result<Session> {
    let bearer = token(&link.credential, TokenReason::Renew).await?;
    let response = http()?
        .post(app_url(&link.endpoint, link.app, "backend/user-session"))
        .bearer_auth(bearer)
        .json(&json!({ "userId": user_id }))
        .send()
        .await
        .map_err(|error| Error::Server(format!("for_user: {error}")))?;
    let status = response.status().as_u16();
    let body: Value = response.json().await.unwrap_or(Value::Null);
    match status {
        200 => {}
        404 if body["code"] == "no_account" => return Err(Error::NoAccount(user_id.to_owned())),
        403 => {
            return Err(Error::Refused(format!(
                "for_user: the main backend refused {user_id:?} (identity_not_authorized: it \
                 was revoked)"
            )));
        }
        401 => {
            return Err(Error::Refused(
                "for_user: the main backend no longer trusts this client as a trusted machine"
                    .into(),
            ));
        }
        status => {
            return Err(Error::Server(format!(
                "for_user: the main backend answered {status}"
            )));
        }
    }
    #[derive(Deserialize)]
    struct Answer {
        account: AccountId,
        issuer: String,
    }
    let Answer { account, issuer } = serde_json::from_value(body).map_err(|_| {
        Error::Server("for_user: the main backend's answer names no account".into())
    })?;
    let claims = match claims {
        Value::Null => json!({}),
        claims => claims,
    };
    let mut session = Session::new(issuer, user_id).with_claims(claims);
    session.account_id = Some(account);
    Ok(session)
}

impl Jazz {
    /// What the main backend trusts this backend client as, for a client
    /// opened with [`crate::Config::credential`].
    pub fn backend_client(&self) -> Option<BackendClientKind> {
        self.info.backend_client.as_ref().map(|link| link.kind)
    }

    /// This trusted backend client acting for one of the app's users (TS
    /// `client.forUser(userId, { claims })`): reads and writes carry their
    /// identity and are checked against their permissions, which see
    /// exactly `claims` (`{}` for `Value::Null`). `user_id` is the id the
    /// main backend's `authenticate` names them by.
    ///
    /// Their writes are checked by the core, against the permissions the
    /// main backend serves. Their reads are evaluated by this client,
    /// against the permissions it was opened with (as TS's): open it with
    /// the app's published `jazz.permissions.json`. A main backend that
    /// names the bundle it serves (`X-Jazz-Bundle` on `backend/admit`) is
    /// checked: with other permissions, `for_user` fails with
    /// [`Error::App`] naming both bundles (jazz#5808).
    ///
    /// Never creates an account: a user who has none yet fails with
    /// [`Error::NoAccount`]; one the main backend refuses (revoked) with
    /// [`Error::Refused`]. Only a trusted client acts for users: a scoped
    /// one already acts as its own.
    pub async fn for_user(&self, user_id: &str, claims: Value) -> Result<Jazz> {
        let link = match &self.info.backend_client {
            Some(link) if link.kind == BackendClientKind::Trusted && !self.for_user => link,
            Some(link) if link.kind == BackendClientKind::Scoped => {
                return Err(Error::Config(
                    "for_user: this backend client is scoped, and acts as its own user".into(),
                ));
            }
            _ => {
                return Err(Error::Config(
                    "for_user is a trusted backend client's (Config::credential); on the main \
                     backend, use its Endpoint"
                        .into(),
                ));
            }
        };
        if user_id.is_empty() {
            return Err(Error::Config(
                "for_user: pass the user's id, as the main backend's authenticate names it".into(),
            ));
        }
        if let Some((served, own)) = &link.bundle_mismatch {
            // A user's reads are evaluated here, by this client's own
            // permissions: refuse rather than read by stale ones.
            return Err(Error::App(format!(
                "for_user: this client's permissions (bundle {own}) differ from the ones the \
                 main backend serves (bundle {served}); open it with the app's published \
                 jazz.permissions.json"
            )));
        }
        // On the client's runtime, whatever the caller's executor.
        let (link, user) = (link.clone(), user_id.to_owned());
        let session = self
            .with_client(move |_| async move { user_session(&link, &user, claims).await })
            .await?;
        Ok(self.scoped(session))
    }
}

/// How long [`Jazz::close`] waits for writes by default, as TypeScript's
/// `close()`.
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

impl Jazz {
    /// Closes the client once the writes it holds have reached the server,
    /// waiting at most `timeout` ([`CLOSE_TIMEOUT`] as TypeScript's
    /// `close()`; `close({ timeoutMs })`). With [`crate::Storage::Memory`],
    /// writes that still have not arrived are lost, and this fails with
    /// [`Error::Unsynced`] saying how many; `Duration::ZERO` closes at once
    /// and accepts that. A persistent store keeps them for the next open.
    ///
    /// A handle acting for a user ([`Jazz::for_user`]) closes nothing: the
    /// client is its trusted client's.
    pub async fn close(&self, timeout: Duration) -> Result<()> {
        if self.for_user {
            return Ok(());
        }
        if !self.info.serverless && !timeout.is_zero() {
            // An outage, or the server refusing, ends the wait: what is still
            // pending then is counted below.
            // Every write the client holds, its users' handles' too (TS
            // `close()` drains them all), not only this handle's: wait until
            // none is pending, re-reading on each change of the client's
            // status. The wait ends on the client's thread too, so shutting
            // down does not wait for it in turn.
            let _ = self
                .with_client(move |client| async move {
                    let mut changes = client.status_changes();
                    let drained = async {
                        while client.pending_uploads() > 0 {
                            if changes.changed().await.is_err() {
                                break;
                            }
                        }
                    };
                    let _ = tokio::time::timeout(timeout, drained).await;
                    Ok(())
                })
                .await;
        }
        let pending = self
            .with_client(|client| async move { Ok(client.pending_uploads()) })
            .await
            .unwrap_or(0);
        self.shutdown().await?;
        if pending > 0 && self.info.memory && !self.info.serverless && !timeout.is_zero() {
            return Err(Error::Unsynced { pending });
        }
        Ok(())
    }
}
