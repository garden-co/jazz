//! An end user's client, signed in through the app's own backend
//! (`createJazz`), as a React Native or browser client is: the app's bearer
//! names the user to the backend's `authenticate`, the backend's sign-in
//! returns their account and session, and the backend relays the client's
//! sync socket to the core.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use jazz::account_registry::AccountId;
use jazz::link::LinkPhase;
use jazz::tools::{AppId, JazzClient, Session};
use serde::Deserialize;

use crate::backend::{app_url, http, refusal};
use crate::error::{Error, Result};

/// What [`EndUser::new`]'s callback returns: the app's bearer for the user.
pub type TokenFuture = Pin<Box<dyn Future<Output = Result<String>> + Send>>;

/// Open the app as one of its end users, signed in through the app's backend
/// with the app's own bearer (an access token its `authenticate` accepts).
///
/// [`Config::server`](crate::Config::server) is then the backend's Jazz mount,
/// e.g. `https://app.example/jazz`, not the core.
///
/// ```no_run
/// # async fn open() -> jazz_app::Result<()> {
/// use jazz_app::{AppFiles, Config, EndUser, Storage};
/// let config = Config::new(jazz_app::AppId::from_name("todos"))
///     .server("https://todos.example/jazz")
///     .storage(Storage::Dir("./todo-data".into()))
///     .end_user(EndUser::new(|| async { Ok(fetch_access_token().await) }));
/// let jazz = jazz_app::Jazz::open(AppFiles::read(".")?, config).await?;
/// # Ok(()) }
/// # async fn fetch_access_token() -> String { String::new() }
/// ```
#[derive(Clone)]
pub struct EndUser {
    token: Arc<dyn Fn() -> TokenFuture + Send + Sync>,
}

impl EndUser {
    /// `token` returns the app's current bearer for the user. It is asked
    /// again before the backend's sign-in expires, and whenever the backend
    /// refuses the bearer it has, so it should return a fresh one (refreshing
    /// it if need be).
    pub fn new<F, Fut>(token: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self {
            token: Arc::new(move || Box::pin(token())),
        }
    }

    /// A bearer that never changes.
    pub fn bearer(token: impl Into<String>) -> Self {
        let token = token.into();
        Self::new(move || {
            let token = token.clone();
            async move { Ok(token) }
        })
    }

    pub(crate) async fn token(&self) -> Result<String> {
        let token = (self.token)().await?;
        if token.trim().is_empty() {
            return Err(Error::Config("the end user's bearer is empty".into()));
        }
        Ok(token)
    }
}

impl std::fmt::Debug for EndUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndUser").finish_non_exhaustive()
    }
}

/// The backend's answer to a sign-in.
#[derive(Debug, Clone)]
pub(crate) struct SignedIn {
    /// The user's session; its `account_id` is always set.
    pub session: Session,
    /// Unix seconds: until when the backend's answer holds.
    pub expires_at: u64,
}

/// `POST <backend>/apps/<id>/accounts/backend-sign-in` with the app's bearer:
/// the backend's `authenticate` names the user and the core resolves (or
/// creates) their account.
pub(crate) async fn sign_in(
    http: &reqwest::Client,
    server: &str,
    app_id: AppId,
    token: &str,
) -> Result<SignedIn> {
    let response = http
        .post(app_url(server, app_id, "accounts/backend-sign-in"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|error| Error::Server(format!("signing in through the app's backend: {error}")))?;
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Err(Error::SignedOut);
    }
    if !status.is_success() {
        return Err(refusal("signing in through the app's backend", response).await);
    }
    #[derive(Deserialize)]
    struct Identity {
        issuer: String,
        subject: String,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Answer {
        account: Option<AccountId>,
        identity: Identity,
        #[serde(default)]
        claims: serde_json::Value,
        expires_at: u64,
    }
    let answer: Answer = response
        .json()
        .await
        .map_err(|error| Error::Server(format!("signing in through the app's backend: {error}")))?;
    let account = answer.account.ok_or_else(|| {
        Error::Server("the app's backend signed the user in without an account".into())
    })?;
    if account.is_system() {
        return Err(Error::Server(
            "the app's backend signed the user in as the system account".into(),
        ));
    }
    let claims = match answer.claims {
        claims @ serde_json::Value::Object(_) => claims,
        _ => serde_json::Value::Object(Default::default()),
    };
    let mut session =
        Session::new(answer.identity.issuer, answer.identity.subject).with_claims(claims);
    session.account_id = Some(account);
    Ok(SignedIn {
        session,
        expires_at: answer.expires_at,
    })
}

/// How long before an expiry the bearer is renewed.
const RENEW_EARLY: Duration = Duration::from_secs(60);
/// The longest wait between failed renewals.
const MAX_RETRY: Duration = Duration::from_secs(60);

/// An end user's sign-in, kept fresh while the client runs.
pub(crate) struct Renewal {
    pub end_user: EndUser,
    pub http: reqwest::Client,
    pub server: String,
    pub app_id: AppId,
    pub token: String,
    pub signed: SignedIn,
    /// Set once the bearer names another user and the client stops syncing.
    pub signed_out: Arc<AtomicBool>,
}

impl Renewal {
    /// Signs in at open: `connect` states this session and bearer.
    pub(crate) async fn start(
        end_user: &EndUser,
        server: &str,
        app_id: AppId,
        signed_out: Arc<AtomicBool>,
    ) -> Result<Self> {
        let http = http()?;
        let token = end_user.token().await?;
        let signed = sign_in(&http, server, app_id, &token).await?;
        Ok(Self {
            end_user: end_user.clone(),
            http,
            server: server.to_owned(),
            app_id,
            token,
            signed,
            signed_out,
        })
    }

    /// The account the user signed in as.
    pub(crate) fn account(&self) -> Option<AccountId> {
        self.signed.session.account_id
    }

    /// Keeps the client's bearer fresh: renews it shortly before the sign-in
    /// or the bearer itself expires, and when the backend refuses it. Ends
    /// with the client, or if the bearer comes to name another user (a store
    /// holds one user's data; open another for them): the client then stops
    /// syncing and [`Jazz::signed_out`](crate::Jazz::signed_out) turns true.
    pub(crate) async fn keep(mut self, client: JazzClient) {
        let mut status = client.upstream_status_changes();
        let mut renew_at = renewal_due(&self.token, self.signed.expires_at);
        let mut retry = Duration::from_secs(1);
        let mut not_before = 0;
        loop {
            let refused = matches!(
                status.borrow_and_update().as_ref().map(|s| &s.phase),
                Some(LinkPhase::AwaitingCredential(_))
            );
            let due = if refused {
                not_before
            } else {
                renew_at.max(not_before)
            };
            if now() < due {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(due - now())) => {}
                    changed = status.changed() => if changed.is_err() {
                        return; // the client closed
                    },
                }
                continue;
            }
            let renewed = async {
                let fresh = self.end_user.token().await?;
                let again = sign_in(&self.http, &self.server, self.app_id, &fresh).await?;
                Ok::<_, Error>((fresh, again))
            }
            .await;
            match renewed {
                Ok((fresh, again)) => {
                    if again.session.issuer != self.signed.session.issuer
                        || again.session.user_id != self.signed.session.user_id
                        || again.session.account_id != self.signed.session.account_id
                    {
                        tracing::error!(
                            "jazz-app: the end user's bearer now names another user; \
                             this client stops syncing (open another store for them)"
                        );
                        self.signed_out.store(true, Ordering::SeqCst);
                        client.stop_upstream();
                        return;
                    }
                    let changed = fresh != self.token || again.session != self.signed.session;
                    if (changed || refused)
                        && client
                            .update_relayed_sign_in(again.session.clone(), fresh.clone())
                            .is_err()
                    {
                        return; // the client closed
                    }
                    renew_at = renewal_due(&fresh, again.expires_at);
                    self.token = fresh;
                    self.signed = again;
                    if refused {
                        // The sign-in took the bearer but the socket refused
                        // it: back off, or it would sign in again at once.
                        not_before = now() + retry.as_secs();
                        retry = (retry * 2).min(MAX_RETRY);
                    } else {
                        retry = Duration::from_secs(1);
                        not_before = 0;
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "jazz-app: renewing the end user's sign-in failed");
                    not_before = now() + retry.as_secs();
                    retry = (retry * 2).min(MAX_RETRY);
                }
            }
        }
    }
}

/// When to renew: before the sign-in or (if it is a JWT) the bearer expires.
fn renewal_due(token: &str, signed_in_until: u64) -> u64 {
    let until = jwt_expiry(token).map_or(signed_in_until, |exp| exp.min(signed_in_until));
    until.saturating_sub(RENEW_EARLY.as_secs()).max(now() + 5)
}

/// The `exp` of a JWT bearer, read without verifying it: only to schedule a
/// renewal, never to trust anything.
fn jwt_expiry(token: &str) -> Option<u64> {
    let payload = token.split('.').nth(1)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    #[derive(Deserialize)]
    struct Claims {
        exp: Option<u64>,
    }
    serde_json::from_slice::<Claims>(&payload).ok()?.exp
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renews_before_the_bearer_expires_when_it_is_a_jwt() {
        let exp = now() + 600;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!(r#"{{"sub":"u","exp":{exp}}}"#));
        let jwt = format!("e30.{payload}.sig");
        assert_eq!(renewal_due(&jwt, now() + 3600), exp - 60);
        assert_eq!(renewal_due("opaque", exp), exp - 60);
    }

    #[test]
    fn never_renews_in_a_tight_loop() {
        assert!(renewal_due("opaque", 0) >= now() + 5);
    }
}
