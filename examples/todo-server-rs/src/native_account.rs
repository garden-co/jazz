use std::time::Duration;

use jazz::account_registry::AccountId;
use jazz::tools::{AppContext, JazzError};
use serde::Deserialize;

/// This service is the app's backend: it vouches for the user it acts as with
/// the backend secret, and the core resolves (or creates) that user's account,
/// as `createJazz` does for its users.
pub(super) async fn enrol(context: &mut AppContext) -> jazz::tools::Result<()> {
    if context.server_url.is_empty() {
        return Ok(());
    }
    let (Some(secret), Some(session)) = (
        context.backend_secret.as_deref(),
        context.backend_session.as_mut(),
    ) else {
        return Ok(());
    };
    if session.account_id.is_some() {
        return Ok(());
    }
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| JazzError::Connection(error.to_string()))?;
    let response = http
        .post(format!(
            "{}/apps/{}/admin/accounts/login-or-register",
            context.server_url.trim_end_matches('/'),
            context.app_id,
        ))
        .header("X-Jazz-Backend-Secret", secret)
        .json(&serde_json::json!({
            "identity": { "issuer": session.issuer, "subject": session.user_id },
        }))
        .send()
        .await
        .map_err(|error| JazzError::Connection(error.to_string()))?;
    if !response.status().is_success() {
        return Err(JazzError::Connection(format!(
            "user account admission failed: {}",
            response.status(),
        )));
    }
    #[derive(Deserialize)]
    struct Admission {
        account: AccountId,
    }
    let admitted: Admission = response
        .json()
        .await
        .map_err(|error| JazzError::Connection(error.to_string()))?;
    if admitted.account.is_system() {
        return Err(JazzError::Connection(
            "user account admission returned the system account".into(),
        ));
    }
    session.account_id = Some(admitted.account);
    // The stated session decides the client's identity; keep the context's
    // account in step with it for anything that reads it.
    context.account_id = Some(admitted.account);
    Ok(())
}
