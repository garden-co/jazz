use std::time::Duration;

use jazz::account_registry::{AccountId, Principal};
use jazz::tools::{AppContext, JazzError, unverified_jwt_scope_subject};
use serde::Deserialize;

// Native user admission uses the user's bearer, never backend/admin credentials.
pub(super) async fn enrol(context: &mut AppContext) -> jazz::tools::Result<()> {
    if context.server_url.is_empty() {
        return Ok(());
    }
    let Some(token) = context.jwt_token.as_deref() else {
        return Ok(());
    };
    let (issuer, subject) = unverified_jwt_scope_subject(token)
        .ok_or_else(|| JazzError::Connection("invalid user identity token".into()))?;
    // These unverified fields select the public route and check its response.
    // The server verifies the bearer and resolves the authoritative account.
    let operation = if issuer == jazz::identity::LOCAL_FIRST_ISSUER {
        "found-local-first"
    } else {
        "login-or-register"
    };
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| JazzError::Connection(error.to_string()))?;
    let response = http
        .post(format!(
            "{}/apps/{}/accounts/{operation}",
            context.server_url.trim_end_matches('/'),
            context.app_id,
        ))
        .bearer_auth(token)
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
        identity: Principal,
    }
    let admitted: Admission = response
        .json()
        .await
        .map_err(|error| JazzError::Connection(error.to_string()))?;
    if admitted.identity.issuer != issuer
        || admitted.identity.subject != subject
        || admitted.account.is_system()
        || context
            .account_id
            .is_some_and(|account| account != admitted.account)
    {
        return Err(JazzError::Connection(
            "user account admission identity mismatch".into(),
        ));
    }
    context.account_id = Some(admitted.account);
    Ok(())
}
