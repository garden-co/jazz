//! How an end user of an app proves who they are: a device account's key,
//! or the app's own bearer token.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jazz::account_registry::{AccountId, local_first_account_id};
use jazz::tools::AppId;
use jazz::tools::identity;

use crate::error::{Error, Result};

/// How an end user signs in to an app ([`crate::Config::user`]).
#[derive(Clone, Debug)]
pub enum Auth {
    /// A local-first device account: the key is the account, and the client
    /// proves it holds it with self-signed tokens it mints and renews itself.
    Device(DeviceKey),
    /// The app's own sign-in: its backend (`createJazz`'s `authenticate`)
    /// names the user from the bearer this provider gives.
    Bearer(Bearer),
}

impl Auth {
    /// Signs in with the app's bearer, as `provider` gives it: see
    /// [`TokenProvider`]. A closure `|fresh| async { .. }` is one.
    ///
    /// ```no_run
    /// # async fn access_token(_fresh: bool) -> Option<String> { None }
    /// let auth = jazz_app::Auth::bearer(|fresh| async move {
    ///     Ok(access_token(fresh).await)
    /// });
    /// ```
    pub fn bearer(provider: impl TokenProvider + 'static) -> Self {
        Self::Bearer(Bearer(Arc::new(provider)))
    }
}

/// A token on its way, from a [`TokenProvider`].
pub type TokenFuture = Pin<Box<dyn Future<Output = Result<Option<String>>> + Send>>;

/// Where an app's bearer tokens come from: the host's own auth (an OAuth
/// client, a platform account), asked whenever a token is needed. On open,
/// before a JWT bearer expires (at 80% of its life, as the TypeScript client
/// renews), and with `fresh = true` when the backend refused the last one,
/// so a cached token must not be given again. `None` means no one is signed
/// in. It never blocks the client: the session waits for the future.
///
/// Closures `Fn(bool) -> impl Future<Output = Result<Option<String>>>`
/// implement it; FFI layers implement it over a host callback.
pub trait TokenProvider: Send + Sync {
    fn token(&self, fresh: bool) -> TokenFuture;
}

impl<F, Fut> TokenProvider for F
where
    F: Fn(bool) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<String>>> + Send + 'static,
{
    fn token(&self, fresh: bool) -> TokenFuture {
        Box::pin(self(fresh))
    }
}

/// A shared [`TokenProvider`]: see [`Auth::bearer`].
#[derive(Clone)]
pub struct Bearer(Arc<dyn TokenProvider>);

impl Bearer {
    pub(crate) async fn token(&self, fresh: bool) -> Result<Option<String>> {
        self.0.token(fresh).await
    }
}

impl std::fmt::Debug for Bearer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Bearer(..)")
    }
}

/// The text a device key is written as, as the TypeScript client stores it.
const KEY_PREFIX: &str = "jazz-auth-v1:";

/// How long a device account's self-signed tokens live: the longest a core
/// accepts. They are renewed before they expire.
pub(crate) const DEVICE_TOKEN_TTL_SECS: u64 = 3600;

/// A local-first device account's secret: 32 random bytes from which its
/// signing key, user id and account derive. Whoever holds it is the account,
/// so keep it as a password (the platform's keychain); losing it loses the
/// account unless it was linked to a provider account.
///
/// It reads and writes as `jazz-auth-v1:<base64url>`, the form the TypeScript
/// client keeps (`exportDeviceKey`), so a key moves between the two.
#[derive(Clone, PartialEq, Eq)]
pub struct DeviceKey([u8; 32]);

impl DeviceKey {
    /// A new device account's key, from the operating system's RNG.
    pub fn generate() -> Result<Self> {
        let mut seed = [0; 32];
        getrandom::fill(&mut seed)
            .map_err(|error| Error::Config(format!("no randomness for a device key: {error}")))?;
        Ok(Self(seed))
    }

    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.0
    }

    /// The account's user id: the subject of its tokens.
    pub fn user_id(&self) -> String {
        identity::derive_user_id(&self.0).to_string()
    }

    /// The account this key founds in `app`.
    pub fn account(&self, app: AppId) -> AccountId {
        local_first_account_id(*app.uuid(), &self.user_id())
    }

    /// A self-signed proof of this key for `app`, valid for `ttl_secs`.
    pub(crate) fn mint(&self, app: AppId, ttl_secs: u64) -> Result<String> {
        identity::mint_jazz_self_signed_token(
            &self.0,
            identity::LOCAL_FIRST_ISSUER,
            &app.to_string(),
            ttl_secs,
        )
        .map_err(Error::Config)
    }
}

impl std::fmt::Display for DeviceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{KEY_PREFIX}{}", URL_SAFE_NO_PAD.encode(self.0))
    }
}

impl std::str::FromStr for DeviceKey {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        let invalid = || Error::Config("not a device key (jazz-auth-v1:…)".into());
        let encoded = text.trim().strip_prefix(KEY_PREFIX).ok_or_else(invalid)?;
        let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
        Ok(Self(bytes.try_into().map_err(|_| invalid())?))
    }
}

impl std::fmt::Debug for DeviceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DeviceKey").field(&self.user_id()).finish()
    }
}

/// The `exp` of a JWT, read without verifying it: when to renew a bearer.
pub(crate) fn jwt_expiry(token: &str) -> Option<u64> {
    let claims = token.split('.').nth(1)?;
    let claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims).ok()?).ok()?;
    claims.get("exp")?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_key_reads_back_as_written() {
        let key = DeviceKey::generate().unwrap();
        let text = key.to_string();
        assert!(text.starts_with(KEY_PREFIX));
        assert_eq!(text.len(), KEY_PREFIX.len() + 43);
        assert_eq!(text.parse::<DeviceKey>().unwrap(), key);
        assert!("jazz-auth-v1:short".parse::<DeviceKey>().is_err());
        assert!(!format!("{key:?}").contains(&text[KEY_PREFIX.len()..]));
    }

    #[test]
    fn a_device_token_proves_the_key_for_its_app() {
        let key = DeviceKey::from_bytes([7; 32]);
        let app = AppId::random();
        let token = key.mint(app, 60).unwrap();
        let verified = identity::verify_jazz_self_signed_proof(&token, &app.to_string()).unwrap();
        assert_eq!(verified.user_id, key.user_id());
        assert!(jwt_expiry(&token).is_some());
    }
}
