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
    /// A backend client's machine credential (an OIDC workload token): the
    /// app's main backend trusts it as a trusted machine or as one user
    /// ([`crate::Config::credential`]).
    Credential(Bearer),
}

impl Auth {
    /// Signs in with the app's bearer, as `provider` gives it: see
    /// [`TokenProvider`]. A closure `|reason| async { .. }` is one.
    ///
    /// ```no_run
    /// # async fn access_token(_fresh: bool) -> Option<String> { None }
    /// let auth = jazz_app_support::Auth::bearer(|reason: jazz_app_support::TokenReason| async move {
    ///     Ok(access_token(reason.is_fresh()).await)
    /// });
    /// ```
    pub fn bearer(provider: impl TokenProvider + 'static) -> Self {
        Self::Bearer(Bearer(Arc::new(provider)))
    }
}

/// A token on its way, from a [`TokenProvider`].
pub type TokenFuture = Pin<Box<dyn Future<Output = Result<Option<String>>> + Send>>;

/// Why a [`TokenProvider`] is asked for a token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum TokenReason {
    /// Opening the client.
    Connect,
    /// Ahead of the last token's expiry (at 80% of a JWT's life, as the
    /// TypeScript client renews): a cached token still valid will do.
    Renew,
    /// The backend refused the last token: give a new one, not from cache.
    Expired,
    /// The app asked for a fresh one ([`crate::Session::refresh`]).
    Refresh,
}

impl TokenReason {
    /// Whether a cached token must not be given (TypeScript's
    /// `getToken({ fresh: true })`).
    pub fn is_fresh(self) -> bool {
        matches!(self, Self::Expired | Self::Refresh)
    }
}

/// Where an app's bearer tokens come from: the host's own auth (an OAuth
/// client, a platform account), asked whenever a token is needed, with why
/// ([`TokenReason`]). `None` means no one is signed in.
///
/// `token` is called on the client's thread and must return at once; the
/// work belongs in the future, which the session awaits as a task of its
/// own, so the client's other calls run on meanwhile.
///
/// Closures `Fn(TokenReason) -> impl Future<Output = Result<Option<String>>>`
/// implement it; FFI layers implement it over a host callback.
pub trait TokenProvider: Send + Sync {
    fn token(&self, reason: TokenReason) -> TokenFuture;
}

impl<F, Fut> TokenProvider for F
where
    F: Fn(TokenReason) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<String>>> + Send + 'static,
{
    fn token(&self, reason: TokenReason) -> TokenFuture {
        Box::pin(self(reason))
    }
}

/// A shared [`TokenProvider`]: see [`Auth::bearer`].
#[derive(Clone)]
pub struct Bearer(Arc<dyn TokenProvider>);

impl Bearer {
    pub(crate) async fn token(&self, reason: TokenReason) -> Result<Option<String>> {
        self.0.token(reason).await
    }

    /// The provider's future for a token, to poll beside other work.
    pub(crate) fn request(&self, reason: TokenReason) -> TokenFuture {
        self.0.token(reason)
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
#[derive(Clone)]
pub struct DeviceKey([u8; 32]);

/// In constant time: comparing keys tells nothing of where they differ.
impl PartialEq for DeviceKey {
    fn eq(&self, other: &Self) -> bool {
        self.0
            .iter()
            .zip(other.0.iter())
            .fold(0u8, |differ, (a, b)| differ | (a ^ b))
            == 0
    }
}

impl Eq for DeviceKey {}

/// The key leaves no copy in memory it frees.
impl Drop for DeviceKey {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            // SAFETY: `byte` is a valid, aligned, exclusive reference.
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    }
}

impl DeviceKey {
    /// A new device account's key, from the operating system's RNG.
    pub fn generate() -> Result<Self> {
        let mut seed = [0; 32];
        getrandom::fill(&mut seed)
            .map_err(|error| Error::Config(format!("no randomness for a device key: {error}")))?;
        Ok(Self(seed))
    }

    /// The key kept in the file at `path`, or a new one written there
    /// (readable by this user only): for hosts without a keychain, such as a
    /// CLI or a server-side test. Apps with a platform keychain keep the
    /// key's text form there instead.
    pub fn load_or_create(path: &std::path::Path) -> Result<Self> {
        let io = |error: std::io::Error| {
            Error::Config(format!("device key at {}: {error}", path.display()))
        };
        match std::fs::read_to_string(path) {
            Ok(text) => return text.parse(),
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(io(error)),
            Err(_) => {}
        }
        let key = Self::generate()?;
        let dir = path
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."));
        std::fs::create_dir_all(dir).map_err(io)?;
        // Written whole beside it, then linked into place: a concurrent
        // opener sees no file or the whole key, never a partial one, and
        // the first link wins.
        let temp = dir.join(format!(
            ".{}.{}.tmp",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("device-key"),
            identity::derive_user_id(&key.0)
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let written = options.open(&temp).and_then(|mut file| {
            std::io::Write::write_all(&mut file, key.to_string().as_bytes())?;
            file.sync_all()
        });
        let linked = written.and_then(|()| std::fs::hard_link(&temp, path));
        let _ = std::fs::remove_file(&temp);
        match linked {
            Ok(()) => Ok(key),
            // Another opener linked theirs first: theirs is the key.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::read_to_string(path).map_err(io)?.parse()
            }
            Err(error) => Err(io(error)),
        }
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

    /// A proof of this key for one challenge a server issued.
    pub(crate) fn sign_challenge(&self, challenge: &str) -> Result<String> {
        identity::sign_local_first_challenge(&self.0, challenge).map_err(Error::Config)
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
    fn a_device_key_kept_in_a_file_is_the_same_key_next_time() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys/device.key");
        let key = DeviceKey::load_or_create(&path).unwrap();
        assert_eq!(DeviceKey::load_or_create(&path).unwrap(), key);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        std::fs::write(&path, "junk").unwrap();
        assert!(DeviceKey::load_or_create(&path).is_err());
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
