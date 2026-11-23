//! The accounts bearers signed in to on this device, so a user signed in
//! with a bearer opens their store again with no network.
//!
//! Each successful sign-in records the bearer's own `iss`/`sub` (read
//! unverified from the JWT the server just accepted) against the account
//! the server named, in `<dir>/bearer-accounts.json`. Offline, a JWT bearer
//! is looked up by its `iss`/`sub`: an unverified token only ever opens a
//! store already on this device, which anyone with its files can read. The
//! client then reaches the server only once the server has signed the bearer
//! in again, as that same account.

use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use jazz::account_registry::AccountId;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const FILE: &str = "bearer-accounts.json";

/// The user a bearer signed in as on this device.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Remembered {
    /// The bearer JWT's `iss` and `sub`.
    pub token_issuer: String,
    pub token_subject: String,
    /// What the server answered: the account, and the identity and claims
    /// it named the user by.
    pub account: AccountId,
    pub issuer: String,
    pub subject: String,
    #[serde(default)]
    pub claims: serde_json::Value,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Records {
    users: Vec<Remembered>,
}

fn path(dir: &Path) -> PathBuf {
    dir.join(FILE)
}

/// A JWT's `iss` and `sub`, unverified; `None` for an opaque bearer.
pub(crate) fn token_identity(token: &str) -> Option<(String, String)> {
    let mut parts = token.split('.');
    let (_, payload, _, None) = (parts.next()?, parts.next()?, parts.next()?, parts.next()) else {
        return None;
    };
    let payload = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    let issuer = claims.get("iss")?.as_str()?;
    let subject = claims.get("sub")?.as_str()?;
    (!issuer.is_empty() && !subject.trim().is_empty())
        .then(|| (issuer.to_owned(), subject.to_owned()))
}

fn read(dir: &Path) -> Records {
    // An unreadable record remembers no one: it only ever enables an
    // offline open, never a sign-in.
    std::fs::read(path(dir))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Who `token`'s `iss`/`sub` signed in as here last, if anyone.
pub(crate) fn lookup(dir: &Path, token: &str) -> Option<Remembered> {
    let (issuer, subject) = token_identity(token)?;
    read(dir)
        .users
        .into_iter()
        .find(|user| user.token_issuer == issuer && user.token_subject == subject)
}

/// Records that `token` signed in as `account` (with the server's
/// `issuer`/`subject`/`claims`), replacing what was recorded for its
/// `iss`/`sub`: written whole beside the file, then renamed into place.
/// An opaque bearer records nothing.
pub(crate) fn record(
    dir: &Path,
    token: &str,
    account: AccountId,
    issuer: &str,
    subject: &str,
    claims: &serde_json::Value,
) -> Result<()> {
    let Some((token_issuer, token_subject)) = token_identity(token) else {
        return Ok(());
    };
    let entry = Remembered {
        token_issuer,
        token_subject,
        account,
        issuer: issuer.to_owned(),
        subject: subject.to_owned(),
        claims: claims.clone(),
    };
    let mut records = read(dir);
    if records.users.contains(&entry) {
        return Ok(());
    }
    records.users.retain(|user| {
        (user.token_issuer.as_str(), user.token_subject.as_str())
            != (entry.token_issuer.as_str(), entry.token_subject.as_str())
    });
    records.users.push(entry);
    let file = path(dir);
    let io = |error: std::io::Error| {
        Error::Config(format!(
            "remembered accounts at {}: {error}",
            file.display()
        ))
    };
    std::fs::create_dir_all(dir).map_err(io)?;
    let bytes = serde_json::to_vec_pretty(&records)
        .map_err(|error| Error::Config(format!("remembered accounts: {error}")))?;
    let temp = dir.join(format!(".{FILE}.{}.tmp", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let written = options.open(&temp).and_then(|mut file| {
        std::io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()
    });
    let renamed = written.and_then(|()| std::fs::rename(&temp, &file));
    if renamed.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    renamed.map_err(io)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt(claims: serde_json::Value) -> String {
        format!(
            "{}.{}.sig",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#),
            URL_SAFE_NO_PAD.encode(claims.to_string())
        )
    }

    #[test]
    fn a_bearer_is_remembered_by_its_issuer_and_subject() {
        let dir = tempfile::tempdir().unwrap();
        let alice = jwt(serde_json::json!({ "iss": "https://idp", "sub": "alice", "exp": 1 }));
        let app = jazz::tools::AppId::random();
        let account = crate::auth::DeviceKey::generate().unwrap().account(app);
        assert_eq!(lookup(dir.path(), &alice), None);
        record(
            dir.path(),
            &alice,
            account,
            "jazz:backend",
            "alice",
            &serde_json::json!({}),
        )
        .unwrap();
        // Another token for the same user finds the account; another user's
        // and an opaque bearer don't.
        let again = jwt(serde_json::json!({ "iss": "https://idp", "sub": "alice", "exp": 2 }));
        assert_eq!(lookup(dir.path(), &again).unwrap().account, account);
        let bob = jwt(serde_json::json!({ "iss": "https://idp", "sub": "bob" }));
        assert_eq!(lookup(dir.path(), &bob), None);
        let mallory = jwt(serde_json::json!({ "iss": "https://other", "sub": "alice" }));
        assert_eq!(lookup(dir.path(), &mallory), None);
        assert_eq!(lookup(dir.path(), "token-alice"), None);
        record(
            dir.path(),
            "token-alice",
            account,
            "i",
            "s",
            &serde_json::json!({}),
        )
        .unwrap();

        // A later sign-in replaces the entry.
        let other = crate::auth::DeviceKey::generate().unwrap().account(app);
        record(
            dir.path(),
            &again,
            other,
            "jazz:backend",
            "alice",
            &serde_json::json!({}),
        )
        .unwrap();
        assert_eq!(lookup(dir.path(), &alice).unwrap().account, other);
        assert_eq!(read(dir.path()).users.len(), 1);
    }
}
