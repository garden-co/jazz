//! The app's backend signing key (`GET /apps/{id}/admin/backend-signing-key`):
//! a per-app 32-byte key the core hands its trusted backend, so every
//! instance of a backend that holds no app secret (`vercel()`, `remote()`
//! with a backend credential) signs and checks its own short-lived
//! statements (guest link challenges, session renewal tickets) with the same
//! key (jazz#5553). The backend derives its keys from it, one per label; the
//! core never sees what it signs.
//!
//! The key must outlive the core process: a backend instance keeps the key
//! it fetched while warm, so a key that changed with a core restart would
//! split the backend's instances again. A core with a configured secret
//! derives it ([`derived`]); one without (a core that trusts a workload holds
//! only random per-process secrets) generates it once and keeps it in the
//! app's store ([`stored`]).

use base64::Engine as _;
use hmac::{Hmac, Mac};
use jazz::storage::{OptionalExtension, Storage};
use sha2::{Digest, Sha256};

use super::accounts::{RegistryError, storage_error, unavailable};

/// The route, after `/apps/{id}/`.
pub const ROUTE: &str = "admin/backend-signing-key";

/// What the derivation of [`derived`] is labelled with.
const DERIVATION_LABEL: &str = "jazz-backend-signing-v1";

/// The table that keeps a generated key: one row, its 32 key bytes. Encoding
/// `jazz-backend-signing-key-v1`: `id` is always 1, `key` the raw key.
const CREATE: &str = "
CREATE TABLE IF NOT EXISTS _jazz_host_backend_signing_key (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    key BLOB NOT NULL CHECK (length(key) = 32)
);
";

/// A backend signing key.
pub type Key = [u8; 32];

/// The key of app `app_id` on a core configured with `secret` (its admin
/// secret, or a Durable Object's core secret):
/// `HMAC-SHA256(secret, "jazz-backend-signing-v1\0" + app_id)`.
pub fn derived(secret: &str, app_id: &str) -> Key {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(DERIVATION_LABEL.as_bytes());
    mac.update(b"\0");
    mac.update(app_id.as_bytes());
    mac.finalize().into_bytes().into()
}

/// The key kept in `storage`, made from `fresh` and kept on the first call.
/// The insert and the read are one scope, and the insert never replaces a
/// key: concurrent first calls (from this host or another owner of the same
/// store) all answer the one key that was kept first.
pub fn stored(storage: &Storage, fresh: impl FnOnce() -> Key) -> Result<Key, RegistryError> {
    let fresh = fresh();
    storage
        .scope("jazz_backend_signing_key", |storage| {
            storage.execute_batch(CREATE).map_err(storage_error)?;
            storage
                .prepare_cached(
                    "INSERT OR IGNORE INTO _jazz_host_backend_signing_key (id, key) VALUES (1, ?1)",
                )
                .and_then(|mut statement| statement.execute([fresh.as_slice()]))
                .map_err(storage_error)?;
            storage
                .prepare_cached("SELECT key FROM _jazz_host_backend_signing_key WHERE id = 1")
                .and_then(|mut statement| {
                    statement
                        .query_row([], |row| row.get::<_, Vec<u8>>(0))
                        .optional()
                })
                .map_err(storage_error)
        })
        .map_err(unavailable)?
        .ok_or_else(|| unavailable("the backend signing key was not kept"))?
        .try_into()
        .map_err(|_| unavailable("the kept backend signing key is not 32 bytes"))
}

/// A fresh random key, from `random` uuids (as the core's internal secrets
/// are made): hashed together, so the key's bits are uniform.
pub fn generate(mut random: impl FnMut() -> uuid::Uuid) -> Key {
    let mut hash = Sha256::new();
    for _ in 0..3 {
        hash.update(random().as_bytes());
    }
    hash.finalize().into()
}

/// The route's JSON body: `{"key": "<base64url, no padding>"}`. Answered
/// with `Cache-Control: no-store`.
pub fn body(key: &Key) -> String {
    serde_json::json!({ "key": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key) })
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pins the derivation: a backend keeps the key across core upgrades, so
    // it must never change for a configured secret.
    #[test]
    fn the_derived_key_is_pinned() {
        let expected = {
            let mut mac = Hmac::<Sha256>::new_from_slice(b"admin-secret").unwrap();
            mac.update(b"jazz-backend-signing-v1\0app");
            mac.finalize().into_bytes()
        };
        assert_eq!(derived("admin-secret", "app")[..], expected[..]);
        assert_ne!(
            derived("admin-secret", "app"),
            derived("admin-secret", "other")
        );
        assert_ne!(
            derived("admin-secret", "app"),
            derived("other-secret", "app")
        );
        assert_eq!(
            body(&[0xfb; 32]),
            r#"{"key":"-_v7-_v7-_v7-_v7-_v7-_v7-_v7-_v7-_v7-_v7-_s"}"#
        );
    }

    #[test]
    fn generated_keys_differ() {
        assert_ne!(generate(uuid::Uuid::new_v4), generate(uuid::Uuid::new_v4));
    }
}
