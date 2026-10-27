//! Argument parsing shared by the NAPI and WASM bindings.
//!
//! Each binding only maps the `String` error into its host's error type, so
//! both JavaScript hosts accept and reject exactly the same inputs with the
//! same messages.

use std::collections::BTreeMap;

use crate::db::ReadTier;
use crate::ids::{AuthorSubject, RowUuid};
use crate::records::Value;
use crate::tx::DurabilityTier;
use jazz_model::model::policy_claims::{
    MAX_SAFE_JS_INTEGER, NumericClaimOrigin, canonical_policy_binding_claims,
    json_value_to_policy_claim,
};

/// Parse a 16-byte row id.
pub fn row_uuid(bytes: &[u8]) -> Result<RowUuid, String> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| "row id must be 16 bytes".to_string())?;
    Ok(RowUuid::from_bytes(bytes))
}

/// A JavaScript number as `u64`, if it is a nonnegative safe integer.
pub fn safe_u64(value: f64) -> Option<u64> {
    (value.is_finite()
        && value >= 0.0
        && value.fract() == 0.0
        && value <= MAX_SAFE_JS_INTEGER as f64)
        .then_some(value as u64)
}

/// [`safe_u64`], naming the argument `name` in the error.
pub fn checked_u64(value: f64, name: &str) -> Result<u64, String> {
    safe_u64(value).ok_or_else(|| format!("{name} must be a nonnegative safe integer"))
}

/// Parse an untrusted canonical author subject from UTF-8 JSON bytes.
pub fn author_subject(bytes: &[u8]) -> Result<AuthorSubject, String> {
    let canonical = std::str::from_utf8(bytes)
        .map_err(|_| "author subject must be canonical UTF-8 JSON".to_string())?;
    AuthorSubject::from_untrusted_canonical(canonical).map_err(|error| error.to_string())
}

/// Parse a write durability tier.
pub fn durability_tier(tier: &str) -> Result<DurabilityTier, String> {
    match tier {
        "None" | "none" => Ok(DurabilityTier::None),
        "Local" | "local" => Ok(DurabilityTier::Local),
        "Global" | "global" => Ok(DurabilityTier::Global),
        other => Err(format!("unknown durability tier {other}")),
    }
}

/// Read-only binding lowering. Write waits keep [`durability_tier`], so a
/// product read choice can never change write-settlement semantics.
pub fn read_tier(tier: &str) -> Result<ReadTier, String> {
    match tier {
        "local-first" | "LocalFirst" => Ok(ReadTier::LocalFirst),
        "remote" | "Remote" => Ok(ReadTier::Remote),
        "local-only" | "LocalOnly" => Ok(ReadTier::LocalOnly),
        other => Err(format!("unknown read tier {other}")),
    }
}

/// Admit raw JSON provider claims for an identity received through a binding.
///
/// The identity is the authority for the reserved policy fields and for
/// `session.claims.iss`/`session.claims.sub`; callers may not replace those
/// values by supplying similarly named provider claims. All other provider
/// values remain below the collision-proof `session.claims` prefix.
pub fn policy_claims(
    author: &AuthorSubject,
    claims: serde_json::Value,
) -> Result<BTreeMap<String, Value>, String> {
    let claims = match claims {
        serde_json::Value::Null => BTreeMap::new(),
        serde_json::Value::Object(map) => {
            let mut projected = BTreeMap::new();
            for (key, value) in map {
                if let Some(value) =
                    json_value_to_policy_claim(value, NumericClaimOrigin::JavaScript)?
                {
                    projected.insert(key, value);
                }
            }
            projected
        }
        _ => return Err("identity claims must be an object".to_string()),
    };
    Ok(canonical_policy_binding_claims(author, claims))
}
