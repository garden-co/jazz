//! Shared conversion rules for JSON session claims at public transport boundaries.

use std::collections::BTreeMap;

use crate::groove::records::Value;
use crate::ids::AuthorSubject;
use crate::query::provider_claim_key;

/// Construct the one canonical policy-binding vocabulary for an admitted
/// identity and its provider claims.
///
/// Provider values always remain namespaced below `session.claims.*`. The
/// identity-derived `session.claims.iss`/`sub`, `session.user`, and
/// `session.authMode` values are overwritten here, after provider claims, so a
/// transport caller cannot create a competing identity revision by spelling a
/// reserved field in its claim object.
pub fn canonical_policy_binding_claims(
    author: &AuthorSubject,
    provider_claims: BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let (issuer, subject) = author.principal_parts();
    let mut admitted = provider_claims
        .into_iter()
        .map(|(name, value)| (provider_claim_key(&name), value))
        .collect::<BTreeMap<_, _>>();
    admitted.insert(provider_claim_key("iss"), Value::String(issuer.clone()));
    admitted.insert(provider_claim_key("sub"), Value::String(subject));
    admitted.extend(author_policy_claims(*author));
    admitted.insert(
        "authMode".to_owned(),
        Value::String(auth_mode_for_author_issuer(&issuer).to_owned()),
    );
    admitted
}

/// Reserved structured author bindings, derived only from admitted identity.
/// Flat keys address explicit public paths; provider keys have another namespace.
pub fn author_policy_claims(author: AuthorSubject) -> BTreeMap<String, Value> {
    let value = author.to_value();
    let Value::Record(record) = &value else {
        unreachable!()
    };
    let account = match record.get("account").expect("author account field") {
        // A present scalar claim is the UUID itself. The surrounding author
        // record remains nullable, but an ownership column need not be.
        Value::Nullable(Some(account)) => *account,
        account => account,
    };
    let identity = record.get("identity").expect("author identity field");
    let Value::Record(principal) = &identity else {
        unreachable!()
    };
    let issuer = principal.get("issuer").expect("author issuer field");
    let subject = principal.get("subject").expect("author subject field");
    BTreeMap::from([
        ("user".into(), value),
        ("user.account".into(), account),
        ("user.identity".into(), identity),
        ("user.identity.issuer".into(), issuer),
        ("user.identity.subject".into(), subject),
    ])
}

fn auth_mode_for_author_issuer(issuer: &str) -> &'static str {
    match issuer {
        AuthorSubject::LOCAL_FIRST_ISSUER => "local-first",
        AuthorSubject::ANONYMOUS_ISSUER => "anonymous",
        _ => "external",
    }
}

/// Largest integer that a JavaScript `number` represents exactly.
pub const MAX_SAFE_JS_INTEGER: u64 = 9_007_199_254_740_991;

/// Describes whether a JSON number originated at a JavaScript binding boundary
/// or in a parsed JSON payload such as a JWT.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericClaimOrigin {
    /// A JavaScript `number`, whose integral precision is limited to the safe range.
    JavaScript,
    /// Parsed JSON, whose serde number preserves an exact signed or unsigned 64-bit integer.
    ExactJson,
}

/// Classify a JSON number for policy evaluation.
///
/// Integral JSON numbers preserve their exact 64-bit representation. At a
/// JavaScript boundary, only safe integral numbers participate as integers;
/// larger values remain doubles and cannot match integer policy columns.
pub fn json_number_to_policy_claim(
    number: serde_json::Number,
    origin: NumericClaimOrigin,
) -> Result<Value, String> {
    if let Some(value) = number.as_u64() {
        return Ok(
            if origin == NumericClaimOrigin::ExactJson || value <= MAX_SAFE_JS_INTEGER {
                Value::U64(value)
            } else {
                Value::F64(value as f64)
            },
        );
    }
    if let Some(value) = number.as_i64() {
        return Ok(
            if origin == NumericClaimOrigin::ExactJson
                || value.unsigned_abs() <= MAX_SAFE_JS_INTEGER
            {
                Value::I64(value)
            } else {
                Value::F64(value as f64)
            },
        );
    }
    let Some(value) = number.as_f64() else {
        return Err("unsupported numeric claim".to_owned());
    };
    if !value.is_finite() {
        return Err("unsupported numeric claim".to_owned());
    }
    if value.fract() == 0.0 && value.abs() <= MAX_SAFE_JS_INTEGER as f64 {
        return Ok(if value < 0.0 {
            Value::I64(value as i64)
        } else {
            Value::U64(value as u64)
        });
    }
    Ok(Value::F64(value))
}

/// Project one JSON provider value into Groove's non-recursive policy value
/// corpus. Objects, including arrays that contain an object at any depth, are
/// intentionally omitted rather than rejecting an otherwise valid session.
/// The original JSON remains available to the handler/session surface.
pub fn json_value_to_policy_claim(
    value: serde_json::Value,
    origin: NumericClaimOrigin,
) -> Result<Option<Value>, String> {
    Ok(match value {
        serde_json::Value::Null => Some(Value::Nullable(None)),
        serde_json::Value::Bool(value) => Some(Value::Bool(value)),
        serde_json::Value::Number(value) => Some(json_number_to_policy_claim(value, origin)?),
        serde_json::Value::String(value) => Some(
            value
                .parse()
                .map(Value::Uuid)
                .unwrap_or(Value::String(value)),
        ),
        serde_json::Value::Array(values) => {
            let mut projected = Vec::with_capacity(values.len());
            for value in values {
                let Some(value) = json_value_to_policy_claim(value, origin)? else {
                    return Ok(None);
                };
                projected.push(value);
            }
            Some(Value::Array(projected))
        }
        serde_json::Value::Object(_) => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::AuthorSubject;
    use serde_json::json;

    #[test]
    fn canonical_binding_keeps_provider_aliases_namespaced_and_derives_reserved_fields() {
        let author = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let claims = canonical_policy_binding_claims(
            &author,
            BTreeMap::from([
                ("user".to_owned(), Value::String("provider-user".to_owned())),
                ("iss".to_owned(), Value::String("spoofed-issuer".to_owned())),
                (
                    "sub".to_owned(),
                    Value::String("spoofed-subject".to_owned()),
                ),
                (
                    "authMode".to_owned(),
                    Value::String("spoofed-mode".to_owned()),
                ),
                ("role".to_owned(), Value::String("writer".to_owned())),
            ]),
        );

        assert_eq!(claims.get("user"), Some(&author.to_value()));
        assert_eq!(
            claims.get("authMode"),
            Some(&Value::String("external".to_owned()))
        );
        assert_eq!(
            claims.get(&provider_claim_key("iss")),
            Some(&Value::String("https://issuer.example".to_owned()))
        );
        assert_eq!(
            claims.get(&provider_claim_key("sub")),
            Some(&Value::String("alice".to_owned()))
        );
        assert_eq!(
            claims.get(&provider_claim_key("user")),
            Some(&Value::String("provider-user".to_owned()))
        );
        assert_eq!(
            claims.get(&provider_claim_key("authMode")),
            Some(&Value::String("spoofed-mode".to_owned()))
        );
        assert_eq!(
            claims.get(&provider_claim_key("role")),
            Some(&Value::String("writer".to_owned()))
        );
    }

    #[test]
    fn canonical_binding_derives_reserved_auth_modes_from_reserved_issuers() {
        for (issuer, expected) in [
            (AuthorSubject::LOCAL_FIRST_ISSUER, "local-first"),
            (AuthorSubject::ANONYMOUS_ISSUER, "anonymous"),
        ] {
            let author = AuthorSubject::reserved(issuer, "alice").unwrap();
            let claims = canonical_policy_binding_claims(&author, BTreeMap::new());
            assert_eq!(
                claims.get("authMode"),
                Some(&Value::String(expected.to_owned()))
            );
        }
    }

    #[test]
    fn numeric_claims_preserve_safe_integers_and_fail_closed_when_lossy() {
        assert_eq!(
            json_number_to_policy_claim(
                json!(7).as_number().unwrap().clone(),
                NumericClaimOrigin::JavaScript,
            )
            .unwrap(),
            Value::U64(7)
        );
        assert_eq!(
            json_number_to_policy_claim(
                json!(-7).as_number().unwrap().clone(),
                NumericClaimOrigin::JavaScript,
            )
            .unwrap(),
            Value::I64(-7)
        );
        assert_eq!(
            json_number_to_policy_claim(
                json!(7.5).as_number().unwrap().clone(),
                NumericClaimOrigin::JavaScript,
            )
            .unwrap(),
            Value::F64(7.5)
        );
        assert_eq!(
            json_number_to_policy_claim(
                json!(9_007_199_254_740_992_u64)
                    .as_number()
                    .unwrap()
                    .clone(),
                NumericClaimOrigin::JavaScript,
            )
            .unwrap(),
            Value::F64(9_007_199_254_740_992.0)
        );
        assert_eq!(
            json_number_to_policy_claim(
                json!(9_007_199_254_740_992_u64)
                    .as_number()
                    .unwrap()
                    .clone(),
                NumericClaimOrigin::ExactJson,
            )
            .unwrap(),
            Value::U64(9_007_199_254_740_992)
        );
    }

    #[test]
    fn policy_projection_omits_recursive_json_without_rejecting_scalars() {
        assert_eq!(
            json_value_to_policy_claim(
                json!(["editor", { "nested": true }]),
                NumericClaimOrigin::JavaScript,
            )
            .unwrap(),
            None
        );
        assert_eq!(
            json_value_to_policy_claim(
                json!({ "profile": "handler-only" }),
                NumericClaimOrigin::ExactJson
            )
            .unwrap(),
            None
        );
        assert_eq!(
            json_value_to_policy_claim(json!("editor"), NumericClaimOrigin::JavaScript).unwrap(),
            Some(Value::String("editor".to_owned()))
        );
    }
}
