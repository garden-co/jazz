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
pub fn canonical_policy_binding_claims<T>(
    author: &AuthorSubject,
    provider_claims: BTreeMap<String, T>,
    string: impl Fn(String) -> T,
) -> BTreeMap<String, T> {
    let (issuer, subject): (String, String) = serde_json::from_str(author.canonical())
        .expect("author subjects always have canonical issuer/subject JSON");
    let mut admitted = provider_claims
        .into_iter()
        .map(|(name, value)| (provider_claim_key(&name), value))
        .collect::<BTreeMap<_, _>>();
    admitted.insert(provider_claim_key("iss"), string(issuer.clone()));
    admitted.insert(provider_claim_key("sub"), string(subject));
    admitted.insert("user".to_owned(), string(author.canonical().to_owned()));
    admitted.insert(
        "authMode".to_owned(),
        string(auth_mode_for_author_issuer(&issuer).to_owned()),
    );
    admitted
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
                ("user".to_owned(), "provider-user".to_owned()),
                ("iss".to_owned(), "spoofed-issuer".to_owned()),
                ("sub".to_owned(), "spoofed-subject".to_owned()),
                ("authMode".to_owned(), "spoofed-mode".to_owned()),
                ("role".to_owned(), "writer".to_owned()),
            ]),
            |value| value,
        );

        assert_eq!(claims.get("user"), Some(&author.canonical().to_owned()));
        assert_eq!(claims.get("authMode"), Some(&"external".to_owned()));
        assert_eq!(
            claims.get(&provider_claim_key("iss")),
            Some(&"https://issuer.example".to_owned())
        );
        assert_eq!(
            claims.get(&provider_claim_key("sub")),
            Some(&"alice".to_owned())
        );
        assert_eq!(
            claims.get(&provider_claim_key("user")),
            Some(&"provider-user".to_owned())
        );
        assert_eq!(
            claims.get(&provider_claim_key("authMode")),
            Some(&"spoofed-mode".to_owned())
        );
        assert_eq!(
            claims.get(&provider_claim_key("role")),
            Some(&"writer".to_owned())
        );
    }

    #[test]
    fn canonical_binding_derives_reserved_auth_modes_from_reserved_issuers() {
        for (issuer, expected) in [
            (AuthorSubject::LOCAL_FIRST_ISSUER, "local-first"),
            (AuthorSubject::ANONYMOUS_ISSUER, "anonymous"),
        ] {
            let author = AuthorSubject::reserved(issuer, "alice").unwrap();
            let claims = canonical_policy_binding_claims(
                &author,
                BTreeMap::<String, String>::new(),
                |value| value,
            );
            assert_eq!(claims.get("authMode"), Some(&expected.to_owned()));
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
}
