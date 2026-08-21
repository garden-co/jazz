//! Shared conversion rules for JSON session claims at public transport boundaries.

use std::collections::BTreeMap;

use crate::groove::records::Value;

/// Flatten a JSON application-claims object into the dotted paths consumed by
/// policy expressions.
///
/// Objects define path segments while arrays and scalar values remain claim
/// values. A literal dotted key and an equivalent nested object are ambiguous,
/// so claim admission rejects that collision instead of choosing one value.
pub fn flatten_json_policy_claims(
    claims: serde_json::Map<String, serde_json::Value>,
    mut convert_leaf: impl FnMut(serde_json::Value) -> Result<Value, String>,
) -> Result<BTreeMap<String, Value>, String> {
    fn visit(
        claims: serde_json::Map<String, serde_json::Value>,
        path: &mut Vec<String>,
        flattened: &mut BTreeMap<String, Value>,
        convert_leaf: &mut impl FnMut(serde_json::Value) -> Result<Value, String>,
    ) -> Result<(), String> {
        for (name, value) in claims {
            path.push(name);
            if let serde_json::Value::Object(nested) = value {
                visit(nested, path, flattened, convert_leaf)?;
            } else {
                let dotted_path = path.join(".");
                let value = convert_leaf(value)?;
                if flattened.insert(dotted_path.clone(), value).is_some() {
                    return Err(format!(
                        "application claims contain ambiguous dotted path {dotted_path}"
                    ));
                }
            }
            path.pop();
        }
        Ok(())
    }

    let mut flattened = BTreeMap::new();
    visit(claims, &mut Vec::new(), &mut flattened, &mut convert_leaf)?;
    Ok(flattened)
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
    use serde_json::json;

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
    fn nested_application_claims_flatten_to_unambiguous_dotted_paths() {
        let serde_json::Value::Object(claims) = serde_json::json!({
            "org": { "slug": "north" },
            "groups": ["eng", "ops"]
        }) else {
            unreachable!()
        };
        let flattened = flatten_json_policy_claims(claims, |value| match value {
            serde_json::Value::String(value) => Ok(Value::String(value)),
            serde_json::Value::Array(values) => Ok(Value::Array(
                values
                    .into_iter()
                    .map(|value| match value {
                        serde_json::Value::String(value) => Ok(Value::String(value)),
                        _ => Err("unsupported test claim".to_owned()),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            _ => Err("unsupported test claim".to_owned()),
        })
        .unwrap();

        assert_eq!(flattened["org.slug"], Value::String("north".to_owned()));
        assert_eq!(
            flattened["groups"],
            Value::Array(vec![
                Value::String("eng".to_owned()),
                Value::String("ops".to_owned())
            ])
        );
    }

    #[test]
    fn nested_application_claims_reject_ambiguous_dotted_paths() {
        let serde_json::Value::Object(claims) = serde_json::json!({
            "org.slug": "flat",
            "org": { "slug": "nested" }
        }) else {
            unreachable!()
        };
        let error = flatten_json_policy_claims(claims, |value| match value {
            serde_json::Value::String(value) => Ok(Value::String(value)),
            _ => Err("unsupported test claim".to_owned()),
        })
        .unwrap_err();

        assert_eq!(
            error,
            "application claims contain ambiguous dotted path org.slug"
        );
    }
}
