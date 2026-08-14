//! Shared conversion rules for JSON session claims at public transport boundaries.

use crate::groove::records::Value;

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
}
