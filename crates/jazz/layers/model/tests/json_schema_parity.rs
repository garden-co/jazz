//! JSON Schema behavior the browser runtime has to match.
//!
//! This is a lower-level test on purpose: the browser WASM build validates
//! JSON columns with a JS validator instead of the `jsonschema` crate, and
//! `fixtures/json_schema_parity.json` is the shared contract between the two.
//! This test pins that the native validator gives the fixture's verdicts;
//! `packages/jazz-tools/src/runtime/json-schema-validator.test.ts` runs the
//! same fixture through the browser's validator.

use jazz_model::model::json_schema::{JsonSchemaError, check_schema, validate};
use serde_json::Value;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    description: String,
    schema: Value,
    #[serde(default)]
    valid: Vec<Value>,
    #[serde(default)]
    invalid: Vec<Value>,
    #[serde(default)]
    invalid_schema: bool,
    #[serde(default)]
    browser_unsupported: bool,
}

fn cases() -> Vec<Case> {
    serde_json::from_str(include_str!("fixtures/json_schema_parity.json"))
        .expect("parity fixture parses")
}

fn check(schema: &Value, instance: &Value) -> Result<(), JsonSchemaError> {
    validate(schema, instance, &instance.to_string())
}

/// Every fixture value gets the verdict the fixture records, and every schema
/// the fixture marks invalid is rejected before any value is checked.
///
/// ```
/// declared schema ──check_schema──► accepted ──validate(value)──► verdict
///                         │
///                         └──► InvalidSchema (fixture: invalidSchema)
/// ```
#[test]
fn native_validator_matches_the_browser_parity_fixture() {
    for case in cases() {
        let description = &case.description;
        if case.invalid_schema {
            assert!(
                matches!(
                    check_schema(&case.schema),
                    Err(JsonSchemaError::InvalidSchema(_))
                ),
                "{description}: schema should be rejected",
            );
            continue;
        }
        // Schemas the browser rejects as unsupported are valid natively; the
        // fixture records that divergence explicitly.
        check_schema(&case.schema).unwrap_or_else(|error| panic!("{description}: {error:?}"));
        if case.browser_unsupported {
            continue;
        }
        for value in &case.valid {
            assert_eq!(
                check(&case.schema, value),
                Ok(()),
                "{description}: {value} should be valid",
            );
        }
        for value in &case.invalid {
            assert!(
                matches!(
                    check(&case.schema, value),
                    Err(JsonSchemaError::Mismatch(_))
                ),
                "{description}: {value} should be invalid",
            );
        }
    }
}
