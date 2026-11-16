//! JSON Schema behavior the browser runtime has to match.
//!
//! This is a lower-level test on purpose: the browser WASM build validates
//! JSON columns with a JS validator instead of the `jsonschema` crate, and
//! `fixtures/json_schema_parity.json` is the shared contract between the two.
//! This test pins that the native validator gives the fixture's verdicts and
//! that Jazz refuses exactly the schemas the browser cannot evaluate;
//! `packages/jazz-tools/src/runtime/json-schema-validator.test.ts` runs the
//! same fixture through the browser's validator.

use jazz_model::model::json_schema::{JsonSchemaError, check_schema, validate};
use jazz_model::model::json_schema_support::check_supported;
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
    validate(schema, instance)
}

/// Every schema Jazz accepts validates alike in every runtime: the fixture
/// records each value's verdict, which this test pins natively and
/// `json-schema-validator.test.ts` pins in the browser's validator. Schemas
/// the fixture marks invalid are rejected before any value is checked, and
/// the ones it marks `browserUnsupported` (valid natively, refused by the
/// browser) are refused by deployments and `jazz-tools validate` everywhere,
/// so no deployed schema is enforced differently by the two.
///
/// ```
/// declared schema ──check_schema──► accepted ──check_supported──► supported ──validate(value)──► verdict
///                         │                            │
///                         │                            └──► Unsupported (fixture: browserUnsupported)
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
        check_schema(&case.schema).unwrap_or_else(|error| panic!("{description}: {error:?}"));
        if case.browser_unsupported {
            let refusal = check_supported(&case.schema)
                .expect_err(&format!("{description}: Jazz should refuse the schema"));
            assert!(
                refusal
                    .to_string()
                    .contains("is not supported in Jazz JSON Schemas yet"),
                "{description}: {refusal}",
            );
            continue;
        }
        check_supported(&case.schema)
            .unwrap_or_else(|refusal| panic!("{description}: Jazz should accept: {refusal}"));
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

/// `uniqueItems` gives the same verdict on every check. The `jsonschema`
/// crate's own check hashes arrays of more than 15 items with a randomly
/// seeded set, so `0` and `-0.0` (equal numbers) were reported as duplicates
/// only when the seed happened to make them collide. Each check here would
/// have drawn a fresh seed.
#[test]
fn unique_items_counts_equal_numbers_as_duplicates_on_every_check() {
    let schema = serde_json::json!({ "uniqueItems": true });
    for length in 2..=64_usize {
        for position in 1..length {
            let mut items: Vec<Value> = (0..length).map(|n| Value::from(n as u64 + 1)).collect();
            items[0] = serde_json::json!(0);
            items[position] = serde_json::json!(-0.0);
            let array = Value::Array(items);
            assert!(
                matches!(check(&schema, &array), Err(JsonSchemaError::Mismatch(_))),
                "{array} should have non-unique elements",
            );
        }
    }
    // Other spellings of one value, and objects in another key order.
    let long = |a: Value, b: Value| {
        let mut items: Vec<Value> = (100..120).map(Value::from).collect();
        items.push(a);
        items.push(b);
        Value::Array(items)
    };
    for (a, b) in [
        (serde_json::json!(1), serde_json::json!(1.0)),
        (
            serde_json::json!({ "a": 1, "b": [0] }),
            serde_json::json!({ "b": [-0.0], "a": 1.0 }),
        ),
    ] {
        let array = long(a, b);
        assert!(
            matches!(check(&schema, &array), Err(JsonSchemaError::Mismatch(_))),
            "{array} should have non-unique elements",
        );
    }
    let distinct = long(serde_json::json!(0.5), serde_json::json!({ "a": 0 }));
    assert_eq!(check(&schema, &distinct), Ok(()));
}
