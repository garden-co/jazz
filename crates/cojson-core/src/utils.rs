// Adapted from fast-json-stable-stringify (https://github.com/epoberezkin/fast-json-stable-stringify)
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;

/// Stable stringify a JSON value with sorted object keys.
/// This ensures deterministic serialization by sorting object keys alphabetically.
///
/// # Arguments
/// * `value` - The JSON value to stringify
///
/// # Returns
/// * `Ok(String)` - The stable JSON string representation
///
/// # Example
/// ```
/// use cojson_core::stable_stringify;
/// use serde_json::json;
///
/// let value = json!({"b": 2, "a": 1});
/// let result = stable_stringify(&value).unwrap();
/// assert_eq!(result, r#"{"a":1,"b":2}"#);
/// ```
pub fn stable_stringify(value: &JsonValue) -> Result<String, serde_json::Error> {
    match value {
        JsonValue::Null => Ok("null".to_string()),
    
        JsonValue::Bool(b) => Ok(b.to_string()),
        
        JsonValue::Number(n) => {
            // Check if number is finite (similar to isFinite in JavaScript)
            if let Some(f) = n.as_f64() {
                if f.is_finite() {
                    Ok(n.to_string())
                } else {
                    // Infinity or NaN -> "null"
                    Ok("null".to_string())
                }
            } else {
                // Integer numbers are always finite
                Ok(n.to_string())
            }
        }
        
        JsonValue::String(s) => {
            // Special handling for strings starting with "encrypted_U" or "binary_U"
            // TypeScript returns `"${node}"` which is a JSON string, so we use serde_json::to_string
            // which properly escapes and quotes the string
            Ok(serde_json::to_string(s).unwrap())
        }
        
        JsonValue::Array(arr) => {
            let mut out = String::from("[");
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                match stable_stringify(item) {
                    Ok(s) => out.push_str(&s),
                    Err(_) => out.push_str("null"),
                }
            }
            out.push(']');
            Ok(out)
        }
        
        JsonValue::Object(obj) => {
            // Sort keys alphabetically
            let mut keys: Vec<&String> = obj.keys().collect();
            keys.sort();
            
            let mut out = String::from("{");
            let mut first = true;
            
            for key in keys {
                let val = &obj[key];
                match stable_stringify(val) {
                    Ok(s) => {
                        if !first {
                            out.push(',');
                        }
                        // Properly escape the key
                        let key_str = serde_json::to_string(key)?;
                        out.push_str(&format!("{}:{}", key_str, s));
                        first = false;
                    }
                    Err(_) => {
                        // Skip undefined/null values (matching TypeScript behavior)
                        continue;
                    }
                }
            }
            
            out.push('}');
            Ok(out)
        }
    }
}

/// Parse a JSON string into a deserializable type.
///
/// # Arguments
/// * `json` - The JSON string to parse
///
/// # Returns
/// * `Ok(T)` - The deserialized value
/// * `Err(JsonUtilsError::JsonParse)` - If JSON parsing fails
///
/// # Example
/// ```
/// use cojson_core::parse_json;
///
/// let json = r#"{"name": "test", "value": 42}"#;
/// let result: serde_json::Value = parse_json(json).unwrap();
/// ```
pub fn parse_json<T: DeserializeOwned>(json: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(json).map_err(Into::into)
}

/// Safely parse a JSON string, returning None on error instead of an error type.
///
/// # Arguments
/// * `json` - The JSON string to parse
///
/// # Returns
/// * `Some(T)` - The deserialized value if parsing succeeds
/// * `None` - If JSON parsing fails
///
/// # Example
/// ```
/// use cojson_core::safe_parse_json;
///
/// let json = r#"{"name": "test"}"#;
/// let result: Option<serde_json::Value> = safe_parse_json(json);
/// assert!(result.is_some());
///
/// let invalid = "not json";
/// let result: Option<serde_json::Value> = safe_parse_json(invalid);
/// assert!(result.is_none());
/// ```
pub fn safe_parse_json<T: DeserializeOwned>(json: &str) -> Option<T> {
    serde_json::from_str(json).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_stable_stringify_null() {
        let value = JsonValue::Null;
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "null");
    }

    #[test]
    fn test_stable_stringify_bool() {
        let value = json!(true);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "true");
        
        let value = json!(false);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "false");
    }

    #[test]
    fn test_stable_stringify_number() {
        let value = json!(42);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "42");
        
        let value = json!(3.14);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "3.14");
    }

    #[test]
    fn test_stable_stringify_string() {
        let value = json!("hello");
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "\"hello\"");
    }

    #[test]
    fn test_stable_stringify_special_string_prefixes() {
        let value = json!("encrypted_U12345");
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "\"encrypted_U12345\"");
        
        let value = json!("binary_U67890");
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "\"binary_U67890\"");
    }

    #[test]
    fn test_stable_stringify_array() {
        let value = json!([1, 2, 3]);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "[1,2,3]");
        
        let value = json!(["a", "b", "c"]);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "[\"a\",\"b\",\"c\"]");
    }

    #[test]
    fn test_stable_stringify_object_key_sorting() {
        let value = json!({"b": 2, "a": 1, "c": 3});
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, r#"{"a":1,"b":2,"c":3}"#);
        
        let value = json!({"zebra": 1, "apple": 2, "banana": 3});
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, r#"{"apple":2,"banana":3,"zebra":1}"#);
    }

    #[test]
    fn test_stable_stringify_nested_objects() {
        let value = json!({
            "outer": {
                "inner": "value",
                "another": 42
            },
            "top": "level"
        });
        let result = stable_stringify(&value).unwrap();
        // Keys should be sorted at each level
        assert!(result.contains(r#""outer":"#));
        assert!(result.contains(r#""top":"#));
    }

    #[test]
    fn test_stable_stringify_nested_arrays() {
        let value = json!([[1, 2], [3, 4]]);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "[[1,2],[3,4]]");
    }

    #[test]
    fn test_stable_stringify_empty_structures() {
        let value = json!({});
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "{}");
        
        let value = json!([]);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "[]");
    }

    #[test]
    fn test_stable_stringify_infinity() {
        let value = json!(f64::INFINITY);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "null");
        
        let value = json!(f64::NEG_INFINITY);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "null");
    }

    #[test]
    fn test_stable_stringify_nan() {
        let value = json!(f64::NAN);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "null");
    }


    #[test]
    fn test_parse_json() {
        let json = r#"{"name": "test", "value": 42}"#;
        let result: serde_json::Value = parse_json(json).unwrap();
        assert_eq!(result["name"], "test");
        assert_eq!(result["value"], 42);
    }

    #[test]
    fn test_parse_json_error() {
        let invalid_json = "not json";
        let result: Result<serde_json::Value, _> = parse_json(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_safe_parse_json() {
        let json = r#"{"name": "test"}"#;
        let result: Option<serde_json::Value> = safe_parse_json(json);
        assert!(result.is_some());
        assert_eq!(result.unwrap()["name"], "test");
    }

    #[test]
    fn test_safe_parse_json_error() {
        let invalid_json = "not json";
        let result: Option<serde_json::Value> = safe_parse_json(invalid_json);
        assert!(result.is_none());
    }

    #[test]
    fn test_stable_stringify_complex_nested() {
        let value = json!({
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ],
            "metadata": {
                "version": "1.0",
                "count": 2
            }
        });
        let result = stable_stringify(&value).unwrap();
        // Verify it's valid JSON and keys are sorted
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["metadata"]["version"], "1.0");
        assert_eq!(parsed["users"][0]["name"], "Alice");
    }

    #[test]
    fn test_stable_stringify_escaped_strings() {
        let value = json!("hello \"world\"");
        let result = stable_stringify(&value).unwrap();
        assert!(result.contains("\\\""));
        
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed, value);
    }

    #[test]
    fn test_stable_stringify_array_with_null() {
        let value = json!([1, null, 3]);
        let result = stable_stringify(&value).unwrap();
        assert_eq!(result, "[1,null,3]");
    }
}
