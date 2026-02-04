//! WASM integration tests for groove-wasm.
//!
//! Run with: wasm-pack test --node

#![cfg(target_arch = "wasm32")]

use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

use groove_wasm::{current_timestamp, generate_id, parse_schema, WasmQueryBuilder};

#[wasm_bindgen_test]
fn test_generate_id() {
    let id1 = generate_id();
    let id2 = generate_id();

    // IDs should be valid UUID format
    assert_eq!(id1.len(), 36);
    assert_eq!(id2.len(), 36);

    // IDs should be unique
    assert_ne!(id1, id2);
}

#[wasm_bindgen_test]
fn test_current_timestamp() {
    let ts1 = current_timestamp();
    let ts2 = current_timestamp();

    // Timestamps should be reasonable (after 2024)
    assert!(ts1 > 1704067200000000); // 2024-01-01 in microseconds

    // Second timestamp should be >= first
    assert!(ts2 >= ts1);
}

#[wasm_bindgen_test]
fn test_parse_schema() {
    let schema_json = r#"{
        "tables": {
            "todos": {
                "columns": [
                    {"name": "title", "column_type": {"type": "Text"}, "nullable": false},
                    {"name": "completed", "column_type": {"type": "Boolean"}, "nullable": false}
                ]
            }
        }
    }"#;

    let result = parse_schema(schema_json);
    assert!(result.is_ok());
}

#[wasm_bindgen_test]
fn test_parse_schema_invalid() {
    let result = parse_schema("not valid json");
    assert!(result.is_err());
}

#[wasm_bindgen_test]
fn test_query_builder_basic() {
    let builder = WasmQueryBuilder::new("todos");
    let query = builder.branch("main").build();

    assert!(query.is_ok());
    let query_str = query.unwrap();
    assert!(query_str.contains("todos"));
    assert!(query_str.contains("main"));
}

#[wasm_bindgen_test]
fn test_query_builder_with_filters() {
    let builder = WasmQueryBuilder::new("todos");

    // Create a text value for filtering
    let value = JsValue::from_serde(&serde_json::json!({
        "type": "Boolean",
        "value": true
    }))
    .unwrap();

    let result = builder.branch("main").filter_eq("completed", value);

    assert!(result.is_ok());

    let query = result.unwrap().build();
    assert!(query.is_ok());

    let query_str = query.unwrap();
    assert!(query_str.contains("completed"));
}

#[wasm_bindgen_test]
fn test_query_builder_order_and_limit() {
    let builder = WasmQueryBuilder::new("todos");
    let query = builder
        .branch("main")
        .order_by_desc("created_at")
        .limit(10)
        .offset(5)
        .build();

    assert!(query.is_ok());
    let query_str = query.unwrap();
    assert!(query_str.contains("created_at"));
    assert!(query_str.contains("10"));
}

#[wasm_bindgen_test]
fn test_query_builder_select() {
    let builder = WasmQueryBuilder::new("todos");
    let query = builder
        .branch("main")
        .select(vec!["title".to_string(), "completed".to_string()])
        .build();

    assert!(query.is_ok());
    let query_str = query.unwrap();
    assert!(query_str.contains("title"));
    assert!(query_str.contains("completed"));
}

#[wasm_bindgen_test]
fn test_query_builder_join() {
    let builder = WasmQueryBuilder::new("posts");
    let query = builder
        .branch("main")
        .alias("p")
        .join("users")
        .alias("u")
        .on("p.author_id", "u.id")
        .build();

    assert!(query.is_ok());
    let query_str = query.unwrap();
    assert!(query_str.contains("posts"));
    assert!(query_str.contains("users"));
}

#[wasm_bindgen_test]
fn test_query_builder_or() {
    let builder = WasmQueryBuilder::new("todos");

    let value1 = JsValue::from_serde(&serde_json::json!({
        "type": "Text",
        "value": "urgent"
    }))
    .unwrap();

    let value2 = JsValue::from_serde(&serde_json::json!({
        "type": "Boolean",
        "value": true
    }))
    .unwrap();

    let result = builder.branch("main").filter_eq("priority", value1);

    assert!(result.is_ok());

    let result2 = result.unwrap().or().filter_eq("urgent", value2);

    assert!(result2.is_ok());

    let query = result2.unwrap().build();
    assert!(query.is_ok());
}

#[wasm_bindgen_test]
fn test_query_builder_multiple_branches() {
    let builder = WasmQueryBuilder::new("todos");
    let query = builder
        .branches(vec!["main".to_string(), "draft".to_string()])
        .build();

    assert!(query.is_ok());
    let query_str = query.unwrap();
    assert!(query_str.contains("main"));
    assert!(query_str.contains("draft"));
}

// Note: WasmRuntime tests require a JS driver implementation,
// which can't be easily mocked in Rust WASM tests.
// Full runtime tests should be done in JavaScript/TypeScript.
