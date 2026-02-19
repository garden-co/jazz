//! OPFS persistence integration tests for jazz-wasm.
//!
//! Run with:
//!   RUSTFLAGS='--cfg=web_sys_unstable_apis --cfg getrandom_backend="wasm_js"' \
//!     wasm-pack test --headless --chrome crates/jazz-wasm

#![cfg(target_arch = "wasm32")]

use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;

use jazz_wasm::WasmRuntime;
use jazz_wasm::types::WasmValue;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

fn test_schema_json() -> &'static str {
    r#"{
        "tables": {
            "todos": {
                "columns": [
                    {"name": "title", "column_type": {"type": "Text"}, "nullable": false},
                    {"name": "completed", "column_type": {"type": "Boolean"}, "nullable": false}
                ]
            }
        }
    }"#
}

fn make_query_json() -> String {
    r#"{"table": "todos"}"#.to_string()
}

fn make_filter_query_json() -> String {
    r#"{
        "table": "todos",
        "disjuncts": [{"conditions": [{"Eq": {"column": "completed", "value": {"Boolean": true}}}]}]
    }"#
    .to_string()
}

/// Helper to remove OPFS files. Ignores errors if files don't exist.
async fn cleanup_opfs(db_name: &str) {
    let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
    let storage = global.navigator().storage();
    let root: web_sys::FileSystemDirectoryHandle =
        wasm_bindgen_futures::JsFuture::from(storage.get_directory())
            .await
            .unwrap()
            .unchecked_into();

    for suffix in &[".opfsbtree"] {
        let name = format!("{}{}", db_name, suffix);
        let opts = js_sys::Object::new();
        js_sys::Reflect::set(&opts, &"recursive".into(), &true.into()).unwrap();
        let remove_fn = js_sys::Reflect::get(&root, &"removeEntry".into()).unwrap();
        let remove_fn: js_sys::Function = remove_fn.unchecked_into();
        let promise = remove_fn.call2(&root, &name.into(), &opts);
        if let Ok(p) = promise {
            let p: js_sys::Promise = p.unchecked_into();
            let _ = wasm_bindgen_futures::JsFuture::from(p).await;
        }
    }
}

fn insert_todo(runtime: &WasmRuntime, title: &str, completed: bool) -> String {
    let wasm_values = vec![
        WasmValue::Text(title.to_string()),
        WasmValue::Boolean(completed),
    ];
    let values = serde_wasm_bindgen::to_value(&wasm_values).unwrap();
    runtime.insert("todos", values).unwrap()
}

async fn query_todos(runtime: &WasmRuntime) -> Vec<serde_json::Value> {
    let query_json = make_query_json();
    let promise = runtime.query(&query_json, None, None).unwrap();
    let result = wasm_bindgen_futures::JsFuture::from(promise).await.unwrap();
    let rows: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    rows
}

async fn query_todos_filtered(runtime: &WasmRuntime) -> Vec<serde_json::Value> {
    let query_json = make_filter_query_json();
    let promise = runtime.query(&query_json, None, None).unwrap();
    let result = wasm_bindgen_futures::JsFuture::from(promise).await.unwrap();
    let rows: Vec<serde_json::Value> = serde_wasm_bindgen::from_value(result).unwrap();
    rows
}

#[wasm_bindgen_test]
async fn opfs_crud_round_trip() {
    let db_name = "test_crud";
    cleanup_opfs(db_name).await;

    let runtime =
        WasmRuntime::open_persistent(test_schema_json(), "test-app", "dev", "main", db_name, None)
            .await
            .unwrap();

    // Insert a todo
    let id = insert_todo(&runtime, "Buy milk", false);
    assert_eq!(id.len(), 36); // UUID format

    // Query
    let rows = query_todos(&runtime).await;
    assert_eq!(
        rows.len(),
        1,
        "Expected 1 todo, got {}: {:?}",
        rows.len(),
        rows
    );

    let values = rows[0]["values"].as_array().unwrap();
    assert_eq!(values[0]["value"], "Buy milk");
    assert_eq!(values[1]["value"], false);

    // Cleanup
    drop(runtime);
    cleanup_opfs(db_name).await;
}

#[wasm_bindgen_test]
async fn opfs_persistence_across_reopen() {
    let db_name = "test_persist";
    cleanup_opfs(db_name).await;

    // Phase 1: Insert data and flush
    {
        let runtime = WasmRuntime::open_persistent(
            test_schema_json(),
            "test-app",
            "dev",
            "main",
            db_name,
            None,
        )
        .await
        .unwrap();

        insert_todo(&runtime, "Task 1", false);
        insert_todo(&runtime, "Task 2", true);
        insert_todo(&runtime, "Task 3", false);

        runtime.flush();
    }

    // Phase 2: Reopen and verify data persisted
    {
        let runtime = WasmRuntime::open_persistent(
            test_schema_json(),
            "test-app",
            "dev",
            "main",
            db_name,
            None,
        )
        .await
        .unwrap();

        let rows = query_todos(&runtime).await;
        assert_eq!(
            rows.len(),
            3,
            "Expected 3 todos after reopen, got {}",
            rows.len()
        );
    }

    cleanup_opfs(db_name).await;
}

#[wasm_bindgen_test]
async fn opfs_index_operations() {
    let db_name = "test_index";
    cleanup_opfs(db_name).await;

    let runtime =
        WasmRuntime::open_persistent(test_schema_json(), "test-app", "dev", "main", db_name, None)
            .await
            .unwrap();

    // Insert todos with different completed states
    insert_todo(&runtime, "Done 1", true);
    insert_todo(&runtime, "Not done", false);
    insert_todo(&runtime, "Done 2", true);

    // Query only completed todos
    let rows = query_todos_filtered(&runtime).await;
    assert_eq!(
        rows.len(),
        2,
        "Expected 2 completed todos, got {}",
        rows.len()
    );

    for row in &rows {
        let values = row["values"].as_array().unwrap();
        assert_eq!(values[1]["value"], true, "Expected completed=true");
    }

    drop(runtime);
    cleanup_opfs(db_name).await;
}

#[wasm_bindgen_test]
async fn opfs_runtime_core_e2e() {
    let db_name = "test_e2e";
    cleanup_opfs(db_name).await;

    let runtime =
        WasmRuntime::open_persistent(test_schema_json(), "test-app", "dev", "main", db_name, None)
            .await
            .unwrap();

    // Insert
    let id = insert_todo(&runtime, "Original", false);

    // Query → verify 1 row
    let rows = query_todos(&runtime).await;
    assert_eq!(rows.len(), 1);
    let values = rows[0]["values"].as_array().unwrap();
    assert_eq!(values[0]["value"], "Original");
    assert_eq!(values[1]["value"], false);

    // Update → set completed=true
    let mut update_map = std::collections::HashMap::new();
    update_map.insert("completed".to_string(), WasmValue::Boolean(true));
    let update_values = serde_wasm_bindgen::to_value(&update_map).unwrap();
    runtime.update(&id, update_values).unwrap();

    // Query → verify updated value
    let rows = query_todos(&runtime).await;
    assert_eq!(rows.len(), 1);
    let values = rows[0]["values"].as_array().unwrap();
    assert_eq!(values[0]["value"], "Original");
    assert_eq!(values[1]["value"], true);

    // Delete
    runtime.delete(&id).unwrap();

    // Query → verify 0 rows
    let rows = query_todos(&runtime).await;
    assert_eq!(rows.len(), 0);

    drop(runtime);
    cleanup_opfs(db_name).await;
}
