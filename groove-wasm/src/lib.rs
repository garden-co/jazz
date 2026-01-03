use wasm_bindgen::prelude::*;
use groove::sql::{Database, ReactiveQuery, Value, ExecuteResult, Row, ObjectId};
use groove::ListenerId;
use std::sync::Arc;

/// WASM-exposed database wrapper.
#[wasm_bindgen]
pub struct WasmDatabase {
    db: Database,
}

#[wasm_bindgen]
impl WasmDatabase {
    /// Create a new in-memory database.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        WasmDatabase {
            db: Database::in_memory(),
        }
    }

    /// Execute a SQL statement.
    #[wasm_bindgen]
    pub fn execute(&self, sql: &str) -> Result<JsValue, JsValue> {
        match self.db.execute(sql) {
            Ok(result) => {
                let js_result = match result {
                    ExecuteResult::Created(_) => {
                        serde_wasm_bindgen::to_value(&"created").unwrap()
                    }
                    ExecuteResult::Inserted(id) => {
                        serde_wasm_bindgen::to_value(&format!("inserted:{}", id)).unwrap()
                    }
                    ExecuteResult::Updated(count) => {
                        serde_wasm_bindgen::to_value(&format!("updated:{}", count)).unwrap()
                    }
                    ExecuteResult::Deleted(count) => {
                        serde_wasm_bindgen::to_value(&format!("deleted:{}", count)).unwrap()
                    }
                    ExecuteResult::Selected(rows) => {
                        let row_data: Vec<Vec<String>> = rows
                            .iter()
                            .map(|row| row_to_strings(row))
                            .collect();
                        serde_wasm_bindgen::to_value(&row_data).unwrap()
                    }
                };
                Ok(js_result)
            }
            Err(e) => Err(JsValue::from_str(&format!("{:?}", e))),
        }
    }

    /// Update a specific row's column value.
    /// row_id should be a Base32 ObjectId string.
    #[wasm_bindgen]
    pub fn update_row(&self, table: &str, row_id: &str, column: &str, value: &str) -> Result<bool, JsValue> {
        let id: ObjectId = row_id.parse()
            .map_err(|e| JsValue::from_str(&format!("invalid row_id: {:?}", e)))?;
        self.db
            .update(table, id, &[(column, Value::String(value.to_string()))])
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    /// Create a reactive query that calls back on changes.
    /// Returns a handle that must be kept alive to maintain the subscription.
    #[wasm_bindgen]
    pub fn subscribe(&self, sql: &str, callback: js_sys::Function) -> Result<WasmQueryHandle, JsValue> {
        let query = self.db
            .reactive_query(sql)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(WasmQueryHandle::new(query, callback))
    }
}

fn row_to_strings(row: &Row) -> Vec<String> {
    row.values
        .iter()
        .map(|v| format!("{:?}", v))
        .collect()
}

/// Handle to a reactive query subscription.
/// The subscription stays active as long as this handle exists.
#[wasm_bindgen]
pub struct WasmQueryHandle {
    // Keep the query alive (it stays registered while we hold this Arc)
    _query: ReactiveQuery,
    // Listener ID for unsubscribing
    listener_id: ListenerId,
}

#[wasm_bindgen]
impl WasmQueryHandle {
    fn new(query: ReactiveQuery, callback: js_sys::Function) -> Self {
        // Wrap the JS callback in a Rust closure
        // The closure will be called synchronously when data changes
        let rust_callback = Box::new(move |rows: Arc<Vec<Row>>| {
            let row_data: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row_to_strings(row))
                .collect();

            // Call the JS callback synchronously
            let js_rows = serde_wasm_bindgen::to_value(&row_data).unwrap();
            let _ = callback.call1(&JsValue::NULL, &js_rows);
        });

        // Subscribe - this will call the callback immediately with current state,
        // and then synchronously whenever data changes
        let listener_id = query.subscribe(rust_callback);

        WasmQueryHandle {
            _query: query,
            listener_id,
        }
    }

    /// Unsubscribe from updates.
    #[wasm_bindgen]
    pub fn unsubscribe(&self) {
        self._query.unsubscribe(self.listener_id);
    }
}

/// Initialize panic hook for better error messages.
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}
