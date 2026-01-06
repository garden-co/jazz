use wasm_bindgen::prelude::*;
use groove::sql::{
    Database, IncrementalQuery, Value, ExecuteResult, Row,
    encode_rows, encode_delta,
};
use groove::ObjectId;
use groove::ListenerId;
use js_sys::{Array, Uint8Array};

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

    /// Execute a SQL statement (legacy string-based results).
    #[wasm_bindgen]
    pub fn execute(&self, sql: &str) -> Result<JsValue, JsValue> {
        match self.db.execute(sql) {
            Ok(result) => {
                let js_result = match result {
                    ExecuteResult::Created(_) => {
                        serde_wasm_bindgen::to_value(&"created").unwrap()
                    }
                    ExecuteResult::PolicyCreated { table, action } => {
                        serde_wasm_bindgen::to_value(&format!("policy_created:{}:{}", table, action)).unwrap()
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

    /// Execute a SELECT query and return results as binary Uint8Array.
    ///
    /// Binary format:
    /// - u32: row_count
    /// - Per row:
    ///   - 26 bytes: ObjectId (Base32 UTF-8)
    ///   - Column values in schema order
    #[wasm_bindgen]
    pub fn select_binary(&self, sql: &str) -> Result<Uint8Array, JsValue> {
        match self.db.execute(sql) {
            Ok(ExecuteResult::Selected(rows)) => {
                let binary = encode_rows(&rows);
                Ok(Uint8Array::from(binary.as_slice()))
            }
            Ok(_) => Err(JsValue::from_str("expected SELECT query")),
            Err(e) => Err(JsValue::from_str(&format!("{:?}", e))),
        }
    }

    /// Update a specific row's column with a string value.
    /// row_id should be a Base32 ObjectId string.
    #[wasm_bindgen]
    pub fn update_row(&self, table: &str, row_id: &str, column: &str, value: &str) -> Result<bool, JsValue> {
        let id: ObjectId = row_id.parse()
            .map_err(|e| JsValue::from_str(&format!("invalid row_id: {:?}", e)))?;
        self.db
            .update(table, id, &[(column, Value::String(value.to_string()))])
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    /// Update a specific row's column with an i64 value.
    /// row_id should be a Base32 ObjectId string.
    #[wasm_bindgen]
    pub fn update_row_i64(&self, table: &str, row_id: &str, column: &str, value: i64) -> Result<bool, JsValue> {
        let id: ObjectId = row_id.parse()
            .map_err(|e| JsValue::from_str(&format!("invalid row_id: {:?}", e)))?;
        self.db
            .update(table, id, &[(column, Value::I64(value))])
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    /// Initialize the database schema from a SQL string containing CREATE TABLE statements.
    /// Statements are separated by semicolons.
    #[wasm_bindgen]
    pub fn init_schema(&self, schema: &str) -> Result<(), JsValue> {
        for stmt in schema.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            self.db.execute(stmt)
                .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;
        }
        Ok(())
    }

    /// Create an incremental query that calls back on changes (legacy string-based).
    /// Returns a handle that must be kept alive to maintain the subscription.
    #[wasm_bindgen]
    pub fn subscribe(&self, sql: &str, callback: js_sys::Function) -> Result<WasmQueryHandle, JsValue> {
        let query = self.db
            .incremental_query(sql)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(WasmQueryHandle::new(query, callback))
    }

    /// Create an incremental query that calls back with binary data on changes.
    /// The callback receives a Uint8Array in the binary row format.
    /// Returns a handle that must be kept alive to maintain the subscription.
    #[wasm_bindgen]
    pub fn subscribe_binary(&self, sql: &str, callback: js_sys::Function) -> Result<WasmQueryHandleBinary, JsValue> {
        let query = self.db
            .incremental_query(sql)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(WasmQueryHandleBinary::new(query, callback))
    }

    /// Create an incremental query that calls back with individual delta buffers.
    /// The callback receives an Array of Uint8Array, one per delta.
    /// Each delta is: u8 type (1=add, 2=update, 3=remove) + row data (or just id for removes).
    /// Returns a handle that must be kept alive to maintain the subscription.
    #[wasm_bindgen]
    pub fn subscribe_delta(&self, sql: &str, callback: js_sys::Function) -> Result<WasmQueryHandleDelta, JsValue> {
        let query = self.db
            .incremental_query(sql)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(WasmQueryHandleDelta::new(query, callback))
    }
}

fn row_to_strings(row: &Row) -> Vec<String> {
    row.values
        .iter()
        .map(|v| format!("{:?}", v))
        .collect()
}

/// Handle to an incremental query subscription.
/// The subscription stays active as long as this handle exists.
#[wasm_bindgen]
pub struct WasmQueryHandle {
    // Keep the query alive (it stays registered while we hold this)
    _query: IncrementalQuery,
    // Listener ID for unsubscribing
    listener_id: Option<ListenerId>,
}

#[wasm_bindgen]
impl WasmQueryHandle {
    fn new(query: IncrementalQuery, callback: js_sys::Function) -> Self {
        // Wrap the JS callback in a Rust closure
        // The closure will be called synchronously when data changes
        let rust_callback = move |rows: Vec<Row>| {
            let row_data: Vec<Vec<String>> = rows
                .iter()
                .map(|row| row_to_strings(row))
                .collect();

            // Call the JS callback synchronously
            let js_rows = serde_wasm_bindgen::to_value(&row_data).unwrap();
            let _ = callback.call1(&JsValue::NULL, &js_rows);
        };

        // Subscribe - this will call the callback whenever data changes
        let listener_id = query.subscribe(rust_callback);

        WasmQueryHandle {
            _query: query,
            listener_id,
        }
    }

    /// Unsubscribe from updates.
    #[wasm_bindgen]
    pub fn unsubscribe(&mut self) {
        if let Some(id) = self.listener_id.take() {
            self._query.unsubscribe(id);
        }
    }
}

/// Handle to an incremental query subscription with binary encoding.
/// The subscription stays active as long as this handle exists.
#[wasm_bindgen]
pub struct WasmQueryHandleBinary {
    _query: IncrementalQuery,
    listener_id: Option<ListenerId>,
}

#[wasm_bindgen]
impl WasmQueryHandleBinary {
    fn new(query: IncrementalQuery, callback: js_sys::Function) -> Self {
        let rust_callback = move |rows: Vec<Row>| {
            let binary = encode_rows(&rows);
            let js_array = Uint8Array::from(binary.as_slice());
            let _ = callback.call1(&JsValue::NULL, &js_array);
        };

        let listener_id = query.subscribe(rust_callback);

        WasmQueryHandleBinary {
            _query: query,
            listener_id,
        }
    }

    /// Unsubscribe from updates.
    #[wasm_bindgen]
    pub fn unsubscribe(&mut self) {
        if let Some(id) = self.listener_id.take() {
            self._query.unsubscribe(id);
        }
    }
}

/// Handle to an incremental query subscription with per-delta binary encoding.
/// Each delta is encoded individually for efficient incremental decoding on JS side.
#[wasm_bindgen]
pub struct WasmQueryHandleDelta {
    _query: IncrementalQuery,
    listener_id: Option<ListenerId>,
}

#[wasm_bindgen]
impl WasmQueryHandleDelta {
    fn new(query: IncrementalQuery, callback: js_sys::Function) -> Self {
        // Use subscribe_delta to get individual RowDeltas
        let rust_callback = Box::new(move |delta_batch: &groove::sql::query_graph::DeltaBatch| {
            // Create a JS array of Uint8Arrays, one per delta
            let js_deltas = Array::new();

            for delta in delta_batch.iter() {
                let binary = encode_delta(delta);
                let js_array = Uint8Array::from(binary.as_slice());
                js_deltas.push(&js_array);
            }

            let _ = callback.call1(&JsValue::NULL, &js_deltas);
        });

        let listener_id = query.subscribe_delta(rust_callback);

        WasmQueryHandleDelta {
            _query: query,
            listener_id,
        }
    }

    /// Unsubscribe from updates.
    #[wasm_bindgen]
    pub fn unsubscribe(&mut self) {
        if let Some(id) = self.listener_id.take() {
            self._query.unsubscribe(id);
        }
    }

    /// Get a text diagram of the query graph.
    ///
    /// Returns a human-readable representation of the computation DAG
    /// showing node types, predicates, and current cache states.
    #[wasm_bindgen]
    pub fn diagram(&self) -> String {
        self._query.diagram()
    }
}

/// Initialize panic hook for better error messages.
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}
