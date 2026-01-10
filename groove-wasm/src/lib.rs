use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use groove::sql::{
    Database, IncrementalQuery, ExecuteResult,
    encode_rows, encode_delta,
    row_buffer::{OwnedRow, RowBuilder, RowDescriptor},
    query_graph::DeltaBatch,
};
use groove::{ObjectId, ContentRef, ChunkHash, INLINE_THRESHOLD};
use groove::ListenerId;
use js_sys::{Array, Promise, Uint8Array};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use bytes::Bytes;

pub mod indexeddb;
pub use indexeddb::IndexedDbEnvironment;

// ==================== Blob Registry ====================

/// Registry for tracking blob handles.
/// Maps handle IDs to ContentRefs. Actual chunk data is stored in the Environment's ChunkStore.
#[derive(Default)]
struct BlobRegistry {
    next_id: u64,
    blobs: HashMap<u64, ContentRef>,
}

impl BlobRegistry {
    fn new() -> Self {
        Self::default()
    }

    fn register(&mut self, content_ref: ContentRef) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.blobs.insert(id, content_ref);
        id
    }

    fn get(&self, id: u64) -> Option<&ContentRef> {
        self.blobs.get(&id)
    }

    fn remove(&mut self, id: u64) -> Option<ContentRef> {
        self.blobs.remove(&id)
    }
}

// ==================== Blob Writer ====================

/// State for incrementally building a blob from chunks.
struct BlobWriterState {
    chunks: Vec<Vec<u8>>,
    total_size: usize,
}

impl BlobWriterState {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            total_size: 0,
        }
    }

    fn write(&mut self, data: &[u8]) {
        self.total_size += data.len();
        self.chunks.push(data.to_vec());
    }

    /// Finish building the blob, returning the ContentRef and chunk data.
    /// For inline blobs, chunk_data will be empty.
    /// For chunked blobs, chunk_data contains the actual bytes for each chunk.
    fn finish(self) -> (ContentRef, Vec<Vec<u8>>) {
        // If total size is small enough, inline it
        if self.total_size <= INLINE_THRESHOLD {
            let mut combined = Vec::with_capacity(self.total_size);
            for chunk in self.chunks {
                combined.extend(chunk);
            }
            (ContentRef::inline(combined), Vec::new())
        } else {
            // Hash each chunk and create a chunked ContentRef
            let hashes: Vec<ChunkHash> = self.chunks
                .iter()
                .map(|chunk| ChunkHash::compute(chunk))
                .collect();
            (ContentRef::chunked(hashes), self.chunks)
        }
    }
}

// ==================== WASM Database ====================

/// WASM-exposed database wrapper.
#[wasm_bindgen]
pub struct WasmDatabase {
    db: Database,
    blob_registry: Rc<RefCell<BlobRegistry>>,
}

#[wasm_bindgen]
impl WasmDatabase {
    /// Create a new in-memory database.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        WasmDatabase {
            db: Database::in_memory(),
            blob_registry: Rc::new(RefCell::new(BlobRegistry::new())),
        }
    }

    /// Create or open a persistent IndexedDB-backed database.
    ///
    /// If a database already exists in IndexedDB, it will be loaded.
    /// Otherwise, a new database will be created.
    ///
    /// @param db_name - Optional database name (defaults to "groove")
    /// @returns Promise that resolves to WasmDatabase
    #[wasm_bindgen(js_name = "withIndexedDb")]
    pub fn with_indexeddb(db_name: Option<String>) -> Promise {
        future_to_promise(async move {
            let name = db_name.as_deref().unwrap_or("groove");
            let env = IndexedDbEnvironment::with_name(name).await?;
            let env = Arc::new(env);

            // Check if database already exists
            let db = if let Some(catalog_id_str) = env.get_catalog_id().await {
                // Load existing database
                let catalog_id: ObjectId = catalog_id_str.parse()
                    .map_err(|e| JsValue::from_str(&format!("invalid catalog_id: {:?}", e)))?;

                Database::from_env(env.clone(), catalog_id).await
                    .map_err(|e| JsValue::from_str(&format!("failed to load database: {:?}", e)))?
            } else {
                // Create new database
                let db = Database::new(env.clone());
                let catalog_id = db.catalog_object_id();

                // Store catalog ID for future sessions
                env.set_catalog_id(&catalog_id.to_string()).await?;

                db
            };

            Ok(JsValue::from(WasmDatabase {
                db,
                blob_registry: Rc::new(RefCell::new(BlobRegistry::new())),
            }))
        })
    }

    /// Check if a persistent database exists in IndexedDB.
    ///
    /// @param db_name - Optional database name (defaults to "groove")
    /// @returns Promise that resolves to boolean
    #[wasm_bindgen(js_name = "hasPersistedDatabase")]
    pub fn has_persisted_database(db_name: Option<String>) -> Promise {
        future_to_promise(async move {
            let name = db_name.as_deref().unwrap_or("groove");
            let env = IndexedDbEnvironment::with_name(name).await?;
            let has_db = env.has_database().await;
            Ok(JsValue::from(has_db))
        })
    }

    /// Delete a persistent database from IndexedDB.
    ///
    /// @param db_name - Optional database name (defaults to "groove")
    /// @returns Promise that resolves when deleted
    #[wasm_bindgen(js_name = "deletePersistedDatabase")]
    pub fn delete_persisted_database(db_name: Option<String>) -> Promise {
        future_to_promise(async move {
            let name = db_name.as_deref().unwrap_or("groove");

            let window = web_sys::window()
                .ok_or_else(|| JsValue::from_str("no window"))?;
            let idb = window
                .indexed_db()?
                .ok_or_else(|| JsValue::from_str("IndexedDB not available"))?;

            let delete_request = idb.delete_database(name)?;

            // Await the delete request
            let (tx, rx) = futures::channel::oneshot::channel::<Result<(), JsValue>>();
            let tx = Rc::new(RefCell::new(Some(tx)));

            let tx_success = tx.clone();
            let on_success = Closure::once(Box::new(move |_event: web_sys::Event| {
                if let Some(tx) = tx_success.borrow_mut().take() {
                    let _ = tx.send(Ok(()));
                }
            }) as Box<dyn FnOnce(_)>);

            let tx_error = tx;
            let on_error = Closure::once(Box::new(move |_event: web_sys::Event| {
                if let Some(tx) = tx_error.borrow_mut().take() {
                    let _ = tx.send(Err(JsValue::from_str("failed to delete database")));
                }
            }) as Box<dyn FnOnce(_)>);

            delete_request.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
            delete_request.set_onerror(Some(on_error.as_ref().unchecked_ref()));

            on_success.forget();
            on_error.forget();

            rx.await
                .map_err(|_| JsValue::from_str("channel closed"))?
                .map_err(|e| e)?;

            Ok(JsValue::UNDEFINED)
        })
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
                            .map(|(_id, row)| row_to_strings(row))
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
        let value = value.to_string();
        self.db
            .update_with(table, id, |b| b.set_string_by_name(column, &value).build())
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }

    /// Update a specific row's column with an i64 value.
    /// row_id should be a Base32 ObjectId string.
    #[wasm_bindgen]
    pub fn update_row_i64(&self, table: &str, row_id: &str, column: &str, value: i64) -> Result<bool, JsValue> {
        let id: ObjectId = row_id.parse()
            .map_err(|e| JsValue::from_str(&format!("invalid row_id: {:?}", e)))?;
        self.db
            .update_with(table, id, |b| b.set_i64_by_name(column, value).build())
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

    /// List all tables in the database.
    /// Returns an array of table names.
    #[wasm_bindgen]
    pub fn list_tables(&self) -> JsValue {
        let tables = self.db.list_tables();
        serde_wasm_bindgen::to_value(&tables).unwrap_or(JsValue::NULL)
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

    // ==================== Blob APIs ====================

    /// Create a blob from raw bytes.
    /// Returns a blob handle ID that can be used in insert/update operations.
    /// Chunks are stored in the Environment's ChunkStore for persistence.
    #[wasm_bindgen]
    pub fn create_blob(&self, data: &[u8]) -> u64 {
        use futures::executor::block_on;

        if data.len() <= INLINE_THRESHOLD {
            let content_ref = ContentRef::inline(data.to_vec());
            self.blob_registry.borrow_mut().register(content_ref)
        } else {
            // For large data, chunk it and store in Environment
            let env = self.db.node().env();
            let hashes: Vec<ChunkHash> = data
                .chunks(INLINE_THRESHOLD)
                .map(|chunk| {
                    // Store each chunk in Environment's ChunkStore
                    block_on(env.put_chunk(Bytes::copy_from_slice(chunk)))
                })
                .collect();
            let content_ref = ContentRef::chunked(hashes);
            self.blob_registry.borrow_mut().register(content_ref)
        }
    }

    /// Create a blob writer for streaming blob creation.
    /// Call write_blob_chunk() to add data, then finish_blob() to get the handle.
    #[wasm_bindgen]
    pub fn create_blob_writer(&self) -> WasmBlobWriter {
        WasmBlobWriter::new(Rc::clone(&self.blob_registry), self.db.node().env().clone())
    }

    /// Read all bytes from a blob handle.
    /// For small blobs this returns the inline data directly.
    /// For large chunked blobs, this reads and concatenates all chunks from Environment.
    /// Use read_blob_chunk() for streaming reads of large blobs.
    #[wasm_bindgen]
    pub fn read_blob(&self, handle_id: u64) -> Result<Uint8Array, JsValue> {
        use futures::executor::block_on;

        let registry = self.blob_registry.borrow();
        let content_ref = registry.get(handle_id)
            .ok_or_else(|| JsValue::from_str("invalid blob handle"))?;

        match content_ref {
            ContentRef::Inline(data) => Ok(Uint8Array::from(data.as_ref())),
            ContentRef::Chunked(hashes) => {
                // Concatenate all chunks from Environment
                let env = self.db.node().env();
                let mut result = Vec::new();
                for hash in hashes {
                    let chunk = block_on(env.get_chunk(hash))
                        .ok_or_else(|| JsValue::from_str("chunk not found in environment"))?;
                    result.extend_from_slice(&chunk);
                }
                Ok(Uint8Array::from(result.as_slice()))
            }
        }
    }

    /// Get information about a blob.
    /// Returns a JS object with: { isInline: bool, chunkCount: number, size?: number }
    #[wasm_bindgen]
    pub fn get_blob_info(&self, handle_id: u64) -> Result<JsValue, JsValue> {
        let registry = self.blob_registry.borrow();
        let content_ref = registry.get(handle_id)
            .ok_or_else(|| JsValue::from_str("invalid blob handle"))?;

        let info = match content_ref {
            ContentRef::Inline(data) => {
                BlobInfo {
                    is_inline: true,
                    chunk_count: 1,
                    size: Some(data.len() as u64),
                }
            }
            ContentRef::Chunked(hashes) => {
                BlobInfo {
                    is_inline: false,
                    chunk_count: hashes.len() as u32,
                    size: None, // Size unknown without reading chunks
                }
            }
        };

        serde_wasm_bindgen::to_value(&info)
            .map_err(|e| JsValue::from_str(&format!("serialization error: {:?}", e)))
    }

    /// Read a specific chunk of a blob by index.
    /// For inline blobs, index 0 returns all data.
    /// For chunked blobs, returns the chunk at the given index from Environment.
    #[wasm_bindgen]
    pub fn read_blob_chunk(&self, handle_id: u64, chunk_index: u32) -> Result<Uint8Array, JsValue> {
        use futures::executor::block_on;

        let registry = self.blob_registry.borrow();
        let content_ref = registry.get(handle_id)
            .ok_or_else(|| JsValue::from_str("invalid blob handle"))?;

        match content_ref {
            ContentRef::Inline(data) => {
                if chunk_index == 0 {
                    Ok(Uint8Array::from(data.as_ref()))
                } else {
                    Err(JsValue::from_str("chunk index out of bounds for inline blob"))
                }
            }
            ContentRef::Chunked(hashes) => {
                let idx = chunk_index as usize;
                if idx < hashes.len() {
                    let hash = &hashes[idx];
                    let env = self.db.node().env();
                    let chunk = block_on(env.get_chunk(hash))
                        .ok_or_else(|| JsValue::from_str("chunk not found in environment"))?;
                    Ok(Uint8Array::from(chunk.as_ref()))
                } else {
                    Err(JsValue::from_str("chunk index out of bounds"))
                }
            }
        }
    }

    /// Release a blob handle, freeing the associated memory.
    /// Call this when you're done with a blob to prevent memory leaks.
    #[wasm_bindgen]
    pub fn release_blob(&self, handle_id: u64) {
        self.blob_registry.borrow_mut().remove(handle_id);
    }

    /// Insert a row with blob values.
    /// string_columns is an array of [column_name, value] pairs for string columns.
    /// blob_columns is an array of [column_name, blob_handle_id] pairs.
    #[wasm_bindgen]
    pub fn insert_with_blobs(
        &self,
        table: &str,
        string_columns: JsValue,
        blob_columns: JsValue,
    ) -> Result<String, JsValue> {
        // Parse string columns: [[name, value], ...]
        let string_cols: Vec<(String, String)> = serde_wasm_bindgen::from_value(string_columns)
            .map_err(|e| JsValue::from_str(&format!("invalid string_columns: {:?}", e)))?;

        // Parse blob columns: [[name, handle_id], ...]
        let blob_cols: Vec<(String, u64)> = serde_wasm_bindgen::from_value(blob_columns)
            .map_err(|e| JsValue::from_str(&format!("invalid blob_columns: {:?}", e)))?;

        // Get table schema and build row using RowBuilder
        let schema = self.db.get_table(table)
            .ok_or_else(|| JsValue::from_str(&format!("table not found: {}", table)))?;
        let descriptor = Arc::new(RowDescriptor::from_table_schema(&schema));
        let mut builder = RowBuilder::new(descriptor);

        // Add string columns
        for (name, value) in string_cols {
            builder = builder.set_string_by_name(&name, &value);
        }

        // Add blob columns
        let registry = self.blob_registry.borrow();
        for (name, handle_id) in blob_cols {
            let content_ref = registry.get(handle_id)
                .ok_or_else(|| JsValue::from_str(&format!("invalid blob handle: {}", handle_id)))?;
            builder = builder.set_blob_by_name(&name, content_ref.clone());
        }
        drop(registry);

        // Execute insert
        let row = builder.build();
        let id = self.db.insert_row(table, row)
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))?;

        Ok(id.to_string())
    }

    /// Update a row's blob column.
    #[wasm_bindgen]
    pub fn update_row_blob(
        &self,
        table: &str,
        row_id: &str,
        column: &str,
        blob_handle_id: u64,
    ) -> Result<bool, JsValue> {
        let id: ObjectId = row_id.parse()
            .map_err(|e| JsValue::from_str(&format!("invalid row_id: {:?}", e)))?;

        let registry = self.blob_registry.borrow();
        let content_ref = registry.get(blob_handle_id)
            .ok_or_else(|| JsValue::from_str("invalid blob handle"))?;
        let content_ref = content_ref.clone();
        drop(registry);

        self.db
            .update_with(table, id, |b| b.set_blob_by_name(column, content_ref.clone()).build())
            .map_err(|e| JsValue::from_str(&format!("{:?}", e)))
    }
}

fn row_to_strings(row: &OwnedRow) -> Vec<String> {
    row.descriptor
        .columns
        .iter()
        .enumerate()
        .map(|(i, _)| {
            if let Some(value) = row.get(i) {
                format!("{:?}", value)
            } else {
                "NULL".to_string()
            }
        })
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
        // Note: This legacy API shows all current rows on each update (not just deltas)
        let rust_callback = Box::new(move |delta_batch: &DeltaBatch| {
            // Collect rows from Added and Updated deltas (representing current state)
            let row_data: Vec<Vec<String>> = delta_batch
                .iter()
                .filter_map(|delta| delta.new_row())
                .map(|row| row_to_strings(row))
                .collect();

            // Call the JS callback synchronously
            let js_rows = serde_wasm_bindgen::to_value(&row_data).unwrap();
            let _ = callback.call1(&JsValue::NULL, &js_rows);
        });

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
        let rust_callback = Box::new(move |delta_batch: &DeltaBatch| {
            // Collect (id, row) pairs from Added and Updated deltas
            let rows: Vec<(ObjectId, OwnedRow)> = delta_batch
                .iter()
                .filter_map(|delta| {
                    delta.new_row().map(|row| (delta.row_id(), row.clone()))
                })
                .collect();
            let binary = encode_rows(&rows);
            let js_array = Uint8Array::from(binary.as_slice());
            let _ = callback.call1(&JsValue::NULL, &js_array);
        });

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

        let listener_id = query.subscribe(rust_callback);

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

// ==================== Blob Types ====================

/// Information about a blob.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BlobInfo {
    is_inline: bool,
    chunk_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
}

/// Handle for streaming blob creation.
/// Use write() to add chunks, then finish() to get a blob handle.
#[wasm_bindgen]
pub struct WasmBlobWriter {
    state: Option<BlobWriterState>,
    registry: Rc<RefCell<BlobRegistry>>,
    env: std::sync::Arc<dyn groove::Environment>,
}

#[wasm_bindgen]
impl WasmBlobWriter {
    fn new(registry: Rc<RefCell<BlobRegistry>>, env: std::sync::Arc<dyn groove::Environment>) -> Self {
        Self {
            state: Some(BlobWriterState::new()),
            registry,
            env,
        }
    }

    /// Write a chunk of data to the blob.
    /// Can be called multiple times before finish().
    #[wasm_bindgen]
    pub fn write(&mut self, data: &[u8]) -> Result<(), JsValue> {
        let state = self.state.as_mut()
            .ok_or_else(|| JsValue::from_str("blob writer already finished"))?;
        state.write(data);
        Ok(())
    }

    /// Get the current total size of data written.
    #[wasm_bindgen]
    pub fn size(&self) -> Result<u32, JsValue> {
        let state = self.state.as_ref()
            .ok_or_else(|| JsValue::from_str("blob writer already finished"))?;
        Ok(state.total_size as u32)
    }

    /// Finish writing and get a blob handle.
    /// Stores chunks in the Environment's ChunkStore for persistence.
    /// The writer cannot be used after this.
    #[wasm_bindgen]
    pub fn finish(&mut self) -> Result<u64, JsValue> {
        use futures::executor::block_on;

        let state = self.state.take()
            .ok_or_else(|| JsValue::from_str("blob writer already finished"))?;
        let (content_ref, chunk_data) = state.finish();

        // Store chunks in Environment if this is a chunked blob
        if let ContentRef::Chunked(hashes) = &content_ref {
            for (hash, data) in hashes.iter().zip(chunk_data.into_iter()) {
                // Verify hash matches (put_chunk returns the computed hash)
                let stored_hash = block_on(self.env.put_chunk(Bytes::copy_from_slice(&data)));
                debug_assert_eq!(hash, &stored_hash, "chunk hash mismatch");
            }
        }

        let handle = self.registry.borrow_mut().register(content_ref);
        Ok(handle)
    }

    /// Abort the blob creation, discarding all written data.
    #[wasm_bindgen]
    pub fn abort(&mut self) {
        self.state = None;
    }
}

// ==================== JS Stream Helpers ====================

/// Create a ReadableStream that reads from a blob.
/// This is a convenience wrapper for JS interop.
#[wasm_bindgen]
pub fn create_blob_readable_stream(db: &WasmDatabase, handle_id: u64) -> Result<JsValue, JsValue> {
    // Get blob info first
    let registry = db.blob_registry.borrow();
    let content_ref = registry.get(handle_id)
        .ok_or_else(|| JsValue::from_str("invalid blob handle"))?;

    let (is_inline, chunk_count) = match content_ref {
        ContentRef::Inline(_) => (true, 1u32),
        ContentRef::Chunked(hashes) => (false, hashes.len() as u32),
    };
    drop(registry);

    // Create a JS ReadableStream using the Streams API
    // We'll return a configuration object that JS can use to create the stream
    let config = StreamConfig {
        handle_id,
        is_inline,
        chunk_count,
    };

    serde_wasm_bindgen::to_value(&config)
        .map_err(|e| JsValue::from_str(&format!("serialization error: {:?}", e)))
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamConfig {
    handle_id: u64,
    is_inline: bool,
    chunk_count: u32,
}

/// Initialize panic hook for better error messages.
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

// ==================== ObjectId Helpers ====================

/// Convert a 16-byte binary ObjectId to a Base32 string.
///
/// This is useful for displaying ObjectIds or using them as string keys.
/// The binary format is u128 little-endian.
#[wasm_bindgen]
pub fn object_id_to_string(bytes: &[u8]) -> Result<String, JsValue> {
    if bytes.len() != 16 {
        return Err(JsValue::from_str("ObjectId must be exactly 16 bytes"));
    }
    let id_bytes: [u8; 16] = bytes.try_into().unwrap();
    let id = ObjectId::from_le_bytes(id_bytes);
    Ok(id.to_string())
}

/// Convert a Base32 string ObjectId to 16-byte binary.
///
/// Returns a Uint8Array containing the u128 little-endian bytes.
#[wasm_bindgen]
pub fn object_id_from_string(s: &str) -> Result<Uint8Array, JsValue> {
    let id: ObjectId = s.parse()
        .map_err(|e| JsValue::from_str(&format!("invalid ObjectId: {:?}", e)))?;
    let bytes = id.to_le_bytes();
    Ok(Uint8Array::from(&bytes[..]))
}

// ==================== JS Helper Code ====================
// See blob-helpers.ts for TypeScript convenience wrappers around these WASM APIs:
// - blobToReadableStream(): Convert blob handle to ReadableStream
// - readableStreamToBlob(): Create blob from ReadableStream
// - GrooveBlob: Wrapper class for blob access
// - GrooveBlobWriter: Wrapper for streaming blob creation with WritableStream
