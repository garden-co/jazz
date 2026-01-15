//! Shared row cache for query graphs.

use std::collections::HashMap;
use std::rc::Rc;

use crate::object::ObjectId;
use crate::sql::row_buffer::{OwnedRow, RowDescriptor};
use crate::sql::schema::TableSchema;

// ============================================================================
// RowCache - unified row format storage (replaces legacy Row-based cache)
// ============================================================================

/// Shared cache of row data, accessible to all query graphs.
///
/// The cache stores row data by table and row ID. A `None` value
/// indicates that the row is confirmed deleted (tombstoned).
/// Rows are stored as `(ObjectId, OwnedRow)` tuples since row IDs
/// are stored out-of-band in the buffer format.
#[derive(Debug, Default)]
pub struct RowCache {
    /// table -> row_id -> cached Row (None = confirmed deleted)
    rows: HashMap<String, HashMap<ObjectId, Option<OwnedRow>>>,
}

impl RowCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a cached row.
    ///
    /// Returns:
    /// - `Some(Some(row))` if the row is cached and exists
    /// - `Some(None)` if the row is cached as deleted
    /// - `None` if the row is not in the cache (needs to be loaded)
    pub fn get(&self, table: &str, id: ObjectId) -> Option<Option<&OwnedRow>> {
        self.rows.get(table)?.get(&id).map(|opt| opt.as_ref())
    }

    /// Check if a row is cached (regardless of whether it exists or is deleted).
    pub fn contains(&self, table: &str, id: ObjectId) -> bool {
        self.rows
            .get(table)
            .map(|t| t.contains_key(&id))
            .unwrap_or(false)
    }

    /// Insert or update a row in the cache.
    pub fn insert(&mut self, table: &str, id: ObjectId, row: OwnedRow) {
        self.rows
            .entry(table.to_string())
            .or_default()
            .insert(id, Some(row));
    }

    /// Mark a row as deleted in the cache.
    pub fn mark_deleted(&mut self, table: &str, id: ObjectId) {
        self.rows
            .entry(table.to_string())
            .or_default()
            .insert(id, None);
    }

    /// Remove a row from the cache entirely.
    ///
    /// The row will need to be re-fetched on next access.
    pub fn invalidate(&mut self, table: &str, id: ObjectId) {
        if let Some(table_cache) = self.rows.get_mut(table) {
            table_cache.remove(&id);
        }
    }

    /// Clear all cached data for a table.
    pub fn clear_table(&mut self, table: &str) {
        self.rows.remove(table);
    }

    /// Clear the entire cache.
    pub fn clear(&mut self) {
        self.rows.clear();
    }

    /// Get the number of cached entries for a table.
    pub fn table_size(&self, table: &str) -> usize {
        self.rows.get(table).map(|t| t.len()).unwrap_or(0)
    }

    /// Get the total number of cached entries across all tables.
    pub fn total_size(&self) -> usize {
        self.rows.values().map(|t| t.len()).sum()
    }
}

// ============================================================================
// Buffer Row Cache - unified row format with schema tracking
// ============================================================================

/// Table metadata for the buffer row cache.
struct TableMeta {
    /// Row descriptor for this table (shared by all rows).
    descriptor: Rc<RowDescriptor>,
    /// Cached rows by ObjectId (None = confirmed deleted).
    rows: HashMap<ObjectId, Option<OwnedRow>>,
}

/// Cache for the new unified row buffer format.
///
/// Unlike `RowCache` which stores legacy `Row` values, this cache stores
/// `OwnedRow` with efficient binary layout. Each table has an associated
/// `RowDescriptor` that defines the column structure.
#[derive(Debug, Default)]
pub struct BufferRowCache {
    /// table -> table metadata with descriptor and rows
    tables: HashMap<String, TableMeta>,
}

impl std::fmt::Debug for TableMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableMeta")
            .field("descriptor", &self.descriptor)
            .field("rows_count", &self.rows.len())
            .finish()
    }
}

impl BufferRowCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table's schema, creating a row descriptor for it.
    ///
    /// This must be called before inserting rows for a table.
    pub fn register_table(&mut self, table: &str, schema: &TableSchema) {
        let descriptor = Rc::new(RowDescriptor::from_table_schema(schema));
        self.tables.insert(
            table.to_string(),
            TableMeta {
                descriptor,
                rows: HashMap::new(),
            },
        );
    }

    /// Register a table with an existing descriptor.
    pub fn register_table_with_descriptor(&mut self, table: &str, descriptor: Rc<RowDescriptor>) {
        self.tables.insert(
            table.to_string(),
            TableMeta {
                descriptor,
                rows: HashMap::new(),
            },
        );
    }

    /// Get the row descriptor for a table.
    pub fn get_descriptor(&self, table: &str) -> Option<Rc<RowDescriptor>> {
        self.tables.get(table).map(|t| t.descriptor.clone())
    }

    /// Get a cached row.
    ///
    /// Returns:
    /// - `Some(Some(row))` if the row is cached and exists
    /// - `Some(None)` if the row is cached as deleted
    /// - `None` if the row is not in the cache (needs to be loaded)
    pub fn get(&self, table: &str, id: ObjectId) -> Option<Option<&OwnedRow>> {
        self.tables
            .get(table)?
            .rows
            .get(&id)
            .map(|opt| opt.as_ref())
    }

    /// Check if a row is cached (regardless of whether it exists or is deleted).
    pub fn contains(&self, table: &str, id: ObjectId) -> bool {
        self.tables
            .get(table)
            .map(|t| t.rows.contains_key(&id))
            .unwrap_or(false)
    }

    /// Insert or update a row in the cache.
    ///
    /// Panics if the table hasn't been registered.
    pub fn insert(&mut self, table: &str, id: ObjectId, row: OwnedRow) {
        if let Some(table_meta) = self.tables.get_mut(table) {
            table_meta.rows.insert(id, Some(row));
        } else {
            panic!(
                "Table '{}' not registered in BufferRowCache. Call register_table first.",
                table
            );
        }
    }

    /// Mark a row as deleted in the cache.
    pub fn mark_deleted(&mut self, table: &str, id: ObjectId) {
        if let Some(table_meta) = self.tables.get_mut(table) {
            table_meta.rows.insert(id, None);
        }
    }

    /// Remove a row from the cache entirely.
    ///
    /// The row will need to be re-fetched on next access.
    pub fn invalidate(&mut self, table: &str, id: ObjectId) {
        if let Some(table_meta) = self.tables.get_mut(table) {
            table_meta.rows.remove(&id);
        }
    }

    /// Clear all cached rows for a table (keeps the descriptor).
    pub fn clear_table_rows(&mut self, table: &str) {
        if let Some(table_meta) = self.tables.get_mut(table) {
            table_meta.rows.clear();
        }
    }

    /// Remove a table entirely (descriptor and rows).
    pub fn remove_table(&mut self, table: &str) {
        self.tables.remove(table);
    }

    /// Clear the entire cache.
    pub fn clear(&mut self) {
        self.tables.clear();
    }

    /// Get the number of cached rows for a table.
    pub fn table_size(&self, table: &str) -> usize {
        self.tables.get(table).map(|t| t.rows.len()).unwrap_or(0)
    }

    /// Get the total number of cached rows across all tables.
    pub fn total_size(&self) -> usize {
        self.tables.values().map(|t| t.rows.len()).sum()
    }

    /// Check if a table is registered.
    pub fn has_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Get all registered table names.
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(|s| s.as_str())
    }

    /// Iterate over all cached rows for a table.
    pub fn iter_table(&self, table: &str) -> impl Iterator<Item = (ObjectId, Option<&OwnedRow>)> {
        self.tables
            .get(table)
            .into_iter()
            .flat_map(|t| t.rows.iter().map(|(&id, opt)| (id, opt.as_ref())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row_buffer::{RowBuilder, RowDescriptor, RowValue};
    use crate::sql::schema::ColumnType;

    fn make_user_descriptor() -> Rc<RowDescriptor> {
        Rc::new(RowDescriptor::new([
            ("name".to_string(), ColumnType::String, false),
            ("age".to_string(), ColumnType::I32, false),
        ]))
    }

    fn make_buffer_row(descriptor: &Rc<RowDescriptor>, name: &str, age: i32) -> OwnedRow {
        let name_idx = descriptor.column_index("name").unwrap();
        let age_idx = descriptor.column_index("age").unwrap();
        RowBuilder::new(descriptor.clone())
            .set_string(name_idx, name)
            .set_i32(age_idx, age)
            .build()
    }

    #[test]
    fn buffer_cache_empty() {
        let cache = BufferRowCache::new();
        assert_eq!(cache.total_size(), 0);
        assert!(!cache.has_table("users"));
        assert!(cache.get("users", ObjectId::new(1)).is_none());
    }

    #[test]
    fn buffer_cache_register_and_insert() {
        let mut cache = BufferRowCache::new();
        let descriptor = make_user_descriptor();

        cache.register_table_with_descriptor("users", descriptor.clone());
        assert!(cache.has_table("users"));

        let id = ObjectId::new(1);
        let row = make_buffer_row(&descriptor, "Alice", 30);
        cache.insert("users", id, row);

        assert_eq!(cache.table_size("users"), 1);
        assert!(cache.contains("users", id));

        let cached = cache.get("users", id);
        assert!(cached.is_some());
        assert!(cached.unwrap().is_some());

        // Verify row data
        let row_ref = cached.unwrap().unwrap().as_ref();
        let name_idx = descriptor.column_index("name").unwrap();
        let age_idx = descriptor.column_index("age").unwrap();

        // Check name
        match row_ref.get(name_idx) {
            Some(RowValue::String(s)) => assert_eq!(s, "Alice"),
            other => panic!("Expected String, got {:?}", other),
        }

        // Check age
        match row_ref.get(age_idx) {
            Some(RowValue::I32(n)) => assert_eq!(n, 30),
            other => panic!("Expected I32, got {:?}", other),
        }
    }

    #[test]
    fn buffer_cache_mark_deleted() {
        let mut cache = BufferRowCache::new();
        let descriptor = make_user_descriptor();

        cache.register_table_with_descriptor("users", descriptor.clone());

        let id = ObjectId::new(1);
        let row = make_buffer_row(&descriptor, "Alice", 30);
        cache.insert("users", id, row);
        cache.mark_deleted("users", id);

        let cached = cache.get("users", id);
        // Should be Some(None) - cached as deleted
        assert!(cached.is_some());
        assert!(cached.unwrap().is_none());
    }

    #[test]
    fn buffer_cache_invalidate() {
        let mut cache = BufferRowCache::new();
        let descriptor = make_user_descriptor();

        cache.register_table_with_descriptor("users", descriptor.clone());

        let id = ObjectId::new(1);
        let row = make_buffer_row(&descriptor, "Alice", 30);
        cache.insert("users", id, row);
        assert!(cache.contains("users", id));

        cache.invalidate("users", id);

        // Should be None - not in cache at all
        assert!(cache.get("users", id).is_none());
        assert!(!cache.contains("users", id));
    }

    #[test]
    fn buffer_cache_get_descriptor() {
        let mut cache = BufferRowCache::new();
        let descriptor = make_user_descriptor();

        cache.register_table_with_descriptor("users", descriptor.clone());

        let retrieved = cache.get_descriptor("users");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().columns.len(), 2);
    }

    #[test]
    fn buffer_cache_clear_table_rows() {
        let mut cache = BufferRowCache::new();
        let descriptor = make_user_descriptor();

        cache.register_table_with_descriptor("users", descriptor.clone());
        cache.insert(
            "users",
            ObjectId::new(1),
            make_buffer_row(&descriptor, "Alice", 30),
        );
        cache.insert(
            "users",
            ObjectId::new(2),
            make_buffer_row(&descriptor, "Bob", 25),
        );

        assert_eq!(cache.table_size("users"), 2);

        cache.clear_table_rows("users");

        assert_eq!(cache.table_size("users"), 0);
        // Table is still registered
        assert!(cache.has_table("users"));
        assert!(cache.get_descriptor("users").is_some());
    }

    #[test]
    fn buffer_cache_remove_table() {
        let mut cache = BufferRowCache::new();
        let descriptor = make_user_descriptor();

        cache.register_table_with_descriptor("users", descriptor.clone());
        cache.insert(
            "users",
            ObjectId::new(1),
            make_buffer_row(&descriptor, "Alice", 30),
        );

        cache.remove_table("users");

        assert!(!cache.has_table("users"));
        assert!(cache.get_descriptor("users").is_none());
    }

    #[test]
    fn buffer_cache_iter_table() {
        let mut cache = BufferRowCache::new();
        let descriptor = make_user_descriptor();

        cache.register_table_with_descriptor("users", descriptor.clone());
        cache.insert(
            "users",
            ObjectId::new(1),
            make_buffer_row(&descriptor, "Alice", 30),
        );
        cache.insert(
            "users",
            ObjectId::new(2),
            make_buffer_row(&descriptor, "Bob", 25),
        );

        let rows: Vec<_> = cache.iter_table("users").collect();
        assert_eq!(rows.len(), 2);

        // Both should be Some (not deleted)
        for (_, opt_row) in rows {
            assert!(opt_row.is_some());
        }
    }
}
