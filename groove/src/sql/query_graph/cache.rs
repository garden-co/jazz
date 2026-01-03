//! Shared row cache for query graphs.

use std::collections::HashMap;

use crate::sql::row::Row;
use crate::object::ObjectId;

/// Shared cache of row data, accessible to all query graphs.
///
/// The cache stores row data by table and row ID. A `None` value
/// indicates that the row is confirmed deleted (tombstoned).
#[derive(Debug, Default)]
pub struct RowCache {
    /// table -> row_id -> cached Row (None = confirmed deleted)
    rows: HashMap<String, HashMap<ObjectId, Option<Row>>>,
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
    pub fn get(&self, table: &str, id: ObjectId) -> Option<Option<&Row>> {
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
    pub fn insert(&mut self, table: &str, row: Row) {
        self.rows
            .entry(table.to_string())
            .or_default()
            .insert(row.id, Some(row));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row::Value;

    fn make_row(id: u128, name: &str) -> Row {
        Row::new(ObjectId::new(id), vec![Value::String(name.to_string())])
    }

    #[test]
    fn cache_empty() {
        let cache = RowCache::new();
        assert_eq!(cache.total_size(), 0);
        assert!(cache.get("users", ObjectId::new(1)).is_none());
    }

    #[test]
    fn cache_insert_and_get() {
        let mut cache = RowCache::new();
        let row = make_row(1, "Alice");

        cache.insert("users", row.clone());

        let cached = cache.get("users", ObjectId::new(1));
        assert!(cached.is_some());
        assert!(cached.unwrap().is_some());
        assert_eq!(cached.unwrap().unwrap().id, ObjectId::new(1));
    }

    #[test]
    fn cache_mark_deleted() {
        let mut cache = RowCache::new();
        let row = make_row(1, "Alice");

        cache.insert("users", row);
        cache.mark_deleted("users", ObjectId::new(1));

        let cached = cache.get("users", ObjectId::new(1));
        // Should be Some(None) - cached as deleted
        assert!(cached.is_some());
        assert!(cached.unwrap().is_none());
    }

    #[test]
    fn cache_invalidate() {
        let mut cache = RowCache::new();
        let row = make_row(1, "Alice");

        cache.insert("users", row);
        assert!(cache.contains("users", ObjectId::new(1)));

        cache.invalidate("users", ObjectId::new(1));

        // Should be None - not in cache at all
        assert!(cache.get("users", ObjectId::new(1)).is_none());
        assert!(!cache.contains("users", ObjectId::new(1)));
    }

    #[test]
    fn cache_clear_table() {
        let mut cache = RowCache::new();

        cache.insert("users", make_row(1, "Alice"));
        cache.insert("users", make_row(2, "Bob"));
        cache.insert("posts", make_row(10, "Hello"));

        assert_eq!(cache.table_size("users"), 2);
        assert_eq!(cache.table_size("posts"), 1);

        cache.clear_table("users");

        assert_eq!(cache.table_size("users"), 0);
        assert_eq!(cache.table_size("posts"), 1);
    }

    #[test]
    fn cache_clear_all() {
        let mut cache = RowCache::new();

        cache.insert("users", make_row(1, "Alice"));
        cache.insert("posts", make_row(10, "Hello"));

        assert_eq!(cache.total_size(), 2);

        cache.clear();

        assert_eq!(cache.total_size(), 0);
    }
}
