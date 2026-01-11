use crate::object::ObjectId;
use crate::sql::row_buffer::OwnedRow;

// Re-export SchemaId from the object module
pub use crate::object::{ObjectIdParseError, SchemaId};

/// Key for a reference index: (source_table, source_column).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    pub source_table: String,
    pub source_column: String,
}

impl IndexKey {
    pub fn new(source_table: impl Into<String>, source_column: impl Into<String>) -> Self {
        IndexKey {
            source_table: source_table.into(),
            source_column: source_column.into(),
        }
    }
}

/// State of a query subscription.
#[derive(Debug, Clone)]
pub enum QueryState {
    /// Query is loading.
    Loading,
    /// Query has results. Each tuple is (ObjectId, OwnedRow).
    Loaded(Vec<(ObjectId, OwnedRow)>),
    /// Query encountered an error.
    Error(String),
}

impl QueryState {
    /// Check if query is in loading state.
    pub fn is_loading(&self) -> bool {
        matches!(self, QueryState::Loading)
    }

    /// Check if query is loaded.
    pub fn is_loaded(&self) -> bool {
        matches!(self, QueryState::Loaded(_))
    }

    /// Check if query has error.
    pub fn is_error(&self) -> bool {
        matches!(self, QueryState::Error(_))
    }

    /// Get rows if loaded.
    pub fn rows(&self) -> Option<Vec<(ObjectId, OwnedRow)>> {
        match self {
            QueryState::Loaded(rows) => Some(rows.clone()),
            _ => None,
        }
    }

    /// Get error message if error.
    pub fn error(&self) -> Option<&str> {
        match self {
            QueryState::Error(msg) => Some(msg),
            _ => None,
        }
    }
}
