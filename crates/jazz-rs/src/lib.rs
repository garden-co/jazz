//! Jazz Rust client library.
//!
//! Provides a high-level API for building Jazz applications with:
//! - Local persistence via BfTree storage
//! - Server sync via HTTP/SSE
//! - Query subscriptions with real-time updates
//!
//! # Example
//!
//! ```ignore
//! use jazz_rs::{JazzClient, AppContext};
//!
//! let context = AppContext {
//!     app_id: AppId::from_name("my-app"),
//!     schema: my_schema,
//!     server_url: "http://localhost:1625".to_string(),
//!     data_dir: PathBuf::from("./data"),
//! };
//!
//! let client = JazzClient::connect(context).await?;
//!
//! // Subscribe to query
//! let stream = client.subscribe(query).await?;
//!
//! // One-shot query
//! let rows = client.query(query, None).await?;
//!
//! // Mutations
//! let id = client.create("users", vec![name]).await?;
//! client.update(id, vec![("name", new_name)]).await?;
//! client.delete(id).await?;
//! ```

mod client;
mod transport;

use std::path::PathBuf;

use thiserror::Error;

pub use client::{JazzClient, SessionClient};

// Re-exports for convenience
pub use groove::object::ObjectId;
pub use groove::query_manager::query::{Query, QueryBuilder};
pub use groove::query_manager::session::Session;
pub use groove::query_manager::types::{
    ColumnType, Row, RowDelta, Schema, SchemaBuilder, TableName, TableSchema, Value,
};
pub use groove::schema_manager::AppId;
pub use groove::sync_manager::ClientId;
pub use groove::sync_manager::PersistenceTier;
pub use groove::sync_manager::ServerId;
pub use jazz_transport::ServerEvent;

/// Configuration for connecting to Jazz.
#[derive(Debug, Clone)]
pub struct AppContext {
    /// Application ID.
    pub app_id: AppId,
    /// Client ID (generated if not provided).
    pub client_id: Option<ClientId>,
    /// Schema for this client.
    pub schema: Schema,
    /// Server URL for sync (e.g., "http://localhost:1625").
    pub server_url: String,
    /// Local data directory for BfTree storage.
    pub data_dir: PathBuf,

    // Authentication fields
    /// JWT token for frontend authentication.
    /// Sent as `Authorization: Bearer <token>`.
    pub jwt_token: Option<String>,
    /// Backend secret for session impersonation.
    /// Enables `for_session()` to act as any user.
    pub backend_secret: Option<String>,
    /// Admin secret for schema/policy sync.
    /// Required to sync catalogue objects.
    pub admin_secret: Option<String>,
}

/// Errors from Jazz client operations.
#[derive(Error, Debug)]
pub enum JazzError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Write error: {0}")]
    Write(String),

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel closed")]
    ChannelClosed,
}

/// Result type for Jazz operations.
pub type Result<T> = std::result::Result<T, JazzError>;

/// Handle to a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

/// Stream of row deltas from a subscription.
pub struct SubscriptionStream {
    #[allow(dead_code)]
    handle: SubscriptionHandle,
    receiver: tokio::sync::mpsc::Receiver<RowDelta>,
}

impl SubscriptionStream {
    /// Create a new subscription stream.
    pub(crate) fn new(
        handle: SubscriptionHandle,
        receiver: tokio::sync::mpsc::Receiver<RowDelta>,
    ) -> Self {
        Self { handle, receiver }
    }

    /// Get the next delta, waiting if necessary.
    pub async fn next(&mut self) -> Option<RowDelta> {
        self.receiver.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::query_manager::query::QueryBuilder;
    use tempfile::TempDir;

    fn test_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .build()
    }

    #[tokio::test]
    async fn test_crud_operations() {
        let temp_dir = TempDir::new().unwrap();

        let context = AppContext {
            app_id: AppId::from_name("test-crud"),
            client_id: None,
            schema: test_schema(),
            server_url: String::new(), // No server
            data_dir: temp_dir.path().to_path_buf(),
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
        };

        let client = JazzClient::connect(context).await.unwrap();

        // Create a todo
        let values = vec![Value::Text("Buy milk".to_string()), Value::Boolean(false)];
        let row_id = client.create("todos", values).await.unwrap();
        assert!(!row_id.0.is_nil());

        // Query todos - now returns (ObjectId, Vec<Value>)
        let query = QueryBuilder::new("todos").build();
        let results = client.query(query, None).await.unwrap();
        assert_eq!(results.len(), 1);
        let (id, values) = &results[0];
        assert_eq!(*id, row_id);
        assert_eq!(values[0], Value::Text("Buy milk".to_string()));
        assert_eq!(values[1], Value::Boolean(false));

        // Update todo
        let updates = vec![("completed".to_string(), Value::Boolean(true))];
        client.update(row_id, updates).await.unwrap();

        // Query again to verify update
        let query = QueryBuilder::new("todos").build();
        let results = client.query(query, None).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[1], Value::Boolean(true));

        // Delete todo
        client.delete(row_id).await.unwrap();

        // Query should return empty (soft delete filters by default)
        let query = QueryBuilder::new("todos").build();
        let results = client.query(query, None).await.unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let data_path = temp_dir.path().to_path_buf();

        // Phase 1: Create data
        let created_id = {
            let context = AppContext {
                app_id: AppId::from_name("test-persist"),
                client_id: None,
                schema: test_schema(),
                server_url: String::new(),
                data_dir: data_path.clone(),
                jwt_token: None,
                backend_secret: None,
                admin_secret: None,
            };
            let client = JazzClient::connect(context).await.unwrap();

            let values = vec![Value::Text("Persist me".to_string()), Value::Boolean(false)];
            let row_id = client.create("todos", values).await.unwrap();

            // Verify it exists
            let query = QueryBuilder::new("todos").build();
            let results = client.query(query, None).await.unwrap();
            assert_eq!(results.len(), 1, "Should have created todo");

            client.shutdown().await.unwrap();
            row_id
        };

        // Phase 2: Reopen and verify
        {
            let context = AppContext {
                app_id: AppId::from_name("test-persist"),
                client_id: None,
                schema: test_schema(),
                server_url: String::new(),
                data_dir: data_path,
                jwt_token: None,
                backend_secret: None,
                admin_secret: None,
            };
            let client = JazzClient::connect(context).await.unwrap();

            // Query should return persisted data immediately - no retry needed
            // because one-shot queries now wait for pending local storage loads
            let query = QueryBuilder::new("todos").build();
            let results = client.query(query, None).await.unwrap();

            assert_eq!(results.len(), 1, "Todo should persist");
            assert_eq!(results[0].0, created_id);
            assert_eq!(results[0].1[0], Value::Text("Persist me".to_string()));

            client.shutdown().await.unwrap();
        }
    }
}
