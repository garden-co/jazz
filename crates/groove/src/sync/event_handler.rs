//! Shared SSE event handling logic.
//!
//! This module provides platform-agnostic handling of sync events,
//! used by both native `SyncedNode` and WASM `WasmSyncedLocalNode`.

use std::collections::BTreeMap;

use crate::commit::{Commit, CommitId};
use crate::object::ObjectId;
use crate::sql::Database;

use super::protocol::SseEvent;

/// Result of handling an SSE event.
#[derive(Debug)]
pub enum EventHandlerResult {
    /// Event processed successfully.
    Ok,
    /// Error occurred during processing.
    Error(String),
}

/// Handle a Commits SSE event.
///
/// This is the core logic shared between native and WASM:
/// 1. Apply commits to the database's underlying LocalNode
/// 2. Extract table name from metadata if present
/// 3. Register the row with the database for incremental query notifications
///
/// Returns the table name if one was found in metadata.
pub fn handle_commits_event(
    db: &Database,
    object_id: ObjectId,
    commits: Vec<Commit>,
    _frontier: Vec<CommitId>,
    object_meta: Option<BTreeMap<String, String>>,
) -> Result<Option<String>, String> {
    // Extract table name from metadata if present
    let table_name = object_meta
        .as_ref()
        .and_then(|meta| meta.get("table"))
        .cloned();

    // Ensure the object exists locally with the table name as hint
    if let Some(ref table) = table_name {
        db.node().ensure_object(object_id, table);
    }

    // Apply commits to the LocalNode
    db.node().apply_commits(object_id, "main", commits);

    // Register with Database for incremental query notifications
    if let Some(ref table) = table_name {
        db.register_synced_row_by_table(object_id, table)
            .map_err(|e| format!("Failed to register synced row: {:?}", e))?;
    } else if let Some(descriptor_str) = object_meta.as_ref().and_then(|m| m.get("descriptor")) {
        // Legacy fallback: use descriptor ID lookup
        db.register_synced_row(object_id, descriptor_str)
            .map_err(|e| format!("Failed to register synced row: {:?}", e))?;
    } else {
        // No metadata - this might be an update to an existing row
        // Try to notify query graphs if we already know about this row
        let _ = db.notify_synced_row_update(object_id);
    }

    Ok(table_name)
}

/// Handle an SSE event (all types).
///
/// This provides a unified handler for all SSE event types.
/// Platforms can use this for common handling or implement their own
/// for platform-specific behavior (e.g., broadcast, logging).
pub fn handle_sse_event(
    db: &Database,
    event: &SseEvent,
) -> EventHandlerResult {
    match event {
        SseEvent::Commits {
            object_id,
            commits,
            frontier,
            object_meta,
        } => {
            match handle_commits_event(
                db,
                *object_id,
                commits.clone(),
                frontier.clone(),
                object_meta.clone(),
            ) {
                Ok(_) => EventHandlerResult::Ok,
                Err(e) => EventHandlerResult::Error(e),
            }
        }
        SseEvent::Excluded { object_id: _ } => {
            // Object no longer matches query
            // Platforms may want to clean up local tracking
            EventHandlerResult::Ok
        }
        SseEvent::Truncate {
            object_id: _,
            truncate_at: _,
        } => {
            // Server truncating history
            // TODO: Implement truncation support
            EventHandlerResult::Ok
        }
        SseEvent::Request {
            object_id: _,
            commit_ids: _,
        } => {
            // Server requesting specific commits
            // TODO: Implement push response
            EventHandlerResult::Ok
        }
        SseEvent::Error { code, message } => {
            EventHandlerResult::Error(format!("SSE error {}: {}", code, message))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::Commit;

    fn make_test_commit() -> Commit {
        Commit {
            parents: vec![],
            content: b"test".to_vec().into_boxed_slice(),
            author: "test".to_string(),
            timestamp: 0,
            meta: None,
        }
    }

    #[test]
    fn test_handle_commits_with_table_name() {
        let db = Database::in_memory();

        // Create a table
        db.execute("CREATE TABLE users (name STRING)").unwrap();

        // Create object ID for the row
        let row_id = ObjectId::new_random();

        // Create a commit with some content
        let commit = make_test_commit();

        // Handle commits with table metadata
        let mut meta = BTreeMap::new();
        meta.insert("table".to_string(), "users".to_string());

        let result = handle_commits_event(
            &db,
            row_id,
            vec![commit],
            vec![],
            Some(meta),
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some("users".to_string()));
    }

    #[test]
    fn test_handle_commits_without_metadata() {
        let db = Database::in_memory();

        let row_id = ObjectId::new_random();
        let commit = make_test_commit();

        // Handle commits without metadata
        let result = handle_commits_event(&db, row_id, vec![commit], vec![], None);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_handle_sse_error_event() {
        let db = Database::in_memory();

        let event = SseEvent::Error {
            code: 500,
            message: "Internal error".to_string(),
        };

        let result = handle_sse_event(&db, &event);

        match result {
            EventHandlerResult::Error(msg) => {
                assert!(msg.contains("500"));
                assert!(msg.contains("Internal error"));
            }
            _ => panic!("Expected error result"),
        }
    }
}
