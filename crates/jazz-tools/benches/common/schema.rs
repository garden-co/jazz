//! Benchmark schema: Team collaboration app with teams, folders, and documents.
//!
//! Schema exercises:
//! - Simple session comparisons (`owner_id = @session.user_id`)
//! - INHERITS chains (documents → folders → teams)

use std::collections::HashMap;

use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::policy::{Operation, PolicyExpr};
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::TablePolicies;
use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TableSchema, Value,
};
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore, VecSyncSender};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::{MemoryStorage, Storage};
use jazz_tools::sync_manager::SyncManager;

pub type BenchRuntime<S = MemoryStorage> = RuntimeCore<S, NoopScheduler, VecSyncSender>;

fn row<const N: usize>(pairs: [(&str, Value); N]) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

/// Create the benchmark schema with teams, folders, and documents.
///
/// Policies:
/// - teams SELECT: `owner_id = @session.user_id`
/// - teams INSERT: `owner_id = @session.user_id`
/// - folders SELECT: `INHERITS SELECT VIA team_id`
/// - folders INSERT: `INHERITS SELECT VIA team_id`
/// - documents SELECT: `author_id = @session.user_id OR INHERITS SELECT VIA folder_id`
/// - documents INSERT: `INHERITS SELECT VIA folder_id`
/// - documents UPDATE USING: `author_id = @session.user_id OR INHERITS SELECT VIA folder_id`
/// - documents UPDATE WITH CHECK: `INHERITS SELECT VIA folder_id`
pub fn create_schema() -> Schema {
    let mut schema = Schema::new();

    // Teams table
    let teams_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("owner_id", ColumnType::Text),
    ]);
    let teams_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));
    schema.insert(
        TableName::new("teams"),
        TableSchema::with_policies(teams_descriptor, teams_policies),
    );

    // Folders table
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("team_id", ColumnType::Uuid).references("teams"),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("created_at", ColumnType::Timestamp),
    ]);
    let folders_policies = TablePolicies::new()
        .with_select(PolicyExpr::inherits(Operation::Select, "team_id"))
        .with_insert(PolicyExpr::inherits(Operation::Select, "team_id"));
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor, folders_policies),
    );

    // Documents table
    let documents_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("folder_id", ColumnType::Uuid).references("folders"),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("content", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Text),
        ColumnDescriptor::new("created_at", ColumnType::Timestamp),
    ]);
    let documents_select = PolicyExpr::or(vec![
        PolicyExpr::eq_session("author_id", vec!["user_id".into()]),
        PolicyExpr::inherits(Operation::Select, "folder_id"),
    ]);
    let documents_insert = PolicyExpr::inherits(Operation::Select, "folder_id");
    let documents_update_using = PolicyExpr::or(vec![
        PolicyExpr::eq_session("author_id", vec!["user_id".into()]),
        PolicyExpr::inherits(Operation::Select, "folder_id"),
    ]);
    let documents_update_check = PolicyExpr::inherits(Operation::Select, "folder_id");
    let documents_policies = TablePolicies::new()
        .with_select(documents_select)
        .with_insert(documents_insert)
        .with_update(Some(documents_update_using), documents_update_check);
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(documents_descriptor, documents_policies),
    );

    schema
}

/// Pre-populated data with references to created objects.
#[allow(dead_code)]
pub struct BenchmarkData {
    /// Team ObjectIds owned by the benchmark user.
    pub owned_teams: Vec<ObjectId>,
    /// Folder ObjectIds in owned teams.
    pub owned_folders: Vec<ObjectId>,
    /// Document ObjectIds authored by the benchmark user.
    pub owned_documents: Vec<ObjectId>,
    /// Document ObjectIds in accessible folders but authored by others.
    pub team_documents: Vec<ObjectId>,
    /// All document ObjectIds.
    pub all_documents: Vec<ObjectId>,
}

/// Setup test data at the given scale.
///
/// Scale determines document count:
/// - 10_000 documents: 100 teams, 1000 folders
/// - 100_000 documents: 1000 teams, 10000 folders
///
/// The session user owns 10% of teams and authors 50% of documents.
pub fn setup_data<S: Storage>(
    core: &mut BenchRuntime<S>,
    scale: usize,
    user_id: &str,
) -> BenchmarkData {
    let (num_teams, num_folders) = match scale {
        10_000 => (100, 1000),
        100_000 => (1000, 10000),
        _ => {
            let teams = scale / 100;
            let folders = scale / 10;
            (teams.max(10), folders.max(100))
        }
    };

    let owned_team_count = num_teams / 10; // User owns 10% of teams

    let mut owned_teams = Vec::with_capacity(owned_team_count);
    let mut other_teams = Vec::with_capacity(num_teams - owned_team_count);
    let mut owned_folders = Vec::with_capacity(num_folders / 10);
    let mut other_folders = Vec::with_capacity(num_folders - num_folders / 10);
    let mut owned_documents = Vec::with_capacity(scale / 2);
    let mut team_documents = Vec::with_capacity(scale / 2);
    let mut all_documents = Vec::with_capacity(scale);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    // Create teams
    for i in 0..num_teams {
        let is_owned = i < owned_team_count;
        let owner = if is_owned { user_id } else { "other_user" };
        let (row_id, _row_values) = core
            .insert(
                "teams",
                row([
                    ("name", Value::Text(format!("Team {}", i))),
                    ("owner_id", Value::Text(owner.to_string())),
                ]),
                None,
            )
            .expect("insert team");
        core.immediate_tick();
        core.batched_tick();

        if is_owned {
            owned_teams.push(row_id);
        } else {
            other_teams.push(row_id);
        }
    }

    // Create folders (distributed across teams)
    for i in 0..num_folders {
        let team_idx = i % num_teams;
        let team_id = if team_idx < owned_team_count {
            owned_teams[team_idx]
        } else {
            other_teams[team_idx - owned_team_count]
        };

        let (row_id, _row_values) = core
            .insert(
                "folders",
                row([
                    ("team_id", Value::Uuid(team_id)),
                    ("name", Value::Text(format!("Folder {}", i))),
                    ("created_at", Value::Timestamp(now + i as u64)),
                ]),
                None,
            )
            .expect("insert folder");
        core.immediate_tick();
        core.batched_tick();

        if team_idx < owned_team_count {
            owned_folders.push(row_id);
        } else {
            other_folders.push(row_id);
        }
    }

    // Create documents
    let owned_folder_len = owned_folders.len();
    let other_folder_len = other_folders.len();

    for i in 0..scale {
        // 50% in owned folders authored by user, 50% in other folders authored by others
        let (folder_id, author) = if i % 2 == 0 && owned_folder_len > 0 {
            let folder_idx = (i / 2) % owned_folder_len;
            (owned_folders[folder_idx], user_id)
        } else if other_folder_len > 0 {
            let folder_idx = (i / 2) % other_folder_len;
            (other_folders[folder_idx], "other_author")
        } else if owned_folder_len > 0 {
            let folder_idx = i % owned_folder_len;
            (owned_folders[folder_idx], user_id)
        } else {
            continue;
        };

        let (row_id, _row_values) = core
            .insert(
                "documents",
                row([
                    ("folder_id", Value::Uuid(folder_id)),
                    ("title", Value::Text(format!("Document {}", i))),
                    ("content", Value::Text(format!("Content of document {}", i))),
                    ("author_id", Value::Text(author.to_string())),
                    ("created_at", Value::Timestamp(now + i as u64)),
                ]),
                None,
            )
            .expect("insert document");
        core.immediate_tick();
        core.batched_tick();

        all_documents.push(row_id);
        if author == user_id {
            owned_documents.push(row_id);
        } else {
            team_documents.push(row_id);
        }
    }

    BenchmarkData {
        owned_teams,
        owned_folders,
        owned_documents,
        team_documents,
        all_documents,
    }
}

/// Create a session for the benchmark user.
pub fn create_session(user_id: &str) -> Session {
    Session::new(user_id)
}

pub fn create_runtime_with_storage<S: Storage>(storage: S) -> BenchRuntime<S> {
    let sync_manager = SyncManager::new();
    let schema = create_schema();
    let schema_manager = SchemaManager::new(
        sync_manager,
        schema,
        AppId::from_name("bench"),
        "dev",
        "main",
    )
    .expect("schema manager");
    RuntimeCore::new(schema_manager, storage, NoopScheduler, VecSyncSender::new())
}

/// Create a new RuntimeCore with MemoryStorage for benchmarking.
///
/// Uses MemoryStorage which drops all storage requests, allowing
/// benchmarks to measure pure in-memory performance without storage overhead.
pub fn create_runtime() -> BenchRuntime {
    create_runtime_with_storage(MemoryStorage::new())
}

/// Get the current timestamp in microseconds.
pub fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
