//! ReBAC Policy Evaluation Integration Tests
//!
//! Tests for the async permission evaluation system using policy graphs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smallvec::smallvec;

use crate::batch_fate::BatchFate;
use crate::metadata::{
    DeleteKind, MetadataKey, RowProvenance, SYSTEM_PRINCIPAL_ID, row_provenance_metadata,
};
use crate::object::{BranchName, ObjectId};
use crate::row_histories::BatchId;
use crate::storage::{MemoryStorage, Storage};
use crate::sync_manager::{
    ClientId, Destination, InboxEntry, QueryId, RowMetadata, Source, SyncError, SyncManager,
    SyncPayload,
};
use crate::test_support::{
    apply_test_row_batch, create_test_row, load_test_row_metadata, load_test_row_tip_ids,
    seeded_memory_storage,
};

use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::manager::QueryError;
use crate::query_manager::manager::QueryManager;
use crate::query_manager::policy::Operation;
use crate::query_manager::query::{Query, QueryBuilder};
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, RowPolicyMode, Schema,
    SchemaBuilder, SchemaHash, TableName, TableSchema, Value, permissions, policy_expr as pe,
};
use crate::row_histories::{RowState, StoredRowBatch};

/// Helper to create QueryManager with schema on default branch.
fn create_query_manager(sync_manager: SyncManager, schema: Schema) -> QueryManager {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema, "dev", "main");
    qm
}

fn create_query_manager_with_policy_mode(
    sync_manager: SyncManager,
    schema: Schema,
    row_policy_mode: RowPolicyMode,
) -> QueryManager {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema_with_policy_mode(schema, "dev", "main", row_policy_mode);
    qm
}

/// Get the schema context's branch name.
fn get_branch(qm: &QueryManager) -> String {
    qm.schema_context().branch_name().as_str().to_string()
}

fn connect_client(qm: &mut QueryManager, storage: &MemoryStorage, client_id: ClientId) {
    qm.sync_manager_mut()
        .add_client_with_storage(storage, client_id);
}

fn set_client_query_scope(
    qm: &mut QueryManager,
    storage: &MemoryStorage,
    client_id: ClientId,
    query_id: QueryId,
    scope: HashSet<(ObjectId, BranchName)>,
    session: Option<Session>,
) {
    qm.sync_manager_mut()
        .set_client_query_scope_with_storage(storage, client_id, query_id, scope, session);
}

#[derive(Debug, Clone)]
struct IncomingRowBatch {
    batch_id: BatchId,
    parents: smallvec::SmallVec<[BatchId; 2]>,
    content: Vec<u8>,
    timestamp: u64,
    author: String,
    delete_kind: Option<DeleteKind>,
}

impl IncomingRowBatch {
    fn row_provenance(&self) -> RowProvenance {
        RowProvenance::for_insert(self.author.clone(), self.timestamp)
    }

    fn row_metadata(&self) -> HashMap<String, String> {
        row_provenance_metadata(&self.row_provenance(), self.delete_kind)
            .into_iter()
            .collect()
    }

    fn to_row(&self, object_id: ObjectId, branch: &str, state: RowState) -> StoredRowBatch {
        StoredRowBatch::new_with_batch_id(
            self.batch_id,
            object_id,
            branch,
            self.parents.iter().copied().collect::<Vec<_>>(),
            self.content.clone(),
            self.row_provenance(),
            self.row_metadata(),
            state,
            None,
        )
    }
}

fn stored_row_commit(
    parents: smallvec::SmallVec<[BatchId; 2]>,
    content: Vec<u8>,
    timestamp: u64,
    author: impl Into<String>,
    delete_kind: Option<DeleteKind>,
) -> IncomingRowBatch {
    IncomingRowBatch {
        batch_id: BatchId::new(),
        parents,
        content,
        timestamp,
        author: author.into(),
        delete_kind,
    }
}

fn row_batch_created_payload(
    object_id: ObjectId,
    branch: &str,
    metadata: Option<RowMetadata>,
    commit: &IncomingRowBatch,
) -> SyncPayload {
    SyncPayload::RowBatchCreated {
        metadata,
        row: commit.to_row(object_id, branch, RowState::VisibleDirect),
    }
}

fn row_batch_id_for_commit(
    object_id: ObjectId,
    branch: &str,
    commit: &IncomingRowBatch,
) -> BatchId {
    commit
        .to_row(object_id, branch, RowState::VisibleDirect)
        .batch_id()
}

fn client_write_rejection_reason(
    outbox: &[crate::sync_manager::OutboxEntry],
    client_id: ClientId,
    batch_id: BatchId,
) -> Option<String> {
    let mut settlement_reason = None;

    for entry in outbox {
        if entry.destination != Destination::Client(client_id) {
            continue;
        }

        match &entry.payload {
            SyncPayload::Error(SyncError::PermissionDenied { reason, .. }) => {
                return Some(reason.clone());
            }
            SyncPayload::BatchFate {
                fate:
                    BatchFate::Rejected {
                        batch_id: rejected_batch_id,
                        reason,
                        ..
                    },
            } if *rejected_batch_id == batch_id => {
                settlement_reason = Some(reason.clone());
            }
            _ => {}
        }
    }

    settlement_reason
}

fn client_write_was_rejected(
    outbox: &[crate::sync_manager::OutboxEntry],
    client_id: ClientId,
    batch_id: BatchId,
) -> bool {
    client_write_rejection_reason(outbox, client_id, batch_id).is_some()
}

fn add_row_commit(
    storage: &mut MemoryStorage,
    object_id: ObjectId,
    branch: &str,
    parents: Vec<BatchId>,
    content: Vec<u8>,
    timestamp: u64,
    author: impl Into<String>,
) -> BatchId {
    let author = author.into();
    let provenance = if parents.is_empty() {
        RowProvenance::for_insert(author.clone(), timestamp)
    } else {
        RowProvenance {
            created_by: author.clone(),
            created_at: 1_000,
            updated_by: author.clone(),
            updated_at: timestamp,
        }
    };
    let row = StoredRowBatch::new(
        object_id,
        branch,
        parents,
        content,
        provenance,
        Default::default(),
        RowState::VisibleDirect,
        None,
    );
    let batch_id = row.batch_id();
    apply_test_row_batch(storage, object_id, branch, row).unwrap();
    batch_id
}

fn test_row_metadata(storage: &MemoryStorage, row_id: ObjectId) -> Option<HashMap<String, String>> {
    load_test_row_metadata(storage, row_id)
}

fn test_row_tip_ids(
    storage: &MemoryStorage,
    row_id: ObjectId,
    branch: impl AsRef<str>,
) -> Result<Vec<BatchId>, crate::storage::StorageError> {
    load_test_row_tip_ids(storage, row_id, branch.as_ref())
}

/// Schema for ReBAC tests: documents with owner_id policy + folders for INHERITS
fn rebac_test_schema() -> Schema {
    let folders_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session("user_id")));
        p.allow_insert()
            .where_(pe::eq("owner_id", pe::session("user_id")));
    });

    let docs_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session("user_id")));
        p.allow_insert()
            .where_(pe::eq("owner_id", pe::session("user_id")));
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(folders_policies),
        )
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(docs_policies),
        )
        .build()
}

fn magic_introspection_schema() -> Schema {
    let is_admin = pe::eq("user_id", pe::session("user_id"));
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_update()
            .where_old(pe::exists(pe::table("admins").where_(is_admin.clone())))
            .where_new(pe::always());
        p.allow_delete().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("protected")
                .column("data", ColumnType::Text)
                .policies(protected_policies),
        )
        .build()
}

fn provenance_notes_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("notes").column("title", ColumnType::Text))
        .build()
}

fn authorship_permissions_schema() -> Schema {
    let created_by_is_session = pe::eq("$createdBy", pe::session("user_id"));
    let notes_policies = permissions(|p| {
        p.allow_read().where_(created_by_is_session.clone());
        p.allow_insert().where_(created_by_is_session.clone());
        p.allow_update().where_(created_by_is_session.clone());
        p.allow_delete().where_(created_by_is_session);
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .policies(notes_policies),
        )
        .build()
}

fn query_rows(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    let sub_id = qm
        .subscribe_with_session(query, session, None)
        .expect("query subscription should be created");

    for _ in 0..10 {
        qm.process(storage);
    }

    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);
    results
}

fn recursive_folders_schema(max_depth: Option<usize>) -> Schema {
    let select_inherited = match max_depth {
        Some(max_depth) => pe::allowed_to_read_with_depth("parent_id", max_depth),
        None => pe::allowed_to_read("parent_id"),
    };
    let update_inherited = match max_depth {
        Some(max_depth) => pe::allowed_to_update_with_depth("parent_id", max_depth),
        None => pe::allowed_to_update("parent_id"),
    };

    let folders_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session("user_id")),
            select_inherited,
        ]));
        p.allow_update()
            .where_old(pe::any_of([
                pe::eq("owner_id", pe::session("user_id")),
                update_inherited,
            ]))
            .where_new(pe::always());
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folders_policies),
        )
        .build()
}

/// Helper to encode a document row
fn encode_document(owner_id: &str, title: &str, folder_id: Option<ObjectId>) -> Vec<u8> {
    let docs_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid).nullable(),
    ]);
    encode_row(
        &docs_desc,
        &[
            Value::Text(owner_id.into()),
            Value::Text(title.into()),
            match folder_id {
                Some(id) => Value::Uuid(id),
                None => Value::Null,
            },
        ],
    )
    .unwrap()
}

fn encode_folder(owner_id: &str, name: &str) -> Vec<u8> {
    let folders_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    encode_row(
        &folders_desc,
        &[Value::Text(owner_id.into()), Value::Text(name.into())],
    )
    .unwrap()
}

/// Helper to create a document metadata map
fn document_metadata() -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    m.insert(MetadataKey::Table.to_string(), "documents".to_string());
    m
}

fn folder_metadata() -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    m.insert(MetadataKey::Table.to_string(), "folders".to_string());
    m
}

fn inherited_insert_schema() -> (Schema, RowDescriptor, SchemaHash) {
    let folders_table = TableSchema::builder("folders")
        .column("owner_id", ColumnType::Text)
        .column("name", ColumnType::Text);
    let folders_descriptor = folders_table.clone().build().columns;
    let folders_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session("user_id")));
    });

    let documents_policies = permissions(|p| {
        p.allow_insert().where_(pe::all_of([
            pe::eq("owner_id", pe::session("user_id")),
            pe::any_of([pe::is_null("folder_id"), pe::allowed_to_read("folder_id")]),
        ]));
    });

    let schema = SchemaBuilder::new()
        .table(folders_table.policies(folders_policies))
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(documents_policies),
        )
        .build();

    let schema_hash = SchemaHash::compute(&schema);
    (schema, folders_descriptor, schema_hash)
}

fn inherited_insert_branch(schema_hash: SchemaHash) -> String {
    ComposedBranchName::new("dev", schema_hash, "client-alice-main")
        .to_branch_name()
        .as_str()
        .to_string()
}

fn create_server_mode_query_manager(schema: Schema, schema_hash: SchemaHash) -> QueryManager {
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    qm.schema = Arc::new(schema.clone());
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));
    qm
}

fn seed_folder_on_branch(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    branch: &str,
    owner_id: &str,
    name: &str,
    folders_descriptor: &RowDescriptor,
) -> ObjectId {
    let folder_id = create_test_row(storage, Some(folder_metadata()));
    let folder_content = encode_folder(owner_id, name);
    add_row_commit(
        storage,
        folder_id,
        branch,
        vec![],
        folder_content.clone(),
        1000,
        ObjectId::new().to_string(),
    );
    QueryManager::update_indices_for_insert_on_branch(
        storage,
        "folders",
        branch,
        folder_id,
        &folder_content,
        folders_descriptor,
        None,
    )
    .unwrap();
    qm.persist_row_region_tip(storage, "folders", folder_id, branch);
    folder_id
}

fn enqueue_inherited_insert(
    qm: &mut QueryManager,
    client_id: ClientId,
    doc_id: ObjectId,
    branch: &str,
    folder_id: ObjectId,
    title: &str,
) -> IncomingRowBatch {
    let commit = stored_row_commit(
        smallvec![],
        encode_document("alice", title, Some(folder_id)),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            doc_id,
            branch,
            Some(RowMetadata {
                id: doc_id,
                metadata: document_metadata(),
            }),
            &commit,
        ),
    });

    commit
}

/// Test that EXISTS clause in INSERT policy correctly denies writes.
///
/// Scenario: Insert policy requires EXISTS (SELECT FROM admins WHERE user_id = @session.user_id)
/// A non-admin user tries to insert - should be denied.

/// Test that UPDATE checks USING policy (can session see the old row?).
///
/// Scenario: Alice owns a document. Bob tries to update it.
/// The USING policy (owner_id = @session.user_id) should deny Bob because
/// he cannot "see" Alice's document.
///
/// CURRENT BUG: Only WITH CHECK is evaluated for UPDATE, not USING.
/// See: manager.rs:1246-1247 - "TODO: Full USING check for UPDATE"

/// Test that INHERITS in SELECT policy correctly filters rows in query results.
///
/// Scenario: Documents inherit SELECT policy from their parent folder.
/// Alice owns folder F. Bob owns document D in folder F.
/// When Alice queries documents, she should NOT see Bob's document D
/// because even though D is in her folder, INHERITS should check
/// if Alice can see D directly (which requires owner_id = alice).
///
/// Actually, let's reverse this: Alice should be able to see documents
/// in her folder via INHERITS, even if she doesn't own them directly.
///
/// Scenario revised:
/// - Folder F owned by Alice
/// - Document D in folder F, owned by Bob
/// - SELECT policy: owner_id = @user_id OR INHERITS SELECT VIA folder_id
/// - Alice should see D because she owns the folder (INHERITS passes)
/// - Charlie (owns neither) should NOT see D
///
/// FIXED: PolicyFilterNode now properly evaluates INHERITS using PolicyGraph.

/// Test that EXISTS clause in UPDATE USING policy correctly denies updates.
///
/// Scenario: UPDATE policy has USING = EXISTS (only admins can update protected rows)
/// - Alice is an admin, Bob is not
/// - Both try to update a protected row
/// - Bob should be denied (USING EXISTS fails), Alice should be allowed

// ============================================================================
// INHERITS Cycle Detection Tests
// ============================================================================

/// Test that INHERITS cycles are detected during schema validation.
/// Cycle: A → B → A (direct cycle between two tables)

/// Test that self-referential INHERITS is detected as a cycle.
/// Cycle: Folder → Folder (self-reference via parent_id)

/// Test that valid INHERITS chains (no cycles) pass validation.

/// Test that bounded self-referential INHERITS is accepted by cycle validation.

fn declared_file_inheritance_schema(array_edge: bool) -> Schema {
    let source_fk_column = if array_edge { "images" } else { "image" };
    let files_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session("user_id")),
            pe::allowed_to_read_referencing("todos", source_fk_column),
        ]));
        p.allow_update()
            .where_old(pe::any_of([
                pe::eq("owner_id", pe::session("user_id")),
                pe::allowed_to_update_referencing("todos", source_fk_column),
            ]))
            .where_new(pe::always());
    });

    let todos_table = if array_edge {
        TableSchema::builder("todos")
            .column("owner_id", ColumnType::Text)
            .column("title", ColumnType::Text)
            .array_fk_column("images", "files")
    } else {
        TableSchema::builder("todos")
            .column("owner_id", ColumnType::Text)
            .column("title", ColumnType::Text)
            .nullable_fk_column("image", "files")
    };
    let todos_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session("user_id")));
        p.allow_update()
            .where_old(pe::eq("owner_id", pe::session("user_id")))
            .where_new(pe::always());
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("files")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(files_policies),
        )
        .table(todos_table.policies(todos_policies))
        .build()
}

mod declared_fk_inheritance;
mod exists_policies;
mod exists_rel_policies;
mod inheritance_validation;
mod inherited_policies;
mod insert_policies;
mod magic_provenance;
mod mutations;
#[cfg(feature = "client")]
mod recursive_inheritance;
#[cfg(feature = "client")]
mod select_policies;
