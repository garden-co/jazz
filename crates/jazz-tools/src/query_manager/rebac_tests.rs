//! ReBAC Policy Evaluation Integration Tests
//!
//! Tests for the async permission evaluation system using policy graphs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smallvec::smallvec;

use crate::commit::Commit;
use crate::metadata::MetadataKey;
use crate::object::ObjectId;
use crate::storage::MemoryStorage;
use crate::sync_manager::{
    ClientId, Destination, InboxEntry, ObjectMetadata, QueryId, Source, SyncError, SyncManager,
    SyncPayload,
};

use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::manager::QueryError;
use crate::query_manager::manager::QueryManager;
use crate::query_manager::policy::Operation;
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::relation_ir::{
    ColumnRef, PredicateCmpOp, PredicateExpr, RelExpr, ValueRef,
};
use crate::query_manager::session::Session;
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, Schema, SchemaHash, TableName,
    TablePolicies, TableSchema, Value,
};

/// Helper to create QueryManager with schema on default branch.
fn create_query_manager(sync_manager: SyncManager, schema: Schema) -> QueryManager {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema, "dev", "main");
    qm
}

/// Get the schema context's branch name.
fn get_branch(qm: &QueryManager) -> String {
    qm.schema_context().branch_name().as_str().to_string()
}

/// Schema for ReBAC tests: documents with owner_id policy + folders for INHERITS
fn rebac_test_schema() -> Schema {
    let mut schema = Schema::new();

    // Folders table (parent for documents)
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let folders_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor, folders_policies),
    );

    // Documents table with owner_id policy
    let docs_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let docs_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(docs_descriptor, docs_policies),
    );

    schema
}

fn magic_introspection_schema() -> Schema {
    let mut schema = Schema::new();

    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(admins_descriptor),
    );

    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new()
        .with_update(
            Some(PolicyExpr::Exists {
                table: "admins".into(),
                condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
            }),
            PolicyExpr::True,
        )
        .with_delete(PolicyExpr::ExistsRel {
            rel: RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("admins"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
            },
        });
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor, protected_policies),
    );

    schema
}

fn recursive_folders_schema(max_depth: Option<usize>) -> Schema {
    let mut schema = Schema::new();

    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);

    let select_policy = PolicyExpr::Or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column: "parent_id".into(),
            max_depth,
        },
    ]);

    let update_using = PolicyExpr::Or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "parent_id".into(),
            max_depth,
        },
    ]);

    let folders_policies = TablePolicies::new()
        .with_select(select_policy)
        .with_update(Some(update_using), PolicyExpr::True);

    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor, folders_policies),
    );

    schema
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

/// Helper to create a document metadata map
fn document_metadata() -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    m.insert(MetadataKey::Table.to_string(), "documents".to_string());
    m
}

fn run_recursive_folder_update(max_depth: Option<usize>) -> (bool, bool) {
    let schema = recursive_folders_schema(max_depth);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let root_handle = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap();
    let child_handle = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root_handle.row_id),
            ],
        )
        .unwrap();
    let grand_handle = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Grandchild".into()),
                Value::Uuid(child_handle.row_id),
            ],
        )
        .unwrap();

    let grand_id = grand_handle.row_id;
    let branch = get_branch(&qm);

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let mut scope = HashSet::new();
    scope.insert((grand_id, branch.clone().into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(100), scope, None);
    qm.sync_manager_mut().take_outbox();

    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);

    let update_content = encode_row(
        &folders_descriptor,
        &[
            Value::Text("bob".into()),
            Value::Text("Renamed by Alice".into()),
            Value::Uuid(child_handle.row_id),
        ],
    )
    .unwrap();

    let update_commit = Commit {
        parents: smallvec![grand_handle.row_commit_id],
        content: update_content,
        timestamp: 4200,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    let object_metadata = qm
        .sync_manager()
        .object_manager
        .get(grand_id)
        .map(|obj| obj.metadata.clone())
        .unwrap_or_default();

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: grand_id,
            metadata: Some(ObjectMetadata {
                id: grand_id,
                metadata: object_metadata,
            }),
            branch_name: branch.clone().into(),
            commits: vec![update_commit.clone()],
        },
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = outbox.iter().any(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (Destination::Client(id), SyncPayload::Error(SyncError::PermissionDenied { .. }))
                if *id == client_id
        )
    });

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(grand_id, &branch)
        .unwrap();
    let applied = tips.contains(&update_commit.id());

    (denied, applied)
}

#[test]
fn rebac_insert_allowed_by_simple_policy() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(document_metadata()));

    // Register a query scope so the update is in-scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content: owner_id = "alice", title = "My Doc", folder_id = NULL
    let content = encode_document("alice", "My Doc", None);

    // Client sends insert
    let commit = Commit {
        parents: smallvec![],
        content,
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: document_metadata(),
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    // Process - should evaluate policy and approve
    qm.process(&mut storage);

    // Commit should be applied (owner matches session user)
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main")
        .unwrap();
    assert!(
        tips.contains(&commit.id()),
        "Insert should be approved when owner matches session"
    );
}

#[test]
fn rebac_insert_denied_by_simple_policy() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(document_metadata()));

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content: owner_id = "bob" (different from session user)
    let content = encode_document("bob", "Stolen Doc", None);

    // Client sends insert
    let commit = Commit {
        parents: smallvec![],
        content,
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: document_metadata(),
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    // Process - should evaluate policy and reject
    qm.process(&mut storage);

    // Should get permission denied error
    let outbox = qm.sync_manager_mut().take_outbox();
    let error = outbox
        .iter()
        .find(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .expect("Should receive error response");

    match &error.payload {
        SyncPayload::Error(SyncError::PermissionDenied { reason, .. }) => {
            assert!(
                reason.contains("denied by policy"),
                "Error should mention policy denial: {reason}"
            );
        }
        _ => panic!("Expected PermissionDenied error"),
    }

    // Commit should NOT be applied
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main");
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Insert should be denied when owner doesn't match session"
    );
}

#[test]
fn rebac_insert_denied_by_current_permissions_in_server_mode_known_schema() {
    let authorization_schema = rebac_test_schema();
    let schema: Schema = authorization_schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    // Server mode: the branch schema has no embedded policies, but the server should still
    // enforce the latest authorization schema.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));
    qm.set_authorization_schema(authorization_schema);

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let metadata = document_metadata();
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(metadata.clone()));

    let mut scope = HashSet::new();
    scope.insert((obj_id, branch.clone().into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    let commit = Commit {
        parents: smallvec![],
        content: encode_document("bob", "Should Be Denied", None),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata,
            }),
            branch_name: branch.clone().into(),
            commits: vec![commit.clone()],
        },
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = outbox.iter().any(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::PermissionDenied { .. }),
            ) if *id == client_id
        )
    });
    assert!(
        denied,
        "Insert should be denied by current permissions in server mode"
    );

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, &branch);
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Denied insert should not be applied on the branch"
    );
}

#[test]
fn rebac_insert_denied_for_new_object_uses_payload_metadata_in_server_mode() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    // Server mode: no current schema, schema available via known_schemas.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // New row object: metadata exists only in payload, not in ObjectManager.
    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = Commit {
        parents: smallvec![],
        content: encode_document("bob", "Should Be Denied", None),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata,
            }),
            branch_name: branch.clone().into(),
            commits: vec![commit.clone()],
        },
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = outbox.iter().any(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::PermissionDenied { .. }),
            ) if *id == client_id
        )
    });
    assert!(
        denied,
        "Insert should be denied for new objects using payload metadata in server mode"
    );

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, &branch);
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Denied insert should not be applied on the branch"
    );
}

#[test]
fn rebac_insert_waits_for_schema_then_denies_for_composed_branch() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    // Server mode starts without a fixed current schema and may learn schemas later.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = Commit {
        parents: smallvec![],
        content: encode_document("bob", "Should Be Denied", None),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: metadata.clone(),
            }),
            branch_name: branch.clone().into(),
            commits: vec![commit.clone()],
        },
    });

    // First pass should defer until the schema becomes available instead of allowing or denying.
    qm.process(&mut storage);

    assert!(
        qm.sync_manager_mut().take_outbox().is_empty(),
        "Composed-branch writes should wait for schema activation before emitting a result"
    );

    let pending = qm.sync_manager_mut().take_pending_permission_checks();
    assert_eq!(
        pending.len(),
        1,
        "Write should remain pending until the matching schema arrives"
    );
    qm.sync_manager_mut()
        .requeue_pending_permission_checks(pending);

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, &branch);
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Deferred insert must not be applied before the schema is known"
    );

    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = outbox.iter().any(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::PermissionDenied { .. }),
            ) if *id == client_id
        )
    });
    assert!(
        denied,
        "Once the schema is available, the deferred insert should be denied by policy"
    );
}

#[test]
fn rebac_insert_denied_when_schema_never_arrives_before_timeout() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = Commit {
        parents: smallvec![],
        content: encode_document("bob", "Should Time Out", None),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: metadata.clone(),
            }),
            branch_name: branch.clone().into(),
            commits: vec![commit.clone()],
        },
    });

    qm.process(&mut storage);

    assert!(
        qm.sync_manager_mut().take_outbox().is_empty(),
        "First pass should defer while waiting for schema activation"
    );

    let mut pending = qm.sync_manager_mut().take_pending_permission_checks();
    assert_eq!(pending.len(), 1, "Deferred write should remain pending");
    pending[0].schema_wait_started_at = Some(Instant::now() - Duration::from_secs(11));
    qm.sync_manager_mut()
        .requeue_pending_permission_checks(pending);

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let error = outbox
        .iter()
        .find(|entry| matches!(entry.destination, Destination::Client(id) if id == client_id))
        .expect("Timed-out schema wait should return an error to the client");

    match &error.payload {
        SyncPayload::Error(SyncError::PermissionDenied { reason, .. }) => {
            assert!(
                reason.contains("after waiting 10s"),
                "Timed-out schema wait should mention the 10s timeout: {reason}"
            );
        }
        other => panic!("Expected PermissionDenied error, got {:?}", other),
    }

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, &branch);
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Timed-out insert should not be applied on the branch"
    );
}

#[test]
fn rebac_insert_denied_when_schema_unresolved_for_branch() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);

    // Server mode: no current schema, only known_schemas.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = Commit {
        parents: smallvec![],
        content: encode_document("bob", "Should Be Denied", None),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    // Plain "main" branch without schema hash context can fail schema resolution.
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata,
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = outbox.iter().any(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::PermissionDenied { .. }),
            ) if *id == client_id
        )
    });
    assert!(
        denied,
        "Insert should be denied when schema cannot be resolved for the write branch"
    );

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main");
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Denied insert should not be applied on unresolved branch writes"
    );
}

#[test]
fn rebac_insert_denied_when_stale_self_schema_would_otherwise_allow() {
    let restrictive = rebac_test_schema();
    let restrictive_hash = SchemaHash::compute(&restrictive);

    // Permissive local schema (no insert policy) that should NOT be used for server writes
    // on unrelated branches.
    let mut permissive = Schema::new();
    permissive.insert(
        TableName::new("documents"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("folder_id", ColumnType::Uuid).nullable(),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, permissive);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(restrictive_hash, restrictive);
    qm.set_known_schemas(Arc::new(known_schemas));

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let metadata = document_metadata();
    let commit = Commit {
        parents: smallvec![],
        content: encode_document("bob", "Should Be Denied", None),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    // Simulate write on an unresolved branch. Prior behavior could fall back to stale
    // self.schema (permissive) and incorrectly allow this insert.
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata,
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = outbox.iter().any(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::PermissionDenied { .. }),
            ) if *id == client_id
        )
    });
    assert!(
        denied,
        "Insert should be denied instead of using stale self.schema on unresolved branches"
    );

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main");
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Denied insert should not be applied when stale self.schema fallback is unsafe"
    );
}

#[test]
fn rebac_table_without_policy_allows_all_writes() {
    // Schema with no policies
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("notes"),
        RowDescriptor::new(vec![ColumnDescriptor::new("content", ColumnType::Text)]).into(),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "notes".to_string());
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(metadata.clone()));

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content
    let notes_desc = RowDescriptor::new(vec![ColumnDescriptor::new("content", ColumnType::Text)]);
    let content = encode_row(&notes_desc, &[Value::Text("A note".into())]).unwrap();

    // Client sends insert
    let commit = Commit {
        parents: smallvec![],
        content,
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata,
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    // Process - table without policy should allow
    qm.process(&mut storage);

    // Commit should be applied
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main")
        .unwrap();
    assert!(
        tips.contains(&commit.id()),
        "Table without policy should allow all writes"
    );
}

#[test]
fn rebac_non_row_object_allowed() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object WITHOUT table metadata (not a row)
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, None);

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Client sends update
    let commit = Commit {
        parents: smallvec![],
        content: b"some data".to_vec(),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None, // No metadata = not a row
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    // Process - non-row objects should be allowed
    qm.process(&mut storage);

    // Commit should be applied
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main")
        .unwrap();
    assert!(
        tips.contains(&commit.id()),
        "Non-row objects should be allowed without policy check"
    );
}

#[test]
fn rebac_non_row_object_allowed_in_server_mode() {
    let schema = rebac_test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    // Server mode: schema is available through known_schemas only.
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));

    let mut storage = MemoryStorage::new();

    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Non-row object: no table metadata.
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, None);

    let mut scope = HashSet::new();
    scope.insert((obj_id, branch.clone().into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    let commit = Commit {
        parents: smallvec![],
        content: b"some data".to_vec(),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: branch.clone().into(),
            commits: vec![commit.clone()],
        },
    });

    qm.process(&mut storage);

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, &branch)
        .unwrap();
    assert!(
        tips.contains(&commit.id()),
        "Non-row objects should remain writable in server mode"
    );
}

#[test]
fn rebac_two_clients_different_sessions() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Client 1: alice
    let client1 = ClientId::new();
    qm.sync_manager_mut().add_client(client1);
    qm.sync_manager_mut()
        .set_client_session(client1, Session::new("alice"));

    // Client 2: bob
    let client2 = ClientId::new();
    qm.sync_manager_mut().add_client(client2);
    qm.sync_manager_mut()
        .set_client_session(client2, Session::new("bob"));

    // Create objects for both clients
    let obj1 = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(document_metadata()));
    let obj2 = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(document_metadata()));

    // Register query scopes
    let mut scope1 = HashSet::new();
    scope1.insert((obj1, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client1, QueryId(1), scope1, None);

    let mut scope2 = HashSet::new();
    scope2.insert((obj2, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client2, QueryId(2), scope2, None);

    qm.sync_manager_mut().take_outbox();

    // Alice's document
    let content1 = encode_document("alice", "Alice's Doc", None);
    let commit1 = Commit {
        parents: smallvec![],
        content: content1,
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    // Bob's document
    let content2 = encode_document("bob", "Bob's Doc", None);
    let commit2 = Commit {
        parents: smallvec![],
        content: content2,
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    // Both clients send their documents
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client1),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj1,
            metadata: Some(ObjectMetadata {
                id: obj1,
                metadata: document_metadata(),
            }),
            branch_name: "main".into(),
            commits: vec![commit1.clone()],
        },
    });

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client2),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj2,
            metadata: Some(ObjectMetadata {
                id: obj2,
                metadata: document_metadata(),
            }),
            branch_name: "main".into(),
            commits: vec![commit2.clone()],
        },
    });

    // Process
    qm.process(&mut storage);

    // Both commits should be applied (each owner matches their session)
    let tips1 = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj1, "main")
        .unwrap();
    assert!(
        tips1.contains(&commit1.id()),
        "Alice's document should be approved"
    );

    let tips2 = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj2, "main")
        .unwrap();
    assert!(
        tips2.contains(&commit2.id()),
        "Bob's document should be approved"
    );
}

/// Test that EXISTS clause in INSERT policy correctly denies writes.
///
/// Scenario: Insert policy requires EXISTS (SELECT FROM admins WHERE user_id = @session.user_id)
/// A non-admin user tries to insert - should be denied.
#[test]
fn rebac_exists_clause_denies_non_matching_insert() {
    // Schema with EXISTS policy: only admins can insert
    let mut schema = Schema::new();

    // Admins table
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(admins_descriptor),
    );

    // Protected table: only admins can insert
    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_insert(PolicyExpr::Exists {
        table: "admins".into(),
        condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
    });
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor, protected_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Add a client with session for non-admin user
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("regular_user"));

    // Note: We do NOT add "regular_user" to admins table

    // Create object for protected row
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "protected".to_string());
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(metadata.clone()));

    // Register query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content
    let protected_desc = RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let content = encode_row(&protected_desc, &[Value::Text("secret data".into())]).unwrap();

    // Non-admin tries to insert
    let commit = Commit {
        parents: smallvec![],
        content,
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata,
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    // Process
    qm.process(&mut storage);

    // Should get permission denied (non-admin cannot insert)
    let outbox = qm.sync_manager_mut().take_outbox();
    let error = outbox
        .iter()
        .find(|e| matches!(e.destination, Destination::Client(id) if id == client_id));

    assert!(
        error.is_some(),
        "Non-admin insert should be denied by EXISTS policy"
    );

    match &error.unwrap().payload {
        SyncPayload::Error(SyncError::PermissionDenied { .. }) => {
            // Expected
        }
        other => panic!("Expected PermissionDenied error, got {:?}", other),
    }

    // Commit should NOT be applied to the branch.
    assert!(
        qm.sync_manager_mut().object_manager.get(obj_id).is_some(),
        "Object should still exist after denied insert"
    );
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main");
    assert!(
        tips.is_err(),
        "Denied insert should not create tips on branch main"
    );
}

/// Test that UPDATE checks USING policy (can session see the old row?).
///
/// Scenario: Alice owns a document. Bob tries to update it.
/// The USING policy (owner_id = @session.user_id) should deny Bob because
/// he cannot "see" Alice's document.
///
/// CURRENT BUG: Only WITH CHECK is evaluated for UPDATE, not USING.
/// See: manager.rs:1246-1247 - "TODO: Full USING check for UPDATE"
#[test]
fn rebac_update_denied_by_using_policy() {
    // Schema with both USING and WITH CHECK for updates
    let mut schema = Schema::new();

    let docs_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("content", ColumnType::Text),
    ]);

    // UPDATE policy: USING (owner_id = @user_id) WITH CHECK (owner_id = @user_id)
    // This means: you can only update rows you own, and the result must still be owned by you
    let docs_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_update(
            Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])), // USING
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),       // WITH CHECK
        );

    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(docs_descriptor.clone(), docs_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Create Alice's document first (as server/no session)
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "documents".to_string());
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(metadata.clone()));

    let alice_content = encode_row(
        &docs_descriptor,
        &[
            Value::Text("alice".into()),
            Value::Text("Alice's secret".into()),
        ],
    )
    .unwrap();
    let author = ObjectId::new();
    let initial_commit = qm
        .sync_manager_mut()
        .object_manager
        .add_commit(
            &mut storage,
            obj_id,
            "main",
            vec![],
            alice_content,
            author,
            None,
        )
        .unwrap();

    // Now Bob connects and tries to update Alice's document
    let bob_client = ClientId::new();
    qm.sync_manager_mut().add_client(bob_client);
    qm.sync_manager_mut()
        .set_client_session(bob_client, Session::new("bob"));

    // Register query scope for Bob
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .set_client_query_scope(bob_client, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Bob tries to update Alice's document (keeping owner as alice to pass WITH CHECK,
    // but USING should still deny because Bob can't see Alice's row)
    let bob_update_content = encode_row(
        &docs_descriptor,
        &[
            Value::Text("alice".into()),
            Value::Text("Hacked by Bob".into()),
        ],
    )
    .unwrap();

    let update_commit = Commit {
        parents: smallvec![initial_commit],
        content: bob_update_content,
        timestamp: 2000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(bob_client),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata,
            }),
            branch_name: "main".into(),
            commits: vec![update_commit.clone()],
        },
    });

    // Process
    qm.process(&mut storage);

    // Should get permission denied (Bob cannot see Alice's row via USING)
    let outbox = qm.sync_manager_mut().take_outbox();
    let error = outbox
        .iter()
        .find(|e| matches!(e.destination, Destination::Client(id) if id == bob_client));

    assert!(
        error.is_some(),
        "Bob's update of Alice's document should be denied by USING policy"
    );

    match &error.unwrap().payload {
        SyncPayload::Error(SyncError::PermissionDenied { .. }) => {
            // Expected
        }
        other => panic!("Expected PermissionDenied error, got {:?}", other),
    }

    // Update should NOT be applied
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main")
        .unwrap();
    assert!(
        !tips.contains(&update_commit.id()),
        "Bob's update should be denied - he cannot see Alice's document"
    );
}

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
#[test]
fn rebac_inherits_filters_select_query_results() {
    use crate::query_manager::query::QueryBuilder;

    // Schema with INHERITS policy
    let mut schema = Schema::new();

    // Folders table
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let folders_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor.clone(), folders_policies),
    );

    // Documents table with INHERITS
    let docs_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);

    // SELECT policy: owner_id = @user_id OR INHERITS SELECT VIA folder_id
    let docs_policies = TablePolicies::new().with_select(PolicyExpr::Or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column: "folder_id".into(),
            max_depth: None,
        },
    ]));
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(docs_descriptor.clone(), docs_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Create Alice's folder
    let mut folder_meta = std::collections::HashMap::new();
    folder_meta.insert(MetadataKey::Table.to_string(), "folders".to_string());
    let folder_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(folder_meta));

    let folder_content = encode_row(
        &folders_descriptor,
        &[
            Value::Text("alice".into()),
            Value::Text("Alice's Folder".into()),
        ],
    )
    .unwrap();
    let author = ObjectId::new();
    qm.sync_manager_mut()
        .object_manager
        .add_commit(
            &mut storage,
            folder_id,
            "main",
            vec![],
            folder_content,
            author,
            None,
        )
        .unwrap();

    // Create Bob's document in Alice's folder
    let mut doc_meta = std::collections::HashMap::new();
    doc_meta.insert(MetadataKey::Table.to_string(), "documents".to_string());
    let doc_id = qm
        .sync_manager_mut()
        .object_manager
        .create(&mut storage, Some(doc_meta));

    let doc_content = encode_row(
        &docs_descriptor,
        &[
            Value::Text("bob".into()),
            Value::Text("Bob's Doc in Alice's Folder".into()),
            Value::Uuid(folder_id),
        ],
    )
    .unwrap();
    qm.sync_manager_mut()
        .object_manager
        .add_commit(
            &mut storage,
            doc_id,
            "main",
            vec![],
            doc_content,
            author,
            None,
        )
        .unwrap();

    // Charlie subscribes to documents query with his session
    let charlie_session = Session::new("charlie");
    let query = QueryBuilder::new("documents").branch("main").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(charlie_session), None)
        .unwrap();

    // Process to settle the query
    for _ in 0..10 {
        qm.process(&mut storage);
    }

    // Get Charlie's query results via take_updates
    let updates = qm.take_updates();
    let charlie_update = updates.iter().find(|u| u.subscription_id == sub_id);

    // Charlie should NOT see Bob's document (doesn't own it, doesn't own folder)
    // The update should either be missing or have an empty added set
    let has_rows = charlie_update
        .map(|u| !u.delta.added.is_empty())
        .unwrap_or(false);

    assert!(
        !has_rows,
        "Charlie should not see Bob's document - he owns neither the doc nor the folder. \
         INHERITS should have denied access, but currently it always returns true."
    );
}

#[test]
fn rebac_recursive_inherits_allows_ancestor_access() {
    use crate::query_manager::query::QueryBuilder;

    let schema = recursive_folders_schema(None);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let root = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let child = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root),
            ],
        )
        .unwrap()
        .row_id;
    let grand = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("carol".into()),
                Value::Text("Grandchild".into()),
                Value::Uuid(child),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("folders").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let result_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible via recursive INHERITS"
    );
    assert!(
        result_ids.contains(&grand),
        "Grandchild should be visible via recursive INHERITS"
    );
}

#[test]
fn rebac_recursive_inherits_respects_depth_override() {
    use crate::query_manager::query::QueryBuilder;

    let schema = recursive_folders_schema(Some(1));
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let root = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let child = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root),
            ],
        )
        .unwrap()
        .row_id;
    let grand = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("carol".into()),
                Value::Text("Grandchild".into()),
                Value::Uuid(child),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("folders").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let result_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible at depth=1"
    );
    assert!(
        !result_ids.contains(&grand),
        "Grandchild should be hidden when max_depth=1"
    );
}

#[test]
fn rebac_recursive_inherits_write_checks_allow_and_deny() {
    let (denied_shallow, applied_shallow) = run_recursive_folder_update(Some(1));
    assert!(
        denied_shallow,
        "Update should be denied when recursive INHERITS max depth is too shallow"
    );
    assert!(
        !applied_shallow,
        "Denied update must not be applied to the row"
    );

    let (denied_deep, applied_deep) = run_recursive_folder_update(Some(2));
    assert!(
        !denied_deep,
        "Update should be allowed when max depth reaches the ancestor owner"
    );
    assert!(applied_deep, "Allowed update should be applied");
}

#[test]
fn rebac_recursive_inherits_cycle_does_not_overgrant() {
    use crate::query_manager::query::QueryBuilder;

    let schema = recursive_folders_schema(Some(10));
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let a = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("A".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let b = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("carol".into()),
                Value::Text("B".into()),
                Value::Uuid(a),
            ],
        )
        .unwrap()
        .row_id;

    // Close the cycle: A.parent_id = B
    let _ = qm
        .update(
            &mut storage,
            a,
            &[
                Value::Text("bob".into()),
                Value::Text("A".into()),
                Value::Uuid(b),
            ],
        )
        .unwrap();

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("folders").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let result_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(
        result_ids.is_empty(),
        "Cycle should not grant access when no ancestor is owned by session user"
    );
}

/// Test that EXISTS clause in UPDATE USING policy correctly denies updates.
///
/// Scenario: UPDATE policy has USING = EXISTS (only admins can update protected rows)
/// - Alice is an admin, Bob is not
/// - Both try to update a protected row
/// - Bob should be denied (USING EXISTS fails), Alice should be allowed
#[test]
fn rebac_update_denied_by_using_exists_policy() {
    // Schema with EXISTS policy: only admins can update
    let mut schema = Schema::new();

    // Admins table
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(admins_descriptor.clone()),
    );

    // Protected table: only admins can update (via EXISTS in USING)
    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_update(
        // USING: EXISTS (SELECT FROM admins WHERE user_id = @session.user_id)
        Some(PolicyExpr::Exists {
            table: "admins".into(),
            condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
        }),
        // WITH CHECK: no restriction on new row
        PolicyExpr::True,
    );
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor.clone(), protected_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    // Add Alice as admin (using insert to properly index the row)
    let _alice_admin = qm
        .insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .unwrap();

    // Create a protected row (as server, no session) - also using insert for proper indexing
    let protected_handle = qm
        .insert(
            &mut storage,
            "protected",
            &[Value::Text("original data".into())],
        )
        .unwrap();
    let protected_obj = protected_handle.row_id;
    let initial_commit = protected_handle.row_commit_id;

    // Get object metadata for later use in update payloads
    let protected_metadata = qm
        .sync_manager()
        .object_manager
        .get(protected_obj)
        .map(|obj| obj.metadata.clone())
        .unwrap_or_default();

    // ---- Bob (non-admin) tries to update ----
    let branch = get_branch(&qm);
    let bob_client = ClientId::new();
    qm.sync_manager_mut().add_client(bob_client);
    qm.sync_manager_mut()
        .set_client_session(bob_client, Session::new("bob"));

    // Register query scope for Bob
    let mut bob_scope = HashSet::new();
    bob_scope.insert((protected_obj, branch.clone().into()));
    qm.sync_manager_mut()
        .set_client_query_scope(bob_client, QueryId(1), bob_scope, None);
    qm.sync_manager_mut().take_outbox();

    // Bob tries to update the protected row
    let bob_update_content = encode_row(
        &protected_descriptor,
        &[Value::Text("hacked by bob".into())],
    )
    .unwrap();
    let bob_commit = Commit {
        parents: smallvec![initial_commit],
        content: bob_update_content,
        timestamp: 2000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(bob_client),
        payload: SyncPayload::ObjectUpdated {
            object_id: protected_obj,
            metadata: Some(ObjectMetadata {
                id: protected_obj,
                metadata: protected_metadata.clone(),
            }),
            branch_name: branch.clone().into(),
            commits: vec![bob_commit.clone()],
        },
    });

    // Process - may need multiple iterations for EXISTS to settle
    for _ in 0..10 {
        qm.process(&mut storage);
    }

    // Bob should get permission denied
    let outbox = qm.sync_manager_mut().take_outbox();
    let bob_error = outbox
        .iter()
        .find(|e| matches!(e.destination, Destination::Client(id) if id == bob_client));

    assert!(
        bob_error.is_some(),
        "Bob's update should be denied by EXISTS in USING policy"
    );
    match &bob_error.unwrap().payload {
        SyncPayload::Error(SyncError::PermissionDenied { .. }) => {
            // Expected
        }
        other => panic!("Expected PermissionDenied error for Bob, got {:?}", other),
    }

    // Bob's update should NOT be applied
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(protected_obj, &branch)
        .unwrap();
    assert!(
        !tips.contains(&bob_commit.id()),
        "Bob's update should not be applied - he is not an admin"
    );

    // ---- Alice (admin) tries to update ----
    let alice_client = ClientId::new();
    qm.sync_manager_mut().add_client(alice_client);
    qm.sync_manager_mut()
        .set_client_session(alice_client, Session::new("alice"));

    // Register query scope for Alice
    let mut alice_scope = HashSet::new();
    alice_scope.insert((protected_obj, branch.clone().into()));
    qm.sync_manager_mut()
        .set_client_query_scope(alice_client, QueryId(2), alice_scope, None);
    qm.sync_manager_mut().take_outbox();

    // Alice tries to update the protected row
    let alice_update_content = encode_row(
        &protected_descriptor,
        &[Value::Text("updated by admin alice".into())],
    )
    .unwrap();
    let alice_commit = Commit {
        parents: smallvec![initial_commit],
        content: alice_update_content,
        timestamp: 3000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(alice_client),
        payload: SyncPayload::ObjectUpdated {
            object_id: protected_obj,
            metadata: Some(ObjectMetadata {
                id: protected_obj,
                metadata: protected_metadata.clone(),
            }),
            branch_name: branch.clone().into(),
            commits: vec![alice_commit.clone()],
        },
    });

    // Process - may need multiple iterations for EXISTS to settle
    for _ in 0..10 {
        qm.process(&mut storage);
    }

    // Alice should NOT get permission denied
    let outbox = qm.sync_manager_mut().take_outbox();
    let alice_error = outbox.iter().find(|e| {
        matches!(
            (&e.destination, &e.payload),
            (Destination::Client(id), SyncPayload::Error(SyncError::PermissionDenied { .. })) if *id == alice_client
        )
    });

    assert!(
        alice_error.is_none(),
        "Alice's update should be allowed by EXISTS in USING policy (she is an admin)"
    );

    // Alice's update SHOULD be applied
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(protected_obj, &branch)
        .unwrap();
    assert!(
        tips.contains(&alice_commit.id()),
        "Alice's update should be applied - she is an admin"
    );
}

#[test]
fn local_insert_with_exists_rel_policy_denies_non_admin() {
    let mut schema = Schema::new();
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(admins_descriptor.clone()),
    );

    let projects_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
    let projects_policies = TablePolicies::new().with_insert(PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("admins"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::unscoped("user_id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::SessionRef(vec!["user_id".into()]),
            },
        },
    });
    schema.insert(
        TableName::new("projects"),
        TableSchema::with_policies(projects_descriptor, projects_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");

    let bob_err = qm
        .insert_with_session(
            &mut storage,
            "projects",
            &[Value::Text("bob project".into())],
            Some(&Session::new("bob")),
        )
        .expect_err("non-admin insert should be denied");
    assert!(matches!(
        bob_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert
        } if table == TableName::new("projects")
    ));

    qm.insert_with_session(
        &mut storage,
        "projects",
        &[Value::Text("alice project".into())],
        Some(&Session::new("alice")),
    )
    .expect("admin insert should be allowed");
}

#[test]
fn local_update_with_check_inherits_denies_when_parent_is_not_updateable() {
    let mut schema = Schema::new();
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folders_policies = TablePolicies::new().with_update(
        Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "parent_id".into(),
            max_depth: Some(10),
        },
    );
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor.clone(), folders_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let root = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .expect("create root");
    let child = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root.row_id),
            ],
        )
        .expect("create child");

    let update_err = qm
        .update_with_session(
            &mut storage,
            child.row_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child renamed".into()),
                Value::Uuid(root.row_id),
            ],
            Some(&Session::new("bob")),
        )
        .expect_err("update should fail inherited WITH CHECK");
    assert!(matches!(
        update_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Update
        } if table == TableName::new("folders")
    ));
}

#[test]
fn local_update_using_exists_policy_allows_admin_and_denies_non_admin() {
    let mut schema = Schema::new();
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(admins_descriptor.clone()),
    );

    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_update(
        Some(PolicyExpr::Exists {
            table: "admins".into(),
            condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
        }),
        PolicyExpr::True,
    );
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor.clone(), protected_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");
    let protected = qm
        .insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");

    let bob_err = qm
        .update_with_session(
            &mut storage,
            protected.row_id,
            &[Value::Text("bob update".into())],
            Some(&Session::new("bob")),
        )
        .expect_err("non-admin update should be denied");
    assert!(matches!(
        bob_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Update
        } if table == TableName::new("protected")
    ));

    qm.update_with_session(
        &mut storage,
        protected.row_id,
        &[Value::Text("alice update".into())],
        Some(&Session::new("alice")),
    )
    .expect("admin update should be allowed");
}

#[test]
fn local_delete_with_exists_rel_policy_allows_admin_and_denies_non_admin() {
    let mut schema = Schema::new();
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(admins_descriptor.clone()),
    );

    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_delete(PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("admins"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::unscoped("user_id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::SessionRef(vec!["user_id".into()]),
            },
        },
    });
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor.clone(), protected_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");
    let protected = qm
        .insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");

    let bob_err = qm
        .delete_with_session(&mut storage, protected.row_id, Some(&Session::new("bob")))
        .expect_err("non-admin delete should be denied");
    assert!(matches!(
        bob_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Delete
        } if table == TableName::new("protected")
    ));

    qm.delete_with_session(&mut storage, protected.row_id, Some(&Session::new("alice")))
        .expect("admin delete should be allowed");
    assert!(qm.row_is_deleted(&storage, "protected", protected.row_id));
}

#[test]
fn synced_soft_delete_should_use_delete_policy() {
    let mut schema = Schema::new();
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(admins_descriptor.clone()),
    );

    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_delete(PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("admins"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::unscoped("user_id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::SessionRef(vec!["user_id".into()]),
            },
        },
    });
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor.clone(), protected_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");
    let protected = qm
        .insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");
    let branch = get_branch(&qm);

    let protected_metadata = qm
        .sync_manager()
        .object_manager
        .get(protected.row_id)
        .map(|obj| obj.metadata.clone())
        .expect("protected row metadata");

    let bob_client = ClientId::new();
    qm.sync_manager_mut().add_client(bob_client);
    qm.sync_manager_mut()
        .set_client_session(bob_client, Session::new("bob"));

    let mut bob_scope = HashSet::new();
    bob_scope.insert((protected.row_id, branch.clone().into()));
    qm.sync_manager_mut()
        .set_client_query_scope(bob_client, QueryId(1), bob_scope, None);
    qm.sync_manager_mut().take_outbox();

    let delete_content =
        encode_row(&protected_descriptor, &[Value::Text("initial".into())]).unwrap();
    let delete_commit = Commit {
        parents: smallvec![protected.row_commit_id],
        content: delete_content,
        timestamp: 2000,
        author: ObjectId::new(),
        metadata: Some(crate::metadata::soft_delete_metadata()),
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(bob_client),
        payload: SyncPayload::ObjectUpdated {
            object_id: protected.row_id,
            metadata: Some(ObjectMetadata {
                id: protected.row_id,
                metadata: protected_metadata,
            }),
            branch_name: branch.clone().into(),
            commits: vec![delete_commit.clone()],
        },
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = outbox.iter().any(|entry| {
        matches!(
            (&entry.destination, &entry.payload),
            (Destination::Client(id), SyncPayload::Error(SyncError::PermissionDenied { .. }))
                if *id == bob_client
        )
    });
    assert!(
        denied,
        "soft deletes replicated over sync should be checked against DELETE policy"
    );

    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(protected.row_id, &branch)
        .unwrap();
    assert!(
        !tips.contains(&delete_commit.id()),
        "denied synced soft delete should not be applied"
    );
    assert!(
        !qm.row_is_deleted(&storage, "protected", protected.row_id),
        "denied synced soft delete should leave the row visible"
    );
}

#[test]
fn magic_columns_reactively_track_update_and_delete_permissions() {
    let schema = magic_introspection_schema();
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let protected = qm
        .insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");

    let query = qm
        .query("protected")
        .select(&["data", "$canRead", "$canEdit", "$canDelete"])
        .build();
    let sub_id = qm
        .subscribe_with_session(query, Some(Session::new("alice")), None)
        .expect("subscribe with session");

    qm.process(&mut storage);
    let initial_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == sub_id)
        .expect("initial magic column update");
    let initial_row = initial_update
        .delta
        .added
        .first()
        .expect("initial protected row");
    let initial_values = decode_row(&initial_update.descriptor, &initial_row.data).unwrap();
    assert_eq!(
        initial_values,
        vec![
            Value::Text("initial".into()),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
        ]
    );

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("grant alice admin");

    qm.process(&mut storage);
    let dependency_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == sub_id)
        .expect("magic column dependency update");
    let (_old_row, new_row) = dependency_update
        .delta
        .updated
        .first()
        .expect("magic columns should re-evaluate existing row");
    let updated_values = decode_row(&dependency_update.descriptor, &new_row.data).unwrap();
    assert_eq!(
        updated_values,
        vec![
            Value::Text("initial".into()),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]
    );

    qm.update_with_session(
        &mut storage,
        protected.row_id,
        &[Value::Text("updated".into())],
        Some(&Session::new("alice")),
    )
    .expect("magic $canEdit should match actual update permission");
    qm.delete_with_session(&mut storage, protected.row_id, Some(&Session::new("alice")))
        .expect("magic $canDelete should match actual delete permission");
}

#[test]
fn magic_columns_return_null_without_session_and_do_not_change_default_output_shape() {
    let schema = magic_introspection_schema();
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    qm.insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");
    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("grant alice admin");

    let projected_query = qm
        .query("protected")
        .select(&["data", "$canRead", "$canEdit", "$canDelete"])
        .build();
    let projected_sub = qm
        .subscribe_with_session(projected_query, None, None)
        .expect("subscribe without session");

    qm.process(&mut storage);
    let projected_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == projected_sub)
        .expect("initial projected update");
    let projected_row = projected_update
        .delta
        .added
        .first()
        .expect("projected protected row");
    let projected_values = decode_row(&projected_update.descriptor, &projected_row.data).unwrap();
    assert_eq!(
        projected_values,
        vec![
            Value::Text("initial".into()),
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );

    let filtered_query = qm
        .query("protected")
        .filter_eq("$canDelete", Value::Boolean(true))
        .build();
    let filtered_sub = qm
        .subscribe_with_session(filtered_query, Some(Session::new("alice")), None)
        .expect("subscribe filtered query");

    qm.process(&mut storage);
    let filtered_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == filtered_sub)
        .expect("filtered update");
    assert_eq!(filtered_update.descriptor.columns.len(), 1);
    assert_eq!(filtered_update.descriptor.columns[0].name.as_str(), "data");

    let filtered_row = filtered_update
        .delta
        .added
        .first()
        .expect("filtered protected row");
    let filtered_values = decode_row(&filtered_update.descriptor, &filtered_row.data).unwrap();
    assert_eq!(filtered_values, vec![Value::Text("initial".into())]);
}

// ============================================================================
// INHERITS Cycle Detection Tests
// ============================================================================

/// Test that INHERITS cycles are detected during schema validation.
/// Cycle: A → B → A (direct cycle between two tables)
#[test]
fn rebac_inherits_cycle_detection() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Table A references B via INHERITS
    let a_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("b_id", ColumnType::Uuid)
            .nullable()
            .references("table_b"),
    ]);
    let a_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "b_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("table_a"),
        TableSchema::with_policies(a_desc, a_policy),
    );

    // Table B references A via INHERITS (creates cycle!)
    let b_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("a_id", ColumnType::Uuid)
            .nullable()
            .references("table_a"),
    ]);
    let b_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "a_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("table_b"),
        TableSchema::with_policies(b_desc, b_policy),
    );

    // Should fail validation with cycle detected
    let result = validate_no_inherits_cycles(&schema);
    assert!(result.is_err(), "Should detect INHERITS cycle: A → B → A");
    let err = result.unwrap_err();
    assert!(
        err.contains("cycle"),
        "Error message should mention cycle: {}",
        err
    );
}

/// Test that self-referential INHERITS is detected as a cycle.
/// Cycle: Folder → Folder (self-reference via parent_id)
#[test]
fn rebac_inherits_self_reference_detection() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Folder table with parent_id referencing itself
    let folder_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folder_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "parent_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folder_desc, folder_policy),
    );

    // Should fail validation - self-reference is a cycle of length 1
    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_err(),
        "Should detect INHERITS self-reference cycle: folders → folders"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("cycle"),
        "Error message should mention cycle: {}",
        err
    );
}

/// Test that valid INHERITS chains (no cycles) pass validation.
#[test]
fn rebac_inherits_no_cycle_passes() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Organizations table (no INHERITS)
    let org_desc = RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
    let org_policy =
        TablePolicies::new().with_select(PolicyExpr::eq_session("name", vec!["org".into()]));
    schema.insert(
        TableName::new("orgs"),
        TableSchema::with_policies(org_desc, org_policy),
    );

    // Teams table - INHERITS from orgs
    let team_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("org_id", ColumnType::Uuid)
            .nullable()
            .references("orgs"),
    ]);
    let team_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "org_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("teams"),
        TableSchema::with_policies(team_desc, team_policy),
    );

    // Projects table - INHERITS from teams
    let project_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("team_id", ColumnType::Uuid)
            .nullable()
            .references("teams"),
    ]);
    let project_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "team_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("projects"),
        TableSchema::with_policies(project_desc, project_policy),
    );

    // Should pass - this is a valid chain: projects → teams → orgs
    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_ok(),
        "Valid INHERITS chain should pass validation: {:?}",
        result
    );
}

/// Test that bounded self-referential INHERITS is accepted by cycle validation.
#[test]
fn rebac_inherits_bounded_self_reference_passes_validation() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    let folder_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folder_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "parent_id".into(),
        max_depth: Some(10),
    });
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folder_desc, folder_policy),
    );

    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_ok(),
        "Bounded self-referential INHERITS should pass cycle validation: {:?}",
        result
    );
}

fn declared_file_inheritance_schema(array_edge: bool) -> Schema {
    let mut schema = Schema::new();

    let source_fk_column = if array_edge { "images" } else { "image" };
    let inherited_read = PolicyExpr::InheritsReferencing {
        operation: Operation::Select,
        source_table: "todos".into(),
        via_column: source_fk_column.into(),
        max_depth: None,
    };
    let inherited_update = PolicyExpr::InheritsReferencing {
        operation: Operation::Update,
        source_table: "todos".into(),
        via_column: source_fk_column.into(),
        max_depth: None,
    };

    let files_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let files_policies = TablePolicies::new()
        .with_select(PolicyExpr::or(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            inherited_read,
        ]))
        .with_update(
            Some(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                inherited_update,
            ])),
            PolicyExpr::True,
        );
    schema.insert(
        TableName::new("files"),
        TableSchema::with_policies(files_descriptor, files_policies),
    );

    let image_column = if array_edge {
        ColumnDescriptor::new(
            "images",
            ColumnType::Array {
                element: Box::new(ColumnType::Uuid),
            },
        )
        .references("files")
    } else {
        ColumnDescriptor::new("image", ColumnType::Uuid)
            .nullable()
            .references("files")
    };
    let todos_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        image_column,
    ]);
    let todos_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_update(
            Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
            PolicyExpr::True,
        );
    schema.insert(
        TableName::new("todos"),
        TableSchema::with_policies(todos_descriptor, todos_policies),
    );

    schema
}

#[test]
fn rebac_declared_fk_inheritance_grants_select_access() {
    use crate::query_manager::query::QueryBuilder;

    let schema = declared_file_inheritance_schema(false);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Text("bob".into()), Value::Text("bob-file".into())],
        )
        .unwrap()
        .row_id;
    let _todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Uuid(file_id),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("files").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.contains(&file_id),
        "alice should see file via allowedTo.readReferencing(policy.todos, \"image\")"
    );
}

#[test]
fn rebac_declared_fk_inheritance_grants_update_access() {
    let schema = declared_file_inheritance_schema(false);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Text("bob".into()), Value::Text("bob-file".into())],
        )
        .unwrap()
        .row_id;
    let _todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Uuid(file_id),
            ],
        )
        .unwrap()
        .row_id;

    let update = qm.update_with_session(
        &mut storage,
        file_id,
        &[
            Value::Text("bob".into()),
            Value::Text("updated by alice".into()),
        ],
        Some(&Session::new("alice")),
    );
    assert!(
        update.is_ok(),
        "alice should update file via declared inherited access from todos row"
    );
}

#[test]
fn rebac_declared_fk_inheritance_array_membership_grants_access() {
    use crate::query_manager::query::QueryBuilder;

    let schema = declared_file_inheritance_schema(true);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Text("bob".into()), Value::Text("array-file".into())],
        )
        .unwrap()
        .row_id;
    let _todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Array(vec![Value::Uuid(file_id), Value::Uuid(file_id)]),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("files").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.contains(&file_id),
        "array FK membership should grant inherited access when target id is present"
    );
}

#[test]
fn rebac_declared_fk_inheritance_cycle_fails_closed() {
    use crate::query_manager::query::QueryBuilder;

    let mut schema = Schema::new();
    let a_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("b_id", ColumnType::Uuid)
            .nullable()
            .references("table_b"),
    ]);
    let b_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("a_id", ColumnType::Uuid)
            .nullable()
            .references("table_a"),
    ]);
    let a_policies = TablePolicies::new().with_select(PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::InheritsReferencing {
            operation: Operation::Select,
            source_table: "table_b".into(),
            via_column: "a_id".into(),
            max_depth: None,
        },
    ]));
    let b_policies = TablePolicies::new().with_select(PolicyExpr::or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::InheritsReferencing {
            operation: Operation::Select,
            source_table: "table_a".into(),
            via_column: "b_id".into(),
            max_depth: None,
        },
    ]));
    schema.insert(
        TableName::new("table_a"),
        TableSchema::with_policies(a_descriptor.clone(), a_policies),
    );
    schema.insert(
        TableName::new("table_b"),
        TableSchema::with_policies(b_descriptor.clone(), b_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let a_id = qm
        .insert(
            &mut storage,
            "table_a",
            &[Value::Text("bob".into()), Value::Null],
        )
        .unwrap()
        .row_id;
    let b_id = qm
        .insert(
            &mut storage,
            "table_b",
            &[Value::Text("carol".into()), Value::Uuid(a_id)],
        )
        .unwrap()
        .row_id;

    qm.update(
        &mut storage,
        a_id,
        &[Value::Text("bob".into()), Value::Uuid(b_id)],
    )
    .unwrap();

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("table_a").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.is_empty(),
        "cycle path should fail closed and not grant access"
    );
}

#[test]
fn rebac_declared_fk_inheritance_reacts_to_fk_updates() {
    use crate::query_manager::query::QueryBuilder;

    let schema = declared_file_inheritance_schema(false);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = MemoryStorage::new();

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[
                Value::Text("bob".into()),
                Value::Text("delayed-link".into()),
            ],
        )
        .unwrap()
        .row_id;
    let todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("files").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }
    let initially_visible: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        !initially_visible.contains(&file_id),
        "file should be hidden before an inheriting reference exists"
    );

    qm.update(
        &mut storage,
        todo_id,
        &[
            Value::Text("alice".into()),
            Value::Text("todo".into()),
            Value::Uuid(file_id),
        ],
    )
    .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }
    let visible_after_link: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_after_link.contains(&file_id),
        "updating referencing FK should re-evaluate and grant access to linked target row"
    );
}
