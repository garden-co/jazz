//! ReBAC Policy Evaluation Integration Tests
//!
//! Tests for the async permission evaluation system using policy graphs.

use std::collections::HashSet;

use smallvec::smallvec;

use crate::commit::Commit;
use crate::object::ObjectId;
use crate::sync_manager::{
    ClientId, Destination, InboxEntry, ObjectMetadata, QueryId, Source, SyncError, SyncManager,
    SyncPayload,
};

use super::QueryManager;
use super::encoding::encode_row;
use super::policy::PolicyExpr;
use super::session::Session;
use super::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TablePolicies, TableSchema,
    Value,
};

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
    m.insert("table".to_string(), "documents".to_string());
    m
}

#[test]
fn rebac_insert_allowed_by_simple_policy() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = QueryManager::new(sync_manager, schema);

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(document_metadata()));

    // Register a query scope so the update is in-scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client_id, QueryId(1), scope);
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
    qm.process();

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
    let mut qm = QueryManager::new(sync_manager, schema);

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(document_metadata()));

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client_id, QueryId(1), scope);
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
    qm.process();

    // Should get permission denied error
    let outbox = qm.sync_manager_mut().take_outbox();
    let error = outbox
        .iter()
        .find(|e| matches!(e.destination, Destination::Client(id) if id == client_id));

    assert!(error.is_some(), "Should receive error response");

    match &error.unwrap().payload {
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
fn rebac_no_session_allows_all_writes() {
    // Setup without session
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = QueryManager::new(sync_manager, schema);

    // Add a client WITHOUT session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    // Note: NOT setting session

    // Create an object for the row
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(document_metadata()));

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client_id, QueryId(1), scope);
    qm.sync_manager_mut().take_outbox();

    // Encode row content
    let content = encode_document("anyone", "Doc", None);

    // Client sends insert
    let commit = Commit {
        parents: smallvec![],
        content,
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
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

    // Process - without session, should be allowed immediately
    qm.process();

    // Commit should be applied (no session = permissive mode)
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main")
        .unwrap();
    assert!(
        tips.contains(&commit.id()),
        "Without session, writes should be allowed"
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
    let mut qm = QueryManager::new(sync_manager, schema);

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object for the row
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("table".to_string(), "notes".to_string());
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(metadata.clone()));

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client_id, QueryId(1), scope);
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
    qm.process();

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
    let mut qm = QueryManager::new(sync_manager, schema);

    // Add a client with session
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    // Create an object WITHOUT table metadata (not a row)
    let obj_id = qm.sync_manager_mut().object_manager.create(None);

    // Register a query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client_id, QueryId(1), scope);
    qm.sync_manager_mut().take_outbox();

    // Client sends update
    let commit = Commit {
        parents: smallvec![],
        content: b"some data".to_vec(),
        timestamp: 1000,
        author: ObjectId::new(),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
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
    qm.process();

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
fn rebac_two_clients_different_sessions() {
    // Setup
    let sync_manager = SyncManager::new();
    let schema = rebac_test_schema();
    let mut qm = QueryManager::new(sync_manager, schema);

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
        .create(Some(document_metadata()));
    let obj2 = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(document_metadata()));

    // Register query scopes
    let mut scope1 = HashSet::new();
    scope1.insert((obj1, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client1, QueryId(1), scope1);

    let mut scope2 = HashSet::new();
    scope2.insert((obj2, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client2, QueryId(2), scope2);

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
    qm.process();

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

// =============================================================================
// Failing tests for unimplemented features
// =============================================================================
// These tests document expected behavior that is not yet implemented.
// They are marked #[ignore] until the features are complete.

/// Test that EXISTS clause in INSERT policy correctly denies writes.
///
/// Scenario: Insert policy requires EXISTS (SELECT FROM admins WHERE user_id = @session.user_id)
/// A non-admin user tries to insert - should be denied.
///
/// CURRENT BUG: EXISTS clauses are not evaluated (always pass), so this incorrectly allows the insert.
/// See: manager.rs:1398 - "TODO: Implement EXISTS clause evaluation"
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
    let mut qm = QueryManager::new(sync_manager, schema);

    // Add a client with session for non-admin user
    let client_id = ClientId::new();
    qm.sync_manager_mut().add_client(client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("regular_user"));

    // Note: We do NOT add "regular_user" to admins table

    // Create object for protected row
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("table".to_string(), "protected".to_string());
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(metadata.clone()));

    // Register query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(client_id, QueryId(1), scope);
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
    qm.process();

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

    // Commit should NOT be applied
    let tips = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(obj_id, "main");
    assert!(
        tips.is_err() || !tips.unwrap().contains(&commit.id()),
        "Non-admin insert should be denied by EXISTS policy"
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
    let mut qm = QueryManager::new(sync_manager, schema);

    // Create Alice's document first (as server/no session)
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("table".to_string(), "documents".to_string());
    let obj_id = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(metadata.clone()));

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
        .add_commit(obj_id, "main", vec![], alice_content, author, None)
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
        .add_or_update_query(bob_client, QueryId(1), scope);
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
    qm.process();

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
    use super::query::QueryBuilder;

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
            operation: super::policy::Operation::Select,
            via_column: "folder_id".into(),
        },
    ]));
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(docs_descriptor.clone(), docs_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager, schema);

    // Create Alice's folder
    let mut folder_meta = std::collections::HashMap::new();
    folder_meta.insert("table".to_string(), "folders".to_string());
    let folder_id = qm
        .sync_manager_mut()
        .object_manager
        .create(Some(folder_meta));

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
        .add_commit(folder_id, "main", vec![], folder_content, author, None)
        .unwrap();

    // Create Bob's document in Alice's folder
    let mut doc_meta = std::collections::HashMap::new();
    doc_meta.insert("table".to_string(), "documents".to_string());
    let doc_id = qm.sync_manager_mut().object_manager.create(Some(doc_meta));

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
        .add_commit(doc_id, "main", vec![], doc_content, author, None)
        .unwrap();

    // Charlie subscribes to documents query with his session
    let charlie_session = Session::new("charlie");
    let query = QueryBuilder::new("documents").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(charlie_session))
        .unwrap();

    // Process to settle the query
    for _ in 0..10 {
        qm.process();
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
    let mut qm = QueryManager::new(sync_manager, schema);

    // Add Alice as admin (using insert to properly index the row)
    let _alice_admin = qm.insert("admins", &[Value::Text("alice".into())]).unwrap();

    // Create a protected row (as server, no session) - also using insert for proper indexing
    let protected_handle = qm
        .insert("protected", &[Value::Text("original data".into())])
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
    let bob_client = ClientId::new();
    qm.sync_manager_mut().add_client(bob_client);
    qm.sync_manager_mut()
        .set_client_session(bob_client, Session::new("bob"));

    // Register query scope for Bob
    let mut bob_scope = HashSet::new();
    bob_scope.insert((protected_obj, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(bob_client, QueryId(1), bob_scope);
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
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(bob_client),
        payload: SyncPayload::ObjectUpdated {
            object_id: protected_obj,
            metadata: Some(ObjectMetadata {
                id: protected_obj,
                metadata: protected_metadata.clone(),
            }),
            branch_name: "main".into(),
            commits: vec![bob_commit.clone()],
        },
    });

    // Process - may need multiple iterations for EXISTS to settle
    for _ in 0..10 {
        qm.process();
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
        .get_tip_ids(protected_obj, "main")
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
    alice_scope.insert((protected_obj, "main".into()));
    qm.sync_manager_mut()
        .add_or_update_query(alice_client, QueryId(2), alice_scope);
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
    };

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(alice_client),
        payload: SyncPayload::ObjectUpdated {
            object_id: protected_obj,
            metadata: Some(ObjectMetadata {
                id: protected_obj,
                metadata: protected_metadata.clone(),
            }),
            branch_name: "main".into(),
            commits: vec![alice_commit.clone()],
        },
    });

    // Process - may need multiple iterations for EXISTS to settle
    for _ in 0..10 {
        qm.process();
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
        .get_tip_ids(protected_obj, "main")
        .unwrap();
    assert!(
        tips.contains(&alice_commit.id()),
        "Alice's update should be applied - she is an admin"
    );
}

// ============================================================================
// INHERITS Cycle Detection Tests
// ============================================================================

/// Test that INHERITS cycles are detected during schema validation.
/// Cycle: A → B → A (direct cycle between two tables)
#[test]
fn rebac_inherits_cycle_detection() {
    use super::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Table A references B via INHERITS
    let a_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("b_id", ColumnType::Uuid)
            .nullable()
            .references("table_b"),
    ]);
    let a_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: super::policy::Operation::Select,
        via_column: "b_id".into(),
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
        operation: super::policy::Operation::Select,
        via_column: "a_id".into(),
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
    use super::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Folder table with parent_id referencing itself
    let folder_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folder_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: super::policy::Operation::Select,
        via_column: "parent_id".into(),
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
    use super::types::validate_no_inherits_cycles;

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
        operation: super::policy::Operation::Select,
        via_column: "org_id".into(),
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
        operation: super::policy::Operation::Select,
        via_column: "team_id".into(),
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
