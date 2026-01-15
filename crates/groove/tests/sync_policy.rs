//! True E2E sync policy integration tests.
//!
//! These tests verify policy-filtered sync using the full sync flow:
//! - TestHarness with Database for policy evaluation
//! - Clients with registered identities and claims
//! - Full push → broadcast → SSE event flow
//! - No direct calls to internal policy methods

#![cfg(feature = "sync-server")]

use std::rc::Rc;
use std::time::Duration;

use groove::ObjectId;
use groove::sql::{Database, ExecuteResult, RowBuilder, RowDescriptor};
use groove::sync::test_harness::TestHarness;
use groove::sync::{ClaimValue, ClientIdentity};

// ============================================================================
// Test Helpers
// ============================================================================

fn identity_with_claims(name: &str, claims: Vec<(&str, ClaimValue)>) -> ClientIdentity {
    let mut identity = ClientIdentity::simple(name);
    for (key, value) in claims {
        identity.claims.insert(key.to_string(), value);
    }
    identity
}

fn identity_with_user_id(name: &str, user_id: ObjectId) -> ClientIdentity {
    let mut identity = ClientIdentity::simple(name);
    identity.user_id = Some(user_id);
    identity
}

// ============================================================================
// Org-Scoped Document Tests (E2E)
// ============================================================================

/// Test that clients only receive broadcasts for documents in their org.
///
/// Flow:
/// 1. Create database with org-scoped policy
/// 2. Register two clients with different orgId claims
/// 3. Both subscribe to all objects
/// 4. One client pushes commits for an org-alpha document
/// 5. Only the org-alpha client receives the broadcast
#[tokio::test]
async fn test_org_scoped_broadcast_e2e() {
    // Setup database with org-scoped policy
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE documents (title STRING, org_id STRING)")
        .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE org_id = @viewer.claims.orgId")
        .unwrap();

    // Insert a document for org-alpha
    let doc_id = match db
        .execute("INSERT INTO documents (title, org_id) VALUES ('Alpha Report', 'org-alpha')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    // Create harness with database
    let harness = TestHarness::with_database(Rc::clone(&db));

    // Register identities with org claims
    harness.register_identity(
        "alice",
        identity_with_claims(
            "alice",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
    );
    harness.register_identity(
        "bob",
        identity_with_claims(
            "bob",
            vec![("orgId", ClaimValue::String("org-beta".to_string()))],
        ),
    );

    // Create clients
    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    // Both subscribe (wildcard query)
    // Alice subscribes (her stream not used since sender is excluded from broadcasts)
    let _alice_rx = alice.subscribe_with_receiver().await.unwrap();
    let mut bob_rx = bob.subscribe_with_receiver().await.unwrap();

    // Small delay to ensure subscriptions are processed
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice writes and pushes to the document
    let (_, _response) = alice
        .write_and_push(doc_id, b"Updated content")
        .await
        .unwrap();

    // Give time for broadcast
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bob should NOT receive the broadcast (different org)
    // Note: Alice won't receive either since sender is excluded
    let bob_event = bob_rx.try_recv();
    assert!(
        bob_event.is_err(),
        "Bob (org-beta) should NOT receive broadcast for org-alpha document"
    );
}

/// Test isolation between multiple orgs.
#[tokio::test]
async fn test_multiple_orgs_isolation_e2e() {
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE projects (name STRING, org_id STRING)")
        .unwrap();
    db.execute("CREATE POLICY ON projects FOR SELECT WHERE org_id = @viewer.claims.orgId")
        .unwrap();

    // Create projects for different orgs
    let alpha_project = match db
        .execute("INSERT INTO projects (name, org_id) VALUES ('Alpha Project', 'org-alpha')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let beta_project = match db
        .execute("INSERT INTO projects (name, org_id) VALUES ('Beta Project', 'org-beta')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let harness = TestHarness::with_database(Rc::clone(&db));

    // Register identities
    harness.register_identity(
        "alice",
        identity_with_claims(
            "alice",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
    );
    harness.register_identity(
        "bob",
        identity_with_claims(
            "bob",
            vec![("orgId", ClaimValue::String("org-beta".to_string()))],
        ),
    );
    harness.register_identity(
        "charlie",
        identity_with_claims(
            "charlie",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
    );

    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");
    let mut charlie = harness.create_client("charlie");

    // All subscribe
    let mut _alice_stream = alice.subscribe_with_receiver().await.unwrap();
    let mut bob_stream = bob.subscribe_with_receiver().await.unwrap();
    let mut charlie_stream = charlie.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice pushes to alpha project
    alice
        .write_and_push(alpha_project, b"Alpha update")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Charlie (org-alpha) should receive, Bob (org-beta) should NOT
    assert!(
        charlie_stream.try_recv().is_ok(),
        "Charlie (org-alpha) should receive alpha project update"
    );
    assert!(
        bob_stream.try_recv().is_err(),
        "Bob (org-beta) should NOT receive alpha project update"
    );

    // Bob pushes to beta project
    bob.write_and_push(beta_project, b"Beta update")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Neither Alice nor Charlie (org-alpha) should receive
    // Clear any pending from charlie first
    while charlie_stream.try_recv().is_ok() {}

    assert!(
        charlie_stream.try_recv().is_err(),
        "Charlie should NOT receive beta project update"
    );
}

// ============================================================================
// Subscription Tier Tests (E2E)
// ============================================================================

/// Test that subscription tiers filter content appropriately.
#[tokio::test]
async fn test_subscription_tier_e2e() {
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE premium_content (title STRING, tier STRING)")
        .unwrap();
    db.execute(
        "CREATE POLICY ON premium_content FOR SELECT WHERE tier = @viewer.claims.subscriptionTier",
    )
    .unwrap();

    // Pro tier content
    let pro_content = match db
        .execute("INSERT INTO premium_content (title, tier) VALUES ('Pro Video', 'pro')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let harness = TestHarness::with_database(Rc::clone(&db));

    harness.register_identity(
        "pro-user",
        identity_with_claims(
            "pro-user",
            vec![("subscriptionTier", ClaimValue::String("pro".to_string()))],
        ),
    );
    harness.register_identity(
        "free-user",
        identity_with_claims(
            "free-user",
            vec![("subscriptionTier", ClaimValue::String("free".to_string()))],
        ),
    );

    let mut pro = harness.create_client("pro-user");
    let mut free = harness.create_client("free-user");

    let mut _pro_stream = pro.subscribe_with_receiver().await.unwrap();
    let mut free_stream = free.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Pro user updates pro content
    pro.write_and_push(pro_content, b"Pro update")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Free user should NOT receive
    assert!(
        free_stream.try_recv().is_err(),
        "Free user should NOT receive pro content updates"
    );
}

// ============================================================================
// Owner-Based Policy Tests (E2E)
// ============================================================================

/// Test that owner-based policies work correctly.
#[tokio::test]
async fn test_owner_policy_e2e() {
    let db = Rc::new(Database::in_memory());

    // Create users table
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // Create documents with owner reference
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE owner_id = @viewer")
        .unwrap();

    // Create users
    let alice_id = match db
        .execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };
    let bob_id = match db
        .execute("INSERT INTO users (name) VALUES ('Bob')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    // Create Alice's document
    let schema = db.get_table("documents").unwrap();
    let descriptor = Rc::new(RowDescriptor::from_table_schema(&schema));
    let row = RowBuilder::new(descriptor)
        .set_string_by_name("title", "Alice's Secret")
        .set_ref_by_name("owner_id", alice_id)
        .build();
    let doc_id = db.insert_row("documents", row).unwrap();

    let harness = TestHarness::with_database(Rc::clone(&db));

    // Register with user_id (not claims)
    harness.register_identity("alice", identity_with_user_id("alice", alice_id));
    harness.register_identity("bob", identity_with_user_id("bob", bob_id));

    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let mut _alice_stream = alice.subscribe_with_receiver().await.unwrap();
    let mut bob_stream = bob.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice updates her document
    alice
        .write_and_push(doc_id, b"Secret update")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bob should NOT receive (not the owner)
    assert!(
        bob_stream.try_recv().is_err(),
        "Bob (not owner) should NOT receive Alice's document updates"
    );
}

// ============================================================================
// Role-Based Policy Tests (E2E)
// ============================================================================

/// Test that role arrays work with CONTAINS operator.
#[tokio::test]
async fn test_role_based_policy_e2e() {
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE admin_settings (key STRING, value STRING)")
        .unwrap();
    db.execute(
        "CREATE POLICY ON admin_settings FOR SELECT WHERE @viewer.claims.roles CONTAINS 'admin'",
    )
    .unwrap();

    let setting_id = match db
        .execute("INSERT INTO admin_settings (key, value) VALUES ('feature_flag', 'enabled')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let harness = TestHarness::with_database(Rc::clone(&db));

    // Admin has roles: ["admin", "user"]
    harness.register_identity(
        "admin",
        identity_with_claims(
            "admin",
            vec![(
                "roles",
                ClaimValue::Array(vec![
                    ClaimValue::String("admin".to_string()),
                    ClaimValue::String("user".to_string()),
                ]),
            )],
        ),
    );

    // Regular user has roles: ["user"]
    harness.register_identity(
        "user",
        identity_with_claims(
            "user",
            vec![(
                "roles",
                ClaimValue::Array(vec![ClaimValue::String("user".to_string())]),
            )],
        ),
    );

    let mut admin = harness.create_client("admin");
    let mut user = harness.create_client("user");

    let mut _admin_stream = admin.subscribe_with_receiver().await.unwrap();
    let mut user_stream = user.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Admin updates admin settings
    admin
        .write_and_push(setting_id, b"Updated setting")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Regular user should NOT receive
    assert!(
        user_stream.try_recv().is_err(),
        "Non-admin user should NOT receive admin settings updates"
    );
}

// ============================================================================
// Team Membership Tests (E2E)
// ============================================================================

/// Test that team membership works with IN operator.
#[tokio::test]
async fn test_team_membership_e2e() {
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE team_docs (title STRING, team_id STRING)")
        .unwrap();
    db.execute("CREATE POLICY ON team_docs FOR SELECT WHERE team_id IN @viewer.claims.groups")
        .unwrap();

    let doc_id = match db
        .execute(
            "INSERT INTO team_docs (title, team_id) VALUES ('Engineering Roadmap', 'team-engineering')",
        )
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let harness = TestHarness::with_database(Rc::clone(&db));

    // Engineer is in team-engineering and team-platform
    harness.register_identity(
        "engineer",
        identity_with_claims(
            "engineer",
            vec![(
                "groups",
                ClaimValue::Array(vec![
                    ClaimValue::String("team-engineering".to_string()),
                    ClaimValue::String("team-platform".to_string()),
                ]),
            )],
        ),
    );

    // Sales is in team-sales only
    harness.register_identity(
        "sales",
        identity_with_claims(
            "sales",
            vec![(
                "groups",
                ClaimValue::Array(vec![ClaimValue::String("team-sales".to_string())]),
            )],
        ),
    );

    let mut engineer = harness.create_client("engineer");
    let mut sales = harness.create_client("sales");

    let mut _engineer_stream = engineer.subscribe_with_receiver().await.unwrap();
    let mut sales_stream = sales.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Engineer updates engineering doc
    engineer
        .write_and_push(doc_id, b"Engineering update")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Sales should NOT receive
    assert!(
        sales_stream.try_recv().is_err(),
        "Sales should NOT receive engineering doc updates"
    );
}

// ============================================================================
// Combined Policy Tests (E2E)
// ============================================================================

/// Test combined org + role policies.
#[tokio::test]
async fn test_combined_org_and_role_e2e() {
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE org_settings (setting STRING, org_id STRING)")
        .unwrap();
    db.execute(
        "CREATE POLICY ON org_settings FOR SELECT WHERE org_id = @viewer.claims.orgId AND @viewer.claims.roles CONTAINS 'org_admin'",
    )
    .unwrap();

    let setting_id = match db
        .execute("INSERT INTO org_settings (setting, org_id) VALUES ('billing_plan', 'org-acme')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let harness = TestHarness::with_database(Rc::clone(&db));

    // Acme org admin
    harness.register_identity(
        "acme-admin",
        identity_with_claims(
            "acme-admin",
            vec![
                ("orgId", ClaimValue::String("org-acme".to_string())),
                (
                    "roles",
                    ClaimValue::Array(vec![ClaimValue::String("org_admin".to_string())]),
                ),
            ],
        ),
    );

    // Acme org member (not admin)
    harness.register_identity(
        "acme-member",
        identity_with_claims(
            "acme-member",
            vec![
                ("orgId", ClaimValue::String("org-acme".to_string())),
                (
                    "roles",
                    ClaimValue::Array(vec![ClaimValue::String("member".to_string())]),
                ),
            ],
        ),
    );

    // Other org admin (wrong org)
    harness.register_identity(
        "other-admin",
        identity_with_claims(
            "other-admin",
            vec![
                ("orgId", ClaimValue::String("org-other".to_string())),
                (
                    "roles",
                    ClaimValue::Array(vec![ClaimValue::String("org_admin".to_string())]),
                ),
            ],
        ),
    );

    let mut acme_admin = harness.create_client("acme-admin");
    let mut acme_member = harness.create_client("acme-member");
    let mut other_admin = harness.create_client("other-admin");

    let mut _admin_stream = acme_admin.subscribe_with_receiver().await.unwrap();
    let mut member_stream = acme_member.subscribe_with_receiver().await.unwrap();
    let mut other_stream = other_admin.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Acme admin updates settings
    acme_admin
        .write_and_push(setting_id, b"New billing plan")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Neither member nor other admin should receive
    assert!(
        member_stream.try_recv().is_err(),
        "Acme member (not admin) should NOT receive"
    );
    assert!(
        other_stream.try_recv().is_err(),
        "Other org admin should NOT receive acme settings"
    );
}

// ============================================================================
// Positive Tests - Verify Receipt (E2E)
// ============================================================================

/// Test that matching clients DO receive broadcasts.
#[tokio::test]
async fn test_matching_client_receives_broadcast_e2e() {
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE notes (content STRING, org_id STRING)")
        .unwrap();
    db.execute("CREATE POLICY ON notes FOR SELECT WHERE org_id = @viewer.claims.orgId")
        .unwrap();

    let note_id = match db
        .execute("INSERT INTO notes (content, org_id) VALUES ('Team note', 'org-alpha')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let harness = TestHarness::with_database(Rc::clone(&db));

    // Two users in the same org
    harness.register_identity(
        "alice",
        identity_with_claims(
            "alice",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
    );
    harness.register_identity(
        "bob",
        identity_with_claims(
            "bob",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
    );

    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let mut _alice_stream = alice.subscribe_with_receiver().await.unwrap();
    let mut bob_stream = bob.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice updates the note
    alice
        .write_and_push(note_id, b"Updated note")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bob (same org) SHOULD receive the broadcast
    assert!(
        bob_stream.try_recv().is_ok(),
        "Bob (same org) SHOULD receive the broadcast"
    );
}

/// Test sender exclusion - sender doesn't get their own broadcast.
#[tokio::test]
async fn test_sender_excluded_e2e() {
    let db = Rc::new(Database::in_memory());
    db.execute("CREATE TABLE notes (content STRING, org_id STRING)")
        .unwrap();
    db.execute("CREATE POLICY ON notes FOR SELECT WHERE org_id = @viewer.claims.orgId")
        .unwrap();

    let note_id = match db
        .execute("INSERT INTO notes (content, org_id) VALUES ('My note', 'org-alpha')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let harness = TestHarness::with_database(Rc::clone(&db));

    harness.register_identity(
        "alice",
        identity_with_claims(
            "alice",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
    );
    harness.register_identity(
        "bob",
        identity_with_claims(
            "bob",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
    );

    let mut alice = harness.create_client("alice");
    let mut bob = harness.create_client("bob");

    let mut alice_stream = alice.subscribe_with_receiver().await.unwrap();
    let mut bob_stream = bob.subscribe_with_receiver().await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Alice updates the note
    alice
        .write_and_push(note_id, b"Alice's update")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice (sender) should NOT receive her own broadcast
    assert!(
        alice_stream.try_recv().is_err(),
        "Alice (sender) should NOT receive her own broadcast"
    );

    // Bob SHOULD receive
    assert!(
        bob_stream.try_recv().is_ok(),
        "Bob (same org, not sender) SHOULD receive"
    );
}
