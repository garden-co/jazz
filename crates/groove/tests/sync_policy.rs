//! Sync policy integration tests.
//!
//! These tests verify policy-filtered sync using:
//! - Database with SQL execution (exercising the parser)
//! - SyncServer with ClientIdentity claims
//! - Policy evaluation for SELECT filtering in broadcasts

#![cfg(feature = "sync-server")]

use std::sync::Arc;

use groove::sql::{Database, ExecuteResult};
use groove::sync::{AcceptAllTokens, ClaimValue, ClientIdentity, SyncServer, TokenValidator};
use groove::{MemoryEnvironment, ObjectId};

// ============================================================================
// Test Helpers
// ============================================================================

fn make_server() -> SyncServer<MemoryEnvironment> {
    let env = Arc::new(MemoryEnvironment::new());
    let validator: Arc<dyn TokenValidator> = Arc::new(AcceptAllTokens);
    SyncServer::new(env, validator)
}

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
// Org-Scoped Document Tests
// ============================================================================

#[tokio::test]
async fn test_org_scoped_documents_broadcast() {
    // Setup database with SQL
    let db = Database::in_memory();

    // Create documents table with org_id column
    db.execute("CREATE TABLE documents (title STRING, org_id STRING)")
        .unwrap();

    // Create org-based SELECT policy via SQL parser
    db.execute("CREATE POLICY ON documents FOR SELECT WHERE org_id = @viewer.claims.orgId")
        .unwrap();

    // Insert a document for org-alpha
    let result = db
        .execute("INSERT INTO documents (title, org_id) VALUES ('Alpha Report', 'org-alpha')")
        .unwrap();
    let doc_id = match result {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    // Setup sync server with two sessions (different orgs)
    let mut server = make_server();
    let (tx_alpha, mut rx_alpha) = tokio::sync::mpsc::channel(16);
    let (tx_beta, mut rx_beta) = tokio::sync::mpsc::channel(16);

    let alpha_identity = identity_with_claims(
        "user-alpha",
        vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
    );
    let beta_identity = identity_with_claims(
        "user-beta",
        vec![("orgId", ClaimValue::String("org-beta".to_string()))],
    );

    let s_alpha = server.create_session(alpha_identity, tx_alpha);
    let s_beta = server.create_session(beta_identity, tx_beta);

    // Register both sessions for the document
    server.register_object_session(doc_id, s_alpha);
    server.register_object_session(doc_id, s_beta);

    // Get the row data
    let (_, row) = db.get("documents", doc_id).unwrap().unwrap();

    // Broadcast with policy filtering
    server
        .broadcast_commits_with_policy(
            doc_id,
            "documents",
            &row,
            vec![], // empty commits for test
            vec![],
            None,
            &db, // Database implements RowLookup
            &db, // Database implements PolicyLookup
            None,
        )
        .await;

    // Alpha (org-alpha) should receive the broadcast
    assert!(
        rx_alpha.try_recv().is_ok(),
        "org-alpha user should receive broadcast for org-alpha document"
    );

    // Beta (org-beta) should NOT receive
    assert!(
        rx_beta.try_recv().is_err(),
        "org-beta user should NOT receive broadcast for org-alpha document"
    );
}

#[tokio::test]
async fn test_multiple_orgs_isolation() {
    let db = Database::in_memory();

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

    let mut server = make_server();
    let (tx_alpha, mut rx_alpha) = tokio::sync::mpsc::channel(16);
    let (tx_beta, mut rx_beta) = tokio::sync::mpsc::channel(16);

    let s_alpha = server.create_session(
        identity_with_claims(
            "user-alpha",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
        tx_alpha,
    );
    let s_beta = server.create_session(
        identity_with_claims(
            "user-beta",
            vec![("orgId", ClaimValue::String("org-beta".to_string()))],
        ),
        tx_beta,
    );

    // Both sessions track both projects
    server.register_object_session(alpha_project, s_alpha);
    server.register_object_session(alpha_project, s_beta);
    server.register_object_session(beta_project, s_alpha);
    server.register_object_session(beta_project, s_beta);

    // Broadcast alpha project update
    let (_, alpha_row) = db.get("projects", alpha_project).unwrap().unwrap();
    server
        .broadcast_commits_with_policy(
            alpha_project,
            "projects",
            &alpha_row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            None,
        )
        .await;

    // Only alpha receives
    assert!(rx_alpha.try_recv().is_ok());
    assert!(rx_beta.try_recv().is_err());

    // Clear alpha's channel
    while rx_alpha.try_recv().is_ok() {}

    // Broadcast beta project update
    let (_, beta_row) = db.get("projects", beta_project).unwrap().unwrap();
    server
        .broadcast_commits_with_policy(
            beta_project,
            "projects",
            &beta_row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            None,
        )
        .await;

    // Only beta receives
    assert!(rx_alpha.try_recv().is_err());
    assert!(rx_beta.try_recv().is_ok());
}

// ============================================================================
// Subscription Tier Tests
// ============================================================================

#[tokio::test]
async fn test_subscription_tier_policy() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE premium_content (title STRING, tier STRING)")
        .unwrap();

    // Policy: content tier must match viewer's subscription tier
    db.execute(
        "CREATE POLICY ON premium_content FOR SELECT WHERE tier = @viewer.claims.subscriptionTier",
    )
    .unwrap();

    // Create pro-tier content
    let pro_content = match db
        .execute("INSERT INTO premium_content (title, tier) VALUES ('Pro Video', 'pro')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let mut server = make_server();
    let (tx_pro, mut rx_pro) = tokio::sync::mpsc::channel(16);
    let (tx_free, mut rx_free) = tokio::sync::mpsc::channel(16);

    let s_pro = server.create_session(
        identity_with_claims(
            "pro-user",
            vec![("subscriptionTier", ClaimValue::String("pro".to_string()))],
        ),
        tx_pro,
    );
    let s_free = server.create_session(
        identity_with_claims(
            "free-user",
            vec![("subscriptionTier", ClaimValue::String("free".to_string()))],
        ),
        tx_free,
    );

    server.register_object_session(pro_content, s_pro);
    server.register_object_session(pro_content, s_free);

    let (_, row) = db.get("premium_content", pro_content).unwrap().unwrap();
    server
        .broadcast_commits_with_policy(
            pro_content,
            "premium_content",
            &row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            None,
        )
        .await;

    assert!(rx_pro.try_recv().is_ok(), "pro user should see pro content");
    assert!(
        rx_free.try_recv().is_err(),
        "free user should NOT see pro content"
    );
}

// ============================================================================
// Owner-Based Policy Tests
// ============================================================================

#[tokio::test]
async fn test_owner_policy_with_user_id() {
    let db = Database::in_memory();

    // Create users and documents tables
    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();
    db.execute(
        "CREATE TABLE documents (title STRING NOT NULL, owner_id REFERENCES users NOT NULL)",
    )
    .unwrap();

    // Policy: only owner can see
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
    let doc_id = {
        // Use direct insert since SQL INSERT with REF needs the ObjectId
        let schema = db.get_table("documents").unwrap();
        let descriptor = Arc::new(groove::sql::RowDescriptor::from_table_schema(&schema));
        let row = groove::sql::RowBuilder::new(descriptor)
            .set_string_by_name("title", "Alice's Secret")
            .set_ref_by_name("owner_id", alice_id)
            .build();
        db.insert_row("documents", row).unwrap()
    };

    let mut server = make_server();
    let (tx_alice, mut rx_alice) = tokio::sync::mpsc::channel(16);
    let (tx_bob, mut rx_bob) = tokio::sync::mpsc::channel(16);

    let s_alice = server.create_session(identity_with_user_id("alice", alice_id), tx_alice);
    let s_bob = server.create_session(identity_with_user_id("bob", bob_id), tx_bob);

    server.register_object_session(doc_id, s_alice);
    server.register_object_session(doc_id, s_bob);

    let (_, row) = db.get("documents", doc_id).unwrap().unwrap();
    server
        .broadcast_commits_with_policy(
            doc_id,
            "documents",
            &row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            None,
        )
        .await;

    assert!(rx_alice.try_recv().is_ok(), "Alice (owner) should receive");
    assert!(
        rx_bob.try_recv().is_err(),
        "Bob (not owner) should NOT receive"
    );
}

// ============================================================================
// Role-Based Policy Tests (CONTAINS)
// ============================================================================

#[tokio::test]
async fn test_role_based_policy_with_contains() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE admin_settings (key STRING, value STRING)")
        .unwrap();

    // Policy: viewer must have 'admin' in their roles array
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

    let mut server = make_server();
    let (tx_admin, mut rx_admin) = tokio::sync::mpsc::channel(16);
    let (tx_user, mut rx_user) = tokio::sync::mpsc::channel(16);

    // Admin has roles: ["admin", "user"]
    let s_admin = server.create_session(
        identity_with_claims(
            "admin-user",
            vec![(
                "roles",
                ClaimValue::Array(vec![
                    ClaimValue::String("admin".to_string()),
                    ClaimValue::String("user".to_string()),
                ]),
            )],
        ),
        tx_admin,
    );

    // Regular user has roles: ["user"]
    let s_user = server.create_session(
        identity_with_claims(
            "regular-user",
            vec![(
                "roles",
                ClaimValue::Array(vec![ClaimValue::String("user".to_string())]),
            )],
        ),
        tx_user,
    );

    server.register_object_session(setting_id, s_admin);
    server.register_object_session(setting_id, s_user);

    let (_, row) = db.get("admin_settings", setting_id).unwrap().unwrap();
    server
        .broadcast_commits_with_policy(
            setting_id,
            "admin_settings",
            &row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            None,
        )
        .await;

    assert!(
        rx_admin.try_recv().is_ok(),
        "admin should receive admin settings"
    );
    assert!(
        rx_user.try_recv().is_err(),
        "non-admin should NOT receive admin settings"
    );
}

// ============================================================================
// Team Membership Policy Tests (IN)
// ============================================================================

#[tokio::test]
async fn test_team_membership_policy_with_in() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE team_docs (title STRING, team_id STRING)")
        .unwrap();

    // Policy: document's team_id must be in viewer's groups array
    db.execute("CREATE POLICY ON team_docs FOR SELECT WHERE team_id IN @viewer.claims.groups")
        .unwrap();

    // Create doc for team-engineering
    let doc_id = match db
        .execute("INSERT INTO team_docs (title, team_id) VALUES ('Engineering Roadmap', 'team-engineering')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let mut server = make_server();
    let (tx_eng, mut rx_eng) = tokio::sync::mpsc::channel(16);
    let (tx_sales, mut rx_sales) = tokio::sync::mpsc::channel(16);

    // Engineer is in team-engineering and team-platform
    let s_eng = server.create_session(
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
        tx_eng,
    );

    // Sales is in team-sales only
    let s_sales = server.create_session(
        identity_with_claims(
            "sales-rep",
            vec![(
                "groups",
                ClaimValue::Array(vec![ClaimValue::String("team-sales".to_string())]),
            )],
        ),
        tx_sales,
    );

    server.register_object_session(doc_id, s_eng);
    server.register_object_session(doc_id, s_sales);

    let (_, row) = db.get("team_docs", doc_id).unwrap().unwrap();
    server
        .broadcast_commits_with_policy(
            doc_id,
            "team_docs",
            &row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            None,
        )
        .await;

    assert!(
        rx_eng.try_recv().is_ok(),
        "engineer should see engineering doc"
    );
    assert!(
        rx_sales.try_recv().is_err(),
        "sales should NOT see engineering doc"
    );
}

// ============================================================================
// Combined Policy Tests
// ============================================================================

#[tokio::test]
async fn test_combined_org_and_role_policy() {
    let db = Database::in_memory();

    db.execute("CREATE TABLE org_settings (setting STRING, org_id STRING)")
        .unwrap();

    // Policy: must be in the org AND have org_admin role
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

    let mut server = make_server();
    let (tx_admin, mut rx_admin) = tokio::sync::mpsc::channel(16);
    let (tx_member, mut rx_member) = tokio::sync::mpsc::channel(16);
    let (tx_other_admin, mut rx_other_admin) = tokio::sync::mpsc::channel(16);

    // Acme org admin
    let s_admin = server.create_session(
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
        tx_admin,
    );

    // Acme org member (not admin)
    let s_member = server.create_session(
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
        tx_member,
    );

    // Other org admin (different org)
    let s_other = server.create_session(
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
        tx_other_admin,
    );

    server.register_object_session(setting_id, s_admin);
    server.register_object_session(setting_id, s_member);
    server.register_object_session(setting_id, s_other);

    let (_, row) = db.get("org_settings", setting_id).unwrap().unwrap();
    server
        .broadcast_commits_with_policy(
            setting_id,
            "org_settings",
            &row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            None,
        )
        .await;

    assert!(
        rx_admin.try_recv().is_ok(),
        "acme admin should see acme settings"
    );
    assert!(
        rx_member.try_recv().is_err(),
        "acme member (not admin) should NOT see"
    );
    assert!(
        rx_other_admin.try_recv().is_err(),
        "other org admin should NOT see acme settings"
    );
}

// ============================================================================
// Sender Exclusion Tests
// ============================================================================

#[tokio::test]
async fn test_sender_excluded_even_with_policy_match() {
    let db = Database::in_memory();

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

    let mut server = make_server();
    let (tx1, mut rx1) = tokio::sync::mpsc::channel(16);
    let (tx2, mut rx2) = tokio::sync::mpsc::channel(16);

    // Both in same org
    let s1 = server.create_session(
        identity_with_claims(
            "user1",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
        tx1,
    );
    let s2 = server.create_session(
        identity_with_claims(
            "user2",
            vec![("orgId", ClaimValue::String("org-alpha".to_string()))],
        ),
        tx2,
    );

    server.register_object_session(note_id, s1);
    server.register_object_session(note_id, s2);

    let (_, row) = db.get("notes", note_id).unwrap().unwrap();

    // Broadcast from s1 (sender) - s1 should be excluded
    server
        .broadcast_commits_with_policy(
            note_id,
            "notes",
            &row,
            vec![],
            vec![],
            None,
            &db,
            &db,
            Some(s1), // exclude sender
        )
        .await;

    assert!(
        rx1.try_recv().is_err(),
        "sender should NOT receive own broadcast"
    );
    assert!(
        rx2.try_recv().is_ok(),
        "other user in same org should receive"
    );
}

// ============================================================================
// check_select_policy Helper Tests
// ============================================================================

#[tokio::test]
async fn test_check_select_policy_helper() {
    use groove::sql::ViewerContext;

    let db = Database::in_memory();

    db.execute("CREATE TABLE docs (title STRING, tier STRING)")
        .unwrap();
    db.execute("CREATE POLICY ON docs FOR SELECT WHERE tier = @viewer.claims.tier")
        .unwrap();

    let doc_id = match db
        .execute("INSERT INTO docs (title, tier) VALUES ('Enterprise Guide', 'enterprise')")
        .unwrap()
    {
        ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("expected Inserted"),
    };

    let server = make_server();
    let (_, row) = db.get("docs", doc_id).unwrap().unwrap();

    // Enterprise user
    let enterprise_identity = identity_with_claims(
        "enterprise-user",
        vec![("tier", ClaimValue::String("enterprise".to_string()))],
    );
    let enterprise_viewer = ViewerContext::from_identity(&enterprise_identity);

    let can_access = server.check_select_policy("docs", doc_id, &row, enterprise_viewer, &db, &db);
    assert!(can_access, "enterprise user should access enterprise doc");

    // Basic user
    let basic_identity = identity_with_claims(
        "basic-user",
        vec![("tier", ClaimValue::String("basic".to_string()))],
    );
    let basic_viewer = ViewerContext::from_identity(&basic_identity);

    let can_access = server.check_select_policy("docs", doc_id, &row, basic_viewer, &db, &db);
    assert!(!can_access, "basic user should NOT access enterprise doc");
}
