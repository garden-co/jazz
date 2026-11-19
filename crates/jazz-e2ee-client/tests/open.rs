//! Opening an encrypted app with the runtime's hooks (the hooks-only open of
//! `DbConfig::with_e2ee`): the database accepts the runtime's hooks, which
//! are built from the app's own declarations, refuses the app without them,
//! and accepts the runtime of a version that keeps the system tables but
//! declares no encrypted column (its declarations name no table); an account
//! then starts through a database opened that way.

mod common;

use common::*;
use jazz::binding_host::open_memory;
use jazz::db::{DbConfig, DbIdentity, block_on};
use jazz::e2ee::EncryptionSettings;
use jazz_e2ee_client::{AccountStatus, MemoryStore};

/// Contract: an encrypted app opens through the host entry points with the
/// runtime's hooks and not without them; a version that keeps the system
/// tables without declaring an encrypted column opens with the runtime built
/// from its (empty) declarations; the account starts on the database opened
/// with the runtime's hooks.
#[test]
fn an_encrypted_app_opens_only_with_the_runtimes_hooks() {
    let settings = EncryptionSettings::default();
    let alice = account(0xa1);
    let client = open_account(
        std::rc::Rc::new(MemoryStore::new()),
        alice,
        settings.clone(),
        None,
    );
    let config = || {
        DbConfig::new(
            secure_schema_with(&settings),
            DbIdentity {
                node: jazz::ids::NodeUuid::from_bytes([1; 16]),
                author: alice,
            },
        )
    };

    let mut none = declarations_with(settings.clone());
    none.tables.clear();
    let bare = block_on(jazz_e2ee_client::E2eeClient::open(
        jazz_e2ee_client::ClientConfig {
            store: std::rc::Rc::new(MemoryStore::new()),
            declarations: none,
            account_id: account_id(alice),
            registry: jazz_e2ee::registry::Registry::builtin(),
            random: Box::new(jazz_e2ee::OsRandom),
            device_account: None,
        },
    ))
    .unwrap();
    // The version still has Jazz's system tables: it is an encrypted app, and
    // the runtime built from its declarations (no table) says so.
    let opened = block_on(open_memory(config().with_e2ee(bare.hooks())));
    assert!(
        opened.is_ok(),
        "a runtime with empty declarations opens an encrypted app: {:?}",
        opened.err()
    );

    let server = secure_authority_with(&settings, None);
    let db = block_on(open_memory(config().with_e2ee(client.hooks()))).unwrap();
    let (_link, _wire) = link_as(&db, &server, alice);
    pump(&server, &db);
    assert!(client.prepare(&db).unwrap());
    let status = drive(&server, &[&db], client.ensure_account(&db)).unwrap();
    assert_eq!(status, AccountStatus::Active);
}
