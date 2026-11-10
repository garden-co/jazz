//! Trusted catalogue metadata is reusable across account roots, never row evidence.
use super::common;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, ReadOpts, block_on, duplex,
    validate_catalogue_capture_replacement,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::{collections::BTreeMap, path::Path};
fn author(name: &str) -> AuthorSubject {
    AuthorSubject::authenticated("https://issuer.example", name).unwrap()
}
fn schema(table: &str) -> JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    )
}
fn config(schema: JazzSchema, node: u8, who: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: who,
        },
    )
}
fn open(schema: JazzSchema, node: u8) -> Db {
    block_on(Db::open(config(schema, node, author("alice")))).unwrap()
}
fn server(schema: JazzSchema) -> Db {
    // The multi-user server is distinct from a private user's replica. The link
    // below is always admitted as real Alice and evaluates that session's policy.
    let db = block_on(Db::open(config(schema, 2, AuthorSubject::SYSTEM))).unwrap();
    db.set_authority();
    db
}
fn capture(db: &Db) -> Vec<u8> {
    db.take_authenticated_catalogue_state()
        .unwrap()
        .capture
        .expect("authenticated capture")
}
fn install(db: &Db, bytes: &[u8]) -> Result<(), jazz::db::Error> {
    // SAFETY: each positive uses bytes from this fixture's real authority in
    // exactly one app scope; corrupt/foreign bytes below deliberately test refusal.
    unsafe { db.install_cached_catalogue(bytes) }
}
fn connect(client: &Db, server: &Db) -> jazz::db::Upstream {
    let (up, down) = duplex();
    let link = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, author("alice")).unwrap();
    for _ in 0..8 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    link
}
fn insert(db: &Db, table: &str) {
    block_on(db.insert(
        table,
        BTreeMap::from([("text".into(), Value::String("offline".into()))]),
        Default::default(),
    ))
    .unwrap();
}

/// Alice learns metadata over her admitted link; Bob reuses only that app metadata offline.
#[test]
fn catalogue_cache_online_capture_restores_names_without_row_coverage() {
    let s = schema("notes");
    let authority = server(s.clone());
    let alice = open(s.clone(), 1);
    assert!(!alice.has_authenticated_catalogue().unwrap());
    assert!(
        alice
            .take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
    let _link = connect(&alice, &authority);
    assert!(alice.has_authenticated_catalogue().unwrap());
    let bytes = capture(&alice);
    assert!(
        alice
            .take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
    let bob = block_on(Db::open(config(s, 3, author("bob")))).unwrap();
    install(&bob, &bytes).unwrap();
    assert!(bob.has_authenticated_catalogue().unwrap());
    assert!(
        bob.take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
    assert_eq!(
        bob.table_identity("notes").unwrap(),
        authority.table_identity("notes").unwrap()
    );
    assert!(block_on(bob.cover_catalogue_with_timeout(None, 0).unwrap()).is_err());
    let q = bob.prepare_query(&Query::from("notes")).unwrap();
    assert!(
        block_on(bob.all(&q, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
    insert(&bob, "notes");
    assert_eq!(block_on(bob.all(&q, ReadOpts::default())).unwrap().len(), 1);
}

/// An unpublished requested schema stays refused even when the same app has another version.
#[test]
fn catalogue_cache_unpublished_requested_schema_is_not_admitted() {
    let authority = server(schema("notes"));
    let bytes = capture(&authority);
    let unknown = open(schema("unpublished"), 3);
    install(&unknown, &bytes).unwrap();
    assert!(!unknown.has_authenticated_catalogue().unwrap());
    assert_eq!(
        unknown
            .prepare_query(&Query::from("unpublished"))
            .unwrap_err()
            .code,
        ErrorCode::Schema
    );
    assert!(
        block_on(unknown.insert(
            "unpublished",
            BTreeMap::from([("text".into(), Value::String("no".into()))]),
            Default::default()
        ))
        .is_err()
    );
}

/// Restoring another authority's namespace is atomic and cannot rebind Alice's names.
#[test]
fn catalogue_cache_foreign_namespace_leaves_existing_metadata_unchanged() {
    let s = schema("notes");
    let first = server(s.clone());
    let second = server(s.clone());
    let a = capture(&first);
    let b = capture(&second);
    let alice = open(s, 3);
    install(&alice, &a).unwrap();
    let identity = alice.table_identity("notes").unwrap();
    let before = alice.catalogue().unwrap();
    assert!(install(&alice, &b).is_err());
    assert_eq!(alice.table_identity("notes").unwrap(), identity);
    assert_eq!(alice.catalogue().unwrap(), before);
    assert!(validate_catalogue_capture_replacement(&a, &b).is_err());
    validate_catalogue_capture_replacement(&a, &a).unwrap();
}

/// Corrupt, truncated and trailing data cannot partially admit schemas or names.
#[test]
fn catalogue_cache_malformed_capture_is_atomic() {
    let s = schema("notes");
    let authority = server(s.clone());
    let bytes = capture(&authority);
    let mut trailing = bytes.clone();
    trailing.push(0);
    let mut corrupt = bytes.clone();
    corrupt[0] ^= 1;
    for bad in [
        Vec::new(),
        bytes[..bytes.len() - 1].to_vec(),
        trailing,
        corrupt,
    ] {
        let alice = open(s.clone(), 3);
        let before = alice.catalogue().unwrap();
        assert!(install(&alice, &bad).is_err());
        assert_eq!(alice.catalogue().unwrap(), before);
        assert!(!alice.has_authenticated_catalogue().unwrap());
    }
}

/// Metadata contains no policy override: Bob's actual governing SELECT still denies.
#[test]
fn catalogue_cache_never_installs_captured_permissions() {
    let allowed = schema("notes");
    let authority = server(allowed.clone());
    let bytes = capture(&authority);
    let deny = common::allow_all_policies().with_select(PolicyExpr::False);
    let denied = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(deny),
            )
            .build(),
    );
    assert_eq!(allowed.version_id(), denied.version_id());
    let bob = block_on(Db::open(config(denied, 3, author("bob")))).unwrap();
    install(&bob, &bytes).unwrap();
    insert(&bob, "notes");
    let q = bob.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(bob.all_for_identity(&q, ReadOpts::default(), author("bob"))).unwrap();
    assert!(rows.is_empty());
}

/// A persistent cache owner retains confirmed names and the requested-schema fence on reopen.
#[test]
fn catalogue_cache_persistent_reopen_preserves_metadata_and_mode() {
    let s = schema("notes");
    let authority = server(s.clone());
    let bytes = capture(&authority);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let open_at = |path: &Path| {
        block_on(Db::open_at(
            config(s.clone(), 3, author("alice")),
            path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    {
        let alice = open_at(&path);
        install(&alice, &bytes).unwrap();
        insert(&alice, "notes");
    }
    let reopened = open_at(&path);
    assert!(reopened.has_authenticated_catalogue().unwrap());
    let q = reopened.prepare_query(&Query::from("notes")).unwrap();
    assert_eq!(
        block_on(reopened.all(&q, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
    let unconfirmed = block_on(reopened.register_schema_view(schema("unknown"))).unwrap();
    assert!(!unconfirmed.has_authenticated_catalogue().unwrap());
    assert!(unconfirmed.prepare_query(&Query::from("unknown")).is_err());
}

/// A cached view cannot mutate owner metadata; retirement revokes every cached handle.
#[test]
fn catalogue_cache_wrong_view_and_retired_owner_refuse() {
    let s = schema("notes");
    let authority = server(s.clone());
    let bytes = capture(&authority);
    let alice = open(s.clone(), 3);
    let view = block_on(alice.register_schema_view(s)).unwrap();
    assert!(install(&view, &bytes).is_err());
    install(&alice, &bytes).unwrap();
    alice.retire_storage_owner().unwrap();
    assert!(alice.take_authenticated_catalogue_state().is_err());
    assert!(view.has_authenticated_catalogue().is_err());
    assert!(install(&alice, &bytes).is_err());
}

/// A query prepared before cache installation cannot bypass the requested-schema fence.
#[test]
fn catalogue_cache_prepared_before_restore_still_requires_admission() {
    let authority = server(schema("notes"));
    let bytes = capture(&authority);
    let alice = open(schema("unknown"), 3);
    let prepared = alice.prepare_query(&Query::from("unknown")).unwrap();
    install(&alice, &bytes).unwrap();
    assert!(!alice.has_authenticated_catalogue().unwrap());
    assert!(
        block_on(alice.all(&prepared, ReadOpts::default())).is_err(),
        "an old prepared handle bypassed requested-schema admission"
    );
}

/// New authority versions are captured; a stale host cache cannot erase a newer durable one.
#[test]
fn catalogue_cache_new_version_is_captured_and_stale_restore_preserves_it() {
    let s = schema("notes");
    let authority = server(s.clone());
    let old = capture(&authority);
    let extra = block_on(authority.register_schema_view(schema("extra"))).unwrap();
    assert!(extra.has_authenticated_catalogue().unwrap());
    let next = capture(&authority);
    validate_catalogue_capture_replacement(&old, &next).unwrap();
    assert!(validate_catalogue_capture_replacement(&next, &old).is_err());
    let alice = open(s, 3);
    install(&alice, &next).unwrap();
    install(&alice, &old).unwrap();
    let extra_local = block_on(alice.register_schema_view(schema("extra"))).unwrap();
    assert!(extra_local.has_authenticated_catalogue().unwrap());
    assert_eq!(
        extra_local.table_identity("extra").unwrap(),
        extra.table_identity("extra").unwrap()
    );
}

// Preserve the original public contract: opening owns a handle, while actual
// operations refuse a requested schema absent from the authenticated capsule.
fn unpublished_transaction_contract(serializable: bool) {
    let authority = server(schema("notes"));
    let bytes = capture(&authority);
    let alice = open(schema("drafts"), 8);
    let prepared = alice
        .prepare_query(&Query::from("drafts").limit(0))
        .unwrap();
    install(&alice, &bytes).unwrap();
    let id = jazz::tools::OpenTransactionId::new();
    if serializable {
        block_on(alice.begin_serializable(id)).unwrap();
    } else {
        block_on(alice.begin_mergeable(id)).unwrap();
    }
    let tx = alice.tx_ref(id);
    assert_eq!(
        block_on(tx.all_prepared(&prepared)).unwrap_err().code,
        ErrorCode::Schema
    );
    assert_eq!(
        block_on(tx.insert(
            "drafts",
            BTreeMap::from([("text".into(), Value::String("forbidden".into()))]),
            Default::default()
        ))
        .err()
        .unwrap()
        .code,
        ErrorCode::Schema
    );
    alice.abandon_transaction_handle(id).unwrap();
}
/// Alice may open a mergeable handle, but its unpublished schema refuses reads/writes.
#[test]
fn catalogue_cache_unpublished_mergeable_transaction() {
    unpublished_transaction_contract(false);
}
/// Alice may open a serializable handle, but its unpublished schema refuses reads/writes.
#[test]
fn catalogue_cache_unpublished_serializable_transaction() {
    unpublished_transaction_contract(true);
}
