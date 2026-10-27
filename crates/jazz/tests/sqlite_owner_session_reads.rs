//! Account-filtered reads through the browser's private SQLite owner.
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, RemoteLinkHint,
    SerializedReadResult, SyncMessage, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
fn schema() -> JazzSchema {
    let policy = PolicyExpr::And(vec![
        PolicyExpr::eq_session("id", vec!["user".into(), "account".into()]),
        PolicyExpr::eq_session("$createdBy.account", vec!["user".into(), "account".into()]),
    ]);
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("identities")
                    .column("name", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(policy.clone())
                            .with_insert(policy),
                    ),
            )
            .build(),
    )
    .unwrap()
}
fn config(node: u8) -> DbConfig {
    DbConfig::new(
        if node == 90 {
            schema()
        } else {
            schema().without_permissions()
        },
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: alice(),
        },
    )
}
fn open(node: u8) -> Db {
    let db = block_on(Db::open(config(node))).unwrap();
    if node != 90 {
        db.set_remote_link_hint(RemoteLinkHint::Failed);
    }
    db
}
fn connect(a: &Db, b: &Db) {
    let (up, down) = duplex();
    block_on(a.connect_upstream(up));
    b.accept_subscriber(down, alice());
}
struct MetadataGate {
    inner: Box<dyn Transport>,
    hold: Rc<Cell<bool>>,
    delayed: Rc<RefCell<VecDeque<SyncMessage>>>,
}
impl Transport for MetadataGate {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if !self.hold.get() {
            if let Some(message) = self.delayed.borrow_mut().pop_front() {
                return Some(message);
            }
            return self.inner.try_recv();
        }
        while let Some(message) = self.inner.try_recv() {
            if matches!(message, SyncMessage::CatalogueMetadataV1 { .. }) {
                return Some(message);
            }
            self.delayed.borrow_mut().push_back(message);
        }
        None
    }
}
fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("read did not finish")
}
/// Alice's account identity is visible through owner/foreground policy checks
/// after its serializable creation is globally accepted. Both browser runtimes
/// open with the public policy-free schema; only the authority supplies policies.
#[test]
fn account_provenance_policy_survives_private_owner_sync() {
    let server = open(90);
    server.set_authority();
    server.initialize_authority_catalogue().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    let tab = open(1);
    tab.set_private_owner_client().unwrap();
    connect(&owner, &server);
    connect(&tab, &owner);
    drive(&[&tab, &owner, &server], tab.wait_for_storage_owner()).unwrap();
    let tx = jazz::tools::OpenTransactionId::new();
    block_on(tab.begin_serializable(tx)).unwrap();
    block_on(tab.tx_ref(tx).insert(
        "identities",
        BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
        InsertOptions {
            row_id: Some(RowUuid(alice().account_id().unwrap().0)),
            ..Default::default()
        },
    ))
    .unwrap();
    let receipt = block_on(tab.commit_serializable_handle(tx)).unwrap().0;
    drive(
        &[&tab, &owner, &server],
        tab.wait_for_transaction(receipt, jazz::tx::DurabilityTier::Global),
    )
    .unwrap();
    for db in [&server, &owner] {
        let query = db.prepare_query(&Query::from("identities")).unwrap();
        assert_eq!(
            block_on(db.all_for_identity(
                &query,
                ReadOpts::for_read_tier(ReadTier::LocalOnly),
                alice()
            ))
            .unwrap()
            .len(),
            1
        );
    }
    let bytes = postcard::to_allocvec(&Query::from("identities")).unwrap();
    let read = drive(
        &[&tab, &owner, &server],
        tab.all_serialized_query_as(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let SerializedReadResult::Rows(rows) = read else {
        panic!("expected rows")
    };
    assert_eq!(rows.len(), 1);
    block_on(tab.close()).unwrap();
    block_on(owner.close()).unwrap();
    drop(tab);
    drop(owner);
    let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(local_count(&owner), 1);
    let bob = AuthorSubject::for_test_bytes([2; 16]);
    let query = owner.prepare_query(&Query::from("identities")).unwrap();
    assert!(
        block_on(owner.all_for_identity(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly), bob))
            .is_err()
    );
}

/// Alice's server changes policies without changing the structural schema UUID.
/// A private owner must adopt the newer revision, retain it after restart, and
/// never accept two policy meanings at the same revision.
#[test]
fn policy_revision_revokes_cached_reads_and_survives_restart() {
    use jazz::protocol::{
        CatalogueSnapshot, CurrentWriteSchema, PhysicalIdentityManifest, SchemaVersion,
    };
    let allowed = schema();
    let mut snapshot = CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&allowed),
        schemas: vec![SchemaVersion::new(allowed.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: allowed.version_id(),
        },
    };
    let server = open(90);
    server.set_authority();
    server.install_authority_catalogue(&snapshot).unwrap();
    block_on(server.insert(
        "identities",
        BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
        InsertOptions {
            row_id: Some(RowUuid(alice().account_id().unwrap().0)),
            ..Default::default()
        },
    ))
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    connect(&owner, &server);
    let bytes = postcard::to_allocvec(&Query::from("identities")).unwrap();
    let read = drive(
        &[&owner, &server],
        owner.all_serialized_query_as(
            &bytes,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
            false,
            || false,
            |_| {},
        ),
    )
    .unwrap();
    let SerializedReadResult::Rows(rows) = read else {
        panic!("expected rows")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(local_count(&owner), 1);
    block_on(owner.close()).unwrap();
    drop(owner);
    let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(local_count(&owner), 1);
    let denied = allowed.without_permissions();
    assert_eq!(allowed.version_id(), denied.version_id());
    snapshot.schemas = vec![SchemaVersion::new(denied.clone())];
    assert!(server.install_authority_catalogue(&snapshot).is_err());
    snapshot.current_write_schema.revision += 1;
    block_on(server.activate_schema(denied)).unwrap();
    server.install_authority_catalogue(&snapshot).unwrap();
    let (up, down) = duplex();
    let hold = Rc::new(Cell::new(true));
    let delayed = Rc::new(RefCell::new(VecDeque::new()));
    block_on(owner.connect_upstream(Box::new(MetadataGate {
        inner: up,
        hold: hold.clone(),
        delayed: delayed.clone(),
    })));
    server.accept_subscriber(down, alice());
    for _ in 0..12 {
        block_on(server.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    assert!(
        delayed
            .borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::PolicyDescriptorV1(_)))
    );
    // The new head alone removes the old grant, before either the new policy
    // descriptor or an ordinary upstream row eviction can arrive.
    assert_eq!(local_count(&owner), 0);
    let query = owner.prepare_query(&Query::from("identities")).unwrap();
    assert_eq!(
        owner.read(&query).unwrap().len(),
        1,
        "cached row is still physically present"
    );
    hold.set(false);
    for _ in 0..12 {
        block_on(server.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    assert_eq!(local_count(&owner), 0);
    block_on(owner.close()).unwrap();
    drop(owner);
    let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(local_count(&owner), 0);
}
fn local_count(db: &Db) -> usize {
    let query = db.prepare_query(&Query::from("identities")).unwrap();
    block_on(db.all_for_identity(
        &query,
        ReadOpts::for_read_tier(ReadTier::LocalOnly),
        alice(),
    ))
    .unwrap()
    .len()
}

/// Mallory cannot rebind Alice's authority policy descriptor to another head,
/// schema or revision. Failure rolls back the accompanying catalogue metadata.
#[test]
fn mismatched_policy_metadata_rolls_back_with_its_catalogue() {
    use jazz::db::{CatalogueHeadV1, CatalogueSchemaV1, PolicyDescriptorV1};
    use jazz::protocol::{PhysicalIdentityManifest, SchemaVersion};
    let full = schema();
    let identities = PhysicalIdentityManifest::allocate(&full);
    let root = *blake3::Hasher::new_derive_key("jazz.sqlite.catalogue-namespace.v1")
        .update(&postcard::to_allocvec(&(full.version_id(), &identities)).unwrap())
        .finalize()
        .as_bytes();
    let head = CatalogueHeadV1 {
        root,
        through: 0,
        revision: 1,
        current: full.version_id(),
    };
    let structural = CatalogueSchemaV1 {
        schema: SchemaVersion::new(full.without_permissions()),
        identities,
    };
    for case in ["namespace", "revision", "current", "schema", "conflict"] {
        let owner = open(50);
        owner.set_scope_isolated_storage_owner().unwrap();
        let (up, mut server) = duplex();
        block_on(owner.connect_upstream(up));
        server
            .send(SyncMessage::CatalogueMetadataV1 {
                head: head.clone(),
                schemas: vec![structural.clone()],
            })
            .unwrap();
        let mut descriptor = PolicyDescriptorV1 {
            head: head.clone(),
            schema: SchemaVersion::new(full.clone()),
        };
        match case {
            "namespace" => descriptor.head.root = [0x44; 32],
            "revision" => descriptor.head.revision += 1,
            "current" => {
                descriptor.head.current =
                    jazz::ids::SchemaVersionId(uuid::Uuid::from_bytes([0x55; 16]))
            }
            "schema" => {
                descriptor.schema.id =
                    jazz::ids::SchemaVersionId(uuid::Uuid::from_bytes([0x55; 16]))
            }
            "conflict" => {
                server
                    .send(SyncMessage::PolicyDescriptorV1(descriptor.clone()))
                    .unwrap();
                descriptor.schema = SchemaVersion::new(full.without_permissions());
            }
            _ => unreachable!(),
        }
        server
            .send(SyncMessage::PolicyDescriptorV1(descriptor))
            .unwrap();
        assert!(block_on(owner.tick()).is_err(), "{case}");
        assert!(
            owner.table_identity("identities").is_err(),
            "{case}: metadata escaped rollback"
        );
    }
}

/// Alice's own pending row is visible under the admitted read policy. A newer
/// catalogue head must revoke it before the replacement policy arrives; a
/// stale descriptor cannot re-enable cold-owner previews, including on reopen.
#[test]
fn pending_previews_stay_revoked_during_split_policy_update() {
    use jazz::protocol::{
        CatalogueSnapshot, CurrentWriteSchema, PhysicalIdentityManifest, SchemaVersion,
    };
    let allowed = schema();
    let mut snapshot = CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&allowed),
        schemas: vec![SchemaVersion::new(allowed.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: allowed.version_id(),
        },
    };
    let server = open(90);
    server.set_authority();
    server.install_authority_catalogue(&snapshot).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pending-policy.sqlite");
    let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    let (up, down) = duplex();
    let connection = block_on(owner.connect_upstream(up));
    server.accept_subscriber(down, alice());
    for _ in 0..12 {
        block_on(server.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    assert!(owner.detach_connection(&connection));
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    let write = block_on(owner.insert(
        "identities",
        BTreeMap::from([("name".into(), Value::String("Alice pending".into()))]),
        InsertOptions {
            row_id: Some(RowUuid(alice().account_id().unwrap().0)),
            ..Default::default()
        },
    ))
    .unwrap();
    assert_eq!(local_count(&owner), 1);
    let denied = allowed.without_permissions();
    snapshot.schemas = vec![SchemaVersion::new(denied.clone())];
    snapshot.current_write_schema.revision += 1;
    block_on(server.activate_schema(denied)).unwrap();
    server.install_authority_catalogue(&snapshot).unwrap();
    let (up, down) = duplex();
    let delayed = Rc::new(RefCell::new(VecDeque::new()));
    block_on(owner.connect_upstream(Box::new(MetadataGate {
        inner: up,
        hold: Rc::new(Cell::new(true)),
        delayed: delayed.clone(),
    })));
    server.accept_subscriber(down, alice());
    for _ in 0..12 {
        block_on(server.tick()).unwrap();
        block_on(owner.tick()).unwrap();
    }
    assert!(
        delayed
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::PolicyDescriptorV1(_)))
    );
    assert_eq!(
        owner.write_state(write.mergeable_tx_id()).unwrap().fate,
        jazz::tx::Fate::Pending
    );
    assert_eq!(local_count(&owner), 0);
    let query = owner.prepare_query(&Query::from("identities")).unwrap();
    assert_eq!(
        owner.read(&query).unwrap().len(),
        1,
        "the pending row remains physically present"
    );
    block_on(owner.close()).unwrap();
    drop(owner);
    let owner = block_on(Db::open_at(config(50), &path, Default::default())).unwrap();
    owner.set_scope_isolated_storage_owner().unwrap();
    owner.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(local_count(&owner), 0);
}
