//! Binding-facing private initialization capability regressions.

use super::*;

fn initialization_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::True)
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                        .with_delete(PublicPolicyExpr::True),
                )
                .column("title", PublicColumnType::Text),
        ),
    )
}

fn initialization_owner(
    storage: groove::storage::TestStorage,
    author: AuthorSubject,
    node: u8,
) -> Db {
    block_on(Box::pin(Db::open(DbConfig::new(
        initialization_schema(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    ))))
    .unwrap()
}

fn initialization_storage() -> groove::storage::TestStorage {
    let families = initialization_schema().column_families();
    groove::storage::TestStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>())
}

async fn stage_initialization(db: &Db, row: RowUuid) -> OpenTransactionId {
    let open = OpenTransactionId::new();
    db.begin_exclusive(open).await.unwrap();
    db.prepare_initialization_insert(open, "todos", row)
        .await
        .unwrap();
    db.exclusive_tx_ref(open)
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("sealed".to_owned()))]),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    open
}

#[test]
fn initialization_seal_freezes_without_publication_and_recovers_exact_local_identity() {
    let storage = initialization_storage();
    let author = AuthorSubject::for_test_bytes([0xe1; 16]);
    let db = initialization_owner(storage.clone(), author, 0xe1);
    let open = block_on(stage_initialization(&db, row(0xe1)));
    let seal = block_on(db.seal_initialization_transaction(open)).unwrap();
    let reserved = seal.reservation();
    assert_eq!(ReservedTxId::decode(&reserved.encode()).unwrap(), reserved);
    assert_eq!(
        block_on(db.initialization_transaction_status(&[reserved])).unwrap(),
        vec![InitializationTransactionStatus::NotObserved]
    );
    assert!(block_on(db.exclusive_tx_ref(open).read("todos", row(0xe1))).is_err());
    assert!(block_on(db.commit_exclusive_handle(open)).is_err());
    assert!(block_on(db.seal_initialization_transaction(open)).is_err());
    let write = block_on(db.publish_initialization_transaction(seal)).unwrap();
    for _ in 0..16 {
        db.tick().unwrap();
    }
    let committed = block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert_eq!(ReservedTxId(committed), reserved);
    assert_eq!(
        block_on(db.initialization_transaction_status(&[reserved])).unwrap(),
        vec![InitializationTransactionStatus::Complete {
            fate: Fate::Pending,
            durability: DurabilityTier::Local
        }]
    );
    block_on(db.close()).unwrap();
    drop(write);
    drop(db);
    let reopened = initialization_owner(storage, author, 0xe1);
    assert_eq!(
        block_on(reopened.initialization_transaction_status(&[reserved])).unwrap(),
        vec![InitializationTransactionStatus::Complete {
            fate: Fate::Pending,
            durability: DurabilityTier::Local
        }]
    );
    let query = reopened.prepare_query(&reopened.table("todos")).unwrap();
    let rows = block_on(reopened.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(rows[0].row_uuid(), row(0xe1));
    assert_eq!(rows[0].cell_at(0), Some(Value::String("sealed".to_owned())));
}

#[test]
fn initialization_cancel_and_wrong_owner_never_publish_or_reuse_reservation() {
    let author = AuthorSubject::for_test_bytes([0xe2; 16]);
    let db = initialization_owner(initialization_storage(), author, 0xe2);
    let other = initialization_owner(initialization_storage(), author, 0xe3);
    let first = block_on(stage_initialization(&db, row(0xe2)));
    let seal = block_on(db.seal_initialization_transaction(first)).unwrap();
    let cancelled = seal.reservation();
    block_on(db.cancel_initialization_transaction(seal)).unwrap();
    assert!(block_on(db.begin_exclusive(first)).is_err());
    let second = block_on(stage_initialization(&db, row(0xe3)));
    let seal = block_on(db.seal_initialization_transaction(second)).unwrap();
    let next = seal.reservation();
    assert_ne!(cancelled, next);
    assert!(block_on(other.publish_initialization_transaction(seal)).is_err());
    for _ in 0..16 {
        db.tick().unwrap();
    }
    assert_eq!(
        block_on(db.initialization_transaction_status(&[cancelled, next])).unwrap(),
        vec![InitializationTransactionStatus::NotObserved; 2]
    );
    assert!(block_on(db.exclusive_tx_ref(second).read("todos", row(0xe3))).is_err());
}

#[test]
fn initialization_exact_absence_rejects_existing_and_staged_insert_coordinates() {
    let author = AuthorSubject::for_test_bytes([0xe4; 16]);
    let db = initialization_owner(initialization_storage(), author, 0xe4);
    let open = block_on(stage_initialization(&db, row(0xe4)));
    assert!(block_on(db.prepare_initialization_insert(open, "todos", row(0xe4))).is_err());
    let committed = block_on(db.commit_exclusive_handle(open)).unwrap();
    assert_eq!(
        db.write_state(committed).unwrap().durability,
        DurabilityTier::Local
    );
    let later = OpenTransactionId::new();
    block_on(db.begin_exclusive(later)).unwrap();
    assert!(block_on(db.prepare_initialization_insert(later, "todos", row(0xe4))).is_err());
    let observed = block_on(db.exclusive_tx_ref(later).read("todos", row(0xe4)))
        .unwrap()
        .unwrap();
    assert_eq!(
        observed.get("title"),
        Some(&Value::String("sealed".to_owned()))
    );
    db.abandon_transaction_handle(later).unwrap();
    block_on(db.delete("todos", row(0xe4), Default::default())).unwrap();
    let deleted = OpenTransactionId::new();
    block_on(db.begin_exclusive(deleted)).unwrap();
    assert!(block_on(db.prepare_initialization_insert(deleted, "todos", row(0xe4))).is_err());
}

#[test]
fn initialization_status_is_bounded_author_scoped_and_rejects_unbound_relay() {
    let storage = initialization_storage();
    let author = AuthorSubject::for_test_bytes([0xe5; 16]);
    let db = initialization_owner(storage.clone(), author, 0xe5);
    let open = block_on(stage_initialization(&db, row(0xe5)));
    let id = ReservedTxId(block_on(db.commit_exclusive_handle(open)).unwrap());
    assert!(block_on(db.initialization_transaction_status(&[id; 65])).is_err());
    block_on(db.close()).unwrap();
    drop(db);
    let foreign = initialization_owner(storage, AuthorSubject::for_test_bytes([0xe6; 16]), 0xe6);
    let missing =
        ReservedTxId::decode("jazz-init-v1:eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee:1:0").unwrap();
    assert_eq!(
        block_on(foreign.initialization_transaction_status(&[id, missing])).unwrap(),
        vec![InitializationTransactionStatus::NotObserved; 2]
    );
    foreign.set_relay_authority_session_owner_for_test();
    assert!(block_on(foreign.initialization_transaction_status(&[id])).is_err());
}

#[test]
fn initialization_status_propagates_storage_failure_instead_of_missing() {
    let schema = initialization_schema();
    let families = schema.column_families();
    let (storage, control) = groove::storage::TestStorage::controlled(
        &families.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let eviction = storage.clone();
    let db = initialization_owner(storage, AuthorSubject::for_test_bytes([0xe7; 16]), 0xe7);
    let open = block_on(stage_initialization(&db, row(0xe7)));
    let id = ReservedTxId(block_on(db.commit_exclusive_handle(open)).unwrap());
    eviction.evict_all();
    control.fail_next(groove::storage::TestStorageOperation::Get);
    let error = block_on(db.initialization_transaction_status(&[id])).unwrap_err();
    assert!(
        error.to_string().contains("injected Get failure"),
        "{error}"
    );
}

#[test]
fn initialization_absence_propagates_storage_failure() {
    let families = initialization_schema().column_families();
    let (storage, control) = groove::storage::TestStorage::controlled(
        &families.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let eviction = storage.clone();
    let db = initialization_owner(storage, AuthorSubject::for_test_bytes([0xe8; 16]), 0xe8);
    let open = block_on(stage_initialization(&db, row(0xe8)));
    block_on(db.commit_exclusive_handle(open)).unwrap();
    let next = OpenTransactionId::new();
    block_on(db.begin_exclusive(next)).unwrap();
    eviction.evict_all();
    control.fail_next(groove::storage::TestStorageOperation::Get);
    let error = block_on(db.prepare_initialization_insert(next, "todos", row(0xe8))).unwrap_err();
    assert!(
        error.to_string().contains("injected Get failure"),
        "{error}"
    );
}

fn authenticated_initialization_owner(authority_node: u8) -> Db {
    let schema = initialization_schema();
    let author = AuthorSubject::for_test_bytes([0xe9; 16]);
    let db = initialization_owner(initialization_storage(), author, 0xe9);
    let state = block_on(db.take_authenticated_catalogue_state()).unwrap();
    assert!(!state.ready);
    assert!(state.capture.is_none());
    let authority = open_core(authority_node, AuthorSubject::SYSTEM, &schema);
    let (upstream, downstream) = duplex_with_admitted_session_context(
        author,
        db.identity().node,
        1,
        NodeUuid::from_bytes([authority_node; 16]),
        2,
    );
    block_on(db.connect_upstream(upstream));
    let peer = authority.accept_subscriber(downstream, author);
    for _ in 0..32 {
        db.tick().unwrap();
        peer.borrow_mut().tick().unwrap();
    }
    db
}

fn authenticated_initialization_catalogue(authority_node: u8) -> Vec<u8> {
    let db = authenticated_initialization_owner(authority_node);
    let state = block_on(db.take_authenticated_catalogue_state()).unwrap();
    assert!(state.ready);
    let capture = state
        .capture
        .expect("authenticated authority snapshot captured");
    let drained = block_on(db.take_authenticated_catalogue_state()).unwrap();
    assert!(drained.ready);
    assert!(drained.capture.is_none(), "capture drains once");
    capture
}

#[test]
fn initialization_catalogue_observation_waits_for_owner_and_drains_once() {
    // Internal binding seam: applications cannot hold the native node owner,
    // but bindings must observe Pending rather than false readiness or panic.
    let db = authenticated_initialization_owner(0xf7);
    let owner = block_on(db.node.node.lock());
    let mut observation = Box::pin(db.take_authenticated_catalogue_state());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(observation.as_mut().poll(&mut context).is_pending());
    drop(owner);
    let state = block_on(observation).unwrap();
    assert!(state.ready);
    let capture = state
        .capture
        .expect("pending observation retains the authenticated capture");
    assert_eq!(&capture[17..33], &[0xf7; 16]);
    let drained = block_on(db.take_authenticated_catalogue_state()).unwrap();
    assert!(drained.ready);
    assert!(drained.capture.is_none());
}

#[test]
fn initialization_catalogue_installs_authority_identity_before_fresh_bootstrap() {
    let cache = authenticated_initialization_catalogue(0xea);
    // Pin the new envelope prefix/source bytes; the payload remains covered by
    // the existing canonical wire-v3 codec corpus.
    assert_eq!(&cache[..17], b"JAZZ-CATALOGUE\0\x01\x03");
    assert_eq!(&cache[17..33], &[0xea; 16]);
    let expected = super::super::initialization::decode_catalogue_capture(&cache).unwrap();
    let storage = initialization_storage();
    let config = || {
        DbConfig::new(
            initialization_schema(),
            storage.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xeb; 16]),
                author: AuthorSubject::for_test_bytes([0xeb; 16]),
            },
        )
    };
    // SAFETY: this fixture obtained the bytes from its admitted authority link.
    let db = block_on(Box::pin(unsafe {
        Db::open_with_cached_catalogue(config(), Some(&cache))
    }))
    .unwrap();
    let state = block_on(db.take_authenticated_catalogue_state()).unwrap();
    assert!(state.ready);
    assert!(state.capture.is_none(), "install is not a new capture");
    let table = db
        .catalogue_table_identity(initialization_schema().version_id(), "todos")
        .unwrap();
    // Internal catalogue seam: public table identity exposes only one UUID,
    // not the complete genesis manifest whose exact installation is required.
    let received = db.node.node.borrow().catalogue_snapshot().unwrap();
    assert_eq!(
        received.genesis_physical_identities,
        expected.genesis_physical_identities
    );
    let write = block_on(db.insert(
        "todos",
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("survives rotation".to_owned()),
        )]),
        InsertOptions {
            row_id: Some(row(0xeb)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    drop(write);
    block_on(db.close()).unwrap();
    drop(db);
    let rotated = authenticated_initialization_catalogue(0xec);
    let rotated_snapshot =
        super::super::initialization::decode_catalogue_capture(&rotated).unwrap();
    assert_ne!(
        rotated_snapshot.genesis_physical_identities,
        expected.genesis_physical_identities
    );
    let mut incompatible = rotated_snapshot.clone();
    let identities = incompatible
        .genesis_physical_identities
        .tables
        .get_mut("todos")
        .unwrap();
    std::mem::swap(
        &mut identities.id.0,
        &mut identities.columns.get_mut("title").unwrap().id.0,
    );
    let incompatible = super::super::initialization::encode_catalogue_capture(
        NodeUuid::from_bytes([0xec; 16]),
        &SyncMessage::CatalogueSnapshot(Box::new(incompatible)),
    )
    .unwrap();
    assert!(validate_catalogue_capture_replacement(&rotated, &incompatible).is_err());
    // This malformed cache must not overwrite the existing root.
    let mut missing = rotated_snapshot;
    missing
        .genesis_physical_identities
        .tables
        .get_mut("todos")
        .unwrap()
        .columns
        .clear();
    let missing = super::super::initialization::encode_catalogue_capture(
        NodeUuid::from_bytes([0xec; 16]),
        &SyncMessage::CatalogueSnapshot(Box::new(missing)),
    )
    .unwrap();
    assert!(validate_catalogue_capture_replacement(&cache, &missing).is_err());
    assert!(
        block_on(Box::pin(unsafe {
            Db::open_with_cached_catalogue(config(), Some(&missing))
        }))
        .is_err()
    );
    validate_catalogue_capture_replacement(&cache, &rotated).unwrap();
    let reopened = block_on(Box::pin(unsafe {
        Db::open_with_cached_catalogue(config(), Some(&rotated))
    }))
    .unwrap();
    assert!(
        block_on(reopened.take_authenticated_catalogue_state())
            .unwrap()
            .ready
    );
    assert_eq!(
        reopened.node.node.borrow().catalogue_snapshot().unwrap(),
        received,
        "validating another authority's genesis coordinates must not mutate the existing root"
    );
    assert_eq!(
        reopened
            .catalogue_table_identity(initialization_schema().version_id(), "todos")
            .unwrap(),
        table
    );
    let query = reopened.prepare_query(&reopened.table("todos")).unwrap();
    let rows = block_on(reopened.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(
        rows.iter()
            .map(|value| value.row_uuid())
            .collect::<Vec<_>>(),
        vec![row(0xeb)]
    );
    assert_eq!(
        rows[0].cell_at(0),
        Some(Value::String("survives rotation".to_owned()))
    );
}

#[test]
fn initialization_cache_replacement_preserves_immutable_lineage_receipts() {
    use crate::protocol::{
        LensOp, MigrationLens, PhysicalIdentityManifest, SchemaLineagePublication, SchemaVersion,
        TableLens,
    };
    // Internal cache seam: public application reads cannot submit conflicting
    // catalogue histories or observe their canonical publication identities.
    let cache = authenticated_initialization_catalogue(0xf8);
    let mut snapshot = super::super::initialization::decode_catalogue_capture(&cache).unwrap();
    let base = initialization_schema();
    let evolved = SchemaVersion::new(build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::True)
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                        .with_delete(PublicPolicyExpr::True),
                )
                .column("title", PublicColumnType::Text)
                .column("body", PublicColumnType::Text),
        ),
    ));
    let lens = MigrationLens::new(
        base.version_id(),
        evolved.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .unwrap();
    let publication = SchemaLineagePublication::author_from_prior(
        &base,
        &snapshot.genesis_physical_identities,
        evolved.clone(),
        lens,
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot.schemas.push(evolved.clone());
    snapshot.lineages.push((1, publication));
    snapshot.current_write_schema.revision += 1;
    snapshot.current_write_schema.schema = evolved.id;
    let encode = |snapshot| {
        super::super::initialization::encode_catalogue_capture(
            NodeUuid::from_bytes([0xf8; 16]),
            &SyncMessage::CatalogueSnapshot(Box::new(snapshot)),
        )
        .unwrap()
    };
    let current = encode(snapshot.clone());
    validate_catalogue_capture_replacement(&cache, &current).unwrap();
    assert!(validate_catalogue_capture_replacement(&current, &cache).is_err());

    let storage = initialization_storage();
    let config = || {
        DbConfig::new(
            base.clone(),
            storage.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xf9; 16]),
                author: AuthorSubject::for_test_bytes([0xf9; 16]),
            },
        )
    };
    // SAFETY: fixture constructs an authority publication from the admitted
    // capture using the same catalogue authoring routine as live ingestion.
    let db = block_on(Box::pin(unsafe {
        Db::open_with_cached_catalogue(config(), Some(&current))
    }))
    .unwrap();
    let before = db.node.node.borrow().catalogue_snapshot().unwrap();
    block_on(db.close()).unwrap();
    drop(db);

    let mut changed_new_identity = snapshot.clone();
    let publication = &mut changed_new_identity.lineages[0].1;
    publication
        .physical_identities
        .tables
        .get_mut("todos")
        .unwrap()
        .columns
        .get_mut("body")
        .unwrap()
        .id
        .0 = uuid::Uuid::from_u128(0xfeed);
    publication.id = publication.content_id();

    let mut changed_inherited_identity = snapshot.clone();
    changed_inherited_identity.genesis_physical_identities =
        PhysicalIdentityManifest::allocate(&base);
    let genesis = &changed_inherited_identity
        .genesis_physical_identities
        .tables["todos"];
    let publication = &mut changed_inherited_identity.lineages[0].1;
    let identities = publication
        .physical_identities
        .tables
        .get_mut("todos")
        .unwrap();
    identities.id = genesis.id;
    identities
        .columns
        .insert("title".to_owned(), genesis.columns["title"].clone());
    publication.id = publication.content_id();

    let mut changed_lens = snapshot.clone();
    let publication = &mut changed_lens.lineages[0].1;
    publication.lens.table_lenses[0].ops[0] = LensOp::AddColumn {
        column: "body".to_owned(),
        default: Value::String("changed".to_owned()),
    };
    publication.lens.id = publication.lens.content_id();
    publication.id = publication.content_id();

    let mut changed_partition = snapshot.clone();
    let publication = &mut changed_partition.lineages[0].1;
    publication.new_tables.push("todos".to_owned());
    publication.id = publication.content_id();
    let mut changed_sequence = snapshot;
    changed_sequence.lineages[0].0 = 2;

    for incompatible in [
        changed_new_identity,
        changed_inherited_identity,
        changed_lens,
        changed_partition,
        changed_sequence,
    ] {
        let incompatible = encode(incompatible);
        assert!(validate_catalogue_capture_replacement(&current, &incompatible).is_err());
        assert!(
            block_on(Box::pin(unsafe {
                Db::open_with_cached_catalogue(config(), Some(&incompatible))
            }))
            .is_err()
        );
    }
    let reopened = block_on(Box::pin(unsafe {
        Db::open_with_cached_catalogue(config(), Some(&current))
    }))
    .unwrap();
    assert_eq!(
        reopened.node.node.borrow().catalogue_snapshot().unwrap(),
        before
    );
}

#[test]
fn initialization_private_encodings_reject_malformed_and_regressing_captures() {
    let literal = "jazz-init-v1:00112233-4455-6677-8899-aabbccddeeff:42:7";
    assert_eq!(ReservedTxId::decode(literal).unwrap().encode(), literal);
    for invalid in [
        "jazz-init-v2:00112233-4455-6677-8899-aabbccddeeff:42:7",
        "jazz-init-v1:00112233-4455-6677-8899-aabbccddeeff:042:7",
        "jazz-init-v1:00112233-4455-6677-8899-aabbccddeeff:42:4294967295",
        "jazz-init-v1:00112233-4455-6677-8899-aabbccddeeff:18446744073709551615:0",
    ] {
        assert!(ReservedTxId::decode(invalid).is_err());
    }
    let cache = authenticated_initialization_catalogue(0xed);
    let mut trailing = cache.clone();
    trailing.push(0);
    assert!(super::super::initialization::decode_catalogue_capture(&trailing).is_err());
    let mut unknown = cache.clone();
    unknown[15] = 2;
    assert!(super::super::initialization::decode_catalogue_capture(&unknown).is_err());
    assert!(super::super::initialization::decode_catalogue_capture(&cache[..33]).is_err());
    let mut newer = super::super::initialization::decode_catalogue_capture(&cache).unwrap();
    newer.current_write_schema.revision += 1;
    let newer = super::super::initialization::encode_catalogue_capture(
        NodeUuid::from_bytes([0xed; 16]),
        &SyncMessage::CatalogueSnapshot(Box::new(newer)),
    )
    .unwrap();
    validate_catalogue_capture_replacement(&cache, &newer).unwrap();
    assert!(validate_catalogue_capture_replacement(&newer, &cache).is_err());
}

#[test]
fn initialization_status_preserves_terminal_rejection_after_payload_removal() {
    let author = AuthorSubject::for_test_bytes([0xee; 16]);
    let db = initialization_owner(initialization_storage(), author, 0xee);
    let open = block_on(stage_initialization(&db, row(0xee)));
    let id = ReservedTxId(block_on(db.commit_exclusive_handle(open)).unwrap());
    let (upstream, mut authority) = duplex();
    block_on(db.connect_upstream(upstream));
    authority
        .send(SyncMessage::FateUpdate {
            tx_id: id.0,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    for _ in 0..16 {
        db.tick().unwrap();
    }
    assert_eq!(
        block_on(db.initialization_transaction_status(&[id])).unwrap(),
        vec![InitializationTransactionStatus::Complete {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            durability: DurabilityTier::Global,
        }],
    );
    let query = db.prepare_query(&db.table("todos")).unwrap();
    assert!(
        block_on(db.all(&query, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn initialization_stale_cache_validates_without_rewinding_existing_catalogue() {
    let schema = initialization_schema();
    let author = AuthorSubject::for_test_bytes([0xf1; 16]);
    let warm = initialization_owner(initialization_storage(), author, 0xf1);
    let authority = open_core(0xf2, AuthorSubject::SYSTEM, &schema);
    let (upstream, downstream) = duplex_with_admitted_session_context(
        author,
        warm.identity().node,
        1,
        NodeUuid::from_bytes([0xf2; 16]),
        2,
    );
    let upstream = block_on(warm.connect_upstream(upstream));
    let peer = authority.accept_subscriber(downstream, author);
    for _ in 0..32 {
        warm.tick().unwrap();
        peer.borrow_mut().tick().unwrap();
    }
    let stale = block_on(warm.take_authenticated_catalogue_state())
        .unwrap()
        .capture
        .unwrap();
    authority
        .activate_schema_for_test(2, schema.clone())
        .unwrap();
    assert!(warm.detach_connection(&upstream));
    assert!(authority.server.detach_connection(&peer));
    let (upstream, downstream) = duplex_with_admitted_session_context(
        author,
        warm.identity().node,
        3,
        NodeUuid::from_bytes([0xf2; 16]),
        4,
    );
    block_on(warm.connect_upstream(upstream));
    let peer = authority.accept_subscriber(downstream, author);
    for _ in 0..32 {
        warm.tick().unwrap();
        peer.borrow_mut().tick().unwrap();
    }
    let current = block_on(warm.take_authenticated_catalogue_state())
        .unwrap()
        .capture
        .unwrap();
    let storage = initialization_storage();
    let config = || {
        DbConfig::new(
            schema.clone(),
            storage.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xf3; 16]),
                author,
            },
        )
    };
    // SAFETY: both captures came from the same admitted authority scope.
    let db = block_on(Box::pin(unsafe {
        Db::open_with_cached_catalogue(config(), Some(&current))
    }))
    .unwrap();
    // Internal catalogue seam: readiness and individual public identities cannot
    // prove that every retained lineage and write-pointer field stayed unchanged.
    let before = db.node.node.borrow().catalogue_snapshot().unwrap();
    assert_eq!(before.current_write_schema.revision, 2);
    block_on(db.close()).unwrap();
    drop(db);
    let mut malformed = super::super::initialization::decode_catalogue_capture(&stale).unwrap();
    malformed.schemas.push(malformed.schemas[0].clone());
    let malformed = super::super::initialization::encode_catalogue_capture(
        NodeUuid::from_bytes([0xf2; 16]),
        &SyncMessage::CatalogueSnapshot(Box::new(malformed)),
    )
    .unwrap();
    assert!(
        block_on(Box::pin(unsafe {
            Db::open_with_cached_catalogue(config(), Some(&malformed))
        }))
        .is_err()
    );
    let reopened = block_on(Box::pin(unsafe {
        Db::open_with_cached_catalogue(config(), Some(&stale))
    }))
    .unwrap();
    assert!(
        block_on(reopened.take_authenticated_catalogue_state())
            .unwrap()
            .ready
    );
    assert_eq!(
        reopened.node.node.borrow().catalogue_snapshot().unwrap(),
        before
    );
}

async fn stage_initialization_update(db: &Db, target: RowUuid, title: &str) -> TxId {
    let open = OpenTransactionId::new();
    db.begin_exclusive(open).await.unwrap();
    db.exclusive_tx_ref(open)
        .update(
            "todos",
            target,
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            Default::default(),
        )
        .await
        .unwrap();
    db.commit_exclusive_handle(open).await.unwrap()
}

#[test]
fn initialization_owner_replays_pending_child_after_global_parent_eviction_and_lease_rotation() {
    // Internal eviction/journal seam: public rows cannot prove an ancestor's
    // payload is absent, and receipts omit the exact proof replay must preserve.
    let storage = initialization_storage();
    let author = AuthorSubject::for_test_bytes([0xf4; 16]);
    let db = initialization_owner(storage.clone(), author, 0xf4);
    let core = open_core(0xf5, AuthorSubject::SYSTEM, &initialization_schema());
    let (upstream, downstream) = duplex();
    let upstream = block_on(db.connect_upstream(upstream));
    let downstream = core.accept_subscriber(downstream, author);
    let open = block_on(stage_initialization(&db, row(0xf4)));
    let parent = block_on(db.commit_exclusive_handle(open)).unwrap();
    for _ in 0..32 {
        db.tick().unwrap();
        core.tick().unwrap();
    }
    assert_eq!(db.write_state(parent).unwrap().fate, Fate::Accepted);
    assert_eq!(
        db.write_state(parent).unwrap().durability,
        DurabilityTier::Global
    );
    assert!(db.detach_connection(&upstream));
    assert!(core.server.detach_connection(&downstream));
    let child = block_on(stage_initialization_update(
        &db,
        row(0xf4),
        "after evicted parent",
    ));
    let original = block_on(db.node.node.borrow_mut().commit_unit_for(child)).unwrap();
    block_on(db.node.node.borrow_mut().evict_cold()).unwrap();
    let SyncMessage::CommitUnit { versions, .. } =
        block_on(db.node.node.borrow_mut().commit_unit_for(parent)).unwrap()
    else {
        unreachable!()
    };
    assert!(
        versions.is_empty(),
        "accepted parent payload is genuinely cold-evicted"
    );
    block_on(db.close()).unwrap();
    drop(db);
    let reopened = initialization_owner(storage, author, 0xf6);
    // SAFETY: this fixture owns the same author's durable root across a retired lease.
    block_on(unsafe { reopened.restore_initialization_owner_pending_uploads() }).unwrap();
    let (upstream, downstream, sent) = duplex_with_client_outbound_tap();
    block_on(reopened.connect_upstream(upstream));
    let _downstream = core.accept_subscriber(downstream, author);
    let mut observed = None;
    for _ in 0..32 {
        reopened.tick().unwrap();
        if let Some(unit) = sent.borrow().iter().find(|message| {
            matches!(
                message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == child
            )
        }) {
            observed = Some(unit.clone());
        }
        core.tick().unwrap();
    }
    assert_eq!(observed, Some(original));
    assert_eq!(reopened.write_state(child).unwrap().fate, Fate::Accepted);
    assert_eq!(
        reopened.write_state(child).unwrap().durability,
        DurabilityTier::Global
    );
}

#[test]
fn initialization_owner_replays_pending_parent_before_exact_exclusive_child() {
    let storage = initialization_storage();
    let author = AuthorSubject::for_test_bytes([0xf7; 16]);
    let db = initialization_owner(storage.clone(), author, 0xf7);
    let open = block_on(stage_initialization(&db, row(0xf7)));
    let parent = block_on(db.commit_exclusive_handle(open)).unwrap();
    let child = block_on(stage_initialization_update(
        &db,
        row(0xf7),
        "second pending unit",
    ));
    // Internal journal seam: public receipts omit exclusive read evidence.
    // Capture complete authored units to compare against actual replayed frames.
    let original_parent = block_on(db.node.node.borrow_mut().commit_unit_for(parent)).unwrap();
    let original_child = block_on(db.node.node.borrow_mut().commit_unit_for(child)).unwrap();
    block_on(db.close()).unwrap();
    drop(db);
    let reopened = initialization_owner(storage, author, 0xf8);
    // SAFETY: direct owner startup, same admitted author and retired prior lease.
    block_on(unsafe { reopened.restore_initialization_owner_pending_uploads() }).unwrap();
    let (upstream, _downstream, sent) = duplex_with_client_outbound_tap();
    block_on(reopened.connect_upstream(upstream));
    for _ in 0..8 {
        reopened.tick().unwrap();
    }
    let replayed = sent.borrow().iter().filter(|message| matches!(
        message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == parent || tx.tx_id == child
    )).cloned().collect::<Vec<_>>();
    assert_eq!(replayed, vec![original_parent, original_child]);
}

#[test]
fn initialization_owner_refuses_incomplete_pending_recovery_before_any_upload() {
    // Internal protocol seam: a legacy durable exclusive audit can lack proof;
    // ordinary local commit cannot produce that state. Recovery must fail closed.
    let author = AuthorSubject::for_test_bytes([0xf9; 16]);
    let origin = initialization_owner(initialization_storage(), author, 0xf9);
    let open = block_on(stage_initialization(&origin, row(0xf9)));
    let id = block_on(origin.commit_exclusive_handle(open)).unwrap();
    let SyncMessage::CommitUnit { mut tx, versions } =
        block_on(origin.node.node.borrow_mut().commit_unit_for(id)).unwrap()
    else {
        unreachable!()
    };
    tx.base_snapshot = None;
    tx.row_read_set = None;
    tx.absent_read_set = None;
    tx.predicate_read_set = None;
    let storage = initialization_storage();
    let worker = initialization_owner(storage.clone(), author, 0xfa);
    block_on(
        worker
            .node
            .node
            .borrow_mut()
            .ingest_relay_commit_unit(tx, versions),
    )
    .unwrap();
    block_on(worker.close()).unwrap();
    drop(worker);
    let reopened = initialization_owner(storage, author, 0xfb);
    // SAFETY: this is the admitted owner startup path, deliberately corrupt proof.
    let error =
        block_on(unsafe { reopened.restore_initialization_owner_pending_uploads() }).unwrap_err();
    assert_eq!(error.code, ErrorCode::Storage);
    assert!(
        error
            .message
            .contains("incomplete owned pending transaction")
    );
    assert!(
        reopened.node.outbox.borrow().iter().next().is_none(),
        "failed recovery cannot publish a prefix"
    );
    assert_eq!(
        block_on(reopened.initialization_transaction_status(&[ReservedTxId(id)])).unwrap(),
        vec![InitializationTransactionStatus::Incomplete]
    );
}

#[test]
fn initialization_absence_cannot_treat_policy_hidden_row_as_new_insert() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::False)
                        .with_insert(PublicPolicyExpr::True),
                ),
        ),
    );
    let families = schema.column_families();
    let db = block_on(Db::open(DbConfig::new(
        schema,
        groove::storage::TestStorage::new(&families.iter().map(String::as_str).collect::<Vec<_>>()),
        DbIdentity {
            node: NodeUuid::from_bytes([0xfc; 16]),
            author: AuthorSubject::for_test_bytes([0xfc; 16]),
        },
    )))
    .unwrap();
    block_on(db.insert(
        "todos",
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("hidden existing root".to_owned()),
        )]),
        InsertOptions {
            row_id: Some(row(0xfc)),
            ..Default::default()
        },
    ))
    .unwrap();
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    assert!(block_on(db.prepare_initialization_insert(open, "todos", row(0xfc))).is_err());
}

#[test]
fn initialization_failed_sealed_publish_closes_before_following_owner_operation() {
    let author = AuthorSubject::for_test_bytes([0xfd; 16]);
    let db = initialization_owner(initialization_storage(), author, 0xfd);
    let open = block_on(stage_initialization(&db, row(0xfd)));
    let seal = block_on(db.seal_initialization_transaction(open)).unwrap();
    let reserved = seal.reservation();
    block_on(db.insert(
        "todos",
        BTreeMap::from([(
            "title".to_owned(),
            Value::String("competing insert".to_owned()),
        )]),
        InsertOptions {
            row_id: Some(row(0xfd)),
            ..Default::default()
        },
    ))
    .unwrap();
    let write = block_on(db.publish_initialization_transaction(seal)).unwrap();
    // The binding's single-operation pump exposes publication failure before
    // deferred cleanup runs. A full tick could hide the mutable-handle window.
    let mut failure = None;
    for _ in 0..64 {
        db.drive_queued_mutation_once();
        if let Err(error) = block_on(write.write_state()) {
            failure = Some(error);
            break;
        }
    }
    assert_eq!(
        failure.expect("sealed publish must conflict").code,
        ErrorCode::TransactionConflict
    );
    assert!(
        block_on(db.exclusive_tx_ref(open).insert(
            "todos",
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("must not stage".to_owned())
            )]),
            InsertOptions {
                row_id: Some(row(0xfe)),
                ..Default::default()
            },
        ))
        .is_err()
    );
    assert!(block_on(db.commit_exclusive_handle(open)).is_err());
    assert!(block_on(db.begin_exclusive(open)).is_err());
    assert_eq!(
        block_on(db.initialization_transaction_status(&[reserved])).unwrap(),
        vec![InitializationTransactionStatus::NotObserved]
    );
    let query = db.prepare_query(&db.table("todos")).unwrap();
    let rows = block_on(db.all(&query, ReadOpts::default())).unwrap();
    assert_eq!(
        rows.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>(),
        vec![row(0xfd)]
    );
    assert_eq!(
        rows[0].cell_at(0),
        Some(Value::String("competing insert".to_owned()))
    );
}
