//! Host cache lifecycle through public SQLite APIs and real admitted sync links.
mod common;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, SyncMessage, Transport, TransportError,
    block_on, duplex, validate_catalogue_capture_replacement,
};
use jazz::ids::{AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalTableId, NodeUuid};
use jazz::protocol::{
    CatalogueSnapshot, CurrentWriteSchema, LensOp, MigrationLens, PhysicalColumnIdentity,
    PhysicalIdentityManifest, PhysicalTableIdentity, SchemaLineagePublication, SchemaVersion,
    TableLens,
};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
};
fn schema(table: &str, column: &str) -> JazzSchema {
    common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(table)
                    .column(column, ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    )
}
fn genesis(schema: JazzSchema) -> CatalogueSnapshot {
    CatalogueSnapshot {
        genesis_physical_identities: PhysicalIdentityManifest::allocate(&schema),
        schemas: vec![SchemaVersion::new(schema.clone())],
        lineages: vec![],
        current_write_schema: CurrentWriteSchema {
            revision: 1,
            schema: schema.version_id(),
        },
    }
}
fn open(node: u8, schema: JazzSchema, path: Option<&Path>) -> Db {
    let config = DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    match path {
        Some(path) => block_on(Db::open_at(config, path, Default::default())),
        None => block_on(Db::open(config)),
    }
    .unwrap()
}
fn authority(node: u8, snapshot: &CatalogueSnapshot, path: Option<&Path>) -> Db {
    let db = open(node, snapshot.schemas[0].schema.clone(), path);
    db.set_authority();
    db.install_authority_catalogue(snapshot).unwrap();
    db
}
fn rename(snapshot: &mut CatalogueSnapshot, table: &str, column: &str) -> MigrationLens {
    let source = snapshot.schemas.last().unwrap().schema.clone();
    let source_table = &source.tables()[0];
    let from = &source_table.name;
    let from_col = &source_table.columns[0].name;
    let target = schema(table, column);
    let lens = MigrationLens::new(
        source.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: from.clone(),
            target_table: table.into(),
            ops: if from_col != column {
                vec![LensOp::RenameColumn {
                    from: from_col.clone(),
                    to: column.into(),
                }]
            } else {
                vec![]
            },
        }],
    )
    .unwrap();
    let identities = snapshot
        .lineages
        .last()
        .map_or(&snapshot.genesis_physical_identities, |(_, p)| {
            &p.physical_identities
        });
    let history = std::iter::once(snapshot.genesis_physical_identities.clone()).chain(
        snapshot
            .lineages
            .iter()
            .map(|(_, p)| p.physical_identities.clone()),
    );
    let publication = SchemaLineagePublication::author_from_prior_with_history(
        &source,
        identities,
        history,
        SchemaVersion::new(target.clone()),
        lens.clone(),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    snapshot
        .lineages
        .push((snapshot.lineages.len() as u64 + 1, publication));
    snapshot.schemas.push(SchemaVersion::new(target.clone()));
    snapshot.current_write_schema = CurrentWriteSchema {
        revision: snapshot.current_write_schema.revision + 1,
        schema: target.version_id(),
    };
    lens
}

fn capture(db: &Db) -> Vec<u8> {
    db.take_authenticated_catalogue_state()
        .unwrap()
        .capture
        .expect("pending capture")
}
fn install(db: &Db, bytes: &[u8]) -> Result<(), jazz::db::Error> {
    // SAFETY: fixtures only restore bytes captured from their own authority,
    // or deliberately malformed copies to exercise the trusted-host validator.
    unsafe { db.install_cached_catalogue(bytes) }
}
fn connect(alice: &Db, bob: &Db) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(up));
    bob.accept_subscriber(down, AuthorSubject::SYSTEM);
    for _ in 0..8 {
        block_on(alice.tick()).unwrap();
        block_on(bob.tick()).unwrap();
    }
}
/// Alice captures metadata from Bob's authenticated upstream, persists it outside
/// her account root, and reopens offline with identical IDs but no fresh coverage.
#[test]
fn network_capture_restores_offline_without_claiming_coverage() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    assert!(!alice.has_authenticated_catalogue().unwrap());
    assert!(
        alice
            .take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
    connect(&alice, &bob);
    assert!(alice.has_authenticated_catalogue().unwrap());
    assert!(alice.has_authenticated_catalogue().unwrap());
    let bytes = capture(&alice);
    assert!(
        alice
            .take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
    let reopened = open(2, snapshot.schemas[0].schema.clone(), None);
    install(&reopened, &bytes).unwrap();
    assert!(reopened.has_authenticated_catalogue().unwrap());
    assert!(
        reopened
            .take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
    assert_eq!(
        reopened.table_identity("notes").unwrap(),
        bob.table_identity("notes").unwrap()
    );
    assert!(block_on(reopened.cover_catalogue("notes", None)).is_err());
    let query = reopened.prepare_query(&Query::from("notes")).unwrap();
    assert!(
        block_on(reopened.all(&query, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
    block_on(reopened.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String("offline".into()))]),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        block_on(reopened.all(&query, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
}
/// Alice's pending view can move Bob's accepted capture, but neither it nor a
/// cached owner opened on an unpublished schema can read or write that schema.
#[test]
fn unpublished_requested_schema_is_not_admitted_by_cached_ids() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let bytes = capture(&bob);
    let pending_schema = schema("drafts", "text");
    let alice = open(1, pending_schema.clone(), None);
    let prepared_before_cache = alice
        .prepare_query(&Query::from("drafts").limit(0))
        .unwrap();
    install(&alice, &bytes).unwrap();
    assert!(!alice.has_authenticated_catalogue().unwrap());
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        bob.table_identity("notes").unwrap()
    );
    assert!(
        alice
            .prepare_query(&Query::from("drafts").limit(0))
            .is_err()
    );
    assert_eq!(
        block_on(alice.all(&prepared_before_cache, ReadOpts::default()))
            .unwrap_err()
            .code,
        ErrorCode::Schema
    );
    assert!(block_on(alice.subscribe(&prepared_before_cache, ReadOpts::default())).is_err());
    assert_eq!(
        block_on(alice.insert(
            "drafts",
            BTreeMap::from([("text".into(), Value::String("must fail".into()))]),
            Default::default()
        ))
        .err()
        .unwrap()
        .code,
        ErrorCode::Schema
    );
    let known = block_on(alice.register_schema_view(snapshot.schemas[0].schema.clone())).unwrap();
    assert!(known.has_authenticated_catalogue().unwrap());
    assert!(!alice.has_authenticated_catalogue().unwrap());
    // A second owner gets current metadata while an unpublished view shares its
    // capture queue. Readiness stays view-specific; capture ownership does not.
    let owner = open(2, snapshot.schemas[0].schema.clone(), None);
    let pending = block_on(owner.register_schema_view(pending_schema)).unwrap();
    connect(&owner, &bob);
    let state = pending.take_authenticated_catalogue_state().unwrap();
    assert!(!state.ready);
    assert!(state.capture.is_some());
    let state = owner.take_authenticated_catalogue_state().unwrap();
    assert!(state.ready);
    assert!(state.capture.is_none());
}
/// Alice's current cache keeps Bob's old and renamed descriptors; replacement
/// cannot go backwards, forget an accepted view or swap physical UUIDs.
#[test]
fn replacement_preserves_namespace_history_and_active_head() {
    let mut snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let first = capture(&bob);
    let lens = rename(&mut snapshot, "tasks", "body");
    block_on(
        bob.activate_schema_with_lenses(snapshot.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    bob.install_authority_catalogue(&snapshot).unwrap();
    let next = capture(&bob);
    validate_catalogue_capture_replacement(&first, &next).unwrap();
    assert!(validate_catalogue_capture_replacement(&next, &first).is_err());
    let unrelated = authority(4, &genesis(schema("notes", "text")), None);
    assert!(validate_catalogue_capture_replacement(&first, &capture(&unrelated)).is_err());
    let alice = open(1, snapshot.schemas.last().unwrap().schema.clone(), None);
    install(&alice, &next).unwrap();
    assert!(alice.has_authenticated_catalogue().unwrap());
    let old = block_on(alice.register_schema_view(snapshot.schemas[0].schema.clone())).unwrap();
    assert!(old.has_authenticated_catalogue().unwrap());
    assert_eq!(
        old.table_identity("notes").unwrap(),
        alice.table_identity("tasks").unwrap()
    );
    let (head, mut records) = unpack(&next);
    let current = records
        .iter()
        .find(|r| r.schema.id == head.current)
        .unwrap()
        .clone();
    assert!(validate_catalogue_capture_replacement(&next, &pack(&head, &[current])).is_err());
    records[0]
        .identities
        .tables
        .values_mut()
        .next()
        .unwrap()
        .id
        .0 = uuid::Uuid::from_bytes([0xee; 16]);
    assert!(validate_catalogue_capture_replacement(&next, &pack(&head, &records)).is_err());
}
/// Alice reopens her SQLite file with an older host cache after Bob's rename.
/// The newer durable head wins; accepted descriptors and admission survive even
/// when the next reopen supplies no external cache.
#[test]
fn durable_newer_catalogue_wins_and_admission_mode_survives_reopen() {
    let mut snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let first = capture(&bob);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    {
        // SQLite's public opener is its codec admission path; retired Groove
        // storage_codec_profile does not apply to this engine.
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        install(&alice, &first).unwrap();
        let lens = rename(&mut snapshot, "tasks", "body");
        block_on(
            bob.activate_schema_with_lenses(
                snapshot.schemas.last().unwrap().schema.clone(),
                &[lens],
            ),
        )
        .unwrap();
        bob.install_authority_catalogue(&snapshot).unwrap();
        let next = capture(&bob);
        install(&alice, &next).unwrap();
    }
    {
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        install(&alice, &first).unwrap();
        assert_eq!(
            alice.table_identity("tasks").unwrap(),
            bob.table_identity("tasks").unwrap()
        );
        let union = capture(&alice);
        validate_catalogue_capture_replacement(&first, &union).unwrap();
    }
    let alice = open(1, schema("drafts", "text"), Some(&path));
    assert!(!alice.has_authenticated_catalogue().unwrap());
    assert!(
        alice
            .prepare_query(&Query::from("drafts").limit(0))
            .is_err()
    );
    assert_eq!(
        alice.table_identity("tasks").unwrap(),
        bob.table_identity("tasks").unwrap()
    );
}
const HEADER: &[u8] = b"JAZZ-SQLITE-CATALOGUE\0\x01";
fn unpack(bytes: &[u8]) -> (jazz::db::CatalogueHeadV1, Vec<jazz::db::CatalogueSchemaV1>) {
    postcard::from_bytes(bytes.strip_prefix(HEADER).unwrap()).unwrap()
}
fn pack(head: &jazz::db::CatalogueHeadV1, records: &[jazz::db::CatalogueSchemaV1]) -> Vec<u8> {
    let mut bytes = HEADER.to_vec();
    bytes.extend(postcard::to_allocvec(&(head, records)).unwrap());
    bytes
}
/// Mallory's malformed/truncated/wrong-version captures cannot overwrite Alice's
/// accepted IDs or enable admission on her fresh local schema.
#[test]
fn malformed_cache_is_atomic_and_does_not_enable_admission() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let bytes = capture(&bob);
    let (head, records) = unpack(&bytes);
    let mut malformed = vec![vec![], bytes[..bytes.len() - 1].to_vec()];
    let mut trailing = bytes.clone();
    trailing.push(0);
    malformed.push(trailing);
    let mut wrong_version = bytes.clone();
    wrong_version[HEADER.len() - 1] = 2;
    malformed.push(wrong_version);
    let mut duplicate = records.clone();
    duplicate.push(records[0].clone());
    malformed.push(pack(&head, &duplicate));
    let mut missing = records.clone();
    missing[0]
        .identities
        .tables
        .get_mut("notes")
        .unwrap()
        .columns
        .clear();
    malformed.push(pack(&head, &missing));
    let mut nil = head.clone();
    nil.root = [0; 32];
    malformed.push(pack(&nil, &records));
    let mut conflict = records.clone();
    conflict[0].identities.tables.get_mut("notes").unwrap().id.0 =
        uuid::Uuid::from_bytes([0xcc; 16]);
    malformed.push(pack(&head, &conflict));
    for bad in malformed {
        let alice = open(1, snapshot.schemas[0].schema.clone(), None);
        install(&alice, &bytes).unwrap();
        assert!(install(&alice, &bad).is_err());
        assert_eq!(
            alice.table_identity("notes").unwrap(),
            bob.table_identity("notes").unwrap()
        );
        assert!(
            alice
                .take_authenticated_catalogue_state()
                .unwrap()
                .capture
                .is_none()
        );
        assert!(validate_catalogue_capture_replacement(&bytes, &bad).is_err());
    }
    let fresh = open(2, schema("drafts", "text"), None);
    assert!(install(&fresh, b"bad").is_err());
    assert!(fresh.prepare_query(&Query::from("drafts")).is_ok());
}
struct Inbox(VecDeque<SyncMessage>);
impl Transport for Inbox {
    fn send(&mut self, _: SyncMessage) -> Result<(), TransportError> {
        Ok(())
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.0.pop_front()
    }
}
/// Bob's metadata followed by corrupt metadata in the same tick rolls back as a
/// unit. Alice must never capture or become ready from the rolled-back head.
#[test]
fn rolled_back_network_metadata_never_becomes_a_capture() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let (head, records) = unpack(&capture(&bob));
    let mut bad = head.clone();
    bad.root = [0; 32];
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    assert!(
        alice
            .take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
    block_on(alice.connect_upstream(Box::new(Inbox(VecDeque::from([
        SyncMessage::CatalogueMetadataV1 {
            head,
            schemas: records.clone(),
        },
        SyncMessage::CatalogueMetadataV1 {
            head: bad,
            schemas: records,
        },
    ])))));
    assert!(block_on(alice.tick()).is_err());
    let state = alice.take_authenticated_catalogue_state().unwrap();
    assert!(!state.ready);
    assert!(state.capture.is_none());
}
/// Bob's capture pins the exact header, namespace, head, descriptor and UUID
/// encoding. Alice restores it and the durable cache-mode row survives reopen.
#[test]
fn capture_v1_byte_fixture_and_durable_mode() {
    let mut snapshot = genesis(schema("notes", "text"));
    snapshot.genesis_physical_identities = PhysicalIdentityManifest {
        tables: BTreeMap::from([(
            "notes".into(),
            PhysicalTableIdentity {
                id: GlobalPhysicalTableId(uuid::Uuid::from_bytes([0x11; 16])),
                columns: BTreeMap::from([(
                    "text".into(),
                    PhysicalColumnIdentity {
                        id: GlobalPhysicalColumnId(uuid::Uuid::from_bytes([0x22; 16])),
                        enum_variants: BTreeMap::new(),
                    },
                )]),
            },
        )]),
    };
    snapshot.current_write_schema.revision = 128;
    let bob = authority(3, &snapshot, None);
    let bytes = capture(&bob);
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_catalogue_capture_v1.json")).unwrap();
    assert_eq!(hex::encode(&bytes), fixture["capture"].as_str().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("alice.sqlite");
    {
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        install(&alice, &bytes).unwrap();
    }
    // Direct SQL is needed only to pin the actual durable admission marker.
    let conn = rusqlite::Connection::open(&path).unwrap();
    let slot: i64 = conn
        .query_row("SELECT slot FROM _jazz_catalogue_cache_mode_v1", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(slot, 1);
    let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
    assert!(alice.has_authenticated_catalogue().unwrap());
    validate_catalogue_capture_replacement(&bytes, &capture(&alice)).unwrap();
}

/// Alice starts offline with accepted A IDs and proposed schema B. Bob's later
/// publication, delivered by normal metadata sync, admits B without changing IDs.
#[test]
fn pending_cached_owner_becomes_ready_only_after_publication() {
    let mut snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let first = capture(&bob);
    let lens = rename(&mut snapshot, "tasks", "body");
    let requested = snapshot.schemas.last().unwrap().schema.clone();
    let alice = open(1, requested.clone(), None);
    install(&alice, &first).unwrap();
    assert!(!alice.has_authenticated_catalogue().unwrap());
    connect(&alice, &bob);
    let before = alice.take_authenticated_catalogue_state().unwrap();
    assert!(!before.ready);
    assert!(before.capture.is_some());
    block_on(bob.activate_schema_with_lenses(requested, &[lens])).unwrap();
    bob.install_authority_catalogue(&snapshot).unwrap();
    for _ in 0..8 {
        block_on(bob.tick()).unwrap();
        block_on(alice.tick()).unwrap();
    }
    assert!(alice.has_authenticated_catalogue().unwrap());
    assert_eq!(
        alice.table_identity("tasks").unwrap(),
        bob.table_identity("tasks").unwrap()
    );
    validate_catalogue_capture_replacement(&first, &capture(&alice)).unwrap();
    let query = alice.prepare_query(&Query::from("tasks").limit(0)).unwrap();
    assert!(
        block_on(alice.all(&query, ReadOpts::default()))
            .unwrap()
            .is_empty()
    );
}

/// A conflicting cached record encountered after a new descriptor cannot leave
/// partial catalogue state behind. Alice keeps her old head and the extra schema
/// remains unadmitted after the SQL savepoint rolls back.
#[test]
fn conflicting_cache_rolls_back_an_earlier_descriptor_insert() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let bytes = capture(&bob);
    let (mut head, mut records) = unpack(&bytes);
    let extra = (0..100)
        .map(|i| schema(&format!("drafts{i}"), "text"))
        .find(|s| s.version_id() < records[0].schema.id)
        .unwrap();
    let extra_record = jazz::db::CatalogueSchemaV1 {
        schema: SchemaVersion::new(extra.without_permissions()),
        identities: PhysicalIdentityManifest::allocate(&extra),
    };
    records[0].identities.tables.get_mut("notes").unwrap().id.0 =
        uuid::Uuid::from_bytes([0xed; 16]);
    records.insert(0, extra_record);
    head.through += 1;
    let alice = open(1, snapshot.schemas[0].schema.clone(), None);
    install(&alice, &bytes).unwrap();
    assert!(install(&alice, &pack(&head, &records)).is_err());
    let view = block_on(alice.register_schema_view(extra)).unwrap();
    assert!(!view.has_authenticated_catalogue().unwrap());
    assert_eq!(
        alice.table_identity("notes").unwrap(),
        bob.table_identity("notes").unwrap()
    );
    assert!(
        alice
            .take_authenticated_catalogue_state()
            .unwrap()
            .capture
            .is_none()
    );
}

/// Alice cannot use either transaction kind to bypass the cached owner's
/// requested-schema admission, even with a query prepared before restoration.
#[test]
fn transactions_reject_unpublished_cached_schema_reads_and_writes() {
    let snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let bytes = capture(&bob);
    for serializable in [false, true] {
        let alice = open(1, schema("drafts", "text"), None);
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
        let tx = alice.mergeable_tx_ref(id);
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
}

/// Alice and Bob publish different authenticated subsets in either order. Their
/// host keeps the union at the newer head, including after an offline reopen,
/// without claiming that cached identities grant any fresh row coverage.
#[test]
fn capture_merge_unions_subsets_and_preserves_newer_head_in_either_order() {
    use jazz::db::merge_catalogue_captures;
    let mut snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let first = capture(&bob);
    let lens = rename(&mut snapshot, "tasks", "body");
    block_on(
        bob.activate_schema_with_lenses(snapshot.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    bob.install_authority_catalogue(&snapshot).unwrap();
    let complete = capture(&bob);
    let (head, records) = unpack(&complete);
    let partial = pack(
        &head,
        &records
            .iter()
            .filter(|r| r.schema.id == head.current)
            .cloned()
            .collect::<Vec<_>>(),
    );
    for (left, right) in [
        (&first, &partial),
        (&partial, &first),
        (&complete, &partial),
        (&partial, &complete),
    ] {
        let merged = merge_catalogue_captures(left, right).unwrap();
        assert_eq!(merged, complete);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alice.sqlite");
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        install(&alice, &merged).unwrap();
        block_on(alice.close()).unwrap();
        drop(alice);
        let alice = open(1, snapshot.schemas[0].schema.clone(), Some(&path));
        let current =
            block_on(alice.register_schema_view(snapshot.schemas.last().unwrap().schema.clone()))
                .unwrap();
        assert!(alice.has_authenticated_catalogue().unwrap());
        assert!(current.has_authenticated_catalogue().unwrap());
        let old = block_on(alice.register_schema_view(snapshot.schemas[0].schema.clone())).unwrap();
        let expected = Some(snapshot.genesis_physical_identities.tables["notes"].id);
        assert_eq!(old.table_identity("notes").unwrap(), expected);
        assert_eq!(current.table_identity("tasks").unwrap(), expected);
        assert!(block_on(current.cover_catalogue("tasks", None)).is_err());
    }
}

/// Mallory cannot use host merge to replace an authority namespace, disagree
/// about a revision or an accepted physical identity, or smuggle malformed bytes.
#[test]
fn capture_merge_refuses_conflicting_and_malformed_inputs_in_both_orders() {
    use jazz::db::merge_catalogue_captures;
    let mut snapshot = genesis(schema("notes", "text"));
    let bob = authority(3, &snapshot, None);
    let first = capture(&bob);
    let lens = rename(&mut snapshot, "tasks", "body");
    block_on(
        bob.activate_schema_with_lenses(snapshot.schemas.last().unwrap().schema.clone(), &[lens]),
    )
    .unwrap();
    bob.install_authority_catalogue(&snapshot).unwrap();
    let complete = capture(&bob);
    let (head, records) = unpack(&complete);
    let mut wrong_namespace = head.clone();
    wrong_namespace.root = [0xee; 32];
    let mut wrong_revision = head.clone();
    wrong_revision.current = snapshot.schemas[0].id;
    let mut incomparable = head.clone();
    incomparable.through += 1;
    incomparable.revision -= 1;
    let mut wrong_ids = records.clone();
    wrong_ids[0]
        .identities
        .tables
        .values_mut()
        .next()
        .unwrap()
        .id
        .0 = uuid::Uuid::from_bytes([0xee; 16]);
    let mut trailing = complete.clone();
    trailing.push(0);
    for bad in [
        pack(&wrong_namespace, &records),
        pack(&wrong_revision, &records),
        pack(&incomparable, &records),
        pack(&head, &wrong_ids),
        trailing,
        vec![],
        complete[..complete.len() - 1].to_vec(),
    ] {
        assert!(merge_catalogue_captures(&complete, &bad).is_err());
        assert!(merge_catalogue_captures(&bad, &complete).is_err());
    }
    assert_eq!(
        merge_catalogue_captures(&complete, &first).unwrap(),
        complete
    );
}
