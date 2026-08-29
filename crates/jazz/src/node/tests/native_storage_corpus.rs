// Settlement-baseline native Jazz storage corpus.
//
// This is deliberately an integration receipt over the production ordered-KV
// adapters rather than a second hand-written byte codec.  The eventual checked
// in physical fixtures generated from this scenario live in #2307; keeping the
// producer here makes the semantic shape, backend profile, and reopen contract
// executable beside the NodeState paths that actually write it.

use crate::storage_codec_profile::epoch_1_storage_codec_profile;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use jazz_storage_rocksdb::Durability as RocksDurability;
use jazz_storage_sqlite::{Durability as SqliteDurability, SqliteStorage as ImmediateSqliteStorage};
use sha2::{Digest, Sha256};

/// The storage families that the native settlement producer must prove before
/// its logical pack can be promoted to a committed historical fixture.
///
/// `catalogue` is intentionally limited to the genesis/current-pointer path
/// on this base.  The durable typed staged/active-lineage envelopes are owned
/// by the still-unmerged #2306 stack; the corpus must not smuggle those heads
/// in just to make a fixture look more complete.
const NATIVE_CORPUS_REQUIRED_STORES: &[&str] = &[
    "jazz_catalogue",
    "jazz_catalogue_pointer",
    "jazz_nodes",
    "jazz_schema_versions",
    "jazz_transactions",
    "jazz_merge_heads",
    "jazz_global_changes",
    "jazz_deletion_history",
    "jazz_known_state_facts",
    "jazz_settled_result_members",
    "jazz_settled_program_facts",
];
const NATIVE_CORPUS_PACK_HEADER: &str = "JAZZ-NATIVE-STORAGE-CORPUS-1";
const EPOCH_1_NATIVE_CORPUS_PACK_BASE64: &str =
    include_str!("../../../fixtures/epoch-1-native-jazz-corpus.pack.base64");
const EPOCH_1_NATIVE_CORPUS_PACK_SHA256: &str =
    "32f1c10bb767fa5d24e5a1cf2d8fb5cc6cf7ef596a6b0a83250f84c7c28cf2df";
const EPOCH_1_NATIVE_SQLITE_BASE64: &str =
    include_str!("../../../fixtures/epoch-1-native-jazz.sqlite.gz.base64");
const EPOCH_1_NATIVE_SQLITE_ARCHIVE_SHA256: &str =
    "047e161f88160edde3d9362ab0704f1b8ae4f2d92eedb22645fbafba14962e41";
const EPOCH_1_NATIVE_SQLITE_SHA256: &str =
    "8d07832629559d30b30e2e075b6c4cf8b410c26f5d888bf360e7f100bb02450b";
const EPOCH_1_NATIVE_ROCKSDB_BASE64: &str =
    include_str!("../../../fixtures/epoch-1-native-jazz-rocksdb.tar.gz.base64");
const EPOCH_1_NATIVE_ROCKSDB_SHA256: &str =
    "876f60de7c6fd2242d6065462f1453574287ab33ad1b1c84e79a3492bba9cfdc";

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeCorpusReceipt {
    /// Exact raw primary-key/value pairs in canonical scan order, grouped by
    /// logical Jazz store.  This is backend-neutral and makes a later binary
    /// fixture reviewable even though SQLite pages and RocksDB SSTs are not an
    /// interchange format.
    stores: BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>,
}

fn native_corpus_checksum(receipt: &NativeCorpusReceipt) -> String {
    let mut digest = Sha256::new();
    for (store, rows) in &receipt.stores {
        digest.update((store.len() as u64).to_be_bytes());
        digest.update(store.as_bytes());
        digest.update((rows.len() as u64).to_be_bytes());
        for (key, value) in rows {
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key);
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value);
        }
    }
    format!("{:x}", digest.finalize())
}

fn native_corpus_pack(receipt: &NativeCorpusReceipt) -> String {
    let mut pack = format!("{NATIVE_CORPUS_PACK_HEADER}\n");
    for (store, rows) in &receipt.stores {
        // Preserve empty authoritative families too.  A row-only listing
        // cannot distinguish an empty-but-opened family from one the
        // producer accidentally omitted from the corpus.
        use std::fmt::Write as _;
        writeln!(pack, "store\t{store}")
            .expect("writing an in-memory corpus pack cannot fail");
        for (key, value) in rows {
            writeln!(pack, "entry\t{store}\t{}\t{}", hex::encode(key), hex::encode(value))
                .expect("writing an in-memory corpus pack cannot fail");
        }
    }
    pack
}

fn epoch_1_native_corpus_pack() -> String {
    let bytes = STANDARD
        .decode(EPOCH_1_NATIVE_CORPUS_PACK_BASE64.trim())
        .expect("committed native corpus pack must be base64");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        EPOCH_1_NATIVE_CORPUS_PACK_SHA256,
        "committed native corpus pack checksum must match before it is compared"
    );
    let pack = String::from_utf8(bytes).expect("native corpus pack must be UTF-8");
    assert!(
        pack.starts_with(&format!("{NATIVE_CORPUS_PACK_HEADER}\n")),
        "native corpus pack must retain its exact epoch header"
    );
    pack
}

fn decode_native_physical_fixture(
    base64: &str,
    expected_sha256: &str,
    family: &str,
) -> Result<Vec<u8>, String> {
    let bytes = STANDARD
        .decode(base64.lines().collect::<String>())
        .map_err(|error| format!("{family} corpus fixture is not base64: {error}"))?;
    if format!("{:x}", Sha256::digest(&bytes)) != expected_sha256 {
        return Err(format!("{family} corpus fixture checksum does not match"));
    }
    Ok(bytes)
}

fn materialize_native_sqlite_fixture(path: &std::path::Path, base64: &str) -> Result<(), String> {
    // Verify the immutable payload before creating a target. This is both a
    // corruption receipt and a guard against a bad checked-in fixture being
    // reported later as an adapter-open failure.
    let bytes = decode_native_physical_fixture(
        base64,
        EPOCH_1_NATIVE_SQLITE_ARCHIVE_SHA256,
        "SQLite",
    )?;
    let mut sqlite = Vec::new();
    std::io::Read::read_to_end(
        &mut flate2::read::GzDecoder::new(std::io::Cursor::new(bytes)),
        &mut sqlite,
    )
    .map_err(|error| format!("SQLite corpus fixture is not gzip: {error}"))?;
    if format!("{:x}", Sha256::digest(&sqlite)) != EPOCH_1_NATIVE_SQLITE_SHA256 {
        return Err("SQLite corpus decompressed checksum does not match".to_owned());
    }
    std::fs::write(path, sqlite).map_err(|error| error.to_string())
}

fn unpack_native_rocksdb_fixture(
    root: &std::path::Path,
    base64: &str,
) -> Result<std::path::PathBuf, String> {
    let bytes = decode_native_physical_fixture(
        base64,
        EPOCH_1_NATIVE_ROCKSDB_SHA256,
        "RocksDB",
    )?;
    let decoder = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes));
    tar::Archive::new(decoder)
        .unpack(root)
        .map_err(|error| format!("native RocksDB corpus archive is not safe: {error}"))?;
    Ok(root.join("rocksdb-epoch-1"))
}

fn assert_same_native_corpus(
    left: &NativeCorpusReceipt,
    right: &NativeCorpusReceipt,
    message: &str,
) {
    if left == right {
        return;
    }
    let differing_store = left
        .stores
        .keys()
        .chain(right.stores.keys())
        .find(|store| left.stores.get(*store) != right.stores.get(*store));
    panic!(
        "{message}; first differing store={differing_store:?}, left checksum={}, right checksum={}",
        native_corpus_checksum(left),
        native_corpus_checksum(right),
    );
}

fn native_corpus_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("branch_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text)
                    .column("attachment", PublicColumnType::Bytea)
                    .branch_by("branch_id"),
            )
            .table(PublicTableSchemaBuilder::new("notes").column("body", PublicColumnType::Text)),
    )
}

fn native_corpus_evolved_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("branch_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text)
                    .column("attachment", PublicColumnType::Bytea)
                    .branch_by("branch_id"),
            )
            .table(
                PublicTableSchemaBuilder::new("notes")
                    .column("body", PublicColumnType::Text)
                    .column("genre", PublicColumnType::Text),
            ),
    )
}

fn native_corpus_lineage(
    snapshot: &crate::protocol::CatalogueSnapshot,
) -> SchemaLineagePublication {
    let namespace = uuid::Uuid::from_bytes([0x4a; 16]);
    let source = native_corpus_schema();
    let target = SchemaVersion::new(native_corpus_evolved_schema());
    let mut publication = SchemaLineagePublication::author_from_prior(
        &source,
        &snapshot.genesis_physical_identities,
        target.clone(),
        MigrationLens::new(
            source.version_id(),
            target.id,
            vec![
                TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: Vec::new(),
                },
                TableLens {
                    source_table: "notes".to_owned(),
                    target_table: "notes".to_owned(),
                    ops: vec![LensOp::AddColumn {
                        column: "genre".to_owned(),
                        default: v("instrumental"),
                    }],
                },
            ],
        )
        .expect("native corpus lineage is valid"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .expect("native corpus lineage is valid");
    publication.physical_identities.tables.get_mut("notes")
        .expect("lineage retains notes table")
        .columns.get_mut("genre")
        .expect("lineage allocates genre column")
        .id = crate::ids::GlobalPhysicalColumnId(uuid::Uuid::new_v5(
            &namespace,
            b"table/notes/column/genre",
        ));
    publication.id = publication.content_id();
    publication
}

fn publish_native_corpus_lineage<S>(
    node: &mut NodeState<S>,
    snapshot: &crate::protocol::CatalogueSnapshot,
)
where
    S: ReopenableStorage,
{
    let publication = native_corpus_lineage(snapshot);
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(publication),
    })
    .expect("publish native corpus lineage");
}

fn seed_native_corpus_settled_query_state<S>(node: &mut NodeState<S>)
where
    S: ReopenableStorage,
{
    let (shape, binding) = node
        .whole_table_shape_binding("notes")
        .expect("native corpus prepares a notes query shape");
    register_shape_binding(node, &shape, &binding);
    let subscription = node
        .whole_table_subscription_key("notes")
        .expect("native corpus registers its notes subscription");
    let notes_version = node
        .query_table_versions("notes")
        .expect("native corpus reads notes history")
        .into_iter()
        .find(|version| version.row_uuid() == row(0xc3))
        .expect("native corpus note version exists");
    let notes_tx = node
        .version_tx_id(&notes_version)
        .expect("native corpus note has a transaction id");
    let fact = crate::protocol::ProgramFactEntry::PathCorrelationCoverage(
        crate::protocol::PathCorrelationCoverageEntry {
            path: "native-corpus".to_owned(),
            source_table: "notes".to_owned().into(),
            source_row: row(0xc3),
            correlation_key: vec![0xc3],
            complete: true,
        },
    );
    node.apply_sync_message_settled(SyncMessage::ViewUpdate(
        crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set: false,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: vec![crate::protocol::ResultMemberEntry::row((
                "notes".to_owned().into(),
                row(0xc3),
                notes_tx,
            ))],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: vec![fact],
            program_fact_removes: Vec::new(),
        },
    ))
    .expect("native corpus persists settled result and program facts");
}

fn native_corpus_branch(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn native_corpus_large_attachment() -> Vec<u8> {
    // Cross the public inline boundary by one byte.  The repeated payload
    // keeps the checked-in physical fixture compact under the native adapters'
    // ordinary compression while still forcing a real indirect root/node
    // closure into the historical store.
    vec![0x5a; groove::large_values::INLINE_VALUE_MAX_BYTES + 1]
}

fn native_corpus_storage_tables(schema: &JazzSchema) -> (Vec<String>, Vec<String>) {
    let lowered = schema.lower_to_groove();
    let mut tables = lowered
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    tables.sort();
    tables.dedup();
    let mut direct_stores = lowered
        .direct_record_stores
        .iter()
        .map(|store| store.name.clone())
        .collect::<Vec<_>>();
    direct_stores.sort();
    direct_stores.dedup();
    (tables, direct_stores)
}

/// The authority mints permanent physical identities exactly once.  A native
/// compatibility corpus must replay that one authority snapshot into every
/// backend: independently calling `NodeState::new` would correctly allocate
/// different UUID identities, but would make the purportedly backend-neutral
/// historical pack meaningless.
fn native_corpus_authority_snapshot(schema: &JazzSchema) -> crate::protocol::CatalogueSnapshot {
    // This deliberately mirrors the authority's one-time UUID allocation,
    // but derives the fixture identities from a fixed namespace. Random UUIDs
    // are right in real authority publication; they would make a committed
    // byte corpus change on every test run.
    let namespace = uuid::Uuid::from_bytes([0x4a; 16]);
    let physical_identities = crate::protocol::PhysicalIdentityManifest {
        tables: schema
            .tables
            .iter()
            .map(|table| {
                let columns = table
                    .columns
                    .iter()
                    .map(|column| {
                        let path = format!("table/{}/column/{}", table.name, column.name);
                        (
                            column.name.clone(),
                            crate::protocol::PhysicalColumnIdentity {
                                id: crate::ids::GlobalPhysicalColumnId(uuid::Uuid::new_v5(
                                    &namespace,
                                    path.as_bytes(),
                                )),
                                // The corpus schema intentionally contains no
                                // enums; enum identity fixtures remain in the
                                // focused catalogue corpus.
                                enum_variants: BTreeMap::new(),
                            },
                        )
                    })
                    .collect();
                (
                    table.name.clone(),
                    crate::protocol::PhysicalTableIdentity {
                        id: crate::ids::GlobalPhysicalTableId(uuid::Uuid::new_v5(
                            &namespace,
                            format!("table/{}", table.name).as_bytes(),
                        )),
                        columns,
                    },
                )
            })
            .collect(),
    };
    crate::protocol::CatalogueSnapshot {
        genesis_physical_identities: physical_identities,
        schemas: vec![crate::protocol::SchemaVersion::new(schema.clone())],
        lineages: Vec::new(),
        current_write_schema: crate::protocol::CurrentWriteSchema {
            revision: 0,
            schema: schema.version_id(),
        },
    }
}

fn native_corpus_receipt<S>(node: &NodeState<S>, schema: &JazzSchema) -> NativeCorpusReceipt
where
    S: OrderedKvStorage,
{
    let (tables, direct_stores) = native_corpus_storage_tables(schema);
    let mut stores = tables
        .into_iter()
        .map(|table| {
            let rows = crate::db::block_on(node.database.primary_key_scan_raw(&table, &[]))
                .unwrap_or_else(|error| panic!("scan corpus store {table}: {error}"));
            (
                table,
                rows.into_iter()
                    .map(|row| row.into_parts())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for store_name in direct_stores {
        let store = node
            .database
            .direct_record_store(&store_name)
            .unwrap_or_else(|error| panic!("open corpus direct store {store_name}: {error}"));
        let entries = crate::db::block_on(store.prefix_entries(&[]))
            .unwrap_or_else(|error| panic!("scan corpus direct store {store_name}: {error}"));
        let rows = entries
            .into_iter()
            .map(|entry| {
                let key = postcard::to_allocvec(&entry.key)
                    .expect("corpus direct-store key has a canonical semantic fixture");
                let values = entry
                    .value
                    .to_values()
                    .expect("corpus direct-store value decodes");
                let value = postcard::to_allocvec(&values)
                    .expect("corpus direct-store value has a canonical semantic fixture");
                (key, value)
            })
            .collect();
        stores.insert(store_name, rows);
    }

    // Application rows do not live in the authored logical table declarations.
    // They are lowered into permanent physical table identities, with separate
    // immutable-history, register, current-winner, and rejected families. A
    // corpus that scans only `lower_to_groove()` therefore misses the bytes it
    // exists to freeze.
    for table in &schema.tables {
        let table_id = node
            .physical_table_id_for_schema(schema.version_id(), &table.name)
            .unwrap_or_else(|error| panic!("resolve physical corpus table {}: {error}", table.name));
        for storage_table in [
            physical_history_table_name(table_id),
            physical_register_table_name(table_id),
            physical_global_current_table_name(table_id),
            physical_register_global_current_table_name(table_id),
            physical_ahead_current_table_name(table_id),
            physical_register_ahead_current_table_name(table_id),
            physical_rejected_versions_table_name(table_id),
        ] {
            let rows = crate::db::block_on(
                node.database.primary_key_scan_raw(&storage_table, &[]),
            )
            .unwrap_or_else(|error| panic!("scan physical corpus store {storage_table}: {error}"));
            assert!(
                stores
                    .insert(
                        storage_table.clone(),
                        rows.into_iter().map(|row| row.into_parts()).collect(),
                    )
                    .is_none(),
                "one permanent physical table identity must have one receipt entry: {storage_table}"
            );
        }
    }
    NativeCorpusReceipt { stores }
}

fn assert_native_corpus_has_required_families(receipt: &NativeCorpusReceipt) {
    for store in NATIVE_CORPUS_REQUIRED_STORES {
        assert!(
            receipt.stores.contains_key(*store),
            "the producer registry must include {store}"
        );
    }
    for store in [
        "jazz_catalogue",
        "jazz_nodes",
        "jazz_schema_versions",
        "jazz_transactions",
        "jazz_merge_heads",
        "jazz_global_changes",
        "jazz_deletion_history",
        "jazz_known_state_facts",
        "jazz_settled_result_members",
        "jazz_settled_program_facts",
    ] {
        assert!(
            !receipt.stores[store].is_empty(),
            "the settlement producer must actually write {store}"
        );
    }
}

fn seed_native_corpus<S>(
    node: &mut NodeState<S>,
    first_title: &str,
    note_body: &str,
) -> (RowUuid, TxId)
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let row_uuid = row(0xc1);
    let branch = native_corpus_branch(0xc2);
    let mut first_commit = MergeableCommit::new("todos", row_uuid, 100)
        .branch(branch.clone())
        .cells(BTreeMap::from([("title".to_owned(), v(first_title))]));
    // Normal applications deliberately receive fresh random retrieval
    // capabilities. The corpus is the exception: it needs one replayable
    // authority-produced byte snapshot, so Groove's test-only constructor
    // supplies deterministic capabilities while the ordinary stage/finalize
    // and row-publication paths remain unchanged.
    let prepared = groove::large_values::prepare_with_fixture_locators(
        groove::large_values::LargeValueKind::Bytes,
        &native_corpus_large_attachment(),
        b"jazz-storage-epoch-1-native-corpus",
    )
    .expect("fixture large value prepares");
    let upload_id = groove::large_values::StagedLargeValueId([0xc5; 16]);
    crate::db::block_on(node.begin_streaming_large_value_upload(
        upload_id,
        groove::large_values::LargeValueKind::Bytes,
    ))
    .expect("fixture upload establishes its normal pending journal");
    crate::db::block_on(node.stage_large_value_chunk_batch(
        upload_id,
        groove::large_values::LargeValueKind::Bytes,
        prepared.staged_chunks,
    ))
    .expect("fixture chunks stage through the normal node admission path");
    let staged = crate::db::block_on(node.finalize_large_value_upload(upload_id, prepared.value_ref))
        .expect("fixture root finalizes through the normal node admission path");
    first_commit
        .cells
        .insert("attachment".to_owned(), Value::Large(staged.value_ref));
    first_commit
        .prepared_large_columns
        .insert("attachment".to_owned());
    first_commit.staged_large_values.push(staged.id);
    let first = node
        .commit_mergeable_settled(first_commit)
        .expect("seed branch/history/current transaction");
    let second = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 101)
                .branch(branch)
                .parents(vec![first])
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("mixed-write predecessor")),
                    ("attachment".to_owned(), Value::Bytes(vec![3, 4, 5, 6])),
                ])),
        )
        .expect("seed second historical version");
    node.accept_global_for_test(first)
        .expect("settle first history version globally");
    node.accept_global_for_test(second)
        .expect("settle second history version globally");
    node.commit_mergeable_settled(
        MergeableCommit::new("notes", row(0xc3), 102)
            .cells(BTreeMap::from([("body".to_owned(), v(note_body))])),
    )
    .expect("seed independent current row");
    let deletion = node
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0xc6), 103)
                .deletion(DeletionEvent::Deleted),
        )
        .expect("seed an independent deletion history entry");
    node.accept_global_for_test(deletion)
        .expect("settle deletion history globally");
    (row_uuid, second)
}

fn assert_native_corpus_semantics<S>(node: &mut NodeState<S>, row_uuid: RowUuid)
where
    S: OrderedKvStorage,
{
    let versions = node.query_table_versions("todos").expect("history reads");
    assert_eq!(versions.len(), 2, "both immutable history versions survive");
    let latest = node.version_tx_id(&versions[1]).expect("latest history tx id");
    assert_eq!(node.version_tx_id(&versions[1]).unwrap(), latest);
    assert_eq!(
        node.transaction_record(latest)
            .expect("latest transaction is durable")
            .tx_id,
        latest
    );
    let rows = node
        .current_rows("todos", DurabilityTier::Local)
        .expect("current rows reopen");
    assert_eq!(rows.len(), 0, "branch rows stay out of the shared read view");
    assert!(node
        .query_table_versions("notes")
        .expect("independent table history")
        .iter()
        .any(|version| version.row_uuid() == row(0xc3)));
    assert!(versions.iter().all(|version| version.row_uuid() == row_uuid));

    // The receipt above deliberately records the row descriptor, not the
    // chunk backend's private install receipt.  Prove the complementary
    // durable contract directly: reopening the current node materializes the
    // whole indirect value through Groove's ordered chunk plane.
    let table = node.table("todos").expect("todos table remains known").clone();
    let attachment = versions[0]
        .cell(&table, "attachment")
        .expect("history attachment decodes")
        .expect("first version carries the indirect attachment");
    let Value::Large(value_ref) = attachment else {
        panic!("first history attachment must retain its large-value descriptor");
    };
    assert_eq!(
        crate::db::block_on(node.read_large_value_range(&value_ref, 0..value_ref.byte_length))
            .expect("reopened large tree materializes"),
        native_corpus_large_attachment(),
        "the indirect byte tree survives the native reopen"
    );
}

fn exercise_native_corpus<S>(
    schema: JazzSchema,
    snapshot: &crate::protocol::CatalogueSnapshot,
    open: impl Fn() -> S,
    open_with_incomplete_profile: impl Fn() -> Result<S, groove::storage::Error>,
    archive_historical: impl Fn(),
) -> NativeCorpusReceipt
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    // The physical manifest is authority-owned state.  Installing the same
    // published snapshot into the independent SQLite and RocksDB roots makes
    // their logical packs comparable without pretending local integer aliases
    // or freshly minted UUIDs are an interchange format.
    let mut producer = crate::db::block_on(NodeState::new_catalogue_uninitialized(node(0xc0), open()))
        .expect("open uninitialized settlement-baseline producer");
    producer
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .expect("install the one authority snapshot before corpus writes");
    let (row_uuid, _latest) = seed_native_corpus(
        &mut producer,
        "settlement baseline",
        "independent table",
    );
    publish_native_corpus_lineage(&mut producer, snapshot);
    seed_native_corpus_settled_query_state(&mut producer);
    let before_close = native_corpus_receipt(&producer, &schema);
    assert_native_corpus_has_required_families(&before_close);
    drop(producer);
    archive_historical();

    // A profile that omits Jazz's own codec IDs must be rejected at the
    // adapter manifest boundary.  Reopening the right profile immediately
    // afterwards proves that the failed admission did not reinterpret or
    // rewrite any producer bytes before reporting the incompatibility.
    assert!(
        open_with_incomplete_profile().is_err(),
        "a native store must reject an incomplete codec profile before opening Jazz data"
    );
    let unchanged_after_rejection =
        crate::db::block_on(NodeState::new(node(0xc0), schema.clone(), open()))
            .expect("correct profile reopens after rejected admission");
    assert_eq!(
        native_corpus_receipt(&unchanged_after_rejection, &schema),
        before_close,
        "rejected codec admission must not mutate the historical corpus"
    );
    drop(unchanged_after_rejection);

    // This open performs no application writes.  It proves that the current
    // runtime reads a separately constructed durable root before the mixed
    // current-format write below changes any physical family.
    let mut reopened = crate::db::block_on(NodeState::new(node(0xc0), schema.clone(), open()))
        .expect("open historical native corpus without mutation");
    assert_eq!(native_corpus_receipt(&reopened, &schema), before_close);
    assert_native_corpus_semantics(&mut reopened, row_uuid);

    reopened
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0xc4), 104)
                .cells(BTreeMap::from([("body".to_owned(), v("current writer"))])),
        )
        .expect("mixed current write");
    drop(reopened);

    let mut after_mixed_write =
        crate::db::block_on(NodeState::new(node(0xc0), schema.clone(), open()))
            .expect("reopen mixed store");
    assert_native_corpus_semantics(&mut after_mixed_write, row_uuid);
    assert!(
        after_mixed_write
            .query_table_versions("notes")
            .expect("mixed write history")
            .iter()
        .any(|version| version.row_uuid() == row(0xc4)),
        "new writer data survives without rewriting historical transaction bytes"
    );
    assert_ne!(
        native_corpus_checksum(&native_corpus_receipt(&after_mixed_write, &schema)),
        native_corpus_checksum(&before_close),
        "a real application-row write must change the corpus digest"
    );
    before_close
}

/// Open a committed historical physical fixture with current Jazz, without
/// giving the verifier any producer-only state. The fixture itself carries the
/// catalogue snapshot, physical IDs, row histories, current registers, and
/// large-value descriptor; current code must recover all of those before it
/// is allowed to add a new row.
fn verify_historical_native_corpus<S>(schema: JazzSchema, open: impl Fn() -> S)
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut reader = crate::db::block_on(NodeState::new(node(0xc0), schema.clone(), open()))
        .expect("current Jazz opens committed native corpus");
    let before_write = native_corpus_receipt(&reader, &schema);
    assert_native_corpus_has_required_families(&before_write);
    assert_eq!(
        native_corpus_pack(&before_write),
        epoch_1_native_corpus_pack(),
        "current Jazz reads the full committed historical logical pack"
    );
    assert_native_corpus_semantics(&mut reader, row(0xc1));
    reader
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0xc4), 104)
                .cells(BTreeMap::from([("body".to_owned(), v("current writer"))])),
        )
        .expect("current Jazz writes alongside committed history");
    drop(reader);

    let mut reopened = crate::db::block_on(NodeState::new(node(0xc0), schema, open()))
        .expect("current Jazz reopens mixed native corpus");
    assert_native_corpus_semantics(&mut reopened, row(0xc1));
    assert!(
        reopened
            .query_table_versions("notes")
            .expect("mixed-write notes history")
            .iter()
            .any(|version| version.row_uuid() == row(0xc4)),
        "current write survives a third-process reopen"
    );
}

fn in_memory_native_corpus_receipt(first_title: &str, note_body: &str) -> NativeCorpusReceipt {
    let schema = native_corpus_schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs).expect("open in-memory sensitivity store");
    let mut node = crate::db::block_on(NodeState::new(node(0xc0), schema.clone(), storage))
        .expect("open in-memory sensitivity node");
    seed_native_corpus(&mut node, first_title, note_body);
    native_corpus_receipt(&node, &schema)
}

/// Proves that both production native adapters share the same full Jazz
/// producer/reopen/mixed-write semantics under the closed epoch-one profile.
///
/// alice creates a branch-local row with two immutable versions and a byte
/// scalar, then closes the native store.  A fresh process reads the old state,
/// writes a new independent row, and a third process verifies both generations.
///
/// ```text
/// producer ──history + branch──► native store ──reopen──► reader
///                                                    │
///                                            current write
///                                                    ▼
///                                            third-process reopen
/// ```
#[test]
fn settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes() {
    let schema = native_corpus_schema();
    let snapshot = native_corpus_authority_snapshot(&schema);
    let profile = epoch_1_storage_codec_profile().expect("closed Jazz profile");

    let rocks_directory = tempfile::tempdir().expect("create RocksDB corpus directory");
    let rocks_path = rocks_directory.path().to_path_buf();
    let rocks_schema = schema.clone();
    let rocks_profile = profile.clone();
    let rocks_open_path = rocks_path.clone();
    let rocks_wrong_path = rocks_path.clone();
    let rocks_wrong_schema = rocks_schema.clone();
    let rocks_archive_path = std::env::var_os("JAZZ_NATIVE_CORPUS_ROCKS_ARCHIVE_OUT")
        .map(std::path::PathBuf::from);
    let rocks_receipt = exercise_native_corpus(rocks_schema.clone(), &snapshot, move || {
        let families = rocks_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        YieldingStorage::wrap(
            ImmediateRocksDbStorage::open_with_durability_and_codec_profile(
                &rocks_open_path,
                &refs,
                RocksDurability::FullSync,
                &rocks_profile,
            )
            .expect("open RocksDB corpus storage"),
        )
    }, move || {
        let families = rocks_wrong_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        ImmediateRocksDbStorage::open_with_durability_and_codec_profile(
            &rocks_wrong_path,
            &refs,
            RocksDurability::FullSync,
            &groove::storage::StorageCodecProfile::groove_epoch_1(),
        )
        .map(YieldingStorage::wrap)
    }, move || {
        let Some(output) = &rocks_archive_path else {
            return;
        };
        let output_file = std::fs::File::create(output).expect("create requested RocksDB archive");
        let encoder = flate2::write::GzEncoder::new(output_file, flate2::Compression::best());
        let mut archive = tar::Builder::new(encoder);
        archive
            .append_dir_all("rocksdb-epoch-1", &rocks_path)
            .expect("archive requested RocksDB corpus store");
        archive
            .into_inner()
            .expect("finish RocksDB tar archive")
            .finish()
            .expect("finish RocksDB gzip archive");
    });

    let sqlite_directory = tempfile::tempdir().expect("create SQLite corpus directory");
    let sqlite_path = sqlite_directory.path().join("jazz.sqlite");
    let sqlite_schema = schema;
    let sqlite_open_path = sqlite_path.clone();
    let sqlite_wrong_path = sqlite_path.clone();
    let sqlite_wrong_schema = sqlite_schema.clone();
    let sqlite_fixture_path = std::env::var_os("JAZZ_NATIVE_CORPUS_SQLITE_OUT")
        .map(std::path::PathBuf::from);
    let sqlite_receipt = exercise_native_corpus(sqlite_schema.clone(), &snapshot, move || {
        let families = sqlite_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        YieldingStorage::wrap(
            ImmediateSqliteStorage::open_with_durability_and_codec_profile(
                &sqlite_open_path,
                &refs,
                SqliteDurability::FullSync,
                &profile,
            )
            .expect("open SQLite corpus storage"),
        )
    }, move || {
        let families = sqlite_wrong_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        ImmediateSqliteStorage::open_with_durability_and_codec_profile(
            &sqlite_wrong_path,
            &refs,
            SqliteDurability::FullSync,
            &groove::storage::StorageCodecProfile::groove_epoch_1(),
        )
        .map(YieldingStorage::wrap)
    }, move || {
        let Some(output) = &sqlite_fixture_path else {
            return;
        };
        std::fs::copy(&sqlite_path, output).expect("copy requested SQLite corpus store");
    });

    assert_same_native_corpus(
        &rocks_receipt,
        &sqlite_receipt,
        "the native adapters must preserve the same canonical Jazz logical pack",
    );
    if let Some(path) = std::env::var_os("JAZZ_NATIVE_CORPUS_PACK_OUT") {
        // Maintainers deliberately request this output only while reviewing a
        // new epoch producer. Write before the pinned checks so the candidate
        // receipt remains available when either check correctly fails after
        // an intentional producer change.
        std::fs::write(&path, native_corpus_pack(&rocks_receipt))
            .expect("write requested native corpus pack");
    }
    assert_eq!(
        native_corpus_checksum(&rocks_receipt),
        "3a76fc5eb548bce90b16ac3a3c77daef32463b8290497f84e500226434a9d2d1",
        "a producer/codecs change must explicitly update the reviewed epoch-one corpus fixture"
    );
    assert_eq!(
        native_corpus_pack(&rocks_receipt),
        epoch_1_native_corpus_pack(),
        "the pinned producer must reproduce the committed backend-neutral logical pack"
    );
}

/// The committed SQLite and RocksDB byte fixtures are deliberately verified
/// independently from the live producer above. This is the compatibility
/// direction that matters at a later epoch: current code must open a store
/// created by the pinned producer, not merely agree with a store it just made.
#[test]
fn committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes() {
    let schema = native_corpus_schema();
    let profile = epoch_1_storage_codec_profile().expect("closed Jazz profile");

    let sqlite_directory = tempfile::tempdir().expect("create SQLite fixture directory");
    let sqlite_path = sqlite_directory.path().join("epoch-1-native-jazz.sqlite");
    materialize_native_sqlite_fixture(&sqlite_path, EPOCH_1_NATIVE_SQLITE_BASE64)
        .expect("materialize checksum-guarded SQLite corpus");
    {
        // This first physical inspection is read-only and intentionally below
        // the Jazz/Groove adapter. It proves the committed file is a SQLite
        // store with data before current code gets an opportunity to create a
        // journal or a metadata marker.
        let connection = rusqlite::Connection::open_with_flags(
            &sqlite_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .expect("open committed SQLite corpus read-only");
        let rows: u64 = connection
            .query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))
            .expect("read committed SQLite key/value rows");
        assert!(rows > 0, "committed SQLite corpus contains durable rows");
    }
    let sqlite_schema = schema.clone();
    let sqlite_profile = profile.clone();
    let sqlite_open_path = sqlite_path.clone();
    verify_historical_native_corpus(sqlite_schema.clone(), move || {
        let families = sqlite_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        YieldingStorage::wrap(
            ImmediateSqliteStorage::open_with_durability_and_codec_profile(
                &sqlite_open_path,
                &refs,
                SqliteDurability::FullSync,
                &sqlite_profile,
            )
            .expect("current SQLite adapter opens committed native corpus"),
        )
    });

    let rocks_directory = tempfile::tempdir().expect("create RocksDB fixture directory");
    let rocks_path = unpack_native_rocksdb_fixture(
        rocks_directory.path(),
        EPOCH_1_NATIVE_ROCKSDB_BASE64,
    )
    .expect("extract checksum-guarded RocksDB corpus");
    {
        let options = rocksdb::Options::default();
        let families = rocksdb::DB::list_cf(&options, &rocks_path)
            .expect("list committed RocksDB column families");
        let read_only = rocksdb::DB::open_cf_for_read_only(&options, &rocks_path, &families, false)
            .expect("open committed RocksDB corpus read-only");
        assert!(
            families.iter().any(|family| family == "__groove_storage_internal_v3"),
            "committed RocksDB corpus retains Groove's immutable internal family"
        );
        let rows = families
            .iter()
            .filter_map(|family| read_only.cf_handle(family))
            .map(|family| {
                read_only
                    .iterator_cf(family, rocksdb::IteratorMode::Start)
                    .count()
            })
            .sum::<usize>();
        assert!(rows > 0, "committed RocksDB corpus contains durable rows");
    }
    let rocks_schema = schema;
    let rocks_open_path = rocks_path.clone();
    verify_historical_native_corpus(rocks_schema.clone(), move || {
        let families = rocks_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        YieldingStorage::wrap(
            ImmediateRocksDbStorage::open_with_durability_and_codec_profile(
                &rocks_open_path,
                &refs,
                RocksDurability::FullSync,
                &profile,
            )
            .expect("current RocksDB adapter opens committed native corpus"),
        )
    });
}

#[test]
fn committed_native_jazz_physical_corpus_rejects_corruption_before_materialization() {
    let sqlite_root = tempfile::tempdir().expect("create SQLite corruption root");
    let sqlite_target = sqlite_root.path().join("must-not-exist.sqlite");
    let corrupt_sqlite = EPOCH_1_NATIVE_SQLITE_BASE64.replacen('A', "B", 1);
    assert!(
        materialize_native_sqlite_fixture(&sqlite_target, &corrupt_sqlite).is_err(),
        "a corrupt authoritative SQLite payload is rejected"
    );
    assert!(
        !sqlite_target.exists(),
        "SQLite checksum rejection precedes fixture materialization"
    );

    let rocks_root = tempfile::tempdir().expect("create RocksDB corruption root");
    let rocks_target = rocks_root.path().join("must-not-exist");
    let corrupt_rocks = EPOCH_1_NATIVE_ROCKSDB_BASE64.replacen('A', "B", 1);
    assert!(
        unpack_native_rocksdb_fixture(&rocks_target, &corrupt_rocks).is_err(),
        "a corrupt authoritative RocksDB payload is rejected"
    );
    assert!(
        !rocks_target.exists(),
        "RocksDB checksum rejection precedes archive extraction"
    );
}

/// Proves the frozen digest actually observes authored application content.
///
/// alice repeats the deterministic producer twice, changing only one branch
/// row's title, then repeats it again changing only an independent note body.
/// Both changes must perturb the logical pack; otherwise the receipt has
/// accidentally fallen back to system metadata only.
#[test]
fn native_jazz_corpus_digest_is_sensitive_to_application_row_bytes() {
    let baseline = in_memory_native_corpus_receipt("settlement baseline", "independent table");
    let changed_branch = in_memory_native_corpus_receipt("changed branch title", "independent table");
    let changed_note = in_memory_native_corpus_receipt("settlement baseline", "changed note body");

    assert_ne!(native_corpus_checksum(&baseline), native_corpus_checksum(&changed_branch));
    assert_ne!(native_corpus_checksum(&baseline), native_corpus_checksum(&changed_note));
}
