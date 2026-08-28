// Settlement-baseline native Jazz storage corpus.
//
// This is deliberately an integration receipt over the production ordered-KV
// adapters rather than a second hand-written byte codec.  The eventual checked
// in physical fixtures generated from this scenario live in #2307; keeping the
// producer here makes the semantic shape, backend profile, and reopen contract
// executable beside the NodeState paths that actually write it.

use crate::storage_codec_profile::epoch_1_storage_codec_profile;
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

fn native_corpus_branch(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
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
    let first = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 100)
                .branch(branch.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v(first_title)),
                    ("attachment".to_owned(), Value::Bytes(vec![0, 1, 2, 255])),
                ])),
        )
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
    (row_uuid, second)
}

fn assert_native_corpus_semantics<S>(node: &mut NodeState<S>, row_uuid: RowUuid, latest: TxId)
where
    S: OrderedKvStorage,
{
    let versions = node.query_table_versions("todos").expect("history reads");
    assert_eq!(versions.len(), 2, "both immutable history versions survive");
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
}

fn exercise_native_corpus<S>(
    schema: JazzSchema,
    open: impl Fn() -> S,
    open_with_incomplete_profile: impl Fn() -> Result<S, groove::storage::Error>,
) -> NativeCorpusReceipt
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut producer = crate::db::block_on(NodeState::new(node(0xc0), schema.clone(), open()))
        .expect("open settlement-baseline producer");
    let (row_uuid, latest) = seed_native_corpus(
        &mut producer,
        "settlement baseline",
        "independent table",
    );
    let before_close = native_corpus_receipt(&producer, &schema);
    assert_native_corpus_has_required_families(&before_close);
    drop(producer);

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
    assert_native_corpus_semantics(&mut reopened, row_uuid, latest);

    reopened
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0xc4), 103)
                .cells(BTreeMap::from([("body".to_owned(), v("current writer"))])),
        )
        .expect("mixed current write");
    drop(reopened);

    let mut after_mixed_write =
        crate::db::block_on(NodeState::new(node(0xc0), schema.clone(), open()))
            .expect("reopen mixed store");
    assert_native_corpus_semantics(&mut after_mixed_write, row_uuid, latest);
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
    let profile = epoch_1_storage_codec_profile().expect("closed Jazz profile");

    let rocks_directory = tempfile::tempdir().expect("create RocksDB corpus directory");
    let rocks_path = rocks_directory.path().to_path_buf();
    let rocks_schema = schema.clone();
    let rocks_profile = profile.clone();
    let rocks_wrong_path = rocks_path.clone();
    let rocks_wrong_schema = rocks_schema.clone();
    let rocks_receipt = exercise_native_corpus(rocks_schema.clone(), move || {
        let families = rocks_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        YieldingStorage::wrap(
            ImmediateRocksDbStorage::open_with_durability_and_codec_profile(
                &rocks_path,
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
    });

    let sqlite_directory = tempfile::tempdir().expect("create SQLite corpus directory");
    let sqlite_path = sqlite_directory.path().join("jazz.sqlite");
    let sqlite_schema = schema;
    let sqlite_wrong_path = sqlite_path.clone();
    let sqlite_wrong_schema = sqlite_schema.clone();
    let sqlite_receipt = exercise_native_corpus(sqlite_schema.clone(), move || {
        let families = sqlite_schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        YieldingStorage::wrap(
            ImmediateSqliteStorage::open_with_durability_and_codec_profile(
                &sqlite_path,
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
    });

    assert_eq!(
        rocks_receipt, sqlite_receipt,
        "the native adapters must preserve the same canonical Jazz logical pack"
    );
    assert_eq!(
        native_corpus_checksum(&rocks_receipt),
        "fd05bb7c4d1fe89a5eb35cb7031e7af18f150eb6377166e858e135eaee35cbc7",
        "a producer/codecs change must explicitly update the reviewed epoch-one corpus fixture"
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
