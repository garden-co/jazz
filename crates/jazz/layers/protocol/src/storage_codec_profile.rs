//! Jazz's closed epoch-one persistent-codec inventory.
//!
//! This module owns only the names of Jazz byte families, not adapter opening
//! or backend semantics. Groove composes the profile into a durable manifest
//! as opaque identifiers; every adapter validates the same resulting set
//! before it interprets or mutates a persistent Jazz root.

use crate::groove::storage::{Error, StorageCodecProfile};

/// Epoch-one Jazz-owned durable codec families, in canonical lexical order.
///
/// Each identifier covers one independently versioned semantic byte family.
/// Values that merely use Groove's typed record encoding do not acquire a
/// second Jazz codec ID; byte fields whose interpretation belongs to Jazz do.
pub const JAZZ_EPOCH_1_STORAGE_CODECS: &[&str] = &[
    "jazz.branch-key.v1",
    "jazz.catalogue.activation.v1",
    "jazz.catalogue.bootstrap-ready.v1",
    "jazz.catalogue.lens.v1",
    "jazz.catalogue.lineage.v1",
    "jazz.catalogue.physical-mapping.v1",
    "jazz.catalogue.schema.v1",
    "jazz.catalogue.write-pointer.v1",
    // Reserved to open old roots and discard their retired subscription caches.
    // No active scope writer or payload decoder uses this family.
    "jazz.subscription-program-fact-key.v1",
];

/// The closed base profile required by every persistent Jazz node.
///
/// Groove's mandatory epoch-one families remain first because codec IDs are
/// sorted by the profile constructor. An incompatible addition changes the
/// top-level manifest and therefore requires a new storage epoch. A separate
/// durable root (such as the server's catalogue-entry store) composes this
/// profile with its own root-local codec family before opening its adapter.
pub fn epoch_1_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    StorageCodecProfile::groove_epoch_1()
        .with_additional_codecs(JAZZ_EPOCH_1_STORAGE_CODECS.iter().copied())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_one_jazz_profile_is_closed_and_canonically_sorted() {
        let profile = epoch_1_storage_codec_profile().expect("valid fixed profile");
        assert_eq!(
            profile.codec_ids().collect::<Vec<_>>(),
            vec![
                "groove.large-value.v1",
                "groove.ordered-chunk-storage.v1",
                "groove.ordered-kv.v1",
                "jazz.branch-key.v1",
                "jazz.catalogue.activation.v1",
                "jazz.catalogue.bootstrap-ready.v1",
                "jazz.catalogue.lens.v1",
                "jazz.catalogue.lineage.v1",
                "jazz.catalogue.physical-mapping.v1",
                "jazz.catalogue.schema.v1",
                "jazz.catalogue.write-pointer.v1",
                "jazz.subscription-program-fact-key.v1",
            ]
        );
    }

    #[test]
    fn epoch_one_jazz_profile_has_a_pinned_manifest_receipt() {
        use std::collections::BTreeMap;

        let manifest = crate::groove::storage::StorageEpochManifest::epoch_1_with_codec_profile(
            "memory",
            1,
            BTreeMap::from([("key-order".to_owned(), b"unsigned-lexicographic".to_vec())]),
            &epoch_1_storage_codec_profile().expect("valid fixed profile"),
        )
        .expect("valid manifest");
        let expected = b"JSM1\0\x01\0\x01\x06memory\x0c\x15groove.large-value.v1\x1fgroove.ordered-chunk-storage.v1\x14groove.ordered-kv.v1\x12jazz.branch-key.v1\x1cjazz.catalogue.activation.v1\x21jazz.catalogue.bootstrap-ready.v1\x16jazz.catalogue.lens.v1\x19jazz.catalogue.lineage.v1\x22jazz.catalogue.physical-mapping.v1\x18jazz.catalogue.schema.v1\x1fjazz.catalogue.write-pointer.v1\x25jazz.subscription-program-fact-key.v1\x01\x09key-order\0\x16unsigned-lexicographic";
        assert_eq!(manifest.encode().expect("canonical manifest"), expected);
        assert_eq!(
            crate::groove::storage::StorageEpochManifest::decode(expected)
                .expect("fixture decodes")
                .encode()
                .expect("fixture re-encodes"),
            expected
        );
    }

    #[cfg(feature = "runtime")]
    #[test]
    fn epoch_one_reserved_transaction_slots_reject_normal_jazz_open_without_mutation() {
        use std::collections::BTreeMap;
        use std::path::Path;
        use std::sync::Arc;

        use crate::db::{Db, DbConfig, DbIdentity, block_on};
        use crate::groove::db::Database;
        use crate::groove::records::Value;
        use crate::groove::storage::StorageLayout;
        use crate::ids::{AuthorSubject, NodeUuid};
        use crate::schema::JazzSchema;
        use crate::serving::{
            InMemoryServerShell, InMemoryServerShellConfig, ShellError, StorageConfig,
        };
        use crate::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
        use crate::tx::DurabilityTier;
        use jazz_storage_rocksdb::{Durability, RocksDbStorage, RocksDbStorageFactory};

        // Internal fixture access is necessary: ordinary writes cannot create
        // legacy reserved-slot bytes, and app queries cannot observe a manifest.
        // Compare logical CF contents, not SST/WAL files that RocksDB may compact.
        fn snapshot(path: &Path) -> BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>> {
            let options = rocksdb::Options::default();
            let families = rocksdb::DB::list_cf(&options, path).expect("list fixture families");
            let storage = rocksdb::DB::open_cf_for_read_only(&options, path, &families, false)
                .expect("inspect fixture without mutation");
            families
                .iter()
                .map(|family| {
                    let handle = storage.cf_handle(family).expect("existing fixture family");
                    let rows = storage
                        .iterator_cf(handle, rocksdb::IteratorMode::Start)
                        .map(|entry| {
                            let (key, value) = entry.expect("read fixture entry");
                            (key.to_vec(), value.to_vec())
                        })
                        .collect();
                    (family.clone(), rows)
                })
                .collect()
        }

        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("notes").column("title", ColumnType::Text))
                .build(),
        )
        .expect("valid migration fixture schema");
        let identity = || DbIdentity {
            node: NodeUuid::from_bytes([0x73; 16]),
            author: AuthorSubject::SYSTEM,
        };
        let profile = epoch_1_storage_codec_profile().expect("exact admitted Jazz E1 profile");
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let open_legacy = |path: &Path| {
            RocksDbStorage::open_with_durability_and_codec_profile(
                path,
                &refs,
                Durability::FullSync,
                &profile,
            )
            .expect("open exact Jazz E1 fixture")
        };
        let seed = |path: &Path| {
            let db = block_on(Db::open_history_complete(DbConfig::new(
                schema.clone(),
                open_legacy(path),
                identity(),
            )))
            .expect("open legacy node");
            for title in ["legacy transaction", "unrelated transaction"] {
                let write = block_on(db.insert(
                    "notes",
                    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
                    Default::default(),
                ))
                .expect("seed a real mergeable transaction");
                block_on(write.wait(DurabilityTier::Local)).expect("persist fixture write");
            }
            block_on(db.close()).expect("close legacy node");
        };
        let open_normally = |path: &Path| {
            InMemoryServerShell::start_with_storage(
                InMemoryServerShellConfig::new(schema.clone(), identity())
                    .with_storage_factory(Arc::new(RocksDbStorageFactory)),
                StorageConfig::RocksDb {
                    path: path.to_path_buf(),
                },
            )
        };

        // This otherwise identical E1 root must remain admissible. In particular,
        // rejecting every old manifest cannot satisfy the corruption regression.
        let valid = tempfile::tempdir().expect("create valid E1 root");
        seed(valid.path());
        drop(open_normally(valid.path()).expect("normal Jazz open must admit valid E1 rows"));

        for slot in 5..=8 {
            let directory = tempfile::tempdir().expect("create reserved-slot fixture");
            seed(directory.path());
            {
                let mut database = block_on(Database::new_with_storage_layout(
                    schema.lower_to_groove(),
                    open_legacy(directory.path()),
                    StorageLayout::jazz_class_v1(),
                ))
                .expect("open legacy records for fixture corruption");
                let transactions =
                    block_on(database.primary_key_scan_raw("jazz_transactions", &[]))
                        .expect("scan seeded transaction records");
                let mut values = transactions
                    .first()
                    .expect("public insert persisted a transaction")
                    .record()
                    .to_values()
                    .expect("decode otherwise valid transaction");
                assert!(
                    values[5..=8]
                        .iter()
                        .all(|value| matches!(value, Value::Nullable(None))),
                    "legacy mergeable fixture must start without exclusive evidence"
                );
                values[slot] = Value::Nullable(Some(Box::new(Value::Bytes(
                    b"unexpected legacy reserved bytes".to_vec(),
                ))));
                let mut batch = database.open_batch();
                batch.update("jazz_transactions", values);
                let applied =
                    block_on(database.apply_batch(batch)).expect("stage fixture mutation");
                let persisted = block_on(applied.persist());
                database
                    .finish_persistence(persisted)
                    .expect("persist fixture mutation");
                block_on(database.close()).expect("close fixture record access");
            }

            let before = snapshot(directory.path());
            let error = open_normally(directory.path()).expect_err(
                "normal Jazz open must reject non-NULL E1 transaction evidence before migration",
            );
            assert!(
                matches!(error, ShellError::Storage(_)),
                "slot {slot} must fail storage preflight, not schema or node initialization: {error}"
            );
            assert_eq!(
                snapshot(directory.path()),
                before,
                "slot {slot} rejection must preserve the manifest, every row and every family"
            );
        }
    }
}
