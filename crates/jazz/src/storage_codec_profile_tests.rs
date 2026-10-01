//! Cross-layer node admission and serving-root compatibility receipts.
use crate::storage_codec_profile::*;
use groove::storage::{
    BoxedStorage, Error, OrderedKvStorage, ReadOnlyLayoutView, ReadOnlyStorage, StagedStorageOpen,
    StorageAdmission, StorageCodecProfile, StorageFactory, StorageLayout, StorageOpenSpec,
};
#[cfg(test)]
mod tests {
    use super::*;

    /// Deliberately implements the pre-admission adapter interface. Omitting
    /// admission must fail rather than silently treating a third-party adapter
    /// as ephemeral.
    struct UnsupportedAdapter(crate::groove::storage::MemoryStorage);

    impl OrderedKvStorage for UnsupportedAdapter {
        fn get(
            &self,
            cf: String,
            key: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<Option<Vec<u8>>, Error>> {
            self.0.get(cf, key)
        }
        fn set(
            &self,
            cf: String,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<(), Error>> {
            self.0.set(cf, key, value)
        }
        fn delete(
            &self,
            cf: String,
            key: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<(), Error>> {
            self.0.delete(cf, key)
        }
        fn put_if_absent(
            &self,
            cf: String,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<Option<Vec<u8>>, Error>> {
            self.0.put_if_absent(cf, key, value)
        }
        fn compare_and_delete(
            &self,
            cf: String,
            key: Vec<u8>,
            expected: Vec<u8>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<bool, Error>> {
            self.0.compare_and_delete(cf, key, expected)
        }
        fn scan(
            &self,
            request: crate::groove::storage::ScanRequest,
        ) -> crate::groove::storage::StorageFuture<
            '_,
            Result<crate::groove::storage::StorageScan<'_>, Error>,
        > {
            self.0.scan(request)
        }
        fn write_many(
            &self,
            operations: Vec<crate::groove::storage::OwnedWriteOperation>,
        ) -> crate::groove::storage::StorageFuture<'_, Result<(), Error>> {
            self.0.write_many(operations)
        }
        fn column_family_names(&self) -> Option<Vec<String>> {
            self.0.column_family_names()
        }
    }

    impl crate::groove::storage::ReopenableStorage for UnsupportedAdapter {
        fn reopen(
            self,
            families: Vec<String>,
        ) -> crate::groove::storage::StorageFuture<'static, Result<Self, Error>> {
            Box::pin(async move {
                Ok(Self(
                    crate::groove::storage::ReopenableStorage::reopen(self.0, families).await?,
                ))
            })
        }
    }

    #[test]
    fn node_entries_reject_unsupported_adapter_before_layout_writes() {
        crate::db::block_on(async {
            let families = crate::schema::JazzSchema::empty().column_families();
            let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
            for entry in 0..4 {
                let inner = crate::groove::storage::MemoryStorage::new(&refs).unwrap();
                let retained = inner.clone();
                assert!(matches!(
                    open_at_node_entry(entry, BoxedStorage::new(UnsupportedAdapter(inner))).await,
                    Err(crate::node::Error::Storage(Error::UnsupportedAdmission))
                ));
                for family in &families {
                    assert!(
                        retained
                            .prefix(family.clone(), Vec::new())
                            .await
                            .unwrap()
                            .is_empty()
                    );
                }
            }
        });
    }

    async fn open_at_node_entry(
        entry: usize,
        storage: BoxedStorage,
    ) -> Result<crate::node::NodeState<BoxedStorage>, crate::node::Error> {
        let node = crate::ids::NodeUuid::from_bytes([0x61; 16]);
        let schema = crate::schema::JazzSchema::empty();
        match entry {
            0 => crate::node::NodeState::new(node, schema, storage).await,
            1 => crate::node::NodeState::new_history_complete(node, schema, storage).await,
            2 => crate::node::NodeState::new_client(node, schema, storage, false).await,
            3 => crate::node::NodeState::new_catalogue_uninitialized(node, storage).await,
            _ => unreachable!(),
        }
    }

    // These are storage-admission tests, not application-query tests: an
    // untouched layout marker and unchanged manifest are the public failure
    // contract and cannot be observed through a Db that correctly failed open.
    #[test]
    fn node_entries_reject_unadmitted_durable_roots_before_layout_writes() {
        crate::db::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let rocks = jazz_storage_rocksdb::RocksDbStorageFactory::default();
            let sqlite = jazz_storage_sqlite::SqliteStorageFactory::default();
            let families = crate::schema::JazzSchema::empty().column_families();
            for (adapter, factory) in [
                ("rocks", &rocks as &dyn StorageFactory),
                ("sqlite", &sqlite as &dyn StorageFactory),
            ] {
                for (profile_index, profile) in [
                    StorageCodecProfile::groove_epoch_1(),
                    epoch_1_storage_codec_profile().unwrap(),
                ]
                .into_iter()
                .enumerate()
                {
                    for entry in 0..4 {
                        let path = directory
                            .path()
                            .join(format!("{adapter}-{profile_index}-{entry}"));
                        let storage = factory
                            .open(path.clone(), families.clone(), profile.clone())
                            .await
                            .unwrap();
                        let before = storage.admission().unwrap();
                        let names = storage.column_family_names().unwrap();
                        assert!(matches!(
                            open_at_node_entry(entry, storage).await,
                            Err(crate::node::Error::Storage(Error::InvalidStorageLayout(_)))
                        ));
                        let unchanged = factory
                            .open(path, families.clone(), profile.clone())
                            .await
                            .unwrap();
                        assert_eq!(unchanged.admission().unwrap(), before);
                        assert_eq!(unchanged.column_family_names().unwrap(), names);
                        for family in names {
                            assert!(
                                unchanged
                                    .prefix(family, Vec::new())
                                    .await
                                    .unwrap()
                                    .is_empty(),
                                "{adapter} entry {entry} wrote before admission"
                            );
                        }
                    }
                }
            }
        });
    }

    #[test]
    fn exact_empty_epoch_one_crash_root_migrates_but_unmarked_payload_does_not() {
        crate::db::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let rocks = jazz_storage_rocksdb::RocksDbStorageFactory::default();
            let sqlite = jazz_storage_sqlite::SqliteStorageFactory::default();
            let profile = epoch_1_storage_codec_profile().unwrap();
            let mut families = crate::schema::JazzSchema::empty().column_families();
            families.push("unmapped-payload".into());
            for (adapter, factory) in [
                ("rocks", &rocks as &dyn StorageFactory),
                ("sqlite", &sqlite as &dyn StorageFactory),
            ] {
                let path = directory.path().join(format!("{adapter}-empty"));
                // Crash boundary: adapter manifest committed, no Node or
                // LayoutStorage constructor has ever run.
                drop(
                    factory
                        .open(path.clone(), families.clone(), profile.clone())
                        .await
                        .unwrap(),
                );
                let admitted = open_node_storage(factory, path.clone(), families.clone())
                    .await
                    .unwrap();
                assert!(matches!(
                    admitted.admission().unwrap(),
                    StorageAdmission::Durable(admission) if admission.is_migrated()
                ));
                for family in admitted.column_family_names().unwrap() {
                    assert!(
                        admitted
                            .prefix(family, Vec::new())
                            .await
                            .unwrap()
                            .is_empty(),
                        "preflight must not install the layout marker"
                    );
                }
                drop(admitted);
                drop(
                    open_node_storage(factory, path, families.clone())
                        .await
                        .unwrap(),
                );

                let path = directory.path().join(format!("{adapter}-unmarked"));
                let storage = factory
                    .open(path.clone(), families.clone(), profile.clone())
                    .await
                    .unwrap();
                storage
                    .set(
                        "unmapped-payload".into(),
                        b"key".to_vec(),
                        b"do not adopt".to_vec(),
                    )
                    .await
                    .unwrap();
                let before = storage.admission().unwrap();
                drop(storage);
                assert!(
                    open_node_storage(factory, path.clone(), families.clone())
                        .await
                        .is_err()
                );
                let unchanged = factory
                    .open(path, families.clone(), profile.clone())
                    .await
                    .unwrap();
                assert_eq!(unchanged.admission().unwrap(), before);
                assert_eq!(
                    unchanged
                        .get("unmapped-payload".into(), b"key".to_vec())
                        .await
                        .unwrap(),
                    Some(b"do not adopt".to_vec())
                );
            }
        });
    }

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
    fn epoch_two_jazz_profile_has_a_pinned_manifest_receipt() {
        use std::collections::BTreeMap;
        let legacy = crate::groove::storage::StorageEpochManifest::epoch_1_with_codec_profile(
            "memory",
            1,
            BTreeMap::from([("key-order".to_owned(), b"unsigned-lexicographic".to_vec())]),
            &epoch_1_storage_codec_profile().unwrap(),
        )
        .unwrap();
        let manifest = legacy
            .with_open_spec(&node_storage_open_specs().unwrap().1)
            .unwrap();
        let expected = b"JSM1\0\x02\0\x01\x06memory\x0d\x15groove.large-value.v1\x1fgroove.ordered-chunk-storage.v1\x14groove.ordered-kv.v1\x12jazz.branch-key.v1\x1cjazz.catalogue.activation.v1\x21jazz.catalogue.bootstrap-ready.v1\x16jazz.catalogue.lens.v1\x19jazz.catalogue.lineage.v1\x22jazz.catalogue.physical-mapping.v1\x18jazz.catalogue.schema.v1\x1fjazz.catalogue.write-pointer.v1\x1fjazz.exclusive-read-evidence.v1\x25jazz.subscription-program-fact-key.v1\x01\x09key-order\0\x16unsigned-lexicographic";
        assert_eq!(manifest.encode().unwrap(), expected);
        assert_eq!(
            crate::groove::storage::StorageEpochManifest::decode(expected).unwrap(),
            manifest
        );
        assert!(
            legacy.admit_existing(expected).is_err(),
            "E1 open must reject the E2 profile"
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
                block_on(open_node_storage(
                    &RocksDbStorageFactory::default(),
                    path.to_path_buf(),
                    families.clone(),
                ))
                .expect("admit fixture writer"),
                identity(),
            )))
            .expect("open fixture node");
            for title in ["legacy transaction", "unrelated transaction"] {
                let write = block_on(db.insert(
                    "notes",
                    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
                    Default::default(),
                ))
                .expect("seed a real mergeable transaction");
                block_on(write.wait(DurabilityTier::Local)).expect("persist fixture write");
            }
            block_on(db.close()).expect("close fixture node");
            drop(db);
            // The current public writer must obey E2 admission. Convert only
            // this closed fixture to exact legacy form: no evidence in any of
            // slots 5–8, immutable E1 profile, and no admission receipt.
            let storage = block_on(open_node_storage(
                &RocksDbStorageFactory::default(),
                path.to_path_buf(),
                families.clone(),
            ))
            .expect("reopen fixture for legacy conversion");
            let mut database = block_on(Database::new_with_storage_layout(
                schema.lower_to_groove(),
                storage,
                StorageLayout::jazz_class_v1(),
            ))
            .expect("open fixture records");
            let rows = block_on(database.primary_key_scan_raw("jazz_transactions", &[]))
                .expect("scan fixture transactions")
                .into_iter()
                .map(|row| {
                    let mut values = row.record().to_values().expect("fixture transaction");
                    for value in &mut values[5..=8] {
                        *value = Value::Nullable(None);
                    }
                    values
                })
                .collect::<Vec<_>>();
            let mut batch = database.open_batch();
            for values in rows {
                batch.update("jazz_transactions", values);
            }
            let applied =
                block_on(database.apply_batch(batch)).expect("stage legacy evidence slots");
            database
                .finish_persistence(block_on(applied.persist()))
                .expect("persist legacy evidence slots");
            block_on(database.close()).expect("close fixture conversion");
            drop(database);
            let options = rocksdb::Options::default();
            let cfs = rocksdb::DB::list_cf(&options, path).expect("fixture families");
            let raw =
                rocksdb::DB::open_cf(&options, path, cfs).expect("exclusive fixture metadata");
            let internal = raw
                .cf_handle("__groove_storage_internal_v1")
                .expect("manifest family");
            let current = raw.get_cf(internal, b"epoch-manifest").unwrap().unwrap();
            let legacy = crate::groove::storage::StorageEpochManifest::decode(&current)
                .unwrap()
                .with_open_spec(&StorageOpenSpec {
                    epoch: 1,
                    codec_profile: profile.clone(),
                })
                .unwrap();
            let mut batch = rocksdb::WriteBatch::default();
            batch.put_cf(internal, b"epoch-manifest", legacy.encode().unwrap());
            batch.delete_cf(internal, b"admission-receipt");
            raw.write_opt(&batch, &rocksdb::WriteOptions::default())
                .expect("publish exact E1 fixture");
        };
        let open_normally = |path: &Path| {
            InMemoryServerShell::start_with_storage(
                InMemoryServerShellConfig::new(schema.clone(), identity())
                    .with_storage_factory(Arc::new(RocksDbStorageFactory::default())),
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
