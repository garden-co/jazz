//! Closed storage profiles for independently persisted Jazz roots.
//!
//! Each Jazz marker versions the complete root-owned layout. Individual
//! encoding receipts remain documented and tested without separate manifest
//! markers. Groove and adapter formats retain their own compatibility gates.

use crate::groove::storage::{
    BoxedStorage, Error, OrderedKvStorage, ReadOnlyLayoutView, ReadOnlyStorage, StagedStorageOpen,
    StorageAdmission, StorageCodecProfile, StorageFactory, StorageLayout, StorageOpenSpec,
};

/// Node-owned format plus its durable schema indexes and Groove's dictionary
/// of interned column values.
///
/// `groove.dictionary.v1`: every stored author column (`made_by`,
/// `created_by`, `updated_by`) holds a 4-byte code into the node-local
/// `authors` dictionary instead of the full author. A node root written before
/// interned author columns lacks it and is refused with
/// [`Error::UnsupportedStorageCodecs`] before any record is decoded.
pub const JAZZ_NODE_STORAGE_CODECS: &[&str] = &[
    crate::groove::storage::GROOVE_DICTIONARY_V1_CODEC,
    crate::groove::storage::GROOVE_DURABLE_INDEX_V2_CODEC,
    "jazz.node-storage.v2",
];
/// Format owned by the independent server catalogue.
pub const JAZZ_CATALOGUE_STORAGE_CODECS: &[&str] = &["jazz.catalogue-storage.v1"];
/// Format owned by the independent account journal.
pub const JAZZ_ACCOUNT_STORAGE_CODECS: &[&str] = &["jazz.account-storage.v1"];

/// Immutable legacy profile, also retained by separately composed auxiliary
/// roots that do not persist Jazz node transactions.
pub fn epoch_1_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    StorageCodecProfile::groove_epoch_1()
        .with_additional_codecs(JAZZ_NODE_STORAGE_CODECS.iter().copied())
}

/// The server's independent catalogue entries and their persisted payloads.
pub fn catalogue_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    StorageCodecProfile::groove_epoch_1()
        .with_additional_codecs(JAZZ_CATALOGUE_STORAGE_CODECS.iter().copied())
}

/// The account registry's independent ordered command journal.
pub fn account_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    StorageCodecProfile::groove_epoch_1()
        .with_additional_codecs(JAZZ_ACCOUNT_STORAGE_CODECS.iter().copied())
}

/// Legacy node profile used by epoch-one roots.
pub fn node_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    epoch_1_storage_codec_profile()
}

/// Epoch two retains complete exclusive proof after settlement. The root marker
/// versions that replay contract and retains the predicate-evidence v2 grammar.
pub fn epoch_2_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    StorageCodecProfile::groove_epoch_1().with_additional_codecs([
        crate::groove::storage::GROOVE_DICTIONARY_V1_CODEC,
        crate::groove::storage::GROOVE_DURABLE_INDEX_V2_CODEC,
        "jazz.node-storage.v3",
    ])
}

/// Exact legacy source and current target specifications for node admission.
pub fn node_storage_open_specs() -> Result<(StorageOpenSpec, StorageOpenSpec), Error> {
    Ok((
        StorageOpenSpec {
            epoch: 1,
            codec_profile: epoch_1_storage_codec_profile()?,
        },
        StorageOpenSpec {
            epoch: 2,
            codec_profile: epoch_2_storage_codec_profile()?,
        },
    ))
}

/// Reject unadmitted durable handles before any Groove constructor can write
/// even a layout marker. Explicitly ephemeral adapters need no durable receipt.
pub fn require_node_storage_admission(storage: &impl OrderedKvStorage) -> Result<(), Error> {
    match storage.admission()? {
        StorageAdmission::Ephemeral => Ok(()),
        StorageAdmission::Durable(admission) => {
            let manifest = admission.manifest();
            let (source_spec, target_spec) = node_storage_open_specs()?;
            let source = manifest.with_open_spec(&source_spec)?;
            let target = manifest.with_open_spec(&target_spec)?;
            if manifest != &target {
                return Err(Error::InvalidStorageLayout(
                    "Jazz node requires admitted epoch two storage".into(),
                ));
            }
            admission.validate(&source, &target)
        }
    }
}

/// Normal persistent node open: an exact legacy root is scanned under the
/// adapter's exclusive guard, then manifest and receipt are published together.
pub async fn open_node_storage(
    factory: &dyn StorageFactory,
    path: std::path::PathBuf,
    column_families: Vec<String>,
) -> Result<BoxedStorage, Error> {
    let (source, target) = node_storage_open_specs()?;
    let storage = match factory
        .open_staged(path, column_families, source, target)
        .await?
    {
        StagedStorageOpen::Ready(storage) => storage,
        StagedStorageOpen::Guard(guard) => {
            preflight_epoch_one_node_storage(guard.read_only()).await?;
            guard.complete().await?
        }
    };
    require_node_storage_admission(&storage)?;
    Ok(storage)
}

/// Browser admission invokes this same Jazz-owned scanner before publishing
/// its receipt. Neither the backend nor JavaScript knows transaction slots.
pub async fn preflight_epoch_one_node_storage(storage: ReadOnlyStorage<'_>) -> Result<(), Error> {
    let layout = ReadOnlyLayoutView::new(storage, StorageLayout::jazz_class_v3()).await?;
    let schema = crate::schema::JazzSchema::empty().lower_to_groove();
    let transaction_descriptor = schema
        .table("jazz_transactions")
        .expect("transaction table")
        .record_schema();
    let evidence_descriptor = schema
        .table("jazz_pending_transactions")
        .expect("evidence table")
        .record_schema();
    let invalid = |message: &str| Error::InvalidStorageLayout(message.to_owned());
    let mut transactions = std::collections::BTreeMap::new();
    let mut scan = layout.scan_prefix("jazz_transactions", &[]).await?;
    while let Some(rows) = scan.next_batch().await? {
        for (_, bytes) in rows {
            let (_, payload) = crate::groove::records::split_variant_record(&bytes)
                .map_err(|error| invalid(&error.to_string()))?;
            let record =
                crate::groove::records::BorrowedRecord::new(payload, &transaction_descriptor);
            let key = (record.get_u64(0)?, record.get_u64(1)?);
            transactions.insert(key, record.get_enum(2)?);
        }
    }
    let mut scan = layout.scan_prefix("jazz_pending_transactions", &[]).await?;
    while let Some(rows) = scan.next_batch().await? {
        for (_, bytes) in rows {
            let (_, payload) = crate::groove::records::split_variant_record(&bytes)
                .map_err(|error| invalid(&error.to_string()))?;
            let record = crate::groove::records::BorrowedRecord::new(payload, &evidence_descriptor);
            let key = (record.get_u64(0)?, record.get_u64(1)?);
            if transactions.get(&key) != Some(&1) {
                return Err(invalid(
                    "exclusive proof has no exclusive transaction header",
                ));
            }
            crate::exclusive_read_evidence::validate_epoch_one_transaction_record(record)
                .map_err(|error| invalid(&error.to_string()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groove::storage::StorageEpochManifest;
    use std::collections::BTreeMap;

    fn manifest(profile: &StorageCodecProfile) -> StorageEpochManifest {
        StorageEpochManifest::epoch_1_with_codec_profile("memory", 1, BTreeMap::new(), profile)
            .unwrap()
    }

    /// Internal byte receipt: Alice's target root changes epoch and Jazz
    /// marker together, while preserving all Groove encoding families.
    #[test]
    fn epoch_two_root_has_pinned_manifest_bytes() {
        let target = epoch_2_storage_codec_profile().unwrap();
        let manifest = manifest(&target)
            .with_open_spec(&StorageOpenSpec {
                epoch: 2,
                codec_profile: target,
            })
            .unwrap();
        let expected = b"JSM1\x00\x02\x00\x01\x06memory\x06\x14groove.dictionary.v1\x17groove.durable-index.v2\x15groove.large-value.v2\x1fgroove.ordered-chunk-storage.v2\x14groove.ordered-kv.v1\x14jazz.node-storage.v3\x00";
        assert_eq!(manifest.encode().unwrap(), expected);
        assert_eq!(StorageEpochManifest::decode(expected).unwrap(), manifest);
    }

    /// Internal admission receipt: Alice's old root cannot be read as the
    /// retained-proof format, and an old build cannot read the target root.
    #[test]
    fn epoch_one_and_two_node_roots_refuse_each_other() {
        let (source, target) = node_storage_open_specs().unwrap();
        let source = manifest(&source.codec_profile)
            .with_open_spec(&source)
            .unwrap();
        let target = manifest(&target.codec_profile)
            .with_open_spec(&target)
            .unwrap();
        assert!(source.admit_existing(&target.encode().unwrap()).is_err());
        assert!(target.admit_existing(&source.encode().unwrap()).is_err());
    }

    /// Internal byte receipts: Alice's node, catalogue and account roots each
    /// have one Jazz marker. Public rows cannot expose the manifest grammar.
    #[test]
    fn root_profiles_have_pinned_manifest_bytes() {
        for (profile, expected) in [
            (node_storage_codec_profile().unwrap(), b"JSM1\x00\x01\x00\x01\x06memory\x06\x14groove.dictionary.v1\x17groove.durable-index.v2\x15groove.large-value.v2\x1fgroove.ordered-chunk-storage.v2\x14groove.ordered-kv.v1\x14jazz.node-storage.v2\x00".as_slice()),
            (catalogue_storage_codec_profile().unwrap(), b"JSM1\x00\x01\x00\x01\x06memory\x04\x15groove.large-value.v2\x1fgroove.ordered-chunk-storage.v2\x14groove.ordered-kv.v1\x19jazz.catalogue-storage.v1\x00".as_slice()),
            (account_storage_codec_profile().unwrap(), b"JSM1\x00\x01\x00\x01\x06memory\x04\x15groove.large-value.v2\x1fgroove.ordered-chunk-storage.v2\x14groove.ordered-kv.v1\x17jazz.account-storage.v1\x00".as_slice()),
        ] {
            assert_eq!(manifest(&profile).encode().unwrap(), expected);
            assert_eq!(StorageEpochManifest::decode(expected).unwrap().encode().unwrap(), expected);
            assert_eq!(profile.codec_ids().filter(|id| id.starts_with("jazz.")).count(), 1);
        }
    }

    /// Node format v2 adds union row cases (SPEC 10 §10.3). Alice's v2 build
    /// refuses a v1 node root at open, and a v1 build refuses her v2 root
    /// at open instead of failing at its first union row.
    #[test]
    fn node_storage_v1_and_v2_roots_refuse_each_other() {
        let v2 = node_storage_codec_profile().unwrap();
        let v1 = StorageCodecProfile::groove_epoch_1()
            .with_additional_codecs([
                crate::groove::storage::GROOVE_DICTIONARY_V1_CODEC,
                crate::groove::storage::GROOVE_DURABLE_INDEX_V2_CODEC,
                "jazz.node-storage.v1",
            ])
            .unwrap();
        for (opener, root, missing_id, unknown_id) in [
            (&v2, &v1, "jazz.node-storage.v2", "jazz.node-storage.v1"),
            (&v1, &v2, "jazz.node-storage.v1", "jazz.node-storage.v2"),
        ] {
            assert!(matches!(
                manifest(opener).admit_existing(&manifest(root).encode().unwrap()),
                Err(Error::UnsupportedStorageCodecs { missing, unknown, .. })
                    if missing == [missing_id] && unknown == [unknown_id]
            ));
        }
    }

    /// Alice cannot open one root type as another, or admit missing, future,
    /// or retired formats. No ordinary record is read at this boundary.
    #[test]
    fn root_profiles_reject_other_roots_and_changed_inventories() {
        let profiles = [
            node_storage_codec_profile().unwrap(),
            catalogue_storage_codec_profile().unwrap(),
            account_storage_codec_profile().unwrap(),
        ];
        for (index, profile) in profiles.iter().enumerate() {
            for (other_index, other) in profiles.iter().enumerate() {
                let bytes = manifest(other).encode().unwrap();
                assert_eq!(
                    manifest(profile).admit_existing(&bytes).is_ok(),
                    index == other_index
                );
            }
            let bare = manifest(&StorageCodecProfile::groove_epoch_1())
                .encode()
                .unwrap();
            assert!(matches!(
                manifest(profile).admit_existing(&bare),
                Err(Error::UnsupportedStorageCodecs { .. })
            ));
            for extra in ["jazz.subscription-program-fact-key.v1", "jazz.future.v2"] {
                let changed = profile.with_additional_codecs([extra]).unwrap();
                assert!(
                    matches!(manifest(profile).admit_existing(&manifest(&changed).encode().unwrap()),
                    Err(Error::UnsupportedStorageCodecs { missing, unknown, .. }) if missing.is_empty() && unknown == [extra])
                );
            }
        }
    }
}
