//! Closed Jazz storage profiles and the sole node-root admission coordinator.
//! Backends own physical atomicity; Jazz alone interprets legacy transaction
//! records before the no-payload-transform E1 to E2 transition.

use crate::groove::storage::{
    BoxedStorage, Error, OrderedKvStorage, ReadOnlyLayoutView, ReadOnlyStorage, StagedStorageOpen,
    StorageAdmission, StorageCodecProfile, StorageFactory, StorageLayout, StorageOpenSpec,
};

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

/// Immutable legacy profile, also retained by separately composed auxiliary
/// roots that do not persist Jazz node transactions.
pub fn epoch_1_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    StorageCodecProfile::groove_epoch_1()
        .with_additional_codecs(JAZZ_EPOCH_1_STORAGE_CODECS.iter().copied())
}

/// Node storage profile with the original exclusive read evidence codec.
pub fn epoch_2_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    epoch_1_storage_codec_profile()?.with_additional_codecs(["jazz.exclusive-read-evidence.v1"])
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
    let layout = ReadOnlyLayoutView::new(storage, StorageLayout::jazz_class_v1()).await?;
    let schema = crate::schema::JazzSchema::empty().lower_to_groove();
    let descriptor = schema
        .table("jazz_transactions")
        .expect("fixed legacy transaction table")
        .record_schema();
    let mut scan = layout.scan_prefix("jazz_transactions", &[]).await?;
    while let Some(rows) = scan.next_batch().await? {
        for (_, bytes) in rows {
            // Mapped storage values retain Groove's canonical whole-row tag;
            // the frozen transaction descriptor describes only its payload.
            let (_, payload) = crate::groove::records::split_variant_record(&bytes)
                .map_err(|error| Error::InvalidStorageLayout(error.to_string()))?;
            crate::exclusive_read_evidence::validate_epoch_one_transaction_record(
                crate::groove::records::BorrowedRecord::new(payload, &descriptor),
            )
            .map_err(|error| Error::InvalidStorageLayout(error.to_string()))?;
        }
    }
    Ok(())
}
