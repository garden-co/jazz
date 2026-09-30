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

/// The closed base profile shared by every persistent Jazz root.
///
/// Groove's mandatory epoch-one families remain first because codec IDs are
/// sorted by the profile constructor. An incompatible addition changes the
/// top-level manifest and therefore requires a new storage epoch. A separate
/// durable root (such as the server's catalogue-entry store) composes this
/// profile with its own root-local codec family before opening its adapter.
/// A root that holds row history opens with [`node_storage_codec_profile`].
pub fn epoch_1_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    StorageCodecProfile::groove_epoch_1()
        .with_additional_codecs(JAZZ_EPOCH_1_STORAGE_CODECS.iter().copied())
}

/// Row-history families declared by every root that stores Jazz rows (Core,
/// relay and client node stores, on every adapter), in canonical order.
///
/// `jazz.history-version-current.v4` is the linear row-state layout: one
/// history record per accepted transaction holding the merged row state,
/// `_deletion` as a cell, hidden `U48` column stamps, a `by_seq` current
/// index and a per-row ahead overlay. History and ahead-current tables carry
/// no `by_tx` secondary index; each `jazz_transactions` record lists the rows
/// it touched (`touched_rows`) instead. A history image stores `updated_by`
/// only when it differs from its transaction's `made_by`.
///
/// It replaces the unreleased `v2` (`by_tx` indexes, no `touched_rows`) and
/// `v3` (`touched_rows`, `updated_by` always stored) and the DAG layout (`jazz.history-version-current.v1`, alpha.54 to alpha.57),
/// which was never a manifest member. A root written by either lacks this
/// family and is refused at manifest admission with
/// [`Error::UnsupportedStorageCodecs`] before any record is decoded.
pub const JAZZ_NODE_STORAGE_CODECS: &[&str] = &["jazz.history-version-current.v4"];

/// The closed profile for a root that stores Jazz rows: the epoch-one base
/// plus [`JAZZ_NODE_STORAGE_CODECS`]. Auxiliary roots that hold no row
/// history (account registry, server catalogue entries) keep the base profile.
pub fn node_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
    epoch_1_storage_codec_profile()?
        .with_additional_codecs(JAZZ_NODE_STORAGE_CODECS.iter().copied())
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

    /// Internal manifest receipt: the node-root inventory is a durable byte
    /// contract that no public API exposes. It differs from the epoch-one
    /// base only by the row-history family, so an old node root (base only)
    /// is refused with the typed codec-inventory error.
    #[test]
    fn node_profile_has_a_pinned_manifest_receipt_and_refuses_base_only_roots() {
        use std::collections::BTreeMap;

        let parameters =
            BTreeMap::from([("key-order".to_owned(), b"unsigned-lexicographic".to_vec())]);
        let node = crate::groove::storage::StorageEpochManifest::epoch_1_with_codec_profile(
            "memory",
            1,
            parameters.clone(),
            &node_storage_codec_profile().expect("valid node profile"),
        )
        .expect("valid manifest");
        let expected = b"JSM1\0\x01\0\x01\x06memory\x0d\x15groove.large-value.v1\x1fgroove.ordered-chunk-storage.v1\x14groove.ordered-kv.v1\x12jazz.branch-key.v1\x1cjazz.catalogue.activation.v1\x21jazz.catalogue.bootstrap-ready.v1\x16jazz.catalogue.lens.v1\x19jazz.catalogue.lineage.v1\x22jazz.catalogue.physical-mapping.v1\x18jazz.catalogue.schema.v1\x1fjazz.catalogue.write-pointer.v1\x1fjazz.history-version-current.v4\x25jazz.subscription-program-fact-key.v1\x01\x09key-order\0\x16unsigned-lexicographic";
        assert_eq!(node.encode().expect("canonical manifest"), expected);

        let base_root = crate::groove::storage::StorageEpochManifest::epoch_1_with_codec_profile(
            "memory",
            1,
            parameters,
            &epoch_1_storage_codec_profile().expect("valid base profile"),
        )
        .expect("valid manifest")
        .encode()
        .expect("canonical manifest");
        match node.admit(Some(&base_root)) {
            Err(Error::UnsupportedStorageCodecs {
                epoch: 1,
                missing,
                unknown,
            }) => {
                assert_eq!(missing, vec!["jazz.history-version-current.v4".to_owned()]);
                assert!(unknown.is_empty());
            }
            other => panic!("expected a typed refusal of a base-only root, got {other:?}"),
        }
    }

    /// Roots written by the unreleased v2 row layout (history `by_tx`
    /// indexes) and v3 (`updated_by` in every history image) are refused
    /// with the typed error naming both families, before any record is
    /// decoded.
    #[test]
    fn node_profile_refuses_history_v2_and_v3_roots() {
        use std::collections::BTreeMap;

        let parameters =
            BTreeMap::from([("key-order".to_owned(), b"unsigned-lexicographic".to_vec())]);
        let node = crate::groove::storage::StorageEpochManifest::epoch_1_with_codec_profile(
            "memory",
            1,
            parameters.clone(),
            &node_storage_codec_profile().expect("valid node profile"),
        )
        .expect("valid manifest");
        for retired in [
            "jazz.history-version-current.v2",
            "jazz.history-version-current.v3",
        ] {
            let retired_root =
                crate::groove::storage::StorageEpochManifest::epoch_1_with_codec_profile(
                    "memory",
                    1,
                    parameters.clone(),
                    &epoch_1_storage_codec_profile()
                        .and_then(|profile| profile.with_additional_codecs([retired]))
                        .expect("valid retired profile"),
                )
                .expect("valid manifest")
                .encode()
                .expect("canonical manifest");
            match node.admit(Some(&retired_root)) {
                Err(Error::UnsupportedStorageCodecs {
                    epoch: 1,
                    missing,
                    unknown,
                }) => {
                    assert_eq!(missing, vec!["jazz.history-version-current.v4".to_owned()]);
                    assert_eq!(unknown, vec![retired.to_owned()]);
                }
                other => panic!("expected a typed refusal of a {retired} root, got {other:?}"),
            }
        }
    }
}
