//! Closed storage profiles for independently persisted Jazz roots.
//!
//! Each Jazz marker versions the complete root-owned layout. Individual
//! encoding receipts remain documented and tested without separate manifest
//! markers. Groove and adapter formats retain their own compatibility gates.

use crate::groove::storage::{Error, StorageCodecProfile};

/// Node-owned format plus its durable schema indexes.
pub const JAZZ_NODE_STORAGE_CODECS: &[&str] = &[
    crate::groove::storage::GROOVE_DURABLE_INDEX_V2_CODEC,
    "jazz.node-storage.v1",
];
/// Format owned by the independent server catalogue.
pub const JAZZ_CATALOGUE_STORAGE_CODECS: &[&str] = &["jazz.catalogue-storage.v1"];
/// Format owned by the independent account journal.
pub const JAZZ_ACCOUNT_STORAGE_CODECS: &[&str] = &["jazz.account-storage.v1"];

/// Client, relay and Core records, including transactions, history, branches,
/// catalogue state and receiver metadata. Only node roots need schema indexes.
pub fn node_storage_codec_profile() -> Result<StorageCodecProfile, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groove::storage::StorageEpochManifest;
    use std::collections::BTreeMap;

    fn manifest(profile: &StorageCodecProfile) -> StorageEpochManifest {
        StorageEpochManifest::epoch_1_with_codec_profile("memory", 1, BTreeMap::new(), profile)
            .unwrap()
    }

    /// Internal byte receipts: Alice's node, catalogue and account roots each
    /// have one Jazz marker. Public rows cannot expose the manifest grammar.
    #[test]
    fn root_profiles_have_pinned_manifest_bytes() {
        for (profile, expected) in [
            (node_storage_codec_profile().unwrap(), b"JSM1\x00\x01\x00\x01\x06memory\x05\x17groove.durable-index.v2\x15groove.large-value.v2\x1fgroove.ordered-chunk-storage.v1\x14groove.ordered-kv.v1\x14jazz.node-storage.v1\x00".as_slice()),
            (catalogue_storage_codec_profile().unwrap(), b"JSM1\x00\x01\x00\x01\x06memory\x04\x15groove.large-value.v2\x1fgroove.ordered-chunk-storage.v1\x14groove.ordered-kv.v1\x19jazz.catalogue-storage.v1\x00".as_slice()),
            (account_storage_codec_profile().unwrap(), b"JSM1\x00\x01\x00\x01\x06memory\x04\x15groove.large-value.v2\x1fgroove.ordered-chunk-storage.v1\x14groove.ordered-kv.v1\x17jazz.account-storage.v1\x00".as_slice()),
        ] {
            assert_eq!(manifest(&profile).encode().unwrap(), expected);
            assert_eq!(StorageEpochManifest::decode(expected).unwrap().encode().unwrap(), expected);
            assert_eq!(profile.codec_ids().filter(|id| id.starts_with("jazz.")).count(), 1);
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
