//! The durable, top-level storage compatibility boundary.
//!
//! An adapter persists these bytes in its own fixed metadata location before it
//! writes ordinary ordered-KV data.  This module deliberately does not choose
//! that location: RocksDB, SQLite, and IndexedDB have different physical
//! metadata planes.  It does make the compatibility decision uniform and,
//! importantly, complete before an adapter is allowed to create a family, page,
//! table, or ordinary key.

use std::collections::{BTreeMap, BTreeSet};

use super::Error;

/// First settled Jazz/Groove durable format. Earlier alpha stores are unsupported.
pub const STORAGE_EPOCH_1: u16 = 1;
const MAGIC: &[u8; 4] = b"JSM1";

/// Adapter-specific physical format identity, pinned by the top-level epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterFormat {
    pub id: String,
    pub version: u16,
}

/// The complete decoding-relevant contract for one durable storage root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageEpochManifest {
    pub epoch: u16,
    pub adapter: AdapterFormat,
    /// Stable IDs of every authoritative codec reachable from this root.
    pub required_codecs: BTreeSet<String>,
    /// Adapter-owned, decode-relevant parameters, in canonical key order.
    pub parameters: BTreeMap<String, Vec<u8>>,
}

/// Evidence returned by the manifest gate. A successful receipt is the only
/// condition under which an adapter may mutate its ordinary data plane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestOpenReceipt {
    FreshEpoch1,
    ExistingEpoch1,
}

impl StorageEpochManifest {
    pub fn epoch_1(
        adapter_id: impl Into<String>,
        adapter_version: u16,
        required_codecs: impl IntoIterator<Item = impl Into<String>>,
        parameters: BTreeMap<String, Vec<u8>>,
    ) -> Result<Self, Error> {
        let manifest = Self {
            epoch: STORAGE_EPOCH_1,
            adapter: AdapterFormat {
                id: adapter_id.into(),
                version: adapter_version,
            },
            required_codecs: required_codecs.into_iter().map(Into::into).collect(),
            parameters,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Stable, length-delimited bytes. This is intentionally not serde: map
    /// ordering and omitted/default fields must not change a durable manifest.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut bytes = Vec::from(*MAGIC);
        bytes.extend_from_slice(&self.epoch.to_be_bytes());
        bytes.extend_from_slice(&self.adapter.version.to_be_bytes());
        put_string(&mut bytes, &self.adapter.id)?;
        put_count(&mut bytes, self.required_codecs.len())?;
        for codec in &self.required_codecs {
            put_string(&mut bytes, codec)?;
        }
        put_count(&mut bytes, self.parameters.len())?;
        for (key, value) in &self.parameters {
            put_string(&mut bytes, key)?;
            let length = u16::try_from(value.len()).map_err(|_| invalid("parameter too long"))?;
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(value);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut input = bytes;
        if take(&mut input, 4)? != MAGIC {
            return Err(invalid("manifest magic is invalid"));
        }
        let epoch = take_u16(&mut input)?;
        let version = take_u16(&mut input)?;
        let id = take_string(&mut input)?;
        let mut codecs = BTreeSet::new();
        let codec_count = take_u8(&mut input)?;
        for _ in 0..codec_count {
            let codec = take_string(&mut input)?;
            if !codecs.insert(codec) {
                return Err(invalid("manifest has duplicate codec ID"));
            }
        }
        let mut parameters = BTreeMap::new();
        let parameter_count = take_u8(&mut input)?;
        for _ in 0..parameter_count {
            let key = take_string(&mut input)?;
            let value_len = take_u16(&mut input)? as usize;
            let value = take(&mut input, value_len)?.to_vec();
            if parameters.insert(key, value).is_some() {
                return Err(invalid("manifest has duplicate parameter ID"));
            }
        }
        if !input.is_empty() {
            return Err(invalid("manifest has trailing bytes"));
        }
        let manifest = Self {
            epoch,
            adapter: AdapterFormat { id, version },
            required_codecs: codecs,
            parameters,
        };
        manifest.validate()?;
        // Require one canonical encoding rather than merely an equivalent map.
        if manifest.encode()? != bytes {
            return Err(invalid("manifest is noncanonical"));
        }
        Ok(manifest)
    }

    /// Validates an existing root before the caller mutates any ordinary state.
    pub fn admit_existing(&self, bytes: &[u8]) -> Result<ManifestOpenReceipt, Error> {
        let found = Self::decode(bytes)?;
        if found.epoch != self.epoch {
            return Err(invalid("unsupported storage epoch"));
        }
        if found != *self {
            return Err(invalid(
                "storage manifest is inconsistent with this adapter",
            ));
        }
        Ok(ManifestOpenReceipt::ExistingEpoch1)
    }

    /// Shared opening gate for an adapter's fixed metadata location. `None`
    /// denotes a verified empty root; only then may the adapter write these
    /// manifest bytes as its first durable mutation.
    pub fn admit(&self, existing: Option<&[u8]>) -> Result<ManifestOpenReceipt, Error> {
        match existing {
            Some(bytes) => self.admit_existing(bytes),
            None => Ok(ManifestOpenReceipt::FreshEpoch1),
        }
    }

    fn validate(&self) -> Result<(), Error> {
        if self.epoch != STORAGE_EPOCH_1 {
            return Err(invalid("unsupported storage epoch"));
        }
        valid_id("adapter ID", &self.adapter.id)?;
        if self.adapter.version == 0 {
            return Err(invalid("adapter format version is zero"));
        }
        for codec in &self.required_codecs {
            valid_id("codec ID", codec)?;
        }
        for key in self.parameters.keys() {
            valid_id("parameter ID", key)?;
        }
        Ok(())
    }
}

/// A future migration must be explicitly registered for exactly one adjacent
/// epoch transition. There is intentionally no epoch-1 migration.
pub trait StorageMigration {
    fn source_epoch(&self) -> u16;
    fn target_epoch(&self) -> u16;
}

/// Durable state an adapter records while it copy-on-write migrates. The
/// concrete namespace and recovery mechanics remain adapter-owned.
pub trait MigrationJournal {
    fn source_epoch(&self) -> u16;
    fn target_epoch(&self) -> u16;
}

#[derive(Default)]
pub struct MigrationRegistry<'a> {
    steps: Vec<&'a dyn StorageMigration>,
}

impl<'a> MigrationRegistry<'a> {
    pub fn new(steps: impl IntoIterator<Item = &'a dyn StorageMigration>) -> Result<Self, Error> {
        let registry = Self {
            steps: steps.into_iter().collect(),
        };
        for step in &registry.steps {
            if step.target_epoch()
                != step
                    .source_epoch()
                    .checked_add(1)
                    .ok_or_else(|| invalid("migration epoch overflow"))?
            {
                return Err(invalid("migration is not adjacent"));
            }
        }
        Ok(registry)
    }
    pub fn adjacent(&self, from: u16) -> Option<&'a dyn StorageMigration> {
        self.steps
            .iter()
            .copied()
            .find(|step| step.source_epoch() == from)
    }
}

fn valid_id(kind: &str, value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > u8::MAX as usize
        || !value.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.')
        })
    {
        return Err(invalid(&format!("invalid {kind}")));
    }
    Ok(())
}
fn invalid(message: &str) -> Error {
    Error::InvalidStorageLayout(message.to_owned())
}
fn put_count(bytes: &mut Vec<u8>, count: usize) -> Result<(), Error> {
    bytes.push(u8::try_from(count).map_err(|_| invalid("too many manifest entries"))?);
    Ok(())
}
fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<(), Error> {
    valid_id("manifest ID", value)?;
    bytes.push(value.len() as u8);
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}
fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], Error> {
    if input.len() < count {
        return Err(invalid("manifest is truncated"));
    }
    let (head, tail) = input.split_at(count);
    *input = tail;
    Ok(head)
}
fn take_u8(input: &mut &[u8]) -> Result<u8, Error> {
    Ok(take(input, 1)?[0])
}
fn take_u16(input: &mut &[u8]) -> Result<u16, Error> {
    Ok(u16::from_be_bytes(
        take(input, 2)?.try_into().expect("two bytes"),
    ))
}
fn take_string(input: &mut &[u8]) -> Result<String, Error> {
    let len = take_u8(input)? as usize;
    let raw = take(input, len)?;
    let value = std::str::from_utf8(raw)
        .map_err(|_| invalid("manifest ID is not UTF-8"))?
        .to_owned();
    valid_id("manifest ID", &value)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn manifest() -> StorageEpochManifest {
        StorageEpochManifest::epoch_1(
            "memory",
            1,
            ["groove.record.v1", "groove.key.v1"],
            BTreeMap::from([("key-order".into(), b"unsigned-lexicographic".to_vec())]),
        )
        .unwrap()
    }
    #[test]
    fn epoch_1_manifest_has_frozen_golden_bytes() {
        assert_eq!(manifest().encode().unwrap(), b"JSM1\0\x01\0\x01\x06memory\x02\x0dgroove.key.v1\x10groove.record.v1\x01\x09key-order\0\x16unsigned-lexicographic");
    }
    #[test]
    fn missing_unknown_inconsistent_and_corrupt_manifests_fail_closed() {
        let expected = manifest();
        assert!(expected.admit_existing(&[]).is_err());
        let mut unknown = expected.encode().unwrap();
        unknown[5] = 2;
        assert!(expected.admit_existing(&unknown).is_err());
        let other =
            StorageEpochManifest::epoch_1("memory", 2, ["groove.key.v1"], BTreeMap::new()).unwrap();
        assert!(expected.admit_existing(&other.encode().unwrap()).is_err());
        let mut corrupt = expected.encode().unwrap();
        corrupt[0] = b'X';
        assert!(expected.admit_existing(&corrupt).is_err());
    }
    #[test]
    fn planted_unknown_epoch_cannot_be_accepted() {
        let expected = manifest();
        let mut unknown = expected.encode().unwrap();
        unknown[5] = 2;
        assert!(expected.admit_existing(&unknown).is_err());
    }
    #[test]
    fn unknown_epoch_fails_before_an_open_can_mutate_ordinary_data() {
        let expected = manifest();
        let mut unknown = expected.encode().unwrap();
        unknown[5] = 2;
        let mut ordinary_mutations = 0;
        if expected.admit(Some(&unknown)).is_ok() {
            ordinary_mutations += 1;
        }
        assert_eq!(ordinary_mutations, 0);
    }
    struct Skip;
    impl StorageMigration for Skip {
        fn source_epoch(&self) -> u16 {
            1
        }
        fn target_epoch(&self) -> u16 {
            3
        }
    }
    #[test]
    fn migration_registry_rejects_skip_steps() {
        assert!(MigrationRegistry::new([&Skip as &dyn StorageMigration]).is_err());
    }
}
