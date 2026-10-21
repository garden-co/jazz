//! Application snapshot codec. This is separate from SnapshotRef's established
//! storage/wire serde layout; see dev/proofs/snapshot-reference-compatibility.md.

use super::*;
use serde::Deserialize;

const PREFIX: &str = "jazz-snapshot-v1:";
const HEADER_BYTES: usize = 36;
const DOT_BYTES: usize = 24;

/// An application snapshot reference has an unknown version or malformed bytes.
#[derive(Debug, thiserror::Error)]
#[error("invalid or unsupported snapshot reference")]
pub struct SnapshotRefFormatError;

impl SnapshotRef {
    /// Encode an application-persistable v1 reference with lossless u64 fields.
    pub fn to_persisted(&self) -> String {
        let dots = self
            .dots
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut bytes = Vec::with_capacity(HEADER_BYTES + DOT_BYTES * dots.len());
        bytes.extend_from_slice(self.owner.0.as_bytes());
        bytes.extend_from_slice(&self.global_base.0.to_le_bytes());
        bytes.extend_from_slice(&self.local_base.0.to_le_bytes());
        bytes.extend_from_slice(
            &u32::try_from(dots.len())
                .expect("snapshot dot count fits u32")
                .to_le_bytes(),
        );
        for dot in dots {
            bytes.extend_from_slice(&dot.time.0.to_le_bytes());
            bytes.extend_from_slice(dot.node.0.as_bytes());
        }
        format!("{PREFIX}{}", hex::encode(bytes))
    }

    /// Restore a v1 reference. Parsing neither loads history nor grants access.
    pub fn from_persisted(value: &str) -> Result<Self, SnapshotRefFormatError> {
        let encoded = value.strip_prefix(PREFIX).ok_or(SnapshotRefFormatError)?;
        if encoded.len() < HEADER_BYTES * 2
            || !encoded
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(SnapshotRefFormatError);
        }
        let bytes = hex::decode(encoded).map_err(|_| SnapshotRefFormatError)?;
        let count = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
        if bytes.len().checked_sub(HEADER_BYTES) != count.checked_mul(DOT_BYTES) {
            return Err(SnapshotRefFormatError);
        }
        let owner = NodeUuid::from_bytes(bytes[..16].try_into().unwrap());
        let global_base = GlobalTime(u64::from_le_bytes(bytes[16..24].try_into().unwrap()));
        let local_base = TxTime(u64::from_le_bytes(bytes[24..32].try_into().unwrap()));
        let dots = bytes[HEADER_BYTES..]
            .chunks_exact(DOT_BYTES)
            .map(|dot| TxId {
                time: TxTime(u64::from_le_bytes(dot[..8].try_into().unwrap())),
                node: NodeUuid::from_bytes(dot[8..].try_into().unwrap()),
            })
            .collect();
        Ok(Self {
            owner,
            global_base,
            local_base,
            dots,
        })
    }
}

// Keep the non-human-readable layout byte-for-byte compatible. Human-readable
// binding arguments additionally accept the application string, so neither
// JavaScript host has to round a snapshot's u64 coordinates through a number.
impl<'de> Deserialize<'de> for SnapshotRef {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            owner: NodeUuid,
            global_base: GlobalTime,
            local_base: TxTime,
            #[serde(default)]
            dots: Vec<TxId>,
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Human {
            Reference(String),
            Fields(Fields),
        }
        let fields = if deserializer.is_human_readable() {
            match Human::deserialize(deserializer)? {
                Human::Reference(value) => {
                    return Self::from_persisted(&value).map_err(serde::de::Error::custom);
                }
                Human::Fields(fields) => fields,
            }
        } else {
            Fields::deserialize(deserializer)?
        };
        Ok(Self {
            owner: fields.owner,
            global_base: fields.global_base,
            local_base: fields.local_base,
            dots: fields.dots,
        })
    }
}
