//! Neutral transaction vocabulary shared by public bindings and core glue.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::tx::TxId;

macro_rules! batch_id {
    ($name:ident, $kind:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub(crate) [u8; 16]);

        impl $name {
            pub fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", hex::encode(self.0))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                // These are compact semantic identifiers, so retain their
                // established hex form rather than expanding to a byte array.
                write!(f, concat!(stringify!($name), "({})"), self)
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(raw: &str) -> Result<Self, Self::Err> {
                let bytes =
                    hex::decode(raw).map_err(|err| format!("invalid {} hex: {err}", $kind))?;
                let len = bytes.len();
                let bytes: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| format!("expected 16-byte {}, got {len}", $kind))?;
                Ok(Self(bytes))
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                if serializer.is_human_readable() {
                    self.to_string().serialize(serializer)
                } else {
                    self.0.serialize(serializer)
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                if deserializer.is_human_readable() {
                    let raw = String::deserialize(deserializer)?;
                    raw.parse().map_err(serde::de::Error::custom)
                } else {
                    <[u8; 16]>::deserialize(deserializer).map(Self)
                }
            }
        }
    };
}

batch_id!(
    OpenBatchId,
    "open batch id",
    "Coordination-free identity for mutable, runtime-local work before commit."
);

impl OpenBatchId {
    /// Mint an open-batch identity without coordinating with a runtime.
    pub fn new() -> Self {
        Self(*Uuid::now_v7().as_bytes())
    }
}

impl Default for OpenBatchId {
    fn default() -> Self {
        Self::new()
    }
}

batch_id!(
    BatchId,
    "batch id",
    "Identity of an immutable committed batch."
);

impl BatchId {
    /// Derive the public committed-batch identity from core causal identity.
    ///
    /// The domain-separated digest keeps the core's wider `TxId` private while
    /// preserving a stable 128-bit identifier across bindings and runtimes.
    pub fn from_committed_tx(tx_id: TxId) -> Self {
        let mut input = [0_u8; 24];
        input[..8].copy_from_slice(&tx_id.time.0.to_be_bytes());
        input[8..].copy_from_slice(tx_id.node.0.as_bytes());
        let digest = blake3::derive_key("jazz committed batch id v1", &input);
        Self(digest[..16].try_into().expect("16-byte digest prefix"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeUuid;
    use crate::time::TxTime;

    #[test]
    fn open_batch_ids_are_canonical_uuid_v7_values() {
        let id = OpenBatchId::new();
        assert_eq!(id.to_string().len(), 32);
        assert_eq!(id.as_bytes()[6] >> 4, 7);
        assert_eq!(id.as_bytes()[8] >> 6, 2);
        assert_eq!(id.to_string().parse::<OpenBatchId>().unwrap(), id);
    }

    #[test]
    fn committed_batch_id_is_stable_and_domain_derived() {
        let tx = TxId::new(TxTime::from(42), NodeUuid(Uuid::from_bytes([7; 16])));
        let first = BatchId::from_committed_tx(tx);
        assert_eq!(first, BatchId::from_committed_tx(tx));
        assert_eq!(first.to_string().parse::<BatchId>().unwrap(), first);
        assert_ne!(
            first,
            BatchId::from_committed_tx(TxId::new(
                TxTime::from(43),
                NodeUuid(Uuid::from_bytes([7; 16]))
            ))
        );
    }

    #[test]
    fn batch_id_debug_is_compact_and_stable() {
        let id = BatchId::from_committed_tx(TxId::new(
            TxTime::from(42),
            NodeUuid(Uuid::from_bytes([7; 16])),
        ));
        let debug = format!("{id:?}");
        assert_eq!(debug, format!("BatchId({id})"));
        assert_eq!(debug.len(), "BatchId()".len() + 32);

        let open = OpenBatchId([0xab; 16]);
        assert_eq!(
            format!("{open:?}"),
            "OpenBatchId(abababababababababababababababab)"
        );
    }
}
