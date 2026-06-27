//! Neutral transaction vocabulary shared by public bindings and direct-core glue.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::digest::Digest32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BatchId(pub [u8; 16]);

impl BatchId {
    pub fn new() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(*uuid.as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for BatchId {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(raw).map_err(|err| format!("invalid batch id hex: {err}"))?;
        let len = bytes.len();
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| format!("expected 16-byte batch id, got {len}"))?;
        Ok(Self(bytes))
    }
}

impl From<BatchId> for Digest32 {
    fn from(value: BatchId) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(&value.0);
        Digest32(bytes)
    }
}

impl From<Digest32> for BatchId {
    fn from(value: Digest32) -> Self {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&value.0[..16]);
        Self(bytes)
    }
}

impl Serialize for BatchId {
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

impl<'de> Deserialize<'de> for BatchId {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchMode {
    Direct,
    Transactional,
}

impl BatchMode {
    pub fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "direct" | "Direct" => Ok(Self::Direct),
            "transactional" | "Transactional" => Ok(Self::Transactional),
            _ => Err(format!("invalid batch mode: {raw}")),
        }
    }
}
