//! Chunk inputs for interruptible large-value operations.
//!
//! Every operation that may need a node it does not have runs as an
//! *attempt*: it reads nodes from [`ChunkInputs`], records each one that is
//! missing, and fails with [`AttemptError::Blocked`] once nothing else can
//! proceed. The caller fetches the missing set, installs it, and runs the
//! attempt again. Nodes are immutable, so an installed node stays valid for
//! every later attempt and every snapshot.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use thiserror::Error;

use super::chunks::ChunkRequest;

/// The nodes an attempt may read, and the ones it found missing.
#[derive(Debug, Default)]
pub struct ChunkInputs {
    loaded: BTreeMap<ChunkRequest, LoadedChunk>,
    missing: BTreeSet<ChunkRequest>,
}

/// One installed node.
#[derive(Debug)]
pub struct LoadedChunk {
    bytes: Bytes,
    verified: Option<[u8; 32]>,
}

impl LoadedChunk {
    /// The encoded node.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// Whether the installer already proved these bytes hash to `expected`,
    /// so decoding need not hash them again.
    pub fn verifies_object_hash(&self, expected: &[u8; 32]) -> bool {
        self.verified.as_ref() == Some(expected)
    }
}

impl ChunkInputs {
    /// Install a node whose bytes have not been verified yet.
    pub fn install_chunk(&mut self, request: ChunkRequest, bytes: Bytes) {
        self.loaded.insert(
            request,
            LoadedChunk {
                bytes,
                verified: None,
            },
        );
    }

    /// Install a node read from a store that verified, on the way in, that
    /// its bytes hash to `request.object_hash`.
    pub fn install_verified_chunk(&mut self, request: ChunkRequest, bytes: Bytes) {
        let verified = Some(request.object_hash);
        self.loaded.insert(request, LoadedChunk { bytes, verified });
    }

    /// The installed node `request` names, or [`AttemptError::Blocked`]
    /// after recording it as missing.
    pub fn loaded_chunk(&mut self, request: ChunkRequest) -> Result<&LoadedChunk, AttemptError> {
        if !self.loaded.contains_key(&request) {
            self.missing.insert(request);
            return Err(AttemptError::Blocked);
        }
        Ok(&self.loaded[&request])
    }

    /// The installed node's bytes.
    pub fn chunk(&mut self, request: ChunkRequest) -> Result<&Bytes, AttemptError> {
        self.loaded_chunk(request).map(LoadedChunk::bytes)
    }

    /// Every node found missing since the last call.
    pub fn take_missing_chunks(&mut self) -> Vec<ChunkRequest> {
        std::mem::take(&mut self.missing).into_iter().collect()
    }

    /// Forget every installed node, to bound memory between operations.
    pub fn clear(&mut self) {
        self.loaded.clear();
        self.missing.clear();
    }

    /// The number of installed nodes.
    pub fn len(&self) -> usize {
        self.loaded.len()
    }

    /// Whether no node is installed.
    pub fn is_empty(&self) -> bool {
        self.loaded.is_empty()
    }
}

/// Why an attempt did not finish.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum AttemptError {
    /// A node it needs is missing; fetch [`ChunkInputs::take_missing_chunks`]
    /// and run the attempt again.
    #[error("large-value attempt is blocked on missing chunks")]
    Blocked,
    /// The value or a node is invalid.
    #[error(transparent)]
    LargeValue(#[from] super::Error),
}
