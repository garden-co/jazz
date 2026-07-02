//! Admission and semantic size limits for Jazz protocol payloads.
//!
//! These limits protect allocation at the wire boundary and keep oversized
//! semantic requests recoverable. Server shells may eventually surface these as
//! configuration, but the core owns the default contract.

use crate::protocol::{ContentExtent, RowVersionRef, ShapeAst, SyncMessage, VersionRecord};

/// Maximum encoded `WireFrame` bytes accepted before postcard decode.
///
/// Source: twice the 1 MiB scalar-byte payload budget called out by the
/// unbounded-payload issue, leaving room for one legitimate large scalar row and
/// envelope overhead while forcing large batches to split by bytes.
pub const MAX_WIRE_FRAME_BYTES: usize = 2 * 1024 * 1024;

/// Maximum encoded `SyncMessage` bytes accepted inside a `WireEnvelope`.
///
/// Source: same budget as `MAX_WIRE_FRAME_BYTES`; semantic payloads must fit
/// the negotiated frame budget, while batching happens outside `SyncMessage`.
pub const MAX_SYNC_MESSAGE_BYTES: usize = MAX_WIRE_FRAME_BYTES;

/// Maximum postcard-encoded query shape registration payload.
///
/// Source: existing wire fixtures use tiny shapes; 64 KiB leaves headroom for
/// generated policy/query shapes without letting `ShapeAst` become an allocation
/// vector. Server shells may make this configurable later for unusually large
/// generated schemas.
pub const MAX_SHAPE_AST_BYTES: usize = 64 * 1024;

/// Maximum number of row-version records in one commit unit.
///
/// Source: matches the node's existing tx-version table cache order of
/// magnitude and is far above current fixture/bench commit arity. Byte limits
/// remain the primary memory guard.
pub const MAX_COMMIT_UNIT_VERSIONS: usize = 4096;

/// Maximum encoded bytes for one commit unit.
///
/// Source: same 2 MiB semantic payload budget. A larger transaction must be
/// split into multiple mergeable commits or rejected as malformed.
pub const MAX_COMMIT_UNIT_BYTES: usize = MAX_SYNC_MESSAGE_BYTES;

/// Maximum row-version repair refs in one `FetchRowVersions` request.
///
/// Source: matches the first known-state repair tier; large reconnect holes
/// should batch exact requests instead of creating unbounded semantic vectors.
pub const MAX_FETCH_ROW_VERSIONS: usize = 1024;

/// Maximum bytes in one `ContentExtent` response payload.
///
/// Source: ch. 12's content lane has 64 KiB blob chunk targets and 64 MiB bundle
/// targets; 1 MiB comfortably exceeds legitimate current chunks while bounding a
/// single bulk-lane allocation.
pub const MAX_CONTENT_EXTENT_BYTES: usize = 1024 * 1024;

/// Validate raw frame bytes before postcard can allocate from declared lengths.
pub fn validate_wire_frame_len(len: usize) -> Result<(), String> {
    validate_len("wire frame", len, MAX_WIRE_FRAME_BYTES)
}

/// Validate raw encoded sync payload bytes before decoding the semantic message.
pub fn validate_sync_message_len(len: usize) -> Result<(), String> {
    validate_len("sync message payload", len, MAX_SYNC_MESSAGE_BYTES)
}

/// Validate a shape registration after sync-message decode but before storing it.
pub fn validate_shape_ast_size(ast: &ShapeAst) -> Result<(), String> {
    let bytes = postcard::to_allocvec(ast)
        .map_err(|err| format!("failed to measure shape AST payload: {err}"))?;
    validate_len("shape AST", bytes.len(), MAX_SHAPE_AST_BYTES)
}

/// Validate content extent payloads after sync-message decode.
pub fn validate_content_extents(extents: &[ContentExtent]) -> Result<(), String> {
    for extent in extents {
        validate_len(
            "content extent bytes",
            extent.bytes.len(),
            MAX_CONTENT_EXTENT_BYTES,
        )?;
    }
    Ok(())
}

/// Validate row-version repair request size after sync-message decode.
pub fn validate_fetch_row_versions(requests: &[RowVersionRef]) -> Result<(), String> {
    if requests.len() > MAX_FETCH_ROW_VERSIONS {
        return Err(format!(
            "row-version repair request count {} exceeds max {}",
            requests.len(),
            MAX_FETCH_ROW_VERSIONS
        ));
    }
    Ok(())
}

/// Return a malformed-commit reason when the commit unit exceeds protocol limits.
pub fn commit_unit_limit_violation(
    tx: &crate::tx::Transaction,
    versions: &[VersionRecord],
    encoded_len: Option<usize>,
) -> Option<String> {
    if versions.len() > MAX_COMMIT_UNIT_VERSIONS {
        return Some(format!(
            "commit unit version count {} exceeds max {}",
            versions.len(),
            MAX_COMMIT_UNIT_VERSIONS
        ));
    }
    if let Some(len) = encoded_len {
        return (len > MAX_COMMIT_UNIT_BYTES).then(|| {
            format!(
                "commit unit encoded size {} exceeds max {}",
                len, MAX_COMMIT_UNIT_BYTES
            )
        });
    }
    let message = SyncMessage::CommitUnit {
        tx: tx.clone(),
        versions: versions.to_vec(),
    };
    match postcard::to_allocvec(&message) {
        Ok(bytes) if bytes.len() > MAX_COMMIT_UNIT_BYTES => Some(format!(
            "commit unit encoded size {} exceeds max {}",
            bytes.len(),
            MAX_COMMIT_UNIT_BYTES
        )),
        Ok(_) => None,
        Err(err) => Some(format!("failed to measure commit unit payload: {err}")),
    }
}

fn validate_len(label: &str, len: usize, max: usize) -> Result<(), String> {
    if len > max {
        Err(format!("{label} size {len} exceeds max {max}"))
    } else {
        Ok(())
    }
}
