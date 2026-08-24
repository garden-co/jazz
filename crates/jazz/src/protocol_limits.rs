//! Admission and semantic size limits for Jazz protocol payloads.
//!
//! These limits protect allocation at the wire boundary and keep oversized
//! semantic requests recoverable. Server shells may eventually surface these as
//! configuration, but the core owns the default contract.

use crate::protocol::{KnownStateDeclaration, RowVersionRef, ShapeAst, VersionRecord};

/// Maximum encoded `WireFrame` bytes accepted before postcard decode.
///
/// Source: twice the 1 MiB scalar-byte payload budget called out by the
/// unbounded-payload issue, leaving room for one legitimate large scalar row and
/// envelope overhead while forcing large batches to split by bytes.
pub const MAX_WIRE_FRAME_BYTES: usize = 2 * 1024 * 1024;

/// Maximum raw wire frames carried by one postcard WebSocket batch.
///
/// Carrier encoders split above this count. It is deliberately the same as
/// the maximum atomic commit-unit cardinality; meanwhile the 512 KiB
/// fragmentation extent means even a maximum legal 256 MiB logical message
/// needs only 512 physical frames. This keeps a tiny-frame flood from being
/// retained or staged beyond a bounded cardinality at the WebSocket boundary.
pub const MAX_WIRE_BATCH_FRAMES: usize = MAX_COMMIT_UNIT_VERSIONS;

/// Resource ceiling for one decoded logical message, independent of framing.
///
/// This prevents allocation bombs while allowing normal database payloads to
/// span many physical frames. Deployments may make it configurable later.
pub const MAX_LOGICAL_MESSAGE_BYTES: usize = 256 * 1024 * 1024;
/// Per-peer aggregate memory budget for incomplete logical messages.
pub const MAX_INFLIGHT_LOGICAL_MESSAGE_BYTES: usize = MAX_LOGICAL_MESSAGE_BYTES;
/// Per-peer fairness bound for concurrently incomplete logical messages.
pub const MAX_INFLIGHT_LOGICAL_MESSAGES: usize = 4;

/// Maximum postcard-encoded query shape registration payload.
///
/// Source: existing wire fixtures use tiny shapes; 64 KiB leaves headroom for
/// generated policy/query shapes without letting `ShapeAst` become an allocation
/// vector. Server shells may make this configurable later for unusually large
/// generated schemas.
pub const MAX_SHAPE_AST_BYTES: usize = 64 * 1024;

/// Maximum number of row-version records in one commit unit.
///
/// This bounds one atomic validation/storage critical section and the number of
/// authorization decisions charged to one peer turn. It is independent of
/// encoded byte size and physical framing.
pub const MAX_COMMIT_UNIT_VERSIONS: usize = 4096;

/// Maximum row-version repair refs in one `FetchRowVersions` request.
///
/// Source: matches the first known-state repair tier; large reconnect holes
/// should batch exact requests instead of creating unbounded semantic vectors.
pub const MAX_FETCH_ROW_VERSIONS: usize = 1024;
/// Maximum exact row-version refs in one slow known-state declaration.
///
/// Source: same count tier as `FetchRowVersions`; larger local holdings should
/// degrade to no declaration and full ship. Truncation is forbidden because it
/// would silently overclaim.
pub const MAX_KNOWN_STATE_EXACT_REFS: usize = MAX_FETCH_ROW_VERSIONS;

/// Maximum immutable-chunk requests admitted from one auxiliary message.
///
/// Four maximum-size encoded nodes plus response envelopes remain below the
/// ordinary wire-frame ceiling. Local producers currently send one request per
/// frame; this headroom permits small peer-side coalescing without amplification.
pub const MAX_CHUNK_REQUEST_BATCH_ENTRIES: usize = 4;

/// Validate raw frame bytes before postcard can allocate from declared lengths.
pub fn validate_wire_frame_len(len: usize) -> Result<(), String> {
    validate_len("wire frame", len, MAX_WIRE_FRAME_BYTES)
}

/// Validate raw encoded sync payload bytes before decoding the semantic message.
pub fn validate_logical_message_len(len: usize) -> Result<(), String> {
    validate_len("logical message payload", len, MAX_LOGICAL_MESSAGE_BYTES)
}

/// Validate a shape registration after sync-message decode but before storing it.
pub fn validate_shape_ast_size(ast: &ShapeAst) -> Result<(), String> {
    let bytes = postcard::to_allocvec(ast)
        .map_err(|err| format!("failed to measure shape AST payload: {err}"))?;
    validate_len("shape AST", bytes.len(), MAX_SHAPE_AST_BYTES)
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

/// Validate an optional known-state declaration after sync-message decode.
pub fn validate_known_state_declaration(
    declaration: &Option<KnownStateDeclaration>,
) -> Result<(), String> {
    let Some(KnownStateDeclaration::ExactVersionSet { versions }) = declaration else {
        return Ok(());
    };
    if versions.len() > MAX_KNOWN_STATE_EXACT_REFS {
        return Err(format!(
            "known-state exact declaration count {} exceeds max {}",
            versions.len(),
            MAX_KNOWN_STATE_EXACT_REFS
        ));
    }
    Ok(())
}

/// Return a malformed-commit reason when the commit unit exceeds protocol limits.
pub fn commit_unit_limit_violation(versions: &[VersionRecord]) -> Option<String> {
    if versions.len() > MAX_COMMIT_UNIT_VERSIONS {
        return Some(format!(
            "commit unit version count {} exceeds max {}",
            versions.len(),
            MAX_COMMIT_UNIT_VERSIONS
        ));
    }
    None
}

fn validate_len(label: &str, len: usize, max: usize) -> Result<(), String> {
    if len > max {
        Err(format!("{label} size {len} exceeds max {max}"))
    } else {
        Ok(())
    }
}
