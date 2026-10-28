//! Versioned logical content evidence for serializable read validation.
use super::*;

/// jazz.sqlite.serializable-read-content.v1: BLAKE3 derive-key over the
/// versioned postcard preimage pinned by sqlite_serializable_content_v1.json.
/// Union rows bind that preimage to their semantic arm under the separate
/// serializable-union-read-content.v1 role.
/// Numeric publication aliases are deliberately excluded; original history
/// identities, result occurrences, deletion and every projected cell remain.
fn read_content(row: &CurrentRow) -> Result<[u8; 32], Error> {
    let version = |version: crate::values::SourceVersion| (version.0, *version.1.as_bytes());
    let joined = row
        .joined
        .iter()
        .map(|joined| (*joined.row.as_bytes(), joined.version.map(version)))
        .collect::<Vec<_>>();
    let bytes = postcard::to_allocvec(&(
        row.table.as_ref(),
        row.id.as_bytes(),
        row.deleted,
        row.cells.as_ref(),
        joined,
        row.version.map(version),
    ))
    .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
    if let Some(arm) = row.union_arm() {
        // A support-row change can move the same unchanged root between arms.
        // Keep v1 plain-row bytes fixed and bind the semantic union occurrence
        // in a separately versioned preimage, admitted by the union wire kind.
        let bytes = postcard::to_allocvec(&(bytes, arm))
            .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
        return Ok(blake3::derive_key(
            "jazz.sqlite.serializable-union-read-content.v1",
            &bytes,
        ));
    }
    Ok(blake3::derive_key(
        "jazz.sqlite.serializable-read-content.v1",
        &bytes,
    ))
}

pub(super) fn read_contents(rows: &[CurrentRow]) -> Result<Vec<[u8; 32]>, Error> {
    rows.iter().map(read_content).collect()
}
