//! Deprecated schema catalogue push shim.
//!
//! Schema publishing now happens through the TypeScript loader/runtime path in
//! `packages/jazz-tools`, which can compile `schema.ts`, `permissions.ts`, and
//! migration stubs before talking to the server. The older Rust-side directory
//! walker no longer matches the current schema authoring workflow.

/// Push schema catalogue objects to a sync server from a filesystem directory.
///
/// This Rust entrypoint is kept only so existing bindings continue to compile.
/// Callers should use the TypeScript `pushSchemaCatalogue` helper instead.
pub async fn push(
    _server_url: &str,
    _app_id: &str,
    _env: &str,
    _user_branch: &str,
    _admin_secret: &str,
    _schema_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    Err(std::io::Error::other(
        "pushSchemaCatalogue moved to the TypeScript schema loader; use jazz-tools/testing's local implementation instead.",
    )
    .into())
}
