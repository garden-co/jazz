use napi_derive::napi;

/// Check encryption changes before writing a migration stub.
#[napi(js_name = "checkEncryptionMigration")]
pub fn check_encryption_migration(from_json: String, to_json: String) -> super::js::Result<()> {
    jazz_codegen::migrations::check_encryption_migration(from_json.as_bytes(), to_json.as_bytes())
        .map_err(super::codegen_error)
}
