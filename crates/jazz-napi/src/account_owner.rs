//! Host-admitted direct account roots. This marker is an ownership consistency
//! fence, not external JWT verification, remote authorization, or relay mode.

use super::*;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write as IoWrite};
use std::path::Path;

const MARKER_NAME: &str = ".jazz-account-owner";
const MARKER_HEADER: &[u8] = b"JAZZ-NODE-ACCOUNT-OWNER\0\x01";
const MAX_OWNER_FIELD_BYTES: usize = 64 * 1024;
const MAX_MARKER_BYTES: usize = MARKER_HEADER.len() + 8 + 2 * MAX_OWNER_FIELD_BYTES;

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HostStorageOwner {
    version: u8,
    app_id: String,
    env: String,
    auth: HostAccountScope,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct HostAccountScope {
    kind: String,
    account: String,
    registry: String,
}

fn validate_storage_owner(owner: &str, author: CoreAuthorSubject) -> napi::Result<()> {
    if owner.is_empty() || owner.len() > MAX_OWNER_FIELD_BYTES {
        return Err(napi::Error::from_reason(
            "invalid persistent account storage owner",
        ));
    }
    let scope: HostStorageOwner = serde_json::from_str(owner).map_err(napi_error)?;
    let account = author.account_id().ok_or_else(|| {
        napi::Error::from_reason("persistent account owner requires an account-bound author")
    })?;
    if scope.version != 1
        || scope.app_id.is_empty()
        || scope.env.is_empty()
        || scope.auth.kind != "account"
        || scope.auth.registry.is_empty()
        || scope.auth.account != account.0.to_string()
        || serde_json::to_string(&scope).map_err(napi_error)? != owner
    {
        return Err(napi::Error::from_reason(
            "persistent account storage owner does not match the configured account",
        ));
    }
    Ok(())
}

// Explicit V1 grammar: header (including version), BE-u32 UTF-8 owner length,
// exact owner bytes, BE-u32 canonical-author length, exact canonical-author bytes.
fn encode_marker(owner: &str, author: &str) -> napi::Result<Vec<u8>> {
    if owner.is_empty()
        || author.is_empty()
        || owner.len() > MAX_OWNER_FIELD_BYTES
        || author.len() > MAX_OWNER_FIELD_BYTES
    {
        return Err(napi::Error::from_reason(
            "invalid persistent account owner marker fields",
        ));
    }
    let mut marker = Vec::with_capacity(MARKER_HEADER.len() + 8 + owner.len() + author.len());
    marker.extend_from_slice(MARKER_HEADER);
    for field in [owner, author] {
        marker.extend_from_slice(&(field.len() as u32).to_be_bytes());
        marker.extend_from_slice(field.as_bytes());
    }
    Ok(marker)
}

/// Called only while the admitted RocksDB handle holds the root's exclusive
/// open lock. Only this host account-owner route may claim a markerless legacy
/// root selected by the unchanged AccountHandle-derived hashed namespace.
fn admit_marker(root: &Path, expected: &[u8]) -> napi::Result<()> {
    let marker_path = root.join(MARKER_NAME);
    match File::open(&marker_path) {
        Ok(file) => {
            let mut actual = Vec::new();
            file.take((MAX_MARKER_BYTES + 1) as u64)
                .read_to_end(&mut actual)
                .map_err(napi_error)?;
            if actual != expected {
                return Err(napi::Error::from_reason(
                    "persistent account owner marker is mismatched, malformed, or unsupported",
                ));
            }
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(napi_error(error)),
    }
    let temporary = root.join(format!(
        ".jazz-account-owner-{}.tmp",
        CoreOpenTransactionId::new()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).map_err(napi_error)?;
    let result = (|| {
        file.write_all(expected).map_err(napi_error)?;
        file.sync_all().map_err(napi_error)?;
        drop(file);
        install_marker(&temporary, &marker_path, root)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn install_marker(temporary: &Path, marker: &Path, root: &Path) -> napi::Result<()> {
    // A same-directory hard link installs atomically without replacing a marker.
    std::fs::hard_link(temporary, marker).map_err(napi_error)?;
    std::fs::remove_file(temporary).map_err(napi_error)?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(napi_error)
}

#[cfg(windows)]
fn install_marker(temporary: &Path, marker: &Path, _root: &Path) -> napi::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, destination: *const u16, flags: u32) -> i32;
    }
    fn wide(path: &Path) -> napi::Result<Vec<u16>> {
        let mut value: Vec<u16> = path.as_os_str().encode_wide().collect();
        if value.contains(&0) {
            return Err(napi::Error::from_reason(
                "account owner marker path contains NUL",
            ));
        }
        value.push(0);
        Ok(value)
    }
    let source = wide(temporary)?;
    let destination = wide(marker)?;
    // MOVEFILE_WRITE_THROUGH, deliberately without MOVEFILE_REPLACE_EXISTING.
    // SAFETY: both UTF-16 buffers are NUL-terminated and live through the call.
    if unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), 0x8) } == 0 {
        return Err(napi_error(std::io::Error::last_os_error()));
    }
    Ok(())
}

fn open_owner(
    data_path: String,
    schema: JazzSchema,
    config: CoreOpenDbConfig,
    identity: CoreDbIdentity,
    storage_owner: String,
    cached_catalogue: Option<Uint8Array>,
) -> napi::Result<NapiDb> {
    validate_storage_owner(&storage_owner, identity.author)?;
    let expected = encode_marker(&storage_owner, identity.author.canonical())?;
    let root = Path::new(&data_path);
    let storage = open_persistent_core_storage(data_path.clone(), &schema)?;
    // The adapter is exclusively open, but no Db/schema/cache/replay exists yet.
    admit_marker(root, &expected)?;
    let db = initialization::open_cached_db(
        schema,
        storage,
        config,
        identity,
        cached_catalogue.as_deref(),
    )?;
    // SAFETY: this host-only path has durably admitted the exact configured
    // account author under the existing root lock before exposing any runtime.
    core_block_on(unsafe { db.restore_initialization_owner_pending_uploads() })
        .map_err(napi_error)?;
    Ok(NapiDb {
        inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Persistent(Rc::new(
            db,
        ))))),
        owns_runtime: true,
        non_durable_client: Rc::new(Cell::new(false)),
        view_id: 0,
        streaming: StreamingOwnerLifecycle::new(),
        trusted_backend: false,
        author_admissions: NativeAuthorAdmissions::default(),
        initialization_seals: Rc::default(),
    })
}

#[napi]
impl NapiDb {
    #[napi(factory, js_name = "openPersistentAccountOwnerWithSelfSignedProof")]
    #[allow(clippy::too_many_arguments)] // Flat arguments are the generated NAPI ABI.
    pub fn open_persistent_account_owner_with_self_signed_proof(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        storage_owner: String,
        token: String,
        app_id: String,
        claimed_author: String,
        cached_catalogue: Option<Uint8Array>,
    ) -> js::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let proof = CoreSelfSignedClientProof {
            token,
            app_id,
            claimed_author,
        };
        // The serialized config has the existing untrusted placeholder. Native
        // proof verification checks the actual host claim before root admission.
        let identity = core_open_identity(&config, Some(&proof))?;
        Ok(open_owner(
            data_path,
            schema,
            config,
            identity,
            storage_owner,
            cached_catalogue,
        )?)
    }

    #[napi(factory, js_name = "openPersistentAccountOwner")]
    pub fn open_persistent_account_owner(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        storage_owner: String,
        cached_catalogue: Option<Uint8Array>,
    ) -> js::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let identity = core_open_identity(&config, None)?;
        Ok(open_owner(
            data_path,
            schema,
            config,
            identity,
            storage_owner,
            cached_catalogue,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_v1_has_explicit_bounded_length_delimited_fields() {
        let mut expected = b"JAZZ-NODE-ACCOUNT-OWNER\0\x01".to_vec();
        expected.extend_from_slice(&[0, 0, 0, 2, b'o', b'w', 0, 0, 0, 2, b'a', b'u']);
        assert_eq!(encode_marker("ow", "au").unwrap(), expected);
        assert!(encode_marker("", "au").is_err());
        assert!(encode_marker("ow", "").is_err());
        assert!(encode_marker(&"x".repeat(MAX_OWNER_FIELD_BYTES + 1), "au").is_err());
        assert!(encode_marker("ow", &"x".repeat(MAX_OWNER_FIELD_BYTES + 1)).is_err());
    }

    #[test]
    fn durable_marker_installation_never_replaces_an_existing_owner() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join(MARKER_NAME);
        let temporary = directory.path().join("candidate.tmp");
        std::fs::write(&marker, b"authoritative owner").unwrap();
        std::fs::write(&temporary, b"different owner").unwrap();
        assert!(install_marker(&temporary, &marker, directory.path()).is_err());
        assert_eq!(std::fs::read(marker).unwrap(), b"authoritative owner");
        assert_eq!(std::fs::read(temporary).unwrap(), b"different owner");
    }
}
