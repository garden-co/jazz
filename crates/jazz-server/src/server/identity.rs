//! A store's identity, kept in its own file: the host table
//! `_jazz_host_identity` in `jazz.sqlite`.
//!
//! - `store_id`: the store's own id. Blob directories and bucket prefixes
//!   are claimed for it, and a bucket's default prefix is `jazz/<store_id>/`.
//! - `app_id`: the app the store serves, as first given (an import keeps an
//!   alpha.59 app's spelling). A store refuses to open for any other app.
//! - `blob_store`: where its large values live (see `plan_blob_store`);
//!   absent means inside the file.
//! - `token_secret`: what the backend signs agent tokens with.
//!
//! The file alone is the store: a copy of `jazz.sqlite` (a backup, or a
//! restore from a replica) opens as the same store, with the same blobs.
//! Stores from before kept these as files beside `jazz.sqlite`
//! (`store-id`, `app-id`, `blob-store`, `inspect-token-secret`). The first
//! open imports them in one commit and then deletes them; a file that
//! disagrees with what the store holds is refused, never chosen.

use std::path::Path;

use jazz::db::FileOptions;
use jazz::host::{HostTable, HostTables, StoreFile};

/// The host table a store's identity lives in.
pub(crate) const IDENTITY: HostTable = HostTable::new("identity");

pub(crate) const STORE_ID: &[u8] = b"store_id";
pub(crate) const APP_ID: &[u8] = b"app_id";
pub(crate) const BLOB_STORE: &[u8] = b"blob_store";
pub(crate) const TOKEN_SECRET: &[u8] = b"token_secret";

/// The SQLite file of a data directory's store.
pub(crate) const STORE_FILE: &str = "jazz.sqlite";

/// The files stores kept their identity in before, by the key each moves to.
const LEGACY_FILES: [(&[u8], &str); 4] = [
    (STORE_ID, "store-id"),
    (APP_ID, "app-id"),
    (BLOB_STORE, "blob-store"),
    (TOKEN_SECRET, "inspect-token-secret"),
];

/// What a store's file says it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    pub store_id: String,
    pub app_id: String,
    /// Where large values live, as recorded (`files`, `s3 …`); `None`
    /// keeps them in the file.
    pub blob_store: Option<String>,
    pub token_secret: String,
}

/// Open the store in `data_dir` (creating it) just long enough to read its
/// identity: import the files earlier versions kept it in, mint what it
/// lacks, and check `app_id` against the one it serves (recording it on
/// first open). With `app_id` `None`, a new store mints one.
///
/// Call it before the store is opened for serving, under the data dir's
/// lock ([`crate::lock_data_dir`]).
pub fn open_store_identity(data_dir: &Path, app_id: Option<&str>) -> Result<StoreIdentity, String> {
    std::fs::create_dir_all(data_dir)
        .map_err(|e| format!("failed to create data dir '{}': {e}", data_dir.display()))?;
    let file = StoreFile::open(
        &data_dir.join(STORE_FILE),
        FileOptions {
            synchronous_full: true,
            ..FileOptions::default()
        },
    )
    .map_err(|error| format!("open server store: {error}"))?;
    let identity = read_or_mint(&file, data_dir, app_id)?;
    drop(file);
    // Only once the store holds them: a crash before this leaves files the
    // next open finds equal and deletes.
    for (_, name) in LEGACY_FILES {
        remove_if_present(&data_dir.join(name))?;
    }
    Ok(identity)
}

/// Make the store in `data_dir` a store of its own: forget its store id and
/// its blob-store record, so its next open mints a new id and claims its
/// own chunk directory or bucket prefix. For a copy (a staging copy of a
/// backup) that would otherwise claim the original's chunks. The app id and
/// token secret stay: the copy serves the same app.
///
/// Call it under the data dir's lock ([`crate::lock_data_dir`]).
pub fn detach_store_identity(data_dir: &Path) -> Result<(), String> {
    let path = data_dir.join(STORE_FILE);
    if !path.exists() {
        return Err(format!("'{}' holds no store", data_dir.display()));
    }
    // Earlier versions' files say the same: import them first, so none is
    // left to bring the old id back.
    open_store_identity(data_dir, None)?;
    let file = StoreFile::open(
        &path,
        FileOptions {
            synchronous_full: true,
            ..FileOptions::default()
        },
    )
    .map_err(|error| format!("open server store: {error}"))?;
    for key in [STORE_ID, BLOB_STORE] {
        file.host_delete(IDENTITY, key)
            .map_err(|error| format!("store identity: {error}"))?;
    }
    Ok(())
}

fn read_or_mint(
    file: &dyn HostTables,
    data_dir: &Path,
    app_id: Option<&str>,
) -> Result<StoreIdentity, String> {
    let storage = |error: jazz::db::Error| format!("store identity: {error}");
    let mut imports = Vec::new();
    for (key, name) in LEGACY_FILES {
        let path = data_dir.join(name);
        let Some(legacy) = read_trimmed(&path)? else {
            continue;
        };
        match file.host_get(IDENTITY, key).map_err(storage)? {
            Some(kept) if kept == legacy.as_bytes() => {}
            Some(kept) => {
                return Err(format!(
                    "{} says {legacy:?}, but the store in '{}' holds {:?}: \
                     it was copied from another store, or edited; remove it to open this store",
                    path.display(),
                    data_dir.display(),
                    String::from_utf8_lossy(&kept),
                ));
            }
            None => imports.push((key.to_vec(), legacy.into_bytes())),
        }
    }
    if !imports.is_empty() {
        file.host_put_many(IDENTITY, &imports).map_err(storage)?;
    }

    let mint = |key: &[u8], value: String| -> Result<String, String> {
        Ok(
            match file
                .host_put_if_absent(IDENTITY, key, value.as_bytes())
                .map_err(storage)?
            {
                Some(kept) => text(key, kept)?,
                None => value,
            },
        )
    };
    let store_id = mint(STORE_ID, uuid::Uuid::new_v4().to_string())?;
    let token_secret = mint(
        TOKEN_SECRET,
        format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        ),
    )?;
    let wanted = app_id
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let kept_app_id = mint(APP_ID, wanted.clone())?;
    if app_id.is_some() && parse_app_id(&kept_app_id) != parse_app_id(&wanted) {
        return Err(format!(
            "the store in '{}' serves app {kept_app_id}, not {wanted}; \
             a store keeps the app it was first opened for",
            data_dir.display(),
        ));
    }
    let blob_store = file
        .host_get(IDENTITY, BLOB_STORE)
        .map_err(storage)?
        .map(|kept| text(BLOB_STORE, kept))
        .transpose()?;
    Ok(StoreIdentity {
        store_id,
        app_id: kept_app_id,
        blob_store,
        token_secret,
    })
}

/// The app an id names, read as hosts read it: a UUID in any spelling, or
/// else a name.
fn parse_app_id(id: &str) -> jazz::tools::AppId {
    jazz::tools::AppId::from_string(id).unwrap_or_else(|_| jazz::tools::AppId::from_name(id))
}

fn text(key: &[u8], value: Vec<u8>) -> Result<String, String> {
    String::from_utf8(value).map_err(|_| {
        format!(
            "store identity: {} is not text",
            String::from_utf8_lossy(key)
        )
    })
}

/// A legacy file's contents without the trailing newline; `None` if absent
/// or empty.
fn read_trimmed(path: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(contents) => {
            let trimmed = contents.trim();
            Ok((!trimmed.is_empty()).then(|| trimmed.to_owned()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("read {}: {error}", path.display())),
    }
}

fn remove_if_present(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("remove {}: {error}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_store_mints_its_identity_and_keeps_it() {
        let dir = tempfile::tempdir().unwrap();
        let first = open_store_identity(dir.path(), None).unwrap();
        assert!(uuid::Uuid::parse_str(&first.store_id).is_ok());
        assert!(uuid::Uuid::parse_str(&first.app_id).is_ok());
        assert_eq!(first.blob_store, None);
        assert_eq!(open_store_identity(dir.path(), None).unwrap(), first);
        assert_eq!(
            open_store_identity(dir.path(), Some(&first.app_id)).unwrap(),
            first
        );
        // Nothing but the store's own files.
        let mut names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| !name.starts_with("jazz.sqlite"))
            .collect();
        names.sort();
        assert!(names.is_empty(), "{names:?}");
    }

    #[test]
    fn a_store_keeps_its_apps_spelling_and_accepts_the_same_app() {
        let dir = tempfile::tempdir().unwrap();
        let given = "0A590000-0000-4000-8000-000000000059";
        open_store_identity(dir.path(), Some(given)).unwrap();
        let reopened = open_store_identity(dir.path(), Some(&given.to_lowercase())).unwrap();
        assert_eq!(reopened.app_id, given);
    }

    #[test]
    fn a_store_refuses_another_app() {
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), Some("app-a")).unwrap();
        let error = open_store_identity(dir.path(), Some("app-b")).unwrap_err();
        assert!(error.contains("serves app app-a, not app-b"), "{error}");
    }

    #[test]
    fn a_copy_of_the_file_is_the_same_store() {
        let dir = tempfile::tempdir().unwrap();
        let original = open_store_identity(dir.path(), None).unwrap();
        let copy = tempfile::tempdir().unwrap();
        std::fs::copy(dir.path().join(STORE_FILE), copy.path().join(STORE_FILE)).unwrap();
        assert_eq!(open_store_identity(copy.path(), None).unwrap(), original);
    }

    #[test]
    fn a_detached_copy_is_a_store_of_its_own_for_the_same_app() {
        let dir = tempfile::tempdir().unwrap();
        let original = open_store_identity(dir.path(), None).unwrap();
        let copy = tempfile::tempdir().unwrap();
        std::fs::copy(dir.path().join(STORE_FILE), copy.path().join(STORE_FILE)).unwrap();
        detach_store_identity(copy.path()).unwrap();
        let detached = open_store_identity(copy.path(), None).unwrap();
        assert_ne!(detached.store_id, original.store_id);
        assert_eq!(detached.blob_store, None);
        assert_eq!(detached.app_id, original.app_id);
        assert_eq!(detached.token_secret, original.token_secret);
        assert!(detach_store_identity(&copy.path().join("missing")).is_err());
    }

    #[test]
    fn legacy_files_are_imported_then_removed() {
        let dir = tempfile::tempdir().unwrap();
        for (name, value) in [
            ("store-id", "0f6b6a3e-6f3b-4a4e-9b1e-0d8c1f0a7c11"),
            ("app-id", "my-app"),
            ("blob-store", "files"),
            ("inspect-token-secret", "secret"),
        ] {
            std::fs::write(dir.path().join(name), format!("{value}\n")).unwrap();
        }
        let identity = open_store_identity(dir.path(), Some("my-app")).unwrap();
        assert_eq!(
            identity,
            StoreIdentity {
                store_id: "0f6b6a3e-6f3b-4a4e-9b1e-0d8c1f0a7c11".to_owned(),
                app_id: "my-app".to_owned(),
                blob_store: Some("files".to_owned()),
                token_secret: "secret".to_owned(),
            }
        );
        for (_, name) in LEGACY_FILES {
            assert!(!dir.path().join(name).exists(), "{name} is left");
        }
        assert_eq!(open_store_identity(dir.path(), None).unwrap(), identity);
    }

    #[test]
    fn a_legacy_file_that_disagrees_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let identity = open_store_identity(dir.path(), None).unwrap();
        std::fs::write(dir.path().join("store-id"), "another\n").unwrap();
        let error = open_store_identity(dir.path(), None).unwrap_err();
        assert!(error.contains(&identity.store_id), "{error}");
        // Left for the operator: nothing was chosen.
        assert!(dir.path().join("store-id").exists());
        // An equal one is harmless and goes.
        std::fs::write(dir.path().join("store-id"), &identity.store_id).unwrap();
        open_store_identity(dir.path(), None).unwrap();
        assert!(!dir.path().join("store-id").exists());
    }
}
