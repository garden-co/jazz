//! A store's identity, kept in its own file: the host table
//! `_jazz_host_identity` in `jazz.sqlite`.
//!
//! - `store_id`: the store's own id. Blob directories and bucket prefixes
//!   are claimed for it, and a bucket's default prefix is `jazz/<store_id>/`.
//! - `app_id`: the app the store serves, as first given (an import keeps an
//!   alpha.59 app's spelling). A store refuses to open for any other app.
//! - `blob_store`: where its large values live (see `plan_blob_store`);
//!   absent means inside the file.
//!
//! The table is the authority: a copy of `jazz.sqlite` (a backup, or a
//! restore from a replica) opens as the same store, with the same blobs.
//! Stores from before kept these as files beside `jazz.sqlite`
//! (`store-id`, `app-id`, `blob-store`, `inspect-token-secret`). An open
//! imports any the table lacks, in one commit, and refuses a file that
//! disagrees with it, never choosing one. The files stay, as mirrors every
//! open rewrites from the table, so an earlier release opened on the store
//! reads the same identity instead of minting a new one. Each kind of store
//! mirrors only the files it kept before: a server `store-id` and
//! `blob-store`, an in-process core also `app-id`.
//!
//! An in-process core's agent-token secret is not identity and never enters
//! the file, which backups, copies and replicas carry: it lives in its own
//! `inspect-token-secret` beside the store, readable by its owner only.

use std::path::Path;

use jazz::db::FileOptions;
use jazz::host::{HostReader, HostTable, HostTables, StoreFile};

/// The host table a store's identity lives in.
pub(crate) const IDENTITY: HostTable = HostTable::new("identity");

pub(crate) const STORE_ID: &[u8] = b"store_id";
pub(crate) const APP_ID: &[u8] = b"app_id";
pub(crate) const BLOB_STORE: &[u8] = b"blob_store";
/// Where builds before the secret got a file of its own kept it; removed.
const TOKEN_SECRET: &[u8] = b"token_secret";

/// The SQLite file of a data directory's store.
pub(crate) const STORE_FILE: &str = "jazz.sqlite";

const STORE_ID_FILE: &str = "store-id";
const APP_ID_FILE: &str = "app-id";
const BLOB_STORE_FILE: &str = "blob-store";
const TOKEN_SECRET_FILE: &str = "inspect-token-secret";

/// The files stores kept their identity in before, by the key each holds.
const LEGACY_FILES: [(&[u8], &str); 3] = [
    (STORE_ID, STORE_ID_FILE),
    (APP_ID, APP_ID_FILE),
    (BLOB_STORE, BLOB_STORE_FILE),
];

/// Who opens a store, which decides what it may mint and mirror.
#[derive(Clone, Copy, Debug)]
pub enum IdentityOpen<'a> {
    /// A server started for `app_id`: the store records it on first open
    /// and refuses another.
    Server { app_id: &'a str },
    /// An in-process core (`sqlite()` in jazz-tools): a new store mints its
    /// app id (unless one is given) and its agent-token secret.
    Embedded { app_id: Option<&'a str> },
}

/// What a store's file says it is, once opened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreIdentity {
    pub store_id: String,
    pub app_id: String,
    /// Where large values live, as recorded (`files`, `s3 …`); `None`
    /// keeps them in the file.
    pub blob_store: Option<String>,
    /// What an in-process core signs agent tokens with: its
    /// `inspect-token-secret` file (servers keep none).
    pub token_secret: Option<String>,
}

/// What a store records, read without writing anything (see
/// [`read_store_identity`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecordedIdentity {
    pub store_id: Option<String>,
    pub app_id: Option<String>,
    pub blob_store: Option<String>,
}

/// Open the store in `data_dir` (creating it) just long enough to read its
/// identity: import the files earlier releases kept it in, mint what it
/// lacks, check the app id, and rewrite the mirrors.
///
/// Call it before the store is opened for serving, under the data dir's
/// lock ([`crate::lock_data_dir`]).
pub fn open_store_identity(
    data_dir: &Path,
    how: IdentityOpen<'_>,
) -> Result<StoreIdentity, String> {
    std::fs::create_dir_all(data_dir)
        .map_err(|e| format!("failed to create data dir '{}': {e}", data_dir.display()))?;
    let file = open_file(data_dir)?;
    let mut identity = read_or_mint(&file, data_dir, how)?;
    // Only an earlier build of this release kept the secret in the table:
    // its file takes it over, then it leaves the table.
    let in_table = file
        .host_get(IDENTITY, TOKEN_SECRET)
        .map_err(storage)?
        .map(|kept| text(TOKEN_SECRET, kept))
        .transpose()?;
    if matches!(how, IdentityOpen::Embedded { .. }) {
        identity.token_secret = Some(token_secret(data_dir, in_table.as_deref())?);
    }
    if in_table.is_some() {
        file.host_delete(IDENTITY, TOKEN_SECRET).map_err(storage)?;
    }
    drop(file);
    // Only once the store holds them, so a crash leaves mirrors equal to it
    // or missing, never ahead of it.
    // A store id is claimed only by a store whose large values left its
    // file; earlier releases made the file only then.
    if identity.blob_store.is_some() || data_dir.join(STORE_ID_FILE).exists() {
        write_mirror(data_dir, STORE_ID_FILE, Some(&identity.store_id), false)?;
    }
    write_mirror(
        data_dir,
        BLOB_STORE_FILE,
        identity.blob_store.as_deref(),
        false,
    )?;
    if matches!(how, IdentityOpen::Embedded { .. }) {
        write_mirror(data_dir, APP_ID_FILE, Some(&identity.app_id), false)?;
    }
    Ok(identity)
}

/// Rewrite the `blob-store` mirror once a store recorded that it moved its
/// large values out of its file.
pub(crate) fn mirror_blob_store(
    data_dir: &Path,
    store_id: &str,
    blob_store: &str,
) -> Result<(), String> {
    // The store id is claimed from now on (see `open_store_identity`).
    write_mirror(data_dir, STORE_ID_FILE, Some(store_id), false)?;
    write_mirror(data_dir, BLOB_STORE_FILE, Some(blob_store), false)
}

/// What the store in `data_dir` records, merged with the files earlier
/// releases kept, without writing anything or minting: for tools that only
/// read a store (the alpha.59 import's verify).
pub fn read_store_identity(data_dir: &Path) -> Result<RecordedIdentity, String> {
    let path = data_dir.join(STORE_FILE);
    if !path.exists() {
        return Err(format!("'{}' holds no store", data_dir.display()));
    }
    let reader = HostReader::open(&path, None)
        .map_err(|error| format!("open server store reader: {error}"))?;
    let get = |key: &[u8], name: &str| -> Result<Option<String>, String> {
        match reader
            .get(IDENTITY, key)
            .map_err(|error| format!("store identity: {error}"))?
        {
            Some(kept) => text(key, kept).map(Some),
            None => read_trimmed(&data_dir.join(name)),
        }
    };
    let recorded = RecordedIdentity {
        store_id: get(STORE_ID, STORE_ID_FILE)?,
        app_id: get(APP_ID, APP_ID_FILE)?,
        blob_store: get(BLOB_STORE, BLOB_STORE_FILE)?,
    };
    reader.close();
    Ok(recorded)
}

/// Make the stopped store in `data_dir` a store of its own: forget its
/// store id and its blob-store record (and their mirrors), so its next open
/// mints a new id and claims its own chunk directory or bucket prefix. For
/// a copy (a staging copy of a backup) that would otherwise claim the
/// original's chunks. A chunk directory inside the data dir was copied with
/// it, so its `owner` claim goes too; one outside is the original's, and
/// the copy needs one of its own. Its agent-token secret goes, so tokens
/// the original issued don't open the copy. The app id stays: the copy
/// serves the same app. Nothing is minted.
///
/// Call it under the data dir's lock ([`crate::lock_data_dir`]).
pub fn detach_store_identity(data_dir: &Path) -> Result<(), String> {
    let path = data_dir.join(STORE_FILE);
    if !path.exists() {
        return Err(format!("'{}' holds no store", data_dir.display()));
    }
    let file = open_file(data_dir)?;
    // What earlier releases' files say, first: none may bring the old id
    // back.
    import_legacy(&file, data_dir)?;
    let blob_store = file
        .host_get(IDENTITY, BLOB_STORE)
        .map_err(storage)?
        .map(|kept| text(BLOB_STORE, kept))
        .transpose()?;
    // The files go before the table forgets: a crash in between leaves the
    // store as it was (detach runs again), never files that bring the old
    // id back. The secret goes too: the copy mints its own, so tokens the
    // original issued don't open it.
    for name in [STORE_ID_FILE, BLOB_STORE_FILE, TOKEN_SECRET_FILE] {
        remove_if_present(&data_dir.join(name))?;
    }
    if let Some(inside) = blob_store.as_deref().and_then(blob_dir_inside) {
        remove_if_present(&data_dir.join(inside).join(super::builder::BLOB_OWNER_FILE))?;
    }
    for key in [STORE_ID, BLOB_STORE, TOKEN_SECRET] {
        file.host_delete(IDENTITY, key).map_err(storage)?;
    }
    Ok(())
}

/// Make the stopped store in `data_dir` serve `app_id` from its next start:
/// for a store started with the wrong app id, or one whose app was renamed.
/// A store refuses any app but its own, and this is the one way to change
/// it. Rows and history stay as they are, but local-first guests' accounts
/// are derived from the app id, so they no longer match after a change.
///
/// Call it under the data dir's lock ([`crate::lock_data_dir`]).
pub fn set_store_app_id(data_dir: &Path, app_id: &str) -> Result<(), String> {
    let app_id = app_id.trim();
    if app_id.is_empty() {
        return Err("the app id is empty".to_owned());
    }
    if !data_dir.join(STORE_FILE).exists() {
        return Err(format!("'{}' holds no store", data_dir.display()));
    }
    let file = open_file(data_dir)?;
    import_legacy(&file, data_dir)?;
    // The mirror goes first: a crash before the table changes leaves none,
    // which the next open makes again, never one that disagrees.
    let mirrored = data_dir.join(APP_ID_FILE).exists();
    remove_if_present(&data_dir.join(APP_ID_FILE))?;
    file.host_put_many(IDENTITY, &[(APP_ID.to_vec(), app_id.as_bytes().to_vec())])
        .map_err(storage)?;
    file.host_delete(IDENTITY, TOKEN_SECRET).map_err(storage)?;
    drop(file);
    // An in-process store keeps the mirror; a server never had one.
    if mirrored {
        write_mirror(data_dir, APP_ID_FILE, Some(app_id), false)?;
    }
    Ok(())
}

/// The chunk directory a blob-store record names inside the data dir:
/// `files` is `blobs/`, `files <relative>` that directory.
fn blob_dir_inside(record: &str) -> Option<&str> {
    match record {
        "files" => Some("blobs"),
        other => other
            .strip_prefix("files ")
            .filter(|dir| !Path::new(dir).is_absolute()),
    }
}

fn open_file(data_dir: &Path) -> Result<StoreFile, String> {
    StoreFile::open(
        &data_dir.join(STORE_FILE),
        FileOptions {
            synchronous_full: true,
            ..FileOptions::default()
        },
    )
    .map_err(|error| format!("open server store: {error}"))
}

fn storage(error: jazz::db::Error) -> String {
    format!("store identity: {error}")
}

/// Import into the table the files it lacks, in one commit; refuse a file
/// that disagrees with it.
fn import_legacy(file: &dyn HostTables, data_dir: &Path) -> Result<(), String> {
    let mut imports = Vec::new();
    // Until large values leave the file nothing claims the store id, so an
    // earlier release opened on the store may have minted another one: it
    // is the one that may have been claimed since.
    let unclaimed = file
        .host_get(IDENTITY, BLOB_STORE)
        .map_err(storage)?
        .is_none();
    for (key, name) in LEGACY_FILES {
        let path = data_dir.join(name);
        let Some(legacy) = read_trimmed(&path)? else {
            continue;
        };
        match file.host_get(IDENTITY, key).map_err(storage)? {
            Some(kept) if kept == legacy.as_bytes() => {}
            Some(_) if key == STORE_ID && unclaimed => {
                imports.push((key.to_vec(), legacy.into_bytes()));
            }
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
    Ok(())
}

fn read_or_mint(
    file: &dyn HostTables,
    data_dir: &Path,
    how: IdentityOpen<'_>,
) -> Result<StoreIdentity, String> {
    import_legacy(file, data_dir)?;
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
    let given = match how {
        IdentityOpen::Server { app_id } => Some(app_id),
        IdentityOpen::Embedded { app_id } => app_id,
    };
    let wanted = given
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let kept_app_id = mint(APP_ID, wanted.clone())?;
    if given.is_some() && parse_app_id(&kept_app_id) != parse_app_id(&wanted) {
        return Err(format!(
            "the store in '{dir}' serves app {kept_app_id}, not {wanted}: \
             a store keeps the app it was first opened for. If it should serve {wanted}, \
             stop it and run `npx jazz-tools store set-app-id {dir} {wanted}`",
            dir = data_dir.display(),
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
        token_secret: None,
    })
}

/// An in-process core's agent-token secret: its `inspect-token-secret`
/// file, made (readable by its owner only) from `carried` or a new random
/// one when absent.
fn token_secret(data_dir: &Path, carried: Option<&str>) -> Result<String, String> {
    let path = data_dir.join(TOKEN_SECRET_FILE);
    if let Some(kept) = read_trimmed(&path)? {
        return Ok(kept);
    }
    let secret = carried.map(str::to_owned).unwrap_or_else(|| {
        format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        )
    });
    super::builder::write_durably(&path, format!("{secret}\n").as_bytes(), true)?;
    Ok(secret)
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

/// Rewrite the mirror `name` as `value` (as earlier releases wrote it, with
/// a trailing newline), or remove it for `None`. Unchanged mirrors are left
/// alone. A crash leaves the old file or the whole new one.
fn write_mirror(
    data_dir: &Path,
    name: &str,
    value: Option<&str>,
    private: bool,
) -> Result<(), String> {
    let path = data_dir.join(name);
    let Some(value) = value else {
        return remove_if_present(&path);
    };
    let contents = format!("{value}\n");
    if std::fs::read_to_string(&path).is_ok_and(|kept| kept == contents) {
        return Ok(());
    }
    super::builder::write_durably(&path, contents.as_bytes(), private)
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

    const EMBEDDED: IdentityOpen<'static> = IdentityOpen::Embedded { app_id: None };

    fn server(app_id: &str) -> IdentityOpen<'_> {
        IdentityOpen::Server { app_id }
    }

    fn file(dir: &Path, name: &str) -> Option<String> {
        std::fs::read_to_string(dir.join(name)).ok()
    }

    #[test]
    fn an_in_process_store_mints_its_identity_and_keeps_it() {
        let dir = tempfile::tempdir().unwrap();
        let first = open_store_identity(dir.path(), EMBEDDED).unwrap();
        assert!(uuid::Uuid::parse_str(&first.store_id).is_ok());
        assert!(uuid::Uuid::parse_str(&first.app_id).is_ok());
        assert_eq!(first.blob_store, None);
        assert!(first.token_secret.is_some());
        assert_eq!(open_store_identity(dir.path(), EMBEDDED).unwrap(), first);
        let given = IdentityOpen::Embedded {
            app_id: Some(&first.app_id),
        };
        assert_eq!(open_store_identity(dir.path(), given).unwrap(), first);
    }

    #[test]
    fn a_server_store_mints_no_token_secret() {
        let dir = tempfile::tempdir().unwrap();
        let identity = open_store_identity(dir.path(), server("app-a")).unwrap();
        assert_eq!(identity.app_id, "app-a");
        assert_eq!(identity.token_secret, None);
        assert_eq!(file(dir.path(), TOKEN_SECRET_FILE), None);
    }

    /// An earlier release opened on the store reads these files: they must
    /// say what the table says, or it would mint a new identity.
    #[test]
    fn each_open_mirrors_the_files_its_kind_of_store_kept_before() {
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), server("app-a")).unwrap();
        // Values in the file: nothing claimed the id, and earlier releases
        // kept no file for it.
        assert_eq!(file(dir.path(), STORE_ID_FILE), None);
        assert_eq!(file(dir.path(), BLOB_STORE_FILE), None, "sqlite blobs");
        assert_eq!(file(dir.path(), APP_ID_FILE), None, "servers kept none");
        std::fs::write(dir.path().join(BLOB_STORE_FILE), "files\n").unwrap();
        let identity = open_store_identity(dir.path(), server("app-a")).unwrap();
        assert_eq!(
            file(dir.path(), STORE_ID_FILE),
            Some(format!("{}\n", identity.store_id))
        );
        assert_eq!(
            file(dir.path(), BLOB_STORE_FILE).as_deref(),
            Some("files\n")
        );

        let dir = tempfile::tempdir().unwrap();
        let identity = open_store_identity(dir.path(), EMBEDDED).unwrap();
        assert_eq!(
            file(dir.path(), APP_ID_FILE),
            Some(format!("{}\n", identity.app_id))
        );
        assert_eq!(
            file(dir.path(), TOKEN_SECRET_FILE),
            Some(format!("{}\n", identity.token_secret.unwrap()))
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join(TOKEN_SECRET_FILE))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // A deleted mirror comes back from the table.
        std::fs::remove_file(dir.path().join(APP_ID_FILE)).unwrap();
        let again = open_store_identity(dir.path(), EMBEDDED).unwrap();
        assert_eq!(
            file(dir.path(), APP_ID_FILE),
            Some(format!("{}\n", again.app_id))
        );
    }

    #[test]
    fn a_store_refuses_another_app() {
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), server("app-a")).unwrap();
        let error = open_store_identity(dir.path(), server("app-b")).unwrap_err();
        assert!(error.contains("serves app app-a, not app-b"), "{error}");
    }

    #[test]
    fn a_store_refused_for_another_app_can_be_set_to_serve_it() {
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), server("app-a")).unwrap();
        let error = open_store_identity(dir.path(), server("app-b")).unwrap_err();
        assert!(error.contains("jazz-tools store set-app-id"), "{error}");
        set_store_app_id(dir.path(), "app-b").unwrap();
        assert_eq!(
            open_store_identity(dir.path(), server("app-b"))
                .unwrap()
                .app_id,
            "app-b"
        );
        assert!(open_store_identity(dir.path(), server("app-a")).is_err());
        assert!(set_store_app_id(dir.path(), " ").is_err());
        assert!(set_store_app_id(&dir.path().join("missing"), "x").is_err());

        // An in-process store's mirror follows.
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), EMBEDDED).unwrap();
        set_store_app_id(dir.path(), "renamed").unwrap();
        assert_eq!(file(dir.path(), APP_ID_FILE).as_deref(), Some("renamed\n"));
        assert_eq!(
            open_store_identity(dir.path(), EMBEDDED).unwrap().app_id,
            "renamed"
        );
    }

    /// The secret signs agent tokens: it stays out of the file backups,
    /// copies and replicas carry.
    #[test]
    fn the_agent_token_secret_lives_in_its_own_private_file() {
        let dir = tempfile::tempdir().unwrap();
        let identity = open_store_identity(dir.path(), EMBEDDED).unwrap();
        let secret = identity.token_secret.unwrap();
        assert_eq!(
            file(dir.path(), TOKEN_SECRET_FILE),
            Some(format!("{secret}\n"))
        );
        let reader = HostReader::open(&dir.path().join(STORE_FILE), None).unwrap();
        assert_eq!(reader.get(IDENTITY, TOKEN_SECRET).unwrap(), None);
        reader.close();
        // A copy of the store file alone mints its own.
        let copy = tempfile::tempdir().unwrap();
        std::fs::copy(dir.path().join(STORE_FILE), copy.path().join(STORE_FILE)).unwrap();
        let copied = open_store_identity(copy.path(), EMBEDDED).unwrap();
        assert_ne!(copied.token_secret.unwrap(), secret);
    }

    /// Earlier builds of this release kept the secret in the table: the
    /// file takes it over, so tokens stay valid, and the table forgets it.
    /// A crash between a step and the next leaves a store that opens.
    #[test]
    fn set_app_id_and_detach_leave_no_disagreeing_file_at_any_step() {
        // set-app-id: the mirror is gone before the table changes.
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), EMBEDDED).unwrap();
        std::fs::remove_file(dir.path().join(APP_ID_FILE)).unwrap();
        let store = open_file(dir.path()).unwrap();
        store
            .host_put_many(IDENTITY, &[(APP_ID.to_vec(), b"renamed".to_vec())])
            .unwrap();
        drop(store);
        assert_eq!(
            open_store_identity(dir.path(), EMBEDDED).unwrap().app_id,
            "renamed"
        );
        // detach: the files are gone before the table forgets.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(BLOB_STORE_FILE), "files\n").unwrap();
        let original = open_store_identity(dir.path(), EMBEDDED).unwrap();
        for name in [STORE_ID_FILE, BLOB_STORE_FILE, TOKEN_SECRET_FILE] {
            std::fs::remove_file(dir.path().join(name)).unwrap();
        }
        // Not detached yet, and not broken: it runs again.
        let reopened = open_store_identity(dir.path(), EMBEDDED).unwrap();
        assert_eq!(reopened.store_id, original.store_id);
        detach_store_identity(dir.path()).unwrap();
        assert_ne!(
            open_store_identity(dir.path(), EMBEDDED).unwrap().store_id,
            original.store_id
        );
    }

    #[test]
    fn detach_and_set_app_id_forget_a_secret_an_earlier_build_kept_in_the_table() {
        for step in [
            detach_store_identity as fn(&Path) -> Result<(), String>,
            |dir: &Path| set_store_app_id(dir, "app"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            open_store_identity(dir.path(), server("app")).unwrap();
            let store = open_file(dir.path()).unwrap();
            store
                .host_put_many(IDENTITY, &[(TOKEN_SECRET.to_vec(), b"old".to_vec())])
                .unwrap();
            drop(store);
            step(dir.path()).unwrap();
            let reader = HostReader::open(&dir.path().join(STORE_FILE), None).unwrap();
            assert_eq!(reader.get(IDENTITY, TOKEN_SECRET).unwrap(), None);
        }
    }

    #[test]
    fn a_secret_an_earlier_build_kept_in_the_table_moves_to_its_file() {
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), server("app")).unwrap();
        let store = open_file(dir.path()).unwrap();
        store
            .host_put_many(IDENTITY, &[(TOKEN_SECRET.to_vec(), b"old".to_vec())])
            .unwrap();
        drop(store);
        let how = IdentityOpen::Embedded {
            app_id: Some("app"),
        };
        let identity = open_store_identity(dir.path(), how).unwrap();
        assert_eq!(identity.token_secret.as_deref(), Some("old"));
        assert_eq!(
            file(dir.path(), TOKEN_SECRET_FILE).as_deref(),
            Some("old\n")
        );
        let reader = HostReader::open(&dir.path().join(STORE_FILE), None).unwrap();
        assert_eq!(reader.get(IDENTITY, TOKEN_SECRET).unwrap(), None);
    }

    #[test]
    fn a_store_keeps_its_apps_spelling_and_accepts_the_same_app() {
        let dir = tempfile::tempdir().unwrap();
        let given = "0A590000-0000-4000-8000-000000000059";
        open_store_identity(dir.path(), server(given)).unwrap();
        let lower = given.to_lowercase();
        let reopened = open_store_identity(dir.path(), server(&lower)).unwrap();
        assert_eq!(reopened.app_id, given);
    }

    #[test]
    fn a_copy_of_the_file_alone_is_the_same_store() {
        let dir = tempfile::tempdir().unwrap();
        let original = open_store_identity(dir.path(), EMBEDDED).unwrap();
        let copy = tempfile::tempdir().unwrap();
        std::fs::copy(dir.path().join(STORE_FILE), copy.path().join(STORE_FILE)).unwrap();
        let copied = open_store_identity(copy.path(), EMBEDDED).unwrap();
        // All but the agent-token secret, which isn't in the file.
        assert_eq!(
            StoreIdentity {
                token_secret: None,
                ..copied
            },
            StoreIdentity {
                token_secret: None,
                ..original
            }
        );
    }

    #[test]
    fn reading_an_identity_writes_and_mints_nothing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_store_identity(dir.path()).is_err(), "no store");
        open_store_identity(dir.path(), server("app-a")).unwrap();
        // A store from before: its files say what the table lacks.
        std::fs::write(dir.path().join(BLOB_STORE_FILE), "files\n").unwrap();
        let before = std::fs::read(dir.path().join(STORE_FILE)).unwrap();
        let recorded = read_store_identity(dir.path()).unwrap();
        assert_eq!(recorded.app_id.as_deref(), Some("app-a"));
        assert_eq!(recorded.blob_store.as_deref(), Some("files"));
        assert_eq!(std::fs::read(dir.path().join(STORE_FILE)).unwrap(), before);
    }

    #[test]
    fn a_detached_copy_is_a_store_of_its_own_for_the_same_app() {
        let dir = tempfile::tempdir().unwrap();
        let original = open_store_identity(dir.path(), EMBEDDED).unwrap();
        let copy = tempfile::tempdir().unwrap();
        for name in [STORE_FILE, TOKEN_SECRET_FILE] {
            std::fs::copy(dir.path().join(name), copy.path().join(name)).unwrap();
        }
        detach_store_identity(copy.path()).unwrap();
        assert_eq!(file(copy.path(), STORE_ID_FILE), None);
        assert_eq!(file(copy.path(), TOKEN_SECRET_FILE), None);
        let detached = open_store_identity(copy.path(), EMBEDDED).unwrap();
        assert_ne!(detached.store_id, original.store_id);
        assert_eq!(detached.blob_store, None);
        assert_eq!(detached.app_id, original.app_id);
        assert_ne!(detached.token_secret, original.token_secret);
        assert!(detach_store_identity(&copy.path().join("missing")).is_err());
    }

    /// A server's data dir from before has no `app-id`: detaching it must
    /// not make one up, or the server's `--app-id` would be refused.
    #[test]
    fn detaching_mints_no_app_id() {
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), server("app-a")).unwrap();
        let old = tempfile::tempdir().unwrap();
        std::fs::copy(dir.path().join(STORE_FILE), old.path().join(STORE_FILE)).unwrap();
        // As if the table had no app id: a store an earlier release wrote.
        let blank = open_file(old.path()).unwrap();
        blank.host_delete(IDENTITY, APP_ID).unwrap();
        drop(blank);
        detach_store_identity(old.path()).unwrap();
        assert_eq!(read_store_identity(old.path()).unwrap().app_id, None);
        open_store_identity(old.path(), server("app-b")).unwrap();
    }

    #[test]
    fn detaching_drops_the_claim_on_a_chunk_directory_copied_with_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(BLOB_STORE_FILE), "files\n").unwrap();
        open_store_identity(dir.path(), server("app-a")).unwrap();
        std::fs::create_dir_all(dir.path().join("blobs")).unwrap();
        std::fs::write(dir.path().join("blobs/owner"), "original\n").unwrap();
        detach_store_identity(dir.path()).unwrap();
        assert!(!dir.path().join("blobs/owner").exists());
        assert_eq!(file(dir.path(), BLOB_STORE_FILE), None);
    }

    #[test]
    fn legacy_files_are_imported_and_kept_as_mirrors() {
        let dir = tempfile::tempdir().unwrap();
        for (name, value) in [
            (STORE_ID_FILE, "0f6b6a3e-6f3b-4a4e-9b1e-0d8c1f0a7c11"),
            (APP_ID_FILE, "my-app"),
            (BLOB_STORE_FILE, "files"),
            (TOKEN_SECRET_FILE, "secret"),
        ] {
            std::fs::write(dir.path().join(name), format!("{value}\n")).unwrap();
        }
        let how = IdentityOpen::Embedded {
            app_id: Some("my-app"),
        };
        let identity = open_store_identity(dir.path(), how).unwrap();
        assert_eq!(
            identity,
            StoreIdentity {
                store_id: "0f6b6a3e-6f3b-4a4e-9b1e-0d8c1f0a7c11".to_owned(),
                app_id: "my-app".to_owned(),
                blob_store: Some("files".to_owned()),
                token_secret: Some("secret".to_owned()),
            }
        );
        // The table now holds the identity: the store alone carries it.
        for (_, name) in LEGACY_FILES {
            std::fs::remove_file(dir.path().join(name)).unwrap();
        }
        assert_eq!(
            open_store_identity(dir.path(), how).unwrap(),
            identity,
            "the secret's own file stays"
        );
    }

    #[test]
    fn a_legacy_file_that_disagrees_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(BLOB_STORE_FILE), "files\n").unwrap();
        let identity = open_store_identity(dir.path(), EMBEDDED).unwrap();
        std::fs::write(dir.path().join(STORE_ID_FILE), "another\n").unwrap();
        let error = open_store_identity(dir.path(), EMBEDDED).unwrap_err();
        assert!(error.contains(&identity.store_id), "{error}");
        // Left for the operator: nothing was chosen.
        assert_eq!(
            file(dir.path(), STORE_ID_FILE).as_deref(),
            Some("another\n")
        );
    }

    /// An earlier release opened on a store whose values never left its
    /// file mints a store id of its own, and may move the values out under
    /// it: that id is the claimed one.
    #[test]
    fn an_unclaimed_store_id_gives_way_to_the_one_an_earlier_release_used() {
        let dir = tempfile::tempdir().unwrap();
        open_store_identity(dir.path(), server("app-a")).unwrap();
        std::fs::write(dir.path().join(STORE_ID_FILE), "minted-by-main\n").unwrap();
        std::fs::write(dir.path().join(BLOB_STORE_FILE), "files\n").unwrap();
        let identity = open_store_identity(dir.path(), server("app-a")).unwrap();
        assert_eq!(identity.store_id, "minted-by-main");
        assert_eq!(identity.blob_store.as_deref(), Some("files"));
    }
}
