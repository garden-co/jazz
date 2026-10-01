//! Stores written by the DAG history layout are refused at open.
//!
//! These receipts open real committed roots through the public adapter entry
//! point with the node profile, exactly as the server shell, the native
//! client, NAPI and the native relay do. They are adapter-level rather than
//! `JazzServer` tests because the refusal happens at manifest admission,
//! before any node state exists; the public client path reports the same
//! typed error through its display text.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use groove::storage::Error as StorageError;
use jazz::schema::JazzSchema;
use jazz::storage_codec_profile::node_storage_codec_profile;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use sha2::{Digest, Sha256};

const DURABLE_INDEX_V2: &str = "groove.durable-index.v2";
const AUTHOR_ALIAS_V1: &str = "jazz.author-alias.v1";
const ROW_HISTORY: &str = "jazz.history-version-current.v4";

fn notes_schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new("notes").column("body", ColumnType::Text))
        .build();
    JazzSchema::new(&source).expect("notes schema compiles")
}

fn checked_fixture(base64: &str, sha256: &str) -> Vec<u8> {
    let bytes = STANDARD
        .decode(base64.lines().collect::<String>())
        .expect("fixture is base64");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        sha256,
        "committed fixture checksum"
    );
    bytes
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    std::io::Read::read_to_end(
        &mut flate2::read::GzDecoder::new(std::io::Cursor::new(bytes)),
        &mut out,
    )
    .expect("fixture is gzip");
    out
}

fn unpack_rocksdb(root: &std::path::Path, archive: &[u8]) -> std::path::PathBuf {
    tar::Archive::new(flate2::read::GzDecoder::new(std::io::Cursor::new(archive)))
        .unpack(root)
        .expect("RocksDB archive unpacks");
    let path = root.join("rocksdb-epoch-1");
    assert!(path.is_dir(), "archive holds the rocksdb-epoch-1 root");
    path
}

fn open_rocksdb(path: &std::path::Path) -> Result<(), StorageError> {
    let families = notes_schema().column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    jazz_storage_rocksdb::RocksDbStorage::open_with_durability_and_codec_profile(
        path,
        &refs,
        jazz_storage_rocksdb::Durability::FullSync,
        &node_storage_codec_profile().expect("node profile"),
    )
    .map(drop)
}

fn open_sqlite(path: &std::path::Path) -> Result<(), StorageError> {
    let families = notes_schema().column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    jazz_storage_sqlite::SqliteStorage::open_with_durability_and_codec_profile(
        path,
        &refs,
        jazz_storage_sqlite::Durability::FullSync,
        &node_storage_codec_profile().expect("node profile"),
    )
    .map(drop)
}

fn assert_refused(result: Result<(), StorageError>, expected_unknown: &[&str], store: &str) {
    match result {
        Err(StorageError::UnsupportedStorageCodecs {
            epoch,
            missing,
            unknown,
        }) => {
            assert_eq!(epoch, 1, "{store}");
            // A DAG-layout root predates every node-root family.
            assert_eq!(
                missing,
                vec![
                    DURABLE_INDEX_V2.to_owned(),
                    AUTHOR_ALIAS_V1.to_owned(),
                    ROW_HISTORY.to_owned()
                ],
                "{store}"
            );
            assert_eq!(unknown, expected_unknown, "{store}");
        }
        Err(other) => panic!("{store}: expected a typed format refusal, got {other}"),
        Ok(()) => panic!("{store}: a DAG-layout root must not open"),
    }
}

/// A published alpha.54 client root (DAG layout) is refused at open with the
/// typed `UnsupportedStorageCodecs` error naming the durable-index and
/// row-history families it lacks, instead of opening and failing later with a record decode error.
///
/// Actors: `alice` upgrades her native app from alpha.54 over the same
/// data directory.
///
/// ```text
/// alice's alpha.54 root ──open(node profile)──✗ UnsupportedStorageCodecs
///                                               missing [groove.durable-index.v2,
///                                                        jazz.author-alias.v1,
///                                                        jazz.history-version-current.v4]
/// ```
#[test]
fn published_alpha54_rocksdb_root_is_refused_with_a_typed_format_error() {
    let directory = tempfile::tempdir().unwrap();
    let archive = checked_fixture(
        include_str!("../fixtures/published-alpha54-native-rocksdb.tar.gz.base64"),
        "10d139b12fd21530fd553ee148e975d4e2f55d11b43bf3bd90d00179f5703575",
    );
    let path = unpack_rocksdb(directory.path(), &archive);
    assert_refused(open_rocksdb(&path), &[], "published alpha.54 RocksDB");
    // Refusal is stable: the first attempt admitted nothing.
    assert_refused(
        open_rocksdb(&path),
        &[],
        "published alpha.54 RocksDB reopen",
    );
}

/// The published alpha.56 client root that holds an unsynced edge-accepted
/// write is refused at open rather than opening and failing on first read.
///
/// Actors: `bob` still has a write that Core never saw when he upgrades.
///
/// ```text
/// bob's alpha.56 root ──open(node profile)──✗ UnsupportedStorageCodecs
/// ```
#[test]
fn published_alpha56_rocksdb_root_is_refused_with_a_typed_format_error() {
    let directory = tempfile::tempdir().unwrap();
    let archive = checked_fixture(
        include_str!("../fixtures/published-alpha56-legacy-edge-receipt-rocksdb.tar.gz.base64"),
        "784f1ad4473464784f83bf169be6c13a60b84b408e77728ff93a2dc8630248a7",
    );
    let path = unpack_rocksdb(directory.path(), &archive);
    assert_refused(open_rocksdb(&path), &[], "published alpha.56 RocksDB");
}

/// The committed pre-linear native corpora (SQLite and RocksDB) are refused
/// at manifest admission on both adapters, and the SQLite file is left
/// byte-for-byte unchanged. The older epoch-1 SQLite corpus also declares two
/// retired result families, which the error reports as unknown.
///
/// Actors: `carol` runs a relay (SQLite) and a Core (RocksDB) written by the
/// DAG layout.
///
/// ```text
/// carol's SQLite relay root ──open──✗ UnsupportedStorageCodecs (file unchanged)
/// carol's RocksDB Core root ──open──✗ UnsupportedStorageCodecs
/// ```
#[test]
fn pre_linear_native_corpora_are_refused_before_any_mutation() {
    for (base64, archive_sha, sqlite_sha, unknown, store) in [
        (
            include_str!("../fixtures/pre-linear-native-jazz.sqlite.gz.base64"),
            "a3606d7045d477dab33d9bf0c60d3a1d1c1608dfd581f9e1bbf1c8abd6447c13",
            "28902888353cf33af039811b82c45875a3c2f72a4176e06e3e5d50826b8734bd",
            &[][..],
            "pre-linear current SQLite corpus",
        ),
        (
            include_str!("../fixtures/epoch-1-native-jazz.sqlite.gz.base64"),
            "0436f97b2b8bb04ee286b1ce9a7e1866bdd40115e0878223135bb0e700c5c3a8",
            "9cf200ef662e18a0f841b9a9ff6606528b026de2ddde5c66703d762e92f457ac",
            &["jazz.result-member-key.v1", "jazz.result-row-source.v1"][..],
            "epoch-1 settlement SQLite corpus",
        ),
    ] {
        let sqlite = gunzip(&checked_fixture(base64, archive_sha));
        assert_eq!(format!("{:x}", Sha256::digest(&sqlite)), sqlite_sha);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("root.sqlite");
        std::fs::write(&path, &sqlite).unwrap();
        assert_refused(open_sqlite(&path), unknown, store);
        assert_eq!(
            std::fs::read(&path).unwrap(),
            sqlite,
            "{store} is not mutated"
        );
    }

    let directory = tempfile::tempdir().unwrap();
    let archive = checked_fixture(
        include_str!("../fixtures/pre-linear-native-jazz-rocksdb.tar.gz.base64"),
        "21a6c49b90d83f33ba3deba2cfd7605db155e78deeec22f683158037c66d7ba7",
    );
    let path = unpack_rocksdb(directory.path(), &archive);
    assert_refused(
        open_rocksdb(&path),
        &[],
        "pre-linear current RocksDB corpus",
    );
}

/// A root with the current row-history family but written before the compact
/// durable-index layout (no `groove.durable-index.v2`) is refused
/// at manifest admission, naming only the index family, and the SQLite file
/// is left byte-for-byte unchanged.
///
/// Actors: `dave` upgrades a client whose store holds name-prefixed index
/// entries.
///
/// ```text
/// dave's pre-index-v2 root ──open(node profile)──✗ UnsupportedStorageCodecs
///                                              missing [groove.durable-index.v2]
/// ```
#[test]
fn linear_history_root_without_the_durable_index_family_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("root.sqlite");
    let families = notes_schema().column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let pre_index_v2_profile = jazz::storage_codec_profile::epoch_1_storage_codec_profile()
        .expect("base profile")
        .with_additional_codecs([AUTHOR_ALIAS_V1, ROW_HISTORY])
        .expect("pre-index-v2 node profile");
    drop(
        jazz_storage_sqlite::SqliteStorage::open_with_durability_and_codec_profile(
            &path,
            &refs,
            jazz_storage_sqlite::Durability::FullSync,
            &pre_index_v2_profile,
        )
        .expect("pre-index-v2 profile root"),
    );
    let before = std::fs::read(&path).unwrap();
    match open_sqlite(&path) {
        Err(StorageError::UnsupportedStorageCodecs {
            epoch: 1,
            missing,
            unknown,
        }) => {
            assert_eq!(missing, vec![DURABLE_INDEX_V2.to_owned()]);
            assert!(unknown.is_empty());
        }
        Err(other) => panic!("expected a typed format refusal, got {other}"),
        Ok(()) => panic!("a root without the durable-index family must not open"),
    }
    assert_eq!(std::fs::read(&path).unwrap(), before, "root is not mutated");
}

/// A linear-history root written before row-author aliasing (it declares
/// `groove.durable-index.v2` and `jazz.history-version-current.v4` but not
/// `jazz.author-alias.v1`, and
/// stores full `RowAuthor` records where current code reads a 4-byte alias)
/// is refused at open with the typed error naming only the alias family, and
/// the SQLite file is left byte-for-byte unchanged.
///
/// Actors: `dave` upgrades a client whose store was written by a
/// linear-history build before author aliases.
///
/// ```text
/// dave's pre-alias root ──open(node profile)──✗ UnsupportedStorageCodecs
///                                               missing [jazz.author-alias.v1]
/// ```
#[test]
fn pre_alias_linear_history_root_is_refused_with_a_typed_format_error() {
    let families = notes_schema().column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let pre_alias_profile = jazz::storage_codec_profile::epoch_1_storage_codec_profile()
        .expect("base profile")
        .with_additional_codecs([DURABLE_INDEX_V2, ROW_HISTORY])
        .expect("pre-alias node profile");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("root.sqlite");
    jazz_storage_sqlite::SqliteStorage::open_with_durability_and_codec_profile(
        &path,
        &refs,
        jazz_storage_sqlite::Durability::FullSync,
        &pre_alias_profile,
    )
    .map(drop)
    .expect("a pre-alias build creates its root");
    let written = std::fs::read(&path).unwrap();

    match open_sqlite(&path) {
        Err(StorageError::UnsupportedStorageCodecs {
            epoch,
            missing,
            unknown,
        }) => {
            assert_eq!(epoch, 1);
            assert_eq!(missing, vec![AUTHOR_ALIAS_V1.to_owned()]);
            assert!(unknown.is_empty());
        }
        Err(other) => panic!("expected a typed format refusal, got {other}"),
        Ok(()) => panic!("a pre-alias root must not open"),
    }
    assert_eq!(
        std::fs::read(&path).unwrap(),
        written,
        "the refused root is not mutated"
    );
}
