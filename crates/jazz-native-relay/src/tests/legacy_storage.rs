//! Frozen physical layouts from the retired ordered-KV adapter. This test-only
//! fixture replaces calls to a removed crate; it preserves both old codec sets.
//! Raw SQL is intentional: an admission test must create the incompatible layout
//! without invoking the current opener or silently upgrading its manifest.
use std::path::Path;

pub(super) fn plant(path: &Path, profile: &str) {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../fixtures/legacy_sqlite_profiles.json")).unwrap();
    let encoded = fixture[profile].as_str().unwrap();
    let manifest = (0..encoded.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA synchronous=FULL; PRAGMA application_id=1245796954; PRAGMA user_version=1; CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT; CREATE TABLE column_families (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT; CREATE TABLE kv (cf INTEGER NOT NULL, k BLOB NOT NULL, v BLOB NOT NULL, PRIMARY KEY (cf,k)) WITHOUT ROWID, STRICT;").unwrap();
    conn.execute("INSERT INTO meta(key,value) VALUES('format',?1),('format_version',?2),('ddl_id',?3),('epoch_manifest',?4)",rusqlite::params![b"jazz-groove-ordered-kv".as_slice(),1i64.to_be_bytes().as_slice(),b"jazz-groove-ordered-kv-ddl-v1".as_slice(),manifest]).unwrap();
    conn.close().unwrap();
}
