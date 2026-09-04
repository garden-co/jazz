#!/usr/bin/env python3
"""Settlement-baseline recipe for the committed epoch-1 SQLite fixture."""
import sqlite3
import sys

path = sys.argv[1]
connection = sqlite3.connect(path)
connection.execute("PRAGMA page_size = 4096")
connection.execute("PRAGMA journal_mode = DELETE")
connection.execute("PRAGMA application_id = 1245796954")  # `JAZZ`
connection.execute("PRAGMA user_version = 1")
connection.executescript("""
CREATE TABLE meta (key TEXT PRIMARY KEY, value BLOB NOT NULL) STRICT;
CREATE TABLE column_families (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE) STRICT;
CREATE TABLE kv (cf INTEGER NOT NULL, k BLOB NOT NULL, v BLOB NOT NULL, PRIMARY KEY (cf, k)) WITHOUT ROWID, STRICT;
""")

def identifier(value):
    value = value.encode()
    return bytes([len(value)]) + value

codecs = [
    "groove.large-value.v1",
    "groove.ordered-chunk-storage.v1",
    "groove.ordered-kv.v1",
]
manifest = (b"JSM1\0\x01\0\x01" + identifier("sqlite") + bytes([len(codecs)]) +
            b"".join(identifier(codec) for codec in codecs) + b"\x03" +
            identifier("application-id") + b"\0\x08" + (0x4A415A5A).to_bytes(8, "big") +
            identifier("ddl-id") + b"\0\x1d" + b"jazz-groove-ordered-kv-ddl-v1" +
            identifier("key-order") + b"\0\x16" + b"unsigned-lexicographic")
connection.executemany("INSERT INTO meta(key, value) VALUES (?, ?)", [
    ("format", b"jazz-groove-ordered-kv"),
    ("format_version", (1).to_bytes(8, "big")),
    ("ddl_id", b"jazz-groove-ordered-kv-ddl-v1"),
    ("epoch_manifest", manifest),
])
connection.executemany("INSERT INTO column_families(id, name) VALUES (?, ?)", [(1, "records"), (2, "indices")])
connection.executemany("INSERT INTO kv(cf, k, v) VALUES (?, ?, ?)", [
    (1, b"user:1", b"Ada"), (1, b"user:10", b"Ten"), (1, b"user:2", b"Grace"),
    (2, b"name:Ada", b"1"),
])
connection.commit()
connection.close()
