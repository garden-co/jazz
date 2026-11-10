#!/bin/sh
# Independent dependency graph: unmodified Rivet owns SQLite; no rusqlite.
set -eu
cd "$(dirname "$0")"
export LIBSQLITE3_FLAGS='SQLITE_ENABLE_BATCH_ATOMIC_WRITE SQLITE_DIRECT_OVERFLOW_READ=0'
cargo test --locked
