#!/bin/sh
# Public RivetKit integration; tests run against a local Rivet engine.
set -eu
cd "$(dirname "$0")"
export LIBSQLITE3_FLAGS='SQLITE_ENABLE_BATCH_ATOMIC_WRITE SQLITE_DIRECT_OVERFLOW_READ=0'
export JAZZ_CORE_SECRET="jazz-rivet-integration-test-secret-not-for-deployment"
cargo test --locked --features testing
