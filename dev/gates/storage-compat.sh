#!/usr/bin/env bash
# Exact native storage receipts.  Keep this separate from the broad workspace
# suite: a nextest shard or an incidental test selection must never be the only
# thing proving that the pinned SQLite corpus still opens and that stores this
# build does not read are refused without being changed.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

# `dev/t --exact` inventories before running and rejects zero selections, so a
# renamed or removed receipt cannot make this required partition green.
#
# The pinned SQLite storage-format-1 corpus (an authority and a synced client
# with pending uploads) reopens with every row, history version, branch, large
# value and renamed column, resends its uploads, accepts current writes, and
# reopens again.
dev/t --test integration --exact storage_corpus::pinned_sqlite_corpus_reopens_and_accepts_current_writes
# The pinned server data dir (a catalogue-driven server's jazz.sqlite: rows,
# catalogue tables, and the account journal host table)
# serves its deployments, accounts and rows, accepts current writes, and
# reopens again.
dev/t -p jazz-server --exact server::data_dir_corpus::pinned_server_data_dir_reopens_and_accepts_current_writes
# Damage is reported as a storage error, never read as rows.
dev/t --test integration --exact storage_corpus::a_damaged_store_is_refused_rather_than_read
# Another storage format, or a SQLite file Jazz did not write, is refused with
# the typed UnsupportedStorageCodecs error before anything writes to it.
dev/t --test integration --exact storage_corpus::another_storage_format_is_refused_without_changing_the_file
# Immutable Groove stores (RocksDB from the published alpha.54 and alpha.56
# packages and the last Groove format, and a Groove SQLite-adapter file) are
# refused the same way, on the store path and through the native client.
dev/t --test integration --exact storage_corpus::pre_sqlite_stores_are_refused_with_a_typed_error_and_left_unchanged
dev/t --test integration --exact storage_corpus::the_native_client_refuses_a_data_directory_from_before_sqlite
# The refusal stays typed on the public surfaces applications match on.
dev/t --test integration --exact error_code_strings::every_error_code_has_its_pinned_stable_string
