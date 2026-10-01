#!/usr/bin/env bash
# Exact native historical-storage receipts.  Keep this separate from the broad
# workspace suite: a nextest shard or an incidental test selection must never
# be the only thing proving that the pinned epoch fixture still opens.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

# Corpus-only helpers must stay behind Groove's test feature without hiding
# production database methods. Compile the same non-test Jazz surface that
# consumes the historical fixtures before running test-feature receipts.
cargo check -p jazz --no-default-features --features testing,transport-compression-zstd

dev/t --exact node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes
dev/t --exact node::tests::harness::committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes
dev/t --exact node::tests::harness::committed_native_jazz_physical_corpus_rejects_corruption_before_materialization
dev/t --exact node::tests::harness::native_jazz_corpus_candidate_roundtrip_rejects_broken_exports
dev/t --exact node::tests::harness::native_jazz_corpus_staging_rejects_normalized_and_physical_aliases
# `dev/t --exact` inventories before running and rejects zero selections, so a
# renamed/removed publication guard cannot make this required partition green.
dev/t --exact node::tests::harness::native_jazz_corpus_publication_rejects_existing_output_and_preserves_it
dev/t --exact node::tests::harness::native_jazz_corpus_digest_is_sensitive_to_application_row_bytes
dev/t --exact node::tests::harness::native_jazz_corpus_rejects_a_receipt_omitting_all_physical_application_families
# Immutable bytes produced by the distributed alpha.54 Linux NAPI artifact.
# The linear row-history format refuses this DAG-layout root at open with the
# typed UnsupportedStorageCodecs error and leaves every record unchanged.
dev/t --exact node::tests::harness::published_alpha54_native_corpus_is_refused_with_the_typed_codec_error
# Immutable client root produced by the distributed alpha.56 NAPI/jazz-tools
# packages: a retired Edge receipt (Accepted, durability tag 2, no global time).
# Refused the same way, without rewriting the edge or Core records.
dev/t --exact node::tests::harness::published_alpha56_legacy_edge_receipt_is_refused_without_rewriting_its_records
dev/t --exact node::tests::harness::retired_result_codec_profiles_reject_historical_native_roots
# The same refusals through the public adapter entry points with the node
# profile, including the pre-linear current corpora and a root that predates
# the compact durable index or the row-author alias family.
dev/t --test integration --exact storage_format_refusal::published_alpha54_rocksdb_root_is_refused_with_a_typed_format_error
dev/t --test integration --exact storage_format_refusal::published_alpha56_rocksdb_root_is_refused_with_a_typed_format_error
dev/t --test integration --exact storage_format_refusal::pre_linear_native_corpora_are_refused_before_any_mutation
dev/t --test integration --exact storage_format_refusal::linear_history_root_without_the_durable_index_family_is_refused
dev/t --test integration --exact storage_format_refusal::pre_alias_linear_history_root_is_refused_with_a_typed_format_error
