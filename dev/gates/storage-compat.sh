#!/usr/bin/env bash
# Exact native historical-storage receipts.  Keep this separate from the broad
# workspace suite: a nextest shard or an incidental test selection must never
# be the only thing proving that the pinned epoch fixture still opens.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

dev/t --exact node::tests::harness::settlement_baseline_native_jazz_corpus_reopens_and_accepts_mixed_writes
dev/t --exact node::tests::harness::committed_native_jazz_physical_corpus_reopens_and_accepts_current_writes
dev/t --exact node::tests::harness::committed_native_jazz_physical_corpus_rejects_corruption_before_materialization
dev/t --exact node::tests::harness::native_jazz_corpus_candidate_roundtrip_rejects_broken_exports
dev/t --exact node::tests::harness::native_jazz_corpus_staging_rejects_normalized_and_physical_aliases
dev/t --exact node::tests::harness::native_jazz_corpus_publication_copy_failure_preserves_existing_destination
dev/t --exact node::tests::harness::native_jazz_corpus_digest_is_sensitive_to_application_row_bytes
dev/t --exact node::tests::harness::native_jazz_corpus_rejects_a_receipt_omitting_all_physical_application_families
