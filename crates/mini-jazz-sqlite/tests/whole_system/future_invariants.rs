#[test]
#[ignore = "file/blob product surface is not implemented yet"]
fn file_blob_bytes_do_not_bypass_row_policy_placeholder() {
    panic!(
        "future invariant: blob metadata and byte serving must both re-check Jazz session policy"
    );
}

#[test]
#[ignore = "encrypted fields and encrypted indexes are not implemented yet"]
fn encrypted_fields_do_not_participate_in_server_plaintext_querying_placeholder() {
    panic!(
        "future invariant: client-decrypted fields cannot be used by untrusted servers for plaintext filtering, sorting, indexing, or policy"
    );
}

#[test]
#[ignore = "admin/tooling catalogue publication flow is not implemented yet"]
fn catalogue_publication_requires_admin_and_fails_closed_without_permissions_placeholder() {
    panic!(
        "future invariant: schema+permission catalogue publication is admin controlled and missing explicit permissions fail closed"
    );
}

#[test]
#[ignore = "predicate/range read-set validation is not implemented yet"]
fn exclusive_predicate_read_set_rejects_when_matching_row_is_inserted_later_placeholder() {
    panic!(
        "future invariant: exclusive transactions that read a predicate/range must reject if an authority-visible matching row appears before validation"
    );
}

#[test]
#[ignore = "branch backing rows are not yet product permission objects in the prototype"]
fn unreadable_branch_backing_row_prevents_checkout_and_export_placeholder() {
    panic!(
        "future invariant: branch handles/checkouts/exports require readable branch backing rows as well as engine metadata"
    );
}

#[test]
#[ignore = "range observed facts and range read-set validation are not implemented yet"]
fn range_query_refresh_repairs_rows_that_enter_and_leave_boundaries_placeholder() {
    panic!(
        "future invariant: range query interests must sync current matches, repair rows that leave the range, and feed authority range read-set validation"
    );
}

#[test]
#[ignore = "cache eviction policy is not implemented yet"]
fn evicting_uninteresting_local_facts_preserves_history_needed_for_active_queries_placeholder() {
    panic!(
        "future invariant: async cache eviction may drop uninteresting local facts only when active queries, policy deps, and replay/export needs remain reconstructible"
    );
}

#[test]
#[ignore = "as-of-time query API and timestamp-to-epoch mapping are not implemented yet"]
fn as_of_time_query_maps_timestamp_to_stable_global_snapshot_placeholder() {
    panic!(
        "future invariant: as-of-time queries should map wall-clock time to a stable global epoch snapshot without exposing partially settled history"
    );
}

#[test]
#[ignore = "stable public error surface is not implemented yet"]
fn public_errors_use_stable_codes_and_redacted_details_across_surfaces_placeholder() {
    panic!(
        "future invariant: write promises, query failures, sync failures, rejection subscriptions, and global callbacks should expose stable machine codes with redacted details"
    );
}

#[test]
#[ignore = "query settled-state barriers are not implemented yet"]
fn observed_query_refresh_reports_settled_state_after_all_descriptors_refresh_placeholder() {
    panic!(
        "future invariant: query/subscription state should distinguish retained local facts from all active descriptors refreshed through a known upstream authority point"
    );
}

#[test]
#[ignore = "compact reconnect summaries are not implemented yet"]
fn compact_reconnect_summary_refreshes_only_active_query_descriptors_placeholder() {
    panic!(
        "future invariant: reconnect should replay active query descriptors compactly, refresh only live interests, and leave forgotten retained facts as cache state"
    );
}

#[test]
#[ignore = "catalogue observed facts are not implemented yet"]
fn catalogue_observed_fact_invalidates_query_when_schema_head_changes_placeholder() {
    panic!(
        "future invariant: queries interpreted through a catalogue/lens head should observe catalogue facts and invalidate when the relevant schema head changes"
    );
}

#[test]
#[ignore = "permission-only catalogue publication is not implemented yet"]
fn missing_permission_catalogue_fails_closed_for_query_export_placeholder() {
    panic!(
        "future invariant: a known structural schema without explicit current permissions must fail closed for query export and sync"
    );
}

#[test]
#[ignore = "staged untrusted authority apply before publication is not implemented yet"]
fn staged_untrusted_apply_is_not_visible_until_authority_publication_placeholder() {
    panic!(
        "future invariant: an authority may validate/stage incoming untrusted history before publication, but staged rows must not be visible to ordinary reads or subscriptions"
    );
}

#[test]
#[ignore = "resolved conflict provenance metadata shape is not implemented yet"]
fn conflict_resolution_preserves_resolved_candidate_provenance_placeholder() {
    panic!(
        "future invariant: conflict resolution should retain which candidate tx ids / branch bases were resolved, even after current conflict metadata is cleared"
    );
}

#[test]
#[ignore = "generated index and query-plan assertions are not implemented yet"]
fn generated_indexes_are_used_for_ordered_page_query_plan_placeholder() {
    panic!(
        "future invariant: generated SQLite indexes should keep ordered page queries off accidental broad table scans"
    );
}
