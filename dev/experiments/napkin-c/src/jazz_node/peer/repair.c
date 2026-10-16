/* jazz_node::peer — outgoing view-update bookkeeping and terminal commit
 * authorization proof. Rust: crates/jazz/layers/node/src/peer/repair.rs
 * (include!d into peer.rs). */
#include "peer.h"

void JZP(PeerState__record_outgoing_view_update_metadata)(PS *p, jazz_protocol_SyncMessage *update)
{
    jazz_protocol_ViewUpdatePayload *view = jazz_protocol__protocol__SyncMessage__as_view_update(update);
    if (view && jazz_protocol__protocol__SupportingRowsUpdate__is_snapshot(&view->supporting_rows) && !view->peer_payload_inventory.opening_pending) {
        /* A generic fresh snapshot supersedes any incremental publisher
         * baseline. The maintained path installs its prepared successor after
         * this metadata call; other paths rebuild once on next drain. */
        jazz_node_PeerSubscriptionState *state = JZP(PeerState__state_entry)(p, view->subscription);
        state->has_supporting_revision = false;
    }
    if (!view) return;

    jazz_protocol_VersionBundleVec singleton_bundles;
    JZP(delivery__view_update_singleton_bundles)(view->version_carriers.ptr, view->version_carriers.len, &singleton_bundles);
    p->metrics.view_updates_out += 1;
    p->metrics.version_bundles_out += singleton_bundles.len;
    p->metrics.complete_tx_payload_refs_out += view->peer_payload_inventory.complete_tx_payloads.len;

    u64 duplicates = 0;
    for (u32 i = 0; i < singleton_bundles.len; i++) {
        const jazz_protocol_VersionBundle *bundle = &singleton_bundles.ptr[i];
        if (!JZP(delivery__bundle_contains_complete_tx_payload)(bundle)) continue;
        jazz_model_TxId tx_id = jazz_model__tx__Transaction__tx_id(bundle->tx);
        if (jz_btree_contains(&p->shipped_complete_tx_payloads, &tx_id)) duplicates++;
    }
    p->metrics.duplicate_version_bundles_out += duplicates;
    jz_vec_drop(&singleton_bundles, sizeof(singleton_bundles.ptr[0]), jazz_protocol__protocol__VersionBundle__drop);
}

/* Evaluate a client commit's write policies at the terminal authority, under
 * the exact claims admitted for this connection. Each policy input is already
 * at the authority cut in this node's own storage, so the evaluation reads it
 * directly and keeps no per-connection support view. */
jz_err JZP(PeerState__prove_terminal_commit_authorization)(PS *p, NS *node, jazz_types_AuthorSubject writer, jz_btree claims,
                                                           const jazz_protocol_VersionRecordVec *versions, jazz_model_TxId candidate_tx_id, bool *out)
{
    /* SYSTEM is the trusted backend policy subject. Row-policy admission
     * already bypasses it. */
    if (writer.tag == JAZZ_AUTHOR_SYSTEM) {
        jz_btree_drop(&claims);
        *out = true;
        return JZ_OK;
    }
    /* Keep the immutable admitted snapshot installed for the entire proof; a
     * claim-only policy must not become an implicit grant. */
    NS *scoped = jazz_node__node__NodeState__scoped_active_session_claims(node, writer, claims);
    jz_err e = jazz_node__node__ingest__NodeState__commit_unit_satisfies_write_policy(scoped, versions, writer, candidate_tx_id, out);
    jazz_node__node__NodeState__end_scoped_active_session_claims(scoped);
    return e;
}

void JZP(PeerState__record_outgoing_view_update)(PS *p, jazz_protocol_SyncMessage *update)
{
    JZP(PeerState__record_outgoing_view_update_metadata)(p, update);
    jazz_protocol_ViewUpdatePayload *view = jazz_protocol__protocol__SyncMessage__as_view_update(update);
    if (view) {
        u8 revision[16];
        jazz_protocol__protocol__SupportingRowsUpdate__revision(&view->supporting_rows, revision);
        JZP(PeerSubscriptionState__acknowledge_published)(JZP(PeerState__state_entry)(p, view->subscription), revision);
    }
}

void JZP(PeerState__apply_outgoing_view_delta)(PS *p, SubKey sub, bool reset_input_set,
                                               const jazz_protocol_ResultMemberEntry *adds, u32 nadds,
                                               const jazz_protocol_ResultMemberEntry *removes, u32 nremoves)
{
    jazz_node_PeerSubscriptionState *state = JZP(PeerState__state_entry)(p, sub);
    /* This path records an independently supplied delta rather than the
     * maintained journal's exact successor. Re-establish its baseline if this
     * subscription later returns to maintained publication. */
    if (reset_input_set && state->maintained_subscription_view)
        JZN_MSV(MaintainedSubscriptionView__forget_peer_source_closure_baseline)(state->maintained_subscription_view->maintained);
    if (reset_input_set) {
        state->has_supporting_revision = false;
        jz_btree_clear(&state->result_member_set);
        jz_btree_clear(&state->member_index);
    }
    for (u32 i = 0; i < nremoves; i++) {
        jz_btree_remove(&state->result_member_set, &removes[i], NULL, NULL);
        jazz_protocol_ResultMemberEntryVec scratch = {0};
        JZP(delivery__apply_contribution_remove)(state, &removes[i], 1, &scratch);
        jz_vec_drop(&scratch, sizeof(scratch.ptr[0]), jazz_protocol__protocol__ResultMemberEntry__drop);
    }
    for (u32 i = 0; i < nadds; i++) {
        jazz_protocol_ResultMemberEntry c;
        jazz_protocol__protocol__ResultMemberEntry__clone(&c, &adds[i]);
        if (jz_btree_insert(&state->result_member_set, &c, NULL, NULL)) jazz_protocol__protocol__ResultMemberEntry__drop(&c);
        jazz_protocol_ResultMemberEntryVec scratch_adds = {0}, scratch_removes = {0};
        JZP(delivery__apply_contribution_add)(state, &adds[i], 1, &scratch_adds, &scratch_removes);
        jz_vec_drop(&scratch_adds, sizeof(scratch_adds.ptr[0]), jazz_protocol__protocol__ResultMemberEntry__drop);
        jz_vec_drop(&scratch_removes, sizeof(scratch_removes.ptr[0]), jazz_protocol__protocol__ResultMemberEntry__drop);
    }
#ifndef NDEBUG
    /* Diagnostic-only invariant check (cfg(debug_assertions) in Rust). */
    {
        jazz_node_RowKey row_key; jazz_model_TxId first, second;
        if (JZP(delivery__duplicate_physical_row_result_set)(&state->result_member_set, &row_key, &first, &second)) {
            JZ_ASSERT(jazz_model_TxId_eq(&first, &second),
                      "peer subscription has multiple content versions for physical output row");
            jazz_types__object__OutputOccurrenceId__drop(row_key.occurrence);
        }
    }
#endif
}
