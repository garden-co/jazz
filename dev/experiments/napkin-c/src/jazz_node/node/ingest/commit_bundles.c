/* ingest/commit_bundles.c: commit-unit admission at the authority and relay,
 * local self-finalization, and reset-bundle bulk ingestion (include!d into
 * jazz_node::node::ingest).
 * Rust: crates/jazz/layers/node/src/node/ingest/commit_bundles.rs */
#include "../ingest.h"

#define JZN_UNSUPPORTED_COMMIT(msg) jz_err_new(JZN_E_UNSUPPORTED_COMMIT_UNIT, (msg))
static jz_err jzn_conflicting_commit_unit(jazz_types_TxId tx) {
    jz_str d = {0};
    jazz_types__ids__TxId__debug(&d, &tx);
    jz_err e = jz_err_fmt(JZN_E_CONFLICTING_COMMIT_UNIT, "conflicting commit unit for transaction %.*s", (int)d.len, d.ptr);
    jz_str_drop(&d);
    return e;
}
static jz_err jzn_missing_transaction(jazz_types_TxId tx) {
    jz_str d = {0};
    jazz_types__ids__TxId__debug(&d, &tx);
    jz_err e = jz_err_fmt(JZN_E_MISSING_TRANSACTION, "missing transaction %.*s", (int)d.len, d.ptr);
    jz_str_drop(&d);
    return e;
}

static const jazz_protocol_VersionRecord *const *jzn_vptrs(const jazz_protocol_VersionRecordVec *v) {
    return (const jazz_protocol_VersionRecord *const *)v->ptr;
}

static bool jzn_version_vecs_eq(const jazz_protocol_VersionRecordVec *a, const jazz_protocol_VersionRecordVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jazz_protocol_VersionRecord_eq(a->ptr[i], b->ptr[i])) return false;
    return true;
}

/* query_versions_for_tx(..).into_iter().map(version_record_from_row).collect(), then sort() */
static jz_err jzn_existing_versions_sorted(NSA *s, jazz_types_TxId tx_id, jazz_protocol_VersionRecordVec *out) {
    jazz_node_VersionRowVec rows = {0};
    JZ_TRY(jazz_node__node__currency__NodeState__query_versions_for_tx(s, tx_id, &rows));
    jazz_protocol_VersionRecordVec records = {0};
    jz_err err = JZ_OK;
    for (u32 i = 0; i < rows.len; i++) {
        jazz_protocol_VersionRecord *r;
        if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__version_record_from_row(s, &rows.ptr[i], &r))) break;
        JZ_VEC_PUSH(records, r);
    }
    jz_vec_drop(&rows, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
    if (JZ_IS_ERR(err)) { jazz_protocol_VersionRecordVec_drop(&records); return err; }
    jz_sort(records.ptr, records.len, sizeof(jazz_protocol_VersionRecord *), jazz_protocol_VersionRecord_cmp, NULL);
    *out = records;
    return JZ_OK;
}

static jz_err jzn_reject_with_fate(NSA *s, jazz_model_Transaction tx, jazz_model_Fate fate, jazz_node_PublicationOutcome *out) {
    jazz_types_TxId tx_id = tx.tx_id;
    jazz_model_Fate copy;
    jazz_model_Fate_clone(&copy, &fate);
    jz_err err = jazz_node__node__ingest__NodeState__ingest_rejected_transaction(s, tx, copy);
    if (JZ_IS_ERR(err)) { jazz_model_Fate_drop(&fate); return err; }
    jazz_protocol_SyncMessage m;
    jazz_node__node__ingest__fate_update_message(tx_id, fate, false, 0, false, 0, &m);
    jazz_node__node__ingest__settled_one(out, m);
    return JZ_OK;
}

static jz_err jzn_reject_malformed_settled(NSA *s, jazz_model_Transaction tx, jz_str reason, jazz_node_PublicationOutcome *out) {
    jazz_protocol_SyncMessageVec msgs = {0};
    JZ_TRY(jazz_node__node__ingest__NodeState__reject_malformed_commit(s, tx, reason, &msgs));
    jazz_node__node__PublicationOutcome__settled(out, msgs);
    return JZ_OK;
}

/* Ingest a commit unit as fate authority. */
jz_err jazz_node__node__ingest__NodeState__ingest_commit_unit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
                                                             u64 now_ms, jazz_node_PublicationOutcome *out) {
    jz_err err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s);
    if (JZ_IS_ERR(err)) { jazz_model_Transaction_drop(&tx); jazz_protocol_VersionRecordVec_drop(&versions); return err; }
    return jazz_node__node__ingest__NodeState__ingest_commit_unit_with_context(s, tx, versions, now_ms, NULL, out);
}

static jz_err jzn_ingest_commit_unit_checked(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, u64 now_ms,
                                             const jazz_node_CommitUnitIngestContext *ctx, jazz_node_PublicationOutcome *out);

/* Ingest a commit unit as fate authority with an optional authenticated
 * connection identity. SPEC/7 §7.2 evaluates policy against the connection
 * subject; `made_by` is provenance unless the link is an untrusted session. */
jz_err jazz_node__node__ingest__NodeState__ingest_commit_unit_with_context(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
                                                                          u64 now_ms, const jazz_node_CommitUnitIngestContext *ctx,
                                                                          jazz_node_PublicationOutcome *out) {
    jz_err err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s);
    if (JZ_IS_ERR(err)) { jazz_model_Transaction_drop(&tx); jazz_protocol_VersionRecordVec_drop(&versions); return err; }
    if (now_ms > s->parking.newest_ingest_ms) s->parking.newest_ingest_ms = now_ms;
    /* A session or relay transport may not select a durable policy capability. */
    if (ctx && (ctx->trust == JZN_TRUST_SESSION || ctx->trust == JZN_TRUST_RELAY)) tx.has_permission_subject = false;
    jazz_node_PublicationOutcome updates;
    JZ_TRY(jzn_ingest_commit_unit_checked(s, tx, versions, now_ms, ctx, &updates));
    jazz_node_PublicationOutcome drained;
    err = jazz_node__node__ingest__NodeState__drain_parked_commit_units(s, &drained);
    if (JZ_IS_ERR(err)) { jazz_node_PublicationOutcome_drop(&updates); return err; }
    jazz_node__node__PublicationOutcome__append_outcome(&updates, &drained);
    *out = updates;
    return JZ_OK;
}

static void jzn_restore_clock_after_failed_authority_ingest(NSA *s, jazz_node_Clock before);

/* Authority ingest of one received unit: the receipt and size checks, then one admission attempt. */
static jz_err jzn_ingest_commit_unit_checked(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, u64 now_ms,
                                             const jazz_node_CommitUnitIngestContext *ctx, jazz_node_PublicationOutcome *out) {
    /* A checked wire decoder already validated these exact receipts. */
    if (!ctx || (!jazz_node__node__CommitUnitTrust__is_trusted(ctx->trust) && !ctx->version_receipts_validated)) {
        jz_err e = jazz_protocol__protocol__validate_version_records(&versions);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            jazz_protocol_VersionRecordVec_drop(&versions);
            return jzn_reject_malformed_settled(s, tx, jz_str_from("malformed version receipt"), out);
        }
    }
    jz_str reason;
    if (jazz_node__protocol_limits__commit_unit_limit_violation(&versions, &reason)) {
        jazz_protocol_VersionRecordVec_drop(&versions);
        return jzn_reject_with_fate(s, tx, jazz_node__node__ingest__malformed_commit_fate(reason), out);
    }
    if (jazz_node__node__ingest__commit_unit_write_count_matches(&tx, versions.len) &&
        jazz_node__node__ingest__NodeState__malformed_authored_version_reason(s, jzn_vptrs(&versions), versions.len, &reason)) {
        jazz_protocol_VersionRecordVec_drop(&versions);
        return jzn_reject_malformed_settled(s, tx, reason, out);
    }
    jazz_node_Clock clock_before_ingest;
    jazz_node_Clock_clone(&clock_before_ingest, &s->clock);
    /* One admission attempt includes policy evaluation and storage repair. */
    jz_err err = jazz_node__node__ingest__NodeState__ingest_commit_unit_once(s, tx, versions, now_ms, ctx, out);
    if (JZ_IS_ERR(err)) {
        jzn_restore_clock_after_failed_authority_ingest(s, clock_before_ingest);
        return err;
    }
    jazz_node_Clock_drop(&clock_before_ingest);
    return JZ_OK;
}

/* Undo speculative authority clock work after a rejected ingest without
 * reusing a sequence that became durable before a later derived write failed. */
static void jzn_restore_clock_after_failed_authority_ingest(NSA *s, jazz_node_Clock before) {
    if (s->clock.committed_global_time == before.committed_global_time &&
        jazz_node_u64_set_eq(&s->clock.applied_global_times_after_frontier, &before.applied_global_times_after_frontier)) {
        jazz_node_Clock_drop(&s->clock);
        s->clock = before;
        return;
    }
    jazz_types_GlobalTime highest_recovered = s->clock.committed_global_time;
    u64 *last;
    if (jz_btree_last(&s->clock.applied_global_times_after_frontier, (void **)&last, NULL)) highest_recovered = *last;
    jazz_types_GlobalTime reg = s->clock.global_time_register;
    if (before.global_time_register > reg) reg = before.global_time_register;
    if (highest_recovered > reg) reg = highest_recovered;
    s->clock.global_time_register = reg;
    jazz_node_Clock_drop(&before);
}

/* Self-acceptance at the authority: assign the next global timestamp and mark Accepted/Global. */
static jz_err jzn_self_accept(NSA *s, jazz_types_TxId tx_id) {
    jazz_types_GlobalTime global_time;
    JZ_TRY(jazz_node__node__Clock__allocate_global_time(&s->clock, jazz_types__time__TxTime__physical_ms(tx_id.time), &global_time));
    /* This node mints the seq, so it derives the row's post-image. */
    s->minting_global_time = true;
    jazz_model_Fate accepted = { .tag = JAZZ_FATE_ACCEPTED };
    jz_err applied = jazz_node__node__ingest__NodeState__apply_fate_update(s, tx_id, accepted, true, global_time, true, JAZZ_DURABILITY_GLOBAL);
    s->minting_global_time = false;
    return applied;
}

/* Finalize a locally-authored pending mergeable commit as the global authority. */
jz_err jazz_node__node__ingest__NodeState__finalize_local_mergeable_commit(NSA *s, jazz_types_TxId tx_id, jazz_node_PublicationOutcome *out) {
    JZ_TRY(jazz_node__node__state__durable__NodeState__require_catalogue_ready(s));
    bool found;
    jazz_node_StoredTransaction stored;
    JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(s, tx_id, &found, &stored));
    if (!found) return jzn_missing_transaction(tx_id);
    jz_err err = JZ_OK;
    jazz_protocol_VersionRecordVec records = {0};
    if (stored.tx.kind != JAZZ_TX_KIND_MERGEABLE) { err = JZN_UNSUPPORTED_COMMIT("self-finalize is mergeable-only"); goto done; }
    if (stored.fate.tag != JAZZ_FATE_PENDING) { jazz_node__node__ingest__settled_empty(out); goto done; }
    {
        jazz_node_VersionRowVec rows = {0};
        if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_versions_for_tx(s, tx_id, &rows))) goto done;
        for (u32 i = 0; i < rows.len && !JZ_IS_ERR(err); i++) {
            jazz_protocol_VersionRecord *r;
            err = jazz_node__node__state__read_payload__NodeState__version_record_from_row(s, &rows.ptr[i], &r);
            if (!JZ_IS_ERR(err)) JZ_VEC_PUSH(records, r);
        }
        jz_vec_drop(&rows, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
        if (JZ_IS_ERR(err)) goto done;
    }
    jazz_types_AuthorSubject permission_subject = stored.tx.made_by;
    const jazz_types_AuthorSubject *local = jz_btree_get(&s->open_tx.local_permission_subjects, &tx_id);
    if (local) permission_subject = *local;
    else if (stored.tx.has_permission_subject) permission_subject = stored.tx.permission_subject;
    {
        jazz_model_Transaction policy_tx;
        jazz_model_Transaction_clone(&policy_tx, &stored.tx);
        policy_tx.has_permission_subject = true;
        policy_tx.permission_subject = permission_subject;
        bool rejected;
        jazz_model_RejectionReason reason;
        err = jazz_node__node__ingest__NodeState__commit_unit_write_policy_rejection(s, &policy_tx, &records, NULL, &rejected, &reason);
        jazz_model_Transaction_drop(&policy_tx);
        if (JZ_IS_ERR(err)) goto done;
        if (rejected) {
            jazz_model_Fate fate = { .tag = JAZZ_FATE_REJECTED, .reason = reason };
            jazz_model_Transaction tx = stored.tx;
            memset(&stored.tx, 0, sizeof stored.tx);
            if (!JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__ingest_rejected_transaction(s, tx, fate))) jazz_node__node__ingest__settled_empty(out);
            goto done;
        }
    }
    {
        bool rejected;
        jazz_model_RejectionReason reason;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__merge_op_rejection(s, tx_id, &records, &rejected, &reason))) goto done;
        if (rejected) {
            jazz_model_Fate fate = { .tag = JAZZ_FATE_REJECTED, .reason = reason };
            jazz_model_Transaction tx = stored.tx;
            memset(&stored.tx, 0, sizeof stored.tx);
            if (!JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__ingest_rejected_transaction(s, tx, fate))) jazz_node__node__ingest__settled_empty(out);
            goto done;
        }
    }
    if (JZ_IS_ERR(err = jzn_self_accept(s, tx_id))) goto done;
    /* Core merged the writes into each row's post-image per column; nothing further is minted. */
    jazz_node__node__ingest__settled_empty(out);
done:
    jazz_protocol_VersionRecordVec_drop(&records);
    jazz_model_Transaction_drop(&stored.tx);
    jazz_model_Fate_drop(&stored.fate);
    return err;
}

/* Finalize a locally-authored pending exclusive commit as the global
 * authority, returning the accepted or rejected fate. */
jz_err jazz_node__node__ingest__NodeState__finalize_local_exclusive_commit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
                                                                          jazz_model_Fate *fate_out, jazz_node_PublicationOutcome *out) {
    jz_err err;
    if (JZ_IS_ERR(err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s))) goto drop;
    jazz_types_TxId tx_id = tx.tx_id;
    if (tx.kind != JAZZ_TX_KIND_EXCLUSIVE) { err = JZN_UNSUPPORTED_COMMIT("exclusive self-finalize requires an exclusive transaction"); goto drop; }
    bool found;
    jazz_node_StoredTransaction stored;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, tx_id, &found, &stored))) goto drop;
    if (!found) { err = jzn_missing_transaction(tx_id); goto drop; }
    jazz_model_Transaction_drop(&stored.tx);
    if (stored.fate.tag != JAZZ_FATE_PENDING) {
        *fate_out = stored.fate;
        jazz_node__node__ingest__settled_empty(out);
        err = JZ_OK;
        goto drop;
    }
    jazz_model_Fate_drop(&stored.fate);
    /* Locally finalized exclusive commits bypass `ingest_commit_unit_once`, so
     * they must still take the common fate-policy path. */
    bool rejected;
    jazz_model_RejectionReason reason;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__commit_unit_write_policy_rejection(s, &tx, &versions, NULL, &rejected, &reason))) goto drop;
    jazz_model_Fate fate;
    if (rejected) {
        fate = (jazz_model_Fate){ .tag = JAZZ_FATE_REJECTED, .reason = reason };
        goto reject;
    }
    /* Validate through the SAME authority path the core uses for an incoming
     * exclusive commit unit (§3.7). */
    bool valid;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__validate_exclusive_commit_unit(s, &tx, &versions, &valid))) goto drop;
    if (!valid) { fate = jazz_node__node__ingest__rejected_fate(JAZZ_REJ_EXCLUSIVE_CONFLICT); goto reject; }
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__merge_op_rejection(s, tx.tx_id, &versions, &rejected, &reason))) goto drop;
    if (rejected) { fate = (jazz_model_Fate){ .tag = JAZZ_FATE_REJECTED, .reason = reason }; goto reject; }
    jazz_protocol_VersionRecordVec_drop(&versions);
    jazz_model_Transaction_drop(&tx);
    JZ_TRY(jzn_self_accept(s, tx_id));
    fate_out->tag = JAZZ_FATE_ACCEPTED;
    jazz_node__node__ingest__settled_empty(out);
    return JZ_OK;
reject:
    jazz_protocol_VersionRecordVec_drop(&versions);
    jazz_model_Fate_clone(fate_out, &fate);
    JZ_TRY(jazz_node__node__ingest__NodeState__ingest_rejected_transaction(s, tx, fate));
    jazz_node__node__ingest__settled_empty(out);
    return JZ_OK;
drop:
    jazz_model_Transaction_drop(&tx);
    jazz_protocol_VersionRecordVec_drop(&versions);
    return err;
}

/* Ingest an unfated commit unit at a Local relay without assigning fate. */
jz_err jazz_node__node__ingest__NodeState__ingest_relay_commit_unit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions) {
    return jazz_node__node__ingest__NodeState__ingest_relay_commit_unit_with_encoder_trust(s, tx, versions, false);
}

jz_err jazz_node__node__ingest__NodeState__ingest_relay_commit_unit_with_encoder_trust(NSA *s, jazz_model_Transaction tx,
                                                                                      jazz_protocol_VersionRecordVec versions, bool trusted_encoder) {
    jz_err err;
    if (JZ_IS_ERR(err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s))) goto drop;
    tx.has_permission_subject = false;
    bool malformed = false;
    if (!trusted_encoder) {
        jz_err e = jazz_protocol__protocol__validate_version_records(&versions);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); malformed = true; }
    }
    jz_str reason;
    if (!malformed && jazz_node__protocol_limits__commit_unit_limit_violation(&versions, &reason)) { jz_str_drop(&reason); malformed = true; }
    if (!malformed && !jazz_node__node__ingest__commit_unit_write_count_matches(&tx, versions.len)) malformed = true;
    if (malformed) { err = JZN_UNSUPPORTED_COMMIT("malformed relay commit unit"); goto drop; }
    if (jazz_node__node__ingest__NodeState__malformed_authored_version_reason(s, jzn_vptrs(&versions), versions.len, &reason)) {
        jz_str_drop(&reason);
        err = JZN_UNSUPPORTED_COMMIT("malformed relay commit unit");
        goto drop;
    }
    JZ_TRY(jazz_node__node__ingest__NodeState__ingest_relay_commit_unit_once(s, tx, versions));
    return jazz_node__node__ingest__NodeState__drain_parked_relay_commit_units(s);
drop:
    jazz_model_Transaction_drop(&tx);
    jazz_protocol_VersionRecordVec_drop(&versions);
    return err;
}

/* Check whether an untrusted retransmit is already stored with the same durable payload and version set. */
jz_err jazz_node__node__ingest__NodeState__known_untrusted_commit_unit_matches(NSA *s, const jazz_model_Transaction *tx,
                                                                              const jazz_protocol_VersionRecordVec *versions, bool *out) {
    bool found;
    jazz_node_StoredTransaction existing;
    JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(s, tx->tx_id, &found, &existing));
    if (!found) { *out = false; return JZ_OK; }
    jazz_protocol_VersionRecordVec existing_versions;
    jz_err err = jzn_existing_versions_sorted(s, tx->tx_id, &existing_versions);
    if (!JZ_IS_ERR(err)) {
        jazz_protocol_VersionRecordVec canonical;
        jazz_protocol_VersionRecordVec_clone(&canonical, versions);
        jazz_node__node__codec__canonical_versions(&canonical);
        *out = jazz_node__node__codec__known_transaction_payload_matches_redacted_permission_subject(&existing.tx, tx) &&
               jzn_version_vecs_eq(&existing_versions, &canonical);
        jazz_protocol_VersionRecordVec_drop(&canonical);
        jazz_protocol_VersionRecordVec_drop(&existing_versions);
    }
    jazz_model_Transaction_drop(&existing.tx);
    jazz_model_Fate_drop(&existing.fate);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__ingest_relay_commit_unit_once(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions) {
    jz_err err = JZ_OK;
    if (tx.kind != JAZZ_TX_KIND_MERGEABLE && tx.kind != JAZZ_TX_KIND_EXCLUSIVE) { err = JZN_UNSUPPORTED_COMMIT("unsupported commit unit kind"); goto drop; }
    jazz_node__node__codec__canonical_versions(&versions);
    bool found;
    jazz_node_StoredTransaction existing;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, tx.tx_id, &found, &existing))) goto drop;
    if (found) {
        jazz_protocol_VersionRecordVec existing_versions;
        err = jzn_existing_versions_sorted(s, tx.tx_id, &existing_versions);
        if (!JZ_IS_ERR(err)) {
            /* Once Core has sequenced the unit, this relay stores Core's
             * post-images rather than the authored patch, so a resend is
             * matched by the rows it addresses. */
            bool same_payload = existing.fate.tag == JAZZ_FATE_ACCEPTED && existing.has_global_time
                                    ? jazz_node__node__ingest__same_addressed_rows(&existing_versions, &versions)
                                    : jzn_version_vecs_eq(&existing_versions, &versions);
            if (!jazz_node__node__codec__known_transaction_payload_matches_redacted_permission_subject(&existing.tx, &tx) || !same_payload)
                err = jzn_conflicting_commit_unit(tx.tx_id);
            jazz_protocol_VersionRecordVec_drop(&existing_versions);
        }
        jazz_model_Transaction_drop(&existing.tx);
        jazz_model_Fate_drop(&existing.fate);
        goto drop;
    }
    if (!jazz_node__node__ingest__commit_unit_write_count_matches(&tx, versions.len)) {
        err = JZN_UNSUPPORTED_COMMIT("commit unit version count does not match transaction n_total_writes");
        goto drop;
    }
    jazz_node_CommitUnitParkMode relay_mode = jazz_node__node__ingest__CommitUnitParkMode__default();
    relay_mode.ingress_role = JZN_PARKED_RELAY;
    bool parked;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__park_commit_unit_if_missing_schema_versions_with_mode(
                      s, &tx, &versions, UINT64_MAX - JZN_SKEW_TOLERANCE_MS, relay_mode, &parked))) goto drop;
    if (parked) goto drop;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__prepare_authored_schema_variants_for_commit(s, jzn_vptrs(&versions), versions.len))) goto drop;
    jazz_model_Fate pending = { .tag = JAZZ_FATE_PENDING };
    return jazz_node__node__ingest__NodeState__ingest_transaction_and_versions(s, tx, versions, pending, false, 0, JAZZ_DURABILITY_LOCAL);
drop:
    jazz_model_Transaction_drop(&tx);
    jazz_protocol_VersionRecordVec_drop(&versions);
    return err;
}

static jz_err jzn_decide_commit_unit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, u64 now_ms,
                                     const jazz_node_CommitUnitIngestContext *ctx, jazz_node_PublicationOutcome *out);

/* One admission attempt of a unit at the authority: its decision, and, when it
 * is refused, the superseded-writes markers the refusal leaves. */
jz_err jazz_node__node__ingest__NodeState__ingest_commit_unit_once(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
                                                                  u64 now_ms, const jazz_node_CommitUnitIngestContext *ctx,
                                                                  jazz_node_PublicationOutcome *out) {
    jazz_types_TxId tx_id = tx.tx_id;
    jzn_SupersededPredecessorVec predecessors = {0};
    for (u32 i = 0; i < versions.len; i++) {
        const jazz_protocol_VersionRecord *version = versions.ptr[i];
        jazz_protocol_RowBase base = jazz_protocol__protocol__VersionRecord__base(version);
        if (!base.has_pending) continue;
        jzn_SupersededPredecessor p;
        p.schema_version = jazz_protocol__protocol__VersionRecord__schema_version(version);
        p.table = jz_str_clone(jazz_protocol__protocol__VersionRecord__table(version));
        jazz_model_BranchKey_clone(&p.branch_key, jazz_protocol__protocol__VersionRecord__branch_key(version));
        p.row_uuid = jazz_protocol__protocol__VersionRecord__row_uuid(version);
        p.pending = base.pending;
        JZ_VEC_PUSH(predecessors, p);
    }
    jazz_node_PublicationOutcome outcome;
    jz_err err = jzn_decide_commit_unit(s, tx, versions, now_ms, ctx, &outcome);
    if (JZ_IS_ERR(err)) { jz_vec_drop(&predecessors, sizeof(jzn_SupersededPredecessor), NULL); return err; }
    bool refused = false;
    for (u32 i = 0; i < outcome.value.len; i++) {
        const jazz_protocol_SyncMessage *m = &outcome.value.ptr[i];
        if (m->tag == JZP_SM_FATE_UPDATE && m->v.fate_update.fate.tag == JAZZ_FATE_REJECTED &&
            m->v.fate_update.tx_id.time == tx_id.time && jz_uuid_eq(&m->v.fate_update.tx_id.node, &tx_id.node)) { refused = true; break; }
    }
    if (predecessors.len != 0 && refused) {
        err = jazz_node__node__ingest__NodeState__record_superseded_by_refusal(s, tx_id, predecessors);
        if (JZ_IS_ERR(err)) { jazz_node_PublicationOutcome_drop(&outcome); return err; }
    } else {
        jz_vec_drop(&predecessors, sizeof(jzn_SupersededPredecessor), NULL);
    }
    *out = outcome;
    return JZ_OK;
}

static bool jzn_payload_matches(bool redact, const jazz_model_Transaction *existing, const jazz_model_Transaction *tx) {
    return redact ? jazz_node__node__codec__known_transaction_payload_matches_redacted_permission_subject(existing, tx)
                  : jazz_node__node__codec__known_transaction_payload_matches(existing, tx);
}

static void jzn_echo_existing_fate(const jazz_node_StoredTransaction *existing, jazz_types_TxId tx_id, jazz_node_PublicationOutcome *out) {
    jazz_model_Fate fate;
    jazz_model_Fate_clone(&fate, &existing->fate);
    u32 durability = 0;
    bool has_durability = jazz_node__node__ingest__fate_update_durability_claim(&existing->fate, existing->durability, &durability);
    jazz_protocol_SyncMessage m;
    jazz_node__node__ingest__fate_update_message(tx_id, fate, existing->has_global_time, existing->global_time, has_durability, durability, &m);
    jazz_node__node__ingest__settled_one(out, m);
}

static jz_err jzn_decide_commit_unit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, u64 now_ms,
                                     const jazz_node_CommitUnitIngestContext *ctx, jazz_node_PublicationOutcome *out) {
    jz_err err = JZ_OK;
    bool redact_permission_subject = ctx && (ctx->trust == JZN_TRUST_SESSION || ctx->trust == JZN_TRUST_RELAY);
    jazz_node__node__codec__canonical_versions(&versions);
    if (!jazz_node__node__ingest__commit_unit_write_count_matches(&tx, versions.len)) {
        jazz_protocol_VersionRecordVec_drop(&versions);
        return jzn_reject_with_fate(s, tx, jazz_node__node__ingest__malformed_commit_fate(
                                               jz_str_from("commit unit version count does not match transaction n_total_writes")), out);
    }
    jz_str reason;
    if (jazz_node__node__ingest__NodeState__malformed_authored_version_reason(s, jzn_vptrs(&versions), versions.len, &reason)) {
        jazz_protocol_VersionRecordVec_drop(&versions);
        return jzn_reject_malformed_settled(s, tx, reason, out);
    }
    bool found;
    jazz_node_StoredTransaction existing;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, tx.tx_id, &found, &existing))) goto drop;
    if (found) {
        bool done = false;
        if (tx.kind == JAZZ_TX_KIND_EXCLUSIVE || existing.fate.tag == JAZZ_FATE_REJECTED) {
            if (!jzn_payload_matches(redact_permission_subject, &existing.tx, &tx)) err = jzn_conflicting_commit_unit(tx.tx_id);
            else jzn_echo_existing_fate(&existing, tx.tx_id, out);
            done = true;
        } else {
            jazz_protocol_VersionRecordVec existing_versions;
            err = jzn_existing_versions_sorted(s, tx.tx_id, &existing_versions);
            if (!JZ_IS_ERR(err)) {
                bool matches = jzn_payload_matches(redact_permission_subject, &existing.tx, &tx);
                /* Once Core has accepted a unit with a seq, its stored versions
                 * are Core's post-images; tx identity alone then decides. */
                bool stored_post_images = existing.fate.tag == JAZZ_FATE_ACCEPTED && existing.has_global_time;
                bool same_payload = stored_post_images ? jazz_node__node__ingest__same_addressed_rows(&existing_versions, &versions)
                                                       : jzn_version_vecs_eq(&existing_versions, &versions);
                jazz_protocol_VersionRecordVec_drop(&existing_versions);
                if (!matches || !same_payload) { err = jzn_conflicting_commit_unit(tx.tx_id); done = true; }
                else if (tx.kind == JAZZ_TX_KIND_MERGEABLE && existing.fate.tag == JAZZ_FATE_PENDING) {
                    /* A stored Pending mergeable unit has no authority fate yet;
                     * a retransmission re-enters authority validation below. */
                } else {
                    jzn_echo_existing_fate(&existing, tx.tx_id, out);
                    done = true;
                }
            } else {
                done = true;
            }
        }
        jazz_model_Transaction_drop(&existing.tx);
        jazz_model_Fate_drop(&existing.fate);
        if (done) goto drop;
    }
    jazz_node_CommitUnitParkMode mode = jazz_node__node__ingest__CommitUnitParkMode__default();
    if (ctx) { mode.has_ingest_context = true; mode.ingest_context = *ctx; }
    bool parked;
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__park_commit_unit_if_missing_schema_versions_with_mode(s, &tx, &versions, now_ms, mode, &parked))) goto drop;
    if (parked) { jazz_node__node__ingest__settled_empty(out); goto drop; }
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__prepare_authored_schema_variants_for_commit(s, jzn_vptrs(&versions), versions.len))) goto drop;
    /* Validate untrusted metadata before a missing ordinary history parent can park the unit. */
    {
        jz_err e = jazz_node__node__state__contribution_merge__NodeState__validate_contribution_merge_operation_identities(s, &tx);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            jazz_protocol_VersionRecordVec_drop(&versions);
            return jzn_reject_malformed_settled(s, tx, jz_str_from("invalid contribution provenance"), out);
        }
    }
    u64 limit = now_ms + JZN_SKEW_TOLERANCE_MS < now_ms ? UINT64_MAX : now_ms + JZN_SKEW_TOLERANCE_MS;
    if (jazz_types__time__TxTime__physical_ms(tx.tx_id.time) > limit) {
        jazz_protocol_VersionRecordVec_drop(&versions);
        return jzn_reject_with_fate(s, tx, jazz_node__node__ingest__rejected_fate(JAZZ_REJ_CLIENT_CLOCK_TOO_FAR_AHEAD), out);
    }
    {
        bool some;
        jazz_types_AuthorSubject subject;
        jazz_model_RejectionReason r;
        if (!jazz_node__node__ingest__commit_unit_admission_subject(&tx, ctx, &some, &subject, &r)) {
            jazz_protocol_VersionRecordVec_drop(&versions);
            jazz_model_Fate fate = { .tag = JAZZ_FATE_REJECTED, .reason = r };
            return jzn_reject_with_fate(s, tx, fate, out);
        }
    }
    /* A unit of an unknown kind is refused before anything else is looked up for it. */
    if (tx.kind != JAZZ_TX_KIND_MERGEABLE && tx.kind != JAZZ_TX_KIND_EXCLUSIVE) { err = JZN_UNSUPPORTED_COMMIT("unsupported commit unit kind"); goto drop; }
    {
        bool rejected;
        jazz_model_RejectionReason r;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__commit_unit_write_policy_rejection(s, &tx, &versions, ctx, &rejected, &r))) goto drop;
        if (rejected) {
            jazz_protocol_VersionRecordVec_drop(&versions);
            jazz_model_Fate fate = { .tag = JAZZ_FATE_REJECTED, .reason = r };
            return jzn_reject_with_fate(s, tx, fate, out);
        }
    }
    if (tx.kind == JAZZ_TX_KIND_EXCLUSIVE) {
        bool valid;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__validate_exclusive_commit_unit(s, &tx, &versions, &valid))) goto drop;
        if (!valid) {
            jazz_protocol_VersionRecordVec_drop(&versions);
            return jzn_reject_with_fate(s, tx, jazz_node__node__ingest__rejected_fate(JAZZ_REJ_EXCLUSIVE_CONFLICT), out);
        }
    }
    {
        bool rejected;
        jazz_model_RejectionReason r;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__merge_op_rejection(s, tx.tx_id, &versions, &rejected, &r))) goto drop;
        if (rejected) {
            jazz_protocol_VersionRecordVec_drop(&versions);
            jazz_model_Fate fate = { .tag = JAZZ_FATE_REJECTED, .reason = r };
            return jzn_reject_with_fate(s, tx, fate, out);
        }
    }
    jazz_types_GlobalTime authority_now_ms = jazz_types__time__GlobalTime__authority_now_ms(now_ms, jazz_types__time__TxTime__physical_ms(tx.tx_id.time));
    jazz_types_GlobalTime global_time;
    if (JZ_IS_ERR(err = jazz_node__node__Clock__allocate_global_time(&s->clock, authority_now_ms, &global_time))) goto drop;
    jazz_types_TxId tx_id = tx.tx_id;
    /* Keep persistence out of the policy admission frame. This node mints the
     * seq, so it merges the patch into the row's post-image. */
    s->minting_global_time = true;
    jazz_model_Fate accepted = { .tag = JAZZ_FATE_ACCEPTED };
    jz_err ingested = jazz_node__node__ingest__NodeState__ingest_known_transaction(s, tx, versions, accepted, true, global_time, JAZZ_DURABILITY_GLOBAL);
    s->minting_global_time = false;
    JZ_TRY(ingested);
    jazz_protocol_SyncMessage m;
    jazz_node__node__ingest__fate_update_message(tx_id, accepted, true, global_time, true, JAZZ_DURABILITY_GLOBAL, &m);
    jazz_node__node__ingest__settled_one(out, m);
    return JZ_OK;
drop:
    jazz_model_Transaction_drop(&tx);
    jazz_protocol_VersionRecordVec_drop(&versions);
    return err;
}

static bool jzn_fate_is(const jazz_model_Fate *f, u32 tag) { return f->tag == tag; }

jz_err jazz_node__node__ingest__NodeState__ingest_known_transaction(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
                                                                   jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability) {
    jz_err err;
    if (JZ_IS_ERR(err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s))) goto drop;
    jazz_node__node__state__read_payload__NodeState__merge_tx_time(s, tx.tx_id.time);
    jazz_node__node__codec__canonical_versions(&versions);
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__prepare_authored_schema_variants_for_commit(s, jzn_vptrs(&versions), versions.len))) goto drop;
    bool found;
    jazz_node_StoredTransaction existing;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, tx.tx_id, &found, &existing))) goto drop;
    if (!found) return jazz_node__node__ingest__NodeState__ingest_transaction_and_versions(s, tx, versions, fate, has_gt, gt, durability);

    if (!(jazz_node__node__codec__known_transaction_payload_matches_redacted_permission_subject(&existing.tx, &tx) ||
          (existing.is_partial && jazz_node__node__codec__known_transaction_payload_matches_redacted_cardinality(&existing.tx, &tx)))) {
        err = jzn_conflicting_commit_unit(tx.tx_id);
        goto drop_existing;
    }
    jazz_node_StoredTransaction reconciled = existing;
    jazz_model_Fate fate_copy;
    jazz_model_Fate_clone(&fate_copy, &fate);
    if (JZ_IS_ERR(err = jazz_node__node__codec__StoredTransaction__reconcile_fate(&reconciled, fate_copy, has_gt, gt, true, durability))) goto drop_existing;
    if (jzn_fate_is(&reconciled.fate, JAZZ_FATE_REJECTED)) {
        jazz_model_Transaction_drop(&existing.tx);
        jazz_model_Fate_drop(&reconciled.fate);
        return jazz_node__node__ingest__NodeState__ingest_transaction_and_versions(s, tx, versions, fate, has_gt, gt, durability);
    }
    /* Normalize aliases before establishing the batch's resident base. */
    {
        jz_btree schemas;
        jz_btree_init(&schemas, &jzn_desc_uuid_set_a);
        for (u32 i = 0; i < versions.len; i++) {
            jz_uuid v = jazz_protocol__protocol__VersionRecord__schema_version(versions.ptr[i]);
            jz_btree_insert(&schemas, &v, NULL, NULL);
        }
        jz_uuid *k; void *unused;
        JZ_BTREE_FOR(&schemas, k, unused) {
            jazz_types_SchemaVersionAlias alias;
            if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(s, *k, &alias))) break;
        }
        jz_btree_drop(&schemas);
        if (JZ_IS_ERR(err)) goto drop_reconciled;
    }
    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    jazz_protocol_VersionRecordVec version_bundles = {0};
    jazz_node_VersionRowVec previously_stored = {0};
    /* A write is stored at its seq once accepted, at 0 while pending (SPEC 2 §2.7.1). */
    jazz_types_GlobalTime seq = (reconciled.fate.tag == JAZZ_FATE_ACCEPTED && reconciled.has_global_time && !s->minting_global_time)
                                    ? reconciled.global_time : 0;
    for (u32 i = 0; i < versions.len; i++) {
        jazz_protocol_VersionRecord *version = versions.ptr[i];
        jazz_node_VersionRow stored;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__prepare_exact_history_version(s, existing.node_alias, tx.tx_id.time, seq, version, &stored))) break;
        groove_InternStr table;
        groove_ValidatedVariantRecord record;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_write_binding(s, &stored, existing.tx.made_by, &table, &record))) {
            jazz_node_VersionRow_drop(&stored);
            break;
        }
        jazz_node_PrimaryKeyValue key;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, &stored, &key))) { jazz_node_VersionRow_drop(&stored); break; }
        if (has_gt && fate.tag == JAZZ_FATE_ACCEPTED && !s->minting_global_time) {
            /* The authority's post-image replaces this node's copy, which moves
             * from its pending key to its seq. */
            if (seq != 0) {
                jazz_node_VersionRow pending_row;
                jazz_node_PrimaryKeyValue pending_key;
                if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__with_seq(&stored, 0, &pending_row))) { jazz_node_VersionRow_drop(&stored); break; }
                err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, &pending_row, &pending_key);
                jazz_node_VersionRow_drop(&pending_row);
                if (JZ_IS_ERR(err)) { jazz_node_VersionRow_drop(&stored); break; }
                groove__db__DatabaseBatch__delete(batch, table, pending_key);
            }
            groove__db__DatabaseBatch__update_raw(batch, table, key, record);
            JZ_VEC_PUSH(version_bundles, version);
            versions.ptr[i] = NULL;
            jazz_node_VersionRow_drop(&stored);
            continue;
        }
        u32 outcome;
        if (JZ_IS_ERR(err = groove__db__DatabaseBatch__ensure_exact(batch, jzn_db(s), table, key, record, &outcome))) { jazz_node_VersionRow_drop(&stored); break; }
        if (outcome == GROOVE_ENSURE_INSERTED) {
            JZ_VEC_PUSH(version_bundles, version);
            versions.ptr[i] = NULL;
            jazz_node_VersionRow_drop(&stored);
        } else if (outcome == GROOVE_ENSURE_ALREADY_IDENTICAL) {
            JZ_VEC_PUSH(previously_stored, stored);
        } else {
            jazz_node_VersionRow_drop(&stored);
            err = jzn_conflicting_commit_unit(tx.tx_id);
            break;
        }
    }
    if (JZ_IS_ERR(err)) {
        groove__db__DatabaseBatch__drop(batch);
        jazz_protocol_VersionRecordVec_drop(&version_bundles);
        jz_vec_drop(&previously_stored, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
        goto drop_reconciled;
    }
    jazz_protocol_VersionRecordVec_drop(&versions);
    bool existing_partial = existing.is_partial;
    jazz_model_Transaction_drop(&existing.tx);
    jazz_model_Fate_drop(&reconciled.fate);
    if (version_bundles.len == 0 && !existing_partial) {
        groove__db__DatabaseBatch__drop(batch);
        jz_vec_drop(&version_bundles, sizeof(void *), NULL);
        jz_vec_drop(&previously_stored, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
        jazz_types_TxId tx_id = tx.tx_id;
        jazz_model_Transaction_drop(&tx);
        return jazz_node__node__ingest__NodeState__apply_fate_update(s, tx_id, fate, has_gt, gt, true, durability);
    }
    return jazz_node__node__ingest__NodeState__ingest_transaction_and_versions_with_current_indexes_in_batch(
        s, batch, tx, version_bundles, previously_stored, fate, has_gt, gt, durability, true, false);
drop_reconciled:
    jazz_model_Fate_drop(&reconciled.fate);
    jazz_model_Transaction_drop(&existing.tx);
    goto drop;
drop_existing:
    jazz_model_Transaction_drop(&existing.tx);
    jazz_model_Fate_drop(&existing.fate);
drop:
    jazz_model_Transaction_drop(&tx);
    jazz_protocol_VersionRecordVec_drop(&versions);
    jazz_model_Fate_drop(&fate);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__stage_known_transaction(NSA *s, groove_DatabaseBatch *batch, jazz_model_Transaction tx,
                                                                  jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt,
                                                                  jazz_types_GlobalTime gt, u32 durability,
                                                                  jazz_node_GlobalTimeTxVec *staged_global_times,
                                                                  jazz_node_VersionRowVec *staged_content_versions,
                                                                  jazz_node_RejectedTransactionVec *staged_rejections) {
    jz_err err;
    jazz_node__node__codec__canonical_versions(&versions);
    /* Entered by the batched ViewUpdate path, which has no authority role and
     * therefore cannot synthesize a fate. This is the central storage-ingress
     * backstop for other direct callers. */
    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__validate_view_payload_versions(s, &versions))) goto drop;
    jazz_node__node__state__read_payload__NodeState__merge_tx_time(s, tx.tx_id.time);
    jazz_types_TxId tx_id = tx.tx_id;
    bool found;
    jazz_node_StoredTransaction existing;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, tx_id, &found, &existing))) goto drop;
    if (found) {
        jazz_model_Transaction_drop(&existing.tx);
        jazz_model_Fate_drop(&existing.fate);
        return jazz_node__node__ingest__NodeState__ingest_known_transaction(s, tx, versions, fate, has_gt, gt, durability);
    }
    jazz_node_StagedTransaction staged;
    JZ_TRY(jazz_node__node__ingest__NodeState__stage_transaction_and_versions_with_current_indexes(
        s, batch, tx, versions, fate, has_gt, gt, durability, true, false, staged_content_versions, &staged));
    if (staged.rejected) JZ_VEC_PUSH(*staged_rejections, staged.rejected);
    err = jazz_node__node__ingest__NodeState__finalize_staged_transaction_ingest(s, batch, tx_id, &staged.fate, staged.has_global_time,
                                                                                staged.global_time, staged_global_times, &staged.versions);
    jz_vec_drop(&staged.versions, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
    jazz_model_Fate_drop(&staged.fate);
    return err;
drop:
    jazz_model_Transaction_drop(&tx);
    jazz_protocol_VersionRecordVec_drop(&versions);
    jazz_model_Fate_drop(&fate);
    return err;
}

/* ---- reset-bundle bulk ingestion ---- */

/* (String, BranchKey, RowUuid, SchemaVersionId) -> &VersionRecord */
typedef struct jzn_UniqueKey { jz_str table; jazz_model_BranchKey branch_key; jazz_types_RowUuid row_uuid; jazz_types_SchemaVersionId schema; } jzn_UniqueKey;
static int jzn_unique_key_cmp(const void *a, const void *b, void *ctx) {
    const jzn_UniqueKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    if (c) return c;
    if ((c = jazz_model_BranchKey_cmp(&x->branch_key, &y->branch_key, ctx))) return c;
    if ((c = jz_uuid_cmp(&x->row_uuid, &y->row_uuid))) return c;
    return jz_uuid_cmp(&x->schema, &y->schema);
}
static void jzn_unique_key_drop(void *k) { jzn_UniqueKey *x = k; jz_str_drop(&x->table); jazz_model_BranchKey_drop(&x->branch_key); }
static const jz_btree_desc jzn_desc_unique_versions = { sizeof(jzn_UniqueKey), sizeof(const jazz_protocol_VersionRecord *), jzn_unique_key_cmp, jzn_unique_key_drop, NULL };

static void jzn_unique_key_of(const jazz_protocol_VersionRecord *v, jzn_UniqueKey *k) {
    k->table = jz_str_clone(jazz_protocol__protocol__VersionRecord__table(v));
    jazz_model_BranchKey_clone(&k->branch_key, jazz_protocol__protocol__VersionRecord__branch_key(v));
    k->row_uuid = jazz_protocol__protocol__VersionRecord__row_uuid(v);
    k->schema = jazz_protocol__protocol__VersionRecord__schema_version(v);
}

/* TxId -> Vec<VersionBundleRef> */
typedef JZ_VEC(jazz_protocol_VersionBundleRef) jzn_BundleRefVec;
static void jzn_bundle_vec_drop(void *v) { jz_vec_drop(v, sizeof(jazz_protocol_VersionBundleRef), NULL); }
static const jz_btree_desc jzn_desc_txid_bundles = { sizeof(jazz_types_TxId), sizeof(jzn_BundleRefVec), jazz_types__ids__TxId__cmp, NULL, jzn_bundle_vec_drop };

typedef struct jzn_EligibleTx { jzn_BundleRefVec bundles; jazz_model_Transaction local_tx; bool view_scoped; } jzn_EligibleTx;

/* (String, BranchKey, RowUuid) -> (VersionRow, GlobalTime) */
typedef struct jzn_CurrentUpdate { jazz_node_VersionRow stored; jazz_types_GlobalTime global_time; } jzn_CurrentUpdate;
static void jzn_current_update_drop(void *v) { jazz_node_VersionRow_drop(&((jzn_CurrentUpdate *)v)->stored); }
static const jz_btree_desc jzn_desc_current_updates = { sizeof(jazz_node_ViewVersionKey), sizeof(jzn_CurrentUpdate), jazz_node_ViewVersionKey_cmp,
                                                        jazz_node_ViewVersionKey_drop, jzn_current_update_drop };
static int jzn_cmp_str_v(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(a, b); }
static void jzn_str_drop_vv(void *p) { jz_str_drop(p); }
static const jz_btree_desc jzn_desc_str_bool = { sizeof(jz_str), sizeof(bool), jzn_cmp_str_v, jzn_str_drop_vv, NULL };

static bool jzn_bundle_identity_matches(const jazz_protocol_VersionBundleRef *first, const jazz_protocol_VersionBundleRef *bundle,
                                        const jazz_model_Transaction *first_identity) {
    jazz_model_Transaction identity;
    jazz_node__node__codec__transaction_without_permission_subject(bundle->tx, &identity);
    identity.n_total_writes = 0;
    bool same = jazz_model_Transaction_eq(&identity, first_identity);
    jazz_model_Transaction_drop(&identity);
    bool first_scoped = first->scope == JZP_BUNDLE_SCOPE_VIEW_SCOPED, scoped = bundle->scope == JZP_BUNDLE_SCOPE_VIEW_SCOPED;
    bool fate_eq = bundle->fate->tag == first->fate->tag &&
                   (bundle->fate->tag != JAZZ_FATE_REJECTED || jazz_model_RejectionReason_eq(&bundle->fate->reason, &first->fate->reason));
    return same && scoped == first_scoped && fate_eq && bundle->has_global_time == first->has_global_time &&
           (!bundle->has_global_time || bundle->global_time == first->global_time) && bundle->durability == first->durability;
}

jz_err jazz_node__node__ingest__NodeState__ingest_reset_view_bundle_refs_in_bulk(NSA *s, const jazz_protocol_VersionBundleRef *bundles, u32 n,
                                                                                const jz_btree *preflight_persisted_tx_ids, jz_btree *out) {
    jz_err err = JZ_OK;
    jz_btree bundles_by_tx;
    jz_btree_init(&bundles_by_tx, &jzn_desc_txid_bundles);
    for (u32 i = 0; i < n; i++) {
        /* This helper is also called directly by reset fast paths; do not rely
         * on their outer ViewUpdate preflight for durable admission. */
        groove_Value admitted;
        if (JZ_IS_ERR(err = jazz_node__node__state__contribution_merge__NodeState__admit_contribution_merge_for_storage(s, bundles[i].tx, &admitted))) goto fail_map;
        groove_Value_drop(&admitted);
        if (JZ_IS_ERR(err = jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bundles[i].has_global_time, bundles[i].global_time,
                                                                                                         bundles[i].durability))) goto fail_map;
        bool inserted;
        jzn_BundleRefVec *entry = jz_btree_entry(&bundles_by_tx, &bundles[i].tx->tx_id, &inserted);
        JZ_VEC_PUSH(*entry, bundles[i]);
    }
    JZ_VEC(jzn_EligibleTx) eligible = {0};
    jz_btree loaded_tx_ids;
    jz_btree_init(&loaded_tx_ids, &jzn_desc_txid_set_a);
    {
        jazz_types_TxId *tx_id; jzn_BundleRefVec *tx_bundles;
        JZ_BTREE_FOR(&bundles_by_tx, tx_id, tx_bundles) {
            const jazz_protocol_VersionBundleRef *first = &tx_bundles->ptr[0];
            bool view_scoped = first->scope == JZP_BUNDLE_SCOPE_VIEW_SCOPED;
            /* Each view-scoped bundle declares only its own redacted fragment
             * cardinality. Compare the immutable transaction identity here. */
            jazz_model_Transaction first_identity;
            jazz_node__node__codec__transaction_without_permission_subject(first->tx, &first_identity);
            first_identity.n_total_writes = 0;
            bool mismatch = false;
            for (u32 i = 0; i < tx_bundles->len && !mismatch; i++)
                mismatch = !jzn_bundle_identity_matches(first, &tx_bundles->ptr[i], &first_identity);
            jazz_model_Transaction_drop(&first_identity);
            if (mismatch) continue;
            if (first->fate->tag != JAZZ_FATE_ACCEPTED) continue;
            if (!first->has_global_time) continue;
            if (first->tx->kind != JAZZ_TX_KIND_MERGEABLE && first->tx->kind != JAZZ_TX_KIND_EXCLUSIVE) continue;
            jz_btree unique_versions;
            jz_btree_init(&unique_versions, &jzn_desc_unique_versions);
            for (u32 b = 0; b < tx_bundles->len && !JZ_IS_ERR(err); b++) {
                const jazz_protocol_VersionRecordVec *vs = tx_bundles->ptr[b].versions;
                for (u32 i = 0; i < vs->len; i++) {
                    jzn_UniqueKey key;
                    jzn_unique_key_of(vs->ptr[i], &key);
                    const jazz_protocol_VersionRecord **existing = jz_btree_get(&unique_versions, &key);
                    if (existing) {
                        bool differs = !jazz_protocol_VersionRecord_eq(*existing, vs->ptr[i]);
                        jzn_unique_key_drop(&key);
                        if (differs) { err = jzn_conflicting_commit_unit(*tx_id); break; }
                    } else {
                        const jazz_protocol_VersionRecord *v = vs->ptr[i];
                        jz_btree_insert(&unique_versions, &key, &v, NULL);
                    }
                }
            }
            usize version_count = unique_versions.len;
            jz_btree_drop(&unique_versions);
            if (JZ_IS_ERR(err)) goto fail_eligible;
            if (first->tx->kind == JAZZ_TX_KIND_EXCLUSIVE && !view_scoped && (usize)first->tx->n_total_writes != version_count) continue;
            bool known = false;
            if (preflight_persisted_tx_ids) {
                known = jz_btree_contains(preflight_persisted_tx_ids, tx_id);
            } else {
                bool found;
                jazz_node_StoredTransaction existing;
                if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, *tx_id, &found, &existing))) goto fail_eligible;
                if (found) { known = true; jazz_model_Transaction_drop(&existing.tx); jazz_model_Fate_drop(&existing.fate); }
            }
            if (known) continue;
            if (!jz_btree_insert(&loaded_tx_ids, tx_id, NULL, NULL)) {
                jzn_EligibleTx e;
                jz_vec_clone(&e.bundles, tx_bundles, sizeof(jazz_protocol_VersionBundleRef), NULL);
                jazz_node__node__codec__transaction_without_permission_subject(first->tx, &e.local_tx);
                if (view_scoped) {
                    if (version_count > UINT32_MAX) {
                        jzn_bundle_vec_drop(&e.bundles);
                        jazz_model_Transaction_drop(&e.local_tx);
                        err = JZN_ISV("view payload is too large");
                        goto fail_eligible;
                    }
                    e.local_tx.n_total_writes = (u32)version_count;
                }
                e.view_scoped = view_scoped;
                JZ_VEC_PUSH(eligible, e);
            }
        }
    }
    jz_btree_drop(&bundles_by_tx);
    if (eligible.len == 0) { *out = loaded_tx_ids; jz_vec_drop(&eligible, sizeof(jzn_EligibleTx), NULL); return JZ_OK; }
    {
        JZ_VEC(const jazz_protocol_VersionRecord *) eligible_versions = {0};
        for (u32 e = 0; e < eligible.len; e++)
            for (u32 b = 0; b < eligible.ptr[e].bundles.len; b++) {
                const jazz_protocol_VersionRecordVec *vs = eligible.ptr[e].bundles.ptr[b].versions;
                for (u32 i = 0; i < vs->len; i++) JZ_VEC_PUSH(eligible_versions, (const jazz_protocol_VersionRecord *)vs->ptr[i]);
            }
        err = jazz_node__node__ingest__NodeState__prepare_authored_schema_variants_for_commit(s, eligible_versions.ptr, eligible_versions.len);
        jz_vec_drop(&eligible_versions, sizeof(void *), NULL);
        if (JZ_IS_ERR(err)) goto fail_loaded;
    }

    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    s->sync_metrics.receiver_bulk_ingest_commits += 1;
    s->sync_metrics.receiver_bulk_bundle_ingests += eligible.len;
    usize version_count = 0;
    for (u32 e = 0; e < eligible.len; e++)
        for (u32 b = 0; b < eligible.ptr[e].bundles.len; b++) version_count += eligible.ptr[e].bundles.ptr[b].versions->len;
    groove__db__DatabaseBatch__reserve(batch, eligible.len + version_count * 2);
    jz_btree current_updates;
    jz_btree_init(&current_updates, &jzn_desc_current_updates);
    jazz_node_GlobalTimeTxVec applied_global_times = {0};
    jz_vec_reserve(&applied_global_times, eligible.len, sizeof(jazz_node_GlobalTimeTx));

    for (u32 e = 0; e < eligible.len && !JZ_IS_ERR(err); e++) {
        const jazz_protocol_VersionBundleRef *first = &eligible.ptr[e].bundles.ptr[0];
        const jazz_model_Transaction *tx = &eligible.ptr[e].local_tx;
        /* A reset is a clock observation just like incremental ingestion. */
        jazz_node__node__state__read_payload__NodeState__merge_tx_time(s, tx->tx_id.time);
        jazz_types_NodeAlias tx_node_alias;
        if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_node_alias(s, tx->tx_id.node, &tx_node_alias))) break;
        jazz_types_GlobalTime global_time = first->global_time; /* checked above */
        jazz_node_GlobalTimeTx gtx = { global_time, tx->tx_id };
        JZ_VEC_PUSH(applied_global_times, gtx);
        groove_Value contribution_merge;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__contribution_merge_storage_value(s, tx->contribution_merge, &contribution_merge))) break;
        /* A reset may bulk-load only the view-authorized rows of an exclusive
         * transaction. Preserve that scope marker. */
        groove_ValueVec tx_values;
        if (JZ_IS_ERR(err = jazz_node__node__codec__transaction_values_with_partial_flag(tx_node_alias, tx, first->fate, first->has_global_time,
                                                                                        first->global_time, first->durability,
                                                                                        eligible.ptr[e].view_scoped, contribution_merge, &tx_values))) break;
        groove__db__DatabaseBatch__insert(batch, "jazz_transactions", tx_values);

        jz_btree unique_versions;
        jz_btree_init(&unique_versions, &jzn_desc_unique_versions);
        for (u32 b = 0; b < eligible.ptr[e].bundles.len; b++) {
            const jazz_protocol_VersionRecordVec *vs = eligible.ptr[e].bundles.ptr[b].versions;
            for (u32 i = 0; i < vs->len; i++) {
                jzn_UniqueKey key;
                jzn_unique_key_of(vs->ptr[i], &key);
                bool inserted;
                const jazz_protocol_VersionRecord **slot = jz_btree_entry(&unique_versions, &key, &inserted);
                if (inserted) *slot = vs->ptr[i];
                else jzn_unique_key_drop(&key);
            }
        }
        JZ_VEC(const jazz_protocol_VersionRecord *) versions = {0};
        jzn_UniqueKey *uk; const jazz_protocol_VersionRecord **uv;
        JZ_BTREE_FOR(&unique_versions, uk, uv) JZ_VEC_PUSH(versions, *uv);
        jz_btree_drop(&unique_versions);
        jz_sort(versions.ptr, versions.len, sizeof(void *), jazz_protocol_VersionRecord_cmp, NULL);
        for (u32 i = 0; i < versions.len && !JZ_IS_ERR(err); i++) {
            const jazz_protocol_VersionRecord *version = versions.ptr[i];
            jazz_types_SchemaVersionId author_schema = jazz_protocol__protocol__VersionRecord__schema_version(version);
            const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
            const jazz_model_TableSchema *source_table_schema;
            if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, vtable, author_schema, &source_table_schema))) break;
            jazz_types_SchemaVersionAlias schema_version_alias;
            if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(s, author_schema, &schema_version_alias))) break;
            bool has_authored;
            jz_btree authored_column_ids;
            if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__authored_column_ids_for_names(
                              s, author_schema, vtable, jazz_protocol__protocol__VersionRecord__authored_columns(version), &has_authored,
                              &authored_column_ids))) break;
            if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, vtable, author_schema, &source_table_schema))) {
                if (has_authored) jz_btree_drop(&authored_column_ids);
                break;
            }
            jazz_node_VersionRow stored;
            err = jazz_node__node__codec__VersionRow__from_wire_with_schema_version(source_table_schema, version,
                                                                                   has_authored ? &authored_column_ids : NULL, tx_node_alias,
                                                                                   schema_version_alias, tx->tx_id.time, global_time, &stored);
            if (has_authored) jz_btree_drop(&authored_column_ids);
            if (JZ_IS_ERR(err)) break;
            groove_InternStr history_table;
            groove_ValidatedVariantRecord groove_record;
            jazz_node_PrimaryKeyValue key;
            if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_write_binding(s, &stored, tx->made_by, &history_table, &groove_record)) ||
                JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, &stored, &key))) {
                jazz_node_VersionRow_drop(&stored);
                break;
            }
            groove__db__DatabaseBatch__insert_raw(batch, history_table, key, groove_record);
            jazz_node_ViewVersionKey ckey = {
                .table = jz_str_clone(jazz_node__node__codec__VersionRow__table(&stored)),
                .row_uuid = jazz_node__node__codec__VersionRow__row_uuid(&stored),
            };
            jazz_model_BranchKey_clone(&ckey.branch_key, jazz_node__node__codec__VersionRow__branch_key(&stored));
            /* Accepted rows are post-images at their seq: the newest seq is the row. */
            const jzn_CurrentUpdate *existing = jz_btree_get(&current_updates, &ckey);
            if (!existing || existing->global_time < global_time) {
                jzn_CurrentUpdate cu = { stored, global_time };
                jz_btree_insert(&current_updates, &ckey, &cu, NULL);
            } else {
                jazz_node_ViewVersionKey_drop(&ckey);
                jazz_node_VersionRow_drop(&stored);
            }
        }
        jz_vec_drop(&versions, sizeof(void *), NULL);
    }
    if (JZ_IS_ERR(err)) goto fail_batch;

    /* A first snapshot belongs to one subscription, not to an empty database.
     * Keep history ingestion complete without rewinding the node-wide current
     * winner. Empty-table probes preserve the cold load path. */
    {
        jz_btree resident_tables, winning_updates;
        jz_btree_init(&resident_tables, &jzn_desc_str_bool);
        jz_btree_init(&winning_updates, &jzn_desc_current_updates);
        while (current_updates.len) {
            jazz_node_ViewVersionKey key;
            jzn_CurrentUpdate cu;
            jz_btree_pop_first(&current_updates, &key, &cu);
            jazz_types_SchemaVersionId schema;
            if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(
                    s, jazz_node__node__codec__VersionRow__schema_version_alias(&cu.stored), &schema)) {
                err = JZN_ISV("unknown schema version alias");
            }
            jz_str table = {0};
            if (!JZ_IS_ERR(err))
                err = jazz_node__node__physical__NodeState__physical_current_table_for_schema(s, schema, jazz_node__node__codec__VersionRow__table(&cu.stored),
                                                                                             JZN_PCC_GLOBAL, &table);
            bool has_resident_rows = false;
            if (!JZ_IS_ERR(err)) {
                const bool *present = jz_btree_get(&resident_tables, &table);
                if (present) {
                    has_resident_rows = *present;
                    jz_str_drop(&table);
                } else {
                    err = groove__db__Database__table_has_stored_rows(jzn_db(s), &table, &has_resident_rows);
                    if (!JZ_IS_ERR(err)) jz_btree_insert(&resident_tables, &table, &has_resident_rows, NULL);
                    else jz_str_drop(&table);
                }
            }
            bool keep = !JZ_IS_ERR(err);
            if (keep && has_resident_rows) {
                bool has_seq;
                jazz_types_GlobalTime current_seq;
                err = jazz_node__node__ingest__NodeState__global_current_seq_in_batch(s, batch, schema, jazz_node__node__codec__VersionRow__table(&cu.stored),
                                                                                     jazz_node__node__codec__VersionRow__branch_key(&cu.stored),
                                                                                     jazz_node__node__codec__VersionRow__row_uuid(&cu.stored), &has_seq, &current_seq);
                /* The authority's image replaces a locally merged image at the
                 * same seq; only a newer seq keeps the row. */
                if (JZ_IS_ERR(err) || (has_seq && current_seq > cu.global_time)) keep = false;
            }
            if (keep) jz_btree_insert(&winning_updates, &key, &cu, NULL);
            else { jazz_node_ViewVersionKey_drop(&key); jzn_current_update_drop(&cu); }
            if (JZ_IS_ERR(err)) break;
        }
        jz_btree_drop(&resident_tables);
        jz_btree_drop(&current_updates);
        current_updates = winning_updates;
    }
    if (JZ_IS_ERR(err)) goto fail_batch;
    {
        jazz_node_ViewVersionKey *k; jzn_CurrentUpdate *cu;
        JZ_BTREE_FOR(&current_updates, k, cu) {
            if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__write_global_current_update(s, batch, &cu->stored, cu->global_time))) break;
        }
    }
    if (JZ_IS_ERR(err)) goto fail_batch;
    /* A new synced image rebases any pending local patches on that row. */
    {
        jz_btree settling;
        jz_btree_init(&settling, &jzn_desc_txid_set_a);
        jazz_node_VersionRowVec synced = {0};
        jazz_node_ViewVersionKey *k; jzn_CurrentUpdate *cu;
        JZ_BTREE_FOR(&current_updates, k, cu) {
            jazz_types_TxId t;
            if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__version_tx_id(s, &cu->stored, &t))) break;
            jz_btree_insert(&settling, &t, NULL, NULL);
        }
        if (!JZ_IS_ERR(err)) {
            JZ_BTREE_FOR(&current_updates, k, cu) {
                jazz_node_VersionRow copy;
                jazz_node_VersionRow_clone(&copy, &cu->stored);
                JZ_VEC_PUSH(synced, copy);
            }
            err = jazz_node__node__ingest__NodeState__rebase_ahead_overlays(s, batch, &synced, &settling);
        }
        jz_vec_drop(&synced, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
        jz_btree_drop(&settling);
    }
    jz_btree_drop(&current_updates);
    if (JZ_IS_ERR(err)) { groove__db__DatabaseBatch__drop(batch); goto fail_applied; }
    {
        groove_AppliedBatch *applied;
        if (JZ_IS_ERR(err = jazz_node__node__state__commit__NodeState__apply_node_batch(s, batch, &applied))) goto fail_applied;
        groove_PersistProgress *persisted = groove__db__Database__persist_with_progress(jzn_db(s), applied);
        err = groove__db__Database__finish_persistence(jzn_db(s), persisted);
        groove_AppliedBatch_drop(applied);
        if (JZ_IS_ERR(err)) goto fail_applied;
    }
    {
        jazz_types_TxId *t; void *unused;
        JZ_BTREE_FOR(&loaded_tx_ids, t, unused) jazz_node__node__state__commit__NodeState__invalidate_tx_version_tables_cache(s, *t);
    }
    for (u32 i = 0; i < applied_global_times.len; i++)
        jazz_node__node__open_tx__NodeState__record_applied_global_time(s, applied_global_times.ptr[i].global_time, applied_global_times.ptr[i].tx_id);
    jz_vec_drop(&applied_global_times, sizeof(jazz_node_GlobalTimeTx), NULL);
    for (u32 e = 0; e < eligible.len; e++) { jzn_bundle_vec_drop(&eligible.ptr[e].bundles); jazz_model_Transaction_drop(&eligible.ptr[e].local_tx); }
    jz_vec_drop(&eligible, sizeof(jzn_EligibleTx), NULL);
    *out = loaded_tx_ids;
    return JZ_OK;

fail_batch:
    jz_btree_drop(&current_updates);
    groove__db__DatabaseBatch__drop(batch);
fail_applied:
    jz_vec_drop(&applied_global_times, sizeof(jazz_node_GlobalTimeTx), NULL);
fail_loaded:
fail_eligible:
    for (u32 e = 0; e < eligible.len; e++) { jzn_bundle_vec_drop(&eligible.ptr[e].bundles); jazz_model_Transaction_drop(&eligible.ptr[e].local_tx); }
    jz_vec_drop(&eligible, sizeof(jzn_EligibleTx), NULL);
    jz_btree_drop(&loaded_tx_ids);
fail_map:
    jz_btree_drop(&bundles_by_tx);
    return err;
}

/* (table, branch_key, row_uuid, deletes_row) set of a payload */
typedef struct jzn_AddressedRow { jazz_node_ViewVersionKey key; bool deletes_row; } jzn_AddressedRow;
static int jzn_addressed_row_cmp(const void *a, const void *b, void *ctx) {
    const jzn_AddressedRow *x = a, *y = b;
    int c = jazz_node_ViewVersionKey_cmp(&x->key, &y->key, ctx);
    if (c) return c;
    return (int)x->deletes_row - (int)y->deletes_row;
}
static void jzn_addressed_row_drop(void *p) { jazz_node_ViewVersionKey_drop(&((jzn_AddressedRow *)p)->key); }
static const jz_btree_desc jzn_desc_addressed_rows = { sizeof(jzn_AddressedRow), 0, jzn_addressed_row_cmp, jzn_addressed_row_drop, NULL };

static void jzn_addressed_rows(const jazz_protocol_VersionRecordVec *records, jz_btree *out) {
    jz_btree_init(out, &jzn_desc_addressed_rows);
    for (u32 i = 0; i < records->len; i++) {
        jzn_AddressedRow r;
        jazz_node__node__ingest__view_version_key_for_ingest(records->ptr[i], &r.key);
        r.deletes_row = jazz_protocol__protocol__VersionRecord__deletes_row(records->ptr[i]);
        if (jz_btree_insert(out, &r, NULL, NULL)) { /* duplicate key: kept the first */ }
    }
}

static bool jzn_unit_eq(const void *a, const void *b) { (void)a; (void)b; return true; }

/* Whether two unit payloads address the same rows with the same deletion intent. */
bool jazz_node__node__ingest__same_addressed_rows(const jazz_protocol_VersionRecordVec *stored, const jazz_protocol_VersionRecordVec *resent) {
    jz_btree a, b;
    jzn_addressed_rows(stored, &a);
    jzn_addressed_rows(resent, &b);
    bool eq = jz_btree_eq(&a, &b, jzn_unit_eq);
    jz_btree_drop(&a);
    jz_btree_drop(&b);
    return eq;
}
