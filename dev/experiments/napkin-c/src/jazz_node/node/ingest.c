/* ingest.c: jazz_node::node::ingest module root (ingest.rs).
 * Commit, fate, and sync-message ingestion for a storage-backed node. The
 * ingest/{..}.rs files are include!d into this module in Rust; they are ported
 * as separate translation units under ingest/.
 * Rust: crates/jazz/layers/node/src/node/ingest.rs */
#include "ingest.h"

static int jzn_cmp_uuid_v(const void *a, const void *b, void *ctx) { (void)ctx; return jz_uuid_cmp(a, b); }
const jz_btree_desc jzn_desc_uuid_set_a = { sizeof(jz_uuid), 0, jzn_cmp_uuid_v, NULL, NULL };
const jz_btree_desc jzn_desc_txid_set_a = { sizeof(jazz_types_TxId), 0, jazz_types__ids__TxId__cmp, NULL, NULL };

jz_err jazz_node__node__ingest__authority_wall_clock_ms(u64 *out) {
    /* web_time::SystemTime::now().duration_since(UNIX_EPOCH) */
    i64 now = (i64)jz_now_ms();
    if (now < 0) return JZN_ISV("authority clock precedes Unix epoch");
    if ((u64)now > UINT64_MAX) return JZN_ISV("authority clock exceeds u64 milliseconds");
    *out = (u64)now;
    return JZ_OK;
}

jazz_node_CommitUnitParkMode jazz_node__node__ingest__CommitUnitParkMode__default(void) {
    jazz_node_CommitUnitParkMode m;
    memset(&m, 0, sizeof m);
    m.has_ingest_context = false;
    m.ingress_role = JZN_PARKED_AUTHORITY;
    return m;
}

/* A sequence is the global-authority receipt. Peer payloads which pair it
 * with a weaker durability must be rejected before they can reach storage. */
jz_err jazz_node__node__ingest__validate_received_fate_update_global_time_durability(bool has_gt, jazz_types_GlobalTime gt,
                                                                                     bool has_durability, u32 durability) {
    (void)gt;
    if (has_gt && !(has_durability && durability == JAZZ_DURABILITY_GLOBAL))
        return jz_err_new(JZN_E_UNSUPPORTED_SYNC_MESSAGE, "global timestamp requires Global durability");
    return JZ_OK;
}

/* View bundles are peer payloads too, including reset bundles eligible for bulk persistence. */
jz_err jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bool has_gt, jazz_types_GlobalTime gt, u32 durability) {
    (void)gt;
    if (has_gt && durability != JAZZ_DURABILITY_GLOBAL)
        return jz_err_new(JZN_E_MALFORMED_VIEW_UPDATE, "global timestamp requires Global durability");
    return JZ_OK;
}

bool jazz_node__node__ingest__fate_update_durability_claim(const jazz_model_Fate *fate, u32 durability, u32 *out) {
    switch (fate->tag) {
    case JAZZ_FATE_REJECTED: return false;
    default: *out = durability; return true;
    }
}

bool jazz_node__node__ingest__commit_unit_write_count_matches(const jazz_model_Transaction *tx, usize version_count) {
    return (usize)tx->n_total_writes == version_count;
}

/* One transaction carries at most one version per row, so a row names its
 * version within the transaction. */
void jazz_node__node__ingest__view_version_key_for_ingest(const jazz_protocol_VersionRecord *v, jazz_node_ViewVersionKey *out) {
    out->table = jz_str_clone(jazz_protocol__protocol__VersionRecord__table(v));
    jazz_model_BranchKey_clone(&out->branch_key, jazz_protocol__protocol__VersionRecord__branch_key(v));
    out->row_uuid = jazz_protocol__protocol__VersionRecord__row_uuid(v);
}

int jazz_node_ViewVersionKey_cmp(const void *a, const void *b, void *ctx) {
    const jazz_node_ViewVersionKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    if (c) return c;
    c = jazz_model_BranchKey_cmp(&x->branch_key, &y->branch_key, ctx);
    if (c) return c;
    return jz_uuid_cmp(&x->row_uuid, &y->row_uuid);
}

void jazz_node_ViewVersionKey_drop(void *k) {
    jazz_node_ViewVersionKey *x = k;
    jz_str_drop(&x->table);
    jazz_model_BranchKey_drop(&x->branch_key);
}

/* ---- small constructors shared by the ingest/ files ---- */
void jazz_node__node__ingest__fate_update_message(jazz_types_TxId tx_id, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt,
                                                  bool has_durability, u32 durability, jazz_protocol_SyncMessage *out) {
    memset(out, 0, sizeof *out);
    out->tag = JZP_SM_FATE_UPDATE;
    out->v.fate_update.tx_id = tx_id;
    out->v.fate_update.fate = fate;
    out->v.fate_update.has_global_time = has_gt;
    out->v.fate_update.global_time = gt;
    out->v.fate_update.has_durability = has_durability;
    out->v.fate_update.durability = durability;
}

void jazz_node__node__ingest__settled_empty(jazz_node_PublicationOutcome *out) {
    jazz_protocol_SyncMessageVec v = {0};
    jazz_node__node__PublicationOutcome__settled(out, v);
}

void jazz_node__node__ingest__settled_one(jazz_node_PublicationOutcome *out, jazz_protocol_SyncMessage msg) {
    jazz_protocol_SyncMessageVec v = {0};
    JZ_VEC_PUSH(v, msg);
    jazz_node__node__PublicationOutcome__settled(out, v);
}

jazz_model_Fate jazz_node__node__ingest__malformed_commit_fate(jz_str reason) {
    jazz_model_Fate f;
    memset(&f, 0, sizeof f);
    f.tag = JAZZ_FATE_REJECTED;
    f.reason.tag = JAZZ_REJ_MALFORMED_COMMIT;
    f.reason.message = reason;
    return f;
}

jazz_model_Fate jazz_node__node__ingest__rejected_fate(u32 reason_tag) {
    jazz_model_Fate f;
    memset(&f, 0, sizeof f);
    f.tag = JAZZ_FATE_REJECTED;
    f.reason.tag = reason_tag;
    return f;
}
