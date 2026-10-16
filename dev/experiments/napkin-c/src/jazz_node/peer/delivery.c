/* jazz_node::peer::delivery — subscription result-set and wire-update
 * delivery helpers. Pure transformations below PeerState's transport
 * lifecycle. Rust: crates/jazz/layers/node/src/peer/delivery.rs
 * (canonical_set_delta is #[cfg(test)] and not ported). */
#include "peer.h"

typedef jazz_protocol_ResultMemberEntry RME;
typedef jazz_protocol_ResultMemberEntryVec RMEVec;

static bool member_row_key(const RME *member, jazz_node_RowKey *out)
{
    const jazz_protocol_RealRowMemberEntry *row = jazz_protocol__protocol__ResultMemberEntry__as_real_row(member);
    if (!row) return false;
    jazz_types_OutputOccurrenceId *occurrence = jazz_protocol__protocol__ResultMemberEntry__output_occurrence_id(member);
    if (!occurrence) return false;
    out->occurrence = occurrence;
    out->table = jazz_protocol__protocol__RealRowMemberEntry__table_intern(row);
    out->row = jazz_protocol__protocol__RealRowMemberEntry__row_uuid(row);
    return true;
}

static void member_index_key(const RME *member, jazz_node_MemberIndexKey *out)
{
    memset(out, 0, sizeof *out);
    if (member_row_key(member, &out->row)) {
        out->tag = JZN_MIK_ROW;
        return;
    }
    const jz_bytes *row; const jazz_protocol_SyntheticReplacementToken *replacement;
    if (member->tag == JAZZ_RME_SYNTHETIC && jazz_protocol__protocol__ResultMemberEntry__as_synthetic(member, &row, &replacement)) {
        out->tag = JZN_MIK_SYNTHETIC;
        out->synthetic_table = jz_str_clone(jazz_protocol__protocol__ResultMemberEntry__synthetic_table(member));
        out->synthetic_row = jz_bytes_clone(row);
        return;
    }
    out->tag = JZN_MIK_MEMBER;
    jazz_protocol__protocol__ResultMemberEntry__clone(&out->member, member);
}

static bool member_eq(const RME *a, const RME *b) { return jazz_protocol__protocol__ResultMemberEntry__cmp(a, b, NULL) == 0; }

bool JZP(delivery__result_member_replaces)(const RME *replacement, const RME *previous)
{
    jazz_node_MemberIndexKey ka, kb;
    member_index_key(replacement, &ka);
    member_index_key(previous, &kb);
    bool same_key = JZP(MemberIndexKey__cmp)(&ka, &kb, NULL) == 0;
    JZP(MemberIndexKey__drop)(&ka);
    JZP(MemberIndexKey__drop)(&kb);
    if (!same_key || member_eq(replacement, previous)) return false;
    const jz_str *rt, *pt; jazz_types_RowUuid rr, pr; jazz_model_TxId rtx, ptx;
    bool r_row = jazz_protocol__protocol__ResultMemberEntry__as_row_entry(replacement, &rt, &rr, &rtx);
    bool p_row = jazz_protocol__protocol__ResultMemberEntry__as_row_entry(previous, &pt, &pr, &ptx);
    if (r_row && p_row) return jz_str_eq(rt, pt) && jz_uuid_eq(&rr, &pr);
    return replacement->tag == JAZZ_RME_SYNTHETIC;
}

void JZP(delivery__replacement_removals)(const jazz_node_PeerSubscriptionState *previous, const RME *additions, u32 n, RMEVec *out)
{
    jz_btree set;
    jz_btree_init(&set, &jzp_desc_member_set);
    for (u32 i = 0; i < n; i++) {
        jazz_node_MemberIndexKey key;
        member_index_key(&additions[i], &key);
        const jazz_node_MemberSlot *slot = jz_btree_get(&previous->member_index, &key);
        JZP(MemberIndexKey__drop)(&key);
        if (!slot || !JZP(delivery__result_member_replaces)(&additions[i], &slot->member)) continue;
        RME old;
        jazz_protocol__protocol__ResultMemberEntry__clone(&old, &slot->member);
        if (jz_btree_insert(&set, &old, NULL, NULL)) jazz_protocol__protocol__ResultMemberEntry__drop(&old);
    }
    memset(out, 0, sizeof *out);
    RME m;
    while (jz_btree_pop_first(&set, &m, NULL)) JZ_VEC_PUSH(*out, m);
    jz_btree_drop(&set);
}

static void set_insert_clone(jz_btree *set, const RME *m)
{
    RME c;
    jazz_protocol__protocol__ResultMemberEntry__clone(&c, m);
    if (jz_btree_insert(set, &c, NULL, NULL)) jazz_protocol__protocol__ResultMemberEntry__drop(&c);
}

static void push_clone(RMEVec *v, const RME *m)
{
    RME c;
    jazz_protocol__protocol__ResultMemberEntry__clone(&c, m);
    JZ_VEC_PUSH(*v, c);
}

void JZP(delivery__apply_contribution_add)(jazz_node_PeerSubscriptionState *state, const RME *contribution, u32 n, RMEVec *adds, RMEVec *removes)
{
    for (u32 i = 0; i < n; i++) {
        const RME *member = &contribution[i];
        jazz_node_MemberIndexKey key;
        member_index_key(member, &key);
        jazz_node_MemberSlot *slot = jz_btree_get(&state->member_index, &key);
        if (slot && member_eq(&slot->member, member)) {
            slot->refcount += 1;
            JZP(MemberIndexKey__drop)(&key);
        } else if (slot && JZP(delivery__result_member_replaces)(member, &slot->member)) {
            RME old_member;
            jazz_protocol__protocol__ResultMemberEntry__clone(&old_member, &slot->member);
            push_clone(removes, &old_member);
            push_clone(adds, member);
            jz_btree_remove(&state->result_member_set, &old_member, NULL, NULL);
            set_insert_clone(&state->result_member_set, member);
            jazz_protocol__protocol__ResultMemberEntry__drop(&slot->member);
            jazz_protocol__protocol__ResultMemberEntry__clone(&slot->member, member);
            jazz_protocol__protocol__ResultMemberEntry__drop(&old_member);
            JZP(MemberIndexKey__drop)(&key);
        } else if (slot) {
            slot->refcount += 1;
            JZP(MemberIndexKey__drop)(&key);
        } else {
            jazz_node_MemberSlot fresh = { .refcount = 1 };
            jazz_protocol__protocol__ResultMemberEntry__clone(&fresh.member, member);
            jz_btree_insert(&state->member_index, &key, &fresh, NULL);
            push_clone(adds, member);
            set_insert_clone(&state->result_member_set, member);
        }
    }
}

void JZP(delivery__apply_contribution_remove)(jazz_node_PeerSubscriptionState *state, const RME *contribution, u32 n, RMEVec *removes)
{
    for (u32 i = 0; i < n; i++) {
        jazz_node_MemberIndexKey key;
        member_index_key(&contribution[i], &key);
        jazz_node_MemberSlot *slot = jz_btree_get(&state->member_index, &key);
        if (!slot) {
            JZP(MemberIndexKey__drop)(&key);
            continue;
        }
        if (slot->refcount > 1) {
            slot->refcount -= 1;
        } else {
            RME removed;
            jazz_protocol__protocol__ResultMemberEntry__clone(&removed, &slot->member);
            jz_btree_remove(&state->member_index, &key, NULL, NULL);
            jz_btree_remove(&state->result_member_set, &removed, NULL, NULL);
            JZ_VEC_PUSH(*removes, removed);
        }
        JZP(MemberIndexKey__drop)(&key);
    }
}

/* #[cfg(debug_assertions)] */
static int cmp_row_key_v(const void *a, const void *b, void *ctx)
{
    jazz_node_MemberIndexKey ka = { .tag = JZN_MIK_ROW, .row = *(const jazz_node_RowKey *)a };
    jazz_node_MemberIndexKey kb = { .tag = JZN_MIK_ROW, .row = *(const jazz_node_RowKey *)b };
    return JZP(MemberIndexKey__cmp)(&ka, &kb, ctx);
}
static void drop_row_key_v(void *k) { jazz_types__object__OutputOccurrenceId__drop(((jazz_node_RowKey *)k)->occurrence); }
static const jz_btree_desc desc_row_key_tx = { sizeof(jazz_node_RowKey), sizeof(jazz_model_TxId), cmp_row_key_v, drop_row_key_v, NULL };

bool JZP(delivery__duplicate_physical_row_result_set)(const jz_btree *result_set, jazz_node_RowKey *row_key_out, jazz_model_TxId *first, jazz_model_TxId *second)
{
    jz_btree rows;
    jz_btree_init(&rows, &desc_row_key_tx);
    bool found = false;
    RME *member; void *unused;
    JZ_BTREE_FOR(result_set, member, unused) {
        jazz_node_RowKey row_key;
        if (!member_row_key(member, &row_key)) continue;
        const jz_str *t; jazz_types_RowUuid r; jazz_model_TxId tx_id;
        if (!jazz_protocol__protocol__ResultMemberEntry__as_row_entry(member, &t, &r, &tx_id)) {
            jazz_types__object__OutputOccurrenceId__drop(row_key.occurrence);
            continue;
        }
        jazz_model_TxId *existing = jz_btree_get(&rows, &row_key);
        if (existing) {
            row_key_out->occurrence = row_key.occurrence;
            row_key_out->table = row_key.table;
            row_key_out->row = row_key.row;
            *first = *existing;
            *second = tx_id;
            found = true;
            break;
        }
        jz_btree_insert(&rows, &row_key, &tx_id, NULL);
    }
    jz_btree_drop(&rows);
    return found;
}

bool JZP(delivery__bundle_contains_complete_tx_payload)(const jazz_protocol_VersionBundle *bundle)
{
    return bundle->scope == JAZZ_BUNDLE_SCOPE_COMPLETE_TX;
}

void JZP(delivery__view_update_singleton_bundles)(jazz_protocol_VersionCarrier *const *carriers, u32 n, jazz_protocol_VersionBundleVec *out)
{
    memset(out, 0, sizeof *out);
    jz_err e = jazz_protocol__protocol__expand_version_carriers(carriers, n, out);
    if (JZ_IS_ERR(e)) { /* unwrap_or_default */
        jz_err_drop(&e);
        jz_vec_drop(out, sizeof(out->ptr[0]), jazz_protocol__protocol__VersionBundle__drop);
        memset(out, 0, sizeof *out);
    }
}

static void storage_read_bucket_field(jz_str *out, const char *name, groove_StorageReadBucket bucket)
{
    jz_str_appendf(out, "%s.reads=%llu:%s.ranges=%llu", name, (unsigned long long)bucket.reads, name, (unsigned long long)bucket.ranges);
}

jz_str JZP(delivery__storage_read_metrics_buckets)(const groove_StorageReadMetrics *m)
{
    struct { const char *name; groove_StorageReadBucket bucket; } fields[] = {
        { "history_rows", m->history_rows },
        { "history_indexes", m->history_indexes },
        { "ahead_current_rows", m->ahead_current_rows },
        { "global_current_rows", m->global_current_rows },
        { "global_current_indexes", m->global_current_indexes },
        { "register_global_current_rows", m->register_global_current_rows },
        { "global_changes_rows", m->global_changes_rows },
        { "global_changes_indexes", m->global_changes_indexes },
        { "transactions_rows", m->transactions_rows },
        { "transactions_indexes", m->transactions_indexes },
        { "other", m->other },
    };
    jz_str out = {0};
    for (u32 i = 0; i < sizeof fields / sizeof fields[0]; i++) {
        if (i) jz_str_push_char(&out, ',');
        storage_read_bucket_field(&out, fields[i].name, fields[i].bucket);
    }
    return out;
}

void JZP(delivery__binding_values_in_param_order)(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, groove_ValueVec *out)
{
    memset(out, 0, sizeof *out);
    const jz_btree *values = jazz_model__query__Binding__values(binding);
    jz_str *name; void *ty;
    JZ_BTREE_FOR(jazz_model__query__ValidatedQuery__params(shape), name, ty) {
        const groove_Value *v = jz_btree_get(values, name);
        if (!v) jz_panic("validated binding contains every shape param");
        groove_Value c;
        groove__records__values__Value__clone(&c, v);
        JZ_VEC_PUSH(*out, c);
    }
}
