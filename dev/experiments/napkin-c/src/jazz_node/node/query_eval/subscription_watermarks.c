/* Port of crates/jazz/layers/node/src/node/query_eval/subscription_watermarks.rs
 * Subscription watermark record v1 ("Q at W"). */
#include "jazz_node/node/node_qe_b.h"

typedef struct PersistedWatermark { jazz_types_GlobalTime seq; u8 revision[16]; } PersistedWatermark;

/* A view whose membership depends only on each row's own image and on nothing
 * the session carries, so a catch-up from W is exact. */
bool jazz_node__node__query_eval__subscription_watermarks__NodeState__watermark_catch_up_view(
    const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape)
{
    const jazz_model_Query *query = &shape->query;
    if (!jazz_node__peer__publication__row_local_membership(query) || jazz_node__peer__publication__query_uses_claims(query))
        return false;
    const jazz_model_TableSchema *table;
    jz_err e = jazz_node__node__NodeState__table(self, &query->table, &table);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    if (!table->read_policy) return true;
    return jazz_node__peer__publication__row_local_membership(table->read_policy) &&
           !jazz_node__peer__publication__query_uses_claims(table->read_policy);
}

static jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__subscription_watermark_key(
    jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, groove_ValueVec *out)
{
    u8 scope;
    jz_bytes digest = {0};
    if (key->has_policy_binding) {
        JZ_TRY(jazz_node__node__NodeState__persist_policy_binding_directory(self, &key->policy_binding));
        u8 d[32];
        jazz_protocol__protocol__PolicyBindingKey__directory_digest(&key->policy_binding, d);
        scope = 1;
        digest = jz_bytes_from(d, 32);
    } else {
        scope = 0;
    }
    memset(out, 0, sizeof *out);
    jz_vec_reserve(out, 5, sizeof(groove_Value));
    JZ_VEC_PUSH(*out, gv_uuid(key->binding_view.shape_id));
    JZ_VEC_PUSH(*out, gv_uuid(key->binding_view.binding_id));
    JZ_VEC_PUSH(*out, gv_uuid(key->binding_view.read_view.id));
    JZ_VEC_PUSH(*out, gv_u8(scope));
    JZ_VEC_PUSH(*out, gv_bytes(digest));
    return JZ_OK;
}

/* Value of subscription watermark record v1: format byte 1, the settled seq,
 * and the 16-byte supporting revision. */
static void jazz_node__node__query_eval__subscription_watermarks__encode_watermark_v1(
    jazz_types_GlobalTime settled_through, const u8 revision[16], groove_Value out[3])
{
    out[0] = gv_u8(1);
    out[1] = gv_u64(settled_through);
    out[2] = gv_bytes(jz_bytes_from(revision, 16));
}

/* Decode and validate a watermark record v1. */
static jz_err jazz_node__node__query_eval__subscription_watermarks__decode_watermark_v1(
    const groove_Value *values, u32 n, jazz_types_GlobalTime *seq, u8 revision[16])
{
    if (n != 3 || values[0].tag != GV_U8 || values[0].v.u8_ != 1 || values[1].tag != GV_U64 || values[2].tag != GV_BYTES)
        return JZN_INVALID_STORED("invalid subscription watermark record v1");
    if (values[2].v.bytes.len != 16)
        return JZN_INVALID_STORED("subscription watermark revision must be 16 bytes");
    memcpy(revision, values[2].v.bytes.ptr, 16);
    static const u8 zero[16];
    if (values[1].v.u64_ == 0 || memcmp(revision, zero, 16) == 0)
        return JZN_INVALID_STORED("subscription watermark record v1 must name a settled revision");
    *seq = values[1].v.u64_;
    return JZ_OK;
}

static void value_vec_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop); }

jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__purge_subscription_watermarks(jazz_node_NodeState *self)
{
    groove_DirectRecordStore store;
    JZ_TRY(groove__db__facade__Database__direct_record_store(self->database, jazz_model__schema__SUBSCRIPTION_WATERMARKS_STORE, &store));
    groove_DirectRecordEntryVec entries;
    jz_err e = groove__db__storage_helpers__DirectRecordStore__prefix_entries(&store, NULL, 0, &entries);
    if (JZ_IS_ERR(e)) { groove_DirectRecordStore_drop(&store); return e; }
    groove_DirectRecordStoreWriteVec deletes = {0};
    for (u32 i = 0; i < entries.len; i++) {
        groove_DirectRecordStoreWrite w = {0};
        w.tag = GDRSW_DELETE;
        w.key = entries.ptr[i].key;
        memset(&entries.ptr[i].key, 0, sizeof w.key);
        jz_vec_push(&deletes, &w, sizeof w);
    }
    jz_vec_drop(&entries, sizeof(groove_DirectRecordEntry), groove_DirectRecordEntry_drop);
    if (deletes.len) e = groove__db__storage_helpers__DirectRecordStore__write_many(&store, deletes.ptr, deletes.len);
    jz_vec_drop(&deletes, sizeof(groove_DirectRecordStoreWrite), groove_DirectRecordStoreWrite_drop);
    groove_DirectRecordStore_drop(&store);
    if (JZ_IS_ERR(e)) return e;
    jz_btree_clear(&self->query.persisted_watermarks);
    return JZ_OK;
}

jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__persist_subscription_watermark(
    jazz_node_NodeState *self, const jazz_protocol_AuthorityResultKey *key, jazz_types_GlobalTime settled_through, const u8 revision[16])
{
    jazz_protocol_AuthorityResultKey k;
    jazz_protocol_AuthorityResultKey_clone(&k, key);
    if (jz_btree_contains(&self->query.watermark_restore_seen, &k)) jazz_protocol_AuthorityResultKey_drop(&k);
    else jz_btree_insert(&self->query.watermark_restore_seen, &k, NULL, NULL);
    const PersistedWatermark *prior = jz_btree_get(&self->query.persisted_watermarks, key);
    if (prior && prior->seq == settled_through && memcmp(prior->revision, revision, 16) == 0) return JZ_OK;

    groove_ValueVec store_key;
    JZ_TRY(jazz_node__node__query_eval__subscription_watermarks__NodeState__subscription_watermark_key(self, key, &store_key));
    groove_DirectRecordStore store;
    jz_err e = groove__db__facade__Database__direct_record_store(self->database, jazz_model__schema__SUBSCRIPTION_WATERMARKS_STORE, &store);
    if (!JZ_IS_ERR(e)) {
        groove_Value value[3];
        jazz_node__node__query_eval__subscription_watermarks__encode_watermark_v1(settled_through, revision, value);
        e = groove__db__storage_helpers__DirectRecordStore__set(&store, store_key.ptr, store_key.len, value, 3);
        for (int i = 0; i < 3; i++) groove_Value_drop(&value[i]);
        groove_DirectRecordStore_drop(&store);
    }
    value_vec_drop(&store_key);
    if (JZ_IS_ERR(e)) return e;
    PersistedWatermark w;
    w.seq = settled_through;
    memcpy(w.revision, revision, 16);
    jazz_protocol_AuthorityResultKey_clone(&k, key);
    jz_btree_insert(&self->query.persisted_watermarks, &k, &w, NULL);
    return JZ_OK;
}

/* After a reopen, rebuild the receipt a row-local view needs to declare
 * "Q at W". Returns whether a receipt was restored. */
jz_err jazz_node__node__query_eval__subscription_watermarks__NodeState__restore_subscription_watermark(
    jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    const jazz_protocol_AuthorityResultKey *key, jazz_types_AuthorSubject identity, bool *out)
{
    *out = false;
    if (self->query.watermarks_invalidated) return JZ_OK;
    if (jz_btree_contains(&self->query.watermark_restore_seen, key)) return JZ_OK;
    jazz_protocol_AuthorityResultKey k;
    jazz_protocol_AuthorityResultKey_clone(&k, key);
    jz_btree_insert(&self->query.watermark_restore_seen, &k, NULL, NULL);
    if (jz_btree_contains(&self->query.authority_results, key) ||
        !jazz_node__node__query_eval__subscription_watermarks__NodeState__watermark_catch_up_view(self, shape))
        return JZ_OK;

    groove_ValueVec store_key;
    JZ_TRY(jazz_node__node__query_eval__subscription_watermarks__NodeState__subscription_watermark_key(self, key, &store_key));
    groove_DirectRecordStore store;
    jz_err e = groove__db__facade__Database__direct_record_store(self->database, jazz_model__schema__SUBSCRIPTION_WATERMARKS_STORE, &store);
    if (JZ_IS_ERR(e)) { value_vec_drop(&store_key); return e; }
    bool found = false;
    groove_OwnedRecord record;
    e = groove__db__storage_helpers__DirectRecordStore__get(&store, store_key.ptr, store_key.len, &found, &record);
    value_vec_drop(&store_key);
    groove_DirectRecordStore_drop(&store);
    if (JZ_IS_ERR(e)) return e;
    if (!found) return JZ_OK;
    groove_ValueVec values;
    groove_BorrowedRecord br = groove__records__record__OwnedRecord__borrowed(&record);
    e = groove__records__record__BorrowedRecord__to_values(&br, &values);
    groove_OwnedRecord_drop(&record);
    if (JZ_IS_ERR(e)) return e;
    jazz_types_GlobalTime settled_through;
    u8 revision[16];
    e = jazz_node__node__query_eval__subscription_watermarks__decode_watermark_v1(values.ptr, values.len, &settled_through, revision);
    value_vec_drop(&values);
    if (JZ_IS_ERR(e)) return e;

    const jz_str *table = &shape->query.table;
    jazz_types_GlobalPhysicalTableId physical_table;
    JZ_TRY(jazz_node__node__views__NodeState__scope_physical_table(self, shape->schema_version, table, &physical_table));
    jazz_model_BranchKey default_branch = {0};
    jz_bytes branch = jazz_model__model__branch__BranchKey__canonical_bytes(&default_branch);
    jz_btree held;
    jz_btree_init(&held, &jazz_node_COVERED_INPUTS_DESC);
    jazz_node_CurrentRowVec rows;
    e = jazz_node__node__query_eval__NodeState__query_rows_for_link(self, shape, binding, JZ_DURABILITY_GLOBAL, identity, &rows);
    if (JZ_IS_ERR(e)) { jz_bytes_drop(&branch); jz_btree_drop(&held); return e; }
    for (u32 i = 0; i < rows.len; i++) {
        jazz_model_TxId tx;
        if (!jazz_node__node__NodeState__current_row_tx_id(self, &rows.ptr[i], &tx)) continue;
        jazz_protocol_SupportingRow input;
        memset(&input, 0, sizeof input);
        input.physical_table = physical_table;
        input.version_table = groove__intern_string(table);
        input.row = jazz_node__node__CurrentRow__row_uuid(&rows.ptr[i]);
        input.version.tx = tx;
        input.version.has_schema_version = false;
        input.version.layer = JZ_LAYER_CONTENT;
        input.version.has_batch = true;
        input.version.batch = tx;
        if (branch.len != 0) {
            input.version.has_branch_or_prefix = true;
            input.version.branch_or_prefix = jz_bytes_clone(&branch);
        }
        input.version.has_row_digest = false;
        jazz_node_CoveredInputCoordinate coord = jazz_node__node__CoveredInputCoordinate__from(&input);
        jz_btree_insert(&held, &coord, &input, NULL);
    }
    jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
    jz_bytes_drop(&branch);

    bool inserted;
    jazz_protocol_AuthorityResultKey_clone(&k, key);
    jazz_node_AuthorityResultState *state = jz_btree_entry(&self->query.authority_results, &k, &inserted);
    if (inserted) jazz_node_AuthorityResultState_init(state);
    else jazz_protocol_AuthorityResultKey_drop(&k);
    if (state->has_settled_through || state->has_supporting_revision) {
        jz_btree_drop(&held);
        return JZ_OK;
    }
    if (state->applied_view_update_generation < 1) state->applied_view_update_generation = 1;
    state->source_closure.tag = JZN_ASC_CLAIMED;
    state->source_closure.generation = state->applied_view_update_generation;
    jz_btree_drop(&state->covered_input_versions);
    state->covered_input_versions = held;
    state->has_settled_through = true;
    state->settled_through = settled_through;
    state->has_supporting_revision = true;
    memcpy(state->supporting_revision, revision, 16);
    *out = true;
    return JZ_OK;
}
