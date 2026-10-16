/* Port of crates/jazz/layers/node/src/node/global_state.rs (non-test) */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

static groove_Value v_bytes(jz_bytes b) { groove_Value v = { GROOVE_V_BYTES }; v.v.bytes = b; return v; }
static groove_Value v_uuid(jz_uuid u) { groove_Value v = { GROOVE_V_UUID }; v.v.uuid = u; return v; }
static groove_Value default_branch_value(void) {
    jazz_model_BranchKey *k = jazz_model__model__branch__BranchKey__default();
    jz_bytes b = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(k, &b);
    jazz_model__model__branch__BranchKey__drop(k);
    return v_bytes(b);
}
static void value_drop_v(void *p) { groove__records__values__Value__drop(p); }

/* Reads (tx_time, tx_node_alias) of a global-current record and resolves the node. */
static bool record_tx_id(const NS *self, const groove_OwnedRecord *record, jazz_model_TxId *out) {
    u64 time, alias;
    jz_err e = groove__records__BorrowedRecord__get_u64(record, JZN_GCR_FIELD_TX_TIME_IDX, &time);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    e = groove__records__BorrowedRecord__get_u64(record, JZN_GCR_FIELD_TX_NODE_ID_IDX, &alias);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    jazz_types_NodeUuid node;
    if (!jazz_node__node__NodeState__node_for_alias(self, alias, &node)) return false;
    out->time = time; out->node = node;
    return true;
}

bool jazz_node__node__global_state__NodeState__visible_global_tx_id_for_physical_table_now(NS *self, jazz_types_PhysicalTableId table_id,
                                                                                         jazz_types_RowUuid row_uuid, jazz_model_TxId *out) {
    jz_str current_table = jazz_node__node__physical__physical_global_current_table_name(table_id);
    groove_Value key[2] = { default_branch_value(), v_uuid(row_uuid) };
    groove_RawRow *raw = NULL;
    jz_err e = groove__db__primary_storage__Database__primary_key_get_raw(
        jazz_node__node__database_slot__DatabaseSlot__DerefMut__deref_mut(&self->database), &current_table, key, 2, &raw);
    groove__records__values__Value__drop(&key[0]);
    jz_str_drop(&current_table);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    if (!raw) return false;
    bool ok = record_tx_id(self, groove__db__RawRow__record(raw), out);
    groove__db__RawRow__drop(raw);
    return ok;
}

static jz_err jazz_node__node__global_state__NodeState__global_current_records_after(NS *self, const jz_str *table, jazz_types_GlobalTime position,
                                                                                    groove_OwnedRecordVec *out) {
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, self->catalogue.local_schema_version_id, table, &table_id));
    jz_str current_table = jazz_node__node__physical__physical_global_current_table_name(table_id);
    groove_Value branch = default_branch_value();
    groove_Value lo_seq = { GROOVE_V_U64 }; lo_seq.v.u = position + 1 < position ? (u64)-1 : position + 1;
    groove_Value hi_seq = { GROOVE_V_U64 }; hi_seq.v.u = UINT64_MAX;
    groove_Value lo[2], hi[2];
    groove__records__values__Value__clone(&lo[0], &branch);
    lo[1].tag = GROOVE_V_NULLABLE; lo[1].v.nullable = jz_memdup(&lo_seq, sizeof lo_seq);
    hi[0] = branch;
    hi[1].tag = GROOVE_V_NULLABLE; hi[1].v.nullable = jz_memdup(&hi_seq, sizeof hi_seq);
    groove_RawRowVec raws = {0};
    jz_err e = groove__db__primary_storage__Database__index_scan_range_raw(
        jazz_node__node__database_slot__DatabaseSlot__DerefMut__deref_mut(&self->database), &current_table,
        JAZZ_GLOBAL_CURRENT_BY_SEQ_INDEX, lo, 2, hi, 2, &raws);
    for (int i = 0; i < 2; i++) { groove__records__values__Value__drop(&lo[i]); groove__records__values__Value__drop(&hi[i]); }
    jz_str_drop(&current_table);
    JZ_TRY(e);
    for (u32 i = 0; i < raws.len; i++) {
        groove_OwnedRecord *rec = groove__db__RawRow__owned_record(raws.ptr[i]);
        JZ_VEC_PUSH(*out, rec);
    }
    jz_free(raws.ptr);
    return JZ_OK;
}

static void rec_drop_v(void *p) { groove__records__OwnedRecord__drop(*(groove_OwnedRecord **)p); }

jz_err jazz_node__node__global_state__NodeState__global_rows_changed_after(NS *self, const jz_str *table, jazz_types_GlobalTime position, jz_btree *rows) {
    groove_OwnedRecordVec records = {0};
    JZ_TRY(jazz_node__node__global_state__NodeState__global_current_records_after(self, table, position, &records));
    jz_btree_init(rows, &jzn_desc_uuid_set);
    for (u32 i = 0; i < records.len; i++) {
        jz_uuid u;
        jz_err e = groove__records__BorrowedRecord__get_uuid(records.ptr[i], JZN_GCR_FIELD_ROW_UUID_IDX, &u);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&records, sizeof(void *), rec_drop_v); jz_btree_drop(rows); return e; }
        jz_btree_insert(rows, &u, NULL, NULL);
    }
    jz_vec_drop(&records, sizeof(void *), rec_drop_v);
    return JZ_OK;
}

static const jz_btree_desc desc_row_tx = { sizeof(jazz_types_RowUuid), sizeof(jazz_model_TxId), jz_cmp_uuid, NULL, NULL };
typedef jazz_protocol_SupportingRowVec srow_vec;

jz_err jazz_node__node__global_state__NodeState__supporting_catch_up_after(NS *self, const jz_str *table, jazz_types_GlobalPhysicalTableId physical_table,
                                                                         const jazz_protocol_SupportingRow *const *current, u32 ncurrent,
                                                                         jazz_types_GlobalTime position, const u8 predecessor[16],
                                                                         jazz_protocol_SupportingRowsUpdate **out) {
    jz_btree moved;
    jz_btree_init(&moved, &desc_row_tx);
    groove_OwnedRecordVec records = {0};
    JZ_TRY(jazz_node__node__global_state__NodeState__global_current_records_after(self, table, position, &records));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < records.len; i++) {
        const groove_OwnedRecord *record = records.ptr[i];
        u64 alias, time; jz_uuid row;
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_GCR_FIELD_TX_NODE_ID_IDX, &alias))) goto fail;
        jazz_types_NodeUuid node;
        if (!jazz_node__node__NodeState__node_for_alias(self, alias, &node)) { e = JZN_INVALID_STORED("global current node alias must exist"); goto fail; }
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_GCR_FIELD_TX_TIME_IDX, &time))) goto fail;
        jazz_model_TxId tx_id = { time, node };
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_uuid(record, JZN_GCR_FIELD_ROW_UUID_IDX, &row))) goto fail;
        jz_btree_insert(&moved, &row, &tx_id, NULL);
    }
    jz_vec_drop(&records, sizeof(void *), rec_drop_v);
    srow_vec changed = {0}, left = {0};
    for (u32 i = 0; i < ncurrent; i++) {
        const jazz_protocol_SupportingRow *row = current[i];
        if (!jz_uuid_eq(&row->physical_table, &physical_table)) continue;
        if (jz_btree_remove(&moved, &row->row, NULL, NULL)) {
            jazz_protocol_SupportingRow c; jazz_protocol__protocol__SupportingRow__clone(&c, row);
            JZ_VEC_PUSH(changed, c);
        }
    }
    jazz_model_BranchKey *dk = jazz_model__model__branch__BranchKey__default();
    jz_bytes branch = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(dk, &branch);
    jazz_model__model__branch__BranchKey__drop(dk);
    jz_uuid *row; jazz_model_TxId *tx;
    JZ_BTREE_FOR(&moved, row, tx) {
        jazz_protocol_SupportingRow s = {0};
        s.physical_table = physical_table;
        s.version_table = groove__Intern__new_str(jz_str_clone(table));
        s.row = *row;
        s.version.tx = *tx;
        s.version.layer = JAZZ_LAYER_CONTENT;
        s.version.has_batch = true; s.version.batch = *tx;
        if (branch.len != 0) { s.version.has_branch_or_prefix = true; s.version.branch_or_prefix = jz_bytes_clone(&branch); }
        JZ_VEC_PUSH(left, s);
    }
    jz_bytes_drop(&branch);
    jz_btree_drop(&moved);
    jz_uuid revision = jz_uuid_v4();
    *out = jazz_protocol__protocol__SupportingRowsUpdate__catch_up(predecessor, revision.b, changed,
                                                                  left);
    return JZ_OK;
fail:
    jz_vec_drop(&records, sizeof(void *), rec_drop_v);
    jz_btree_drop(&moved);
    return e;
}

bool jazz_node__node__global_state__NodeState__visible_global_current_now(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                          jazz_types_RowUuid row_uuid, jazz_model_TxId *tx_out, bool *deleted_out) {
    jz_str current_table;
    jz_err e = jazz_node__node__physical__NodeState__physical_current_table_for_schema(self, schema_version, table, JZN_PHYS_CLASS_GLOBAL, &current_table);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    groove_Value key[2] = { default_branch_value(), v_uuid(row_uuid) };
    groove_RawRow *raw = NULL;
    e = groove__db__primary_storage__Database__primary_key_get_raw(
        jazz_node__node__database_slot__DatabaseSlot__DerefMut__deref_mut(&self->database), &current_table, key, 2, &raw);
    groove__records__values__Value__drop(&key[0]);
    jz_str_drop(&current_table);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    if (!raw) return false;
    const groove_OwnedRecord *record = groove__db__RawRow__record(raw);
    u64 time, alias;
    bool ok = false;
    if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_GCR_FIELD_TX_TIME_IDX, &time))) goto done;
    if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_GCR_FIELD_TX_NODE_ID_IDX, &alias))) goto done;
    bool some; u8 tag;
    if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_nullable_enum(record, JZN_GCR_FIELD__DELETION_IDX, &some, &tag))) goto done;
    bool deleted = false;
    if (some) {
        groove_Value v = { GROOVE_V_ENUM_TAG }; v.v.u = tag;
        jazz_model_DeletionEvent ev;
        jz_err de = jazz_node__node__codec__deletion_event_from_value(v, &ev);
        deleted = !JZ_IS_ERR(de) && ev == JAZZ_DEL_DELETED;
        jz_err_drop(&de);
    }
    jazz_types_NodeUuid node;
    if (!jazz_node__node__NodeState__node_for_alias(self, alias, &node)) goto done;
    tx_out->time = time; tx_out->node = node;
    *deleted_out = deleted;
    ok = true;
done:
    jz_err_drop(&e);
    groove__db__RawRow__drop(raw);
    return ok;
}

bool jazz_node__node__global_state__NodeState__visible_global_row_tx_id_now(NS *self, jazz_types_SchemaVersionId v, const jz_str *table,
                                                                            jazz_types_RowUuid row, jazz_model_TxId *out) {
    bool deleted;
    return jazz_node__node__global_state__NodeState__visible_global_current_now(self, v, table, row, out, &deleted);
}

bool jazz_node__node__global_state__NodeState__visible_global_content_tx_id_in_schema_now(NS *self, jazz_types_SchemaVersionId v, const jz_str *table,
                                                                                          jazz_types_RowUuid row, jazz_model_TxId *out) {
    bool deleted;
    if (!jazz_node__node__global_state__NodeState__visible_global_current_now(self, v, table, row, out, &deleted)) return false;
    return !deleted;
}

/* updates: BTreeMap<(String, BranchKey, RowUuid), VersionRow> */
typedef struct upd_key { jz_str table; jazz_model_BranchKey *branch; jazz_types_RowUuid row; } upd_key;
static int upd_key_cmp(const void *a, const void *b, void *c) {
    const upd_key *x = a, *y = b;
    int r = jz_str_cmp(&x->table, &y->table); if (r) return r;
    r = jazz_model__model__branch__BranchKey__cmp(x->branch, y->branch); if (r) return r;
    return jz_uuid_cmp(&x->row, &y->row);
}
static void upd_key_drop(void *p) { upd_key *k = p; jz_str_drop(&k->table); jazz_model__model__branch__BranchKey__drop(k->branch); }
static void vrow_ptr_drop(void *p) { jazz_node__node__codec__VersionRow__drop(*(jazz_node_VersionRow **)p); }
static const jz_btree_desc desc_updates = { sizeof(upd_key), sizeof(jazz_node_VersionRow *), upd_key_cmp, upd_key_drop, vrow_ptr_drop };

jz_err jazz_node__node__global_state__NodeState__global_current_updates_for_versions(NS *self, const groove_DatabaseBatch *batch, jazz_model_TxId tx_id,
                                                                                   jazz_types_GlobalTime global_time, jazz_node_VersionRow *const *versions,
                                                                                   u32 n, jazz_node_VersionRowVec *out) {
    jz_btree updates;
    jz_btree_init(&updates, &desc_updates);
    for (u32 i = 0; i < n; i++) {
        const jazz_node_VersionRow *version = versions[i];
        jazz_types_SchemaVersionId authored_schema;
        if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(version), &authored_schema)) {
            jz_btree_drop(&updates);
            return JZN_INVALID_STORED("global version schema alias must exist");
        }
        jazz_model_TableSchema *table_schema;
        jz_err e = jazz_node__node__NodeState__table_in_schema(self, jazz_node__node__codec__VersionRow__table(version), authored_schema, &table_schema);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&updates); return e; }
        jazz_node_VersionRow *merged = NULL;
        e = jazz_node__node__ingest__NodeState__merged_global_post_image(self, batch, authored_schema, table_schema, version, tx_id, global_time,
                                                                        self->minting_global_time, &merged);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&updates); return e; }
        if (merged) {
            upd_key k = { jz_str_clone(jazz_node__node__codec__VersionRow__table(version)),
                          jazz_model__model__branch__BranchKey__clone(jazz_node__node__codec__VersionRow__branch_key(version)),
                          jazz_node__node__codec__VersionRow__row_uuid(version) };
            jz_btree_insert(&updates, &k, &merged, NULL);
        }
    }
    upd_key k; jazz_node_VersionRow *v;
    while (jz_btree_pop_first(&updates, &k, &v)) { upd_key_drop(&k); JZ_VEC_PUSH(*out, v); }
    jz_btree_drop(&updates);
    return JZ_OK;
}
