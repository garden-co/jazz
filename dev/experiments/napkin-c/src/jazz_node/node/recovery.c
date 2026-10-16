/* Port of crates/jazz/layers/node/src/node/recovery.rs (non-test) */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

static void raws_drop(groove_RawRowVec *v) { for (u32 i = 0; i < v->len; i++) groove__db__RawRow__drop(v->ptr[i]); jz_free(v->ptr); *v = (groove_RawRowVec){0}; }

typedef struct rv_sort { const jz_str *table; jazz_types_RowUuid row; bool deletion; jazz_model_RejectedVersion *v; } rv_sort;
static int rv_cmp(const void *a, const void *b, void *c) {
    const rv_sort *x = a, *y = b;
    int r = jz_str_cmp(x->table, y->table); if (r) return r;
    r = jz_uuid_cmp(&x->row, &y->row); if (r) return r;
    return (int)x->deletion - (int)y->deletion;
}

jz_err jazz_node__node__recovery__NodeState__rejected_versions_for(NS *self, jazz_types_NodeAlias alias, jazz_model_TxId tx_id, jzn_ptrvec *out) {
    JZ_VEC(rv_sort) versions = {0};
    jzn_u64vec table_ids = {0};
    jazz_node__node__physical__NodeState__physical_table_ids(self, &table_ids);
    jz_err e = JZ_OK;
    for (u32 t = 0; t < table_ids.len; t++) {
        jazz_types_PhysicalTableId table_id = table_ids.ptr[t];
        jz_str storage_table = jazz_node__node__physical__physical_rejected_versions_table_name(table_id);
        groove_Value prefix[2] = { { GROOVE_V_U64 }, { GROOVE_V_U64 } };
        prefix[0].v.u = tx_id.time; prefix[1].v.u = alias;
        groove_RawRowVec raws = {0};
        e = groove__db__primary_storage__Database__primary_key_scan_raw(jzn_db(self), &storage_table, prefix, 2, &raws);
        jz_str_drop(&storage_table);
        if (JZ_IS_ERR(e)) goto out;
        for (u32 i = 0; i < raws.len; i++) {
            groove_RawRow *raw = raws.ptr[i];
            const groove_OwnedRecord *record = groove__db__RawRow__record(raw);
            u64 node_id, time;
            if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_RVR_FIELD_TX_NODE_ID_IDX, &node_id))) { raws_drop(&raws); goto out; }
            if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_RVR_FIELD_TX_TIME_IDX, &time))) { raws_drop(&raws); goto out; }
            if (node_id != alias || time != tx_id.time) continue;
            jazz_types_SchemaVersionAlias schema_alias = groove__db__RawRow__variant_tag(raw);
            jazz_types_SchemaVersionId schema_version;
            if (!jazz_node__node__NodeState__schema_version_for_alias(self, schema_alias, &schema_version)) {
                e = JZN_INVALID_STORED("rejected row schema version alias missing"); raws_drop(&raws); goto out;
            }
            jz_str logical_table;
            if (JZ_IS_ERR(e = jazz_node__node__physical__NodeState__logical_table_for_physical_alias(self, table_id, schema_alias, &logical_table))) { raws_drop(&raws); goto out; }
            const jazz_model_TableSchema *ts;
            if (JZ_IS_ERR(e = jazz_node__node__NodeState__table_in_schema_ref(self, &logical_table, schema_version, &ts))) { jz_str_drop(&logical_table); raws_drop(&raws); goto out; }
            groove_RecordDescriptor *logical_descriptor = jazz_model__schema__TableSchema__rejected_versions_record_schema(ts);
            jz_slice rb = groove__db__RawRow__raw(raw);
            jazz_model_RejectedVersion *v = jazz_model__tx__RejectedVersion__new(logical_table,
                groove__records__OwnedRecord__new(jz_bytes_from(rb.ptr, rb.len), logical_descriptor));
            rv_sort s = { jazz_model__tx__RejectedVersion__table(v), jazz_model__tx__RejectedVersion__row_uuid(v), jazz_model__tx__RejectedVersion__has_deletion(v), v };
            JZ_VEC_PUSH(versions, s);
        }
        raws_drop(&raws);
    }
    jz_sort(versions.ptr, versions.len, sizeof(rv_sort), rv_cmp, NULL);
    for (u32 i = 0; i < versions.len; i++) JZ_VEC_PUSH(*out, (void *)versions.ptr[i].v);
out:
    jz_free(table_ids.ptr);
    jz_free(versions.ptr);
    return e;
}

typedef struct gt_tx { jazz_types_GlobalTime gt; jazz_model_TxId tx; } gt_tx;
static int gt_tx_cmp(const void *a, const void *b, void *c) {
    const gt_tx *x = a, *y = b;
    if (x->gt != y->gt) return x->gt < y->gt ? -1 : 1;
    return jazz_model_TxId_cmp(&x->tx, &y->tx);
}

static jz_err jazz_node__node__recovery__NodeState__recover_from_storage_inner(NS *self) {
    jz_str jazz_nodes = jz_str_from("jazz_nodes");
    groove_RawRowVec raws = {0};
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw(jzn_db(self), &jazz_nodes, NULL, 0, &raws);
    jz_str_drop(&jazz_nodes);
    JZ_TRY(e);
    for (u32 i = 0; i < raws.len; i++) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        u64 alias; jz_uuid uuid;
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_NAR_FIELD_ID_IDX, &alias))) { raws_drop(&raws); return e; }
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_uuid(record, JZN_NAR_FIELD_UUID_IDX, &uuid))) { raws_drop(&raws); return e; }
        const jazz_types_NodeAlias *existing = jazz_node__node__node_aliases__NodeAliases__get(&self->node_aliases, &uuid);
        if (existing && *existing != alias) { raws_drop(&raws); return JZN_INVALID_STORED("node UUID has conflicting durable aliases"); }
        jazz_types_NodeUuid existing_uuid;
        if (jazz_node__node__node_aliases__NodeAliases__node_for_alias(&self->node_aliases, alias, &existing_uuid) && !jz_uuid_eq(&existing_uuid, &uuid)) {
            raws_drop(&raws); return JZN_INVALID_STORED("node alias maps to multiple durable UUIDs");
        }
        jazz_node__node__node_aliases__NodeAliases__insert(&self->node_aliases, uuid, alias, NULL);
    }
    raws_drop(&raws);
    /* alias_to_node: BTreeMap<NodeAlias, NodeUuid> */
    static const jz_btree_desc desc_alias_node = { sizeof(u64), sizeof(jz_uuid), jz_cmp_u64, NULL, NULL };
    jz_btree alias_to_node;
    jz_btree_init(&alias_to_node, &desc_alias_node);
    {
        jz_uuid *node; u64 *alias;
        JZ_BTREE_FOR(&self->node_aliases.by_node, node, alias) jz_btree_insert(&alias_to_node, alias, node, NULL);
    }

    jz_str jazz_transactions = jz_str_from("jazz_transactions");
    groove_RawRow *last = NULL;
    if (JZ_IS_ERR(e = groove__db__primary_storage__Database__primary_key_last_raw(jzn_db(self), &jazz_transactions, NULL, 0, &last))) goto out;
    if (last) {
        u64 t;
        e = groove__records__BorrowedRecord__get_u64(groove__db__RawRow__record(last), JZN_TXR_FIELD_TIME_IDX, &t);
        groove__db__RawRow__drop(last);
        if (JZ_IS_ERR(e)) goto out;
        jazz_node__node__NodeState__merge_tx_time(self, t);
    }

    JZ_VEC(gt_tx) accepted_global_times = {0};
    groove_Value first_inner = { GROOVE_V_U64 }; first_inner.v.u = 0;
    groove_Value last_inner = { GROOVE_V_U64 }; last_inner.v.u = UINT64_MAX;
    groove_Value first_global_time = { GROOVE_V_NULLABLE }; first_global_time.v.nullable = &first_inner;
    groove_Value last_global_time = { GROOVE_V_NULLABLE }; last_global_time.v.nullable = &last_inner;
    groove_RawRowVec sequenced = {0}, exact = {0};
    if (JZ_IS_ERR(e = groove__db__primary_storage__Database__index_scan_range_raw(jzn_db(self), &jazz_transactions, "by_global_time",
                                                                                 &first_global_time, 1, &last_global_time, 1, &sequenced))) goto out;
    if (JZ_IS_ERR(e = groove__db__primary_storage__Database__index_scan_raw(jzn_db(self), &jazz_transactions, "by_global_time", &last_global_time, 1, &exact))) { raws_drop(&sequenced); goto out; }
    jz_vec_extend(&sequenced, exact.ptr, exact.len, sizeof(void *));
    jz_free(exact.ptr);
    for (u32 i = 0; i < sequenced.len; i++) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(sequenced.ptr[i]);
        bool has_gt; u64 global_time;
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_nullable_u64(record, JZN_TXR_FIELD_GLOBAL_TIME_IDX, &has_gt, &global_time))) break;
        if (has_gt) {
            u8 d; jazz_model_DurabilityTier dur;
            if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_enum(record, JZN_TXR_FIELD_DURABILITY_IDX, &d))) break;
            if (JZ_IS_ERR(e = jazz_node__node__codec__durability_from_discriminant(d, &dur))) break;
            if (dur != JAZZ_DUR_GLOBAL) { e = JZN_INVALID_STORED("global timestamp requires Global durability"); break; }
        }
        jazz_model_Fate fate;
        if (JZ_IS_ERR(e = jazz_node__node__codec__fate_from_encoded_fields(record, &fate))) break;
        if (fate.tag != JAZZ_FATE_ACCEPTED) continue;
        if (has_gt) {
            u64 node_alias, time;
            if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_NODE_ID_IDX, &node_alias))) break;
            jazz_types_NodeUuid node;
            if (!jazz_node__node__NodeState__node_for_alias(self, node_alias, &node)) { e = JZN_INVALID_STORED("transaction node alias must exist"); break; }
            if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_TIME_IDX, &time))) break;
            gt_tx g = { global_time, { time, node } };
            JZ_VEC_PUSH(accepted_global_times, g);
        }
    }
    raws_drop(&sequenced);
    if (JZ_IS_ERR(e)) { jz_free(accepted_global_times.ptr); goto out; }
    jz_sort(accepted_global_times.ptr, accepted_global_times.len, sizeof(gt_tx), gt_tx_cmp, NULL);
    {
        u32 w = 0;
        for (u32 i = 0; i < accepted_global_times.len; i++)
            if (w == 0 || gt_tx_cmp(&accepted_global_times.ptr[w - 1], &accepted_global_times.ptr[i], NULL) != 0) accepted_global_times.ptr[w++] = accepted_global_times.ptr[i];
        accepted_global_times.len = w;
    }
    for (u32 i = 0; i < accepted_global_times.len; i++) {
        jazz_types_GlobalTime newly; bool has_newly;
        (void)jazz_node__node__open_tx__NodeState__record_applied_global_time(self, accepted_global_times.ptr[i].gt, accepted_global_times.ptr[i].tx, &newly, &has_newly);
    }
    jz_free(accepted_global_times.ptr);
    if (self->history_complete) {
        self->clock.committed_global_time = self->clock.global_time_register;
        jz_btree_clear(&self->clock.applied_global_times_after_frontier);
        jz_btree_clear(&self->clock.frontier_dots);
    }

    typedef struct { jazz_types_NodeAlias alias; jazz_model_TxId tx; groove_OwnedRecord *record; } header;
    JZ_VEC(header) rejected_headers = {0};
    jz_str jazz_rejected = jz_str_from("jazz_rejected_transactions");
    e = groove__db__primary_storage__Database__primary_key_scan_raw(jzn_db(self), &jazz_rejected, NULL, 0, &raws);
    jz_str_drop(&jazz_rejected);
    if (JZ_IS_ERR(e)) goto out;
    for (u32 i = 0; i < raws.len; i++) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        u64 node_alias, time;
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_RTR_FIELD_NODE_ID_IDX, &node_alias))) break;
        const jz_uuid *node = jz_btree_get(&alias_to_node, &node_alias);
        if (!node) { e = JZN_INVALID_STORED("rejected transaction node alias must exist"); break; }
        if (!jz_uuid_eq(node, &self->node_uuid)) continue;
        if (JZ_IS_ERR(e = groove__records__BorrowedRecord__get_u64(record, JZN_RTR_FIELD_TIME_IDX, &time))) break;
        jz_slice rb = groove__db__RawRow__raw(raws.ptr[i]);
        header h = { node_alias, { time, *node },
                     groove__records__OwnedRecord__new(jz_bytes_from(rb.ptr, rb.len), groove__records__RecordDescriptor__clone_box(groove__records__OwnedRecord__descriptor(record))) };
        JZ_VEC_PUSH(rejected_headers, h);
    }
    raws_drop(&raws);
    for (u32 i = 0; i < rejected_headers.len && !JZ_IS_ERR(e); i++) {
        jzn_ptrvec versions = {0};
        e = jazz_node__node__recovery__NodeState__rejected_versions_for(self, rejected_headers.ptr[i].alias, rejected_headers.ptr[i].tx, &versions);
        if (JZ_IS_ERR(e)) break;
        jazz_model_RejectedTransaction *rt = jazz_model__tx__RejectedTransaction__new(rejected_headers.ptr[i].tx, rejected_headers.ptr[i].record, &versions);
        rejected_headers.ptr[i].record = NULL;
        jz_btree_insert(&self->rejections.rejected_transactions, &rejected_headers.ptr[i].tx, &rt, NULL);
    }
    for (u32 i = 0; i < rejected_headers.len; i++) if (rejected_headers.ptr[i].record) groove__records__OwnedRecord__drop(rejected_headers.ptr[i].record);
    jz_free(rejected_headers.ptr);
out:
    jz_str_drop(&jazz_transactions);
    jz_btree_drop(&alias_to_node);
    return e;
}

jz_err jazz_node__node__recovery__NodeState__recover_from_storage(NS *self) {
    return jazz_node__node__recovery__NodeState__recover_from_storage_inner(self);
}
