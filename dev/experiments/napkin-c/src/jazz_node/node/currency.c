/* Port of crates/jazz/layers/node/src/node/currency.rs
 *
 * Currency and version-selection reads over local and global storage. This
 * module owns row-version scans, winner lookup, transaction lookup, and
 * history queries that implement the merge/currency rules in `jazz/README.md`;
 * write ingestion lives in `ingest`, read-only global derivations in
 * `global_state`, and storage encoding in `codec`. It is a node-level read
 * layer over groove tables.
 *
 * Generic `impl<S: OrderedKvStorage> NodeState<S>` is one C copy; async is
 * synchronous. The cfg(test) decode counters and query_all_versions are omitted.
 * `query_transaction_fields<T>` (generic over a decode closure) becomes a C
 * function taking a decode callback + context. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

static const jz_str JAZZ_TRANSACTIONS = { (char *)"jazz_transactions", 17, 0 };
static const jz_str JAZZ_NODES = { (char *)"jazz_nodes", 10, 0 };

typedef struct { jazz_types_TxTime time; jazz_types_NodeAlias alias; } tx_alias_key;

static void vr_vec_drop(jazz_node_VersionRowVec *v) { jz_vec_drop(v, sizeof(jazz_node_VersionRow *), jazz_node__node__codec__VersionRow__drop_v); }
static void raw_vec_drop(groove_RawRowVec *v) {
    for (u32 i = 0; i < v->len; i++) groove__db__RawRow__drop(v->ptr[i]);
    jz_vec_drop(v, sizeof(groove_RawRow *), NULL);
}
static void values_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove__records__values__Value__drop); }
static groove_Value v_bytes(jz_bytes b) { groove_Value v = { GROOVE_V_BYTES }; v.v.bytes = b; return v; }
static groove_Value v_uuid(jz_uuid u) { groove_Value v = { GROOVE_V_UUID }; v.v.uuid = u; return v; }
static groove_Value v_u64(u64 x) { groove_Value v = { GROOVE_V_U64 }; v.v.u = x; return v; }
static jz_bytes branch_bytes(const jazz_model_BranchKey *k) { jz_bytes b = { 0 }; jazz_model__model__branch__BranchKey__canonical_bytes(k, &b); return b; }
static jz_err table_not_found(const jz_str *t) { return jz_err_fmt(JZN_E_TABLE_NOT_FOUND, "%.*s", (int)t->len, t->ptr); }

/* versions.sort_by_key(|v| (v.row_uuid(), version_tx_id_from_aliases(v, aliases).expect(..))) */
static int cmp_row_then_tx(const void *a, const void *b, void *ctx) {
    const jazz_node_VersionRow *x = *(jazz_node_VersionRow *const *)a, *y = *(jazz_node_VersionRow *const *)b;
    jazz_types_RowUuid rx = jazz_node__node__codec__VersionRow__row_uuid(x), ry = jazz_node__node__codec__VersionRow__row_uuid(y);
    int c = jz_uuid_cmp(&rx, &ry);
    if (c) return c;
    jazz_model_TxId tx, ty;
    jz_err e1 = jazz_node__node__codec__version_tx_id_from_aliases(x, ctx, &tx);
    jz_err e2 = jazz_node__node__codec__version_tx_id_from_aliases(y, ctx, &ty);
    if (e1.code || e2.code) jz_panic("valid version tx id");
    return jazz_model_TxId_cmp(&tx, &ty);
}
static void sort_by_row_then_tx(NS *self, jazz_node_VersionRowVec *v) {
    jz_sort(v->ptr, v->len, sizeof(jazz_node_VersionRow *), cmp_row_then_tx, &self->node_aliases);
}
/* left.table().cmp(right.table()).then_with(|| left.row_uuid().cmp(&right.row_uuid())) */
static int cmp_table_then_row(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const jazz_node_VersionRow *x = *(jazz_node_VersionRow *const *)a, *y = *(jazz_node_VersionRow *const *)b;
    int c = jz_str_cmp(jazz_node__node__codec__VersionRow__table(x), jazz_node__node__codec__VersionRow__table(y));
    if (c) return c;
    jazz_types_RowUuid rx = jazz_node__node__codec__VersionRow__row_uuid(x), ry = jazz_node__node__codec__VersionRow__row_uuid(y);
    return jz_uuid_cmp(&rx, &ry);
}

jz_err jazz_node__node__currency__NodeState__decode_stored_history_record(NS *self, const groove_DatabaseBatch *batch, const jz_str *requested_table,
                                                                          const jz_str *storage_table, groove_OwnedRecord *record, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__decode_stored_physical_history_record(NS *self, const jz_str *storage_table, groove_OwnedRecord *record,
                                                                                   jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__version_storage_sources(NS *self, const jz_str *table, jzn_strvec *out);
jz_err jazz_node__node__currency__NodeState__decode_history_owned_record(NS *self, const jz_str *requested_table, const jz_str *storage_table,
                                                                        groove_OwnedRecord *record, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_version_by_alias_in_branch(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
    const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_version_by_alias_with_storage_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jz_str *storage_table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time,
    jazz_types_NodeAlias alias, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__history_image_from_current_record(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
    u32 variant_tag, const groove_OwnedRecord *current, jazz_node_VersionRow **out);

/* Scan `storage tables` of `table` with `prefix` and decode every record. */
static jz_err scan_versions(NS *self, const jz_str *table, const groove_Value *prefix, u32 nprefix, jazz_node_VersionRowVec *versions) {
    jzn_strvec sources = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__version_storage_sources(self, table, &sources));
    jz_err e = JZ_OK;
    for (u32 s = 0; s < sources.len && !e.code; s++) {
        const jz_str *storage_table = &sources.ptr[s];
        groove_RawRowVec raws = { 0 };
        e = groove__db__primary_storage__Database__primary_key_scan_raw(jzn_db(self), storage_table, prefix, nprefix, &raws);
        if (e.code) break;
        for (u32 i = 0; i < raws.len; i++) {
            groove_OwnedRecord *record = groove__db__RawRow__owned_record(raws.ptr[i]);
            if (e.code) { groove__records__OwnedRecord__drop(record); continue; }
            jazz_node_VersionRow *v;
            e = jazz_node__node__currency__NodeState__decode_stored_history_record(self, NULL, table, storage_table, record, &v);
            if (!e.code) JZ_VEC_PUSH(*versions, v);
        }
        raw_vec_drop(&raws);
    }
    jz_vec_drop(&sources, sizeof(jz_str), jzn_str_drop_v);
    return e;
}

jz_err jazz_node__node__currency__NodeState__query_row_versions_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                                                          jazz_types_RowUuid row_uuid, jazz_node_VersionRowVec *out) {
    jazz_node_VersionRowVec versions = { 0 };
    groove_Value prefix[2] = { v_bytes(branch_bytes(branch_key)), v_uuid(row_uuid) };
    jz_err e = scan_versions(self, table, prefix, 2, &versions);
    jz_bytes_drop(&prefix[0].v.bytes);
    if (e.code) { vr_vec_drop(&versions); return e; }
    sort_by_row_then_tx(self, &versions);
    *out = versions;
    return JZ_OK;
}

/* #[allow(dead_code)] Stage 1 read primitive. */
jz_err jazz_node__node__currency__NodeState__query_row_versions(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRowVec *out) {
    jazz_model_BranchKey *k = jazz_model__model__branch__BranchKey__default();
    jz_err e = jazz_node__node__currency__NodeState__query_row_versions_in_branch(self, table, k, row_uuid, out);
    jazz_model__model__branch__BranchKey__drop(k);
    return e;
}

jz_err jazz_node__node__currency__NodeState__query_versions_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                      const jazz_types_RowUuid *row_uuid, jazz_node_VersionRowVec *out) {
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, schema_version, table, &table_id));
    jazz_model_BranchKey *branch = jazz_model__model__branch__BranchKey__default();
    groove_Value content_prefix[2] = { v_bytes(branch_bytes(branch)) };
    u32 nprefix = 1;
    jazz_model__model__branch__BranchKey__drop(branch);
    if (row_uuid) content_prefix[nprefix++] = v_uuid(*row_uuid);
    jazz_node_VersionRowVec versions = { 0 };
    jz_str storage_table = jazz_node__node__physical__physical_history_table_name(table_id);
    groove_RawRowVec records = { 0 };
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw(jzn_db(self), &storage_table, content_prefix, nprefix, &records);
    for (u32 i = 0; !e.code && i < records.len; i++) {
        /* Keep the authoring name; the schema selected the physical
         * identity before the scan, including after a table rename. */
        jazz_node_VersionRow *v;
        e = jazz_node__node__currency__NodeState__decode_stored_physical_history_record(self, &storage_table, groove__db__RawRow__owned_record(records.ptr[i]), &v);
        if (!e.code) JZ_VEC_PUSH(versions, v);
    }
    raw_vec_drop(&records);
    jz_str_drop(&storage_table);
    jz_bytes_drop(&content_prefix[0].v.bytes);
    if (e.code) { vr_vec_drop(&versions); return e; }
    sort_by_row_then_tx(self, &versions);
    *out = versions;
    return JZ_OK;
}

jz_err jazz_node__node__currency__NodeState__query_table_versions_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                                                            jazz_node_VersionRowVec *out) {
    jazz_node_VersionRowVec versions = { 0 };
    groove_Value prefix[1] = { v_bytes(branch_bytes(branch_key)) };
    jz_err e = scan_versions(self, table, prefix, 1, &versions);
    jz_bytes_drop(&prefix[0].v.bytes);
    if (e.code) { vr_vec_drop(&versions); return e; }
    sort_by_row_then_tx(self, &versions);
    *out = versions;
    return JZ_OK;
}

jz_err jazz_node__node__currency__NodeState__query_winner_from_pk(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);

/* The row's local winner (its newest pending write, else its newest
 * accepted one). Update and delete permission advice read it through
 * `local_current_row_exists`. */
jz_err jazz_node__node__currency__NodeState__query_local_winner(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    return jazz_node__node__currency__NodeState__query_winner_from_pk(self, table, row_uuid, out);
}

static jz_err query_current_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                             jazz_node_PhysicalCurrentClass cls, jazz_node_VersionRow **out);

jz_err jazz_node__node__currency__NodeState__query_local_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                                                          jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    /* The pending overlay, when the row has one, is the local row: it is
     * the synced image with every pending patch folded on top. */
    return query_current_winner_in_branch(self, table, branch_key, row_uuid, JZN_PHYS_CLASS_AHEAD, out);
}

jz_err jazz_node__node__currency__NodeState__query_global_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                                                           jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    return query_current_winner_in_branch(self, table, branch_key, row_uuid, JZN_PHYS_CLASS_GLOBAL, out);
}

/* The row as this node sees it locally: the pending overlay when the row
 * has one, and the synced (global current) image otherwise. A settled
 * row has no overlay, so an overlay-only read would miss it entirely. */
jz_err jazz_node__node__currency__NodeState__query_local_view_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                                                               jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    jazz_node_VersionRow *overlay = NULL;
    JZ_TRY(jazz_node__node__currency__NodeState__query_local_winner_in_branch(self, table, branch_key, row_uuid, &overlay));
    if (overlay) { *out = overlay; return JZ_OK; }
    return jazz_node__node__currency__NodeState__query_global_winner_in_branch(self, table, branch_key, row_uuid, out);
}

/* Return the newest locally known version for a row/layer except one
 * candidate transaction.  Authority finalization persists its candidate
 * before assigning fate, so policy classification must be able to find
 * the next older pending or accepted version rather than falling straight
 * through to global current state. */
jz_err jazz_node__node__currency__NodeState__query_local_winner_in_branch_excluding_tx(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
    jazz_types_RowUuid row_uuid, jazz_model_TxId excluded_tx_id, jazz_node_VersionRow **out) {
    *out = NULL;
    jazz_node_VersionRowVec candidates = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__query_row_versions_in_branch(self, table, branch_key, row_uuid, &candidates));
    jazz_node_VersionRow *winner = NULL;
    jz_err e = JZ_OK;
    for (u32 i = 0; i < candidates.len; i++) {
        jazz_node_VersionRow *candidate = candidates.ptr[i];
        candidates.ptr[i] = NULL;
        jazz_model_TxId candidate_tx;
        if (!e.code) e = jazz_node__node__NodeState__version_tx_id(self, candidate, &candidate_tx);
        if (e.code || jazz_model_TxId_eq(&candidate_tx, &excluded_tx_id)) { jazz_node__node__codec__VersionRow__drop(candidate); continue; }
        bool replace = true;
        if (winner) {
            jazz_model_TxId current_tx;
            e = jazz_node__node__NodeState__version_tx_id(self, winner, &current_tx);
            if (e.code) { jazz_node__node__codec__VersionRow__drop(candidate); continue; }
            jazz_model_TxId ck = { jazz_node__node__codec__VersionRow__tx_time(candidate), candidate_tx.node };
            jazz_model_TxId wk = { jazz_node__node__codec__VersionRow__tx_time(winner), current_tx.node };
            replace = jazz_model_TxId_cmp(&ck, &wk) > 0;
        }
        if (replace) { if (winner) jazz_node__node__codec__VersionRow__drop(winner); winner = candidate; }
        else jazz_node__node__codec__VersionRow__drop(candidate);
    }
    jz_vec_drop(&candidates, sizeof(jazz_node_VersionRow *), NULL);
    if (e.code) { if (winner) jazz_node__node__codec__VersionRow__drop(winner); return e; }
    *out = winner;
    return JZ_OK;
}

jz_err jazz_node__node__currency__NodeState__query_global_winner_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                           jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);

/* #[allow(dead_code)] Stage 1 read primitive. */
jz_err jazz_node__node__currency__NodeState__query_global_winner(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    const jazz_model_TableSchema *t;
    jazz_types_SchemaVersionId schema_version;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, table, self->catalogue.active_schema.schema, &t);
    if (!e.code) {
        schema_version = self->catalogue.active_schema.schema;
    } else {
        jz_err_drop(&e);
        JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, table, self->catalogue.local_schema_version_id, &t));
        schema_version = self->catalogue.local_schema_version_id;
    }
    return jazz_node__node__currency__NodeState__query_global_winner_in_schema(self, schema_version, table, row_uuid, out);
}

/* Shared tail of query_current_winner_in_branch and
 * query_global_winner_in_schema_and_branch. */
static jz_err winner_from_current_table(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const jz_str *current_table,
                                        const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    *out = NULL;
    groove_Value key[2] = { v_bytes(branch_bytes(branch_key)), v_uuid(row_uuid) };
    groove_RawRow *raw = NULL;
    jz_err e = groove__db__primary_storage__Database__primary_key_get_raw(jzn_db(self), current_table, key, 2, &raw);
    jz_bytes_drop(&key[0].v.bytes);
    if (e.code || !raw) return e;
    u32 variant_tag = groove__db__RawRow__variant_tag(raw);
    groove_OwnedRecord *current = groove__db__RawRow__owned_record(raw);
    groove__db__RawRow__drop(raw);
    jazz_node_VersionRow *winner = NULL;
    e = jazz_node__node__currency__NodeState__history_image_from_current_record(self, schema_version, table, variant_tag, current, &winner);
    if (e.code || winner) { groove__records__OwnedRecord__drop(current); *out = winner; return e; }
    u64 tx_time, tx_node_alias;
    e = groove__records__BorrowedRecord__get_u64(current, JZN_GCR_FIELD_TX_TIME_IDX, &tx_time);
    if (!e.code) e = groove__records__BorrowedRecord__get_u64(current, JZN_GCR_FIELD_TX_NODE_ID_IDX, &tx_node_alias);
    groove__records__OwnedRecord__drop(current);
    if (e.code) return e;
    return jazz_node__node__currency__NodeState__query_version_by_alias_in_branch(self, schema_version, table, branch_key, row_uuid, tx_time, tx_node_alias, out);
}

static jz_err query_current_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                             jazz_node_PhysicalCurrentClass cls, jazz_node_VersionRow **out) {
    jazz_types_SchemaVersionId schema_version = self->catalogue.active_schema.schema;
    jz_str current_table = { 0 };
    JZ_TRY(jazz_node__node__physical__NodeState__physical_current_table_for_schema(self, schema_version, table, cls, &current_table));
    jz_err e = winner_from_current_table(self, schema_version, table, &current_table, branch_key, row_uuid, out);
    jz_str_drop(&current_table);
    return e;
}

jz_err jazz_node__node__currency__NodeState__query_global_winner_in_schema_and_branch(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
    const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    jz_str current_table = { 0 };
    JZ_TRY(jazz_node__node__physical__NodeState__physical_current_table_for_schema(self, schema_version, table, JZN_PHYS_CLASS_GLOBAL, &current_table));
    jz_err e = winner_from_current_table(self, schema_version, table, &current_table, branch_key, row_uuid, out);
    jz_str_drop(&current_table);
    return e;
}

/* Read the global winner through a specified schema's physical lineage.
 * Incoming historical versions retain their authored table literal, which
 * need not exist in the currently selected write/read schema after a rename. */
jz_err jazz_node__node__currency__NodeState__query_global_winner_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                           jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    jazz_model_BranchKey *k = jazz_model__model__branch__BranchKey__default();
    jz_err e = jazz_node__node__currency__NodeState__query_global_winner_in_schema_and_branch(self, schema_version, table, k, row_uuid, out);
    jazz_model__model__branch__BranchKey__drop(k);
    return e;
}

/* --- history_image_from_current_record --- */
typedef struct {
    const groove_RecordDescriptor *history_descriptor;
    const groove_RecordDescriptor *current_descriptor;
    const groove_OwnedRecord *current;
    u32 signs_idx, nfields;
} image_ctx;

static u32 source_of(u32 index) { return index >= JZN_GCR_FIELD_GLOBAL_TIME_IDX ? index + 1 : index; }

static jz_err image_encode_field(u32 index, jz_bytes *output, void *vctx) {
    image_ctx *c = vctx;
    const groove_DescriptorFieldVec *hf = groove__records__RecordDescriptor__fields(c->history_descriptor);
    if (index >= c->signs_idx && index < c->nfields) {
        const groove_DescriptorField *f = &hf->ptr[index];
        groove_Value value;
        if (f->has_name && jz_str_eq_cstr(&f->name, JAZZ_SEQ_FIELD)) {
            bool some; u64 gt = 0;
            JZ_TRY(groove__records__BorrowedRecord__get_nullable_u64(c->current, JZN_GCR_FIELD_GLOBAL_TIME_IDX, &some, &gt));
            value = v_u64(some ? gt : 0);
        } else if (f->has_name && (jz_str_eq_cstr(&f->name, JAZZ_BASE_SEQ_FIELD) || jz_str_eq_cstr(&f->name, JAZZ_BASE_PENDING_FIELD))) {
            value.tag = GROOVE_V_NULLABLE;
            value.v.nullable = NULL;
        } else {
            value = v_bytes((jz_bytes){ 0 });
        }
        return groove__records__RecordDescriptor__encode_field_into(c->history_descriptor, index, &value, output);
    }
    if (index == JZN_HRR_FIELD_UPDATED_BY_IDX) {
        groove_Value author;
        JZ_TRY(groove__records__BorrowedRecord__get_idx(c->current, JZN_GCR_FIELD_UPDATED_BY_IDX, &author));
        groove_Value nullable = { GROOVE_V_NULLABLE };
        nullable.v.nullable = JZ_NEW(groove_Value);
        *nullable.v.nullable = author;
        jz_err e = groove__records__RecordDescriptor__encode_field_into(c->history_descriptor, index, &nullable, output);
        groove__records__values__Value__drop(&nullable);
        return e;
    }
    bool has_ms = false;
    u64 ms = 0;
    if (index == JZN_HRR_FIELD_CREATED_AT_IDX) { JZ_TRY(groove__records__BorrowedRecord__get_u64(c->current, JZN_GCR_FIELD_CREATED_AT_IDX, &ms)); has_ms = true; }
    else if (index == JZN_HRR_FIELD_UPDATED_AT_IDX) { JZ_TRY(groove__records__BorrowedRecord__get_u64(c->current, JZN_GCR_FIELD_UPDATED_AT_IDX, &ms)); has_ms = true; }
    if (has_ms) {
        jazz_types_TxTime time;
        jz_err te = jazz_types__time__TxTime__from_physical_ms(ms, &time);
        if (te.code) { jz_err_drop(&te); return JZN_INVALID_STORED("current provenance ms exceeds HLC range"); }
        groove_Value tv = v_u64(time);
        return groove__records__RecordDescriptor__encode_field_into(c->history_descriptor, index, &tv, output);
    }
    u32 source = source_of(index), start, end;
    jz_slice raw = groove__records__OwnedRecord__raw(c->current);
    JZ_TRY(groove__records__RecordDescriptor__field_span(c->current_descriptor, raw, source, &start, &end));
    jz_bytes_push(output, raw.ptr + start, end - start);
    return JZ_OK;
}

/* Global current stores the winner's full body, so the winner is read
 * from it rather than re-fetched from history by `(tx_time, node)`.
 * The two layouts differ only in the `global_time` cell and the
 * provenance timestamps: current stores whole milliseconds, history the
 * same instant as an HLC. Provenance HLCs are always minted from
 * milliseconds (logical counter zero), so the conversion is lossless.
 *
 * The image is built in the history layout of the current row's own
 * schema variant (`variant_tag`), not the physical table's widest
 * layout. An ahead-current row is the synced image with pending patches
 * folded on top, so it has no history row equal to it. */
jz_err jazz_node__node__currency__NodeState__history_image_from_current_record(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
    u32 variant_tag, const groove_OwnedRecord *current, jazz_node_VersionRow **out) {
    *out = NULL;
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, schema_version, table, &table_id));
    jz_str history_table = jazz_node__node__physical__physical_history_table_name(table_id);
    const groove_TableSchema *ts;
    jz_err e = groove__db__schema_admission__Database__table_schema(jzn_db(self), &history_table, &ts);
    if (e.code) { jz_str_drop(&history_table); return e; }
    const groove_RecordDescriptor *history_descriptor = groove__schema__TableSchema__record_schema_for_variant(ts, variant_tag);
    if (!history_descriptor) { jz_str_drop(&history_table); return JZ_OK; }
    const groove_RecordDescriptor *current_descriptor = groove__records__OwnedRecord__descriptor(current);
    /* History also ends with `counter_signs` and the merge fields, which
     * current lacks. */
    u32 signs_idx;
    if (!groove__records__RecordDescriptor__field_index(history_descriptor, JAZZ_COUNTER_SIGNS_FIELD, &signs_idx)) { jz_str_drop(&history_table); return JZ_OK; }
    const groove_DescriptorFieldVec *hf = groove__records__RecordDescriptor__fields(history_descriptor);
    const groove_DescriptorFieldVec *cf = groove__records__RecordDescriptor__fields(current_descriptor);
    u32 nsynth = hf->len - signs_idx;
    bool mismatch = cf->len + nsynth != hf->len + 1;
    for (u32 index = 0; !mismatch && index < hf->len; index++) {
        if (index >= signs_idx) continue;
        u32 source = source_of(index);
        const groove_ValueType *history_type = &hf->ptr[index].value_type;
        if (index == JZN_HRR_FIELD_UPDATED_BY_IDX && history_type->tag == GROOVE_VT_NULLABLE) history_type = history_type->v.inner;
        if (source >= cf->len || !groove__records__values__ValueType__eq(&cf->ptr[source].value_type, history_type)) mismatch = true;
    }
    if (mismatch) { jz_str_drop(&history_table); return JZ_OK; }
    image_ctx ctx = { history_descriptor, current_descriptor, current, signs_idx, hf->len };
    jz_bytes raw = { 0 };
    e = groove__records__RecordDescriptor__create_with_encoded_fields(history_descriptor, groove__records__OwnedRecord__raw(current).len, image_encode_field, &ctx, &raw);
    if (e.code) { jz_str_drop(&history_table); return e; }
    groove_OwnedRecord *record = groove__records__OwnedRecord__new(raw, groove__records__RecordDescriptor__clone_box(history_descriptor));
    e = jazz_node__node__currency__NodeState__decode_history_owned_record(self, table, &history_table, record, out);
    jz_str_drop(&history_table);
    return e;
}

jz_err jazz_node__node__currency__NodeState__query_winner_from_pk_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                                                            jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);

jz_err jazz_node__node__currency__NodeState__query_winner_from_pk(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    jazz_model_BranchKey *k = jazz_model__model__branch__BranchKey__default();
    jz_err e = jazz_node__node__currency__NodeState__query_winner_from_pk_in_branch(self, table, k, row_uuid, out);
    jazz_model__model__branch__BranchKey__drop(k);
    return e;
}

/* The row's local winner in history: its newest pending write when it
 * has one, and otherwise its newest accepted write. History is keyed by
 * seq with pending writes at seq 0, so pending writes sort first in the
 * row's key range and the last key there is the newest accepted write,
 * never a pending one: the pending range is read first. */
jz_err jazz_node__node__currency__NodeState__query_winner_from_pk_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                                                            jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out) {
    *out = NULL;
    /* (pending, seq, version): a pending write beats an accepted one, a
     * later seq beats an earlier one, then the newer transaction wins. */
    bool has_winner = false, w_pending = false;
    jazz_types_GlobalTime w_seq = 0;
    jazz_node_VersionRow *winner = NULL;
    jzn_strvec sources = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__version_storage_sources(self, table, &sources));
    jz_err e = JZ_OK;
    for (u32 s = 0; s < sources.len && !e.code; s++) {
        const jz_str *storage_table = &sources.ptr[s];
        groove_Value row_prefix[3] = { v_bytes(branch_bytes(branch_key)), v_uuid(row_uuid), v_u64(0) };
        bool found = false, pending = false;
        groove_OwnedRecord *raw = NULL;
        bool pendings[2] = { true, false };
        u32 lens[2] = { 3, 2 };
        for (int k = 0; k < 2 && !e.code; k++) {
            groove_RawRow *r = NULL;
            e = groove__db__primary_storage__Database__primary_key_last_raw(jzn_db(self), storage_table, row_prefix, lens[k], &r);
            if (!e.code && r) { raw = groove__db__RawRow__owned_record(r); groove__db__RawRow__drop(r); found = true; pending = pendings[k]; break; }
        }
        jz_bytes_drop(&row_prefix[0].v.bytes);
        if (e.code) break;
        if (!found) continue;
        jazz_node_VersionRow *candidate;
        e = jazz_node__node__currency__NodeState__decode_stored_history_record(self, NULL, table, storage_table, raw, &candidate);
        if (e.code) break;
        jazz_types_GlobalTime seq;
        e = jazz_node__node__codec__VersionRow__seq(candidate, &seq);
        bool replace = true;
        if (!e.code && has_winner) {
            jazz_model_TxId ctx_id, wtx_id;
            e = jazz_node__node__NodeState__version_tx_id(self, candidate, &ctx_id);
            if (!e.code) e = jazz_node__node__NodeState__version_tx_id(self, winner, &wtx_id);
            if (!e.code) {
                jazz_model_TxId ck = { jazz_node__node__codec__VersionRow__tx_time(candidate), ctx_id.node };
                jazz_model_TxId wk = { jazz_node__node__codec__VersionRow__tx_time(winner), wtx_id.node };
                int c = pending != w_pending ? (pending ? 1 : -1) : (seq != w_seq ? (seq > w_seq ? 1 : -1) : jazz_model_TxId_cmp(&ck, &wk));
                replace = c > 0;
            }
        }
        if (e.code) { jazz_node__node__codec__VersionRow__drop(candidate); break; }
        if (replace) {
            if (winner) jazz_node__node__codec__VersionRow__drop(winner);
            winner = candidate; has_winner = true; w_pending = pending; w_seq = seq;
        } else {
            jazz_node__node__codec__VersionRow__drop(candidate);
        }
    }
    jz_vec_drop(&sources, sizeof(jz_str), jzn_str_drop_v);
    if (e.code) { if (winner) jazz_node__node__codec__VersionRow__drop(winner); return e; }
    *out = winner;
    return JZ_OK;
}

/* (BranchKey, RowUuid, TxId) -> VersionRow* */
typedef struct { jazz_model_BranchKey *branch; jazz_types_RowUuid row; jazz_model_TxId tx; } version_key;
static int cmp_version_key(const void *a, const void *b, void *c) {
    (void)c;
    const version_key *x = a, *y = b;
    int r = jazz_model__model__branch__BranchKey__cmp(x->branch, y->branch);
    if (r) return r;
    r = jz_uuid_cmp(&x->row, &y->row);
    if (r) return r;
    return jazz_model_TxId_cmp(&x->tx, &y->tx);
}
static void version_key_drop(void *p) { jazz_model__model__branch__BranchKey__drop(((version_key *)p)->branch); }
static const jz_btree_desc desc_versions_by_key = { sizeof(version_key), sizeof(jazz_node_VersionRow *), cmp_version_key, version_key_drop, jazz_node__node__codec__VersionRow__drop_v };

jz_err jazz_node__node__currency__NodeState__query_table_versions(NS *self, const jz_str *table, jazz_node_VersionRowVec *out) {
    jz_btree versions_by_key;
    jz_btree_init(&versions_by_key, &desc_versions_by_key);
    jzn_strvec sources = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__version_storage_sources(self, table, &sources));
    jz_err e = JZ_OK;
    for (u32 s = 0; s < sources.len && !e.code; s++) {
        groove_RawRowVec raws = { 0 };
        e = groove__db__primary_storage__Database__primary_key_scan_raw(jzn_db(self), &sources.ptr[s], NULL, 0, &raws);
        for (u32 i = 0; !e.code && i < raws.len; i++) {
            jazz_node_VersionRow *version;
            e = jazz_node__node__currency__NodeState__decode_stored_history_record(self, NULL, table, &sources.ptr[s], groove__db__RawRow__owned_record(raws.ptr[i]), &version);
            if (e.code) break;
            version_key k;
            e = jazz_node__node__NodeState__version_tx_id(self, version, &k.tx);
            if (e.code) { jazz_node__node__codec__VersionRow__drop(version); break; }
            k.branch = jazz_model__model__branch__BranchKey__clone(jazz_node__node__codec__VersionRow__branch_key(version));
            k.row = jazz_node__node__codec__VersionRow__row_uuid(version);
            jazz_node_VersionRow *old;
            version_key oldk;
            if (jz_btree_get(&versions_by_key, &k)) {
                jz_btree_remove(&versions_by_key, &k, &oldk, &old);
                version_key_drop(&oldk);
                jazz_node__node__codec__VersionRow__drop(old);
            }
            jz_btree_insert(&versions_by_key, &k, &version, NULL);
        }
        raw_vec_drop(&raws);
    }
    jz_vec_drop(&sources, sizeof(jz_str), jzn_str_drop_v);
    if (e.code) { jz_btree_drop(&versions_by_key); return e; }
    jazz_node_VersionRowVec versions = { 0 };
    version_key k;
    jazz_node_VersionRow *v;
    while (jz_btree_pop_first(&versions_by_key, &k, &v)) { version_key_drop(&k); JZ_VEC_PUSH(versions, v); }
    jz_btree_drop(&versions_by_key);
    sort_by_row_then_tx(self, &versions);
    *out = versions;
    return JZ_OK;
}

void jazz_node__node__currency__NodeState__remember_history_tx_author_bytes(NS *self, tx_alias_key key, jz_bytes author);
jz_err jazz_node__node__currency__NodeState__remember_history_tx_author(NS *self, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, jazz_types_AuthorSubject made_by);

jz_err jazz_node__node__currency__NodeState__query_versions_for_tx(NS *self, jazz_model_TxId tx_id, jazz_node_VersionRowVec *out) {
    *out = (jazz_node_VersionRowVec){ 0 };
    bool found = false;
    jazz_node_StoredTransaction tx;
    JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &tx));
    if (!found) return JZ_OK;
    /* Its images omit `updated_by` when it is this author. */
    jz_err e = jazz_node__node__currency__NodeState__remember_history_tx_author(self, jazz_model__tx__Transaction__tx_id(tx.tx).time, tx.node_alias,
                                                                               *jazz_model__tx__Transaction__made_by(tx.tx));
    if (e.code) { jazz_node__node__codec__StoredTransaction__drop(&tx); return e; }
    jazz_node_VersionRowVec versions = { 0 };
    if (jazz_node__node__NodeState__cached_tx_versions(self, tx_id, &versions)) {
        jz_sort(versions.ptr, versions.len, sizeof(jazz_node_VersionRow *), cmp_table_then_row, NULL);
        jazz_node__node__codec__StoredTransaction__drop(&tx);
        *out = versions;
        return JZ_OK;
    }
    /* The transaction record lists every history row this node stored
     * for it; each is one exact history point read. A listed row that is
     * no longer stored (evicted) is skipped. */
    jazz_node_TouchedRowVec touched_rows = { 0 };
    e = jazz_node__node__NodeState__load_tx_touched_rows(self, NULL, tx_id.time, tx.node_alias, &touched_rows);
    jzn_u64vec seqs = { 0 };
    if (!e.code) jazz_node__node__codec__history_seq_candidates(tx.has_global_time, tx.global_time, &seqs);
    for (u32 i = 0; !e.code && i < touched_rows.len; i++) {
        const jazz_node_TouchedRow *row = &touched_rows.ptr[i];
        jz_str storage_table = jazz_node__node__physical__physical_history_table_name(row->table_id);
        groove_OwnedRecord *record = NULL;
        for (u32 s = 0; s < seqs.len && !e.code; s++) {
            groove_ValueVec key = { 0 };
            jazz_node__node__codec__history_key_values(jz_bytes_slice(&row->branch_key), row->row_uuid, seqs.ptr[s], tx_id.time, tx.node_alias, &key);
            groove_RawRow *raw = NULL;
            e = groove__db__primary_storage__Database__primary_key_get_raw(jzn_db(self), &storage_table, key.ptr, key.len, &raw);
            values_drop(&key);
            if (!e.code && raw) { record = groove__db__RawRow__owned_record(raw); groove__db__RawRow__drop(raw); break; }
        }
        if (!e.code && record) {
            jazz_node_VersionRow *v;
            e = jazz_node__node__currency__NodeState__decode_stored_physical_history_record(self, &storage_table, record, &v);
            if (!e.code) JZ_VEC_PUSH(versions, v);
        }
        jz_str_drop(&storage_table);
    }
    for (u32 i = 0; i < touched_rows.len; i++) jz_bytes_drop(&touched_rows.ptr[i].branch_key);
    jz_vec_drop(&touched_rows, sizeof(jazz_node_TouchedRow), NULL);
    jz_vec_drop(&seqs, sizeof(u64), NULL);
    jazz_node__node__codec__StoredTransaction__drop(&tx);
    if (e.code) { vr_vec_drop(&versions); return e; }
    jz_sort(versions.ptr, versions.len, sizeof(jazz_node_VersionRow *), cmp_table_then_row, NULL);
    *out = versions;
    return JZ_OK;
}

jz_err jazz_node__node__currency__NodeState__query_version_by_alias(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time,
                                                                    jazz_types_NodeAlias alias, jazz_node_VersionRow **out);

/* rows: set<(String, RowUuid)> */
jz_err jazz_node__node__currency__NodeState__query_versions_for_tx_rows_by_alias(NS *self, jazz_model_TxId tx_id, jazz_types_NodeAlias tx_node_alias,
                                                                                 const jz_btree *rows, jazz_node_VersionRowVec *out) {
    jazz_node_VersionRowVec versions = { 0 };
    typedef struct { jz_str table; jazz_types_RowUuid row; } table_row;
    const table_row *tr;
    void *unused;
    JZ_BTREE_FOR(rows, tr, unused) {
        (void)unused;
        jazz_node_VersionRow *version = NULL;
        jz_err e = jazz_node__node__currency__NodeState__query_version_by_alias(self, &tr->table, tr->row, tx_id.time, tx_node_alias, &version);
        if (e.code) { vr_vec_drop(&versions); return e; }
        if (version) JZ_VEC_PUSH(versions, version);
    }
    jz_sort(versions.ptr, versions.len, sizeof(jazz_node_VersionRow *), cmp_table_then_row, NULL);
    *out = versions;
    return JZ_OK;
}

static void physical_version_storage_sources(const NS *self, const jz_str *table, jzn_strvec *out) {
    const jz_uuid *schema;
    const jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&self->catalogue.physical_mappings, schema, mapping) {
        (void)schema;
        const jazz_node_TablePhysicalMapping *tm = jz_btree_get(&mapping->tables, table);
        if (tm) JZ_VEC_PUSH(*out, jazz_node__node__physical__physical_history_table_name(tm->table_id));
    }
}
static int cmp_str_v(const void *a, const void *b) { return jz_str_cmp(a, b); }
static void strvec_clone(jzn_strvec *dst, const jzn_strvec *src) {
    *dst = (jzn_strvec){ 0 };
    for (u32 i = 0; i < src->len; i++) JZ_VEC_PUSH(*dst, jz_str_clone(&src->ptr[i]));
}

jz_err jazz_node__node__currency__NodeState__version_storage_sources(NS *self, const jz_str *table, jzn_strvec *out) {
    const jzn_strvec *cached = jz_btree_get(&self->query.version_storage_sources_cache, table);
    if (cached) { strvec_clone(out, cached); return JZ_OK; }
    jzn_strvec sources = { 0 };
    physical_version_storage_sources(self, table, &sources);
    jz_sort(sources.ptr, sources.len, sizeof(jz_str), jz_cmp_str, NULL);
    jz_vec_dedup(&sources, sizeof(jz_str), cmp_str_v, jzn_str_drop_v);
    if (sources.len == 0) return table_not_found(table);
    jz_str cache_key = jz_str_clone(table);
    jzn_strvec cached_copy;
    strvec_clone(&cached_copy, &sources);
    jz_btree_insert(&self->query.version_storage_sources_cache, &cache_key, &cached_copy, NULL);
    *out = sources;
    return JZ_OK;
}

/* #[allow(dead_code)] Stage 1 read primitive. */
jz_err jazz_node__node__currency__NodeState__decode_history_record(NS *self, const jz_str *table, const groove_OwnedRecord *record, jazz_node_VersionRow **out) {
    static const jz_str empty = { (char *)"", 0, 0 };
    groove_OwnedRecord *owned = groove__records__OwnedRecord__clone(record);
    return jazz_node__node__currency__NodeState__decode_history_owned_record(self, table, &empty, owned, out);
}

jz_err jazz_node__node__currency__NodeState__resolve_history_updated_by(NS *self, const groove_DatabaseBatch *batch, jazz_node_VersionRow *version,
                                                                        jazz_node_VersionRow **out);

/* Decode a history image read from storage and fill in the `updated_by`
 * that storage omits when it is the author of the image's own transaction
 * (SPEC 2 §2.7.1). `batch` makes a transaction record staged in the open
 * batch visible. */
jz_err jazz_node__node__currency__NodeState__decode_stored_history_record(NS *self, const groove_DatabaseBatch *batch, const jz_str *requested_table,
                                                                          const jz_str *storage_table, groove_OwnedRecord *record, jazz_node_VersionRow **out) {
    jazz_node_VersionRow *version;
    JZ_TRY(jazz_node__node__currency__NodeState__decode_history_owned_record(self, requested_table, storage_table, record, &version));
    return jazz_node__node__currency__NodeState__resolve_history_updated_by(self, batch, version, out);
}

/* The encoded history `updated_by` field (`Nullable<RowAuthor>`) holding
 * `author`. A variable field's bytes do not depend on its position, so one
 * encoding serves every history table's field
 * `HistoryRowRecord::FIELD_UPDATED_BY_IDX`, and resolving an image copies
 * it instead of re-encoding the author. Consumes `author`. */
static jz_err encode_history_updated_by(groove_Value author, jz_bytes *out) {
    static const char *const names[] = { "updated_by" };
    groove_ValueType inner = jazz_types__ids__RowAuthor__value_type();
    groove_ValueType nullable = { GROOVE_VT_NULLABLE };
    nullable.v.inner = jz_memdup(&inner, sizeof inner);
    groove_RecordDescriptor *descriptor = groove__records__RecordDescriptor__new(names, &nullable, 1);
    groove_Value v = { GROOVE_V_NULLABLE };
    v.v.nullable = JZ_NEW(groove_Value);
    *v.v.nullable = author;
    jz_bytes encoded = { 0 };
    jz_err e = groove__records__RecordDescriptor__encode_field_into(descriptor, 0, &v, &encoded);
    groove__records__values__Value__drop(&v);
    groove__records__RecordDescriptor__drop_box(descriptor);
    if (e.code) { jz_bytes_drop(&encoded); return e; }
    *out = encoded;
    return JZ_OK;
}

void jazz_node__node__currency__NodeState__remember_history_tx_author_bytes(NS *self, tx_alias_key key, jz_bytes author) {
    if (self->history_tx_authors.len >= JZN_HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES && !jz_btree_contains(&self->history_tx_authors, &key))
        jz_btree_clear(&self->history_tx_authors);
    jz_bytes old;
    if (jz_btree_insert(&self->history_tx_authors, &key, &author, &old)) jz_bytes_drop(&old);
}

/* Cache a transaction's author for `resolve_history_updated_by`. */
jz_err jazz_node__node__currency__NodeState__remember_history_tx_author(NS *self, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias,
                                                                        jazz_types_AuthorSubject made_by) {
    groove_Value author;
    JZ_TRY(jazz_node__node__codec__row_author_value(made_by, &author));
    jz_bytes encoded;
    JZ_TRY(encode_history_updated_by(author, &encoded));
    tx_alias_key key = { tx_time, tx_node_alias };
    jazz_node__node__currency__NodeState__remember_history_tx_author_bytes(self, key, encoded);
    return JZ_OK;
}

/* Decode a record read from a `jazz_physical_{id}_history` table. The
 * logical table comes from the catalogue mapping of the record's own
 * schema version, so no table name is passed. */
jz_err jazz_node__node__currency__NodeState__decode_stored_physical_history_record(NS *self, const jz_str *storage_table, groove_OwnedRecord *record,
                                                                                   jazz_node_VersionRow **out) {
    jazz_types_PhysicalTableId id;
    if (!jazz_node__node__physical__physical_version_table_id(storage_table, &id)) {
        groove__records__OwnedRecord__drop(record);
        return JZN_INVALID_STORED("history record is not from a physical history table");
    }
    static const jz_str empty = { (char *)"", 0, 0 };
    return jazz_node__node__currency__NodeState__decode_stored_history_record(self, NULL, &empty, storage_table, record, out);
}

static jz_err load_history_tx_author(NS *self, const groove_DatabaseBatch *batch, tx_alias_key key, jz_bytes *out);

typedef struct { const groove_RecordDescriptor *descriptor; jz_slice raw; jz_slice author; } updated_by_ctx;
static jz_err updated_by_encode_field(u32 index, jz_bytes *output, void *vctx) {
    updated_by_ctx *c = vctx;
    if (index == JZN_HRR_FIELD_UPDATED_BY_IDX) {
        jz_bytes_push(output, c->author.ptr, c->author.len);
    } else {
        u32 start, end;
        JZ_TRY(groove__records__RecordDescriptor__field_span(c->descriptor, c->raw, index, &start, &end));
        jz_bytes_push(output, c->raw.ptr + start, end - start);
    }
    return JZ_OK;
}

/* Fill in `updated_by` from the `made_by` of the image's transaction
 * when history storage omitted it. Transaction authors are immutable,
 * so they are cached by `(tx_time, tx_node)`. Consumes `version`. */
jz_err jazz_node__node__currency__NodeState__resolve_history_updated_by(NS *self, const groove_DatabaseBatch *batch, jazz_node_VersionRow *version,
                                                                        jazz_node_VersionRow **out) {
    bool implicit;
    jz_err e = jazz_node__node__codec__VersionRow__updated_by_is_implicit(version, &implicit);
    if (e.code) { jazz_node__node__codec__VersionRow__drop(version); return e; }
    if (!implicit) { *out = version; return JZ_OK; }
    tx_alias_key key = { jazz_node__node__codec__VersionRow__tx_time(version), jazz_node__node__codec__VersionRow__tx_node_alias(version) };
    jz_bytes author = { 0 };
    const jz_bytes *cached = jz_btree_get(&self->history_tx_authors, &key);
    if (cached) author = jz_bytes_clone(cached);
    else {
        e = load_history_tx_author(self, batch, key, &author);
        if (e.code) { jazz_node__node__codec__VersionRow__drop(version); return e; }
    }
    const groove_OwnedRecord *input = jazz_node__node__codec__VersionRow__record(version);
    const groove_RecordDescriptor *descriptor = groove__records__OwnedRecord__descriptor(input);
    updated_by_ctx ctx = { descriptor, groove__records__OwnedRecord__raw(input), jz_bytes_slice(&author) };
    jz_bytes raw = { 0 };
    e = groove__records__RecordDescriptor__create_with_encoded_fields(descriptor, ctx.raw.len + author.len, updated_by_encode_field, &ctx, &raw);
    jz_bytes_drop(&author);
    if (e.code) { jazz_node__node__codec__VersionRow__drop(version); return e; }
    *out = jazz_node__node__codec__VersionRow__from_parts(jz_str_clone(jazz_node__node__codec__VersionRow__table(version)),
                                                         jazz_model__model__branch__BranchKey__clone(jazz_node__node__codec__VersionRow__branch_key(version)),
                                                         groove__records__OwnedRecord__new(raw, groove__records__RecordDescriptor__clone_box(descriptor)));
    jazz_node__node__codec__VersionRow__drop(version);
    return JZ_OK;
}

/* history_tx_seqs: HashMap<(TxTime, NodeAlias), GlobalTime> behind a RefCell. */
static jz_err remember_history_tx_seq(const NS *self, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, const groove_OwnedRecord *record,
                                      bool *some, jazz_types_GlobalTime *out) {
    u64 gt = 0;
    JZ_TRY(groove__records__BorrowedRecord__get_nullable_u64(record, JZN_TXR_FIELD_GLOBAL_TIME_IDX, some, &gt));
    if (*some) {
        jz_hmap *seqs = (jz_hmap *)&self->history_tx_seqs;
        if (seqs->len >= JZN_HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES) jz_hmap_clear(seqs);
        tx_alias_key k = { tx_time, alias };
        jz_hmap_insert(seqs, &k, &gt, NULL);
        *out = gt;
    }
    return JZ_OK;
}

static jz_err get_transaction_raw(const NS *self, const groove_DatabaseBatch *batch, jazz_types_TxTime t, jazz_types_NodeAlias a, groove_RawRow **raw) {
    groove_Value primary_key[2] = { v_u64(t), v_u64(a) };
    groove_Database *db = jazz_node__node__database_slot__DatabaseSlot__Deref__deref(&self->database);
    if (batch) return groove__db__primary_storage__Database__primary_key_get_raw_in_batch(db, batch, &JAZZ_TRANSACTIONS, primary_key, 2, raw);
    return groove__db__primary_storage__Database__primary_key_get_raw(db, &JAZZ_TRANSACTIONS, primary_key, 2, raw);
}

/* The seq a transaction's history records are stored at: its global
 * time once it has one (SPEC 2 §2.7.1). */
jz_err jazz_node__node__currency__NodeState__history_tx_seq(const NS *self, const groove_DatabaseBatch *batch, jazz_types_TxTime tx_time,
                                                            jazz_types_NodeAlias tx_node_alias, bool *some, jazz_types_GlobalTime *out) {
    tx_alias_key k = { tx_time, tx_node_alias };
    const u64 *seq = jz_hmap_get(&self->history_tx_seqs, &k);
    if (seq) { *some = true; *out = *seq; return JZ_OK; }
    *some = false;
    groove_RawRow *raw = NULL;
    JZ_TRY(get_transaction_raw(self, batch, tx_time, tx_node_alias, &raw));
    if (!raw) return JZ_OK;
    jz_err e = remember_history_tx_seq(self, tx_time, tx_node_alias, groove__db__RawRow__record(raw), some, out);
    groove__db__RawRow__drop(raw);
    return e;
}

static jz_err load_history_tx_author(NS *self, const groove_DatabaseBatch *batch, tx_alias_key key, jz_bytes *out) {
    groove_RawRow *raw = NULL;
    JZ_TRY(get_transaction_raw(self, batch, key.time, key.alias, &raw));
    if (!raw) return JZN_INVALID_STORED("a history image without updated_by needs its transaction record");
    const groove_OwnedRecord *record = groove__db__RawRow__record(raw);
    bool some; jazz_types_GlobalTime gt;
    jz_err e = remember_history_tx_seq(self, key.time, key.alias, record, &some, &gt);
    groove_Value made_by;
    if (!e.code) e = groove__records__BorrowedRecord__get_idx(record, JZN_TXR_FIELD_MADE_BY_IDX, &made_by);
    groove__db__RawRow__drop(raw);
    if (e.code) return e;
    jz_bytes author;
    JZ_TRY(encode_history_updated_by(made_by, &author));
    if (self->history_tx_authors.len >= JZN_HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES) jz_btree_clear(&self->history_tx_authors);
    jz_bytes cached = jz_bytes_clone(&author), old;
    if (jz_btree_insert(&self->history_tx_authors, &key, &cached, &old)) jz_bytes_drop(&old);
    *out = author;
    return JZ_OK;
}

static bool str_starts_with(const jz_str *s, const char *prefix) {
    usize n = strlen(prefix);
    return s->len >= n && memcmp(s->ptr, prefix, n) == 0;
}

/* Consumes `record`. */
jz_err jazz_node__node__currency__NodeState__decode_history_owned_record(NS *self, const jz_str *requested_table, const jz_str *storage_table,
                                                                        groove_OwnedRecord *record, jazz_node_VersionRow **out) {
    jz_err e;
    u64 schema_alias;
    if ((e = groove__records__BorrowedRecord__get_u64(record, JZN_HRR_FIELD_SCHEMA_VERSION_IDX, &schema_alias)).code) goto fail;
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, schema_alias, &schema_version)) {
        e = JZN_INVALID_STORED("version storage schema version alias must exist");
        goto fail;
    }
    jz_str table;
    if (!str_starts_with(storage_table, "jazz_physical_")) {
        table = jz_str_clone(requested_table);
    } else {
        jazz_types_PhysicalTableId table_id;
        if (!jazz_node__node__physical__physical_version_table_id(storage_table, &table_id)) {
            e = JZN_INVALID_STORED("physical version storage logical table mapping missing");
            goto fail;
        }
        const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema_version);
        const jz_str *found = NULL;
        if (mapping) {
            const jz_str *logical_table;
            const jazz_node_TablePhysicalMapping *tm;
            JZ_BTREE_FOR(&mapping->tables, logical_table, tm) { if (tm->table_id == table_id) { found = logical_table; break; } }
        }
        if (!found) { e = JZN_INVALID_STORED("physical version storage logical table mapping missing"); goto fail; }
        table = jz_str_clone(found);
    }
    const jazz_model_TableSchema *table_schema;
    if ((e = jazz_node__node__NodeState__table_in_schema_ref(self, &table, schema_version, &table_schema)).code) { jz_str_drop(&table); goto fail; }
    u64 tx_node_alias, tx_time;
    if ((e = groove__records__BorrowedRecord__get_u64(record, JZN_HRR_FIELD_TX_NODE_ID_IDX, &tx_node_alias)).code) { jz_str_drop(&table); goto fail; }
    jazz_types_NodeUuid tx_node;
    if (!jazz_node__node__node_aliases__NodeAliases__node_for_alias(&self->node_aliases, tx_node_alias, &tx_node)) {
        jz_str_drop(&table);
        e = JZN_INVALID_STORED("history tx node alias must exist");
        goto fail;
    }
    if ((e = groove__records__BorrowedRecord__get_u64(record, JZN_HRR_FIELD_TX_TIME_IDX, &tx_time)).code) { jz_str_drop(&table); goto fail; }
    jz_slice bk_bytes;
    if ((e = groove__records__BorrowedRecord__get_bytes(record, JZN_HRR_FIELD_BRANCH_KEY_IDX, &bk_bytes)).code) { jz_str_drop(&table); goto fail; }
    jazz_model_BranchKey *branch_key = NULL;
    jz_err be = jazz_model__schema__RuntimeSchema__decode_persisted_branch_key(table_schema, bk_bytes, &branch_key);
    if (be.code) { jz_err_drop(&be); jz_str_drop(&table); e = JZN_INVALID_STORED("invalid stored branch key"); goto fail; }
    jazz_node_VersionRow *version = jazz_node__node__codec__VersionRow__from_parts(table, branch_key, record);
    e = jazz_node__node__codec__VersionRow__validate_canonical(version);
    if (e.code) { jazz_node__node__codec__VersionRow__drop(version); return e; }
    *out = version;
    return JZ_OK;
fail:
    groove__records__OwnedRecord__drop(record);
    return e;
}

/* `query_transaction_fields<T>`: the decode closure becomes a callback. */
typedef jz_err (*tx_decode_fn)(const NS *self, jazz_types_NodeAlias alias, const groove_OwnedRecord *record, void *ctx, void *out);

static jz_err query_transaction_fields_by_alias(const NS *self, jazz_model_TxId tx_id, jazz_types_NodeAlias expected_alias, tx_decode_fn decode,
                                                void *ctx, bool *found, void *out) {
    *found = false;
    groove_RawRow *raw = NULL;
    JZ_TRY(get_transaction_raw(self, NULL, tx_id.time, expected_alias, &raw));
    if (!raw) return JZ_OK;
    const groove_OwnedRecord *record = groove__db__RawRow__record(raw);
    u64 node_alias, time;
    jz_err e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_NODE_ID_IDX, &node_alias);
    if (!e.code) e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_TIME_IDX, &time);
    if (!e.code && (node_alias != expected_alias || time != tx_id.time)) { groove__db__RawRow__drop(raw); return JZ_OK; }
    bool some; jazz_types_GlobalTime gt;
    if (!e.code) e = remember_history_tx_seq(self, time, node_alias, record, &some, &gt);
    if (!e.code) e = decode(self, expected_alias, record, ctx, out);
    if (!e.code) *found = true;
    groove__db__RawRow__drop(raw);
    return e;
}

static jz_err query_transaction_fields(NS *self, jazz_model_TxId tx_id, tx_decode_fn decode, void *ctx, bool *found, void *out) {
    *found = false;
    const jazz_types_NodeAlias *alias = jazz_node__node__node_aliases__NodeAliases__get(&self->node_aliases, &tx_id.node);
    if (alias) {
        /* Recovery rejects conflicting durable aliases, and alias creation
         * installs this mapping only after persistence. Do not cache the miss. */
        return query_transaction_fields_by_alias(self, tx_id, *alias, decode, ctx, found, out);
    }
    if (self->has_absent_node_alias && jz_uuid_eq(&self->absent_node_alias, &tx_id.node)) {
        /* Preserve the ordinary read's table/poison check even though no
         * storage operation is needed for this proven catalogue absence. */
        const groove_TableSchema *ts;
        return groove__db__schema_admission__Database__table_schema(jzn_db(self), &JAZZ_NODES, &ts);
    }
    jzn_u64vec aliases = { 0 };
    groove_RawRowVec raws = { 0 };
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw(jzn_db(self), &JAZZ_NODES, NULL, 0, &raws);
    for (u32 i = 0; !e.code && i < raws.len; i++) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        jz_uuid uuid;
        e = groove__records__BorrowedRecord__get_uuid(record, JZN_NAR_FIELD_UUID_IDX, &uuid);
        if (!e.code && jz_uuid_eq(&uuid, &tx_id.node)) {
            u64 a;
            e = groove__records__BorrowedRecord__get_u64(record, JZN_NAR_FIELD_ID_IDX, &a);
            if (!e.code) JZ_VEC_PUSH(aliases, a);
        }
    }
    raw_vec_drop(&raws);
    if (e.code) { jz_vec_drop(&aliases, sizeof(u64), NULL); return e; }
    if (aliases.len == 0) {
        self->has_absent_node_alias = true;
        self->absent_node_alias = tx_id.node;
        return JZ_OK;
    }
    if (aliases.len == 1) {
        /* Alias identity does not depend on this particular transaction
         * existing. Recover it even when the transaction is still absent. */
        jazz_types_NodeAlias prev;
        jazz_node__node__node_aliases__NodeAliases__insert(&self->node_aliases, tx_id.node, aliases.ptr[0], &prev);
    }
    for (u32 i = 0; i < aliases.len; i++) {
        e = query_transaction_fields_by_alias(self, tx_id, aliases.ptr[i], decode, ctx, found, out);
        if (e.code) break;
        if (*found) {
            jazz_types_NodeAlias prev;
            jazz_node__node__node_aliases__NodeAliases__insert(&self->node_aliases, tx_id.node, aliases.ptr[i], &prev);
            break;
        }
    }
    jz_vec_drop(&aliases, sizeof(u64), NULL);
    return e;
}

jz_err jazz_node__node__currency__NodeState__stored_transaction_from_record(const NS *self, jazz_model_TxId tx_id, jazz_types_NodeAlias expected_alias,
                                                                            const groove_OwnedRecord *record, jazz_node_StoredTransaction *out);

static jz_err decode_stored_transaction(const NS *self, jazz_types_NodeAlias alias, const groove_OwnedRecord *record, void *ctx, void *out) {
    return jazz_node__node__currency__NodeState__stored_transaction_from_record(self, *(const jazz_model_TxId *)ctx, alias, record, out);
}

jz_err jazz_node__node__currency__NodeState__query_transaction(NS *self, jazz_model_TxId tx_id, bool *found, jazz_node_StoredTransaction *out) {
    return query_transaction_fields(self, tx_id, decode_stored_transaction, &tx_id, found, out);
}

typedef struct { jazz_model_Fate fate; bool has_global_time; jazz_types_GlobalTime global_time; jazz_model_DurabilityTier durability; } tx_state;
static jz_err decode_tx_state(const NS *self, jazz_types_NodeAlias alias, const groove_OwnedRecord *record, void *ctx, void *vout) {
    (void)self; (void)alias; (void)ctx;
    tx_state *out = vout;
    JZ_TRY(jazz_node__node__codec__fate_from_encoded_fields(record, &out->fate));
    JZ_TRY(groove__records__BorrowedRecord__get_nullable_u64(record, JZN_TXR_FIELD_GLOBAL_TIME_IDX, &out->has_global_time, &out->global_time));
    u8 d;
    JZ_TRY(groove__records__BorrowedRecord__get_enum(record, JZN_TXR_FIELD_DURABILITY_IDX, &d));
    return jazz_node__node__codec__durability_from_discriminant(d, &out->durability);
}

/* Status is a projection, not a payload audit. Full transaction readers
 * continue validating author and contribution identities independently. */
jz_err jazz_node__node__currency__NodeState__query_transaction_state(NS *self, jazz_model_TxId tx_id, bool *found, jazz_model_Fate *fate,
                                                                     bool *has_global_time, jazz_types_GlobalTime *global_time, jazz_model_DurabilityTier *durability) {
    tx_state st;
    JZ_TRY(query_transaction_fields(self, tx_id, decode_tx_state, NULL, found, &st));
    if (*found) { *fate = st.fate; *has_global_time = st.has_global_time; *global_time = st.global_time; *durability = st.durability; }
    return JZ_OK;
}

typedef struct { bool some; u64 gt; } opt_gt;
static jz_err decode_tx_global_time(const NS *self, jazz_types_NodeAlias alias, const groove_OwnedRecord *record, void *ctx, void *vout) {
    (void)self; (void)alias; (void)ctx;
    opt_gt *out = vout;
    return groove__records__BorrowedRecord__get_nullable_u64(record, JZN_TXR_FIELD_GLOBAL_TIME_IDX, &out->some, &out->gt);
}

/* The stored transaction's global time, if the transaction is stored. */
jz_err jazz_node__node__currency__NodeState__query_transaction_global_time(NS *self, jazz_model_TxId tx_id, bool *found, bool *has_global_time,
                                                                           jazz_types_GlobalTime *global_time) {
    opt_gt g = { false, 0 };
    JZ_TRY(query_transaction_fields(self, tx_id, decode_tx_global_time, NULL, found, &g));
    *has_global_time = g.some;
    *global_time = g.gt;
    return JZ_OK;
}

static jz_err query_transaction_by_alias(const NS *self, jazz_model_TxId tx_id, jazz_types_NodeAlias expected_alias, bool *found, jazz_node_StoredTransaction *out) {
    return query_transaction_fields_by_alias(self, tx_id, expected_alias, decode_stored_transaction, &tx_id, found, out);
}

/* ViewEvaluationContext.tx_rows: TxId -> Option<StoredTransaction> */
static void tx_rows_insert(jazz_node_ViewEvaluationContext *context, jazz_model_TxId tx_id, bool some, jazz_node_StoredTransaction *tx) {
    jazz_node_OptStoredTransaction v = { some };
    if (some) v.tx = *tx;
    jazz_node_OptStoredTransaction old;
    if (jz_btree_insert(&context->tx_rows, &tx_id, &v, &old) && old.some) jazz_node__node__codec__StoredTransaction__drop(&old.tx);
}

/* (NodeUuid, NodeAlias) -> set<TxTime> */
typedef struct { jazz_types_NodeUuid node; jazz_types_NodeAlias alias; } node_alias_key;
static int cmp_node_alias_key(const void *a, const void *b, void *c) {
    (void)c;
    const node_alias_key *x = a, *y = b;
    int r = jz_uuid_cmp(&x->node, &y->node);
    if (r) return r;
    return x->alias < y->alias ? -1 : x->alias > y->alias;
}
static void btree_drop_v(void *p) { jz_btree_drop(p); }
static const jz_btree_desc desc_by_alias = { sizeof(node_alias_key), sizeof(jz_btree), cmp_node_alias_key, NULL, btree_drop_v };

jz_err jazz_node__node__currency__NodeState__preload_transaction_memo(NS *self, const jazz_model_TxId *tx_ids, u32 n, jazz_node_ViewEvaluationContext *context) {
    jz_btree by_alias;
    jz_btree_init(&by_alias, &desc_by_alias);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n && !e.code; i++) {
        jazz_model_TxId tx_id = tx_ids[i];
        if (jz_btree_contains(&context->tx_rows, &tx_id)) continue;
        const jazz_types_NodeAlias *alias = jazz_node__node__node_aliases__NodeAliases__get(&self->node_aliases, &tx_id.node);
        if (alias) {
            node_alias_key k = { tx_id.node, *alias };
            bool inserted;
            jz_btree *times = jz_btree_entry(&by_alias, &k, &inserted);
            if (inserted) jz_btree_init(times, &jzn_desc_u64_set);
            jz_btree_insert(times, &tx_id.time, NULL, NULL);
        } else {
            bool found;
            jazz_node_StoredTransaction tx;
            e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &tx);
            if (!e.code) tx_rows_insert(context, tx_id, found, &tx);
        }
    }
    const node_alias_key *k;
    jz_btree *times;
    JZ_BTREE_FOR(&by_alias, k, times) {
        if (e.code) break;
        jazz_types_NodeUuid node = k->node;
        jazz_types_NodeAlias alias = k->alias;
        if (times->len == 1) {
            u64 *time; void *unused;
            jz_btree_first(times, (void **)&time, &unused);
            jazz_model_TxId tx_id = { *time, node };
            bool found;
            jazz_node_StoredTransaction tx;
            e = query_transaction_by_alias(self, tx_id, alias, &found, &tx);
            if (!e.code) tx_rows_insert(context, tx_id, found, &tx);
            continue;
        }
        u64 *min_time, *max_time; void *unused;
        jz_btree_first(times, (void **)&min_time, &unused);
        jz_btree_last(times, (void **)&max_time, &unused);
        if (*max_time == UINT64_MAX) {
            const u64 *time; void *u2;
            JZ_BTREE_FOR(times, time, u2) {
                (void)u2;
                jazz_model_TxId tx_id = { *time, node };
                bool found;
                jazz_node_StoredTransaction tx;
                e = query_transaction_by_alias(self, tx_id, alias, &found, &tx);
                if (e.code) break;
                tx_rows_insert(context, tx_id, found, &tx);
            }
            continue;
        }
        u64 end_time = *max_time + 1;
        {
            const u64 *time; void *u2;
            JZ_BTREE_FOR(times, time, u2) { (void)u2; jazz_model_TxId tx_id = { *time, node }; tx_rows_insert(context, tx_id, false, NULL); }
        }
        groove_Value lo[2] = { v_u64(*min_time), v_u64(0) }, hi[2] = { v_u64(end_time), v_u64(0) };
        groove_RawRowVec raws = { 0 };
        e = groove__db__primary_storage__Database__primary_key_scan_range_raw(jzn_db(self), &JAZZ_TRANSACTIONS, lo, 2, hi, 2, &raws);
        for (u32 i = 0; !e.code && i < raws.len; i++) {
            const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
            u64 row_alias, time;
            e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_NODE_ID_IDX, &row_alias);
            if (!e.code) e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_TIME_IDX, &time);
            if (e.code) break;
            if (row_alias != alias || !jz_btree_contains(times, &time)) continue;
            jazz_model_TxId tx_id = { time, node };
            jazz_node_StoredTransaction tx;
            e = jazz_node__node__currency__NodeState__stored_transaction_from_record(self, tx_id, alias, record, &tx);
            if (!e.code) tx_rows_insert(context, tx_id, true, &tx);
        }
        raw_vec_drop(&raws);
    }
    jz_btree_drop(&by_alias);
    return e;
}

/* evidence_slot: Nullable(None) | Nullable(Some(Bytes)) */
static jz_err evidence_slot(const groove_OwnedRecord *record, u32 idx, bool *some, jz_bytes *out) {
    groove_Value v;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, idx, &v));
    *some = false;
    jz_err e = JZ_OK;
    if (v.tag != GROOVE_V_NULLABLE) e = JZN_INVALID_STORED("exclusive read evidence slot is not nullable");
    else if (v.v.nullable) {
        if (v.v.nullable->tag != GROOVE_V_BYTES) e = JZN_INVALID_STORED("exclusive read evidence slot is not bytes");
        else { *out = jz_bytes_clone(&v.v.nullable->v.bytes); *some = true; }
    }
    groove__records__values__Value__drop(&v);
    return e;
}

jz_err jazz_node__node__currency__NodeState__stored_transaction_from_record(const NS *self, jazz_model_TxId tx_id, jazz_types_NodeAlias expected_alias,
                                                                            const groove_OwnedRecord *record, jazz_node_StoredTransaction *out) {
    u8 kind_d;
    JZ_TRY(groove__records__BorrowedRecord__get_enum(record, JZN_TXR_FIELD_KIND_IDX, &kind_d));
    jazz_model_TxKind kind;
    JZ_TRY(jazz_node__node__codec__tx_kind_from_discriminant(kind_d, &kind));
    static const u32 slot_idx[4] = { JZN_TXR_FIELD_BASE_SNAPSHOT_IDX, JZN_TXR_FIELD_ROW_READ_SET_IDX, JZN_TXR_FIELD_ABSENT_READ_SET_IDX,
                                     JZN_TXR_FIELD_PREDICATE_READ_SET_IDX };
    bool has[4] = { 0 };
    jz_bytes slots[4] = { { 0 } };
    jz_err e = JZ_OK;
    for (int i = 0; i < 4 && !e.code; i++) e = evidence_slot(record, slot_idx[i], &has[i], &slots[i]);
    if (!e.code && kind != JAZZ_TXKIND_EXCLUSIVE && (has[0] || has[1] || has[2] || has[3]))
        e = JZN_INVALID_STORED("mergeable transaction carries exclusive read evidence");
    jazz_node_StoredEvidence evidence;
    memset(&evidence, 0, sizeof evidence);
    if (!e.code) {
        jz_slice s[4];
        for (int i = 0; i < 4; i++) s[i] = jz_bytes_slice(&slots[i]);
        e = jazz_node__node__exclusive_read_evidence__decode_evidence_slots(has[0] ? &s[0] : NULL, has[1] ? &s[1] : NULL, has[2] ? &s[2] : NULL,
                                                                           has[3] ? &s[3] : NULL, &evidence);
    }
    for (int i = 0; i < 4; i++) jz_bytes_drop(&slots[i]);
    if (e.code) return e;
    u32 n_total_writes = 0;
    groove_OwnedRecord *made_by_rec = NULL;
    jazz_types_RowAuthor made_by;
    bool has_ps = false;
    jazz_types_AuthorSubject permission_subject = { 0 };
    bool has_meta = false;
    jz_slice meta = { 0 };
    groove_OwnedRecord *merge_rec = NULL;
    void *contribution_merge = NULL;
    if (!e.code) e = groove__records__BorrowedRecord__get_u32(record, JZN_TXR_FIELD_N_TOTAL_WRITES_IDX, &n_total_writes);
    if (!e.code) e = groove__records__BorrowedRecord__get_record(record, JZN_TXR_FIELD_MADE_BY_IDX, &made_by_rec);
    if (!e.code) {
        jz_err re = jazz_types__ids__RowAuthor__from_record(made_by_rec, &made_by);
        if (re.code) { jz_err_drop(&re); e = jz_err_new(GROOVE_REC_ERR_NON_CANONICAL, "non-canonical record"); }
    }
    if (made_by_rec) groove__records__OwnedRecord__drop(made_by_rec);
    if (!e.code) e = groove__records__RecordField__read_option_author_subject(record, JZN_TXR_FIELD_PERMISSION_SUBJECT_IDX, &has_ps, &permission_subject);
    if (!e.code) e = groove__records__BorrowedRecord__get_nullable_string(record, JZN_TXR_FIELD_USER_METADATA_IDX, &has_meta, &meta);
    if (!e.code) e = groove__records__BorrowedRecord__get_nullable_record(record, JZN_TXR_FIELD_CONTRIBUTION_MERGE_IDX, &merge_rec);
    if (!e.code && merge_rec) e = jazz_node__node__physical__NodeState__contribution_merge_from_storage_record(self, merge_rec, &contribution_merge);
    if (e.code) { jazz_node__node__exclusive_read_evidence__StoredEvidence__drop(&evidence); return e; }
    jazz_model_Transaction *tx = jazz_model__tx__Transaction__new(
        tx_id, kind, n_total_writes, jazz_types__ids__RowAuthor__as_author_subject(made_by), has_ps, permission_subject,
        evidence.has_base_snapshot, evidence.base_snapshot,
        evidence.has_row_read_set ? &evidence.row_read_set : NULL,
        evidence.has_absent_read_set ? &evidence.absent_read_set : NULL,
        evidence.has_predicate_read_set ? &evidence.predicate_read_set : NULL,
        has_meta ? &meta : NULL, contribution_merge);
    /* Recovery and ordinary durable reads must fail closed on malformed
     * strategy-defined contribution identities before they can influence
     * a later merge calculation. */
    e = jazz_node__node__NodeState__validate_contribution_merge_operation_identities(self, tx);
    jazz_model_Fate fate;
    if (!e.code) e = jazz_node__node__codec__fate_from_encoded_fields(record, &fate);
    bool has_gt = false; u64 gt = 0;
    if (!e.code) e = groove__records__BorrowedRecord__get_nullable_u64(record, JZN_TXR_FIELD_GLOBAL_TIME_IDX, &has_gt, &gt);
    u8 d = 0;
    jazz_model_DurabilityTier durability = JAZZ_DUR_NONE;
    if (!e.code) e = groove__records__BorrowedRecord__get_enum(record, JZN_TXR_FIELD_DURABILITY_IDX, &d);
    if (!e.code) e = jazz_node__node__codec__durability_from_discriminant(d, &durability);
    bool is_partial = false;
    if (!e.code) e = groove__records__BorrowedRecord__get_bool(record, JZN_TXR_FIELD_IS_PARTIAL_IDX, &is_partial);
    if (e.code) { jazz_model__tx__Transaction__drop(tx); return e; }
    out->tx = tx;
    out->node_alias = expected_alias;
    out->fate = fate;
    out->has_global_time = has_gt;
    out->global_time = gt;
    out->durability = durability;
    out->is_partial = is_partial;
    return JZ_OK;
}

jz_err jazz_node__node__currency__NodeState__transaction_exists(const NS *self, jazz_model_TxId tx_id, bool *out) {
    *out = false;
    const jazz_types_NodeAlias *expected_alias = jazz_node__node__node_aliases__NodeAliases__get(&self->node_aliases, &tx_id.node);
    if (!expected_alias) return JZ_OK;
    groove_RawRow *raw = NULL;
    JZ_TRY(get_transaction_raw(self, NULL, tx_id.time, *expected_alias, &raw));
    *out = raw != NULL;
    if (raw) groove__db__RawRow__drop(raw);
    return JZ_OK;
}

jz_err jazz_node__node__currency__NodeState__query_version_by_alias_with_storage(NS *self, const jz_str *table, const jz_str *storage_table,
    jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, jazz_node_VersionRow **out);

jz_err jazz_node__node__currency__NodeState__query_version_by_alias(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time,
                                                                    jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out) {
    *out = NULL;
    jzn_strvec sources = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__version_storage_sources(self, table, &sources));
    jz_err e = JZ_OK;
    for (u32 s = 0; s < sources.len; s++) {
        jazz_node_VersionRow *version = NULL;
        e = jazz_node__node__currency__NodeState__query_version_by_alias_with_storage(self, table, &sources.ptr[s], row_uuid, tx_time, tx_node_alias, &version);
        if (e.code || version) { *out = version; break; }
    }
    jz_vec_drop(&sources, sizeof(jz_str), jzn_str_drop_v);
    return e;
}

/* Resolve an exact historical witness through the schema that authored
 * its table literal. Shared deletion history is keyed by physical table,
 * so an old `todos` witness must not construct its prefix via renamed
 * current `tasks` schema state. */
jz_err jazz_node__node__currency__NodeState__query_version_by_alias_in_branch(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
    const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out) {
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, schema_version, table, &table_id));
    jz_str storage_table = jazz_node__node__physical__physical_history_table_name(table_id);
    jz_err e = jazz_node__node__currency__NodeState__query_version_by_alias_with_storage_in_schema(self, schema_version, table, &storage_table, branch_key,
                                                                                                  row_uuid, tx_time, tx_node_alias, out);
    jz_str_drop(&storage_table);
    return e;
}

jz_err jazz_node__node__currency__NodeState__query_version_by_alias_with_storage(NS *self, const jz_str *table, const jz_str *storage_table,
    jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out) {
    const jazz_model_TableSchema *t;
    jazz_types_SchemaVersionId schema_version;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, table, self->catalogue.active_schema.schema, &t);
    if (!e.code) schema_version = self->catalogue.active_schema.schema;
    else { jz_err_drop(&e); schema_version = self->catalogue.local_schema_version_id; }
    jazz_model_BranchKey *k = jazz_model__model__branch__BranchKey__default();
    e = jazz_node__node__currency__NodeState__query_version_by_alias_with_storage_in_schema(self, schema_version, table, storage_table, k, row_uuid,
                                                                                           tx_time, tx_node_alias, out);
    jazz_model__model__branch__BranchKey__drop(k);
    return e;
}

jz_err jazz_node__node__currency__NodeState__query_version_by_alias_with_storage_in_schema(NS *self, jazz_types_SchemaVersionId schema_version,
    const jz_str *table, const jz_str *storage_table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time,
    jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out) {
    (void)schema_version;
    *out = NULL;
    bool has_seq; jazz_types_GlobalTime seq;
    JZ_TRY(jazz_node__node__currency__NodeState__history_tx_seq(self, NULL, tx_time, tx_node_alias, &has_seq, &seq));
    jzn_u64vec seqs = { 0 };
    jazz_node__node__codec__history_seq_candidates(has_seq, seq, &seqs);
    jz_bytes bk = branch_bytes(branch_key);
    groove_OwnedRecord *record = NULL;
    jz_err e = JZ_OK;
    for (u32 i = 0; i < seqs.len && !e.code; i++) {
        groove_ValueVec key = { 0 };
        jazz_node__node__codec__history_key_values(jz_bytes_slice(&bk), row_uuid, seqs.ptr[i], tx_time, tx_node_alias, &key);
        groove_RawRow *raw = NULL;
        e = groove__db__primary_storage__Database__primary_key_get_raw(jzn_db(self), storage_table, key.ptr, key.len, &raw);
        values_drop(&key);
        if (!e.code && raw) { record = groove__db__RawRow__owned_record(raw); groove__db__RawRow__drop(raw); break; }
    }
    jz_bytes_drop(&bk);
    jz_vec_drop(&seqs, sizeof(u64), NULL);
    if (e.code || !record) return e;
    return jazz_node__node__currency__NodeState__decode_stored_history_record(self, NULL, table, storage_table, record, out);
}
