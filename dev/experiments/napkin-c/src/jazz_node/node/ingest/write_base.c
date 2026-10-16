/* ingest/write_base.c: authority-side base validation of writes, late-write
 * refusal and superseded-writes markers (include!d into jazz_node::node::ingest).
 * Rust: crates/jazz/layers/node/src/node/ingest/write_base.rs
 *
 * Skipped (dead outside cfg(test), absent from browser coverage):
 * WriteConflict, authored_overlap, write_conflict, row_write_conflicts. */
#include "../ingest.h"

/* PhysicalCells = BTreeMap<PhysicalColumnId, (ValueType, Option<Value>)> */
static void jzn_physical_cell_drop(void *p) {
    jzn_PhysicalCell *c = p;
    groove_ValueType_drop(&c->column_type);
    if (c->has_value) groove_Value_drop(&c->value);
}
const jz_btree_desc jzn_desc_physical_cells = { sizeof(jazz_types_PhysicalColumnId), sizeof(jzn_PhysicalCell), jz_cmp_u64, NULL,
    jzn_physical_cell_drop };

/* The `_deletion` cell's type: the same in every layout. */
groove_ValueType jazz_node__node__ingest__deletion_cell_type(void) {
    return groove__records__values__ValueType__nullable(groove_vt_simple(GROOVE_VT_BOOL));
}

/* Why Core cannot chain a write over the pending predecessor its base names,
 * or NULL when it can: the predecessor is an older transaction of the
 * writer's own node (`INV-HIST-20`). This is the only place cross-writer
 * chains are refused. */
const char *jazz_node__node__ingest__pending_predecessor_refusal(jazz_types_TxId predecessor, jazz_types_TxId writer) {
    if (!jz_uuid_eq(&predecessor.node, &writer.node)) return "its pending predecessor was written by another node";
    if (predecessor.time >= writer.time) return "its pending predecessor is not older than the write";
    return NULL;
}

/* in_writers_chain: only used by the conflict walk (write_conflict), which is
 * test-only; kept as the documented counterpart of pending_predecessor_refusal. */
static inline bool jzn_in_writers_chain(jazz_types_TxId pending, jazz_types_TxId record) {
    return jz_uuid_eq(&record.node, &pending.node) && record.time <= pending.time;
}

static groove_Value jzn_wb_bytes(jz_bytes b) { groove_Value v; v.tag = GROOVE_V_BYTES; v.v.bytes = b; return v; }
static groove_Value jzn_wb_uuid(jz_uuid u) { groove_Value v; v.tag = GROOVE_V_UUID; v.v.uuid = u; return v; }
static void jzn_wb_values_drop(groove_Value *v, u32 n) { for (u32 i = 0; i < n; i++) groove_Value_drop(&v[i]); }
static void jzn_wb_strvec_drop(jzn_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), (void (*)(void *))jz_str_drop); }

/* "the base of the write to '{table}' row {uuid} cannot be resolved: {detail}; ..." */
static jz_str jzn_base_not_supported(const jz_str *table, jazz_types_RowUuid row_uuid, const char *detail) {
    char uuid[37];
    jz_uuid_to_string(&row_uuid, uuid);
    return jz_format("the base of the write to '%.*s' row %s cannot be resolved: %s; writes over such a base are not supported yet",
                     (int)table->len, table->ptr, uuid, detail);
}

/* Check the base of a write by `writer` to a row whose current image is at
 * `current_seq` (SPEC 4 §4.6, "Validating the base at Core").
 * `Ok(Err(reason))` (*refused) refuses the write: the authority accepts a base
 * it can name exactly or not at all (`INV-HIST-20`). It reads at most the
 * row's history record at the base seq. */
jz_err jazz_node__node__ingest__NodeState__validate_write_base(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId schema_version,
                                                              const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                                              jazz_protocol_RowBase base, jazz_types_TxId writer, bool has_current_seq,
                                                              jazz_types_GlobalTime current_seq, bool *refused, jz_str *reason) {
    *refused = false;
    if (!base.has_seq && !base.has_pending) return JZ_OK;
    if (base.has_seq && base.seq == 0) {
        /* Seq 0 is where history keeps writes without an accepted fate;
         * no accepted image is at it. */
        *refused = true;
        *reason = jzn_base_not_supported(table, row_uuid, "base seq 0 names no accepted write");
        return JZ_OK;
    }
    if (base.has_seq && (!has_current_seq || base.seq > current_seq)) {
        jz_str detail = jz_format("base seq %llu is above the row's current seq", (unsigned long long)base.seq);
        jz_str_push_char(&detail, '\0');
        *refused = true;
        *reason = jzn_base_not_supported(table, row_uuid, detail.ptr);
        jz_str_drop(&detail);
        return JZ_OK;
    }
    if (base.has_pending) {
        const char *refusal = jazz_node__node__ingest__pending_predecessor_refusal(base.pending, writer);
        if (refusal) {
            *refused = true;
            *reason = jzn_base_not_supported(table, row_uuid, refusal);
            return JZ_OK;
        }
        /* The predecessor's fate is not looked up. A writer's writes
         * reach Core in their authored order, so a predecessor Core has
         * not decided by now never arrives: like one Core rejected, it
         * is not in history, and the write applies as written over the
         * row's current image (`INV-HIST-20`). */
    }
    if (!base.has_seq) return JZ_OK;
    jazz_types_GlobalTime seq = base.seq;
    /* The row's current image is its accepted write at that seq. */
    if (has_current_seq && seq == current_seq) return JZ_OK;
    /* The base seq must name an accepted write of this row. */
    jazz_types_PhysicalTableId table_id;
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, schema_version, table, &table_id));
    jz_str history_table = jazz_node__node__physical__physical_history_table_name(table_id);
    jz_bytes canonical = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &canonical);
    groove_Value prefix[3] = { jzn_wb_bytes(canonical), jzn_wb_uuid(row_uuid), groove_value_u64(seq) };
    groove_RawRowVec rows = {0};
    jz_err err = groove__db__Database__primary_key_scan_raw_in_batch(jzn_db(s), batch, &history_table, prefix, 3, &rows);
    jzn_wb_values_drop(prefix, 3);
    jz_str_drop(&history_table);
    JZ_TRY(err);
    bool found = rows.len != 0;
    groove_RawRowVec_drop(&rows);
    if (!found) {
        jz_str detail = jz_format("no accepted write of the row is at base seq %llu", (unsigned long long)seq);
        jz_str_push_char(&detail, '\0');
        *refused = true;
        *reason = jzn_base_not_supported(table, row_uuid, detail.ptr);
        jz_str_drop(&detail);
    }
    return JZ_OK;
}

static jz_err jzn_own_write_accepted_between(NSA *s, const groove_DatabaseBatch *batch, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                             jazz_types_RowUuid row_uuid, jazz_types_TxId writer, jazz_types_GlobalTime after,
                                             jazz_types_GlobalTime through, bool *out);
static jz_err jzn_superseded_through(NSA *s, jazz_types_PhysicalTableId table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                     jazz_types_NodeUuid node, bool *some, jazz_types_TxTime *out);

static jz_str jzn_late(const jz_str *table, jazz_types_RowUuid row_uuid) {
    char uuid[37];
    jz_uuid_to_string(&row_uuid, uuid);
    return jz_format("the write to '%.*s' row %s arrived after a later write of its writer; a writer's writes must reach the authority in the "
                     "order they were written",
                     (int)table->len, table->ptr, uuid);
}

/* Why Core refuses `writer`'s write to a row as arriving after a later
 * write of its own node that Core already accepted there, or `None`.
 * Applying it would put an older image over a newer one (`INV-HIST-23`).
 * Reads are bounded by the writes accepted since the writer's base. */
jz_err jazz_node__node__ingest__NodeState__late_write_refusal(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId schema_version,
                                                             const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                                             jazz_protocol_RowBase base, jazz_types_TxId writer, bool has_current_seq,
                                                             jazz_types_GlobalTime current_seq, bool has_current_writer,
                                                             jazz_types_TxId current_writer, bool *refused, jz_str *reason) {
    *refused = false;
    /* No accepted write of the row. */
    if (!has_current_seq) return JZ_OK;
    if (has_current_writer && jz_uuid_eq(&current_writer.node, &writer.node)) {
        /* The writer's newest accepted write of the row is the current
         * image's: its writes before that were accepted in order. */
        if (current_writer.time > writer.time) { *refused = true; *reason = jzn_late(table, row_uuid); }
        return JZ_OK;
    }
    bool is_late;
    if (base.has_seq && base.seq == current_seq) {
        /* Nothing was accepted after the image the writer had seen. */
        is_late = false;
    } else if (base.has_seq && base.seq > current_seq) {
        /* A base above the row's current seq is refused by `validate_write_base`. */
        is_late = false;
    } else if (base.has_seq) {
        JZ_TRY(jzn_own_write_accepted_between(s, batch, table, branch_key, row_uuid, writer, base.seq, current_seq, &is_late));
    } else {
        jazz_types_PhysicalTableId table_id;
        JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, schema_version, table, &table_id));
        bool some = false;
        jazz_types_TxTime through = 0;
        JZ_TRY(jzn_superseded_through(s, table_id, branch_key, row_uuid, writer.node, &some, &through));
        is_late = some && writer.time <= through;
    }
    if (is_late) { *refused = true; *reason = jzn_late(table, row_uuid); }
    return JZ_OK;
}

static void jzn_history_bound(const jz_bytes *canonical, jazz_types_RowUuid row_uuid, u64 seq, groove_Value out[5]) {
    out[0] = jzn_wb_bytes(jz_bytes_clone(canonical));
    out[1] = jzn_wb_uuid(row_uuid);
    out[2] = groove_value_u64(seq);
    out[3] = groove_value_u64(0);
    out[4] = groove_value_u64(0);
}
static u64 jzn_sat_add1(u64 v) { return v == UINT64_MAX ? v : v + 1; }

/* Whether the row holds an accepted record of `writer`'s node newer than
 * `writer` at a seq in `after+1..=through`. One range read per history
 * table of the row's table; only key fields are read. */
static jz_err jzn_own_write_accepted_between(NSA *s, const groove_DatabaseBatch *batch, const jz_str *table, const jazz_model_BranchKey *branch_key,
                                             jazz_types_RowUuid row_uuid, jazz_types_TxId writer, jazz_types_GlobalTime after,
                                             jazz_types_GlobalTime through, bool *out) {
    *out = false;
    jz_bytes canonical = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &canonical);
    groove_Value start[5], end[5];
    jzn_history_bound(&canonical, row_uuid, jzn_sat_add1(after), start);
    jzn_history_bound(&canonical, row_uuid, jzn_sat_add1(through), end);
    jz_bytes_drop(&canonical);
    jzn_StrVec history_tables = {0};
    jz_err err = jazz_node__node__currency__NodeState__version_storage_sources(s, table, &history_tables);
    for (u32 t = 0; t < history_tables.len && !JZ_IS_ERR(err) && !*out; t++) {
        groove_RawRowVec rows = {0};
        if (JZ_IS_ERR(err = groove__db__Database__primary_key_scan_range_raw_in_batch(jzn_db(s), batch, &history_tables.ptr[t], start, end, 5, &rows)))
            break;
        for (u32 i = 0; i < rows.len; i++) {
            groove_BorrowedRecord record = groove_OwnedRecord_borrowed(&rows.ptr[i].record);
            u64 time, alias;
            if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&record, JZN_HISTORY_FIELD_TX_TIME_IDX, &time))) break;
            if (time <= writer.time) continue;
            if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&record, JZN_HISTORY_FIELD_TX_NODE_ID_IDX, &alias))) break;
            jazz_types_NodeUuid node;
            if (jazz_node__node__state__read_payload__NodeState__node_for_alias(s, alias, &node) && jz_uuid_eq(&node, &writer.node)) {
                *out = true;
                break;
            }
        }
        groove_RawRowVec_drop(&rows);
    }
    jzn_wb_strvec_drop(&history_tables);
    jzn_wb_values_drop(start, 5);
    jzn_wb_values_drop(end, 5);
    return err;
}

/* The superseded-writes marker key of `node` for a row. It names the row's
 * table by its physical id, which a table rename keeps. */
static void jzn_superseded_writes_key(jazz_types_PhysicalTableId table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                      jazz_types_NodeUuid node, groove_Value out[4]) {
    jz_bytes canonical = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &canonical);
    out[0] = groove_value_u64(table);
    out[1] = jzn_wb_bytes(canonical);
    out[2] = jzn_wb_uuid(row_uuid);
    out[3] = jzn_wb_uuid(node);
}

/* The newest transaction time of `node`'s writes to the row that Core
 * passed over: one point read. */
static jz_err jzn_superseded_through(NSA *s, jazz_types_PhysicalTableId table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                     jazz_types_NodeUuid node, bool *some, jazz_types_TxTime *out) {
    *some = false;
    groove_DirectRecordStore *store;
    JZ_TRY(groove__db__Database__direct_record_store(jzn_db(s), JAZZ_SCHEMA_SUPERSEDED_WRITES_STORE, &store));
    groove_Value key[4];
    jzn_superseded_writes_key(table, branch_key, row_uuid, node, key);
    bool found = false;
    groove_OwnedRecord record;
    jz_err err = groove__db__DirectRecordStore__get(store, key, 4, &found, &record);
    jzn_wb_values_drop(key, 4);
    JZ_TRY(err);
    if (!found) return JZ_OK;
    groove_Value v;
    err = groove__records__OwnedRecord__get_idx(&record, 0, &v);
    groove_OwnedRecord_drop(&record);
    JZ_TRY(err);
    if (v.tag != GROOVE_V_U64) {
        groove_Value_drop(&v);
        return JZN_ISV("superseded-writes marker is not a transaction time");
    }
    *some = true;
    *out = v.v.u;
    return JZ_OK;
}

static jz_err jzn_undecided_predecessor(NSA *s, jazz_types_TxId pending, jazz_types_TxId writer, bool *some, jazz_types_TxTime *out);

/* The writes of `writer`'s node to a row that Core passes over by applying
 * `writer`'s write: through its pending predecessor's time, when Core holds
 * no decision for that predecessor (it never arrived, or is stored
 * Pending). `None` when the write passes nothing over. See write_base.rs
 * for why this keeps the late check of a base-less write one point read. */
jz_err jazz_node__node__ingest__NodeState__passed_over_predecessor(NSA *s, jazz_protocol_RowBase base, jazz_types_TxId writer, bool has_current_writer,
                                                                  jazz_types_TxId current_writer, bool *some, jazz_types_TxTime *out) {
    *some = false;
    if (!base.has_pending) return JZ_OK;
    jazz_types_TxId pending = base.pending;
    if (has_current_writer && jz_uuid_eq(&current_writer.node, &writer.node) && current_writer.time >= pending.time) {
        /* The row's image is the predecessor's, or a newer write of its
         * node Core accepted after it. */
        return JZ_OK;
    }
    return jzn_undecided_predecessor(s, pending, writer, some, out);
}

/* `pending`'s time when it is a well-formed predecessor of `writer` that Core
 * holds no decision for: not stored, or stored Pending. */
static jz_err jzn_undecided_predecessor(NSA *s, jazz_types_TxId pending, jazz_types_TxId writer, bool *some, jazz_types_TxTime *out) {
    *some = false;
    if (jazz_node__node__ingest__pending_predecessor_refusal(pending, writer) != NULL) return JZ_OK;
    bool found = false;
    jazz_node_StoredTransaction stored;
    JZ_TRY(jazz_node__node__currency__NodeState__query_transaction(s, pending, &found, &stored));
    bool undecided = !found || stored.fate.tag == JAZZ_FATE_PENDING;
    if (found) jazz_node__node__codec__StoredTransaction__drop(&stored);
    if (undecided) { *some = true; *out = pending.time; }
    return JZ_OK;
}

/* Record the marker a refused write `writer` leaves for each row it
 * addressed whose pending predecessor Core holds no decision for. A version
 * whose table this node cannot resolve leaves none. */
jz_err jazz_node__node__ingest__NodeState__record_superseded_by_refusal(NSA *s, jazz_types_TxId writer, jzn_SupersededPredecessorVec predecessors) {
    jz_err err = JZ_OK;
    for (u32 i = 0; i < predecessors.len && !JZ_IS_ERR(err); i++) {
        const jzn_SupersededPredecessor *p = &predecessors.ptr[i];
        jazz_types_PhysicalTableId physical_table;
        jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, p->schema_version, &p->table, &physical_table);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); continue; }
        bool some = false;
        jazz_types_TxTime through;
        if (JZ_IS_ERR(err = jzn_undecided_predecessor(s, p->pending, writer, &some, &through))) break;
        if (some)
            err = jazz_node__node__ingest__NodeState__record_superseded_writes(s, physical_table, &p->branch_key, p->row_uuid, writer.node, through);
    }
    for (u32 i = 0; i < predecessors.len; i++) {
        jz_str_drop(&predecessors.ptr[i].table);
        jazz_model_BranchKey_drop(&predecessors.ptr[i].branch_key);
    }
    jz_vec_drop(&predecessors, sizeof(jzn_SupersededPredecessor), NULL);
    return err;
}

/* Record that Core passed over `node`'s writes to a row through `through`.
 * Markers only grow. */
jz_err jazz_node__node__ingest__NodeState__record_superseded_writes(NSA *s, jazz_types_PhysicalTableId table, const jazz_model_BranchKey *branch_key,
                                                                   jazz_types_RowUuid row_uuid, jazz_types_NodeUuid node, jazz_types_TxTime through) {
    bool some = false;
    jazz_types_TxTime recorded = 0;
    JZ_TRY(jzn_superseded_through(s, table, branch_key, row_uuid, node, &some, &recorded));
    if (some && recorded >= through) return JZ_OK;
    groove_DirectRecordStore *store;
    JZ_TRY(groove__db__Database__direct_record_store(jzn_db(s), JAZZ_SCHEMA_SUPERSEDED_WRITES_STORE, &store));
    groove_Value key[4];
    jzn_superseded_writes_key(table, branch_key, row_uuid, node, key);
    groove_Value value = groove_value_u64(through);
    jz_err err = groove__db__DirectRecordStore__set(store, key, 4, &value, 1);
    jzn_wb_values_drop(key, 4);
    return err;
}

/* write_conflict / row_write_conflicts: test-only analysis, skipped. */

/* This node's newest write to the row that has no fate yet, as a writer's
 * base names it (SPEC 4 §4.6, "Base of a write"). History keeps such writes
 * at seq 0; foreign pending writes this node holds (as a relay) are skipped. */
jz_err jazz_node__node__ingest__NodeState__newest_own_pending_write_in_batch(NSA *s, const groove_DatabaseBatch *batch, const jz_str *table,
                                                                            const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
                                                                            bool *some, jazz_types_TxId *out) {
    *some = false;
    if (!s->has_self_node_alias) return JZ_OK;
    jazz_types_NodeAlias own_alias = s->self_node_alias;
    jz_bytes canonical = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &canonical);
    groove_Value prefix[3] = { jzn_wb_bytes(canonical), jzn_wb_uuid(row_uuid), groove_value_u64(0) };
    bool have_newest = false;
    jazz_types_TxTime newest = 0;
    jzn_StrVec history_tables = {0};
    jz_err err = jazz_node__node__currency__NodeState__version_storage_sources(s, table, &history_tables);
    for (u32 t = 0; t < history_tables.len && !JZ_IS_ERR(err); t++) {
        groove_RawRowVec rows = {0};
        if (JZ_IS_ERR(err = groove__db__Database__primary_key_scan_raw_in_batch(jzn_db(s), batch, &history_tables.ptr[t], prefix, 3, &rows))) break;
        for (u32 i = 0; i < rows.len; i++) {
            groove_BorrowedRecord record = groove_OwnedRecord_borrowed(&rows.ptr[i].record);
            u64 alias, time;
            if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&record, JZN_HISTORY_FIELD_TX_NODE_ID_IDX, &alias))) break;
            if (alias != own_alias) continue;
            if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&record, JZN_HISTORY_FIELD_TX_TIME_IDX, &time))) break;
            if (!have_newest || time > newest) { have_newest = true; newest = time; }
        }
        groove_RawRowVec_drop(&rows);
    }
    jzn_wb_strvec_drop(&history_tables);
    jzn_wb_values_drop(prefix, 3);
    JZ_TRY(err);
    if (have_newest) {
        *some = true;
        out->time = newest;
        out->node = s->node_uuid;
    }
    return JZ_OK;
}

/* Every plain cell and `_deletion` of a row image, keyed by physical column id. */
jz_err jazz_node__node__ingest__NodeState__image_physical_cells(const NSA *s, const jazz_node_VersionRow *image, jz_btree *out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(image),
                                                                               &schema_version))
        return JZN_ISV("history schema version alias must exist");
    const jazz_model_TableSchema *table;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref((NSA *)s, image->table, schema_version, &table));
    const jazz_node_SchemaPhysicalMapping *m = jz_btree_get(&s->catalogue.physical_mappings, &schema_version);
    const jazz_node_TablePhysicalMapping *tm = m ? jz_btree_get(&m->tables, image->table) : NULL;
    if (!tm) return JZN_ISV("history physical table mapping missing");
    const jz_btree *mapping = &tm->columns;
    groove_BorrowedRecord record = groove_OwnedRecord_borrowed(&image->record);
    jz_btree_init(out, &jzn_desc_physical_cells);
    jz_err err = JZ_OK;
    for (u32 index = 0; index < table->columns.len; index++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[index];
        if (jazz_model__schema__TableSchema__merge_strategy(table, &column->name) != JAZZ_MERGE_STRATEGY_LWW) continue;
        const jazz_types_PhysicalColumnId *id = jz_btree_get(mapping, &column->name);
        if (!id) { err = JZN_ISV("history column physical mapping missing"); break; }
        groove_Value raw;
        if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_idx(&record, JZN_HISTORY_USER_CELLS + index, &raw))) break;
        jzn_PhysicalCell cell;
        if (JZ_IS_ERR(err = jazz_node__node__codec__nullable_value(raw, &cell.has_value, &cell.value))) break;
        groove_ValueType_clone(&cell.column_type, &column->column_type);
        jzn_PhysicalCell old;
        if (jz_btree_insert(out, id, &cell, &old)) jzn_physical_cell_drop(&old);
    }
    if (!JZ_IS_ERR(err)) {
        groove_Value raw;
        jzn_PhysicalCell cell;
        if (!JZ_IS_ERR(err = groove__records__BorrowedRecord__get_idx(&record, JZN_HISTORY_FIELD__DELETION_IDX, &raw)) &&
            !JZ_IS_ERR(err = jazz_node__node__codec__nullable_value(raw, &cell.has_value, &cell.value))) {
            cell.column_type = jazz_node__node__ingest__deletion_cell_type();
            jazz_types_PhysicalColumnId id = JZN_DELETION_COLUMN_ID;
            jzn_PhysicalCell old;
            if (jz_btree_insert(out, &id, &cell, &old)) jzn_physical_cell_drop(&old);
        }
    }
    if (JZ_IS_ERR(err)) jz_btree_drop(out);
    return err;
}
