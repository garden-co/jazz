/* ingest/view_updates.c: global post-image merging, merge-op admission,
 * global/ahead current maintenance (include!d into jazz_node::node::ingest).
 * Rust: crates/jazz/layers/node/src/node/ingest/view_updates.rs */
#include "../ingest.h"

/* ---------------- local helpers ---------------- */

static void jzn_vu_values_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop); }
static void jzn_vu_rows_drop(jazz_node_VersionRowVec *v) { jz_vec_drop(v, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop); }
static void jzn_vu_set_value(groove_ValueVec *values, u32 idx, groove_Value v) {
    groove_Value_drop(&values->ptr[idx]);
    values->ptr[idx] = v;
}
static bool jzn_authors(bool has_authored, const jz_btree *authored, const jz_str *name) {
    return !has_authored || jz_btree_contains(authored, name);
}
static bool jzn_authors_cstr(bool has_authored, const jz_btree *authored, const char *name) {
    jz_str n = jz_str_from(name);
    bool r = jzn_authors(has_authored, authored, &n);
    jz_str_drop(&n);
    return r;
}
static bool jzn_is_merge(const jazz_model_TableSchema *t, const jz_str *column) {
    return jazz_model__schema__TableSchema__merge_strategy(t, column) != JAZZ_MERGE_STRATEGY_LWW;
}
static bool jzn_txid_eq_vu(const jazz_types_TxId *a, const jazz_types_TxId *b) {
    return a->time == b->time && jz_uuid_eq(&a->node, &b->node);
}
static groove_Value jzn_bytes_value(jz_bytes b) { groove_Value v; v.tag = GROOVE_V_BYTES; v.v.bytes = b; return v; }
static groove_Value jzn_uuid_value(jz_uuid u) { groove_Value v; v.tag = GROOVE_V_UUID; v.v.uuid = u; return v; }
/* Value::Nullable(value.map(Box::new)) */
static groove_Value jzn_cell_value(bool has, const groove_Value *value) {
    if (!has) return groove_value_null();
    groove_Value c;
    groove_Value_clone(&c, value);
    return groove_value_some(c);
}
static void jzn_vu_str_clone(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone(src); }

/* format!("{}", i128) / format!("{:+}", i128) */
static void jzn_i128_append(jz_str *out, __int128 v, bool plus) {
    char buf[48];
    int n = 0;
    bool neg = v < 0;
    unsigned __int128 u = neg ? (unsigned __int128)(-(v + 1)) + 1 : (unsigned __int128)v;
    do { buf[n++] = (char)('0' + (int)(u % 10)); u /= 10; } while (u);
    if (neg) jz_str_push_char(out, '-');
    else if (plus) jz_str_push_char(out, '+');
    while (n) jz_str_push_char(out, buf[--n]);
}

static jz_err jzn_query_global_winner_with_seq_in_batch(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
                                                        const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row,
                                                        bool *found, jazz_node_VersionRow *winner, bool *has_seq, jazz_types_GlobalTime *seq);
static jz_err jzn_fold_pending_patch(NSA *s, const jazz_model_TableSchema *t, const jazz_node_VersionRow *base, const jazz_node_VersionRow *patch,
                                     jazz_node_VersionRow *out);

/* at_seq(values) = incoming.with_record_values(values)?.with_merge_fields(global_time, base) */
static jz_err jzn_at_seq(const jazz_node_VersionRow *incoming, groove_ValueVec values, jazz_types_GlobalTime gt, jazz_protocol_RowBase base,
                         jazz_node_VersionRow *out) {
    jazz_node_VersionRow tmp;
    JZ_TRY(jazz_node__node__codec__VersionRow__with_record_values(incoming, values, &tmp));
    jz_err err = jazz_node__node__codec__VersionRow__with_merge_fields(&tmp, gt, base, out);
    jazz_node_VersionRow_drop(&tmp);
    return err;
}

/* the current value of a cell when the current image carries its physical
 * column with this layout's type: Option<Option<Value>> */
static bool jzn_carried(const jz_btree *current, jazz_types_PhysicalColumnId id, const groove_ValueType *column_type, bool *has_value,
                        const groove_Value **value) {
    const jzn_PhysicalCell *cell = jz_btree_get(current, &id);
    if (!cell || !groove_ValueType_eq(&cell->column_type, column_type)) return false;
    *has_value = cell->has_value;
    *value = &cell->value;
    return true;
}

/* Apply an accepted write to the row's post-image as a patch, one cell at a
 * time (SPEC 4 §4.6, "Patch rule"). Returns `None` (*some=false) when the
 * post-image does not change. Only the node that minted `global_time`
 * (`authority`) derives the row's post-image; other nodes only predict it
 * (see view_updates.rs for the full rule). */
jz_err jazz_node__node__ingest__NodeState__merged_global_post_image(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId schema_version,
                                                                   const jazz_model_TableSchema *table_schema, const jazz_node_VersionRow *incoming,
                                                                   jazz_types_TxId incoming_tx, jazz_types_GlobalTime global_time, bool authority,
                                                                   bool *some, jazz_node_VersionRow *out) {
    *some = false;
    bool has_authored = false;
    jz_btree authored;
    JZ_TRY(jazz_node__node__physical__NodeState__authored_columns_for_version(s, incoming, &has_authored, &authored));
    groove_RecordDescriptor incoming_descriptor = incoming->record.descriptor;
    jz_err err = JZ_OK;
    jazz_protocol_RowBase base;
    bool found = false, has_previous_seq = false;
    jazz_types_GlobalTime previous_seq = 0;
    jazz_node_VersionRow previous;
    jz_btree current;
    bool have_current = false;
    groove_ValueVec merged = {0}, previous_values = {0};
    bool have_previous_values = false;
    jz_bytes counter_signs = {0};
    jzn_FieldValueVec cross_layout_merge_cells = {0};

    if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__base(incoming, &base))) goto out;
    if (JZ_IS_ERR(err = jzn_query_global_winner_with_seq_in_batch(s, batch, schema_version, &table_schema->name, &incoming->branch_key,
                      jazz_node__node__codec__VersionRow__row_uuid(incoming), &found, &previous, &has_previous_seq, &previous_seq)))
        goto out;
    if (!found) {
        /* The first image of a row. Over no image, each counter cell's
         * low bits are the written value itself (a delta from zero): the
         * image keeps the cells. */
        groove_ValueVec values = {0};
        if (JZ_IS_ERR(err = groove__records__OwnedRecord__to_values(&incoming->record, &values))) goto out;
        jazz_node__node__merge_ops__clear_counter_signs(&values, incoming_descriptor);
        if (!JZ_IS_ERR(err = jzn_at_seq(incoming, values, global_time, base, out))) *some = true;
        goto out;
    }
    jazz_types_TxId previous_tx;
    if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__version_tx_id(s, &previous, &previous_tx))) goto out;
    if (jzn_txid_eq_vu(&previous_tx, &incoming_tx)) goto out;
    if (has_previous_seq && previous_seq > global_time) {
        /* Core's image at a later seq already counts this write. The
         * authority mints seqs in order and never gets here. */
        goto out;
    }
    bool same_layout = jazz_node__node__codec__VersionRow__schema_version_alias(&previous) ==
                       jazz_node__node__codec__VersionRow__schema_version_alias(incoming);
    if (!authority && !same_layout) {
        /* A prediction across layouts: the write replaces the row whole,
         * keeping the merge columns' settled values (below). */
        groove_ValueVec values = {0};
        if (JZ_IS_ERR(err = groove__records__OwnedRecord__to_values(&incoming->record, &values))) goto out;
        bool ok = false;
        jzn_FieldValueVec carried = {0};
        jz_str column = {0};
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__cross_schema_settled_merge_cells(s, schema_version, table_schema,
                          has_authored ? &authored : NULL, &previous, &ok, &carried, &column))) {
            jzn_vu_values_drop(&values);
            goto out;
        }
        if (!ok) { jz_str_drop(&column); jzn_vu_values_drop(&values); goto out; }
        for (u32 i = 0; i < carried.len; i++) jzn_vu_set_value(&values, carried.ptr[i].field, carried.ptr[i].value);
        jz_vec_drop(&carried, sizeof(jzn_FieldValue), NULL);
        jazz_node__node__merge_ops__clear_counter_signs(&values, incoming_descriptor);
        if (!JZ_IS_ERR(err = jzn_at_seq(incoming, values, global_time, base, out))) *some = true;
        goto out;
    }

    if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__image_physical_cells(s, &previous, &current))) goto out;
    have_current = true;
    const jz_btree *mapping = NULL;
    {
        const jazz_node_SchemaPhysicalMapping *m = jz_btree_get(&s->catalogue.physical_mappings, &schema_version);
        const jazz_node_TablePhysicalMapping *tm = m ? jz_btree_get(&m->tables, &table_schema->name) : NULL;
        if (!tm) { err = JZN_ISV("incoming physical table mapping missing"); goto out; }
        mapping = &tm->columns;
    }
    /* The post-image keeps the incoming write's identity and layout:
     * it is the row as of this write's seq. */
    if (JZ_IS_ERR(err = groove__records__OwnedRecord__to_values(&incoming->record, &merged))) goto out;
    if (same_layout) {
        if (JZ_IS_ERR(err = groove__records__OwnedRecord__to_values(&previous.record, &previous_values))) goto out;
        have_previous_values = true;
    }
    /* Stored images spell enum cells with the lineage's physical tags; an
     * uploaded write spells them with its schema version's authored
     * tags. A cell carried from the current image into an uploaded write
     * is re-tagged to the write's spelling. */
    groove_RecordDescriptor logical = jazz_node__node__codec__history_record_descriptor(table_schema);
    bool incoming_authored = groove_RecordDescriptor_eq(incoming_descriptor, logical);
    if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__counter_signs(incoming, &counter_signs))) goto out;
    /* An authored `_deletion` applies; otherwise the row keeps its
     * deletion state, so an edit over an image older than an accepted
     * delete leaves the row deleted (SPEC 4 §4.6, "Edit after delete"). */
    if (!jzn_authors_cstr(has_authored, &authored, JZN_DELETION_COLUMN_NAME)) {
        groove_ValueType deletion_type = jazz_node__node__ingest__deletion_cell_type();
        bool has_value;
        const groove_Value *value;
        if (jzn_carried(&current, JZN_DELETION_COLUMN_ID, &deletion_type, &has_value, &value))
            jzn_vu_set_value(&merged, JZN_HISTORY_FIELD__DELETION_IDX, jzn_cell_value(has_value, value));
        groove_ValueType_drop(&deletion_type);
    }
    if (!same_layout) {
        bool ok = false;
        jz_str column = {0};
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__cross_schema_settled_merge_cells(s, schema_version, table_schema,
                          has_authored ? &authored : NULL, &previous, &ok, &cross_layout_merge_cells, &column)))
            goto out;
        if (!ok) {
            jz_str_drop(&column);
            err = JZN_ISV("accepted cross-schema write cannot carry a merge column's settled value");
            goto out;
        }
    }
    for (u32 index = 0; index < table_schema->columns.len; index++) {
        const jazz_model_ColumnSchema *column = &table_schema->columns.ptr[index];
        u32 field = JZN_HISTORY_USER_CELLS + index;
        u32 strategy = jazz_model__schema__TableSchema__merge_strategy(table_schema, &column->name);
        bool authors = jzn_authors(has_authored, &authored, &column->name);
        if (strategy == JAZZ_MERGE_STRATEGY_LWW) {
            const jazz_types_PhysicalColumnId *id = jz_btree_get(mapping, &column->name);
            if (!id) { err = JZN_ISV("incoming column physical mapping missing"); goto out; }
            /* Authored cells take the write's value. Unauthored cells
             * keep the row's value where its image carries the column,
             * and the writer's snapshot otherwise. */
            bool has_value;
            const groove_Value *value;
            if (!authors && jzn_carried(&current, *id, &column->column_type, &has_value, &value)) {
                groove_Value cell = jzn_cell_value(has_value, value);
                if (incoming_authored) {
                    groove_Value authored_cell;
                    if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__physical_cell_to_authored(s, schema_version, &table_schema->name,
                                      index, cell, &authored_cell)))
                        goto out;
                    jzn_vu_set_value(&merged, field, authored_cell);
                } else {
                    jzn_vu_set_value(&merged, field, cell);
                }
            }
            continue;
        }
        if (!have_previous_values) {
            /* Across layouts the authority refused merge-column writes
             * before minting the seq (#3899); unauthored merge cells take
             * the row's settled value below. */
            if (authors) { err = JZN_ISV("accepted merge-column write across schema versions"); goto out; }
            continue;
        }
        /* Merge columns are ops: they apply in seq order. */
        if (authors) {
            bool negative = jazz_node__node__merge_ops__counter_sign(table_schema, jz_bytes_slice(&counter_signs), index);
            groove_Value applied;
            if (strategy == JAZZ_MERGE_STRATEGY_COUNTER) {
                bool in_range = false;
                if (JZ_IS_ERR(err = jazz_node__node__merge_ops__counter_after_op(&column->column_type, &previous_values.ptr[field],
                                  &merged.ptr[field], negative, &in_range, &applied)))
                    goto out;
                if (!in_range) {
                    /* Out of range over a stale local image. */
                    if (!authority) goto out;
                    err = JZN_ISV("accepted counter op leaves the column's range");
                    goto out;
                }
            } else {
                if (JZ_IS_ERR(err = jazz_node__node__merge_ops__apply_merge_op(strategy, &column->column_type, &previous_values.ptr[field],
                                  &merged.ptr[field], negative, &applied)))
                    goto out;
            }
            /* History cells are stored nullable. */
            jzn_vu_set_value(&merged, field, groove_value_some(applied));
        } else {
            groove_Value c;
            groove_Value_clone(&c, &previous_values.ptr[field]);
            jzn_vu_set_value(&merged, field, c);
        }
    }
    for (u32 i = 0; i < cross_layout_merge_cells.len; i++)
        jzn_vu_set_value(&merged, cross_layout_merge_cells.ptr[i].field, cross_layout_merge_cells.ptr[i].value);
    cross_layout_merge_cells.len = 0;
    {
        const u32 keep[2] = { JZN_HISTORY_FIELD_CREATED_BY_IDX, JZN_HISTORY_FIELD_CREATED_AT_IDX };
        groove_BorrowedRecord prev = groove_OwnedRecord_borrowed(&previous.record);
        for (u32 i = 0; i < 2; i++) {
            groove_Value v;
            if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_idx(&prev, keep[i], &v))) goto out;
            jzn_vu_set_value(&merged, keep[i], v);
        }
    }
    jazz_node__node__merge_ops__clear_counter_signs(&merged, incoming_descriptor);
    {
        groove_ValueVec values = merged;
        merged = (groove_ValueVec){0};
        if (!JZ_IS_ERR(err = jzn_at_seq(incoming, values, global_time, base, out))) *some = true;
    }
out:
    if (has_authored) jz_btree_drop(&authored);
    if (found) jazz_node_VersionRow_drop(&previous);
    if (have_current) jz_btree_drop(&current);
    jzn_vu_values_drop(&merged);
    if (have_previous_values) jzn_vu_values_drop(&previous_values);
    jz_bytes_drop(&counter_signs);
    for (u32 i = 0; i < cross_layout_merge_cells.len; i++) groove_Value_drop(&cross_layout_merge_cells.ptr[i].value);
    jz_vec_drop(&cross_layout_merge_cells, sizeof(jzn_FieldValue), NULL);
    return err;
}

static bool jzn_any_known_merge_column(const NSA *s, const jz_str *table);
static jz_err jzn_image_merge_column_not_carried(NSA *s, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *t,
                                                 const jazz_node_VersionRow *current, bool *some, jz_str *column);

/* merge column '{table}.{column}' cannot be merged ... (#3899) */
static jazz_model_RejectionReason jzn_not_supported(const jz_str *table, const jz_str *column) {
    jazz_model_RejectionReason r = {0};
    r.tag = JAZZ_REJ_MALFORMED_COMMIT;
    r.message = jz_format("merge column '%.*s.%.*s' cannot be merged with the row's current image, which is stored under a different schema "
                          "version; merge-column writes across schema versions are not supported yet (#3899)",
                          (int)table->len, table->ptr, (int)column->len, column->ptr);
    return r;
}

typedef struct jzn_PassedOver { const jazz_protocol_VersionRecord *version; jazz_types_TxTime through; } jzn_PassedOver;

/* The authority refuses a write it cannot merge into the row without
 * losing or inventing merge-column state (#3899 cross-layout merge ops,
 * unrepresentable settled values, image merge columns not carried,
 * counter ops out of range), and a write whose base it cannot name
 * exactly (`INV-HIST-20`). */
jz_err jazz_node__node__ingest__NodeState__merge_op_rejection(NSA *s, jazz_types_TxId tx_id, const jazz_protocol_VersionRecordVec *versions,
                                                              bool *some, jazz_model_RejectionReason *out) {
    *some = false;
    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    /* Writes of the unit's node that applying it passes over, recorded
     * once the unit passes every check here (`passed_over_predecessor`). */
    JZ_VEC(jzn_PassedOver) passed_over = {0};
    jz_err err = JZ_OK;
    for (u32 vi = 0; vi < versions->len; vi++) {
        const jazz_protocol_VersionRecord *version = versions->ptr[vi];
        jazz_types_SchemaVersionId schema_version = jazz_protocol__protocol__VersionRecord__schema_version(version);
        const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
        const jazz_model_BranchKey *branch_key = jazz_protocol__protocol__VersionRecord__branch_key(version);
        jazz_types_RowUuid row_uuid = jazz_protocol__protocol__VersionRecord__row_uuid(version);
        jazz_protocol_RowBase base = jazz_protocol__protocol__VersionRecord__base(version);
        bool found = false, has_seq = false, has_writer = false;
        jazz_types_GlobalTime current_seq = 0;
        jazz_types_TxId current_writer = {0};
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__global_current_seq_and_writer_in_batch(s, batch, schema_version, vtable, branch_key,
                          row_uuid, &found, &has_seq, &current_seq, &has_writer, &current_writer)))
            goto out;
        if (!found) { has_seq = false; has_writer = false; }
        bool refused = false;
        jz_str reason = {0};
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__late_write_refusal(s, batch, schema_version, vtable, branch_key, row_uuid, base, tx_id,
                          has_seq, current_seq, has_writer, current_writer, &refused, &reason)))
            goto out;
        if (refused) {
            *some = true;
            *out = (jazz_model_RejectionReason){ .tag = JAZZ_REJ_MALFORMED_COMMIT, .message = reason };
            goto out;
        }
        if (base.has_seq || base.has_pending) {
            if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__validate_write_base(s, batch, schema_version, vtable, branch_key, row_uuid, base,
                              tx_id, has_seq, current_seq, &refused, &reason)))
                goto out;
            if (refused) {
                *some = true;
                *out = (jazz_model_RejectionReason){ .tag = JAZZ_REJ_MALFORMED_COMMIT, .message = reason };
                goto out;
            }
        }
        bool has_through = false;
        jazz_types_TxTime through;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__passed_over_predecessor(s, base, tx_id, has_writer, current_writer, &has_through,
                          &through)))
            goto out;
        if (has_through) {
            jzn_PassedOver p = { version, through };
            JZ_VEC_PUSH(passed_over, p);
        }
        const jazz_model_TableSchema *table_schema;
        if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, vtable, schema_version, &table_schema)))
            goto out;
        const jz_btree *authored = jazz_protocol__protocol__VersionRecord__authored_columns(version);
        bool any_merge = false;
        for (u32 c = 0; c < table_schema->columns.len; c++)
            if (jzn_is_merge(table_schema, &table_schema->columns.ptr[c].name)) { any_merge = true; break; }
        /* A write whose own table has no merge column still has to be
         * checked against the row's image when another schema version
         * gives the row one: the image may hold merge state this write's
         * layout cannot carry. */
        if (!any_merge && !jzn_any_known_merge_column(s, vtable)) continue;
        bool have_current = false, unused_has_seq;
        jazz_types_GlobalTime unused_seq;
        jazz_node_VersionRow current;
        if (JZ_IS_ERR(err = jzn_query_global_winner_with_seq_in_batch(s, batch, schema_version, vtable, branch_key, row_uuid, &have_current, &current,
                          &unused_has_seq, &unused_seq)))
            goto out;
        if (!have_current) continue;
        jazz_types_SchemaVersionId current_schema;
        bool known = jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(
            s, jazz_node__node__codec__VersionRow__schema_version_alias(&current), &current_schema);
        if (!known || !jz_uuid_eq(&current_schema, &schema_version)) {
            for (u32 c = 0; c < table_schema->columns.len; c++) {
                const jz_str *name = &table_schema->columns.ptr[c].name;
                if (jzn_is_merge(table_schema, name) && jzn_authors(authored != NULL, authored, name)) {
                    *some = true;
                    *out = jzn_not_supported(vtable, name);
                    jazz_node_VersionRow_drop(&current);
                    goto out;
                }
            }
            bool not_carried = false;
            jz_str column = {0};
            if (JZ_IS_ERR(err = jzn_image_merge_column_not_carried(s, schema_version, table_schema, &current, &not_carried, &column))) {
                jazz_node_VersionRow_drop(&current);
                goto out;
            }
            if (not_carried) {
                *some = true;
                *out = jzn_not_supported(vtable, &column);
                jz_str_drop(&column);
                jazz_node_VersionRow_drop(&current);
                goto out;
            }
            bool ok = false;
            jzn_FieldValueVec carried = {0};
            err = jazz_node__node__ingest__NodeState__cross_schema_settled_merge_cells(s, schema_version, table_schema, authored, &current, &ok,
                                                                                      &carried, &column);
            for (u32 i = 0; i < carried.len; i++) groove_Value_drop(&carried.ptr[i].value);
            jz_vec_drop(&carried, sizeof(jzn_FieldValue), NULL);
            jazz_node_VersionRow_drop(&current);
            if (JZ_IS_ERR(err)) goto out;
            if (!ok) {
                *some = true;
                *out = jzn_not_supported(vtable, &column);
                jz_str_drop(&column);
                goto out;
            }
            continue;
        }
        if (!any_merge) { jazz_node_VersionRow_drop(&current); continue; }
        for (u32 index = 0; index < table_schema->columns.len; index++) {
            const jazz_model_ColumnSchema *column = &table_schema->columns.ptr[index];
            if (jazz_model__schema__TableSchema__merge_strategy(table_schema, &column->name) != JAZZ_MERGE_STRATEGY_COUNTER ||
                !jzn_authors(authored != NULL, authored, &column->name))
                continue;
            groove_Value op;
            if (!jazz_protocol__protocol__VersionRecord__optional_cell_at(version, index, &op)) continue;
            bool negative = jazz_node__node__merge_ops__counter_sign(table_schema, jazz_protocol__protocol__VersionRecord__counter_signs(version), index);
            bool has_previous = false;
            groove_Value previous;
            if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__cell(&current, table_schema, &column->name, &has_previous, &previous))) {
                groove_Value_drop(&op);
                break;
            }
            if (!has_previous) previous = groove_value_null();
            bool in_range = false;
            groove_Value applied;
            err = jazz_node__node__merge_ops__counter_after_op(&column->column_type, &previous, &op, negative, &in_range, &applied);
            if (!JZ_IS_ERR(err) && in_range) groove_Value_drop(&applied);
            if (!JZ_IS_ERR(err) && !in_range) {
                __int128 previous_i = 0, delta = 0;
                if (!JZ_IS_ERR(err = jazz_node__node__merge_ops__counter_to_i128(&previous, &previous_i)) &&
                    !JZ_IS_ERR(err = jazz_node__node__merge_ops__counter_op_delta(&column->column_type, &op, negative, &delta))) {
                    jz_str type_debug = {0};
                    groove_ValueType_debug(&type_debug, &column->column_type);
                    jz_str msg = jz_format("counter '%.*s.%.*s' would be out of range for its %.*s column: ", (int)vtable->len, vtable->ptr,
                                           (int)column->name.len, column->name.ptr, (int)type_debug.len, type_debug.ptr);
                    jzn_i128_append(&msg, previous_i, false);
                    jz_str_push_char(&msg, ' ');
                    jzn_i128_append(&msg, delta, true);
                    jz_str_push_cstr(&msg, " leaves the type's range, so the write is rejected rather than wrapped");
                    jz_str_drop(&type_debug);
                    *some = true;
                    *out = (jazz_model_RejectionReason){ .tag = JAZZ_REJ_MALFORMED_COMMIT, .message = msg };
                }
            }
            groove_Value_drop(&op);
            groove_Value_drop(&previous);
            if (JZ_IS_ERR(err) || *some) break;
        }
        jazz_node_VersionRow_drop(&current);
        if (JZ_IS_ERR(err) || *some) goto out;
    }
    groove__db__DatabaseBatch__drop(batch);
    batch = NULL;
    for (u32 i = 0; i < passed_over.len; i++) {
        const jazz_protocol_VersionRecord *version = passed_over.ptr[i].version;
        jazz_types_PhysicalTableId table_id;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__physical_table_id_for_schema(s,
                          jazz_protocol__protocol__VersionRecord__schema_version(version), jazz_protocol__protocol__VersionRecord__table(version),
                          &table_id)))
            goto out;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__record_superseded_writes(s, table_id,
                          jazz_protocol__protocol__VersionRecord__branch_key(version), jazz_protocol__protocol__VersionRecord__row_uuid(version),
                          tx_id.node, passed_over.ptr[i].through)))
            goto out;
    }
out:
    if (batch) groove__db__DatabaseBatch__drop(batch);
    jz_vec_drop(&passed_over, sizeof(jzn_PassedOver), NULL);
    return err;
}

/* has_merge(schema, name): some table (named `name`, or any when NULL) has a merge column */
static bool jzn_schema_has_merge(const jazz_model_JazzSchema *schema, const jz_str *name) {
    for (u32 i = 0; i < schema->tables.len; i++) {
        const jazz_model_TableSchema *candidate = &schema->tables.ptr[i];
        if (name && !jz_str_eq(&candidate->name, name)) continue;
        for (u32 c = 0; c < candidate->columns.len; c++)
            if (jzn_is_merge(candidate, &candidate->columns.ptr[c].name)) return true;
    }
    return false;
}
static bool jzn_any_schema_has_merge(const NSA *s, const jz_str *name) {
    if (jzn_schema_has_merge(&s->catalogue.schema, name) || jzn_schema_has_merge(&s->catalogue.active_schema.compiled, name)) return true;
    jazz_types_SchemaVersionId *k;
    jazz_model_SchemaVersion *v;
    JZ_BTREE_FOR(&s->catalogue.catalogue_schemas, k, v) {
        (void)k;
        if (jzn_schema_has_merge(&v->schema, name)) return true;
    }
    return false;
}
static bool jzn_renames_a_table(const NSA *s) {
    jazz_types_MigrationLensId *k;
    jazz_protocol_MigrationLens *lens;
    JZ_BTREE_FOR(&s->catalogue.catalogue_lenses, k, lens) {
        (void)k;
        const jazz_protocol_TableLens *tl = (const jazz_protocol_TableLens *)lens->table_lenses.ptr;
        for (u32 i = 0; i < lens->table_lenses.len; i++)
            if (!jz_str_eq(&tl[i].source_table, &tl[i].target_table)) return true;
    }
    return false;
}

/* Whether a row of `table` can have an image holding merge state under
 * some schema version this node knows: some version gives a table of
 * that name a merge column, or gives any table one while some lens
 * renames a table (the image may then be stored under another name). */
static bool jzn_any_known_merge_column(const NSA *s, const jz_str *table) {
    return jzn_any_schema_has_merge(s, table) || (jzn_renames_a_table(s) && jzn_any_schema_has_merge(s, NULL));
}

static bool jzn_lens_touches(const jazz_node_CompiledLensPath *path, const jz_str *name, bool *mapped) {
    bool touched = false;
    if (mapped) *mapped = false;
    for (u32 i = 0; i < path->ops.len; i++) {
        const jazz_node_CompiledLensOp *op = &path->ops.ptr[i];
        switch (op->tag) {
        case JAZZ_LENS_OP_RENAME:
        case JAZZ_LENS_OP_COPY:
            if (jz_str_eq(&op->a, name) || jz_str_eq(&op->b, name)) { touched = true; if (mapped) *mapped = true; }
            break;
        case JAZZ_LENS_OP_ADD:
        case JAZZ_LENS_OP_DROP:
            if (jz_str_eq(&op->a, name)) touched = true;
            break;
        }
    }
    return touched;
}

static const jazz_model_ColumnSchema *jzn_column_named(const jazz_model_TableSchema *t, const jz_str *name) {
    for (u32 c = 0; c < t->columns.len; c++)
        if (jz_str_eq(&t->columns.ptr[c].name, name)) return &t->columns.ptr[c];
    return NULL;
}

/* A merge column of the row's current image (stored under another schema
 * version) that a write under `schema_version` would not carry. Across
 * layouts the winning write replaces the row whole, so a merge column
 * survives only if the write's table has a column of the same name, type
 * and merge strategy that the lens path leaves alone. */
static jz_err jzn_image_merge_column_not_carried(NSA *s, jazz_types_SchemaVersionId schema_version, const jazz_model_TableSchema *table_schema,
                                                 const jazz_node_VersionRow *current, bool *some, jz_str *column_out) {
    *some = false;
    jazz_types_SchemaVersionId image_schema;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(current),
                                                                               &image_schema))
        return JZN_ISV("global version schema alias must exist");
    const jazz_model_TableSchema *image_table;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, current->table, image_schema, &image_table));
    const jazz_model_ColumnSchema *first = NULL;
    for (u32 c = 0; c < image_table->columns.len; c++)
        if (jzn_is_merge(image_table, &image_table->columns.ptr[c].name)) { first = &image_table->columns.ptr[c]; break; }
    if (!first) return JZ_OK;
    bool has_path = false;
    jazz_node_CompiledLensPath path;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__compiled_lens_path(s, image_schema, schema_version, current->table, &has_path, &path));
    if (!has_path || !jz_str_eq(&path.target_table, &table_schema->name)) {
        if (has_path) jazz_node_CompiledLensPath_drop(&path);
        *some = true;
        *column_out = jz_str_clone(&first->name);
        return JZ_OK;
    }
    for (u32 c = 0; c < image_table->columns.len; c++) {
        const jazz_model_ColumnSchema *column = &image_table->columns.ptr[c];
        if (!jzn_is_merge(image_table, &column->name)) continue;
        bool touched = jzn_lens_touches(&path, &column->name, NULL);
        const jazz_model_ColumnSchema *candidate = jzn_column_named(table_schema, &column->name);
        bool carried = !touched && candidate && groove_ValueType_eq(&candidate->column_type, &column->column_type) &&
                       jazz_model__schema__TableSchema__merge_strategy(table_schema, &column->name) ==
                           jazz_model__schema__TableSchema__merge_strategy(image_table, &column->name);
        if (!carried) {
            *some = true;
            *column_out = jz_str_clone(&column->name);
            break;
        }
    }
    jazz_node_CompiledLensPath_drop(&path);
    return JZ_OK;
}

/* The row's settled values of the merge columns a write across schema
 * versions did not author, as `(history field, value)` in the write's
 * layout. Any mapping that cannot be carried without guessing is
 * `Err(column)` (*ok=false). */
jz_err jazz_node__node__ingest__NodeState__cross_schema_settled_merge_cells(NSA *s, jazz_types_SchemaVersionId schema_version,
                                                                            const jazz_model_TableSchema *table_schema, const jz_btree *authored,
                                                                            const jazz_node_VersionRow *previous, bool *ok,
                                                                            jzn_FieldValueVec *carried, jz_str *column_out) {
    *ok = true;
    *carried = (jzn_FieldValueVec){0};
    JZ_VEC(u32) unauthored = {0};
    for (u32 c = 0; c < table_schema->columns.len; c++) {
        const jz_str *name = &table_schema->columns.ptr[c].name;
        if (jzn_is_merge(table_schema, name) && authored && !jz_btree_contains(authored, name)) JZ_VEC_PUSH(unauthored, c);
    }
    if (unauthored.len == 0) return JZ_OK;
    const jz_str *first = &table_schema->columns.ptr[unauthored.ptr[0]].name;
    jz_err err = JZ_OK;
    jazz_types_SchemaVersionId previous_schema;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(previous),
                                                                               &previous_schema)) {
        *ok = false;
        *column_out = jz_str_clone(first);
        goto done;
    }
    const jazz_model_TableSchema *previous_table;
    if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, previous->table, previous_schema, &previous_table)))
        goto done;
    bool has_path = false;
    jazz_node_CompiledLensPath path;
    if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__compiled_lens_path(s, previous_schema, schema_version, previous->table,
                      &has_path, &path)))
        goto done;
    if (!has_path || !jz_str_eq(&path.target_table, &table_schema->name)) {
        if (has_path) jazz_node_CompiledLensPath_drop(&path);
        *ok = false;
        *column_out = jz_str_clone(first);
        goto done;
    }
    jz_btree previous_cells, translated;
    if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__cells(previous, previous_table, &previous_cells))) {
        jazz_node_CompiledLensPath_drop(&path);
        goto done;
    }
    jz_btree_clone(&translated, &previous_cells, jzn_vu_str_clone, groove_Value_clone);
    jazz_node__node__apply_compiled_lens_path(&path, &translated, NULL);
    jz_vec_reserve(carried, unauthored.len, sizeof(jzn_FieldValue));
    for (u32 i = 0; i < unauthored.len; i++) {
        u32 index = unauthored.ptr[i];
        const jazz_model_ColumnSchema *column = &table_schema->columns.ptr[index];
        bool mapped_from_another_column = false;
        bool touched = jzn_lens_touches(&path, &column->name, &mapped_from_another_column);
        const jazz_model_ColumnSchema *same_column = jzn_column_named(previous_table, &column->name);
        const groove_Value *value;
        if (same_column && !touched && groove_ValueType_eq(&same_column->column_type, &column->column_type) &&
            jazz_model__schema__TableSchema__merge_strategy(previous_table, &column->name) ==
                jazz_model__schema__TableSchema__merge_strategy(table_schema, &column->name)) {
            value = jz_btree_get(&previous_cells, &column->name);
        } else if (!same_column && !mapped_from_another_column) {
            value = jz_btree_get(&translated, &column->name);
        } else {
            *ok = false;
            *column_out = jz_str_clone(&column->name);
            for (u32 j = 0; j < carried->len; j++) groove_Value_drop(&carried->ptr[j].value);
            jz_vec_drop(carried, sizeof(jzn_FieldValue), NULL);
            *carried = (jzn_FieldValueVec){0};
            break;
        }
        /* History cells are stored nullable. */
        jzn_FieldValue fv = { JZN_HISTORY_USER_CELLS + index, jzn_cell_value(value != NULL, value) };
        JZ_VEC_PUSH(*carried, fv);
    }
    jz_btree_drop(&previous_cells);
    jz_btree_drop(&translated);
    jazz_node_CompiledLensPath_drop(&path);
done:
    jz_vec_drop(&unauthored, sizeof(u32), NULL);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__global_current_seq_in_batch(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
                                                                      const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row,
                                                                      bool *has, jazz_types_GlobalTime *out) {
    bool found = false, has_writer = false;
    jazz_types_TxId writer;
    JZ_TRY(jazz_node__node__ingest__NodeState__global_current_seq_and_writer_in_batch(s, batch, v, table, branch_key, row, &found, has, out,
                                                                                     &has_writer, &writer));
    if (!found) *has = false;
    return JZ_OK;
}

/* primary_key_get_raw_in_batch on the global-current table for (branch, row) */
static jz_err jzn_global_current_raw(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v, const jz_str *table,
                                     const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, bool *some, groove_RawRow *raw) {
    jz_str current_table = {0};
    JZ_TRY(jazz_node__node__physical__NodeState__physical_current_table_for_schema(s, v, table, JZN_PCC_GLOBAL, &current_table));
    jz_bytes canonical = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &canonical);
    groove_Value key[2] = { jzn_bytes_value(canonical), jzn_uuid_value(row) };
    jz_err err = groove__db__Database__primary_key_get_raw_in_batch(jzn_db(s), batch, &current_table, key, 2, some, raw);
    groove_Value_drop(&key[0]);
    jz_str_drop(&current_table);
    return err;
}

/* The row's current accepted seq and the transaction that wrote its
 * current image, read from the one global-current record. `None`
 * (*found=false) when the row has no global-current record. */
jz_err jazz_node__node__ingest__NodeState__global_current_seq_and_writer_in_batch(NSA *s, const groove_DatabaseBatch *batch,
    jazz_types_SchemaVersionId v, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row,
    bool *found, bool *has_seq, jazz_types_GlobalTime *seq, bool *has_writer, jazz_types_TxId *writer) {
    *found = false;
    groove_RawRow raw;
    JZ_TRY(jzn_global_current_raw(s, batch, v, table, branch_key, row, found, &raw));
    if (!*found) return JZ_OK;
    groove_BorrowedRecord record = groove_OwnedRecord_borrowed(&raw.record);
    /* `global_time` is a nullable field: read it as one. */
    u64 tx_time = 0, alias = 0;
    jz_err err = groove__records__BorrowedRecord__get_nullable_u64(&record, JZN_GCUR_FIELD_GLOBAL_TIME_IDX, has_seq, seq);
    if (!JZ_IS_ERR(err)) err = groove__records__BorrowedRecord__get_u64(&record, JZN_GCUR_FIELD_TX_TIME_IDX, &tx_time);
    if (!JZ_IS_ERR(err)) err = groove__records__BorrowedRecord__get_u64(&record, JZN_GCUR_FIELD_TX_NODE_ID_IDX, &alias);
    if (!JZ_IS_ERR(err)) {
        jazz_types_NodeUuid node;
        *has_writer = jazz_node__node__state__read_payload__NodeState__node_for_alias(s, alias, &node);
        if (*has_writer) { writer->time = tx_time; writer->node = node; }
    }
    groove_OwnedRecord_drop(&raw.record);
    if (JZ_IS_ERR(err)) *found = false;
    return err;
}

static jz_err jzn_query_version_by_alias_in_batch(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v, const jz_str *table,
                                                  const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, jazz_types_TxTime tx_time,
                                                  jazz_types_NodeAlias tx_node_alias, bool *found, jazz_node_VersionRow *out);

static jz_err jzn_query_global_winner_in_batch(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v, const jz_str *table,
                                               const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, bool *found,
                                               jazz_node_VersionRow *winner) {
    bool has_seq;
    jazz_types_GlobalTime seq;
    return jzn_query_global_winner_with_seq_in_batch(s, batch, v, table, branch_key, row, found, winner, &has_seq, &seq);
}

/* The row's global post-image together with the seq it is the row at. */
static jz_err jzn_query_global_winner_with_seq_in_batch(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
                                                        const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row,
                                                        bool *found, jazz_node_VersionRow *winner, bool *has_seq, jazz_types_GlobalTime *seq) {
    *found = false;
    bool some = false;
    groove_RawRow raw;
    JZ_TRY(jzn_global_current_raw(s, batch, v, table, branch_key, row, &some, &raw));
    if (!some) return JZ_OK;
    u32 variant_tag = raw.variant_tag;
    groove_OwnedRecord current = raw.record;
    groove_BorrowedRecord borrowed = groove_OwnedRecord_borrowed(&current);
    jz_err err = groove__records__BorrowedRecord__get_nullable_u64(&borrowed, JZN_GCUR_FIELD_GLOBAL_TIME_IDX, has_seq, seq);
    if (JZ_IS_ERR(err)) goto out;
    bool from_current = false;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__history_image_from_current_record(s, v, table, variant_tag, borrowed, &from_current,
                      winner)))
        goto out;
    if (from_current) { *found = true; goto out; }
    u64 tx_time, alias;
    if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&borrowed, JZN_GCUR_FIELD_TX_TIME_IDX, &tx_time))) goto out;
    if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&borrowed, JZN_GCUR_FIELD_TX_NODE_ID_IDX, &alias))) goto out;
    err = jzn_query_version_by_alias_in_batch(s, batch, v, table, branch_key, row, tx_time, alias, found, winner);
out:
    groove_OwnedRecord_drop(&current);
    return err;
}

static jz_err jzn_query_version_by_alias_in_batch(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v, const jz_str *table,
                                                  const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, jazz_types_TxTime tx_time,
                                                  jazz_types_NodeAlias tx_node_alias, bool *found, jazz_node_VersionRow *out) {
    (void)v;
    *found = false;
    bool has_seq = false;
    jazz_types_GlobalTime history_seq = 0;
    JZ_TRY(jazz_node__node__currency__NodeState__history_tx_seq(s, batch, tx_time, tx_node_alias, &has_seq, &history_seq));
    jzn_U64Vec seqs = {0};
    jazz_node__node__codec__history_seq_candidates(has_seq, history_seq, &seqs);
    jzn_StrVec storage_tables = {0};
    jz_err err = jazz_node__node__currency__NodeState__version_storage_sources(s, table, &storage_tables);
    jz_bytes canonical = {0};
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &canonical);
    for (u32 t = 0; t < storage_tables.len && !JZ_IS_ERR(err); t++) {
        const jz_str *storage_table = &storage_tables.ptr[t];
        bool have = false;
        groove_RawRow raw;
        for (u32 i = 0; i < seqs.len; i++) {
            groove_ValueVec key = {0};
            jazz_node__node__codec__history_key_values(jz_bytes_slice(&canonical), row, seqs.ptr[i], tx_time, tx_node_alias, &key);
            err = groove__db__Database__primary_key_get_raw_in_batch(jzn_db(s), batch, storage_table, key.ptr, key.len, &have, &raw);
            jzn_vu_values_drop(&key);
            if (JZ_IS_ERR(err) || have) break;
        }
        if (JZ_IS_ERR(err)) break;
        if (!have) continue;
        err = jazz_node__node__currency__NodeState__decode_stored_history_record(s, batch, table, storage_table, raw.record, out);
        if (!JZ_IS_ERR(err)) *found = true;
        break;
    }
    jz_bytes_drop(&canonical);
    jz_vec_drop(&storage_tables, sizeof(jz_str), (void (*)(void *))jz_str_drop);
    jz_vec_drop(&seqs, sizeof(u64), NULL);
    return err;
}

/* #[cfg(test)] recomputed_global_winner_from_history_for_test,
 * assert_global_current_updates_match_history_for_test,
 * assert_global_current_row_matches_version_for_test: skipped */

jz_err jazz_node__node__ingest__NodeState__write_history_post_image(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *version,
                                                                   jazz_types_AuthorSubject tx_author) {
    groove_InternStr history_table;
    groove_ValidatedVariantRecord record;
    JZ_TRY(jazz_node__node__physical__NodeState__version_storage_write_binding(s, version, tx_author, &history_table, &record));
    jazz_types_GlobalTime seq;
    jz_err err = jazz_node__node__codec__VersionRow__seq(version, &seq);
    if (!JZ_IS_ERR(err) && seq != 0) {
        jazz_node_VersionRow at_zero;
        jazz_node_PrimaryKeyValue pending_key;
        if (!JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__with_seq(version, 0, &at_zero))) {
            err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, &at_zero, &pending_key);
            jazz_node_VersionRow_drop(&at_zero);
            if (!JZ_IS_ERR(err)) groove__db__DatabaseBatch__delete(batch, history_table, pending_key);
        }
    }
    jazz_node_PrimaryKeyValue key;
    if (!JZ_IS_ERR(err) && !JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, version, &key))) {
        groove__db__DatabaseBatch__update_raw(batch, history_table, key, record);
        return JZ_OK;
    }
    groove_OwnedRecord_drop(&record.record);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__write_global_current_update(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *version,
                                                                      jazz_types_GlobalTime gt) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(version),
                                                                               &schema_version))
        return JZN_ISV("unknown schema version alias");
    jazz_node_PreparedPhysicalWritePlan *plan;
    JZ_TRY(jazz_node__node__physical__NodeState__prepared_physical_write_plan(s, schema_version, version->table, JZN_PWT_GLOBAL_CURRENT, &plan));
    bool has_authored;
    jz_btree authored;
    JZ_TRY(jazz_node__node__physical__NodeState__authored_columns_for_version(s, version, &has_authored, &authored));
    if (has_authored) jz_btree_drop(&authored);
    groove_ValidatedVariantRecord physical;
    jazz_types_AuthorSubject none = {0};
    JZ_TRY(jazz_node__node__physical__NodeState__encode_physical_version_record(s, plan, version, true, gt, false, none, &physical));
    groove__db__DatabaseBatch__update_raw(batch, &plan->storage_table,
                                          jazz_node__node__codec__global_current_primary_key(&version->branch_key,
                                                                                             jazz_node__node__codec__VersionRow__row_uuid(version)),
                                          physical);
    return JZ_OK;
}

static jz_err jzn_overlay_key_for(NSA *s, const jazz_node_VersionRow *version, jazz_types_SchemaVersionId *schema_out, jzn_AheadOverlayKey *out) {
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(version),
                                                                               &schema_version))
        return JZN_ISV("unknown schema version alias");
    JZ_TRY(jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, schema_version, version->table, &out->table));
    out->key = (jz_bytes){0};
    groove__storage__PrimaryKeyValue__into_bytes(
        jazz_node__node__codec__global_current_primary_key(&version->branch_key, jazz_node__node__codec__VersionRow__row_uuid(version)), &out->key);
    if (schema_out) *schema_out = schema_version;
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__write_ahead_current_insert(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *version) {
    jazz_types_SchemaVersionId schema_version;
    jzn_AheadOverlayKey overlay_key;
    JZ_TRY(jzn_overlay_key_for(s, version, &schema_version, &overlay_key));
    jazz_types_TxId tx_id;
    jz_err err = jazz_node__node__state__read_payload__NodeState__version_tx_id(s, version, &tx_id);
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&overlay_key.key); return err; }
    const jazz_types_TxId *existing = jazz_node__node__AheadCurrentKeys__get(&s->ahead_current_keys, &overlay_key);
    if (existing && jazz_types__ids__TxId__cmp(existing, &tx_id, NULL) >= 0) { jz_bytes_drop(&overlay_key.key); return JZ_OK; }
    jazz_node_PreparedPhysicalWritePlan *plan;
    bool has_authored = false;
    jz_btree authored;
    groove_ValidatedVariantRecord physical;
    jazz_types_AuthorSubject none = {0};
    if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__prepared_physical_write_plan(s, schema_version, version->table, JZN_PWT_AHEAD_CURRENT,
                      &plan)) ||
        JZ_IS_ERR(err = jazz_node__node__physical__NodeState__authored_columns_for_version(s, version, &has_authored, &authored))) {
        jz_bytes_drop(&overlay_key.key);
        return err;
    }
    if (has_authored) jz_btree_drop(&authored);
    if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__encode_physical_version_record(s, plan, version, false, 0, false, none, &physical))) {
        jz_bytes_drop(&overlay_key.key);
        return err;
    }
    groove__db__DatabaseBatch__update_raw(batch, &plan->storage_table,
                                          jazz_node__node__codec__global_current_primary_key(&version->branch_key,
                                                                                             jazz_node__node__codec__VersionRow__row_uuid(version)),
                                          physical);
    jazz_node__node__AheadCurrentKeys__insert(&s->ahead_current_keys, overlay_key, tx_id, NULL);
    return JZ_OK;
}

/* #[cfg(test)] public_current_values: skipped */

jz_err jazz_node__node__ingest__NodeState__write_ahead_current_delete(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *version,
                                                                     bool *out) {
    *out = false;
    jazz_types_SchemaVersionId schema_version;
    jzn_AheadOverlayKey overlay_key;
    JZ_TRY(jzn_overlay_key_for(s, version, &schema_version, &overlay_key));
    jazz_types_TxId tx_id;
    jz_err err = jazz_node__node__state__read_payload__NodeState__version_tx_id(s, version, &tx_id);
    if (JZ_IS_ERR(err)) goto out;
    const jazz_types_TxId *existing = jazz_node__node__AheadCurrentKeys__get(&s->ahead_current_keys, &overlay_key);
    if (!existing || !jzn_txid_eq_vu(existing, &tx_id)) goto out;
    jz_str table = {0};
    if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__physical_current_table_for_schema(s, schema_version, version->table, JZN_PCC_AHEAD,
                      &table)))
        goto out;
    groove__db__DatabaseBatch__delete(batch, groove__Intern__new_str(table),
                                      jazz_node__node__codec__global_current_primary_key(&version->branch_key,
                                                                                         jazz_node__node__codec__VersionRow__row_uuid(version)));
    jazz_node__node__AheadCurrentKeys__remove(&s->ahead_current_keys, &overlay_key, NULL);
    *out = true;
out:
    jz_bytes_drop(&overlay_key.key);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__restore_ahead_overlay_after_reject(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *rejected) {
    jazz_types_TxId tx_id;
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__version_tx_id(s, rejected, &tx_id));
    jz_btree settling;
    jz_btree_init(&settling, &jzn_desc_txid_set_a);
    jz_btree_insert(&settling, &tx_id, NULL, NULL);
    jz_err err = jazz_node__node__ingest__NodeState__recompute_ahead_overlay(s, batch, rejected, &settling);
    jz_btree_drop(&settling);
    return err;
}

typedef struct jzn_PendingPatch { jazz_types_TxId tx_id; jazz_node_VersionRow version; } jzn_PendingPatch;
static int jzn_pending_patch_cmp(const void *a, const void *b, void *ctx) {
    return jazz_types__ids__TxId__cmp(&((const jzn_PendingPatch *)a)->tx_id, &((const jzn_PendingPatch *)b)->tx_id, ctx);
}
static void jzn_pending_patch_drop(void *p) { jazz_node_VersionRow_drop(&((jzn_PendingPatch *)p)->version); }

jz_err jazz_node__node__ingest__NodeState__recompute_ahead_overlay(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *row,
                                                                  const jz_btree *settling) {
    jazz_node_VersionRowVec versions = {0};
    JZ_TRY(jazz_node__node__currency__NodeState__query_row_versions_in_branch(s, row->table, &row->branch_key,
                                                                            jazz_node__node__codec__VersionRow__row_uuid(row), &versions));
    JZ_VEC(jzn_PendingPatch) pending = {0};
    jz_err err = JZ_OK;
    u32 i = 0;
    for (; i < versions.len; i++) {
        jazz_types_TxId tx_id;
        if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__version_tx_id(s, &versions.ptr[i], &tx_id))) break;
        if (jz_btree_contains(settling, &tx_id)) { jazz_node_VersionRow_drop(&versions.ptr[i]); continue; }
        bool some = false, has_gt = false;
        jazz_model_Fate fate;
        jazz_types_GlobalTime gt;
        u32 durability;
        if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction_state(s, tx_id, &some, &fate, &has_gt, &gt, &durability))) break;
        bool is_pending = some && fate.tag == JAZZ_FATE_PENDING && !has_gt;
        if (some) jazz_model_Fate_drop(&fate);
        if (is_pending) {
            jzn_PendingPatch p = { tx_id, versions.ptr[i] };
            JZ_VEC_PUSH(pending, p);
        } else {
            jazz_node_VersionRow_drop(&versions.ptr[i]);
        }
    }
    for (u32 j = i + (JZ_IS_ERR(err) ? 0 : 0); JZ_IS_ERR(err) && j < versions.len; j++) jazz_node_VersionRow_drop(&versions.ptr[j]);
    jz_vec_drop(&versions, sizeof(jazz_node_VersionRow), NULL);
    if (JZ_IS_ERR(err)) goto out;
    jz_sort(pending.ptr, pending.len, sizeof(jzn_PendingPatch), jzn_pending_patch_cmp, NULL);
    if (pending.len == 0) goto out;
    const jazz_node_VersionRow *first = &pending.ptr[0].version;
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(s, jazz_node__node__codec__VersionRow__schema_version_alias(first),
                                                                               &schema_version)) {
        err = JZN_ISV("pending version schema alias must exist");
        goto out;
    }
    const jazz_model_TableSchema *table_schema;
    if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, first->table, schema_version, &table_schema))) goto out;
    bool have_image = false;
    jazz_node_VersionRow image;
    if (JZ_IS_ERR(err = jzn_query_global_winner_in_batch(s, batch, schema_version, &table_schema->name, &row->branch_key,
                      jazz_node__node__codec__VersionRow__row_uuid(row), &have_image, &image)))
        goto out;
    for (u32 k = 0; k < pending.len; k++) {
        jazz_node_VersionRow *patch = &pending.ptr[k].version;
        if (have_image && jazz_node__node__codec__VersionRow__schema_version_alias(&image) ==
                              jazz_node__node__codec__VersionRow__schema_version_alias(patch)) {
            jazz_node_VersionRow folded;
            err = jzn_fold_pending_patch(s, table_schema, &image, patch, &folded);
            jazz_node_VersionRow_drop(&image);
            if (JZ_IS_ERR(err)) { have_image = false; goto out; }
            image = folded;
        } else {
            if (have_image) jazz_node_VersionRow_drop(&image);
            jazz_node_VersionRow_clone(&image, patch);
            have_image = true;
        }
    }
    if (have_image) {
        jzn_AheadOverlayKey key;
        if (!JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__ahead_overlay_key(s, &image, &key))) {
            jazz_node__node__AheadCurrentKeys__remove(&s->ahead_current_keys, &key, NULL);
            jz_bytes_drop(&key.key);
            err = jazz_node__node__ingest__NodeState__write_ahead_current_insert(s, batch, &image);
        }
        jazz_node_VersionRow_drop(&image);
    }
out:
    jz_vec_drop(&pending, sizeof(jzn_PendingPatch), jzn_pending_patch_drop);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__rebase_ahead_overlays(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRowVec *synced,
                                                                const jz_btree *settling) {
    for (u32 i = 0; i < synced->len; i++) {
        const jazz_node_VersionRow *version = &synced->ptr[i];
        jazz_types_TxId tx_id;
        JZ_TRY(jazz_node__node__state__read_payload__NodeState__version_tx_id(s, version, &tx_id));
        bool foreign = false;
        JZ_TRY(jazz_node__node__ingest__NodeState__has_foreign_ahead_overlay(s, version, tx_id, &foreign));
        if (foreign) JZ_TRY(jazz_node__node__ingest__NodeState__recompute_ahead_overlay(s, batch, version, settling));
    }
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__ahead_overlay_key(const NSA *s, const jazz_node_VersionRow *version, jzn_AheadOverlayKey *out) {
    return jzn_overlay_key_for((NSA *)s, version, NULL, out);
}

jz_err jazz_node__node__ingest__NodeState__has_foreign_ahead_overlay(const NSA *s, const jazz_node_VersionRow *version, jazz_types_TxId tx_id,
                                                                    bool *out) {
    jzn_AheadOverlayKey key;
    JZ_TRY(jazz_node__node__ingest__NodeState__ahead_overlay_key(s, version, &key));
    const jazz_types_TxId *overlay = jazz_node__node__AheadCurrentKeys__get(&s->ahead_current_keys, &key);
    *out = overlay && !jzn_txid_eq_vu(overlay, &tx_id);
    jz_bytes_drop(&key.key);
    return JZ_OK;
}

static jz_err jzn_fold_pending_patch(NSA *s, const jazz_model_TableSchema *table_schema, const jazz_node_VersionRow *base,
                                     const jazz_node_VersionRow *patch, jazz_node_VersionRow *out) {
    bool has_authored = false;
    jz_btree authored;
    JZ_TRY(jazz_node__node__physical__NodeState__authored_columns_for_version(s, patch, &has_authored, &authored));
    groove_ValueVec folded = {0}, base_values = {0};
    jz_bytes counter_signs = {0};
    jz_err err = groove__records__OwnedRecord__to_values(&patch->record, &folded);
    if (!JZ_IS_ERR(err)) err = groove__records__OwnedRecord__to_values(&base->record, &base_values);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__VersionRow__counter_signs(patch, &counter_signs);
    if (JZ_IS_ERR(err)) goto out;
    if (!jzn_authors_cstr(has_authored, &authored, JZN_DELETION_COLUMN_NAME)) {
        groove_Value c;
        groove_Value_clone(&c, &base_values.ptr[JZN_HISTORY_FIELD__DELETION_IDX]);
        jzn_vu_set_value(&folded, JZN_HISTORY_FIELD__DELETION_IDX, c);
    }
    for (u32 position = 0; position < table_schema->columns.len; position++) {
        const jazz_model_ColumnSchema *column = &table_schema->columns.ptr[position];
        u32 index = JZN_HISTORY_USER_CELLS + position;
        u32 strategy = jazz_model__schema__TableSchema__merge_strategy(table_schema, &column->name);
        if (!jzn_authors(has_authored, &authored, &column->name)) {
            groove_Value c;
            groove_Value_clone(&c, &base_values.ptr[index]);
            jzn_vu_set_value(&folded, index, c);
            continue;
        }
        if (strategy == JAZZ_MERGE_STRATEGY_LWW) continue;
        groove_Value applied;
        if (JZ_IS_ERR(err = jazz_node__node__merge_ops__apply_pending_merge_op(strategy, &column->column_type, &base_values.ptr[index],
                          &folded.ptr[index],
                          jazz_node__node__merge_ops__counter_sign(table_schema, jz_bytes_slice(&counter_signs), position), &applied)))
            goto out;
        jzn_vu_set_value(&folded, index, groove_value_some(applied));
    }
    {
        const u32 keep[2] = { JZN_HISTORY_FIELD_CREATED_BY_IDX, JZN_HISTORY_FIELD_CREATED_AT_IDX };
        for (u32 i = 0; i < 2; i++) {
            groove_Value c;
            groove_Value_clone(&c, &base_values.ptr[keep[i]]);
            jzn_vu_set_value(&folded, keep[i], c);
        }
    }
    jazz_node__node__merge_ops__clear_counter_signs(&folded, patch->record.descriptor);
    err = jazz_node__node__codec__VersionRow__with_record_values(patch, folded, out);
    folded = (groove_ValueVec){0};
out:
    if (has_authored) jz_btree_drop(&authored);
    jzn_vu_values_drop(&folded);
    jzn_vu_values_drop(&base_values);
    jz_bytes_drop(&counter_signs);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__cleanup_fated_ahead_current_for_versions(NSA *s, groove_DatabaseBatch *batch,
                                                                                   const jazz_node_VersionRowVec *versions) {
    jz_btree settling;
    jz_btree_init(&settling, &jzn_desc_txid_set_a);
    jz_err err = JZ_OK;
    for (u32 i = 0; i < versions->len && !JZ_IS_ERR(err); i++) {
        jazz_types_TxId tx_id;
        if (!JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__version_tx_id(s, &versions->ptr[i], &tx_id)))
            jz_btree_insert(&settling, &tx_id, NULL, NULL);
    }
    for (u32 i = 0; i < versions->len && !JZ_IS_ERR(err); i++) {
        bool deleted = false;
        if (!JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__write_ahead_current_delete(s, batch, &versions->ptr[i], &deleted)) && deleted)
            err = jazz_node__node__ingest__NodeState__recompute_ahead_overlay(s, batch, &versions->ptr[i], &settling);
    }
    jz_btree_drop(&settling);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__prune_ahead_current_for_global_time(NSA *s, groove_DatabaseBatch *batch, jazz_types_GlobalTime gt) {
    groove_RawRowVec raws = {0};
    groove_Value key = groove_value_u64(gt);
    JZ_TRY(groove__db__Database__index_scan_raw(jzn_db(s), "jazz_transactions", "by_global_time", &key, 1, &raws));
    JZ_VEC(jazz_types_TxId) tx_ids = {0};
    jz_err err = JZ_OK;
    for (u32 i = 0; i < raws.len; i++) {
        groove_BorrowedRecord record = groove_OwnedRecord_borrowed(&raws.ptr[i].record);
        u64 time, alias;
        if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&record, JZN_TXROW_FIELD_TIME_IDX, &time))) break;
        if (JZ_IS_ERR(err = groove__records__BorrowedRecord__get_u64(&record, JZN_TXROW_FIELD_NODE_ID_IDX, &alias))) break;
        jazz_types_NodeUuid node;
        if (!jazz_node__node__state__read_payload__NodeState__node_for_alias(s, alias, &node)) {
            err = JZN_ISV("transaction node alias must exist");
            break;
        }
        jazz_types_TxId tx = { time, node };
        JZ_VEC_PUSH(tx_ids, tx);
    }
    groove_RawRowVec_drop(&raws);
    for (u32 i = 0; i < tx_ids.len && !JZ_IS_ERR(err); i++) {
        jazz_node_VersionRowVec versions = {0};
        if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_versions_for_tx(s, tx_ids.ptr[i], &versions))) break;
        for (u32 j = 0; j < versions.len && !JZ_IS_ERR(err); j++) {
            bool deleted;
            err = jazz_node__node__ingest__NodeState__write_ahead_current_delete(s, batch, &versions.ptr[j], &deleted);
        }
        jzn_vu_rows_drop(&versions);
    }
    jz_vec_drop(&tx_ids, sizeof(jazz_types_TxId), NULL);
    return err;
}
