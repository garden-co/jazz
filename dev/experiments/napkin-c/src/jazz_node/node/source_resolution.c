/* Port of crates/jazz/layers/node/src/node/source_resolution.rs
 *
 * Snapshot projection used by query-engine source lowering.
 *
 * Live current relations lower to Groove graph sources in `query_eval`; this
 * module is restricted to frozen historical, transaction-overlay, and other
 * explicitly snapshot-valued inputs. It must not launch an ordinary Jazz
 * query to materialize another query's source.
 *
 * `impl<S: OrderedKvStorage> NodeState<S>` becomes one C copy; async methods
 * are synchronous. */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

/* RowUuid -> VersionRow* (owned) */
static void vr_drop_v(void *p) { jazz_node__node__codec__VersionRow__drop_v(p); }
static const jz_btree_desc desc_row_version = { sizeof(jz_uuid), sizeof(jazz_node_VersionRow *), jz_cmp_uuid, NULL, vr_drop_v };
/* RowUuid -> TxId */
static const jz_btree_desc desc_row_txid = { sizeof(jz_uuid), sizeof(jazz_model_TxId), jz_cmp_uuid, NULL, NULL };
/* PhysicalColumnId -> BranchColumnValue */
static const jz_btree_desc desc_phys_bcv = { sizeof(u64), sizeof(jazz_model_BranchColumnValue), jz_cmp_u64, NULL, jazz_model__model__branch__BranchColumnValue__drop };
/* String -> BranchColumnValue */
static const jz_btree_desc desc_str_bcv = { sizeof(jz_str), sizeof(jazz_model_BranchColumnValue), jz_cmp_str, jzn_str_drop_v, jazz_model__model__branch__BranchColumnValue__drop };
/* set<BranchKey*> */
static int cmp_branch_key_ptr(const void *a, const void *b, void *c) {
    (void)c;
    return jazz_model__model__branch__BranchKey__cmp(*(jazz_model_BranchKey *const *)a, *(jazz_model_BranchKey *const *)b);
}
static void branch_key_ptr_drop(void *p) { jazz_model__model__branch__BranchKey__drop(*(jazz_model_BranchKey **)p); }
const jz_btree_desc jzn_desc_branch_key_set = { sizeof(jazz_model_BranchKey *), 0, cmp_branch_key_ptr, branch_key_ptr_drop, NULL };

static jz_err invalid_branch_key(const char *msg) { return jz_err_fmt(JZN_E_INVALID_BRANCH_KEY, "%s", msg); }
static jz_err table_not_found(const jz_str *table) { return jz_err_fmt(JZN_E_TABLE_NOT_FOUND, "%.*s", (int)table->len, table->ptr); }
/* .map_err(Error::InvalidBranchKey) on project_branch_view_selector */
static jz_err map_branch_err(jz_err e, jz_str *msg) {
    if (!e.code) return e;
    jz_err_drop(&e);
    jz_err out = jz_err_fmt(JZN_E_INVALID_BRANCH_KEY, "%.*s", (int)msg->len, msg->ptr);
    jz_str_drop(msg);
    return out;
}
static bool deletion_is_deleted(const jazz_node_VersionRow *v) {
    jazz_model_DeletionEvent d;
    return v && jazz_node__node__codec__VersionRow__deletion(v, &d) && d == JAZZ_DEL_DELETED;
}
static jazz_node_VersionRow *row_get(const jz_btree *m, const jz_uuid *row) {
    jazz_node_VersionRow **p = jz_btree_get(m, row);
    return p ? *p : NULL;
}
static void rows_drop(jazz_node_CurrentRowVec *rows) { jz_vec_drop(rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop); }

static const jazz_model_JazzSchema *catalogue_schema(const NS *self, jazz_types_SchemaVersionId v) {
    jazz_protocol_SchemaVersion **sv = jz_btree_get(&self->catalogue.catalogue_schemas, &v);
    return sv ? (*sv)->schema : NULL;
}

jz_err jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
    jazz_model_DurabilityTier tier, const jazz_model_BranchKey *key, const jazz_model_SnapshotRef *snapshot, jz_btree *content_out, jz_btree *deletions_out);

/* Resolve the exact inherited content version used when a branch-view
 * mutation creates the first physical head overlay.  This is operation
 * evidence only: callers must carry it to authority admission rather than
 * turn it into a history parent or a transaction read set. */
jz_err jazz_node__node__source_resolution__NodeState__inherited_branch_view_copy_evidence(NS *self, const jz_str *table, const jazz_model_BranchSelector *head_sel,
    const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row_uuid, bool *some, jazz_model_BranchViewCopyEvidence *out) {
    *some = false;
    if (!base) return JZ_OK;
    jazz_types_SchemaVersionId schema_version = self->catalogue.active_schema.schema;
    const jazz_model_JazzSchema *schema = catalogue_schema(self, schema_version);
    if (!schema) return JZN_INVALID_STORED("current write schema missing");
    const jazz_model_TableSchema *table_schema;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &table_schema));
    jz_str msg = { 0 };
    jazz_model_BranchKey *head = NULL;
    JZ_TRY(map_branch_err(jazz_model__schema__RuntimeSchema__project_branch_view_selector(schema, table_schema, head_sel, &head, &msg), &msg));
    jazz_model_BranchViewCopyBase copy_base = { 0 };
    jz_err e;
    if (base->tag == JAZZ_BVB_CURRENT) {
        e = map_branch_err(jazz_model__schema__RuntimeSchema__project_branch_view_selector(schema, table_schema, base->branch, &copy_base.branch, &msg), &msg);
        copy_base.tag = JAZZ_BVCB_CURRENT;
    } else {
        e = map_branch_err(jazz_model__schema__RuntimeSchema__project_branch_view_selector(schema, table_schema, base->branch, &copy_base.branch, &msg), &msg);
        copy_base.tag = JAZZ_BVCB_SNAPSHOT;
        if (!e.code) copy_base.snapshot = jazz_model__tx__SnapshotRef__clone(base->snapshot);
    }
    if (e.code) { jazz_model__model__branch__BranchKey__drop(head); return e; }

    jz_btree head_content, head_deletions, content, deletions;
    e = jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(self, table, schema_version, JAZZ_DUR_LOCAL, head, NULL, &head_content, &head_deletions);
    if (e.code) goto fail_base;
    bool in_head = jz_btree_contains(&head_content, &row_uuid) || jz_btree_contains(&head_deletions, &row_uuid);
    jz_btree_drop(&head_content);
    jz_btree_drop(&head_deletions);
    if (in_head) goto none;
    const jazz_model_SnapshotRef *snap = copy_base.tag == JAZZ_BVCB_SNAPSHOT ? copy_base.snapshot : NULL;
    e = jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(self, table, schema_version, JAZZ_DUR_LOCAL, copy_base.branch, snap, &content, &deletions);
    if (e.code) goto fail_base;
    jazz_node_VersionRow *source = row_get(&content, &row_uuid);
    if (!source || deletion_is_deleted(row_get(&deletions, &row_uuid))) {
        jz_btree_drop(&content); jz_btree_drop(&deletions);
        goto none;
    }
    jazz_model_TxId source_version;
    e = jazz_node__node__NodeState__version_tx_id(self, source, &source_version);
    jz_btree_drop(&content); jz_btree_drop(&deletions);
    if (e.code) goto fail_base;
    out->version = 1;
    out->head = head;
    out->base = copy_base;
    out->table = jz_str_clone(table);
    out->row_uuid = row_uuid;
    out->source_version = source_version;
    *some = true;
    return JZ_OK;
none:
    e = JZ_OK;
fail_base:
    jazz_model__model__branch__BranchKey__drop(head);
    jazz_model__model__branch__BranchKey__drop(copy_base.branch);
    if (copy_base.snapshot) jz_free(copy_base.snapshot);
    return e;
}

/* Re-resolve non-causal branch-view copy evidence at an authority.
 *
 * The evidence is deliberately not a history parent, so it cannot rely on
 * ordinary parent parking or causal validation. Instead the authority
 * proves that the target still has no physical head overlay and that the
 * exact source version is the winner selected by the declared live or
 * frozen base. Any malformed coordinate, replacement head, deleted
 * source, or winner mismatch is simply not admissible authorization
 * evidence. */
jz_err jazz_node__node__source_resolution__NodeState__resolve_branch_view_copy_evidence(NS *self, const jazz_model_BranchViewCopyEvidence *evidence,
    jazz_types_SchemaVersionId authored_schema, const jazz_model_TxId *candidate_tx_id, jazz_node_VersionRow **out) {
    *out = NULL;
    if (evidence->version != 1) return JZ_OK;
    const jazz_model_TableSchema *table_schema;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, &evidence->table, authored_schema, &table_schema);
    if (e.code == JZN_E_TABLE_NOT_FOUND) { jz_err_drop(&e); return JZ_OK; }
    JZ_TRY(e);
    /* Evidence stores already-projected physical branch keys. It must not
     * be projected a second time at admission: tables can have different
     * branch dimensions in one transaction. */
    const jazz_model_BranchKey *head = evidence->head;
    const jazz_model_BranchKey *base_key = evidence->base.branch;
    const jazz_model_SnapshotRef *snapshot = evidence->base.tag == JAZZ_BVCB_SNAPSHOT ? evidence->base.snapshot : NULL;
    jz_btree head_content, head_deletions;
    JZ_TRY(jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(self, &evidence->table, authored_schema, JAZZ_DUR_LOCAL, head, NULL,
                                                                                   &head_content, &head_deletions));
    const jazz_node_VersionRow *physical_head[2] = { row_get(&head_content, &evidence->row_uuid), row_get(&head_deletions, &evidence->row_uuid) };
    bool replaced = false;
    for (int i = 0; i < 2 && !e.code; i++) {
        if (!physical_head[i]) continue;
        jazz_model_TxId winner;
        e = jazz_node__node__NodeState__version_tx_id(self, physical_head[i], &winner);
        if (!e.code && (!candidate_tx_id || !jazz_model_TxId_eq(&winner, candidate_tx_id))) replaced = true;
    }
    jz_btree_drop(&head_content);
    jz_btree_drop(&head_deletions);
    JZ_TRY(e);
    if (replaced) return JZ_OK;
    jz_btree content, deletions;
    JZ_TRY(jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(self, &evidence->table, authored_schema, JAZZ_DUR_LOCAL, base_key, snapshot,
                                                                                   &content, &deletions));
    jazz_node_VersionRow *source = row_get(&content, &evidence->row_uuid);
    if (source && !deletion_is_deleted(row_get(&deletions, &evidence->row_uuid))) {
        jazz_model_TxId source_tx;
        e = jazz_node__node__NodeState__version_tx_id(self, source, &source_tx);
        if (!e.code && jazz_model_TxId_eq(&source_tx, &evidence->source_version)) *out = jazz_node__node__codec__VersionRow__clone(source);
    }
    jz_btree_drop(&content);
    jz_btree_drop(&deletions);
    return e;
}

static const jazz_model_TableSchema *find_table(const jazz_model_JazzSchema *schema, const jz_str *name) {
    const jazz_model_TableSchemaVec *tables = jazz_model__schema__JazzSchema__tables(schema);
    for (u32 i = 0; i < tables->len; i++)
        if (jz_str_eq(&tables->ptr[i].name, name)) return &tables->ptr[i];
    return NULL;
}
static int cmp_entry_name(const void *a, const void *b, void *c) {
    (void)c;
    return jz_str_cmp(&((const jazz_model_BranchKeyEntry *)a)->name, &((const jazz_model_BranchKeyEntry *)b)->name);
}
static jazz_model_BranchKey *branch_key_new(jazz_model_BranchKeyEntryVec values) {
    jazz_model_BranchKey *k = JZ_NEW(jazz_model_BranchKey);
    k->values = values;
    return k;
}

/* Return every persisted spelling of a logical branch key that can occur
 * across the table's monotone schema history. Older spellings omit later
 * branch columns and are equivalent only when the selector supplies those
 * columns' immutable defaults. */
jz_err jazz_node__node__source_resolution__NodeState__equivalent_stored_branch_keys(NS *self, jz_slice table_s, jazz_types_SchemaVersionId read_schema_version,
                                                                                   const jazz_model_BranchKey *selected, jz_btree *out) {
    jz_str table = jz_str_from_slice(table_s);
    jz_btree selected_by_name, selected_by_physical, keys;
    jz_btree_init(&selected_by_name, &desc_str_bcv);
    jz_btree_init(&selected_by_physical, &desc_phys_bcv);
    jz_btree_init(&keys, &jzn_desc_branch_key_set);
    jz_err e = JZ_OK;
    jazz_types_PhysicalTableId table_id;
    e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, read_schema_version, &table, &table_id);
    if (e.code) goto done;
    const jazz_model_JazzSchema *read_schema = catalogue_schema(self, read_schema_version);
    if (!read_schema) { e = JZN_INVALID_STORED("read schema is missing"); goto done; }
    const jazz_model_TableSchema *read_table = find_table(read_schema, &table);
    if (!read_table) { e = table_not_found(&table); goto done; }
    const jazz_node_SchemaPhysicalMapping *read_mapping = jz_btree_get(&self->catalogue.physical_mappings, &read_schema_version);
    if (!read_mapping) { e = JZN_INVALID_STORED("read schema physical mapping is missing"); goto done; }
    const jazz_node_TablePhysicalMapping *read_table_mapping = jz_btree_get(&read_mapping->tables, &table);
    if (!read_table_mapping) { e = table_not_found(&table); goto done; }
    for (u32 i = 0; i < selected->values.len; i++) {
        jz_str k = jz_str_clone(&selected->values.ptr[i].name);
        jazz_model_BranchColumnValue v;
        jazz_model__model__branch__BranchColumnValue__clone(&v, &selected->values.ptr[i].value);
        jz_btree_insert(&selected_by_name, &k, &v, NULL);
    }
    for (u32 i = 0; i < read_table->branch_by.len; i++) {
        const jz_str *column_name = &read_table->branch_by.ptr[i];
        const u64 *physical = jz_btree_get(&read_table_mapping->columns, column_name);
        if (!physical) { e = JZN_INVALID_STORED("branch column physical mapping is missing"); goto done; }
        const jazz_model_BranchColumnValue *value = jz_btree_get(&selected_by_name, column_name);
        if (!value) { e = jz_err_fmt(JZN_E_INVALID_BRANCH_KEY, "selected branch key missing %.*s", (int)column_name->len, column_name->ptr); goto done; }
        jazz_model_BranchColumnValue v;
        jazz_model__model__branch__BranchColumnValue__clone(&v, value);
        jz_btree_insert(&selected_by_physical, physical, &v, NULL);
    }
    {
        const jazz_types_SchemaVersionId *schema_version;
        jazz_protocol_SchemaVersion **catalogue_schema_p;
        JZ_BTREE_FOR(&self->catalogue.catalogue_schemas, schema_version, catalogue_schema_p) {
            const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, schema_version);
            if (!mapping) continue;
            const jz_str *logical_table = NULL;
            const jazz_node_TablePhysicalMapping *table_mapping = NULL;
            {
                const jz_str *tn; jazz_node_TablePhysicalMapping *tm;
                JZ_BTREE_FOR(&mapping->tables, tn, tm) {
                    if (tm->table_id == table_id) { logical_table = tn; table_mapping = tm; break; }
                }
            }
            if (!table_mapping) continue;
            const jazz_model_TableSchema *hist_table = find_table((*catalogue_schema_p)->schema, logical_table);
            if (!hist_table) continue;
            jz_btree historical_physical_columns;
            jz_btree_init(&historical_physical_columns, &jzn_desc_u64_set);
            for (u32 i = 0; i < hist_table->branch_by.len; i++) {
                const u64 *p = jz_btree_get(&table_mapping->columns, &hist_table->branch_by.ptr[i]);
                if (p) jz_btree_insert(&historical_physical_columns, p, NULL, NULL);
            }
            bool missing_branch_value_is_non_default = false;
            for (u32 i = 0; i < read_table->branch_by.len && !missing_branch_value_is_non_default; i++) {
                const jz_str *column_name = &read_table->branch_by.ptr[i];
                const u64 *physical = jz_btree_get(&read_table_mapping->columns, column_name);
                if (!physical) { missing_branch_value_is_non_default = true; break; }
                if (jz_btree_contains(&historical_physical_columns, physical)) continue;
                bool has_default = false;
                jazz_model_BranchColumnValue deflt = { 0 };
                for (u32 c = 0; c < read_table->columns.len; c++) {
                    const jazz_model_ColumnSchema *column = &read_table->columns.ptr[c];
                    if (!jz_str_eq(&column->name, column_name)) continue;
                    if (column->has_default) {
                        jz_err de = jazz_model__model__branch__BranchColumnValue__encode_typed(&column->deflt, &column->column_type, &deflt);
                        if (de.code) jz_panic("validated branch default");
                        has_default = true;
                    }
                    break;
                }
                const jazz_model_BranchColumnValue *sel = jz_btree_get(&selected_by_physical, physical);
                bool equal = (sel == NULL && !has_default) || (sel && has_default && jazz_model__model__branch__BranchColumnValue__eq(sel, &deflt));
                if (has_default) jazz_model__model__branch__BranchColumnValue__drop(&deflt);
                if (!equal) missing_branch_value_is_non_default = true;
            }
            jz_btree_drop(&historical_physical_columns);
            if (missing_branch_value_is_non_default) continue;
            jazz_model_BranchKeyEntryVec values = { 0 };
            for (u32 i = 0; i < hist_table->branch_by.len; i++) {
                const jz_str *column_name = &hist_table->branch_by.ptr[i];
                const u64 *physical = jz_btree_get(&table_mapping->columns, column_name);
                if (!physical) e = JZN_INVALID_STORED("historical branch column physical mapping is missing");
                const jazz_model_BranchColumnValue *v = physical ? jz_btree_get(&selected_by_physical, physical) : NULL;
                if (physical && !v) e = JZN_INVALID_STORED("selected branch key missing historical table branch column");
                if (e.code) { jz_vec_drop(&values, sizeof(jazz_model_BranchKeyEntry), jazz_model__model__branch__BranchKeyEntry__drop); goto done; }
                jazz_model_BranchKeyEntry entry;
                entry.name = jz_str_clone(column_name);
                jazz_model__model__branch__BranchColumnValue__clone(&entry.value, v);
                JZ_VEC_PUSH(values, entry);
            }
            jz_sort(values.ptr, values.len, sizeof(jazz_model_BranchKeyEntry), cmp_entry_name, NULL);
            jazz_model_BranchKey *k = branch_key_new(values);
            jz_btree_insert(&keys, &k, NULL, NULL);
        }
    }
    if (keys.len == 0) {
        jazz_model_BranchKeyEntryVec values = { 0 };
        jz_str name; jazz_model_BranchColumnValue v;
        while (jz_btree_pop_first(&selected_by_name, &name, &v)) {
            jazz_model_BranchKeyEntry entry = { name, v };
            JZ_VEC_PUSH(values, entry);
        }
        jazz_model_BranchKey *k = branch_key_new(values);
        jz_btree_insert(&keys, &k, NULL, NULL);
    }
    *out = keys;
    jz_btree_init(&keys, &jzn_desc_branch_key_set);
done:
    jz_btree_drop(&keys);
    jz_btree_drop(&selected_by_name);
    jz_btree_drop(&selected_by_physical);
    jz_str_drop(&table);
    return e;
}

jz_err jazz_node__node__source_resolution__NodeState__materialize_branch_view_winners(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
    const jazz_model_BranchKey *output_branch, jz_btree *head_content, jz_btree *head_deletions, jz_btree *base_content, jz_btree *base_deletions,
    jazz_node_CurrentRowVec *out);

jz_err jazz_node__node__source_resolution__NodeState__branch_view_rows_for_schema(NS *self, jz_slice table_s, jazz_types_SchemaVersionId read_schema_version,
    jazz_model_DurabilityTier tier, const jazz_model_BranchKey *head, const jazz_engine_BranchViewSourceBase *base, jazz_node_CurrentRowVec *out);

/* Resolve one row through a live head-over-base branch view for mutation helpers.
 * `*some` = false is `None`; otherwise `out` is a String -> Value map. */
jz_err jazz_node__node__source_resolution__NodeState__visible_current_cells_in_branch_view(NS *self, const jz_str *table, const jazz_model_BranchSelector *head_sel,
    const jazz_protocol_BranchViewBase *base_sel, jazz_types_RowUuid row_uuid, bool *some, jz_btree *out) {
    *some = false;
    jazz_types_SchemaVersionId schema_version = self->catalogue.active_schema.schema;
    const jazz_model_JazzSchema *schema = catalogue_schema(self, schema_version);
    if (!schema) return JZN_INVALID_STORED("current write schema missing");
    jazz_model_TableSchema *table_schema;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, table, schema_version, &table_schema));
    jazz_model_BranchKey *head = NULL;
    bool has_base = false;
    jazz_engine_BranchViewSourceBase base = { 0 };
    jz_str msg = { 0 };
    JZ_TRY(map_branch_err(jazz_node__node__layer_stack__branch_view_stack(schema, table_schema, head_sel, base_sel, &head, &has_base, &base, &msg), &msg));
    jazz_node_CurrentRowVec rows = { 0 };
    jz_err e = jazz_node__node__source_resolution__NodeState__branch_view_rows_for_schema(self, jz_str_slice(table), schema_version, JAZZ_DUR_LOCAL, head,
                                                                                        has_base ? &base : NULL, &rows);
    jazz_model__model__branch__BranchKey__drop(head);
    if (has_base) jazz_model__model__branch__BranchKey__drop(base.key);
    if (e.code) return e;
    for (u32 i = 0; i < rows.len; i++) {
        const jazz_node_CurrentRow *row = &rows.ptr[i];
        jazz_types_RowUuid id = jazz_node__node__CurrentRow__row_uuid(row);
        if (!jz_uuid_eq(&id, &row_uuid)) continue;
        jz_btree cells;
        jz_btree_init(&cells, &jzn_desc_str_value);
        for (u32 c = 0; c < table_schema->columns.len; c++) {
            const jazz_model_ColumnSchema *column = &table_schema->columns.ptr[c];
            groove_Value v;
            if (jazz_node__node__CurrentRow__cell(row, table_schema, &column->name, &v)) {
                jz_str k = jz_str_clone(&column->name);
                jz_btree_insert(&cells, &k, &v, NULL);
            }
        }
        *out = cells;
        *some = true;
        break;
    }
    rows_drop(&rows);
    return JZ_OK;
}

/* Global reads see only globally durable accepted transactions; local reads
 * see everything not rejected. */
static bool visible_at_tier(const jazz_node_StoredTransaction *tx, jazz_model_DurabilityTier tier) {
    switch (tier) {
    case JAZZ_DUR_GLOBAL: return tx->fate.tag == JAZZ_FATE_ACCEPTED && tx->durability >= JAZZ_DUR_GLOBAL;
    default: return tx->fate.tag != JAZZ_FATE_REJECTED;
    }
}

/* Keep `version` as its row's winner if its transaction sorts after the
 * winner seen so far. Consumes `version`. */
void jazz_node__node__source_resolution__keep_latest_version(jz_btree *content, jz_btree *tx_ids, jazz_model_TxId tx_id, jazz_node_VersionRow *version) {
    jazz_types_RowUuid key = jazz_node__node__codec__VersionRow__row_uuid(version);
    const jazz_model_TxId *existing_tx_id = jz_btree_get(tx_ids, &key);
    bool replace = true;
    if (existing_tx_id) {
        jazz_model_TxId incoming = { jazz_node__node__codec__VersionRow__tx_time(version), tx_id.node };
        jazz_model_TxId current = { jazz_node__node__codec__VersionRow__tx_time(row_get(content, &key)), existing_tx_id->node };
        replace = jazz_model_TxId_cmp(&incoming, &current) > 0;
    }
    if (replace) {
        jz_btree_insert(tx_ids, &key, &tx_id, NULL);
        jz_btree_insert(content, &key, &version, NULL);
    } else {
        jazz_node__node__codec__VersionRow__drop(version);
    }
}

/* Deletion is a cell of the row image. Callers that still distinguish the
 * deletion marker of a winner receive the winners that carry one. */
void jazz_node__node__source_resolution__deletion_marker_winners(const jz_btree *content, jz_btree *out) {
    jz_btree_init(out, &desc_row_version);
    const jz_uuid *row_uuid;
    jazz_node_VersionRow **version;
    JZ_BTREE_FOR(content, row_uuid, version) {
        jazz_model_DeletionEvent d;
        if (jazz_node__node__codec__VersionRow__deletion(*version, &d)) {
            jazz_node_VersionRow *c = jazz_node__node__codec__VersionRow__clone(*version);
            jz_btree_insert(out, row_uuid, &c, NULL);
        }
    }
}

jz_err jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
    jazz_model_DurabilityTier tier, const jazz_model_BranchKey *key, const jazz_model_SnapshotRef *snapshot_ref, jz_btree *content_out, jz_btree *deletions_out) {
    jz_btree content, tx_ids, stored_keys;
    jz_btree_init(&content, &desc_row_version);
    jz_btree_init(&tx_ids, &desc_row_txid);
    jazz_model_Snapshot snapshot;
    if (snapshot_ref) {
        snapshot.owner = snapshot_ref->owner;
        snapshot.global_base = snapshot_ref->global_base;
        snapshot.local_base = snapshot_ref->local_base;
        snapshot.dots = (jazz_model_TxIdVec){ 0 };
        jz_vec_extend(&snapshot.dots, snapshot_ref->dots.ptr, snapshot_ref->dots.len, sizeof(jazz_model_TxId));
    }
    jz_err e = jazz_node__node__source_resolution__NodeState__equivalent_stored_branch_keys(self, jz_str_slice(table), read_schema_version, key, &stored_keys);
    if (e.code) goto fail;
    {
        jazz_model_BranchKey **stored_key;
        void *unused;
        JZ_BTREE_FOR(&stored_keys, stored_key, unused) {
            (void)unused;
            jazz_node_VersionRowVec versions = { 0 };
            e = jazz_node__node__currency__NodeState__query_table_versions_in_branch(self, table, *stored_key, &versions);
            if (e.code) break;
            for (u32 i = 0; i < versions.len; i++) {
                jazz_node_VersionRow *version = versions.ptr[i];
                versions.ptr[i] = NULL;
                jazz_model_TxId tx_id;
                if (!e.code) e = jazz_node__node__NodeState__version_tx_id(self, version, &tx_id);
                bool found = false;
                jazz_node_StoredTransaction tx;
                if (!e.code) e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &tx);
                if (e.code || !found) { jazz_node__node__codec__VersionRow__drop(version); continue; }
                bool visible = snapshot_ref ? jazz_node__node__open_tx__NodeState__snapshot_covers(self, tx_id, &snapshot) : visible_at_tier(&tx, tier);
                jazz_node__node__codec__StoredTransaction__drop(&tx);
                if (visible) jazz_node__node__source_resolution__keep_latest_version(&content, &tx_ids, tx_id, version);
                else jazz_node__node__codec__VersionRow__drop(version);
            }
            jz_vec_drop(&versions, sizeof(jazz_node_VersionRow *), NULL);
            if (e.code) break;
        }
    }
    jz_btree_drop(&stored_keys);
    if (e.code) goto fail;
    jazz_node__node__source_resolution__deletion_marker_winners(&content, deletions_out);
    *content_out = content;
    jz_btree_drop(&tx_ids);
    if (snapshot_ref) jz_vec_drop(&snapshot.dots, sizeof(jazz_model_TxId), NULL);
    return JZ_OK;
fail:
    jz_btree_drop(&content);
    jz_btree_drop(&tx_ids);
    if (snapshot_ref) jz_vec_drop(&snapshot.dots, sizeof(jazz_model_TxId), NULL);
    return e;
}

jz_err jazz_node__node__source_resolution__NodeState__branch_view_rows_for_schema(NS *self, jz_slice table_s, jazz_types_SchemaVersionId read_schema_version,
    jazz_model_DurabilityTier tier, const jazz_model_BranchKey *head, const jazz_engine_BranchViewSourceBase *base, jazz_node_CurrentRowVec *out) {
    jz_str table = jz_str_from_slice(table_s);
    jz_btree head_content, head_deletions, base_content, base_deletions;
    jz_err e = jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(self, &table, read_schema_version, tier, head, NULL, &head_content, &head_deletions);
    if (e.code) { jz_str_drop(&table); return e; }
    if (!base || jazz_model__model__branch__BranchKey__eq(base->key, head)) {
        jz_btree_init(&base_content, &desc_row_version);
        jz_btree_init(&base_deletions, &desc_row_version);
    } else {
        const jazz_model_SnapshotRef *snapshot = base->tag == 0 ? NULL : (const jazz_model_SnapshotRef *)base->snapshot;
        e = jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(self, &table, read_schema_version, tier, base->key, snapshot,
                                                                                    &base_content, &base_deletions);
        if (e.code) { jz_btree_drop(&head_content); jz_btree_drop(&head_deletions); jz_str_drop(&table); return e; }
    }
    e = jazz_node__node__source_resolution__NodeState__materialize_branch_view_winners(self, &table, read_schema_version, head, &head_content, &head_deletions,
                                                                                       &base_content, &base_deletions, out);
    jz_str_drop(&table);
    return e;
}

/* Materialize only the frozen base relation for a snapshot-backed branch
 * view. The live head is deliberately absent: query lowering overlays its
 * maintained content and deletion registers separately. */
jz_err jazz_node__node__source_resolution__NodeState__branch_snapshot_rows_for_schema(NS *self, jz_slice table_s, jazz_types_SchemaVersionId read_schema_version,
    const jazz_model_BranchKey *output_branch, const jazz_model_BranchKey *base_branch, const jazz_model_SnapshotRef *snapshot,
    jazz_node_CurrentRowVec *rows_out, jazz_node_CurrentRowVec *deleted_out) {
    jz_str table = jz_str_from_slice(table_s);
    jz_btree base_content, base_deletions;
    jz_err e = jazz_node__node__source_resolution__NodeState__branch_winners_for_schema(self, &table, read_schema_version, JAZZ_DUR_LOCAL, base_branch, snapshot,
                                                                                       &base_content, &base_deletions);
    if (e.code) { jz_str_drop(&table); return e; }
    /* Keep the frozen content and deletion layers distinct. A live head
     * `Restored` winner can reveal base content even when the frozen base
     * itself had a `Deleted` winner. */
    jz_btree deleted_row_ids;
    jz_btree_init(&deleted_row_ids, &jzn_desc_uuid_set);
    {
        const jz_uuid *row_uuid;
        jazz_node_VersionRow **version;
        JZ_BTREE_FOR(&base_deletions, row_uuid, version) {
            if (deletion_is_deleted(*version)) jz_btree_insert(&deleted_row_ids, row_uuid, NULL, NULL);
        }
    }
    jz_btree_drop(&base_deletions);
    jz_btree empty_a, empty_b, empty_c;
    jz_btree_init(&empty_a, &desc_row_version);
    jz_btree_init(&empty_b, &desc_row_version);
    jz_btree_init(&empty_c, &desc_row_version);
    jazz_node_CurrentRowVec base_rows = { 0 };
    e = jazz_node__node__source_resolution__NodeState__materialize_branch_view_winners(self, &table, read_schema_version, output_branch, &empty_a, &empty_b,
                                                                                       &base_content, &empty_c, &base_rows);
    jz_str_drop(&table);
    if (e.code) { jz_btree_drop(&deleted_row_ids); return e; }
    jazz_node_CurrentRowVec deleted_base_rows = { 0 };
    for (u32 i = 0; i < base_rows.len; i++) {
        jazz_types_RowUuid id = jazz_node__node__CurrentRow__row_uuid(&base_rows.ptr[i]);
        if (jz_btree_contains(&deleted_row_ids, &id)) {
            jazz_node_CurrentRow c;
            jazz_node__node__CurrentRow__clone(&c, &base_rows.ptr[i]);
            JZ_VEC_PUSH(deleted_base_rows, c);
        }
    }
    jz_btree_drop(&deleted_row_ids);
    *rows_out = base_rows;
    *deleted_out = deleted_base_rows;
    return JZ_OK;
}

jz_err jazz_node__node__source_resolution__NodeState__project_version_cells(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
                                                                            const jazz_node_VersionRow *version, bool *some, jz_btree *out);
jz_err jazz_node__node__source_resolution__NodeState__newer_of_deletion(const NS *self, const jazz_node_VersionRow *content, const jazz_node_VersionRow *deletion,
                                                                        const jazz_node_VersionRow **out);

/* Consumes the four maps. */
jz_err jazz_node__node__source_resolution__NodeState__materialize_branch_view_winners(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
    const jazz_model_BranchKey *output_branch, jz_btree *head_content, jz_btree *head_deletions, jz_btree *base_content, jz_btree *base_deletions,
    jazz_node_CurrentRowVec *out) {
    jazz_node_CurrentRowVec rows = { 0 };
    jz_btree row_ids;
    jz_btree_init(&row_ids, &jzn_desc_uuid_set);
    jazz_model_TableSchema *read_table;
    jz_err e = jazz_node__node__NodeState__table_in_schema(self, table, read_schema_version, &read_table);
    if (e.code) goto done;
    {
        const jz_btree *srcs[2] = { head_content, base_content };
        for (int k = 0; k < 2; k++) {
            const jz_uuid *id; jazz_node_VersionRow **v;
            JZ_BTREE_FOR(srcs[k], id, v) { (void)v; jz_btree_insert(&row_ids, id, NULL, NULL); }
        }
    }
    {
        const jz_uuid *row_uuid; void *unused;
        JZ_BTREE_FOR(&row_ids, row_uuid, unused) {
            (void)unused;
            /* Content and deletion registers stack separately, head over
             * base (`INV-BVIEW-9`). */
            const jz_btree *content_layers[2] = { head_content, base_content };
            const jz_btree *deletion_layers[2] = { head_deletions, base_deletions };
            jazz_node_VersionRow *const *vp = (jazz_node_VersionRow *const *)jazz_node__node__layer_stack__top_wins(content_layers, 2, row_uuid);
            if (!vp) continue;
            const jazz_node_VersionRow *version = *vp;
            jazz_node_VersionRow *const *dp = (jazz_node_VersionRow *const *)jazz_node__node__layer_stack__top_wins(deletion_layers, 2, row_uuid);
            const jazz_node_VersionRow *deletion = dp ? *dp : NULL;
            if (deletion_is_deleted(deletion)) continue;
            bool some;
            jz_btree cells;
            e = jazz_node__node__source_resolution__NodeState__project_version_cells(self, table, read_schema_version, version, &some, &cells);
            if (e.code) goto done;
            if (!some) continue;
            for (u32 i = 0; i < read_table->branch_by.len; i++) {
                const jz_str *column_name = &read_table->branch_by.ptr[i];
                const jazz_model_BranchKeyEntry *found = NULL;
                for (u32 j = 0; j < output_branch->values.len; j++)
                    if (jz_str_eq(&output_branch->values.ptr[j].name, column_name)) { found = &output_branch->values.ptr[j]; break; }
                if (!found) { jz_btree_drop(&cells); e = invalid_branch_key("head branch key missing table column"); goto done; }
                groove_Value decoded;
                jz_err de = jazz_model__model__branch__BranchColumnValue__decode(&found->value, &decoded);
                if (de.code) { jz_err_drop(&de); jz_btree_drop(&cells); e = invalid_branch_key("invalid head branch value"); goto done; }
                jz_str k = jz_str_clone(column_name);
                jz_btree_insert(&cells, &k, &decoded, NULL);
            }
            const jazz_node_VersionRow *updated;
            e = jazz_node__node__source_resolution__NodeState__newer_of_deletion(self, version, deletion, &updated);
            if (e.code) { jz_btree_drop(&cells); goto done; }
            jazz_node_CurrentRow row;
            e = jazz_node__node__codec__current_row_from_materialized_cells_with_layer_provenance(read_table, version, version, updated, &cells, &row);
            jz_btree_drop(&cells);
            if (e.code) goto done;
            JZ_VEC_PUSH(rows, row);
        }
    }
    jazz_node__node__codec__sort_current_rows(rows.ptr, rows.len);
    *out = rows;
    rows = (jazz_node_CurrentRowVec){ 0 };
done:
    rows_drop(&rows);
    jz_btree_drop(&row_ids);
    jz_btree_drop(head_content); jz_btree_drop(head_deletions);
    jz_btree_drop(base_content); jz_btree_drop(base_deletions);
    return e;
}

/* Which transactions a version scan admits. (`Tier` is cfg(test) only.) */
typedef enum { JZN_VIS_GLOBAL_AT, JZN_VIS_SNAPSHOT } jazz_node_VersionVisibilityTag;
typedef struct jazz_node_VersionVisibility {
    jazz_node_VersionVisibilityTag tag;
    jazz_types_GlobalTime position;     /* GlobalAt */
    const jazz_model_Snapshot *snapshot; /* Snapshot */
} jazz_node_VersionVisibility;

/* Each row's latest version in `table` among those `visibility` admits. */
static jz_err winning_versions(NS *self, const jz_str *table, jazz_node_VersionVisibility visibility, jz_btree *out) {
    jz_btree content, tx_ids;
    jz_btree_init(&content, &desc_row_version);
    jz_btree_init(&tx_ids, &desc_row_txid);
    jazz_node_VersionRowVec versions = { 0 };
    jz_err e = jazz_node__node__currency__NodeState__query_table_versions(self, table, &versions);
    for (u32 i = 0; i < versions.len; i++) {
        jazz_node_VersionRow *version = versions.ptr[i];
        if (e.code) { jazz_node__node__codec__VersionRow__drop(version); continue; }
        jazz_model_TxId tx_id;
        e = jazz_node__node__NodeState__version_tx_id(self, version, &tx_id);
        if (e.code) { jazz_node__node__codec__VersionRow__drop(version); continue; }
        bool visible = false;
        if (visibility.tag == JZN_VIS_SNAPSHOT) {
            visible = jazz_node__node__open_tx__NodeState__snapshot_covers(self, tx_id, visibility.snapshot);
        } else {
            bool found = false;
            jazz_node_StoredTransaction tx;
            e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &tx);
            if (!e.code && found) {
                visible = tx.fate.tag == JAZZ_FATE_ACCEPTED && tx.durability >= JAZZ_DUR_GLOBAL && tx.has_global_time && tx.global_time <= visibility.position;
                jazz_node__node__codec__StoredTransaction__drop(&tx);
            }
        }
        if (!e.code && visible) jazz_node__node__source_resolution__keep_latest_version(&content, &tx_ids, tx_id, version);
        else jazz_node__node__codec__VersionRow__drop(version);
    }
    jz_vec_drop(&versions, sizeof(jazz_node_VersionRow *), NULL);
    jz_btree_drop(&tx_ids);
    if (e.code) { jz_btree_drop(&content); return e; }
    *out = content;
    return JZ_OK;
}

jz_err jazz_node__node__source_resolution__NodeState__project_version_winners(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
                                                                              const jazz_model_TableSchema *read_table, jz_btree *content, jazz_node_CurrentRowVec *out);

jz_err jazz_node__node__source_resolution__NodeState__projected_historical_current_rows(NS *self, jz_slice table_s, jazz_types_SchemaVersionId read_schema_version,
                                                                                       jazz_types_GlobalTime position, jazz_node_CurrentRowVec *out) {
    jz_str table = jz_str_from_slice(table_s);
    const jazz_model_TableSchema *read_table;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, &table, read_schema_version, &read_table);
    if (!e.code) {
        jz_btree content;
        jazz_node_VersionVisibility vis = { JZN_VIS_GLOBAL_AT, position, NULL };
        e = winning_versions(self, &table, vis, &content);
        if (!e.code) e = jazz_node__node__source_resolution__NodeState__project_version_winners(self, &table, read_schema_version, read_table, &content, out);
    }
    jz_str_drop(&table);
    return e;
}

jz_err jazz_node__node__source_resolution__NodeState__projected_snapshot_current_rows(NS *self, jz_slice table_s, jazz_types_SchemaVersionId read_schema_version,
                                                                                     const jazz_model_Snapshot *snapshot, jazz_node_CurrentRowVec *out) {
    jz_str table = jz_str_from_slice(table_s);
    const jazz_model_TableSchema *read_table;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, &table, read_schema_version, &read_table);
    if (!e.code) {
        jz_btree content;
        jazz_node_VersionVisibility vis = { JZN_VIS_SNAPSHOT, 0, snapshot };
        e = winning_versions(self, &table, vis, &content);
        if (!e.code) e = jazz_node__node__source_resolution__NodeState__project_version_winners(self, &table, read_schema_version, read_table, &content, out);
    }
    jz_str_drop(&table);
    return e;
}

jz_err jazz_node__node__source_resolution__NodeState__translate_cells(NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target,
                                                                      const jz_str *table, jz_btree *cells, bool *some, jz_str *out_table);

/* Materialize `version` in its own schema and translate its cells into
 * `read_schema_version`; `*some = false` when the row now belongs to another table. */
jz_err jazz_node__node__source_resolution__NodeState__project_version_cells(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
                                                                            const jazz_node_VersionRow *version, bool *some, jz_btree *out) {
    *some = false;
    jazz_types_SchemaVersionId source_schema;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(version), &source_schema))
        return JZN_INVALID_STORED("history schema version alias must exist");
    const jazz_model_TableSchema *source_table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, jazz_node__node__codec__VersionRow__table(version), source_schema, &source_table));
    jz_btree cells;
    JZ_TRY(jazz_node__node__NodeState__materialized_cells_for_version(self, source_table, version, &cells));
    bool has_projected;
    jz_str projected_table = { 0 };
    jz_err e = jazz_node__node__source_resolution__NodeState__translate_cells(self, source_schema, read_schema_version, jazz_node__node__codec__VersionRow__table(version),
                                                                              &cells, &has_projected, &projected_table);
    if (e.code) { jz_btree_drop(&cells); return e; }
    if (has_projected && jz_str_eq(&projected_table, table)) {
        *out = cells;
        *some = true;
    } else {
        jz_btree_drop(&cells);
    }
    jz_str_drop(&projected_table);
    return JZ_OK;
}

/* The row's last-updated version: its deletion marker when that sorts
 * after the content version, else the content version itself. */
jz_err jazz_node__node__source_resolution__NodeState__newer_of_deletion(const NS *self, const jazz_node_VersionRow *content, const jazz_node_VersionRow *deletion,
                                                                        const jazz_node_VersionRow **out) {
    *out = content;
    if (!deletion) return JZ_OK;
    jazz_model_TxId d, c;
    JZ_TRY(jazz_node__node__NodeState__version_tx_id(self, deletion, &d));
    JZ_TRY(jazz_node__node__NodeState__version_tx_id(self, content, &c));
    /* tx_id.time.sort_key(tx_id.node) orders by (time, node) */
    if (jazz_model_TxId_cmp(&d, &c) > 0) *out = deletion;
    return JZ_OK;
}

/* Project each row's winning version into `read_schema_version`,
 * dropping deleted rows and rows that migrate to another table. Consumes `content`. */
jz_err jazz_node__node__source_resolution__NodeState__project_version_winners(NS *self, const jz_str *table, jazz_types_SchemaVersionId read_schema_version,
                                                                              const jazz_model_TableSchema *read_table, jz_btree *content, jazz_node_CurrentRowVec *out) {
    jz_btree deletions;
    jazz_node__node__source_resolution__deletion_marker_winners(content, &deletions);
    jazz_node_CurrentRowVec rows = { 0 };
    jz_err e = JZ_OK;
    const jz_uuid *row_uuid;
    jazz_node_VersionRow **vp;
    JZ_BTREE_FOR(content, row_uuid, vp) {
        const jazz_node_VersionRow *version = *vp;
        const jazz_node_VersionRow *deletion = row_get(&deletions, row_uuid);
        if (deletion_is_deleted(deletion)) continue;
        bool some;
        jz_btree cells;
        e = jazz_node__node__source_resolution__NodeState__project_version_cells(self, table, read_schema_version, version, &some, &cells);
        if (e.code) break;
        if (!some) continue;
        const jazz_node_VersionRow *updated;
        e = jazz_node__node__source_resolution__NodeState__newer_of_deletion(self, version, deletion, &updated);
        if (e.code) { jz_btree_drop(&cells); break; }
        jazz_node_CurrentRow row;
        e = jazz_node__node__codec__current_row_from_materialized_cells_with_layer_provenance(read_table, version, version, updated, &cells, &row);
        jz_btree_drop(&cells);
        if (!e.code) {
            JZ_VEC_PUSH(rows, row);
        } else if (jazz_node__node__is_unrepresentable_enum_projection(&e)) {
            jz_err_drop(&e);
            e = JZ_OK;
        } else {
            break;
        }
    }
    jz_btree_drop(&deletions);
    jz_btree_drop(content);
    if (e.code) { rows_drop(&rows); return e; }
    jazz_node__node__codec__sort_current_rows(rows.ptr, rows.len);
    *out = rows;
    return JZ_OK;
}

jz_err jazz_node__node__source_resolution__NodeState__translate_cells(NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target,
                                                                      const jz_str *table, jz_btree *cells, bool *some, jz_str *out_table) {
    *some = false;
    if (jz_uuid_eq(&source, &target)) {
        *out_table = jz_str_clone(table);
        *some = true;
        return JZ_OK;
    }
    bool has_path;
    jazz_node_CompiledLensPath path;
    JZ_TRY(jazz_node__node__NodeState__compiled_lens_path(self, source, target, table, &has_path, &path));
    if (has_path) {
        jazz_node__node__apply_compiled_lens_path(&path, cells, out_table);
        jazz_node__node__CompiledLensPath__drop(&path);
        *some = true;
    }
    return JZ_OK;
}
