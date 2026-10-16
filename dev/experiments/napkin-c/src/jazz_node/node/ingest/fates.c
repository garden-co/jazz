/* ingest/fates.c: upstream fate application, exclusive commit validation,
 * commit-unit write policy admission, and parked commit units (include!d into
 * jazz_node::node::ingest).
 * Rust: crates/jazz/layers/node/src/node/ingest/fates.rs */
#include "../ingest.h"

/* ---------------- local helpers ---------------- */

static jz_err jzn_fates_missing_transaction(jazz_types_TxId tx) {
    jz_str d = {0};
    jazz_types__ids__TxId__debug(&d, &tx);
    jz_err e = jz_err_fmt(JZN_E_MISSING_TRANSACTION, "missing transaction %.*s", (int)d.len, d.ptr);
    jz_str_drop(&d);
    return e;
}
static jz_err jzn_fates_conflicting_commit_unit(jazz_types_TxId tx) {
    jz_str d = {0};
    jazz_types__ids__TxId__debug(&d, &tx);
    jz_err e = jz_err_fmt(JZN_E_CONFLICTING_COMMIT_UNIT, "conflicting commit unit for transaction %.*s", (int)d.len, d.ptr);
    jz_str_drop(&d);
    return e;
}
static bool jzn_fates_version_vecs_eq(const jazz_protocol_VersionRecordVec *a, const jazz_protocol_VersionRecordVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jazz_protocol_VersionRecord_eq(a->ptr[i], b->ptr[i])) return false;
    return true;
}
static bool jzn_txid_eq(const jazz_types_TxId *a, const jazz_types_TxId *b) {
    return a->time == b->time && jz_uuid_eq(&a->node, &b->node);
}
static int jzn_fates_cmp_str(const void *a, const void *b, void *ctx) { (void)ctx; return jz_str_cmp(a, b); }
static void jzn_fates_str_drop(void *p) { jz_str_drop(p); }
static void jzn_values_vec_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop); }
static void jzn_rows_drop(jazz_node_VersionRowVec *v) { jz_vec_drop(v, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop); }
static jazz_model_RejectionReason jzn_reason(u32 tag) { jazz_model_RejectionReason r = {0}; r.tag = tag; return r; }

/* ParkedCommitUnit container (TxId -> ParkedCommitUnit) */
static void jzn_parked_unit_drop(void *p) {
    jazz_node_ParkedCommitUnit *u = p;
    jazz_model_Transaction_drop(&u->tx);
    jazz_protocol_VersionRecordVec_drop(&u->versions);
}
const jz_btree_desc jzn_desc_txid_parked = { sizeof(jazz_types_TxId), sizeof(jazz_node_ParkedCommitUnit),
    jazz_types__ids__TxId__cmp, NULL, jzn_parked_unit_drop };

/* BTreeSet<(RowUuid, TxId)> */
typedef struct jzn_RowTx { jazz_types_RowUuid row; jazz_types_TxId tx; } jzn_RowTx;
static int jzn_row_tx_cmp(const void *a, const void *b, void *ctx) {
    const jzn_RowTx *x = a, *y = b;
    int c = jz_cmp_uuid(&x->row, &y->row, ctx);
    return c ? c : jazz_types__ids__TxId__cmp(&x->tx, &y->tx, ctx);
}
static const jz_btree_desc jzn_desc_row_tx_set = { sizeof(jzn_RowTx), 0, jzn_row_tx_cmp, NULL, NULL };
static bool jzn_unit_veq(const void *a, const void *b) { (void)a; (void)b; return true; }

/* memo keyed by (String, RowUuid) -> Option<TxId> */
typedef struct jzn_TableRowKey { jz_str table; jazz_types_RowUuid row; } jzn_TableRowKey;
typedef struct jzn_OptTx { bool some; jazz_types_TxId tx; } jzn_OptTx;
static int jzn_table_row_cmp(const void *a, const void *b, void *ctx) {
    const jzn_TableRowKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    return c ? c : jz_cmp_uuid(&x->row, &y->row, ctx);
}
static void jzn_table_row_drop(void *k) { jz_str_drop(&((jzn_TableRowKey *)k)->table); }
static const jz_btree_desc jzn_desc_row_memo = { sizeof(jzn_TableRowKey), sizeof(jzn_OptTx), jzn_table_row_cmp, jzn_table_row_drop, NULL };
/* memo keyed by (PhysicalTableId, RowUuid) -> Option<TxId> */
typedef struct jzn_PhysRowKey { jazz_types_PhysicalTableId table; jazz_types_RowUuid row; } jzn_PhysRowKey;
static int jzn_phys_row_cmp(const void *a, const void *b, void *ctx) {
    const jzn_PhysRowKey *x = a, *y = b;
    if (x->table != y->table) return x->table < y->table ? -1 : 1;
    return jz_cmp_uuid(&x->row, &y->row, ctx);
}
static const jz_btree_desc jzn_desc_winner_memo = { sizeof(jzn_PhysRowKey), sizeof(jzn_OptTx), jzn_phys_row_cmp, NULL, NULL };
/* read_schemas: &str -> SchemaVersionId */
static const jz_btree_desc jzn_desc_read_schemas = { sizeof(jz_str), sizeof(jazz_types_SchemaVersionId), jzn_fates_cmp_str, jzn_fates_str_drop, NULL };
/* proven: &str -> BTreeSet<RowUuid> */
static void jzn_set_drop(void *p) { jz_btree_drop(p); }
static const jz_btree_desc jzn_desc_proven = { sizeof(jz_str), sizeof(jz_btree), jzn_fates_cmp_str, jzn_fates_str_drop, jzn_set_drop };

/* ---------------- apply_fate_update ---------------- */

static jz_err jzn_apply_fate_update_once(NSA *s, jazz_types_TxId tx_id, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt,
                                          bool has_durability, u32 durability, bool *terminal_fate_persisted);

/* Apply an upstream fate update. */
jz_err jazz_node__node__ingest__NodeState__apply_fate_update(NSA *s, jazz_types_TxId tx_id, jazz_model_Fate fate, bool has_gt,
                                                             jazz_types_GlobalTime gt, bool has_durability, u32 durability) {
    jz_err err = jazz_node__node__state__durable__NodeState__require_catalogue_ready(s);
    if (JZ_IS_ERR(err)) { jazz_model_Fate_drop(&fate); return err; }
    /* debug_assert!: a global timestamp requires Global durability (debug-only, elided) */
    bool terminal_fate_persisted = false;
    jz_err result = jzn_apply_fate_update_once(s, tx_id, fate, has_gt, gt, has_durability, durability, &terminal_fate_persisted);
    if (terminal_fate_persisted) jz_btree_remove(&s->open_tx.local_permission_subjects, &tx_id, NULL, NULL);
    return result;
}

static jz_err jzn_apply_fate_update_once(NSA *s, jazz_types_TxId tx_id, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt,
                                          bool has_durability, u32 durability, bool *terminal_fate_persisted) {
    if (fate.tag != JAZZ_FATE_PENDING || has_gt) jz_btree_remove(&s->open_tx.pending_foreign_transactions, &tx_id, NULL, NULL);
    jazz_node_StoredTransaction stored;
    bool found = false;
    jz_err err = jazz_node__node__currency__NodeState__query_transaction(s, tx_id, &found, &stored);
    if (JZ_IS_ERR(err)) { jazz_model_Fate_drop(&fate); return err; }
    if (!found) { jazz_model_Fate_drop(&fate); return jzn_fates_missing_transaction(tx_id); }
    bool already_accepted = stored.fate.tag == JAZZ_FATE_ACCEPTED;
    bool previous_has_gt = stored.has_global_time;
    jazz_types_GlobalTime previous_global_time = stored.global_time;
    u32 previous_durability = stored.durability;
    jzn_U64Vec advanced_global_times = {0};
    groove_DatabaseBatch *batch = NULL;
    jazz_node_VersionRowVec global_current_updates = {0};
    jazz_node_VersionRowVec tx_versions = {0};
    jazz_node_RejectedTransaction *rejected_payload = NULL;

    if (JZ_IS_ERR(err = jazz_node__node__codec__StoredTransaction__reconcile_fate(&stored, fate, has_gt, gt, has_durability, durability)))
        goto out;
    if (stored.fate.tag == JAZZ_FATE_ACCEPTED && stored.has_global_time)
        jazz_node__node__open_tx__NodeState__record_applied_global_time_collect(s, stored.global_time, tx_id, &advanced_global_times);

    if (already_accepted && stored.tx.kind == JAZZ_TX_KIND_MERGEABLE && !stored.is_partial && stored.has_global_time &&
        previous_has_gt && stored.global_time == previous_global_time && stored.durability == previous_durability &&
        advanced_global_times.len == 0) {
        /* A complete mergeable transaction installs global current and
         * cleans ahead-current for all of its versions atomically with the
         * accepted metadata, including versions an earlier view-scoped
         * fragment stored before the completing ingest. Replaying the same
         * receipt therefore has no durable work after monotonicity/conflict
         * validation. Exclusive fragments deliberately defer current
         * installation and must still run the ordinary repair path below,
         * even when their fate metadata is unchanged. */
        *terminal_fate_persisted = true;
        goto out;
    }

    batch = groove__db__Database__open_batch(jzn_db(s));
    bool cleanup_rejected_versions = stored.fate.tag == JAZZ_FATE_REJECTED;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_versions_for_tx(s, tx_id, &tx_versions))) goto out;
    if (stored.fate.tag == JAZZ_FATE_ACCEPTED && stored.has_global_time) {
        if (JZ_IS_ERR(err = jazz_node__node__global_state__NodeState__global_current_updates_for_versions(s, batch, tx_id,
                          stored.global_time, &tx_versions, &global_current_updates)))
            goto out;
    }
    groove_Value contribution_merge;
    if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__contribution_merge_storage_value(s, stored.tx.contribution_merge,
                      &contribution_merge)))
        goto out;
    {
        groove_ValueVec values = {0};
        if (JZ_IS_ERR(err = jazz_node__node__codec__transaction_values_with_partial_flag(stored.node_alias, &stored.tx, &stored.fate,
                          stored.has_global_time, stored.global_time, stored.durability, stored.is_partial, contribution_merge, &values)))
            goto out;
        groove__db__DatabaseBatch__update(batch, "jazz_transactions", values);
    }
    if (stored.has_global_time) {
        jazz_types_GlobalTime global_time = stored.global_time;
        for (u32 i = 0; i < global_current_updates.len; i++) {
            const jazz_node_VersionRow *version = &global_current_updates.ptr[i];
            if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__write_global_current_update(s, batch, version, global_time))) goto out;
            /* History holds the row's post-image at this seq, which is
             * what peers receive for this transaction. */
            if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__write_history_post_image(s, batch, version, stored.tx.made_by))) goto out;
        }
        bool moved = global_current_updates.len != 0;
        if (stored.fate.tag == JAZZ_FATE_ACCEPTED) {
            /* Every pending record of an accepted write moves to its
             * seq, also when the row's image here did not change. */
            for (u32 i = 0; i < tx_versions.len; i++) {
                const jazz_node_VersionRow *version = &tx_versions.ptr[i];
                bool updated = false;
                for (u32 j = 0; j < global_current_updates.len && !updated; j++) {
                    const jazz_node_VersionRow *update = &global_current_updates.ptr[j];
                    jazz_types_RowUuid ur = jazz_node__node__codec__VersionRow__row_uuid(update);
                    jazz_types_RowUuid vr = jazz_node__node__codec__VersionRow__row_uuid(version);
                    updated = jz_str_eq(update->table, version->table) &&
                              jazz_model_BranchKey_eq(&update->branch_key, &version->branch_key) && jz_uuid_eq(&ur, &vr);
                }
                if (updated) continue;
                jazz_types_GlobalTime seq;
                if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__seq(version, &seq))) goto out;
                if (seq == 0) {
                    jazz_node_VersionRow at_seq;
                    if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__with_seq(version, global_time, &at_seq))) goto out;
                    err = jazz_node__node__ingest__NodeState__write_history_post_image(s, batch, &at_seq, stored.tx.made_by);
                    jazz_node_VersionRow_drop(&at_seq);
                    if (JZ_IS_ERR(err)) goto out;
                    moved = true;
                }
            }
        }
        if (moved) jazz_node__node__state__commit__NodeState__invalidate_tx_version_tables_cache(s, tx_id);
        jz_btree settling;
        jz_btree_init(&settling, &jzn_desc_txid_set_a);
        jz_btree_insert(&settling, &tx_id, NULL, NULL);
        err = jazz_node__node__ingest__NodeState__rebase_ahead_overlays(s, batch, &global_current_updates, &settling);
        jz_btree_drop(&settling);
        if (JZ_IS_ERR(err)) goto out;
    }
    /* #[cfg(test)] global_current_update_versions: test-only, skipped */
    if (stored.fate.tag == JAZZ_FATE_REJECTED || stored.has_global_time) {
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__cleanup_fated_ahead_current_for_versions(s, batch, &tx_versions))) goto out;
    }
    for (u32 i = 0; i < advanced_global_times.len; i++) {
        jazz_types_GlobalTime advanced = advanced_global_times.ptr[i];
        if (stored.has_global_time && advanced == stored.global_time) continue;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__prune_ahead_current_for_global_time(s, batch, advanced))) goto out;
    }
    if (cleanup_rejected_versions) {
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__remove_rejected_local_versions(s, tx_id, &stored, batch, &rejected_payload)))
            goto out;
    }
    {
        groove_AppliedBatch *applied = NULL;
        groove_DatabaseBatch *b = batch;
        batch = NULL;
        if (JZ_IS_ERR(err = jazz_node__node__state__commit__NodeState__apply_node_batch(s, b, &applied))) goto out;
        groove_PersistProgress *persisted = groove__db__Database__persist_with_progress(jzn_db(s), applied);
        err = groove__db__Database__finish_persistence(jzn_db(s), persisted);
        groove_AppliedBatch_drop(applied);
        if (JZ_IS_ERR(err)) goto out;
    }
    *terminal_fate_persisted = stored.fate.tag != JAZZ_FATE_PENDING;
    /* #[cfg(test)] assert_global_current_updates_match_history_for_test: skipped */
    if (rejected_payload) {
        jazz_types_TxId rid = jazz_node__node__RejectedTransaction__tx_id(rejected_payload);
        jz_btree_insert(&s->rejections.rejected_transactions, &rid, &rejected_payload, NULL);
        rejected_payload = NULL;
    }
out:
    if (batch) groove__db__DatabaseBatch__drop(batch);
    jz_vec_drop(&advanced_global_times, sizeof(u64), NULL);
    jzn_rows_drop(&global_current_updates);
    jzn_rows_drop(&tx_versions);
    jazz_model_Transaction_drop(&stored.tx);
    jazz_model_Fate_drop(&stored.fate);
    (void)rejected_payload;
    return err;
}

/* ---------------- validate_exclusive_commit_unit ---------------- */

static bool jzn_visible_global_row_tx_id_now_memoized(NSA *s, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                      jazz_types_RowUuid row_uuid, jz_btree *memo, jazz_types_TxId *out) {
    jzn_TableRowKey probe = { *table, row_uuid };
    const jzn_OptTx *hit = jz_btree_get(memo, &probe);
    if (hit) { if (hit->some) *out = hit->tx; return hit->some; }
    jzn_OptTx current = {0};
    current.some = jazz_node__node__global_state__NodeState__visible_global_row_tx_id_now(s, schema_version, table, row_uuid, &current.tx);
    jzn_TableRowKey key = { jz_str_clone(table), row_uuid };
    jz_btree_insert(memo, &key, &current, NULL);
    if (current.some) *out = current.tx;
    return current.some;
}

static bool jzn_visible_global_tx_id_now_memoized(NSA *s, jazz_types_PhysicalTableId table_id, jazz_types_RowUuid row_uuid, jz_btree *memo,
                                                  jazz_types_TxId *out) {
    jzn_PhysRowKey key = { table_id, row_uuid };
    const jzn_OptTx *hit = jz_btree_get(memo, &key);
    if (hit) { if (hit->some) *out = hit->tx; return hit->some; }
    jzn_OptTx current = {0};
    current.some = jazz_node__node__global_state__NodeState__visible_global_tx_id_for_physical_table_now(s, table_id, row_uuid, &current.tx);
    jz_btree_insert(memo, &key, &current, NULL);
    if (current.some) *out = current.tx;
    return current.some;
}

static jz_err jzn_predicate_read_differs_from_proven(NSA *s, const jazz_model_PredicateRead *predicate, const jazz_model_Snapshot *snapshot,
                                                     jazz_types_AuthorSubject identity, const jz_btree *proven, bool *out);

/* point-read table lookup: the one schema whose mappings agree on the table's physical id */
static bool jzn_read_schema_for_table(NSA *s, const jz_str *table, jazz_types_SchemaVersionId *out) {
    bool have = false;
    jazz_types_PhysicalTableId first_id = 0;
    jazz_types_SchemaVersionId *schema;
    jazz_node_SchemaPhysicalMapping *mapping;
    JZ_BTREE_FOR(&s->catalogue.physical_mappings, schema, mapping) {
        const jazz_node_TablePhysicalMapping *t = jz_btree_get(&mapping->tables, table);
        if (!t) continue;
        if (!have) { have = true; first_id = t->table_id; *out = *schema; continue; }
        if (t->table_id != first_id) return false;
    }
    return have;
}

/* Return locally visible current cells for one row. */
jz_err jazz_node__node__ingest__NodeState__validate_exclusive_commit_unit(NSA *s, const jazz_model_Transaction *tx,
                                                                          const jazz_protocol_VersionRecordVec *versions, bool *out) {
    *out = false;
    const jazz_model_Snapshot *base_snapshot = tx->base_snapshot;
    if (!base_snapshot) return JZ_OK;
    /* Point-read records carry names, not schema IDs. A retained alias is
     * usable only when every mapping agrees on its physical identity.
     * ponytail: scans retained schemas per distinct read table; index alias
     * agreement at catalogue activation if this becomes a measured cost. */
    jz_btree read_schemas, visible_row_memo, visible_row_winner_memo, proven;
    jz_btree_init(&read_schemas, &jzn_desc_read_schemas);
    jz_btree_init(&visible_row_memo, &jzn_desc_row_memo);
    jz_btree_init(&visible_row_winner_memo, &jzn_desc_winner_memo);
    jz_btree_init(&proven, &jzn_desc_proven);
    jz_err err = JZ_OK;
    u32 n_rows = tx->row_read_set ? tx->row_read_set->len : 0;
    u32 n_absent = tx->absent_read_set ? tx->absent_read_set->len : 0;
    u32 n_pred = tx->predicate_read_set ? tx->predicate_read_set->len : 0;
    for (u32 i = 0; i < n_rows + n_absent; i++) {
        const jz_str *table = i < n_rows ? &tx->row_read_set->v.rows[i].table : &tx->absent_read_set->v.absent[i - n_rows].table;
        if (jz_btree_contains(&read_schemas, table)) continue;
        jazz_types_SchemaVersionId schema;
        if (!jzn_read_schema_for_table(s, table, &schema)) goto done;
        jz_str key = jz_str_clone(table);
        jz_btree_insert(&read_schemas, &key, &schema, NULL);
    }
    /* Read sets and write CAS both validate the current row image, whose
     * deletion state is one of its cells. */
    for (u32 i = 0; i < n_rows; i++) {
        const jazz_model_RowRead *read = &tx->row_read_set->v.rows[i];
        const jazz_types_SchemaVersionId *schema = jz_btree_get(&read_schemas, &read->table);
        jazz_types_TxId current;
        bool some = jzn_visible_global_row_tx_id_now_memoized(s, *schema, &read->table, read->row_uuid, &visible_row_memo, &current);
        if (!some || !jzn_txid_eq(&current, &read->version)) goto done;
    }
    for (u32 i = 0; i < n_absent; i++) {
        const jazz_model_AbsentRead *absent = &tx->absent_read_set->v.absent[i];
        const jazz_types_SchemaVersionId *schema = jz_btree_get(&read_schemas, &absent->table);
        jazz_types_TxId current;
        if (jzn_visible_global_row_tx_id_now_memoized(s, *schema, &absent->table, absent->row_uuid, &visible_row_memo, &current)) goto done;
    }
    /* Predicate reads are validated against the rows the transaction
     * proved it read, not against the authority's state at the base
     * snapshot: a client that read offline, or from a replica that never
     * held every row below its base, only saw what its proofs name
     * (garden-co/jazz#3694). The row proofs above are all still current. */
    jazz_types_AuthorSubject identity = tx->has_permission_subject ? tx->permission_subject : tx->made_by;
    for (u32 i = 0; i < n_rows; i++) {
        const jazz_model_RowRead *read = &tx->row_read_set->v.rows[i];
        bool inserted = false;
        jz_btree *set = jz_btree_entry(&proven, &read->table, &inserted);
        if (inserted) {
            /* entry() stored a bitwise key copy; own it */
            void *k, *v;
            jz_btree_iter it;
            jz_btree_range(&it, &proven, &read->table, true, &read->table, true);
            if (jz_btree_next(&it, &k, &v)) *(jz_str *)k = jz_str_clone(&read->table);
            jz_btree_init(set, &jzn_desc_uuid_set_a);
        }
        jz_btree_insert(set, &read->row_uuid, NULL, NULL);
    }
    jz_btree no_rows;
    jz_btree_init(&no_rows, &jzn_desc_uuid_set_a);
    for (u32 i = 0; i < n_pred; i++) {
        const jazz_model_PredicateRead *predicate = &tx->predicate_read_set->v.predicates[i];
        const jz_btree *proven_rows = jz_btree_get(&proven, &predicate->table);
        if (!proven_rows) proven_rows = &no_rows;
        bool differs = false;
        if (JZ_IS_ERR(err = jzn_predicate_read_differs_from_proven(s, predicate, base_snapshot, identity, proven_rows, &differs))) break;
        if (differs) { jz_btree_drop(&no_rows); goto done; }
    }
    jz_btree_drop(&no_rows);
    if (JZ_IS_ERR(err)) goto done;
    for (u32 i = 0; i < versions->len; i++) {
        const jazz_protocol_VersionRecord *version = versions->ptr[i];
        const jz_str *table = jazz_protocol__protocol__VersionRecord__table(version);
        jazz_types_SchemaVersionId schema_version = jazz_protocol__protocol__VersionRecord__schema_version(version);
        const jazz_model_TableSchema *table_schema;
        if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, table, schema_version, &table_schema)))
            goto done;
        jazz_types_PhysicalTableId table_id;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, schema_version, table, &table_id))) goto done;
        jazz_types_TxId current;
        bool some = jzn_visible_global_tx_id_now_memoized(s, table_id, jazz_protocol__protocol__VersionRecord__row_uuid(version),
                                                          &visible_row_winner_memo, &current);
        /* First committer wins: the written register must be unchanged
         * since the transaction's base snapshot. */
        if (some && !jazz_node__node__open_tx__NodeState__snapshot_covers(s, current, base_snapshot)) goto done;
    }
    *out = true;
done:
    jz_btree_drop(&read_schemas);
    jz_btree_drop(&visible_row_memo);
    jz_btree_drop(&visible_row_winner_memo);
    jz_btree_drop(&proven);
    if (JZ_IS_ERR(err)) *out = false;
    return err;
}

jz_err jazz_node__node__ingest__NodeState__predicate_read_is_degenerate_whole_table(const NSA *s, const jazz_model_PredicateRead *p, bool *out) {
    *out = false;
    jazz_model_Query *query = jazz_model__query__Query__from_table(&p->table);
    jazz_model_ValidatedQuery *shape = NULL;
    jz_err err = jazz_model__query__Query__validate(query, &s->catalogue.schema, &shape);
    jazz_model__query__Query__drop(query);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZ_OK; }
    jazz_model_Binding *binding = NULL;
    jz_btree empty = {0};
    err = jazz_model__query__ValidatedQuery__bind(shape, empty, &binding);
    if (JZ_IS_ERR(err)) { jazz_model__query__ValidatedQuery__drop(shape); return err; }
    jazz_model_ShapeId sid = jazz_model__query__ValidatedQuery__shape_id(shape);
    jazz_model_BindingId bid = jazz_model__query__Binding__binding_id(binding);
    *out = jazz_model_ShapeId_eq(&p->shape_id, &sid) && jazz_model_BindingId_eq(&p->binding_id, &bid);
    jazz_model__query__Binding__drop(binding);
    jazz_model__query__ValidatedQuery__drop(shape);
    return JZ_OK;
}

static jz_err jzn_shape_output_tx_set_now(NSA *s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jz_btree *out);
static jz_err jzn_shape_output_tx_set_at_snapshot(NSA *s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                                  const jazz_model_Snapshot *snapshot, jz_btree *out);

/* validate `shape` against `schema`, keeping it only if its shape id matches */
static jazz_model_ValidatedQuery *jzn_validate_matching_shape(const jazz_model_PredicateRead *predicate, const jazz_model_JazzSchema *schema) {
    jazz_model_ValidatedQuery *shape = NULL;
    jz_err err = jazz_model__query__Query__validate(predicate->shape, schema, &shape);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return NULL; }
    jazz_model_ShapeId sid = jazz_model__query__ValidatedQuery__shape_id(shape);
    if (!jazz_model_ShapeId_eq(&sid, &predicate->shape_id)) { jazz_model__query__ValidatedQuery__drop(shape); return NULL; }
    return shape;
}

/* The shape and binding an authority evaluates to validate a predicate
 * read: the read's own shape, or for an aggregate the rows it consumed.
 * `None` (*shape_out == NULL) when the read no longer names a known shape and binding. */
static jz_err jzn_predicate_read_validation_shape(const NSA *s, const jazz_model_PredicateRead *predicate,
                                                  jazz_model_ValidatedQuery **shape_out, jazz_model_Binding **binding_out) {
    *shape_out = NULL;
    *binding_out = NULL;
    /* Shape IDs include the authoring schema. A migration must not make an
     * unchanged read conflict merely because this authority uses another view. */
    const jazz_model_JazzSchema *schema = &s->catalogue.schema;
    jazz_model_ValidatedQuery *shape = jzn_validate_matching_shape(predicate, schema);
    if (!shape) {
        jazz_types_SchemaVersionId *k;
        jazz_model_SchemaVersion *v;
        JZ_BTREE_FOR(&s->catalogue.catalogue_schemas, k, v) {
            (void)k;
            if ((shape = jzn_validate_matching_shape(predicate, &v->schema))) { schema = &v->schema; break; }
        }
    }
    if (!shape) return JZ_OK;
    jz_btree values;
    jazz_model__query__binding_values_clone(&values, &predicate->binding_values);
    jazz_model_Binding *binding = NULL;
    jz_err err = jazz_model__query__ValidatedQuery__bind(shape, values, &binding);
    if (JZ_IS_ERR(err)) { jazz_model__query__ValidatedQuery__drop(shape); return err; }
    jazz_model_BindingId bid = jazz_model__query__Binding__binding_id(binding);
    if (!jazz_model_BindingId_eq(&bid, &predicate->binding_id)) {
        jazz_model__query__Binding__drop(binding);
        jazz_model__query__ValidatedQuery__drop(shape);
        return JZ_OK;
    }
    const jazz_model_Query *input = jazz_model__query__ValidatedQuery__aggregate_input(shape);
    if (!input) { *shape_out = shape; *binding_out = binding; return JZ_OK; }
    jazz_model_ValidatedQuery *validated_input = NULL;
    err = jazz_model__query__Query__validate(input, schema, &validated_input);
    jazz_model__query__Binding__drop(binding);
    jazz_model__query__ValidatedQuery__drop(shape);
    JZ_TRY(err);
    jazz_model__query__binding_values_clone(&values, &predicate->binding_values);
    jazz_model_Binding *input_binding = NULL;
    err = jazz_model__query__ValidatedQuery__bind(validated_input, values, &input_binding);
    if (JZ_IS_ERR(err)) { jazz_model__query__ValidatedQuery__drop(validated_input); return err; }
    *shape_out = validated_input;
    *binding_out = input_binding;
    return JZ_OK;
}

/* Whether an exclusive predicate read no longer returns what the
 * transaction read. Every row the predicate returns now, evaluated as the
 * transaction's permission subject, must be one the transaction proved
 * it read; otherwise a row appeared or changed into the result since. A
 * proved row the predicate returned at the base snapshot but no longer
 * returns left the result through another row (a join or policy
 * source). An aggregate read is validated through the rows it consumed.
 *
 * A client that recorded no row proofs for its predicate reads (before
 * alpha.58) conflicts whenever such a read returned rows. */
static jz_err jzn_predicate_read_differs_from_proven(NSA *s, const jazz_model_PredicateRead *predicate, const jazz_model_Snapshot *snapshot,
                                                     jazz_types_AuthorSubject identity, const jz_btree *proven, bool *out) {
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    JZ_TRY(jzn_predicate_read_validation_shape(s, predicate, &shape, &binding));
    if (!shape) { *out = true; return JZ_OK; }
    const jz_str *table = jazz_model__query__ValidatedQuery__table(shape);
    jazz_model_CurrentRowVec rows = {0};
    jz_btree now, at_base;
    jz_btree_init(&now, &jzn_desc_uuid_set_a);
    jz_btree_init(&at_base, &jzn_desc_row_tx_set);
    jz_err err = jazz_node__node__query_eval__NodeState__query_rows_with_prepared_plan_for_identity(s, shape, binding, JAZZ_DURABILITY_GLOBAL,
                                                                                                 NULL, identity, &rows);
    if (JZ_IS_ERR(err)) goto out;
    for (u32 i = 0; i < rows.len; i++) {
        if (!jz_str_eq(&rows.ptr[i].table, table)) continue;
        jazz_types_RowUuid row = jazz_model__row__CurrentRow__row_uuid(&rows.ptr[i]);
        jz_btree_insert(&now, &row, NULL, NULL);
    }
    /* now.is_subset(proven) */
    bool subset = true;
    {
        jazz_types_RowUuid *row;
        void *unit;
        JZ_BTREE_FOR(&now, row, unit) {
            (void)unit;
            if (!jz_btree_contains(proven, row)) { subset = false; break; }
        }
    }
    if (!subset) { *out = true; goto out; }
    if (JZ_IS_ERR(err = jzn_shape_output_tx_set_at_snapshot(s, shape, binding, snapshot, &at_base))) goto out;
    *out = false;
    {
        jzn_RowTx *entry;
        void *unit;
        JZ_BTREE_FOR(&at_base, entry, unit) {
            (void)unit;
            if (jz_btree_contains(proven, &entry->row) && !jz_btree_contains(&now, &entry->row)) { *out = true; break; }
        }
    }
out:
    jazz_model_CurrentRowVec_drop(&rows);
    jz_btree_drop(&now);
    jz_btree_drop(&at_base);
    jazz_model__query__Binding__drop(binding);
    jazz_model__query__ValidatedQuery__drop(shape);
    return err;
}

/* #[cfg(test)] shape_predicate_changed_after: skipped */

static bool jzn_aggregate_query_outputs_equivalent(const jazz_model_CurrentRowVec *left, const jazz_model_CurrentRowVec *right);

/* Authority checks use current global output; local checks include their
 * newly visible pending transactions in an explicit comparison snapshot. */
jz_err jazz_node__node__ingest__NodeState__shape_predicate_outputs_differ(NSA *s, const jazz_model_PredicateRead *predicate,
                                                                          const jazz_model_Snapshot *snapshot,
                                                                          const jazz_model_Snapshot *comparison_snapshot, bool *out) {
    /* Shape IDs include the authoring schema. A migration must not make an
     * unchanged read conflict merely because this authority uses another view. */
    jazz_model_ValidatedQuery *shape = jzn_validate_matching_shape(predicate, &s->catalogue.schema);
    if (!shape) {
        /* ponytail: only mismatched views scan schema history; carry an
         * explicit schema ID if large catalogues make this measurable. */
        jazz_types_SchemaVersionId *k;
        jazz_model_SchemaVersion *v;
        JZ_BTREE_FOR(&s->catalogue.catalogue_schemas, k, v) {
            (void)k;
            if ((shape = jzn_validate_matching_shape(predicate, &v->schema))) break;
        }
    }
    if (!shape) { *out = true; return JZ_OK; }
    jz_btree values;
    jazz_model__query__binding_values_clone(&values, &predicate->binding_values);
    jazz_model_Binding *binding = NULL;
    jz_err err = jazz_model__query__ValidatedQuery__bind(shape, values, &binding);
    if (JZ_IS_ERR(err)) { jazz_model__query__ValidatedQuery__drop(shape); return err; }
    jazz_model_BindingId bid = jazz_model__query__Binding__binding_id(binding);
    if (!jazz_model_BindingId_eq(&bid, &predicate->binding_id)) { *out = true; goto out; }
    if (jazz_model__query__ValidatedQuery__has_aggregate(shape)) {
        jazz_model_CurrentRowVec at_base = {0}, at_now = {0};
        err = jazz_node__node__query_eval__NodeState__query_rows_at_snapshot(s, shape, binding, snapshot, &at_base);
        if (!JZ_IS_ERR(err)) {
            if (comparison_snapshot)
                err = jazz_node__node__query_eval__NodeState__query_rows_at_snapshot(s, shape, binding, comparison_snapshot, &at_now);
            else
                err = jazz_node__node__query_eval__authorization__NodeState__query_rows(s, shape, binding, JAZZ_DURABILITY_GLOBAL, &at_now);
        }
        if (!JZ_IS_ERR(err)) *out = !jzn_aggregate_query_outputs_equivalent(&at_base, &at_now);
        jazz_model_CurrentRowVec_drop(&at_base);
        jazz_model_CurrentRowVec_drop(&at_now);
        goto out;
    }
    {
        jz_btree at_base, at_now;
        jz_btree_init(&at_base, &jzn_desc_row_tx_set);
        jz_btree_init(&at_now, &jzn_desc_row_tx_set);
        err = jzn_shape_output_tx_set_at_snapshot(s, shape, binding, snapshot, &at_base);
        if (!JZ_IS_ERR(err)) {
            if (comparison_snapshot) err = jzn_shape_output_tx_set_at_snapshot(s, shape, binding, comparison_snapshot, &at_now);
            else err = jzn_shape_output_tx_set_now(s, shape, binding, &at_now);
        }
        if (!JZ_IS_ERR(err)) *out = !jz_btree_eq(&at_base, &at_now, jzn_unit_veq);
        jz_btree_drop(&at_base);
        jz_btree_drop(&at_now);
    }
out:
    jazz_model__query__Binding__drop(binding);
    jazz_model__query__ValidatedQuery__drop(shape);
    return err;
}

static const jz_btree_desc jzn_desc_uuid_row_ptr = { sizeof(jazz_types_RowUuid), sizeof(const jazz_model_CurrentRow *), jz_cmp_uuid, NULL, NULL };

static bool jzn_aggregate_query_outputs_equivalent(const jazz_model_CurrentRowVec *left, const jazz_model_CurrentRowVec *right) {
    if (left->len != right->len) return false;
    jz_btree right_by_id;
    jz_btree_init(&right_by_id, &jzn_desc_uuid_row_ptr);
    bool result = true;
    for (u32 i = 0; i < right->len && result; i++) {
        jazz_types_RowUuid id = jazz_model__row__CurrentRow__row_uuid(&right->ptr[i]);
        const jazz_model_CurrentRow *row = &right->ptr[i];
        if (jz_btree_insert(&right_by_id, &id, &row, NULL)) result = false;
    }
    for (u32 i = 0; i < left->len && result; i++) {
        jazz_types_RowUuid id = jazz_model__row__CurrentRow__row_uuid(&left->ptr[i]);
        const jazz_model_CurrentRow *other = NULL;
        if (!jz_btree_remove(&right_by_id, &id, NULL, &other)) { result = false; break; }
        if (!jazz_model__row__CurrentRow__aggregate_payload_equivalent(&left->ptr[i], other)) result = false;
    }
    jz_btree_drop(&right_by_id);
    return result;
}

static jz_err jzn_shape_output_tx_set_now(NSA *s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jz_btree *out) {
    const jz_str *table = jazz_model__query__ValidatedQuery__table(shape);
    jazz_model_CurrentRowVec rows = {0};
    JZ_TRY(jazz_node__node__query_eval__authorization__NodeState__query_rows(s, shape, binding, JAZZ_DURABILITY_GLOBAL, &rows));
    for (u32 i = 0; i < rows.len; i++) {
        jzn_RowTx entry;
        entry.row = jazz_model__row__CurrentRow__row_uuid(&rows.ptr[i]);
        if (jazz_node__node__global_state__NodeState__visible_global_content_tx_id_in_schema_now(
                s, jazz_model__query__ValidatedQuery__schema_version(shape), table, entry.row, &entry.tx))
            jz_btree_insert(out, &entry, NULL, NULL);
    }
    jazz_model_CurrentRowVec_drop(&rows);
    return JZ_OK;
}

static jz_err jzn_shape_output_tx_set_at_snapshot(NSA *s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                                  const jazz_model_Snapshot *snapshot, jz_btree *out) {
    /* An origin snapshot has no visible application rows.  Do not ask the
     * historical compiler for provenance columns in that empty state: such
     * columns require a historical storage cut even though the result set
     * is necessarily empty (notably after a transaction reads its own
     * staged insert before the first authority receipt). */
    if (snapshot->global_base == 0 && snapshot->local_base == 0 && snapshot->dots.len == 0) return JZ_OK;
    const jz_str *table = jazz_model__query__ValidatedQuery__table(shape);
    jazz_model_CurrentRowVec rows = {0};
    JZ_TRY(jazz_node__node__query_eval__NodeState__query_rows_at_snapshot(s, shape, binding, snapshot, &rows));
    jz_err err = JZ_OK;
    for (u32 i = 0; i < rows.len; i++) {
        jzn_RowTx entry;
        entry.row = jazz_model__row__CurrentRow__row_uuid(&rows.ptr[i]);
        if (!jazz_node__node__open_tx__NodeState__snapshot_content_witness(s, jazz_model__query__ValidatedQuery__schema_version(shape), table,
                                                                          entry.row, snapshot, &entry.tx)) {
            err = JZN_ISV("historical query output row must have visible content");
            break;
        }
        jz_btree_insert(out, &entry, NULL, NULL);
    }
    jazz_model_CurrentRowVec_drop(&rows);
    return err;
}

/* #[cfg(test)] commit_unit_satisfies_write_policies: skipped */

/* (PhysicalTableId, SchemaVersionId, RowUuid, BranchKey) */
typedef struct jzn_BranchCoordinate {
    jazz_types_PhysicalTableId table_id; jazz_types_SchemaVersionId schema; jazz_types_RowUuid row; jazz_model_BranchKey branch_key;
} jzn_BranchCoordinate;
static int jzn_branch_coordinate_cmp(const void *a, const void *b, void *ctx) {
    const jzn_BranchCoordinate *x = a, *y = b;
    if (x->table_id != y->table_id) return x->table_id < y->table_id ? -1 : 1;
    int c = jz_cmp_uuid(&x->schema, &y->schema, ctx);
    if (c) return c;
    if ((c = jz_cmp_uuid(&x->row, &y->row, ctx))) return c;
    return jazz_model_BranchKey_cmp(&x->branch_key, &y->branch_key, ctx);
}
static void jzn_branch_coordinate_drop(void *p) { jazz_model_BranchKey_drop(&((jzn_BranchCoordinate *)p)->branch_key); }
static const jz_btree_desc jzn_desc_branch_coordinates = { sizeof(jzn_BranchCoordinate), 0, jzn_branch_coordinate_cmp,
    jzn_branch_coordinate_drop, NULL };

/* The rejection a commit unit's write policies call for, if any:
 * `AuthorizationDenied` when a check fails, or `MalformedCommit` naming
 * the unsupported pattern when the unit's checks would read more of its
 * own writes than the evidence budget allows (`INV-RLS-9`). */
jz_err jazz_node__node__ingest__NodeState__commit_unit_write_policy_rejection(NSA *s, const jazz_model_Transaction *tx,
                                                                              const jazz_protocol_VersionRecordVec *versions,
                                                                              const jazz_node_CommitUnitIngestContext *ctx, bool *some,
                                                                              jazz_model_RejectionReason *out) {
    *some = false;
    bool has_subject = false;
    jazz_types_AuthorSubject permission_subject;
    jazz_model_RejectionReason admission;
    if (!jazz_node__node__ingest__commit_unit_admission_subject(tx, ctx, &has_subject, &permission_subject, &admission)) {
        *some = true; *out = admission; return JZ_OK;
    }
    if (!has_subject) return JZ_OK;
    /* Non-root branch writes have mandatory canonical operation intent.
     * Do not treat an absent descriptor as an ordinary insert: that would
     * let a raw sender relabel an inherited first-head overlay and bypass
     * source read policy. The intent binds to the exact authored version,
     * and only the authority proves its declared source/existence facts. */
    JZ_VEC(const jazz_protocol_VersionRecord *) branch_versions = {0};
    for (u32 i = 0; i < versions->len; i++)
        if (jazz_protocol__protocol__VersionRecord__branch_key(versions->ptr[i])->values.len != 0)
            JZ_VEC_PUSH(branch_versions, (const jazz_protocol_VersionRecord *)versions->ptr[i]);
    jz_err err = JZ_OK;
    if (branch_versions.len != 0) {
        const jazz_model_ContributionMergeProvenance *provenance = tx->contribution_merge;
        if (tx->kind != JAZZ_TX_KIND_MERGEABLE || !provenance) goto denied;
        /* Content and deletion registers may emit separate final versions
         * for one physical branch row (for example a move's destination
         * content write plus restore). They share one operation identity;
         * require exactly one canonical intent per physical row/head. */
        jz_btree branch_coordinates;
        jz_btree_init(&branch_coordinates, &jzn_desc_branch_coordinates);
        bool resolvable = true;
        for (u32 i = 0; i < branch_versions.len; i++) {
            const jazz_protocol_VersionRecord *version = branch_versions.ptr[i];
            jzn_BranchCoordinate c;
            c.schema = jazz_protocol__protocol__VersionRecord__schema_version(version);
            jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, c.schema,
                           jazz_protocol__protocol__VersionRecord__table(version), &c.table_id);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); resolvable = false; break; }
            c.row = jazz_protocol__protocol__VersionRecord__row_uuid(version);
            jazz_model_BranchKey_clone(&c.branch_key, jazz_protocol__protocol__VersionRecord__branch_key(version));
            if (jz_btree_contains(&branch_coordinates, &c)) jzn_branch_coordinate_drop(&c);
            else jz_btree_insert(&branch_coordinates, &c, NULL, NULL);
        }
        u32 n_coordinates = branch_coordinates.len;
        jz_btree_drop(&branch_coordinates);
        if (!resolvable || provenance->branch_write_intents.len != n_coordinates) goto denied;
        for (u32 k = 0; k < provenance->branch_write_intents.len; k++) {
            const jazz_model_BranchWriteIntent *intent = &provenance->branch_write_intents.ptr[k];
            u32 matching = 0;
            bool any_deleted = false;
            for (u32 i = 0; i < branch_versions.len; i++) {
                const jazz_protocol_VersionRecord *version = branch_versions.ptr[i];
                jazz_types_SchemaVersionId sv = jazz_protocol__protocol__VersionRecord__schema_version(version);
                jazz_types_RowUuid row = jazz_protocol__protocol__VersionRecord__row_uuid(version);
                if (!jz_uuid_eq(&sv, &intent->authored_schema) || !jz_uuid_eq(&row, &intent->row_uuid) ||
                    !jazz_model_BranchKey_eq(jazz_protocol__protocol__VersionRecord__branch_key(version), &intent->head))
                    continue;
                jazz_types_PhysicalTableId table_id;
                jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(s, sv,
                               jazz_protocol__protocol__VersionRecord__table(version), &table_id);
                if (JZ_IS_ERR(e)) { jz_err_drop(&e); continue; }
                if (table_id != intent->physical_table_id) continue;
                matching++;
                if (jazz_protocol__protocol__VersionRecord__deletion(version) == JZN_DELETION_DELETED) any_deleted = true;
            }
            if (matching == 0) goto denied;
            switch (intent->operation.tag) {
            case JAZZ_BWO_VIEW_UPDATE_COPY: {
                if (any_deleted) goto denied;
                bool satisfied = false;
                if (JZ_IS_ERR(err = jazz_node__node__policy__NodeState__branch_view_copy_satisfies_read_for_write_visibility(
                                  s, &intent->operation.copy, intent->authored_schema, permission_subject, &tx->tx_id, &satisfied)))
                    goto out;
                if (!satisfied) goto denied;
                break;
            }
            /* Linear history: insert vs update is Core's current
             * row, which the ordinary policy checks below evaluate. */
            case JAZZ_BWO_EXACT_HEAD_INSERT:
            case JAZZ_BWO_EXACT_HEAD_UPDATE:
                break;
            }
        }
    }
    if (tx->kind == JAZZ_TX_KIND_MERGEABLE) {
        for (u32 i = 0; i < versions->len; i++) {
            bool satisfied = false;
            if (JZ_IS_ERR(err = jazz_node__node__policy__NodeState__version_satisfies_read_for_write_visibility(s, versions->ptr[i],
                              permission_subject, &tx->tx_id, &satisfied)))
                goto out;
            if (!satisfied) goto denied;
        }
    }
    {
        u32 decision;
        jz_str unsupported = {0};
        if (JZ_IS_ERR(err = jazz_node__node__policy__NodeState__commit_unit_write_policies_allow(s, versions, permission_subject, tx->tx_id,
                          &decision, &unsupported)))
            goto out;
        switch (decision) {
        case JZN_UNIT_POLICY_ALLOWED: break;
        case JZN_UNIT_POLICY_DENIED: jz_vec_drop(&branch_versions, sizeof(void *), NULL); goto denied_no_free;
        case JZN_UNIT_POLICY_UNSUPPORTED:
            *some = true;
            *out = jzn_reason(JAZZ_REJ_MALFORMED_COMMIT);
            out->message = unsupported;
            break;
        }
    }
out:
    jz_vec_drop(&branch_versions, sizeof(void *), NULL);
    return err;
denied:
    jz_vec_drop(&branch_versions, sizeof(void *), NULL);
denied_no_free:
    *some = true;
    *out = jzn_reason(JAZZ_REJ_AUTHORIZATION_DENIED);
    return JZ_OK;
}

/* Evaluate every write policy of one candidate commit unit under the
 * active exact session scope, each write seeing the unit's other writes
 * (`INV-RLS-9`). Terminal relay admission uses this, under the
 * connection's admitted claims, before it may issue a non-wire
 * authorization receipt, so a relay and the fate authority decide the
 * unit alike. */
jz_err jazz_node__node__ingest__NodeState__commit_unit_satisfies_write_policy(NSA *s, const jazz_protocol_VersionRecordVec *versions,
                                                                              jazz_types_AuthorSubject author, jazz_types_TxId candidate,
                                                                              bool *out) {
    u32 decision;
    jz_str unsupported = {0};
    JZ_TRY(jazz_node__node__policy__NodeState__commit_unit_write_policies_allow(s, versions, author, candidate, &decision, &unsupported));
    jz_str_drop(&unsupported);
    *out = decision == JZN_UNIT_POLICY_ALLOWED;
    return JZ_OK;
}

/* ---------------- parked commit units ---------------- */

/* Whether a version of the unit names, as its pending predecessor, an
 * older write of the unit's own node that is parked here for the
 * authority, and has been for less than PARKED_PREDECESSOR_WAIT_MS.
 * (Full rationale on the wait bound: see fates.rs.) */
static bool jzn_has_parked_same_node_predecessor(const NSA *s, const jazz_model_Transaction *tx, const jazz_protocol_VersionRecordVec *versions) {
    for (u32 i = 0; i < versions->len; i++) {
        jazz_protocol_RowBase base = jazz_protocol__protocol__VersionRecord__base(versions->ptr[i]);
        if (!base.has_pending) continue;
        if (jazz_node__node__ingest__pending_predecessor_refusal(base.pending, tx->tx_id) != NULL) continue;
        const jazz_node_ParkedCommitUnit *unit = jz_btree_get(&s->parking.parked_commit_units, &base.pending);
        if (!unit || unit->ingress_role != JZN_PARKED_AUTHORITY) continue;
        u64 deadline = unit->now_ms + JZN_PARKED_PREDECESSOR_WAIT_MS;
        if (deadline < unit->now_ms) deadline = UINT64_MAX; /* saturating_add */
        if (deadline > s->parking.newest_ingest_ms) return true;
    }
    return false;
}

static bool jzn_schemas_known(const NSA *s, const jazz_protocol_VersionRecordVec *versions) {
    for (u32 i = 0; i < versions->len; i++) {
        jazz_types_SchemaVersionId v = jazz_protocol__protocol__VersionRecord__schema_version(versions->ptr[i]);
        if (!jz_btree_contains(&s->catalogue.catalogue_schemas, &v)) return false;
    }
    return true;
}

jz_err jazz_node__node__ingest__NodeState__park_commit_unit_if_missing_schema_versions_with_mode(NSA *s, const jazz_model_Transaction *tx,
                                                                                                 const jazz_protocol_VersionRecordVec *versions,
                                                                                                 u64 now_ms, jazz_node_CommitUnitParkMode mode,
                                                                                                 bool *out) {
    bool schemas_known = jzn_schemas_known(s, versions);
    /* A write chained on a predecessor parked here waits behind it, so
     * the authority still decides one node's writes in their order
     * (SPEC 4 §4.6). The lookup is in memory. */
    bool behind_parked_predecessor = mode.ingress_role == JZN_PARKED_AUTHORITY && jzn_has_parked_same_node_predecessor(s, tx, versions);
    if (schemas_known && !behind_parked_predecessor) { *out = false; return JZ_OK; }
    jazz_node_ParkedCommitUnit *existing = jz_btree_get(&s->parking.parked_commit_units, &tx->tx_id);
    if (existing) {
        if (!jazz_model_Transaction_eq(&existing->tx, tx) || !jzn_fates_version_vecs_eq(&existing->versions, versions))
            return jzn_fates_conflicting_commit_unit(tx->tx_id);
        if (!jazz_node__node__CommitUnitIngestContext__same_parked_authority(existing->has_ingest_context ? &existing->ingest_context : NULL,
                                                                             mode.has_ingest_context ? &mode.ingest_context : NULL))
            return jzn_fates_conflicting_commit_unit(tx->tx_id);
        if (existing->has_ingest_context && mode.has_ingest_context)
            existing->ingest_context.version_receipts_validated &= mode.ingest_context.version_receipts_validated;
        existing->ingress_role = jazz_node__node__codec__ParkedIngressRole__strongest(existing->ingress_role, mode.ingress_role);
        *out = true;
        return JZ_OK;
    }
    s->sync_metrics.parked_orphans += 1;
    if (!schemas_known) {
        s->sync_metrics.parked_catalogue_orphans += 1;
        jz_btree_insert(&s->parking.parked_catalogue_commit_units, &tx->tx_id, NULL, NULL);
    }
    jazz_node_ParkedCommitUnit unit;
    jazz_model_Transaction_clone(&unit.tx, tx);
    jazz_protocol_VersionRecordVec_clone(&unit.versions, versions);
    unit.now_ms = now_ms;
    unit.has_ingest_context = mode.has_ingest_context;
    unit.ingest_context = mode.ingest_context;
    unit.ingress_role = mode.ingress_role;
    jz_btree_insert(&s->parking.parked_commit_units, &tx->tx_id, &unit, NULL);
    *out = true;
    return JZ_OK;
}

typedef JZ_VEC(jazz_types_TxId) jzn_TxIdVec;

jz_err jazz_node__node__ingest__NodeState__drain_parked_commit_units(NSA *s, jazz_node_PublicationOutcome *out) {
    jazz_node_PublicationOutcome updates;
    jazz_node__node__ingest__settled_empty(&updates);
    for (;;) {
        jzn_TxIdVec ready = {0};
        jazz_types_TxId *tx_id;
        jazz_node_ParkedCommitUnit *unit;
        JZ_BTREE_FOR(&s->parking.parked_commit_units, tx_id, unit) {
            if (unit->ingress_role != JZN_PARKED_RELAY && jzn_schemas_known(s, &unit->versions)
                /* Its parked predecessor goes first, on a later pass. */
                && !jzn_has_parked_same_node_predecessor(s, &unit->tx, &unit->versions))
                JZ_VEC_PUSH(ready, *tx_id);
        }
        if (ready.len == 0) { jz_vec_drop(&ready, sizeof(jazz_types_TxId), NULL); break; }
        for (u32 i = 0; i < ready.len; i++) {
            jazz_types_TxId id = ready.ptr[i];
            jazz_node_ParkedCommitUnit taken;
            if (!jz_btree_remove(&s->parking.parked_commit_units, &id, NULL, &taken)) continue;
            s->sync_metrics.parked_orphans_resolved += 1;
            if (jz_btree_remove(&s->parking.parked_catalogue_commit_units, &id, NULL, NULL))
                s->sync_metrics.parked_catalogue_orphans_resolved += 1;
            jazz_node_PublicationOutcome outcome;
            jz_err err = jazz_node__node__ingest__NodeState__ingest_commit_unit_once(s, taken.tx, taken.versions, taken.now_ms,
                             taken.has_ingest_context ? &taken.ingest_context : NULL, &outcome);
            if (JZ_IS_ERR(err)) {
                jz_vec_drop(&ready, sizeof(jazz_types_TxId), NULL);
                jazz_node_PublicationOutcome_drop(&updates);
                return err;
            }
            jazz_node__node__PublicationOutcome__append_outcome(&updates, &outcome);
        }
        jz_vec_drop(&ready, sizeof(jazz_types_TxId), NULL);
    }
    *out = updates;
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__drain_parked_relay_commit_units(NSA *s) {
    for (;;) {
        jzn_TxIdVec ready = {0};
        jazz_types_TxId *tx_id;
        jazz_node_ParkedCommitUnit *unit;
        JZ_BTREE_FOR(&s->parking.parked_commit_units, tx_id, unit) {
            if (unit->ingress_role == JZN_PARKED_RELAY && jzn_schemas_known(s, &unit->versions)) JZ_VEC_PUSH(ready, *tx_id);
        }
        if (ready.len == 0) { jz_vec_drop(&ready, sizeof(jazz_types_TxId), NULL); break; }
        for (u32 i = 0; i < ready.len; i++) {
            jazz_types_TxId id = ready.ptr[i];
            jazz_node_ParkedCommitUnit taken;
            if (!jz_btree_remove(&s->parking.parked_commit_units, &id, NULL, &taken)) continue;
            /* A relay has no fate authority. Once its deferred schema is
             * known, an incomplete row record has a terminal local
             * disposition: discard it without writing a synthetic rejected
             * transaction or failing the catalogue publication that made
             * the violation observable. */
            jz_str reason = {0};
            if (jazz_node__node__ingest__NodeState__malformed_authored_version_reason(
                    s, (const jazz_protocol_VersionRecord *const *)taken.versions.ptr, taken.versions.len, &reason)) {
                jz_str_drop(&reason);
                jz_btree_remove(&s->parking.parked_catalogue_commit_units, &id, NULL, NULL);
                s->sync_metrics.dropped_malformed_relay_commit_units += 1;
                jzn_parked_unit_drop(&taken);
                continue;
            }
            s->sync_metrics.parked_orphans_resolved += 1;
            if (jz_btree_remove(&s->parking.parked_catalogue_commit_units, &id, NULL, NULL))
                s->sync_metrics.parked_catalogue_orphans_resolved += 1;
            jz_err err = jazz_node__node__ingest__NodeState__ingest_relay_commit_unit_once(s, taken.tx, taken.versions);
            if (JZ_IS_ERR(err)) { jz_vec_drop(&ready, sizeof(jazz_types_TxId), NULL); return err; }
        }
        jz_vec_drop(&ready, sizeof(jazz_types_TxId), NULL);
    }
    return JZ_OK;
}

/* #[cfg(test)] transaction_ids: skipped */

static void jzn_value_clone_into(groove_ValueVec *dst, const groove_ValueVec *src) {
    jz_vec_clone(dst, src, sizeof(groove_Value), groove_Value_clone);
}

jz_err jazz_node__node__ingest__NodeState__remove_rejected_local_versions(NSA *s, jazz_types_TxId tx_id, const jazz_node_StoredTransaction *tx,
                                                                          groove_DatabaseBatch *batch, jazz_node_RejectedTransaction **out) {
    *out = NULL;
    jazz_node_VersionRowVec rejected = {0};
    JZ_TRY(jazz_node__node__currency__NodeState__query_versions_for_tx(s, tx_id, &rejected));
    if (rejected.len == 0) { jzn_rows_drop(&rejected); return JZ_OK; }
    /* `affected` (BTreeSet<(table, row_uuid)>) is computed and discarded in Rust; elided. */
    jz_err err = JZ_OK;
    jazz_node_RejectedTransaction *rejected_payload = NULL;
    if (jz_uuid_eq(&tx_id.node, &s->node_uuid) && tx->fate.tag == JAZZ_FATE_REJECTED) {
        groove_ValueVec rejected_tx_values = {0};
        if (JZ_IS_ERR(err = jazz_node__node__codec__rejected_transaction_values(tx->node_alias, &tx->tx, &tx->fate.reason, &rejected_tx_values)))
            goto out;
        groove_ValueVec inserted = {0};
        jzn_value_clone_into(&inserted, &rejected_tx_values);
        groove__db__DatabaseBatch__insert(batch, "jazz_rejected_transactions", inserted);
        groove_TableSchemaVec tables = {0};
        jazz_model__schema__JazzSchema__storage_tables(&s->catalogue.schema, &tables);
        const groove_TableSchema *rejected_tx_table = NULL;
        for (u32 i = 0; i < tables.len; i++)
            if (jz_str_eq_cstr(&tables.ptr[i].name, "jazz_rejected_transactions")) { rejected_tx_table = &tables.ptr[i]; break; }
        if (!rejected_tx_table) {
            jzn_values_vec_drop(&rejected_tx_values);
            jz_vec_drop(&tables, sizeof(groove_TableSchema), groove_TableSchema_drop);
            err = JZN_ISV("missing rejected transaction table");
            goto out;
        }
        groove_OwnedRecord rejected_tx_record;
        err = jazz_node__node__codec__owned_record_from_storage_values(rejected_tx_table, rejected_tx_values, &rejected_tx_record);
        jz_vec_drop(&tables, sizeof(groove_TableSchema), groove_TableSchema_drop);
        if (JZ_IS_ERR(err)) goto out;
        jazz_node_RejectedVersionVec rejected_versions = {0};
        for (u32 i = 0; i < rejected.len; i++) {
            const jazz_node_VersionRow *version = &rejected.ptr[i];
            jazz_types_SchemaVersionId schema_version;
            if (!jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(
                    s, jazz_node__node__codec__VersionRow__schema_version_alias(version), &schema_version)) {
                err = JZN_ISV("unknown schema version alias");
                break;
            }
            const jazz_model_TableSchema *table_schema;
            if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, version->table, schema_version,
                              &table_schema)))
                break;
            groove_TableSchema rejected_version_table = jazz_model__schema__TableSchema__rejected_versions_storage_table(table_schema);
            groove_ValueVec rejected_version_values = {0};
            err = jazz_node__node__codec__rejected_version_values(table_schema, version, &rejected_version_values);
            groove_OwnedRecord rejected_version_record;
            if (!JZ_IS_ERR(err))
                err = jazz_node__node__codec__owned_record_from_storage_values(&rejected_version_table, rejected_version_values,
                                                                               &rejected_version_record);
            groove_TableSchema_drop(&rejected_version_table);
            if (JZ_IS_ERR(err)) break;
            groove_InternStr storage_table;
            groove_VariantRecord storage_record;
            if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__rejected_version_storage_write_binding(s, version, &rejected_version_record,
                              &storage_table, &storage_record))) {
                groove_OwnedRecord_drop(&rejected_version_record);
                break;
            }
            groove__db__DatabaseBatch__insert_variant(batch, storage_table, storage_record);
            JZ_VEC_PUSH(rejected_versions, jazz_node__node__RejectedVersion__new(jz_str_clone(version->table), rejected_version_record));
        }
        if (JZ_IS_ERR(err)) {
            groove_OwnedRecord_drop(&rejected_tx_record);
            jz_vec_drop(&rejected_versions, sizeof(jazz_node_RejectedVersion *), NULL);
            goto out;
        }
        jz_sort(rejected_versions.ptr, rejected_versions.len, sizeof(jazz_node_RejectedVersion *), jazz_node__node__RejectedVersion__sort_key_cmp,
                NULL);
        rejected_payload = jazz_node__node__RejectedTransaction__new(tx_id, rejected_tx_record, rejected_versions);
    }
    for (u32 i = 0; i < rejected.len; i++) {
        const jazz_node_VersionRow *version = &rejected.ptr[i];
        bool deleted = false;
        if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__write_ahead_current_delete(s, batch, version, &deleted))) goto out;
        if (deleted && JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__restore_ahead_overlay_after_reject(s, batch, version))) goto out;
        groove_InternStr history_table;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_table_for_row(s, version, &history_table))) goto out;
        jazz_node_PrimaryKeyValue key;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, version, &key))) goto out;
        groove__db__DatabaseBatch__delete(batch, history_table, key);
    }
    /* The rejected images are gone, so is the list naming them. */
    {
        groove_PrimaryKeyValue *parts[2] = { groove__storage__PrimaryKeyValue__u64(tx_id.time), groove__storage__PrimaryKeyValue__u64(tx->node_alias) };
        groove__db__DatabaseBatch__delete_str(batch, JZN_TX_TOUCHED_ROWS_TABLE, groove__storage__PrimaryKeyValue__composite(parts, 2));
    }
    jazz_node__node__state__commit__NodeState__invalidate_tx_version_tables_cache(s, tx_id);
    *out = rejected_payload;
    rejected_payload = NULL;
out:
    jzn_rows_drop(&rejected);
    (void)rejected_payload; /* dropped with the batch on error in Rust */
    return err;
}

/* The cheap identity part of commit-unit admission: the permission subject
 * write policies evaluate, or the refusal when the connection may not make
 * the unit at all. `Ok(None)` means no policy evaluation is owed (a trusted
 * admin, or a relay whose connection already proved the write). It reads
 * no storage, so the fate authority runs it before holding a unit. */
bool jazz_node__node__ingest__commit_unit_admission_subject(const jazz_model_Transaction *tx, const jazz_node_CommitUnitIngestContext *ctx,
                                                            bool *some, jazz_types_AuthorSubject *subject, jazz_model_RejectionReason *reason) {
    *some = false;
    jazz_types_AuthorSubject permission_subject;
    if (ctx) {
        if (ctx->trust == JZN_TRUST_ADMIN) return true;
        if (ctx->trust == JZN_TRUST_SESSION && !jazz_types__ids__AuthorSubject__eq(&tx->made_by, &ctx->identity)) {
            *reason = jzn_reason(JAZZ_REJ_AUTHORIZATION_DENIED);
            return false;
        }
        switch (ctx->trust) {
        case JZN_TRUST_SESSION: permission_subject = ctx->identity; break;
        /* Relay transport has no permission subject. A relayed
         * write must reach a serving authority through its
         * topology-owned admission path; it cannot borrow SYSTEM
         * or the transport identity here. */
        case JZN_TRUST_RELAY:
            if (ctx->admitted_write_authorization) return true;
            *reason = jzn_reason(JAZZ_REJ_AUTHORIZATION_DENIED);
            return false;
        case JZN_TRUST_BACKEND:
        case JZN_TRUST_AUTHORITY:
            permission_subject = tx->has_permission_subject ? tx->permission_subject : tx->made_by;
            break;
        default: jz_panic("handled above");
        }
    } else {
        permission_subject = tx->has_permission_subject ? tx->permission_subject : tx->made_by;
    }
    /* Gate the effective permission subject so relayed anonymous sessions
     * stay read-only without changing trusted-backend attribution. */
    if (jazz_types__ids__AuthorSubject__is_anonymous(&permission_subject)) {
        *reason = jzn_reason(JAZZ_REJ_AUTHORIZATION_DENIED);
        return false;
    }
    *some = true;
    *subject = permission_subject;
    return true;
}
