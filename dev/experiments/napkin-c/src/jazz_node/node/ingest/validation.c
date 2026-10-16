/* ingest/validation.c: transaction + version staging, authored-version
 * validation, malformed-commit rejection (include!d into jazz_node::node::ingest).
 * Rust: crates/jazz/layers/node/src/node/ingest/validation.rs */
#include "../ingest.h"

static jz_err jzn_val_conflicting_commit_unit(jazz_types_TxId tx) {
    jz_str d = {0};
    jazz_types__ids__TxId__debug(&d, &tx);
    jz_err e = jz_err_fmt(JZN_E_CONFLICTING_COMMIT_UNIT, "conflicting commit unit for transaction %.*s", (int)d.len, d.ptr);
    jz_str_drop(&d);
    return e;
}
static void jzn_val_rows_drop(jazz_node_VersionRowVec *v) { jz_vec_drop(v, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop); }
static void jzn_val_rows_clone(jazz_node_VersionRowVec *dst, const jazz_node_VersionRowVec *src) {
    jz_vec_clone(dst, src, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_clone);
}

jz_err jazz_node__node__ingest__NodeState__ingest_transaction_and_versions(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
                                                                           jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability) {
    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    jazz_node_VersionRowVec none = {0};
    return jazz_node__node__ingest__NodeState__ingest_transaction_and_versions_with_current_indexes_in_batch(
        s, batch, tx, versions, none, fate, has_gt, gt, durability, true, false);
}

jz_err jazz_node__node__ingest__NodeState__ingest_transaction_fragment_without_current_indexes(NSA *s, jazz_model_Transaction tx,
                                                                                               jazz_protocol_VersionRecordVec versions,
                                                                                               jazz_model_Fate fate, bool has_gt,
                                                                                               jazz_types_GlobalTime gt, u32 durability) {
    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    jazz_node_VersionRowVec none = {0};
    return jazz_node__node__ingest__NodeState__ingest_transaction_and_versions_with_current_indexes_in_batch(
        s, batch, tx, versions, none, fate, has_gt, gt, durability, false, true);
}

jz_err jazz_node__node__ingest__NodeState__ingest_view_scoped_transaction_with_current_indexes(NSA *s, jazz_model_Transaction tx,
                                                                                               jazz_protocol_VersionRecordVec versions,
                                                                                               jazz_model_Fate fate, bool has_gt,
                                                                                               jazz_types_GlobalTime gt, u32 durability) {
    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    jazz_node_VersionRowVec none = {0};
    return jazz_node__node__ingest__NodeState__ingest_transaction_and_versions_with_current_indexes_in_batch(
        s, batch, tx, versions, none, fate, has_gt, gt, durability, true, true);
}

static void jzn_staged_drop(jazz_node_StagedTransaction *st) {
    jzn_val_rows_drop(&st->versions);
    jazz_model_Fate_drop(&st->fate);
}

jz_err jazz_node__node__ingest__NodeState__stage_view_scoped_transaction_with_current_indexes(NSA *s, groove_DatabaseBatch *batch,
    jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability,
    jazz_node_GlobalTimeTxVec *staged_global_times, jazz_node_VersionRowVec *staged_content_versions, jazz_node_RejectedTransactionVec *staged_rejections) {
    jazz_types_TxId tx_id = tx.tx_id;
    jazz_node_StagedTransaction staged;
    JZ_TRY(jazz_node__node__ingest__NodeState__stage_transaction_and_versions_with_current_indexes(
        s, batch, tx, versions, fate, has_gt, gt, durability, true, true, staged_content_versions, &staged));
    if (staged.rejected) JZ_VEC_PUSH(*staged_rejections, staged.rejected);
    jz_err err = jazz_node__node__ingest__NodeState__finalize_staged_transaction_ingest(
        s, batch, tx_id, &staged.fate, staged.has_global_time, staged.global_time, staged_global_times, &staged.versions);
    jzn_staged_drop(&staged);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__publish_pending_transaction_and_versions(NSA *s, jazz_model_Transaction tx,
                                                                                   jazz_protocol_VersionRecordVec versions, u32 durability,
                                                                                   jazz_node_PublishedTransaction *out) {
    jazz_types_TxId tx_id = tx.tx_id;
    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    jazz_model_Fate pending = { JAZZ_FATE_PENDING };
    jazz_node_StagedTransaction staged;
    jz_err err = jazz_node__node__ingest__NodeState__stage_transaction_and_versions_with_current_indexes(
        s, batch, tx, versions, pending, false, 0, durability, true, false, NULL, &staged);
    if (JZ_IS_ERR(err)) { groove__db__DatabaseBatch__drop(batch); return err; }
    jzn_staged_drop(&staged);
    groove_AppliedBatch *persistence = NULL;
    JZ_TRY(jazz_node__node__state__commit__NodeState__apply_node_batch(s, batch, &persistence));
    jz_btree_insert(&s->pending_persistence, &tx_id, NULL, NULL);
    out->tx_id = tx_id;
    out->persistence = persistence;
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__prepare_exact_history_version(NSA *s, jazz_types_NodeAlias tx_node_alias, jazz_types_TxTime tx_time,
                                                                         jazz_types_GlobalTime seq, const jazz_protocol_VersionRecord *version,
                                                                         jazz_node_VersionRow *out) {
    jazz_types_SchemaVersionId author_schema = jazz_protocol__protocol__VersionRecord__schema_version(version);
    const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
    const jazz_model_TableSchema *table;
    /* Fail with TableNotFound before allocating aliases or resolving authored
     * columns, matching the error ordering in commit_bundles. */
    JZ_TRY(jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, vtable, author_schema, &table));
    jazz_types_SchemaVersionAlias schema_alias;
    JZ_TRY(jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(s, author_schema, &schema_alias));
    bool has_authored = false;
    jz_btree authored;
    JZ_TRY(jazz_node__node__physical__NodeState__authored_column_ids_for_names(s, author_schema, vtable,
                                                                               jazz_protocol__protocol__VersionRecord__authored_columns(version),
                                                                               &has_authored, &authored));
    jz_err err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, vtable, author_schema, &table);
    if (!JZ_IS_ERR(err))
        err = jazz_node__node__codec__VersionRow__from_wire_with_schema_version(table, version, has_authored ? &authored : NULL, tx_node_alias,
                                                                               schema_alias, tx_time, seq, out);
    if (has_authored) jz_btree_drop(&authored);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__ingest_transaction_and_versions_with_current_indexes_in_batch(NSA *s, groove_DatabaseBatch *batch,
    jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, jazz_node_VersionRowVec previously_stored, jazz_model_Fate fate,
    bool has_gt, jazz_types_GlobalTime gt, u32 durability, bool update_current_indexes, bool is_partial) {
    jazz_types_TxId tx_id = tx.tx_id;
    /* Staging owns the decoded records and derived-index working set (boxed in Rust). */
    jazz_node_StagedTransaction staged;
    jz_err err = jazz_node__node__ingest__NodeState__stage_transaction_and_versions_with_current_indexes(
        s, batch, tx, versions, fate, has_gt, gt, durability, update_current_indexes, is_partial, NULL, &staged);
    /* `previously_stored` are versions of this transaction that an earlier
     * ingest (typically a Pending view-scoped fragment) already wrote to
     * history. Staging already re-stages them on the `settle_existing` path,
     * which assembles every stored version with the incoming ones and merges
     * each row into global current exactly once. Merging them again here
     * would apply a merge column's operation twice. */
    jzn_val_rows_drop(&previously_stored);
    if (JZ_IS_ERR(err)) { groove__db__DatabaseBatch__drop(batch); return err; }
    jazz_node_GlobalTimeTxVec staged_global_times = {0};
    err = jazz_node__node__ingest__NodeState__finalize_staged_transaction_ingest(s, batch, tx_id, &staged.fate, staged.has_global_time,
                                                                                staged.global_time, &staged_global_times, &staged.versions);
    jz_vec_drop(&staged_global_times, sizeof(jazz_node_GlobalTimeTx), NULL);
    jzn_staged_drop(&staged);
    if (JZ_IS_ERR(err)) { groove__db__DatabaseBatch__drop(batch); return err; }
    groove__db__DatabaseBatch__deliver_notifications(batch, GROOVE_NOTIFY_AFTER_PERSISTENCE);
    groove_AppliedBatch *applied = NULL;
    JZ_TRY(jazz_node__node__state__commit__NodeState__apply_node_batch(s, batch, &applied));
    groove_PersistProgress *persisted = groove__db__Database__persist_with_progress(jzn_db(s), applied);
    err = groove__db__Database__finish_persistence(jzn_db(s), persisted);
    groove_AppliedBatch_drop(applied);
    JZ_TRY(err);
    if (staged.rejected) jz_btree_insert(&s->rejections.rejected_transactions, &tx_id, &staged.rejected, NULL);
    /* Pending fragments may cache only this call's versions. Never let
     * subsequent constraints or callers mistake that subset for the whole
     * stored transaction. */
    jazz_node__node__state__commit__NodeState__invalidate_tx_version_tables_cache(s, tx_id);
    return JZ_OK;
}

/* (String, BranchKey, RowUuid) -> VersionRow */
static void jzn_vrow_drop_v(void *v) { jazz_node_VersionRow_drop(v); }
static const jz_btree_desc jzn_desc_pending_global_updates = { sizeof(jazz_node_ViewVersionKey), sizeof(jazz_node_VersionRow),
    jazz_node_ViewVersionKey_cmp, jazz_node_ViewVersionKey_drop, jzn_vrow_drop_v };
/* ViewVersionKey -> index */
static const jz_btree_desc jzn_desc_incoming = { sizeof(jazz_node_ViewVersionKey), sizeof(u32), jazz_node_ViewVersionKey_cmp,
    jazz_node_ViewVersionKey_drop, NULL };
static const jz_btree_desc jzn_desc_schema_set = { sizeof(jazz_types_SchemaVersionId), 0, jz_cmp_uuid, NULL, NULL };

static void jzn_row_key(const jazz_node_VersionRow *stored, jazz_node_ViewVersionKey *key) {
    key->table = jz_str_clone(jazz_node__node__codec__VersionRow__table(stored));
    jazz_model_BranchKey_clone(&key->branch_key, jazz_node__node__codec__VersionRow__branch_key(stored));
    key->row_uuid = jazz_node__node__codec__VersionRow__row_uuid(stored);
}

jz_err jazz_node__node__ingest__NodeState__stage_transaction_and_versions_with_current_indexes(NSA *s, groove_DatabaseBatch *batch,
    jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt,
    u32 durability, bool update_current_indexes, bool is_partial, jazz_node_VersionRowVec *staged_content_versions,
    jazz_node_StagedTransaction *out) {
    jz_err err = JZ_OK;
    bool fate_owned = true;
    bool stored_found = false;
    jazz_node_StoredTransaction stored_tx;
    jz_btree pending_global_updates, incoming;
    jz_btree_init(&pending_global_updates, &jzn_desc_pending_global_updates);
    jz_btree_init(&incoming, &jzn_desc_incoming);
    jazz_node_VersionRowVec content_versions = {0};
    jazz_node_VersionRowVec stored_versions = {0};
    jazz_node_VersionRowVec existing_rows = {0};
    jzn_LargeValueRefVec large_value_descriptors = {0};
    jzn_StagedIdVec staged_ids = {0};
    u32 next_version = 0;

    /* Provenance operation identities participate in merge deduplication.
     * Admit them before accepting staged values or writing any derived
     * transaction/current state, on every local, remote, and view ingress. */
    groove_Value contribution_merge;
    if (JZ_IS_ERR(err = jazz_node__node__state__contribution_merge__NodeState__admit_contribution_merge_for_storage(s, &tx, &contribution_merge)))
        goto out;
    jazz_node__node__version_indirect_descriptors(&versions, &large_value_descriptors);
    if (JZ_IS_ERR(err = jazz_node__node__state__lifecycle__NodeState__current_staged_ids_for_descriptors_vec(s, &large_value_descriptors, false,
                      &staged_ids)))
        goto out;
    for (u32 i = 0; i < staged_ids.len; i++) groove__db__DatabaseBatch__accept_large_value(batch, staged_ids.ptr[i]);
    jazz_node__node__state__read_payload__NodeState__merge_tx_time(s, tx.tx_id.time);
    jazz_types_NodeAlias tx_node_alias;
    if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_node_alias(s, tx.tx_id.node, &tx_node_alias))) goto out;
    {
        jz_btree schemas;
        jz_btree_init(&schemas, &jzn_desc_schema_set);
        for (u32 i = 0; i < versions.len; i++) {
            jazz_types_SchemaVersionId v = jazz_protocol__protocol__VersionRecord__schema_version(versions.ptr[i]);
            jz_btree_insert(&schemas, &v, NULL, NULL);
        }
        jazz_types_SchemaVersionId *schema;
        void *unit;
        JZ_BTREE_FOR(&schemas, schema, unit) {
            (void)unit;
            jazz_types_SchemaVersionAlias alias;
            if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(s, *schema, &alias))) break;
        }
        jz_btree_drop(&schemas);
        if (JZ_IS_ERR(err)) goto out;
    }
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, tx.tx_id, &stored_found, &stored_tx))) goto out;
    if (stored_found) {
        jazz_node_StoredTransaction stored;
        jazz_node__node__codec__StoredTransaction__clone(&stored, &stored_tx);
        fate_owned = false;
        err = jazz_node__node__codec__StoredTransaction__reconcile_fate(&stored, fate, has_gt, gt, true, durability);
        if (JZ_IS_ERR(err)) { jazz_node__node__codec__StoredTransaction__drop(&stored); goto out; }
        fate = stored.fate;
        fate_owned = true;
        has_gt = stored.has_global_time;
        gt = stored.global_time;
        durability = stored.durability;
        jazz_model_Transaction_drop(&stored.tx);
    }
    /* A rejected transaction has no live history left to extend. Keep its
     * retained retry payload intact while reconciling the header normally. */
    if (stored_found && stored_tx.fate.tag == JAZZ_FATE_REJECTED) {
        jazz_protocol_VersionRecordVec_drop(&versions);
        versions = (jazz_protocol_VersionRecordVec){0};
    }
    /* Fate belongs to the whole transaction. A new fragment may settle
     * versions received earlier, including history-only exclusive fragments.
     * Assemble them before publishing either fate or derived current state.
     * Already-settled view fragments only add their incoming versions.
     * A terminal transition or a complete payload must cover earlier ones. */
    bool settle_existing = stored_found &&
        (fate.tag == JAZZ_FATE_REJECTED ||
         (has_gt && (!stored_tx.has_global_time || stored_tx.global_time != gt || !is_partial)));
    if (settle_existing) {
        for (u32 i = 0; i < versions.len; i++) {
            jazz_node_ViewVersionKey key;
            jazz_node__node__ingest__view_version_key_for_ingest(versions.ptr[i], &key);
            if (!jz_btree_insert(&incoming, &key, &i, NULL)) continue;
            jazz_node_ViewVersionKey_drop(&key); /* replaced: map keeps the first key, the latest index */
        }
        if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_versions_for_tx(s, tx.tx_id, &existing_rows))) goto out;
        for (u32 i = 0; i < existing_rows.len; i++) {
            const jazz_node_VersionRow *stored = &existing_rows.ptr[i];
            jzn_AheadOverlayKey okey;
            if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__ahead_overlay_key(s, stored, &okey))) goto out;
            update_current_indexes |= jazz_node__node__AheadCurrentKeys__contains_key(&s->ahead_current_keys, &okey);
            jz_bytes_drop(&okey.key);
            jazz_protocol_VersionRecord *version;
            if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__version_record_from_row(s, stored, &version))) goto out;
            jazz_node_ViewVersionKey vkey;
            jazz_node__node__ingest__view_version_key_for_ingest(version, &vkey);
            const u32 *index = jz_btree_get(&incoming, &vkey);
            jazz_node_ViewVersionKey_drop(&vkey);
            if (index && fate.tag == JAZZ_FATE_REJECTED && !jazz_protocol_VersionRecord_eq(versions.ptr[*index], version)) {
                jazz_protocol_VersionRecord_drop(version);
                err = jzn_val_conflicting_commit_unit(tx.tx_id);
                goto out;
            }
            if (index) jazz_protocol_VersionRecord_drop(version);
            else JZ_VEC_PUSH(versions, version);
        }
    }
    bool tx_already_known = stored_found;
    bool preserve_authoritative_cardinality = is_partial && stored_found && !stored_tx.is_partial;
    const jazz_model_Transaction *storage_tx = preserve_authoritative_cardinality ? &stored_tx.tx : &tx;
    if (storage_tx != &tx) {
        groove_Value_drop(&contribution_merge);
        if (JZ_IS_ERR(err = jazz_node__node__state__contribution_merge__NodeState__admit_contribution_merge_for_storage(s, storage_tx,
                          &contribution_merge)))
            goto out;
    }
    {
        groove_ValueVec tx_values = {0};
        if (JZ_IS_ERR(err = jazz_node__node__codec__transaction_values_with_partial_flag(tx_node_alias, storage_tx, &fate, has_gt, gt, durability,
                          is_partial && !preserve_authoritative_cardinality, contribution_merge, &tx_values)))
            goto out;
        if (tx_already_known) groove__db__DatabaseBatch__update(batch, "jazz_transactions", tx_values);
        else groove__db__DatabaseBatch__insert(batch, "jazz_transactions", tx_values);
    }
    if (!jz_uuid_eq(&tx.tx_id.node, &s->node_uuid)) {
        if (fate.tag == JAZZ_FATE_PENDING && !has_gt) jz_btree_insert(&s->open_tx.pending_foreign_transactions, &tx.tx_id, NULL, NULL);
        else jz_btree_remove(&s->open_tx.pending_foreign_transactions, &tx.tx_id, NULL, NULL);
    }

    for (; next_version < versions.len; next_version++) {
        const jazz_protocol_VersionRecord *version = versions.ptr[next_version];
        jazz_types_SchemaVersionId author_schema = jazz_protocol__protocol__VersionRecord__schema_version(version);
        const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
        const jazz_model_TableSchema *table_schema;
        /* Fail with TableNotFound first, matching the error ordering in commit_bundles. */
        if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, vtable, author_schema, &table_schema))) goto out;
        jazz_types_SchemaVersionAlias schema_version_alias;
        if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(s, author_schema, &schema_version_alias)))
            goto out;
        bool has_authored = false;
        jz_btree authored_column_ids;
        if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__authored_column_ids_for_names(s, author_schema, vtable,
                          jazz_protocol__protocol__VersionRecord__authored_columns(version), &has_authored, &authored_column_ids)))
            goto out;
        /* Rust clones the table schema (table_in_schema); the borrowed form is equivalent here. */
        err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, vtable, author_schema, &table_schema);
        /* An accepted write is stored at its seq, a pending one at 0 (SPEC 2 §2.7.1). */
        jazz_types_GlobalTime seq = (fate.tag == JAZZ_FATE_ACCEPTED && has_gt) ? gt : 0;
        jazz_node_VersionRow stored;
        if (!JZ_IS_ERR(err))
            err = jazz_node__node__codec__VersionRow__from_wire_with_schema_version(table_schema, version, has_authored ? &authored_column_ids : NULL,
                                                                                   tx_node_alias, schema_version_alias, tx.tx_id.time, seq, &stored);
        if (has_authored) jz_btree_drop(&authored_column_ids);
        if (JZ_IS_ERR(err)) goto out;
        if (update_current_indexes && fate.tag == JAZZ_FATE_ACCEPTED) {
            if (has_gt && s->minting_global_time) {
                bool some = false;
                jazz_node_VersionRow merged;
                if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__merged_global_post_image(s, batch, author_schema, table_schema, &stored,
                                  tx.tx_id, gt, true, &some, &merged))) {
                    jazz_node_VersionRow_drop(&stored);
                    goto out;
                }
                if (some) {
                    /* History holds the post-image at this seq; peers
                     * receive it for this transaction. */
                    jazz_node_ViewVersionKey key;
                    jzn_row_key(&stored, &key);
                    jazz_node_VersionRow_drop(&stored);
                    jazz_node_VersionRow_clone(&stored, &merged);
                    jazz_node_VersionRow old;
                    if (jz_btree_insert(&pending_global_updates, &key, &merged, &old)) { jazz_node_ViewVersionKey_drop(&key); jazz_node_VersionRow_drop(&old); }
                }
            } else if (has_gt) {
                /* Accepted rows from upstream are post-images at their
                 * seq: a newer seq replaces the row whole. So does the
                 * authority's image at the seq this node already holds:
                 * an originator merges its own write when its fate
                 * arrives, possibly over a base that misses earlier seqs
                 * it has not received yet, and that local merge is only
                 * a prediction of Core's post-image. */
                bool has_current = false;
                jazz_types_GlobalTime current_seq = 0;
                if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__global_current_seq_in_batch(s, batch, author_schema, stored.table,
                                  &stored.branch_key, jazz_node__node__codec__VersionRow__row_uuid(&stored), &has_current, &current_seq))) {
                    jazz_node_VersionRow_drop(&stored);
                    goto out;
                }
                if (!has_current || current_seq <= gt) {
                    jazz_node_ViewVersionKey key;
                    jzn_row_key(&stored, &key);
                    jazz_node_VersionRow copy, old;
                    jazz_node_VersionRow_clone(&copy, &stored);
                    if (jz_btree_insert(&pending_global_updates, &key, &copy, &old)) { jazz_node_ViewVersionKey_drop(&key); jazz_node_VersionRow_drop(&old); }
                }
            }
        }
        if (fate.tag != JAZZ_FATE_REJECTED) {
            jazz_node_VersionRow copy;
            jazz_node_VersionRow_clone(&copy, &stored);
            JZ_VEC_PUSH(content_versions, copy);
        }
        /* Rejected payloads go directly to rejection cleanup. Inserting
         * immutable history and deleting it in the same batch would violate
         * ensure_exact's promise that admitted immutable bytes survive. */
        if (fate.tag != JAZZ_FATE_REJECTED) {
            groove_InternStr history_table;
            groove_ValidatedVariantRecord groove_record;
            jazz_node_PrimaryKeyValue storage_key;
            if (JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_write_binding(s, &stored, tx.made_by, &history_table,
                              &groove_record)) ||
                JZ_IS_ERR(err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, &stored, &storage_key))) {
                jazz_node_VersionRow_drop(&stored);
                goto out;
            }
            jazz_types_GlobalTime stored_seq;
            if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__seq(&stored, &stored_seq))) { jazz_node_VersionRow_drop(&stored); goto out; }
            if (tx_already_known && stored_seq != 0) {
                /* The write is accepted now: its pending record moves to its seq. */
                jazz_node_VersionRow at_zero;
                jazz_node_PrimaryKeyValue pending_key;
                if (JZ_IS_ERR(err = jazz_node__node__codec__VersionRow__with_seq(&stored, 0, &at_zero))) { jazz_node_VersionRow_drop(&stored); goto out; }
                err = jazz_node__node__physical__NodeState__version_storage_primary_key(s, &at_zero, &pending_key);
                jazz_node_VersionRow_drop(&at_zero);
                if (JZ_IS_ERR(err)) { jazz_node_VersionRow_drop(&stored); goto out; }
                groove__db__DatabaseBatch__delete(batch, history_table, pending_key);
            }
            if (has_gt && fate.tag == JAZZ_FATE_ACCEPTED && !s->minting_global_time) {
                /* The authority's post-image replaces this node's own copy. */
                groove__db__DatabaseBatch__update_raw(batch, history_table, storage_key, groove_record);
            } else {
                u32 outcome;
                if (JZ_IS_ERR(err = groove__db__DatabaseBatch__ensure_exact(batch, jzn_db(s), history_table, storage_key, groove_record, &outcome))) {
                    jazz_node_VersionRow_drop(&stored);
                    goto out;
                }
                if (outcome == GROOVE_ENSURE_CONFLICT) {
                    jazz_node_VersionRow_drop(&stored);
                    err = jzn_val_conflicting_commit_unit(tx.tx_id);
                    goto out;
                }
            }
        }
        if (update_current_indexes && fate.tag != JAZZ_FATE_REJECTED && !has_gt) {
            if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__write_ahead_current_insert(s, batch, &stored))) {
                jazz_node_VersionRow_drop(&stored);
                goto out;
            }
        }
        JZ_VEC_PUSH(stored_versions, stored);
    }
    if (update_current_indexes && fate.tag != JAZZ_FATE_REJECTED && staged_content_versions) {
        for (u32 i = 0; i < content_versions.len; i++) {
            jazz_node_VersionRow copy;
            jazz_node_VersionRow_clone(&copy, &content_versions.ptr[i]);
            JZ_VEC_PUSH(*staged_content_versions, copy);
        }
    }
    if (update_current_indexes && fate.tag == JAZZ_FATE_ACCEPTED && has_gt) {
        jazz_node_VersionRowVec synced = {0};
        jazz_node_ViewVersionKey *k;
        jazz_node_VersionRow *row;
        JZ_BTREE_FOR(&pending_global_updates, k, row) {
            (void)k;
            if (JZ_IS_ERR(err = jazz_node__node__ingest__NodeState__write_global_current_update(s, batch, row, gt))) break;
            jazz_node_VersionRow copy;
            jazz_node_VersionRow_clone(&copy, row);
            JZ_VEC_PUSH(synced, copy);
        }
        if (!JZ_IS_ERR(err)) {
            jz_btree settling;
            jz_btree_init(&settling, &jzn_desc_txid_set_a);
            jz_btree_insert(&settling, &tx.tx_id, NULL, NULL);
            err = jazz_node__node__ingest__NodeState__rebase_ahead_overlays(s, batch, &synced, &settling);
            jz_btree_drop(&settling);
        }
        jzn_val_rows_drop(&synced);
        if (JZ_IS_ERR(err)) goto out;
    }
    {
        jazz_node_VersionRowVec cached = {0};
        jzn_val_rows_clone(&cached, &stored_versions);
        jazz_node__node__state__commit__NodeState__cache_tx_versions(s, tx.tx_id, cached);
    }
    jazz_node_RejectedTransaction *rejected_payload = NULL;
    if (fate.tag == JAZZ_FATE_REJECTED) {
        jazz_node_StoredTransaction rejected_tx;
        jazz_model_Transaction_clone(&rejected_tx.tx, storage_tx);
        rejected_tx.node_alias = tx_node_alias;
        jazz_model_Fate_clone(&rejected_tx.fate, &fate);
        rejected_tx.has_global_time = has_gt;
        rejected_tx.global_time = gt;
        rejected_tx.durability = durability;
        rejected_tx.is_partial = is_partial && !preserve_authoritative_cardinality;
        err = jazz_node__node__ingest__NodeState__remove_rejected_local_versions(s, tx.tx_id, &rejected_tx, batch, &rejected_payload);
        jazz_node__node__codec__StoredTransaction__drop(&rejected_tx);
        if (JZ_IS_ERR(err)) goto out;
    }
    out->versions = stored_versions;
    stored_versions = (jazz_node_VersionRowVec){0};
    out->fate = fate;
    fate_owned = false;
    out->has_global_time = has_gt;
    out->global_time = gt;
    out->rejected = rejected_payload;
out:
    if (fate_owned) jazz_model_Fate_drop(&fate);
    if (stored_found) jazz_node__node__codec__StoredTransaction__drop(&stored_tx);
    jz_btree_drop(&pending_global_updates);
    jz_btree_drop(&incoming);
    jzn_val_rows_drop(&content_versions);
    jzn_val_rows_drop(&stored_versions);
    jzn_val_rows_drop(&existing_rows);
    jz_vec_drop(&large_value_descriptors, sizeof(groove_LargeValueRef *), NULL);
    jz_vec_drop(&staged_ids, sizeof(u64), NULL);
    jazz_protocol_VersionRecordVec_drop(&versions);
    jazz_model_Transaction_drop(&tx);
    return err;
}

jz_err jazz_node__node__ingest__NodeState__finalize_staged_transaction_ingest(NSA *s, groove_DatabaseBatch *batch, jazz_types_TxId tx_id,
                                                                              const jazz_model_Fate *fate, bool has_gt, jazz_types_GlobalTime gt,
                                                                              jazz_node_GlobalTimeTxVec *staged_global_times,
                                                                              const jazz_node_VersionRowVec *staged_versions) {
    if (fate->tag != JAZZ_FATE_ACCEPTED || !has_gt) return JZ_OK;
    jazz_node_GlobalTimeTx entry = { gt, tx_id };
    JZ_VEC_PUSH(*staged_global_times, entry);
    jzn_U64Vec advanced_global_times = {0};
    jazz_node__node__open_tx__NodeState__record_applied_global_time_collect(s, gt, tx_id, &advanced_global_times);
    jz_err err = jazz_node__node__ingest__NodeState__cleanup_fated_ahead_current_for_versions(s, batch, staged_versions);
    for (u32 i = 0; i < advanced_global_times.len && !JZ_IS_ERR(err); i++) {
        if (advanced_global_times.ptr[i] == gt) continue;
        err = jazz_node__node__ingest__NodeState__prune_ahead_current_for_global_time(s, batch, advanced_global_times.ptr[i]);
    }
    jz_vec_drop(&advanced_global_times, sizeof(u64), NULL);
    return err;
}

static const jazz_model_TableSchema *jzn_schema_table(const jazz_model_JazzSchema *schema, const jz_str *name) {
    for (u32 i = 0; i < schema->tables.len; i++)
        if (jz_str_eq(&schema->tables.ptr[i].name, name)) return &schema->tables.ptr[i];
    return NULL;
}

/* A wire row version is a complete row under the schema id it declares.
 * An unknown schema cannot be checked until its catalogue value arrives,
 * but a known schema must never accept a descriptor borrowed from another
 * version: that would make the omitted trailing columns indistinguishable
 * from an authored value and reintroduce partial-row sync semantics. */
bool jazz_node__node__ingest__NodeState__malformed_authored_version_reason(const NSA *s, const jazz_protocol_VersionRecord *const *versions,
                                                                           u32 n, jz_str *reason) {
    for (u32 i = 0; i < n; i++) {
        const jazz_protocol_VersionRecord *version = versions[i];
        const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
        const char *fields[2] = { "created_at_ms", "updated_at_ms" };
        u64 values[2] = { jazz_protocol__protocol__VersionRecord__created_at_ms(version), jazz_protocol__protocol__VersionRecord__updated_at_ms(version) };
        for (u32 f = 0; f < 2; f++) {
            jazz_types_TxTime t;
            jz_err e = jazz_types__time__TxTime__from_physical_ms(values[f], &t);
            if (JZ_IS_ERR(e)) {
                jz_err_drop(&e);
                *reason = jz_format("row version for table '%.*s' has %s outside the packed HLC physical-millisecond range",
                                    (int)vtable->len, vtable->ptr, fields[f]);
                return true;
            }
        }
        jz_err e = jazz_node__node__codec__validate_canonical_version_parts(jazz_protocol__protocol__VersionRecord__branch_key(version));
        if (JZ_IS_ERR(e)) {
            if (e.code == JZN_E_INVALID_MERGEABLE_COMMIT) {
                *reason = e.msg ? jz_str_clone(e.msg) : (jz_str){0};
                jz_err_drop(&e);
                return true;
            }
            jz_err_drop(&e);
        }
        jazz_types_SchemaVersionId sv = jazz_protocol__protocol__VersionRecord__schema_version(version);
        const jazz_model_SchemaVersion *schema = jz_btree_get(&s->catalogue.catalogue_schemas, &sv);
        if (!schema) continue;
        const jazz_model_TableSchema *table = jzn_schema_table(&schema->schema, vtable);
        if (!table) {
            *reason = jz_format("row version table '%.*s' is absent from its authored schema", (int)vtable->len, vtable->ptr);
            return true;
        }
        if (!groove_RecordDescriptor_eq(jazz_protocol__protocol__VersionRecord__record(version)->descriptor,
                                        jazz_node__node__codec__prepared_wire_record_descriptor(table))) {
            *reason = jz_format("row version for table '%.*s' does not carry the complete descriptor of its authored schema",
                                (int)vtable->len, vtable->ptr);
            return true;
        }
        if (jazz_node__node__ingest__NodeState__malformed_authored_branch_key_reason(&schema->schema, table, version, reason)) return true;
    }
    return false;
}

bool jazz_node__node__ingest__NodeState__malformed_authored_branch_key_reason(const jazz_model_JazzSchema *schema, const jazz_model_TableSchema *table,
                                                                              const jazz_protocol_VersionRecord *version, jz_str *reason) {
    const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
    jazz_model_BranchCellVec branch_cells = {0};
    jz_str invalid = {0};
    jz_err e = jazz_model__schema__JazzSchema__validate_authored_branch_key(schema, table, jazz_protocol__protocol__VersionRecord__branch_key(version),
                                                                          &branch_cells, &invalid);
    if (JZ_IS_ERR(e)) {
        jz_err_drop(&e);
        *reason = jz_format("row version for table '%.*s' has an invalid branch key: %.*s", (int)vtable->len, vtable->ptr, (int)invalid.len,
                            invalid.ptr);
        jz_str_drop(&invalid);
        return true;
    }
    bool malformed = false;
    for (u32 i = 0; i < branch_cells.len && !malformed; i++) {
        const jazz_model_BranchCell *cell = &branch_cells.ptr[i];
        u32 position = UINT32_MAX;
        for (u32 c = 0; c < table->columns.len; c++)
            if (jz_str_eq(&table->columns.ptr[c].name, &cell->column)) { position = c; break; }
        if (position == UINT32_MAX) {
            *reason = jz_format("row version for table '%.*s' binds a missing branch column '%.*s'", (int)vtable->len, vtable->ptr,
                                (int)cell->column.len, cell->column.ptr);
            malformed = true;
            break;
        }
        groove_Value at;
        bool some = jazz_protocol__protocol__VersionRecord__cell_at(version, position, &at);
        bool agrees = some && groove_Value_eq(&at, &cell->value);
        if (some) groove_Value_drop(&at);
        if (!agrees) {
            *reason = jz_format("row version for table '%.*s' has branch key content that disagrees with column '%.*s'", (int)vtable->len,
                                vtable->ptr, (int)cell->column.len, cell->column.ptr);
            malformed = true;
        }
    }
    for (u32 i = 0; i < branch_cells.len; i++) {
        jz_str_drop(&branch_cells.ptr[i].column);
        groove_Value_drop(&branch_cells.ptr[i].value);
    }
    jz_vec_drop(&branch_cells, sizeof(jazz_model_BranchCell), NULL);
    return malformed;
}

/* Validate row versions carried by a view or repair payload before that
 * payload may advance local receiver state. View payloads cannot park for
 * a missing catalogue entry: unlike an authored commit unit, they have no
 * protocol disposition that can defer a partial application of the frame. */
jz_err jazz_node__node__ingest__NodeState__validate_view_payload_versions(const NSA *s, const jazz_protocol_VersionRecordVec *versions) {
    jz_btree descriptors;
    jz_btree_init(&descriptors, &jzn_desc_schema_table_descriptor);
    jz_err err = jazz_node__node__ingest__NodeState__validate_view_payload_versions_prepared(s, versions, &descriptors);
    jz_btree_drop(&descriptors);
    return err;
}

static int jzn_schema_table_key_cmp(const void *a, const void *b, void *ctx) {
    const jzn_SchemaTableKey *x = a, *y = b;
    int c = jz_cmp_uuid(&x->schema, &y->schema, ctx);
    return c ? c : jz_str_cmp(x->table, y->table);
}
const jz_btree_desc jzn_desc_schema_table_descriptor = { sizeof(jzn_SchemaTableKey), sizeof(groove_RecordDescriptor), jzn_schema_table_key_cmp,
    NULL, NULL };

jz_err jazz_node__node__ingest__NodeState__validate_view_payload_versions_prepared(const NSA *s, const jazz_protocol_VersionRecordVec *versions,
                                                                                   jz_btree *descriptors) {
    /* View/repair bodies come from an admitted authority encoder. Check
     * semantic catalogue compatibility below, not their byte representation.
     * Catalogue state cannot change during this shared-borrow preflight. */
    for (u32 i = 0; i < versions->len; i++) {
        const jazz_protocol_VersionRecord *version = versions->ptr[i];
        jazz_types_SchemaVersionId sv = jazz_protocol__protocol__VersionRecord__schema_version(version);
        const jz_str *vtable = jazz_protocol__protocol__VersionRecord__table(version);
        const jazz_model_SchemaVersion *schema = jz_btree_get(&s->catalogue.catalogue_schemas, &sv);
        if (!schema) return jz_err_new(JZN_E_MALFORMED_VIEW_UPDATE, "row version names an unknown authored schema");
        const jazz_model_TableSchema *table = jzn_schema_table(&schema->schema, vtable);
        if (!table) return jz_err_new(JZN_E_MALFORMED_VIEW_UPDATE, "row version table is absent from its authored schema");
        jzn_SchemaTableKey key = { sv, vtable };
        bool inserted = false;
        groove_RecordDescriptor *descriptor = jz_btree_entry(descriptors, &key, &inserted);
        if (inserted) *descriptor = jazz_model__schema__TableSchema__wire_record_descriptor(table);
        if (!groove_RecordDescriptor_eq(jazz_protocol__protocol__VersionRecord__record(version)->descriptor, *descriptor))
            return jz_err_new(JZN_E_MALFORMED_VIEW_UPDATE, "row version does not carry the complete descriptor of its authored schema");
        jazz_types_TxTime t;
        jz_err e1 = jazz_types__time__TxTime__from_physical_ms(jazz_protocol__protocol__VersionRecord__created_at_ms(version), &t);
        jz_err e2 = jazz_types__time__TxTime__from_physical_ms(jazz_protocol__protocol__VersionRecord__updated_at_ms(version), &t);
        bool out_of_range = JZ_IS_ERR(e1) || JZ_IS_ERR(e2);
        jz_err_drop(&e1);
        jz_err_drop(&e2);
        if (out_of_range)
            return jz_err_new(JZN_E_MALFORMED_VIEW_UPDATE, "row version provenance exceeds packed HLC physical-millisecond range");
        jz_str reason = {0};
        if (jazz_node__node__ingest__NodeState__malformed_authored_branch_key_reason(&schema->schema, table, version, &reason)) {
            jz_str_drop(&reason);
            return jz_err_new(JZN_E_MALFORMED_VIEW_UPDATE, "row version does not carry a valid authored branch key");
        }
    }
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__reject_malformed_commit(NSA *s, jazz_model_Transaction tx, jz_str reason, jazz_protocol_SyncMessageVec *out) {
    jazz_model_Fate fate = jazz_node__node__ingest__malformed_commit_fate(reason);
    /* Malformed provenance cannot be encoded safely. Retain the terminal
     * outcome, but never make an invalid payload durable merely to report
     * it; recovery continues to reject any independently corrupted stored
     * record at decode time. */
    jazz_types_TxId tx_id = tx.tx_id;
    jazz_model_Transaction storage_tx;
    jazz_model_Transaction_clone(&storage_tx, &tx);
    if (storage_tx.contribution_merge) {
        jazz_model_ContributionMergeProvenance_drop(storage_tx.contribution_merge);
        storage_tx.contribution_merge = NULL;
    }
    jazz_model_Transaction_drop(&tx);
    jazz_model_Fate copy;
    jazz_model_Fate_clone(&copy, &fate);
    jz_err err = jazz_node__node__ingest__NodeState__ingest_rejected_transaction(s, storage_tx, copy);
    if (JZ_IS_ERR(err)) { jazz_model_Fate_drop(&fate); return err; }
    jazz_protocol_SyncMessage m;
    jazz_node__node__ingest__fate_update_message(tx_id, fate, false, 0, false, 0, &m);
    *out = (jazz_protocol_SyncMessageVec){0};
    JZ_VEC_PUSH(*out, m);
    return JZ_OK;
}

/* (String, SchemaVersionId) */
typedef struct jzn_AuthoredVariant { jz_str table; jazz_types_SchemaVersionId schema; } jzn_AuthoredVariant;
static int jzn_authored_variant_cmp(const void *a, const void *b, void *ctx) {
    const jzn_AuthoredVariant *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    return c ? c : jz_cmp_uuid(&x->schema, &y->schema, ctx);
}
static void jzn_authored_variant_drop(void *p) { jz_str_drop(&((jzn_AuthoredVariant *)p)->table); }
static const jz_btree_desc jzn_desc_authored_variants = { sizeof(jzn_AuthoredVariant), 0, jzn_authored_variant_cmp, jzn_authored_variant_drop, NULL };

/* Ensure every known authored schema named by an arriving commit has a
 * local alias and registered shared-storage variant. Unknown schemas stay
 * parked until their catalogue lineage arrives and re-enters this path. */
jz_err jazz_node__node__ingest__NodeState__prepare_authored_schema_variants_for_commit(NSA *s, const jazz_protocol_VersionRecord *const *versions,
                                                                                       u32 n) {
    jz_str reason = {0};
    if (jazz_node__node__ingest__NodeState__malformed_authored_version_reason(s, versions, n, &reason)) {
        jz_str_drop(&reason);
        return JZN_ISV("wire version record does not match authored schema");
    }
    for (u32 i = 0; i < n; i++) {
        jazz_types_SchemaVersionId sv = jazz_protocol__protocol__VersionRecord__schema_version(versions[i]);
        if (!jz_btree_contains(&s->catalogue.catalogue_schemas, &sv)) return JZ_OK;
    }
    jz_btree authored_variants;
    jz_btree_init(&authored_variants, &jzn_desc_authored_variants);
    for (u32 i = 0; i < n; i++) {
        jzn_AuthoredVariant v = { jz_str_clone(jazz_protocol__protocol__VersionRecord__table(versions[i])),
                                  jazz_protocol__protocol__VersionRecord__schema_version(versions[i]) };
        if (jz_btree_contains(&authored_variants, &v)) jzn_authored_variant_drop(&v);
        else jz_btree_insert(&authored_variants, &v, NULL, NULL);
    }
    bool registered_mapping = false;
    jz_err err = JZ_OK;
    jzn_AuthoredVariant *v;
    void *unit;
    JZ_BTREE_FOR(&authored_variants, v, unit) {
        (void)unit;
        const jazz_model_TableSchema *t;
        if (JZ_IS_ERR(err = jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(s, &v->table, v->schema, &t))) break;
        registered_mapping |= !jz_btree_contains(&s->catalogue.schema_version_aliases, &v->schema) ||
                              !jz_btree_contains(&s->catalogue.physical_mappings, &v->schema);
        jazz_types_SchemaVersionAlias alias;
        if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(s, v->schema, &alias))) break;
    }
    jz_btree_drop(&authored_variants);
    JZ_TRY(err);
    if (registered_mapping) JZ_TRY(jazz_node__node__physical__NodeState__synchronize_physical_version_tables(s));
    return JZ_OK;
}

jz_err jazz_node__node__ingest__NodeState__ingest_rejected_transaction(NSA *s, jazz_model_Transaction tx, jazz_model_Fate fate) {
    groove_Value contribution_merge;
    jz_err err = jazz_node__node__state__contribution_merge__NodeState__admit_contribution_merge_for_storage(s, &tx, &contribution_merge);
    if (JZ_IS_ERR(err)) goto fail;
    bool found = false;
    jazz_node_StoredTransaction existing;
    if (JZ_IS_ERR(err = jazz_node__node__currency__NodeState__query_transaction(s, tx.tx_id, &found, &existing))) { groove_Value_drop(&contribution_merge); goto fail; }
    if (found) {
        jazz_node__node__codec__StoredTransaction__drop(&existing);
        groove_Value_drop(&contribution_merge);
        jazz_types_TxId tx_id = tx.tx_id;
        jazz_model_Transaction_drop(&tx);
        return jazz_node__node__ingest__NodeState__apply_fate_update(s, tx_id, fate, false, 0, false, 0);
    }
    jazz_types_NodeAlias tx_node_alias;
    if (JZ_IS_ERR(err = jazz_node__node__state__catalogue__NodeState__ensure_node_alias(s, tx.tx_id.node, &tx_node_alias))) {
        groove_Value_drop(&contribution_merge);
        goto fail;
    }
    groove_DatabaseBatch *batch = groove__db__Database__open_batch(jzn_db(s));
    groove_ValueVec values = {0};
    if (JZ_IS_ERR(err = jazz_node__node__codec__transaction_values(tx_node_alias, &tx, &fate, false, 0, JAZZ_DURABILITY_LOCAL, contribution_merge,
                      &values))) {
        groove__db__DatabaseBatch__drop(batch);
        goto fail;
    }
    groove__db__DatabaseBatch__insert(batch, "jazz_transactions", values);
    groove_AppliedBatch *applied = NULL;
    if (JZ_IS_ERR(err = jazz_node__node__state__commit__NodeState__apply_node_batch(s, batch, &applied))) goto fail;
    groove_PersistProgress *persisted = groove__db__Database__persist_with_progress(jzn_db(s), applied);
    err = groove__db__Database__finish_persistence(jzn_db(s), persisted);
    groove_AppliedBatch_drop(applied);
fail:
    jazz_model_Transaction_drop(&tx);
    jazz_model_Fate_drop(&fate);
    return err;
}
