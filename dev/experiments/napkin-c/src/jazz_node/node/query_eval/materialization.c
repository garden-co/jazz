/* Port of crates/jazz/layers/node/src/node/query_eval/materialization.rs
 * Query result and relation snapshot materialization. */
#include "jazz_node/node/node_qe_b.h"

typedef jazz_node_LocalMaintainedViewSubscription Local;

/* LocalMaintainedMaterializationCache { tx_versions: BTreeMap<TxId, Vec<VersionRow>> } */
typedef struct LocalMaintainedMaterializationCache { jz_btree tx_versions; } LocalMaintainedMaterializationCache;

static jz_err MAT(NodeState__project_relation_edge_table_through_read_schema)(jazz_node_NodeState *self, const jz_str *canonical_table, jazz_types_RowUuid row_uuid, const jazz_protocol_RowVersionRefEntry *version_ref, jazz_types_SchemaVersionId read_schema, jz_str *out);

jz_err MAT(NodeState__project_relation_edge_through_read_schema)(jazz_node_NodeState *self, const jazz_protocol_RelationEdgeEntry *edge, jazz_types_SchemaVersionId read_schema, jazz_node_RelationEdge *out)
{
    jz_str source_table, target_table;
    JZ_TRY(MAT(NodeState__project_relation_edge_table_through_read_schema)(self, edge->source_table, edge->source_row,
        edge->has_source_version ? &edge->source_version : NULL, read_schema, &source_table));
    jz_err e = MAT(NodeState__project_relation_edge_table_through_read_schema)(self, edge->target_table, edge->target_row,
        edge->has_target_version ? &edge->target_version : NULL, read_schema, &target_table);
    if (JZ_IS_ERR(e)) { jz_str_drop(&source_table); return e; }
    out->source_table = source_table;
    out->source_row = edge->source_row;
    out->relation = jz_str_clone(&edge->path);
    out->target_table = target_table;
    out->target_row = edge->target_row;
    return JZ_OK;
}

static jz_err MAT(NodeState__project_relation_edge_table_through_read_schema)(jazz_node_NodeState *self, const jz_str *canonical_table, jazz_types_RowUuid row_uuid, const jazz_protocol_RowVersionRefEntry *version_ref, jazz_types_SchemaVersionId read_schema, jz_str *out)
{
    if (!version_ref) {
        /* A fact without a concrete version is already required to name the
         * read view. Never relabel it optimistically. */
        const jazz_model_TableSchema *t;
        JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, canonical_table, read_schema, &t));
        *out = jz_str_clone(canonical_table);
        return JZ_OK;
    }
    jazz_node_VersionRow version;
    JZ_TRY(MAT(NodeState__resolve_relation_edge_version)(self, canonical_table, row_uuid, version_ref, &version));
    jz_err e;
    jazz_types_SchemaVersionId authored_schema;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(&version), &authored_schema)) {
        e = JZN_INVALID_STORED("relation edge witness schema version alias must exist");
        goto out;
    }
    jz_btree cells;
    jz_btree_init(&cells, &jazz_node_qeb_STR_VALUE_DESC);
    bool has;
    e = jazz_node__node__lenses__NodeState__translate_cells(self, authored_schema, read_schema, jazz_node__node__codec__VersionRow__table(&version), &cells, &has, out);
    jz_btree_drop(&cells);
    if (!JZ_IS_ERR(e) && !has) e = JZN_INVALID_STORED("relation edge witness does not project into the read schema");
out:
    jazz_node_VersionRow_drop(&version);
    return e;
}

jz_err MAT(NodeState__resolve_relation_edge_version)(jazz_node_NodeState *self, const jz_str *canonical_table, jazz_types_RowUuid row_uuid, const jazz_protocol_RowVersionRefEntry *version_ref, jazz_node_VersionRow *out)
{
    jazz_types_NodeAlias tx_node_alias;
    if (!jazz_node__node__NodeAliases__get(self->node_aliases, &version_ref->tx.node, &tx_node_alias))
        return jazz_node__node__Error_missing_transaction(version_ref->tx);
    bool has;
    JZ_TRY(jazz_node__node__views__NodeState__query_version_by_alias(self, canonical_table, row_uuid, version_ref->tx.time, tx_node_alias, &has, out));
    if (!has) return jazz_node__node__Error_missing_transaction(version_ref->tx);
    return JZ_OK;
}

jz_err MAT(NodeState__resolve_relation_terminal_version)(jazz_node_NodeState *self, const jz_str *emitted_table, jazz_types_RowUuid row_uuid, const jazz_protocol_RowVersionRefEntry *version_ref, jazz_types_SchemaVersionId read_schema, jazz_node_VersionRow *out)
{
    jazz_node_VersionRowVec all;
    JZ_TRY(jazz_node__node__views__NodeState__query_versions_for_tx(self, version_ref->tx, &all));
    jazz_node_VersionRowVec matching = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < all.len; i++) {
        jazz_node_VersionRow *version = JZ_VEC_AT(all, i);
        jazz_types_RowUuid r = jazz_node__node__codec__VersionRow__row_uuid(version);
        if (!jz_uuid_eq(&r, &row_uuid)) continue;
        jazz_types_SchemaVersionId authored_schema;
        if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(version), &authored_schema)) {
            e = JZN_INVALID_STORED("relation edge witness schema version alias must exist");
            goto out;
        }
        jz_btree cells;
        jz_btree_init(&cells, &jazz_node_qeb_STR_VALUE_DESC);
        bool has;
        jz_str projected = {0};
        e = jazz_node__node__lenses__NodeState__translate_cells(self, authored_schema, read_schema, jazz_node__node__codec__VersionRow__table(version), &cells, &has, &projected);
        jz_btree_drop(&cells);
        if (JZ_IS_ERR(e)) goto out;
        if (has && jz_str_eq(&projected, emitted_table)) {
            jazz_node_VersionRow c;
            jazz_node_VersionRow_clone(&c, version);
            JZ_VEC_PUSH(matching, c);
        }
        if (has) jz_str_drop(&projected);
    }
    if (matching.len == 1) jazz_node_VersionRow_clone(out, JZ_VEC_AT(matching, 0));
    else if (matching.len == 0) e = JZN_INVALID_STORED("relation edge terminal witness does not project to its emitted table");
    else e = JZN_INVALID_STORED("relation edge terminal witness is ambiguous after projection");
out:
    jz_vec_drop(&matching, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
    jz_vec_drop(&all, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
    return e;
}

/* #[allow(dead_code)] relation_edge_branch_id */
__attribute__((unused))
static jz_err MAT(NodeState__relation_edge_branch_id)(const jazz_protocol_RowVersionRefEntry *version_ref, bool *has, jz_uuid *out)
{
    *has = false;
    if (!version_ref->has_branch_or_prefix) return JZ_OK;
    if (version_ref->branch_or_prefix.len != 16)
        return JZN_INVALID_STORED("relation edge branch discriminator must be a UUID");
    memcpy(out->b, version_ref->branch_or_prefix.ptr, 16); /* SchemaFamilyId::from_bytes */
    *has = true;
    return JZ_OK;
}

jz_err MAT(NodeState__materialize_historical_query_rows)(jazz_node_NodeState *self, jazz_model_TableSchema table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out)
{
    memset(out, 0, sizeof *out);
    jz_err e = JZ_OK;
    groove_RecordDeltasIter it = {deltas, 0};
    groove_BorrowedRecord record;
    i64 weight;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
        if (weight <= 0) continue;
        jazz_node_CurrentRow row, m;
        e = jazz_node__node__decode_current_row(&table, &record, &row);
        if (JZ_IS_ERR(e)) break;
        e = jazz_node__node__views__NodeState__materialize_current_row(self, &table, row, &m);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC_PUSH(*out, m);
    }
    if (JZ_IS_ERR(e)) jz_vec_drop(out, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
    jazz_model_TableSchema_drop(&table);
    return e;
}

jz_err MAT(NodeState__materialize_include_deleted_query_rows)(jazz_node_NodeState *self, jazz_model_TableSchema table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out)
{
    memset(out, 0, sizeof *out);
    u32 deleted_field_idx = jazz_node__node__current_row_fields_len(&table);
    jz_err e = JZ_OK;
    groove_RecordDeltasIter it = {deltas, 0};
    groove_BorrowedRecord record;
    i64 weight;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
        if (weight <= 0) continue;
        bool deleted;
        e = groove__records__record__BorrowedRecord__get_bool(&record, deleted_field_idx, &deleted);
        if (JZ_IS_ERR(e)) break;
        jazz_node_CurrentRow row, m;
        e = jazz_node__node__decode_current_row(&table, &record, &row);
        if (JZ_IS_ERR(e)) break;
        e = jazz_node__node__views__NodeState__materialize_current_row(self, &table, row, &m);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC_PUSH(*out, deleted ? jazz_node__node__CurrentRow__into_deleted(m) : m);
    }
    if (JZ_IS_ERR(e)) jz_vec_drop(out, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
    jazz_model_TableSchema_drop(&table);
    return e;
}

jz_err MAT(NodeState__materialize_inline_current_query_rows)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out)
{
    memset(out, 0, sizeof *out);
    jz_err e = JZ_OK;
    groove_RecordDeltasIter it = {deltas, 0};
    groove_BorrowedRecord record;
    i64 weight;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
        if (weight <= 0) continue;
        jazz_node_CurrentRow row, m;
        e = jazz_node__node__decode_current_row(table, &record, &row);
        if (JZ_IS_ERR(e)) break;
        e = jazz_node__node__views__NodeState__materialize_current_row(self, table, row, &m);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC_PUSH(*out, m);
    }
    if (JZ_IS_ERR(e)) jz_vec_drop(out, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
    return e;
}

jz_err MAT(NodeState__materialize_aggregate_query_rows)(jazz_node_NodeState *self, const jazz_model_Query *query, const jazz_engine_AppRowSchema *output, const groove_RecordDeltas *deltas, jazz_node_CurrentRowVec *out)
{
    (void)self;
    memset(out, 0, sizeof *out);
    jz_err e = JZ_OK;
    groove_RecordDeltasIter it = {deltas, 0};
    groove_BorrowedRecord record;
    i64 weight;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
        if (weight <= 0) continue;
        jazz_types_RowUuid uuid;
        e = jazz_node__node__query_eval__query_result_rows__aggregate_query_row_uuid(output, &record, &uuid);
        if (JZ_IS_ERR(e)) break;
        jazz_node_CurrentRow row;
        e = jazz_node__node__query_eval__aggregate_current_row_from_record(query, output, uuid, &record, &row);
        if (JZ_IS_ERR(e)) break;
        JZ_VEC_PUSH(*out, row);
    }
    if (JZ_IS_ERR(e)) jz_vec_drop(out, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
    return e;
}

static jazz_node_VersionRowVec *tx_versions_entry(jz_btree *tx_versions, const Local *local, jazz_model_TxId tx_id)
{
    bool inserted;
    jazz_node_VersionRowVec *v = jz_btree_entry(tx_versions, &tx_id, &inserted);
    if (inserted) *v = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__versions_by_tx(local->maintained, tx_id);
    return v;
}

static jz_err MAT(NodeState__preload_tx_versions_for_materialization)(jazz_node_NodeState *self, const jz_btree *tx_ids, jz_btree *cache);

static jz_err MAT(NodeState__preload_local_maintained_materialization_cache)(jazz_node_NodeState *self, const Local *local, LocalMaintainedMaterializationCache *cache)
{
    jz_btree_init(&cache->tx_versions, &jazz_node_TX_VERSIONS_DESC);
    /* Storage-backed root subscriptions carry exact result-member identities,
     * not a source-wide witness cache. Loading the whole transaction set here
     * would silently recreate the retained-source cost at every reset; the
     * per-member path below performs an exact `(table, row, tx)` lookup
     * instead. */
    if (jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__uses_storage_backed_result_materialization(local->maintained))
        return JZ_OK;
    jz_btree tx_ids;
    jz_btree_init(&tx_ids, &jazz_node_TX_ID_SET_DESC);
    const jazz_node_ResultMemberEntry *member;
    const void *mv;
    JZ_BTREE_FOR(&local->result_set, member, mv) {
        (void)mv;
        const jz_str *t;
        jazz_types_RowUuid r;
        jazz_model_TxId tx_id;
        if (!jazz_protocol__protocol__ResultMemberEntry__as_row(member, &t, &r, &tx_id)) continue;
        jz_btree_insert(&tx_ids, &tx_id, NULL, NULL);
        tx_versions_entry(&cache->tx_versions, local, tx_id);
    }
    const jazz_protocol_ProgramFactEntry *fact;
    JZ_BTREE_FOR(&local->program_facts, fact, mv) {
        (void)mv;
        if (fact->tag != JZ_PFE_RELATION_EDGE) continue;
        const jazz_protocol_RelationEdgeEntry *edge = &fact->relation_edge;
        if (!edge->has_target_version) continue;
        jz_btree_insert(&tx_ids, &edge->target_version.tx, NULL, NULL);
        tx_versions_entry(&cache->tx_versions, local, edge->target_version.tx);
    }
    jz_err e = MAT(NodeState__preload_tx_versions_for_materialization)(self, &tx_ids, &cache->tx_versions);
    jz_btree_drop(&tx_ids);
    if (JZ_IS_ERR(e)) jz_btree_drop(&cache->tx_versions);
    return e;
}

static const jazz_node_VersionRowVec *MAT(NodeState__local_maintained_tx_versions)(jazz_node_NodeState *self, const Local *local, jazz_model_TxId tx_id, LocalMaintainedMaterializationCache *cache)
{
    (void)self;
    return tx_versions_entry(&cache->tx_versions, local, tx_id);
}

static jz_err MAT(NodeState__materialize_local_maintained_view_relation_edge_row_with_cache)(jazz_node_NodeState *self, const Local *local, const jz_str *table_name, jazz_types_RowUuid row_uuid, jazz_model_TxId tx_id, LocalMaintainedMaterializationCache *cache, bool *has, jazz_node_CurrentRow *out)
{
    *has = false;
    const jazz_node_VersionRowVec *tx_versions = MAT(NodeState__local_maintained_tx_versions)(self, local, tx_id, cache);
    const jazz_node_VersionRow *found = jazz_node__node__query_eval__local_maintained_view_content_witness(tx_versions, table_name, row_uuid);
    if (!found) return JZ_OK;
    jazz_node_VersionRow version;
    jazz_node_VersionRow_clone(&version, found);
    jz_err e = MAT(NodeState__projected_current_row_from_materialized_version_in_read_schema)(self, local->result_schema_version, &version, has, out);
    jazz_node_VersionRow_drop(&version);
    return e;
}

static jz_err MAT(NodeState__materialize_local_maintained_view_result_member_unbound)(jazz_node_NodeState *self, const Local *local, const jazz_node_ResultMemberEntry *member, bool *has, jazz_node_CurrentRow *out);
static jz_err MAT(NodeState__current_row_from_aggregate_result_payload)(jazz_node_NodeState *self, jazz_types_SchemaVersionId read_schema, const jazz_model_Query *query, const jazz_engine_AppRowSchema *output, const jazz_node_ResultMemberEntry *member, const jazz_protocol_ResultMemberPayloadEntry *payload, jazz_node_CurrentRow *out);
static jz_err MAT(NodeState__storage_backed_maintained_result_versions)(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_model_TxId tx_id, jazz_node_VersionRowVec *out);

jz_err MAT(NodeState__materialize_local_maintained_view_result_member)(jazz_node_NodeState *self, const Local *local, const jazz_node_ResultMemberEntry *member, bool *has, jazz_node_CurrentRow *out)
{
    JZ_TRY(MAT(NodeState__materialize_local_maintained_view_result_member_unbound)(self, local, member, has, out));
    if (!*has) return JZ_OK;
    jz_err e;
    const jazz_model_RelationProjectColumnVec *columns;
    e = MV(LocalMaintainedViewSubscription__relation_projection_for_member)(local, member, &columns);
    if (JZ_IS_ERR(e)) goto fail;
    if (columns) {
        const jazz_model_TableSchema *t;
        e = jazz_node__node__NodeState__table(self, &local->result_table, &t);
        if (JZ_IS_ERR(e)) goto fail;
        jazz_model_TableSchema table;
        jazz_model_TableSchema_clone(&table, t);
        jazz_node_CurrentRow projected;
        e = jazz_node__node__CurrentRow__project_relation(out, &table, columns, &projected);
        jazz_model_TableSchema_drop(&table);
        if (JZ_IS_ERR(e)) goto fail;
        jazz_node_CurrentRow_drop(out);
        *out = projected;
        const jazz_engine_AppRowSchema *schema;
        e = jazz_node__node__MaintainedTerminalSchemas__direct_app_row_schema(local->terminal_schemas, &schema);
        if (JZ_IS_ERR(e)) goto fail;
        e = MAT(NodeState__bind_app_row_schema_fields)(out, schema);
        if (JZ_IS_ERR(e)) goto fail;
    }
    e = MAT(NodeState__bind_current_row_columns_in_schema)(self, local->result_schema_version, out);
    if (JZ_IS_ERR(e)) goto fail;
    return JZ_OK;
fail:
    jazz_node_CurrentRow_drop(out);
    *has = false;
    return e;
}

static jz_err project_select(const jazz_model_TableSchema *table, const Local *local, jazz_node_CurrentRow *row)
{
    if (!local->has_result_select) return JZ_OK;
    jazz_node_CurrentRow p;
    JZ_TRY(jazz_node__node__CurrentRow__project(row, table, local->result_select.ptr, local->result_select.len, &p));
    jazz_node_CurrentRow_drop(row);
    *row = p;
    return JZ_OK;
}

static jz_err MAT(NodeState__materialize_local_maintained_view_result_member_unbound)(jazz_node_NodeState *self, const Local *local, const jazz_node_ResultMemberEntry *member, bool *has, jazz_node_CurrentRow *out)
{
    *has = false;
    bool agg = local->result_query.aggregate != NULL;
    if (jazz_node__node__query_eval__is_public_aggregate_result_member(member, &local->result_table, agg)) {
        const jazz_protocol_ResultMemberPayloadEntry *payload = jz_btree_get(&local->result_payloads, member);
        if (!payload) return JZN_INVALID_STORED("aggregate result member is missing its payload");
        const jazz_engine_AppRowSchema *schema;
        JZ_TRY(jazz_node__node__MaintainedTerminalSchemas__aggregate_app_row_schema(local->terminal_schemas, &schema));
        JZ_TRY(MAT(NodeState__current_row_from_aggregate_result_payload)(self, local->result_schema_version, &local->result_query, schema, member, payload, out));
        *has = true;
        return JZ_OK;
    }
    const jz_str *entry_table;
    jazz_types_RowUuid entry_row;
    jazz_model_TxId entry_tx;
    if (!jazz_protocol__protocol__ResultMemberEntry__as_row(member, &entry_table, &entry_row, &entry_tx))
        return JZN_INVALID_STORED("local maintained subscription cannot materialize non-row result member yet");
    /* Result-member entries retain the canonical authored table so the
     * membership witness continues to identify the exact immutable version
     * across a rename.  Payloads and application rows, however, are
     * interpreted in this subscription's read schema. */
    const jazz_model_TableSchema *tref;
    JZ_TRY(jazz_node__node__NodeState__table(self, &local->result_table, &tref));
    jazz_model_TableSchema table;
    jazz_model_TableSchema_clone(&table, tref);
    jz_err e = JZ_OK;
    if (local->result_query.flat_join) {
        const jazz_protocol_ResultMemberPayloadEntry *payload = jz_btree_get(&local->result_payloads, member);
        if (!payload) { e = JZN_INVALID_STORED("flat joined result member is missing its tuple payload"); goto out_table; }
        const jazz_node_ResultMembershipSchema *schema;
        e = jazz_node__node__MaintainedTerminalSchemas__current_payload_schema(local->terminal_schemas, &schema);
        if (JZ_IS_ERR(e)) goto out_table;
        e = MAT(NodeState__current_row_from_result_payload)(self, &table, payload, schema, out);
        if (!JZ_IS_ERR(e)) *has = true;
        goto out_table;
    }
    if (local->has_result_select) {
        const jazz_protocol_ResultMemberPayloadEntry *payload = jz_btree_get(&local->result_payloads, member);
        if (payload) {
            const jazz_node_ResultMembershipSchema *schema;
            e = jazz_node__node__MaintainedTerminalSchemas__current_payload_schema(local->terminal_schemas, &schema);
            if (JZ_IS_ERR(e)) goto out_table;
            e = MAT(NodeState__current_row_from_result_payload)(self, &table, payload, schema, out);
            if (JZ_IS_ERR(e)) goto out_table;
            e = project_select(&table, local, out);
            if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(out); goto out_table; }
            *has = true;
            goto out_table;
        }
    }
    jazz_node_VersionRowVec tx_versions = jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__versions_by_tx(local->maintained, entry_tx);
    jazz_node_VersionRow version;
    const jazz_node_VersionRow *witness;
    e = jazz_node__node__views__NodeState__maintained_witness_for_result_member(self, &tx_versions, local->result_schema_version, &local->result_table, entry_row, &witness);
    if (JZ_IS_ERR(e)) goto out_versions;
    if (witness) {
        jazz_node_VersionRow_clone(&version, witness);
    } else {
        jazz_node_VersionRow content_winner;
        if (jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__replacement_for(local->maintained, entry_table, entry_row, &content_winner)) {
            jazz_model_TxId wtx;
            e = jazz_node__node__views__NodeState__version_tx_id(self, &content_winner, &wtx);
            if (JZ_IS_ERR(e)) { jazz_node_VersionRow_drop(&content_winner); goto out_versions; }
            if (!jazz_model_TxId_eq(&wtx, &entry_tx)) { jazz_node_VersionRow_drop(&content_winner); goto out_versions; }
            JZ_VEC_PUSH(tx_versions, content_winner);
            if (tx_versions.len == 0) { e = jazz_node__node__Error_missing_transaction(entry_tx); goto out_versions; }
            jazz_node_VersionRow_clone(&version, JZ_VEC_AT(tx_versions, tx_versions.len - 1));
        } else {
            /* A client-local maintained graph can lag its remote authority's
             * source. The authority ViewUpdate has already authenticated both
             * this exact result member and its content bundle; use that
             * member's exact `(table, row, tx)` witness to materialize the
             * newly admitted row. This is payload lookup, not a facade-side
             * query or recompute. */
            jazz_node_VersionRowVec fetched;
            if (jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__uses_storage_backed_result_materialization(local->maintained))
                e = MAT(NodeState__storage_backed_maintained_result_versions)(self, entry_table, entry_row, entry_tx, &fetched);
            else
                e = jazz_node__node__views__NodeState__query_versions_for_tx(self, entry_tx, &fetched);
            if (JZ_IS_ERR(e)) goto out_versions;
            e = jazz_node__node__views__NodeState__maintained_witness_for_result_member(self, &fetched, local->result_schema_version, &local->result_table, entry_row, &witness);
            if (!JZ_IS_ERR(e) && witness) jazz_node_VersionRow_clone(&version, witness);
            jz_vec_drop(&fetched, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
            if (JZ_IS_ERR(e) || !witness) goto out_versions;
        }
    }
    bool projected;
    e = MAT(NodeState__projected_current_row_from_materialized_version_in_read_schema)(self, local->result_schema_version, &version, &projected, out);
    jazz_node_VersionRow_drop(&version);
    if (JZ_IS_ERR(e)) goto out_versions;
    if (!projected) { e = JZN_INVALID_STORED("maintained result witness does not project into the read schema"); goto out_versions; }
    e = project_select(&table, local, out);
    if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(out); goto out_versions; }
    *has = true;
out_versions:
    jz_vec_drop(&tx_versions, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
out_table:
    jazz_model_TableSchema_drop(&table);
    return e;
}

static jz_err MAT(NodeState__materialize_local_maintained_view_result_member_with_cache)(jazz_node_NodeState *self, const Local *local, const jazz_node_ResultMemberEntry *member, LocalMaintainedMaterializationCache *cache, bool *has, jazz_node_CurrentRow *out)
{
    *has = false;
    bool agg = local->result_query.aggregate != NULL;
    if (jazz_node__node__query_eval__is_public_aggregate_result_member(member, &local->result_table, agg)) {
        const jazz_protocol_ResultMemberPayloadEntry *payload = jz_btree_get(&local->result_payloads, member);
        if (!payload) return JZN_INVALID_STORED("aggregate result member is missing its payload");
        const jazz_engine_AppRowSchema *schema;
        JZ_TRY(jazz_node__node__MaintainedTerminalSchemas__aggregate_app_row_schema(local->terminal_schemas, &schema));
        JZ_TRY(MAT(NodeState__current_row_from_aggregate_result_payload)(self, local->result_schema_version, &local->result_query, schema, member, payload, out));
        *has = true;
        return JZ_OK;
    }
    const jz_str *entry_table;
    jazz_types_RowUuid entry_row;
    jazz_model_TxId entry_tx;
    if (!jazz_protocol__protocol__ResultMemberEntry__as_row(member, &entry_table, &entry_row, &entry_tx))
        return JZN_INVALID_STORED("local maintained subscription cannot materialize non-row result member yet");
    /* See the non-cached path above: the entry label is canonical while the
     * materialized row belongs to the subscription read schema. */
    const jazz_model_TableSchema *tref;
    JZ_TRY(jazz_node__node__NodeState__table(self, &local->result_table, &tref));
    jazz_model_TableSchema table;
    jazz_model_TableSchema_clone(&table, tref);
    jz_err e = JZ_OK;
    if (local->result_query.flat_join) {
        const jazz_protocol_ResultMemberPayloadEntry *payload = jz_btree_get(&local->result_payloads, member);
        if (!payload) { e = JZN_INVALID_STORED("flat joined result member is missing its tuple payload"); goto out_table; }
        const jazz_node_ResultMembershipSchema *schema;
        e = jazz_node__node__MaintainedTerminalSchemas__current_payload_schema(local->terminal_schemas, &schema);
        if (JZ_IS_ERR(e)) goto out_table;
        e = MAT(NodeState__current_row_from_result_payload)(self, &table, payload, schema, out);
        if (JZ_IS_ERR(e)) goto out_table;
        const jazz_model_RelationProjectColumnVec *columns;
        e = MV(LocalMaintainedViewSubscription__relation_projection_for_member)(local, member, &columns);
        if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(out); goto out_table; }
        if (columns) {
            jazz_node_CurrentRow p;
            e = jazz_node__node__CurrentRow__project_relation(out, &table, columns, &p);
            if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(out); goto out_table; }
            jazz_node_CurrentRow_drop(out);
            *out = p;
            const jazz_engine_AppRowSchema *aschema;
            e = jazz_node__node__MaintainedTerminalSchemas__direct_app_row_schema(local->terminal_schemas, &aschema);
            if (!JZ_IS_ERR(e)) e = MAT(NodeState__bind_app_row_schema_fields)(out, aschema);
            if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(out); goto out_table; }
        }
        *has = true;
        goto out_table;
    }
    jazz_node_VersionRowVec tx_versions;
    jz_vec_clone(&tx_versions, MAT(NodeState__local_maintained_tx_versions)(self, local, entry_tx, cache), sizeof(jazz_node_VersionRow), (void (*)(void *, const void *))jazz_node_VersionRow_clone);
    jazz_node_VersionRow version;
    const jazz_node_VersionRow *witness;
    e = jazz_node__node__views__NodeState__maintained_witness_for_result_member(self, &tx_versions, local->result_schema_version, &local->result_table, entry_row, &witness);
    if (JZ_IS_ERR(e)) goto out_versions;
    if (witness) {
        jazz_node_VersionRow_clone(&version, witness);
    } else {
        jazz_node_VersionRowVec fetched;
        if (jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__uses_storage_backed_result_materialization(local->maintained))
            e = MAT(NodeState__storage_backed_maintained_result_versions)(self, entry_table, entry_row, entry_tx, &fetched);
        else
            e = jazz_node__node__views__NodeState__query_versions_for_tx(self, entry_tx, &fetched);
        if (JZ_IS_ERR(e)) goto out_versions;
        e = jazz_node__node__views__NodeState__maintained_witness_for_result_member(self, &fetched, local->result_schema_version, &local->result_table, entry_row, &witness);
        if (!JZ_IS_ERR(e) && witness) jazz_node_VersionRow_clone(&version, witness);
        jz_vec_drop(&fetched, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
        if (JZ_IS_ERR(e) || !witness) goto out_versions;
    }
    e = MAT(NodeState__projected_current_row_from_materialized_version_in_read_schema)(self, local->result_schema_version, &version, has, out);
    jazz_node_VersionRow_drop(&version);
out_versions:
    jz_vec_drop(&tx_versions, sizeof(jazz_node_VersionRow), jazz_node_VersionRow_drop);
out_table:
    jazz_model_TableSchema_drop(&table);
    return e;
}

/* Load only the immutable content/deletion cells for one delivered
 * result-member identity.  This is deliberately narrower than
 * `query_versions_for_tx`: a multi-row transaction must not turn a one-row
 * subscription entry into a whole-transaction materialization. */
static jz_err MAT(NodeState__storage_backed_maintained_result_versions)(jazz_node_NodeState *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_model_TxId tx_id, jazz_node_VersionRowVec *out)
{
    bool has;
    jazz_node_StoredTransaction stored_tx;
    JZ_TRY(jazz_node__node__views__NodeState__query_transaction(self, tx_id, &has, &stored_tx));
    if (!has) return jazz_node__node__Error_missing_transaction(tx_id);
    jz_btree rows;
    jz_btree_init(&rows, &jazz_node_TABLE_ROW_SET_DESC);
    jazz_node_TableRowKey k = {jz_str_clone(table), row_uuid};
    jz_btree_insert(&rows, &k, NULL, NULL);
    jz_err e = jazz_node__node__views__NodeState__query_versions_for_tx_rows_by_alias(self, tx_id, stored_tx.node_alias, &rows, out);
    jz_btree_drop(&rows);
    jazz_node_StoredTransaction_drop(&stored_tx);
    return e;
}

static jz_err MAT(NodeState__preload_tx_versions_for_materialization)(jazz_node_NodeState *self, const jz_btree *tx_ids, jz_btree *cache)
{
    const jazz_model_TxId *tx_id;
    const void *v;
    JZ_BTREE_FOR(tx_ids, tx_id, v) {
        (void)v;
        const jazz_node_VersionRowVec *existing = jz_btree_get(cache, tx_id);
        if (existing && existing->len != 0) continue;
        jazz_node_VersionRowVec versions, old;
        if (jazz_node__node__views__NodeState__cached_tx_versions(self, *tx_id, &versions)) {
            if (jz_btree_insert(cache, tx_id, &versions, &old)) jazz_node_VersionRowVec_drop(&old);
            continue;
        }
        jazz_types_NodeAlias alias;
        if (!jazz_node__node__NodeAliases__get(self->node_aliases, &tx_id->node, &alias)) continue;
        /* Each transaction record lists the history rows it wrote, so its
         * versions are exact point reads; `query_versions_for_tx` returns them
         * in `(table, row)` order. */
        JZ_TRY(jazz_node__node__views__NodeState__query_versions_for_tx(self, *tx_id, &versions));
        if (jz_btree_insert(cache, tx_id, &versions, &old)) jazz_node_VersionRowVec_drop(&old);
    }
    return JZ_OK;
}

/* Render a maintained relation witness through the subscription's read schema
 * before exposing it as an application row.
 *
 * Relation-edge witnesses retain the authored immutable record so that their
 * identity and provenance remain exact.  They are not, however, rendered
 * directly; apply the complete schema lens before decoding. */
jz_err MAT(NodeState__projected_current_row_from_materialized_version_in_read_schema)(jazz_node_NodeState *self, jazz_types_SchemaVersionId read_schema, const jazz_node_VersionRow *version, bool *has, jazz_node_CurrentRow *out)
{
    *has = false;
    jazz_types_SchemaVersionId authored_schema;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(version), &authored_schema))
        return JZN_INVALID_STORED("relation edge witness schema version alias must exist");
    const jazz_model_TableSchema *authored_table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, jazz_node__node__codec__VersionRow__table(version), authored_schema, &authored_table));
    jz_btree cells;
    JZ_TRY(jazz_node__node__views__NodeState__materialized_cells_for_version(self, authored_table, version, &cells));
    bool projected;
    jz_str projected_table;
    jz_err e = jazz_node__node__lenses__NodeState__translate_cells(self, authored_schema, read_schema, jazz_node__node__codec__VersionRow__table(version), &cells, &projected, &projected_table);
    if (JZ_IS_ERR(e) || !projected) goto out;
    const jazz_model_TableSchema *read_table;
    e = jazz_node__node__NodeState__table_in_schema_ref(self, &projected_table, read_schema, &read_table);
    jz_str_drop(&projected_table);
    if (JZ_IS_ERR(e)) goto out;
    e = jazz_node__node__current_row_from_materialized_cells(read_table, version, &cells, out);
    if (JZ_IS_ERR(e)) goto out;
    e = MAT(NodeState__bind_current_row_columns_in_schema)(self, read_schema, out);
    if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(out); goto out; }
    *has = true;
out:
    jz_btree_drop(&cells);
    return e;
}

static jz_err MAT(NodeState__current_row_from_aggregate_result_payload)(jazz_node_NodeState *self, jazz_types_SchemaVersionId read_schema, const jazz_model_Query *query, const jazz_engine_AppRowSchema *output, const jazz_node_ResultMemberEntry *member, const jazz_protocol_ResultMemberPayloadEntry *payload, jazz_node_CurrentRow *out)
{
    const jazz_engine_AggregateResultSchema *schema;
    JZ_TRY(jazz_node__node__query_eval__query_result_rows__aggregate_output_schema(output, &schema));
    groove_OwnedRecord decoded;
    JZ_TRY(jazz_node__node__descriptor_roles__decode_aggregate_payload_record(payload, schema, &decoded));
    groove_BorrowedRecord payload_record = groove__records__record__OwnedRecord__borrowed(&decoded);
    jazz_types_RowUuid row_uuid;
    jz_err e = jazz_node__node__query_eval__aggregate_result_member_row_uuid(member, &row_uuid);
    if (!JZ_IS_ERR(e)) e = jazz_node__node__query_eval__aggregate_current_row_from_record(query, output, row_uuid, &payload_record, out);
    if (!JZ_IS_ERR(e)) {
        e = MAT(NodeState__bind_current_row_columns_in_schema)(self, read_schema, out);
        if (JZ_IS_ERR(e)) jazz_node_CurrentRow_drop(out);
    }
    groove_OwnedRecord_drop(&decoded);
    return e;
}

jz_err MAT(NodeState__current_row_from_result_payload)(jazz_node_NodeState *self, const jazz_model_TableSchema *table, const jazz_protocol_ResultMemberPayloadEntry *payload, const jazz_node_ResultMembershipSchema *schema, jazz_node_CurrentRow *out)
{
    (void)self;
    return jazz_node__node__descriptor_roles__decode_current_payload_record(&table->name, payload, schema, out);
}

static jz_err MAT(NodeState__materialize_relation_snapshot_root_rows)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const groove_MultisinkDeltas *snapshots, const jazz_engine_QueryProgram *program, jazz_node_CurrentRowVec *out);

typedef struct RelationEdgeCandidate {
    jazz_node_RelationEdge edge;
    jz_str canonical_target_table;
    jazz_types_TxTime target_tx_time;
    jazz_types_NodeAlias target_tx_node;
} RelationEdgeCandidate;
static void candidate_drop(void *p) { RelationEdgeCandidate *c = p; jazz_node_RelationEdge_drop(&c->edge); jz_str_drop(&c->canonical_target_table); }
static int candidate_cmp(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    const jazz_node_RelationEdge *l = &((const RelationEdgeCandidate *)a)->edge, *r = &((const RelationEdgeCandidate *)b)->edge;
    int c = jz_str_cmp(&l->source_table, &r->source_table);
    if (c) return c;
    c = jz_uuid_cmp(&l->source_row, &r->source_row);
    if (c) return c;
    c = jz_str_cmp(&l->relation, &r->relation);
    if (c) return c;
    return jz_uuid_cmp(&l->target_row, &r->target_row);
}
typedef struct { jz_str table; jazz_types_RowUuid row; jz_str relation; } EdgeGroup;
static int edge_group_cmp(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    const EdgeGroup *l = a, *r = b;
    int c = jz_str_cmp(&l->table, &r->table);
    if (c) return c;
    c = jz_uuid_cmp(&l->row, &r->row);
    if (c) return c;
    return jz_str_cmp(&l->relation, &r->relation);
}
static void edge_group_drop(void *p) { EdgeGroup *g = p; jz_str_drop(&g->table); jz_str_drop(&g->relation); }
static const jz_btree_desc EDGE_GROUP_COUNTS_DESC = {sizeof(EdgeGroup), sizeof(usize), edge_group_cmp, edge_group_drop, NULL};
static const jz_btree_desc WINDOWS_DESC = {sizeof(jz_str), sizeof(jazz_node_RelationSnapshotWindow), jz_cmp_str, (void (*)(void *))jz_str_drop, NULL};

static bool field_position(const groove_RecordDescriptor *d, const char *name, u32 *out)
{
    u32 n;
    const groove_DescriptorField *fields = groove__records__descriptor__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++)
        if (fields[i].has_name && jz_str_eq_cstr(&fields[i].name, name)) { *out = i; return true; }
    return false;
}

static jz_err branch_discriminator(const groove_BorrowedRecord *record, bool has_idx, u32 idx, bool *has, jz_bytes *out)
{
    *has = false;
    if (!has_idx) return JZ_OK;
    groove_Value v;
    JZ_TRY(groove__records__record__BorrowedRecord__get_idx(record, idx, &v));
    jz_err e = JZ_OK;
    const groove_Value *inner = &v;
    if (v.tag == GV_NULLABLE) {
        if (!v.v.nullable) goto out;
        inner = v.v.nullable;
    }
    if (inner->tag == GV_UUID) { *out = jz_bytes_from(inner->v.uuid.b, 16); *has = true; }
    else if (inner->tag == GV_BYTES) { *out = jz_bytes_clone(&inner->v.bytes); *has = true; }
    else e = JZN_INVALID_STORED("relation edge branch discriminator must be UUID or bytes");
out:
    groove_Value_drop(&v);
    return e;
}

static jz_err version_ref(const jazz_node_NodeState *self, jazz_types_TxTime time, jazz_types_NodeAlias alias, bool has_branch, jz_bytes branch_or_prefix, jazz_protocol_RowVersionRefEntry *out)
{
    jazz_types_NodeUuid node;
    if (!jazz_node__node__NodeAliases__node_for_alias(self->node_aliases, alias, &node)) {
        if (has_branch) jz_bytes_drop(&branch_or_prefix);
        return JZN_INVALID_STORED("relation edge node alias is missing");
    }
    memset(out, 0, sizeof *out);
    out->tx.time = time;
    out->tx.node = node;
    out->layer = JZ_LAYER_CONTENT;
    out->has_branch_or_prefix = has_branch;
    out->branch_or_prefix = branch_or_prefix;
    return JZ_OK;
}

static void row_version_ref_drop(jazz_protocol_RowVersionRefEntry *r)
{
    if (r->has_branch_or_prefix) jz_bytes_drop(&r->branch_or_prefix);
    if (r->has_row_digest) jz_bytes_drop(&r->row_digest);
}

jz_err MAT(NodeState__materialize_relation_snapshot_from_query_engine)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_protocol_ReadViewSpec *read_view, const groove_MultisinkDeltas *snapshots, const jazz_engine_QueryProgram *program, jazz_node_RelationSnapshot *out)
{
    jazz_node_CurrentRowVec root_rows;
    JZ_TRY(MAT(NodeState__materialize_relation_snapshot_root_rows)(self, shape, snapshots, program, &root_rows));
    usize root_count = root_rows.len;
    memset(out, 0, sizeof *out);
    out->root_count = root_count;
    out->rows = root_rows;
    /* Groove's app-rows terminal is the sole structured-output owner. Jazz
     * transports its recursive roots; relation facts are not a second public
     * representation and never participate in tree assembly. */
    if (shape->query.array_subqueries.len != 0) return JZ_OK;
    /* Aggregate result records intentionally have no application row identity
     * and cannot own relation edges. They are already fully materialized by
     * the root terminal. */
    if (shape->query.aggregate) return JZ_OK;
    jz_btree row_keys;
    jz_btree_init(&row_keys, &jazz_node_TABLE_ROW_SET_DESC);
    JZ_VEC_FOR(jazz_node_CurrentRow, row, out->rows) {
        jazz_node_TableRowKey k = {jz_str_clone(jazz_node__node__CurrentRow__table(row)), jazz_node__node__CurrentRow__row_uuid(row)};
        if (!jz_btree_insert(&row_keys, &k, NULL, NULL)) {} else jz_str_drop(&k.table);
    }
    const groove_RecordDeltas *edges = groove__ivm__MultisinkDeltas__get(snapshots, "maintained.relation_edges");
    if (!edges) { jz_btree_drop(&row_keys); return JZ_OK; }

    jz_btree windows;
    jz_btree_init(&windows, &WINDOWS_DESC);
    MAT(NodeState__relation_snapshot_no_order_windows)(shape->query.array_subqueries.ptr, shape->query.array_subqueries.len, &windows);
    const groove_RecordDescriptor *descriptor = groove__ivm__RecordDeltas__descriptor(edges);
    JZ_VEC(RelationEdgeCandidate) candidates = {0};
    jz_btree counts;
    jz_btree_init(&counts, &EDGE_GROUP_COUNTS_DESC);
    jz_err e;
    u32 source_table_idx, source_row_idx, source_tx_time_idx, source_tx_node_idx, relation_idx;
    u32 target_table_idx, target_row_idx, target_tx_time_idx, target_tx_node_idx;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "source_table", &source_table_idx))) goto out;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "source_row", &source_row_idx))) goto out;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "source_tx_time", &source_tx_time_idx))) goto out;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "source_tx_node_id", &source_tx_node_idx))) goto out;
    u32 source_branch_idx;
    bool has_source_branch_idx = field_position(descriptor, "source_branch_or_prefix", &source_branch_idx);
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "path", &relation_idx))) goto out;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "target_table", &target_table_idx))) goto out;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "target_row", &target_row_idx))) goto out;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "target_tx_time", &target_tx_time_idx))) goto out;
    if (JZ_IS_ERR(e = jazz_node__node__query_eval__required_field_idx(descriptor, "target_tx_node_id", &target_tx_node_idx))) goto out;
    u32 target_branch_idx;
    bool has_target_branch_idx = field_position(descriptor, "target_branch_or_prefix", &target_branch_idx);

    groove_RecordDeltasIter it = {edges, 0};
    groove_BorrowedRecord record;
    i64 weight;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
        if (weight <= 0) continue;
        jz_slice s;
        jazz_types_RowUuid source_row, target_row;
        u64 source_tx_time, source_tx_node, target_tx_time, target_tx_node;
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_str(&record, source_table_idx, &s))) goto out;
        jz_str source_table = jz_str_from_slice(s);
        jz_str relation = {0}, target_table_name = {0};
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_uuid(&record, source_row_idx, &source_row))) goto rec_fail;
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_u64(&record, source_tx_time_idx, &source_tx_time))) goto rec_fail;
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_u64(&record, source_tx_node_idx, &source_tx_node))) goto rec_fail;
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_str(&record, relation_idx, &s))) goto rec_fail;
        relation = jz_str_from_slice(s);
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_str(&record, target_table_idx, &s))) goto rec_fail;
        target_table_name = jz_str_from_slice(s);
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_uuid(&record, target_row_idx, &target_row))) goto rec_fail;
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_u64(&record, target_tx_time_idx, &target_tx_time))) goto rec_fail;
        if (JZ_IS_ERR(e = groove__records__record__BorrowedRecord__get_u64(&record, target_tx_node_idx, &target_tx_node))) goto rec_fail;
        bool hb;
        jz_bytes branch;
        jazz_protocol_RowVersionRefEntry source_version, target_version;
        if (JZ_IS_ERR(e = branch_discriminator(&record, has_source_branch_idx, source_branch_idx, &hb, &branch))) goto rec_fail;
        if (JZ_IS_ERR(e = version_ref(self, source_tx_time, source_tx_node, hb, branch, &source_version))) goto rec_fail;
        if (JZ_IS_ERR(e = branch_discriminator(&record, has_target_branch_idx, target_branch_idx, &hb, &branch))) { row_version_ref_drop(&source_version); goto rec_fail; }
        if (JZ_IS_ERR(e = version_ref(self, target_tx_time, target_tx_node, hb, branch, &target_version))) { row_version_ref_drop(&source_version); goto rec_fail; }
        jazz_node_VersionRow canonical_source_version, canonical_target_version;
        e = MAT(NodeState__resolve_relation_terminal_version)(self, &source_table, source_row, &source_version, shape->schema_version, &canonical_source_version);
        if (JZ_IS_ERR(e)) goto ver_fail;
        e = MAT(NodeState__resolve_relation_terminal_version)(self, &target_table_name, target_row, &target_version, shape->schema_version, &canonical_target_version);
        if (JZ_IS_ERR(e)) { jazz_node_VersionRow_drop(&canonical_source_version); goto ver_fail; }
        jz_str projected_source_table, projected_target_table;
        e = MAT(NodeState__project_relation_edge_table_through_read_schema)(self, jazz_node__node__codec__VersionRow__table(&canonical_source_version), source_row, &source_version, shape->schema_version, &projected_source_table);
        if (!JZ_IS_ERR(e)) {
            e = MAT(NodeState__project_relation_edge_table_through_read_schema)(self, jazz_node__node__codec__VersionRow__table(&canonical_target_version), target_row, &target_version, shape->schema_version, &projected_target_table);
            if (JZ_IS_ERR(e)) jz_str_drop(&projected_source_table);
        }
        if (!JZ_IS_ERR(e)) {
            RelationEdgeCandidate c;
            c.edge.source_table = projected_source_table;
            c.edge.source_row = source_row;
            c.edge.relation = relation;
            relation.ptr = NULL; relation.len = relation.cap = 0;
            c.edge.target_table = projected_target_table;
            c.edge.target_row = target_row;
            c.canonical_target_table = jz_str_clone(jazz_node__node__codec__VersionRow__table(&canonical_target_version));
            c.target_tx_time = target_tx_time;
            c.target_tx_node = target_tx_node;
            JZ_VEC_PUSH(candidates, c);
        }
        jazz_node_VersionRow_drop(&canonical_source_version);
        jazz_node_VersionRow_drop(&canonical_target_version);
ver_fail:
        row_version_ref_drop(&source_version);
        row_version_ref_drop(&target_version);
rec_fail:
        jz_str_drop(&source_table);
        jz_str_drop(&relation);
        jz_str_drop(&target_table_name);
        if (JZ_IS_ERR(e)) goto out;
    }
    jz_sort(candidates.ptr, candidates.len, sizeof(RelationEdgeCandidate), candidate_cmp, NULL);
    u32 i = 0;
    for (; i < candidates.len; i++) {
        RelationEdgeCandidate *candidate = JZ_VEC_AT(candidates, i);
        EdgeGroup group = {jz_str_clone(&candidate->edge.source_table), candidate->edge.source_row, jz_str_clone(&candidate->edge.relation)};
        bool inserted;
        usize *count = jz_btree_entry(&counts, &group, &inserted);
        if (inserted) *count = 0; else edge_group_drop(&group);
        const jazz_node_RelationSnapshotWindow *window = jz_btree_get(&windows, &candidate->edge.relation);
        usize ordinal = *count;
        *count += 1;
        if (window) {
            usize end = window->offset + window->limit;
            if (end < window->offset) end = SIZE_MAX; /* saturating_add */
            if (ordinal < window->offset || (window->has_limit && ordinal >= end)) {
                candidate_drop(candidate);
                continue;
            }
        }
        jazz_node_TableRowKey k = {jz_str_clone(&candidate->edge.target_table), candidate->edge.target_row};
        if (!jz_btree_contains(&row_keys, &k)) {
            jz_btree_insert(&row_keys, &k, NULL, NULL);
            jazz_node_CurrentRow row;
            e = MAT(NodeState__materialize_relation_edge_target_row)(self, read_view, shape->schema_version, &candidate->canonical_target_table,
                candidate->edge.target_row, candidate->target_tx_time, candidate->target_tx_node, &row);
            if (JZ_IS_ERR(e)) break;
            JZ_VEC_PUSH(out->rows, row);
        } else {
            jz_str_drop(&k.table);
        }
        JZ_VEC_PUSH(out->edges, candidate->edge);
        jz_str_drop(&candidate->canonical_target_table);
    }
    for (; i < candidates.len; i++) candidate_drop(JZ_VEC_AT(candidates, i));
    candidates.len = 0;
out:
    jz_vec_drop(&candidates, sizeof(RelationEdgeCandidate), candidate_drop);
    jz_btree_drop(&counts);
    jz_btree_drop(&windows);
    jz_btree_drop(&row_keys);
    if (JZ_IS_ERR(e)) jazz_node_RelationSnapshot_drop(out);
    return e;
}

jz_err MAT(NodeState__materialize_relation_edge_target_row)(jazz_node_NodeState *self, const jazz_protocol_ReadViewSpec *read_view, jazz_types_SchemaVersionId read_schema, const jz_str *target_table_name, jazz_types_RowUuid target_row, jazz_types_TxTime target_tx_time, jazz_types_NodeAlias target_tx_node, jazz_node_CurrentRow *out)
{
    (void)read_view;
    bool has;
    jazz_node_VersionRow version;
    JZ_TRY(jazz_node__node__views__NodeState__query_version_by_alias(self, target_table_name, target_row, target_tx_time, target_tx_node, &has, &version));
    if (has) {
        bool projected;
        jz_err e = MAT(NodeState__projected_current_row_from_materialized_version_in_read_schema)(self, read_schema, &version, &projected, out);
        jazz_node_VersionRow_drop(&version);
        JZ_TRY(e);
        if (!projected) return JZN_INVALID_STORED("relation edge target version does not project into the read schema");
        return JZ_OK;
    }
    return JZN_INVALID_STORED("relation edge target version is missing");
}

void MAT(NodeState__relation_snapshot_no_order_windows)(const jazz_model_ArraySubquery *subqueries, u32 n, jz_btree *windows)
{
    if (!windows->d) jz_btree_init(windows, &WINDOWS_DESC);
    for (u32 i = 0; i < n; i++) {
        const jazz_model_ArraySubquery *subquery = &subqueries[i];
        if (subquery->order_by.len == 0 && (subquery->has_limit || subquery->offset != 0)) {
            jz_str k = jz_str_clone(&subquery->column_name);
            jazz_node_RelationSnapshotWindow w = {subquery->offset, subquery->has_limit, subquery->limit};
            jazz_node_RelationSnapshotWindow old;
            jz_str oldk;
            if (jz_btree_remove(windows, &k, &oldk, &old)) jz_str_drop(&oldk);
            jz_btree_insert(windows, &k, &w, NULL);
        }
        /* windows.extend(nested) */
        MAT(NodeState__relation_snapshot_no_order_windows)(subquery->nested_arrays.ptr, subquery->nested_arrays.len, windows);
    }
}

static jz_err MAT(NodeState__bind_compiled_app_row_fields)(jazz_node_CurrentRow *row, const jazz_engine_QueryProgram *program);

static jz_err MAT(NodeState__materialize_relation_snapshot_root_rows)(jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const groove_MultisinkDeltas *snapshots, const jazz_engine_QueryProgram *program, jazz_node_CurrentRowVec *out)
{
    const groove_RecordDeltas *app_rows = groove__ivm__MultisinkDeltas__get(snapshots, jazz_engine__query_engine__JAZZ_APP_ROWS_SINK);
    if (!app_rows)
        return jazz_node__node__Error_query_lowering(jz_str_from("relation snapshot program did not emit app rows"));
    if (shape->query.aggregate) {
        /* Aggregate terminals own synthetic result identities and public
         * bindings. Decoding these as source-table rows would turn count
         * aliases into unresolved catalogue columns. */
        jazz_engine_AppRowSchema output;
        JZ_TRY(jazz_node__node__query_eval__materialization_app_row_schema(NULL, program, &output));
        JZ_TRY(MAT(NodeState__materialize_aggregate_query_rows)(self, &shape->query, &output, app_rows, out));
        jz_err e = MAT(NodeState__finish_engine_query_rows_in_schema)(self, &shape->query, shape->schema_version, out);
        if (JZ_IS_ERR(e)) jz_vec_drop(out, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
        return e;
    }
    const jazz_model_TableSchema *tref;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, &shape->query.table, shape->schema_version, &tref));
    jazz_model_TableSchema table;
    jazz_model_TableSchema_clone(&table, tref);
    memset(out, 0, sizeof *out);
    jz_err e = JZ_OK;
    groove_RecordDeltasIter it = {app_rows, 0};
    groove_BorrowedRecord record;
    i64 weight;
    while (groove__ivm__RecordDeltas__next(&it, &record, &weight)) {
        if (weight <= 0) continue;
        jazz_node_CurrentRow row, m;
        if (JZ_IS_ERR(e = jazz_node__node__decode_current_row(&table, &record, &row))) goto fail;
        if (JZ_IS_ERR(e = jazz_node__node__views__NodeState__materialize_current_row(self, &table, row, &m))) goto fail;
        JZ_VEC_PUSH(*out, m);
    }
    /* Multisink records are transport-key ordered. Restore public root rank
     * while retaining the lowered program's membership and window. */
    JZ_VEC_FOR(jazz_node_CurrentRow, row, *out)
        if (JZ_IS_ERR(e = MAT(NodeState__bind_compiled_app_row_fields)(row, program))) goto fail;
    if (!shape->query.relation)
        if (JZ_IS_ERR(e = MAT(NodeState__apply_query_order_in_schema)(self, &shape->query, shape->schema_version, out->ptr, out->len))) goto fail;
    jazz_model_TableSchema_drop(&table);
    return JZ_OK;
fail:
    jz_vec_drop(out, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
    jazz_model_TableSchema_drop(&table);
    return e;
}

enum { JZ_OTS_APP_ROWS = 0 };
static jz_err MAT(NodeState__bind_compiled_app_row_fields)(jazz_node_CurrentRow *row, const jazz_engine_QueryProgram *program)
{
    /* let ProgramOutputSchemas::RowSet(outputs) = &program.lowered.output; */
    const jazz_engine_AppRowSchema *schema = NULL;
    JZ_VEC_FOR(const jazz_engine_LoweredTerminal, t, program->lowered.terminals) {
        if (t->output.tag == JZ_OTS_APP_ROWS) { schema = t->output.payload; break; }
    }
    if (!schema) return JZN_INVALID_STORED("compiled query has no application row schema");
    return MAT(NodeState__bind_app_row_schema_fields)(row, schema);
}

jz_err MAT(NodeState__bind_app_row_schema_fields)(jazz_node_CurrentRow *row, const jazz_engine_AppRowSchema *schema)
{
    u32 n;
    const groove_DescriptorField *fields = groove__records__descriptor__RecordDescriptor__fields(row->record->descriptor, &n);
    jazz_engine_PublicationFieldVec *pf = jz_rc_new(sizeof(jazz_engine_PublicationFieldVec), jazz_engine_PublicationFieldVec_drop);
    memset(pf, 0, sizeof *pf);
    for (u32 i = 0; i < n; i++) {
        if (!fields[i].has_name) {
            jz_rc_release(pf);
            return JZN_INVALID_STORED("application row field is unnamed");
        }
        const jazz_engine_CurrentRowPublicationField *found = jz_btree_get(&schema->publication_fields, &fields[i].name);
        jazz_engine_CurrentRowPublicationField f;
        if (found) {
            jazz_engine_CurrentRowPublicationField_clone(&f, found);
        } else {
            memset(&f, 0, sizeof f);
            f.tag = JZ_CRPF_RESULT_FIELD;
            f.name = jz_str_clone(&fields[i].name);
            f.visibility = JZ_CRRV_HIDDEN_METADATA;
        }
        JZ_VEC_PUSH(*pf, f);
    }
    if (row->publication_fields) jz_rc_release(row->publication_fields);
    row->publication_fields = pf;
    return JZ_OK;
}

jz_err MAT(NodeState__bind_current_row_columns_in_schema)(const jazz_node_NodeState *self, jazz_types_SchemaVersionId schema_version, jazz_node_CurrentRow *row)
{
    const jazz_engine_PublicationFieldVec *pf = row->publication_fields;
    bool any = false;
    if (pf) JZ_VEC_FOR(const jazz_engine_CurrentRowPublicationField, f, *pf)
        if (f->tag == JZ_CRPF_UNRESOLVED_SOURCE_CELL) { any = true; break; }
    if (!any) return JZ_OK;
    const jazz_node_SchemaPhysicalMapping *mapping = jz_btree_get(&self->catalogue.physical_mappings, &schema_version);
    const jazz_node_PhysicalTableMapping *table = mapping ? jz_btree_get(&mapping->tables, jazz_node__node__CurrentRow__table(row)) : NULL;
    if (!table) return JZN_INVALID_STORED("native row has no read-schema physical mapping");
    jazz_engine_PublicationFieldVec *fields = jz_rc_make_mut(&row->publication_fields, sizeof(jazz_engine_PublicationFieldVec), jazz_engine_PublicationFieldVec_clone);
    JZ_VEC_FOR(jazz_engine_CurrentRowPublicationField, field, *fields) {
        if (field->tag != JZ_CRPF_UNRESOLVED_SOURCE_CELL) continue;
        const jazz_types_PhysicalColumnId *id = jz_btree_get(&table->columns, &field->name);
        if (!id) return JZN_INVALID_STORED("source application cell has no catalogue column identity");
        field->tag = JZ_CRPF_STORED_COLUMN;
        field->id = *id;
        /* output_name: kept in `name` */
    }
    return JZ_OK;
}

jz_err MAT(NodeState__finish_engine_query_rows_in_schema)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version, jazz_node_CurrentRowVec *rows)
{
    JZ_VEC_FOR(jazz_node_CurrentRow, row, *rows)
        JZ_TRY(MAT(NodeState__bind_current_row_columns_in_schema)(self, schema_version, row));
    if (query->aggregate) {
        JZ_TRY(MAT(NodeState__apply_query_order_in_schema)(self, query, schema_version, rows->ptr, rows->len));
        jazz_node__node__apply_query_window(query, rows);
        return JZ_OK;
    }
    if (query->relation) {
        /* Relation terminals have already applied source-bound ordering. Their
         * public rows contain aliases only, so re-reading order keys from the
         * root table would compare `None` values and replace the engine's
         * order. */
        return JZ_OK;
    }
    /* Groove lowering owns membership/windowing, but one-shot APIs still
     * return a deterministic Vec. Re-apply ordering to the selected rows
     * without re-applying pagination. */
    return MAT(NodeState__apply_query_order_in_schema)(self, query, schema_version, rows->ptr, rows->len);
}

jz_err MAT(NodeState__query_output_table)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version, jazz_model_TableSchema *out)
{
    jazz_model_TableSchema source_table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema(self, &query->table, schema_version, &source_table));
    if (!query->aggregate) { *out = source_table; return JZ_OK; }
    jz_err e = jazz_node__node__query_eval__query_result_rows__aggregate_result_table(query, &source_table, out);
    jazz_model_TableSchema_drop(&source_table);
    return e;
}

typedef struct { jazz_node_CurrentRow row; jazz_types_OutputOccurrenceId occurrence; } Paired;
typedef struct { const jazz_model_Query *query; const jazz_model_TableSchema *table; } OrderCtx;

static int ordered_rows_cmp(const jazz_node_CurrentRow *left, const jazz_node_CurrentRow *right, const OrderCtx *c)
{
    JZ_VEC_FOR(const jazz_model_OrderBy, order, c->query->order_by) {
        groove_Value ls, rs;
        const groove_Value *lv = jazz_node__node__query_order_value(left, c->table, &order->column, &ls);
        const groove_Value *rv = jazz_node__node__query_order_value(right, c->table, &order->column, &rs);
        int ordering = jazz_node__node__query_eval__query_result_rows__compare_optional_values(lv, rv);
        if (order->direction == JZ_ORDER_DESC) ordering = -ordering;
        if (ordering != 0) return ordering;
    }
    return 0;
}

static int paired_default_cmp(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    const Paired *l = a, *r = b;
    int c = jazz_node__node__default_query_row_order(&l->row, &r->row);
    return c ? c : jazz_types_OutputOccurrenceId_cmp(&l->occurrence, &r->occurrence, NULL);
}
static int paired_tie_break(const Paired *l, const Paired *r)
{
    jazz_types_RowUuid lu = jazz_node__node__CurrentRow__row_uuid(&l->row), ru = jazz_node__node__CurrentRow__row_uuid(&r->row);
    int c = jz_uuid_cmp(&lu, &ru);
    if (c) return c;
    c = jazz_node__node__record_raw_cmp(&l->row, &r->row);
    if (c) return c;
    return jazz_types_OutputOccurrenceId_cmp(&l->occurrence, &r->occurrence, NULL);
}
static int paired_ordered_cmp(const void *a, const void *b, void *ctx)
{
    const Paired *l = a, *r = b;
    int c = ordered_rows_cmp(&l->row, &r->row, ctx);
    return c ? c : paired_tie_break(l, r);
}
static const jazz_node_CurrentRow *paired_row(const void *item, void *ctx) { (void)ctx; return &((const Paired *)item)->row; }
static int paired_agg_tie_break(const void *a, const void *b, void *ctx) { (void)ctx; return paired_tie_break(a, b); }

jz_err MAT(NodeState__sort_query_rows_with_occurrences)(const jazz_model_Query *query, const jazz_model_TableSchema *table, jazz_node_CurrentRowVec *rows, jazz_types_OutputOccurrenceIdVec *occurrence_ids)
{
    if (rows->len != occurrence_ids->len)
        return JZN_INVALID_STORED("maintained root occurrence sidecar length does not match rows");
    JZ_VEC(Paired) paired = {0};
    jz_vec_reserve(&paired, rows->len, sizeof(Paired));
    for (u32 i = 0; i < rows->len; i++) {
        Paired p = {rows->ptr[i], occurrence_ids->ptr[i]};
        JZ_VEC_PUSH(paired, p);
    }
    rows->len = 0;
    occurrence_ids->len = 0;
    jz_err e = JZ_OK;
    if (query->order_by.len == 0) {
        jz_sort(paired.ptr, paired.len, sizeof(Paired), paired_default_cmp, NULL);
    } else if (query->aggregate) {
        jazz_node_AggregateSortVt vt = {paired_row, paired_agg_tie_break, NULL};
        e = jazz_node__node__query_eval__query_result_rows__sort_aggregate_rows(query, paired.ptr, paired.len, sizeof(Paired), &vt);
    } else {
        if (!table) {
            e = JZN_INVALID_STORED("ordered maintained rows are missing their table schema");
        } else {
            OrderCtx c = {query, table};
            jz_sort(paired.ptr, paired.len, sizeof(Paired), paired_ordered_cmp, &c);
        }
    }
    /* `paired` was drained out of rows/occurrence_ids; push back (on error the
     * Rust vecs are left drained and `paired` is dropped). */
    JZ_VEC_FOR(Paired, p, paired) {
        if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(&p->row); jazz_types_OutputOccurrenceId_drop(&p->occurrence); continue; }
        JZ_VEC_PUSH(*rows, p->row);
        JZ_VEC_PUSH(*occurrence_ids, p->occurrence);
    }
    jz_vec_drop(&paired, sizeof(Paired), NULL);
    return e;
}

/* Aggregate terminals produce every group so their receiver can maintain it
 * incrementally. Presentation ordering and pagination therefore happen while
 * materializing the local result, after the aggregate rows have been paired
 * with their opaque occurrence identities. */
jz_err MAT(NodeState__apply_aggregate_window_with_occurrences)(const jazz_model_Query *query, jazz_node_CurrentRowVec *rows, jazz_types_OutputOccurrenceIdVec *occurrence_ids)
{
    if (rows->len != occurrence_ids->len)
        return JZN_INVALID_STORED("maintained aggregate occurrence sidecar length does not match rows");
    usize len = rows->len;
    usize offset = query->offset < len ? query->offset : len;
    usize limit = query->has_limit ? query->limit : (len > offset ? len - offset : 0);
    usize end = offset + limit;
    if (end < offset) end = SIZE_MAX;
    if (end > len) end = len;
    if (offset > 0 || end < len) {
        jz_vec_truncate(rows, (u32)end, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
        jz_vec_truncate(occurrence_ids, (u32)end, sizeof(jazz_types_OutputOccurrenceId), jazz_types_OutputOccurrenceId_drop);
        for (usize i = 0; i < offset; i++) {
            jazz_node_CurrentRow r;
            jazz_types_OutputOccurrenceId o;
            jz_vec_remove(rows, 0, &r, sizeof r);
            jz_vec_remove(occurrence_ids, 0, &o, sizeof o);
            jazz_node_CurrentRow_drop(&r);
            jazz_types_OutputOccurrenceId_drop(&o);
        }
    }
    return JZ_OK;
}

static const jazz_node_CurrentRow *plain_row(const void *item, void *ctx) { (void)ctx; return item; }
static int row_uuid_tie_break(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    jazz_types_RowUuid l = jazz_node__node__CurrentRow__row_uuid(a), r = jazz_node__node__CurrentRow__row_uuid(b);
    return jz_uuid_cmp(&l, &r);
}
static int rows_ordered_cmp(const void *a, const void *b, void *ctx)
{
    int c = ordered_rows_cmp(a, b, ctx);
    return c ? c : row_uuid_tie_break(a, b, NULL);
}

jz_err MAT(NodeState__apply_query_order_in_schema)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version, jazz_node_CurrentRow *rows, u32 n)
{
    if (query->order_by.len == 0) {
        jazz_node__node__sort_query_default_rows(rows, n);
        return JZ_OK;
    }
    jazz_node__node__sort_current_rows(rows, n);
    if (query->aggregate) {
        jazz_node_AggregateSortVt vt = {plain_row, row_uuid_tie_break, NULL};
        return jazz_node__node__query_eval__query_result_rows__sort_aggregate_rows(query, rows, n, sizeof(jazz_node_CurrentRow), &vt);
    }
    const jazz_model_TableSchema *table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, &query->table, schema_version, &table));
    OrderCtx c = {query, table};
    jz_sort(rows, n, sizeof(jazz_node_CurrentRow), rows_ordered_cmp, &c);
    return JZ_OK;
}

jz_err MAT(NodeState__apply_projection_in_schema)(const jazz_node_NodeState *self, const jazz_model_Query *query, jazz_types_SchemaVersionId schema_version, jazz_node_CurrentRow *rows, u32 n)
{
    if (!query->has_select) return JZ_OK;
    const jazz_model_TableSchema *table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, &query->table, schema_version, &table));
    for (u32 i = 0; i < n; i++) {
        jazz_node_CurrentRow p;
        JZ_TRY(jazz_node__node__CurrentRow__project(&rows[i], table, query->select.ptr, query->select.len, &p));
        jazz_node_CurrentRow_drop(&rows[i]);
        rows[i] = p;
    }
    return JZ_OK;
}

/* Required cells belong to the query's validated schema view, which may
 * differ from the owner's current write schema. */
jz_err MAT(NodeState__relation_snapshot_has_materialized_required_cells)(const jazz_node_NodeState *self, const jazz_model_ValidatedQuery *shape, const jazz_node_RelationSnapshot *snapshot, bool *out)
{
    const jazz_model_Query *query = &shape->query;
    if (query->aggregate || query->flat_join) { *out = true; return JZ_OK; }
    bool has_projection = false;
    jazz_model_RelationProjectColumnVec relation_projection = {0};
    if (query->relation)
        JZ_TRY(jazz_model__query__relation_output_projection_if_present(query->relation, &has_projection, &relation_projection));
    jz_err e = JZ_OK;
    *out = true;
    for (usize index = 0; index < snapshot->rows.len; index++) {
        const jazz_node_CurrentRow *row = &snapshot->rows.ptr[index];
        bool is_root = index < snapshot->root_count && jz_str_eq(jazz_node__node__CurrentRow__table(row), &query->table);
        if (is_root && has_projection) {
            bool all = true;
            JZ_VEC_FOR(jazz_model_RelationProjectColumn, column, relation_projection)
                if (!jazz_node__node__CurrentRow__raw_field(row, &column->alias)) { all = false; break; }
            /* A projected nullable value is materialized when its field is
             * present, including an explicit `Nullable(None)` value.
             * Supporting rows from joined sources do not carry the relation
             * aliases and are checked against their own schema. */
            if (all) continue;
        }
        const jazz_model_TableSchema *table;
        e = jazz_node__node__NodeState__table_in_schema_ref(self, jazz_node__node__CurrentRow__table(row), shape->schema_version, &table);
        if (JZ_IS_ERR(e)) break;
        const jz_str_vec *projection = (is_root && query->has_select) ? &query->select : NULL;
        if (!jazz_node__node__current_row_has_required_subscription_cells(row, table, projection)) { *out = false; break; }
    }
    if (has_projection) jazz_model_RelationProjectColumnVec_drop(&relation_projection);
    return e;
}

static void cache_drop(LocalMaintainedMaterializationCache *c) { jz_btree_drop(&c->tx_versions); }

jz_err MAT(NodeState__materialize_local_maintained_relation_snapshot_with_occurrences)(jazz_node_NodeState *self, const Local *local, jazz_node_LocalMaintainedRelationSnapshot *out)
{
    memset(out, 0, sizeof *out);
    jz_err e = JZ_OK;
    if (jazz_node__node__MaintainedTerminalSchemas__has_root_collector(local->terminal_schemas)) {
        jazz_node_TerminalKeyRecordVec terminal_rows;
        JZ_TRY(jazz_node__node__maintained_subscription_view__MaintainedSubscriptionView__structured_app_rows_by_terminal_key(local->maintained, &terminal_rows));
        const jazz_node_TerminalRootLayout *layout = MV(LocalMaintainedViewSubscription__terminal_root_layout)(local);
        u32 i = 0;
        for (; i < terminal_rows.len; i++) {
            jazz_node_TerminalKeyRecord *tr = JZ_VEC_AT(terminal_rows, i);
            jazz_types_OutputOccurrenceId occ;
            jz_err oe = jazz_node__node__terminal_root__terminal_root_occurrence_id_with_root_union(&tr->terminal_key, layout && layout->root_union_arm, &occ);
            if (JZ_IS_ERR(oe)) {
                jz_err_drop(&oe);
                e = JZN_INVALID_STORED("collector terminal key cannot identify its root occurrence");
                break;
            }
            if (!layout) {
                jazz_types_OutputOccurrenceId_drop(&occ);
                e = JZN_INVALID_STORED("collector terminal has no root layout");
                break;
            }
            jazz_node_CurrentRow row = jazz_node__node__CurrentRow__new_with_publication_fields(
                &local->result_table, tr->record, jazz_node__node__terminal_root__terminal_root_publication_fields(layout));
            jz_bytes_drop(&tr->terminal_key);
            JZ_VEC_PUSH(out->root_occurrence_ids, occ);
            JZ_VEC_PUSH(out->snapshot.rows, row);
        }
        for (u32 j = JZ_IS_ERR(e) ? i : terminal_rows.len; j < terminal_rows.len; j++) {
            jz_bytes_drop(&terminal_rows.ptr[j].terminal_key);
            groove_OwnedRecord_drop(&terminal_rows.ptr[j].record);
        }
        jz_vec_drop(&terminal_rows, sizeof(jazz_node_TerminalKeyRecord), NULL);
        if (JZ_IS_ERR(e)) goto fail;
        out->snapshot.root_count = out->snapshot.rows.len;
        return JZ_OK;
    }
    LocalMaintainedMaterializationCache cache;
    JZ_TRY(MAT(NodeState__preload_local_maintained_materialization_cache)(self, local, &cache));
    jazz_node_CurrentRowVec rows = {0};
    jazz_types_OutputOccurrenceIdVec root_occurrence_ids = {0};
    jz_vec_reserve(&rows, local->result_set.len, sizeof(jazz_node_CurrentRow));
    jz_vec_reserve(&root_occurrence_ids, local->result_set.len, sizeof(jazz_types_OutputOccurrenceId));
    jz_btree row_keys;
    jz_btree_init(&row_keys, &jazz_node_TABLE_ROW_SET_DESC);
    bool agg = local->result_query.aggregate != NULL;
    const jazz_node_ResultMemberEntry *member;
    const void *mv;
    JZ_BTREE_FOR(&local->result_set, member, mv) {
        (void)mv;
        bool has;
        jazz_node_CurrentRow row;
        e = MAT(NodeState__materialize_local_maintained_view_result_member_with_cache)(self, local, member, &cache, &has, &row);
        if (JZ_IS_ERR(e)) goto fail_rows;
        if (!has) continue;
        bool has_occ;
        jazz_types_OutputOccurrenceId occurrence_id;
        e = jazz_node__node__query_eval__public_result_member_occurrence_id(member, &local->result_table, agg, &has_occ, &occurrence_id);
        if (!JZ_IS_ERR(e) && !has_occ) e = JZN_INVALID_STORED("maintained root member has no occurrence identity");
        if (JZ_IS_ERR(e)) { jazz_node_CurrentRow_drop(&row); goto fail_rows; }
        jazz_node_TableRowKey k = {jz_str_clone(jazz_node__node__CurrentRow__table(&row)), jazz_node__node__CurrentRow__row_uuid(&row)};
        if (jz_btree_contains(&row_keys, &k)) jz_str_drop(&k.table);
        else jz_btree_insert(&row_keys, &k, NULL, NULL);
        JZ_VEC_PUSH(rows, row);
        JZ_VEC_PUSH(root_occurrence_ids, occurrence_id);
    }
    /* `result_set` is keyed by member identity, so its BTreeSet iteration
     * order cannot be exposed as a query reset order. Do not re-window: the
     * maintained program already chose this result set. Materialize full rows
     * first, because an order key may not be in the public projection. */
    e = jazz_node__node__query_eval__NodeState__apply_query_order_with_occurrences(self, &local->result_query, &rows, &root_occurrence_ids);
    if (JZ_IS_ERR(e)) goto fail_rows;
    if (agg) {
        e = MAT(NodeState__apply_aggregate_window_with_occurrences)(&local->result_query, &rows, &root_occurrence_ids);
        if (JZ_IS_ERR(e)) goto fail_rows;
    }
    if (local->has_result_relation_projections || local->has_result_relation_projection) {
        const jazz_model_TableSchema *tref;
        e = jazz_node__node__NodeState__table(self, &local->result_table, &tref);
        if (JZ_IS_ERR(e)) goto fail_rows;
        jazz_model_TableSchema table;
        jazz_model_TableSchema_clone(&table, tref);
        const jazz_engine_AppRowSchema *schema;
        e = jazz_node__node__MaintainedTerminalSchemas__direct_app_row_schema(local->terminal_schemas, &schema);
        for (u32 i = 0; !JZ_IS_ERR(e) && i < rows.len; i++) {
            const jazz_model_RelationProjectColumnVec *columns;
            if (local->has_result_relation_projections) {
                e = MV(LocalMaintainedViewSubscription__relation_projection_for_occurrence)(local, &root_occurrence_ids.ptr[i], &columns);
                if (JZ_IS_ERR(e)) break;
                if (!columns) continue;
            } else {
                columns = &local->result_relation_projection;
            }
            jazz_node_CurrentRow p;
            e = jazz_node__node__CurrentRow__project_relation(&rows.ptr[i], &table, columns, &p);
            if (JZ_IS_ERR(e)) break;
            jazz_node_CurrentRow_drop(&rows.ptr[i]);
            rows.ptr[i] = p;
            e = MAT(NodeState__bind_app_row_schema_fields)(&rows.ptr[i], schema);
        }
        jazz_model_TableSchema_drop(&table);
        if (JZ_IS_ERR(e)) goto fail_rows;
    }
    e = jazz_node__node__query_eval__NodeState__apply_projection(self, &local->result_query, &rows);
    if (JZ_IS_ERR(e)) goto fail_rows;
    usize root_count = rows.len;
    jazz_node_RelationEdgeVec edges = {0};
    jz_vec_reserve(&edges, local->program_facts.len, sizeof(jazz_node_RelationEdge));
    const jazz_protocol_ProgramFactEntry *fact;
    JZ_BTREE_FOR(&local->program_facts, fact, mv) {
        (void)mv;
        if (fact->tag != JZ_PFE_RELATION_EDGE) continue;
        const jazz_protocol_RelationEdgeEntry *edge = &fact->relation_edge;
        jazz_node_RelationEdge read_edge;
        e = MAT(NodeState__project_relation_edge_through_read_schema)(self, edge, local->result_schema_version, &read_edge);
        if (JZ_IS_ERR(e)) break;
        jazz_node_TableRowKey k = {jz_str_clone(&read_edge.target_table), read_edge.target_row};
        bool fresh = !jz_btree_contains(&row_keys, &k);
        if (fresh) jz_btree_insert(&row_keys, &k, NULL, NULL); else jz_str_drop(&k.table);
        if (fresh && edge->has_target_version) {
            bool has;
            jazz_node_CurrentRow row;
            e = MAT(NodeState__materialize_local_maintained_view_relation_edge_row_with_cache)(self, local, edge->target_table, edge->target_row, edge->target_version.tx, &cache, &has, &row);
            if (JZ_IS_ERR(e)) { jazz_node_RelationEdge_drop(&read_edge); break; }
            if (has) JZ_VEC_PUSH(rows, row);
        }
        JZ_VEC_PUSH(edges, read_edge);
    }
    if (JZ_IS_ERR(e)) { jz_vec_drop(&edges, sizeof(jazz_node_RelationEdge), jazz_node_RelationEdge_drop); goto fail_rows; }
    out->snapshot.root_count = root_count;
    out->snapshot.rows = rows;
    out->snapshot.edges = edges;
    out->root_occurrence_ids = root_occurrence_ids;
    jz_btree_drop(&row_keys);
    cache_drop(&cache);
    return JZ_OK;
fail_rows:
    jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node_CurrentRow_drop);
    jz_vec_drop(&root_occurrence_ids, sizeof(jazz_types_OutputOccurrenceId), jazz_types_OutputOccurrenceId_drop);
    jz_btree_drop(&row_keys);
    cache_drop(&cache);
    return e;
fail:
    jazz_node_RelationSnapshot_drop(&out->snapshot);
    jz_vec_drop(&out->root_occurrence_ids, sizeof(jazz_types_OutputOccurrenceId), jazz_types_OutputOccurrenceId_drop);
    return e;
}
