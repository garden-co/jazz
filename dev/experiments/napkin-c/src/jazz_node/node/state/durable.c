/* Port of crates/jazz/layers/node/src/node/state/durable.rs
 * (include!d into node/mod.rs, so the Rust module is jazz_node::node).
 * Skipped (#[cfg(any(test, feature = "testing"))] / feature = "testing" only):
 * set_catalogue_activation_failpoint, mutate_current_schema_for_testing,
 * invalidate_groove_runtime_for_test, rebuild_groove_runtime_for_test,
 * runtime_stats_for_test, history_class_bytes_for_test,
 * encoded_storage_bytes_for_test, and the delivery_diagnostics record in
 * invalidate_subscription_scopes. */
#include "jz_rt.h"
#include "../../_ext_node-qe-b.h"
#include "../node_qe_b.h"

#define NS jazz_node_NodeState

/* Return local synchronization counters. */
const jazz_node_SyncMetrics *jazz_node__node__NodeState__sync_metrics(const NS *self) { return &self->sync_metrics; }
void jazz_node__node__NodeState__record_dropped_peer_request(NS *self) { self->sync_metrics.dropped_peer_request_messages += 1; }
void jazz_node__node__NodeState__record_transport_backpressure_retry(NS *self) { self->sync_metrics.transport_backpressure_retries += 1; }
void jazz_node__node__NodeState__record_authoritative_reset_missing_payload_fallback(NS *self) { self->sync_metrics.authoritative_reset_missing_payload_fallbacks += 1; }
void jazz_node__node__NodeState__record_peer_payload_inventory_missing_fallback(NS *self) { self->sync_metrics.peer_payload_inventory_missing_fallbacks += 1; }

/* Deterministic counters for query-engine read authorization paths. */
const void *jazz_node__node__NodeState__query_engine_read_metrics(const NS *self) { return self->query_engine_read_metrics; }
/* Reset query-engine read authorization counters. */
void jazz_node__node__NodeState__reset_query_engine_read_metrics(NS *self) { jazz_node__node__QueryEngineReadMetrics__reset_default(self->query_engine_read_metrics); }

/* Published schema-version payloads known to this node. */
const jz_btree *jazz_node__node__NodeState__catalogue_schemas(const NS *self) { return &self->catalogue.catalogue_schemas; }

const jazz_model_JazzSchema *jazz_node__node__NodeState__schema_with_active_permissions(const NS *self, jazz_types_SchemaVersionId id)
{
    if (jz_uuid_eq(&id, &self->catalogue.active_schema.schema)) return &self->catalogue.active_schema.compiled;
    const jazz_protocol_SchemaVersion *schema = jz_btree_get(&self->catalogue.catalogue_schemas, &id);
    return schema ? &schema->schema : NULL;
}

/* Highest contiguously activated authoritative catalogue position. */
u64 jazz_node__node__NodeState__active_catalogue_seq(const NS *self) { return self->catalogue.active_catalogue_seq; }

/* Published migration lenses known to this node. */
const jz_btree *jazz_node__node__NodeState__catalogue_lenses(const NS *self) { return &self->catalogue.catalogue_lenses; }

/* Current dynamic-catalogue bootstrap state. */
u32 jazz_node__node__NodeState__catalogue_bootstrap_state(const NS *self) { return self->catalogue_bootstrap_state; }

/* Return the authoritative current-write pointer, or fail closed before the
 * node has adopted its first trusted catalogue snapshot. */
jz_err jazz_node__node__NodeState__try_current_write_schema(const NS *self, jazz_protocol_CurrentWriteSchema *out)
{
    JZ_TRY(jazz_node__node__NodeState__require_catalogue_ready(self));
    *out = jazz_node__node__ActiveSchema__wire_pointer(&self->catalogue.active_schema);
    return JZ_OK;
}

/* Return the active read-schema only after an authority catalogue has been
 * durably adopted. */
jz_err jazz_node__node__NodeState__try_current_schema(const NS *self, const jazz_model_JazzSchema **out)
{
    JZ_TRY(jazz_node__node__NodeState__require_catalogue_ready(self));
    *out = &self->catalogue.schema;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__require_catalogue_ready(const NS *self)
{
    jz_err e = groove__db__facade__Database__ensure_usable(self->database);
    if (e.code) return jazz_node__node__Error_groove(e);
    if (self->catalogue_bootstrap_state == JZN_CBS_UNINITIALIZED) return JZN_CATALOGUE_UNINITIALIZED();
    return JZ_OK;
}

/* Current write-schema pointer known to this node. An uninitialized
 * dynamic-catalogue node has no current application schema; the temporary
 * system-only layout must not leak through this API. */
jz_err jazz_node__node__NodeState__current_write_schema(const NS *self, jazz_protocol_CurrentWriteSchema *out)
{
    return jazz_node__node__NodeState__try_current_write_schema(self, out);
}

typedef struct LineageEntry { u64 catalogue_seq; jazz_protocol_SchemaLineagePublication publication; } LineageEntry;
static int lineage_seq_cmp(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    u64 x = ((const LineageEntry *)a)->catalogue_seq, y = ((const LineageEntry *)b)->catalogue_seq;
    return (x > y) - (x < y);
}

jz_err jazz_node__node__NodeState__catalogue_snapshot(const NS *self, jazz_protocol_CatalogueSnapshot *out)
{
    JZ_TRY(jazz_node__node__NodeState__require_catalogue_ready(self));
    JZ_VEC(jazz_protocol_SchemaVersion) schemas = {0};
    {
        const jazz_types_SchemaVersionId *id; const jazz_protocol_SchemaVersion *schema;
        JZ_BTREE_FOR(&self->catalogue.catalogue_schemas, id, schema) {
            jazz_protocol_SchemaVersion copy; jazz_protocol_SchemaVersion_clone(&copy, schema);
            JZ_VEC_PUSH(schemas, copy);
        }
    }
    JZ_VEC_FOR(jazz_protocol_SchemaVersion, schema, schemas) {
        if (jz_uuid_eq(&schema->id, &self->catalogue.active_schema.schema)) {
            jazz_model_JazzSchema_drop(&schema->schema);
            jazz_model_JazzSchema_clone(&schema->schema, &self->catalogue.active_schema.compiled);
            break;
        }
    }
    jz_sort(schemas.ptr, schemas.len, sizeof(jazz_protocol_SchemaVersion), jazz_protocol_SchemaVersion_id_cmp, NULL);
    JZ_VEC(LineageEntry) lineages = {0};
    {
        const jazz_types_SchemaVersionId *target; const jazz_node_StagedSchemaLineage *lineage;
        JZ_BTREE_FOR(&self->catalogue.active_lineages_by_target, target, lineage) {
            LineageEntry entry = { lineage->catalogue_seq, {0} };
            jazz_protocol_SchemaLineagePublication_clone(&entry.publication, &lineage->publication);
            JZ_VEC_PUSH(lineages, entry);
        }
    }
    jz_sort(lineages.ptr, lineages.len, sizeof(LineageEntry), lineage_seq_cmp, NULL);
    /* The write pointer is deliberately independent of the authority's unique
     * genesis. Once a lineage is active it normally points at a descendant,
     * whose manifest must never be re-labelled as genesis in a snapshot (doing
     * so lets a receiver allocate a different physical root for the real
     * genesis schema). active_lineages_by_target is keyed by target, so its
     * key set is the lineage-target set. */
    const jazz_types_SchemaVersionId *genesis = NULL;
    u32 genesis_count = 0;
    {
        const jazz_types_SchemaVersionId *id; const jazz_protocol_SchemaVersion *schema;
        JZ_BTREE_FOR(&self->catalogue.catalogue_schemas, id, schema) {
            if (jz_btree_contains(&self->catalogue.active_lineages_by_target, id)) continue;
            if (!genesis) genesis = id;
            if (++genesis_count > 1) break;
        }
    }
    jz_err e = JZ_OK;
    if (!genesis) e = JZN_INVALID_STORED("catalogue genesis schema is missing");
    else if (genesis_count > 1) e = JZN_INVALID_STORED("catalogue has multiple genesis schemas");
    const jazz_node_SchemaPhysicalMapping *mapping = NULL;
    if (!e.code) {
        mapping = jz_btree_get(&self->catalogue.physical_mappings, genesis);
        if (!mapping) e = JZN_INVALID_STORED("genesis physical mapping missing");
    }
    if (e.code) {
        for (u32 i = 0; i < schemas.len; ++i) jazz_protocol_SchemaVersion_drop(&schemas.ptr[i]);
        jz_free(schemas.ptr);
        for (u32 i = 0; i < lineages.len; ++i) jazz_protocol_SchemaLineagePublication_drop(&lineages.ptr[i].publication);
        jz_free(lineages.ptr);
        return e;
    }
    jazz_protocol_PhysicalIdentityManifest_clone(&out->genesis_physical_identities, &mapping->identities);
    out->schemas.ptr = schemas.ptr; out->schemas.len = schemas.len; out->schemas.cap = schemas.cap;
    out->lineages.ptr = lineages.ptr; out->lineages.len = lineages.len; out->lineages.cap = lineages.cap;
    out->current_write_schema = jazz_node__node__ActiveSchema__wire_pointer(&self->catalogue.active_schema);
    return JZ_OK;
}

/* Return a historical read handle at an exact global settle position. */
jazz_node_HistoricalRead jazz_node__node__NodeState__at(NS *self, jazz_types_GlobalTime position)
{
    return (jazz_node_HistoricalRead){ self, position };
}

/* Return a historical read handle for the latest settle position whose
 * transaction time is less than or equal to `time`. This is deterministic,
 * not a wall-clock truth claim. (block_on: async is synchronous here.) */
jz_err jazz_node__node__NodeState__at_time(NS *self, jazz_types_TxTime time, jazz_node_HistoricalRead *out)
{
    jazz_types_GlobalTime position;
    JZ_TRY(jazz_node__node__query_eval__NodeState__resolve_time_travel_position(self, time, &position));
    *out = jazz_node__node__NodeState__at(self, position);
    return JZ_OK;
}

/* v1 is conservative: authorities/history-complete nodes can answer cuts up
 * to their contiguous applied watermark; partial clients return false. */
bool jazz_node__node__NodeState__is_history_complete_for(const NS *self, const jazz_model_ValidatedQuery *shape, jazz_types_GlobalTime position)
{
    (void)shape;
    return self->history_complete && position <= self->clock.committed_global_time;
}

/* Whether this node was opened as a complete serving authority. */
bool jazz_node__node__NodeState__is_history_complete(const NS *self) { return self->history_complete; }

/* Return current rows for a subscription at the requested tier. */
jz_err jazz_node__node__NodeState__subscription_current_rows(NS *self, const jz_str *table, jazz_model_DurabilityTier settled, jazz_node_CurrentRowVec *out)
{
    switch (settled) {
    case JZ_DURABILITY_NONE:
    case JZ_DURABILITY_LOCAL:
        return jazz_node__node__NodeState__current_rows(self, table, settled, out);
    default: {
        /* This convenience surface is used by topology tests, but it must
         * exercise the same local Groove terminal as a serving subscription.
         * Reading the authority result cache here would silently preserve the
         * retired result-output bypass. (Edge is matched by Global here: the
         * Rust match covers None | Local | Global.) */
        jazz_model_Query query = jazz_model__query__Query__from_table(table);
        jazz_model_ValidatedQuery shape;
        jz_err e = jazz_model__query__Query__validate(&query, &self->catalogue.schema, &shape);
        jazz_model_Query_drop(&query);
        if (e.code) return jazz_node__node__Error_query(e);
        jz_btree params; jz_btree_init(&params, &jazz_node_STRING_VALUE_MAP_DESC);
        jazz_model_Binding binding;
        e = jazz_model__query__ValidatedQuery__bind(&shape, params, &binding);
        if (e.code) { jazz_model_ValidatedQuery_drop(&shape); return jazz_node__node__Error_query(e); }
        if (jazz_node__node__NodeState__is_history_complete(self)) {
            e = jazz_node__node__query_eval__NodeState__query_rows_with_prepared_plan_for_identity(self, &shape, &binding,
                JZ_DURABILITY_GLOBAL, NULL, jazz_types_AuthorSubject_SYSTEM, out);
        } else {
            /* A partial client has no authority to reopen this query as
             * SYSTEM. It consumes the exact scoped CoveredInput closure
             * installed by the received whole-table subscription, through the
             * same receiver-local graph as every other client read. */
            e = jazz_node__node__query_eval__NodeState__query_rows_for_client(self, &shape, &binding, JZ_DURABILITY_GLOBAL, jazz_types_AuthorSubject_SYSTEM, out);
        }
        jazz_model_Binding_drop(&binding);
        jazz_model_ValidatedQuery_drop(&shape);
        return e;
    }
    }
}

/* Return the legacy transaction fate tuple by projecting stored status. */
bool jazz_node__node__NodeState__transaction_state(NS *self, jazz_model_TxId tx_id, jazz_model_Fate *fate, bool *has_global_time, jazz_types_GlobalTime *global_time, jazz_model_DurabilityTier *durability)
{
    bool found = false;
    jazz_model_DurabilityTier stored_durability;
    jz_err e = jazz_node__node__currency__NodeState__query_transaction_state(self, tx_id, &found, fate, has_global_time, global_time, &stored_durability);
    if (e.code) { jz_err_drop(&e); return false; } /* .ok() */
    if (!found) return false;
    *durability = jz_btree_contains(&self->pending_persistence, &tx_id) ? JZ_DURABILITY_NONE : stored_durability;
    return true;
}

/* Return the durable audit record for a transaction, including rejected
 * transactions whose row versions were removed from history. */
bool jazz_node__node__NodeState__transaction_record(NS *self, jazz_model_TxId tx_id, jazz_node_TransactionRecord *out)
{
    bool found = false;
    jazz_node_StoredTransaction stored;
    jz_err e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &stored);
    if (e.code) { jz_err_drop(&e); return false; }
    if (!found) return false;
    *out = jazz_node__node__codec__StoredTransaction__to_record(&stored);
    jazz_node_StoredTransaction_drop(&stored);
    return true;
}

/* Return locally originated transactions that still need upstream settlement. */
jz_err jazz_node__node__NodeState__pending_transaction_ids_for(NS *self, jazz_types_NodeUuid node, jazz_types_AuthorSubject author, jazz_model_TxIdVec *out)
{
    jazz_node_PendingTransactionScan scan;
    JZ_TRY(jazz_node__node__NodeState__pending_transaction_scan_for(self, node, author, &scan));
    *out = scan.tx_ids;
    return JZ_OK;
}

/* Return unsettled transactions by `author`, irrespective of their originating node. */
jz_err jazz_node__node__NodeState__pending_transaction_ids_for_author(NS *self, jazz_types_AuthorSubject author, jazz_model_TxIdVec *out)
{
    return jazz_node__node__NodeState__below_global_transaction_ids(self, true, author, false, out);
}

/* A Global synchronization barrier also needs the authority timestamp. */
jz_err jazz_node__node__NodeState__synchronizing_transaction_ids_for_author(NS *self, jazz_types_AuthorSubject author, jazz_model_TxIdVec *out)
{
    return jazz_node__node__NodeState__below_global_transaction_ids(self, true, author, true, out);
}

/* A trusted backend owns every author scope created by its node. Restrict
 * this scan by transaction origin, never by its SYSTEM display identity. */
jz_err jazz_node__node__NodeState__synchronizing_transaction_ids_for_node(NS *self, jazz_types_NodeUuid node, jazz_model_TxIdVec *out)
{
    jazz_model_TxIdVec all = {0};
    JZ_TRY(jazz_node__node__NodeState__below_global_transaction_ids(self, false, jazz_types_AuthorSubject_SYSTEM, true, &all));
    out->ptr = NULL; out->len = out->cap = 0;
    JZ_VEC_FOR(jazz_model_TxId, tx, all) if (jz_uuid_eq(&tx->node, &node)) JZ_VEC_PUSH(*out, *tx);
    jz_free(all.ptr);
    return JZ_OK;
}

static int tx_id_cmp(const void *a, const void *b, void *ctx) { return jazz_model_TxId_cmp(a, b, ctx); }
static void tx_ids_sort_dedup(jazz_model_TxIdVec *v)
{
    jz_sort_unstable(v->ptr, v->len, sizeof(jazz_model_TxId), tx_id_cmp, NULL);
    u32 w = 0;
    for (u32 i = 0; i < v->len; ++i)
        if (w == 0 || !jazz_model_TxId_eq(&v->ptr[w - 1], &v->ptr[i])) v->ptr[w++] = v->ptr[i];
    v->len = w;
}

typedef struct Candidate { jazz_types_NodeAlias alias; jazz_types_TxTime time; } Candidate;

/* null slice of `by_global_time` */
static jz_err scan_null_global_time(NS *self, groove_RawRowVec *raws)
{
    jz_str table = jz_str_from("jazz_transactions");
    groove_Value null_key = { .tag = GV_NULLABLE, .v.nullable = NULL };
    jz_err e = groove__db__primary_storage__Database__index_scan_raw(self->database, &table, "by_global_time", &null_key, 1, raws);
    jz_str_drop(&table);
    return e.code ? jazz_node__node__Error_groove(e) : JZ_OK;
}

static jz_err record_made_by(const groove_OwnedRecord *record, jazz_types_AuthorSubject *out)
{
    groove_OwnedRecord made_by_record;
    JZ_TRY(jazz_node__node__Error_record(groove__records__OwnedRecord__get_record(record, JZN_TX_ROW_FIELD_MADE_BY_IDX, &made_by_record)));
    jazz_types_RowAuthor author;
    jz_err e = jazz_types__ids__RowAuthor__from_record(&made_by_record, &author);
    if (e.code) { jz_err_drop(&e); return jazz_node__node__Error_non_canonical_record(); }
    *out = jazz_types__ids__RowAuthor__as_author_subject(&author);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__below_global_transaction_ids(NS *self, bool has_author, jazz_types_AuthorSubject author, bool include_missing_authority_timestamp, jazz_model_TxIdVec *out)
{
    JZ_VEC(Candidate) candidates = {0};
    groove_RawRowVec raws = {0};
    JZ_TRY(scan_null_global_time(self, &raws));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < raws.len; ++i) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        u8 fate, durability_tag; jazz_types_AuthorSubject made_by; jazz_model_DurabilityTier durability;
        if ((e = groove__records__OwnedRecord__get_enum(record, JZN_TX_ROW_FIELD_FATE_IDX, &fate)).code) { e = jazz_node__node__Error_record(e); break; }
        if ((e = record_made_by(record, &made_by)).code) break;
        if ((e = groove__records__OwnedRecord__get_enum(record, JZN_TX_ROW_FIELD_DURABILITY_IDX, &durability_tag)).code) { e = jazz_node__node__Error_record(e); break; }
        if ((e = jazz_node__node__codec__durability_from_discriminant(durability_tag, &durability)).code) break;
        if ((has_author && !jazz_node__node__durable_author_matches(author, made_by))
            || !(fate == 0 || fate == 1)
            || (!include_missing_authority_timestamp && durability >= JZ_DURABILITY_GLOBAL))
            continue;
        u64 node_id, time;
        if ((e = groove__records__OwnedRecord__get_u64(record, JZN_TX_ROW_FIELD_NODE_ID_IDX, &node_id)).code) { e = jazz_node__node__Error_record(e); break; }
        if ((e = groove__records__OwnedRecord__get_u64(record, JZN_TX_ROW_FIELD_TIME_IDX, &time)).code) { e = jazz_node__node__Error_record(e); break; }
        JZ_VEC_PUSH(candidates, ((Candidate){ node_id, time }));
    }
    groove_RawRowVec_drop(&raws);
    if (e.code) { jz_free(candidates.ptr); return e; }
    jazz_model_TxIdVec tx_ids = {0};
    jz_vec_reserve(&tx_ids, candidates.len, sizeof(jazz_model_TxId));
    JZ_VEC_FOR(Candidate, c, candidates) {
        bool has; jazz_types_NodeUuid node;
        e = jazz_node__node__NodeState__resolve_node_alias(self, c->alias, &has, &node);
        if (e.code) { jz_free(candidates.ptr); jz_free(tx_ids.ptr); return e; }
        if (!has) continue;
        JZ_VEC_PUSH(tx_ids, ((jazz_model_TxId){ c->time, node }));
    }
    jz_free(candidates.ptr);
    tx_ids_sort_dedup(&tx_ids);
    *out = tx_ids;
    return JZ_OK;
}

/* Find replayable local transactions in the null slice of `by_global_time`.
 * The sequence/durability invariant makes every below-Global transaction
 * sequence-null, so settled history is outside this scan without a second
 * index or an upgrade backfill. */
jz_err jazz_node__node__NodeState__pending_transaction_scan_for(NS *self, jazz_types_NodeUuid node, jazz_types_AuthorSubject author, jazz_node_PendingTransactionScan *out)
{
    memset(out, 0, sizeof *out);
    jazz_types_NodeAlias node_alias;
    if (!jazz_node__node__NodeAliases__get(self->node_aliases, &node, &node_alias)) return JZ_OK;
    /* `$madeBy` is durable provenance. A local system capability records its
     * specific node origin, while authority evaluation continues to use
     * `AuthorSubject::SYSTEM` separately. */
    jazz_types_AuthorSubject durable_author = jazz_types_AuthorSubject_eq(author, jazz_types_AuthorSubject_SYSTEM)
        ? jazz_types__ids__AuthorSubject__system_at(node) : author;
    groove_RawRowVec raws = {0};
    JZ_TRY(scan_null_global_time(self, &raws));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < raws.len; ++i) {
        out->records_visited += 1;
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        u64 alias; jazz_types_AuthorSubject made_by;
        if ((e = groove__records__OwnedRecord__get_u64(record, JZN_TX_ROW_FIELD_NODE_ID_IDX, &alias)).code) { e = jazz_node__node__Error_record(e); break; }
        if (alias != node_alias) continue;
        if ((e = record_made_by(record, &made_by)).code) break;
        if (!jazz_types_AuthorSubject_eq(made_by, durable_author)) continue;
        u8 fate, durability_tag; jazz_model_DurabilityTier durability;
        if ((e = groove__records__OwnedRecord__get_enum(record, JZN_TX_ROW_FIELD_FATE_IDX, &fate)).code) { e = jazz_node__node__Error_record(e); break; }
        if (!(fate == 0 || fate == 1)) continue;
        if ((e = groove__records__OwnedRecord__get_enum(record, JZN_TX_ROW_FIELD_DURABILITY_IDX, &durability_tag)).code) { e = jazz_node__node__Error_record(e); break; }
        if ((e = jazz_node__node__codec__durability_from_discriminant(durability_tag, &durability)).code) break;
        if (durability >= JZ_DURABILITY_GLOBAL) continue;
        u64 time;
        if ((e = groove__records__OwnedRecord__get_u64(record, JZN_TX_ROW_FIELD_TIME_IDX, &time)).code) { e = jazz_node__node__Error_record(e); break; }
        JZ_VEC_PUSH(out->tx_ids, ((jazz_model_TxId){ time, node }));
    }
    groove_RawRowVec_drop(&raws);
    if (e.code) { jz_free(out->tx_ids.ptr); memset(out, 0, sizeof *out); return e; }
    tx_ids_sort_dedup(&out->tx_ids);
    return JZ_OK;
}

/* Resolve creator/updater provenance for a projected current row. */
jz_err jazz_node__node__NodeState__row_provenance(NS *self, const jazz_node_CurrentRow *row, bool *has, jazz_model_RowProvenance *out)
{
    (void)self;
    return jazz_node__node__CurrentRow__provenance(row, has, out);
}

bool jazz_node__node__NodeState__current_row_tx_id(NS *self, const jazz_node_CurrentRow *row, jazz_model_TxId *out)
{
    jazz_types_TxTime time; jazz_types_NodeAlias alias;
    if (!jazz_node__node__CurrentRow__projected_tx_alias(row, &time, &alias)) return false;
    bool has; jazz_types_NodeUuid node;
    jz_err e = jazz_node__node__NodeState__resolve_node_alias(self, alias, &has, &node);
    if (e.code) { jz_err_drop(&e); return false; } /* .ok()?? */
    if (!has) return false;
    out->time = time; out->node = node;
    return true;
}

/* Discard authority proof during rebuild or eviction, preserving live receipt ordering. */
void jazz_node__node__NodeState__invalidate_subscription_scopes(NS *self)
{
    /* Rebuild/eviction invalidates authority proof, not the ordering of
     * receipts awaited by still-live foregrounds. Reusing generation one
     * after invalidation can strand a read already waiting for > one. Keep
     * only this process-local counter; no membership, settlement,
     * predecessor, compiled source, or pending publication survives. */
    jazz_protocol_AuthorityResultKey *key; jazz_node_AuthorityResultState *state;
    JZ_BTREE_FOR(&self->query.authority_results, key, state) {
        u64 generation = state->applied_view_update_generation;
        jazz_node_AuthorityResultState_drop_fields(state);
        jazz_node_AuthorityResultState_init(state);
        state->applied_view_update_generation = generation;
    }
    jz_btree_clear(&self->query.retained_root_window_sources);
    self->query.watermarks_invalidated = true;
}

jz_err jazz_node__node__NodeState__persist_policy_binding_directory(const NS *self, const jazz_protocol_PolicyBindingKey *policy)
{
    u8 digest[32];
    jazz_protocol__protocol__PolicyBindingKey__directory_digest(policy, digest);
    groove_Value claims;
    jz_err e = jazz_protocol__protocol__PolicyBindingKey__directory_value(policy, &claims);
    if (e.code) { jz_err_drop(&e); return JZN_INVALID_STORED("policy binding claims must encode"); }
    groove_DirectRecordStore store;
    e = groove__db__facade__Database__direct_record_store(self->database, jazz_model__schema__AUTHORITY_POLICY_BINDINGS_STORE, &store);
    if (e.code) { groove_Value_drop(&claims); return jazz_node__node__Error_groove(e); }
    groove_Value key = gv_bytes(jz_bytes_from(digest, 32));
    bool claims_moved = false;
    bool found = false; groove_OwnedRecord existing;
    e = groove__db__storage_helpers__DirectRecordStore__get(&store, &key, 1, &found, &existing);
    if (e.code) { e = jazz_node__node__Error_groove(e); goto done; }
    if (found) {
        groove_Value subject, value;
        e = groove__records__OwnedRecord__get_idx(&existing, 0, &subject);
        if (e.code) { e = jazz_node__node__Error_record(e); groove_OwnedRecord_drop(&existing); goto done; }
        if (subject.tag != GV_STRING) {
            groove_Value_drop(&subject); groove_OwnedRecord_drop(&existing);
            e = JZN_INVALID_STORED("policy binding directory subject must be string");
            goto done;
        }
        jazz_types_AuthorSubject identity;
        e = jazz_types__ids__AuthorSubject__from_canonical(jz_str_slice(&subject.v.s), &identity);
        groove_Value_drop(&subject);
        if (e.code) { jz_err_drop(&e); groove_OwnedRecord_drop(&existing); e = JZN_INVALID_STORED("policy binding directory subject is invalid"); goto done; }
        e = groove__records__OwnedRecord__get_idx(&existing, 1, &value);
        groove_OwnedRecord_drop(&existing);
        if (e.code) { e = jazz_node__node__Error_record(e); goto done; }
        jazz_protocol_PolicyBindingKey stored;
        e = jazz_protocol__protocol__PolicyBindingKey__from_directory_value(identity, value, &stored);
        if (e.code) { jz_err_drop(&e); e = JZN_INVALID_STORED("policy binding directory claims are invalid"); goto done; }
        bool same = jazz_protocol_PolicyBindingKey_eq(&stored, policy);
        jazz_protocol_PolicyBindingKey_drop(&stored);
        if (!same) e = JZN_INVALID_STORED("policy binding digest aliases a distinct exact policy identity");
        goto done;
    }
    {
        groove_Value values[2] = { gv_str(jazz_types__ids__AuthorSubject__canonical(&policy->identity)), claims };
        claims_moved = true;
        e = groove__db__storage_helpers__DirectRecordStore__set(&store, &key, 1, values, 2);
        groove_Value_drop(&values[0]); groove_Value_drop(&values[1]);
        if (e.code) e = jazz_node__node__Error_groove(e);
    }
done:
    if (!claims_moved) groove_Value_drop(&claims);
    groove_Value_drop(&key);
    groove_DirectRecordStore_drop(&store);
    return e;
}

jz_err jazz_node__node__NodeState__close(NS *self)
{
    jz_err e = groove__db__commit__Database__flush(self->database);
    if (e.code) return jazz_node__node__Error_groove(e);
    e = groove__db__facade__Database__close(self->database);
    if (e.code) return jazz_node__node__Error_groove(e);
    return JZ_OK;
}

/* Return locally-originated rejected transactions retained for retry. */
void jazz_node__node__NodeState__rejected_transactions(const NS *self, jazz_model_TxIdVec *out)
{
    out->ptr = NULL; out->len = out->cap = 0;
    const jazz_model_TxId *tx; void *v;
    JZ_BTREE_FOR(&self->rejections.rejected_transactions, tx, v) JZ_VEC_PUSH(*out, *tx);
}

/* Return a locally-retained rejected transaction payload retained for retry (cloned). */
jazz_model_RejectedTransaction *jazz_node__node__NodeState__rejected_transaction(const NS *self, jazz_model_TxId tx_id)
{
    jazz_model_RejectedTransaction *const *found = jz_btree_get(&self->rejections.rejected_transactions, &tx_id);
    return found ? jazz_model__tx__RejectedTransaction__clone(*found) : NULL;
}

/* Discard a locally-retained rejected transaction after the app acknowledges it. */
jz_err jazz_node__node__NodeState__discard_rejection(NS *self, jazz_model_TxId tx_id)
{
    if (!jz_uuid_eq(&tx_id.node, &self->node_uuid)) return JZ_OK;
    jazz_types_NodeAlias alias;
    if (!jazz_node__node__NodeAliases__get(self->node_aliases, &self->node_uuid, &alias)) return JZ_OK;
    groove_DatabaseBatch *batch = groove__db__facade__Database__open_batch(self->database);
    jz_str rejected = jz_str_from("jazz_rejected_transactions");
    groove__db__batch__DatabaseBatch__delete(batch, &rejected, jazz_node__node__codec__rejected_transaction_primary_key(alias, tx_id));
    jz_str_drop(&rejected);
    jz_btree table_ids;
    jazz_node__node__physical__NodeState__physical_table_ids(self, &table_ids);
    jz_err e = JZ_OK;
    const jazz_types_PhysicalTableId *table_id; void *unused;
    JZ_BTREE_FOR(&table_ids, table_id, unused) {
        jz_str storage_table = jazz_node__node__physical__physical_rejected_versions_table_name(*table_id);
        groove_Value prefix[2] = { gv_u64(tx_id.time), gv_u64(alias) };
        groove_RawRowVec raws = {0};
        e = groove__db__primary_storage__Database__primary_key_scan_raw(self->database, &storage_table, prefix, 2, &raws);
        if (e.code) { e = jazz_node__node__Error_groove(e); jz_str_drop(&storage_table); break; }
        for (u32 i = 0; i < raws.len && !e.code; ++i) {
            const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
            u64 node_id, time;
            if ((e = groove__records__OwnedRecord__get_u64(record, JZN_REJECTED_VERSION_ROW_FIELD_TX_NODE_ID_IDX, &node_id)).code) { e = jazz_node__node__Error_record(e); break; }
            if ((e = groove__records__OwnedRecord__get_u64(record, JZN_REJECTED_VERSION_ROW_FIELD_TX_TIME_IDX, &time)).code) { e = jazz_node__node__Error_record(e); break; }
            if (node_id != alias || time != tx_id.time) continue;
            jazz_node_PrimaryKeyValue key;
            if ((e = jazz_node__node__codec__rejected_version_primary_key_from_record(record, &key)).code) break;
            groove__db__batch__DatabaseBatch__delete(batch, &storage_table, key);
        }
        groove_RawRowVec_drop(&raws);
        jz_str_drop(&storage_table);
        if (e.code) break;
    }
    jz_btree_drop(&table_ids);
    if (e.code) { groove__db__batch__DatabaseBatch__drop(batch); return e; }
    JZ_TRY(jazz_node__node__NodeState__apply_and_persist_node_batch(self, batch));
    jazz_model_RejectedTransaction *removed;
    if (jz_btree_remove(&self->rejections.rejected_transactions, &tx_id, NULL, &removed)) jazz_model__tx__RejectedTransaction__drop(removed);
    return JZ_OK;
}

typedef struct HistoryEntrySlot { jazz_node_HistoryEntry *entry; u64 key_hi, key_lo; } HistoryEntrySlot;
static int history_entry_cmp(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    const HistoryEntrySlot *x = a, *y = b;
    if (x->key_hi != y->key_hi) return x->key_hi < y->key_hi ? -1 : 1;
    return (x->key_lo > y->key_lo) - (x->key_lo < y->key_lo);
}

/* `winner.is_some_and(|w| version_tx_id(w) == tx_id)` */
static bool winner_is_tx(const NS *self, const jazz_node_VersionRow *winner, jazz_model_TxId tx_id)
{
    if (!winner) return false;
    jazz_model_TxId winner_tx;
    jz_err e = jazz_node__node__NodeState__version_tx_id(self, winner, &winner_tx);
    if (e.code) { jz_err_drop(&e); return false; }
    return jazz_model_TxId_eq(&winner_tx, &tx_id);
}

/* Return stored edit-history entries for one row ordered by HLC observation
 * order. The parents DAG is the authoritative causal structure; HLC order is a
 * readable observation order. This method intentionally does no policy
 * filtering. Rejected transaction versions are not returned because rejection
 * cleanup removes their stored row versions. */
jz_err jazz_node__node__NodeState__row_history(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_HistoryEntryVec *out)
{
    out->ptr = NULL; out->len = out->cap = 0;
    jazz_node_VersionRowVec versions = {0};
    JZ_TRY(jazz_node__node__currency__NodeState__query_row_versions(self, table, row_uuid, &versions));
    JZ_VEC(HistoryEntrySlot) entries = {0};
    jz_err e = JZ_OK;
    JZ_VEC_FOR(const jazz_node_VersionRow, version, versions) {
        jazz_model_TxId tx_id;
        if ((e = jazz_node__node__NodeState__version_tx_id(self, version, &tx_id)).code) break;
        bool found; jazz_node_StoredTransaction tx;
        if ((e = jazz_node__node__currency__NodeState__query_transaction(self, tx_id, &found, &tx)).code) break;
        if (!found) { e = jazz_node__node__Error_missing_transaction(tx_id); break; }
        jazz_model_BranchKey branch = jazz_node__node__codec__VersionRow__branch_key(version);
        jazz_node_VersionRow *local = NULL, *global = NULL;
        e = jazz_node__node__currency__NodeState__query_local_winner_in_branch(self, table, &branch, row_uuid, &local);
        if (!e.code) e = jazz_node__node__currency__NodeState__query_global_winner_in_branch(self, table, &branch, row_uuid, &global);
        jazz_model_BranchKey_drop(&branch);
        if (e.code) {
            if (local) jazz_node__node__codec__VersionRow__free(local);
            jazz_node_StoredTransaction_drop(&tx);
            break;
        }
        bool local_current = winner_is_tx(self, local, tx_id);
        bool global_current = winner_is_tx(self, global, tx_id);
        if (local) jazz_node__node__codec__VersionRow__free(local);
        if (global) jazz_node__node__codec__VersionRow__free(global);
        HistoryEntrySlot slot = { jazz_node__node__codec__VersionRow__to_history_entry(version, &tx, local_current, global_current), 0, 0 };
        jazz_node_StoredTransaction_drop(&tx);
        jazz_model_TxId entry_tx = jazz_node__node__HistoryEntry__tx_id(slot.entry);
        jazz_types__time__TxTime__sort_key128(entry_tx.time, entry_tx.node, &slot.key_hi, &slot.key_lo);
        JZ_VEC_PUSH(entries, slot);
    }
    jazz_node_VersionRowVec_drop(&versions);
    if (e.code) {
        for (u32 i = 0; i < entries.len; ++i) jazz_node__node__HistoryEntry__drop(entries.ptr[i].entry);
        jz_free(entries.ptr);
        return e;
    }
    jz_sort(entries.ptr, entries.len, sizeof(HistoryEntrySlot), history_entry_cmp, NULL);
    JZ_VEC_FOR(HistoryEntrySlot, slot, entries) JZ_VEC_PUSH(*out, slot->entry);
    jz_free(entries.ptr);
    return JZ_OK;
}

/* Consume the node and return the underlying groove database. */
groove_Database *jazz_node__node__NodeState__into_database(NS *self)
{
    return jazz_node__node__database_slot__DatabaseSlot__into_inner(self->database);
}

/* Eagerly remove a Groove subscription from the runtime. */
bool jazz_node__node__NodeState__unsubscribe_groove_subscription(NS *self, groove_SubscriptionId subscription_id)
{
    return groove__db__query__Database__unsubscribe(self->database, subscription_id);
}

/* Resume suspended Groove evaluation, awaiting storage until every active
 * session completes. */
jz_err jazz_node__node__NodeState__drive_query_runtime(NS *self)
{
    jz_err e = groove__db__query__Database__drive_progress(self->database);
    return e.code ? jazz_node__node__Error_groove(e) : JZ_OK;
}

/* Resume all query work that can make progress now. */
jz_err jazz_node__node__NodeState__drive_ready_query_runtime(NS *self)
{
    return jazz_node__node__NodeState__drive_ready_query_runtime_with_waker(self, NULL);
}

/* Resume runnable query work and retain a host-owned wake bridge. */
jz_err jazz_node__node__NodeState__drive_ready_query_runtime_with_waker(NS *self, const jz_waker *progress_waker)
{
    jz_err e = groove__db__query__Database__drive_ready_progress_with_waker(self->database, progress_waker);
    return e.code ? jazz_node__node__Error_groove(e) : JZ_OK;
}

bool jazz_node__node__NodeState__has_pending_query_runtime(const NS *self) { return groove__db__query__Database__has_pending_progress(self->database); }
bool jazz_node__node__NodeState__subscription_has_pending_query_runtime(const NS *self, groove_SubscriptionId subscription)
{
    return groove__db__query__Database__subscription_has_pending_progress(self->database, subscription);
}
bool jazz_node__node__NodeState__subscription_has_pending_query_evaluation(const NS *self, groove_SubscriptionId subscription)
{
    return groove__db__query__Database__subscription_has_pending_evaluation(self->database, subscription);
}

jz_err jazz_node__node__NodeState__set_initial_sync_flush_cadence(NS *self, u32 every)
{
    JZ_ASSERT(every > 0, "debug_assert!(every > 0)");
    self->has_initial_sync_flush_cadence = true;
    self->initial_sync_flush_cadence = every;
    /* The cadence only relaxes durability while the first snapshot is active.
     * Normal client writes retain a boundary per committed batch. */
    jz_err e = groove__db__facade__Database__set_write_flush_cadence(self->database, 1);
    return e.code ? jazz_node__node__Error_groove(e) : JZ_OK;
}

jz_err jazz_node__node__NodeState__begin_initial_sync_flush_cadence(NS *self)
{
    if (!self->has_initial_sync_flush_cadence) return JZ_OK;
    u32 every = self->initial_sync_flush_cadence;
    if (self->initial_sync_flush_active || self->initial_sync_flush_completed) return JZ_OK;
    jz_err e = groove__db__facade__Database__set_write_flush_cadence(self->database, every);
    if (e.code) return jazz_node__node__Error_groove(e);
    self->initial_sync_flush_active = true;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__finish_initial_sync_flush_cadence(NS *self)
{
    if (!self->initial_sync_flush_active) return JZ_OK;
    jz_err e = groove__db__facade__Database__flush_write_boundary(self->database);
    if (e.code) return jazz_node__node__Error_groove(e);
    e = groove__db__facade__Database__set_write_flush_cadence(self->database, 1);
    if (e.code) return jazz_node__node__Error_groove(e);
    self->initial_sync_flush_active = false;
    self->initial_sync_flush_completed = true;
    return JZ_OK;
}

/* All peer connections share the cached database-owner wake. Check and
 * register under the owner lock so settlement cannot race registration. */
jz_err jazz_node__node__NodeState__defer_catalogue_for_persistence(const NS *self, const jz_waker *waker, bool *out)
{
    jz_err e = groove__db__facade__Database__wait_for_publication_settlement(self->database, waker, out);
    return e.code ? jazz_node__node__Error_groove(e) : JZ_OK;
}

u64 jazz_node__node__NodeState__physical_identity_generation(const NS *self) { return self->physical_identity_generation; }
u64 jazz_node__node__NodeState__groove_runtime_token(const NS *self) { return self->groove_runtime_token; }

/* Return metrics for the most recent committed storage batch, if any. */
const groove_CommitMetrics *jazz_node__node__NodeState__last_commit_metrics(const NS *self) { return groove__db__commit__Database__last_commit_metrics(self->database); }
/* Return metrics for the most recent Groove runtime tick, if any. */
const groove_TickMetrics *jazz_node__node__NodeState__last_tick_metrics(const NS *self) { return groove__db__commit__Database__last_tick_metrics(self->database); }
/* Return accumulated storage-read metrics since the last reset. */
groove_StorageReadMetrics jazz_node__node__NodeState__storage_read_metrics(const NS *self) { return groove__db__commit__Database__storage_read_metrics(self->database); }
/* Reset accumulated storage-read metrics. */
void jazz_node__node__NodeState__reset_storage_read_metrics(const NS *self) { groove__db__commit__Database__reset_storage_read_metrics(self->database); }
/* Return accumulated storage-read metrics and reset them. */
groove_StorageReadMetrics jazz_node__node__NodeState__take_storage_read_metrics(const NS *self) { return groove__db__commit__Database__take_storage_read_metrics(self->database); }

/* Compare an authority/session request with durable provenance without ever
 * turning persisted node attribution back into the system capability. */
bool jazz_node__node__durable_author_matches(jazz_types_AuthorSubject requested, jazz_types_AuthorSubject stored)
{
    if (requested.tag == JZ_AUTHOR_SYSTEM) return stored.tag == JZ_AUTHOR_SYSTEM_AT;
    return jazz_types_AuthorSubject_eq(stored, requested);
}
