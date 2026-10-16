/* Port of crates/jazz/layers/node/src/node/state/read_payload.rs
 * (include!d into node/mod.rs, so the Rust module is jazz_node::node). */
#include "jz_rt.h"
#include "../../_ext_node-qe-b.h"
#include "../node_qe_b.h"

#define NS jazz_node_NodeState

static const jazz_model_TableSchema *find_table_in(const jazz_model_JazzSchema *schema, const jz_str *name)
{
    JZ_VEC_FOR(const jazz_model_TableSchema, t, schema->tables)
        if (jz_str_eq(&t->name, name)) return t;
    return NULL;
}

jz_err jazz_node__node__NodeState__table(const NS *self, const jz_str *table, const jazz_model_TableSchema **out)
{
    *out = find_table_in(&self->catalogue.schema, table);
    if (!*out) return JZN_TABLE_NOT_FOUND(table);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__table_in_schema(const NS *self, const jz_str *table, jazz_types_SchemaVersionId schema_version, jazz_model_TableSchema *out)
{
    const jazz_model_TableSchema *t;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &t));
    jazz_model_TableSchema_clone(out, t);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__table_in_schema_ref(const NS *self, const jz_str *table, jazz_types_SchemaVersionId schema_version, const jazz_model_TableSchema **out)
{
    if (jz_uuid_eq(&schema_version, &self->catalogue.active_schema.schema)) {
        *out = find_table_in(&self->catalogue.active_schema.compiled, table);
        if (!*out) return JZN_TABLE_NOT_FOUND(table);
        return JZ_OK;
    }
    const jazz_protocol_SchemaVersion *schema = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
    const jazz_model_TableSchema *found = schema ? find_table_in(&schema->schema, table) : NULL;
    if (!found && jz_uuid_eq(&schema_version, &self->catalogue.local_schema_version_id)) {
        const jazz_model_TableSchema *t;
        jz_err e = jazz_node__node__NodeState__table(self, table, &t);
        if (e.code) jz_err_drop(&e); else found = t;
    }
    if (!found) return JZN_TABLE_NOT_FOUND(table);
    *out = found;
    return JZ_OK;
}

bool jazz_node__node__NodeState__shortest_lens_path_cached(NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, jazz_node_LensStepVec *out)
{
    jazz_node_LensPathCacheKey key = { source, target };
    const jazz_node_LensPathCacheEntry *cached = jz_btree_get(&self->catalogue.lens_path_cache, &key);
    if (cached) {
        if (!cached->has) return false;
        out->ptr = NULL; out->len = out->cap = 0;
        if (cached->path.len) {
            jz_vec_reserve(out, cached->path.len, sizeof(jazz_node_LensStep));
            memcpy(out->ptr, cached->path.ptr, cached->path.len * sizeof(jazz_node_LensStep));
            out->len = cached->path.len;
        }
        return true;
    }
    jazz_node_LensStepVec path = {0};
    bool has = jazz_node__node__NodeState__shortest_lens_path(self, source, target, &path);
    jazz_node_LensPathCacheEntry entry = { has, {0} };
    if (has && path.len) {
        jz_vec_reserve(&entry.path, path.len, sizeof(jazz_node_LensStep));
        memcpy(entry.path.ptr, path.ptr, path.len * sizeof(jazz_node_LensStep));
        entry.path.len = path.len;
    }
    jz_btree_insert(&self->catalogue.lens_path_cache, &key, &entry, NULL);
    if (has) *out = path;
    return has;
}

/* (lens, direction) edge from ordered_lens_edges */
typedef struct LensEdge { const jazz_protocol_MigrationLens *lens; u32 direction; } LensEdge;
typedef JZ_VEC(LensEdge) LensEdgeVec;
static jazz_types_SchemaVersionId edge_next(const LensEdge *e)
{
    return e->direction == JZN_LENS_FORWARD ? e->lens->target : e->lens->source;
}
static int lens_edge_cmp(const void *a, const void *b, void *ctx)
{
    (void)ctx;
    const LensEdge *x = a, *y = b;
    jazz_types_SchemaVersionId nx = edge_next(x), ny = edge_next(y);
    int c = jz_uuid_cmp(&nx, &ny);
    return c ? c : jz_uuid_cmp(&x->lens->id, &y->lens->id);
}

static LensEdgeVec ordered_lens_edges(const NS *self, jazz_types_SchemaVersionId schema)
{
    LensEdgeVec edges = {0};
    const jazz_types_MigrationLensId *id; const jazz_protocol_MigrationLens *lens;
    JZ_BTREE_FOR(&self->catalogue.catalogue_lenses, id, lens) {
        if (jz_uuid_eq(&lens->source, &schema)) JZ_VEC_PUSH(edges, ((LensEdge){ lens, JZN_LENS_FORWARD }));
        else if (jz_uuid_eq(&lens->target, &schema)) JZ_VEC_PUSH(edges, ((LensEdge){ lens, JZN_LENS_REVERSE }));
    }
    /* sort_by_key is stable */
    jz_sort(edges.ptr, edges.len, sizeof(LensEdge), lens_edge_cmp, NULL);
    return edges;
}

typedef struct QueueItem { jazz_types_SchemaVersionId schema; jazz_node_LensStepVec path; } QueueItem;

static jazz_node_LensStepVec step_vec_clone(const jazz_node_LensStepVec *v)
{
    jazz_node_LensStepVec out = {0};
    if (v->len) {
        jz_vec_reserve(&out, v->len, sizeof(jazz_node_LensStep));
        memcpy(out.ptr, v->ptr, v->len * sizeof(jazz_node_LensStep));
        out.len = v->len;
    }
    return out;
}

static void queue_item_drop(void *p) { QueueItem *q = p; jz_free(q->path.ptr); }

bool jazz_node__node__NodeState__shortest_lens_path(const NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, jazz_node_LensStepVec *out)
{
    if (jz_uuid_eq(&source, &target)) {
        out->ptr = NULL; out->len = out->cap = 0;
        return true;
    }
    /* Preserve ancestor-authored columns when a forward route exists. A
     * shorter reverse detour can drop and reintroduce those columns. */
    static const bool passes[2] = { true, false };
    for (int pass = 0; pass < 2; ++pass) {
        bool forward_only = passes[pass];
        jz_btree seen; jz_btree_init(&seen, &jazz_node_UUID_SET_DESC);
        jz_btree_insert(&seen, &source, NULL, NULL);
        jz_deque queue; jz_deque_init(&queue, sizeof(QueueItem));
        QueueItem first = { source, {0} };
        jz_deque_push_back(&queue, &first);
        QueueItem item;
        while (jz_deque_pop_front(&queue, &item)) {
            LensEdgeVec edges = ordered_lens_edges(self, item.schema);
            JZ_VEC_FOR(LensEdge, edge, edges) {
                if (forward_only && edge->direction == JZN_LENS_REVERSE) continue;
                jazz_types_SchemaVersionId next = edge_next(edge);
                if (jz_btree_contains(&seen, &next)) continue;
                jazz_node_LensStepVec next_path = step_vec_clone(&item.path);
                JZ_VEC_PUSH(next_path, ((jazz_node_LensStep){ edge->lens->id, edge->direction }));
                if (jz_uuid_eq(&next, &target)) {
                    jz_free(edges.ptr);
                    queue_item_drop(&item);
                    jz_deque_drop(&queue, queue_item_drop);
                    jz_btree_drop(&seen);
                    *out = next_path;
                    return true;
                }
                jz_btree_insert(&seen, &next, NULL, NULL);
                QueueItem queued = { next, next_path };
                jz_deque_push_back(&queue, &queued);
            }
            jz_free(edges.ptr);
            queue_item_drop(&item);
        }
        jz_deque_drop(&queue, queue_item_drop);
        jz_btree_drop(&seen);
    }
    return false;
}

static void compiled_cache_insert(NS *self, jazz_node_CompiledLensCacheKey *key, bool has, const jazz_node_CompiledLensPath *path)
{
    jazz_node_CompiledLensCacheEntry entry = { has, {0} };
    if (has) jazz_node_CompiledLensPath_clone(&entry.path, path);
    jz_btree_insert(&self->catalogue.compiled_lens_cache, key, &entry, NULL);
}

jz_err jazz_node__node__NodeState__compiled_lens_path(NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target, const jz_str *table, bool *has_path, jazz_node_CompiledLensPath *out)
{
    *has_path = false;
    jazz_node_CompiledLensCacheKey key = { source, target, jz_str_clone(table) };
    const jazz_node_CompiledLensCacheEntry *cached = jz_btree_get(&self->catalogue.compiled_lens_cache, &key);
    if (cached) {
        jz_str_drop(&key.table);
        *has_path = cached->has;
        if (cached->has) jazz_node_CompiledLensPath_clone(out, &cached->path);
        return JZ_OK;
    }

    jazz_node_LensStepVec steps = {0};
    if (!jazz_node__node__NodeState__shortest_lens_path_cached(self, source, target, &steps)) {
        compiled_cache_insert(self, &key, false, NULL);
        return JZ_OK;
    }
    jz_str current_table = jz_str_clone(table);
    jazz_node_CompiledLensOpVec ops = {0};
    jz_err e = JZ_OK;
    JZ_VEC_FOR(jazz_node_LensStep, step, steps) {
        const jazz_protocol_MigrationLens *lens = jz_btree_get(&self->catalogue.catalogue_lenses, &step->lens);
        if (!lens) { e = JZN_INVALID_CATALOGUE("lens chain is unknown"); goto fail; }
        const jazz_protocol_TableLens *table_lens = NULL;
        JZ_VEC_FOR(const jazz_protocol_TableLens, candidate, lens->table_lenses) {
            const jz_str *side = step->direction == JZN_LENS_FORWARD ? &candidate->source_table : &candidate->target_table;
            if (jz_str_eq(side, &current_table)) { table_lens = candidate; break; }
        }
        if (!table_lens) {
            compiled_cache_insert(self, &key, false, NULL);
            jz_str_drop(&current_table);
            for (u32 i = 0; i < ops.len; ++i) { jz_str_drop(&ops.ptr[i].from); jz_str_drop(&ops.ptr[i].to); groove_Value_drop(&ops.ptr[i].default_); }
            jz_free(ops.ptr); jz_free(steps.ptr);
            return JZ_OK;
        }
        if (step->direction == JZN_LENS_FORWARD) {
            JZ_VEC_FOR(const jazz_protocol_LensOp, op, table_lens->ops) {
                e = jazz_node__node__push_compiled_forward_lens_op(op, &ops);
                if (e.code) goto fail;
            }
            jz_str_drop(&current_table);
            current_table = jz_str_clone(&table_lens->target_table);
        } else {
            for (u32 i = table_lens->ops.len; i-- > 0;) {
                e = jazz_node__node__push_compiled_reverse_lens_op(&table_lens->ops.ptr[i], &ops);
                if (e.code) goto fail;
            }
            jz_str_drop(&current_table);
            current_table = jz_str_clone(&table_lens->source_table);
        }
    }
    jz_free(steps.ptr);
    {
        jazz_node_CompiledLensPath path = { current_table, ops };
        compiled_cache_insert(self, &key, true, &path);
        *out = path;
        *has_path = true;
    }
    return JZ_OK;
fail:
    jz_str_drop(&key.table);
    jz_str_drop(&current_table);
    for (u32 i = 0; i < ops.len; ++i) { jz_str_drop(&ops.ptr[i].from); jz_str_drop(&ops.ptr[i].to); groove_Value_drop(&ops.ptr[i].default_); }
    jz_free(ops.ptr);
    jz_free(steps.ptr);
    return e;
}

bool jazz_node__node__NodeState__node_for_alias(const NS *self, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out)
{
    return jazz_node__node__NodeAliases__node_for_alias(self->node_aliases, alias, out);
}

jz_err jazz_node__node__NodeState__resolve_node_alias(NS *self, jazz_types_NodeAlias alias, bool *has, jazz_types_NodeUuid *out)
{
    *has = false;
    if (jazz_node__node__NodeState__node_for_alias(self, alias, out)) { *has = true; return JZ_OK; }
    jz_str jazz_nodes = jz_str_from("jazz_nodes");
    groove_RawRowVec raws = {0};
    jz_err e = groove__db__primary_storage__Database__primary_key_scan_raw(self->database, &jazz_nodes, NULL, 0, &raws);
    jz_str_drop(&jazz_nodes);
    if (e.code) return jazz_node__node__Error_groove(e);
    for (u32 i = 0; i < raws.len && !*has; ++i) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        u64 id; jz_uuid uuid;
        e = groove__records__OwnedRecord__get_u64(record, JZN_NODE_ALIAS_ROW_FIELD_ID_IDX, &id);
        if (e.code) break;
        if (id != alias) continue;
        e = groove__records__OwnedRecord__get_uuid(record, JZN_NODE_ALIAS_ROW_FIELD_UUID_IDX, &uuid);
        if (e.code) break;
        jazz_types_NodeUuid node = uuid;
        jazz_node__node__NodeAliases__insert(self->node_aliases, node, alias);
        if (jz_uuid_eq(&node, &self->node_uuid)) { self->has_self_node_alias = true; self->self_node_alias = alias; }
        *out = node;
        *has = true;
    }
    for (u32 i = 0; i < raws.len; ++i) groove__db__RawRow__drop(raws.ptr[i]);
    jz_free(raws.ptr);
    if (e.code) return jazz_node__node__Error_record(e);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__version_tx_id(const NS *self, const jazz_node_VersionRow *version, jazz_model_TxId *out)
{
    jazz_types_NodeUuid node;
    if (!jazz_node__node__NodeState__node_for_alias(self, jazz_node__node__codec__VersionRow__tx_node_alias(version), &node))
        return JZN_INVALID_STORED("history tx node alias must exist");
    out->time = jazz_node__node__codec__VersionRow__tx_time(version);
    out->node = node;
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__version_record_from_row(NS *self, const jazz_node_VersionRow *version, jazz_protocol_VersionRecord **out)
{
    /* #[cfg(feature = "cold-settle-attribution")] record_conversion: not ported (feature off). */
    jazz_types_SchemaVersionId schema_version;
    if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(version), &schema_version))
        return JZN_INVALID_STORED("history schema version alias must exist");
    const jazz_model_TableSchema *table;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, jazz_node__node__codec__VersionRow__table(version), schema_version, &table));
    bool has_authored; jz_btree authored_columns;
    JZ_TRY(jazz_node__node__physical__NodeState__authored_columns_for_version(self, version, &has_authored, &authored_columns));
    return jazz_protocol__protocol__VersionRecord__from_stored(version, table, schema_version, has_authored ? &authored_columns : NULL, out);
}

/* (row_uuid, tx_time, tx_node_id, table_ids) */
typedef struct RequestedPhysical {
    jazz_types_RowUuid row_uuid;
    jazz_types_TxTime tx_time;
    jazz_types_NodeUuid tx_node_id;
    jz_btree table_ids; /* BTreeSet<PhysicalTableId> */
} RequestedPhysical;
typedef JZ_VEC(RequestedPhysical) RequestedPhysicalVec;
static void requested_physical_vec_drop(RequestedPhysicalVec *v)
{
    for (u32 i = 0; i < v->len; ++i) jz_btree_drop(&v->ptr[i].table_ids);
    jz_free(v->ptr);
}

static void bundle_vec_drop(jazz_protocol_VersionBundleVec *v)
{
    for (u32 i = 0; i < v->len; ++i) jazz_protocol_VersionBundle_drop(&v->ptr[i]);
    jz_free(v->ptr);
    v->ptr = NULL; v->len = v->cap = 0;
}

/* filter(): the version belongs to a requested physical row of this bundle's transaction. */
static bool version_requested(NS *self, const jazz_protocol_VersionRecord *version, const jazz_model_TxId *tx_id, const RequestedPhysicalVec *requested)
{
    jazz_types_PhysicalTableId table_id;
    jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self,
        jazz_protocol__protocol__VersionRecord__schema_version(version), jazz_protocol__protocol__VersionRecord__table(version), &table_id);
    if (e.code) { jz_err_drop(&e); return false; }
    jazz_types_RowUuid row = jazz_protocol__protocol__VersionRecord__row_uuid(version);
    JZ_VEC_FOR(const RequestedPhysical, r, *requested)
        if (jz_uuid_eq(&r->row_uuid, &row) && r->tx_time == tx_id->time && jz_uuid_eq(&r->tx_node_id, &tx_id->node)
            && jz_btree_contains(&r->table_ids, &table_id))
            return true;
    return false;
}

jz_err jazz_node__node__NodeState__apply_row_version_payloads_for_requests(NS *self, const jazz_protocol_RowVersionRef *requests, u32 n_requests,
    jazz_protocol_VersionBundleVec version_bundles, jazz_protocol_VersionBundleVec *out)
{
    jz_err e = JZ_OK;
    for (u32 i = 0; i < version_bundles.len; ++i) {
        e = jazz_protocol__protocol__validate_version_records(&version_bundles.ptr[i].versions);
        if (e.code) { jz_err_drop(&e); bundle_vec_drop(&version_bundles); return JZN_MALFORMED_VIEW("malformed version receipt"); }
    }
    RequestedPhysicalVec requested_physical = {0};
    for (u32 i = 0; i < n_requests; ++i) {
        const jazz_protocol_RowVersionRef *request = &requests[i];
        RequestedPhysical r = { request->row_uuid, request->tx_time, request->tx_node_id, {0} };
        jz_btree_init(&r.table_ids, &jazz_node_U64_SET_DESC);
        const jazz_types_SchemaVersionId *sv; const jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&self->catalogue.physical_mappings, sv, mapping) {
            const jazz_node_PhysicalTableMapping *table = jz_btree_get(&mapping->tables, request->table);
            if (table) jz_btree_insert(&r.table_ids, &table->table_id, NULL, NULL);
        }
        if (r.table_ids.len == 0) {
            jz_btree_drop(&r.table_ids);
            requested_physical_vec_drop(&requested_physical);
            bundle_vec_drop(&version_bundles);
            return JZN_TABLE_NOT_FOUND(request->table);
        }
        JZ_VEC_PUSH(requested_physical, r);
    }
    /* One `RowVersionPayloads` message is one repair frame. Preflight and
     * filter every bundle before ingesting the first one so a malformed later
     * carrier cannot leave earlier transaction/history/clock state behind
     * after the frame is rejected. */
    jazz_protocol_VersionBundleVec prevalidated_bundles = {0};
    for (u32 b = 0; b < version_bundles.len; ++b) {
        jazz_protocol_VersionBundle *bundle = &version_bundles.ptr[b];
        e = jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bundle->has_global_time, bundle->global_time, bundle->durability);
        if (e.code) goto fail;
        jazz_protocol_VersionRecordVec taken = bundle->versions;
        bundle->versions.ptr = NULL; bundle->versions.len = bundle->versions.cap = 0;
        jazz_protocol_VersionRecordVec versions = {0};
        for (u32 i = 0; i < taken.len; ++i) {
            if (version_requested(self, taken.ptr[i], &bundle->tx.tx_id, &requested_physical)) JZ_VEC_PUSH(versions, taken.ptr[i]);
            else jazz_protocol_VersionRecord_drop(taken.ptr[i]);
        }
        jz_free(taken.ptr);
        if (versions.len == 0) { jz_free(versions.ptr); continue; }
        JZ_VEC_FOR(const RequestedPhysical, r, requested_physical) {
            jz_btree matched_table_ids; jz_btree_init(&matched_table_ids, &jazz_node_U64_SET_DESC);
            for (u32 i = 0; i < versions.len; ++i) {
                const jazz_protocol_VersionRecord *version = versions.ptr[i];
                jazz_types_RowUuid row = jazz_protocol__protocol__VersionRecord__row_uuid(version);
                if (!(jz_uuid_eq(&r->row_uuid, &row) && r->tx_time == bundle->tx.tx_id.time && jz_uuid_eq(&r->tx_node_id, &bundle->tx.tx_id.node))) continue;
                jazz_types_PhysicalTableId table_id;
                jz_err te = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self,
                    jazz_protocol__protocol__VersionRecord__schema_version(version), jazz_protocol__protocol__VersionRecord__table(version), &table_id);
                if (te.code) { jz_err_drop(&te); continue; }
                if (jz_btree_contains(&r->table_ids, &table_id)) jz_btree_insert(&matched_table_ids, &table_id, NULL, NULL);
            }
            u32 matched = matched_table_ids.len;
            jz_btree_drop(&matched_table_ids);
            if (matched > 1) {
                e = JZN_INVALID_STORED("repair response maps to multiple physical tables");
                for (u32 i = 0; i < versions.len; ++i) jazz_protocol_VersionRecord_drop(versions.ptr[i]);
                jz_free(versions.ptr);
                goto fail;
            }
        }
        e = jazz_node__node__ingest__NodeState__validate_view_payload_versions(self, &versions);
        if (e.code) {
            for (u32 i = 0; i < versions.len; ++i) jazz_protocol_VersionRecord_drop(versions.ptr[i]);
            jz_free(versions.ptr);
            goto fail;
        }
        bundle->versions = versions;
        JZ_VEC_PUSH(prevalidated_bundles, *bundle);
        memset(bundle, 0, sizeof *bundle); /* moved */
    }
    bundle_vec_drop(&version_bundles);
    requested_physical_vec_drop(&requested_physical);

    jazz_protocol_VersionBundleVec applied_bundles = {0};
    jz_vec_reserve(&applied_bundles, prevalidated_bundles.len, sizeof(jazz_protocol_VersionBundle));
    for (u32 b = 0; b < prevalidated_bundles.len; ++b) {
        jazz_protocol_VersionBundle *bundle = &prevalidated_bundles.ptr[b];
        /* View and repair carriers can replay a locally-authorized transaction,
         * but their transport never selects its durable policy capability.
         * Retain a local stored hint for restart; do not import or republish a
         * received one. */
        jazz_model_Transaction stripped = jazz_node__node__transaction_without_permission_subject(&bundle->tx);
        jazz_model_Transaction_drop(&bundle->tx);
        bundle->tx = stripped;
        jazz_model_Transaction tx; jazz_model_Transaction_clone(&tx, &bundle->tx);
        jazz_protocol_VersionRecordVec versions = jazz_protocol_VersionRecordVec_clone(&bundle->versions);
        jazz_model_Fate fate; jazz_model_Fate_clone(&fate, &bundle->fate);
        if (bundle->scope == JZ_VBS_VIEW_SCOPED) {
            /* A row repair is still a partial transaction carrier. Its visible
             * cardinality cannot certify that a withheld sibling/parent
             * coordinate does not exist. */
            e = jazz_node__node__ingest__NodeState__ingest_view_scoped_transaction_with_current_indexes(self, tx, versions, fate,
                bundle->has_global_time, bundle->global_time, bundle->durability);
        } else { /* CompleteTransaction */
            e = jazz_node__node__ingest__NodeState__ingest_known_transaction(self, tx, versions, fate,
                bundle->has_global_time, bundle->global_time, bundle->durability);
        }
        if (e.code) {
            for (u32 j = b; j < prevalidated_bundles.len; ++j) jazz_protocol_VersionBundle_drop(&prevalidated_bundles.ptr[j]);
            jz_free(prevalidated_bundles.ptr);
            bundle_vec_drop(&applied_bundles);
            return e;
        }
        JZ_VEC_PUSH(applied_bundles, *bundle);
    }
    jz_free(prevalidated_bundles.ptr);
    *out = applied_bundles;
    return JZ_OK;
fail:
    bundle_vec_drop(&prevalidated_bundles);
    bundle_vec_drop(&version_bundles);
    requested_physical_vec_drop(&requested_physical);
    return e;
}

/* (TxId, RowUuid, PhysicalTableId, Vec<u8>) */
typedef struct IncomingVersion { jazz_model_TxId tx; jazz_types_RowUuid row; jazz_types_PhysicalTableId table; jz_bytes branch; } IncomingVersion;
static int incoming_version_cmp(const void *a, const void *b, void *ctx)
{
    const IncomingVersion *x = a, *y = b;
    int c = jazz_model_TxId_cmp(&x->tx, &y->tx, ctx); if (c) return c;
    c = jz_uuid_cmp(&x->row, &y->row); if (c) return c;
    c = (x->table > y->table) - (x->table < y->table); if (c) return c;
    return jz_bytes_cmp(&x->branch, &y->branch);
}
static void incoming_version_drop(void *p) { jz_bytes_drop(&((IncomingVersion *)p)->branch); }
static const jz_btree_desc INCOMING_VERSIONS_DESC = { sizeof(IncomingVersion), 0, incoming_version_cmp, incoming_version_drop, NULL };

static void row_version_ref_drop(void *p) { (void)p; /* table is Intern<String> */ }
static const jz_btree_desc ROW_VERSION_REF_SET_DESC = { sizeof(jazz_protocol_RowVersionRef), 0, jazz_protocol_RowVersionRef_cmp, row_version_ref_drop, NULL };

jz_err jazz_node__node__NodeState__missing_known_state_row_version_refs(NS *self, const jazz_protocol_SyncMessage *message, jazz_protocol_RowVersionRefVec *out)
{
    out->ptr = NULL; out->len = out->cap = 0;
    if (jazz_protocol__protocol__SyncMessage__tag(message) != JZ_SYNC_VIEW_UPDATE) return JZ_OK;
    const jazz_protocol_ViewUpdatePayload *payload = jazz_protocol__protocol__SyncMessage__view_update(message);
    if (payload->peer_payload_inventory.opening_pending) {
        /* Pending is not a row snapshot. Let normal admission reject any
         * attached rows immediately, rather than trying to repair their bytes
         * and postponing the protocol error indefinitely. */
        return JZ_OK;
    }
    jazz_protocol_SubscriptionKey subscription = payload->subscription;
    jazz_protocol_VersionBundleVec normalized_bundles = {0};
    jz_err e = jazz_protocol__protocol__expand_version_carriers(payload->version_carriers, &normalized_bundles);
    if (e.code) { jz_err_drop(&e); return JZN_UNSUPPORTED_SYNC("malformed version-bundle run"); }
    /* Index inline witnesses once. Searching every incoming body for every
     * supporting row makes initial hydration quadratic. */
    jz_btree incoming_versions; jz_btree_init(&incoming_versions, &INCOMING_VERSIONS_DESC);
    JZ_VEC_FOR(const jazz_protocol_VersionBundle, bundle, normalized_bundles) {
        for (u32 i = 0; i < bundle->versions.len; ++i) {
            const jazz_protocol_VersionRecord *version = bundle->versions.ptr[i];
            jazz_types_PhysicalTableId table;
            jz_err te = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self,
                jazz_protocol__protocol__VersionRecord__schema_version(version), jazz_protocol__protocol__VersionRecord__table(version), &table);
            if (te.code) { jz_err_drop(&te); continue; }
            jazz_model_BranchKey bk = jazz_protocol__protocol__VersionRecord__branch_key(version);
            IncomingVersion iv = { bundle->tx.tx_id, jazz_protocol__protocol__VersionRecord__row_uuid(version), table,
                                   jazz_model__model__branch__BranchKey__canonical_bytes(&bk) };
            jazz_model_BranchKey_drop(&bk);
            bool inserted;
            jz_btree_entry(&incoming_versions, &iv, &inserted);
            if (!inserted) incoming_version_drop(&iv);
        }
    }
    bundle_vec_drop(&normalized_bundles);
    jazz_model_ValidatedQuery registered_shape;
    if (!jazz_node__node__query_eval__NodeState__registered_shape(self, subscription.shape_id, &registered_shape)) {
        /* A late update may race a local unsubscribe. There is no live result
         * shape to repair or apply, so preserve the existing stale-update
         * behavior and ignore it rather than turning normal teardown into a
         * protocol error. */
        jz_btree_drop(&incoming_versions);
        return JZ_OK;
    }
    jazz_types_SchemaVersionId result_schema_version = registered_shape.schema_version;
    jazz_model_ValidatedQuery_drop(&registered_shape);
    /* GlobalPhysicalTableId -> table name (borrowed from the catalogue) */
    jz_btree table_names; jz_btree_init(&table_names, &jazz_node_UUID_PTR_MAP_DESC);
    const jazz_node_SchemaPhysicalMapping *result_mapping = jz_btree_get(&self->catalogue.physical_mappings, &result_schema_version);
    if (result_mapping) {
        const jz_str *name; const jazz_node_PhysicalTableIdentity *identity;
        JZ_BTREE_FOR(&result_mapping->identities.tables, name, identity) jz_btree_insert(&table_names, &identity->id, &name, NULL);
    }
    jz_btree missing; jz_btree_init(&missing, &ROW_VERSION_REF_SET_DESC);
    /* Every referenced native body must be available, including retained
     * rows whose bytes may have been evicted since the previous complete
     * snapshot. */
    jazz_protocol_SupportingRowVec added = jazz_protocol__protocol__ProgramFacts__added_rows(payload->supporting_rows);
    for (u32 i = 0; i < added.len && !e.code; ++i) {
        const jazz_protocol_SupportingRow *row = added.ptr[i];
        jazz_model_TxId tx_id = row->version.tx;
        jazz_protocol_RowVersionRef version_ref = jazz_protocol__protocol__RowVersionRef__new(row->version_table, row->row, tx_id);
        bool covers;
        e = jazz_node__node__NodeState__inline_version_bundle_covers(self, &version_ref, result_schema_version, &row->version, &incoming_versions, &covers);
        if (e.code) break;
        if (covers) continue;
        bool resident = false;
        const jz_str *const *table = jz_btree_get(&table_names, &row->physical_table);
        if (table) {
            bool has; jazz_node_VersionRow version;
            e = jazz_node__node__NodeState__local_supporting_row_version(self, row, result_schema_version, *table, &has, &version);
            if (e.code) break;
            if (has) { resident = true; jazz_node_VersionRow_drop(&version); }
        }
        bool has_transaction_exists = false, transaction_exists = false;
        if (resident) {
            e = jazz_node__node__currency__NodeState__transaction_exists(self, tx_id, &transaction_exists);
            if (e.code) break;
            has_transaction_exists = true;
        }
        /* #[cfg(any(test, feature = "testing"))] delivery_diagnostics::record(repair_body_lookup ..): not ported. */
        if (!resident || (has_transaction_exists && !transaction_exists)) {
            bool inserted;
            jz_btree_entry(&missing, &version_ref, &inserted);
        }
    }
    jz_free(added.ptr);
    jz_btree_drop(&table_names);
    jz_btree_drop(&incoming_versions);
    if (e.code) { jz_btree_drop(&missing); return e; }
    jazz_protocol_RowVersionRef *k;
    while (jz_btree_first(&missing, (void **)&k, NULL)) {
        jazz_protocol_RowVersionRef ref = *k;
        JZ_VEC_PUSH(*out, ref);
        jz_btree_pop_first(&missing, NULL, NULL);
    }
    jz_btree_drop(&missing);
    return JZ_OK;
}

/* Check inline ViewUpdate bundles against the same unambiguous physical
 * identity used by row-version repair. A logical name may be dropped and
 * later reused for another physical lineage, so name equality is never
 * sufficient evidence that the incoming body covers a result member. */
jz_err jazz_node__node__NodeState__inline_version_bundle_covers(NS *self, const jazz_protocol_RowVersionRef *request, jazz_types_SchemaVersionId result_schema_version,
    const jazz_protocol_RowVersionRefEntry *version, const jz_btree *incoming_versions, bool *out)
{
    *out = false;
    /* Unlike a standalone RowVersionRef repair request, an inline witness is
     * carried by a registered subscription whose schema version makes a
     * reused logical table name unambiguous. */
    jazz_types_PhysicalTableId requested_table_id;
    jz_err e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, result_schema_version, request->table, &requested_table_id);
    if (e.code) {
        if (e.code != JZN_E_TableNotFound) return e;
        jz_err_drop(&e);
        /* Contributor facts deliberately name their authored table, which may
         * have been renamed out of the current result schema. An inline body
         * can cover it only when that old name still has a unique physical
         * lineage across the catalogue. */
        jz_btree candidates; jz_btree_init(&candidates, &jazz_node_U64_SET_DESC);
        const jazz_types_SchemaVersionId *sv; const jazz_node_SchemaPhysicalMapping *mapping;
        JZ_BTREE_FOR(&self->catalogue.physical_mappings, sv, mapping) {
            const jazz_node_PhysicalTableMapping *table = jz_btree_get(&mapping->tables, request->table);
            if (table) jz_btree_insert(&candidates, &table->table_id, NULL, NULL);
        }
        if (candidates.len == 0) { jz_btree_drop(&candidates); return JZN_TABLE_NOT_FOUND(request->table); }
        if (candidates.len != 1) { jz_btree_drop(&candidates); return JZ_OK; }
        jazz_types_PhysicalTableId *only;
        JZ_ASSERT(jz_btree_first(&candidates, (void **)&only, NULL), "unique candidate");
        requested_table_id = *only;
        jz_btree_drop(&candidates);
    }
    IncomingVersion probe = { jazz_protocol__protocol__RowVersionRef__tx_id(request), request->row_uuid, requested_table_id,
                              version->has_branch_or_prefix ? version->branch_or_prefix : (jz_bytes){0} };
    *out = jz_btree_contains(incoming_versions, &probe);
    return JZ_OK;
}

jz_err jazz_node__node__NodeState__local_supporting_row_version(NS *self, const jazz_protocol_SupportingRow *row, jazz_types_SchemaVersionId read_schema, const jz_str *table,
    bool *has, jazz_node_VersionRow *out)
{
#ifndef NDEBUG
    jazz_types_GlobalPhysicalTableId scoped;
    JZ_TRY(jazz_node__node__views__NodeState__scope_physical_table(self, read_schema, table, &scoped));
    JZ_ASSERT(jz_uuid_eq(&scoped, &row->physical_table), "debug_assert_eq!(scope_physical_table, row.physical_table)");
#endif
    return jazz_node__node__views__NodeState__covered_input_version(self, row, read_schema, has, out);
}

jz_err jazz_node__node__NodeState__mint_tx_time(NS *self, u64 now_ms, jazz_types_TxTime *out)
{
    jazz_types_TxTime prev = self->clock.tx_time > *self->clock.reservation_high_water ? self->clock.tx_time : *self->clock.reservation_high_water;
    jazz_types_TxTime made_at;
    JZ_TRY(jazz_types__time__TxTime__tick(prev, now_ms, &made_at));
    self->clock.tx_time = made_at;
    *self->clock.reservation_high_water = made_at;
    *out = made_at;
    return JZ_OK;
}

void jazz_node__node__NodeState__merge_tx_time(NS *self, jazz_types_TxTime observed)
{
    if (observed > self->clock.tx_time) self->clock.tx_time = observed;
    if (observed > *self->clock.reservation_high_water) *self->clock.reservation_high_water = observed;
}
