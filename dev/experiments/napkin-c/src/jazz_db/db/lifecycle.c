/* lifecycle.c: port of crates/jazz/layers/db/src/db/lifecycle.rs
 * Database construction, schema views, write-state waiting, and connection
 * controls.
 *
 * Async methods run synchronously (`.await` on the db-a node lock is
 * SharedNodeState__lock/unlock; StackSafeFuture is dropped). Items behind
 * `cfg(test)`, the `testing` feature (`*_for_test`, TestUpstreamConnectionHandle,
 * ObservedTestTransport, size receipts) and the `runtime` feature
 * (catalogue snapshots, upload root, wall clock, scalar-exit refresh) are not
 * part of the browser build and are not ported. */
#include "jz_rt.h"
#include "../_ext_db-b-lc.h"
#include <string.h>

#define LC_FN(n) jazz_db__db__lifecycle__Db__##n
#define NS(n) jazz_node__node__NodeState__##n
#define NODE(n) jazz_db__db__node_runtime__Node__##n
#define MUT(n) jazz_db__db__mutations__Db__##n

static jz_err ne(jz_err e) {
    return JZ_IS_ERR(e) ? jazz_node__node__api_error__Error__from_node_error(e) : e;
}
static jazz_node_NodeState *lock_node(const jazz_db_Db *db) { return jazz_db__db__SharedNodeState__lock(db->node->node); }
static void unlock_node(const jazz_db_Db *db) { jazz_db__db__SharedNodeState__unlock(db->node->node); }
static jazz_node_NodeState *borrow_node(const jazz_db_Db *db, const char *caller) {
    return jazz_db__db__LocalMutexBorrow__borrow(db->node->node, caller);
}

/* ---- Rc payloads and the schema-view map ---- */
static int schema_view_id_cmp(const void *a, const void *b, void *ctx) { return memcmp(a, b, sizeof(jazz_db_SchemaViewId)); }
static const jz_btree_desc schema_views_desc = {
    sizeof(jazz_db_SchemaViewId), sizeof(jazz_model_JazzSchema), schema_view_id_cmp, NULL, jazz_model__schema__JazzSchema__drop,
};
static void schema_views_cell_drop(void *p) { jz_btree_drop(&((jazz_db_SchemaViewsCell *)p)->map); }
static void row_id_source_cell_drop(void *p) {
    jazz_db_RowIdSourceCell *c = p;
    if (c->source.vt->drop) c->source.vt->drop(c->source.self);
}

/* Db drop glue: clone of the schema dropped, Rc handles released. */
void jazz_db__db__Db__drop(jazz_db_Db *db) {
    jazz_model__schema__JazzSchema__drop(&db->schema);
    jz_rc_release(db->schema_views);
    jz_rc_release(db->node);
    jz_rc_release(db->row_id_source);
    jz_rc_release(db->next_now_ms);
}

/* Shared-handle copy used by register_schema_view and the owner clones. */
static void clone_shared(const jazz_db_Db *db, jazz_db_Db *out) {
    jazz_model__schema__JazzSchema__clone(&out->schema, &db->schema);
    out->schema_version_id = db->schema_version_id;
    out->schema_view_is_fixed = db->schema_view_is_fixed;
    out->requires_open_schema_admission = db->requires_open_schema_admission;
    out->schema_views = jz_rc_clone(db->schema_views);
    out->identity = db->identity;
    out->node = jz_rc_clone(db->node);
    out->row_id_source = jz_rc_clone(db->row_id_source);
    out->row_id_source_guarantees_fresh = db->row_id_source_guarantees_fresh;
    out->next_now_ms = jz_rc_clone(db->next_now_ms);
    out->has_reserved_tx_id = false;
    out->owner_operation_admitted = false;
    out->backend_attribution = db->backend_attribution;
}

/* ===== impl<S> Db<S> (lines 63-126) ===== */

jazz_db_DbIdentity LC_FN(identity)(const jazz_db_Db *db) { return db->identity; }

void LC_FN(set_large_value_staging_policy)(const jazz_db_Db *db, const jazz_node_LargeValueStagingPolicy *policy) {
    NODE(set_large_value_staging_policy)(db->node, policy);
}

jz_err LC_FN(evict_expired_staged_large_values)(const jazz_db_Db *db, u32 *out) {
    return NODE(evict_expired_staged_large_values)(db->node, out);
}

/* ===== impl Db constructors (lines 128-379) ===== */

/* Wrap an opened node as a `Db` over `schema`, leaving a schema the node's
 * catalogue does not know yet pending open-time admission. */
static void LC_FN(from_opened_node)(jazz_node_NodeState *node_state, jazz_model_JazzSchema schema, jazz_db_DbIdentity identity,
                                    bool has_id_source, jazz_db_RowIdSource id_source, jazz_db_Db *out) {
    jazz_types_SchemaVersionId schema_version_id = jazz_model__schema__JazzSchema__version_id(&schema);
    jazz_db_SchemaViewsCell *schema_views = jz_rc_new(sizeof *schema_views, schema_views_cell_drop);
    jz_btree_init(&schema_views->map, &schema_views_desc);
    {
        jazz_db_SchemaViewId view_id = jazz_db__db__SchemaViewId__for_schema(&schema);
        jazz_model_JazzSchema copy;
        jazz_model__schema__JazzSchema__clone(&copy, &schema);
        jz_btree_insert(&schema_views->map, &view_id, &copy, NULL);
    }
    bool requires_open_schema_admission = NS(catalogue_schema)(node_state, &schema_version_id) == NULL;
    jazz_db_Node *node = NODE(new)(node_state);
    if (requires_open_schema_admission) {
        jazz_db_OpenSchemaAdmissionCell *cell = node->open_schema_admission;
        if (cell->has) jazz_db__db__PendingOpenSchema__drop(&cell->pending);
        jazz_db__db__PendingOpenSchema__new(&cell->pending, schema_version_id);
        cell->has = true;
    }
    bool row_id_source_guarantees_fresh = !has_id_source;
    jazz_db_RowIdSourceCell *row_id_source = jz_rc_new(sizeof *row_id_source, row_id_source_cell_drop);
    row_id_source->source = has_id_source ? id_source : jazz_db__db__config__ProductionRowIdSource__boxed();
    jazz_db_WriteClock *clock = jz_rc_new(sizeof *clock, NULL);
    jazz_db__db__WriteClock__default(clock);

    out->schema = schema;
    out->schema_version_id = schema_version_id;
    out->schema_view_is_fixed = false;
    out->requires_open_schema_admission = requires_open_schema_admission;
    out->schema_views = schema_views;
    out->identity = identity;
    out->node = node; /* Rc::new(node) (Node::new allocates the Rc payload) */
    out->row_id_source = row_id_source;
    out->row_id_source_guarantees_fresh = row_id_source_guarantees_fresh;
    out->next_now_ms = clock;
    out->has_reserved_tx_id = false;
    out->owner_operation_admitted = false;
    out->backend_attribution = false;
}

/* Open a database over the supplied storage and recover local state. */
jz_err LC_FN(open)(jazz_db_DbConfig config, jazz_db_Db *out) {
    jazz_node_NodeState *node;
    jazz_model_JazzSchema schema_clone;
    jazz_model__schema__JazzSchema__clone(&schema_clone, &config.schema);
    jz_err e = ne(NS(new_client)(config.identity.node, schema_clone, config.storage, false, &node));
    if (JZ_IS_ERR(e)) {
        jazz_model__schema__JazzSchema__drop(&config.schema);
        if (config.has_id_source && config.id_source.vt->drop) config.id_source.vt->drop(config.id_source.self);
        return e;
    }
    LC_FN(from_opened_node)(node, config.schema, config.identity, config.has_id_source, config.id_source, out);
    e = NODE(restore_pending_uploads)(out->node, out->identity);
    if (JZ_IS_ERR(e)) { jazz_db__db__Db__drop(out); return e; }
    return JZ_OK;
}

/* Open the durable, scope-admitted half of a client relay (unsafe host capability). */
jz_err LC_FN(open_scope_isolated_client_relay)(jazz_db_DbConfig config, jazz_node_ClientRelayScope *scope, jazz_db_Db *out) {
    JZ_TRY(LC_FN(open)(config, out));
    jz_err e = NODE(configure_scope_isolated_client_relay)(out->node, scope);
    if (JZ_IS_ERR(e)) { jazz_db__db__Db__drop(out); return e; }
    return JZ_OK;
}

/* Open a Db allowed to record external provenance (trusted backend). */
jz_err LC_FN(open_with_backend_attribution)(jazz_db_DbConfig config, jazz_db_Db *out) {
    JZ_TRY(LC_FN(open)(config, out));
    out->backend_attribution = true;
    jz_err e = NODE(restore_backend_pending_uploads)(out->node, out->identity.node);
    if (JZ_IS_ERR(e)) { jazz_db__db__Db__drop(out); return e; }
    return JZ_OK;
}

static jz_err LC_FN(open_history_complete_inner)(jazz_db_DbConfig config, bool recover_client, jazz_db_Db *out) {
    jazz_node_NodeState *node;
    jazz_model_JazzSchema schema_clone;
    jazz_model__schema__JazzSchema__clone(&schema_clone, &config.schema);
    jz_err e = recover_client
        ? NS(new_client)(config.identity.node, schema_clone, config.storage, true, &node)
        : NS(new_history_complete)(config.identity.node, schema_clone, config.storage, &node);
    e = ne(e);
    if (JZ_IS_ERR(e)) {
        jazz_model__schema__JazzSchema__drop(&config.schema);
        if (config.has_id_source && config.id_source.vt->drop) config.id_source.vt->drop(config.id_source.self);
        return e;
    }
    LC_FN(from_opened_node)(node, config.schema, config.identity, config.has_id_source, config.id_source, out);
    return JZ_OK;
}

/* Open a database as a history-complete serving core. */
jz_err LC_FN(open_history_complete)(jazz_db_DbConfig config, jazz_db_Db *out) {
    return LC_FN(open_history_complete_inner)(config, false, out);
}

jz_err LC_FN(open_history_complete_with_backend_attribution)(jazz_db_DbConfig config, jazz_db_Db *out) {
    JZ_TRY(LC_FN(open_history_complete_inner)(config, true, out));
    out->backend_attribution = true;
    jz_err e = NODE(restore_backend_pending_uploads)(out->node, out->identity.node);
    if (JZ_IS_ERR(e)) { jazz_db__db__Db__drop(out); return e; }
    return JZ_OK;
}

/* ===== schema views (lines 433-575) ===== */

static bool schema_policy_metadata_matches(const jazz_model_JazzSchema *left, const jazz_model_JazzSchema *right) {
    if (left->tables.len != right->tables.len) return false;
    JZ_VEC_FOR(const jazz_model_TableSchema, l, left->tables) {
        bool any = false;
        JZ_VEC_FOR(const jazz_model_TableSchema, r, right->tables) {
            if (jz_str_eq(&l->name, &r->name)
                && jazz_model__schema__TableSchema__read_policy_eq(l, r)
                && jazz_model__schema__TableSchema__write_policies_eq(l, r)) { any = true; break; }
        }
        if (!any) return false;
    }
    return true;
}

static bool schema_index_metadata_matches(const jazz_model_JazzSchema *left, const jazz_model_JazzSchema *right) {
    if (left->tables.len != right->tables.len) return false;
    JZ_VEC_FOR(const jazz_model_TableSchema, l, left->tables) {
        bool any = false;
        JZ_VEC_FOR(const jazz_model_TableSchema, r, right->tables) {
            if (jz_str_eq(&l->name, &r->name)
                && jazz_model__schema__TableSchema__indexed_columns_eq(l, r)
                && jazz_model__schema__TableSchema__composite_indexes_eq(l, r)) { any = true; break; }
        }
        if (!any) return false;
    }
    return true;
}

/* Admit the first application schema into an owner deliberately opened with
 * the empty schema; later schemas still arrive through catalogue lineage. */
static jz_err LC_FN(admit_local_schema_view_if_needed)(const jazz_db_Db *db, const jazz_model_JazzSchema *schema) {
    jazz_model_JazzSchema empty_schema;
    jazz_model__schema__JazzSchema__empty(&empty_schema);
    jazz_types_SchemaVersionId empty_id = jazz_model__schema__JazzSchema__version_id(&empty_schema);
    jazz_model__schema__JazzSchema__drop(&empty_schema);
    jazz_types_SchemaVersionId target_id = jazz_model__schema__JazzSchema__version_id(schema);
    jazz_model_JazzSchema source;
    u64 catalogue_seq;
    bool bootstrap_current;
    {
        jazz_node_NodeState *node = borrow_node(db, "admit_local_schema_view_if_needed");
        if (NS(catalogue_schema)(node, &target_id) != NULL) { unlock_node(db); return JZ_OK; }
        jazz_types_SchemaVersionId current;
        jz_err e = ne(NS(current_write_schema)(node, &current));
        if (JZ_IS_ERR(e)) { unlock_node(db); return e; }
        const jazz_node_CatalogueSchemaEntry *version = NS(catalogue_schema)(node, &current);
        if (!version) { unlock_node(db); return jz_err_new(JAZZ_EC_SCHEMA, "current schema view is missing"); }
        jazz_model__schema__JazzSchema__clone(&source, &version->schema);
        u64 seq = NS(active_catalogue_seq)(node);
        catalogue_seq = seq == UINT64_MAX ? seq : seq + 1; /* saturating_add(1) */
        bootstrap_current = memcmp(&current, &empty_id, sizeof current) == 0 && NS(catalogue_schemas_len)(node) == 1;
        unlock_node(db);
    }
    jazz_protocol_MigrationLens *lens;
    jazz_db_StrVec new_tables = {0}, dropped_tables = {0};
    jz_err e = jazz_db__db__direct_schema_view_lens(&source, schema, &lens, &new_tables, &dropped_tables);
    jazz_model__schema__JazzSchema__drop(&source);
    JZ_TRY(e);
    jazz_db_bPublicationOutcome outcome;
    {
        jazz_node_NodeState *node = lock_node(db);
        jazz_model_JazzSchema schema_clone;
        jazz_model__schema__JazzSchema__clone(&schema_clone, schema);
        jazz_node_SchemaLineagePublication *publication;
        e = ne(NS(author_schema_lineage_publication)(node, jazz_model__schema__SchemaVersion__new(schema_clone), lens,
                                                    new_tables, dropped_tables, &publication));
        if (JZ_IS_ERR(e)) { unlock_node(db); return e; }
        jazz_protocol_SyncMessage message;
        jazz_protocol__protocol__SyncMessage__publish_schema_with_lens(&message, jazz_types__ids__AuthorSubject__SYSTEM, catalogue_seq, publication);
        e = ne(NS(apply_trusted_catalogue_message)(node, message, &outcome));
        unlock_node(db);
        JZ_TRY(e);
    }
    JZ_TRY(MUT(finish_publication_outcome)(db, outcome));
    if (bootstrap_current) {
        jazz_node_NodeState *node = lock_node(db);
        jazz_model_JazzSchema schema_clone;
        jazz_model__schema__JazzSchema__clone(&schema_clone, schema);
        e = ne(NS(activate_schema)(node, 1, schema_clone));
        unlock_node(db);
        JZ_TRY(e);
    }
    return JZ_OK;
}

/* Register a typed schema view on this database owner. Registration is
 * process-local and idempotent by the exact typed schema content. */
jz_err LC_FN(register_schema_view)(const jazz_db_Db *db, jazz_model_JazzSchema schema, jazz_db_Db *out) {
    jazz_types_SchemaVersionId schema_version_id = jazz_model__schema__JazzSchema__version_id(&schema);
    jazz_db_SchemaViewId schema_view_id = jazz_db__db__SchemaViewId__for_schema(&schema);
    jz_err e;
    /* A replica opened ahead of its durable catalogue must first receive the
     * published lineage; view registration cannot manufacture it. */
    e = MUT(ensure_open_schema_admitted)(db);
    if (!JZ_IS_ERR(e)) e = LC_FN(admit_local_schema_view_if_needed)(db, &schema);
    if (JZ_IS_ERR(e)) { jazz_model__schema__JazzSchema__drop(&schema); return e; }
    {
        jazz_node_NodeState *node = borrow_node(db, "register_schema_view");
        const jazz_model_JazzSchema *admitted = NS(schema_with_active_permissions)(node, schema_version_id);
        if (!admitted) e = jz_err_new(JAZZ_EC_SCHEMA, "registered schema is missing");
        else if (!schema_policy_metadata_matches(admitted, &schema))
            e = jz_err_new(JAZZ_EC_SCHEMA, "schema view policy metadata conflicts with its admitted structural schema");
        else if (!schema_index_metadata_matches(admitted, &schema))
            e = jz_err_new(JAZZ_EC_SCHEMA, "schema view index metadata conflicts with its admitted structural schema");
        unlock_node(db);
        if (JZ_IS_ERR(e)) { jazz_model__schema__JazzSchema__drop(&schema); return e; }
    }
    jz_btree *views = &db->schema_views->map;
    const jazz_model_JazzSchema *existing = jz_btree_get(views, &schema_view_id);
    if (existing) {
        if (!jazz_model__schema__JazzSchema__eq(existing, &schema)) {
            jz_str dbg = {0};
            jazz_db__db__SchemaViewId__debug(&dbg, &schema_view_id);
            e = jz_err_fmt(JAZZ_EC_SCHEMA, "schema view id collision for %.*s", (int)dbg.len, (const char *)dbg.ptr);
            jz_str_drop(&dbg);
            jazz_model__schema__JazzSchema__drop(&schema);
            return e;
        }
    } else {
        jazz_model_JazzSchema copy;
        jazz_model__schema__JazzSchema__clone(&copy, &schema);
        jz_btree_insert(views, &schema_view_id, &copy, NULL);
    }
    clone_shared(db, out);
    jazz_model__schema__JazzSchema__drop(&out->schema);
    out->schema = schema;
    out->schema_version_id = schema_version_id;
    out->schema_view_is_fixed = true;
    out->requires_open_schema_admission = false;
    return JZ_OK;
}

/* Attach an already-registered typed schema view to this owner. */
jz_err LC_FN(schema_view)(const jazz_db_Db *db, jazz_db_SchemaViewId schema_view_id, jazz_db_Db *out) {
    const jazz_model_JazzSchema *found = jz_btree_get(&db->schema_views->map, &schema_view_id);
    if (!found) {
        jz_str dbg = {0};
        jazz_db__db__SchemaViewId__debug(&dbg, &schema_view_id);
        jz_err e = jz_err_fmt(JAZZ_EC_SCHEMA, "schema view %.*s is not registered", (int)dbg.len, (const char *)dbg.ptr);
        jz_str_drop(&dbg);
        return e;
    }
    jazz_model_JazzSchema schema;
    jazz_model__schema__JazzSchema__clone(&schema, found);
    return LC_FN(register_schema_view)(db, schema, out);
}

/* Canonical id of this handle's typed schema view. */
jazz_db_SchemaViewId LC_FN(schema_view_id)(const jazz_db_Db *db) {
    return jazz_db__db__SchemaViewId__for_schema(&db->schema);
}

/* ===== close and owner controls (lines 575-670) ===== */

/* Close maintenance admission, terminalize open transactions and streams,
 * flush node-local state and close storage. */
jz_err LC_FN(close)(const jazz_db_Db *db) {
    if (db->schema_view_is_fixed) return JZ_OK;
    jazz_db_Node *n = db->node;
    NODE(begin_mutation_shutdown)(n);
    /* Transaction admission and Drop maintenance cross their shutdown boundary
     * before close can suspend. */
    NODE(begin_transaction_abandonment_shutdown)(n);
    /* Capture every live stream before waiting for a close owner. */
    NODE(begin_subscription_finalization_shutdown)(n);
    NODE(lock_close_owner)(n); /* let _close_owner = ... */
    jz_err e;
    /* Await every accepted operation, in FIFO order, before storage retires. */
    NODE(drain_queued_mutations)(n);
    /* Terminalize waits that cannot progress after retirement. */
    NODE(drain_transaction_wait_observers)(n);
    /* Acknowledge durable rejections claimed by drained waiters. */
    e = NODE(flush_deferred_rejection_discards)(n);
    if (JZ_IS_ERR(e)) goto out;
    {
        u32 swept;
        e = NODE(finish_transaction_abandonment_shutdown)(n, &swept);
        if (JZ_IS_ERR(e)) goto out;
        e = NODE(drain_subscription_finalizations)(n, &swept);
        if (JZ_IS_ERR(e)) goto out;
    }
    /* Query evaluation detached on a remote chunk cannot finish once this
     * runtime closes; end it rather than wait for a reconnect. */
    jazz_db__db__PeerChunkResolver__fail_local_demand_for_close(&n->chunk_resolver);
    {
        jazz_node_NodeState *node = lock_node(db);
        e = ne(NS(close)(node));
        unlock_node(db);
        if (JZ_IS_ERR(e)) goto out;
    }
    NODE(retire_subscription_runtime_after_close)(n);
out:
    NODE(release_close_owner)(n);
    return e;
}

/* Configure this database as an optimistic foreground whose upstream owns Local durability. */
void LC_FN(set_non_durable_client)(const jazz_db_Db *db) { NODE(set_non_durable_client)(db->node); }

/* Highest transaction HLC observed by this live runtime (lease handoff). */
jazz_types_TxTime LC_FN(foreground_tx_time_high_water)(const jazz_db_Db *db) {
    jazz_node_NodeState *node = lock_node(db);
    jazz_types_TxTime t = NS(tx_time_high_water)(node);
    unlock_node(db);
    return t;
}

/* Seed a foreground runtime from the durable lease owner's high-water mark. */
void LC_FN(seed_foreground_tx_time_high_water)(const jazz_db_Db *db, jazz_types_TxTime high_water) {
    jazz_node_NodeState *node = lock_node(db);
    NS(seed_tx_time_high_water)(node, high_water);
    unlock_node(db);
}

/* Reserve a definitive local transaction identity for an owner-retained mutation. */
jz_err LC_FN(reserve_transaction_id_at_ms)(const jazz_db_Db *db, u64 now_ms, jazz_model_TxId *out) {
    return NODE(reserve_transaction_id)(db->node, now_ms, out);
}

/* Drive one bounded resident mutation turn without awaiting cold work. */
void LC_FN(drive_queued_mutation_once)(const jazz_db_Db *db) { (void)NODE(poll_queued_mutation_once)(db->node); }

u32 LC_FN(queued_mutation_count)(const jazz_db_Db *db) { return NODE(queued_mutation_count)(db->node); }

jazz_db_Oneshot *LC_FN(queued_mutation_barrier)(const jazz_db_Db *db) { return NODE(queued_mutation_barrier)(db->node); }

bool LC_FN(queued_transaction_error)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_err *out) {
    return NODE(queued_transaction_error)(db->node, id, out);
}

/* enqueue_streaming_value_upload_cleanup: async move { node.lock().await.evict_pending_large_value_upload(id) } */
typedef struct UploadCleanupFuture { jazz_db_SharedNodeState *node; groove_StagedLargeValueId id; } UploadCleanupFuture;
static bool upload_cleanup_poll(void *self, const jz_waker *cx, void *out) {
    UploadCleanupFuture *f = self;
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(f->node);
    jz_err e = ne(NS(evict_pending_large_value_upload)(node, f->id));
    jazz_db__db__SharedNodeState__unlock(f->node);
    *(jz_err *)out = e;
    return true;
}
static void upload_cleanup_drop(void *self) {
    UploadCleanupFuture *f = self;
    jz_rc_release(f->node);
    jz_free(f);
}
static void upload_cleanup_complete(void *ctx, jz_err outcome) {
    jazz_db_SharedResultCell *cell = ctx;
    cell->ready = true;
    cell->result = outcome;
}
static void upload_cleanup_complete_drop(void *ctx) { jz_rc_release(ctx); }

jazz_db_StreamingValueUploadCleanupTicket LC_FN(enqueue_streaming_value_upload_cleanup)(const jazz_db_Db *db, jazz_db_StreamingValueUpload *upload) {
    jazz_db_SharedResultCell *result = jz_rc_new(sizeof *result, NULL);
    result->ready = false;
    result->sender_dropped = false;
    groove_StagedLargeValueId id = jazz_db__db__mutations__StreamingValueUpload__cleanup_id(upload);
    UploadCleanupFuture *f = jz_alloc(sizeof *f);
    f->node = jz_rc_clone(db->node->node);
    f->id = id;
    jazz_db_LocalFuture future = { f, upload_cleanup_poll, upload_cleanup_drop };
    jazz_db_bQueuedMutationCompletion completion = { upload_cleanup_complete, jz_rc_clone(result), upload_cleanup_complete_drop };
    NODE(enqueue_transaction_cleanup_with_completion)(db->node, future, &completion);
    jazz_db_StreamingValueUploadCleanupTicket ticket = { result };
    return ticket;
}

/* Drain operations admitted by a binding without closing mutation admission. */
void LC_FN(drain_queued_mutations_for_binding)(const jazz_db_Db *db) { NODE(drain_queued_mutations)(db->node); }

/* Queue a transaction-local read behind already-admitted transaction work. */
jazz_db_Oneshot *LC_FN(enqueue_transaction_read)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jazz_db_LocalFuture read, u32 out_size) {
    return NODE(enqueue_transaction_read)(db->node, tx_id, read, out_size);
}

void LC_FN(clone_for_reserved_transaction)(const jazz_db_Db *db, jazz_model_TxId tx_id, jazz_db_Db *out) {
    clone_shared(db, out);
    out->has_reserved_tx_id = true;
    out->reserved_tx_id = tx_id;
    out->owner_operation_admitted = true;
}

void LC_FN(clone_for_owner_operation)(const jazz_db_Db *db, jazz_db_Db *out) {
    clone_shared(db, out);
    out->has_reserved_tx_id = false;
    out->owner_operation_admitted = true;
}

jz_err LC_FN(ensure_mutation_operation_admitted)(const jazz_db_Db *db) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    if (db->owner_operation_admitted) return JZ_OK;
    return NODE(ensure_mutation_admission_open)(db->node);
}

/* Restore unsettled writes relayed from a browser client sharing this worker's author. */
jz_err LC_FN(restore_browser_relay_pending_uploads)(const jazz_db_Db *db) {
    return NODE(restore_browser_relay_pending_uploads)(db->node, db->identity.author);
}

void LC_FN(set_deferred_local_persistence)(const jazz_db_Db *db, bool deferred) {
    NODE(set_deferred_local_persistence)(db->node, deferred);
}

/* Configure this client database's first-snapshot durability cadence. */
jz_err LC_FN(set_initial_sync_flush_cadence)(const jazz_db_Db *db, jazz_db_InitialSyncFlushCadence cadence) {
    jazz_node_NodeState *node = borrow_node(db, "set_initial_sync_flush_cadence");
    jz_err e = ne(NS(set_initial_sync_flush_cadence)(node, cadence.writes));
    unlock_node(db);
    return e;
}

/* Seed a settled mergeable row for server bootstrap/import flows. */
jz_err LC_FN(seed_settled_mergeable_for_bootstrap)(const jazz_db_Db *db, jz_slice table, jazz_types_RowUuid row,
                                                   jazz_types_AuthorSubject made_by, jz_btree cells, jazz_model_TxId *out) {
    jazz_model_JazzSchema *write_schema;
    jazz_types_SchemaVersionId write_schema_version;
    jz_err e = MUT(current_write_schema_for_query)(db, &write_schema, &write_schema_version);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&cells); return e; }
    const jazz_model_TableSchema *table_schema = NULL;
    JZ_VEC_FOR(const jazz_model_TableSchema, candidate, write_schema->tables) {
        if (candidate->name.len == table.len && memcmp(candidate->name.ptr, table.ptr, table.len) == 0) { table_schema = candidate; break; }
    }
    if (!table_schema) {
        jz_btree_drop(&cells);
        return jz_err_fmt(JAZZ_EC_SCHEMA, "unknown table %.*s", (int)table.len, (const char *)table.ptr);
    }
    JZ_VEC_FOR(const jazz_model_ColumnSchema, column, table_schema->columns) {
        if (!jz_btree_contains(&cells, &column->name) && column->has_default) {
            jz_str key = jz_str_clone(&column->name);
            groove_Value v = jazz_db__db__default_cell_for_column_type(&column->column_type, &column->default_);
            jz_btree_insert(&cells, &key, &v, NULL);
        }
    }
    jazz_node_PublishedTransaction published;
    {
        jazz_node_NodeState *node = borrow_node(db, "seed_settled_mergeable_for_bootstrap");
        jazz_node_MergeableCommit *commit = jazz_node__node__MergeableCommit__new(table, row, MUT(next_now_ms)(db));
        jazz_node__node__MergeableCommit__made_by(commit, made_by);
        jazz_node__node__MergeableCommit__cells(commit, cells);
        e = ne(NS(commit_mergeable_in_schema)(node, write_schema_version, commit, &published));
        unlock_node(db);
        JZ_TRY(e);
    }
    jazz_model_TxId tx_id = published.tx_id;
    {
        jazz_db_bPublicationOutcome outcome;
        jazz_node_PublishedTransaction *boxed = jz_alloc(sizeof *boxed);
        *boxed = published;
        jazz_node__node__PublicationOutcome__published(&outcome, NULL, 0, boxed);
        JZ_TRY(MUT(finish_publication_outcome)(db, outcome));
    }
    {
        jazz_db_bPublicationOutcome outcome;
        jazz_node_NodeState *node = borrow_node(db, "seed_settled_mergeable_for_bootstrap");
        e = ne(NS(finalize_local_mergeable_commit)(node, tx_id, &outcome));
        unlock_node(db);
        JZ_TRY(e);
        JZ_TRY(MUT(finish_publication_outcome)(db, outcome));
    }
    NODE(mark_subscriber_connections_dirty)(db->node);
    *out = tx_id;
    return JZ_OK;
}

/* Test/bench-only authority finalization (not cfg-gated in Rust, so kept). */
jz_err LC_FN(finalize_local_mergeable_commit_for_test)(const jazz_db_Db *db, jazz_model_TxId tx_id) {
    jazz_db_bPublicationOutcome outcome;
    jazz_node_NodeState *node = borrow_node(db, "finalize_local_mergeable_commit_for_test");
    jz_err e = ne(NS(finalize_local_mergeable_commit)(node, tx_id, &outcome));
    unlock_node(db);
    JZ_TRY(e);
    JZ_TRY(MUT(finish_publication_outcome)(db, outcome));
    NODE(mark_subscriber_connections_dirty)(db->node);
    return JZ_OK;
}

/* ===== write state (lines 905-1045) ===== */

static jz_err not_known_locally(jazz_model_TxId tx_id) {
    jz_str dbg = {0};
    jazz_model__tx__TxId__debug(&dbg, &tx_id);
    jz_err e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "transaction %.*s is not known locally", (int)dbg.len, (const char *)dbg.ptr);
    jz_str_drop(&dbg);
    return e;
}

static jz_err write_state_from_node(jazz_node_NodeState *node, jazz_model_TxId tx_id, jazz_db_WriteState *out) {
    jazz_model_Fate fate;
    bool has_global_time = false;
    u64 global_time = 0;
    u32 durability = 0;
    if (!NS(transaction_state)(node, tx_id, &fate, &has_global_time, &global_time, &durability)) return not_known_locally(tx_id);
    out->fate = fate;
    out->has_global_time = has_global_time;
    memcpy(&out->global_time, &global_time, sizeof global_time < sizeof out->global_time ? sizeof global_time : sizeof out->global_time);
    out->durability = durability;
    return JZ_OK;
}

/* Return the locally observed fate and durability for a write transaction. */
jz_err LC_FN(write_state)(const jazz_db_Db *db, jazz_model_TxId tx_id, jazz_db_WriteState *out) {
    jz_err queued;
    if (NODE(queued_mutation_write_state)(db->node, tx_id, out, &queued)) return queued;
    jazz_node_NodeState *node = borrow_node(db, "write_state");
    jz_err e = write_state_from_node(node, tx_id, out);
    unlock_node(db);
    return e;
}

/* Read write state without synchronously reentering a suspended owner. */
jz_err LC_FN(write_state_async)(const jazz_db_Db *db, jazz_model_TxId tx_id, jazz_db_WriteState *out) {
    jz_err queued;
    if (NODE(queued_mutation_write_state)(db->node, tx_id, out, &queued)) return queued;
    jazz_node_NodeState *node = lock_node(db);
    jz_err e = write_state_from_node(node, tx_id, out);
    unlock_node(db);
    return e;
}

/* Consume a failure discovered by a binding-owned bounded admission turn. */
bool LC_FN(take_queued_mutation_failure)(const jazz_db_Db *db, jazz_model_TxId tx_id, jz_err *out) {
    return NODE(take_queued_mutation_failure)(db->node, tx_id, out);
}

/* Wait until `tx_id` reaches `tier` or is rejected. */
jz_err LC_FN(wait_for_transaction)(const jazz_db_Db *db, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions options, jazz_model_TxId *out) {
    return NODE(wait_for_transaction)(db->node, tx_id, options, out);
}

/* Wait for this author's pending writes, including recovered durable writes. */
jz_err LC_FN(wait_for_pending_writes)(const jazz_db_Db *db, u32 tier) {
    JZ_TRY(NODE(settle_local_publications)(db->node));
    jazz_db_TxIdVec pending = {0};
    {
        jazz_node_NodeState *node = lock_node(db);
        jz_err e = db->backend_attribution
            ? NS(synchronizing_transaction_ids_for_node)(node, db->identity.node, &pending)
            : NS(synchronizing_transaction_ids_for_author)(node, db->identity.author, &pending);
        unlock_node(db);
        e = ne(e);
        JZ_TRY(e);
    }
    jz_err e = JZ_OK;
    JZ_VEC_FOR(jazz_model_TxId, tx_id, pending) {
        jazz_model_TxId done;
        e = LC_FN(wait_for_transaction)(db, *tx_id, jazz_db__db__WriteWaitOptions__from_tier(tier), &done);
        if (JZ_IS_ERR(e)) break;
    }
    jz_vec_drop(&pending, sizeof(jazz_model_TxId), NULL);
    return e;
}

/* Callback form of wait_for_transaction for bindings. */
void LC_FN(wait_for_transaction_with)(const jazz_db_Db *db, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions options, jazz_db_bTxIdCallback callback) {
    NODE(wait_for_transaction_with)(db->node, tx_id, options, callback);
}

/* Binding-only callback wait that preserves a queued write handle's completion target. */
void LC_FN(wait_for_write_with)(const jazz_db_Db *db, const jazz_db_WriteHandle *write, jazz_db_WriteWaitOptions options, jazz_db_bTxIdCallback callback) {
    jazz_db_QueuedMutationAlias alias_copy, *alias = NULL;
    if (write->queued_alias) { alias_copy = *write->queued_alias; alias = &alias_copy; }
    NODE(wait_for_write_with)(db->node, write->tx_id, alias, options, callback);
}

/* Wake primitive for the next state transition of `tx_id`. */
jazz_db_WriteStateChange LC_FN(next_write_state_change)(const jazz_db_Db *db, jazz_model_TxId tx_id) {
    return NODE(register_write_state_waiter)(db->node, tx_id);
}

void LC_FN(on_mutation_error)(const jazz_db_Db *db, jazz_db_MutationErrorCallback callback) {
    NODE(set_mutation_error_callback)(db->node, &callback);
}

void LC_FN(clear_mutation_error_callback)(const jazz_db_Db *db) {
    NODE(set_mutation_error_callback)(db->node, NULL);
}

/* ===== connections (lines 1053-1330) ===== */

/* Attach this Db to an upstream peer over a binding-supplied transport. */
jazz_db_PeerMutex *LC_FN(connect_upstream)(const jazz_db_Db *db, jazz_db_Transport transport) {
    jazz_db_PeerMutex *conn = NULL;
    (void)NODE(connect_upstream)(db->node, transport, &conn);
    return conn;
}

/* Attach after asynchronously loading parked replay units (strict entry point). */
jz_err LC_FN(try_connect_upstream)(const jazz_db_Db *db, jazz_db_Transport transport, jazz_db_PeerMutex **out) {
    return NODE(try_connect_upstream)(db->node, transport, out);
}

void LC_FN(set_tick_scheduler)(const jazz_db_Db *db, const jazz_db_TickScheduler *scheduler) {
    NODE(set_scheduler)(db->node, scheduler);
}

void LC_FN(mark_subscriber_connections_dirty_after_query_runtime_wake)(const jazz_db_Db *db) {
    NODE(mark_subscriber_connections_dirty_after_query_runtime_wake)(db->node);
}

void LC_FN(schedule_tick)(const jazz_db_Db *db, u32 urgency) { NODE(schedule_tick)(db->node, urgency); }

jazz_db_PermissionAdviceFuture LC_FN(request_permission_advice)(const jazz_db_Db *db, jazz_protocol_PermissionAdviceAction action) {
    return NODE(request_permission_advice)(db->node, &action);
}

jazz_db_PermissionAdviceFuture LC_FN(request_permission_advice_with_delegated_session)(const jazz_db_Db *db, jazz_protocol_PermissionAdviceAction action,
                                                                                       jazz_protocol_DelegatedSessionBinding session) {
    return NODE(request_permission_advice_with_delegated_session)(db->node, &action, session);
}

void LC_FN(cancel_permission_advice_request)(const jazz_db_Db *db, jazz_protocol_PermissionAdviceRequestId request_id) {
    NODE(cancel_permission_advice_request)(db->node, request_id);
}

jazz_db_PeerMutex *LC_FN(accept_subscriber)(const jazz_db_Db *db, jazz_db_Transport transport, jazz_types_AuthorSubject identity) {
    return NODE(accept_subscriber)(db->node, transport, identity);
}

/* Server/host-only scope-relay admission after authenticated handshake. */
jazz_db_PeerMutex *LC_FN(accept_scope_isolated_relay_subscriber)(const jazz_db_Db *db, jazz_db_Transport transport, jazz_types_AuthorSubject identity,
                                                                 jz_btree claims, u64 admission_epoch) {
    return NODE(accept_scope_isolated_relay_subscriber)(db->node, transport, identity, claims, admission_epoch);
}

jazz_db_PeerMutex *LC_FN(accept_subscriber_with_claims)(const jazz_db_Db *db, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jz_btree claims) {
    return NODE(accept_subscriber_with_claims)(db->node, transport, identity, claims);
}

/* Admit a subscriber after its semantic owner and local replay are ready.
 * (Node's async variant takes the trust mode; lifecycle passes Session.) */
jz_err LC_FN(accept_subscriber_with_claims_async)(const jazz_db_Db *db, jazz_db_Transport transport, jazz_types_AuthorSubject identity,
                                                  jz_btree claims, jazz_db_PeerMutex **out) {
    return NODE(accept_subscriber_with_claims_async)(db->node, transport, identity, claims, JAZZ_CUT_SESSION, out);
}

jazz_db_PeerMutex *LC_FN(accept_subscriber_with_claims_and_trust)(const jazz_db_Db *db, jazz_db_Transport transport, jazz_types_AuthorSubject identity,
                                                                  jz_btree claims, jazz_node_CommitUnitTrust trust) {
    return NODE(accept_subscriber_with_claims_and_trust)(db->node, transport, identity, claims, trust);
}

jazz_db_PeerMutex *LC_FN(accept_subscriber_with_resume)(const jazz_db_Db *db, jazz_db_Transport transport, jazz_types_AuthorSubject identity,
                                                        jazz_protocol_ResumeCursor *cursor) {
    return NODE(accept_subscriber_with_resume)(db->node, transport, identity, cursor);
}

bool LC_FN(detach_connection)(const jazz_db_Db *db, jazz_db_PeerMutex *connection) {
    return NODE(detach_connection)(db->node, connection);
}

jz_err LC_FN(detach_connection_async)(const jazz_db_Db *db, jazz_db_PeerMutex *connection, bool *out) {
    return NODE(detach_connection_async)(db->node, connection, out);
}

/* ===== tick (lines 1329-1432) ===== */

/* A durable rejection acknowledgement is part of the owner turn; a failure is
 * surfaced instead of scheduling an unbounded retry loop. */
static jz_err LC_FN(flush_deferred_rejection_discards_after_tick)(const jazz_db_Db *db) {
    return NODE(flush_deferred_rejection_discards)(db->node);
}

/* Shared body of tick_inner / tick_stats_inner. */
static jz_err tick_body(const jazz_db_Db *db, jazz_db_DbTickStats *stats) {
    jazz_db_Node *n = db->node;
    if (NODE(owner_is_available)(n)) NODE(deliver_pending_mutation_errors)(n);
    /* Advance both retained queues: a later cold mutation can own the node lock
     * needed to settle an earlier publication. */
    if (NODE(has_pending_local_publications)(n)) {
        bool pending;
        JZ_TRY(NODE(poll_local_publication_settlement_once)(n, &pending));
    }
    (void)NODE(poll_queued_mutation_once)(n);
    NODE(poll_transaction_wait_observers)(n);
    /* A retained operation may need the next owner/peer turn to release this lock. */
    if (!NODE(owner_is_available_or_wake_when_released)(n)) return JZ_OK;
    JZ_TRY(LC_FN(flush_deferred_rejection_discards_after_tick)(db));
    u32 drained;
    JZ_TRY(NODE(drain_subscription_finalizations)(n, &drained));
    if (NODE(has_pending_local_publications)(n)) JZ_TRY(NODE(settle_local_publications)(n));
    JZ_TRY(NODE(tick)(n, stats));
    NODE(poll_transaction_wait_observers)(n);
    if (NODE(owner_is_available_or_wake_when_released)(n)) JZ_TRY(LC_FN(flush_deferred_rejection_discards_after_tick)(db));
    return JZ_OK;
}

/* Service every connection once. */
jz_err LC_FN(tick)(const jazz_db_Db *db) {
    jazz_db_DbTickStats stats = {0};
    return tick_body(db, &stats);
}

/* Service every connection once and return binding-observable wake counts. */
jz_err LC_FN(tick_stats)(const jazz_db_Db *db, jazz_db_DbTickStats *out) {
    jazz_db_DbTickStats stats = {0}; /* DbTickStats::default() on early return */
    jz_err e = tick_body(db, &stats);
    if (!JZ_IS_ERR(e)) *out = stats;
    return e;
}

jz_err LC_FN(refresh_subscriptions_for_tables)(const jazz_db_Db *db, const jz_hmap *changed_tables, u32 *out) {
    u32 refreshed = 0;
    JZ_TRY(NODE(refresh_subscriptions_with_tables)(db->node, changed_tables, &refreshed));
    if (refreshed > 0) NODE(mark_subscriber_connections_dirty)(db->node);
    *out = refreshed;
    return JZ_OK;
}

jz_err LC_FN(refresh_subscriptions)(const jazz_db_Db *db, u32 *out) {
    return LC_FN(refresh_subscriptions_for_tables)(db, NULL, out);
}
