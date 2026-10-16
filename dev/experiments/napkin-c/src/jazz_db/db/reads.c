/* reads.c: port of crates/jazz/layers/db/src/db/reads.rs
 * Prepared, one-shot, relation, and result-tree read APIs.
 *
 * Async methods run synchronously; `poll_fn` waits become loops that drive
 * jz_task_run_ready() between polls, and futures raced against remote-link
 * loss are jazz_db_LocalFuture closures. */
#include "jz_rt.h"
#include "../db.h"
#include "first_load.h"
#include "transactions.h"

#define RD_FN(n) jazz_db__db__reads__Db__##n
#define NS(n) jazz_node__node__NodeState__##n
#define NODE(n) jazz_db__db__node_runtime__Node__##n
#define MUT(n) jazz_db__db__mutations__Db__##n
#define SUBS(n) jazz_db__db__subscriptions__Db__##n
#define TX(n) jazz_db__db__transactions__Db__##n

static jz_err ne(jz_err e) { return JZ_IS_ERR(e) ? jazz_node__node__api_error__Error__from_node_error(e) : e; }
static jz_err qe(jz_err e) { return JZ_IS_ERR(e) ? jazz_node__node__api_error__Error__from_query_error(e) : e; }
static void unlock_node(const jazz_db_Db *db) { jazz_db__db__SharedNodeState__unlock(db->node->node); }
static jz_err coverage_timed_out(void) { return jz_err_new(JAZZ_EC_NOT_OBSERVED, "Timed out waiting for query coverage"); }

/* BTreeSet<String> */
static void str_set_key_drop(void *k) { jz_str_drop((jz_str *)k); }
const jz_btree_desc jazz_db_str_set_desc = { sizeof(jz_str), 0, jz_cmp_str, str_set_key_drop, NULL };

/* Binding-only classification for a value whose referenced immutable content
 * has not arrived locally yet. */
typedef enum { JAZZ_BHE_RETRYABLE_CHUNK_UNAVAILABLE, JAZZ_BHE_ERROR } jazz_db_BindingHydrationErrorTag;
typedef struct jazz_db_BindingHydrationError { u32 tag; u32 retry_after_ms; jz_err error; } jazz_db_BindingHydrationError;

/* SerializedReadCoverage<F: FnOnce(QueryAttachment)> */
typedef void (*jazz_db_ReleaseCoverageFn)(void *ctx, jazz_db_QueryAttachment attachment);
typedef bool (*jazz_db_CoverageExpiredFn)(void *ctx);
typedef struct jazz_db_SerializedReadCoverage {
    bool has_attachment; jazz_db_QueryAttachment attachment;
    jazz_db_ReleaseCoverageFn release; void *release_ctx; /* NULL release = None */
} jazz_db_SerializedReadCoverage;

static void SerializedReadCoverage__Drop__drop(jazz_db_SerializedReadCoverage *c) {
    if (c->has_attachment && c->release) {
        c->has_attachment = false;
        jazz_db_ReleaseCoverageFn release = c->release;
        c->release = NULL;
        release(c->release_ctx, c->attachment);
    } else if (c->has_attachment) {
        jazz_db__db__QueryAttachment__drop(&c->attachment);
        c->has_attachment = false;
    }
}

static jazz_db_BindingHydrationError binding_hydration_error(jz_err error) {
    jazz_db_BindingHydrationError out = {0};
    u32 retry_after_ms;
    if (jazz_node__node__Error__chunk_retryable(&error, &retry_after_ms)) {
        jz_err_drop(&error);
        out.tag = JAZZ_BHE_RETRYABLE_CHUNK_UNAVAILABLE;
        out.retry_after_ms = retry_after_ms;
        return out;
    }
    out.tag = JAZZ_BHE_ERROR;
    if (jazz_node__node__Error__chunk_unavailable(&error)) {
        jz_err_drop(&error);
        out.error = jz_err_new(JAZZ_EC_NOT_OBSERVED, "large-value chunk is permanently unavailable");
    } else {
        out.error = jazz_node__node__api_error__Error__from_node_error(error);
    }
    return out;
}

/* Start a query rooted at `table`. */
jazz_model_Query *RD_FN(table)(const jazz_db_Db *db, jz_str table) {
    return jazz_model__query__Query__from(table);
}

static jz_err prepared_plans(jazz_node_NodeState *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_node_PreparedQueryPlanHandle *out[2]);

jz_err RD_FN(prepare_query_bound_for_schema)(const jazz_db_Db *db, const jazz_model_Query *query, jz_btree params, const jazz_model_JazzSchema *schema, jazz_types_SchemaVersionId schema_version, jazz_db_PreparedQuery *out);

/* Prepare a query for repeated reads or subscriptions. */
jz_err RD_FN(prepare_query_bound)(const jazz_db_Db *db, const jazz_model_Query *query, jz_btree params, jazz_db_PreparedQuery *out) {
    jazz_model_JazzSchema *schema;
    jazz_types_SchemaVersionId schema_version;
    jz_err e = MUT(current_write_schema_for_query)(db, &schema, &schema_version);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&params); return e; }
    e = RD_FN(prepare_query_bound_for_schema)(db, query, params, schema, schema_version, out);
    jazz_model__schema__JazzSchema__drop(schema);
    jz_free(schema);
    return e;
}

jz_err RD_FN(prepare_query)(const jazz_db_Db *db, const jazz_model_Query *query, jazz_db_PreparedQuery *out) {
    jz_btree params;
    jz_btree_init(&params, &jazz_db_str_value_desc);
    return RD_FN(prepare_query_bound)(db, query, params, out);
}

static jz_err RD_FN(prepare_query_async_for)(const jazz_db_Db *db, const jazz_model_Query *query, bool open_schema, jazz_db_PreparedQuery *out) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    jazz_model_JazzSchema schema;
    jazz_types_SchemaVersionId schema_version;
    jz_err e = JZ_OK;
    if (open_schema) {
        jazz_model__schema__JazzSchema__clone(&schema, &db->schema);
        schema_version = db->schema_version_id;
    } else {
        jazz_types_SchemaVersionId current;
        e = ne(NS(current_write_schema)(node, &current));
        if (JZ_IS_ERR(e)) { unlock_node(db); return e; }
        if (memcmp(&current, &db->schema_version_id, sizeof current) == 0) {
            jazz_model__schema__JazzSchema__clone(&schema, &db->schema);
        } else {
            const jazz_node_CatalogueSchemaEntry *entry = NS(catalogue_schema)(node, &current);
            if (!entry) {
                unlock_node(db);
                return jz_err_new(JAZZ_EC_SCHEMA, "current write schema is missing from catalogue");
            }
            jazz_model__schema__JazzSchema__clone(&schema, &entry->schema);
        }
        schema_version = current;
    }
    jazz_model_ValidatedQuery *shape = NULL;
    jazz_model_Binding *binding = NULL;
    e = qe(jazz_model__query__Query__validate_with_schema_version(query, &schema, schema_version, &shape));
    jazz_model__schema__JazzSchema__drop(&schema);
    if (!JZ_IS_ERR(e)) {
        jz_btree params;
        jz_btree_init(&params, &jazz_db_str_value_desc);
        e = qe(jazz_model__query__ValidatedQuery__bind(shape, params, &binding));
    }
    jazz_node_PreparedQueryPlanHandle *plans[2] = { NULL, NULL };
    if (!JZ_IS_ERR(e)) e = prepared_plans(node, shape, binding, plans);
    if (JZ_IS_ERR(e)) {
        unlock_node(db);
        if (shape) jazz_model__query__ValidatedQuery__drop(shape);
        if (binding) jazz_model__query__Binding__drop(binding);
        return e;
    }
    memset(out, 0, sizeof *out);
    out->has_request_identity_claims = false;
    out->shape = shape;
    out->binding = binding;
    out->local_plan = plans[0];
    out->global_plan = plans[1];
    out->groove_runtime_token = NS(groove_runtime_token)(node);
    unlock_node(db);
    return JZ_OK;
}

/* Prepare a query after asynchronously acquiring the node owner. */
jz_err RD_FN(prepare_query_async)(const jazz_db_Db *db, const jazz_model_Query *query, jazz_db_PreparedQuery *out) {
    return RD_FN(prepare_query_async_for)(db, query, db->schema_view_is_fixed, out);
}

/* Prepare a query against the schema this database handle was opened with. */
jz_err RD_FN(prepare_query_for_open_schema_async)(const jazz_db_Db *db, const jazz_model_Query *query, jazz_db_PreparedQuery *out) {
    return RD_FN(prepare_query_async_for)(db, query, true, out);
}

/* Normalize relation syntax and prepare it after asynchronously acquiring the node owner. */
jz_err RD_FN(prepare_relation_query_async)(const jazz_db_Db *db, const jazz_model_RelationQuery *query, jazz_db_PreparedQuery *out) {
    jazz_model_Query *q;
    JZ_TRY(qe(jazz_model__query__relation_resolution__relation_query_to_query(query, &q)));
    jz_err e = RD_FN(prepare_query_async)(db, q, out);
    jazz_model__query__Query__drop(q);
    return e;
}

static jz_err decode_query(jz_slice bytes, jazz_model_Query **out) {
    jz_err e = jazz_db__wire__decode_postcard_exact_query(bytes, out);
    if (JZ_IS_ERR(e)) {
        jz_err wrapped = jz_err_fmt(JAZZ_EC_QUERY, "decode query: %.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return wrapped;
    }
    return JZ_OK;
}

/* Decode and prepare the canonical serialized query accepted by host bindings. */
jz_err RD_FN(prepare_serialized_query_async)(const jazz_db_Db *db, jz_slice query, const jazz_db_PolicyBinding *request_scope /* NULL = None */, jazz_db_PreparedQuery *out) {
    jazz_model_Query *decoded;
    JZ_TRY(decode_query(query, &decoded));
    jz_err e = RD_FN(prepare_query_async)(db, decoded, out);
    jazz_model__query__Query__drop(decoded);
    if (JZ_IS_ERR(e)) return e;
    if (request_scope) {
        jz_btree claims;
        jazz_db__db__RowCells__clone(&claims, &request_scope->claims);
        jazz_db__db__PreparedQuery__with_identity_claims(out, request_scope->author, claims);
    }
    return JZ_OK;
}

/* Wait until a serialized read's coverage attachment is covered. */
static jz_err RD_FN(wait_for_serialized_read_coverage)(const jazz_db_Db *db, const jazz_db_SerializedReadCoverage *coverage, jazz_db_CoverageExpiredFn coverage_expired, void *expired_ctx) {
    if (!coverage->has_attachment) jz_panic("live serialized read coverage");
    for (;;) {
        if (SUBS(query_attachment_is_covered)(db, &coverage->attachment)) return JZ_OK;
        if (coverage_expired(expired_ctx)) return coverage_timed_out();
        jz_task_run_ready(); /* Poll::Pending */
    }
}

/* LocalFuture over wait_for_serialized_read_coverage (raced against link loss). */
typedef struct CoverageWaitFuture {
    const jazz_db_Db *db; const jazz_db_SerializedReadCoverage *coverage;
    jazz_db_CoverageExpiredFn expired; void *expired_ctx;
} CoverageWaitFuture;
static bool coverage_wait_poll(void *self, const jz_waker *cx, void *out) {
    CoverageWaitFuture *f = self;
    if (!f->coverage->has_attachment) jz_panic("live serialized read coverage");
    if (SUBS(query_attachment_is_covered)(f->db, &f->coverage->attachment)) { *(jz_err *)out = JZ_OK; return true; }
    if (f->expired(f->expired_ctx)) { *(jz_err *)out = coverage_timed_out(); return true; }
    return false;
}
static void coverage_wait_drop(void *self) { jz_free(self); }
static jazz_db_LocalFuture coverage_wait_future(const jazz_db_Db *db, const jazz_db_SerializedReadCoverage *coverage, jazz_db_CoverageExpiredFn expired, void *expired_ctx) {
    CoverageWaitFuture *f = JZ_NEW(CoverageWaitFuture);
    f->db = db; f->coverage = coverage; f->expired = expired; f->expired_ctx = expired_ctx;
    return (jazz_db_LocalFuture){ f, coverage_wait_poll, coverage_wait_drop };
}

/* ---- free helpers ---- */

/* The one table a plain read consults, or `None` when its shape reaches
 * further (joins, includes, subqueries, relations) and every table counts. */
static bool plain_read_table(const jazz_model_Query *query, jz_slice *out) {
    u32 n_arrays = 0;
    jazz_model__query__Query__array_subqueries(query, &n_arrays);
    if (jazz_model__query__Query__joins_len(query) == 0
        && !jazz_model__query__Query__has_flat_join(query)
        && jazz_model__query__Query__policy_branches_len(query) == 0
        && jazz_model__query__Query__reachable_len(query) == 0
        && jazz_model__query__Query__inherits_len(query) == 0
        && jazz_model__query__Query__includes_len(query) == 0
        && n_arrays == 0
        && !jazz_model__query__Query__has_aggregate(query)
        && !jazz_model__query__Query__has_relation(query)) {
        *out = jazz_model__query__Query__table(query);
        return true;
    }
    return false;
}

static bool read_footprint_overlaps(bool has_read_table, jz_slice read_table, const jz_btree *written) {
    if (!has_read_table) return true;
    jz_str key = { (char *)read_table.ptr, read_table.len, 0 };
    return jz_btree_contains(written, &key);
}

/* How long a Global read waits for the local writes it must observe to make
 * upload progress before it gives up (#3839). */
#define LOCAL_WRITE_UPLOAD_STALL_MS ((u64)15000)

static jz_str quoted_tables(const jz_btree *tables) {
    if (tables->len == 0) return jz_str_from("its tables");
    jz_str out = jz_str_from("");
    jz_str *table;
    void *unit;
    bool first = true;
    JZ_BTREE_FOR(tables, table, unit) {
        jz_str_appendf(&out, "%s`%.*s`", first ? "" : ", ", (int)table->len, table->ptr);
        first = false;
    }
    return out;
}

static jz_err local_writes_upload_stalled(const jz_btree *tables) {
    jz_str quoted = quoted_tables(tables);
    jz_err e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED,
                          "Timed out waiting for local writes to %.*s to upload before a remote read: no upload progress for %llu s",
                          (int)quoted.len, quoted.ptr, (unsigned long long)(LOCAL_WRITE_UPLOAD_STALL_MS / 1000));
    jz_str_drop(&quoted);
    return e;
}

static jz_err local_writes_not_uploaded(const jz_btree *tables, const char *reason) {
    jz_str quoted = quoted_tables(tables);
    jz_err e = jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "Remote read waits on local writes to %.*s that could not be uploaded (%s)",
                          (int)quoted.len, quoted.ptr, reason);
    jz_str_drop(&quoted);
    return e;
}

/* The one table a read's result depends on, or `None` when every table
 * counts: the query reaches further, its table is unknown, or its read
 * policy consults another table. */
static bool RD_FN(read_footprint_table)(const jazz_db_Db *db, const jazz_model_Query *query, jz_slice *out) {
    jz_slice table;
    if (!plain_read_table(query, &table)) return false;
    const jazz_node_TableSchema *schema;
    jz_err e = MUT(table_schema)(db, table, &schema);
    if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
    const jazz_node_PolicyQuery *policy = jazz_node__schema__TableSchema__read_policy(schema);
    if (policy) {
        jz_slice policy_table;
        bool plain = plain_read_table(jazz_node__schema__PolicyQuery__as_query(policy), &policy_table);
        if (!plain || policy_table.len != table.len || memcmp(policy_table.ptr, table.ptr, table.len) != 0) return false;
    }
    *out = table;
    return true;
}

/* Waiting entry: (TxId, BTreeSet<String>) */
typedef struct WaitingWrite { jazz_model_TxId tx_id; jz_btree tables; } WaitingWrite;
typedef JZ_VEC(WaitingWrite) WaitingWriteVec;
typedef struct QueuedUnit { jazz_model_TxId tx_id; bool has_unit; jazz_protocol_SyncMessage unit; } QueuedUnit;
typedef JZ_VEC(QueuedUnit) QueuedUnitVec;

/* The on-wire wait future of await_preceding_local_writes_on_wire. */
typedef struct OnWireFuture {
    const jazz_db_Db *db;
    WaitingWriteVec *waiting;
    bool has_last_progress; u64 seen_progress; usize seen_held; u64 since_ms;
} OnWireFuture;

static bool on_wire_poll(void *self, const jz_waker *cx, void *out) {
    OnWireFuture *f = self;
    const jazz_db_Db *db = f->db;
    const jazz_db_UploadOutbox *outbox = db->node->outbox;
    usize held = 0;
    jz_btree held_tables;
    jz_btree_init(&held_tables, &jazz_db_str_set_desc);
    JZ_VEC_FOR(WaitingWrite, w, *f->waiting) {
        const char *reason = jazz_db__db__UploadOutbox__upload_failure(outbox, w->tx_id);
        if (reason) {
            jz_btree_drop(&held_tables);
            *(jz_err *)out = local_writes_not_uploaded(&w->tables, reason);
            return true;
        }
        if (jazz_db__db__UploadOutbox__awaits_wire(outbox, w->tx_id) || NODE(is_pending_local_publication)(db->node, w->tx_id)) {
            held += 1;
            jz_str *t;
            void *unit;
            JZ_BTREE_FOR(&w->tables, t, unit) {
                jz_str k = jz_str_clone(t);
                if (jz_btree_insert(&held_tables, &k, NULL, NULL)) { /* already present: old key dropped */ }
            }
        }
    }
    if (held == 0) {
        jz_btree_drop(&held_tables);
        *(jz_err *)out = JZ_OK;
        return true;
    }
    u64 progress = jazz_db__db__UploadOutbox__upload_progress(outbox);
    const jazz_db_UploadRetryClock *clock = &db->node->upload_retry_clock->clock;
    u64 now_ms = clock->vt->now_ms(clock->self);
    if (f->has_last_progress && f->seen_progress == progress && f->seen_held == held) {
        if ((now_ms > f->since_ms ? now_ms - f->since_ms : 0) >= LOCAL_WRITE_UPLOAD_STALL_MS) {
            *(jz_err *)out = local_writes_upload_stalled(&held_tables);
            jz_btree_drop(&held_tables);
            return true;
        }
    } else {
        f->has_last_progress = true;
        f->seen_progress = progress; f->seen_held = held; f->since_ms = now_ms;
    }
    jz_btree_drop(&held_tables);
    return false;
}
static void on_wire_drop(void *self) { jz_free(self); }

/* Order a Global (remote) read's open after the local writes it must
 * observe, so a remote read sees this node's earlier writes (#3839).
 * Fails with NotObserved after LOCAL_WRITE_UPLOAD_STALL_MS without upload
 * progress, fails at once when an upload fails, and ends when the link is lost. */
static jz_err RD_FN(await_preceding_local_writes_on_wire)(const jazz_db_Db *db, const jazz_model_Query *query) {
    QueuedUnitVec queued = {0};
    {
        const jazz_db_UploadOutbox *outbox = db->node->outbox;
        u32 n = jazz_db__db__UploadOutbox__len(outbox);
        for (u32 i = 0; i < n; i++) {
            const jazz_db_PendingUpload *pending = jazz_db__db__UploadOutbox__at(outbox, i);
            if (!jazz_db__db__UploadOutbox__awaits_wire(outbox, pending->tx_id)) continue;
            QueuedUnit q = { pending->tx_id, pending->has_unit };
            if (pending->has_unit) jazz_protocol__protocol__SyncMessage__clone(&q.unit, &pending->unit);
            JZ_VEC_PUSH(queued, q);
        }
    }
    jazz_db_TxTablesVec settling = {0};
    NODE(pending_local_publication_tables)(db->node, &settling);
    jz_err result = JZ_OK;
    WaitingWriteVec waiting = {0};
    if (queued.len == 0 && settling.len == 0) goto done;
    u64 epoch;
    if (!jazz_db__db__first_load__RemoteLinkTracker__arm_until_loss(db->node->remote_link, &epoch)) goto done;
    jz_slice read_table = {0};
    bool has_read_table = RD_FN(read_footprint_table)(db, query, &read_table);
    {
        jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
        JZ_VEC_FOR(jazz_db_StorageTables, s, settling) {
            jz_btree tables;
            NS(logical_table_names_for_storage_tables)(node, &s->tables, &tables);
            if (read_footprint_overlaps(has_read_table, read_table, &tables)) {
                WaitingWrite w = { s->tx_id, tables };
                JZ_VEC_PUSH(waiting, w);
            } else {
                jz_btree_drop(&tables);
            }
        }
        JZ_VEC_FOR(QueuedUnit, q, queued) {
            bool has_unit = q->has_unit;
            jazz_protocol_SyncMessage unit;
            if (has_unit) { unit = q->unit; q->has_unit = false; }
            else {
                jz_err e = NS(commit_unit_for)(node, q->tx_id, &unit); /* .ok() */
                if (JZ_IS_ERR(e)) jz_err_drop(&e); else has_unit = true;
            }
            if (has_unit) {
                jz_btree tables;
                jazz_protocol__protocol__SyncMessage__uploaded_version_tables(&unit, &tables);
                jazz_protocol__protocol__SyncMessage__drop(&unit);
                if (read_footprint_overlaps(has_read_table, read_table, &tables)) {
                    WaitingWrite w = { q->tx_id, tables };
                    JZ_VEC_PUSH(waiting, w);
                } else {
                    jz_btree_drop(&tables);
                }
            } else {
                /* An unreadable commit may write anything the read reads. */
                WaitingWrite w = { q->tx_id };
                jz_btree_init(&w.tables, &jazz_db_str_set_desc);
                JZ_VEC_PUSH(waiting, w);
            }
        }
        unlock_node(db);
    }
    if (waiting.len == 0) goto done;
    {
        OnWireFuture *f = JZ_NEW(OnWireFuture);
        f->db = db;
        f->waiting = &waiting;
        jazz_db_LocalFuture on_wire = { f, on_wire_poll, on_wire_drop };
        jz_err answer = JZ_OK;
        if (jazz_db__db__first_load__Db__race_remote_answer(db, epoch, false, 0, on_wire, &answer)) result = answer;
        /* None (link lost) => Ok(()) */
    }
done:
    JZ_VEC_FOR(QueuedUnit, q, queued) if (q->has_unit) jazz_protocol__protocol__SyncMessage__drop(&q->unit);
    jz_free(queued.ptr);
    JZ_VEC_FOR(jazz_db_StorageTables, s, settling) jz_btree_drop(&s->tables);
    jz_free(settling.ptr);
    JZ_VEC_FOR(WaitingWrite, w, waiting) jz_btree_drop(&w->tables);
    jz_free(waiting.ptr);
    return result;
}

/* await_preceding_local_writes_on_wire for a serialized query, boxed and
 * built in this frame rather than the caller's (keeps one-shot read frames
 * small; the 1 MiB WASM dev stack overflowed otherwise). */
static jz_err RD_FN(preceding_local_writes_on_wire)(const jazz_db_Db *db, jz_slice query) {
    jazz_model_Query *decoded;
    JZ_TRY(decode_query(query, &decoded));
    jz_err e = RD_FN(await_preceding_local_writes_on_wire)(db, decoded);
    jazz_model__query__Query__drop(decoded);
    return e;
}

/* ---- one-shot reads ---- */

jz_err RD_FN(all_for_identity_in_authorization_mode)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, u32 coverage_tier, jazz_types_AuthorSubject author, u32 authorization_mode, jazz_db_CurrentRowVec *out);
jz_err RD_FN(relation_snapshot_with_coverage)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, const jazz_db_ReadOpts *opts, u32 coverage_tier, jazz_types_AuthorSubject author, u32 authorization_mode, jazz_node_RelationSnapshot *out);
jz_err RD_FN(hydrate_rows_for_binding)(const jazz_db_Db *db, jazz_db_CurrentRowVec *rows);
jz_err RD_FN(hydrate_relation_snapshot_for_binding)(const jazz_db_Db *db, jazz_node_RelationSnapshot *snapshot);

/* Closure environment shared by the attach_coverage helper. */
typedef struct OnceEnv {
    const jazz_db_Db *db;
    const jazz_db_ReadOpts *opts;
    bool has_author; jazz_types_AuthorSubject author;
    jazz_db_ReleaseCoverageFn release; void *release_ctx;
} OnceEnv;

/* Attach a query's coverage, released through `release_coverage` when the
 * returned guard drops. */
static jz_err attach_coverage(const OnceEnv *env, const jazz_db_PreparedQuery *query, u32 tier, bool has_tx, jazz_model_OpenTransactionId tx, jazz_db_SerializedReadCoverage *out) {
    jazz_db_ReadOpts opts = *env->opts;
    jazz_protocol__protocol__ReadViewSpec__clone(&opts.read_view, &env->opts->read_view);
    jazz_db_QueryAttachment attachment;
    JZ_TRY(SUBS(attach_query_with_coverage_async)(env->db, query, opts, tier, has_tx, tx, env->has_author, env->author, &attachment));
    out->has_attachment = true;
    out->attachment = attachment;
    out->release = env->release;
    out->release_ctx = env->release_ctx;
    return JZ_OK;
}

static jz_err RD_FN(all_serialized_query_once)(const jazz_db_Db *db, jz_slice query, jazz_db_ReadOpts opts, u32 coverage_tier,
                                              bool has_open_tx, jazz_model_OpenTransactionId open_tx,
                                              const jazz_db_PolicyBinding *request_scope, bool has_author, jazz_types_AuthorSubject author,
                                              bool require_coverage, jazz_db_CoverageExpiredFn coverage_expired, void *expired_ctx,
                                              jazz_db_ReleaseCoverageFn release_coverage, void *release_ctx, jazz_db_SerializedReadResult *out) {
    jz_err e = JZ_OK;
    jazz_model_Query *decoded = NULL;
    jazz_db_PreparedQuery prepared;
    bool has_prepared = false;
    if (require_coverage && !has_open_tx && coverage_tier >= JAZZ_DUR_GLOBAL) {
        e = RD_FN(preceding_local_writes_on_wire)(db, query);
        if (JZ_IS_ERR(e)) goto out_opts;
    }
    {
        /* await_open_schema_for_coverage, raced against coverage expiry */
        e = MUT(await_open_schema_for_coverage)(db, coverage_tier);
        if (JZ_IS_ERR(e) && coverage_expired(expired_ctx)) { jz_err_drop(&e); e = coverage_timed_out(); }
        if (JZ_IS_ERR(e)) goto out_opts;
    }
    e = decode_query(query, &decoded);
    if (JZ_IS_ERR(e)) goto out_opts;
    bool is_relation = jazz_model__query__Query__has_relation(decoded);
    if (has_open_tx && is_relation) {
        e = jz_err_new(JAZZ_EC_QUERY, "relation reads inside a transaction are not supported");
        goto out;
    }
    e = RD_FN(prepare_query_async)(db, decoded, &prepared);
    if (JZ_IS_ERR(e)) goto out;
    has_prepared = true;
    if (request_scope) {
        jz_btree claims;
        jazz_db__db__RowCells__clone(&claims, &request_scope->claims);
        jazz_db__db__PreparedQuery__with_identity_claims(&prepared, request_scope->author, claims);
    }
    OnceEnv env = { db, &opts, has_author, author, release_coverage, release_ctx };
    jazz_model_OpenTransactionId no_tx = {0};
    u32 n_arrays = 0;
    jazz_model__query__Query__array_subqueries(jazz_model__query__ValidatedQuery__query(jazz_db__db__PreparedQuery__shape(&prepared)), &n_arrays);
    /* Read the maintained result itself, not just its coverage signal.
     * A strict (Global) client one-shot reads through its own fresh stream
     * as well, so its first-settlement deletion reconciliation runs (INV-SYNC-48). */
    bool strict_client_one_shot = !is_relation && !has_open_tx && !has_author && n_arrays == 0
        && coverage_tier >= JAZZ_DUR_GLOBAL && jazz_protocol__protocol__ReadViewSpec__is_default(&opts.read_view) && !opts.include_deleted;
    if (require_coverage && (is_relation || strict_client_one_shot)) {
        jazz_db_SerializedReadCoverage local_coverage = {0}, strict_coverage = {0};
        if (coverage_tier == JAZZ_DUR_LOCAL) {
            e = attach_coverage(&env, &prepared, coverage_tier, false, no_tx, &local_coverage);
            if (JZ_IS_ERR(e)) goto out;
        }
        jazz_db_ReadOpts stream_opts = opts;
        jazz_protocol__protocol__ReadViewSpec__clone(&stream_opts.read_view, &opts.read_view);
        jazz_db_SubscriptionStream stream;
        e = SUBS(subscribe_one_shot_with_coverage)(db, &prepared, stream_opts, has_author, author, coverage_tier, &stream);
        if (JZ_IS_ERR(e)) { SerializedReadCoverage__Drop__drop(&local_coverage); goto out; }
        /* A strict one-shot also waits for an authority receipt newer than its own open. */
        if (strict_client_one_shot) {
            e = attach_coverage(&env, &prepared, coverage_tier, false, no_tx, &strict_coverage);
            if (JZ_IS_ERR(e)) {
                SerializedReadCoverage__Drop__drop(&local_coverage);
                jazz_db__db__SubscriptionStream__Drop__drop(&stream);
                goto out;
            }
        }
        jz_err outcome = JZ_OK;
        bool has_event = false;
        jazz_db_SubscriptionEvent event;
        for (;;) {
            if (coverage_expired(expired_ctx)) { outcome = coverage_timed_out(); break; }
            bool pending = false;
            if (local_coverage.has_attachment && !SUBS(query_attachment_is_covered)(db, &local_coverage.attachment)) pending = true;
            if (strict_coverage.has_attachment && !SUBS(query_attachment_is_covered)(db, &strict_coverage.attachment)) pending = true;
            if (!pending) {
                bool has = false;
                if (jazz_db__db__SubscriptionStream__Stream__poll_next(&stream, NULL, &has, &event)) { has_event = has; break; }
            }
            jz_task_run_ready();
        }
        if (!JZ_IS_ERR(outcome)) {
            if (!has_event || event.tag == JAZZ_SE_CLOSED) {
                outcome = jz_err_new(JAZZ_EC_NOT_OBSERVED, "query subscription ended before its published result");
            } else if (event.tag == JAZZ_SE_REJECTED) {
                jz_str dbg = jz_str_from("");
                jazz_protocol__protocol__SubscribeRejectReason__debug(&dbg, &event.reason);
                outcome = jz_err_fmt(JAZZ_EC_QUERY, "query subscription rejected: %.*s", (int)dbg.len, dbg.ptr);
                jz_str_drop(&dbg);
            } else if (!event.reset) {
                outcome = jz_err_new(JAZZ_EC_PROTOCOL, "query subscription did not open with a reset");
            } else {
                jazz_node_RelationSnapshot snapshot;
                outcome = jazz_db__db__SubscriptionStream__settled_receiver_local_snapshot(&stream, &snapshot);
                if (!JZ_IS_ERR(outcome)) outcome = RD_FN(hydrate_relation_snapshot_for_binding)(db, &snapshot);
                if (!JZ_IS_ERR(outcome)) {
                    memset(out, 0, sizeof *out);
                    if (n_arrays == 0) {
                        /* rows.into_iter().take(root_count) */
                        for (u32 i = snapshot.root_count; i < snapshot.rows.len; i++) jazz_node__node__CurrentRow__drop(&snapshot.rows.ptr[i]);
                        if (snapshot.rows.len > snapshot.root_count) snapshot.rows.len = snapshot.root_count;
                        out->tag = JAZZ_SRR_ROWS;
                        memcpy(&out->rows, &snapshot.rows, sizeof out->rows);
                        JZ_VEC_FOR(jazz_node_RelationEdge, edge, snapshot.edges) jazz_node__node__RelationEdge__drop(edge);
                        jz_free(snapshot.edges.ptr);
                    } else {
                        out->tag = JAZZ_SRR_RELATION;
                        out->relation = snapshot;
                    }
                }
            }
            if (has_event) jazz_db__db__SubscriptionEvent__drop(&event);
        }
        jz_err finalization = jazz_db__db__SubscriptionStream__close(&stream);
        jazz_db__db__SubscriptionStream__Drop__drop(&stream);
        SerializedReadCoverage__Drop__drop(&strict_coverage);
        SerializedReadCoverage__Drop__drop(&local_coverage);
        if (JZ_IS_ERR(outcome)) { jz_err_drop(&finalization); e = outcome; }
        else if (JZ_IS_ERR(finalization)) {
            if (out->tag == JAZZ_SRR_RELATION) jazz_node__node__RelationSnapshot__drop(&out->relation);
            else jz_vec_drop(&out->rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
            e = finalization;
        }
        goto out;
    }
    /* A partial node holds no node-wide history below its exclusive
     * snapshot's `global_base`: its exclusive reads hydrate through the
     * authority at the frozen snapshot (INV-TX-13, garden-co/jazz#3694).
     * Offline, the read keeps the replica's answer. */
    bool exclusive_snapshot_read = false;
    if (has_open_tx && NODE(receives_commits_as_local)(db->node)) {
        bool exclusive;
        e = TX(transaction_is_exclusive)(db, open_tx, &exclusive);
        if (JZ_IS_ERR(e)) goto out;
        exclusive_snapshot_read = exclusive && opts.tier != JAZZ_DB_READ_LOCAL_ONLY;
    }
    /* Each joined, included or related source is a narrowed read; hydrate
     * those too. A source with no narrowed read fails here. */
    jazz_db_QueryVec source_queries = {0};
    if (has_open_tx && exclusive_snapshot_read) {
        e = TX(exclusive_source_hydration_queries)(db, open_tx, &prepared, opts.include_deleted, &source_queries);
        if (JZ_IS_ERR(e)) goto out;
    }
    jazz_db_SerializedReadCoverage coverage = {0};
    bool hydrate = exclusive_snapshot_read && coverage_tier < JAZZ_DUR_GLOBAL;
    u64 epoch;
    if (hydrate && jazz_db__db__first_load__RemoteLinkTracker__arm_until_loss(db->node->remote_link, &epoch)) {
        jazz_db_SerializedReadCoverage hydration = {0};
        e = attach_coverage(&env, &prepared, JAZZ_DUR_GLOBAL, has_open_tx, open_tx, &hydration);
        if (JZ_IS_ERR(e)) goto out_sources;
        jz_err covered = JZ_OK;
        if (jazz_db__db__first_load__Db__race_remote_answer(db, epoch, false, 0, coverage_wait_future(db, &hydration, coverage_expired, expired_ctx), &covered)) {
            if (JZ_IS_ERR(covered)) { SerializedReadCoverage__Drop__drop(&hydration); e = covered; goto out_sources; }
            coverage = hydration;
        } else {
            /* The authority became unreachable: drop the pending hydration and read the replica. */
            SerializedReadCoverage__Drop__drop(&hydration);
        }
    }
    if (!coverage.has_attachment && require_coverage) {
        jazz_db_SerializedReadCoverage required = {0};
        e = attach_coverage(&env, &prepared, coverage_tier, has_open_tx, open_tx, &required);
        if (JZ_IS_ERR(e)) goto out_sources;
        e = RD_FN(wait_for_serialized_read_coverage)(db, &required, coverage_expired, expired_ctx);
        if (JZ_IS_ERR(e)) { SerializedReadCoverage__Drop__drop(&required); goto out_sources; }
        coverage = required;
    }
    JZ_VEC(jazz_db_SerializedReadCoverage) table_coverage = {0};
    if (exclusive_snapshot_read && coverage.has_attachment && has_open_tx) {
        JZ_VEC_FOR(jazz_model_Query *, source, source_queries) {
            if (!jazz_db__db__first_load__RemoteLinkTracker__arm_until_loss(db->node->remote_link, &epoch)) break;
            jazz_db_PreparedQuery source_query;
            e = RD_FN(prepare_query_async)(db, *source, &source_query);
            if (JZ_IS_ERR(e)) break;
            jazz_db_SerializedReadCoverage hydration = {0};
            e = attach_coverage(&env, &source_query, JAZZ_DUR_GLOBAL, true, open_tx, &hydration);
            jazz_db__db__PreparedQuery__drop(&source_query);
            if (JZ_IS_ERR(e)) break;
            jz_err covered = JZ_OK;
            if (!jazz_db__db__first_load__Db__race_remote_answer(db, epoch, false, 0, coverage_wait_future(db, &hydration, coverage_expired, expired_ctx), &covered)) {
                SerializedReadCoverage__Drop__drop(&hydration);
                break;
            }
            if (JZ_IS_ERR(covered)) { SerializedReadCoverage__Drop__drop(&hydration); e = covered; break; }
            JZ_VEC_PUSH(table_coverage, hydration);
        }
    }
    if (!JZ_IS_ERR(e)) {
        if (n_arrays != 0) {
            jazz_node_RelationSnapshot snapshot;
            jazz_db_ReadOpts read_opts = opts;
            jazz_protocol__protocol__ReadViewSpec__clone(&read_opts.read_view, &opts.read_view);
            if (has_open_tx) {
                e = TX(relation_snapshot_in_open_transaction)(db, open_tx, &prepared, read_opts, has_author, author, &snapshot);
            } else {
                e = RD_FN(relation_snapshot_with_coverage)(db, &prepared, &read_opts, coverage_tier, has_author ? author : db->identity.author,
                                                           has_author ? JAZZ_QAM_TRUSTED_SERVING : JAZZ_QAM_CLIENT_LOCAL, &snapshot);
                jazz_protocol__protocol__ReadViewSpec__drop(&read_opts.read_view);
                if (!JZ_IS_ERR(e)) {
                    e = RD_FN(hydrate_relation_snapshot_for_binding)(db, &snapshot);
                    if (JZ_IS_ERR(e)) jazz_node__node__RelationSnapshot__drop(&snapshot);
                }
            }
            if (!JZ_IS_ERR(e)) { memset(out, 0, sizeof *out); out->tag = JAZZ_SRR_RELATION; out->relation = snapshot; }
        } else {
            jazz_db_CurrentRowVec rows = {0};
            jazz_db_ReadOpts read_opts = opts;
            jazz_protocol__protocol__ReadViewSpec__clone(&read_opts.read_view, &opts.read_view);
            if (has_open_tx)
                e = TX(all_in_open_transaction)(db, open_tx, &prepared, read_opts, has_author, author, &rows);
            else
                e = RD_FN(all_for_identity_in_authorization_mode)(db, &prepared, read_opts, coverage_tier, has_author ? author : db->identity.author,
                                                                  has_author ? JAZZ_QAM_TRUSTED_SERVING : JAZZ_QAM_CLIENT_LOCAL, &rows);
            if (!JZ_IS_ERR(e)) e = RD_FN(hydrate_rows_for_binding)(db, &rows);
            if (!JZ_IS_ERR(e)) { memset(out, 0, sizeof *out); out->tag = JAZZ_SRR_ROWS; memcpy(&out->rows, &rows, sizeof rows); }
            else jz_vec_drop(&rows, sizeof(jazz_node_CurrentRow), jazz_node__node__CurrentRow__drop);
        }
    }
    JZ_VEC_FOR(jazz_db_SerializedReadCoverage, c, table_coverage) SerializedReadCoverage__Drop__drop(c);
    jz_free(table_coverage.ptr);
    SerializedReadCoverage__Drop__drop(&coverage);
out_sources:
    JZ_VEC_FOR(jazz_model_Query *, q, source_queries) jazz_model__query__Query__drop(*q);
    jz_free(source_queries.ptr);
out:
    if (has_prepared) jazz_db__db__PreparedQuery__drop(&prepared);
    if (decoded) jazz_model__query__Query__drop(decoded);
out_opts:
    jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
    return e;
}

/* Closure context of the gated (WaitForRemote) serialized read. */
typedef struct GatedReadCtx {
    const jazz_db_Db *db; jz_slice query;
    jazz_db_ReadOpts opts, remote_opts;
    u32 coverage_tier;
    const jazz_db_PolicyBinding *request_scope; const jazz_db_PolicyBinding *remote_scope;
    bool require_coverage;
    jazz_db_CoverageExpiredFn coverage_expired; void *expired_ctx;
    jazz_db_ReleaseCoverageFn release; void *release_ctx;
    jazz_db_SerializedReadResult *result;
} GatedReadCtx;
typedef struct GatedReadFuture { GatedReadCtx *ctx; bool remote; } GatedReadFuture;
static bool gated_read_poll(void *self, const jz_waker *cx, void *out) {
    GatedReadFuture *f = self;
    GatedReadCtx *c = f->ctx;
    jazz_model_OpenTransactionId no_tx = {0};
    jazz_types_AuthorSubject no_author = {0};
    jz_err e;
    if (f->remote) {
        jazz_db_ReadOpts opts = c->remote_opts;
        jazz_protocol__protocol__ReadViewSpec__clone(&opts.read_view, &c->remote_opts.read_view);
        e = RD_FN(all_serialized_query_once)(c->db, c->query, opts, JAZZ_DUR_GLOBAL, false, no_tx, c->remote_scope, false, no_author, true,
                                             c->coverage_expired, c->expired_ctx, c->release, c->release_ctx, c->result);
    } else {
        jazz_db_ReadOpts opts = c->opts;
        jazz_protocol__protocol__ReadViewSpec__clone(&opts.read_view, &c->opts.read_view);
        e = RD_FN(all_serialized_query_once)(c->db, c->query, opts, c->coverage_tier, false, no_tx, c->request_scope, false, no_author,
                                             c->require_coverage, c->coverage_expired, c->expired_ctx, c->release, c->release_ctx, c->result);
    }
    *(jz_err *)out = e;
    return true;
}
static void gated_read_drop(void *self) { jz_free(self); }
static jazz_db_LocalFuture gated_local(void *ctx) {
    GatedReadFuture *f = JZ_NEW(GatedReadFuture); f->ctx = ctx; f->remote = false;
    return (jazz_db_LocalFuture){ f, gated_read_poll, gated_read_drop };
}
static jazz_db_LocalFuture gated_remote(void *ctx) {
    GatedReadFuture *f = JZ_NEW(GatedReadFuture); f->ctx = ctx; f->remote = true;
    return (jazz_db_LocalFuture){ f, gated_read_poll, gated_read_drop };
}

/* Execute the complete serialized-query path for a host binding.
 * A FirstLoad::WaitForRemote request from a client-local read outside a
 * transaction applies the shared one-shot rule of read_local_first_within. */
jz_err RD_FN(all_serialized_query)(const jazz_db_Db *db, jz_slice query, jazz_db_ReadOpts opts, bool has_open_tx, jazz_model_OpenTransactionId open_tx,
                                   const jazz_db_PolicyBinding *request_scope, bool has_author, jazz_types_AuthorSubject author, bool require_coverage,
                                   jazz_db_CoverageExpiredFn coverage_expired, void *expired_ctx,
                                   jazz_db_ReleaseCoverageFn release_coverage, void *release_ctx, jazz_db_SerializedReadResult *out) {
    u32 coverage_tier = jazz_db__db__read_coverage_tier(&opts);
    jazz_db_FirstLoad first_load = opts.first_load;   /* mem::take */
    opts.first_load.tag = JAZZ_FL_DELIVER;
    opts.first_load.timeout_ms = 0;
    bool gated = !has_open_tx && !has_author && opts.tier != JAZZ_DB_READ_LOCAL_ONLY && coverage_tier == JAZZ_DUR_LOCAL;
    bool has_wait = first_load.tag == JAZZ_FL_WAIT_FOR_REMOTE && gated && first_load.timeout_ms > 0;
    if (has_wait) {
        GatedReadCtx ctx = {0};
        ctx.db = db; ctx.query = query;
        ctx.opts = opts;
        ctx.remote_opts = opts;
        jazz_protocol__protocol__ReadViewSpec__clone(&ctx.remote_opts.read_view, &opts.read_view);
        ctx.coverage_tier = coverage_tier;
        ctx.request_scope = request_scope; ctx.remote_scope = request_scope; /* request_scope.clone() */
        ctx.require_coverage = require_coverage;
        ctx.coverage_expired = coverage_expired; ctx.expired_ctx = expired_ctx;
        ctx.release = release_coverage; ctx.release_ctx = release_ctx;
        ctx.result = out;
        /* Boxed: the gated read nests two full one-shot reads. */
        jz_err e = jazz_db__db__first_load__Db__read_local_first_within(db, first_load.timeout_ms, gated_local, gated_remote, &ctx);
        jazz_protocol__protocol__ReadViewSpec__drop(&ctx.opts.read_view);
        jazz_protocol__protocol__ReadViewSpec__drop(&ctx.remote_opts.read_view);
        return e;
    }
    return RD_FN(all_serialized_query_once)(db, query, opts, coverage_tier, has_open_tx, open_tx, request_scope, has_author, author,
                                            require_coverage, coverage_expired, expired_ctx, release_coverage, release_ctx, out);
}

jz_err RD_FN(prepare_query_bound_for_schema)(const jazz_db_Db *db, const jazz_model_Query *query, jz_btree params, const jazz_model_JazzSchema *schema, jazz_types_SchemaVersionId schema_version, jazz_db_PreparedQuery *out) {
    jz_err e = MUT(ensure_open_schema_admitted)(db);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&params); return e; }
    jazz_model_ValidatedQuery *shape;
    e = qe(jazz_model__query__Query__validate_with_schema_version(query, schema, schema_version, &shape));
    if (JZ_IS_ERR(e)) { jz_btree_drop(&params); return e; }
    jazz_model_Binding *binding;
    e = qe(jazz_model__query__ValidatedQuery__bind(shape, params, &binding));
    if (JZ_IS_ERR(e)) { jazz_model__query__ValidatedQuery__drop(shape); return e; }
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "prepare_query_bound_for_schema");
    jazz_node_PreparedQueryPlanHandle *plans[2] = { NULL, NULL };
    e = prepared_plans(node, shape, binding, plans); /* super::block_on */
    u64 groove_runtime_token = NS(groove_runtime_token)(node);
    unlock_node(db);
    if (JZ_IS_ERR(e)) { jazz_model__query__ValidatedQuery__drop(shape); jazz_model__query__Binding__drop(binding); return e; }
    memset(out, 0, sizeof *out);
    out->shape = shape;
    out->binding = binding;
    out->local_plan = plans[0];
    out->global_plan = plans[1];
    out->groove_runtime_token = groove_runtime_token;
    return JZ_OK;
}

/* Synchronously read rows for a prepared query (local preview). */
jz_err RD_FN(read)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_CurrentRowVec *out) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "read");
    u64 groove_runtime_token = NS(groove_runtime_token)(node);
    jz_err e = ne(NS(query_rows_local_preview)(node, prepared->shape, prepared->binding,
                                               jazz_db__db__PreparedQuery__plan_for_tier(prepared, JAZZ_DUR_LOCAL, groove_runtime_token), out));
    unlock_node(db);
    return e;
}

/* Synchronously read rows and attribute work inside the node query path. */
jz_err RD_FN(read_profiled)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_CurrentRowVec *out, jazz_node_QueryReadProfile *profile) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "read_profiled");
    u64 groove_runtime_token = NS(groove_runtime_token)(node);
    jz_err e = ne(NS(query_rows_local_preview_profiled)(node, prepared->shape, prepared->binding,
                                                        jazz_db__db__PreparedQuery__plan_for_tier(prepared, JAZZ_DUR_LOCAL, groove_runtime_token), out, profile));
    unlock_node(db);
    return e;
}

/* Synchronously read exactly one local row if present. */
jz_err RD_FN(one)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, bool *has, jazz_node_CurrentRow *out) {
    jazz_db_CurrentRowVec rows = {0};
    JZ_TRY(RD_FN(read)(db, prepared, &rows));
    *has = rows.len > 0;
    if (*has) {
        *out = rows.ptr[0];
        for (u32 i = 1; i < rows.len; i++) jazz_node__node__CurrentRow__drop(&rows.ptr[i]);
    }
    jz_free(rows.ptr);
    return JZ_OK;
}

/* Resolve creator/updater provenance for a row returned by this database. */
jz_err RD_FN(row_provenance)(const jazz_db_Db *db, const jazz_node_CurrentRow *row, bool *has, jazz_node_RowProvenance *out) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "row_provenance");
    jz_err e = ne(NS(row_provenance)(node, row, has, out));
    unlock_node(db);
    return e;
}

/* Resolve provenance after a storage-suspended node turn releases its owner mutex. */
jz_err RD_FN(row_provenance_async)(const jazz_db_Db *db, const jazz_node_CurrentRow *row, bool *has, jazz_node_RowProvenance *out) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    jz_err e = ne(NS(row_provenance)(node, row, has, out));
    unlock_node(db);
    return e;
}

static jz_err RD_FN(at_prepared)(const jazz_db_Db *db, jazz_types_GlobalTime position, const jazz_db_PreparedQuery *prepared, jazz_db_CurrentRowVec *out) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "at");
    jz_err e = ne(NS(at_read)(node, position, prepared->shape, prepared->binding, out));
    unlock_node(db);
    return e;
}

/* Read local settled history at an exact global timestamp cut.
 * History-incomplete facades return HistoricalReadRequiresServer from the node layer. */
jz_err RD_FN(at)(const jazz_db_Db *db, jazz_types_GlobalTime position, const jazz_db_PreparedQuery *prepared, jazz_db_CurrentRowVec *out) {
    return RD_FN(at_prepared)(db, position, prepared, out);
}

/* Tier-gated one-shot read. */
jz_err RD_FN(all)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out) {
    u32 coverage_tier = jazz_db__db__read_coverage_tier(&opts);
    return RD_FN(all_for_identity_in_authorization_mode)(db, prepared, opts, coverage_tier, db->identity.author, JAZZ_QAM_CLIENT_LOCAL, out);
}

/* Tier-gated one-shot read evaluated by a trusted host as `author`. */
jz_err RD_FN(all_for_identity)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, jazz_db_CurrentRowVec *out) {
    u32 coverage_tier = jazz_db__db__read_coverage_tier(&opts);
    return RD_FN(all_for_identity_in_authorization_mode)(db, prepared, opts, coverage_tier, author, JAZZ_QAM_TRUSTED_SERVING, out);
}

/* Use the host-selected authority tier, matching subscription registration.
 * Local reads remain local regardless of the upstream durability floor. */
static u32 RD_FN(client_authority_read_tier)(const jazz_db_Db *db, u32 tier) {
    if (tier >= JAZZ_DUR_GLOBAL) return jazz_db__db__remote_subscription_tier(tier, db->node->upstream_durability_floor);
    return tier;
}

jz_err RD_FN(all_for_identity_in_authorization_mode)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, u32 coverage_tier, jazz_types_AuthorSubject author, u32 authorization_mode, jazz_db_CurrentRowVec *out) {
    jz_err e = MUT(await_open_schema_for_coverage)(db, coverage_tier);
    if (JZ_IS_ERR(e)) goto done;
    /* Follow the same host-selected authority route as subscription registration. */
    u32 tier = authorization_mode == JAZZ_QAM_CLIENT_LOCAL ? RD_FN(client_authority_read_tier)(db, coverage_tier) : coverage_tier;
    jazz_node_NodeState *locked = jazz_db__db__SharedNodeState__lock(db->node->node);
    jazz_node_ActiveSessionClaimsScope *scope;
    e = jazz_db__db__PreparedQuery__scoped_node(prepared, locked, author, &scope);
    if (JZ_IS_ERR(e)) { unlock_node(db); goto done; }
    jazz_node_NodeState *node = jazz_node__node__ActiveSessionClaimsScope__node(scope);
    if (opts.read_view.source_tag != JAZZ_RVS_CURRENT) {
        e = jazz_db__db__ensure_supported_read_view(&opts);
        if (!JZ_IS_ERR(e) && opts.include_deleted)
            e = jz_err_new(JAZZ_EC_QUERY, "branch views do not support include_deleted yet");
        if (!JZ_IS_ERR(e)) {
            jazz_node_RelationSnapshot snapshot;
            if (authorization_mode == JAZZ_QAM_TRUSTED_SERVING)
                e = ne(NS(query_relation_snapshot_for_serving_in_read_view)(node, prepared->shape, prepared->binding, tier, author, &opts.read_view, &snapshot));
            else
                e = ne(NS(query_relation_snapshot_for_client)(node, prepared->shape, prepared->binding, tier, author, &opts.read_view, &snapshot));
            if (!JZ_IS_ERR(e)) {
                for (u32 i = snapshot.root_count; i < snapshot.rows.len; i++) jazz_node__node__CurrentRow__drop(&snapshot.rows.ptr[i]);
                if (snapshot.rows.len > snapshot.root_count) snapshot.rows.len = snapshot.root_count;
                memcpy(out, &snapshot.rows, sizeof *out);
                JZ_VEC_FOR(jazz_node_RelationEdge, edge, snapshot.edges) jazz_node__node__RelationEdge__drop(edge);
                jz_free(snapshot.edges.ptr);
            }
        }
    } else if (opts.include_deleted) {
        e = ne(NS(query_rows_including_deleted_in_authorization_mode)(node, prepared->shape, prepared->binding, tier, NULL, author, authorization_mode, out));
    } else if (authorization_mode == JAZZ_QAM_TRUSTED_SERVING) {
        e = ne(NS(query_rows_with_prepared_plan_for_identity)(node, prepared->shape, prepared->binding, tier, NULL, author, out));
    } else {
        /* A client consumes identity-scoped rows emitted by its trusted
         * upstream; local reads must not apply policy again. */
        e = ne(NS(query_rows_for_client)(node, prepared->shape, prepared->binding, tier, author, out));
    }
    jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    unlock_node(db);
done:
    jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
    return e;
}

/* The descriptor on a terminal operation describes its root record. Nested
 * insertions and updates carry only their child record bytes, so follow the
 * operation path to find the descriptor which actually owns `edit.value`. */
static bool terminal_operation_value_descriptor(const groove_TerminalOperation *operation, const groove_RecordDescriptor **out, jazz_db_BindingHydrationError *err) {
    const groove_RecordDescriptor *descriptor = operation->root_descriptor;
    bool expect_collection = true;
    err->tag = JAZZ_BHE_ERROR;
    JZ_VEC_FOR(const groove_TerminalPathSegment, segment, operation->path) {
        if (expect_collection && segment->tag == GROOVE_TPS_COLLECTION) {
            u32 n = 0;
            const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &n);
            const groove_DescriptorField *field = NULL;
            for (u32 i = 0; i < n; i++) {
                if (fields[i].has_name && fields[i].name.len == segment->collection.len
                    && memcmp(fields[i].name.ptr, segment->collection.ptr, segment->collection.len) == 0) { field = &fields[i]; break; }
            }
            if (!field) { err->error = jz_err_new(JAZZ_EC_PROTOCOL, "terminal operation references an unknown collection field"); return false; }
            if (field->value_type.tag != GROOVE_VT_ARRAY) { err->error = jz_err_new(JAZZ_EC_PROTOCOL, "terminal operation collection field is not an array"); return false; }
            const groove_ValueType *element = field->value_type.v.inner;
            if (element->tag != GROOVE_VT_RECORD) { err->error = jz_err_new(JAZZ_EC_PROTOCOL, "terminal operation collection does not contain records"); return false; }
            descriptor = element->v.record;
            expect_collection = false;
        } else if (!expect_collection && segment->tag == GROOVE_TPS_KEY) {
            expect_collection = true;
        } else if (expect_collection) {
            err->error = jz_err_new(JAZZ_EC_PROTOCOL, "terminal operation path starts with a key");
            return false;
        } else {
            err->error = jz_err_new(JAZZ_EC_PROTOCOL, "terminal operation path is missing a child key");
            return false;
        }
    }
    *out = descriptor;
    return true;
}

/* Like hydrate_subscription_event_for_binding, but retains the sole
 * retryable cause for host bindings that can await chunk delivery.
 * Returns false with *err set on failure. */
bool RD_FN(hydrate_subscription_event_for_binding_outcome)(const jazz_db_Db *db, jazz_db_SubscriptionEvent *event, jazz_db_BindingHydrationError *err) {
    jz_err e = MUT(ensure_open_schema_admitted)(db);
    if (JZ_IS_ERR(e)) { err->tag = JAZZ_BHE_ERROR; err->error = e; return false; }
    if (event->tag != JAZZ_SE_DELTA) return true;
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    bool ok = true;
    if (event->terminal_operations.len == 0) {
        jazz_db_OutputRowVec *batches[2] = { &event->added, &event->updated };
        for (int b = 0; b < 2 && ok; b++) {
            JZ_VEC_FOR(jazz_db_SubscriptionOutputRow, output, *batches[b]) {
                jazz_db_CurrentRowVec one = { &output->row, 1, 1 }; /* slice::from_mut */
                e = NS(hydrate_current_rows)(node, &one);
                if (JZ_IS_ERR(e)) { *err = binding_hydration_error(e); ok = false; break; }
            }
        }
    } else {
        /* Structured terminal edits replace the row batches at this binding. */
        JZ_VEC_FOR(groove_TerminalOperation, operation, event->terminal_operations) {
            if (operation->edit.tag == GROOVE_TE_REMOVE || operation->edit.tag == GROOVE_TE_MOVE) continue;
            const groove_RecordDescriptor *descriptor;
            if (!terminal_operation_value_descriptor(operation, &descriptor, err)) { ok = false; break; }
            if (operation->edit.tag != GROOVE_TE_INSERT && operation->edit.tag != GROOVE_TE_UPDATE)
                jz_panic("terminal operation payload shape changed after classification");
            e = NS(hydrate_encoded_record)(node, descriptor, &operation->edit.value);
            if (JZ_IS_ERR(e)) { *err = binding_hydration_error(e); ok = false; break; }
        }
    }
    unlock_node(db);
    return ok;
}

/* Resolve physical indirect scalars before a subscription event crosses
 * a language binding boundary. */
jz_err RD_FN(hydrate_subscription_event_for_binding)(const jazz_db_Db *db, jazz_db_SubscriptionEvent *event) {
    jazz_db_BindingHydrationError err;
    if (RD_FN(hydrate_subscription_event_for_binding_outcome)(db, event, &err)) return JZ_OK;
    if (err.tag == JAZZ_BHE_RETRYABLE_CHUNK_UNAVAILABLE)
        return jz_err_new(JAZZ_EC_NOT_OBSERVED, "large-value chunk is temporarily unavailable");
    return err.error;
}

/* Resolve physical indirect scalars in ordinary row output immediately
 * before a language binding encodes it. */
jz_err RD_FN(hydrate_rows_for_binding)(const jazz_db_Db *db, jazz_db_CurrentRowVec *rows) {
    JZ_TRY(MUT(ensure_open_schema_admitted)(db));
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    jz_err e = ne(NS(hydrate_current_rows)(node, rows));
    unlock_node(db);
    return e;
}

/* Resolve physical indirect scalars in a relation snapshot immediately
 * before a language binding encodes it. */
jz_err RD_FN(hydrate_relation_snapshot_for_binding)(const jazz_db_Db *db, jazz_node_RelationSnapshot *snapshot) {
    return RD_FN(hydrate_rows_for_binding)(db, (jazz_db_CurrentRowVec *)&snapshot->rows);
}

/* Tier-gated one-shot relation read evaluated as the database identity. */
jz_err RD_FN(all_relation_snapshot)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    jz_err e = RD_FN(relation_snapshot_with_coverage)(db, prepared, &opts, jazz_db__db__read_coverage_tier(&opts), db->identity.author, JAZZ_QAM_CLIENT_LOCAL, out);
    jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
    return e;
}

/* Tier-gated one-shot relation read evaluated as `author`. */
jz_err RD_FN(all_relation_snapshot_for_identity)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_types_AuthorSubject author, jazz_node_RelationSnapshot *out) {
    jz_err e = RD_FN(relation_snapshot_with_coverage)(db, prepared, &opts, jazz_db__db__read_coverage_tier(&opts), author, JAZZ_QAM_TRUSTED_SERVING, out);
    jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
    return e;
}

jz_err RD_FN(relation_snapshot_with_coverage)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, const jazz_db_ReadOpts *opts, u32 coverage_tier, jazz_types_AuthorSubject author, u32 authorization_mode, jazz_node_RelationSnapshot *out) {
    JZ_TRY(MUT(await_open_schema_for_coverage)(db, coverage_tier));
    JZ_TRY(jazz_db__db__ensure_supported_read_view(opts));
    if (opts->include_deleted)
        return jz_err_new(JAZZ_EC_QUERY, "relation snapshots do not support include_deleted yet");
    jazz_node_NodeState *owner = jazz_db__db__SharedNodeState__lock(db->node->node);
    jazz_node_ActiveSessionClaimsScope *scope;
    jz_err e = jazz_db__db__PreparedQuery__scoped_node(prepared, owner, author, &scope);
    if (JZ_IS_ERR(e)) { unlock_node(db); return e; }
    jazz_node_NodeState *node = jazz_node__node__ActiveSessionClaimsScope__node(scope);
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL)
        e = ne(NS(query_relation_snapshot_for_client)(node, prepared->shape, prepared->binding, RD_FN(client_authority_read_tier)(db, coverage_tier),
                                                      author, &opts->read_view, out));
    else
        e = ne(NS(query_relation_snapshot_for_serving_in_read_view)(node, prepared->shape, prepared->binding, coverage_tier, author, &opts->read_view, out));
    jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    unlock_node(db);
    return e;
}

/* Tier-gated canonical structured result read: the sole Jazz-boundary
 * materialization of relation facts into recursive output. */
jz_err RD_FN(all_result_tree)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_ResultTree *out) {
    jazz_node_RelationSnapshot snapshot;
    JZ_TRY(RD_FN(all_relation_snapshot)(db, prepared, opts, &snapshot));
    return jazz_db__db__materialize_result_tree(jazz_model__query__ValidatedQuery__query(prepared->shape), snapshot, out);
}

/* Tier-gated one-shot output-changing relation read evaluated as the database identity. */
jz_err RD_FN(all_relation_query)(const jazz_db_Db *db, const jazz_model_RelationQuery *query, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    jz_err e = MUT(await_open_schema_for_coverage)(db, jazz_db__db__read_coverage_tier(&opts));
    if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_default_read_view(&opts);
    if (JZ_IS_ERR(e)) { jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view); return e; }
    jazz_db_PreparedQuery prepared;
    e = RD_FN(prepare_relation_query_async)(db, query, &prepared);
    if (JZ_IS_ERR(e)) { jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view); return e; }
    /* The relation terminal already emits the requested aliases and row
     * identity; re-projecting would reinterpret alias positions. */
    jazz_db_CurrentRowVec rows = {0};
    e = RD_FN(all)(db, &prepared, opts, &rows);
    jazz_db__db__PreparedQuery__drop(&prepared);
    if (JZ_IS_ERR(e)) return e;
    out->root_count = rows.len;
    memcpy(&out->rows, &rows, sizeof out->rows);
    memset(&out->edges, 0, sizeof out->edges);
    return JZ_OK;
}

/* Install the `[local, global]` system plans a prepared query reuses, unless
 * its shape is not worth preparing or reads through a schema projection. */
static jz_err prepared_plans(jazz_node_NodeState *node, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_node_PreparedQueryPlanHandle *out[2]) {
    out[0] = out[1] = NULL;
    if (!jazz_db__db__should_install_prepared_plan(shape) || NS(uses_schema_projected_read)(node, shape)) return JZ_OK;
    jz_err e = ne(NS(prepared_query_plan)(node, shape, binding, JAZZ_DUR_LOCAL, jazz_types__ids__AuthorSubject__SYSTEM, &out[0]));
    if (JZ_IS_ERR(e)) return e;
    e = ne(NS(prepared_query_plan)(node, shape, binding, JAZZ_DUR_GLOBAL, jazz_types__ids__AuthorSubject__SYSTEM, &out[1]));
    if (JZ_IS_ERR(e)) { jazz_node__node__PreparedQueryPlanHandle__drop(out[0]); out[0] = NULL; }
    return e;
}
