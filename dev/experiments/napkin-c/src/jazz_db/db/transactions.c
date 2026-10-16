/* transactions.c: port of crates/jazz/layers/db/src/db/transactions.rs
 * Mergeable and exclusive transaction handles and staging.
 *
 * Async methods run synchronously: `lock().await` is the db-a node_runtime
 * lock (returns the NodeState, released with SharedNodeState__unlock), and
 * every queued `Box::pin(async move {..})` becomes a heap closure behind a
 * jazz_db_LocalFuture whose poll runs the body to completion. */
#include "jz_rt.h"
#include "../db.h"
#include "transactions.h"

#define TX_FN(n) jazz_db__db__transactions__Db__##n
#define NS(n) jazz_node__node__NodeState__##n
#define NODE(n) jazz_db__db__node_runtime__Node__##n
#define MUT(n) jazz_db__db__mutations__Db__##n
#define LC(n) jazz_db__db__lifecycle__Db__##n

/* `.map_err(Into::into)` from crate::node::Error */
static jz_err ne(jz_err e) {
    return JZ_IS_ERR(e) ? jazz_node__node__api_error__Error__from_node_error(e) : e;
}
static void unlock_node(const jazz_db_Db *db) { jazz_db__db__SharedNodeState__unlock(db->node->node); }
static void drop_cells(jz_btree *c) { jz_btree_drop(c); }
static jz_btree empty_cells(void) {
    jz_btree b;
    jz_btree_init(&b, &jazz_db_str_value_desc);
    return b;
}
static u64 now_or_next(const jazz_db_Db *db, bool has_now, u64 now_ms) {
    return has_now ? now_ms : MUT(next_now_ms)(db);
}
static u64 db_next_now_ms(const void *ctx) { return MUT(next_now_ms)((const jazz_db_Db *)ctx); }
static u64 captured_now_ms(const void *ctx) { return *(const u64 *)ctx; }
static jz_err row_not_visible_in_branch_view(jazz_types_RowUuid row) {
    char s[37];
    jz_uuid_to_string(&row, s);
    return jz_err_fmt(JAZZ_EC_NOT_OBSERVED, "row is not visible in branch view: %s", s);
}

static jz_err TX_FN(lock_for_transaction_open)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_node_NodeState **out) {
    return jazz_db__db__node_runtime__Node__lock_for_transaction_open(db->node, open_tx_id, db->owner_operation_admitted, out);
}

jz_err TX_FN(lock_for_transaction_operation)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_node_NodeState **out) {
    return jazz_db__db__node_runtime__Node__lock_for_transaction_operation(db->node, open_tx_id, db->owner_operation_admitted, out);
}

static jz_err TX_FN(lock_for_transaction_commit)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_node_NodeState **out) {
    for (;;) {
        jazz_node_NodeState *node;
        JZ_TRY(TX_FN(lock_for_transaction_operation)(db, open_tx_id, &node));
        if (!NODE(has_pending_local_publications)(db->node)) { *out = node; return JZ_OK; }
        bool needs = false;
        jz_err e = ne(NS(transaction_needs_large_value_staging)(node, open_tx_id, &needs));
        if (JZ_IS_ERR(e)) { unlock_node(db); return e; }
        if (!needs) { *out = node; return JZ_OK; }
        unlock_node(db); /* drop(node) */
        JZ_TRY(NODE(settle_local_publications)(db->node));
        /* Reacquire through the normal admission/tombstone checks after
         * suspension; cancellation must not revive an abandoned handle. */
    }
}

jz_err TX_FN(transaction_is_exclusive)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, bool *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, id, &node));
    jz_err e = ne(NS(transaction_is_exclusive)(node, id, out));
    unlock_node(db);
    return e;
}

/* The queries an exclusive read of `prepared` in `id` hydrates for the
 * sources it reads beyond its root: each source's narrowed read. Fails
 * when a source has none. */
jz_err TX_FN(exclusive_source_hydration_queries)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, const jazz_db_PreparedQuery *prepared, bool include_deleted, jazz_db_QueryVec *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, id, &node));
    jz_err e = ne(NS(exclusive_source_hydration_queries)(node, jazz_db__db__PreparedQuery__shape(prepared),
                                                         jazz_db__db__PreparedQuery__binding(prepared), include_deleted, out));
    unlock_node(db);
    return e;
}

jz_err TX_FN(begin_mergeable_with_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_db_WriteIdentity identity);
jz_err TX_FN(begin_exclusive_with_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_db_WriteIdentity identity);
jz_err TX_FN(open_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId id);

/* Build a mergeable transaction that commits multiple writes under one id. */
jz_err TX_FN(mergeable_tx)(const jazz_db_Db *db, jazz_db_TxHandle *out) {
    jazz_model_OpenTransactionId tx_id = jazz_model__model__transaction__OpenTransactionId__new();
    JZ_TRY(TX_FN(begin_mergeable_with_identity)(db, tx_id, (jazz_db_WriteIdentity){ .tag = JAZZ_WI_DATABASE }));
    out->db = db; out->tx_id = tx_id; out->committed = false;
    return JZ_OK;
}


/* Run `callback` in a mergeable transaction and commit all staged writes as one transaction.
 * If `callback` returns an error, the transaction is dropped without committing. */
jz_err TX_FN(transaction)(const jazz_db_Db *db, jazz_db_TxCallback callback, void *ctx, void *value_out, jazz_model_TxId *tx_id_out) {
    jazz_db_TxHandle tx;
    JZ_TRY(TX_FN(mergeable_tx)(db, &tx));
    jz_err e = callback(ctx, &tx, value_out);
    if (JZ_IS_ERR(e)) { jazz_db__db__MergeableTx__Drop__drop(&tx); return e; }
    return jazz_db__db__MergeableTx__commit(&tx, tx_id_out);
}

/* Build a mergeable transaction authored and permission-checked as `author`. */
jz_err TX_FN(mergeable_tx_for_identity)(const jazz_db_Db *db, jazz_types_AuthorSubject author, jazz_db_TxHandle *out) {
    jazz_model_OpenTransactionId tx_id = jazz_model__model__transaction__OpenTransactionId__new();
    JZ_TRY(TX_FN(begin_mergeable_with_identity)(db, tx_id, (jazz_db_WriteIdentity){ .tag = JAZZ_WI_SESSION, .subject = author }));
    out->db = db; out->tx_id = tx_id; out->committed = false;
    return JZ_OK;
}

jz_err TX_FN(transaction_for_identity)(const jazz_db_Db *db, jazz_types_AuthorSubject author, jazz_db_TxCallback callback, void *ctx, void *value_out, jazz_model_TxId *tx_id_out) {
    jazz_db_TxHandle tx;
    JZ_TRY(TX_FN(mergeable_tx_for_identity)(db, author, &tx));
    jz_err e = callback(ctx, &tx, value_out);
    if (JZ_IS_ERR(e)) { jazz_db__db__MergeableTx__Drop__drop(&tx); return e; }
    return jazz_db__db__MergeableTx__commit(&tx, tx_id_out);
}

/* Open a mergeable transaction and return its id. The caller owns this
 * transaction's lifetime (commit_mergeable_handle / abandon_transaction_handle). */
jz_err TX_FN(begin_mergeable)(const jazz_db_Db *db, jazz_model_OpenTransactionId id) {
    return TX_FN(begin_mergeable_with_identity)(db, id, (jazz_db_WriteIdentity){ .tag = JAZZ_WI_DATABASE });
}

jz_err TX_FN(begin_mergeable_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject author) {
    return TX_FN(begin_mergeable_with_identity)(db, id, (jazz_db_WriteIdentity){ .tag = JAZZ_WI_SESSION, .subject = author });
}

jz_err TX_FN(begin_mergeable_with_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_db_WriteIdentity identity) {
    JZ_TRY(LC(ensure_mutation_operation_admitted)(db));
    jazz_db_ResolvedWriteIdentity resolved;
    JZ_TRY(MUT(resolve_write_identity)(db, identity, &resolved));
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_open)(db, id, &node));
    jz_err e = ne(NS(open_mergeable)(node, id, resolved.made_by, resolved.has_permission_subject, resolved.permission_subject));
    unlock_node(db);
    return e;
}

/* Admission identity for a queued transaction begin: an attribution, a
 * session author, or the database itself, but never both of the first two. */
static jz_err queued_begin_identity(bool has_author, jazz_types_AuthorSubject author, bool has_attribution, jazz_types_AuthorSubject attribution, jazz_db_WriteIdentity *out) {
    if (has_author && has_attribution)
        return jz_err_new(JAZZ_EC_WRITE_REJECTED, "attributed transaction cannot override admission identity");
    if (has_attribution) { out->tag = JAZZ_WI_ATTRIBUTION; out->subject = attribution; }
    else if (has_author) { out->tag = JAZZ_WI_SESSION; out->subject = author; }
    else { out->tag = JAZZ_WI_DATABASE; }
    return JZ_OK;
}

/* ======================= queued owner operations ======================= */

typedef enum {
    QOP_BEGIN_MERGEABLE, QOP_BEGIN_EXCLUSIVE, QOP_INSERT, QOP_UPDATE, QOP_LARGE_VALUE_UPDATE, QOP_UPSERT,
    QOP_DELETE, QOP_RESTORE, QOP_COMMIT_MERGEABLE, QOP_COMMIT_EXCLUSIVE, QOP_ABANDON,
} jazz_db_QueuedTxOpKind;

/* The `async move` capture set of the enqueue_* closures. */
typedef struct jazz_db_QueuedTxOp {
    u32 kind;
    jazz_db_Db db;                       /* clone_for_owner_operation / clone_for_reserved_transaction */
    jazz_model_OpenTransactionId id;
    jazz_db_WriteIdentity identity;
    jz_str table;
    jazz_types_RowUuid row;
    jz_btree cells; bool has_cells;
    bool explicit_row_id;
    jazz_db_InsertOptions insert;
    jazz_db_UpdateOptions update;
    jazz_db_UpsertOptions upsert;
    jazz_db_DeleteOptions delete_;
    jazz_db_RestoreOptions restore;
    jz_vec mutations;                    /* Vec<LargeValueUpdate> */
    bool has_updated_at_ms; u64 updated_at_ms;
    jazz_model_TxId tx_id;               /* reserved final identity */
    u64 now_ms;
} jazz_db_QueuedTxOp;

static jz_err TX_FN(transaction_read_raw)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool *has, jz_btree *out);
static jz_err TX_FN(finish_exclusive_publication)(const jazz_db_Db *db, jazz_node_PublishedTransaction published, jazz_protocol_SyncMessage unit, jazz_model_TxId *out);

static jz_err queued_insert(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    bool exclusive;
    JZ_TRY(TX_FN(transaction_is_exclusive)(db, op->id, &exclusive));
    jz_slice table = jz_str_slice(&op->table);
    jz_btree cells = op->cells; op->has_cells = false;
    if (exclusive) {
        /* The id is fixed here so it can be returned synchronously;
         * only a caller-supplied id gets the create-only check. */
        jz_err e = jazz_db__db__ensure_transaction_identity(op->insert.identity);
        if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_exclusive_target(&op->insert.target);
        if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
        return TX_FN(stage_exclusive_insert)(db, op->id, table, op->row, cells, op->insert.has_updated_at_ms,
                                            op->insert.updated_at_ms, op->explicit_row_id);
    }
    jazz_db_TxHandle tx = { db, op->id, false }; /* mergeable_tx_ref(id) */
    jazz_types_RowUuid row;
    return jazz_db__db__MergeableTxOps__insert(&tx, table, cells, op->insert, &row);
}

static jz_err queued_update(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    bool exclusive;
    JZ_TRY(TX_FN(transaction_is_exclusive)(db, op->id, &exclusive));
    jz_slice table = jz_str_slice(&op->table);
    jazz_db_TxHandle tx = { db, op->id, false };
    jz_btree patch = op->cells; op->has_cells = false;
    if (exclusive) return jazz_db__db__ExclusiveTxOps__update(&tx, table, op->row, patch, op->update);
    return jazz_db__db__MergeableTxOps__update(&tx, table, op->row, patch, op->update);
}

static jz_err queued_upsert(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    bool exclusive;
    JZ_TRY(TX_FN(transaction_is_exclusive)(db, op->id, &exclusive));
    jz_slice table = jz_str_slice(&op->table);
    /* Typed bindings submit patches without a synchronous preflight.
     * Preserve complete-insert validation once the owner has resolved
     * the transaction's staged overlay (raw Rust APIs allow sparse rows). */
    if (op->upsert.target.tag == JAZZ_WT_ROOT) {
        bool has = false;
        jz_btree existing;
        JZ_TRY(TX_FN(transaction_read_raw)(db, op->id, table, op->row, &has, &existing));
        if (has) jz_btree_drop(&existing);
        else JZ_TRY(MUT(validate_complete_insert)(db, table, &op->cells));
    }
    jz_btree cells = op->cells; op->has_cells = false;
    if (exclusive) {
        jz_err e = jazz_db__db__ensure_transaction_identity(op->upsert.identity);
        if (!JZ_IS_ERR(e)) e = jazz_db__db__ensure_exclusive_view_target(&op->upsert.target);
        if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
        return TX_FN(stage_exclusive_upsert)(db, op->id, table, op->row, cells, op->upsert.has_updated_at_ms, op->upsert.updated_at_ms, true);
    }
    return TX_FN(stage_mergeable_upsert)(db, op->id, table, op->row, cells, op->upsert, true);
}

static jz_err queued_delete(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    bool exclusive;
    JZ_TRY(TX_FN(transaction_is_exclusive)(db, op->id, &exclusive));
    jz_slice table = jz_str_slice(&op->table);
    jazz_db_TxHandle tx = { db, op->id, false };
    if (exclusive) return jazz_db__db__ExclusiveTxOps__delete(&tx, table, op->row, op->delete_);
    return jazz_db__db__MergeableTxOps__delete(&tx, table, op->row, op->delete_);
}

static jz_err queued_restore(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    bool exclusive;
    JZ_TRY(TX_FN(transaction_is_exclusive)(db, op->id, &exclusive));
    jz_slice table = jz_str_slice(&op->table);
    jazz_db_TxHandle tx = { db, op->id, false };
    bool has_cells = op->has_cells;
    jz_btree cells = op->cells; op->has_cells = false;
    if (exclusive) return jazz_db__db__ExclusiveTxOps__restore(&tx, table, op->row, has_cells, cells, op->restore);
    return jazz_db__db__MergeableTxOps__restore(&tx, table, op->row, has_cells, cells, op->restore);
}

static jz_err queued_commit_mergeable(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_commit)(db, op->id, &node));
    jazz_node_PublishedTransaction published;
    /* `|| now_ms`: the host clock sample captured at enqueue time */
    jazz_db_NowFn now = { captured_now_ms, &op->now_ms };
    jz_err e = ne(NS(commit_mergeable_open_at)(node, op->id, op->tx_id, now, &published));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    /* debug_assert_eq!(published.tx_id, tx_id) */
    jazz_protocol_SyncMessage no_unit;
    memset(&no_unit, 0, sizeof no_unit);
    jazz_model_DurabilityTier tier;
    return MUT(settle_local_publication)(db, &published, false, no_unit, &tier);
}

static jz_err queued_commit_exclusive(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_commit)(db, op->id, &node));
    jazz_node_PublishedTransaction published;
    jazz_protocol_SyncMessage unit;
    jz_err e = ne(NS(commit_exclusive_bound_at)(node, op->id, op->tx_id, &published, &unit));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    jazz_model_TxId committed;
    JZ_TRY(TX_FN(finish_exclusive_publication)(db, published, unit, &committed));
    /* debug_assert_eq!(committed, tx_id) */
    return JZ_OK;
}

static jz_err queued_abandon(jazz_db_QueuedTxOp *op) {
    const jazz_db_Db *db = &op->db;
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    jz_err abandoned = NS(abandon_tx)(node, op->id);
    unlock_node(db);
    NODE(retire_queued_transaction_error)(db->node, op->id);
    if (JZ_IS_ERR(abandoned)) {
        if (abandoned.code != JAZZ_NODE_ERR_MISSING_OPEN_BATCH) return ne(abandoned);
        jz_err_drop(&abandoned);
    }
    NODE(evict_released_large_values)(db->node);
    return JZ_OK;
}

static bool queued_tx_op_poll(void *self, const jz_waker *cx, void *out) {
    jazz_db_QueuedTxOp *op = self;
    jz_err e;
    switch (op->kind) {
    case QOP_BEGIN_MERGEABLE: e = TX_FN(begin_mergeable_with_identity)(&op->db, op->id, op->identity); break;
    case QOP_BEGIN_EXCLUSIVE: e = TX_FN(begin_exclusive_with_identity)(&op->db, op->id, op->identity); break;
    case QOP_INSERT: e = queued_insert(op); break;
    case QOP_UPDATE: e = queued_update(op); break;
    case QOP_LARGE_VALUE_UPDATE: {
        jz_btree patch = op->cells; op->has_cells = false;
        jz_vec mutations = op->mutations; op->mutations = (jz_vec){0};
        e = MUT(stage_transaction_large_value_update)(&op->db, op->id, jz_str_slice(&op->table), op->row, patch, mutations,
                                                      op->has_updated_at_ms, op->updated_at_ms);
        break;
    }
    case QOP_UPSERT: e = queued_upsert(op); break;
    case QOP_DELETE: e = queued_delete(op); break;
    case QOP_RESTORE: e = queued_restore(op); break;
    case QOP_COMMIT_MERGEABLE: e = queued_commit_mergeable(op); break;
    case QOP_COMMIT_EXCLUSIVE: e = queued_commit_exclusive(op); break;
    default: e = queued_abandon(op); break;
    }
    *(jz_err *)out = e;
    return true;
}

static void queued_tx_op_drop(void *self) {
    jazz_db_QueuedTxOp *op = self;
    if (op->has_cells) jz_btree_drop(&op->cells);
    jz_str_drop(&op->table);
    jz_free(op->mutations.ptr);
    jazz_db__db__Db__drop(&op->db);
    jz_free(op);
}

static jazz_db_QueuedTxOp *queued_tx_op_new(const jazz_db_Db *db, u32 kind, jazz_model_OpenTransactionId id) {
    jazz_db_QueuedTxOp *op = JZ_NEW(jazz_db_QueuedTxOp);
    op->kind = kind;
    op->id = id;
    LC(clone_for_owner_operation)(db, &op->db);
    return op;
}

static jazz_db_LocalFuture queued_tx_op_future(jazz_db_QueuedTxOp *op) {
    return (jazz_db_LocalFuture){ op, queued_tx_op_poll, queued_tx_op_drop };
}

/* Queue mergeable transaction admission behind earlier owner operations. */
jz_err TX_FN(enqueue_begin_mergeable)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, bool has_author, jazz_types_AuthorSubject author, bool has_attribution, jazz_types_AuthorSubject attribution) {
    jazz_db_WriteIdentity identity;
    JZ_TRY(queued_begin_identity(has_author, author, has_attribution, attribution, &identity));
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_BEGIN_MERGEABLE, id);
    op->identity = identity;
    return NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op));
}

/* Return a non-owning operations handle for an already-open mergeable transaction. */
jazz_db_TxHandle TX_FN(mergeable_tx_ref)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id) {
    return (jazz_db_TxHandle){ db, tx_id, true /* never closes the transaction when dropped */ };
}

static jz_err TX_FN(reject_attributed_mergeable_branch)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    bool attributed = false;
    jz_err e = ne(NS(transaction_is_attributed)(node, tx_id, &attributed));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    if (attributed)
        return jz_err_new(JAZZ_EC_WRITE_REJECTED, "backend-attributed transactions do not support branch targets");
    return JZ_OK;
}

jz_err TX_FN(stage_mergeable_insert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms, bool known_fresh_row) {
    u64 now = now_or_next(db, has_now, now_ms);
    jz_err e = MUT(apply_insert_defaults)(db, table, &cells);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    jazz_node_NodeState *node;
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    e = ne(NS(tx_write_mergeable_in_schema)(node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now, known_fresh_row));
    unlock_node(db);
    return e;
}

static jz_err TX_FN(stage_mergeable_insert_in_branch_with_verified_inherited_cells)(
    const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector branch, jazz_types_RowUuid row,
    jz_btree cells, bool has_now, u64 now_ms, bool known_fresh_row, bool has_verified, jz_btree verified_inherited_cells,
    bool replace_pending_deletion, jazz_node_BranchViewCopyEvidence *branch_view_copy);

jz_err TX_FN(stage_mergeable_insert_in_branch)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector branch, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms, bool known_fresh_row) {
    jz_btree none;
    memset(&none, 0, sizeof none);
    return TX_FN(stage_mergeable_insert_in_branch_with_verified_inherited_cells)(db, tx_id, table, branch, row, cells, has_now, now_ms,
                                                                                 known_fresh_row, false, none, false, NULL);
}

/* Stage an exact branch write and, only for a branch-view fallback, carry
 * the engine-read base cells that may safely retain indirect values. */
static jz_err TX_FN(stage_mergeable_insert_in_branch_with_verified_inherited_cells)(
    const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector branch, jazz_types_RowUuid row,
    jz_btree cells, bool has_now, u64 now_ms, bool known_fresh_row, bool has_verified, jz_btree verified_inherited_cells,
    bool replace_pending_deletion, jazz_node_BranchViewCopyEvidence *branch_view_copy) {
    jz_err e = TX_FN(reject_attributed_mergeable_branch)(db, tx_id);
    if (!JZ_IS_ERR(e)) {
        u64 now = now_or_next(db, has_now, now_ms);
        e = MUT(apply_insert_defaults)(db, table, &cells);
        if (!JZ_IS_ERR(e)) {
            jazz_node_NodeState *node;
            e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
            if (!JZ_IS_ERR(e)) {
                e = ne(NS(tx_write_mergeable_in_schema_and_branch_with_verified_inherited_cells)(
                    node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now, branch, known_fresh_row,
                    has_verified, verified_inherited_cells, replace_pending_deletion, branch_view_copy));
                unlock_node(db);
                return e;
            }
        }
    }
    drop_cells(&cells);
    jazz_model__model__branch__BranchSelector__drop(&branch);
    if (has_verified) jz_btree_drop(&verified_inherited_cells);
    if (branch_view_copy) jazz_node__tx__BranchViewCopyEvidence__drop(branch_view_copy);
    return e;
}

jz_err TX_FN(stage_mergeable_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, bool has_now, u64 now_ms) {
    /* Match upsert's transaction-local visibility rule: staged inserts or
     * restores remain writable, but committed tombstones must reject a
     * patch rather than accepting content hidden by the deletion register. */
    u64 now = now_or_next(db, has_now, now_ms);
    jazz_node_NodeState *node;
    jz_err e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&patch); return e; }
    jazz_model_BranchSelector root;
    jazz_model__model__branch__BranchSelector__default(&root);
    bool visible = false;
    jz_btree visible_cells;
    e = ne(NS(tx_visible_current_cells_in_branch)(node, tx_id, table, row, &root, &visible, &visible_cells));
    jazz_model__model__branch__BranchSelector__drop(&root);
    if (JZ_IS_ERR(e)) { unlock_node(db); drop_cells(&patch); return e; }
    if (visible) jz_btree_drop(&visible_cells);
    else {
        bool has_winner = false;
        jazz_model_TxId winner;
        e = ne(NS(local_deletion_winner_tx_id_in_schema)(node, db->schema_version_id, table, row, &has_winner, &winner));
        if (JZ_IS_ERR(e)) { unlock_node(db); drop_cells(&patch); return e; }
        if (has_winner) { unlock_node(db); drop_cells(&patch); return jazz_db__db__row_already_deleted(row); }
    }
    e = ne(NS(tx_patch_mergeable_in_schema)(node, tx_id, db->schema_version_id, table, row, patch, true, now));
    unlock_node(db);
    return e;
}

jz_err TX_FN(stage_mergeable_update_in_branch_view)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base, jazz_types_RowUuid row, jazz_db_RowCells patch, bool has_now, u64 now_ms) {
    jz_err e = TX_FN(reject_attributed_mergeable_branch)(db, tx_id);
    if (JZ_IS_ERR(e)) goto fail;
    if (patch.len == 0) {
        e = jz_err_new(JAZZ_EC_SCHEMA, "branch-view update requires at least one authored column");
        goto fail;
    }
    u64 now = now_or_next(db, has_now, now_ms);
    const jazz_protocol_BranchViewBase *base_ref = has_base ? &base : NULL;
    jazz_node_NodeState *node;
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) goto fail;
    bool head_has = false;
    jz_btree head_cells;
    e = ne(NS(visible_current_cells_in_branch)(node, table, &head, row, &head_has, &head_cells));
    unlock_node(db);
    if (JZ_IS_ERR(e)) goto fail;
    if (head_has) {
        jz_btree_drop(&head_cells);
        e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
        if (JZ_IS_ERR(e)) goto fail;
        e = ne(NS(tx_patch_mergeable_in_schema_and_branch)(node, tx_id, db->schema_version_id, table, row, patch, true, now, head, false));
        unlock_node(db);
        if (has_base) jazz_protocol__protocol__BranchViewBase__drop(&base);
        return e;
    }
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) goto fail;
    bool inherited_has = false;
    jz_btree inherited;
    e = ne(NS(visible_current_cells_in_branch_view)(node, table, &head, base_ref, row, &inherited_has, &inherited));
    unlock_node(db);
    if (JZ_IS_ERR(e)) goto fail;
    if (!inherited_has) { e = row_not_visible_in_branch_view(row); goto fail; }
    jz_btree verified_inherited_cells;
    jazz_db__db__RowCells__clone(&verified_inherited_cells, &inherited);
    jazz_node_BranchViewCopyEvidence *branch_view_copy = NULL;
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (!JZ_IS_ERR(e)) {
        e = ne(NS(inherited_branch_view_copy_evidence)(node, table, &head, base_ref, row, &branch_view_copy));
        unlock_node(db);
    }
    if (JZ_IS_ERR(e)) { jz_btree_drop(&inherited); jz_btree_drop(&verified_inherited_cells); goto fail; }
    jz_btree_append(&inherited, &patch); /* inherited.extend(patch) */
    jz_btree_drop(&patch);
    if (has_base) jazz_protocol__protocol__BranchViewBase__drop(&base);
    return TX_FN(stage_mergeable_insert_in_branch_with_verified_inherited_cells)(db, tx_id, table, head, row, inherited, true, now, false,
                                                                                 true, verified_inherited_cells, false, branch_view_copy);
fail:
    drop_cells(&patch);
    jazz_model__model__branch__BranchSelector__drop(&head);
    if (has_base) jazz_protocol__protocol__BranchViewBase__drop(&base);
    return e;
}

jz_err TX_FN(mergeable_transaction_upsert_exists)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool *out) {
    bool exists = false;
    jz_btree cells;
    JZ_TRY(TX_FN(transaction_read)(db, tx_id, table, row, &exists, &cells));
    if (exists) jz_btree_drop(&cells);
    else JZ_TRY(MUT(ensure_row_not_deleted)(db, table, row));
    *out = exists;
    return JZ_OK;
}

jz_err TX_FN(stage_mergeable_upsert_in_branch_view)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base, jazz_types_RowUuid row, jz_btree cells, bool has_now, u64 now_ms, bool require_complete_insert);

/* Resolve a staged upsert in Rust, optionally checking typed insert completeness. */
jz_err TX_FN(stage_mergeable_upsert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options, bool require_complete_insert) {
    jz_err e = jazz_db__db__ensure_transaction_identity(options.identity);
    if (JZ_IS_ERR(e)) {
        drop_cells(&cells);
        if (options.target.tag == JAZZ_WT_BRANCH_VIEW) {
            jazz_model__model__branch__BranchSelector__drop(&options.target.head);
            if (options.target.has_base) jazz_protocol__protocol__BranchViewBase__drop(&options.target.base);
        }
        return e;
    }
    if (options.target.tag == JAZZ_WT_ROOT) {
        bool exists;
        e = TX_FN(mergeable_transaction_upsert_exists)(db, tx_id, table, row, &exists);
        if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
        if (exists) return TX_FN(stage_mergeable_update)(db, tx_id, table, row, cells, options.has_updated_at_ms, options.updated_at_ms);
        if (require_complete_insert) {
            e = MUT(validate_complete_insert)(db, table, &cells);
            if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
        }
        return TX_FN(stage_mergeable_insert)(db, tx_id, table, row, cells, options.has_updated_at_ms, options.updated_at_ms, false);
    }
    return TX_FN(stage_mergeable_upsert_in_branch_view)(db, tx_id, table, options.target.head, options.target.has_base, options.target.base,
                                                        row, cells, options.has_updated_at_ms, options.updated_at_ms, require_complete_insert);
}

jz_err TX_FN(stage_mergeable_upsert_in_branch_view)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base, jazz_types_RowUuid row, jz_btree cells, bool has_now, u64 now_ms, bool require_complete_insert) {
    const jazz_protocol_BranchViewBase *base_ref = has_base ? &base : NULL;
    jz_err e = TX_FN(reject_attributed_mergeable_branch)(db, tx_id);
    if (!JZ_IS_ERR(e)) e = MUT(ensure_branch_view_row_not_deleted)(db, table, &head, base_ref, row);
    if (JZ_IS_ERR(e)) goto fail;
    jazz_node_NodeState *node = jazz_db__db__SharedNodeState__lock(db->node->node);
    u32 state;
    e = ne(NS(tx_current_row_state_in_branch)(node, tx_id, table, row, &head, &state));
    if (JZ_IS_ERR(e)) { unlock_node(db); goto fail; }
    bool head_exists = false, replace_pending_deletion = false;
    if (state == JAZZ_TBRS_VISIBLE) {
        head_exists = true;
    } else if (state == JAZZ_TBRS_PENDING_DELETION) {
        jz_btree visible;
        e = ne(NS(visible_current_cells_in_branch)(node, table, &head, row, &head_exists, &visible));
        if (JZ_IS_ERR(e)) { unlock_node(db); goto fail; }
        if (head_exists) jz_btree_drop(&visible);
        replace_pending_deletion = true;
    }
    bool has_inherited = false;
    jz_btree inherited;
    if (!head_exists) {
        e = ne(NS(visible_current_cells_in_branch_view)(node, table, &head, base_ref, row, &has_inherited, &inherited));
        if (JZ_IS_ERR(e)) { unlock_node(db); goto fail; }
    }
    unlock_node(db); /* drop(node) */

    if (head_exists) {
        if (cells.len == 0) {
            e = jz_err_new(JAZZ_EC_SCHEMA, "branch upsert update requires at least one authored column");
            goto fail;
        }
        u64 now = now_or_next(db, has_now, now_ms);
        node = jazz_db__db__SharedNodeState__lock(db->node->node);
        e = ne(NS(tx_patch_mergeable_in_schema_and_branch)(node, tx_id, db->schema_version_id, table, row, cells, true, now, head, replace_pending_deletion));
        unlock_node(db);
        if (has_base) jazz_protocol__protocol__BranchViewBase__drop(&base);
        return e;
    }

    jz_btree verified_inherited_cells;
    memset(&verified_inherited_cells, 0, sizeof verified_inherited_cells);
    if (has_inherited) jazz_db__db__RowCells__clone(&verified_inherited_cells, &inherited);
    jazz_node_BranchViewCopyEvidence *branch_view_copy = NULL;
    if (has_inherited) {
        e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
        if (!JZ_IS_ERR(e)) {
            e = ne(NS(inherited_branch_view_copy_evidence)(node, table, &head, base_ref, row, &branch_view_copy));
            unlock_node(db);
        }
        if (JZ_IS_ERR(e)) { jz_btree_drop(&inherited); jz_btree_drop(&verified_inherited_cells); goto fail; }
    }
    jz_btree inserted = has_inherited ? inherited : empty_cells(); /* unwrap_or_default */
    jz_btree_append(&inserted, &cells);
    jz_btree_drop(&cells);
    if (has_base) jazz_protocol__protocol__BranchViewBase__drop(&base);
    if (require_complete_insert) {
        e = MUT(validate_complete_insert)(db, table, &inserted);
        if (JZ_IS_ERR(e)) {
            jz_btree_drop(&inserted);
            if (has_inherited) jz_btree_drop(&verified_inherited_cells);
            if (branch_view_copy) jazz_node__tx__BranchViewCopyEvidence__drop(branch_view_copy);
            jazz_model__model__branch__BranchSelector__drop(&head);
            return e;
        }
    }
    return TX_FN(stage_mergeable_insert_in_branch_with_verified_inherited_cells)(db, tx_id, table, head, row, inserted, has_now, now_ms, false,
                                                                                 has_inherited, verified_inherited_cells, replace_pending_deletion,
                                                                                 branch_view_copy);
fail:
    drop_cells(&cells);
    jazz_model__model__branch__BranchSelector__drop(&head);
    if (has_base) jazz_protocol__protocol__BranchViewBase__drop(&base);
    return e;
}

jz_err TX_FN(stage_mergeable_delete)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool has_now, u64 now_ms) {
    u64 now = now_or_next(db, has_now, now_ms);
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    jz_err e = ne(NS(tx_write_mergeable_in_schema)(node, tx_id, db->schema_version_id, table, row, empty_cells(), true, JAZZ_DELETION_DELETED, true, now, false));
    unlock_node(db);
    return e;
}

jz_err TX_FN(stage_mergeable_delete_in_branch_view)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base, jazz_types_RowUuid row, bool has_now, u64 now_ms) {
    jz_err e = TX_FN(reject_attributed_mergeable_branch)(db, tx_id);
    jazz_node_NodeState *node;
    if (!JZ_IS_ERR(e)) e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (!JZ_IS_ERR(e)) {
        bool has = false;
        jz_btree visible;
        e = ne(NS(visible_current_cells_in_branch_view)(node, table, &head, has_base ? &base : NULL, row, &has, &visible));
        unlock_node(db);
        if (!JZ_IS_ERR(e)) {
            if (has) jz_btree_drop(&visible);
            else e = row_not_visible_in_branch_view(row);
        }
    }
    if (has_base) jazz_protocol__protocol__BranchViewBase__drop(&base);
    if (JZ_IS_ERR(e)) { jazz_model__model__branch__BranchSelector__drop(&head); return e; }
    u64 now = now_or_next(db, has_now, now_ms);
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { jazz_model__model__branch__BranchSelector__drop(&head); return e; }
    e = ne(NS(tx_write_mergeable_in_schema_and_branch)(node, tx_id, db->schema_version_id, table, row, empty_cells(), true, JAZZ_DELETION_DELETED,
                                                       true, now, head, false));
    unlock_node(db);
    return e;
}

jz_err TX_FN(stage_mergeable_restore)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms) {
    jz_err e = TX_FN(reject_attributed_mergeable_branch)(db, tx_id);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    u64 now = now_or_next(db, has_now, now_ms);
    e = MUT(apply_insert_defaults)(db, table, &cells);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    jazz_node_NodeState *node;
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    e = ne(NS(tx_write_mergeable_in_schema)(node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now, false));
    if (!JZ_IS_ERR(e))
        e = ne(NS(tx_write_mergeable_in_schema)(node, tx_id, db->schema_version_id, table, row, empty_cells(), true, JAZZ_DELETION_RESTORED, true, now, false));
    unlock_node(db);
    return e;
}

jz_err TX_FN(stage_mergeable_restore_in_branch)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector branch, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms) {
    u64 now = now_or_next(db, has_now, now_ms);
    jz_err e = MUT(apply_insert_defaults)(db, table, &cells);
    jazz_node_NodeState *node;
    if (!JZ_IS_ERR(e)) e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); jazz_model__model__branch__BranchSelector__drop(&branch); return e; }
    jazz_model_BranchSelector branch_clone;
    jazz_model__model__branch__BranchSelector__clone(&branch_clone, &branch);
    e = ne(NS(tx_write_mergeable_in_schema_and_branch)(node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now, branch_clone, false));
    if (!JZ_IS_ERR(e))
        e = ne(NS(tx_write_mergeable_in_schema_and_branch)(node, tx_id, db->schema_version_id, table, row, empty_cells(), true, JAZZ_DELETION_RESTORED,
                                                           true, now, branch, false));
    else
        jazz_model__model__branch__BranchSelector__drop(&branch);
    unlock_node(db);
    return e;
}

/* Commit an owned mergeable transaction handle. */
jz_err TX_FN(commit_mergeable_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_model_TxId *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_commit)(db, open_tx_id, &node));
    jazz_node_PublishedTransaction published;
    jz_err e = ne(NS(commit_mergeable_open)(node, open_tx_id, (jazz_db_NowFn){ db_next_now_ms, db }, &published));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    jazz_model_TxId tx_id = published.tx_id;
    jazz_protocol_SyncMessage no_unit;
    memset(&no_unit, 0, sizeof no_unit);
    jazz_model_DurabilityTier tier;
    JZ_TRY(MUT(settle_local_publication)(db, &published, false, no_unit, &tier));
    *out = tx_id;
    return JZ_OK;
}

/* Reserve a queued commit using the host's Unix-millisecond clock sample.
 * Bindings must supply this separately from per-row provenance timestamps:
 * staging may still be queued when the final identity is returned. */
jz_err TX_FN(enqueue_commit_mergeable_handle_at_ms)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, u64 now_ms, jazz_db_WriteHandle *out) {
    jazz_model_TxId tx_id;
    JZ_TRY(LC(reserve_transaction_id_at_ms)(db, now_ms, &tx_id));
    jazz_db_QueuedTxOp *op = JZ_NEW(jazz_db_QueuedTxOp);
    op->kind = QOP_COMMIT_MERGEABLE;
    op->id = open_tx_id;
    op->tx_id = tx_id;
    op->now_ms = now_ms;
    LC(clone_for_reserved_transaction)(db, tx_id, &op->db);
    jazz_db_QueuedMutationStatus *status = NODE(enqueue_transaction_commit)(db->node, open_tx_id, tx_id, JAZZ_TX_MERGEABLE, queued_tx_op_future(op));
    jazz_types_RowUuid zero;
    memset(&zero, 0, sizeof zero);
    MUT(queued_write_handle)(db, zero, tx_id, status, NULL, out);
    return JZ_OK;
}

/* Reserve the final identity and retain mergeable commit/finalization on
 * the node owner. */
jz_err TX_FN(enqueue_commit_mergeable_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_db_WriteHandle *out) {
    return TX_FN(enqueue_commit_mergeable_handle_at_ms)(db, open_tx_id, MUT(next_now_ms)(db), out);
}

/* Abandon an owned open transaction handle. */
jz_err TX_FN(abandon_transaction_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id) {
    NODE(mark_transaction_abandoned)(db->node, open_tx_id);
    jazz_node_NodeState *node = jazz_db__db__LocalMutexBorrow__borrow(db->node->node, "abandon_transaction_handle");
    jz_err result = ne(NS(abandon_tx)(node, open_tx_id));
    /* Eviction is async: the next tick releases this transaction's
     * staged large-value roots. */
    if (NS(has_released_large_values)(node)) NODE(schedule_tick)(db->node, JAZZ_TICK_IMMEDIATE);
    unlock_node(db);
    NODE(clear_transaction_abandonment)(db->node, open_tx_id);
    return result;
}

/* Queue rollback after all earlier admission/staging work. Missing state
 * is accepted because a failed queued begin is already terminal. */
void TX_FN(enqueue_abandon_transaction_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id) {
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_ABANDON, open_tx_id);
    NODE(enqueue_transaction_cleanup)(db->node, queued_tx_op_future(op));
}

/* Open an exclusive transaction over the current local snapshot (RAII flavour). */
jz_err TX_FN(exclusive_tx)(const jazz_db_Db *db, jazz_db_TxHandle *out) {
    jazz_model_OpenTransactionId tx_id = jazz_model__model__transaction__OpenTransactionId__new();
    JZ_TRY(TX_FN(open_exclusive_handle)(db, tx_id));
    out->db = db; out->tx_id = tx_id; out->committed = false;
    return JZ_OK;
}

jz_err TX_FN(begin_exclusive)(const jazz_db_Db *db, jazz_model_OpenTransactionId id) {
    return TX_FN(begin_exclusive_with_identity)(db, id, (jazz_db_WriteIdentity){ .tag = JAZZ_WI_DATABASE });
}

/* Open an exclusive transaction whose identity is fixed for its lifetime. */
jz_err TX_FN(begin_exclusive_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject author) {
    return TX_FN(begin_exclusive_with_identity)(db, id, (jazz_db_WriteIdentity){ .tag = JAZZ_WI_SESSION, .subject = author });
}

jz_err TX_FN(begin_exclusive_with_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_db_WriteIdentity identity) {
    JZ_TRY(LC(ensure_mutation_operation_admitted)(db));
    jazz_db_ResolvedWriteIdentity resolved;
    JZ_TRY(MUT(resolve_write_identity)(db, identity, &resolved));
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_open)(db, id, &node));
    jz_err e = ne(NS(open_exclusive_with_identity)(node, id, resolved.made_by,
                                                   resolved.has_permission_subject ? resolved.permission_subject : resolved.made_by));
    unlock_node(db);
    return e;
}

/* Queue exclusive snapshot admission behind earlier owner operations. */
jz_err TX_FN(enqueue_begin_exclusive)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, bool has_author, jazz_types_AuthorSubject author, bool has_attribution, jazz_types_AuthorSubject attribution) {
    jazz_db_WriteIdentity identity;
    JZ_TRY(queued_begin_identity(has_author, author, has_attribution, attribution, &identity));
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_BEGIN_EXCLUSIVE, id);
    op->identity = identity;
    return NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op));
}

/* Return a non-owning operations handle for an already-open exclusive transaction. */
jazz_db_TxHandle TX_FN(exclusive_tx_ref)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id) {
    return (jazz_db_TxHandle){ db, tx_id, true };
}

jz_err TX_FN(enqueue_transaction_insert)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_db_RowCells cells, jazz_db_InsertOptions options, jazz_types_RowUuid *out) {
    bool explicit_row_id = options.has_row_id;
    jazz_types_RowUuid row = options.has_row_id ? options.row_id : jazz_db__db__Db__next_row_id(db);
    options.has_row_id = true;
    options.row_id = row;
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_INSERT, id);
    op->table = table;
    op->cells = cells; op->has_cells = true;
    op->insert = options;
    op->row = row;
    op->explicit_row_id = explicit_row_id;
    JZ_TRY(NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op)));
    *out = row;
    return JZ_OK;
}

jz_err TX_FN(enqueue_transaction_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_RowCells patch, jazz_db_UpdateOptions options) {
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_UPDATE, id);
    op->table = table; op->row = row;
    op->cells = patch; op->has_cells = true;
    op->update = options;
    return NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op));
}

/* Queue typed partial large-value descriptors (the `applyDiffs` DSL)
 * behind earlier operations of an open transaction. */
jz_err TX_FN(enqueue_transaction_large_value_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_RowCells patch, jz_vec mutations, bool has_updated_at_ms, u64 updated_at_ms) {
    jz_err e = jazz_db__db__mutations__validate_updated_at_ms(has_updated_at_ms, updated_at_ms);
    if (JZ_IS_ERR(e)) { jz_str_drop(&table); jz_btree_drop(&patch); jz_free(mutations.ptr); return e; }
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_LARGE_VALUE_UPDATE, id);
    op->table = table; op->row = row;
    op->cells = patch; op->has_cells = true;
    op->mutations = mutations;
    op->has_updated_at_ms = has_updated_at_ms; op->updated_at_ms = updated_at_ms;
    return NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op));
}

jz_err TX_FN(enqueue_transaction_upsert)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options) {
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_UPSERT, id);
    op->table = table; op->row = row;
    op->cells = cells; op->has_cells = true;
    op->upsert = options;
    return NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op));
}

jz_err TX_FN(enqueue_transaction_delete)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_DeleteOptions options) {
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_DELETE, id);
    op->table = table; op->row = row;
    op->delete_ = options;
    return NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op));
}

jz_err TX_FN(enqueue_transaction_restore)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, bool has_cells, jazz_db_RowCells cells, jazz_db_RestoreOptions options) {
    jazz_db_QueuedTxOp *op = queued_tx_op_new(db, QOP_RESTORE, id);
    op->table = table; op->row = row;
    op->has_cells = has_cells;
    if (has_cells) op->cells = cells;
    op->restore = options;
    return NODE(enqueue_transaction_operation)(db->node, id, queued_tx_op_future(op));
}

jz_err TX_FN(transaction_read)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_db_RowCells *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    *has = false;
    jz_err e = ne(NS(tx_read_in_schema)(node, tx_id, db->schema_version_id, table, row, has, out));
    if (!JZ_IS_ERR(e) && *has) {
        e = ne(NS(hydrate_large_value_cells)(node, out));
        if (JZ_IS_ERR(e)) { jz_btree_drop(out); *has = false; }
    }
    unlock_node(db);
    return e;
}

/* Read the storage-form transaction row used as a mutation base.
 * Mutation staging must preserve existing large-value locators; public
 * transaction reads hydrate those values separately above. */
static jz_err TX_FN(transaction_read_raw)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool *has, jz_btree *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    *has = false;
    jz_err e = ne(NS(tx_read_in_schema)(node, tx_id, db->schema_version_id, table, row, has, out));
    unlock_node(db);
    return e;
}

static jz_err TX_FN(transaction_all_in_authorization_mode)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, u32 authorization_mode, jazz_db_CurrentRowVec *out);
static jz_err TX_FN(transaction_relation_snapshot_in_authorization_mode)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, u32 authorization_mode, jazz_node_RelationSnapshot *out);

jz_err TX_FN(transaction_all)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out) {
    return TX_FN(transaction_all_in_authorization_mode)(db, tx_id, prepared, db->identity.author, opts, JAZZ_QAM_CLIENT_LOCAL, out);
}

jz_err TX_FN(transaction_all_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out) {
    return TX_FN(transaction_all_in_authorization_mode)(db, tx_id, prepared, author, opts, JAZZ_QAM_TRUSTED_SERVING, out);
}

/* Binding-facing transaction read selected by open id, with an optional
 * trusted-serving identity. */
jz_err TX_FN(all_in_open_transaction)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, bool has_author, jazz_types_AuthorSubject author, jazz_db_CurrentRowVec *out) {
    if (has_author) return TX_FN(transaction_all_for_identity)(db, tx_id, prepared, author, opts, out);
    return TX_FN(transaction_all)(db, tx_id, prepared, opts, out);
}

jz_err TX_FN(transaction_relation_snapshot)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    return TX_FN(transaction_relation_snapshot_in_authorization_mode)(db, tx_id, prepared, db->identity.author, opts, JAZZ_QAM_CLIENT_LOCAL, out);
}

jz_err TX_FN(transaction_relation_snapshot_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out) {
    return TX_FN(transaction_relation_snapshot_in_authorization_mode)(db, tx_id, prepared, author, opts, JAZZ_QAM_TRUSTED_SERVING, out);
}

/* Binding-facing relation snapshot selected by open transaction id. */
jz_err TX_FN(relation_snapshot_in_open_transaction)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, bool has_author, jazz_types_AuthorSubject author, jazz_node_RelationSnapshot *out) {
    if (has_author) return TX_FN(transaction_relation_snapshot_for_identity)(db, tx_id, prepared, author, opts, out);
    return TX_FN(transaction_relation_snapshot)(db, tx_id, prepared, opts, out);
}

static jz_err TX_FN(transaction_relation_snapshot_in_authorization_mode)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, u32 authorization_mode, jazz_node_RelationSnapshot *out) {
    jz_err e = jazz_db__db__ensure_default_read_view(&opts);
    jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
    if (JZ_IS_ERR(e)) return e;
    jazz_node_NodeState *locked;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &locked));
    jazz_node_ActiveSessionClaimsScope *scope;
    e = jazz_db__db__PreparedQuery__scoped_node(prepared, locked, author, &scope);
    if (JZ_IS_ERR(e)) { unlock_node(db); return e; }
    jazz_node_NodeState *node = jazz_node__node__ActiveSessionClaimsScope__node(scope);
    const jazz_model_ValidatedQuery *shape = jazz_db__db__PreparedQuery__shape(prepared);
    const jazz_model_Binding *binding = jazz_db__db__PreparedQuery__binding(prepared);
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL)
        e = ne(NS(tx_relation_snapshot_with_options)(node, tx_id, shape, binding, opts.include_deleted, out));
    else
        e = ne(NS(tx_relation_snapshot_for_identity_with_options)(node, tx_id, shape, binding, author, opts.include_deleted, out));
    if (!JZ_IS_ERR(e)) {
        e = ne(NS(hydrate_current_rows)(node, (jazz_db_CurrentRowVec *)&out->rows));
        if (JZ_IS_ERR(e)) jazz_node__node__RelationSnapshot__drop(out);
    }
    jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    unlock_node(db);
    return e;
}

static jz_err TX_FN(transaction_all_in_authorization_mode)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, u32 authorization_mode, jazz_db_CurrentRowVec *out) {
    jz_err e = jazz_db__db__ensure_default_read_view(&opts);
    jazz_protocol__protocol__ReadViewSpec__drop(&opts.read_view);
    if (JZ_IS_ERR(e)) return e;
    jazz_node_NodeState *locked;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &locked));
    jazz_node_ActiveSessionClaimsScope *scope;
    e = jazz_db__db__PreparedQuery__scoped_node(prepared, locked, author, &scope);
    if (JZ_IS_ERR(e)) { unlock_node(db); return e; }
    jazz_node_NodeState *node = jazz_node__node__ActiveSessionClaimsScope__node(scope);
    const jazz_model_ValidatedQuery *shape = jazz_db__db__PreparedQuery__shape(prepared);
    const jazz_model_Binding *binding = jazz_db__db__PreparedQuery__binding(prepared);
    if (authorization_mode == JAZZ_QAM_CLIENT_LOCAL)
        e = ne(NS(tx_query_with_options)(node, tx_id, shape, binding, opts.include_deleted, out));
    else
        e = ne(NS(tx_query_for_identity_with_options)(node, tx_id, shape, binding, author, opts.include_deleted, out));
    if (!JZ_IS_ERR(e)) e = ne(NS(hydrate_current_rows)(node, out));
    jazz_node__node__ActiveSessionClaimsScope__drop(scope);
    unlock_node(db);
    return e;
}

jz_err TX_FN(transaction_current_rows)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_db_CurrentRowVec *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    jz_err e = ne(NS(tx_current_rows)(node, tx_id, table, out));
    if (!JZ_IS_ERR(e)) e = ne(NS(hydrate_current_rows)(node, out));
    unlock_node(db);
    return e;
}

static jz_err TX_FN(exclusive_transaction_target_for_write)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, const char *operation, bool allow_absent, bool *has, jz_btree *out);

jz_err TX_FN(stage_exclusive_insert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms, bool explicit_row_id) {
    u64 now = now_or_next(db, has_now, now_ms);
    jz_err e = MUT(apply_insert_defaults)(db, table, &cells);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    if (explicit_row_id) {
        /* An explicit-id insert only creates. Deciding whether the target
         * exists is a user read (ch. 7), so resolve it exactly as UPSERT
         * does: a row hidden by read policy is rejected with UPSERT's
         * error, never disclosed as an existing row. */
        bool exists = false;
        jz_btree existing;
        e = TX_FN(exclusive_transaction_target_for_write)(db, tx_id, table, row, "UPSERT", true, &exists, &existing);
        if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
        if (exists) { jz_btree_drop(&existing); drop_cells(&cells); return jazz_db__db__row_already_exists(table, row); }
    }
    jazz_node_NodeState *node;
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    if (explicit_row_id) {
        u32 state;
        e = ne(NS(tx_insert_target_state_in_schema)(node, tx_id, db->schema_version_id, table, row, &state));
        if (JZ_IS_ERR(e)) { unlock_node(db); drop_cells(&cells); return e; }
        if (state == JAZZ_TITS_DELETED) { unlock_node(db); drop_cells(&cells); return jazz_db__db__row_already_deleted(row); }
    }
    e = ne(NS(tx_write_in_schema_at_ms)(node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now));
    unlock_node(db);
    return e;
}

jz_err TX_FN(stage_exclusive_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, bool has_now, u64 now_ms) {
    u64 now = now_or_next(db, has_now, now_ms);
    bool has = false;
    jz_btree cells;
    jz_err e = TX_FN(exclusive_transaction_target_for_write)(db, tx_id, table, row, "UPDATE", false, &has, &cells);
    if (JZ_IS_ERR(e)) { drop_cells(&patch); return e; }
    if (!has) jz_panic("exclusive UPDATE requires a visible target");
    jz_btree_append(&cells, &patch);
    jz_btree_drop(&patch);
    jazz_node_NodeState *node;
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    e = ne(NS(tx_write_in_schema_at_ms)(node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now));
    unlock_node(db);
    return e;
}

jz_err TX_FN(stage_exclusive_upsert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, bool has_now, u64 now_ms, bool require_complete_insert) {
    u64 now = now_or_next(db, has_now, now_ms);
    bool has = false;
    jz_btree existing;
    jz_err e = TX_FN(exclusive_transaction_target_for_write)(db, tx_id, table, row, "UPSERT", true, &has, &existing);
    if (JZ_IS_ERR(e)) { drop_cells(&patch); return e; }
    if (require_complete_insert && !has) {
        e = MUT(validate_complete_insert)(db, table, &patch);
        if (JZ_IS_ERR(e)) { drop_cells(&patch); return e; }
    }
    jz_btree cells = has ? existing : empty_cells();
    jz_btree_append(&cells, &patch);
    jz_btree_drop(&patch);
    e = MUT(apply_insert_defaults)(db, table, &cells);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    jazz_node_NodeState *node;
    e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    e = ne(NS(tx_write_in_schema_at_ms)(node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now));
    unlock_node(db);
    return e;
}

/* Read the storage-form row a transaction write builds on, as the
 * transaction sees it (its snapshot plus its own staged writes), after
 * proving the transaction's permission subject may read it. */
jz_err TX_FN(transaction_target_for_write)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, const char *operation, bool allow_absent, bool *has, jz_btree *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    jazz_types_AuthorSubject identity;
    jz_err e = ne(NS(transaction_permission_subject)(node, tx_id, &identity));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    const jazz_node_TableSchema *schema;
    JZ_TRY(MUT(table_schema)(db, table, &schema));
    const jazz_node_PolicyQuery *policy_ref = jazz_node__schema__TableSchema__read_policy(schema);
    jazz_node_PolicyQuery *read_policy = policy_ref ? jazz_node__schema__PolicyQuery__clone(policy_ref) : NULL;
    /* This authoritative point read distinguishes a hidden target from a
     * genuinely absent one and records the exact snapshot/absence read for
     * conflict detection. Its result is never returned to the session. */
    bool target = false;
    e = TX_FN(transaction_read_raw)(db, tx_id, table, row, &target, out);
    if (JZ_IS_ERR(e)) { if (read_policy) jazz_node__schema__PolicyQuery__drop(read_policy); return e; }
    bool visible;
    if (!target) visible = false;
    else if (identity.tag == JAZZ_AUTHOR_SYSTEM) visible = true;
    else if (!read_policy) visible = true;
    else {
        e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
        if (!JZ_IS_ERR(e)) {
            e = ne(NS(read_policy_query_allows_open_tx_row)(node, tx_id, read_policy, db->schema_version_id, row, identity, &visible));
            unlock_node(db);
        }
        if (JZ_IS_ERR(e)) { jazz_node__schema__PolicyQuery__drop(read_policy); jz_btree_drop(out); return e; }
    }
    if (read_policy) jazz_node__schema__PolicyQuery__drop(read_policy);
    if ((target && !visible) || (!target && !allow_absent)) {
        if (target) jz_btree_drop(out);
        return jazz_db__db__read_for_write_denied(operation, table);
    }
    *has = target;
    return JZ_OK;
}

/* Resolve an exclusive mutation target through the identity fixed when
 * the transaction opened, then record the corresponding snapshot row or
 * absence read used by optimistic conflict validation. */
static jz_err TX_FN(exclusive_transaction_target_for_write)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, const char *operation, bool allow_absent, bool *has, jz_btree *out) {
    /* Exclusive transactions are the only callers that may skip the kind
     * check: the subject lookup rejects a mergeable id. */
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    jazz_types_AuthorSubject subject;
    jz_err e = ne(NS(exclusive_transaction_permission_subject)(node, tx_id, &subject));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    return TX_FN(transaction_target_for_write)(db, tx_id, table, row, operation, allow_absent, has, out);
}

jz_err TX_FN(stage_exclusive_delete)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool has_now, u64 now_ms) {
    u64 now = now_or_next(db, has_now, now_ms);
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_operation)(db, tx_id, &node));
    jz_err e = ne(NS(tx_write_in_schema_at_ms)(node, tx_id, db->schema_version_id, table, row, empty_cells(), true, JAZZ_DELETION_DELETED, true, now));
    unlock_node(db);
    return e;
}

jz_err TX_FN(stage_exclusive_restore)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms) {
    u64 now = now_or_next(db, has_now, now_ms);
    jz_err e = MUT(apply_insert_defaults)(db, table, &cells);
    jazz_node_NodeState *node;
    if (!JZ_IS_ERR(e)) e = TX_FN(lock_for_transaction_operation)(db, tx_id, &node);
    if (JZ_IS_ERR(e)) { drop_cells(&cells); return e; }
    /* Restore needs one content version and one deletion-register version:
     * `tx_write` rejects a version carrying both. The layers have separate
     * winners and parent chains; keep this staged form aligned with the
     * committed restore path. */
    e = ne(NS(tx_write_in_schema_at_ms)(node, tx_id, db->schema_version_id, table, row, cells, false, 0, true, now));
    if (!JZ_IS_ERR(e))
        e = ne(NS(tx_write_in_schema_at_ms)(node, tx_id, db->schema_version_id, table, row, empty_cells(), true, JAZZ_DELETION_RESTORED, true, now));
    unlock_node(db);
    return e;
}

/* Commit an owned exclusive transaction handle. */
jz_err TX_FN(commit_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_model_TxId *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_commit)(db, open_tx_id, &node));
    jazz_node_PublishedTransaction published;
    jazz_protocol_SyncMessage unit;
    jz_err e = ne(NS(commit_exclusive_bound)(node, open_tx_id, MUT(next_now_ms)(db), &published, &unit));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    return TX_FN(finish_exclusive_publication)(db, published, unit, out);
}

/* Exclusive counterpart of enqueue_commit_mergeable_handle_at_ms. */
jz_err TX_FN(enqueue_commit_exclusive_handle_at_ms)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, u64 now_ms, jazz_db_WriteHandle *out) {
    jazz_model_TxId tx_id;
    JZ_TRY(LC(reserve_transaction_id_at_ms)(db, now_ms, &tx_id));
    jazz_db_QueuedTxOp *op = JZ_NEW(jazz_db_QueuedTxOp);
    op->kind = QOP_COMMIT_EXCLUSIVE;
    op->id = open_tx_id;
    op->tx_id = tx_id;
    op->now_ms = now_ms;
    LC(clone_for_reserved_transaction)(db, tx_id, &op->db);
    jazz_db_QueuedMutationStatus *status = NODE(enqueue_transaction_commit)(db->node, open_tx_id, tx_id, JAZZ_TX_EXCLUSIVE, queued_tx_op_future(op));
    jazz_types_RowUuid zero;
    memset(&zero, 0, sizeof zero);
    MUT(queued_write_handle)(db, zero, tx_id, status, NULL, out);
    return JZ_OK;
}

/* Reserve the final identity and retain exclusive serializability,
 * publication, and finalization on the node owner. */
jz_err TX_FN(enqueue_commit_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_db_WriteHandle *out) {
    return TX_FN(enqueue_commit_exclusive_handle_at_ms)(db, open_tx_id, MUT(next_now_ms)(db), out);
}

/* Commit an owned exclusive transaction as an explicit policy identity. */
jz_err TX_FN(commit_exclusive_handle_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_types_AuthorSubject author, jazz_model_TxId *out) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_commit)(db, open_tx_id, &node));
    jazz_node_PublishedTransaction published;
    jazz_protocol_SyncMessage unit;
    jz_err e = ne(NS(commit_exclusive)(node, open_tx_id, author, MUT(next_now_ms)(db), &published, &unit));
    unlock_node(db);
    if (JZ_IS_ERR(e)) return e;
    return TX_FN(finish_exclusive_publication)(db, published, unit, out);
}

static jz_err TX_FN(finish_exclusive_publication)(const jazz_db_Db *db, jazz_node_PublishedTransaction published, jazz_protocol_SyncMessage unit, jazz_model_TxId *out) {
    jazz_model_TxId tx_id = published.tx_id;
    jazz_model_DurabilityTier tier;
    JZ_TRY(MUT(settle_local_publication)(db, &published, true, unit, &tier));
    *out = tx_id;
    return JZ_OK;
}

/* Abandon an owned exclusive transaction handle. */
jz_err TX_FN(abandon_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id) {
    return TX_FN(abandon_transaction_handle)(db, open_tx_id);
}

static jz_err TX_FN(open_exclusive_handle_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject author) {
    jazz_node_NodeState *node;
    JZ_TRY(TX_FN(lock_for_transaction_open)(db, id, &node));
    jz_err e = ne(NS(open_exclusive_for_identity)(node, id, author));
    unlock_node(db);
    return e;
}

jz_err TX_FN(open_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId id) {
    return TX_FN(open_exclusive_handle_for_identity)(db, id, db->identity.author);
}
