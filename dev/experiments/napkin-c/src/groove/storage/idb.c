/* groove::storage::idb (crates/groove/src/storage/idb.rs), non-test part.
 *
 * async -> sync: the futures::lock::Mutex mutation gate becomes a flag held
 * for the duration of each serialized call; IdbTree operations complete
 * synchronously, so read_resident's Poll::Pending (cold page) branch cannot
 * arise and its serialized attempt is always the answering one. The
 * cooperative back-off yields drive the task queue (jz_task_run_ready). */
#include "jz_rt.h"
#include "idb.h"

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static const jz_btree_desc CF_SET_DESC = {sizeof(jz_str), 0, jz_cmp_str, str_drop_v, NULL};

static void idb_shared_drop(void *p) {
    groove_IdbShared *s = (groove_IdbShared *)p;
    idb_tree__IdbTree__drop(&s->tree);
    jz_btree_drop(&s->column_families);
}

/* ResetIfCancelled: an armed guard dropped without disarm (cancellation, or
 * an error returned through `?`) forces the next operation to reload. */
typedef struct ResetIfCancelled { bool *needs_reset; } ResetIfCancelled;
static ResetIfCancelled reset_if_cancelled_arm(bool *needs_reset) { ResetIfCancelled g = {needs_reset}; return g; }
static void reset_if_cancelled_disarm(ResetIfCancelled *g) { g->needs_reset = NULL; }
static void reset_if_cancelled_drop(ResetIfCancelled *g) {
    if (g->needs_reset) {
        *g->needs_reset = true;
        g->needs_reset = NULL;
    }
}

static void cf_set_insert(jz_btree *set, jz_slice cf) {
    jz_str view = {(char *)cf.ptr, cf.len, 0};
    if (jz_btree_contains(set, &view)) return;
    jz_str owned = jz_str_from_slice(cf);
    jz_btree_insert(set, &owned, NULL, NULL);
}

jz_err groove__storage__idb__IdbStorage__open(idb_tree_PageStore store, const char *const *column_families, u32 n, groove_IdbStorage *out) {
    jz_err e = groove__storage__validate_physical_storage_names_cstr(column_families, n);
    if (e.code) {
        idb_tree__store__PageStore__drop(&store);
        return e;
    }
    idb_tree_Options options;
    idb_tree__Options__Default__default(&options);
    idb_tree_IdbTree tree;
    e = idb_tree__IdbTree__open(idb_tree__store__PageStore__clone(&store), options, &tree);
    idb_tree__store__PageStore__drop(&store);
    JZ_TRY(e);
    groove_IdbShared *shared = (groove_IdbShared *)jz_rc_new(sizeof(groove_IdbShared), idb_shared_drop);
    shared->tree = tree;
    jz_btree_init(&shared->column_families, &CF_SET_DESC);
    for (u32 i = 0; i < n; i++) cf_set_insert(&shared->column_families, jz_slice_of(column_families[i], (u32)strlen(column_families[i])));
    shared->mutation_gate_locked = false;
    shared->needs_reset = false;
    shared->tree_epoch = 0;
    out->shared = shared;
    return JZ_OK;
}

groove_IdbStorage groove__storage__idb__IdbStorage__Clone__clone(const groove_IdbStorage *s) {
    groove_IdbStorage c = {(groove_IdbShared *)jz_rc_clone(s->shared)};
    return c;
}
void groove__storage__idb__IdbStorage__drop(groove_IdbStorage *s) {
    jz_rc_release(s->shared);
    s->shared = NULL;
}

static jz_err ensure_cf(const groove_IdbStorage *s, jz_slice cf) {
    jz_str view = {(char *)cf.ptr, cf.len, 0};
    if (jz_btree_contains(&s->shared->column_families, &view)) return JZ_OK;
    return groove__storage__Error__column_family_not_found(cf);
}

static jz_err encoded_key(const groove_IdbStorage *s, jz_slice cf, jz_slice key, jz_bytes *out) {
    JZ_TRY(ensure_cf(s, cf));
    return groove__storage__key_codec__encode_column_family_key(cf, key, out);
}

/* consumes rows */
static jz_err decode_rows(idb_tree_KeyValueVec rows, groove_KeyValueVec *out) {
    groove_KeyValueVec decoded = {0};
    jz_vec_reserve(&decoded, rows.len, sizeof(groove_KeyValue));
    for (u32 i = 0; i < rows.len; i++) {
        jz_slice cf, user_key;
        jz_err e = groove__storage__key_codec__decode_column_family_key(jz_bytes_slice(&rows.ptr[i].key), &cf, &user_key);
        if (e.code) {
            jz_vec_drop(&rows, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
            groove__storage__KeyValueVec__drop(&decoded);
            return e;
        }
        groove_KeyValue kv = {jz_bytes_from(user_key.ptr, user_key.len), rows.ptr[i].value};
        rows.ptr[i].value = (jz_bytes){0};
        JZ_VEC_PUSH(decoded, kv);
    }
    jz_vec_drop(&rows, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
    *out = decoded;
    return JZ_OK;
}

static jz_err prevalidate_write_many(const groove_IdbStorage *s, const groove_OwnedWriteOperation *ops, u32 n) {
    for (u32 i = 0; i < n; i++) JZ_TRY(ensure_cf(s, jz_str_slice(&ops[i].cf)));
    return JZ_OK;
}

static idb_tree_IdbTree tree_clone(const groove_IdbStorage *s) {
    idb_tree_IdbTree t;
    idb_tree__IdbTree__clone(&t, &s->shared->tree);
    return t;
}

static jz_err reopen_after_generation_conflict(const groove_IdbStorage *s) {
    /* Discard this stale cache rather than replaying its dirty pages, then
     * recompute the whole logical batch from the newly durable tree. */
    idb_tree_IdbTree tree = tree_clone(s);
    jz_err e = idb_tree__IdbTree__reload(&tree);
    idb_tree__IdbTree__drop(&tree);
    JZ_TRY(e);
    s->shared->tree_epoch += 1; /* wrapping_add */
    s->shared->needs_reset = false;
    return JZ_OK;
}

static jz_err ensure_ready(const groove_IdbStorage *s) {
    if (s->shared->needs_reset) JZ_TRY(reopen_after_generation_conflict(s));
    return JZ_OK;
}

static jz_err discard_failed_tree(const groove_IdbStorage *s, jz_err error) {
    s->shared->needs_reset = true;
    jz_err reset_error = reopen_after_generation_conflict(s);
    if (!reset_error.code) return error;
    jz_err_drop(&error);
    return reset_error;
}

static bool is_generation_conflict(const jz_err *error) { return error->code == IDB_ERR_GENERATION_CONFLICT; }

static void yield_once(void) { jz_task_run_ready(); }

static void back_off_after_generation_conflict(usize retry) {
    /* Executor-cooperative rather than wall-clock: yields let the winning tab
     * finish. Exponential schedule capped with the retry cap. */
    usize shift = retry < 4 ? retry : 4;
    usize yields = (usize)1 << shift;
    if (yields > GROOVE_IDB_MAX_CONFLICT_BACKOFF_YIELDS) yields = GROOVE_IDB_MAX_CONFLICT_BACKOFF_YIELDS;
    for (usize i = 0; i < yields; i++) yield_once();
}

/* ---- read_resident ---- */
typedef jz_err (*ReadFn)(const idb_tree_IdbTree *tree, void *ctx);

/* One synchronous attempt against the committed generation. Any miss or
 * error falls back to the serialized path. */
static bool poll_read_committed(ReadFn read, void *ctx, const idb_tree_IdbTree *tree) {
    idb_tree_IdbTree committed;
    idb_tree__IdbTree__read_committed(tree, &committed);
    jz_err e = read(&committed, ctx);
    idb_tree__IdbTree__drop(&committed);
    if (e.code) {
        jz_err_drop(&e);
        return false;
    }
    return true;
}

static jz_err read_resident(const groove_IdbStorage *s, ReadFn read, void *ctx) {
    for (;;) {
        /* While a writer holds the gate, answer read-committed from the last
         * durable generation. */
        if (s->shared->mutation_gate_locked && !s->shared->needs_reset) {
            idb_tree_IdbTree tree = tree_clone(s);
            bool ok = poll_read_committed(read, ctx, &tree);
            idb_tree__IdbTree__drop(&tree);
            if (ok) return JZ_OK;
        }
        bool was_locked = s->shared->mutation_gate_locked;
        s->shared->mutation_gate_locked = true;
        jz_err e = ensure_ready(s);
        if (e.code) {
            s->shared->mutation_gate_locked = was_locked;
            return e;
        }
        u64 epoch = s->shared->tree_epoch;
        (void)epoch;
        idb_tree_IdbTree tree = tree_clone(s);
        e = read(&tree, ctx);
        idb_tree__IdbTree__drop(&tree);
        s->shared->mutation_gate_locked = was_locked;
        return e;
    }
}

/* ---- writes ---- */
static jz_err write_many_once(const groove_IdbStorage *s, const idb_tree_IdbTree *tree, const groove_OwnedWriteOperation *ops, u32 n) {
    idb_tree_WriteOperationVec writes = {0};
    jz_vec_reserve(&writes, n, sizeof(idb_tree_WriteOperation));
    for (u32 i = 0; i < n; i++) {
        const groove_OwnedWriteOperation *op = &ops[i];
        idb_tree_WriteOperation w = {op->tag == GROOVE_WRITE_SET ? IDB_WRITE_SET : IDB_WRITE_DELETE, {0}, {0}};
        jz_err e = encoded_key(s, jz_str_slice(&op->cf), jz_bytes_slice(&op->key), &w.key);
        if (e.code) {
            jz_vec_drop(&writes, sizeof(idb_tree_WriteOperation), idb_tree__WriteOperation__drop);
            return e;
        }
        if (op->tag == GROOVE_WRITE_SET) w.value = jz_bytes_clone(&op->value);
        JZ_VEC_PUSH(writes, w);
    }
    ResetIfCancelled cancelled = reset_if_cancelled_arm(&s->shared->needs_reset);
    jz_err e = idb_tree__IdbTree__write_many(tree, writes);
    if (!e.code) e = idb_tree__IdbTree__flush(tree);
    if (e.code) {
        reset_if_cancelled_drop(&cancelled);
        return e;
    }
    reset_if_cancelled_disarm(&cancelled);
    return JZ_OK;
}

static jz_err flush_tree(const groove_IdbStorage *s) {
    ResetIfCancelled cancelled = reset_if_cancelled_arm(&s->shared->needs_reset);
    idb_tree_IdbTree tree = tree_clone(s);
    jz_err result = idb_tree__IdbTree__flush(&tree);
    idb_tree__IdbTree__drop(&tree);
    reset_if_cancelled_disarm(&cancelled);
    if (!result.code) return JZ_OK;
    return discard_failed_tree(s, result);
}

/* settled(current) -> Some(outcome): returns true and records the outcome in ctx */
typedef bool (*SettledFn)(void *ctx, bool current_some, jz_bytes *current);

/* Apply `operation` unless `settled` maps the current value at its key to an
 * outcome. A generation conflict reloads and repeats the check. *wrote is
 * true when the write landed (the caller's `written` outcome). */
static jz_err write_unless_locked(const groove_IdbStorage *s, const groove_OwnedWriteOperation *operation, SettledFn settled, void *ctx, bool *wrote) {
    JZ_TRY(ensure_ready(s));
    jz_bytes key;
    JZ_TRY(encoded_key(s, jz_str_slice(&operation->cf), jz_bytes_slice(&operation->key), &key));
    for (usize retry = 0; retry <= GROOVE_IDB_MAX_GENERATION_CONFLICT_RETRIES; retry++) {
        idb_tree_IdbTree tree = tree_clone(s);
        bool some = false;
        jz_bytes current = {0};
        jz_err e = idb_tree__IdbTree__get(&tree, jz_bytes_slice(&key), &some, &current);
        if (e.code) {
            idb_tree__IdbTree__drop(&tree);
            jz_bytes_drop(&key);
            return e;
        }
        if (settled(ctx, some, &current)) {
            jz_bytes_drop(&current);
            idb_tree__IdbTree__drop(&tree);
            jz_bytes_drop(&key);
            *wrote = false;
            return JZ_OK;
        }
        jz_bytes_drop(&current);
        e = write_many_once(s, &tree, operation, 1);
        idb_tree__IdbTree__drop(&tree);
        if (!e.code) {
            jz_bytes_drop(&key);
            *wrote = true;
            return JZ_OK;
        }
        if (is_generation_conflict(&e)) {
            jz_err_drop(&e);
            jz_err r = reopen_after_generation_conflict(s);
            if (r.code) {
                jz_bytes_drop(&key);
                return r;
            }
            if (retry == GROOVE_IDB_MAX_GENERATION_CONFLICT_RETRIES) {
                jz_bytes_drop(&key);
                return groove__storage__Error__idb_generation_contention(GROOVE_IDB_MAX_GENERATION_CONFLICT_RETRIES);
            }
            back_off_after_generation_conflict(retry);
            continue;
        }
        jz_bytes_drop(&key);
        return discard_failed_tree(s, e);
    }
    jz_panic("bounded retry loop returns");
    return JZ_OK;
}

static jz_err write_unless(const groove_IdbStorage *s, groove_OwnedWriteOperation operation, SettledFn settled, void *ctx, bool *wrote) {
    jz_err e = ensure_cf(s, jz_str_slice(&operation.cf));
    if (e.code) {
        groove__storage__OwnedWriteOperation__drop(&operation);
        return e;
    }
    bool was_locked = s->shared->mutation_gate_locked;
    s->shared->mutation_gate_locked = true;
    e = write_unless_locked(s, &operation, settled, ctx, wrote);
    s->shared->mutation_gate_locked = was_locked;
    groove__storage__OwnedWriteOperation__drop(&operation);
    return e;
}

static jz_err write_many_replaying_generation_conflicts(const groove_IdbStorage *s, const groove_OwnedWriteOperation *ops, u32 n) {
    usize retries = 0;
    for (;;) {
        idb_tree_IdbTree tree = tree_clone(s);
        jz_err e = write_many_once(s, &tree, ops, n);
        idb_tree__IdbTree__drop(&tree);
        if (!e.code) return JZ_OK;
        if (is_generation_conflict(&e)) {
            jz_err_drop(&e);
            if (retries == GROOVE_IDB_MAX_GENERATION_CONFLICT_RETRIES) {
                /* Reopen even on the terminal path so a caller cannot observe
                 * a failed, non-durable write through a later get. */
                JZ_TRY(reopen_after_generation_conflict(s));
                return groove__storage__Error__idb_generation_contention(retries);
            }
            retries += 1;
            JZ_TRY(reopen_after_generation_conflict(s));
            back_off_after_generation_conflict(retries);
            continue;
        }
        return discard_failed_tree(s, e);
    }
}

/* prevalidate; lock gate; ensure_ready; replay (consumes operations) */
static jz_err gated_write_many(const groove_IdbStorage *s, groove_OwnedWriteOperationVec operations) {
    jz_err e = prevalidate_write_many(s, operations.ptr, operations.len);
    if (!e.code) {
        bool was_locked = s->shared->mutation_gate_locked;
        s->shared->mutation_gate_locked = true;
        e = ensure_ready(s);
        if (!e.code) e = write_many_replaying_generation_conflicts(s, operations.ptr, operations.len);
        s->shared->mutation_gate_locked = was_locked;
    }
    groove__storage__OwnedWriteOperationVec__drop(&operations);
    return e;
}

static jz_err gated_flush(const groove_IdbStorage *s) {
    bool was_locked = s->shared->mutation_gate_locked;
    s->shared->mutation_gate_locked = true;
    jz_err e = ensure_ready(s);
    if (!e.code) e = flush_tree(s);
    s->shared->mutation_gate_locked = was_locked;
    return e;
}

/* ======================================================================
 * impl OrderedKvStorage for IdbStorage<S>
 * ====================================================================== */
#define IS(self) ((groove_IdbStorage *)(self))

typedef struct ValueEqualsCtx { jz_slice key, expected; u8 result; } ValueEqualsCtx;
static jz_err read_value_equals(const idb_tree_IdbTree *tree, void *ctx) {
    ValueEqualsCtx *c = (ValueEqualsCtx *)ctx;
    return idb_tree__IdbTree__value_equals(tree, c->key, c->expected, &c->result);
}
static jz_err idb_compare_value(void *self, jz_slice cf, jz_slice key, jz_slice expected, groove_ValueComparison *out) {
    jz_bytes k;
    JZ_TRY(encoded_key(IS(self), cf, key, &k));
    ValueEqualsCtx c = {jz_bytes_slice(&k), expected, 0};
    jz_err e = read_resident(IS(self), read_value_equals, &c);
    jz_bytes_drop(&k);
    JZ_TRY(e);
    *out = c.result == 0 ? GROOVE_VALUE_ABSENT : c.result == 2 ? GROOVE_VALUE_IDENTICAL : GROOVE_VALUE_DIFFERENT;
    return JZ_OK;
}

typedef struct GetCtx { jz_slice key; bool some; jz_bytes value; } GetCtx;
static jz_err read_get(const idb_tree_IdbTree *tree, void *ctx) {
    GetCtx *c = (GetCtx *)ctx;
    jz_bytes_drop(&c->value);
    c->some = false;
    return idb_tree__IdbTree__get(tree, c->key, &c->some, &c->value);
}
static jz_err idb_get(void *self, jz_slice cf, jz_slice key, bool *some, jz_bytes *out) {
    jz_bytes k;
    JZ_TRY(encoded_key(IS(self), cf, key, &k));
    GetCtx c = {jz_bytes_slice(&k), false, {0}};
    jz_err e = read_resident(IS(self), read_get, &c);
    jz_bytes_drop(&k);
    if (e.code) {
        jz_bytes_drop(&c.value);
        return e;
    }
    *some = c.some;
    if (c.some) *out = c.value;
    return JZ_OK;
}

/* put_if_absent: |existing| existing.map(Some) */
typedef struct PutIfAbsentCtx { bool some; jz_bytes existing; } PutIfAbsentCtx;
static bool settled_put_if_absent(void *ctx, bool current_some, jz_bytes *current) {
    PutIfAbsentCtx *c = (PutIfAbsentCtx *)ctx;
    if (!current_some) return false;
    c->some = true;
    c->existing = *current;
    *current = (jz_bytes){0};
    return true;
}
static jz_err idb_put_if_absent(void *self, jz_slice cf, jz_slice key, jz_bytes value, bool *some, jz_bytes *existing) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_SET, jz_str_from_slice(cf), jz_bytes_from(key.ptr, key.len), value};
    PutIfAbsentCtx c = {false, {0}};
    bool wrote = false;
    JZ_TRY(write_unless(IS(self), op, settled_put_if_absent, &c, &wrote));
    /* written outcome: None */
    *some = !wrote && c.some;
    if (*some) *existing = c.existing;
    return JZ_OK;
}

/* compare_and_delete: (current.as_deref() != Some(expected)).then_some(false) */
typedef struct CompareDeleteCtx { jz_slice expected; } CompareDeleteCtx;
static bool settled_compare_and_delete(void *ctx, bool current_some, jz_bytes *current) {
    CompareDeleteCtx *c = (CompareDeleteCtx *)ctx;
    return !current_some || jz_slice_cmp(jz_bytes_slice(current), c->expected) != 0;
}
static jz_err idb_compare_and_delete(void *self, jz_slice cf, jz_slice key, jz_slice expected, bool *removed) {
    groove_OwnedWriteOperation op = {GROOVE_WRITE_DELETE, jz_str_from_slice(cf), jz_bytes_from(key.ptr, key.len), {0}};
    CompareDeleteCtx c = {expected};
    bool wrote = false;
    JZ_TRY(write_unless(IS(self), op, settled_compare_and_delete, &c, &wrote));
    *removed = wrote; /* settled -> false, written -> true */
    return JZ_OK;
}

static jz_err idb_set(void *self, jz_slice cf, jz_slice key, jz_bytes value) {
    groove_OwnedWriteOperationVec operations = {0};
    groove_OwnedWriteOperation op = {GROOVE_WRITE_SET, jz_str_from_slice(cf), jz_bytes_from(key.ptr, key.len), value};
    JZ_VEC_PUSH(operations, op);
    return gated_write_many(IS(self), operations);
}
static jz_err idb_delete(void *self, jz_slice cf, jz_slice key) {
    groove_OwnedWriteOperationVec operations = {0};
    groove_OwnedWriteOperation op = {GROOVE_WRITE_DELETE, jz_str_from_slice(cf), jz_bytes_from(key.ptr, key.len), {0}};
    JZ_VEC_PUSH(operations, op);
    return gated_write_many(IS(self), operations);
}
static jz_err idb_close(void *self) { return gated_flush(IS(self)); }
static jz_err idb_flush_write_boundary(void *self) { return gated_flush(IS(self)); }

typedef struct RangeCtx { jz_slice start, end; usize limit; groove_ScanDirection direction; idb_tree_KeyValueVec rows; } RangeCtx;
static jz_err read_range(const idb_tree_IdbTree *tree, void *ctx) {
    RangeCtx *c = (RangeCtx *)ctx;
    jz_vec_drop(&c->rows, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
    c->rows = (idb_tree_KeyValueVec){0};
    if (c->direction == GROOVE_SCAN_FORWARD) return idb_tree__IdbTree__range_limit(tree, c->start, c->end, c->limit, &c->rows);
    return idb_tree__IdbTree__range_reverse(tree, c->start, c->end, c->limit, &c->rows);
}

/* prefix_successor(&start).unwrap_or_else(|| vec![0xff]) */
static jz_bytes prefix_end(const jz_bytes *start) {
    jz_bytes end;
    if (groove__storage__prefix_successor(jz_bytes_slice(start), &end)) return end;
    u8 ff = 0xff;
    return jz_bytes_from(&ff, 1);
}

static jz_err idb_scan(void *self, groove_ScanRequest request, groove_StorageScan *out) {
    jz_err e = JZ_OK;
    jz_slice cf = jz_str_slice(&request.cf);
    if ((request.has_max_items && request.max_items == 0) || groove__storage__ScanBounds__is_empty_range(&request.bounds)) {
        jz_bytes unused;
        e = encoded_key(IS(self), cf, (jz_slice){0}, &unused);
        groove__storage__ScanRequest__drop(&request);
        JZ_TRY(e);
        jz_bytes_drop(&unused);
        *out = groove__storage__ReadyStorageCursor__new((groove_KeyValueVec){0});
        return JZ_OK;
    }
    jz_bytes start = {0}, end = {0};
    if (request.bounds.tag == GROOVE_SCAN_RANGE) {
        e = encoded_key(IS(self), cf, jz_bytes_slice(&request.bounds.start), &start);
        if (!e.code) {
            e = encoded_key(IS(self), cf, jz_bytes_slice(&request.bounds.end), &end);
            if (e.code) jz_bytes_drop(&start);
        }
    } else {
        e = encoded_key(IS(self), cf, jz_bytes_slice(&request.bounds.start), &start);
        if (!e.code) end = prefix_end(&start);
    }
    usize limit = request.has_max_items ? request.max_items : SIZE_MAX;
    groove_ScanDirection direction = request.direction;
    groove__storage__ScanRequest__drop(&request);
    JZ_TRY(e);
    RangeCtx c = {jz_bytes_slice(&start), jz_bytes_slice(&end), limit, direction, {0}};
    e = read_resident(IS(self), read_range, &c);
    jz_bytes_drop(&start);
    jz_bytes_drop(&end);
    if (e.code) {
        jz_vec_drop(&c.rows, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
        return e;
    }
    groove_KeyValueVec decoded;
    JZ_TRY(decode_rows(c.rows, &decoded));
    *out = groove__storage__ReadyStorageCursor__new(decoded);
    return JZ_OK;
}

static jz_err idb_last_with_prefix(void *self, jz_slice cf, jz_slice prefix, bool *some, groove_KeyValue *out) {
    jz_bytes start;
    JZ_TRY(encoded_key(IS(self), cf, prefix, &start));
    jz_bytes end = prefix_end(&start);
    RangeCtx c = {jz_bytes_slice(&start), jz_bytes_slice(&end), 1, GROOVE_SCAN_REVERSE, {0}};
    jz_err e = read_resident(IS(self), read_range, &c);
    jz_bytes_drop(&start);
    jz_bytes_drop(&end);
    if (e.code) {
        jz_vec_drop(&c.rows, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
        return e;
    }
    /* .into_iter().next() */
    if (c.rows.len > 1) jz_vec_truncate(&c.rows, 1, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
    groove_KeyValueVec decoded;
    JZ_TRY(decode_rows(c.rows, &decoded));
    *some = decoded.len > 0;
    if (*some) {
        *out = decoded.ptr[decoded.len - 1]; /* .pop() */
        decoded.len -= 1;
    }
    groove__storage__KeyValueVec__drop(&decoded);
    return JZ_OK;
}

static jz_err idb_last_with_prefix_before_or_at(void *self, jz_slice cf, jz_slice prefix, jz_slice upper, bool *some, groove_KeyValue *out) {
    jz_bytes start, end;
    JZ_TRY(encoded_key(IS(self), cf, prefix, &start));
    jz_err e = encoded_key(IS(self), cf, upper, &end);
    if (e.code) {
        jz_bytes_drop(&start);
        return e;
    }
    jz_bytes_push_u8(&end, 0);
    RangeCtx c = {jz_bytes_slice(&start), jz_bytes_slice(&end), 1, GROOVE_SCAN_REVERSE, {0}};
    e = read_resident(IS(self), read_range, &c);
    jz_bytes_drop(&start);
    jz_bytes_drop(&end);
    if (e.code) {
        jz_vec_drop(&c.rows, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
        return e;
    }
    if (c.rows.len == 0) {
        jz_vec_drop(&c.rows, sizeof(idb_tree_KeyValue), NULL);
        *some = false;
        return JZ_OK;
    }
    if (c.rows.len > 1) jz_vec_truncate(&c.rows, 1, sizeof(idb_tree_KeyValue), idb_tree__KeyValue__drop);
    groove_KeyValueVec decoded;
    JZ_TRY(decode_rows(c.rows, &decoded));
    *some = false;
    if (decoded.len) {
        groove_KeyValue *kv = &decoded.ptr[decoded.len - 1];
        jz_slice k = jz_bytes_slice(&kv->key);
        bool keep = k.len >= prefix.len && (prefix.len == 0 || memcmp(k.ptr, prefix.ptr, prefix.len) == 0) && jz_slice_cmp(k, upper) <= 0;
        if (keep) {
            *some = true;
            *out = *kv;
            decoded.len -= 1;
        }
    }
    groove__storage__KeyValueVec__drop(&decoded);
    return JZ_OK;
}

static jz_err idb_write_many(void *self, groove_OwnedWriteOperationVec operations) { return gated_write_many(IS(self), operations); }

static groove_WriteManyOutcome idb_write_many_outcome(void *self, groove_OwnedWriteOperationVec operations) {
    groove_WriteManyOutcome o = {GROOVE_WMO_COMMITTED, JZ_OK};
    jz_err e = prevalidate_write_many(IS(self), operations.ptr, operations.len);
    if (e.code) {
        groove__storage__OwnedWriteOperationVec__drop(&operations);
        o.tag = GROOVE_WMO_UNCOMMITTED;
        o.error = e;
        return o;
    }
    e = idb_write_many(self, operations);
    if (e.code) {
        o.tag = GROOVE_WMO_POSSIBLY_COMMITTED;
        o.error = e;
    }
    return o;
}

static bool idb_column_family_names(void *self, groove_StrVec *out) {
    groove_StrVec names = {0};
    jz_str *name;
    void *unused;
    JZ_BTREE_FOR(&IS(self)->shared->column_families, name, unused) JZ_VEC_PUSH(names, jz_str_clone(name));
    *out = names;
    return true;
}

jz_err groove__storage__idb__IdbStorage__ReopenableStorage__reopen(groove_IdbStorage self, groove_StrVec column_families, groove_IdbStorage *out) {
    jz_err e = groove__storage__validate_physical_storage_names(column_families.ptr, column_families.len);
    if (e.code) {
        groove__storage__StrVec__drop(&column_families);
        groove__storage__idb__IdbStorage__drop(&self);
        return e;
    }
    for (u32 i = 0; i < column_families.len; i++) cf_set_insert(&self.shared->column_families, jz_str_slice(&column_families.ptr[i]));
    groove__storage__StrVec__drop(&column_families);
    *out = self;
    return JZ_OK;
}

static jz_err idb_reopen_boxed(void *self, groove_StrVec column_families, groove_DynStorage *out) {
    groove_IdbStorage *boxed = IS(self);
    groove_IdbStorage reopened;
    jz_err e = groove__storage__idb__IdbStorage__ReopenableStorage__reopen(*boxed, column_families, &reopened);
    jz_free(boxed);
    JZ_TRY(e);
    *out = groove__storage__idb__IdbStorage__into_dyn(reopened);
    return JZ_OK;
}
static void idb_drop_boxed(void *self) {
    groove__storage__idb__IdbStorage__drop(IS(self));
    jz_free(self);
}

const groove_OrderedKvStorageVtbl groove__storage__idb__IdbStorage__OrderedKvStorage__VTBL = {
    .permits_eager_read_retry = NULL,
    .get = idb_get,
    .compare_value = idb_compare_value,
    .put_if_absent = idb_put_if_absent,
    .compare_and_delete = idb_compare_and_delete,
    .set = idb_set,
    .delete_ = idb_delete,
    .close = idb_close,
    .flush_write_boundary = idb_flush_write_boundary,
    .scan = idb_scan,
    .last_with_prefix = idb_last_with_prefix,
    .last_with_prefix_before_or_at = idb_last_with_prefix_before_or_at,
    .write_many = idb_write_many,
    .write_many_outcome = idb_write_many_outcome,
    .column_family_names = idb_column_family_names,
    .reopen = idb_reopen_boxed,
    .drop = idb_drop_boxed,
};

groove_DynStorage groove__storage__idb__IdbStorage__into_dyn(groove_IdbStorage s) {
    groove_IdbStorage *boxed = JZ_NEW(groove_IdbStorage);
    *boxed = s;
    groove_DynStorage d = {boxed, &groove__storage__idb__IdbStorage__OrderedKvStorage__VTBL};
    return d;
}
