/* Port of crates/jazz/layers/node/src/node/open_tx.rs
 *
 * Open transaction lifecycle and snapshot-overlay reads. This module owns
 * `tx_read`, `tx_query`, mergeable/exclusive write staging, and commit-unit
 * construction for open transactions; authority-side validation and fate
 * assignment live in `ingest`, while query execution helpers live in
 * `query_eval`. It is the node API layer used by the `Db` facade before
 * writes become protocol commit units.
 *
 * `impl<S: OrderedKvStorage> NodeState<S>` becomes one C copy; async methods
 * are synchronous. Generic `V: Into<Value>` cell maps arrive already as
 * `BTreeMap<String, Value>`. `impl FnMut() -> u64` is a (fn, ctx) pair.
 * #[cfg(test)] helpers (assert_*_consistent) and the dead-outside-tests
 * wrappers tx_write_mergeable / tx_patch_mergeable are not ported. */
#include <stdio.h>
#include <string.h>
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef jazz_node_OpenTransaction OpenTransaction;
typedef jazz_node_PendingWrite PendingWrite;
typedef jazz_node_StagedWrites StagedWrites;
typedef jazz_node_SnapshotRow SnapshotRow;
typedef jazz_node_CurrentRow CurrentRow;
typedef jazz_node_VersionRow VersionRow;
typedef jazz_model_OpenTransactionId OpenTransactionId;
typedef jazz_model_TxId TxId;

/* ---------------- errors ---------------- */

static jz_err open_batch_err(u32 code, OpenTransactionId id) {
    char buf[37];
    jz_uuid_to_string(&id, buf);
    return jz_err_fmt(code, "%s", buf);
}
static jz_err missing_open_batch(OpenTransactionId id) { return open_batch_err(JZN_E_MISSING_OPEN_BATCH, id); }
static jz_err invalid_mergeable(const char *msg) { return JZN_ERR(JZN_E_INVALID_MERGEABLE_COMMIT, msg); }

/* ---------------- small helpers ---------------- */

static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone(s); }
static void cells_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, str_clone_v, groove__records__values__Value__clone); }
/* cells.extend(other.clone()) */
static void cells_extend_cloned(jz_btree *cells, const jz_btree *other) {
    const jz_str *k; const groove_Value *v;
    JZ_BTREE_FOR(other, k, v) {
        jz_str key = jz_str_clone(k);
        groove_Value value, old;
        groove__records__values__Value__clone(&value, v);
        if (jz_btree_insert(cells, &key, &value, &old)) { jz_str_drop(&key); groove__records__values__Value__drop(&old); }
    }
}
static bool cells_eq(const jz_btree *a, const jz_btree *b) { return jz_btree_eq(a, b, groove__records__values__Value__eq_v); }

static jz_err table_not_found(const jz_str *table) { return jz_err_fmt(JZN_E_TABLE_NOT_FOUND, "%.*s", (int)table->len, table->ptr); }

/* String -> StagedLargeValue */
static const jz_btree_desc desc_staged_large_cells = { sizeof(jz_str), sizeof(groove_StagedLargeValue), jz_cmp_str, jzn_str_drop_v, groove__large_values__StagedLargeValue__drop };
/* set<StagedLargeValueId> */
static int cmp_staged_id(const void *a, const void *b, void *c) {
    (void)c;
    return jz_uuid_cmp(&((const groove_StagedLargeValueId *)a)->id, &((const groove_StagedLargeValueId *)b)->id);
}
static const jz_btree_desc desc_staged_id_set = { sizeof(groove_StagedLargeValueId), 0, cmp_staged_id, NULL, NULL };

/* (String, RowUuid) keys */
typedef struct { jz_str table; jazz_types_RowUuid row; } table_row_key;
static int cmp_table_row(const void *a, const void *b, void *c) {
    (void)c;
    const table_row_key *x = a, *y = b;
    int r = jz_slice_cmp(jz_str_slice(&x->table), jz_str_slice(&y->table));
    return r ? r : jz_uuid_cmp(&x->row, &y->row);
}
static void table_row_key_drop(void *p) { jz_str_drop(&((table_row_key *)p)->table); }
static void table_row_key_clone(void *d, const void *s) {
    const table_row_key *src = s;
    table_row_key *dst = d;
    dst->table = jz_str_clone(&src->table);
    dst->row = src->row;
}

/* ---------------- SnapshotRow ---------------- */

static void opt_values_drop(jazz_node_OptValueVec *cells) {
    for (u32 i = 0; i < cells->len; i++)
        if (cells->ptr[i]) { groove__records__values__Value__drop(cells->ptr[i]); jz_free(cells->ptr[i]); }
    jz_free(cells->ptr);
    cells->ptr = NULL; cells->len = cells->cap = 0;
}

void jazz_node__node__open_tx__SnapshotRow__drop(SnapshotRow *r) {
    if (r->has_content_cells) opt_values_drop(&r->content_cells);
    if (r->provenance_created) jazz_node__node__codec__VersionRow__drop(r->provenance_created);
    if (r->provenance_updated) jazz_node__node__codec__VersionRow__drop(r->provenance_updated);
    memset(r, 0, sizeof *r);
}
static void snapshot_row_drop_v(void *p) { jazz_node__node__open_tx__SnapshotRow__drop(p); }

/* #[derive(Clone)] */
static void SnapshotRow__clone(SnapshotRow *dst, const SnapshotRow *src) {
    *dst = *src;
    memset(&dst->content_cells, 0, sizeof dst->content_cells);
    if (src->has_content_cells) {
        for (u32 i = 0; i < src->content_cells.len; i++) {
            groove_Value *v = NULL;
            if (src->content_cells.ptr[i]) { v = JZ_NEW(groove_Value); groove__records__values__Value__clone(v, src->content_cells.ptr[i]); }
            JZ_VEC_PUSH(dst->content_cells, v);
        }
    }
    dst->provenance_created = src->provenance_created ? jazz_node__node__codec__VersionRow__clone(src->provenance_created) : NULL;
    dst->provenance_updated = src->provenance_updated ? jazz_node__node__codec__VersionRow__clone(src->provenance_updated) : NULL;
}
static void snapshot_row_clone_v(void *d, const void *s) { SnapshotRow__clone(d, s); }

/* SchemaVersionId -> SnapshotRow */
static const jz_btree_desc desc_schema_snapshot_row = { sizeof(jz_uuid), sizeof(SnapshotRow), jz_cmp_uuid, NULL, snapshot_row_drop_v };
static void schema_rows_drop_v(void *p) { jz_btree_drop(p); }
static void schema_rows_clone_v(void *d, const void *s) { jz_btree_clone(d, s, NULL, snapshot_row_clone_v); }
/* (String, RowUuid) -> BTreeMap<SchemaVersionId, SnapshotRow> */
static const jz_btree_desc desc_base_snapshot_rows = { sizeof(table_row_key), sizeof(jz_btree), cmp_table_row, table_row_key_drop, schema_rows_drop_v };
/* RowUuid -> SnapshotRow */
static const jz_btree_desc desc_row_snapshot_row = { sizeof(jz_uuid), sizeof(SnapshotRow), jz_cmp_uuid, NULL, snapshot_row_drop_v };

/* ---------------- PendingWrite ---------------- */

static void PendingWrite__drop(PendingWrite *w) {
    jz_str_drop(&w->table);
    if (w->branch) jazz_model__model__branch__BranchSelector__drop(w->branch);
    jz_btree_drop(&w->cells.cells);
    if (w->has_verified_inherited_cells) jz_btree_drop(&w->verified_inherited_cells);
    if (w->branch_view_copy) { jazz_model__tx__BranchViewCopyEvidence__drop(w->branch_view_copy); jz_free(w->branch_view_copy); }
    jz_btree_drop(&w->staged_large_cells);
}
static void pending_write_free(PendingWrite *w) { if (w) { PendingWrite__drop(w); jz_free(w); } }

static jazz_model_BranchViewCopyEvidence *evidence_clone(const jazz_model_BranchViewCopyEvidence *e) {
    if (!e) return NULL;
    jazz_model_BranchViewCopyEvidence *out = JZ_NEW(jazz_model_BranchViewCopyEvidence);
    jazz_model__tx__BranchViewCopyEvidence__clone(out, e);
    return out;
}

/* #[derive(Clone)] */
static PendingWrite *PendingWrite__clone(const PendingWrite *src) {
    PendingWrite *dst = JZ_NEW(PendingWrite);
    *dst = *src;
    dst->table = jz_str_clone(&src->table);
    dst->branch = jazz_model__model__branch__BranchSelector__clone(src->branch);
    cells_clone(&dst->cells.cells, &src->cells.cells);
    if (src->has_verified_inherited_cells) cells_clone(&dst->verified_inherited_cells, &src->verified_inherited_cells);
    dst->branch_view_copy = evidence_clone(src->branch_view_copy);
    jz_btree_clone(&dst->staged_large_cells, &src->staged_large_cells, str_clone_v, groove__large_values__StagedLargeValue__clone);
    return dst;
}

/* The cells this write currently stages, whether replacement or patch. */
static const jz_btree *PendingWrite__staged_cells(const PendingWrite *self) { return &self->cells.cells; }

static void collect_descriptors(const groove_Value *value, JZ_VEC(groove_LargeValueRef *) *out) {
    jazz_node__node__collect_indirect_descriptors(value, out);
}

/* Whether `value` (optionally nullable) is exactly the indirect descriptor
 * `target`, and nothing else. */
static bool value_is_exact_descriptor(const groove_Value *value, const groove_LargeValueRef *target) {
    JZ_VEC(groove_LargeValueRef *) descriptors = { 0 };
    collect_descriptors(value, (void *)&descriptors);
    bool exact = descriptors.len == 1 && groove__large_values__LargeValueRef__eq(descriptors.ptr[0], target);
    jz_free(descriptors.ptr);
    return exact;
}

/* Keep only claims whose column still holds exactly the claimed descriptor,
 * moving every other claim to `superseded`. */
static void PendingWrite__retain_current_large_value_claims(PendingWrite *self, jazz_node_StagedLargeValueIdVec *superseded) {
    jz_btree claims = self->staged_large_cells;
    jz_btree_init(&self->staged_large_cells, &desc_staged_large_cells);
    jz_str column; groove_StagedLargeValue staged;
    while (jz_btree_pop_first(&claims, &column, &staged)) {
        const groove_Value *value = jz_btree_get(PendingWrite__staged_cells(self), &column);
        if (value && value_is_exact_descriptor(value, staged.value_ref)) {
            jz_btree_insert(&self->staged_large_cells, &column, &staged, NULL);
        } else {
            JZ_VEC_PUSH(*superseded, staged.id);
            jz_str_drop(&column);
            groove__large_values__StagedLargeValue__drop(&staged);
        }
    }
    jz_btree_drop(&claims);
}

/* Combine a pending write's claims with a later write's claims for the same
 * row. A later claim for a column supersedes the earlier one. (consumes both) */
static jz_btree merge_large_value_claims(jz_btree earlier, jz_btree later, jazz_node_StagedLargeValueIdVec *superseded) {
    jz_str column; groove_StagedLargeValue staged;
    while (jz_btree_pop_first(&later, &column, &staged)) {
        groove_StagedLargeValue previous;
        if (jz_btree_insert(&earlier, &column, &staged, &previous)) {
            jz_str_drop(&column);
            bool kept = false;
            const jz_str *k; const groove_StagedLargeValue *v;
            JZ_BTREE_FOR(&earlier, k, v) { (void)k; if (jz_uuid_eq(&v->id.id, &previous.id.id)) kept = true; }
            if (!kept) JZ_VEC_PUSH(*superseded, previous.id);
            groove__large_values__StagedLargeValue__drop(&previous);
        }
    }
    jz_btree_drop(&later);
    return earlier;
}

/* ---------------- StagedWrites ---------------- */

JZ_NVEC(usize_vec, usize);
static void usize_vec_drop_v(void *p) { jz_free(((usize_vec *)p)->ptr); }
static void usize_vec_clone_v(void *d, const void *s) {
    const usize_vec *src = s;
    usize_vec *dst = d;
    dst->ptr = src->len ? jz_memdup(src->ptr, src->len * sizeof(usize)) : NULL;
    dst->len = dst->cap = src->len;
}
/* RowUuid -> Vec<usize> */
static const jz_btree_desc desc_row_positions = { sizeof(jz_uuid), sizeof(usize_vec), jz_cmp_uuid, NULL, usize_vec_drop_v };
static void rows_btree_drop_v(void *p) { jz_btree_drop(p); }
static void rows_btree_clone_v(void *d, const void *s) { jz_btree_clone(d, s, NULL, usize_vec_clone_v); }
/* String -> BTreeMap<RowUuid, Vec<usize>> */
static const jz_btree_desc desc_positions = { sizeof(jz_str), sizeof(jz_btree), jz_cmp_str, jzn_str_drop_v, rows_btree_drop_v };

static void StagedWrites__default(StagedWrites *w) {
    memset(w, 0, sizeof *w);
    jz_btree_init(&w->positions, &desc_positions);
}
static void StagedWrites__drop(StagedWrites *w) {
    for (u32 i = 0; i < w->slots.len; i++) pending_write_free(w->slots.ptr[i]);
    jz_free(w->slots.ptr);
    jz_btree_drop(&w->positions);
}
static void StagedWrites__clone(StagedWrites *dst, const StagedWrites *src) {
    memset(&dst->slots, 0, sizeof dst->slots);
    for (u32 i = 0; i < src->slots.len; i++) {
        PendingWrite *w = src->slots.ptr[i] ? PendingWrite__clone(src->slots.ptr[i]) : NULL;
        JZ_VEC_PUSH(dst->slots, w);
    }
    jz_btree_clone(&dst->positions, &src->positions, str_clone_v, rows_btree_clone_v);
    dst->live = src->live;
}

/* Number of staged writes. */
static usize StagedWrites__len(const StagedWrites *self) { return self->live; }

static const usize_vec EMPTY_POSITIONS = { 0 };
static const usize_vec *StagedWrites__row_positions(const StagedWrites *self, const jz_str *table, jazz_types_RowUuid row_uuid) {
    const jz_btree *rows = jz_btree_get(&self->positions, table);
    if (!rows) return &EMPTY_POSITIONS;
    const usize_vec *positions = jz_btree_get(rows, &row_uuid);
    return positions ? positions : &EMPTY_POSITIONS;
}

static PendingWrite *StagedWrites__live_slot(const StagedWrites *self, usize position) {
    PendingWrite *w = self->slots.ptr[position];
    if (!w) jz_panic("staged write positions point at live slots");
    return w;
}

/* Every row of `table` with a staged write, ascending (inserted into `out`). */
static void StagedWrites__table_rows(const StagedWrites *self, const jz_str *table, jz_btree *out /* set<RowUuid> */) {
    const jz_btree *rows = jz_btree_get(&self->positions, table);
    if (!rows) return;
    const jz_uuid *row; const usize_vec *u;
    JZ_BTREE_FOR(rows, row, u) { (void)u; jz_btree_insert(out, row, NULL, NULL); }
}

typedef bool (*write_pred)(const PendingWrite *write, void *ctx);

/* Position of the first staged write of one row that `matches` selects. */
static bool StagedWrites__find_row_write(const StagedWrites *self, const jz_str *table, jazz_types_RowUuid row_uuid, write_pred matches, void *ctx, usize *out) {
    const usize_vec *positions = StagedWrites__row_positions(self, table, row_uuid);
    for (u32 i = 0; i < positions->len; i++)
        if (matches(StagedWrites__live_slot(self, positions->ptr[i]), ctx)) { *out = positions->ptr[i]; return true; }
    return false;
}

/* The staged write at a position `find_row_write` returned. Callers may change
 * anything but its table and row. */
static PendingWrite *StagedWrites__get_mut(StagedWrites *self, usize position) {
    PendingWrite *w = self->slots.ptr[position];
    if (!w) jz_panic("staged write positions point at live slots");
    return w;
}

/* Stage a write after every write already staged. (takes ownership) */
static void StagedWrites__push(StagedWrites *self, PendingWrite *write) {
    jz_str table = jz_str_clone(&write->table);
    bool inserted = false;
    jz_btree *rows = jz_btree_entry(&self->positions, &table, &inserted);
    if (inserted) jz_btree_init(rows, &desc_row_positions);
    else jz_str_drop(&table);
    usize_vec *positions = jz_btree_entry(rows, &write->row_uuid, &inserted);
    usize position = self->slots.len;
    JZ_VEC_PUSH(*positions, position);
    JZ_VEC_PUSH(self->slots, write);
    self->live += 1;
}

/* Drop empty slots and renumber the index. */
static void StagedWrites__compact(StagedWrites *self) {
    u32 kept = 0;
    for (u32 i = 0; i < self->slots.len; i++) if (self->slots.ptr[i]) self->slots.ptr[kept++] = self->slots.ptr[i];
    self->slots.len = kept;
    void *t; jz_btree *rows;
    JZ_BTREE_FOR(&self->positions, t, rows) {
        (void)t;
        void *r; usize_vec *positions;
        JZ_BTREE_FOR(rows, r, positions) { (void)r; positions->len = 0; }
    }
    for (u32 position = 0; position < self->slots.len; position++) {
        const PendingWrite *write = self->slots.ptr[position];
        jz_btree *by_row = jz_btree_get(&self->positions, &write->table);
        usize_vec *positions = by_row ? jz_btree_get(by_row, &write->row_uuid) : NULL;
        if (!positions) jz_panic("every staged write is indexed");
        usize p = position;
        JZ_VEC_PUSH(*positions, p);
    }
}

JZ_NVEC(pending_write_vec, PendingWrite *);
static void pending_write_vec_drop(pending_write_vec *v) {
    for (u32 i = 0; i < v->len; i++) pending_write_free(v->ptr[i]);
    jz_free(v->ptr);
}

/* Unstage the writes of one row that `remove` selects and return them in
 * staging order. Costs O(log n + k) for a row with k staged writes, plus an
 * amortized O(1) share of compaction. */
static void StagedWrites__remove_row_writes(StagedWrites *self, const jz_str *table, jazz_types_RowUuid row_uuid, write_pred remove, void *ctx,
                                            pending_write_vec *removed) {
    removed->ptr = NULL; removed->len = removed->cap = 0;
    jz_btree *rows = jz_btree_get(&self->positions, table);
    if (!rows) return;
    usize_vec *positions = jz_btree_get(rows, &row_uuid);
    if (!positions) return;
    u32 kept = 0;
    for (u32 i = 0; i < positions->len; i++) {
        usize position = positions->ptr[i];
        PendingWrite *write = self->slots.ptr[position];
        if (!write) jz_panic("staged write positions point at live slots");
        if (remove(write, ctx)) {
            JZ_VEC_PUSH(*removed, write);
            self->slots.ptr[position] = NULL;
        } else {
            positions->ptr[kept++] = position;
        }
    }
    positions->len = kept;
    if (positions->len == 0) {
        jz_btree_remove(rows, &row_uuid, NULL, NULL);
        if (rows->len == 0) jz_btree_remove(&self->positions, table, NULL, NULL);
    }
    self->live -= removed->len;
    /* Compact once empty slots outnumber live ones, so the slot vector stays
     * within twice the live writes and compaction stays amortized. */
    usize floor = self->live > 32 ? self->live : 32;
    if (self->slots.len - self->live > floor) StagedWrites__compact(self);
}

/* ---------------- OpenTransaction ---------------- */

static void narrowed_reads_vec_drop(jazz_node_NarrowedSourceReadVec *v) {
    for (u32 i = 0; i < v->len; i++) jazz_node_NarrowedSourceRead_drop(&v->ptr[i]);
    jz_free(v->ptr);
    v->ptr = NULL; v->len = v->cap = 0;
}

static void SourceNarrowing__default(jazz_node_SourceNarrowing *n) {
    memset(n, 0, sizeof *n);
    jz_btree_init(&n->offered, &jazz_engine_SourceId_narrowed_read_desc);
    jz_btree_init(&n->payload, &jazz_engine_SourceId_set_desc);
}
static void SourceNarrowing__drop(jazz_node_SourceNarrowing *n) {
    jz_btree_drop(&n->offered);
    jz_btree_drop(&n->payload);
    narrowed_reads_vec_drop(&n->claimed);
}
static void SourceNarrowing__clone(jazz_node_SourceNarrowing *dst, const jazz_node_SourceNarrowing *src) {
    dst->recording = src->recording;
    jz_btree_clone(&dst->offered, &src->offered, jazz_engine__query_engine__input__SourceId__clone, jazz_node_NarrowedSourceRead_clone);
    jz_btree_clone(&dst->payload, &src->payload, jazz_engine__query_engine__input__SourceId__clone, NULL);
    memset(&dst->claimed, 0, sizeof dst->claimed);
    for (u32 i = 0; i < src->claimed.len; i++) {
        jazz_node_NarrowedSourceRead r;
        jazz_node_NarrowedSourceRead_clone(&r, &src->claimed.ptr[i]);
        JZ_VEC_PUSH(dst->claimed, r);
    }
}

/* set<(ShapeId, BindingId)> */
typedef struct { jz_uuid shape_id, binding_id; } shape_binding_key;
static int cmp_shape_binding(const void *a, const void *b, void *c) {
    (void)c;
    const shape_binding_key *x = a, *y = b;
    int r = jz_uuid_cmp(&x->shape_id, &y->shape_id);
    return r ? r : jz_uuid_cmp(&x->binding_id, &y->binding_id);
}
static const jz_btree_desc desc_shape_binding_set = { sizeof(shape_binding_key), 0, cmp_shape_binding, NULL, NULL };

#define VEC_CLONE(dst, src, T, clonefn) do { \
        memset(&(dst), 0, sizeof(dst)); \
        for (u32 _i = 0; _i < (src).len; _i++) { T _x; clonefn(&_x, &(src).ptr[_i]); JZ_VEC_PUSH(dst, _x); } \
    } while (0)

void jazz_node__node__open_tx__OpenTransaction__drop(void *p) {
    OpenTransaction *t = p;
    jazz_model__tx__Snapshot__drop(&t->base_snapshot);
    jz_btree_drop(&t->base_snapshot_rows);
    jz_vec_drop(&t->row_reads, sizeof(jazz_model_RowRead), jazz_model__tx__RowRead__drop);
    jz_vec_drop(&t->absent_reads, sizeof(struct jazz_model_AbsentRead), jazz_model__tx__AbsentRead__drop);
    jz_vec_drop(&t->predicate_reads, sizeof(struct jazz_model_PredicateRead), jazz_model__tx__PredicateRead__drop);
    SourceNarrowing__drop(&t->source_narrowing);
    jz_btree_drop(&t->narrowed_predicate_reads);
    StagedWrites__drop(&t->writes);
    jz_free(t->superseded_large_values.ptr);
    if (t->has_user_metadata_json) jz_str_drop(&t->user_metadata_json);
}
static void open_tx_free(OpenTransaction *t) { if (t) { jazz_node__node__open_tx__OpenTransaction__drop(t); jz_free(t); } }

/* #[derive(Clone)] */
static OpenTransaction *OpenTransaction__clone(const OpenTransaction *src) {
    OpenTransaction *dst = JZ_NEW(OpenTransaction);
    dst->kind = src->kind;
    dst->provisional_author = src->provisional_author;
    jazz_model__tx__Snapshot__clone(&dst->base_snapshot, &src->base_snapshot);
    jz_btree_clone(&dst->base_snapshot_rows, &src->base_snapshot_rows, table_row_key_clone, schema_rows_clone_v);
    VEC_CLONE(dst->row_reads, src->row_reads, jazz_model_RowRead, jazz_model__tx__RowRead__clone);
    VEC_CLONE(dst->absent_reads, src->absent_reads, struct jazz_model_AbsentRead, jazz_model__tx__AbsentRead__clone);
    VEC_CLONE(dst->predicate_reads, src->predicate_reads, struct jazz_model_PredicateRead, jazz_model__tx__PredicateRead__clone);
    SourceNarrowing__clone(&dst->source_narrowing, &src->source_narrowing);
    jz_btree_clone(&dst->narrowed_predicate_reads, &src->narrowed_predicate_reads, NULL, NULL);
    StagedWrites__clone(&dst->writes, &src->writes);
    dst->superseded_large_values.ptr = src->superseded_large_values.len
        ? jz_memdup(src->superseded_large_values.ptr, src->superseded_large_values.len * sizeof(groove_StagedLargeValueId)) : NULL;
    dst->superseded_large_values.len = dst->superseded_large_values.cap = src->superseded_large_values.len;
    dst->has_user_metadata_json = src->has_user_metadata_json;
    if (src->has_user_metadata_json) dst->user_metadata_json = jz_str_clone(&src->user_metadata_json);
    return dst;
}

/* RowUuid/TxId pair set */
typedef struct { jazz_types_RowUuid row; TxId version; } row_version;
static int cmp_row_version(const void *a, const void *b, void *c) {
    (void)c;
    const row_version *x = a, *y = b;
    int r = jz_uuid_cmp(&x->row, &y->row);
    return r ? r : jazz_model_TxId_cmp(&x->version, &y->version);
}
static const jz_btree_desc desc_row_version_set = { sizeof(row_version), 0, cmp_row_version, NULL, NULL };
JZ_NVEC(row_version_vec, row_version);

/* Record row proofs for `table`, skipping ones already recorded. */
static void OpenTransaction__record_row_reads(OpenTransaction *self, const jz_str *table, const row_version_vec *proofs) {
    jz_btree recorded;
    jz_btree_init(&recorded, &desc_row_version_set);
    for (u32 i = 0; i < self->row_reads.len; i++) {
        const jazz_model_RowRead *read = &self->row_reads.ptr[i];
        if (!jz_str_eq(&read->table, table)) continue;
        row_version rv = { read->row, read->proof };
        jz_btree_insert(&recorded, &rv, NULL, NULL);
    }
    for (u32 i = 0; i < proofs->len; i++) {
        if (!jz_btree_insert(&recorded, &proofs->ptr[i], NULL, NULL)) {
            jazz_model_RowRead read = { jz_str_clone(table), proofs->ptr[i].row, proofs->ptr[i].version };
            JZ_VEC_PUSH(self->row_reads, read);
        }
    }
    jz_btree_drop(&recorded);
}

/* Every Groove root this transaction staged and has not yet released. */
static void OpenTransaction__staged_large_value_ids(const OpenTransaction *self, jazz_node_StagedLargeValueIdVec *out) {
    for (u32 i = 0; i < self->superseded_large_values.len; i++) JZ_VEC_PUSH(*out, self->superseded_large_values.ptr[i]);
    for (u32 i = 0; i < self->writes.slots.len; i++) {
        const PendingWrite *write = self->writes.slots.ptr[i];
        if (!write) continue;
        const jz_str *k; const groove_StagedLargeValue *staged;
        JZ_BTREE_FOR(&write->staged_large_cells, k, staged) { (void)k; JZ_VEC_PUSH(*out, staged->id); }
    }
}

/* OpenTransactionId -> OpenTransaction* */
static OpenTransaction *open_tx_lookup(const NS *self, OpenTransactionId tx_id) {
    OpenTransaction *const *t = jz_btree_get(&self->open_tx.open_transactions, &tx_id);
    return t ? *t : NULL;
}

jz_err jazz_node__node__open_tx__NodeState__open_tx(const NS *self, OpenTransactionId tx_id, const OpenTransaction **out) {
    const OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) return missing_open_batch(tx_id);
    *out = t;
    return JZ_OK;
}

jz_err jazz_node__node__open_tx__NodeState__open_tx_mut(NS *self, OpenTransactionId tx_id, OpenTransaction **out) {
    OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) return missing_open_batch(tx_id);
    *out = t;
    return JZ_OK;
}
#define OPEN_TX(self, id, var) const OpenTransaction *var; JZ_TRY(jazz_node__node__open_tx__NodeState__open_tx((self), (id), &var))
#define OPEN_TX_MUT(self, id, var) OpenTransaction *var; JZ_TRY(jazz_node__node__open_tx__NodeState__open_tx_mut((self), (id), &var))

/* ---------------- opening ---------------- */

static jz_err open_transaction(NS *self, OpenTransactionId id, jazz_node_OpenTransactionKind kind, jazz_types_AuthorSubject provisional_author);

/* Open an exclusive transaction over the current snapshot. */
jz_err jazz_node__node__open_tx__NodeState__open_exclusive(NS *self, OpenTransactionId id) {
    jazz_node_OpenTransactionKind kind = { JZN_OTX_KIND_EXCLUSIVE, jazz_types__ids__AuthorSubject__SYSTEM, true, jazz_types__ids__AuthorSubject__SYSTEM };
    return open_transaction(self, id, kind, jazz_types__ids__AuthorSubject__SYSTEM);
}

jz_err jazz_node__node__open_tx__NodeState__open_exclusive_with_identity(NS *self, OpenTransactionId id, jazz_types_AuthorSubject made_by,
                                                                        jazz_types_AuthorSubject permission_subject) {
    jazz_node_OpenTransactionKind kind = { JZN_OTX_KIND_EXCLUSIVE, made_by, true, permission_subject };
    return open_transaction(self, id, kind, made_by);
}

jz_err jazz_node__node__open_tx__NodeState__open_exclusive_for_identity(NS *self, OpenTransactionId id, jazz_types_AuthorSubject made_by) {
    return jazz_node__node__open_tx__NodeState__open_exclusive_with_identity(self, id, made_by, made_by);
}

/* Open a mergeable transaction over the current snapshot. */
jz_err jazz_node__node__open_tx__NodeState__open_mergeable(NS *self, OpenTransactionId id, jazz_types_AuthorSubject made_by,
                                                          const jazz_types_AuthorSubject *permission_subject) {
    jazz_node_OpenTransactionKind kind = { JZN_OTX_KIND_MERGEABLE, made_by, permission_subject != NULL, { 0 } };
    if (permission_subject) kind.permission_subject = *permission_subject;
    return open_transaction(self, id, kind, made_by);
}

/* Whether an open transaction separates durable provenance from its
 * permission subject. Such transactions are deliberately root-only until
 * branch attribution has a complete representation. */
jz_err jazz_node__node__open_tx__NodeState__transaction_is_attributed(const NS *self, OpenTransactionId id, bool *out) {
    OPEN_TX(self, id, t);
    if (t->kind.tag == JZN_OTX_KIND_MERGEABLE && !t->kind.has_permission_subject) *out = false;
    else *out = !jazz_types_AuthorSubject_eq(&t->kind.permission_subject, &t->kind.made_by);
    return JZ_OK;
}

jz_err jazz_node__node__open_tx__NodeState__transaction_is_exclusive(const NS *self, OpenTransactionId id, bool *out) {
    OPEN_TX(self, id, t);
    *out = t->kind.tag == JZN_OTX_KIND_EXCLUSIVE;
    return JZ_OK;
}

/* Return the subject whose policies govern an open transaction's writes. */
jz_err jazz_node__node__open_tx__NodeState__transaction_permission_subject(const NS *self, OpenTransactionId id, jazz_types_AuthorSubject *out) {
    OPEN_TX(self, id, t);
    *out = t->kind.has_permission_subject ? t->kind.permission_subject : t->kind.made_by;
    return JZ_OK;
}

/* Return the permission subject bound to an open exclusive transaction. */
jz_err jazz_node__node__open_tx__NodeState__exclusive_transaction_permission_subject(const NS *self, OpenTransactionId id, jazz_types_AuthorSubject *out) {
    OPEN_TX(self, id, t);
    if (t->kind.tag != JZN_OTX_KIND_EXCLUSIVE) return invalid_mergeable("open transaction is not exclusive");
    *out = t->kind.permission_subject;
    return JZ_OK;
}

static jz_err transaction_ids_for_global_time(NS *self, jazz_types_GlobalTime global_time, jazz_model_TxIdVec *out);
static jz_err pending_foreign_transaction_ids(NS *self, jazz_model_TxIdVec *out);

static jz_err open_transaction(NS *self, OpenTransactionId id, jazz_node_OpenTransactionKind kind, jazz_types_AuthorSubject provisional_author) {
    JZ_TRY(jazz_node__node__NodeState__require_catalogue_ready(self));
    if (jz_btree_contains(&self->open_tx.open_transactions, &id) || jz_btree_contains(&self->open_tx.closed_batches, &id))
        return open_batch_err(JZN_E_DUPLICATE_OPEN_BATCH, id);
    /* Pending rows use the same non-null durable attribution shape as a
     * committed row. Keep `kind`'s bound/session subject untouched for
     * authorization; only its provisional metadata becomes SystemAt. */
    jazz_types_RowAuthor author;
    jz_err e = jazz_types__ids__RowAuthor__from_session(provisional_author, self->node_uuid, &author);
    if (e.code) { jz_err_drop(&e); return jz_err_new(JZN_E_UNADMITTED_WRITE_AUTHOR, NULL); }
    provisional_author = jazz_types__ids__RowAuthor__as_author_subject(author);
    jazz_types_TxTime local_base = jazz_node__node__NodeState__tx_time_high_water(self);
    /* Every time applied past the frontier was recorded with its transaction,
     * so the base names them without a scan (#3390). */
    jazz_model_TxIdVec dots = { 0 };
    JZ_NVEC(gt_vec, jazz_types_GlobalTime);
    gt_vec applied = { 0 };
    const jazz_types_GlobalTime *gt; void *u;
    JZ_BTREE_FOR(&self->clock.applied_global_times_after_frontier, gt, u) { (void)u; JZ_VEC_PUSH(applied, *gt); }
    for (u32 i = 0; i < applied.len && !e.code; i++) {
        const jazz_model_TxIdVec *tx_ids = jz_btree_get(&self->clock.frontier_dots, &applied.ptr[i]);
        if (tx_ids) {
            for (u32 j = 0; j < tx_ids->len; j++) JZ_VEC_PUSH(dots, tx_ids->ptr[j]);
        } else {
            jazz_model_TxIdVec more = { 0 };
            e = transaction_ids_for_global_time(self, applied.ptr[i], &more);
            for (u32 j = 0; j < more.len; j++) JZ_VEC_PUSH(dots, more.ptr[j]);
            jz_free(more.ptr);
        }
    }
    jz_free(applied.ptr);
    if (!e.code) {
        jazz_model_TxIdVec foreign = { 0 };
        e = pending_foreign_transaction_ids(self, &foreign);
        for (u32 j = 0; j < foreign.len; j++) JZ_VEC_PUSH(dots, foreign.ptr[j]);
        jz_free(foreign.ptr);
    }
    if (e.code) { jz_free(dots.ptr); return e; }
    jazz_model_Snapshot base_snapshot;
    e = jazz_model__tx__Snapshot__exclusive_base(self->node_uuid, self->clock.committed_global_time, local_base, dots, &base_snapshot);
    if (e.code) {
        /* .map_err(Error::InvalidStoredValue) */
        jz_err mapped = jz_err_fmt(JZN_E_INVALID_STORED_VALUE, "%.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return mapped;
    }
    OpenTransaction *t = JZ_NEW(OpenTransaction);
    t->kind = kind;
    t->provisional_author = provisional_author;
    t->base_snapshot = base_snapshot;
    jz_btree_init(&t->base_snapshot_rows, &desc_base_snapshot_rows);
    SourceNarrowing__default(&t->source_narrowing);
    jz_btree_init(&t->narrowed_predicate_reads, &desc_shape_binding_set);
    StagedWrites__default(&t->writes);
    OpenTransaction *old = NULL;
    if (jz_btree_insert(&self->open_tx.open_transactions, &id, &t, &old)) open_tx_free(old);
    return JZ_OK;
}

/* ---------------- reads ---------------- */

jz_err jazz_node__node__open_tx__NodeState__snapshot_row_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                  jazz_types_RowUuid row_uuid, const jazz_model_Snapshot *snapshot, SnapshotRow *out);
static jz_err snapshot_row_from_winners(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, VersionRow *content,
                                        VersionRow *deletion, SnapshotRow *out);
static jz_err snapshot_rows_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const jazz_model_Snapshot *snapshot,
                                      jz_btree *out);
static jz_err overlay_pending_writes_in_schema(const NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                               jazz_types_RowUuid row_uuid, SnapshotRow *snapshot_row, bool *some, jz_btree *out);
static jz_err overlay_pending_cells_and_deletion_with_table(const NS *self, OpenTransactionId tx_id, const jazz_model_TableSchema *table_schema,
                                                            const jz_str *table, jazz_types_RowUuid row_uuid, SnapshotRow *snapshot_row,
                                                            bool *has_cells, jz_btree *cells, bool *deleted);

static jz_err tx_read_unchecked(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                jazz_types_RowUuid row_uuid, bool *some, jz_btree *out) {
    jazz_model_Snapshot snapshot;
    {
        OPEN_TX(self, tx_id, t);
        jazz_model__tx__Snapshot__clone(&snapshot, &t->base_snapshot);
    }
    SnapshotRow snapshot_row;
    jz_err e = jazz_node__node__open_tx__NodeState__snapshot_row_in_schema(self, schema_version, table, row_uuid, &snapshot, &snapshot_row);
    jazz_model__tx__Snapshot__drop(&snapshot);
    if (e.code) return e;
    OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { jazz_node__node__open_tx__SnapshotRow__drop(&snapshot_row); return missing_open_batch(tx_id); }
    table_row_key key = { jz_str_clone(table), row_uuid };
    bool inserted = false;
    jz_btree *by_schema = jz_btree_entry(&t->base_snapshot_rows, &key, &inserted);
    if (inserted) jz_btree_init(by_schema, &desc_schema_snapshot_row);
    else table_row_key_drop(&key);
    SnapshotRow cached, old;
    SnapshotRow__clone(&cached, &snapshot_row);
    if (jz_btree_insert(by_schema, &schema_version, &cached, &old)) jazz_node__node__open_tx__SnapshotRow__drop(&old);
    bool has_read = snapshot_row.has_read_version;
    TxId version = snapshot_row.read_version;
    e = overlay_pending_writes_in_schema(self, tx_id, schema_version, table, row_uuid, &snapshot_row, some, out);
    if (e.code) return e;
    if (has_read) {
        bool seen = false;
        for (u32 i = 0; i < t->row_reads.len && !seen; i++) {
            const jazz_model_RowRead *read = &t->row_reads.ptr[i];
            seen = jz_str_eq(&read->table, table) && jz_uuid_eq(&read->row, &row_uuid) && jazz_model_TxId_eq(&read->proof, &version);
        }
        if (!seen) {
            jazz_model_RowRead read = { jz_str_clone(table), row_uuid, version };
            JZ_VEC_PUSH(t->row_reads, read);
        }
    } else {
        bool seen = false;
        for (u32 i = 0; i < t->absent_reads.len && !seen; i++)
            seen = jz_str_eq(&t->absent_reads.ptr[i].table, table) && jz_uuid_eq(&t->absent_reads.ptr[i].row_uuid, &row_uuid);
        if (!seen) {
            struct jazz_model_AbsentRead read = { jz_str_clone(table), row_uuid };
            JZ_VEC_PUSH(t->absent_reads, read);
        }
    }
    return JZ_OK;
}

/* Read a row inside an exclusive transaction. */
jz_err jazz_node__node__open_tx__NodeState__tx_read(NS *self, OpenTransactionId tx_id, const jz_str *table, jazz_types_RowUuid row_uuid, bool *some, jz_btree *out) {
    const jazz_model_TableSchema *t = NULL;
    JZ_TRY(jazz_node__node__NodeState__table(self, table, &t));
    return tx_read_unchecked(self, tx_id, self->catalogue.active_schema.schema, table, row_uuid, some, out);
}

/* Read a row through an explicit registered schema view. */
jz_err jazz_node__node__open_tx__NodeState__tx_read_in_schema(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version,
                                                             const jz_str *table, jazz_types_RowUuid row_uuid, bool *some, jz_btree *out) {
    const jazz_model_TableSchema *t = NULL;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &t));
    return tx_read_unchecked(self, tx_id, schema_version, table, row_uuid, some, out);
}

/* Pin the snapshot version of each root row a transaction query returned.
 *
 * A predicate read alone is validated by rebuilding the query at the base
 * snapshot. A partial node's base cut is only a coordinate in the authority's
 * history (any receipt advances it), so a row this node still holds may
 * already be deleted or replaced at that cut. Recording the version actually
 * observed lets the authority reject that stale read row by row
 * (garden-co/jazz#3694). Rows staged by this transaction have no snapshot
 * version and need no proof. */
jz_err jazz_node__node__open_tx__NodeState__record_tx_query_row_reads(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version,
                                                                     const jz_str *table, const jazz_types_RowUuid *rows, u32 nrows) {
    jazz_model_Snapshot snapshot;
    {
        OPEN_TX(self, tx_id, t);
        jazz_model__tx__Snapshot__clone(&snapshot, &t->base_snapshot);
    }
    row_version_vec proofs = { 0 };
    jz_err e = JZ_OK;
    for (u32 i = 0; i < nrows && !e.code; i++) {
        SnapshotRow row;
        e = jazz_node__node__open_tx__NodeState__snapshot_row_in_schema(self, schema_version, table, rows[i], &snapshot, &row);
        if (e.code) break;
        if (row.has_read_version) { row_version rv = { rows[i], row.read_version }; JZ_VEC_PUSH(proofs, rv); }
        jazz_node__node__open_tx__SnapshotRow__drop(&row);
    }
    jazz_model__tx__Snapshot__drop(&snapshot);
    if (!e.code) {
        OpenTransaction *t = open_tx_lookup(self, tx_id);
        if (!t) e = missing_open_batch(tx_id);
        else OpenTransaction__record_row_reads(t, table, &proofs);
    }
    jz_free(proofs.ptr);
    return e;
}

/* Classify an explicit exclusive insert target that the transaction's
 * overlaid point read reports as absent: a committed or staged deletion still
 * occupies the id. The staged overlay is keyed by `(table, row)`, so the same
 * row UUID in another table never counts. */
jz_err jazz_node__node__open_tx__NodeState__tx_insert_target_state_in_schema(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version,
                                                                            const jz_str *table, jazz_types_RowUuid row_uuid, u32 *out) {
    SnapshotRow snapshot_row;
    bool cached = false;
    {
        OPEN_TX(self, tx_id, t);
        table_row_key key = { *table, row_uuid };
        const jz_btree *by_schema = jz_btree_get(&t->base_snapshot_rows, &key);
        const SnapshotRow *row = by_schema ? jz_btree_get(by_schema, &schema_version) : NULL;
        if (row) { SnapshotRow__clone(&snapshot_row, row); cached = true; }
    }
    if (!cached) {
        jazz_model_Snapshot snapshot;
        {
            OPEN_TX(self, tx_id, t);
            jazz_model__tx__Snapshot__clone(&snapshot, &t->base_snapshot);
        }
        jz_err e = jazz_node__node__open_tx__NodeState__snapshot_row_in_schema(self, schema_version, table, row_uuid, &snapshot, &snapshot_row);
        jazz_model__tx__Snapshot__drop(&snapshot);
        if (e.code) return e;
    }
    const OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { jazz_node__node__open_tx__SnapshotRow__drop(&snapshot_row); return missing_open_batch(tx_id); }
    bool staged = StagedWrites__row_positions(&t->writes, table, row_uuid)->len != 0;
    *out = (snapshot_row.has_content_cells || snapshot_row.deleted || staged) ? JZN_TITS_DELETED : JZN_TITS_ABSENT;
    jazz_node__node__open_tx__SnapshotRow__drop(&snapshot_row);
    return JZ_OK;
}

static jz_err tx_current_rows_with_table(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                         const jazz_model_TableSchema *table_schema, bool include_deleted, bool record_table_read,
                                         jazz_node_CurrentRowVec *out);

/* Read all current rows inside an exclusive transaction. */
jz_err jazz_node__node__open_tx__NodeState__tx_current_rows(NS *self, OpenTransactionId tx_id, const jz_str *table, jazz_node_CurrentRowVec *out) {
    jazz_types_SchemaVersionId schema_version = self->catalogue.active_schema.schema;
    const jazz_model_TableSchema *t = NULL;
    JZ_TRY(jazz_node__node__NodeState__table(self, table, &t));
    jazz_model_TableSchema table_schema;
    jazz_model__schema__TableSchema__clone(&table_schema, t);
    /* A direct table read proves each row it returns; see
     * `record_tx_query_row_reads` (garden-co/jazz#3694). */
    jz_err e = tx_current_rows_with_table(self, tx_id, schema_version, table, &table_schema, false, true, out);
    jazz_model__schema__TableSchema__drop(&table_schema);
    return e;
}

static jz_err table_in_schema_owned(NS *self, const jz_str *table, jazz_types_SchemaVersionId schema_version, jazz_model_TableSchema *out) {
    const jazz_model_TableSchema *t = NULL;
    JZ_TRY(jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &t));
    jazz_model__schema__TableSchema__clone(out, t);
    return JZ_OK;
}

/* Read current rows through an explicit registered schema view. */
jz_err jazz_node__node__open_tx__NodeState__tx_current_rows_in_schema(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version,
                                                                     const jz_str *table, jazz_node_CurrentRowVec *out) {
    jazz_model_TableSchema table_schema;
    JZ_TRY(table_in_schema_owned(self, table, schema_version, &table_schema));
    /* A direct table read proves each row it returns; see
     * `record_tx_query_row_reads` (garden-co/jazz#3694). */
    jz_err e = tx_current_rows_with_table(self, tx_id, schema_version, table, &table_schema, false, true, out);
    jazz_model__schema__TableSchema__drop(&table_schema);
    return e;
}

/* Offer `reads` to the sources of the query an open exclusive transaction is
 * about to evaluate. Returns the narrowing in effect before, which
 * `withdraw_tx_narrowed_source_reads` restores. (consumes `reads`) */
jz_err jazz_node__node__open_tx__NodeState__offer_tx_narrowed_source_reads(NS *self, OpenTransactionId tx_id, jazz_node_ExclusiveSourceReads reads,
                                                                          jazz_node_SourceNarrowing *previous) {
    OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { jz_btree_drop(&reads.reads); jz_btree_drop(&reads.payload); return missing_open_batch(tx_id); }
    jazz_node_SourceNarrowing offered = { t->source_narrowing.recording, reads.reads, reads.payload, { 0 } };
    *previous = t->source_narrowing; /* std::mem::replace */
    t->source_narrowing = offered;
    return JZ_OK;
}

/* Restore the narrowing `previous` and return the reads the query's sources
 * claimed since it was offered. */
jz_err jazz_node__node__open_tx__NodeState__withdraw_tx_narrowed_source_reads(NS *self, OpenTransactionId tx_id, jazz_node_SourceNarrowing previous,
                                                                             jazz_node_NarrowedSourceReadVec *claimed) {
    OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { SourceNarrowing__drop(&previous); return missing_open_batch(tx_id); }
    jazz_node_SourceNarrowing narrowing = t->source_narrowing;
    t->source_narrowing = previous;
    *claimed = narrowing.claimed;
    narrowing.claimed.ptr = NULL; narrowing.claimed.len = narrowing.claimed.cap = 0;
    SourceNarrowing__drop(&narrowing);
    return JZ_OK;
}

/* Claim the read of `source`, a source beyond its query's root: the narrowed
 * read offered for it, or nothing for sync payload or while a narrowed read is
 * being recorded. An exclusive transaction reads such a source no other way:
 * its query already failed with UnsupportedExclusiveRead if a source had no
 * narrowed read (garden-co/jazz#3694). */
jz_err jazz_node__node__open_tx__NodeState__claim_tx_source_read(NS *self, OpenTransactionId tx_id, const jazz_engine_SourceId *source) {
    bool exclusive = false;
    JZ_TRY(jazz_node__node__open_tx__NodeState__transaction_is_exclusive(self, tx_id, &exclusive));
    OPEN_TX_MUT(self, tx_id, t);
    jazz_node_SourceNarrowing *narrowing = &t->source_narrowing;
    if (narrowing->recording || jz_btree_contains(&narrowing->payload, source)) return JZ_OK;
    const jazz_node_NarrowedSourceRead *read = jz_btree_get(&narrowing->offered, source);
    if (read) {
        jazz_node_NarrowedSourceRead claimed;
        jazz_node_NarrowedSourceRead_clone(&claimed, read);
        JZ_VEC_PUSH(narrowing->claimed, claimed);
        return JZ_OK;
    }
    if (exclusive)
        return jz_err_fmt(JZN_E_UNSUPPORTED_EXCLUSIVE_READ, "`%.*s` beyond the root of its query", (int)source->table.len, source->table.ptr);
    return JZ_OK;
}

/* Read transaction rows through a registered schema view, optionally
 * retaining root rows whose deletion register wins. Records no read: query
 * source resolution records its sources' reads itself. */
jz_err jazz_node__node__open_tx__NodeState__tx_current_rows_in_schema_with_options(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version,
                                                                                  const jz_str *table, bool include_deleted, jazz_node_CurrentRowVec *out) {
    jazz_model_TableSchema table_schema;
    JZ_TRY(table_in_schema_owned(self, table, schema_version, &table_schema));
    jz_err e = tx_current_rows_with_table(self, tx_id, schema_version, table, &table_schema, include_deleted, false, out);
    jazz_model__schema__TableSchema__drop(&table_schema);
    return e;
}

static void uuid_set_init(jz_btree *t) { jz_btree_init(t, &jzn_desc_uuid_set); }

static jz_err tx_current_rows_with_table(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                         const jazz_model_TableSchema *table_schema, bool include_deleted, bool record_table_read,
                                         jazz_node_CurrentRowVec *out) {
    out->ptr = NULL; out->len = out->cap = 0;
    jazz_model_Snapshot snapshot;
    {
        OPEN_TX(self, tx_id, t);
        jazz_model__tx__Snapshot__clone(&snapshot, &t->base_snapshot);
    }
    row_version_vec proofs = { 0 };
    jz_btree snapshot_rows;
    jz_err e = snapshot_rows_in_schema(self, schema_version, table, &snapshot, &snapshot_rows);
    jazz_model__tx__Snapshot__drop(&snapshot);
    if (e.code) return e;
    jz_btree rows;
    uuid_set_init(&rows);
    {
        const jz_uuid *row; const SnapshotRow *u;
        JZ_BTREE_FOR(&snapshot_rows, row, u) { (void)u; jz_btree_insert(&rows, row, NULL, NULL); }
    }
    const OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { e = missing_open_batch(tx_id); goto done; }
    StagedWrites__table_rows(&t->writes, table, &rows);
    {
        const jz_uuid *row_ptr; void *u;
        JZ_BTREE_FOR(&rows, row_ptr, u) {
            (void)u;
            jazz_types_RowUuid row_uuid = *row_ptr;
            SnapshotRow snapshot_row;
            if (!jz_btree_remove(&snapshot_rows, &row_uuid, NULL, &snapshot_row)) {
                /* A row staged by this transaction with no history at all. */
                e = snapshot_row_from_winners(self, schema_version, table, NULL, NULL, &snapshot_row);
                if (e.code) break;
            }
            VersionRow *prov_created = snapshot_row.provenance_created ? jazz_node__node__codec__VersionRow__clone(snapshot_row.provenance_created) : NULL;
            VersionRow *prov_updated = snapshot_row.provenance_updated ? jazz_node__node__codec__VersionRow__clone(snapshot_row.provenance_updated) : NULL;
            bool has_read = snapshot_row.has_read_version;
            TxId read_version = snapshot_row.read_version;
            t = open_tx_lookup(self, tx_id);
            jazz_types_AuthorSubject provisional_author = t->provisional_author;
            pending_write_vec pending_writes = { 0 };
            const usize_vec *positions = StagedWrites__row_positions(&t->writes, table, row_uuid);
            for (u32 i = 0; i < positions->len; i++) JZ_VEC_PUSH(pending_writes, PendingWrite__clone(StagedWrites__live_slot(&t->writes, positions->ptr[i])));
            bool has_cells = false, deleted = false;
            jz_btree cells;
            e = overlay_pending_cells_and_deletion_with_table(self, tx_id, table_schema, table, row_uuid, &snapshot_row, &has_cells, &cells, &deleted);
            if (!e.code && has_cells && (!deleted || include_deleted)) {
                bool has_projection = prov_created != NULL;
                jazz_node_RowProvenance provenance = { 0 };
                jazz_types_TxTime projected_time = 0; jazz_types_NodeAlias projected_alias = 0;
                if (has_projection) {
                    provenance.created_by = jazz_node__node__codec__VersionRow__created_by(prov_created);
                    provenance.created_at = jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__created_at(prov_created));
                    provenance.updated_by = jazz_node__node__codec__VersionRow__updated_by(prov_updated);
                    provenance.updated_at = jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__updated_at(prov_updated));
                    projected_time = jazz_node__node__codec__VersionRow__tx_time(prov_updated);
                    projected_alias = jazz_node__node__codec__VersionRow__tx_node_alias(prov_updated);
                }
                bool has_provenance = has_projection;
                for (u32 i = 0; i < pending_writes.len; i++) {
                    const PendingWrite *write = pending_writes.ptr[i];
                    if (!write->has_now_ms) continue;
                    u64 updated_at = write->now_ms;
                    if (has_provenance) {
                        provenance.updated_by = provisional_author;
                        provenance.updated_at = updated_at;
                    } else {
                        jazz_node_RowProvenance p = { provisional_author, updated_at, provisional_author, updated_at };
                        provenance = p;
                        has_provenance = true;
                    }
                }
                CurrentRow row;
                if (has_provenance) {
                    e = jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(table_schema, row_uuid, &cells, provenance, has_projection,
                                                                                            projected_time, projected_alias, &row);
                } else {
                    groove_ValueVec positional = { 0 };
                    e = jazz_node__node__codec__positional_cells_from_map(table_schema, &cells, &positional);
                    if (!e.code) e = jazz_node__node__codec__current_row_from_positional_cells(table_schema, row_uuid, &positional, &row);
                    jz_vec_drop(&positional, sizeof(groove_Value), groove__records__values__Value__drop);
                }
                if (!e.code) {
                    if (deleted) row = jazz_node__node__CurrentRow__into_deleted(row);
                    JZ_VEC_PUSH(*out, row);
                    if (record_table_read && has_read) { row_version rv = { row_uuid, read_version }; JZ_VEC_PUSH(proofs, rv); }
                }
            }
            if (has_cells) jz_btree_drop(&cells);
            pending_write_vec_drop(&pending_writes);
            if (prov_created) jazz_node__node__codec__VersionRow__drop(prov_created);
            if (prov_updated) jazz_node__node__codec__VersionRow__drop(prov_updated);
            if (e.code) break;
        }
    }
    if (e.code) goto done;
    {
        OpenTransaction *tm = open_tx_lookup(self, tx_id);
        OpenTransaction__record_row_reads(tm, table, &proofs);
        jazz_node__node__codec__sort_current_rows(out->ptr, out->len);
        if (!record_table_read) goto done;
        jazz_protocol_SchemaVersion *const *schema = jz_btree_get(&self->catalogue.catalogue_schemas, &schema_version);
        if (!schema) { e = JZN_INVALID_STORED("transaction schema is unknown"); goto done; }
        jazz_model_OptStr no_alias = { false, { 0 } };
        jazz_model_Query query = jazz_model__query__Query__from(jz_str_clone(table), no_alias);
        jazz_model_ValidatedQuery *shape = NULL;
        e = jazz_model__query__Query__validate(&query, (*schema)->schema, &shape);
        jazz_model__query__Query__drop_v(&query);
        if (e.code) goto done;
        jz_btree params;
        jz_btree_init(&params, &jzn_desc_str_value);
        jazz_model_Binding *binding = NULL;
        e = jazz_model__query__ValidatedQuery__bind(shape, params, &binding);
        if (e.code) { jazz_model__query__ValidatedQuery__drop(shape); goto done; }
        struct jazz_model_PredicateRead read;
        read.table = jz_str_clone(table);
        read.shape_id = jazz_model__query__ValidatedQuery__shape_id(shape);
        read.shape = jazz_model__query__Query__clone(jazz_model__query__ValidatedQuery__query(shape));
        read.binding_id = jazz_model__query__Binding__binding_id(binding);
        cells_clone(&read.binding_values, jazz_model__query__Binding__values(binding));
        JZ_VEC_PUSH(tm->predicate_reads, read);
        jazz_model__query__Binding__drop(binding);
        jazz_model__query__ValidatedQuery__drop(shape);
    }
done:
    jz_free(proofs.ptr);
    jz_btree_drop(&rows);
    jz_btree_drop(&snapshot_rows);
    if (e.code) { jz_vec_drop(out, sizeof(CurrentRow), jazz_node__node__CurrentRow__drop); out->ptr = NULL; out->len = out->cap = 0; }
    return e;
}

/* ---------------- staging ---------------- */

static bool same_deletion_presence(const PendingWrite *write, void *ctx) { return write->has_deletion == ((const PendingWrite *)ctx)->has_deletion; }

/* Stage an exclusive row replacement whose `staged_large_cells` columns hold
 * descriptors of Groove roots this transaction staged itself for a partial
 * large-value update (#2087). The transaction takes ownership of those roots:
 * commit publishes them, and a later replacement of the column supersedes
 * them. (consumes `cells` and `staged_large_cells`) */
jz_err jazz_node__node__open_tx__NodeState__tx_write_in_schema_with_staged_large_values(NS *self, OpenTransactionId tx_id,
                                                                                       jazz_types_SchemaVersionId write_schema_version, const jz_str *table,
                                                                                       jazz_types_RowUuid row_uuid, jz_btree cells,
                                                                                       const jazz_model_DeletionEvent *deletion, const u64 *now_ms,
                                                                                       jz_btree staged_large_cells) {
    jz_err e = JZ_OK;
    const OpenTransaction *t0 = open_tx_lookup(self, tx_id);
    if (!t0) e = missing_open_batch(tx_id);
    else if (t0->kind.tag != JZN_OTX_KIND_EXCLUSIVE) e = invalid_mergeable("open transaction is not exclusive");
    const jazz_model_TableSchema *table_schema = NULL;
    if (!e.code) e = jazz_node__node__NodeState__table_in_schema_ref(self, table, write_schema_version, &table_schema);
    if (!e.code) e = jazz_node__node__validate_mergeable_write_shape(cells.len == 0, deletion != NULL);
    if (!e.code) {
        groove_ValueVec positional = { 0 };
        e = jazz_node__node__codec__positional_cells_from_map(table_schema, &cells, &positional);
        jz_vec_drop(&positional, sizeof(groove_Value), groove__records__values__Value__drop);
    }
    if (e.code) { jz_btree_drop(&cells); jz_btree_drop(&staged_large_cells); return e; }
    PendingWrite *pending = JZ_NEW(PendingWrite);
    pending->table = jz_str_clone(table);
    pending->row_uuid = row_uuid;
    pending->schema_version = write_schema_version;
    pending->branch = jazz_model__model__branch__BranchSelector__default();
    pending->cells.tag = JZN_PENDING_CELLS_REPLACE;
    pending->cells.cells = cells;
    pending->has_deletion = deletion != NULL;
    if (deletion) pending->deletion = *deletion;
    pending->has_now_ms = now_ms != NULL;
    if (now_ms) pending->now_ms = *now_ms;
    pending->known_fresh_row = false;
    pending->has_verified_inherited_cells = false;
    pending->branch_view_copy = NULL;
    pending->staged_large_cells = staged_large_cells;
    OpenTransaction *open_tx = open_tx_lookup(self, tx_id);
    table_row_key key = { *table, row_uuid };
    jz_btree_remove(&open_tx->base_snapshot_rows, &key, NULL, NULL);
    usize position;
    if (StagedWrites__find_row_write(&open_tx->writes, table, row_uuid, same_deletion_presence, pending, &position)) {
        PendingWrite *existing = StagedWrites__get_mut(&open_tx->writes, position);
        /* A replacement usually carries the earlier staged descriptor forward
         * (it was read back through the transaction overlay), so the earlier
         * claim stays valid exactly while its cell is unchanged. */
        jz_btree earlier = existing->staged_large_cells;
        jz_btree_init(&existing->staged_large_cells, &desc_staged_large_cells);
        pending->staged_large_cells = merge_large_value_claims(earlier, pending->staged_large_cells, &open_tx->superseded_large_values);
        PendingWrite__drop(existing);
        *existing = *pending;
        jz_free(pending);
        PendingWrite__retain_current_large_value_claims(existing, &open_tx->superseded_large_values);
    } else {
        PendingWrite__retain_current_large_value_claims(pending, &open_tx->superseded_large_values);
        StagedWrites__push(&open_tx->writes, pending);
    }
    return JZ_OK;
}

static jz_btree empty_staged_large_cells(void) {
    jz_btree t;
    jz_btree_init(&t, &desc_staged_large_cells);
    return t;
}

jz_err jazz_node__node__open_tx__NodeState__tx_write_in_schema_at_ms(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId write_schema_version,
                                                                    const jz_str *table, jazz_types_RowUuid row_uuid, jz_btree cells,
                                                                    const jazz_model_DeletionEvent *deletion, const u64 *now_ms) {
    return jazz_node__node__open_tx__NodeState__tx_write_in_schema_with_staged_large_values(self, tx_id, write_schema_version, table, row_uuid, cells,
                                                                                           deletion, now_ms, empty_staged_large_cells());
}

/* Stage a row write through an explicit registered schema view. */
jz_err jazz_node__node__open_tx__NodeState__tx_write_in_schema(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId write_schema_version,
                                                              const jz_str *table, jazz_types_RowUuid row_uuid, jz_btree cells,
                                                              const jazz_model_DeletionEvent *deletion) {
    return jazz_node__node__open_tx__NodeState__tx_write_in_schema_at_ms(self, tx_id, write_schema_version, table, row_uuid, cells, deletion, NULL);
}

/* Stage a row write inside an exclusive transaction. */
jz_err jazz_node__node__open_tx__NodeState__tx_write(NS *self, OpenTransactionId tx_id, const jz_str *table, jazz_types_RowUuid row_uuid, jz_btree cells,
                                                    const jazz_model_DeletionEvent *deletion) {
    return jazz_node__node__open_tx__NodeState__tx_write_in_schema(self, tx_id, self->catalogue.active_schema.schema, table, row_uuid, cells, deletion);
}

static jz_err stage_mergeable_write(NS *self, OpenTransactionId tx_id, PendingWrite *pending, bool replace_pending_deletion);

/* Stage a branch-local replacement whose unchanged cells were read from a
 * visible branch-view base. Commit construction reuses an indirect descriptor
 * only when its final cell still exactly matches this private engine
 * provenance. (consumes cells, branch, verified_inherited_cells, branch_view_copy) */
jz_err jazz_node__node__open_tx__NodeState__tx_write_mergeable_in_schema_and_branch_with_verified_inherited_cells(
    NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId write_schema_version, const jz_str *table, jazz_types_RowUuid row_uuid,
    jz_btree cells, const jazz_model_DeletionEvent *deletion, const u64 *now_ms, jazz_model_BranchSelector *branch, bool known_fresh_row,
    jz_btree *verified_inherited_cells, bool replace_pending_deletion, jazz_model_BranchViewCopyEvidence *branch_view_copy) {
    jz_err e = JZ_OK;
    const OpenTransaction *t0 = open_tx_lookup(self, tx_id);
    if (!t0) e = missing_open_batch(tx_id);
    else if (t0->kind.tag != JZN_OTX_KIND_MERGEABLE) e = invalid_mergeable("open transaction is not mergeable");
    if (!e.code) e = jazz_node__node__validate_mergeable_write_shape(cells.len == 0, deletion != NULL);
    const jazz_model_TableSchema *table_schema = NULL;
    if (!e.code) e = jazz_node__node__NodeState__table_in_schema_ref(self, table, write_schema_version, &table_schema);
    if (!e.code) {
        groove_ValueVec positional = { 0 };
        e = jazz_node__node__codec__positional_cells_from_map(table_schema, &cells, &positional);
        jz_vec_drop(&positional, sizeof(groove_Value), groove__records__values__Value__drop);
    }
    if (e.code) {
        jz_btree_drop(&cells);
        jazz_model__model__branch__BranchSelector__drop(branch);
        if (verified_inherited_cells) jz_btree_drop(verified_inherited_cells);
        if (branch_view_copy) { jazz_model__tx__BranchViewCopyEvidence__drop(branch_view_copy); jz_free(branch_view_copy); }
        return e;
    }
    PendingWrite *pending = JZ_NEW(PendingWrite);
    pending->table = jz_str_clone(table);
    pending->row_uuid = row_uuid;
    pending->schema_version = write_schema_version;
    pending->branch = branch;
    pending->cells.tag = JZN_PENDING_CELLS_REPLACE;
    pending->cells.cells = cells;
    pending->has_deletion = deletion != NULL;
    if (deletion) pending->deletion = *deletion;
    pending->has_now_ms = now_ms != NULL;
    if (now_ms) pending->now_ms = *now_ms;
    pending->known_fresh_row = known_fresh_row;
    pending->has_verified_inherited_cells = verified_inherited_cells != NULL;
    if (verified_inherited_cells) pending->verified_inherited_cells = *verified_inherited_cells;
    pending->branch_view_copy = branch_view_copy;
    pending->staged_large_cells = empty_staged_large_cells();
    return stage_mergeable_write(self, tx_id, pending, replace_pending_deletion);
}

jz_err jazz_node__node__open_tx__NodeState__tx_write_mergeable_in_schema_and_branch(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId write_schema_version,
                                                                                   const jz_str *table, jazz_types_RowUuid row_uuid, jz_btree cells,
                                                                                   const jazz_model_DeletionEvent *deletion, const u64 *now_ms,
                                                                                   jazz_model_BranchSelector *branch, bool known_fresh_row) {
    return jazz_node__node__open_tx__NodeState__tx_write_mergeable_in_schema_and_branch_with_verified_inherited_cells(
        self, tx_id, write_schema_version, table, row_uuid, cells, deletion, now_ms, branch, known_fresh_row, NULL, false, NULL);
}

jz_err jazz_node__node__open_tx__NodeState__tx_write_mergeable_in_schema(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId write_schema_version,
                                                                        const jz_str *table, jazz_types_RowUuid row_uuid, jz_btree cells,
                                                                        const jazz_model_DeletionEvent *deletion, const u64 *now_ms, bool known_fresh_row) {
    return jazz_node__node__open_tx__NodeState__tx_write_mergeable_in_schema_and_branch(self, tx_id, write_schema_version, table, row_uuid, cells, deletion,
                                                                                       now_ms, jazz_model__model__branch__BranchSelector__default(), known_fresh_row);
}

/* Stage a mergeable root patch whose `staged_large_cells` columns hold
 * descriptors of Groove roots this transaction staged itself for a partial
 * large-value update (#2087). See tx_write_in_schema_with_staged_large_values.
 * (consumes patch, branch, staged_large_cells) */
jz_err jazz_node__node__open_tx__NodeState__tx_patch_mergeable_with_staged_large_values(NS *self, OpenTransactionId tx_id,
                                                                                       jazz_types_SchemaVersionId write_schema_version, const jz_str *table,
                                                                                       jazz_types_RowUuid row_uuid, jz_btree patch, const u64 *now_ms,
                                                                                       jazz_model_BranchSelector *branch, bool replace_pending_deletion,
                                                                                       jz_btree staged_large_cells) {
    jz_err e = JZ_OK;
    const OpenTransaction *t0 = open_tx_lookup(self, tx_id);
    if (!t0) e = missing_open_batch(tx_id);
    else if (t0->kind.tag != JZN_OTX_KIND_MERGEABLE) e = invalid_mergeable("open transaction is not mergeable");
    bool some = false;
    jz_btree staged_cells;
    if (!e.code) e = jazz_node__node__open_tx__NodeState__tx_visible_current_cells_in_branch(self, tx_id, table, row_uuid, branch, &some, &staged_cells);
    if (!e.code) {
        if (!some) jz_btree_init(&staged_cells, &jzn_desc_str_value);
        cells_extend_cloned(&staged_cells, &patch);
        e = jazz_node__node__validate_mergeable_write_shape(staged_cells.len == 0, false);
        jz_btree_drop(&staged_cells);
    }
    const jazz_model_TableSchema *table_schema = NULL;
    if (!e.code) e = jazz_node__node__NodeState__table_in_schema_ref(self, table, write_schema_version, &table_schema);
    if (!e.code) {
        groove_ValueVec positional = { 0 };
        e = jazz_node__node__codec__positional_cells_from_map(table_schema, &patch, &positional);
        jz_vec_drop(&positional, sizeof(groove_Value), groove__records__values__Value__drop);
    }
    if (e.code) {
        jz_btree_drop(&patch);
        jazz_model__model__branch__BranchSelector__drop(branch);
        jz_btree_drop(&staged_large_cells);
        return e;
    }
    PendingWrite *pending = JZ_NEW(PendingWrite);
    pending->table = jz_str_clone(table);
    pending->row_uuid = row_uuid;
    pending->schema_version = write_schema_version;
    pending->branch = branch;
    pending->cells.tag = JZN_PENDING_CELLS_PATCH;
    pending->cells.cells = patch;
    pending->has_deletion = false;
    pending->has_now_ms = now_ms != NULL;
    if (now_ms) pending->now_ms = *now_ms;
    pending->known_fresh_row = false;
    pending->has_verified_inherited_cells = false;
    pending->branch_view_copy = NULL;
    pending->staged_large_cells = staged_large_cells;
    return stage_mergeable_write(self, tx_id, pending, replace_pending_deletion);
}

jz_err jazz_node__node__open_tx__NodeState__tx_patch_mergeable_in_schema_and_branch(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId write_schema_version,
                                                                                   const jz_str *table, jazz_types_RowUuid row_uuid, jz_btree patch, const u64 *now_ms,
                                                                                   jazz_model_BranchSelector *branch, bool replace_pending_deletion) {
    return jazz_node__node__open_tx__NodeState__tx_patch_mergeable_with_staged_large_values(self, tx_id, write_schema_version, table, row_uuid, patch, now_ms,
                                                                                           branch, replace_pending_deletion, empty_staged_large_cells());
}

jz_err jazz_node__node__open_tx__NodeState__tx_patch_mergeable_in_schema(NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId write_schema_version,
                                                                        const jz_str *table, jazz_types_RowUuid row_uuid, jz_btree patch, const u64 *now_ms) {
    return jazz_node__node__open_tx__NodeState__tx_patch_mergeable_in_schema_and_branch(self, tx_id, write_schema_version, table, row_uuid, patch, now_ms,
                                                                                       jazz_model__model__branch__BranchSelector__default(), false);
}

jz_err jazz_node__node__open_tx__NodeState__tx_current_row_state_in_branch(NS *self, OpenTransactionId tx_id, const jz_str *table, jazz_types_RowUuid row_uuid,
                                                                          const jazz_model_BranchSelector *branch, jazz_node_TransactionBranchRowState *out) {
    bool has_cells = false;
    jz_btree cells;
    JZ_TRY(jazz_node__node__NodeState__visible_current_cells_in_branch(self, table, branch, row_uuid, &has_cells, &cells));
    bool pending_deletion = false, staged_content = false;
    const OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { if (has_cells) jz_btree_drop(&cells); return missing_open_batch(tx_id); }
    const usize_vec *positions = StagedWrites__row_positions(&t->writes, table, row_uuid);
    for (u32 i = 0; i < positions->len; i++) {
        const PendingWrite *write = StagedWrites__live_slot(&t->writes, positions->ptr[i]);
        if (!jazz_model__model__branch__BranchSelector__eq(write->branch, branch)) continue;
        if (write->has_deletion) {
            if (write->deletion == JAZZ_DEL_DELETED) {
                if (has_cells) { jz_btree_drop(&cells); has_cells = false; }
                pending_deletion = true;
                continue;
            }
            pending_deletion = false; /* Restored */
        }
        staged_content = true;
        if (write->cells.tag == JZN_PENDING_CELLS_REPLACE) {
            if (has_cells) jz_btree_drop(&cells);
            cells_clone(&cells, &write->cells.cells);
            has_cells = true;
        } else {
            if (!has_cells) { jz_btree_init(&cells, &jzn_desc_str_value); has_cells = true; }
            cells_extend_cloned(&cells, &write->cells.cells);
        }
    }
    if (pending_deletion) {
        if (has_cells) jz_btree_drop(&cells);
        out->tag = JZN_TBRS_PENDING_DELETION;
        return JZ_OK;
    }
    if (has_cells) { out->tag = JZN_TBRS_VISIBLE; out->cells = cells; out->staged = staged_content; }
    else out->tag = JZN_TBRS_ABSENT;
    return JZ_OK;
}

jz_err jazz_node__node__open_tx__NodeState__tx_visible_current_cells_in_branch(NS *self, OpenTransactionId tx_id, const jz_str *table, jazz_types_RowUuid row_uuid,
                                                                              const jazz_model_BranchSelector *branch, bool *some, jz_btree *out) {
    jazz_node_TransactionBranchRowState state;
    JZ_TRY(jazz_node__node__open_tx__NodeState__tx_current_row_state_in_branch(self, tx_id, table, row_uuid, branch, &state));
    *some = state.tag == JZN_TBRS_VISIBLE;
    if (*some) *out = state.cells;
    return JZ_OK;
}

typedef struct { const PendingWrite *pending; } stage_ctx;
static bool is_same_branch_deleted(const PendingWrite *write, void *ctx) {
    const PendingWrite *pending = ctx;
    return jazz_model__model__branch__BranchSelector__eq(write->branch, pending->branch) && write->has_deletion && write->deletion == JAZZ_DEL_DELETED;
}
static bool is_same_branch_content(const PendingWrite *write, void *ctx) {
    const PendingWrite *pending = ctx;
    return jazz_model__model__branch__BranchSelector__eq(write->branch, pending->branch) && !write->has_deletion;
}
static bool is_superseded_by_deletion_event(const PendingWrite *existing, void *ctx) {
    const PendingWrite *pending = ctx;
    if (!jazz_model__model__branch__BranchSelector__eq(existing->branch, pending->branch)) return false;
    if (!pending->has_deletion) return false;
    if (pending->deletion == JAZZ_DEL_DELETED) return true;
    return existing->has_deletion; /* Restored */
}

/* (consumes `pending`) */
static jz_err stage_mergeable_write(NS *self, OpenTransactionId tx_id, PendingWrite *pending, bool replace_pending_deletion) {
    OpenTransaction *open_tx = open_tx_lookup(self, tx_id);
    if (!open_tx) { pending_write_free(pending); return missing_open_batch(tx_id); }
    table_row_key key = { pending->table, pending->row_uuid };
    jz_btree_remove(&open_tx->base_snapshot_rows, &key, NULL, NULL);
    /* A branch-view upsert may supersede a delete staged by this same
     * transaction. Reach this point only after validating the replacement,
     * then remove exactly that coordinate's pending tombstone atomically with
     * staging the visible content. */
    if (replace_pending_deletion) {
        pending_write_vec dropped;
        StagedWrites__remove_row_writes(&open_tx->writes, &pending->table, pending->row_uuid, is_same_branch_deleted, pending, &dropped);
        pending_write_vec_drop(&dropped);
    }
    if (!pending->has_deletion) {
        usize position;
        if (StagedWrites__find_row_write(&open_tx->writes, &pending->table, pending->row_uuid, is_same_branch_content, pending, &position)) {
            PendingWrite *existing = StagedWrites__get_mut(&open_tx->writes, position);
            pending->known_fresh_row |= existing->known_fresh_row;
            /* A patch extends a previously materialized branch-view
             * replacement, so it keeps its engine-proven preimage. A later
             * complete replacement deliberately replaces that proof. */
            if (pending->cells.tag == JZN_PENDING_CELLS_PATCH && !pending->has_verified_inherited_cells && existing->has_verified_inherited_cells) {
                cells_clone(&pending->verified_inherited_cells, &existing->verified_inherited_cells);
                pending->has_verified_inherited_cells = true;
            }
            if (pending->cells.tag == JZN_PENDING_CELLS_PATCH && !pending->branch_view_copy)
                pending->branch_view_copy = evidence_clone(existing->branch_view_copy);
            jazz_node_PendingCells cells;
            if (pending->cells.tag == JZN_PENDING_CELLS_PATCH) {
                cells.tag = existing->cells.tag; /* Replace+Patch -> Replace, Patch+Patch -> Patch */
                cells_clone(&cells.cells, &existing->cells.cells);
                cells_extend_cloned(&cells.cells, &pending->cells.cells);
            } else {
                cells.tag = JZN_PENDING_CELLS_REPLACE;
                cells_clone(&cells.cells, &pending->cells.cells);
            }
            jz_btree earlier = existing->staged_large_cells;
            jz_btree_init(&existing->staged_large_cells, &desc_staged_large_cells);
            jz_btree later = pending->staged_large_cells;
            jz_btree_init(&pending->staged_large_cells, &desc_staged_large_cells);
            jz_btree staged_large_cells = merge_large_value_claims(earlier, later, &open_tx->superseded_large_values);
            /* *existing = PendingWrite { cells, staged_large_cells, ..pending } */
            jz_btree_drop(&pending->cells.cells);
            jz_btree_drop(&pending->staged_large_cells);
            pending->cells = cells;
            pending->staged_large_cells = staged_large_cells;
            PendingWrite__drop(existing);
            *existing = *pending;
            jz_free(pending);
            PendingWrite__retain_current_large_value_claims(existing, &open_tx->superseded_large_values);
        } else {
            PendingWrite__retain_current_large_value_claims(pending, &open_tx->superseded_large_values);
            StagedWrites__push(&open_tx->writes, pending);
        }
        return JZ_OK;
    }

    pending_write_vec removed;
    StagedWrites__remove_row_writes(&open_tx->writes, &pending->table, pending->row_uuid, is_superseded_by_deletion_event, pending, &removed);
    for (u32 i = 0; i < removed.len; i++) {
        const jz_str *k; const groove_StagedLargeValue *staged;
        JZ_BTREE_FOR(&removed.ptr[i]->staged_large_cells, k, staged) { (void)k; JZ_VEC_PUSH(open_tx->superseded_large_values, staged->id); }
    }
    pending_write_vec_drop(&removed);
    StagedWrites__push(&open_tx->writes, pending);
    return JZ_OK;
}

/* Attach application metadata to an open transaction. */
jz_err jazz_node__node__open_tx__NodeState__tx_set_metadata(NS *self, OpenTransactionId tx_id, jz_str json) {
    OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { jz_str_drop(&json); return missing_open_batch(tx_id); }
    if (t->has_user_metadata_json) jz_str_drop(&t->user_metadata_json);
    t->has_user_metadata_json = true;
    t->user_metadata_json = json;
    return JZ_OK;
}

/* Whether committing this transaction can stage large scalar content. */
jz_err jazz_node__node__open_tx__NodeState__transaction_needs_large_value_staging(const NS *self, OpenTransactionId id, bool *out) {
    OPEN_TX(self, id, open_tx);
    *out = open_tx->superseded_large_values.len != 0;
    for (u32 i = 0; !*out && i < open_tx->writes.slots.len; i++) {
        const PendingWrite *write = open_tx->writes.slots.ptr[i];
        if (!write) continue;
        if (write->staged_large_cells.len != 0) { *out = true; break; }
        const jz_str *k; const groove_Value *v;
        JZ_BTREE_FOR(PendingWrite__staged_cells(write), k, v) { (void)k; if (jazz_node__node__value_needs_large_value_staging(v)) { *out = true; break; } }
    }
    return JZ_OK;
}

/* ---------------- commit ---------------- */

/* One row has one image per transaction: merge a row's staged content
 * replacement with its staged delete/restore event, in staging order. Returns
 * each merged write with whether it carries replacement content. (consumes
 * `writes`) */
typedef struct { PendingWrite *write; bool has_content; } merged_write;
JZ_NVEC(merged_write_vec, merged_write);
typedef struct { jz_str table; jazz_types_RowUuid row; jazz_types_SchemaVersionId schema; const jazz_model_BranchSelector *branch; } coalesce_key;
static int cmp_coalesce_key(const void *a, const void *b, void *c) {
    (void)c;
    const coalesce_key *x = a, *y = b;
    int r = jz_slice_cmp(jz_str_slice(&x->table), jz_str_slice(&y->table));
    if (!r) r = jz_uuid_cmp(&x->row, &y->row);
    if (!r) r = jz_uuid_cmp(&x->schema, &y->schema);
    if (!r) r = jazz_model__model__branch__BranchSelector__cmp(x->branch, y->branch);
    return r;
}
static const jz_btree_desc desc_coalesce = { sizeof(coalesce_key), sizeof(usize), cmp_coalesce_key, NULL, NULL };

static void coalesce_exclusive_writes(StagedWrites writes, merged_write_vec *merged) {
    merged->ptr = NULL; merged->len = merged->cap = 0;
    jz_btree merged_positions;
    jz_btree_init(&merged_positions, &desc_coalesce);
    for (u32 i = 0; i < writes.slots.len; i++) {
        PendingWrite *write = writes.slots.ptr[i];
        if (!write) continue;
        writes.slots.ptr[i] = NULL;
        bool has_content = !write->has_deletion;
        coalesce_key key = { write->table, write->row_uuid, write->schema_version, write->branch };
        const usize *existing = jz_btree_get(&merged_positions, &key);
        if (existing) {
            merged_write *m = &merged->ptr[*existing];
            PendingWrite *candidate = m->write;
            /* Content staged after a delete resurrects the row. */
            bool resurrects = has_content && candidate->has_deletion && candidate->deletion == JAZZ_DEL_DELETED;
            if (has_content) {
                jz_btree_drop(&candidate->cells.cells);
                candidate->cells = write->cells;
                jz_btree_init(&write->cells.cells, &jzn_desc_str_value);
                /* The staged roots claimed by those cells move with them. */
                jz_btree_drop(&candidate->staged_large_cells);
                candidate->staged_large_cells = write->staged_large_cells;
                jz_btree_init(&write->staged_large_cells, &desc_staged_large_cells);
                m->has_content = true;
            }
            if (resurrects) { candidate->has_deletion = true; candidate->deletion = JAZZ_DEL_RESTORED; }
            else if (write->has_deletion) { candidate->has_deletion = true; candidate->deletion = write->deletion; }
            if (write->has_now_ms) { candidate->has_now_ms = true; candidate->now_ms = write->now_ms; }
            pending_write_free(write);
        } else {
            usize position = merged->len;
            jz_btree_insert(&merged_positions, &key, &position, NULL);
            merged_write m = { write, has_content };
            JZ_VEC_PUSH(*merged, m);
        }
    }
    jz_btree_drop(&merged_positions);
    StagedWrites__drop(&writes);
}

static jz_err open_exclusive_is_locally_serializable(NS *self, OpenTransactionId open_batch_id, bool *out);
static void evict_superseded_large_values(NS *self, jazz_node_StagedLargeValueIdVec ids);

static jz_err exclusive_now_ms_range(u64 ms, jazz_types_TxTime *out) {
    jz_err e = jazz_types__time__TxTime__from_physical_ms(ms, out);
    if (e.code) { jz_err_drop(&e); return invalid_mergeable("exclusive write now_ms exceeds packed HLC physical-millisecond range"); }
    return JZ_OK;
}

static jz_err commit_exclusive_inner(NS *self, OpenTransactionId open_batch_id, jazz_types_AuthorSubject made_by, u64 now_ms, const TxId *reserved,
                                     jazz_node_PublishedTransaction *publication_out, jazz_protocol_SyncMessage **message_out) {
    jazz_types_AuthorSubject provenance_subject, permission_subject;
    {
        OPEN_TX(self, open_batch_id, t);
        if (t->kind.tag == JZN_OTX_KIND_MERGEABLE) return invalid_mergeable("open transaction is not exclusive");
        if (!jazz_types_AuthorSubject_eq(&t->kind.made_by, &made_by)) return jz_err_new(JZN_E_OPEN_TRANSACTION_IDENTITY_MISMATCH, NULL);
        provenance_subject = t->kind.made_by;
        permission_subject = t->kind.permission_subject;
    }
    jazz_types_RowAuthor author;
    jz_err e = jazz_types__ids__RowAuthor__from_session(provenance_subject, self->node_uuid, &author);
    if (e.code) { jz_err_drop(&e); return jz_err_new(JZN_E_UNADMITTED_WRITE_AUTHOR, NULL); }
    made_by = jazz_types__ids__RowAuthor__as_author_subject(author);
    bool serializable = false;
    JZ_TRY(open_exclusive_is_locally_serializable(self, open_batch_id, &serializable));
    if (!serializable) return jz_err_new(JZN_E_TRANSACTION_CONFLICT, NULL);
    const OpenTransaction *live = open_tx_lookup(self, open_batch_id);
    if (!live) return missing_open_batch(open_batch_id);
    OpenTransaction *open_tx = OpenTransaction__clone(live);
    for (u32 i = 0; i < open_tx->writes.slots.len; i++) {
        const PendingWrite *write = open_tx->writes.slots.ptr[i];
        jazz_types_TxTime ignored;
        if (write && write->has_now_ms && (e = exclusive_now_ms_range(write->now_ms, &ignored)).code) { open_tx_free(open_tx); return e; }
    }
    TxId tx_id;
    if (reserved) {
        if (!jz_uuid_eq(&reserved->node, &self->node_uuid)) {
            open_tx_free(open_tx);
            return invalid_mergeable("reserved transaction identity belongs to another node");
        }
        jazz_node__node__NodeState__merge_tx_time(self, reserved->time);
        tx_id = *reserved;
    } else {
        jazz_types_TxTime t;
        e = jazz_node__node__NodeState__mint_tx_time(self, now_ms, &t);
        if (e.code) { open_tx_free(open_tx); return e; }
        tx_id.time = t; tx_id.node = self->node_uuid;
    }
    jazz_model_Snapshot provenance_snapshot;
    jazz_model__tx__Snapshot__clone(&provenance_snapshot, &open_tx->base_snapshot);
    jazz_protocol_VersionRecordVec versions = { 0 };
    jz_btree claimed_large_values;
    jz_btree_init(&claimed_large_values, &desc_staged_id_set);
    jazz_node_StagedLargeValueIdVec superseded_large_values = open_tx->superseded_large_values;
    open_tx->superseded_large_values.ptr = NULL; open_tx->superseded_large_values.len = open_tx->superseded_large_values.cap = 0;
    merged_write_vec merged;
    StagedWrites writes = open_tx->writes;
    StagedWrites__default(&open_tx->writes);
    coalesce_exclusive_writes(writes, &merged);
    u32 mi = 0;
    for (; mi < merged.len && !e.code; mi++) {
        PendingWrite *write = merged.ptr[mi].write;
        bool has_content = merged.ptr[mi].has_content;
        VersionRow *snapshot_content = jazz_node__node__open_tx__NodeState__snapshot_winner(self, write->schema_version, &write->table, write->row_uuid,
                                                                                            &provenance_snapshot);
        const jazz_model_TableSchema *table_schema = NULL;
        jz_btree cells, inherited;
        bool have_cells = false, have_inherited = false;
        SnapshotRow snapshot_row;
        bool have_snapshot_row = false;
        e = jazz_node__node__NodeState__table_in_schema_ref(self, &write->table, write->schema_version, &table_schema);
        if (!e.code && write->cells.tag != JZN_PENDING_CELLS_REPLACE) e = invalid_mergeable("exclusive transaction cannot contain update patches");
        if (!e.code) { cells_clone(&cells, &write->cells.cells); have_cells = true; }
        if (!e.code) {
            e = jazz_node__node__open_tx__NodeState__snapshot_row_in_schema(self, write->schema_version, &write->table, write->row_uuid,
                                                                           &provenance_snapshot, &snapshot_row);
            have_snapshot_row = !e.code;
        }
        if (!e.code) e = jazz_node__node__NodeState__table_in_schema_ref(self, &write->table, write->schema_version, &table_schema);
        if (!e.code) {
            jz_btree_init(&inherited, &jzn_desc_str_value);
            have_inherited = true;
            if (snapshot_row.has_content_cells) {
                u32 n = table_schema->columns.len < snapshot_row.content_cells.len ? table_schema->columns.len : snapshot_row.content_cells.len;
                for (u32 i = 0; i < n; i++) {
                    const groove_Value *value = snapshot_row.content_cells.ptr[i];
                    if (!value) continue;
                    jz_str name = jz_str_clone(&table_schema->columns.ptr[i].name);
                    groove_Value v;
                    groove__records__values__Value__clone(&v, value);
                    jz_btree_insert(&inherited, &name, &v, NULL);
                }
            }
            const jz_str *column; const groove_Value *value;
            JZ_BTREE_FOR(&cells, column, value) {
                const groove_Value *inh = jz_btree_get(&inherited, column);
                if (!jazz_node__node__value_contains_indirect_descriptor(value) || (inh && groove__records__values__Value__eq(inh, value))) continue;
                const groove_StagedLargeValue *staged = jz_btree_get(&write->staged_large_cells, column);
                if (staged && value_is_exact_descriptor(value, staged->value_ref)) {
                    jz_btree_insert(&claimed_large_values, &staged->id, NULL, NULL);
                } else {
                    e = invalid_mergeable("exclusive transaction contains an unverified large-value descriptor");
                    break;
                }
            }
        }
        if (!e.code) {
            const jz_str *k; const groove_StagedLargeValue *staged;
            JZ_BTREE_FOR(&write->staged_large_cells, k, staged) {
                (void)k;
                if (!jz_btree_contains(&claimed_large_values, &staged->id)) JZ_VEC_PUSH(superseded_large_values, staged->id);
            }
            /* A delete or restore without replacement content writes the
             * snapshot's row image with its new deletion state. */
            if (!has_content) { jz_btree_drop(&cells); cells_clone(&cells, &inherited); }
            /* Content replacements carry the snapshot row's deletion state. */
            bool has_deletion = write->has_deletion;
            jazz_model_DeletionEvent deletion = write->deletion;
            if (!has_deletion && snapshot_content) has_deletion = jazz_node__node__codec__VersionRow__deletion(snapshot_content, &deletion);
            JZ_NVEC(u8_vec, u8);
            u8_vec semantic_kinds = { 0 };
            const jz_str *column; void *u;
            JZ_BTREE_FOR(&cells, column, u) {
                (void)u;
                u8 kind = JAZZ_LVK_NOT_LARGE;
                for (u32 c = 0; c < table_schema->columns.len; c++)
                    if (jz_str_eq(&table_schema->columns.ptr[c].name, column)) { kind = table_schema->columns.ptr[c].large_value_kind; break; }
                JZ_VEC_PUSH(semantic_kinds, kind);
            }
            u32 idx = 0;
            groove_Value *value;
            JZ_BTREE_FOR(&cells, column, value) {
                (void)column;
                bool has_staged = false;
                groove_StagedLargeValue staged;
                e = jazz_node__node__NodeState__prepare_and_stage_large_scalar(self, value, semantic_kinds.ptr[idx++], &has_staged, &staged);
                if (e.code) break;
                if (has_staged) groove__large_values__StagedLargeValue__drop(&staged);
            }
            jz_free(semantic_kinds.ptr);
            if (!e.code) e = jazz_node__node__NodeState__table_in_schema_ref(self, &write->table, write->schema_version, &table_schema);
            groove_ValueVec positional = { 0 };
            if (!e.code) e = jazz_node__node__codec__positional_cells_from_map(table_schema, &cells, &positional);
            jazz_types_TxTime provenance_at = 0;
            if (!e.code) e = exclusive_now_ms_range(write->has_now_ms ? write->now_ms : now_ms, &provenance_at);
            if (!e.code) {
                jazz_types_AuthorSubject created_by = made_by;
                jazz_types_TxTime created_at = provenance_at;
                if (snapshot_content) {
                    created_by = jazz_node__node__codec__VersionRow__created_by(snapshot_content);
                    created_at = jazz_node__node__codec__VersionRow__created_at(snapshot_content);
                }
                jazz_protocol_VersionRecord *version = NULL;
                e = jazz_protocol__protocol__VersionRecord__encode(table_schema, write->schema_version, write->row_uuid, created_by,
                                                                    jazz_types__time__TxTime__physical_ms(created_at), made_by,
                                                                    jazz_types__time__TxTime__physical_ms(provenance_at), &positional, has_deletion, deletion, &version);
                if (!e.code) JZ_VEC_PUSH(versions, version);
            }
            jz_vec_drop(&positional, sizeof(groove_Value), groove__records__values__Value__drop);
        }
        if (have_cells) jz_btree_drop(&cells);
        if (have_inherited) jz_btree_drop(&inherited);
        if (have_snapshot_row) jazz_node__node__open_tx__SnapshotRow__drop(&snapshot_row);
        if (snapshot_content) jazz_node__node__codec__VersionRow__drop(snapshot_content);
    }
    for (u32 i = 0; i < merged.len; i++) pending_write_free(merged.ptr[i].write);
    jz_free(merged.ptr);
    jazz_model__tx__Snapshot__drop(&provenance_snapshot);
    jazz_model_Transaction *tx = NULL;
    if (!e.code && versions.len > UINT32_MAX) e = invalid_mergeable("transaction write count exceeds u32");
    if (!e.code) {
        jz_slice meta = open_tx->has_user_metadata_json ? jz_str_slice(&open_tx->user_metadata_json) : (jz_slice){ 0 };
        /* Keep policy evaluation independent from durable provenance and from
         * the transport link's SYSTEM credential. */
        tx = jazz_model__tx__Transaction__new(tx_id, JAZZ_TXKIND_EXCLUSIVE, versions.len, made_by, true, permission_subject, true, open_tx->base_snapshot,
                                              &open_tx->row_reads, &open_tx->absent_reads, &open_tx->predicate_reads,
                                              open_tx->has_user_metadata_json ? &meta : NULL, NULL);
        /* Publication accepts staged roots by descriptor and tolerates a
         * missing one; a root this transaction staged must still be present. */
        e = jazz_node__node__NodeState__ensure_large_value_stages_current(self, &claimed_large_values);
    }
    jz_btree_drop(&claimed_large_values);
    if (e.code) {
        if (tx) jazz_model__tx__Transaction__drop(tx);
        for (u32 i = 0; i < versions.len; i++) jazz_protocol__protocol__VersionRecord__drop(versions.ptr[i]);
        jz_free(versions.ptr);
        jz_free(superseded_large_values.ptr);
        open_tx_free(open_tx);
        return e;
    }
    /* Evict before publishing: a resident publication holds Groove's
     * large-value lifecycle until it is durable, and eviction then defers. */
    evict_superseded_large_values(self, superseded_large_values);
    jazz_protocol_VersionRecordVec published_versions = { 0 };
    for (u32 i = 0; i < versions.len; i++) JZ_VEC_PUSH(published_versions, jazz_protocol__protocol__VersionRecord__clone(versions.ptr[i]));
    e = jazz_node__node__ingest__NodeState__publish_pending_transaction_and_versions(self, jazz_model__tx__Transaction__clone(tx), published_versions,
                                                                                    self->authored_commit_durability, publication_out);
    open_tx_free(open_tx);
    if (e.code) {
        jazz_model__tx__Transaction__drop(tx);
        for (u32 i = 0; i < versions.len; i++) jazz_protocol__protocol__VersionRecord__drop(versions.ptr[i]);
        jz_free(versions.ptr);
        return e;
    }
    OpenTransaction *removed = NULL;
    if (jz_btree_remove(&self->open_tx.open_transactions, &open_batch_id, NULL, &removed)) open_tx_free(removed);
    jz_btree_insert(&self->open_tx.closed_batches, &open_batch_id, NULL, NULL);
    *message_out = jazz_protocol__protocol__SyncMessage__CommitUnit(tx, versions.ptr, versions.len);
    return JZ_OK;
}

/* Commit an exclusive transaction and return its sync commit unit. */
jz_err jazz_node__node__open_tx__NodeState__commit_exclusive(NS *self, OpenTransactionId open_batch_id, jazz_types_AuthorSubject made_by, u64 now_ms,
                                                            jazz_node_PublishedTransaction *publication, jazz_protocol_SyncMessage **message) {
    return commit_exclusive_inner(self, open_batch_id, made_by, now_ms, NULL, publication, message);
}

/* Commit an exclusive transaction and return its sync commit unit. */
jz_err jazz_node__node__open_tx__NodeState__commit_exclusive_bound(NS *self, OpenTransactionId open_batch_id, u64 now_ms,
                                                                  jazz_node_PublishedTransaction *publication, jazz_protocol_SyncMessage **message) {
    OPEN_TX(self, open_batch_id, t);
    if (t->kind.tag != JZN_OTX_KIND_EXCLUSIVE) return jz_err_new(JZN_E_OPEN_TRANSACTION_IDENTITY_MISMATCH, NULL);
    return jazz_node__node__open_tx__NodeState__commit_exclusive(self, open_batch_id, t->kind.made_by, now_ms, publication, message);
}

/* Commit a bound exclusive transaction at a synchronously reserved local
 * identity after all cold serializability and large-value checks finish. */
jz_err jazz_node__node__open_tx__NodeState__commit_exclusive_bound_at(NS *self, OpenTransactionId open_batch_id, TxId reserved,
                                                                     jazz_node_PublishedTransaction *publication, jazz_protocol_SyncMessage **message) {
    OPEN_TX(self, open_batch_id, t);
    if (t->kind.tag != JZN_OTX_KIND_EXCLUSIVE) return jz_err_new(JZN_E_OPEN_TRANSACTION_IDENTITY_MISMATCH, NULL);
    return commit_exclusive_inner(self, open_batch_id, t->kind.made_by, jazz_types__time__TxTime__physical_ms(reserved.time), &reserved, publication, message);
}

/* `visible && !snapshot_covers` over every version of one row (or table). */
static jz_err versions_stay_covered(NS *self, const jazz_node_VersionRowVec *versions, const jazz_model_Snapshot *base, bool *covered) {
    *covered = true;
    for (u32 i = 0; i < versions->len; i++) {
        TxId tx_id;
        JZ_TRY(jazz_node__node__NodeState__version_tx_id(self, versions->ptr[i], &tx_id));
        bool found = false;
        jazz_model_Fate fate = { 0 };
        bool has_gt = false;
        jazz_types_GlobalTime gt; jazz_model_DurabilityTier dur;
        JZ_TRY(jazz_node__node__currency__NodeState__query_transaction_state(self, tx_id, &found, &fate, &has_gt, &gt, &dur));
        bool visible = found && fate.tag != JAZZ_FATE_REJECTED;
        if (visible && !jazz_node__node__open_tx__NodeState__snapshot_covers(self, tx_id, base)) { *covered = false; return JZ_OK; }
    }
    return JZ_OK;
}

static jz_err row_versions_stay_covered(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, const jazz_model_Snapshot *base, bool *covered) {
    jazz_node_VersionRowVec versions = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__query_row_versions(self, table, row_uuid, &versions));
    jz_err e = versions_stay_covered(self, &versions, base, covered);
    jz_vec_drop(&versions, sizeof(VersionRow *), jazz_node__node__codec__VersionRow__drop_v);
    return e;
}

static int cmp_txid_plain(const void *a, const void *b) { return jazz_model_TxId_cmp(a, b); }
static int cmp_txid_ctx(const void *a, const void *b, void *ctx) { (void)ctx; return jazz_model_TxId_cmp(a, b); }

static jz_err open_exclusive_is_locally_serializable(NS *self, OpenTransactionId open_batch_id, bool *out) {
    *out = false;
    const OpenTransaction *live = open_tx_lookup(self, open_batch_id);
    if (!live) return missing_open_batch(open_batch_id);
    OpenTransaction *open_tx = OpenTransaction__clone(live);
    jz_err e = JZ_OK;
    bool covered = true;
    for (u32 i = 0; covered && !e.code && i < open_tx->row_reads.len; i++)
        e = row_versions_stay_covered(self, &open_tx->row_reads.ptr[i].table, open_tx->row_reads.ptr[i].row, &open_tx->base_snapshot, &covered);
    for (u32 i = 0; covered && !e.code && i < open_tx->absent_reads.len; i++)
        e = row_versions_stay_covered(self, &open_tx->absent_reads.ptr[i].table, open_tx->absent_reads.ptr[i].row_uuid, &open_tx->base_snapshot, &covered);
    for (u32 i = 0; covered && !e.code && i < open_tx->writes.slots.len; i++) {
        const PendingWrite *write = open_tx->writes.slots.ptr[i];
        if (write) e = row_versions_stay_covered(self, &write->table, write->row_uuid, &open_tx->base_snapshot, &covered);
    }
    if (e.code || !covered) { open_tx_free(open_tx); return e; }
    for (u32 p = 0; p < open_tx->predicate_reads.len; p++) {
        const struct jazz_model_PredicateRead *predicate = &open_tx->predicate_reads.ptr[p];
        bool has_comparison = false;
        jazz_model_Snapshot comparison;
        jazz_node_VersionRowVec versions = { 0 };
        e = jazz_node__node__currency__NodeState__query_table_versions(self, &predicate->table, &versions);
        for (u32 i = 0; !e.code && i < versions.len; i++) {
            TxId tx_id;
            e = jazz_node__node__NodeState__version_tx_id(self, versions.ptr[i], &tx_id);
            if (e.code) break;
            bool found = false, has_gt = false;
            jazz_model_Fate fate = { 0 };
            jazz_types_GlobalTime gt; jazz_model_DurabilityTier dur;
            e = jazz_node__node__currency__NodeState__query_transaction_state(self, tx_id, &found, &fate, &has_gt, &gt, &dur);
            if (e.code) break;
            bool visible = found && fate.tag != JAZZ_FATE_REJECTED;
            if (!visible || jazz_node__node__open_tx__NodeState__snapshot_covers(self, tx_id, &open_tx->base_snapshot)) continue;
            if (!has_comparison) {
                const jazz_model_Query *query = predicate->shape;
                /* This comparison advances only root-table history. Preserve
                 * conservative rejection for relational reads and aggregates,
                 * whose output hides input rewrites. */
                shape_binding_key key = { predicate->shape_id, predicate->binding_id };
                bool narrowed = jz_btree_contains(&open_tx->narrowed_predicate_reads, &key);
                bool degenerate = false;
                bool reject = query->aggregate != NULL || (!narrowed && (query->joins.len != 0 || query->array_subqueries.len != 0)) ||
                              query->flat_join != NULL || query->policy_branches.len != 0 || query->reachable.len != 0 ||
                              query->inherits.len != 0 || query->includes.len != 0 || query->relation != NULL;
                if (!reject) {
                    e = jazz_node__node__ingest__NodeState__predicate_read_is_degenerate_whole_table(self, predicate, &degenerate);
                    if (e.code) break;
                    reject = degenerate;
                }
                if (reject) {
                    jz_vec_drop(&versions, sizeof(VersionRow *), jazz_node__node__codec__VersionRow__drop_v);
                    open_tx_free(open_tx);
                    return JZ_OK;
                }
                jazz_model__tx__Snapshot__clone(&comparison, &open_tx->base_snapshot);
                has_comparison = true;
            }
            JZ_VEC_PUSH(comparison.dots, tx_id);
        }
        jz_vec_drop(&versions, sizeof(VersionRow *), jazz_node__node__codec__VersionRow__drop_v);
        if (e.code) { if (has_comparison) jazz_model__tx__Snapshot__drop(&comparison); break; }
        if (has_comparison) {
            jz_sort(comparison.dots.ptr, comparison.dots.len, sizeof(TxId), cmp_txid_ctx, NULL);
            jz_vec_dedup(&comparison.dots, sizeof(TxId), cmp_txid_plain, NULL);
            /* A newer row outside the filter is not a phantom. Retain the
             * fixed base and compare against every newly visible version,
             * including local writes that have no authority receipt yet. */
            bool differ = false;
            e = jazz_node__node__ingest__NodeState__shape_predicate_outputs_differ(self, predicate, &open_tx->base_snapshot, &comparison, &differ);
            jazz_model__tx__Snapshot__drop(&comparison);
            if (e.code) break;
            if (differ) { open_tx_free(open_tx); return JZ_OK; }
        }
    }
    open_tx_free(open_tx);
    if (!e.code) *out = true;
    return e;
}

typedef u64 (*next_now_ms_fn)(void *ctx);

static int cmp_branch_write_intent_coordinate(const jazz_model_BranchWriteIntent *left, const jazz_model_BranchWriteIntent *right) {
    if (left->physical_table_id != right->physical_table_id) return left->physical_table_id < right->physical_table_id ? -1 : 1;
    int r = jz_uuid_cmp(&left->authored_schema, &right->authored_schema);
    if (!r) r = jz_uuid_cmp(&left->row_uuid, &right->row_uuid);
    if (!r) r = jazz_model__model__branch__BranchKey__cmp(left->head, right->head);
    return r;
}
static int cmp_branch_write_intent(const void *a, const void *b, void *c) {
    (void)c;
    return cmp_branch_write_intent_coordinate(a, b);
}

static jz_err commit_mergeable_open_inner(NS *self, OpenTransactionId open_batch_id, next_now_ms_fn next_now_ms, void *next_ctx, const TxId *reserved,
                                          jazz_node_PublishedTransaction *out) {
    {
        OPEN_TX(self, open_batch_id, t);
        if (t->kind.tag != JZN_OTX_KIND_MERGEABLE) return invalid_mergeable("open transaction is not mergeable");
    }
    const OpenTransaction *live = open_tx_lookup(self, open_batch_id);
    if (!live) return missing_open_batch(open_batch_id);
    OpenTransaction *open_tx = OpenTransaction__clone(live);
    if (open_tx->kind.tag != JZN_OTX_KIND_MERGEABLE) { open_tx_free(open_tx); return invalid_mergeable("open transaction is not mergeable"); }
    jazz_types_AuthorSubject made_by = open_tx->kind.made_by;
    bool has_permission_subject = open_tx->kind.has_permission_subject;
    jazz_types_AuthorSubject permission_subject = open_tx->kind.permission_subject;
    jz_err e = JZ_OK;
    /* A non-root branch version must never arrive at authority as an
     * unclassified insert-shaped overlay. Capture a sorted exact-version intent
     * before lowering pending writes; this is non-causal metadata carried
     * through the ordinary transaction/outbox/relay path. */
    jazz_model_BranchWriteIntentVec branch_write_intents = { 0 };
    for (u32 i = 0; !e.code && i < open_tx->writes.slots.len; i++) {
        const PendingWrite *write = open_tx->writes.slots.ptr[i];
        if (!write || write->branch->values.len == 0) continue;
        jazz_protocol_SchemaVersion *const *sv = jz_btree_get(&self->catalogue.catalogue_schemas, &write->schema_version);
        if (!sv) { e = JZN_INVALID_STORED("write schema is missing"); break; }
        const jazz_model_TableSchema *table = NULL;
        e = jazz_node__node__NodeState__table_in_schema_ref(self, &write->table, write->schema_version, &table);
        if (e.code) break;
        jazz_model_BranchKey *head = NULL;
        jz_str msg = { 0 };
        e = jazz_model__schema__RuntimeSchema__project_branch_view_selector((*sv)->schema, table, write->branch, &head, &msg);
        if (e.code) {
            /* .map_err(Error::InvalidBranchKey) */
            jz_err_drop(&e);
            e = jz_err_fmt(JZN_E_INVALID_BRANCH_KEY, "%.*s", (int)msg.len, msg.ptr);
            jz_str_drop(&msg);
            break;
        }
        jazz_model_BranchWriteIntent intent = { 0 };
        intent.version = 1;
        e = jazz_node__node__physical__NodeState__physical_table_id_for_schema(self, write->schema_version, &write->table, &intent.physical_table_id);
        if (e.code) { jazz_model__model__branch__BranchKey__drop(head); break; }
        intent.authored_schema = write->schema_version;
        intent.row_uuid = write->row_uuid;
        intent.head = head;
        if (write->branch_view_copy) {
            intent.operation.tag = JAZZ_BWO_VIEW_UPDATE_COPY;
            jazz_model__tx__BranchViewCopyEvidence__clone(&intent.operation.copy, write->branch_view_copy);
        } else {
            intent.operation.tag = write->cells.tag == JZN_PENDING_CELLS_PATCH ? JAZZ_BWO_EXACT_HEAD_UPDATE : JAZZ_BWO_EXACT_HEAD_INSERT;
        }
        JZ_VEC_PUSH(branch_write_intents, intent);
    }
    if (!e.code) {
        jz_sort(branch_write_intents.ptr, branch_write_intents.len, sizeof(jazz_model_BranchWriteIntent), cmp_branch_write_intent, NULL);
        for (u32 i = 0; i + 1 < branch_write_intents.len; i++) {
            const jazz_model_BranchWriteIntent *a = &branch_write_intents.ptr[i], *b = &branch_write_intents.ptr[i + 1];
            if (cmp_branch_write_intent_coordinate(a, b) == 0 && !jazz_model__tx__BranchWriteOperation__eq(&a->operation, &b->operation)) {
                e = invalid_mergeable("branch writes for one physical row must have one operation identity");
                break;
            }
        }
    }
    if (!e.code) {
        /* dedup_by coordinate */
        u32 kept = 0;
        for (u32 i = 0; i < branch_write_intents.len; i++) {
            if (kept > 0 && cmp_branch_write_intent_coordinate(&branch_write_intents.ptr[kept - 1], &branch_write_intents.ptr[i]) == 0) {
                jazz_model__tx__BranchWriteIntent__drop(&branch_write_intents.ptr[i]);
                continue;
            }
            branch_write_intents.ptr[kept++] = branch_write_intents.ptr[i];
        }
        branch_write_intents.len = kept;
    }
    /* Copy evidence is stored by index inside the intent operation enum. Keep
     * that side-list as the exact canonical projection of sorted intents
     * rather than public write order, so a durable round trip cannot relabel
     * equivalent coordinates with another source proof. */
    jazz_model_BranchViewCopyEvidenceVec branch_view_copies = { 0 };
    for (u32 i = 0; !e.code && i < branch_write_intents.len; i++) {
        if (branch_write_intents.ptr[i].operation.tag != JAZZ_BWO_VIEW_UPDATE_COPY) continue;
        jazz_model_BranchViewCopyEvidence copy;
        jazz_model__tx__BranchViewCopyEvidence__clone(&copy, &branch_write_intents.ptr[i].operation.copy);
        JZ_VEC_PUSH(branch_view_copies, copy);
    }
    jazz_node_SchemaCommitVec commits = { 0 };
    jazz_node_StagedLargeValueIdVec superseded_large_values = open_tx->superseded_large_values;
    open_tx->superseded_large_values.ptr = NULL; open_tx->superseded_large_values.len = open_tx->superseded_large_values.cap = 0;
    u32 index = 0;
    for (u32 i = 0; !e.code && i < open_tx->writes.slots.len; i++) {
        PendingWrite *write = open_tx->writes.slots.ptr[i];
        if (!write) continue;
        jz_btree staged_large_cells = write->staged_large_cells;
        jz_btree_init(&write->staged_large_cells, &desc_staged_large_cells);
        jz_btree cells;
        bool has_authored = false, has_preimage = false;
        jz_btree authored_columns, preimage;
        if (write->cells.tag == JZN_PENDING_CELLS_REPLACE) {
            cells = write->cells.cells;
            jz_btree_init(&write->cells.cells, &jzn_desc_str_value);
        } else {
            jz_btree patch = write->cells.cells;
            jz_btree_init(&write->cells.cells, &jzn_desc_str_value);
            jz_btree_init(&cells, &jzn_desc_str_value);
            /* The engine reads this preimage physically, so an untouched large
             * column is an indirect descriptor rather than its logical scalar.
             * Keep only the untouched cells as engine-proven provenance: every
             * authored patch value still has to be a logical scalar (#3507). */
            bool some = false;
            e = jazz_node__node__NodeState__visible_current_cells_in_branch(self, &write->table, write->branch, write->row_uuid, &some, &preimage);
            if (e.code) { jz_btree_drop(&patch); jz_btree_drop(&cells); jz_btree_drop(&staged_large_cells); break; }
            if (!some) jz_btree_init(&preimage, &jzn_desc_str_value);
            cells_extend_cloned(&cells, &preimage);
            const jz_str *column; void *u;
            JZ_BTREE_FOR(&patch, column, u) { (void)u; jz_btree_remove(&preimage, column, NULL, NULL); }
            jz_btree_init(&authored_columns, &jzn_desc_str_set);
            JZ_BTREE_FOR(&patch, column, u) { (void)u; jz_str c = jz_str_clone(column); jz_btree_insert(&authored_columns, &c, NULL, NULL); }
            cells_extend_cloned(&cells, &patch);
            jz_btree_drop(&patch);
            has_authored = has_preimage = true;
        }
        jazz_node_SchemaCommit sc;
        sc.schema = write->schema_version;
        jazz_node__node__MergeableCommit__new(&sc.commit, jz_str_clone(&write->table), write->row_uuid,
                                              write->has_now_ms ? write->now_ms : next_now_ms(next_ctx));
        jazz_model__model__branch__BranchSelector__drop(sc.commit.branch);
        jazz_node__node__MergeableCommit__branch(&sc.commit, write->branch);
        write->branch = jazz_model__model__branch__BranchSelector__default();
        jazz_node__node__MergeableCommit__made_by(&sc.commit, made_by);
        jazz_node__node__MergeableCommit__cells(&sc.commit, &cells);
        if (write->has_verified_inherited_cells) jazz_node__node__MergeableCommit__verified_inherited_large_cells(&sc.commit, &write->verified_inherited_cells);
        if (has_preimage) { jazz_node__node__MergeableCommit__verified_inherited_large_cells(&sc.commit, &preimage); jz_btree_drop(&preimage); }
        jz_str column; groove_StagedLargeValue staged;
        while (jz_btree_pop_first(&staged_large_cells, &column, &staged)) {
            const groove_Value *value = jz_btree_get(&sc.commit.cells, &column);
            if (value && value_is_exact_descriptor(value, staged.value_ref)) {
                if (!jz_btree_insert(&sc.commit.prepared_large_columns, &column, NULL, NULL)) column.ptr = NULL;
                JZ_VEC_PUSH(sc.commit.staged_large_values, staged.id);
            } else {
                JZ_VEC_PUSH(superseded_large_values, staged.id);
            }
            if (column.ptr) jz_str_drop(&column);
            groove__large_values__StagedLargeValue__drop(&staged);
        }
        jz_btree_drop(&staged_large_cells);
        if (has_authored) jazz_node__node__MergeableCommit__authored_columns(&sc.commit, authored_columns);
        if (has_permission_subject) jazz_node__node__MergeableCommit__permission_subject(&sc.commit, permission_subject);
        if (write->has_deletion) jazz_node__node__MergeableCommit__deletion(&sc.commit, write->deletion);
        if (write->known_fresh_row) jazz_node__node__MergeableCommit__known_fresh_row(&sc.commit);
        if (index == 0 && open_tx->has_user_metadata_json)
            jazz_node__node__MergeableCommit__user_metadata(&sc.commit, jz_str_clone(&open_tx->user_metadata_json));
        JZ_VEC_PUSH(commits, sc);
        index++;
    }
    /* Constructing an open batch may require snapshot reads, but it must not
     * advance HLC/parent state until *every* lowered write is valid. In
     * particular, a later invalid public provenance value must not make an
     * otherwise valid first write observably consume a clock position. */
    for (u32 i = 0; !e.code && i < commits.len; i++) e = jazz_node__node__MergeableCommit__validate(&commits.ptr[i].commit);
    if (!e.code && commits.len == 0) e = invalid_mergeable("mergeable transaction requires at least one write");
    jazz_types_TxTime made_at = 0;
    if (!e.code) {
        if (reserved) {
            if (!jz_uuid_eq(&reserved->node, &self->node_uuid)) e = invalid_mergeable("reserved transaction identity belongs to another node");
            else { jazz_node__node__NodeState__merge_tx_time(self, reserved->time); made_at = reserved->time; }
        } else {
            e = jazz_node__node__NodeState__mint_tx_time(self, commits.ptr[0].commit.now_ms, &made_at);
        }
    }
    open_tx_free(open_tx);
    if (e.code) {
        for (u32 i = 0; i < commits.len; i++) jazz_node__node__MergeableCommit__drop(&commits.ptr[i].commit);
        jz_free(commits.ptr);
        jz_vec_drop(&branch_write_intents, sizeof(jazz_model_BranchWriteIntent), jazz_model__tx__BranchWriteIntent__drop);
        for (u32 i = 0; i < branch_view_copies.len; i++) jazz_model__tx__BranchViewCopyEvidence__drop(&branch_view_copies.ptr[i]);
        jz_free(branch_view_copies.ptr);
        jz_free(superseded_large_values.ptr);
        return e;
    }
    jazz_model_ContributionMergeProvenance *contribution_merge = NULL;
    if (branch_view_copies.len != 0 || branch_write_intents.len != 0) {
        contribution_merge = JZ_NEW(jazz_model_ContributionMergeProvenance);
        contribution_merge->source = jazz_model__model__branch__BranchKey__default();
        contribution_merge->target = jazz_model__model__branch__BranchKey__default();
        contribution_merge->branch_view_copies = branch_view_copies;
        contribution_merge->branch_write_intents = branch_write_intents;
    } else {
        jz_free(branch_view_copies.ptr);
        jz_free(branch_write_intents.ptr);
    }
    /* Evict before publishing: a resident publication holds Groove's
     * large-value lifecycle until it is durable, and eviction then defers. */
    evict_superseded_large_values(self, superseded_large_values);
    JZ_TRY(jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions_and_provenance(self, commits, made_at, contribution_merge, out));
    OpenTransaction *removed = NULL;
    if (jz_btree_remove(&self->open_tx.open_transactions, &open_batch_id, NULL, &removed)) open_tx_free(removed);
    jz_btree_insert(&self->open_tx.closed_batches, &open_batch_id, NULL, NULL);
    return JZ_OK;
}

/* Commit a mergeable open transaction through the ordinary mergeable batch path. */
jz_err jazz_node__node__open_tx__NodeState__commit_mergeable_open(NS *self, OpenTransactionId open_batch_id, next_now_ms_fn next_now_ms, void *ctx,
                                                                 jazz_node_PublishedTransaction *out) {
    return commit_mergeable_open_inner(self, open_batch_id, next_now_ms, ctx, NULL, out);
}

/* Commit an open mergeable transaction at an identity synchronously reserved
 * by the owning runtime before cold preparation begins. */
jz_err jazz_node__node__open_tx__NodeState__commit_mergeable_open_at(NS *self, OpenTransactionId open_batch_id, TxId reserved, next_now_ms_fn next_now_ms,
                                                                    void *ctx, jazz_node_PublishedTransaction *out) {
    return commit_mergeable_open_inner(self, open_batch_id, next_now_ms, ctx, &reserved, out);
}

/* Evict Groove roots a committing transaction staged but no longer
 * references. Callers commit under a lock that has settled earlier local
 * publications, so Groove does not defer these evictions. Eviction is
 * idempotent, and a root that survives a failed eviction is reclaimed by the
 * staging TTL, so failures are not surfaced. (consumes `ids`) */
static void evict_superseded_large_values(NS *self, jazz_node_StagedLargeValueIdVec ids) {
    for (u32 i = 0; i < ids.len; i++) {
        bool evicted;
        jz_err e = jazz_node__node__NodeState__evict_staged_large_value(self, ids.ptr[i], &evicted);
        jz_err_drop(&e);
    }
    jz_free(ids.ptr);
}

/* Abandon an open transaction.
 *
 * Groove roots its partial large-value updates staged become unreachable;
 * they are queued for evict_released_large_values, and the staging TTL
 * reclaims any that eviction misses. */
jz_err jazz_node__node__open_tx__NodeState__abandon_tx(NS *self, OpenTransactionId tx_id) {
    OpenTransaction *open_tx = NULL;
    if (!jz_btree_remove(&self->open_tx.open_transactions, &tx_id, NULL, &open_tx)) return missing_open_batch(tx_id);
    jz_btree_insert(&self->open_tx.closed_batches, &tx_id, NULL, NULL);
    jazz_node_StagedLargeValueIdVec ids = { 0 };
    OpenTransaction__staged_large_value_ids(open_tx, &ids);
    for (u32 i = 0; i < ids.len; i++) JZ_VEC_PUSH(self->open_tx.released_large_values, ids.ptr[i]);
    jz_free(ids.ptr);
    open_tx_free(open_tx);
    return JZ_OK;
}

/* Terminalize every transaction still owned by this runtime.
 *
 * Database shutdown closes transaction admission before calling this, so
 * moving the live ids into `closed_batches` is the final ownership pass. */
void jazz_node__node__open_tx__NodeState__abandon_all_open_transactions(NS *self) {
    OpenTransactionId tx_id;
    OpenTransaction *open_tx;
    while (jz_btree_pop_first(&self->open_tx.open_transactions, &tx_id, &open_tx)) {
        jz_btree_insert(&self->open_tx.closed_batches, &tx_id, NULL, NULL);
        jazz_node_StagedLargeValueIdVec ids = { 0 };
        OpenTransaction__staged_large_value_ids(open_tx, &ids);
        for (u32 i = 0; i < ids.len; i++) JZ_VEC_PUSH(self->open_tx.released_large_values, ids.ptr[i]);
        jz_free(ids.ptr);
        open_tx_free(open_tx);
    }
}

/* Whether abandoned transactions left Groove roots awaiting eviction. */
bool jazz_node__node__open_tx__NodeState__has_released_large_values(const NS *self) { return self->open_tx.released_large_values.len != 0; }

/* Evict Groove roots staged by abandoned transactions.
 *
 * Call with earlier local publications settled: while one is resident, Groove
 * defers eviction. Eviction is idempotent, and a root that survives a failed
 * eviction is reclaimed by the staging TTL. */
void jazz_node__node__open_tx__NodeState__evict_released_large_values(NS *self) {
    jazz_node_StagedLargeValueIdVec released = { self->open_tx.released_large_values.ptr, self->open_tx.released_large_values.len,
                                                 self->open_tx.released_large_values.cap };
    self->open_tx.released_large_values.ptr = NULL;
    self->open_tx.released_large_values.len = self->open_tx.released_large_values.cap = 0;
    evict_superseded_large_values(self, released);
}

/* Return whether local transaction time advanced after this transaction opened. */
jz_err jazz_node__node__open_tx__NodeState__open_exclusive_snapshot_moved(const NS *self, OpenTransactionId tx_id, bool *out) {
    OPEN_TX(self, tx_id, t);
    *out = jazz_node__node__NodeState__tx_time_high_water(self) > t->base_snapshot.local_base;
    return JZ_OK;
}

/* ---------------- clock frontier ---------------- */

static bool gt_after(const void *k, void *v, void *ctx) { (void)v; return *(const jazz_types_GlobalTime *)k > *(const jazz_types_GlobalTime *)ctx; }

jz_err jazz_node__node__open_tx__NodeState__record_applied_global_time(NS *self, jazz_types_GlobalTime global_time, TxId tx_id,
                                                                      jazz_types_GlobalTime *newly, bool *has_newly) {
    *has_newly = false;
    if (global_time > self->clock.global_time_register) self->clock.global_time_register = global_time;
    if (global_time <= self->clock.committed_global_time) return JZ_OK;
    bool newly_applied = !jz_btree_insert(&self->clock.applied_global_times_after_frontier, &global_time, NULL, NULL);
    bool inserted = false;
    jazz_model_TxIdVec *dots = jz_btree_entry(&self->clock.frontier_dots, &global_time, &inserted);
    bool contains = false;
    for (u32 i = 0; i < dots->len && !contains; i++) contains = jazz_model_TxId_eq(&dots->ptr[i], &tx_id);
    if (!contains) JZ_VEC_PUSH(*dots, tx_id);
    bool locally_minted = jz_btree_remove(&self->clock.locally_minted_global_times, &global_time, NULL, NULL);
    if (self->history_complete || locally_minted) {
        self->clock.committed_global_time = global_time;
        jz_btree_retain(&self->clock.applied_global_times_after_frontier, gt_after, &global_time);
        jz_btree_retain(&self->clock.frontier_dots, gt_after, &global_time);
    }
    if (newly_applied) { *newly = global_time; *has_newly = true; }
    return JZ_OK;
}

/* Advance the highest authority-committed cut known to this node.
 *
 * On a history-complete node this is also a local completeness frontier. On a
 * partial node it is only a stable coordinate in the authority's history;
 * query-specific receipts still decide which rows are locally materialized.
 * Callers must admit only validated receipts from the currently selected
 * authority. */
void jazz_node__node__open_tx__NodeState__record_authoritative_settled_through(NS *self, jazz_types_GlobalTime settled_through) {
    if (settled_through == 0) return;
    if (settled_through > self->clock.global_time_register) self->clock.global_time_register = settled_through;
    if (settled_through <= self->clock.committed_global_time) return;
    self->clock.committed_global_time = settled_through;
    jz_btree_retain(&self->clock.applied_global_times_after_frontier, gt_after, &settled_through);
    jz_btree_retain(&self->clock.frontier_dots, gt_after, &settled_through);
}

jz_err jazz_node__node__open_tx__NodeState__open_transaction_snapshot(const NS *self, OpenTransactionId tx_id, jazz_model_Snapshot *out) {
    OPEN_TX(self, tx_id, t);
    jazz_model__tx__Snapshot__clone(out, &t->base_snapshot);
    return JZ_OK;
}

static const jz_str JAZZ_TRANSACTIONS = { (char *)"jazz_transactions", 17, 0 };

static jz_err transaction_ids_for_global_time(NS *self, jazz_types_GlobalTime global_time, jazz_model_TxIdVec *out) {
    groove_Value *inner = JZ_NEW(groove_Value);
    inner->tag = GROOVE_V_U64;
    inner->v.u = global_time;
    groove_Value key = { GROOVE_V_NULLABLE };
    key.v.nullable = inner;
    groove_RawRowVec raws = { 0 };
    jz_err e = groove__db__primary_storage__Database__index_scan_raw(jzn_db(self), &JAZZ_TRANSACTIONS, "by_global_time", &key, 1, &raws);
    groove__records__values__Value__drop(&key);
    for (u32 i = 0; !e.code && i < raws.len; i++) {
        const groove_OwnedRecord *record = groove__db__RawRow__record(raws.ptr[i]);
        u64 alias, time;
        e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_NODE_ID_IDX, &alias);
        if (e.code) break;
        jazz_types_NodeUuid node;
        if (!jazz_node__node__NodeState__node_for_alias(self, alias, &node)) { e = JZN_INVALID_STORED("transaction node alias must exist"); break; }
        e = groove__records__BorrowedRecord__get_u64(record, JZN_TXR_FIELD_TIME_IDX, &time);
        if (e.code) break;
        TxId tx_id = { time, node };
        JZ_VEC_PUSH(*out, tx_id);
    }
    for (u32 i = 0; i < raws.len; i++) groove__db__RawRow__drop(raws.ptr[i]);
    jz_free(raws.ptr);
    return e;
}

/* Pending transactions of other nodes that are present locally. Current reads
 * already include their versions, so a transaction opened now must read the
 * same visible set. A runtime reopened as a new node after an unclean close
 * (every browser foreground runtime) receives its previous node's unsynced
 * writes this way. Only transactions ingested by this runtime are tracked, so
 * opening a transaction never scans history; each is confirmed pending and
 * unsequenced by a point lookup, and dropped once it is sequenced, settled or
 * gone. */
static jz_err pending_foreign_transaction_ids(NS *self, jazz_model_TxIdVec *out) {
    jazz_model_TxIdVec tracked = { 0 };
    const TxId *id; void *u;
    JZ_BTREE_FOR(&self->open_tx.pending_foreign_transactions, id, u) { (void)u; JZ_VEC_PUSH(tracked, *id); }
    jz_err e = JZ_OK;
    for (u32 i = 0; i < tracked.len; i++) {
        bool found = false, has_gt = false;
        jazz_model_Fate fate = { 0 };
        jazz_types_GlobalTime gt; jazz_model_DurabilityTier dur;
        e = jazz_node__node__currency__NodeState__query_transaction_state(self, tracked.ptr[i], &found, &fate, &has_gt, &gt, &dur);
        if (e.code) break;
        if (found && fate.tag == JAZZ_FATE_PENDING && !has_gt) JZ_VEC_PUSH(*out, tracked.ptr[i]);
        else jz_btree_remove(&self->open_tx.pending_foreign_transactions, &tracked.ptr[i], NULL, NULL);
    }
    jz_free(tracked.ptr);
    return e;
}

/* ---------------- snapshot reads ---------------- */

bool jazz_node__node__open_tx__NodeState__snapshot_covers(NS *self, TxId tx_id, const jazz_model_Snapshot *snapshot) {
    /* Coverage needs only the transaction's existence and global time, so
     * read that projection instead of decoding the whole stored payload. */
    bool found = false, has_global_time = false;
    jazz_types_GlobalTime global_time = 0;
    jz_err e = jazz_node__node__currency__NodeState__query_transaction_global_time(self, tx_id, &found, &has_global_time, &global_time);
    if (e.code) { jz_err_drop(&e); return false; }
    if (!found) return false;
    if (has_global_time && global_time <= snapshot->global_base) return true;
    if (jz_uuid_eq(&tx_id.node, &snapshot->owner) && tx_id.time <= snapshot->local_base) return true;
    for (u32 i = 0; i < snapshot->dots.len; i++) if (jazz_model_TxId_eq(&snapshot->dots.ptr[i], &tx_id)) return true;
    return false;
}

jz_err jazz_node__node__open_tx__NodeState__snapshot_row_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                  jazz_types_RowUuid row_uuid, const jazz_model_Snapshot *snapshot, SnapshotRow *out) {
    VersionRow *content = jazz_node__node__open_tx__NodeState__snapshot_winner(self, schema_version, table, row_uuid, snapshot);
    VersionRow *deletion = jazz_node__node__open_tx__NodeState__snapshot_winner(self, schema_version, table, row_uuid, snapshot);
    return snapshot_row_from_winners(self, schema_version, table, content, deletion, out);
}

/* TxId -> bool */
static const jz_btree_desc desc_txid_bool = { sizeof(TxId), sizeof(bool), jzn_cmp_txid, NULL, NULL };

/* Derive every row's snapshot state from one scan of the table's history.
 *
 * Equivalent to calling snapshot_row_in_schema for each row with history: the
 * row-prefixed scans that call performs select exactly the row's slice of
 * this table-wide scan, and the winner is chosen by the same rule over the
 * same snapshot-covered candidates. Coverage is decided once per transaction
 * rather than once per version. */
static jz_err snapshot_rows_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const jazz_model_Snapshot *snapshot,
                                      jz_btree *out) {
    jz_btree_init(out, &desc_row_snapshot_row);
    jazz_node_VersionRowVec versions = { 0 };
    JZ_TRY(jazz_node__node__currency__NodeState__query_versions_in_schema(self, schema_version, table, NULL, &versions));
    jz_btree covered;
    jz_btree_init(&covered, &desc_txid_bool);
    jz_err e = JZ_OK;
    usize_vec candidate_indices = { 0 };
    u32 start = 0;
    while (start < versions.len && !e.code) {
        /* chunk_by row_uuid */
        jazz_types_RowUuid row_uuid = jazz_node__node__codec__VersionRow__row_uuid(versions.ptr[start]);
        u32 end = start + 1;
        while (end < versions.len) {
            jazz_types_RowUuid r = jazz_node__node__codec__VersionRow__row_uuid(versions.ptr[end]);
            if (!jz_uuid_eq(&r, &row_uuid)) break;
            end++;
        }
        /* One version per transaction carries both content and deletion, so
         * the covered winner answers both, as in the per-row read. */
        candidate_indices.len = 0;
        bool undecodable = false;
        for (u32 idx = start; idx < end; idx++) {
            /* A per-row read treats an undecodable identity as "no winner";
             * keep that outcome. */
            TxId tx_id;
            jz_err ie = jazz_node__node__NodeState__version_tx_id(self, versions.ptr[idx], &tx_id);
            if (ie.code) { jz_err_drop(&ie); undecodable = true; break; }
            const bool *known = jz_btree_get(&covered, &tx_id);
            bool is_covered;
            if (known) is_covered = *known;
            else {
                is_covered = jazz_node__node__open_tx__NodeState__snapshot_covers(self, tx_id, snapshot);
                jz_btree_insert(&covered, &tx_id, &is_covered, NULL);
            }
            if (is_covered) { usize local = idx - start; JZ_VEC_PUSH(candidate_indices, local); }
        }
        VersionRow *content = NULL, *deletion = NULL;
        usize winner;
        if (!undecodable && jazz_node__node__codec__current_version_index(&versions.ptr[start], candidate_indices.ptr, candidate_indices.len,
                                                                          &self->node_aliases, &winner)) {
            content = jazz_node__node__codec__VersionRow__clone(versions.ptr[start + winner]);
            deletion = jazz_node__node__codec__VersionRow__clone(versions.ptr[start + winner]);
        }
        SnapshotRow row;
        e = snapshot_row_from_winners(self, schema_version, table, content, deletion, &row);
        if (!e.code) {
            SnapshotRow old;
            if (jz_btree_insert(out, &row_uuid, &row, &old)) jazz_node__node__open_tx__SnapshotRow__drop(&old);
        }
        start = end;
    }
    jz_free(candidate_indices.ptr);
    jz_btree_drop(&covered);
    jz_vec_drop(&versions, sizeof(VersionRow *), jazz_node__node__codec__VersionRow__drop_v);
    if (e.code) jz_btree_drop(out);
    return e;
}

static bool version_tx_id_ok(NS *self, const VersionRow *v, TxId *out) {
    jz_err e = jazz_node__node__NodeState__version_tx_id(self, v, out);
    if (e.code) { jz_err_drop(&e); return false; }
    return true;
}

/* (consumes `content` and `deletion`) */
static jz_err snapshot_row_from_winners(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, VersionRow *content,
                                        VersionRow *deletion, SnapshotRow *out) {
    memset(out, 0, sizeof *out);
    jazz_model_DeletionEvent d;
    bool deleted = deletion && jazz_node__node__codec__VersionRow__deletion(deletion, &d) && d == JAZZ_DEL_DELETED;
    const jazz_model_TableSchema *target = NULL;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &target);
    if (!e.code && content) {
        jazz_types_SchemaVersionId source_schema;
        if (!jazz_node__node__NodeState__schema_version_for_alias(self, jazz_node__node__codec__VersionRow__schema_version_alias(content), &source_schema)) {
            e = JZN_INVALID_STORED("history schema version alias must exist");
        } else {
            const jazz_model_TableSchema *source_table = NULL;
            e = jazz_node__node__NodeState__table_in_schema_ref(self, jazz_node__node__codec__VersionRow__table(content), source_schema, &source_table);
            jz_btree cells;
            if (!e.code) e = jazz_node__node__NodeState__materialized_cells_for_version(self, source_table, content, &cells);
            if (!e.code) {
                bool some = false;
                jz_str projected_table = { 0 };
                e = jazz_node__node__source_resolution__NodeState__translate_cells(self, source_schema, schema_version,
                                                                                  jazz_node__node__codec__VersionRow__table(content), &cells, &some, &projected_table);
                if (!e.code && some && jz_str_eq(&projected_table, table)) {
                    e = jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &target);
                    if (!e.code) {
                        out->has_content_cells = true;
                        for (u32 i = 0; i < target->columns.len; i++) {
                            const groove_Value *v = jz_btree_get(&cells, &target->columns.ptr[i].name);
                            groove_Value *copy = NULL;
                            if (v) { copy = JZ_NEW(groove_Value); groove__records__values__Value__clone(copy, v); }
                            JZ_VEC_PUSH(out->content_cells, copy);
                        }
                    }
                }
                if (some) jz_str_drop(&projected_table);
                jz_btree_drop(&cells);
            }
        }
    }
    if (!e.code && content) {
        TxId content_tx;
        e = jazz_node__node__NodeState__version_tx_id(self, content, &content_tx);
        const VersionRow *updated = content;
        if (!e.code && deletion) {
            TxId deletion_tx;
            e = jazz_node__node__NodeState__version_tx_id(self, deletion, &deletion_tx);
            /* deletion.tx_time().sort_key(deletion_tx.node) > content.tx_time().sort_key(content_tx.node) */
            TxId dk = { jazz_node__node__codec__VersionRow__tx_time(deletion), deletion_tx.node };
            TxId ck = { jazz_node__node__codec__VersionRow__tx_time(content), content_tx.node };
            if (!e.code && jazz_model_TxId_cmp(&dk, &ck) > 0) updated = deletion;
        }
        if (!e.code) {
            out->provenance_created = jazz_node__node__codec__VersionRow__clone(content);
            out->provenance_updated = jazz_node__node__codec__VersionRow__clone(updated);
        }
    }
    if (!e.code) {
        out->has_content_version = content && version_tx_id_ok(self, content, &out->content_version);
        out->has_deletion_version = deletion && version_tx_id_ok(self, deletion, &out->deletion_version);
        if (deleted) out->has_read_version = deletion && version_tx_id_ok(self, deletion, &out->read_version);
        else out->has_read_version = content && version_tx_id_ok(self, content, &out->read_version);
        out->deleted = deleted;
    } else {
        jazz_node__node__open_tx__SnapshotRow__drop(out);
    }
    if (content) jazz_node__node__codec__VersionRow__drop(content);
    if (deletion) jazz_node__node__codec__VersionRow__drop(deletion);
    return e;
}

/* Returns an owned VersionRow*, or NULL for None. */
VersionRow *jazz_node__node__open_tx__NodeState__snapshot_winner(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                 jazz_types_RowUuid row_uuid, const jazz_model_Snapshot *snapshot) {
    /* Snapshot reads must be stable for the whole transaction lifetime.
     * Intervals can REOPEN when a late arrival shifts the DAG winner, so they
     * cannot serve snapshot reads; domination over the fixed member set
     * depends only on immutable payload and is stable by construction. */
    jazz_node_VersionRowVec versions = { 0 };
    jz_err e = jazz_node__node__currency__NodeState__query_versions_in_schema(self, schema_version, table, &row_uuid, &versions);
    if (e.code) { jz_err_drop(&e); return NULL; }
    usize_vec candidate_indices = { 0 };
    VersionRow *winner = NULL;
    bool ok = true;
    for (u32 idx = 0; idx < versions.len; idx++) {
        TxId tx_id;
        if (!version_tx_id_ok(self, versions.ptr[idx], &tx_id)) { ok = false; break; }
        if (jazz_node__node__open_tx__NodeState__snapshot_covers(self, tx_id, snapshot)) { usize i = idx; JZ_VEC_PUSH(candidate_indices, i); }
    }
    usize w;
    if (ok && jazz_node__node__codec__current_version_index(versions.ptr, candidate_indices.ptr, candidate_indices.len, &self->node_aliases, &w))
        winner = jazz_node__node__codec__VersionRow__clone(versions.ptr[w]);
    jz_free(candidate_indices.ptr);
    jz_vec_drop(&versions, sizeof(VersionRow *), jazz_node__node__codec__VersionRow__drop_v);
    return winner;
}

bool jazz_node__node__open_tx__NodeState__snapshot_content_witness(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                   jazz_types_RowUuid row_uuid, const jazz_model_Snapshot *snapshot, TxId *out) {
    VersionRow *version = jazz_node__node__open_tx__NodeState__snapshot_winner(self, schema_version, table, row_uuid, snapshot);
    if (!version) return false;
    bool ok = version_tx_id_ok(self, version, out);
    jazz_node__node__codec__VersionRow__drop(version);
    return ok;
}

/* ---------------- pending-write overlay ---------------- */

/* (consumes `snapshot_row`) */
static jz_err overlay_pending_cells_and_deletion_with_table(const NS *self, OpenTransactionId tx_id, const jazz_model_TableSchema *table_schema,
                                                            const jz_str *table, jazz_types_RowUuid row_uuid, SnapshotRow *snapshot_row,
                                                            bool *has_cells, jz_btree *cells, bool *deleted) {
    *has_cells = snapshot_row->has_content_cells;
    if (*has_cells)
        jazz_node__node__codec__cells_from_positional(table_schema, (const groove_Value *const *)snapshot_row->content_cells.ptr,
                                                      snapshot_row->content_cells.len, cells);
    *deleted = snapshot_row->deleted;
    jazz_node__node__open_tx__SnapshotRow__drop(snapshot_row);
    const OpenTransaction *t = open_tx_lookup(self, tx_id);
    if (!t) { if (*has_cells) jz_btree_drop(cells); *has_cells = false; return missing_open_batch(tx_id); }
    const usize_vec *positions = StagedWrites__row_positions(&t->writes, table, row_uuid);
    for (u32 i = 0; i < positions->len; i++) {
        const PendingWrite *write = StagedWrites__live_slot(&t->writes, positions->ptr[i]);
        if (write->cells.tag == JZN_PENDING_CELLS_REPLACE && write->cells.cells.len != 0) {
            if (*has_cells) jz_btree_drop(cells);
            cells_clone(cells, &write->cells.cells);
            *has_cells = true;
        } else if (write->cells.tag == JZN_PENDING_CELLS_PATCH) {
            if (!*has_cells) { jz_btree_init(cells, &jzn_desc_str_value); *has_cells = true; }
            cells_extend_cloned(cells, &write->cells.cells);
        }
        if (write->has_deletion) *deleted = write->deletion == JAZZ_DEL_DELETED;
    }
    return JZ_OK;
}

static jz_err overlay_pending_writes_with_table(const NS *self, OpenTransactionId tx_id, const jazz_model_TableSchema *table_schema, const jz_str *table,
                                                jazz_types_RowUuid row_uuid, SnapshotRow *snapshot_row, bool *some, jz_btree *out) {
    bool deleted = false;
    JZ_TRY(overlay_pending_cells_and_deletion_with_table(self, tx_id, table_schema, table, row_uuid, snapshot_row, some, out, &deleted));
    if (deleted && *some) { jz_btree_drop(out); *some = false; }
    return JZ_OK;
}

static jz_err overlay_pending_writes_in_schema(const NS *self, OpenTransactionId tx_id, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                               jazz_types_RowUuid row_uuid, SnapshotRow *snapshot_row, bool *some, jz_btree *out) {
    const jazz_model_TableSchema *table_schema = NULL;
    jz_err e = jazz_node__node__NodeState__table_in_schema_ref(self, table, schema_version, &table_schema);
    if (e.code) { jazz_node__node__open_tx__SnapshotRow__drop(snapshot_row); return e; }
    return overlay_pending_writes_with_table(self, tx_id, table_schema, table, row_uuid, snapshot_row, some, out);
}

/* exported for query_eval (pub(super) in Rust) */
jz_err jazz_node__node__open_tx__NodeState__overlay_pending_cells_and_deletion_with_table(const NS *self, OpenTransactionId tx_id,
                                                                                         const jazz_model_TableSchema *table_schema, const jz_str *table,
                                                                                         jazz_types_RowUuid row_uuid, SnapshotRow *snapshot_row,
                                                                                         bool *has_cells, jz_btree *cells, bool *deleted) {
    return overlay_pending_cells_and_deletion_with_table(self, tx_id, table_schema, table, row_uuid, snapshot_row, has_cells, cells, deleted);
}
