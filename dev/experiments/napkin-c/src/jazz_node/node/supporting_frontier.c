/* Port of crates/jazz/layers/node/src/node/supporting_frontier.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef jazz_protocol_SupportingRow SRow;
typedef struct jazz_node_SupportingFrontier {
    jz_btree weights;          /* SupportingRow -> [i64; 4] */
    bool has_unpublished;
    jz_btree unpublished;      /* SupportingRow -> bool */
} jazz_node_SupportingFrontier;

static const jz_btree_desc desc_weights = { sizeof(SRow), sizeof(i64) * 4, jazz_protocol__protocol__SupportingRow__cmp, jazz_protocol__protocol__SupportingRow__drop, NULL };
static const jz_btree_desc desc_unpublished = { sizeof(SRow), sizeof(bool), jazz_protocol__protocol__SupportingRow__cmp, jazz_protocol__protocol__SupportingRow__drop, NULL };

void jazz_node__node__supporting_frontier__SupportingFrontier__default(jazz_node_SupportingFrontier *f) {
    jz_btree_init(&f->weights, &desc_weights);
    f->has_unpublished = false;
}

static bool any_positive(const i64 *w) { for (int i = 0; i < 4; i++) if (w[i] > 0) return true; return false; }

bool jazz_node__node__supporting_frontier__SupportingFrontier__contains(const jazz_node_SupportingFrontier *f, const SRow *row) {
    const i64 *w = jz_btree_get(&f->weights, row);
    return w && any_positive(w);
}

/* returns 0 = None, 1 = Some(false), 2 = Some(true) */
u32 jazz_node__node__supporting_frontier__SupportingFrontier__apply(jazz_node_SupportingFrontier *f, usize origin, SRow row, i64 weight) {
    bool before = jazz_node__node__supporting_frontier__SupportingFrontier__contains(f, &row);
    if (f->has_unpublished) {
        SRow key; jazz_protocol__protocol__SupportingRow__clone(&key, &row);
        bool inserted;
        bool *slot = jz_btree_entry(&f->unpublished, &key, &inserted);
        if (inserted) *slot = before; else jazz_protocol__protocol__SupportingRow__drop(&key);
    }
    SRow key; jazz_protocol__protocol__SupportingRow__clone(&key, &row);
    bool inserted;
    i64 *weights = jz_btree_entry(&f->weights, &key, &inserted);
    if (inserted) memset(weights, 0, sizeof(i64) * 4); else jazz_protocol__protocol__SupportingRow__drop(&key);
    weights[origin] += weight;
    bool after = any_positive(weights);
    bool all_zero = true;
    for (int i = 0; i < 4; i++) if (weights[i] != 0) all_zero = false;
    if (all_zero) {
        SRow ok; i64 ov[4];
        if (jz_btree_remove(&f->weights, &row, &ok, ov)) jazz_protocol__protocol__SupportingRow__drop(&ok);
    }
    jazz_protocol__protocol__SupportingRow__drop(&row);
    if (before != after) return after ? 2 : 1;
    return 0;
}

typedef JZ_VEC(const SRow *) srow_ref_vec;
void jazz_node__node__supporting_frontier__SupportingFrontier__rows(const jazz_node_SupportingFrontier *f, srow_ref_vec *out) {
    SRow *k; i64 *w;
    JZ_BTREE_FOR(&f->weights, k, w) if (any_positive(w)) JZ_VEC_PUSH(*out, (const SRow *)k);
}

void jazz_node__node__supporting_frontier__SupportingFrontier__acknowledged_rows(const jazz_node_SupportingFrontier *f, srow_ref_vec *out) {
    srow_ref_vec rows = {0};
    jazz_node__node__supporting_frontier__SupportingFrontier__rows(f, &rows);
    for (u32 i = 0; i < rows.len; i++) {
        const bool *b = f->has_unpublished ? jz_btree_get(&f->unpublished, rows.ptr[i]) : NULL;
        if (!(b && *b == false)) JZ_VEC_PUSH(*out, rows.ptr[i]);
    }
    jz_free(rows.ptr);
    if (f->has_unpublished) {
        SRow *k; bool *before;
        JZ_BTREE_FOR(&f->unpublished, k, before)
            if (*before && !jazz_node__node__supporting_frontier__SupportingFrontier__contains(f, k)) JZ_VEC_PUSH(*out, (const SRow *)k);
    }
}

typedef JZ_VEC(SRow) srow_vec;
bool jazz_node__node__supporting_frontier__SupportingFrontier__delta(const jazz_node_SupportingFrontier *f, srow_vec *adds, srow_vec *removes) {
    if (!f->has_unpublished) return false;
    SRow *row; bool *before;
    JZ_BTREE_FOR(&f->unpublished, row, before) {
        bool now = jazz_node__node__supporting_frontier__SupportingFrontier__contains(f, row);
        if (!*before && now) { SRow c; jazz_protocol__protocol__SupportingRow__clone(&c, row); JZ_VEC_PUSH(*adds, c); }
        else if (*before && !now) { SRow c; jazz_protocol__protocol__SupportingRow__clone(&c, row); JZ_VEC_PUSH(*removes, c); }
    }
    return true;
}

void jazz_node__node__supporting_frontier__SupportingFrontier__acknowledge(jazz_node_SupportingFrontier *f) {
    if (!f->has_unpublished) { jz_btree_init(&f->unpublished, &desc_unpublished); f->has_unpublished = true; }
    jz_btree_clear(&f->unpublished);
}

void jazz_node__node__supporting_frontier__SupportingFrontier__forget_predecessor(jazz_node_SupportingFrontier *f) {
    if (f->has_unpublished) jz_btree_drop(&f->unpublished);
    f->has_unpublished = false;
}

static usize row_bytes(const SRow *row) {
    return sizeof(SRow) + (row->version.has_branch_or_prefix ? row->version.branch_or_prefix.cap : 0) +
           (row->version.has_row_digest ? row->version.row_digest.cap : 0);
}

usize jazz_node__node__supporting_frontier__SupportingFrontier__retained_bytes(const jazz_node_SupportingFrontier *f) {
    usize total = 0;
    SRow *k; void *v;
    JZ_BTREE_FOR(&f->weights, k, v) total += row_bytes(k) + 4 * sizeof(i64);
    if (f->has_unpublished) {
        SRow *k2; void *v2;
        JZ_BTREE_FOR(&f->unpublished, k2, v2) total += row_bytes(k2) + 1;
    }
    return total;
}

void jazz_node__node__supporting_frontier__SupportingFrontier__clone(jazz_node_SupportingFrontier *dst, const jazz_node_SupportingFrontier *src) {
    jz_btree_clone(&dst->weights, &src->weights, jazz_protocol__protocol__SupportingRow__clone, NULL);
    dst->has_unpublished = src->has_unpublished;
    if (src->has_unpublished) jz_btree_clone(&dst->unpublished, &src->unpublished, jazz_protocol__protocol__SupportingRow__clone, NULL);
}

void jazz_node__node__supporting_frontier__SupportingFrontier__drop(jazz_node_SupportingFrontier *f) {
    jz_btree_drop(&f->weights);
    if (f->has_unpublished) jz_btree_drop(&f->unpublished);
}
