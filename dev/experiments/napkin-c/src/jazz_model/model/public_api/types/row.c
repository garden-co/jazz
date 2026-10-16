/* model/public_api/types/row.rs: materialized query results and row bytes. */
#include "jazz_model/jazz_model.h"

#define R(x) jazz_model__model__public_api__types__row__##x

void R(QueryResultField__drop)(jazz_model_QueryResultField *f) {
    jz_str_drop(&f->name);
    jazz_model__model__public_api__types__value__Value__drop(&f->value);
}

static void field_drop_v(void *f) { R(QueryResultField__drop)((jazz_model_QueryResultField *)f); }

void R(QueryResult__drop)(jazz_model_QueryResult *r) {
    jazz_types__object__OutputOccurrenceId__drop(&r->key.occ);
    jz_vec_drop(&r->fields, sizeof(jazz_model_QueryResultField), field_drop_v);
}

const jazz_model_public_Value *R(QueryResult__get)(const jazz_model_QueryResult *r, jz_slice name) {
    for (u32 i = 0; i < r->fields.len; i++)
        if (jazz_model__slice_eq(jz_str_slice(&r->fields.ptr[i].name), name)) return &r->fields.ptr[i].value;
    return NULL;
}

void R(QueryResult__into_values)(jazz_model_QueryResult *r, jazz_model_public_ValueVec *out) {
    memset(out, 0, sizeof *out);
    jz_vec_reserve(out, r->fields.len, sizeof(jazz_model_public_Value));
    for (u32 i = 0; i < r->fields.len; i++) {
        JZ_VEC_PUSH(*out, r->fields.ptr[i].value);
        jz_str_drop(&r->fields.ptr[i].name);
    }
    jz_vec_drop(&r->fields, sizeof(jazz_model_QueryResultField), NULL);
    jazz_types__object__OutputOccurrenceId__drop(&r->key.occ);
}

/* RowBytes(Arc<[u8]>) */
jazz_model_RowBytes R(RowBytes__from_vec)(jz_bytes v) {
    jazz_model_RowBytes b;
    b.len = v.len;
    b.arc = jz_rc_new(v.len ? v.len : 1, NULL);
    if (v.len) memcpy(b.arc, v.ptr, v.len);
    jz_bytes_drop(&v);
    return b;
}

void R(RowBytes__drop)(jazz_model_RowBytes *b) {
    if (b->arc) jz_rc_release(b->arc);
    b->arc = NULL;
    b->len = 0;
}

void R(RowBytes__Debug__fmt)(jz_str *out, const jazz_model_RowBytes *b) {
    jz_str_appendf(out, "RowBytes { len: %u }", b->len);
}

void R(RowBytes__Serialize__serialize)(jz_pc_w *w, const jazz_model_RowBytes *b) { jz_pc_bytes(w, b->arc, b->len); }

jz_err R(RowBytes__Deserialize__deserialize)(jz_pc_r *r, jazz_model_RowBytes *out) {
    jz_bytes buf;
    JZ_TRY(jz_pc_r_bytes(r, &buf));
    *out = R(RowBytes__from_vec)(buf);
    return JZ_OK;
}

/* ---------------------------------------------------------------- Row / RowDelta */
jazz_model_Row R(Row__new)(jazz_types_ResultKey id, jazz_model_RowBytes data, jazz_model_TransactionId tx, jazz_model_RowProvenance provenance) {
    jazz_model_Row r;
    r.id = id;
    r.data = data;
    r.transaction_id = tx;
    r.provenance = provenance;
    return r;
}

void R(Row__drop)(jazz_model_Row *r) {
    jazz_types__object__OutputOccurrenceId__drop(&r->id.occ);
    R(RowBytes__drop)(&r->data);
    jazz_model__model__metadata__RowProvenance__drop(&r->provenance);
}
static void row_drop_v(void *r) { R(Row__drop)((jazz_model_Row *)r); }

void R(Row__Clone__clone)(jazz_model_Row *dst, const jazz_model_Row *src) {
    jazz_types__object__OutputOccurrenceId__Clone__clone(&dst->id.occ, &src->id.occ);
    dst->data = src->data;
    if (dst->data.arc) jz_rc_clone(dst->data.arc);
    dst->transaction_id = src->transaction_id;
    jazz_model__model__metadata__RowProvenance__Clone__clone(&dst->provenance, &src->provenance);
}

bool R(Row__PartialEq__eq)(const jazz_model_Row *a, const jazz_model_Row *b) {
    return jazz_types__object__OutputOccurrenceId__PartialEq__eq(&a->id.occ, &b->id.occ) && a->data.len == b->data.len &&
           memcmp(a->data.arc, b->data.arc, a->data.len) == 0 &&
           memcmp(a->transaction_id.b, b->transaction_id.b, 16) == 0 &&
           jazz_model__model__metadata__RowProvenance__PartialEq__eq(&a->provenance, &b->provenance);
}

static void result_key_drop_v(void *k) { jazz_types__object__OutputOccurrenceId__drop(&((jazz_types_ResultKey *)k)->occ); }
static void row_pair_drop_v(void *p) {
    jazz_model_RowPair *pair = p;
    R(Row__drop)(&pair->old_row);
    R(Row__drop)(&pair->new_row);
}

void R(RowDelta__drop)(jazz_model_RowDelta *d) {
    jz_vec_drop(&d->added, sizeof(jazz_model_Row), row_drop_v);
    jz_vec_drop(&d->removed, sizeof(jazz_model_Row), row_drop_v);
    jz_vec_drop(&d->moved, sizeof(jazz_types_ResultKey), result_key_drop_v);
    jz_vec_drop(&d->updated, sizeof(jazz_model_RowPair), row_pair_drop_v);
}

bool R(RowDelta__is_empty)(const jazz_model_RowDelta *d) {
    return d->added.len == 0 && d->removed.len == 0 && d->moved.len == 0 && d->updated.len == 0;
}

static void ordered_added_drop_v(void *p) {
    jazz_model_OrderedAdded *a = p;
    jazz_types__object__OutputOccurrenceId__drop(&a->id.occ);
    R(Row__drop)(&a->row);
}
static void ordered_removed_drop_v(void *p) { jazz_types__object__OutputOccurrenceId__drop(&((jazz_model_OrderedRemoved *)p)->id.occ); }
static void ordered_updated_drop_v(void *p) {
    jazz_model_OrderedUpdated *u = p;
    jazz_types__object__OutputOccurrenceId__drop(&u->id.occ);
    if (u->has_row) R(Row__drop)(&u->row);
}

void R(OrderedRowDelta__drop)(jazz_model_OrderedRowDelta *d) {
    jz_vec_drop(&d->added, sizeof(jazz_model_OrderedAdded), ordered_added_drop_v);
    jz_vec_drop(&d->removed, sizeof(jazz_model_OrderedRemoved), ordered_removed_drop_v);
    jz_vec_drop(&d->updated, sizeof(jazz_model_OrderedUpdated), ordered_updated_drop_v);
}

bool R(OrderedRowDelta__is_empty)(const jazz_model_OrderedRowDelta *d) {
    return d->added.len == 0 && d->removed.len == 0 && d->updated.len == 0;
}
