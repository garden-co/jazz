/* groove::ivm::runtime::windows: arg-by, TopBy, and structured CollectBy
 * window maintenance. */
#include "mod.h"

#define TRY_GOTO(expr) do { e = (expr); if (JZ_IS_ERR(e)) goto fail; } while (0)
#define U32S(v) ((const usize *)(v).ptr)

/* ---------- TopBySortKey / TopBySortPart ---------- */
static int top_by_sort_key_cmp(const groove_TopBySortPart *a, const groove_TopBySortPart *b) {
    if (a->key_tag != b->key_tag) return a->key_tag < b->key_tag ? -1 : 1;
    switch (a->key_tag) {
    case groove_TBK_Null: return 0;
    case groove_TBK_I32: case groove_TBK_I64: return a->key.i < b->key.i ? -1 : a->key.i > b->key.i;
    case groove_TBK_Bool: return (int)a->key.b - (int)b->key.b;
    case groove_TBK_String: return jz_str_cmp(&a->key.s, &b->key.s);
    case groove_TBK_Bytes: return jz_bytes_cmp(&a->key.bytes, &b->key.bytes);
    case groove_TBK_Uuid: { int c = memcmp(a->key.uuid, b->key.uuid, 16); return c < 0 ? -1 : c > 0; }
    default: return a->key.u < b->key.u ? -1 : a->key.u > b->key.u;
    }
}

int groove__ivm__runtime__windows__TopBySortPart__Ord__cmp(const groove_TopBySortPart *a, const groove_TopBySortPart *b) {
    int ordering = top_by_sort_key_cmp(a, b);
    return a->direction == groove_TopByDirection_Asc ? ordering : -ordering;
}

void groove__ivm__runtime__windows__TopBySortPart__clone(groove_TopBySortPart *d, const groove_TopBySortPart *s) {
    *d = *s;
    if (s->key_tag == groove_TBK_String) d->key.s = jz_str_clone(&s->key.s);
    else if (s->key_tag == groove_TBK_Bytes) d->key.bytes = jz_bytes_clone(&s->key.bytes);
}

void groove__ivm__runtime__windows__TopBySortPart__drop(groove_TopBySortPart *p) {
    if (p->key_tag == groove_TBK_String) jz_str_drop(&p->key.s);
    else if (p->key_tag == groove_TBK_Bytes) jz_bytes_drop(&p->key.bytes);
}

static void part_drop_v(void *p) { groove__ivm__runtime__windows__TopBySortPart__drop(p); }
static void part_clone_v(void *d, const void *s) { groove__ivm__runtime__windows__TopBySortPart__clone(d, s); }
static void parts_drop(groove_TopBySortPartVec *v) { jz_vec_drop(v, sizeof(groove_TopBySortPart), part_drop_v); }
static int parts_cmp(const groove_TopBySortPartVec *a, const groove_TopBySortPartVec *b) {
    u32 n = a->len < b->len ? a->len : b->len;
    for (u32 i = 0; i < n; i++) {
        int c = groove__ivm__runtime__windows__TopBySortPart__Ord__cmp(&a->ptr[i], &b->ptr[i]);
        if (c) return c;
    }
    return a->len < b->len ? -1 : a->len > b->len;
}
static bool parts_eq(const groove_TopBySortPartVec *a, const groove_TopBySortPartVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (a->ptr[i].direction != b->ptr[i].direction || top_by_sort_key_cmp(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void order_key_drop(groove_CollectByOrderKey *k) { groove__ivm__runtime__evaluator__CollectByOrderKey__drop(k); }
static groove_CollectByOrderKey order_key_clone(const groove_CollectByOrderKey *k) {
    groove_CollectByOrderKey c; groove__ivm__runtime__evaluator__CollectByOrderKey__clone(&c, k); return c;
}

/* ---------- arg_by ---------- */
bool groove__ivm__runtime__windows__arg_by_candidate_replaces(jz_slice candidate_key, jz_slice candidate_record, jz_slice winner_key, jz_slice winner_record, u32 direction) {
    int c = jz_slice_cmp(candidate_key, winner_key);
    if (c < 0) return direction == GROOVE_ARGBY_MIN;
    if (c > 0) return direction == GROOVE_ARGBY_MAX;
    return jz_slice_cmp(candidate_record, winner_record) < 0;
}

/* Only the declared key reverses for Max; the full record tie-breaker
 * remains ascending for both directions. */
jz_err groove__ivm__runtime__windows__arg_by_order_key(groove_RecordDescriptor d, const groove_RecordDelta *delta, const groove_ArgBySpec *spec, groove_CollectByOrderKey *out) {
    groove_TopBySortPart part; memset(&part, 0, sizeof part);
    part.key_tag = groove_TBK_Bytes;
    JZ_TRY(groove__ivm__runtime__windows__encoded_record_key_part(d, delta->record.ptr, delta->record.len, spec->comparison_field_indices, spec->n_comparison, &part.key.bytes));
    part.direction = spec->direction == GROOVE_ARGBY_MIN ? groove_TopByDirection_Asc : groove_TopByDirection_Desc;
    out->parts.ptr = NULL; out->parts.len = 0; out->parts.cap = 0;
    JZ_VEC_PUSH(out->parts, part);
    out->record = jz_bytes_clone(&delta->record);
    return JZ_OK;
}

bool groove__ivm__runtime__windows__arg_by_ordered_winner(const groove_CollectByGroup *g, jz_bytes *out) {
    groove_CollectByGroupIter it; const groove_CollectByOrderKey *key; const i64 *weight;
    groove__ivm__runtime__evaluator__CollectByGroup__iter(g, &it);
    while (groove__ivm__runtime__evaluator__CollectByGroupIter__Iterator__next(&it, &key, &weight)) {
        if (*weight > 0) { *out = jz_bytes_clone(&key->record); return true; }
    }
    return false;
}

/* ---------- window selection (bag semantics, INV-QUERY-24/25) ---------- */
typedef struct WindowCursor { u64 to_skip; bool bounded; u64 remaining; } WindowCursor;
static WindowCursor window_cursor(u64 offset, groove_TopByLimit limit) { WindowCursor c = { offset, limit.finite, limit.n }; return c; }
static bool window_done(const WindowCursor *c) { return c->bounded && c->remaining == 0; }
static u64 window_take(WindowCursor *c, u64 copies) {
    u64 available = copies > c->to_skip ? copies - c->to_skip : 0;
    c->to_skip = c->to_skip > copies ? c->to_skip - copies : 0;
    u64 taken = c->bounded ? (available < c->remaining ? available : c->remaining) : available;
    if (taken > 0 && c->bounded) c->remaining -= taken;
    if (taken > (u64)INT64_MAX) jz_panic("window copies cannot exceed positive input weight");
    return taken;
}

void groove__ivm__runtime__windows__top_by_window_from_ordered_group(const groove_CollectByGroup *g, const groove_TopByOp *top_by, groove_WindowedRecordVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    if (!g) return;
    WindowCursor c = window_cursor(top_by->offset, top_by->limit);
    groove_CollectByGroupIter it; const groove_CollectByOrderKey *key; const i64 *weight;
    groove__ivm__runtime__evaluator__CollectByGroup__iter(g, &it);
    while (groove__ivm__runtime__evaluator__CollectByGroupIter__Iterator__next(&it, &key, &weight)) {
        if (window_done(&c)) break;
        if (*weight <= 0) continue;
        u64 taken = window_take(&c, (u64)*weight);
        if (taken > 0) { groove_WindowedRecord w = { jz_bytes_clone(&key->record), (i64)taken }; JZ_VEC_PUSH(*out, w); }
    }
}

static const jz_btree_desc BYTES_I64 = { sizeof(jz_bytes), sizeof(i64), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, NULL };

static void add_weight(jz_btree *m, const jz_bytes *record, i64 w, bool clone) {
    i64 *slot = jz_btree_get(m, record);
    if (slot) { *slot += w; return; }
    jz_bytes k = clone ? jz_bytes_clone(record) : *record;
    jz_btree_insert(m, &k, &w, NULL);
}

void groove__ivm__runtime__windows__records_before_deltas(groove_RecordDeltaVec after_records, const groove_RecordDelta *deltas, u32 n, groove_RecordDeltaVec *out) {
    jz_btree records; jz_btree_init(&records, &BYTES_I64);
    JZ_VEC_FOR(groove_RecordDelta, r, after_records) {
        i64 *slot = jz_btree_get(&records, &r->record);
        if (slot) { *slot += r->weight; jz_bytes_drop(&r->record); }
        else jz_btree_insert(&records, &r->record, &r->weight, NULL);
    }
    jz_vec_drop(&after_records, sizeof(groove_RecordDelta), NULL);
    for (u32 i = 0; i < n; i++) add_weight(&records, &deltas[i].record, -deltas[i].weight, true);
    out->ptr = NULL; out->len = 0; out->cap = 0;
    groove_RecordDelta d;
    while (jz_btree_pop_first(&records, &d.record, &d.weight)) JZ_VEC_PUSH(*out, d);
    jz_btree_drop(&records);
}

/* ---------- CollectBy projections ---------- */
static jz_err collect_by_projection_value_type(groove_RecordDescriptor input_desc, const groove_CollectByProjection *field, groove_ValueType *out) {
    const groove_DescriptorField *f = groove__records__RecordDescriptor__field(input_desc, field->field_idx);
    if (!f) return groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, field->field_idx);
    if (field->unwrap_nullable && f->value_type.tag == groove_ValueType_Nullable) groove_ValueType_clone(out, f->value_type.v.inner);
    else groove_ValueType_clone(out, &f->value_type);
    return JZ_OK;
}

static groove_Value *box_value(groove_Value v) { groove_Value *b = jz_alloc(sizeof *b); *b = v; return b; }

void groove__ivm__runtime__windows__collect_by_output_value(const groove_ValueType *output_type, groove_Value value, groove_Value *out) {
    if (output_type->tag == groove_ValueType_Nullable && value.tag != groove_Value_Nullable) {
        out->tag = groove_Value_Nullable;
        out->v.nullable = box_value(value);
        return;
    }
    *out = value;
}

static jz_err collect_by_projected_value(const groove_BorrowedRecord *record, const groove_CollectByProjection *field, groove_Value *out) {
    if (field->field_idx >= groove__records__RecordDescriptor__len(record->descriptor))
        return groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, field->field_idx);
    groove_Value value;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, field->field_idx, &value));
    if (!field->unwrap_nullable) { *out = value; return JZ_OK; }
    if (value.tag == groove_Value_Nullable && value.v.nullable) {
        groove_Value *inner = value.v.nullable;
        *out = *inner;
        jz_free(inner);
        return JZ_OK;
    }
    /* A present child can legitimately carry an application NULL. */
    *out = value;
    return JZ_OK;
}

/* values[i] = collect_by_output_value(desc.fields[i].type, projected(field[i])) */
static jz_err project_fields(const groove_BorrowedRecord *src, const groove_CollectByProjection *fields, u32 n, groove_RecordDescriptor out_desc, groove_ValueVec *values) {
    values->ptr = NULL; values->len = 0; values->cap = 0;
    jz_vec_reserve(values, n + 1, sizeof(groove_Value));
    for (u32 i = 0; i < n; i++) {
        groove_Value v;
        jz_err e = collect_by_projected_value(src, &fields[i], &v);
        if (JZ_IS_ERR(e)) { jz_vec_drop(values, sizeof(groove_Value), groove_Value_drop_v); return e; }
        groove_Value o;
        groove__ivm__runtime__windows__collect_by_output_value(groove__records__RecordDescriptor__field_type(out_desc, i), v, &o);
        JZ_VEC_PUSH(*values, o);
    }
    return JZ_OK;
}

static void values_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop_v); }

/* ---------- sort keys ---------- */
static jz_err top_by_sort_value(const groove_ValueType *value_type, groove_Value value, groove_TopBySortPart *out) {
    if (!groove__records__ValueType__collect_by_ordered_scalar(value_type)) {
        groove_Value_drop(&value);
        return GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidTopBy, "sort field value must be an orderable scalar");
    }
    if (groove__ivm__runtime__key_encoding__is_sql_null_value(&value)) {
        groove_Value_drop(&value);
        out->key_tag = groove_TBK_Null;
        return JZ_OK;
    }
    const groove_ValueType *t = groove__records__ValueType__non_nullable(value_type);
    while (value.tag == groove_Value_Nullable && value.v.nullable) {
        groove_Value *inner = value.v.nullable;
        value = *inner;
        jz_free(inner);
    }
    bool ok = true;
    switch (t->tag) {
    case groove_ValueType_U8: ok = value.tag == groove_Value_U8; out->key_tag = groove_TBK_U8; out->key.u = value.v.u8v; break;
    case groove_ValueType_U16: ok = value.tag == groove_Value_U16; out->key_tag = groove_TBK_U16; out->key.u = value.v.u16v; break;
    case groove_ValueType_U32: ok = value.tag == groove_Value_U32; out->key_tag = groove_TBK_U32; out->key.u = value.v.u32v; break;
    case groove_ValueType_U64: ok = value.tag == groove_Value_U64; out->key_tag = groove_TBK_U64; out->key.u = value.v.u64v; break;
    case groove_ValueType_U48: ok = value.tag == groove_Value_U48; out->key_tag = groove_TBK_U48; out->key.u = value.v.u64v; break;
    case groove_ValueType_I32: ok = value.tag == groove_Value_I32; out->key_tag = groove_TBK_I32; out->key.i = value.v.i32v; break;
    case groove_ValueType_I64: ok = value.tag == groove_Value_I64; out->key_tag = groove_TBK_I64; out->key.i = value.v.i64v; break;
    case groove_ValueType_F64: {
        ok = value.tag == groove_Value_F64 && value.v.f64v == value.v.f64v;
        if (!ok) break;
        f64 f = value.v.f64v == 0.0 ? 0.0 : value.v.f64v;
        u64 bits; memcpy(&bits, &f, 8);
        out->key_tag = groove_TBK_F64;
        out->key.u = (bits & (1ull << 63)) == 0 ? bits ^ (1ull << 63) : ~bits;
        break;
    }
    case groove_ValueType_Bool: ok = value.tag == groove_Value_Bool; out->key_tag = groove_TBK_Bool; out->key.b = value.v.b; break;
    case groove_ValueType_String:
        ok = value.tag == groove_Value_String;
        if (ok) { out->key_tag = groove_TBK_String; out->key.s = value.v.s; return JZ_OK; }
        break;
    case groove_ValueType_Bytes:
        ok = value.tag == groove_Value_Bytes;
        if (ok) { out->key_tag = groove_TBK_Bytes; out->key.bytes = value.v.bytes; return JZ_OK; }
        break;
    case groove_ValueType_Uuid: ok = value.tag == groove_Value_Uuid; out->key_tag = groove_TBK_Uuid; memcpy(out->key.uuid, value.v.uuid.b, 16); break;
    case groove_ValueType_EnumTag: ok = value.tag == groove_Value_EnumTag; out->key_tag = groove_TBK_EnumTag; out->key.u = value.v.u8v; break;
    default: ok = false;
    }
    if (!ok) {
        groove_Value_drop(&value);
        out->key_tag = groove_TBK_Null;
        return GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidTopBy, "sort field value must match its orderable scalar type");
    }
    return JZ_OK;
}

static jz_err collect_by_sort_key_for_fields(groove_RecordDescriptor d, jz_slice record, const u32 *idx, u32 nidx, const u8 *dirs, u32 ndirs, groove_TopBySortPartVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    groove_BorrowedRecord input = { record, d };
    u32 n = nidx < ndirs ? nidx : ndirs;
    for (u32 i = 0; i < n; i++) {
        const groove_DescriptorField *f = groove__records__RecordDescriptor__field(d, idx[i]);
        jz_err e;
        if (!f) { e = groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, idx[i]); parts_drop(out); return e; }
        groove_Value value;
        e = groove__records__BorrowedRecord__get_idx(&input, idx[i], &value);
        if (JZ_IS_ERR(e)) { parts_drop(out); return e; }
        groove_TopBySortPart part; memset(&part, 0, sizeof part);
        e = top_by_sort_value(&f->value_type, value, &part);
        if (JZ_IS_ERR(e)) { parts_drop(out); return e; }
        part.direction = dirs[i];
        JZ_VEC_PUSH(*out, part);
    }
    return JZ_OK;
}

static jz_err collect_by_sort_key(groove_RecordDescriptor d, jz_slice record, const groove_CollectByOp *cb, groove_TopBySortPartVec *out) {
    return collect_by_sort_key_for_fields(d, record, cb->sort_field_indices.ptr, cb->sort_field_indices.len, cb->sort_directions.ptr, cb->sort_directions.len, out);
}

jz_err groove__ivm__runtime__windows__top_by_sort_key(groove_RecordDescriptor d, const u8 *raw, u32 len, const groove_TopByOp *top_by, groove_TopBySortPartVec *out) {
    return collect_by_sort_key_for_fields(d, jz_slice_of(raw, len), top_by->sort_field_indices.ptr, top_by->sort_field_indices.len,
                                          top_by->sort_directions.ptr, top_by->sort_directions.len, out);
}

/* ---------- canonical terminal deltas ---------- */
typedef struct KeyedEntry { jz_bytes group_key; groove_TopBySortPartVec sort_key; jz_bytes record; i64 weight; bool phase; } KeyedEntry;
typedef JZ_VEC(KeyedEntry) KeyedEntryVec;
static void keyed_entry_drop(void *p) { KeyedEntry *k = p; jz_bytes_drop(&k->group_key); parts_drop(&k->sort_key); jz_bytes_drop(&k->record); }
static int triple_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const KeyedEntry *a = pa, *b = pb;
    int c = jz_bytes_cmp(&a->group_key, &b->group_key); if (c) return c;
    c = parts_cmp(&a->sort_key, &b->sort_key); if (c) return c;
    return jz_bytes_cmp(&a->record, &b->record);
}
static int canonical_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const KeyedEntry *a = pa, *b = pb;
    int c = jz_bytes_cmp(&a->group_key, &b->group_key); if (c) return c;
    c = (int)(a->weight > 0) - (int)(b->weight > 0); if (c) return c;
    return parts_cmp(&a->sort_key, &b->sort_key);
}
static int phase_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const KeyedEntry *a = pa, *b = pb;
    int c = jz_bytes_cmp(&a->group_key, &b->group_key); if (c) return c;
    c = (int)(a->weight > 0) - (int)(b->weight > 0); if (c) return c;
    return (int)a->phase - (int)b->phase;
}

/* BTreeMap<(group, sort, record), i64> consolidation, then canonical order:
 * net removals first, then net additions, by sort key within each group. */
void groove__ivm__runtime__windows__canonicalize_collect_by_terminal_weights(KeyedEntryVec *keyed) {
    jz_sort(keyed->ptr, keyed->len, sizeof(KeyedEntry), triple_cmp, NULL);
    u32 w = 0;
    for (u32 i = 0; i < keyed->len;) {
        u32 j = i + 1;
        i64 total = keyed->ptr[i].weight;
        while (j < keyed->len && triple_cmp(&keyed->ptr[i], &keyed->ptr[j], NULL) == 0) { total += keyed->ptr[j].weight; keyed_entry_drop(&keyed->ptr[j]); j++; }
        if (total != 0) { keyed->ptr[w] = keyed->ptr[i]; keyed->ptr[w].weight = total; w++; }
        else keyed_entry_drop(&keyed->ptr[i]);
        i = j;
    }
    keyed->len = w;
    jz_sort(keyed->ptr, keyed->len, sizeof(KeyedEntry), canonical_cmp, NULL);
}

static jz_err canonical_collect_by_terminal_deltas(groove_RecordDescriptor input_desc, const groove_CollectByOp *cb, const groove_CollectBySlot *slot,
                                                   const groove_RecordDelta *deltas, u32 n, groove_RecordDeltaVec *out) {
    const u32 *sidx = slot ? slot->sort_field_indices.ptr : cb->sort_field_indices.ptr;
    u32 nsidx = slot ? slot->sort_field_indices.len : cb->sort_field_indices.len;
    const u8 *sdir = slot ? slot->sort_directions.ptr : cb->sort_directions.ptr;
    u32 nsdir = slot ? slot->sort_directions.len : cb->sort_directions.len;
    KeyedEntryVec keyed = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < n; i++) {
        KeyedEntry k; memset(&k, 0, sizeof k);
        TRY_GOTO(groove__ivm__runtime__windows__encoded_record_key_part(input_desc, deltas[i].record.ptr, deltas[i].record.len, U32S(cb->group_field_indices), cb->group_field_indices.len, &k.group_key));
        e = collect_by_sort_key_for_fields(input_desc, jz_bytes_slice(&deltas[i].record), sidx, nsidx, sdir, nsdir, &k.sort_key);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&k.group_key); goto fail; }
        k.record = jz_bytes_clone(&deltas[i].record);
        k.weight = deltas[i].weight;
        JZ_VEC_PUSH(keyed, k);
    }
    groove__ivm__runtime__windows__canonicalize_collect_by_terminal_weights(&keyed);
    if (slot && slot->has_presence) {
        /* Retire children before their parent, then create the new parent
         * before its children. Keep the canonical child sort within each phase. */
        JZ_VEC_FOR(KeyedEntry, k, keyed) {
            groove_BorrowedRecord r = { jz_bytes_slice(&k->record), input_desc };
            bool is_child;
            TRY_GOTO(groove__records__BorrowedRecord__get_bool(&r, slot->presence_field_index, &is_child));
            k->phase = k->weight > 0 ? is_child : !is_child;
        }
        /* Stable sorting preserves child order and exact-record tie breaking. */
        jz_sort(keyed.ptr, keyed.len, sizeof(KeyedEntry), phase_cmp, NULL);
    }
    out->ptr = NULL; out->len = 0; out->cap = 0;
    JZ_VEC_FOR(KeyedEntry, k, keyed) {
        groove_RecordDelta d = { k->record, k->weight };
        JZ_VEC_PUSH(*out, d);
        jz_bytes_drop(&k->group_key);
        parts_drop(&k->sort_key);
    }
    jz_vec_drop(&keyed, sizeof(KeyedEntry), NULL);
    return JZ_OK;
fail:
    jz_vec_drop(&keyed, sizeof(KeyedEntry), keyed_entry_drop);
    return e;
}

/* ---------- terminal operations ---------- */
static void push_op(groove_TerminalOperationVec *ops, groove_RecordDescriptor root_descriptor, jz_bytes root_key, groove_TerminalPath path,
                    u8 tag, u32 index, jz_bytes key, jz_bytes value) {
    groove_TerminalOperation op; memset(&op, 0, sizeof op);
    op.root_descriptor = root_descriptor;
    op.root_key = root_key;
    op.path = path;
    op.edit.tag = tag; op.edit.index = index; op.edit.key = key; op.edit.value = value;
    JZ_VEC_PUSH(*ops, op);
}
static void ops_drop(groove_TerminalOperationVec *ops) { jz_vec_drop(ops, sizeof(groove_TerminalOperation), groove_TerminalOperation_drop); }
static jz_bytes empty_bytes(void) { jz_bytes b = {0}; return b; }
static groove_TerminalPath empty_path(void) { groove_TerminalPath p = {0}; return p; }

/* group.iter() mapped to (record, weight) */
static void group_records(const groove_CollectByGroup *g, groove_RecordWeightVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    groove_CollectByGroupIter it; const groove_CollectByOrderKey *key; const i64 *weight;
    groove__ivm__runtime__evaluator__CollectByGroup__iter(g, &it);
    while (groove__ivm__runtime__evaluator__CollectByGroupIter__Iterator__next(&it, &key, &weight)) {
        groove_RecordWeight rw = { jz_bytes_clone(&key->record), *weight };
        JZ_VEC_PUSH(*out, rw);
    }
}
static void rw_drop_v(void *p) { jz_bytes_drop(&((groove_RecordWeight *)p)->record); }
static void rw_vec_drop(groove_RecordWeightVec *v) { jz_vec_drop(v, sizeof(groove_RecordWeight), rw_drop_v); }

static const jz_btree_desc BYTES_SET = { sizeof(jz_bytes), 0, jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, NULL };

static jz_err update_collect_by_root_terminal_state(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
    groove_CollectByIncrementalState *state, const groove_RecordDelta *deltas, u32 n, bool emit, groove_TerminalOperationVec *out);

jz_err groove__ivm__runtime__windows__update_unbounded_collect_by_terminal_state(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc,
    const groove_CollectByOp *cb, const groove_CollectBySlot *slot, groove_CollectByIncrementalState *st, const groove_RecordDelta *in_deltas, u32 nin,
    bool emit, groove_TerminalOperationVec *out) {
    if (cb->mode == groove_CollectByMode_Root)
        return update_collect_by_root_terminal_state(input_desc, output_desc, cb, st, in_deltas, nin, emit, out);
    groove_CollectByIncrementalPayload *state = groove__ivm__runtime__evaluator__CollectByIncrementalState__deref_mut(st);
    if (!emit) {
        groove__ivm__runtime__evaluator__SparseGroups__clear(&state->groups);
        jz_btree_clear(&state->roots);
    }
    /* Consolidate exact records and canonicalize every surviving transition:
     * net removals first, then net additions. */
    groove_RecordDeltaVec deltas;
    JZ_TRY(canonical_collect_by_terminal_deltas(input_desc, cb, slot, in_deltas, nin, &deltas));
    jz_err e = JZ_OK;
    groove_TerminalOperationVec operations = {0};
    jz_btree touched_group_keys; jz_btree_init(&touched_group_keys, &BYTES_SET);
    jz_btree root_groups_before; jz_btree_init(&root_groups_before, &BYTES_SET);
    jz_btree root_groups_after; jz_btree_init(&root_groups_after, &BYTES_SET);
    bool track_roots = slot == NULL;
    /* Only a group named by this batch can enter or leave the terminal. */
    JZ_VEC_FOR(groove_RecordDelta, d, deltas) {
        jz_bytes k;
        TRY_GOTO(groove__ivm__runtime__windows__encoded_record_key_part(input_desc, d->record.ptr, d->record.len, U32S(cb->group_field_indices), cb->group_field_indices.len, &k));
        if (jz_btree_insert(&touched_group_keys, &k, NULL, NULL)) jz_bytes_drop(&k);
    }
    if (track_roots) {
        jz_bytes *k; void *u;
        JZ_BTREE_FOR(&touched_group_keys, k, u) {
            if (groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, k->ptr, k->len)) { jz_bytes c = jz_bytes_clone(k); jz_btree_insert(&root_groups_before, &c, NULL, NULL); }
        }
    }
    JZ_VEC_FOR(groove_RecordDelta, delta, deltas) {
        jz_bytes group_key;
        TRY_GOTO(groove__ivm__runtime__windows__encoded_record_key_part(input_desc, delta->record.ptr, delta->record.len, U32S(cb->group_field_indices), cb->group_field_indices.len, &group_key));
        if (slot && slot->has_presence) {
            groove_BorrowedRecord r = { jz_bytes_slice(&delta->record), input_desc };
            bool present;
            e = groove__records__BorrowedRecord__get_bool(&r, slot->presence_field_index, &present);
            if (JZ_IS_ERR(e)) { jz_bytes_drop(&group_key); goto fail; }
            if (!present) {
                groove_CollectByOrderKey state_key;
                e = collect_by_sort_key(input_desc, jz_bytes_slice(&delta->record), cb, &state_key.parts);
                if (JZ_IS_ERR(e)) { jz_bytes_drop(&group_key); goto fail; }
                state_key.record = jz_bytes_clone(&delta->record);
                i64 *bw = jz_btree_get(&state->roots, &state_key);
                i64 before_weight = bw ? *bw : 0;
                i64 after_weight = before_weight + delta->weight;
                if (after_weight == 0) {
                    groove_CollectByOrderKey ok;
                    if (jz_btree_remove(&state->roots, &state_key, &ok, NULL)) order_key_drop(&ok);
                } else {
                    groove_CollectByOrderKey c = order_key_clone(&state_key);
                    if (bw) { *bw = after_weight; order_key_drop(&c); }
                    else jz_btree_insert(&state->roots, &c, &after_weight, NULL);
                }
                if (!emit || (before_weight > 0) == (after_weight > 0)) { order_key_drop(&state_key); jz_bytes_drop(&group_key); continue; }
                if (after_weight > 0) {
                    u32 index = 0;
                    jz_btree_iter it; groove_CollectByOrderKey *rk; i64 *rw;
                    jz_btree_range(&it, &state->roots, NULL, false, &state_key, false);
                    while (jz_btree_next(&it, (void **)&rk, (void **)&rw)) if (*rw > 0) index++;
                    groove_RecordWeight one = { delta->record, 1 };
                    bool some; jz_bytes record;
                    e = groove__ivm__runtime__windows__collect_by_tree_parent_from_records(input_desc, output_desc, cb, &one, 1, &some, &record);
                    order_key_drop(&state_key);
                    if (!JZ_IS_ERR(e) && !some) e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "root anchor did not render a terminal row");
                    if (JZ_IS_ERR(e)) { jz_bytes_drop(&group_key); goto fail; }
                    push_op(&operations, output_desc, jz_bytes_clone(&group_key), empty_path(), groove_TerminalEdit_Insert, index, group_key, record);
                } else {
                    order_key_drop(&state_key);
                    push_op(&operations, output_desc, jz_bytes_clone(&group_key), empty_path(), groove_TerminalEdit_Remove, 0, group_key, empty_bytes());
                }
                continue;
            }
        }
        const u32 *sidx = slot ? slot->sort_field_indices.ptr : cb->sort_field_indices.ptr;
        u32 nsidx = slot ? slot->sort_field_indices.len : cb->sort_field_indices.len;
        const u8 *sdir = slot ? slot->sort_directions.ptr : cb->sort_directions.ptr;
        u32 nsdir = slot ? slot->sort_directions.len : cb->sort_directions.len;
        groove_CollectByOrderKey state_key;
        e = collect_by_sort_key_for_fields(input_desc, jz_bytes_slice(&delta->record), sidx, nsidx, sdir, nsdir, &state_key.parts);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&group_key); goto fail; }
        state_key.record = jz_bytes_clone(&delta->record);
        groove_CollectByGroup *group = groove__ivm__runtime__evaluator__SparseGroups__get_or_default(&state->groups, jz_bytes_clone(&group_key));
        const i64 *bw = groove__ivm__runtime__evaluator__CollectByGroup__get(group, &state_key);
        i64 before_weight = bw ? *bw : 0;
        i64 after_weight = before_weight + delta->weight;
        groove__ivm__runtime__evaluator__CollectByGroup__set(group, order_key_clone(&state_key), after_weight);
        if (!emit || (before_weight > 0) == (after_weight > 0)) { order_key_drop(&state_key); jz_bytes_drop(&group_key); continue; }
        /* A group that enters the terminal in this batch is rendered whole as
         * a root insert below, which drops its child edits. */
        if (track_roots && !jz_btree_contains(&root_groups_before, &group_key)) { order_key_drop(&state_key); jz_bytes_drop(&group_key); continue; }
        groove_BorrowedRecord source_input = { jz_bytes_slice(&delta->record), input_desc };
        const groove_CollectByProjection *child_fields = slot ? slot->child_fields.ptr : cb->child_fields.ptr;
        u32 nchild = slot ? slot->child_fields.len : cb->child_fields.len;
        groove_RecordDescriptor child_descriptor = slot ? slot->child_descriptor : cb->child_descriptor;
        const jz_str *collection_field = slot ? &slot->collection_field : &cb->collection_field;
        groove_ValueVec child_values;
        e = project_fields(&source_input, child_fields, nchild, child_descriptor, &child_values);
        jz_bytes child_record = {0}, child_key = {0};
        if (!JZ_IS_ERR(e)) { e = groove__records__RecordDescriptor__create(child_descriptor, child_values.ptr, child_values.len, &child_record); values_drop(&child_values); }
        static const usize FIELD0[1] = { 0 };
        if (!JZ_IS_ERR(e)) e = groove__ivm__runtime__windows__encoded_record_key_part(child_descriptor, child_record.ptr, child_record.len, FIELD0, 1, &child_key);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&child_record); order_key_drop(&state_key); jz_bytes_drop(&group_key); goto fail; }
        groove_TerminalPath path = {0};
        groove_TerminalPathSegment seg; memset(&seg, 0, sizeof seg);
        seg.tag = groove_TerminalPathSegment_Collection; seg.collection = jz_str_clone(collection_field);
        JZ_VEC_PUSH(path, seg);
        if (after_weight > 0) {
            usize index = groove__ivm__runtime__evaluator__CollectByGroup__count_before(group, &state_key);
            push_op(&operations, output_desc, group_key, path, groove_TerminalEdit_Insert, (u32)index, child_key, child_record);
        } else {
            jz_bytes_drop(&child_record);
            push_op(&operations, output_desc, group_key, path, groove_TerminalEdit_Remove, 0, child_key, empty_bytes());
        }
        order_key_drop(&state_key);
    }
    {
        JZ_VEC(jz_bytes) keys = {0};
        jz_bytes *k; void *u;
        JZ_BTREE_FOR(&touched_group_keys, k, u) { jz_bytes c = jz_bytes_clone(k); JZ_VEC_PUSH(keys, c); }
        groove__ivm__runtime__evaluator__SparseGroups__remove_empty_touched_groups(&state->groups, keys.ptr, keys.len);
        jz_vec_drop(&keys, sizeof(jz_bytes), NULL);
    }
    if (track_roots) {
        jz_bytes *k; void *u;
        JZ_BTREE_FOR(&touched_group_keys, k, u) {
            if (groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, k->ptr, k->len)) { jz_bytes c = jz_bytes_clone(k); jz_btree_insert(&root_groups_after, &c, NULL, NULL); }
        }
        /* operations.retain(root in before && root in after) */
        u32 w = 0;
        for (u32 i = 0; i < operations.len; i++) {
            groove_TerminalOperation *op = &operations.ptr[i];
            if (jz_btree_contains(&root_groups_before, &op->root_key) && jz_btree_contains(&root_groups_after, &op->root_key)) operations.ptr[w++] = *op;
            else groove_TerminalOperation_drop(op);
        }
        operations.len = w;
        JZ_BTREE_FOR(&root_groups_before, k, u) {
            if (jz_btree_contains(&root_groups_after, k)) continue;
            push_op(&operations, output_desc, jz_bytes_clone(k), empty_path(), groove_TerminalEdit_Remove, 0, jz_bytes_clone(k), empty_bytes());
        }
        /* Rank against the complete merged group index; one merged walk ranks them all. */
        JZ_VEC(jz_slice) inserted_roots = {0};
        JZ_BTREE_FOR(&root_groups_after, k, u) {
            if (jz_btree_contains(&root_groups_before, k)) continue;
            jz_slice s = jz_bytes_slice(k);
            JZ_VEC_PUSH(inserted_roots, s);
        }
        groove_UsizeVecB root_ranks = {0};
        groove__ivm__runtime__evaluator__SparseGroups__count_before_each(&state->groups, inserted_roots.ptr, inserted_roots.len, &root_ranks);
        for (u32 i = 0; i < inserted_roots.len && i < root_ranks.len; i++) {
            jz_slice root_key = inserted_roots.ptr[i];
            const groove_CollectByGroup *group = groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, root_key.ptr, root_key.len);
            if (!group) jz_panic("root key came from collect state");
            groove_RecordWeightVec records;
            group_records(group, &records);
            bool some; jz_bytes record;
            e = groove__ivm__runtime__windows__collect_by_parent_from_records(input_desc, output_desc, cb, records.ptr, records.len, &some, &record);
            rw_vec_drop(&records);
            if (!JZ_IS_ERR(e) && !some) e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "new collect root did not render a terminal row");
            if (JZ_IS_ERR(e)) { jz_vec_drop(&inserted_roots, sizeof(jz_slice), NULL); jz_vec_drop(&root_ranks, sizeof(usize), NULL); goto fail; }
            push_op(&operations, output_desc, jz_bytes_from(root_key.ptr, root_key.len), empty_path(), groove_TerminalEdit_Insert, (u32)root_ranks.ptr[i],
                    jz_bytes_from(root_key.ptr, root_key.len), record);
        }
        jz_vec_drop(&inserted_roots, sizeof(jz_slice), NULL);
        jz_vec_drop(&root_ranks, sizeof(usize), NULL);
    }
    jz_vec_drop(&deltas, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
    jz_btree_drop(&touched_group_keys);
    jz_btree_drop(&root_groups_before);
    jz_btree_drop(&root_groups_after);
    *out = operations;
    return JZ_OK;
fail:
    jz_vec_drop(&deltas, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
    jz_btree_drop(&touched_group_keys);
    jz_btree_drop(&root_groups_before);
    jz_btree_drop(&root_groups_after);
    ops_drop(&operations);
    return e;
}

/* ---------- root collector ---------- */
static bool collect_by_sort_identifies_group(groove_RecordDescriptor input_desc, const groove_CollectByOp *cb) {
    u32 nsort = cb->sort_field_indices.len < cb->sort_directions.len ? cb->sort_field_indices.len : cb->sort_directions.len;
    JZ_VEC_FOR(const u32, group_field, cb->group_field_indices) {
        /* Only fields consumed by the zipped sort-key builder count; only
         * non-null Uuid/U64 encodings distinguish every possible identity. */
        bool in_sort = false;
        for (u32 i = 0; i < nsort; i++) if (cb->sort_field_indices.ptr[i] == *group_field) { in_sort = true; break; }
        const groove_DescriptorField *f = groove__records__RecordDescriptor__field(input_desc, *group_field);
        if (!in_sort || !f || (f->value_type.tag != groove_ValueType_Uuid && f->value_type.tag != groove_ValueType_U64)) return false;
    }
    return true;
}

static bool collect_by_root_order_key(const groove_CollectByGroup *group, bool unique_group_sort, groove_CollectByOrderKey *out) {
    if (!group) return false;
    groove_CollectByGroupIter it; const groove_CollectByOrderKey *key; const i64 *weight;
    groove__ivm__runtime__evaluator__CollectByGroup__iter(group, &it);
    while (groove__ivm__runtime__evaluator__CollectByGroupIter__Iterator__next(&it, &key, &weight)) {
        if (*weight <= 0) continue;
        /* The public index can omit record bytes once distinct group
         * identities necessarily have distinct sort prefixes. */
        jz_vec_clone(&out->parts, &key->parts, sizeof(groove_TopBySortPart), part_clone_v);
        out->record = unique_group_sort ? empty_bytes() : jz_bytes_clone(&key->record);
        return true;
    }
    return false;
}

typedef struct BeforeRendered { bool some; jz_bytes record; } BeforeRendered;
typedef struct BeforeOrder { bool some; groove_CollectByOrderKey key; } BeforeOrder;
static void before_rendered_drop(void *p) { BeforeRendered *b = p; if (b->some) jz_bytes_drop(&b->record); }
static void before_order_drop(void *p) { BeforeOrder *b = p; if (b->some) order_key_drop(&b->key); }
static const jz_btree_desc BEFORE_MAP = { sizeof(jz_bytes), sizeof(BeforeRendered), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, before_rendered_drop };
static const jz_btree_desc BEFORE_ORDER_MAP = { sizeof(jz_bytes), sizeof(BeforeOrder), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, before_order_drop };

static jz_err render_root(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
                          const groove_CollectByGroup *group, bool *some, jz_bytes *out) {
    *some = false;
    if (!group) return JZ_OK;
    groove_RecordWeightVec records;
    group_records(group, &records);
    jz_err e = groove__ivm__runtime__windows__collect_by_root_from_records(input_desc, output_desc, cb, records.ptr, records.len, some, out);
    rw_vec_drop(&records);
    return e;
}

static bool order_keys_equal(const BeforeOrder *before, bool after_some, const groove_CollectByOrderKey *after) {
    if (before->some != after_some) return false;
    return !before->some || groove__ivm__runtime__evaluator__CollectByOrderKey__eq(&before->key, after);
}

static void rank_order_make_mut(groove_CollectByIncrementalPayload *state, groove_RankIndex **out) {
    *out = jz_rc_make_mut((void **)&state->emitted_root_order, sizeof(groove_RankIndex), (void (*)(void *, const void *))groove_RankIndex_clone);
}
static groove_RankIndex *rank_keys_make_mut(groove_CollectByIncrementalPayload *state) {
    return jz_rc_make_mut((void **)&state->emitted_root_keys, sizeof(groove_RankIndex), (void (*)(void *, const void *))groove_RankIndex_clone);
}

static jz_err collect_by_root_payload_updates(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
                                              const groove_CollectByIncrementalPayload *state, jz_btree *before, groove_TerminalOperationVec *out) {
    jz_bytes root_key; BeforeRendered before_record;
    while (jz_btree_pop_first(before, &root_key, &before_record)) {
        /* A new root already emitted its Insert above. */
        if (!before_record.some) { jz_bytes_drop(&root_key); continue; }
        /* Without a prior Insert, an Update would address no facade occurrence. */
        if (!groove__ivm__runtime__rank_index__RankIndex__contains(state->emitted_root_keys, &root_key)) {
            jz_bytes_drop(&root_key); jz_bytes_drop(&before_record.record); continue;
        }
        bool some; jz_bytes after_record;
        jz_err e = render_root(input_desc, output_desc, cb, groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, root_key.ptr, root_key.len), &some, &after_record);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&root_key); jz_bytes_drop(&before_record.record); jz_btree_drop(before); return e; }
        if (!some || jz_bytes_eq(&before_record.record, &after_record)) {
            if (some) jz_bytes_drop(&after_record);
            jz_bytes_drop(&root_key); jz_bytes_drop(&before_record.record);
            continue;
        }
        jz_bytes_drop(&before_record.record);
        push_op(out, output_desc, jz_bytes_clone(&root_key), empty_path(), groove_TerminalEdit_Update, 0, root_key, after_record);
    }
    jz_btree_drop(before);
    return JZ_OK;
}

typedef struct RootMove { groove_CollectByOrderKey after_key, before_key; jz_bytes root_key; jz_bytes record; } RootMove;
static int root_move_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return groove__ivm__runtime__evaluator__CollectByOrderKey__cmp(&((const RootMove *)a)->after_key, &((const RootMove *)b)->after_key, NULL);
}

static jz_err update_collect_by_root_terminal_state(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
    groove_CollectByIncrementalState *st, const groove_RecordDelta *deltas, u32 n, bool emit, groove_TerminalOperationVec *out) {
    groove_CollectByIncrementalPayload *state = groove__ivm__runtime__evaluator__CollectByIncrementalState__deref_mut(st);
    out->ptr = NULL; out->len = 0; out->cap = 0;
    if (!emit) {
        groove__ivm__runtime__evaluator__SparseGroups__clear(&state->groups);
        jz_btree_clear(&state->roots);
        jz_rc_release(state->emitted_root_order); state->emitted_root_order = groove__ivm__runtime__rank_index__RankIndex__new_order();
        jz_rc_release(state->emitted_root_keys); state->emitted_root_keys = groove__ivm__runtime__rank_index__RankIndex__new_keys();
    }
    bool unique_group_sort = collect_by_sort_identifies_group(input_desc, cb);
    jz_btree before; jz_btree_init(&before, &BEFORE_MAP);
    jz_btree before_order; jz_btree_init(&before_order, &BEFORE_ORDER_MAP);
    jz_err e = JZ_OK;
    JZ_VEC(RootMove) moves = {0};
    JZ_VEC(RootMove) inserts = {0};
    for (u32 i = 0; i < n; i++) {
        const groove_RecordDelta *delta = &deltas[i];
        jz_bytes group_key;
        TRY_GOTO(groove__ivm__runtime__windows__encoded_record_key_part(input_desc, delta->record.ptr, delta->record.len, U32S(cb->group_field_indices), cb->group_field_indices.len, &group_key));
        const groove_CollectByGroup *existing = groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, group_key.ptr, group_key.len);
        if (emit && !jz_btree_contains(&before, &group_key)) {
            BeforeRendered r = { false, {0} };
            e = render_root(input_desc, output_desc, cb, existing, &r.some, &r.record);
            if (JZ_IS_ERR(e)) { jz_bytes_drop(&group_key); goto fail; }
            jz_bytes k = jz_bytes_clone(&group_key);
            jz_btree_insert(&before, &k, &r, NULL);
        }
        if (!jz_btree_contains(&before_order, &group_key)) {
            BeforeOrder o; memset(&o, 0, sizeof o);
            o.some = collect_by_root_order_key(existing, unique_group_sort, &o.key);
            jz_bytes k = jz_bytes_clone(&group_key);
            jz_btree_insert(&before_order, &k, &o, NULL);
        }
        groove_CollectByOrderKey state_key;
        e = collect_by_sort_key(input_desc, jz_bytes_slice(&delta->record), cb, &state_key.parts);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&group_key); goto fail; }
        state_key.record = jz_bytes_clone(&delta->record);
        groove_CollectByGroup *group = groove__ivm__runtime__evaluator__SparseGroups__get_or_default(&state->groups, group_key);
        const i64 *w = groove__ivm__runtime__evaluator__CollectByGroup__get(group, &state_key);
        i64 weight = (w ? *w : 0) + delta->weight;
        groove__ivm__runtime__evaluator__CollectByGroup__set(group, state_key, weight);
    }
    {
        JZ_VEC(jz_bytes) keys = {0};
        jz_bytes *k; void *u;
        JZ_BTREE_FOR(&before, k, u) { jz_bytes c = jz_bytes_clone(k); JZ_VEC_PUSH(keys, c); }
        groove__ivm__runtime__evaluator__SparseGroups__remove_empty_touched_groups(&state->groups, keys.ptr, keys.len);
        jz_vec_drop(&keys, sizeof(jz_bytes), NULL);
    }
    if (!emit) { jz_btree_drop(&before); jz_btree_drop(&before_order); return JZ_OK; }

    /* If every touched group retains exactly the same total order key, no
     * public position can change: render only the affected payloads. */
    bool unchanged = true;
    {
        jz_bytes *root_key; BeforeOrder *bo;
        JZ_BTREE_FOR(&before_order, root_key, bo) {
            groove_CollectByOrderKey after; memset(&after, 0, sizeof after);
            bool some = collect_by_root_order_key(groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, root_key->ptr, root_key->len), unique_group_sort, &after);
            bool eq = order_keys_equal(bo, some, &after);
            if (some) order_key_drop(&after);
            if (!eq) { unchanged = false; break; }
        }
    }
    if (unchanged) {
        jz_btree_drop(&before_order);
        return collect_by_root_payload_updates(input_desc, output_desc, cb, state, &before, out);
    }

    /* Remove first so later positional inserts/moves index the final retained sequence. */
    {
        jz_bytes *root_key; BeforeOrder *bo;
        JZ_BTREE_FOR(&before_order, root_key, bo) {
            if (!groove__ivm__runtime__rank_index__RankIndex__contains(state->emitted_root_keys, root_key)) continue;
            groove_CollectByOrderKey after; memset(&after, 0, sizeof after);
            bool some = collect_by_root_order_key(groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, root_key->ptr, root_key->len), unique_group_sort, &after);
            if (some) { order_key_drop(&after); continue; }
            push_op(out, output_desc, jz_bytes_clone(root_key), empty_path(), groove_TerminalEdit_Remove, 0, jz_bytes_clone(root_key), empty_bytes());
            if (bo->some) { groove_RankIndex *order; rank_order_make_mut(state, &order); groove__ivm__runtime__rank_index__RankIndex__remove(order, &bo->key); }
            groove__ivm__runtime__rank_index__RankIndex__remove(rank_keys_make_mut(state), root_key);
        }
    }
    /* The rank index describes the actual public sequence after each emitted edit. */
    {
        jz_bytes *root_key; BeforeOrder *bo;
        JZ_BTREE_FOR(&before_order, root_key, bo) {
            if (!bo->some) continue;
            BeforeRendered *br = jz_btree_get(&before, root_key);
            if (!(br && br->some) || !groove__ivm__runtime__rank_index__RankIndex__contains(state->emitted_root_keys, root_key)) continue;
            RootMove m; memset(&m, 0, sizeof m);
            if (!collect_by_root_order_key(groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, root_key->ptr, root_key->len), unique_group_sort, &m.after_key)) continue;
            if (groove__ivm__runtime__evaluator__CollectByOrderKey__eq(&bo->key, &m.after_key)) { order_key_drop(&m.after_key); continue; }
            m.before_key = order_key_clone(&bo->key);
            m.root_key = jz_bytes_clone(root_key);
            JZ_VEC_PUSH(moves, m);
        }
    }
    jz_sort(moves.ptr, moves.len, sizeof(RootMove), root_move_cmp, NULL);
    JZ_VEC_FOR(RootMove, m, moves) {
        groove_RankIndex *order; rank_order_make_mut(state, &order);
        u32 current_index = groove__ivm__runtime__rank_index__RankIndex__rank(order, &m->before_key);
        groove__ivm__runtime__rank_index__RankIndex__remove(order, &m->before_key);
        u32 index = groove__ivm__runtime__rank_index__RankIndex__rank(order, &m->after_key);
        jz_bytes rk = jz_bytes_clone(&m->root_key);
        groove__ivm__runtime__rank_index__RankIndex__insert(order, &m->after_key, &rk);
        order_key_drop(&m->before_key);
        if (current_index == index) { jz_bytes_drop(&m->root_key); continue; }
        push_op(out, output_desc, jz_bytes_clone(&m->root_key), empty_path(), groove_TerminalEdit_Move, index, m->root_key, empty_bytes());
    }
    jz_vec_drop(&moves, sizeof(RootMove), NULL);
    moves.len = 0; moves.ptr = NULL; moves.cap = 0;

    {
        jz_bytes *root_key; BeforeOrder *bo;
        JZ_BTREE_FOR(&before_order, root_key, bo) {
            if (bo->some) continue;
            const groove_CollectByGroup *group = groove__ivm__runtime__evaluator__SparseGroups__get(&state->groups, root_key->ptr, root_key->len);
            RootMove ins; memset(&ins, 0, sizeof ins);
            /* Retractions can reach a freshly reset collector before its
             * replacement hydration: no positive order key, no occurrence. */
            if (!collect_by_root_order_key(group, unique_group_sort, &ins.after_key)) continue;
            bool some;
            e = render_root(input_desc, output_desc, cb, group, &some, &ins.record);
            if (!JZ_IS_ERR(e) && !some) e = GROOVE_IVM_ERR_C(GROOVE_IVM_InvalidCollectBy, "new root collector group did not render a terminal row");
            if (JZ_IS_ERR(e)) { order_key_drop(&ins.after_key); goto fail; }
            ins.root_key = jz_bytes_clone(root_key);
            JZ_VEC_PUSH(inserts, ins);
        }
    }
    jz_sort(inserts.ptr, inserts.len, sizeof(RootMove), root_move_cmp, NULL);
    JZ_VEC_FOR(RootMove, ins, inserts) {
        /* Earlier inserts already belong to the rank index. */
        groove_RankIndex *order; rank_order_make_mut(state, &order);
        u32 index = groove__ivm__runtime__rank_index__RankIndex__rank(order, &ins->after_key);
        jz_bytes rk = jz_bytes_clone(&ins->root_key);
        groove__ivm__runtime__rank_index__RankIndex__insert(order, &ins->after_key, &rk);
        jz_bytes kk = jz_bytes_clone(&ins->root_key);
        groove__ivm__runtime__rank_index__RankIndex__insert(rank_keys_make_mut(state), &kk, NULL);
        push_op(out, output_desc, jz_bytes_clone(&ins->root_key), empty_path(), groove_TerminalEdit_Insert, index, ins->root_key, ins->record);
    }
    jz_vec_drop(&inserts, sizeof(RootMove), NULL);
    jz_btree_drop(&before_order);
    {
        groove_TerminalOperationVec payload = {0};
        e = collect_by_root_payload_updates(input_desc, output_desc, cb, state, &before, &payload);
        if (JZ_IS_ERR(e)) { ops_drop(out); return e; }
        jz_vec_extend(out, payload.ptr, payload.len, sizeof(groove_TerminalOperation));
        jz_vec_drop(&payload, sizeof(groove_TerminalOperation), NULL);
    }
    return JZ_OK;
fail:
    JZ_VEC_FOR(RootMove, m, moves) { order_key_drop(&m->after_key); order_key_drop(&m->before_key); jz_bytes_drop(&m->root_key); }
    jz_vec_drop(&moves, sizeof(RootMove), NULL);
    JZ_VEC_FOR(RootMove, m, inserts) { order_key_drop(&m->after_key); jz_bytes_drop(&m->root_key); jz_bytes_drop(&m->record); }
    jz_vec_drop(&inserts, sizeof(RootMove), NULL);
    jz_btree_drop(&before);
    jz_btree_drop(&before_order);
    ops_drop(out);
    return e;
}

/* ---------- rendering ---------- */
static const groove_RecordWeight *first_positive(const groove_RecordWeight *records, u32 n) {
    for (u32 i = 0; i < n; i++) if (records[i].weight > 0) return &records[i];
    return NULL;
}

jz_err groove__ivm__runtime__windows__collect_by_root_from_records(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
    const groove_RecordWeight *records, u32 n, bool *some, jz_bytes *out) {
    *some = false;
    const groove_RecordWeight *parent = first_positive(records, n);
    if (!parent) return JZ_OK;
    groove_BorrowedRecord parent_input = { jz_bytes_slice(&parent->record), input_desc };
    groove_ValueVec values;
    JZ_TRY(project_fields(&parent_input, cb->parent_fields.ptr, cb->parent_fields.len, output_desc, &values));
    jz_err e = groove__records__RecordDescriptor__create(output_desc, values.ptr, values.len, out);
    values_drop(&values);
    if (JZ_IS_ERR(e)) {
        jz_err wrapped = jz_err_fmt(GROOVE_IVM_InvalidCollectBy, "invalid collect_by descriptor: root record render failed: %.*s",
                                    e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return wrapped;
    }
    *some = true;
    return JZ_OK;
}

static jz_err collect_by_window_from_records(groove_RecordDescriptor d, const groove_RecordWeight *records, u32 n, const groove_CollectByOp *cb, groove_WindowedRecordVec *out);

static void record_value(groove_RecordDescriptor d, jz_bytes raw, groove_Value *out) {
    memset(out, 0, sizeof *out);
    out->tag = groove_Value_Record;
    out->v.record.raw = raw;
    out->v.record.descriptor = d;
}

jz_err groove__ivm__runtime__windows__collect_by_parent_from_records(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
    const groove_RecordWeight *records, u32 n, bool *some, jz_bytes *out) {
    *some = false;
    const groove_RecordWeight *parent = first_positive(records, n);
    if (!parent) return JZ_OK;
    groove_BorrowedRecord parent_input = { jz_bytes_slice(&parent->record), input_desc };
    groove_ValueVec values;
    JZ_TRY(project_fields(&parent_input, cb->parent_fields.ptr, cb->parent_fields.len, output_desc, &values));
    groove_WindowedRecordVec window;
    jz_err e = collect_by_window_from_records(input_desc, records, n, cb, &window);
    if (JZ_IS_ERR(e)) { values_drop(&values); return e; }
    groove_ValueVec children = {0};
    JZ_VEC_FOR(groove_WindowedRecord, w, window) {
        groove_BorrowedRecord source_input = { jz_bytes_slice(&w->record), input_desc };
        groove_ValueVec child_values;
        e = project_fields(&source_input, cb->child_fields.ptr, cb->child_fields.len, cb->child_descriptor, &child_values);
        if (JZ_IS_ERR(e)) break;
        jz_bytes child;
        e = groove__records__RecordDescriptor__create(cb->child_descriptor, child_values.ptr, child_values.len, &child);
        values_drop(&child_values);
        if (JZ_IS_ERR(e)) break;
        for (i64 c = 0; c < w->weight; c++) {
            groove_Value v;
            record_value(cb->child_descriptor, jz_bytes_clone(&child), &v);
            JZ_VEC_PUSH(children, v);
        }
        jz_bytes_drop(&child);
    }
    jz_vec_drop(&window, sizeof(groove_WindowedRecord), rw_drop_v);
    if (JZ_IS_ERR(e)) { values_drop(&values); values_drop(&children); return e; }
    groove_Value arr; memset(&arr, 0, sizeof arr);
    arr.tag = groove_Value_Array; arr.v.vals = children;
    JZ_VEC_PUSH(values, arr);
    e = groove__records__RecordDescriptor__create(output_desc, values.ptr, values.len, out);
    values_drop(&values);
    JZ_TRY(e);
    *some = true;
    return JZ_OK;
}

static jz_err render_collect_by_slots(groove_RecordDescriptor input_desc, const groove_RecordWeight *records, u32 n, const jz_bytes *owner_record,
                                      const groove_CollectBySlot *slots, u32 nslots, groove_ValueVec *out);

jz_err groove__ivm__runtime__windows__collect_by_tree_parent_from_records(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
    const groove_RecordWeight *records, u32 n, bool *some, jz_bytes *out) {
    *some = false;
    const groove_RecordWeight *parent = first_positive(records, n);
    if (!parent) return JZ_OK;
    groove_BorrowedRecord parent_input = { jz_bytes_slice(&parent->record), input_desc };
    groove_ValueVec values;
    JZ_TRY(project_fields(&parent_input, cb->parent_fields.ptr, cb->parent_fields.len, output_desc, &values));
    groove_ValueVec slot_values;
    jz_err e = render_collect_by_slots(input_desc, records, n, &parent->record, cb->slots.ptr, cb->slots.len, &slot_values);
    if (JZ_IS_ERR(e)) { values_drop(&values); return e; }
    jz_vec_extend(&values, slot_values.ptr, slot_values.len, sizeof(groove_Value));
    jz_vec_drop(&slot_values, sizeof(groove_Value), NULL);
    e = groove__records__RecordDescriptor__create(output_desc, values.ptr, values.len, out);
    values_drop(&values);
    JZ_TRY(e);
    *some = true;
    return JZ_OK;
}

static jz_err collect_by_slot_window_from_records(groove_RecordDescriptor d, groove_BytesVec records, const groove_CollectBySlot *slot, groove_BytesVec *out);

typedef struct UuidRecord { jz_uuid id; jz_bytes record; } UuidRecord;
static int uuid_cmp_v(const void *a, const void *b, void *ctx) { (void)ctx; return jz_uuid_cmp(a, b); }
static const jz_btree_desc UUID_BYTES_MAP = { sizeof(jz_uuid), sizeof(jz_bytes), uuid_cmp_v, NULL, groove_ivm_rt_bytes_drop_v };
static const jz_btree_desc BYTES_BYTES_MAP = { sizeof(jz_bytes), sizeof(jz_bytes), jz_cmp_bytes, groove_ivm_rt_bytes_drop_v, groove_ivm_rt_bytes_drop_v };

static jz_err render_one_slot(groove_RecordDescriptor input_desc, const groove_RecordWeight *records, u32 n, const jz_bytes *owner_record,
                              const groove_CollectBySlot *slot, groove_Value *out) {
    memset(out, 0, sizeof *out);
    out->tag = groove_Value_Array;
    if (slot->limit.finite && slot->limit.n == 0) return JZ_OK;
    jz_err e = JZ_OK;
    jz_bytes owner_key = {0};
    JZ_TRY(groove__ivm__runtime__windows__encoded_record_key_part(input_desc, owner_record->ptr, owner_record->len, U32S(slot->group_field_indices), slot->group_field_indices.len, &owner_key));
    /* A nested flat association can repeat its owning child once for every
     * grandchild. Collapse those repetitions by the rendered child projection. */
    JZ_VEC(jz_str) names = {0};
    JZ_VEC(groove_ValueType) types = {0};
    groove_RecordDescriptor child_key_descriptor = NULL;
    jz_btree candidates; jz_btree_init(&candidates, &BYTES_BYTES_MAP);
    groove_BytesVec list = {0}, selected = {0};
    JZ_VEC_FOR(const groove_CollectByProjection, field, slot->child_fields) {
        groove_ValueType t;
        TRY_GOTO(collect_by_projection_value_type(input_desc, field, &t));
        jz_str nm = jz_str_clone(&field->output_name);
        JZ_VEC_PUSH(names, nm);
        JZ_VEC_PUSH(types, t);
    }
    child_key_descriptor = groove__records__RecordDescriptor__new_named(names.ptr, types.ptr, names.len);
    for (u32 i = 0; i < n; i++) {
        const groove_RecordWeight *r = &records[i];
        if (r->weight <= 0) continue;
        jz_bytes gk;
        TRY_GOTO(groove__ivm__runtime__windows__encoded_record_key_part(input_desc, r->record.ptr, r->record.len, U32S(slot->group_field_indices), slot->group_field_indices.len, &gk));
        bool same = jz_bytes_eq(&gk, &owner_key);
        jz_bytes_drop(&gk);
        if (!same) continue;
        groove_BorrowedRecord src = { jz_bytes_slice(&r->record), input_desc };
        if (slot->has_presence) {
            bool present;
            TRY_GOTO(groove__records__BorrowedRecord__get_bool(&src, slot->presence_field_index, &present));
            if (!present) continue;
        }
        groove_ValueVec child_values = {0};
        JZ_VEC_FOR(const groove_CollectByProjection, field, slot->child_fields) {
            groove_Value v;
            e = collect_by_projected_value(&src, field, &v);
            if (JZ_IS_ERR(e)) { values_drop(&child_values); goto fail; }
            JZ_VEC_PUSH(child_values, v);
        }
        jz_bytes key;
        e = groove__records__RecordDescriptor__create(child_key_descriptor, child_values.ptr, child_values.len, &key);
        values_drop(&child_values);
        if (JZ_IS_ERR(e)) goto fail;
        if (jz_btree_contains(&candidates, &key)) jz_bytes_drop(&key);
        else { jz_bytes rc = jz_bytes_clone(&r->record); jz_btree_insert(&candidates, &key, &rc, NULL); }
    }
    {
        jz_bytes k, v;
        while (jz_btree_pop_first(&candidates, &k, &v)) { jz_bytes_drop(&k); JZ_VEC_PUSH(list, v); }
    }
    if (slot->has_reference_array) {
        jz_btree by_id; jz_btree_init(&by_id, &UUID_BYTES_MAP);
        bool have_refs = false; groove_ValueVec references = {0};
        JZ_VEC_FOR(jz_bytes, record, list) {
            groove_BorrowedRecord br = { jz_bytes_slice(record), input_desc };
            if (!have_refs) {
                groove_ValueVec values;
                e = groove__records__BorrowedRecord__to_values(&br, &values);
                if (JZ_IS_ERR(e)) { jz_btree_drop(&by_id); values_drop(&references); goto fail; }
                const groove_Value *array = &values.ptr[slot->reference_array_field_index];
                while (array->tag == groove_Value_Nullable && array->v.nullable) array = array->v.nullable;
                if (array->tag == groove_Value_Array)
                    jz_vec_clone(&references, &array->v.vals, sizeof(groove_Value), (void (*)(void *, const void *))groove_Value_clone);
                have_refs = true;
                values_drop(&values);
            }
            groove_Value id;
            e = collect_by_projected_value(&br, &slot->child_fields.ptr[0], &id);
            if (JZ_IS_ERR(e)) { jz_btree_drop(&by_id); values_drop(&references); goto fail; }
            if (id.tag == groove_Value_Uuid) {
                jz_bytes rc = jz_bytes_clone(record); jz_bytes old;
                if (jz_btree_insert(&by_id, &id.v.uuid, &rc, &old)) jz_bytes_drop(&old);
            }
            groove_Value_drop(&id);
        }
        jz_vec_drop(&list, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
        list.ptr = NULL; list.len = 0; list.cap = 0;
        JZ_VEC_FOR(groove_Value, v, references) {
            if (v->tag != groove_Value_Uuid) continue;
            jz_bytes *rec = jz_btree_get(&by_id, &v->v.uuid);
            if (rec) { jz_bytes c = jz_bytes_clone(rec); JZ_VEC_PUSH(list, c); }
        }
        values_drop(&references);
        jz_btree_drop(&by_id);
    }
    TRY_GOTO(collect_by_slot_window_from_records(input_desc, list, slot, &selected));
    list.ptr = NULL; list.len = 0; list.cap = 0; /* moved */
    {
        groove_ValueVec children = {0};
        jz_vec_reserve(&children, selected.len, sizeof(groove_Value));
        JZ_VEC_FOR(jz_bytes, record, selected) {
            groove_BorrowedRecord src = { jz_bytes_slice(record), input_desc };
            groove_ValueVec child_values;
            e = project_fields(&src, slot->child_fields.ptr, slot->child_fields.len, slot->child_descriptor, &child_values);
            if (JZ_IS_ERR(e)) { values_drop(&children); goto fail; }
            groove_ValueVec nested;
            e = render_collect_by_slots(input_desc, records, n, record, slot->slots.ptr, slot->slots.len, &nested);
            if (JZ_IS_ERR(e)) { values_drop(&child_values); values_drop(&children); goto fail; }
            jz_vec_extend(&child_values, nested.ptr, nested.len, sizeof(groove_Value));
            jz_vec_drop(&nested, sizeof(groove_Value), NULL);
            jz_bytes child;
            e = groove__records__RecordDescriptor__create(slot->child_descriptor, child_values.ptr, child_values.len, &child);
            values_drop(&child_values);
            if (JZ_IS_ERR(e)) { values_drop(&children); goto fail; }
            groove_Value v; record_value(slot->child_descriptor, child, &v);
            JZ_VEC_PUSH(children, v);
        }
        out->v.vals = children;
    }
fail:
    jz_bytes_drop(&owner_key);
    jz_vec_drop(&names, sizeof(jz_str), groove_ivm_rt_str_drop_v);
    JZ_VEC_FOR(groove_ValueType, t, types) groove_ValueType_drop(t);
    jz_vec_drop(&types, sizeof(groove_ValueType), NULL);
    jz_btree_drop(&candidates);
    jz_vec_drop(&list, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
    jz_vec_drop(&selected, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v);
    return e;
}

static jz_err render_collect_by_slots(groove_RecordDescriptor input_desc, const groove_RecordWeight *records, u32 n, const jz_bytes *owner_record,
                                      const groove_CollectBySlot *slots, u32 nslots, groove_ValueVec *out) {
    out->ptr = NULL; out->len = 0; out->cap = 0;
    for (u32 i = 0; i < nslots; i++) {
        groove_Value v;
        jz_err e = render_one_slot(input_desc, records, n, owner_record, &slots[i], &v);
        if (JZ_IS_ERR(e)) { values_drop(out); return e; }
        JZ_VEC_PUSH(*out, v);
    }
    return JZ_OK;
}

typedef struct RankedRecord { groove_TopBySortPartVec key; jz_bytes record; i64 weight; } RankedRecord;
static int ranked_cmp(const void *pa, const void *pb, void *ctx) {
    (void)ctx;
    const RankedRecord *a = pa, *b = pb;
    int c = parts_cmp(&a->key, &b->key);
    return c ? c : jz_bytes_cmp(&a->record, &b->record);
}
static void ranked_drop(void *p) { RankedRecord *r = p; parts_drop(&r->key); jz_bytes_drop(&r->record); }

static jz_err collect_by_slot_window_from_records(groove_RecordDescriptor d, groove_BytesVec records, const groove_CollectBySlot *slot, groove_BytesVec *out) {
    JZ_VEC(RankedRecord) ranked = {0};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < records.len; i++) {
        RankedRecord r; memset(&r, 0, sizeof r);
        e = collect_by_sort_key_for_fields(d, jz_bytes_slice(&records.ptr[i]), slot->sort_field_indices.ptr, slot->sort_field_indices.len,
                                           slot->sort_directions.ptr, slot->sort_directions.len, &r.key);
        if (JZ_IS_ERR(e)) {
            for (u32 j = i; j < records.len; j++) jz_bytes_drop(&records.ptr[j]);
            jz_vec_drop(&records, sizeof(jz_bytes), NULL);
            jz_vec_drop(&ranked, sizeof(RankedRecord), ranked_drop);
            return e;
        }
        r.record = records.ptr[i];
        JZ_VEC_PUSH(ranked, r);
    }
    jz_vec_drop(&records, sizeof(jz_bytes), NULL);
    if (!slot->reference_order) jz_sort(ranked.ptr, ranked.len, sizeof(RankedRecord), ranked_cmp, NULL);
    u64 skip = slot->offset;
    u64 take = slot->limit.finite ? slot->limit.n : UINT64_MAX;
    out->ptr = NULL; out->len = 0; out->cap = 0;
    for (u32 i = 0; i < ranked.len; i++) {
        parts_drop(&ranked.ptr[i].key);
        if (i < skip || (u64)(i - skip) >= take) { jz_bytes_drop(&ranked.ptr[i].record); continue; }
        JZ_VEC_PUSH(*out, ranked.ptr[i].record);
    }
    jz_vec_drop(&ranked, sizeof(RankedRecord), NULL);
    return JZ_OK;
}

/* Render the selected expand window keyed by its ordered contributing source ids. */
jz_err groove__ivm__runtime__windows__collect_by_expanded_window(groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, const groove_CollectByOp *cb,
    const groove_RecordWeight *records, u32 n, jz_btree *out) {
    groove_WindowedRecordVec window;
    JZ_TRY(collect_by_window_from_records(input_desc, records, n, cb, &window));
    jz_btree_init(out, &BYTES_BYTES_MAP);
    jz_err e = JZ_OK;
    JZ_VEC_FOR(groove_WindowedRecord, w, window) {
        jz_bytes occurrence;
        TRY_GOTO(groove__ivm__runtime__windows__encoded_record_key_part(input_desc, w->record.ptr, w->record.len, U32S(cb->occurrence_id_field_indices),
                                                                       cb->occurrence_id_field_indices.len, &occurrence));
        /* A residual weighted duplicate is malformed. */
        if (w->weight != 1 || jz_btree_contains(out, &occurrence)) { jz_bytes_drop(&occurrence); e = GROOVE_IVM_ERR(GROOVE_IVM_DuplicateCollectByOccurrenceId); goto fail; }
        groove_BorrowedRecord input = { jz_bytes_slice(&w->record), input_desc };
        groove_ValueVec tuple = {0};
        JZ_VEC_FOR(const groove_CollectByProjection, field, cb->tuple_fields) {
            groove_Value v;
            e = collect_by_projected_value(&input, field, &v);
            if (JZ_IS_ERR(e)) { values_drop(&tuple); jz_bytes_drop(&occurrence); goto fail; }
            JZ_VEC_PUSH(tuple, v);
        }
        jz_bytes rendered;
        e = groove__records__RecordDescriptor__create(output_desc, tuple.ptr, tuple.len, &rendered);
        values_drop(&tuple);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&occurrence); goto fail; }
        jz_btree_insert(out, &occurrence, &rendered, NULL);
    }
    jz_vec_drop(&window, sizeof(groove_WindowedRecord), rw_drop_v);
    return JZ_OK;
fail:
    jz_vec_drop(&window, sizeof(groove_WindowedRecord), rw_drop_v);
    jz_btree_drop(out);
    return e;
}

static jz_err collect_by_window_from_records(groove_RecordDescriptor d, const groove_RecordWeight *records, u32 n, const groove_CollectByOp *cb, groove_WindowedRecordVec *out) {
    JZ_VEC(RankedRecord) ranked = {0};
    for (u32 i = 0; i < n; i++) {
        if (records[i].weight <= 0) continue;
        RankedRecord r; memset(&r, 0, sizeof r);
        jz_err e = collect_by_sort_key(d, jz_bytes_slice(&records[i].record), cb, &r.key);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&ranked, sizeof(RankedRecord), ranked_drop); return e; }
        r.record = jz_bytes_clone(&records[i].record);
        r.weight = records[i].weight;
        JZ_VEC_PUSH(ranked, r);
    }
    jz_sort(ranked.ptr, ranked.len, sizeof(RankedRecord), ranked_cmp, NULL);
    out->ptr = NULL; out->len = 0; out->cap = 0;
    WindowCursor c = window_cursor(cb->offset, cb->limit);
    JZ_VEC_FOR(RankedRecord, r, ranked) {
        parts_drop(&r->key);
        if (window_done(&c)) { jz_bytes_drop(&r->record); continue; }
        u64 taken = window_take(&c, (u64)r->weight);
        if (taken > 0) { groove_WindowedRecord w = { r->record, (i64)taken }; JZ_VEC_PUSH(*out, w); }
        else jz_bytes_drop(&r->record);
    }
    jz_vec_drop(&ranked, sizeof(RankedRecord), NULL);
    return JZ_OK;
}

/* ---------- window diffs ---------- */
void groove__ivm__runtime__windows__diff_record_windows(groove_WindowedRecordVec before, groove_WindowedRecordVec after, groove_RecordDeltaVec *out) {
    jz_btree weights; jz_btree_init(&weights, &BYTES_I64);
    JZ_VEC_FOR(groove_WindowedRecord, w, before) add_weight(&weights, &w->record, -w->weight, true);
    JZ_VEC_FOR(groove_WindowedRecord, w, after) add_weight(&weights, &w->record, w->weight, true);
    out->ptr = NULL; out->len = 0; out->cap = 0;
    JZ_VEC_FOR(groove_WindowedRecord, w, before) {
        i64 *weight = jz_btree_get(&weights, &w->record);
        if (weight && *weight < 0) {
            jz_bytes k; i64 v;
            jz_btree_remove(&weights, &w->record, &k, &v);
            jz_bytes_drop(&k);
            groove_RecordDelta d = { w->record, v };
            JZ_VEC_PUSH(*out, d);
            w->record.ptr = NULL; w->record.len = 0; w->record.cap = 0;
        }
    }
    JZ_VEC_FOR(groove_WindowedRecord, w, after) {
        i64 *weight = jz_btree_get(&weights, &w->record);
        if (weight && *weight > 0) {
            jz_bytes k; i64 v;
            jz_btree_remove(&weights, &w->record, &k, &v);
            jz_bytes_drop(&k);
            groove_RecordDelta d = { w->record, v };
            JZ_VEC_PUSH(*out, d);
            w->record.ptr = NULL; w->record.len = 0; w->record.cap = 0;
        }
    }
    jz_vec_drop(&before, sizeof(groove_WindowedRecord), rw_drop_v);
    jz_vec_drop(&after, sizeof(groove_WindowedRecord), rw_drop_v);
    jz_btree_drop(&weights);
}

/* Unbounded, zero-offset membership is the positive part of each weight. */
typedef struct BeforeAfter { i64 before, after; } BeforeAfter;
static void order_key_drop_v(void *k) { order_key_drop(k); }
static const jz_btree_desc ORDER_KEY_PAIR_MAP = { sizeof(groove_CollectByOrderKey), sizeof(BeforeAfter), groove__ivm__runtime__evaluator__CollectByOrderKey__cmp, order_key_drop_v, NULL };

jz_err groove__ivm__runtime__windows__update_unbounded_top_by_group_touching(groove_RecordDescriptor d, const groove_TopByOp *top_by, groove_CollectByGroup *group,
    const groove_RecordDelta *input, u32 n, groove_CollectByOrderKeyVec *touched_keys, groove_RecordDeltaVec *out) {
    jz_btree touched; jz_btree_init(&touched, &ORDER_KEY_PAIR_MAP);
    for (u32 i = 0; i < n; i++) {
        groove_CollectByOrderKey key;
        jz_err e = groove__ivm__runtime__windows__top_by_sort_key(d, input[i].record.ptr, input[i].record.len, top_by, &key.parts);
        if (JZ_IS_ERR(e)) { jz_btree_drop(&touched); return e; }
        key.record = jz_bytes_clone(&input[i].record);
        BeforeAfter *w = jz_btree_get(&touched, &key);
        if (!w) {
            const i64 *b = groove__ivm__runtime__evaluator__CollectByGroup__get(group, &key);
            BeforeAfter ba = { b ? *b : 0, b ? *b : 0 };
            jz_btree_insert(&touched, &key, &ba, NULL);
            w = jz_btree_get(&touched, &key);
        } else {
            order_key_drop(&key);
            /* key was consumed: look it up again through the stored copy */
        }
        w->after += input[i].weight;
    }
    groove_RecordDeltaVec removed = {0}, added = {0};
    groove_CollectByOrderKey key; BeforeAfter ba;
    while (jz_btree_pop_first(&touched, &key, &ba)) {
        /* Negative internal bag weights are retained but not selected. */
        i64 weight = (ba.after > 0 ? ba.after : 0) - (ba.before > 0 ? ba.before : 0);
        if (weight != 0) {
            groove_RecordDelta delta = { jz_bytes_clone(&key.record), weight };
            if (weight < 0) JZ_VEC_PUSH(removed, delta);
            else JZ_VEC_PUSH(added, delta);
        }
        if (touched_keys) { groove_CollectByOrderKey c = order_key_clone(&key); JZ_VEC_PUSH(*touched_keys, c); }
        groove__ivm__runtime__evaluator__CollectByGroup__set(group, key, ba.after);
    }
    jz_btree_drop(&touched);
    /* Removals in old order, then additions in new order. */
    jz_vec_extend(&removed, added.ptr, added.len, sizeof(groove_RecordDelta));
    jz_vec_drop(&added, sizeof(groove_RecordDelta), NULL);
    *out = removed;
    return JZ_OK;
}

/* ---------- key encodings ---------- */
static jz_err encode_runtime_primary_key_part(jz_bytes *key, const groove_Value *value);

static jz_err get_field(groove_RecordDescriptor d, const u8 *raw, u32 len, usize idx, groove_Value *out) {
    groove_BorrowedRecord r = { jz_slice_of(raw, len), d };
    return groove__records__BorrowedRecord__get_idx(&r, (u32)idx, out);
}

jz_err groove__ivm__runtime__windows__encoded_record_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out) {
    jz_bytes key = {0};
    for (u32 i = 0; i < n; i++) {
        groove_Value value;
        jz_err e = get_field(d, raw, len, idx[i], &value);
        if (!JZ_IS_ERR(e)) { e = encode_runtime_primary_key_part(&key, &value); groove_Value_drop(&value); }
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
    }
    *out = key;
    return JZ_OK;
}

static void push_be64(jz_bytes *key, u64 v) { u8 b[8]; for (int i = 7; i >= 0; i--) { b[i] = (u8)v; v >>= 8; } jz_bytes_push(key, b, 8); }
static void push_be32(jz_bytes *key, u32 v) { u8 b[4] = { (u8)(v >> 24), (u8)(v >> 16), (u8)(v >> 8), (u8)v }; jz_bytes_push(key, b, 4); }

static jz_err encode_identity_key_part(jz_bytes *key, const groove_Value *value) {
    switch (value->tag) {
    case groove_Value_Tuple:
        jz_bytes_push_u8(key, 11);
        JZ_VEC_FOR(const groove_Value, v, value->v.vals) JZ_TRY(encode_identity_key_part(key, v));
        return JZ_OK;
    case groove_Value_Nullable:
        jz_bytes_push_u8(key, 12);
        if (!value->v.nullable) { jz_bytes_push_u8(key, 0); return JZ_OK; }
        jz_bytes_push_u8(key, 1);
        return encode_identity_key_part(key, value->v.nullable);
    case groove_Value_Array:
        jz_bytes_push_u8(key, 16);
        push_be64(key, value->v.vals.len);
        JZ_VEC_FOR(const groove_Value, v, value->v.vals) JZ_TRY(encode_identity_key_part(key, v));
        return JZ_OK;
    case groove_Value_Enum: {
        /* The column's schema fixes each case's payload descriptor. */
        jz_bytes_push_u8(key, 17);
        push_be32(key, value->v.enm.tag);
        const groove_OwnedRecord *payload = &value->v.enm.record;
        u32 fields = groove__records__RecordDescriptor__len(payload->descriptor);
        push_be64(key, fields);
        groove_BorrowedRecord br = { jz_bytes_slice(&payload->raw), payload->descriptor };
        for (u32 i = 0; i < fields; i++) {
            groove_Value v;
            JZ_TRY(groove__records__BorrowedRecord__get_idx(&br, i, &v));
            jz_err e = encode_identity_key_part(key, &v);
            groove_Value_drop(&v);
            JZ_TRY(e);
        }
        return JZ_OK;
    }
    case groove_Value_Large: {
        /* Identified by content, not by where its chunks live. */
        const groove_LargeValueRef *large = value->v.large;
        jz_bytes_push_u8(key, 18);
        jz_bytes_push_u8(key, large->kind); /* String 0, Bytes 1, Json 2 */
        jz_bytes_push_u8(key, large->format_version);
        jz_bytes_push(key, large->logical_hash, 32);
        push_be64(key, large->byte_length);
        if (large->has_utf16_length) { jz_bytes_push_u8(key, 1); push_be64(key, large->utf16_length); }
        else jz_bytes_push_u8(key, 0);
        push_be64(key, large->edit_tail.len);
        JZ_VEC_FOR(const groove_ReplaceEdit_m, edit, large->edit_tail) {
            push_be64(key, edit->offset);
            push_be64(key, edit->delete_length);
            push_be64(key, edit->utf16_offset);
            push_be64(key, edit->delete_utf16_length);
            push_be64(key, edit->insert_utf16_length);
            groove__ivm__runtime__key_encoding__encode_ordered_bytes(key, edit->insert_bytes.ptr, edit->insert_bytes.len);
        }
        return JZ_OK;
    }
    default:
        return encode_runtime_primary_key_part(key, value);
    }
}

/* Terminal root identity key of field_indices (#3309): keys every value a
 * record can hold. A process-local opaque key, not a durable codec. */
jz_err groove__ivm__runtime__windows__encoded_identity_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out) {
    jz_bytes key = {0};
    for (u32 i = 0; i < n; i++) {
        groove_Value value;
        jz_err e = get_field(d, raw, len, idx[i], &value);
        if (!JZ_IS_ERR(e)) { e = encode_identity_key_part(&key, &value); groove_Value_drop(&value); }
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
    }
    *out = key;
    return JZ_OK;
}

jz_err groove__ivm__runtime__windows__encoded_arrangement_key_part(groove_RecordDescriptor d, const u8 *raw, u32 len, const usize *idx, u32 n, jz_bytes *out) {
    jz_bytes key = {0};
    for (u32 i = 0; i < n; i++) {
        groove_Value value;
        jz_err e = get_field(d, raw, len, idx[i], &value);
        if (!JZ_IS_ERR(e)) { e = groove__ivm__runtime__key_encoding__encode_key_part(&key, &value); groove_Value_drop(&value); }
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
    }
    *out = key;
    return JZ_OK;
}

static jz_err encode_runtime_primary_key_part(jz_bytes *key, const groove_Value *value) {
    switch (value->tag) {
    case groove_Value_Tuple:
        jz_bytes_push_u8(key, 11);
        JZ_VEC_FOR(const groove_Value, v, value->v.vals) JZ_TRY(encode_runtime_primary_key_part(key, v));
        return JZ_OK;
    case groove_Value_Nullable:
        jz_bytes_push_u8(key, 12);
        if (!value->v.nullable) { jz_bytes_push_u8(key, 0); return JZ_OK; }
        jz_bytes_push_u8(key, 1);
        return encode_runtime_primary_key_part(key, value->v.nullable);
    case groove_Value_Record: {
        /* Match OwnedRecord identity using both its descriptor and bytes,
         * never process-local intern IDs. */
        jz_bytes_push_u8(key, 15);
        jz_bytes descriptor;
        JZ_TRY(groove__records__encode_record_descriptor(value->v.record.descriptor, &descriptor));
        groove__ivm__runtime__key_encoding__encode_ordered_bytes(key, descriptor.ptr, descriptor.len);
        jz_bytes_drop(&descriptor);
        groove__ivm__runtime__key_encoding__encode_ordered_bytes(key, value->v.record.raw.ptr, value->v.record.raw.len);
        return JZ_OK;
    }
    default:
        return groove__ivm__runtime__key_encoding__encode_scalar_key_part(key, value);
    }
}
