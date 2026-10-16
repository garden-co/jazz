/* Port of crates/jazz/layers/node/src/node/merge_ops.rs (non-test) */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

typedef __int128 i128;
static void value_drop_v(void *p) { groove__records__values__Value__drop(p); }

jz_err jazz_node__node__merge_ops__counter_to_i128(const groove_Value *value, i128 *out) {
    switch (value->tag) {
    case GROOVE_V_U8: case GROOVE_V_U16: case GROOVE_V_U32: case GROOVE_V_U64: *out = (i128)value->v.u; return JZ_OK;
    case GROOVE_V_I32: case GROOVE_V_I64: *out = (i128)value->v.i; return JZ_OK;
    case GROOVE_V_NULLABLE:
        if (!value->v.nullable) { *out = 0; return JZ_OK; }
        return jazz_node__node__merge_ops__counter_to_i128(value->v.nullable, out);
    default: return JZN_INVALID_STORED("counter value must be integer");
    }
}

static jz_err jazz_node__node__merge_ops__counter_range(const groove_ValueType *t, u32 *width, i128 *min, i128 *max) {
    switch (t->tag) {
    case GROOVE_VT_U8: *width = 8; *min = 0; *max = 0xff; return JZ_OK;
    case GROOVE_VT_U16: *width = 16; *min = 0; *max = 0xffff; return JZ_OK;
    case GROOVE_VT_U32: *width = 32; *min = 0; *max = 0xffffffffu; return JZ_OK;
    case GROOVE_VT_U64: *width = 64; *min = 0; *max = (i128)UINT64_MAX; return JZ_OK;
    case GROOVE_VT_I32: *width = 32; *min = INT32_MIN; *max = INT32_MAX; return JZ_OK;
    case GROOVE_VT_I64: *width = 64; *min = INT64_MIN; *max = INT64_MAX; return JZ_OK;
    case GROOVE_VT_NULLABLE: return jazz_node__node__merge_ops__counter_range(t->v.inner, width, min, max);
    default: return JZN_INVALID_STORED("counter strategy requires integer column");
    }
}

static jz_err jazz_node__node__merge_ops__counter_bits(const groove_ValueType *t, i128 value, groove_Value *out) {
    switch (t->tag) {
    case GROOVE_VT_U8: out->tag = GROOVE_V_U8; out->v.u = (u8)value; return JZ_OK;
    case GROOVE_VT_U16: out->tag = GROOVE_V_U16; out->v.u = (u16)value; return JZ_OK;
    case GROOVE_VT_U32: out->tag = GROOVE_V_U32; out->v.u = (u32)value; return JZ_OK;
    case GROOVE_VT_U64: out->tag = GROOVE_V_U64; out->v.u = (u64)value; return JZ_OK;
    case GROOVE_VT_I32: out->tag = GROOVE_V_I32; out->v.i = (i32)value; return JZ_OK;
    case GROOVE_VT_I64: out->tag = GROOVE_V_I64; out->v.i = (i64)value; return JZ_OK;
    case GROOVE_VT_NULLABLE: return jazz_node__node__merge_ops__counter_bits(t->v.inner, value, out);
    default: return JZN_INVALID_STORED("counter strategy requires integer column");
    }
}

jz_err jazz_node__node__merge_ops__counter_op(const groove_ValueType *t, i128 delta, groove_Value *op, bool *negative) {
    u32 width; i128 mn, mx;
    JZ_TRY(jazz_node__node__merge_ops__counter_range(t, &width, &mn, &mx));
    i128 span = (i128)1 << width;
    if (delta < -span || delta >= span) return JZN_INVALID_MERGEABLE("counter delta exceeds the span of the column's type");
    JZ_TRY(jazz_node__node__merge_ops__counter_bits(t, delta, op));
    *negative = delta < 0;
    return JZ_OK;
}

jz_err jazz_node__node__merge_ops__counter_op_delta(const groove_ValueType *t, const groove_Value *op, bool negative, i128 *out) {
    u32 width; i128 mn, mx;
    JZ_TRY(jazz_node__node__merge_ops__counter_range(t, &width, &mn, &mx));
    i128 span = (i128)1 << width;
    i128 raw;
    JZ_TRY(jazz_node__node__merge_ops__counter_to_i128(op, &raw));
    i128 low = raw % span; if (low < 0) low += span;
    *out = negative ? low - span : low;
    return JZ_OK;
}

jz_err jazz_node__node__merge_ops__counter_after_op(const groove_ValueType *t, const groove_Value *previous, const groove_Value *op, bool negative,
                                                   bool *some, groove_Value *out) {
    u32 width; i128 mn, mx;
    JZ_TRY(jazz_node__node__merge_ops__counter_range(t, &width, &mn, &mx));
    i128 prev, delta;
    JZ_TRY(jazz_node__node__merge_ops__counter_to_i128(previous, &prev));
    JZ_TRY(jazz_node__node__merge_ops__counter_op_delta(t, op, negative, &delta));
    i128 sum = prev + delta;
    if (sum < mn || sum > mx) { *some = false; return JZ_OK; }
    *some = true;
    return jazz_node__node__merge_ops__counter_bits(t, sum, out);
}

/* counter_ordinals: per column, Some(ordinal) when its strategy is Counter */
static void counter_ordinals(const jazz_model_TableSchema *table, i64 *out /* -1 = None */) {
    i64 next = 0;
    for (u32 i = 0; i < table->columns.len; i++)
        out[i] = jazz_model__schema__TableSchema__merge_strategy(table, &table->columns.ptr[i].name) == JAZZ_MERGE_COUNTER ? next++ : -1;
}

bool jazz_node__node__merge_ops__counter_sign(const jazz_model_TableSchema *table, const u8 *signs, u32 nsigns, usize index) {
    if (index >= table->columns.len) return false;
    i64 *ords = jz_alloc(sizeof(i64) * (table->columns.len + 1));
    counter_ordinals(table, ords);
    i64 bit = ords[index];
    jz_free(ords);
    if (bit < 0) return false;
    if ((u64)(bit / 8) >= nsigns) return false;
    return (signs[bit / 8] & (1u << (bit % 8))) != 0;
}

void jazz_node__node__merge_ops__clear_counter_signs(groove_Value *values, const groove_RecordDescriptor *descriptor) {
    u32 field;
    if (groove__records__RecordDescriptor__field_index(descriptor, JAZZ_COUNTER_SIGNS_FIELD, &field)) {
        groove__records__values__Value__drop(&values[field]);
        values[field].tag = GROOVE_V_BYTES;
        values[field].v.bytes = (jz_bytes){0};
    }
}

jz_err jazz_node__node__merge_ops__validate_counter_signs(const jazz_model_TableSchema *table, const u8 *signs, u32 n) {
    i64 *ords = jz_alloc(sizeof(i64) * (table->columns.len + 1));
    counter_ordinals(table, ords);
    usize counters = 0;
    for (u32 i = 0; i < table->columns.len; i++) if (ords[i] >= 0) counters++;
    jz_free(ords);
    bool canonical = (n == 0 || signs[n - 1] != 0) && n <= (counters + 7) / 8;
    for (u32 byte_index = 0; canonical && byte_index < n; byte_index++)
        for (u32 bit = 0; bit < 8; bit++)
            if ((signs[byte_index] & (1u << bit)) != 0 && byte_index * 8 + bit >= counters) { canonical = false; break; }
    if (canonical) return JZ_OK;
    return JZN_INVALID_MERGEABLE("counter signs must name only counter columns, without trailing zero bytes");
}

static void bytes_drop_v(void *p) { jz_bytes_drop(p); }
static const jz_btree_desc desc_elements = { sizeof(jz_bytes), sizeof(groove_Value), jz_cmp_bytes, bytes_drop_v, value_drop_v };

static jz_err jazz_node__node__merge_ops__gset_elements(const groove_ValueType *column_type, groove_ValueVec values /* moved */, jz_btree *elements) {
    if (column_type->tag != GROOVE_VT_ARRAY) {
        jz_vec_drop(&values, sizeof(groove_Value), value_drop_v);
        return JZN_INVALID_STORED("g-set merge strategy requires an array column");
    }
    groove_ValueType et; groove__records__values__ValueType__clone(&et, column_type->v.inner);
    groove_RecordDescriptor *descriptor = groove__records__RecordDescriptor__new_single("element", et);
    jz_err e = JZ_OK;
    u32 i = 0;
    for (; i < values.len; i++) {
        jz_bytes key;
        e = groove__records__RecordDescriptor__create(descriptor, &values.ptr[i], 1, &key);
        if (JZ_IS_ERR(e)) break;
        bool inserted;
        groove_Value *slot = jz_btree_entry(elements, &key, &inserted);
        if (inserted) *slot = values.ptr[i];
        else { jz_bytes_drop(&key); groove__records__values__Value__drop(&values.ptr[i]); }
    }
    for (; i < values.len; i++) groove__records__values__Value__drop(&values.ptr[i]);
    jz_free(values.ptr);
    groove__records__RecordDescriptor__drop_box(descriptor);
    return e;
}

static groove_ValueVec jazz_node__node__merge_ops__gset_values(const groove_Value *value) {
    groove_ValueVec out = {0};
    if (!value) return out;
    if (value->tag == GROOVE_V_ARRAY) {
        jz_vec_clone(&out, &value->v.vals, sizeof(groove_Value), groove__records__values__Value__clone);
        return out;
    }
    if (value->tag == GROOVE_V_NULLABLE && value->v.nullable) return jazz_node__node__merge_ops__gset_values(value->v.nullable);
    return out;
}

static void elements_into_array(jz_btree *elements, groove_Value *out, const jz_btree *exclude) {
    out->tag = GROOVE_V_ARRAY;
    out->v.vals = (groove_ValueVec){0};
    jz_bytes k; groove_Value v;
    while (jz_btree_pop_first(elements, &k, &v)) {
        if (exclude && jz_btree_contains(exclude, &k)) groove__records__values__Value__drop(&v);
        else JZ_VEC_PUSH(out->v.vals, v);
        jz_bytes_drop(&k);
    }
    jz_btree_drop(elements);
}

jz_err jazz_node__node__merge_ops__gset_union(const groove_ValueType *t, const groove_Value *left, const groove_Value *right, groove_Value *out) {
    jz_btree elements; jz_btree_init(&elements, &desc_elements);
    jz_err e = jazz_node__node__merge_ops__gset_elements(t, jazz_node__node__merge_ops__gset_values(left), &elements);
    if (!JZ_IS_ERR(e)) e = jazz_node__node__merge_ops__gset_elements(t, jazz_node__node__merge_ops__gset_values(right), &elements);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&elements); return e; }
    elements_into_array(&elements, out, NULL);
    return JZ_OK;
}

jz_err jazz_node__node__merge_ops__gset_added(const groove_ValueType *t, const groove_Value *written, const groove_Value *base, groove_Value *out) {
    jz_btree known, added;
    jz_btree_init(&known, &desc_elements);
    jz_btree_init(&added, &desc_elements);
    jz_err e = jazz_node__node__merge_ops__gset_elements(t, jazz_node__node__merge_ops__gset_values(base), &known);
    if (!JZ_IS_ERR(e)) e = jazz_node__node__merge_ops__gset_elements(t, jazz_node__node__merge_ops__gset_values(written), &added);
    if (JZ_IS_ERR(e)) { jz_btree_drop(&known); jz_btree_drop(&added); return e; }
    elements_into_array(&added, out, &known);
    jz_btree_drop(&known);
    return JZ_OK;
}

static void value_clone_v(void *d, const void *s) { groove__records__values__Value__clone(d, s); }
static void str_clone_v(void *d, const void *s) { *(jz_str *)d = jz_str_clone(s); }

jz_err jazz_node__node__merge_ops__split_merge_ops(const jazz_model_TableSchema *table_schema, const jz_btree *authored, const jz_btree *base,
                                                  jz_btree *cells, bool *has_image, jz_btree *image, jz_bytes *signs) {
    *has_image = false;
    *signs = (jz_bytes){0};
    i64 *ords = jz_alloc(sizeof(i64) * (table_schema->columns.len + 1));
    counter_ordinals(table_schema, ords);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < table_schema->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table_schema->columns.ptr[i];
        jazz_model_MergeStrategy strategy = jazz_model__schema__TableSchema__merge_strategy(table_schema, &column->name);
        if (strategy == JAZZ_MERGE_LWW || !jz_btree_contains(authored, &column->name)) continue;
        const groove_Value *w = jz_btree_get(cells, &column->name);
        if (!w) continue;
        groove_Value written; groove__records__values__Value__clone(&written, w);
        if (!*has_image) { jz_btree_clone(image, cells, str_clone_v, value_clone_v); *has_image = true; }
        const groove_Value *base_value = jz_btree_get(base, &column->name);
        switch (strategy) {
        case JAZZ_MERGE_COUNTER: {
            i128 wv, bv = 0;
            if (JZ_IS_ERR(e = jazz_node__node__merge_ops__counter_to_i128(&written, &wv))) goto fail_w;
            if (base_value && JZ_IS_ERR(e = jazz_node__node__merge_ops__counter_to_i128(base_value, &bv))) goto fail_w;
            groove_Value op; bool negative;
            if (JZ_IS_ERR(e = jazz_node__node__merge_ops__counter_op(&column->column_type, wv - bv, &op, &negative))) goto fail_w;
            if (negative) {
                if (ords[i] < 0) { groove__records__values__Value__drop(&op); e = JZN_INVALID_STORED("counter column must have a sign ordinal"); goto fail_w; }
                u64 bit = (u64)ords[i];
                while (signs->len <= bit / 8) jz_bytes_push_u8(signs, 0);
                signs->ptr[bit / 8] |= (u8)(1u << (bit % 8));
            }
            jz_str k = jz_str_clone(&column->name);
            jz_btree_insert(cells, &k, &op, NULL);
            break;
        }
        case JAZZ_MERGE_GSET: {
            groove_Value u, a;
            if (JZ_IS_ERR(e = jazz_node__node__merge_ops__gset_union(&column->column_type, base_value, &written, &u))) goto fail_w;
            jz_str k = jz_str_clone(&column->name);
            jz_btree_insert(image, &k, &u, NULL);
            if (JZ_IS_ERR(e = jazz_node__node__merge_ops__gset_added(&column->column_type, &written, base_value, &a))) goto fail_w;
            jz_str k2 = jz_str_clone(&column->name);
            jz_btree_insert(cells, &k2, &a, NULL);
            break;
        }
        case JAZZ_MERGE_LWW: break;
        }
        groove__records__values__Value__drop(&written);
        continue;
    fail_w:
        groove__records__values__Value__drop(&written);
        goto fail;
    }
    jz_free(ords);
    return JZ_OK;
fail:
    jz_free(ords);
    if (*has_image) jz_btree_drop(image);
    jz_bytes_drop(signs);
    return e;
}

jz_err jazz_node__node__merge_ops__apply_merge_op(jazz_model_MergeStrategy strategy, const groove_ValueType *t, const groove_Value *previous,
                                                 const groove_Value *op, bool negative, groove_Value *out) {
    switch (strategy) {
    case JAZZ_MERGE_COUNTER: {
        bool some;
        JZ_TRY(jazz_node__node__merge_ops__counter_after_op(t, previous, op, negative, &some, out));
        if (!some) return JZN_INVALID_STORED("accepted counter op leaves the column's range");
        return JZ_OK;
    }
    case JAZZ_MERGE_GSET: return jazz_node__node__merge_ops__gset_union(t, previous, op, out);
    case JAZZ_MERGE_LWW:
    default: groove__records__values__Value__clone(out, op); return JZ_OK;
    }
}

static void jazz_node__node__merge_ops__counter_value_without_null(const groove_Value *value, groove_Value *out) {
    if (value->tag == GROOVE_V_NULLABLE && value->v.nullable) { jazz_node__node__merge_ops__counter_value_without_null(value->v.nullable, out); return; }
    groove__records__values__Value__clone(out, value);
}

jz_err jazz_node__node__merge_ops__apply_pending_merge_op(jazz_model_MergeStrategy strategy, const groove_ValueType *t, const groove_Value *previous,
                                                         const groove_Value *op, bool negative, groove_Value *out) {
    if (strategy == JAZZ_MERGE_COUNTER) {
        bool some;
        JZ_TRY(jazz_node__node__merge_ops__counter_after_op(t, previous, op, negative, &some, out));
        if (!some) jazz_node__node__merge_ops__counter_value_without_null(previous, out);
        return JZ_OK;
    }
    return jazz_node__node__merge_ops__apply_merge_op(strategy, t, previous, op, negative, out);
}
