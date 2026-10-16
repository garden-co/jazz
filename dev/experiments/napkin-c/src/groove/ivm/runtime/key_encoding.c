/* groove::ivm::runtime::key_encoding: index scan bounds, comparison semantics
 * and order-preserving key encoding. */
#include "mod.h"

#define BR_GET(kind, rec, i, out) JZ_TRY(groove__records__BorrowedRecord__get_##kind((rec), (i), (out)))
#define BR_GETN(kind, rec, i, some, out) JZ_TRY(groove__records__BorrowedRecord__get_nullable_##kind((rec), (i), (some), (out)))

/* ---------- PredicateRecord for BorrowedRecord ---------- */
static jz_err br_value(const void *self, const char *field, u32 len, groove_Value *out) {
    const groove_BorrowedRecord *r = self;
    u32 index;
    if (!groove__ivm__runtime__record_projection__resolve_field_name(r->descriptor, field, len, &index))
        return groove__records__Error__field_not_found(field, len);
    return groove__records__BorrowedRecord__get_idx(r, index, out);
}
static jz_err br_literal_ordering(const void *self, const char *field, u32 len, const groove_LiteralValue *value, groove_FieldLiteralOrdering *out) {
    const groove_BorrowedRecord *r = self;
    u32 index;
    if (!groove__ivm__runtime__record_projection__resolve_field_name(r->descriptor, field, len, &index))
        return groove__ivm__runtime__IvmRuntimeError__with_str(GROOVE_IVM_GraphFieldNotFound, field, len);
    return groove__ivm__runtime__key_encoding__record_field_literal_ordering(r, index, value, out);
}
const groove_PredicateRecordVt groove__ivm__runtime__key_encoding__BorrowedRecord__PredicateRecord__VT = { br_value, br_literal_ordering };

static jz_err resolved_record_value(groove_PredicateRecord record, const char *field, u32 len, groove_Value *out) {
    return record.vt->value(record.self, field, len, out);
}

/* ---------- tagged scalar writers ---------- */
static void push_be(jz_bytes *key, u64 v, u32 n) {
    u8 b[8];
    for (u32 i = 0; i < n; i++) b[i] = (u8)(v >> (8 * (n - 1 - i)));
    jz_bytes_push(key, b, n);
}
static jz_err put_u8(jz_bytes *key, u8 v) { u8 b[2] = {0, v}; jz_bytes_push(key, b, 2); return JZ_OK; }
static jz_err put_u32(jz_bytes *key, u32 v) { jz_bytes_push_u8(key, 2); push_be(key, v, 4); return JZ_OK; }
static jz_err put_u64(jz_bytes *key, u64 v) { jz_bytes_push_u8(key, 3); push_be(key, v, 8); return JZ_OK; }
static jz_err put_u48(jz_bytes *key, u64 v) {
    jz_bytes_push_u8(key, 16);
    u8 b[6];
    JZ_TRY(groove__records__u48_be_bytes(v, b));
    jz_bytes_push(key, b, 6);
    return JZ_OK;
}
static jz_err put_i32(jz_bytes *key, i32 v) { jz_bytes_push_u8(key, 14); push_be(key, groove__ivm__runtime__key_encoding__order_preserving_i32_bits(v), 4); return JZ_OK; }
static jz_err put_i64(jz_bytes *key, i64 v) { jz_bytes_push_u8(key, 13); push_be(key, groove__ivm__runtime__key_encoding__order_preserving_i64_bits(v), 8); return JZ_OK; }
static u64 order_preserving_f64_bits(f64 value) {
    u64 bits; memcpy(&bits, &value, 8);
    /* Flip positive signs and invert negatives; the resulting unsigned integer
     * sorts like IEEE numeric order for non-NaN values. */
    if ((bits & (1ull << 63)) == 0) return bits ^ (1ull << 63);
    return ~bits;
}
static jz_err put_f64(jz_bytes *key, f64 v) { jz_bytes_push_u8(key, 4); push_be(key, order_preserving_f64_bits(v), 8); return JZ_OK; }
static jz_err put_bool(jz_bytes *key, bool v) { u8 b[2] = {5, v ? 1 : 0}; jz_bytes_push(key, b, 2); return JZ_OK; }
static jz_err put_bytes_tagged(jz_bytes *key, u8 tag, const u8 *p, u32 n) {
    jz_bytes_push_u8(key, tag);
    groove__ivm__runtime__key_encoding__encode_ordered_bytes(key, p, n);
    return JZ_OK;
}
static jz_err put_str(jz_bytes *key, jz_slice s) { return put_bytes_tagged(key, 6, s.ptr, s.len); }
static jz_err put_bytes(jz_bytes *key, jz_slice s) { return put_bytes_tagged(key, 7, s.ptr, s.len); }
static jz_err put_uuid(jz_bytes *key, jz_uuid u) { jz_bytes_push_u8(key, 10); jz_bytes_push(key, u.b, 16); return JZ_OK; }
static jz_err non_nan(f64 v) {
    if (v != v) return groove__records__Error__invalid_f64_nan();
    return JZ_OK;
}
/* Value::Nullable framing: 8 for null, else 9 followed by the value. */
static void put_null_flag(jz_bytes *key, bool some) { jz_bytes_push_u8(key, some ? 9 : 8); }

u64 groove__ivm__runtime__key_encoding__order_preserving_i64_bits(i64 v) { return ((u64)v) ^ (1ull << 63); }
u32 groove__ivm__runtime__key_encoding__order_preserving_i32_bits(i32 v) { return ((u32)v) ^ (1u << 31); }

void groove__ivm__runtime__key_encoding__encode_ordered_bytes(jz_bytes *key, const u8 *p, u32 n) {
    /* 0x00 terminates the byte string; embedded NULs are escaped as 00 ff. */
    for (u32 i = 0; i < n; i++) {
        if (p[i] == 0) { u8 esc[2] = {0, 0xff}; jz_bytes_push(key, esc, 2); }
        else jz_bytes_push_u8(key, p[i]);
    }
    u8 term[2] = {0, 0};
    jz_bytes_push(key, term, 2);
}

/* ---------- record field key parts ---------- */
jz_err groove__ivm__runtime__key_encoding__primary_key_value_bytes(groove_RecordDescriptor d, jz_slice record, const u32 *pk, u32 n, jz_bytes *out) {
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < n; i++) {
        jz_err e = groove__ivm__runtime__key_encoding__encode_record_field_key_part(out, d, record, pk[i]);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(out); return e; }
    }
    return JZ_OK;
}

jz_err groove__ivm__runtime__key_encoding__primary_key_field_indices(const groove_TableSchema *table, groove_RecordDescriptor d, groove_U32Vec *out) {
    if (!table->has_primary_key) return GROOVE_IVM_ERR_S(GROOVE_IVM_MissingPrimaryKey, &table->name);
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < table->primary_key.columns.len; i++) {
        const jz_str *column = &table->primary_key.columns.ptr[i].column;
        u32 idx;
        if (!groove__records__RecordDescriptor__field_index(d, column->ptr, column->len, &idx)) {
            jz_vec_drop(out, sizeof(u32), NULL);
            return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, column);
        }
        JZ_VEC_PUSH(*out, idx);
    }
    return JZ_OK;
}

static jz_err encode_named(jz_bytes *key, groove_RecordDescriptor d, jz_slice record, u32 field_idx) {
    groove_Value v;
    JZ_TRY(groove__ivm__runtime__key_encoding__named_field_value(d, record, field_idx, &v));
    jz_err e = groove__ivm__runtime__key_encoding__encode_key_part(key, &v);
    groove_Value_drop(&v);
    return e;
}

jz_err groove__ivm__runtime__key_encoding__encode_record_field_key_part(jz_bytes *key, groove_RecordDescriptor d, jz_slice record, u32 field_idx) {
    if (field_idx >= groove__records__RecordDescriptor__len(d))
        return groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, field_idx);
    const groove_DescriptorField *field = groove__records__RecordDescriptor__field(d, field_idx);
    groove_BorrowedRecord b = groove__records__BorrowedRecord__new(record, d);
    u32 i = field_idx;
    /* Typed reads skip materializing a Value; the bytes are exactly those
     * encode_key_part writes for the same value. */
    switch (field->value_type.tag) {
    case groove_ValueType_U8: { u8 v; BR_GET(u8, &b, i, &v); return put_u8(key, v); }
    case groove_ValueType_U32: { u32 v; BR_GET(u32, &b, i, &v); return put_u32(key, v); }
    case groove_ValueType_U64: { u64 v; BR_GET(u64, &b, i, &v); return put_u64(key, v); }
    case groove_ValueType_U48: { u64 v; BR_GET(u48, &b, i, &v); return put_u48(key, v); }
    case groove_ValueType_I32: { i32 v; BR_GET(i32, &b, i, &v); return put_i32(key, v); }
    case groove_ValueType_I64: { i64 v; BR_GET(i64, &b, i, &v); return put_i64(key, v); }
    case groove_ValueType_F64: { f64 v; BR_GET(f64, &b, i, &v); JZ_TRY(non_nan(v)); return put_f64(key, v); }
    case groove_ValueType_Bool: { bool v; BR_GET(bool, &b, i, &v); return put_bool(key, v); }
    case groove_ValueType_String: { jz_slice v; BR_GET(str, &b, i, &v); return put_str(key, v); }
    case groove_ValueType_Bytes: { jz_slice v; BR_GET(bytes, &b, i, &v); return put_bytes(key, v); }
    case groove_ValueType_Uuid: { jz_uuid v; BR_GET(uuid, &b, i, &v); return put_uuid(key, v); }
    case groove_ValueType_EnumTag: { u8 v; BR_GET(enum, &b, i, &v); return put_u8(key, v); }
    case groove_ValueType_Nullable: {
        bool some;
        switch (field->value_type.v.inner->tag) {
        case groove_ValueType_U64: { u64 v; BR_GETN(u64, &b, i, &some, &v); put_null_flag(key, some); return some ? put_u64(key, v) : JZ_OK; }
        case groove_ValueType_I64: { i64 v; BR_GETN(i64, &b, i, &some, &v); put_null_flag(key, some); return some ? put_i64(key, v) : JZ_OK; }
        case groove_ValueType_I32: { i32 v; BR_GETN(i32, &b, i, &some, &v); put_null_flag(key, some); return some ? put_i32(key, v) : JZ_OK; }
        case groove_ValueType_F64: {
            f64 v; BR_GETN(f64, &b, i, &some, &v);
            if (some) JZ_TRY(non_nan(v));
            put_null_flag(key, some);
            return some ? put_f64(key, v) : JZ_OK;
        }
        case groove_ValueType_String: { jz_slice v; BR_GETN(string, &b, i, &some, &v); put_null_flag(key, some); return some ? put_str(key, v) : JZ_OK; }
        case groove_ValueType_Bytes: { jz_slice v; BR_GETN(bytes, &b, i, &some, &v); put_null_flag(key, some); return some ? put_bytes(key, v) : JZ_OK; }
        case groove_ValueType_Uuid: { jz_uuid v; BR_GETN(uuid, &b, i, &some, &v); put_null_flag(key, some); return some ? put_uuid(key, v) : JZ_OK; }
        case groove_ValueType_EnumTag: { u8 v; BR_GETN(enum, &b, i, &some, &v); put_null_flag(key, some); return some ? put_u8(key, v) : JZ_OK; }
        default: return encode_named(key, d, record, field_idx);
        }
    }
    default:
        return encode_named(key, d, record, field_idx);
    }
}

static void bytes_vec_drop(JZ_VEC(jz_bytes) *v) { jz_vec_drop(v, sizeof(jz_bytes), groove_ivm_rt_bytes_drop_v); }

jz_err groove__ivm__runtime__key_encoding__record_field_key_parts(groove_RecordDescriptor d, jz_slice record, u32 field_idx, groove_BytesVec *out) {
    if (field_idx >= groove__records__RecordDescriptor__len(d))
        return groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, field_idx);
    const groove_DescriptorField *field = groove__records__RecordDescriptor__field(d, field_idx);
    memset(out, 0, sizeof(*out));
    if (!groove__ivm__runtime__key_encoding__expands_to_key_parts(&field->value_type)) {
        jz_bytes key = {0};
        jz_err e = groove__ivm__runtime__key_encoding__encode_record_field_key_part(&key, d, record, field_idx);
        if (JZ_IS_ERR(e)) { jz_bytes_drop(&key); return e; }
        JZ_VEC_PUSH(*out, key);
        return JZ_OK;
    }
    groove_Value value;
    JZ_TRY(groove__ivm__runtime__key_encoding__named_field_value(d, record, field_idx, &value));
    if (field->value_type.tag == groove_ValueType_Array && value.tag != groove_Value_Array) {
        groove_Value_drop(&value);
        if (field->has_name) return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &field->name);
        return GROOVE_IVM_ERR_C(GROOVE_IVM_GraphFieldNotFound, "<unnamed>");
    }
    groove_ValueVec parts;
    groove__ivm__runtime__key_encoding__key_parts(value, &parts);
    jz_err e = JZ_OK;
    for (u32 i = 0; i < parts.len && !JZ_IS_ERR(e); i++) {
        jz_bytes key = {0};
        e = groove__ivm__runtime__key_encoding__encode_key_part(&key, &parts.ptr[i]);
        if (JZ_IS_ERR(e)) jz_bytes_drop(&key); else JZ_VEC_PUSH(*out, key);
    }
    jz_vec_drop(&parts, sizeof(groove_Value), groove_Value_drop_v);
    if (JZ_IS_ERR(e)) bytes_vec_drop((void *)out);
    return e;
}

/* Whether a field of this type is indexed once per array element. */
bool groove__ivm__runtime__key_encoding__expands_to_key_parts(const groove_ValueType *t) {
    switch (t->tag) {
    case groove_ValueType_Array: return true;
    case groove_ValueType_Nullable: return t->v.inner->tag == groove_ValueType_Array;
    default: return false;
    }
}

static groove_Value wrap_nullable(groove_Value v) {
    groove_Value out;
    out.tag = groove_Value_Nullable;
    out.v.nullable = jz_alloc(sizeof(groove_Value));
    *out.v.nullable = v;
    return out;
}

/* The values a record is keyed under for one field: each element of an
 * array (nullable arrays keep their elements nullable), else the value. */
void groove__ivm__runtime__key_encoding__key_parts(groove_Value value, groove_ValueVec *out) {
    memset(out, 0, sizeof(*out));
    if (value.tag == groove_Value_Array) { *out = value.v.vals; return; }
    if (value.tag == groove_Value_Nullable && value.v.nullable) {
        groove_Value inner = *value.v.nullable;
        jz_free(value.v.nullable);
        if (inner.tag == groove_Value_Array) {
            for (u32 i = 0; i < inner.v.vals.len; i++) JZ_VEC_PUSH(*out, wrap_nullable(inner.v.vals.ptr[i]));
            jz_vec_drop(&inner.v.vals, sizeof(groove_Value), NULL);
        } else {
            JZ_VEC_PUSH(*out, wrap_nullable(inner));
        }
        return;
    }
    JZ_VEC_PUSH(*out, value);
}

/* Read field `field_idx` through its name, so a renamed or aliased field
 * resolves exactly as predicates resolve it. */
jz_err groove__ivm__runtime__key_encoding__named_field_value(groove_RecordDescriptor d, jz_slice record, u32 field_idx, groove_Value *out) {
    if (field_idx >= groove__records__RecordDescriptor__len(d))
        return groove__ivm__runtime__IvmRuntimeError__with_usize(GROOVE_IVM_GraphFieldIndexOutOfBounds, field_idx);
    const groove_DescriptorField *field = groove__records__RecordDescriptor__field(d, field_idx);
    if (!field->has_name) return GROOVE_IVM_ERR_C(GROOVE_IVM_GraphFieldNotFound, "<unnamed>");
    u32 idx;
    if (!groove__ivm__runtime__record_projection__resolve_field_name(d, field->name.ptr, field->name.len, &idx))
        return GROOVE_IVM_ERR_S(GROOVE_IVM_GraphFieldNotFound, &field->name);
    groove_BorrowedRecord b = groove__records__BorrowedRecord__new(record, d);
    return groove__records__BorrowedRecord__get_idx(&b, idx, out);
}

/* Every composite key of a record: `prefix` followed by one key part of each
 * field, in field order, without duplicates. */
jz_err groove__ivm__runtime__key_encoding__composite_keys(jz_bytes prefix, u32 nfields, groove_FieldValueFn field_value, void *fctx, groove_KeyEncodeFn encode, void *ectx, groove_BytesVec *out) {
    static const jz_hmap_desc SEEN = { sizeof(jz_bytes), 0, groove_ivm_rt_bytes_hash_v, groove_ivm_rt_bytes_eq_v, groove_ivm_rt_bytes_drop_v, NULL };
    JZ_VEC(jz_bytes) keys = {0};
    JZ_VEC_PUSH(keys, prefix);
    jz_hmap seen; jz_hmap_init(&seen, &SEEN);
    jz_err e = JZ_OK;
    for (u32 f = 0; f < nfields; f++) {
        groove_Value value;
        e = field_value(f, &value, fctx);
        if (JZ_IS_ERR(e)) break;
        groove_ValueVec parts;
        groove__ivm__runtime__key_encoding__key_parts(value, &parts);
        if (keys.len == 1 && parts.len == 1) {
            /* One key and one part cannot produce a duplicate. */
            e = encode(&keys.ptr[0], &parts.ptr[0], ectx);
            jz_vec_drop(&parts, sizeof(groove_Value), groove_Value_drop_v);
            if (JZ_IS_ERR(e)) break;
            continue;
        }
        if (parts.len == 0) {
            jz_vec_drop(&parts, sizeof(groove_Value), groove_Value_drop_v);
            bytes_vec_drop((void *)&keys);
            jz_hmap_drop(&seen);
            memset(out, 0, sizeof(*out));
            return JZ_OK;
        }
        JZ_VEC(jz_bytes) next_keys = {0};
        jz_vec_reserve(&next_keys, keys.len * parts.len, sizeof(jz_bytes));
        for (u32 k = 0; k < keys.len && !JZ_IS_ERR(e); k++) {
            for (u32 p = 0; p < parts.len; p++) {
                jz_bytes next = jz_bytes_clone(&keys.ptr[k]);
                e = encode(&next, &parts.ptr[p], ectx);
                if (JZ_IS_ERR(e)) { jz_bytes_drop(&next); break; }
                jz_bytes copy = jz_bytes_clone(&next);
                bool inserted;
                jz_hmap_entry(&seen, &copy, &inserted);
                if (inserted) JZ_VEC_PUSH(next_keys, next);
                else { jz_bytes_drop(&copy); jz_bytes_drop(&next); }
            }
        }
        jz_vec_drop(&parts, sizeof(groove_Value), groove_Value_drop_v);
        bytes_vec_drop((void *)&keys);
        keys.ptr = next_keys.ptr; keys.len = next_keys.len; keys.cap = next_keys.cap;
        jz_hmap_clear(&seen);
        if (JZ_IS_ERR(e)) break;
    }
    jz_hmap_drop(&seen);
    if (JZ_IS_ERR(e)) { bytes_vec_drop((void *)&keys); return e; }
    out->ptr = keys.ptr; out->len = keys.len; out->cap = keys.cap;
    return JZ_OK;
}

/* ---------- comparisons ---------- */
jz_err groove__ivm__runtime__key_encoding__compare_record_field(groove_PredicateRecord record, const char *field, u32 flen, const groove_LiteralValue *value, groove_OrderingPredicate predicate, groove_ValueComparison comparison, bool *out) {
    groove_FieldLiteralOrdering o;
    JZ_TRY(record.vt->literal_ordering(record.self, field, flen, value, &o));
    switch (o.tag) {
    case groove_FLO_Compared: *out = predicate(o.ordering); return JZ_OK;
    case groove_FLO_SqlNull: *out = false; return JZ_OK;
    case groove_FLO_Unsupported: break;
    }
    groove_Value expected;
    groove__ivm__op_types__LiteralValue__to_value(value, &expected);
    groove_Value actual;
    jz_err e = resolved_record_value(record, field, flen, &actual);
    if (JZ_IS_ERR(e)) { groove_Value_drop(&expected); return e; }
    int c = groove__ivm__runtime__key_encoding__compare_values_sql(&actual, &expected, comparison);
    *out = c != GROOVE_ORD_NONE && predicate(c);
    groove_Value_drop(&actual);
    groove_Value_drop(&expected);
    return JZ_OK;
}

groove_FieldLiteralOrdering groove__ivm__runtime__key_encoding__ordering_int(int c) {
    groove_FieldLiteralOrdering o;
    if (c == GROOVE_ORD_NONE) { o.tag = groove_FLO_SqlNull; o.ordering = 0; }
    else { o.tag = groove_FLO_Compared; o.ordering = (i8)c; }
    return o;
}
#define CMP3(a, b) ((a) < (b) ? -1 : (a) > (b) ? 1 : 0)
static int cmp_f64(f64 a, f64 b) { if (a != a || b != b) return GROOVE_ORD_NONE; return CMP3(a, b); }
static int cmp_slices(jz_slice a, jz_slice b) { int c = jz_slice_cmp(a, b); return CMP3(c, 0); }
static groove_FieldLiteralOrdering unsupported(void) { groove_FieldLiteralOrdering o = { groove_FLO_Unsupported, 0 }; return o; }
static groove_FieldLiteralOrdering sql_null(void) { groove_FieldLiteralOrdering o = { groove_FLO_SqlNull, 0 }; return o; }
#define ORD(c) groove__ivm__runtime__key_encoding__ordering_int(c)

static jz_err nullable_record_field_literal_ordering(const groove_BorrowedRecord *r, u32 i, const groove_ValueType *inner, const groove_LiteralValue *e, groove_FieldLiteralOrdering *out) {
    bool some;
    switch (inner->tag) {
    case groove_ValueType_U64: if (e->tag != groove_Lit_U64) break; { u64 v; BR_GETN(u64, r, i, &some, &v); *out = some ? ORD(CMP3(v, e->v.u)) : sql_null(); return JZ_OK; }
    case groove_ValueType_U48: if (e->tag != groove_Lit_U48) break; { u64 v; BR_GETN(u48, r, i, &some, &v); *out = some ? ORD(CMP3(v, e->v.u)) : sql_null(); return JZ_OK; }
    case groove_ValueType_I64: if (e->tag != groove_Lit_I64) break; { i64 v; BR_GETN(i64, r, i, &some, &v); *out = some ? ORD(CMP3(v, e->v.i)) : sql_null(); return JZ_OK; }
    case groove_ValueType_I32: if (e->tag != groove_Lit_I32) break; { i32 v; BR_GETN(i32, r, i, &some, &v); *out = some ? ORD(CMP3(v, (i32)e->v.i)) : sql_null(); return JZ_OK; }
    case groove_ValueType_F64: if (e->tag != groove_Lit_F64) break; {
        f64 v, ev; BR_GETN(f64, r, i, &some, &v); memcpy(&ev, &e->v.u, 8);
        *out = some ? ORD(cmp_f64(v, ev)) : sql_null(); return JZ_OK; }
    case groove_ValueType_String: if (e->tag != groove_Lit_String) break; { jz_slice v; BR_GETN(string, r, i, &some, &v); *out = some ? ORD(cmp_slices(v, jz_str_slice(&e->v.s))) : sql_null(); return JZ_OK; }
    case groove_ValueType_Bytes: if (e->tag != groove_Lit_Bytes) break; { jz_slice v; BR_GETN(bytes, r, i, &some, &v); *out = some ? ORD(cmp_slices(v, jz_bytes_slice(&e->v.bytes))) : sql_null(); return JZ_OK; }
    case groove_ValueType_Uuid: if (e->tag != groove_Lit_Uuid) break; { jz_uuid v; BR_GETN(uuid, r, i, &some, &v); *out = some ? ORD(cmp_slices(jz_slice_of(v.b, 16), jz_slice_of(e->v.uuid.b, 16))) : sql_null(); return JZ_OK; }
    case groove_ValueType_EnumTag: if (e->tag != groove_Lit_EnumTag) break; { u8 v; BR_GETN(enum, r, i, &some, &v); *out = some ? ORD(CMP3(v, (u8)e->v.u)) : sql_null(); return JZ_OK; }
    default: break;
    }
    *out = unsupported();
    return JZ_OK;
}

jz_err groove__ivm__runtime__key_encoding__record_field_literal_ordering(const groove_BorrowedRecord *record, u32 field_idx, const groove_LiteralValue *value, groove_FieldLiteralOrdering *out) {
    const groove_DescriptorField *field;
    JZ_TRY(groove__records__BorrowedRecord__field(record, field_idx, &field));
    const groove_ValueType *t = &field->value_type;
    u32 i = field_idx;
    switch (t->tag) {
    case groove_ValueType_Record:
        if (value->tag == groove_Lit_NativeRecord) {
            groove_RecordDescriptor expected;
            jz_err e = groove__records__RecordDescriptor__from_postcard(jz_bytes_slice(&value->v.native.descriptor), &expected);
            if (JZ_IS_ERR(e)) { jz_err_drop(&e); return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedOperator); }
            if (*t->v.record != expected) { *out = unsupported(); return JZ_OK; }
            groove_Value actual;
            JZ_TRY(groove__records__BorrowedRecord__get_idx(record, i, &actual));
            if (actual.tag != groove_Value_Record) { groove_Value_drop(&actual); *out = unsupported(); return JZ_OK; }
            *out = ORD(cmp_slices(jz_bytes_slice(&actual.v.record.raw), jz_bytes_slice(&value->v.native.raw)));
            groove_Value_drop(&actual);
            return JZ_OK;
        }
        break;
    case groove_ValueType_U8: if (value->tag == groove_Lit_U8) { u8 v; BR_GET(u8, record, i, &v); *out = ORD(CMP3(v, (u8)value->v.u)); return JZ_OK; } break;
    case groove_ValueType_U32: if (value->tag == groove_Lit_U32) { u32 v; BR_GET(u32, record, i, &v); *out = ORD(CMP3(v, (u32)value->v.u)); return JZ_OK; } break;
    case groove_ValueType_U64: if (value->tag == groove_Lit_U64) { u64 v; BR_GET(u64, record, i, &v); *out = ORD(CMP3(v, value->v.u)); return JZ_OK; } break;
    case groove_ValueType_U48: if (value->tag == groove_Lit_U48) { u64 v; BR_GET(u48, record, i, &v); *out = ORD(CMP3(v, value->v.u)); return JZ_OK; } break;
    case groove_ValueType_I32: if (value->tag == groove_Lit_I32) { i32 v; BR_GET(i32, record, i, &v); *out = ORD(CMP3(v, (i32)value->v.i)); return JZ_OK; } break;
    case groove_ValueType_I64: if (value->tag == groove_Lit_I64) { i64 v; BR_GET(i64, record, i, &v); *out = ORD(CMP3(v, value->v.i)); return JZ_OK; } break;
    case groove_ValueType_F64: if (value->tag == groove_Lit_F64) { f64 v, ev; BR_GET(f64, record, i, &v); memcpy(&ev, &value->v.u, 8); *out = ORD(cmp_f64(v, ev)); return JZ_OK; } break;
    case groove_ValueType_Bool: if (value->tag == groove_Lit_Bool) { bool v; BR_GET(bool, record, i, &v); *out = ORD(CMP3((int)v, (int)value->v.b)); return JZ_OK; } break;
    case groove_ValueType_String: if (value->tag == groove_Lit_String) { jz_slice v; BR_GET(str, record, i, &v); *out = ORD(cmp_slices(v, jz_str_slice(&value->v.s))); return JZ_OK; } break;
    case groove_ValueType_Bytes: if (value->tag == groove_Lit_Bytes) { jz_slice v; BR_GET(bytes, record, i, &v); *out = ORD(cmp_slices(v, jz_bytes_slice(&value->v.bytes))); return JZ_OK; } break;
    case groove_ValueType_Uuid: if (value->tag == groove_Lit_Uuid) { jz_uuid v; BR_GET(uuid, record, i, &v); *out = ORD(cmp_slices(jz_slice_of(v.b, 16), jz_slice_of(value->v.uuid.b, 16))); return JZ_OK; } break;
    case groove_ValueType_EnumTag: if (value->tag == groove_Lit_EnumTag) { u8 v; BR_GET(enum, record, i, &v); *out = ORD(CMP3(v, (u8)value->v.u)); return JZ_OK; } break;
    case groove_ValueType_Nullable:
        if (value->tag == groove_Lit_Nullable) {
            if (value->v.nullable) return nullable_record_field_literal_ordering(record, i, t->v.inner, value->v.nullable, out);
            *out = sql_null();
            return JZ_OK;
        }
        break;
    default: break;
    }
    *out = unsupported();
    return JZ_OK;
}

jz_err groove__ivm__runtime__key_encoding__compare_record_fields(groove_PredicateRecord record, const char *field, u32 flen, const char *value_field, u32 vlen, groove_OrderingPredicate predicate, groove_ValueComparison comparison, bool *out) {
    groove_Value left, right;
    JZ_TRY(resolved_record_value(record, field, flen, &left));
    jz_err e = resolved_record_value(record, value_field, vlen, &right);
    if (JZ_IS_ERR(e)) { groove_Value_drop(&left); return e; }
    int c = groove__ivm__runtime__key_encoding__compare_values_sql(&left, &right, comparison);
    *out = c != GROOVE_ORD_NONE && predicate(c);
    groove_Value_drop(&left); groove_Value_drop(&right);
    return JZ_OK;
}

static bool value_contains_sql(const groove_Value *left, const groove_Value *right, groove_ValueComparison comparison) {
    if ((left->tag == groove_Value_Nullable && !left->v.nullable) || (right->tag == groove_Value_Nullable && !right->v.nullable)) return false;
    if (left->tag == groove_Value_Nullable) return value_contains_sql(left->v.nullable, right, comparison);
    if (right->tag == groove_Value_Nullable) return value_contains_sql(left, right->v.nullable, comparison);
    if (left->tag == groove_Value_String && right->tag == groove_Value_String) {
        const jz_str *h = &left->v.s, *n = &right->v.s;
        if (n->len == 0) return true;
        for (u32 i = 0; i + n->len <= h->len; i++) if (memcmp(h->ptr + i, n->ptr, n->len) == 0) return true;
        return false;
    }
    if (left->tag == groove_Value_Array) {
        for (u32 i = 0; i < left->v.vals.len; i++)
            if (groove__ivm__runtime__key_encoding__compare_values_sql(&left->v.vals.ptr[i], right, comparison) == 0) return true;
        return false;
    }
    return false;
}

jz_err groove__ivm__runtime__key_encoding__contains_record_field(groove_PredicateRecord record, const char *field, u32 flen, const groove_LiteralValue *value, groove_ValueComparison comparison, bool *out) {
    groove_Value needle, haystack;
    groove__ivm__op_types__LiteralValue__to_value(value, &needle);
    jz_err e = resolved_record_value(record, field, flen, &haystack);
    if (JZ_IS_ERR(e)) { groove_Value_drop(&needle); return e; }
    *out = value_contains_sql(&haystack, &needle, comparison);
    groove_Value_drop(&needle); groove_Value_drop(&haystack);
    return JZ_OK;
}

jz_err groove__ivm__runtime__key_encoding__contains_record_field_value(groove_PredicateRecord record, const char *field, u32 flen, const char *needle_field, u32 nlen, groove_ValueComparison comparison, bool *out) {
    groove_Value haystack, needle;
    JZ_TRY(resolved_record_value(record, field, flen, &haystack));
    jz_err e = resolved_record_value(record, needle_field, nlen, &needle);
    if (JZ_IS_ERR(e)) { groove_Value_drop(&haystack); return e; }
    *out = value_contains_sql(&haystack, &needle, comparison);
    groove_Value_drop(&needle); groove_Value_drop(&haystack);
    return JZ_OK;
}

static int compare_values(const groove_Value *left, const groove_Value *right, groove_ValueComparison comparison);

int groove__ivm__runtime__key_encoding__compare_values_sql(const groove_Value *left, const groove_Value *right, groove_ValueComparison comparison) {
    if ((left->tag == groove_Value_Nullable && !left->v.nullable) || (right->tag == groove_Value_Nullable && !right->v.nullable)) return GROOVE_ORD_NONE;
    if (left->tag == groove_Value_Nullable) return groove__ivm__runtime__key_encoding__compare_values_sql(left->v.nullable, right, comparison);
    if (right->tag == groove_Value_Nullable) return groove__ivm__runtime__key_encoding__compare_values_sql(left, right->v.nullable, comparison);
    return compare_values(left, right, comparison);
}

bool groove__ivm__runtime__key_encoding__is_sql_null_value(const groove_Value *v) {
    if (v->tag != groove_Value_Nullable) return false;
    if (!v->v.nullable) return true;
    return groove__ivm__runtime__key_encoding__is_sql_null_value(v->v.nullable);
}

static int compare_seq(const groove_ValueVec *l, const groove_ValueVec *r, groove_ValueComparison comparison) {
    u32 n = l->len < r->len ? l->len : r->len;
    for (u32 i = 0; i < n; i++) {
        int c = compare_values(&l->ptr[i], &r->ptr[i], comparison);
        if (c != 0) return c;
    }
    return CMP3(l->len, r->len);
}

static int compare_values(const groove_Value *left, const groove_Value *right, groove_ValueComparison comparison) {
    __int128 li, ri;
    if (comparison == groove_ValueComparison_Policy && groove__ivm__runtime__key_encoding__integer_value(left, &li) &&
        groove__ivm__runtime__key_encoding__integer_value(right, &ri))
        /* i128 represents every supported integer exactly, including U64 values
         * above i64::MAX. Floats deliberately do not participate. */
        return CMP3(li, ri);
    if (left->tag != right->tag) return GROOVE_ORD_NONE;
    switch (left->tag) {
    case groove_Value_U8: return CMP3(left->v.u8v, right->v.u8v);
    case groove_Value_U16: return CMP3(left->v.u16v, right->v.u16v);
    case groove_Value_U32: return CMP3(left->v.u32v, right->v.u32v);
    case groove_Value_U64: case groove_Value_U48: return CMP3(left->v.u64v, right->v.u64v);
    case groove_Value_I32: return CMP3(left->v.i32v, right->v.i32v);
    case groove_Value_I64: return CMP3(left->v.i64v, right->v.i64v);
    case groove_Value_F64: return cmp_f64(left->v.f64v, right->v.f64v);
    case groove_Value_Bool: return CMP3((int)left->v.b, (int)right->v.b);
    case groove_Value_EnumTag: return CMP3(left->v.u8v, right->v.u8v);
    case groove_Value_String: return cmp_slices(jz_str_slice(&left->v.s), jz_str_slice(&right->v.s));
    case groove_Value_Bytes: return cmp_slices(jz_bytes_slice(&left->v.bytes), jz_bytes_slice(&right->v.bytes));
    case groove_Value_Uuid: return cmp_slices(jz_slice_of(left->v.uuid.b, 16), jz_slice_of(right->v.uuid.b, 16));
    case groove_Value_Tuple: case groove_Value_Array: return compare_seq(&left->v.vals, &right->v.vals, comparison);
    default: return GROOVE_ORD_NONE;
    }
}

bool groove__ivm__runtime__key_encoding__integer_value(const groove_Value *v, __int128 *out) {
    switch (v->tag) {
    case groove_Value_U8: *out = v->v.u8v; return true;
    case groove_Value_U16: *out = v->v.u16v; return true;
    case groove_Value_U32: *out = v->v.u32v; return true;
    case groove_Value_U64: case groove_Value_U48: *out = v->v.u64v; return true;
    case groove_Value_I32: *out = v->v.i32v; return true;
    case groove_Value_I64: *out = v->v.i64v; return true;
    default: return false;
    }
}

groove_RecordDescriptor groove__ivm__runtime__key_encoding__join_descriptor(groove_RecordDescriptor left, groove_RecordDescriptor right) {
    groove_DescriptorFieldVec fields = {0};
    for (int side = 0; side < 2; side++) {
        groove_RecordDescriptor d = side ? right : left;
        u32 n;
        const groove_DescriptorField *fs = groove__records__RecordDescriptor__fields(d, &n);
        for (u32 i = 0; i < n; i++) {
            if (!fs[i].has_name) continue;
            groove_DescriptorField f;
            memset(&f, 0, sizeof(f));
            f.has_name = true;
            f.name = jz_format(side ? "right.%.*s" : "left.%.*s", (int)fs[i].name.len, fs[i].name.ptr);
            groove_ValueType_clone(&f.value_type, &fs[i].value_type);
            JZ_VEC_PUSH(fields, f);
        }
    }
    groove_RecordDescriptor d = groove__records__RecordDescriptor__new(fields.ptr, fields.len);
    for (u32 i = 0; i < fields.len; i++) { jz_str_drop(&fields.ptr[i].name); groove_ValueType_drop(&fields.ptr[i].value_type); }
    jz_vec_drop(&fields, sizeof(groove_DescriptorField), NULL);
    return d;
}

jz_err groove__ivm__runtime__key_encoding__encode_key_part(jz_bytes *key, const groove_Value *v) {
    /* Type tags make composite keys unambiguous. Payload bytes are chosen to
     * preserve natural ordering in lexicographic iterator order. */
    switch (v->tag) {
    case groove_Value_F64:
        if (v->v.f64v != v->v.f64v) return groove__records__Error__invalid_f64_nan();
        return groove__ivm__runtime__key_encoding__encode_scalar_key_part(key, v);
    case groove_Value_Tuple:
        jz_bytes_push_u8(key, 11);
        for (u32 i = 0; i < v->v.vals.len; i++) JZ_TRY(groove__ivm__runtime__key_encoding__encode_key_part(key, &v->v.vals.ptr[i]));
        return JZ_OK;
    case groove_Value_Nullable:
        if (!v->v.nullable) { jz_bytes_push_u8(key, 8); return JZ_OK; }
        jz_bytes_push_u8(key, 9);
        return groove__ivm__runtime__key_encoding__encode_key_part(key, v->v.nullable);
    default:
        return groove__ivm__runtime__key_encoding__encode_scalar_key_part(key, v);
    }
}

/* Type-tagged, order-preserving bytes of one scalar key value. */
jz_err groove__ivm__runtime__key_encoding__encode_scalar_key_part(jz_bytes *key, const groove_Value *v) {
    switch (v->tag) {
    case groove_Value_U8: case groove_Value_EnumTag: return put_u8(key, v->v.u8v);
    case groove_Value_U16: jz_bytes_push_u8(key, 1); push_be(key, v->v.u16v, 2); return JZ_OK;
    case groove_Value_U32: return put_u32(key, v->v.u32v);
    case groove_Value_U64: return put_u64(key, v->v.u64v);
    case groove_Value_U48: return put_u48(key, v->v.u64v);
    case groove_Value_I32: return put_i32(key, v->v.i32v);
    case groove_Value_I64: return put_i64(key, v->v.i64v);
    case groove_Value_F64: return put_f64(key, v->v.f64v);
    case groove_Value_Bool: return put_bool(key, v->v.b);
    case groove_Value_String: return put_str(key, jz_str_slice(&v->v.s));
    case groove_Value_Bytes: return put_bytes(key, jz_bytes_slice(&v->v.bytes));
    case groove_Value_Uuid: return put_uuid(key, v->v.uuid);
    case groove_Value_Tuple: case groove_Value_Nullable: case groove_Value_Array: case groove_Value_Record:
    case groove_Value_Enum: case groove_Value_Large: default:
        return GROOVE_IVM_ERR(GROOVE_IVM_UnsupportedJoinKey);
    }
}

jz_err groove__ivm__runtime__key_encoding__project_binding_source_deltas(const groove_RecordDeltas *input, groove_RecordDescriptor output, groove_RecordDeltas *out) {
    if (input->descriptor == output) { groove_RecordDeltas_clone(out, input); return JZ_OK; }
    u32 n;
    const groove_DescriptorField *fs = groove__records__RecordDescriptor__fields(output, &n);
    JZ_VEC(groove_UsizePair) mapping = {0};
    for (u32 i = 0; i < n; i++) {
        if (!fs[i].has_name) { jz_vec_drop(&mapping, sizeof(groove_UsizePair), NULL); return GROOVE_IVM_ERR_C(GROOVE_IVM_GraphFieldNotFound, "<unnamed>"); }
        groove_FieldRef ref = { groove_FieldRef_StoredName, fs[i].name, 0 };
        u32 index;
        jz_err e = groove__ivm__runtime__record_projection__resolve_field_ref(input->descriptor, &ref, &index);
        if (JZ_IS_ERR(e)) { jz_vec_drop(&mapping, sizeof(groove_UsizePair), NULL); return e; }
        groove_UsizePair p = { 0, index };
        JZ_VEC_PUSH(mapping, p);
    }
    out->descriptor = output;
    memset(&out->deltas, 0, sizeof(out->deltas));
    jz_vec_reserve(&out->deltas, input->deltas.len, sizeof(groove_RecordDelta));
    for (u32 i = 0; i < input->deltas.len; i++) {
        const groove_RecordDelta *delta = &input->deltas.ptr[i];
        jz_slice raw = jz_bytes_slice(&delta->record);
        groove_RecordDelta nd = {{0}, delta->weight};
        jz_err e = groove__records__RecordDescriptor__project_record_raw(output, &input->descriptor, &raw, 1, mapping.ptr, mapping.len, &nd.record);
        if (JZ_IS_ERR(e)) {
            jz_vec_drop(&mapping, sizeof(groove_UsizePair), NULL);
            jz_vec_drop(&out->deltas, sizeof(groove_RecordDelta), groove_RecordDelta_drop);
            return e;
        }
        JZ_VEC_PUSH(out->deltas, nd);
    }
    jz_vec_drop(&mapping, sizeof(groove_UsizePair), NULL);
    return JZ_OK;
}

static int delta_record_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_bytes_cmp(&((const groove_RecordDelta *)a)->record, &((const groove_RecordDelta *)b)->record);
}
static bool delta_nonzero(void *item, void *ctx) { (void)ctx; return ((groove_RecordDelta *)item)->weight != 0; }

void groove__ivm__runtime__key_encoding__consolidate_deltas(groove_RecordDeltaVec *deltas) {
    if (deltas->len <= 1) return;
    jz_sort_unstable(deltas->ptr, deltas->len, sizeof(groove_RecordDelta), delta_record_cmp, NULL);
    groove_RecordDeltaVec consolidated = {0};
    jz_vec_reserve(&consolidated, deltas->len, sizeof(groove_RecordDelta));
    for (u32 i = 0; i < deltas->len; i++) {
        groove_RecordDelta *delta = &deltas->ptr[i];
        if (consolidated.len && jz_bytes_eq(&consolidated.ptr[consolidated.len - 1].record, &delta->record)) {
            consolidated.ptr[consolidated.len - 1].weight += delta->weight;
            jz_bytes_drop(&delta->record);
            continue;
        }
        JZ_VEC_PUSH(consolidated, *delta);
    }
    jz_vec_drop(deltas, sizeof(groove_RecordDelta), NULL);
    jz_vec_retain(&consolidated, sizeof(groove_RecordDelta), delta_nonzero, NULL, groove_RecordDelta_drop);
    *deltas = consolidated;
}
