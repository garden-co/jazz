/* model/public_api/types/value.rs: the host-facing public Value. */
#include "jazz_model/jazz_model.h"
#include <math.h>

#define V(x) jazz_model__model__public_api__types__value__##x
#define SCHEMA(x) jazz_model__model__public_api__types__schema__##x

static const char *const VALUE_NAMES[JAZZ_PV_COUNT] = {
    "Integer", "BigInt", "Double", "Boolean", "Text", "Timestamp", "Uuid", "TransactionId", "Bytea", "Array",
    "Row", "Enum", "Null",
};

static const char NON_HUMAN_VALUE_CODEC_ERROR[] =
    "public Value does not support non-human serde codecs; use the descriptor/raw row codec";

void V(Value__drop)(jazz_model_public_Value *v) {
    switch (v->tag) {
    case JAZZ_PV_TEXT: jz_str_drop(&v->u.text); break;
    case JAZZ_PV_BYTEA: jz_bytes_drop(&v->u.bytea); break;
    case JAZZ_PV_ARRAY: V(ValueVec__drop)(&v->u.array); break;
    case JAZZ_PV_ROW: V(ValueVec__drop)(&v->u.row.values); break;
    case JAZZ_PV_ENUM: jz_str_drop(&v->u.enum_.case_); V(ValueVec__drop)(&v->u.enum_.values); break;
    default: break;
    }
    v->tag = JAZZ_PV_NULL;
}
void V(Value__drop_v)(void *v) { V(Value__drop)((jazz_model_public_Value *)v); }
void V(ValueVec__drop)(jazz_model_public_ValueVec *v) { jz_vec_drop(v, sizeof(jazz_model_public_Value), V(Value__drop_v)); }

void V(Value__Clone__clone)(jazz_model_public_Value *dst, const jazz_model_public_Value *src) {
    *dst = *src;
    switch (src->tag) {
    case JAZZ_PV_TEXT: dst->u.text = jz_str_clone(&src->u.text); break;
    case JAZZ_PV_BYTEA: dst->u.bytea = jz_bytes_clone(&src->u.bytea); break;
    case JAZZ_PV_ARRAY: V(ValueVec__clone)(&dst->u.array, &src->u.array); break;
    case JAZZ_PV_ROW: V(ValueVec__clone)(&dst->u.row.values, &src->u.row.values); break;
    case JAZZ_PV_ENUM:
        dst->u.enum_.case_ = jz_str_clone(&src->u.enum_.case_);
        V(ValueVec__clone)(&dst->u.enum_.values, &src->u.enum_.values);
        break;
    default: break;
    }
}
void V(Value__Clone__clone_v)(void *dst, const void *src) { V(Value__Clone__clone)(dst, src); }
void V(ValueVec__clone)(jazz_model_public_ValueVec *dst, const jazz_model_public_ValueVec *src) {
    jz_vec_clone(dst, src, sizeof(jazz_model_public_Value), V(Value__Clone__clone_v));
}

bool V(ValueVec__eq)(const jazz_model_public_ValueVec *a, const jazz_model_public_ValueVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!V(Value__PartialEq__eq)(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

bool V(Value__PartialEq__eq)(const jazz_model_public_Value *a, const jazz_model_public_Value *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case JAZZ_PV_INTEGER: return a->u.integer == b->u.integer;
    case JAZZ_PV_BIGINT: return a->u.bigint == b->u.bigint;
    case JAZZ_PV_DOUBLE: {
        u64 x, y;
        memcpy(&x, &a->u.dbl, 8);
        memcpy(&y, &b->u.dbl, 8);
        return x == y;
    }
    case JAZZ_PV_BOOLEAN: return a->u.boolean == b->u.boolean;
    case JAZZ_PV_TEXT: return jz_str_eq(&a->u.text, &b->u.text);
    case JAZZ_PV_TIMESTAMP: return a->u.timestamp == b->u.timestamp;
    case JAZZ_PV_UUID: return a->u.uuid == b->u.uuid;
    case JAZZ_PV_TRANSACTION_ID: return memcmp(a->u.txid, b->u.txid, 16) == 0;
    case JAZZ_PV_BYTEA: return jz_bytes_eq(&a->u.bytea, &b->u.bytea);
    case JAZZ_PV_ENUM:
        return jz_str_eq(&a->u.enum_.case_, &b->u.enum_.case_) && V(ValueVec__eq)(&a->u.enum_.values, &b->u.enum_.values);
    case JAZZ_PV_ARRAY: return V(ValueVec__eq)(&a->u.array, &b->u.array);
    case JAZZ_PV_ROW: return a->u.row.id == b->u.row.id && V(ValueVec__eq)(&a->u.row.values, &b->u.row.values);
    case JAZZ_PV_NULL: return true;
    default: return false;
    }
}

void V(Value__Debug__fmt)(jz_str *out, const jazz_model_public_Value *v) {
    switch (v->tag) {
    case JAZZ_PV_INTEGER: jz_str_appendf(out, "Integer(%d)", v->u.integer); break;
    case JAZZ_PV_BIGINT: jz_str_appendf(out, "BigInt(%lld)", (long long)v->u.bigint); break;
    case JAZZ_PV_DOUBLE: jz_str_appendf(out, "Double(%g)", v->u.dbl); break;
    case JAZZ_PV_BOOLEAN: jz_str_appendf(out, "Boolean(%s)", v->u.boolean ? "true" : "false"); break;
    case JAZZ_PV_TEXT:
        jz_str_push_cstr(out, "Text(");
        jazz_model__debug_str(out, v->u.text.ptr, v->u.text.len);
        jz_str_push_char(out, ')');
        break;
    case JAZZ_PV_TIMESTAMP: jz_str_appendf(out, "Timestamp(%llu)", (unsigned long long)v->u.timestamp); break;
    case JAZZ_PV_UUID:
        jz_str_push_cstr(out, "Uuid(");
        jazz_types__object__ObjectId__Debug__fmt(out, v->u.uuid);
        jz_str_push_char(out, ')');
        break;
    case JAZZ_PV_TRANSACTION_ID: {
        jz_str hex = {0};
        jz_hex_encode(v->u.txid, 16, &hex);
        jz_str_appendf(out, "TransactionId(%.*s)", (int)hex.len, hex.ptr);
        jz_str_drop(&hex);
        break;
    }
    case JAZZ_PV_BYTEA: jz_str_appendf(out, "Bytea { len: %u }", v->u.bytea.len); break;
    case JAZZ_PV_ARRAY: jz_str_appendf(out, "Array { len: %u }", v->u.array.len); break;
    case JAZZ_PV_ROW:
        jz_str_push_cstr(out, "Row { id: ");
        if (v->u.row.id) {
            jz_str_push_cstr(out, "Some(");
            jazz_types__object__ObjectId__Debug__fmt(out, v->u.row.id);
            jz_str_push_char(out, ')');
        } else jz_str_push_cstr(out, "None");
        jz_str_appendf(out, ", values_len: %u }", v->u.row.values.len);
        break;
    case JAZZ_PV_ENUM:
        jz_str_push_cstr(out, "Enum { case: ");
        jazz_model__debug_str(out, v->u.enum_.case_.ptr, v->u.enum_.case_.len);
        jz_str_appendf(out, ", values_len: %u }", v->u.enum_.values.len);
        break;
    case JAZZ_PV_NULL: jz_str_push_cstr(out, "Null"); break;
    }
}

/* ---------------------------------------------------------------- JSON (ValueJson remote) */
static void object_id_serialize_json(jz_json_w *w, jazz_types_ObjectId id) {
    char buf[37];
    jz_uuid_to_string(id, buf);
    jz_jw_str(w, buf, 36);
}

static jz_err object_id_deserialize_json(const jz_json *j, jazz_types_ObjectId *out) {
    if (!j || j->kind != JZ_JSON_STR) return jazz_model__serde_err("invalid type: expected a UUID string");
    jz_uuid u;
    JZ_TRY(jz_uuid_parse(jz_str_slice(&j->v.s), &u));
    *out = jazz_types__object__ObjectId__from_uuid(&u);
    return JZ_OK;
}

static jz_err value_vec_serialize(jz_json_w *w, const jazz_model_public_ValueVec *vs) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < vs->len; i++) JZ_TRY(V(Value__Serialize__serialize)(w, &vs->ptr[i]));
    jz_jw_end_arr(w);
    return JZ_OK;
}

static jz_err value_vec_deserialize(const jz_json *j, jazz_model_public_ValueVec *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_expect_arr(j, "a sequence"));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        jazz_model_public_Value v;
        jz_err e = V(Value__Deserialize__deserialize)(j->v.arr.ptr[i], &v);
        if (e.code) { V(ValueVec__drop)(out); return e; }
        JZ_VEC_PUSH(*out, v);
    }
    return JZ_OK;
}

static void jazz_model__model__public_api__types__value__serialize_bigint_value(jz_json_w *w, i64 v) {
    char buf[24];
    jz_str s = jz_format("%lld", (long long)v);
    u32 n = s.len < sizeof buf ? s.len : (u32)sizeof buf - 1;
    memcpy(buf, s.ptr, n);
    jz_jw_str(w, buf, n);
    jz_str_drop(&s);
}

static jz_err jazz_model__model__public_api__types__value__deserialize_bigint_value(const jz_json *j, i64 *out) {
    switch (j->kind) {
    case JZ_JSON_I64: *out = j->v.i; return JZ_OK;
    case JZ_JSON_U64:
        if (j->v.u > (u64)INT64_MAX) return jazz_model__serde_err("bigint is out of signed 64-bit range");
        *out = (i64)j->v.u;
        return JZ_OK;
    case JZ_JSON_STR: {
        const jz_str *s = &j->v.s;
        u32 i = 0;
        bool neg = false;
        if (s->len && (s->ptr[0] == '-' || s->ptr[0] == '+')) { neg = s->ptr[0] == '-'; i = 1; }
        if (i == s->len) return jazz_model__serde_err("bigint string must be a signed 64-bit integer");
        u64 acc = 0;
        for (; i < s->len; i++) {
            char c = s->ptr[i];
            if (c < '0' || c > '9') return jazz_model__serde_err("bigint string must be a signed 64-bit integer");
            if (acc > (UINT64_MAX - (u64)(c - '0')) / 10) return jazz_model__serde_err("bigint string must be a signed 64-bit integer");
            acc = acc * 10 + (u64)(c - '0');
        }
        if (neg ? acc > (u64)INT64_MAX + 1 : acc > (u64)INT64_MAX)
            return jazz_model__serde_err("bigint string must be a signed 64-bit integer");
        *out = neg ? (i64)(0 - acc) : (i64)acc;
        return JZ_OK;
    }
    default: return jazz_model__serde_err("invalid type: expected a signed 64-bit integer or decimal string");
    }
}

static jz_err jazz_model__model__public_api__types__value__deserialize_timestamp_value(const jz_json *j, u64 *out) {
    switch (j->kind) {
    case JZ_JSON_U64: *out = j->v.u; return JZ_OK;
    case JZ_JSON_I64:
        if (j->v.i < 0) return jazz_model__serde_err("timestamp must be non-negative");
        *out = (u64)j->v.i;
        return JZ_OK;
    case JZ_JSON_F64: {
        f64 value = j->v.f;
        if (!isfinite(value)) return jazz_model__serde_err("timestamp must be finite");
        if (value < 0.0) return jazz_model__serde_err("timestamp must be non-negative");
        if (value != trunc(value)) return jazz_model__serde_err("timestamp must be an integer");
        if (value > (f64)UINT64_MAX) return jazz_model__serde_err("timestamp is out of range");
        *out = (u64)value;
        return JZ_OK;
    }
    default: return jazz_model__serde_err("invalid type: expected a non-negative integer timestamp");
    }
}

static jz_err bytes16_deserialize(const jz_json *j, u8 out[16]) {
    JZ_TRY(jazz_model__json_expect_arr(j, "an array of length 16"));
    if (j->v.arr.len != 16) return jazz_model__serde_err("invalid length %u, expected an array of length 16", j->v.arr.len);
    for (u32 i = 0; i < 16; i++) {
        u64 b;
        JZ_TRY(jazz_model__json_expect_u64(j->v.arr.ptr[i], &b));
        if (b > 255) return jazz_model__serde_err("invalid value: integer `%llu`, expected u8", (unsigned long long)b);
        out[i] = (u8)b;
    }
    return JZ_OK;
}

static jz_err bytes_deserialize(const jz_json *j, jz_bytes *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_expect_arr(j, "a sequence"));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        u64 b;
        jz_err e = jazz_model__json_expect_u64(j->v.arr.ptr[i], &b);
        if (!e.code && b > 255) e = jazz_model__serde_err("invalid value: integer `%llu`, expected u8", (unsigned long long)b);
        if (e.code) { jz_bytes_drop(out); return e; }
        jz_bytes_push_u8(out, (u8)b);
    }
    return JZ_OK;
}

/* RowHuman { #[serde(default)] id, values } — from a map or a positional sequence */
static jz_err jazz_model__model__public_api__types__value__deserialize_row(const jz_json *j, jazz_types_ObjectId *id, jazz_model_public_ValueVec *values) {
    *id = NULL;
    if (j->kind == JZ_JSON_ARR) {
        if (j->v.arr.len < 2) return jazz_model__serde_err("invalid length %u, expected struct RowHuman with 2 elements", j->v.arr.len);
        if (j->v.arr.ptr[0]->kind != JZ_JSON_NULL) JZ_TRY(object_id_deserialize_json(j->v.arr.ptr[0], id));
        return value_vec_deserialize(j->v.arr.ptr[1], values);
    }
    JZ_TRY(jz_json_expect_obj(j, "struct RowHuman"));
    const jz_json *f = jz_json_obj_get(j, "id");
    if (f && f->kind != JZ_JSON_NULL) JZ_TRY(object_id_deserialize_json(f, id));
    f = jz_json_obj_get(j, "values");
    if (!f) return jazz_model__json_missing_field("values");
    return value_vec_deserialize(f, values);
}

/* EnumHuman { case, values } */
static jz_err jazz_model__model__public_api__types__value__deserialize_enum(const jz_json *j, jz_str *case_, jazz_model_public_ValueVec *values) {
    if (j->kind == JZ_JSON_ARR) {
        if (j->v.arr.len < 2) return jazz_model__serde_err("invalid length %u, expected struct EnumHuman with 2 elements", j->v.arr.len);
        JZ_TRY(jazz_model__json_expect_str(j->v.arr.ptr[0], case_));
        jz_err e = value_vec_deserialize(j->v.arr.ptr[1], values);
        if (e.code) jz_str_drop(case_);
        return e;
    }
    JZ_TRY(jz_json_expect_obj(j, "struct EnumHuman"));
    JZ_TRY(jz_json_field_str(j, "case", case_));
    const jz_json *f = jz_json_obj_get(j, "values");
    if (!f) { jz_str_drop(case_); return jazz_model__json_missing_field("values"); }
    jz_err e = value_vec_deserialize(f, values);
    if (e.code) jz_str_drop(case_);
    return e;
}

jz_err V(Value__Serialize__serialize)(jz_json_w *w, const jazz_model_public_Value *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, "type");
    jazz_model__json_w_cstr(w, VALUE_NAMES[v->tag]);
    if (v->tag != JAZZ_PV_NULL) jz_jw_key(w, "value");
    switch (v->tag) {
    case JAZZ_PV_INTEGER: jz_jw_i64(w, v->u.integer); break;
    case JAZZ_PV_BIGINT: jazz_model__model__public_api__types__value__serialize_bigint_value(w, v->u.bigint); break;
    case JAZZ_PV_DOUBLE: jz_jw_f64(w, v->u.dbl); break;
    case JAZZ_PV_BOOLEAN: jz_jw_bool(w, v->u.boolean); break;
    case JAZZ_PV_TEXT: jazz_model__json_w_str(w, &v->u.text); break;
    case JAZZ_PV_TIMESTAMP: jz_jw_u64(w, v->u.timestamp); break;
    case JAZZ_PV_UUID: object_id_serialize_json(w, v->u.uuid); break;
    case JAZZ_PV_TRANSACTION_ID:
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < 16; i++) jz_jw_u64(w, v->u.txid[i]);
        jz_jw_end_arr(w);
        break;
    case JAZZ_PV_BYTEA:
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < v->u.bytea.len; i++) jz_jw_u64(w, v->u.bytea.ptr[i]);
        jz_jw_end_arr(w);
        break;
    case JAZZ_PV_ARRAY: JZ_TRY(value_vec_serialize(w, &v->u.array)); break;
    case JAZZ_PV_ROW:
        jz_jw_begin_obj(w);
        if (v->u.row.id) {
            jz_jw_key(w, "id");
            object_id_serialize_json(w, v->u.row.id);
        }
        jz_jw_key(w, "values");
        JZ_TRY(value_vec_serialize(w, &v->u.row.values));
        jz_jw_end_obj(w);
        break;
    case JAZZ_PV_ENUM:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "case");
        jazz_model__json_w_str(w, &v->u.enum_.case_);
        jz_jw_key(w, "values");
        JZ_TRY(value_vec_serialize(w, &v->u.enum_.values));
        jz_jw_end_obj(w);
        break;
    case JAZZ_PV_NULL: break;
    }
    jz_jw_end_obj(w);
    return JZ_OK;
}

jz_err V(Value__Deserialize__deserialize)(const jz_json *j, jazz_model_public_Value *out) {
    u32 idx;
    memset(out, 0, sizeof *out);
    out->tag = JAZZ_PV_NULL;
    JZ_TRY(jazz_model__json_internal_tag(j, "type", VALUE_NAMES, JAZZ_PV_COUNT, &idx));
    if (idx == JAZZ_PV_NULL) return JZ_OK;
    const jz_json *c = jz_json_obj_get(j, "value");
    if (!c) return jazz_model__json_missing_field("value");
    jz_err e = JZ_OK;
    switch (idx) {
    case JAZZ_PV_INTEGER: {
        i64 x;
        e = jazz_model__json_expect_i64(c, &x);
        if (!e.code && (x < INT32_MIN || x > INT32_MAX))
            e = jazz_model__serde_err("invalid value: integer `%lld`, expected i32", (long long)x);
        out->u.integer = (i32)x;
        break;
    }
    case JAZZ_PV_BIGINT: e = jazz_model__model__public_api__types__value__deserialize_bigint_value(c, &out->u.bigint); break;
    case JAZZ_PV_DOUBLE: e = jazz_model__json_expect_f64(c, &out->u.dbl); break;
    case JAZZ_PV_BOOLEAN: e = jazz_model__json_expect_bool(c, &out->u.boolean); break;
    case JAZZ_PV_TEXT: e = jazz_model__json_expect_str(c, &out->u.text); break;
    case JAZZ_PV_TIMESTAMP: e = jazz_model__model__public_api__types__value__deserialize_timestamp_value(c, &out->u.timestamp); break;
    case JAZZ_PV_UUID: e = object_id_deserialize_json(c, &out->u.uuid); break;
    case JAZZ_PV_TRANSACTION_ID: e = bytes16_deserialize(c, out->u.txid); break;
    case JAZZ_PV_BYTEA: e = bytes_deserialize(c, &out->u.bytea); break;
    case JAZZ_PV_ARRAY: e = value_vec_deserialize(c, &out->u.array); break;
    case JAZZ_PV_ROW: e = jazz_model__model__public_api__types__value__deserialize_row(c, &out->u.row.id, &out->u.row.values); break;
    case JAZZ_PV_ENUM: e = jazz_model__model__public_api__types__value__deserialize_enum(c, &out->u.enum_.case_, &out->u.enum_.values); break;
    }
    if (e.code) return e;
    out->tag = (u8)idx;
    return JZ_OK;
}

/* Non-human-readable codecs (postcard) are rejected. */
jz_err V(Value__Serialize__serialize_postcard)(jz_pc_w *w, const jazz_model_public_Value *v) {
    (void)w; (void)v;
    return jz_err_new(JAZZ_MODEL_ERR_SERDE, NON_HUMAN_VALUE_CODEC_ERROR);
}

jz_err V(Value__Deserialize__deserialize_postcard)(jz_pc_r *r, jazz_model_public_Value *out) {
    (void)r;
    *out = jazz_model_public_Value_null();
    return jz_err_new(JAZZ_MODEL_ERR_SERDE, NON_HUMAN_VALUE_CODEC_ERROR);
}

/* ---------------------------------------------------------------- Value::column_type */
bool V(Value__column_type)(const jazz_model_public_Value *v, jazz_model_public_ColumnType *out) {
    switch (v->tag) {
    case JAZZ_PV_INTEGER: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_INTEGER); return true;
    case JAZZ_PV_BIGINT: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_BIGINT); return true;
    case JAZZ_PV_DOUBLE: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_DOUBLE); return true;
    case JAZZ_PV_BOOLEAN: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_BOOLEAN); return true;
    case JAZZ_PV_TEXT: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_TEXT); return true;
    case JAZZ_PV_TIMESTAMP: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_TIMESTAMP); return true;
    case JAZZ_PV_UUID: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_UUID); return true;
    case JAZZ_PV_TRANSACTION_ID: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_TRANSACTION_ID); return true;
    case JAZZ_PV_BYTEA: *out = SCHEMA(ColumnType__simple)(JAZZ_CT_BYTEA); return true;
    case JAZZ_PV_ARRAY:
        for (u32 i = 0; i < v->u.array.len; i++) {
            jazz_model_public_ColumnType elem;
            if (V(Value__column_type)(&v->u.array.ptr[i], &elem)) {
                *out = SCHEMA(ColumnType__simple)(JAZZ_CT_ARRAY);
                out->element = JZ_NEW(jazz_model_public_ColumnType);
                *out->element = elem;
                return true;
            }
        }
        return false;
    default: return false;
    }
}
