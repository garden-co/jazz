/* binding_values.rs: canonical representation of values bound into prepared query sources. */
#include "mod.h"

static groove_Value value_new(u8 tag) { groove_Value v; memset(&v, 0, sizeof v); v.tag = tag; return v; }
static groove_Value value_nullable_some(groove_Value inner) {
    groove_Value v = value_new(GROOVE_V_NULLABLE);
    v.u.nullable = (groove_Value *)jz_memdup(&inner, sizeof inner);
    return v;
}

/* i128 stand-in: sign + magnitude. */
typedef struct { bool neg; u64 mag; } wide_int;

static bool coerce_prepared_integer_value(const groove_Value *value, const groove_ColumnType *column_type, groove_Value *out) {
    wide_int w = {false, 0};
    switch (value->tag) {
    case GROOVE_V_U8: w.mag = value->u.u8_; break;
    case GROOVE_V_U16: w.mag = value->u.u16_; break;
    case GROOVE_V_U32: w.mag = value->u.u32_; break;
    case GROOVE_V_U64: w.mag = value->u.u64_; break;
    case GROOVE_V_I32: w.neg = value->u.i32_ < 0; w.mag = w.neg ? (u64)(-(i64)value->u.i32_) : (u64)value->u.i32_; break;
    case GROOVE_V_I64: w.neg = value->u.i64_ < 0; w.mag = w.neg ? (u64)0 - (u64)value->u.i64_ : (u64)value->u.i64_; break;
    default: return false;
    }
    switch (column_type->tag) {
    case GROOVE_VT_U8:
        if (w.neg || w.mag > 0xff) return false;
        *out = value_new(GROOVE_V_U8); out->u.u8_ = (u8)w.mag; return true;
    case GROOVE_VT_U16:
        if (w.neg || w.mag > 0xffff) return false;
        *out = value_new(GROOVE_V_U16); out->u.u16_ = (u16)w.mag; return true;
    case GROOVE_VT_U32:
        if (w.neg || w.mag > 0xffffffffu) return false;
        *out = value_new(GROOVE_V_U32); out->u.u32_ = (u32)w.mag; return true;
    case GROOVE_VT_U64:
        if (w.neg) return false;
        *out = value_new(GROOVE_V_U64); out->u.u64_ = w.mag; return true;
    case GROOVE_VT_I32:
        if (w.neg ? w.mag > 0x80000000u : w.mag > 0x7fffffffu) return false;
        *out = value_new(GROOVE_V_I32); out->u.i32_ = w.neg ? (i32)(0 - (i64)w.mag) : (i32)w.mag; return true;
    case GROOVE_VT_I64:
        if (w.neg ? w.mag > 0x8000000000000000ull : w.mag > 0x7fffffffffffffffull) return false;
        *out = value_new(GROOVE_V_I64); out->u.i64_ = w.neg ? (i64)(0 - w.mag) : (i64)w.mag; return true;
    default:
        return false;
    }
}

groove_Value jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(groove_Value value, const groove_ColumnType *column_type) {
    groove_Value coerced;
    if (coerce_prepared_integer_value(&value, column_type, &coerced)) {
        groove__records__values__Value__drop(&value);
        return coerced;
    }
    if (value.tag == GROOVE_V_UUID && column_type->tag == GROOVE_VT_STRING) {
        char buf[37];
        jz_uuid_to_string(&value.u.uuid, buf);
        groove_Value out = value_new(GROOVE_V_STRING);
        out.u.s = jz_str_from(buf);
        return out;
    }
    if (value.tag == GROOVE_V_STRING && column_type->tag == GROOVE_VT_UUID) {
        jz_uuid u;
        jz_err e = jz_uuid_parse(jz_str_slice(&value.u.s), &u);
        if (JZ_IS_ERR(e)) {
            jz_err_drop(&e);
            return value;
        }
        groove__records__values__Value__drop(&value);
        groove_Value out = value_new(GROOVE_V_UUID);
        out.u.uuid = u;
        return out;
    }
    if (value.tag == GROOVE_V_NULLABLE && column_type->tag == GROOVE_VT_NULLABLE) {
        if (value.u.nullable) {
            groove_Value inner = *value.u.nullable;
            *value.u.nullable = jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(inner, column_type->u.inner);
        }
        return value;
    }
    if (value.tag == GROOVE_V_NULLABLE && value.u.nullable) {
        groove_Value inner = *value.u.nullable;
        *value.u.nullable = jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(inner, column_type);
        return value;
    }
    if (value.tag == GROOVE_V_NULLABLE) return value;
    if (value.tag == GROOVE_V_ARRAY && column_type->tag == GROOVE_VT_ARRAY) {
        JZ_VEC_FOR(groove_Value, item, value.u.items)
            *item = jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(*item, column_type->u.inner);
        return value;
    }
    if (value.tag == GROOVE_V_TUPLE && column_type->tag == GROOVE_VT_TUPLE && value.u.items.len == column_type->u.tuple.len) {
        for (u32 i = 0; i < value.u.items.len; i++)
            value.u.items.ptr[i] = jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(value.u.items.ptr[i], &column_type->u.tuple.ptr[i]);
        return value;
    }
    if (column_type->tag == GROOVE_VT_NULLABLE)
        return value_nullable_some(jazz_engine__query_engine__binding_values__coerce_prepared_binding_value(value, column_type->u.inner));
    return value;
}
