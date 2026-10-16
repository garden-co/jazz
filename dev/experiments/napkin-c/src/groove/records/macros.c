/* groove::records::macros (crates/groove/src/records/macros.rs)
 *
 * RecordField impls. Rust monomorphises one impl per field type and per wrapper
 * newtype (define_record!/impl_record_field_* expand in other crates); here each
 * primitive is one function set reached through a vtable, and Option<T>/(A, B)
 * are single generic copies that dispatch to the inner vtable. */
#include "jz_rt.h"
#include "values.h"
#include "mod.h"
#include "macros.h"
#include "../large_values.h"

#define V(name) groove__records__values__##name
#define VT(t) groove__records__values__ValueType__scalar(t)

bool groove__records__macros__FieldKind__matches(groove_FieldKind kind, const groove_ValueType *t) {
    switch (kind) {
    case GROOVE_FK_U64: return t->tag == GROOVE_VT_U64;
    case GROOVE_FK_U48: return t->tag == GROOVE_VT_U48;
    case GROOVE_FK_I64: return t->tag == GROOVE_VT_I64;
    case GROOVE_FK_U32: return t->tag == GROOVE_VT_U32;
    case GROOVE_FK_I32: return t->tag == GROOVE_VT_I32;
    case GROOVE_FK_F64: return t->tag == GROOVE_VT_F64;
    case GROOVE_FK_U8: return t->tag == GROOVE_VT_U8;
    case GROOVE_FK_BOOL: return t->tag == GROOVE_VT_BOOL;
    case GROOVE_FK_BYTES: return t->tag == GROOVE_VT_BYTES;
    case GROOVE_FK_UUID: return t->tag == GROOVE_VT_UUID;
    case GROOVE_FK_STRING: return t->tag == GROOVE_VT_STRING;
    case GROOVE_FK_ENUM: return t->tag == GROOVE_VT_ENUM_TAG;
    case GROOVE_FK_PAYLOAD_ENUM: return t->tag == GROOVE_VT_ENUM;
    case GROOVE_FK_TUPLE: return t->tag == GROOVE_VT_TUPLE;
    case GROOVE_FK_ARRAY: return t->tag == GROOVE_VT_ARRAY;
    case GROOVE_FK_NULLABLE: return t->tag == GROOVE_VT_NULLABLE;
    case GROOVE_FK_RECORD: return t->tag == GROOVE_VT_RECORD;
    default: return false;
    }
}

static jz_err type_mismatch_scalar(u8 tag) {
    groove_ValueType t = VT(tag);
    return groove__records__Error__type_mismatch(&t);
}

static jz_err read_exact_array(u32 n, u32 want) {
    if (n != want) return groove__records__Error__simple(GROOVE_REC_E_UnexpectedEof);
    return JZ_OK;
}

static u64 le(const u8 *p, u32 n) { u64 v = 0; for (int i = (int)n - 1; i >= 0; i--) v = (v << 8) | p[i]; return v; }
static u64 be(const u8 *p, u32 n) { u64 v = 0; for (u32 i = 0; i < n; i++) v = (v << 8) | p[i]; return v; }

/* default RecordField::read_raw / read_tuple_raw */
static jz_err default_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt; (void)p; (void)n; (void)out;
    return groove__records__Error__type_mismatch(t);
}
static jz_err default_read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    return vt->read_raw(vt, p, n, t, out);
}
static jz_err tuple_member_rejected(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt; (void)p; (void)n; (void)out;
    return groove__records__Error__invalid_tuple_member(t);
}

/* ---------- u64 */
static jz_err u64_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_u64(r, idx, out); }
static groove_Value u64_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_U64}; v.u.u64_ = *(const u64 *)s; return v; }
static jz_err u64_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_U64) return type_mismatch_scalar(GROOVE_VT_U64);
    JZ_TRY(read_exact_array(n, 8));
    *(u64 *)out = le(p, 8);
    return JZ_OK;
}
static jz_err u64_read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_U64) return type_mismatch_scalar(GROOVE_VT_U64);
    JZ_TRY(read_exact_array(n, 8));
    *(u64 *)out = be(p, 8);
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_u64 = {sizeof(u64), u64_read, u64_to_value, GROOVE_FK_U64, u64_read_raw, u64_read_tuple_raw, NULL, NULL};

/* ---------- U48 */
jz_err groove__records__macros__U48__new(u64 value, u64 *out) {
    if (value > GROOVE_U48_MAX) return jz_err_fmt(GROOVE_REC_E_U48OutOfRange, "u48 value %llu exceeds 2^48 - 1", (unsigned long long)value);
    *out = value;
    return JZ_OK;
}
static jz_err u48_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_u48(r, idx, out); }
static groove_Value u48_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_U48}; v.u.u64_ = *(const u64 *)s; return v; }
static jz_err u48_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_U48) return type_mismatch_scalar(GROOVE_VT_U48);
    JZ_TRY(read_exact_array(n, 6));
    *(u64 *)out = V(u48_from_le_bytes)(p);
    return JZ_OK;
}
static jz_err u48_read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_U48) return type_mismatch_scalar(GROOVE_VT_U48);
    JZ_TRY(read_exact_array(n, 6));
    *(u64 *)out = V(u48_from_be_bytes)(p);
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_U48 = {sizeof(u64), u48_read, u48_to_value, GROOVE_FK_U48, u48_read_raw, u48_read_tuple_raw, NULL, NULL};

/* ---------- i64 */
static jz_err i64_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_i64(r, idx, out); }
static groove_Value i64_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_I64}; v.u.i64_ = *(const i64 *)s; return v; }
static jz_err i64_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_I64) return type_mismatch_scalar(GROOVE_VT_I64);
    JZ_TRY(read_exact_array(n, 8));
    *(i64 *)out = (i64)le(p, 8);
    return JZ_OK;
}
static jz_err i64_read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_I64) return type_mismatch_scalar(GROOVE_VT_I64);
    JZ_TRY(read_exact_array(n, 8));
    *(i64 *)out = (i64)(be(p, 8) ^ ((u64)1 << 63));
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_i64 = {sizeof(i64), i64_read, i64_to_value, GROOVE_FK_I64, i64_read_raw, i64_read_tuple_raw, NULL, NULL};

/* ---------- i32 */
static jz_err i32_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_i32(r, idx, out); }
static groove_Value i32_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_I32}; v.u.i32_ = *(const i32 *)s; return v; }
static jz_err i32_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_I32) return type_mismatch_scalar(GROOVE_VT_I32);
    JZ_TRY(read_exact_array(n, 4));
    *(i32 *)out = (i32)(u32)le(p, 4);
    return JZ_OK;
}
static jz_err i32_read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_I32) return type_mismatch_scalar(GROOVE_VT_I32);
    JZ_TRY(read_exact_array(n, 4));
    *(i32 *)out = (i32)((u32)be(p, 4) ^ ((u32)1 << 31));
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_i32 = {sizeof(i32), i32_read, i32_to_value, GROOVE_FK_I32, i32_read_raw, i32_read_tuple_raw, NULL, NULL};

/* ---------- u32 */
static jz_err u32_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_u32(r, idx, out); }
static groove_Value u32_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_U32}; v.u.u32_ = *(const u32 *)s; return v; }
static jz_err u32_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_U32) return type_mismatch_scalar(GROOVE_VT_U32);
    JZ_TRY(read_exact_array(n, 4));
    *(u32 *)out = (u32)le(p, 4);
    return JZ_OK;
}
static jz_err u32_read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_U32) return type_mismatch_scalar(GROOVE_VT_U32);
    JZ_TRY(read_exact_array(n, 4));
    *(u32 *)out = (u32)be(p, 4);
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_u32 = {sizeof(u32), u32_read, u32_to_value, GROOVE_FK_U32, u32_read_raw, u32_read_tuple_raw, NULL, NULL};

/* ---------- f64 */
static jz_err f64_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_f64(r, idx, out); }
static groove_Value f64_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_F64}; v.u.f64_ = *(const f64 *)s; return v; }
static jz_err f64_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_F64) return type_mismatch_scalar(GROOVE_VT_F64);
    JZ_TRY(read_exact_array(n, 8));
    u64 bits = le(p, 8);
    f64 value;
    memcpy(&value, &bits, 8);
    if (value != value) return groove__records__Error__simple(GROOVE_REC_E_InvalidF64NaN);
    *(f64 *)out = value;
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_f64 = {sizeof(f64), f64_read, f64_to_value, GROOVE_FK_F64, f64_read_raw, tuple_member_rejected, NULL, NULL};

/* ---------- u8 */
static jz_err u8_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_u8(r, idx, out); }
static groove_Value u8_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_U8}; v.u.u8_ = *(const u8 *)s; return v; }
static jz_err u8_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_U8) return type_mismatch_scalar(GROOVE_VT_U8);
    JZ_TRY(read_exact_array(n, 1));
    *(u8 *)out = p[0];
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_u8 = {sizeof(u8), u8_read, u8_to_value, GROOVE_FK_U8, u8_read_raw, default_read_tuple_raw, NULL, NULL};

/* ---------- bool */
static jz_err bool_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_bool(r, idx, out); }
static groove_Value bool_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_BOOL}; v.u.b = *(const bool *)s; return v; }
static jz_err bool_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_BOOL) return type_mismatch_scalar(GROOVE_VT_BOOL);
    JZ_TRY(read_exact_array(n, 1));
    switch (p[0]) {
    case 0: *(bool *)out = false; return JZ_OK;
    case 1: *(bool *)out = true; return JZ_OK;
    default: return jz_err_fmt(GROOVE_REC_E_InvalidBool, "invalid boolean byte: %u", (unsigned)p[0]);
    }
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_bool = {sizeof(bool), bool_read, bool_to_value, GROOVE_FK_BOOL, bool_read_raw, default_read_tuple_raw, NULL, NULL};

/* ---------- Vec<u8> */
static jz_err bytes_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) {
    (void)vt;
    jz_slice b;
    JZ_TRY(groove__records__BorrowedRecord__get_bytes(r, idx, &b));
    *(jz_bytes *)out = jz_bytes_from(b.ptr, b.len);
    return JZ_OK;
}
static groove_Value bytes_to_value(const groove_RecordFieldVtbl *vt, const void *s) {
    (void)vt;
    groove_Value v = {.tag = GROOVE_V_BYTES};
    v.u.bytes = jz_bytes_clone((const jz_bytes *)s);
    return v;
}
static jz_err bytes_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_BYTES) return type_mismatch_scalar(GROOVE_VT_BYTES);
    jz_slice b;
    JZ_TRY(groove__large_values__inline_scalar_bytes(GROOVE_LVK_BYTES, p, n, &b));
    *(jz_bytes *)out = jz_bytes_from(b.ptr, b.len);
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_bytes = {sizeof(jz_bytes), bytes_read, bytes_to_value, GROOVE_FK_BYTES, bytes_read_raw, tuple_member_rejected, NULL, NULL};

/* ---------- Uuid */
static jz_err uuid_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) { (void)vt; return groove__records__BorrowedRecord__get_uuid(r, idx, out); }
static groove_Value uuid_to_value(const groove_RecordFieldVtbl *vt, const void *s) { (void)vt; groove_Value v = {.tag = GROOVE_V_UUID}; v.u.uuid = *(const jz_uuid *)s; return v; }
static jz_err uuid_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_UUID) return type_mismatch_scalar(GROOVE_VT_UUID);
    JZ_TRY(read_exact_array(n, 16));
    memcpy(((jz_uuid *)out)->b, p, 16);
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_uuid = {sizeof(jz_uuid), uuid_read, uuid_to_value, GROOVE_FK_UUID, uuid_read_raw, default_read_tuple_raw, NULL, NULL};

/* ---------- String */
static jz_err string_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) {
    (void)vt;
    jz_slice s;
    JZ_TRY(groove__records__BorrowedRecord__get_str(r, idx, &s));
    *(jz_str *)out = jz_str_from_slice(s);
    return JZ_OK;
}
static groove_Value string_to_value(const groove_RecordFieldVtbl *vt, const void *s) {
    (void)vt;
    groove_Value v = {.tag = GROOVE_V_STRING};
    v.u.s = jz_str_clone((const jz_str *)s);
    return v;
}
static jz_err string_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    if (t->tag != GROOVE_VT_STRING) return type_mismatch_scalar(GROOVE_VT_STRING);
    jz_slice b;
    JZ_TRY(groove__large_values__inline_scalar_bytes(GROOVE_LVK_STRING, p, n, &b));
    if (!jz_utf8_valid(b.ptr, b.len)) return groove__records__Error__simple(GROOVE_REC_E_InvalidUtf8);
    *(jz_str *)out = jz_str_from_slice(b);
    return JZ_OK;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_string = {sizeof(jz_str), string_read, string_to_value, GROOVE_FK_STRING, string_read_raw, tuple_member_rejected, NULL, NULL};

/* ---------- Value */
static jz_err value_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) {
    (void)vt;
    const groove_DescriptorField *field;
    jz_slice bytes;
    JZ_TRY(groove__records__BorrowedRecord__field(r, idx, &field));
    JZ_TRY(groove__records__BorrowedRecord__field_bytes_unchecked(r, idx, &bytes));
    return V(decode_value)(bytes.ptr, bytes.len, &field->value_type, out);
}
static groove_Value value_to_value(const groove_RecordFieldVtbl *vt, const void *s) {
    (void)vt;
    groove_Value v;
    V(Value__Clone__clone)(&v, s);
    return v;
}
static jz_err value_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    (void)vt;
    return V(decode_value)(p, n, t, out);
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_value = {sizeof(groove_Value), value_read, value_to_value, GROOVE_FK_NULLABLE, value_read_raw, value_read_raw, NULL, NULL};

/* ---------- impl_value_record_field!(Vec<Value>, Array) / (OwnedRecord, Record) / (EnumValue, Enum):
 * one shared body keyed by the expected Value variant (column_kind selects it). */
static u8 value_variant_for_kind(groove_FieldKind k) {
    return k == GROOVE_FK_ARRAY ? GROOVE_V_ARRAY : k == GROOVE_FK_RECORD ? GROOVE_V_RECORD : GROOVE_V_ENUM;
}
static jz_err variant_read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    groove_Value v;
    JZ_TRY(V(decode_value)(p, n, t, &v));
    if (v.tag != value_variant_for_kind(vt->column_kind)) {
        V(Value__drop)(&v);
        return groove__records__Error__type_mismatch(t);
    }
    switch (v.tag) {
    case GROOVE_V_ARRAY: *(groove_ValueVec *)out = v.u.items; break;
    case GROOVE_V_RECORD: *(groove_OwnedRecord *)out = v.u.record; break;
    default: *(groove_EnumValue *)out = v.u.enum_; break;
    }
    return JZ_OK;
}
static jz_err variant_read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) {
    const groove_DescriptorField *field;
    jz_slice bytes;
    JZ_TRY(groove__records__BorrowedRecord__field(r, idx, &field));
    JZ_TRY(groove__records__BorrowedRecord__field_bytes_unchecked(r, idx, &bytes));
    return vt->read_raw(vt, bytes.ptr, bytes.len, &field->value_type, out);
}
static groove_Value variant_to_value(const groove_RecordFieldVtbl *vt, const void *s) {
    groove_Value v;
    memset(&v, 0, sizeof(v));
    v.tag = value_variant_for_kind(vt->column_kind);
    switch (v.tag) {
    case GROOVE_V_ARRAY: jz_vec_clone(&v.u.items, s, sizeof(groove_Value), V(Value__Clone__clone_v)); break;
    case GROOVE_V_RECORD: groove__records__OwnedRecord__Clone__clone(&v.u.record, s); break;
    default: {
        const groove_EnumValue *ev = s;
        v.u.enum_.tag = ev->tag;
        groove__records__OwnedRecord__Clone__clone(&v.u.enum_.record, &ev->record);
        break;
    }
    }
    return v;
}
const groove_RecordFieldVtbl groove__records__macros__RecordField_value_vec = {sizeof(groove_ValueVec), variant_read, variant_to_value, GROOVE_FK_ARRAY, variant_read_raw, default_read_tuple_raw, NULL, NULL};
const groove_RecordFieldVtbl groove__records__macros__RecordField_owned_record = {sizeof(groove_OwnedRecord), variant_read, variant_to_value, GROOVE_FK_RECORD, variant_read_raw, default_read_tuple_raw, NULL, NULL};
const groove_RecordFieldVtbl groove__records__macros__RecordField_enum_value = {sizeof(groove_EnumValue), variant_read, variant_to_value, GROOVE_FK_PAYLOAD_ENUM, variant_read_raw, default_read_tuple_raw, NULL, NULL};

/* ---------- Option<T> */
typedef jz_err (*read_present_fn)(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out);

static bool any_nonzero(const u8 *p, u32 n) { for (u32 i = 0; i < n; i++) if (p[i]) return true; return false; }

static jz_err read_nullable_raw(const u8 *p, u32 n, const groove_ValueType *inner, const groove_RecordFieldVtbl *ivt, read_present_fn read_present, groove_OptionField *out) {
    if (n == 0) return groove__records__Error__simple(GROOVE_REC_E_UnexpectedEof);
    u8 flag = p[0];
    switch (flag) {
    case 0:
        if (V(ValueType__is_fixed_size)(inner)) {
            if (any_nonzero(p + 1, n - 1)) return groove__records__Error__simple(GROOVE_REC_E_InvalidOffset);
        } else if (n - 1 != 0) {
            return groove__records__Error__simple(GROOVE_REC_E_InvalidOffset);
        }
        out->has = false;
        return JZ_OK;
    case 1:
        JZ_TRY(read_present(ivt, p + 1, n - 1, inner, out->payload));
        out->has = true;
        return JZ_OK;
    default: return jz_err_fmt(GROOVE_REC_E_InvalidNullFlag, "invalid nullable flag byte: %u", (unsigned)flag);
    }
}

static jz_err nullable_bytes_mismatch(void) {
    groove_ValueType expected = V(ValueType__nullable)(VT(GROOVE_VT_BYTES));
    jz_err e = groove__records__Error__type_mismatch(&expected);
    V(ValueType__drop)(&expected);
    return e;
}

jz_err groove__records__macros__Option__RecordField__read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) {
    const groove_DescriptorField *field;
    JZ_TRY(groove__records__BorrowedRecord__field(r, idx, &field));
    if (field->value_type.tag != GROOVE_VT_NULLABLE) return nullable_bytes_mismatch();
    jz_slice bytes;
    JZ_TRY(groove__records__BorrowedRecord__field_bytes_unchecked(r, idx, &bytes));
    return read_nullable_raw(bytes.ptr, bytes.len, field->value_type.u.inner, vt->inner_a, vt->inner_a->read_raw, out);
}

groove_Value groove__records__macros__Option__RecordField__to_value(const groove_RecordFieldVtbl *vt, const void *self) {
    const groove_OptionField *o = self;
    groove_Value v = {.tag = GROOVE_V_NULLABLE};
    if (o->has) {
        v.u.nullable = JZ_NEW(groove_Value);
        *v.u.nullable = vt->inner_a->to_value(vt->inner_a, o->payload);
    }
    return v;
}

jz_err groove__records__macros__Option__RecordField__read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    if (t->tag != GROOVE_VT_NULLABLE) return nullable_bytes_mismatch();
    return read_nullable_raw(p, n, t->u.inner, vt->inner_a, vt->inner_a->read_raw, out);
}

jz_err groove__records__macros__Option__RecordField__read_tuple_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    if (t->tag != GROOVE_VT_NULLABLE) return nullable_bytes_mismatch();
    return read_nullable_raw(p, n, t->u.inner, vt->inner_a, vt->inner_a->read_tuple_raw, out);
}

/* ---------- (A, B) */
static usize pair_b_offset(const groove_RecordFieldVtbl *vt) { return (vt->inner_a->size + 7) & ~(usize)7; }

jz_err groove__records__macros__Pair__RecordField__read(const groove_RecordFieldVtbl *vt, const groove_BorrowedRecord *r, u32 idx, void *out) {
    const groove_DescriptorField *field;
    jz_slice bytes;
    JZ_TRY(groove__records__BorrowedRecord__field(r, idx, &field));
    JZ_TRY(groove__records__BorrowedRecord__field_bytes_unchecked(r, idx, &bytes));
    return groove__records__macros__Pair__RecordField__read_raw(vt, bytes.ptr, bytes.len, &field->value_type, out);
}

groove_Value groove__records__macros__Pair__RecordField__to_value(const groove_RecordFieldVtbl *vt, const void *self) {
    groove_Value v = {.tag = GROOVE_V_TUPLE};
    jz_vec_reserve(&v.u.items, 2, sizeof(groove_Value));
    groove_Value a = vt->inner_a->to_value(vt->inner_a, self);
    groove_Value b = vt->inner_b->to_value(vt->inner_b, (const u8 *)self + pair_b_offset(vt));
    jz_vec_push(&v.u.items, &a, sizeof(a));
    jz_vec_push(&v.u.items, &b, sizeof(b));
    return v;
}

jz_err groove__records__macros__Pair__RecordField__read_raw(const groove_RecordFieldVtbl *vt, const u8 *p, u32 n, const groove_ValueType *t, void *out) {
    if (t->tag != GROOVE_VT_TUPLE) {
        groove_ValueType expected = VT(GROOVE_VT_TUPLE);
        groove_ValueType bytes_t = VT(GROOVE_VT_BYTES);
        jz_vec_push(&expected.u.tuple, &bytes_t, sizeof(bytes_t));
        jz_vec_push(&expected.u.tuple, &bytes_t, sizeof(bytes_t));
        jz_err e = groove__records__Error__type_mismatch(&expected);
        V(ValueType__drop)(&expected);
        return e;
    }
    const groove_ValueTypeVec *members = &t->u.tuple;
    if (members->len != 2) return groove__records__Error__arity_mismatch(2, members->len);
    u32 left_width, right_width, expected_width;
    if (!V(ValueType__fixed_size)(&members->ptr[0], &left_width)) return groove__records__Error__invalid_tuple_member(&members->ptr[0]);
    if (!V(ValueType__fixed_size)(&members->ptr[1], &right_width)) return groove__records__Error__invalid_tuple_member(&members->ptr[1]);
    if (left_width > UINT32_MAX - right_width) return groove__records__Error__simple(GROOVE_REC_E_LengthOverflow);
    expected_width = left_width + right_width;
    if (n != expected_width) return groove__records__Error__simple(GROOVE_REC_E_UnexpectedEof);
    JZ_TRY(vt->inner_a->read_tuple_raw(vt->inner_a, p, left_width, &members->ptr[0], out));
    return vt->inner_b->read_tuple_raw(vt->inner_b, p + left_width, right_width, &members->ptr[1], (u8 *)out + pair_b_offset(vt));
}

/* ---------- assert_record_field_layout (debug_assert! arms compile out in release) */
void groove__records__macros__assert_record_field_layout(groove_RecordDescriptor d, u32 idx, const char *name, groove_FieldKind kind) {
    u32 n;
    groove__records__RecordDescriptor__fields(d, &n);
    if (idx >= n) {
        jz_str msg = jz_format("record field index %u missing for %s", idx, name);
        jz_str_push_char(&msg, 0);
        jz_panic(msg.ptr);
    }
    (void)kind;
}
