/* op_types.c: port of crates/groove/src/ivm/op_types.rs.
 * Operator descriptors carried by IVM graph nodes, plus their derived
 * Clone/PartialEq/Hash/Debug impls (hand-written once each). Also hosts the
 * unit-wide Debug-formatting and small-container helpers. */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "op_types.h"

/* ======================================================================
 * core::fmt::DebugStruct / DebugTuple equivalents (shared by the unit)
 * ====================================================================== */
groove_DebugStruct groove__fmt__debug_struct(jz_str *out, const char *name) {
    groove_DebugStruct d = {out, false};
    jz_str_push_cstr(out, name);
    return d;
}
void groove__fmt__DebugStruct__field(groove_DebugStruct *d, const char *name) {
    jz_str_push_cstr(d->out, d->has_fields ? ", " : " { ");
    jz_str_push_cstr(d->out, name);
    jz_str_push_cstr(d->out, ": ");
    d->has_fields = true;
}
void groove__fmt__DebugStruct__finish(groove_DebugStruct *d) {
    if (d->has_fields) jz_str_push_cstr(d->out, " }");
}
void groove__fmt__debug_tuple_begin(jz_str *out, const char *name) {
    jz_str_push_cstr(out, name);
    jz_str_push_char(out, '(');
}
void groove__fmt__debug_tuple_next(jz_str *out, u32 index) {
    if (index) jz_str_push_cstr(out, ", ");
}
void groove__fmt__debug_tuple_end(jz_str *out) { jz_str_push_char(out, ')'); }

void groove__fmt__debug_str(jz_str *out, const jz_str *s) {
    jz_str_push_char(out, '"');
    for (u32 i = 0; i < s->len; i++) {
        char c = s->ptr[i];
        switch (c) {
        case '"': jz_str_push_cstr(out, "\\\""); break;
        case '\\': jz_str_push_cstr(out, "\\\\"); break;
        case '\n': jz_str_push_cstr(out, "\\n"); break;
        case '\r': jz_str_push_cstr(out, "\\r"); break;
        case '\t': jz_str_push_cstr(out, "\\t"); break;
        case '\0': jz_str_push_cstr(out, "\\0"); break;
        default:
            if ((u8)c < 0x20) jz_str_appendf(out, "\\u{%x}", (unsigned)(u8)c);
            else jz_str_push_char(out, c);
        }
    }
    jz_str_push_char(out, '"');
}
void groove__fmt__debug_bytes(jz_str *out, const u8 *p, u32 n) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) jz_str_appendf(out, i ? ", %u" : "%u", (unsigned)p[i]);
    jz_str_push_char(out, ']');
}
void groove__fmt__debug_usize_list(jz_str *out, const u32 *p, u32 n) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) jz_str_appendf(out, i ? ", %u" : "%u", (unsigned)p[i]);
    jz_str_push_char(out, ']');
}
void groove__fmt__debug_str_list(jz_str *out, const jz_str *p, u32 n) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove__fmt__debug_str(out, &p[i]);
    }
    jz_str_push_char(out, ']');
}
void groove__ivm__op_types__dbg_bool(jz_str *out, bool b) { jz_str_push_cstr(out, b ? "true" : "false"); }
void groove__ivm__op_types__dbg_opt_str(jz_str *out, bool some, const jz_str *s) {
    if (!some) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some(");
    groove__fmt__debug_str(out, s);
    jz_str_push_char(out, ')');
}
void groove__ivm__op_types__dbg_dir(jz_str *out, u8 d) { jz_str_push_cstr(out, d == GROOVE_DIR_ASC ? "Asc" : "Desc"); }
void groove__ivm__op_types__dbg_cmp(jz_str *out, u8 c) { jz_str_push_cstr(out, c == GROOVE_CMP_EXACT ? "Exact" : "Policy"); }
static void dbg_descriptor(jz_str *out, groove_RecordDescriptor d) { groove__records__RecordDescriptor__Debug__fmt(out, d); }

/* ======================================================================
 * small-container helpers
 * ====================================================================== */
void groove__ivm__jz_str_drop_v(void *s) { jz_str_drop((jz_str *)s); }
void groove__ivm__jz_str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }
void groove__ivm__jz_bytes_drop_v(void *b) { jz_bytes_drop((jz_bytes *)b); }
static void bytes_clone_v(void *dst, const void *src) { *(jz_bytes *)dst = jz_bytes_clone((const jz_bytes *)src); }
const jz_btree_desc groove__ivm__STRSET_DESC = { sizeof(jz_str), 0, jz_cmp_str, groove__ivm__jz_str_drop_v, NULL };

void groove__ivm__strvec_clone(groove_StrVec *dst, const groove_StrVec *src) { jz_vec_clone(dst, src, sizeof(jz_str), groove__ivm__jz_str_clone_v); }
void groove__ivm__strvec_drop(groove_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), groove__ivm__jz_str_drop_v); }
bool groove__ivm__strvec_eq(const groove_StrVec *a, const groove_StrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!jz_str_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
void groove__ivm__strvec_hash(const groove_StrVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove_hash_str(h, &v->ptr[i]);
}
void groove__ivm__usizevec_clone(groove_UsizeVec *dst, const groove_UsizeVec *src) { jz_vec_clone(dst, src, sizeof(u32), NULL); }
bool groove__ivm__usizevec_eq(const groove_UsizeVec *a, const groove_UsizeVec *b) {
    return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, a->len * sizeof(u32)) == 0);
}
void groove__ivm__usizevec_hash(const groove_UsizeVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    groove_hash_bytes_raw(h, v->ptr, v->len * (u32)sizeof(u32));
}
void groove__ivm__bytesvec_clone(groove_BytesVec *dst, const groove_BytesVec *src) { jz_vec_clone(dst, src, sizeof(jz_bytes), bytes_clone_v); }
void groove__ivm__bytesvec_drop(groove_BytesVec *v) { jz_vec_drop(v, sizeof(jz_bytes), groove__ivm__jz_bytes_drop_v); }
bool groove__ivm__bytesvec_eq(const groove_BytesVec *a, const groove_BytesVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!jz_bytes_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
void groove__ivm__bytesvec_hash(const groove_BytesVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove_hash_bytes(h, &v->ptr[i]);
}
static void dirvec_hash(const groove_DirectionVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove_hash_discr(h, v->ptr[i]);
}
static bool dirvec_eq(const groove_DirectionVec *a, const groove_DirectionVec *b) {
    return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, a->len) == 0);
}
static void dirvec_debug(jz_str *out, const groove_DirectionVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__dbg_dir(out, v->ptr[i]); }
    jz_str_push_char(out, ']');
}
void groove__ivm__op_types__opt_str_clone(bool some, const jz_str *src, jz_str *dst) { if (some) *dst = jz_str_clone(src); else memset(dst, 0, sizeof *dst); }
void groove__ivm__op_types__hash_opt_str(groove_Hasher *h, bool some, const jz_str *s) { groove_hash_discr(h, some); if (some) groove_hash_str(h, s); }
bool groove__ivm__op_types__opt_str_eq(bool sa, const jz_str *a, bool sb, const jz_str *b) { return sa == sb && (!sa || jz_str_eq(a, b)); }

/* Option<u8> / Option<u32> / Option<String> vectors (enum remap tables) */
void groove__ivm__op_types__optu8vec_clone(groove_OptU8Vec *dst, const groove_OptU8Vec *src) { jz_vec_clone(dst, src, sizeof(groove_OptU8), NULL); }
void groove__ivm__op_types__optu32vec_clone(groove_OptU32Vec *dst, const groove_OptU32Vec *src) { jz_vec_clone(dst, src, sizeof(groove_OptU32), NULL); }
bool groove__ivm__op_types__optu8vec_eq(const groove_OptU8Vec *a, const groove_OptU8Vec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) {
        if (a->ptr[i].some != b->ptr[i].some) return false;
        if (a->ptr[i].some && a->ptr[i].v != b->ptr[i].v) return false;
    }
    return true;
}
bool groove__ivm__op_types__optu32vec_eq(const groove_OptU32Vec *a, const groove_OptU32Vec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) {
        if (a->ptr[i].some != b->ptr[i].some) return false;
        if (a->ptr[i].some && a->ptr[i].v != b->ptr[i].v) return false;
    }
    return true;
}
void groove__ivm__op_types__optu8vec_hash(const groove_OptU8Vec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) { groove_hash_discr(h, v->ptr[i].some); if (v->ptr[i].some) groove_hash_u8(h, v->ptr[i].v); }
}
void groove__ivm__op_types__optu32vec_hash(const groove_OptU32Vec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) { groove_hash_discr(h, v->ptr[i].some); if (v->ptr[i].some) groove_hash_u32(h, v->ptr[i].v); }
}
void groove__ivm__op_types__optu8vec_debug(jz_str *out, const groove_OptU8Vec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        if (v->ptr[i].some) jz_str_appendf(out, "Some(%u)", (unsigned)v->ptr[i].v); else jz_str_push_cstr(out, "None");
    }
    jz_str_push_char(out, ']');
}
void groove__ivm__op_types__optu32vec_debug(jz_str *out, const groove_OptU32Vec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        if (v->ptr[i].some) jz_str_appendf(out, "Some(%u)", (unsigned)v->ptr[i].v); else jz_str_push_cstr(out, "None");
    }
    jz_str_push_char(out, ']');
}
static void optstr_clone_v(void *dst, const void *src) {
    const groove_OptStr *s = (const groove_OptStr *)src; groove_OptStr *d = (groove_OptStr *)dst;
    d->some = s->some; groove__ivm__op_types__opt_str_clone(s->some, &s->v, &d->v);
}
static void optstr_drop_v(void *p) { groove_OptStr *s = (groove_OptStr *)p; if (s->some) jz_str_drop(&s->v); }
static void optu8vec_drop_v(void *p) { jz_vec_drop(p, sizeof(groove_OptU8), NULL); }
static void optu32vec_drop_v(void *p) { jz_vec_drop(p, sizeof(groove_OptU32), NULL); }
static void optstrvec_drop_v(void *p) { jz_vec_drop(p, sizeof(groove_OptStr), optstr_drop_v); }
static void optu8vec_clone_v(void *d, const void *s) { groove__ivm__op_types__optu8vec_clone(d, s); }
static void optu32vec_clone_v(void *d, const void *s) { groove__ivm__op_types__optu32vec_clone(d, s); }
static void optstrvec_clone_v(void *d, const void *s) { jz_vec_clone(d, s, sizeof(groove_OptStr), optstr_clone_v); }

/* ======================================================================
 * LiteralValue
 * ====================================================================== */
void groove__ivm__literalvec_clone(groove_LiteralVec *dst, const groove_LiteralVec *src) {
    dst->ptr = NULL; dst->len = 0; dst->cap = 0;
    jz_vec_reserve(dst, src->len, sizeof(groove_LiteralValue));
    for (u32 i = 0; i < src->len; i++) groove__ivm__op_types__LiteralValue__Clone__clone(&dst->ptr[i], &src->ptr[i]);
    dst->len = src->len;
}
static void literal_drop_v(void *v) { groove__ivm__op_types__LiteralValue__drop((groove_LiteralValue *)v); }
void groove__ivm__literalvec_drop(groove_LiteralVec *v) { jz_vec_drop(v, sizeof(groove_LiteralValue), literal_drop_v); }
bool groove__ivm__literalvec_eq(const groove_LiteralVec *a, const groove_LiteralVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__op_types__LiteralValue__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
void groove__ivm__literalvec_hash(const groove_LiteralVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove__ivm__op_types__LiteralValue__Hash__hash(&v->ptr[i], h);
}
void groove__ivm__literalvec_debug(jz_str *out, const groove_LiteralVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove__ivm__op_types__LiteralValue__Debug__fmt(out, &v->ptr[i]);
    }
    jz_str_push_char(out, ']');
}

void groove__ivm__op_types__LiteralValue__Clone__clone(groove_LiteralValue *dst, const groove_LiteralValue *src) {
    dst->tag = src->tag;
    switch (src->tag) {
    case GROOVE_LIT_STRING: dst->u.s = jz_str_clone(&src->u.s); break;
    case GROOVE_LIT_BYTES: dst->u.bytes = jz_bytes_clone(&src->u.bytes); break;
    case GROOVE_LIT_TUPLE:
    case GROOVE_LIT_ARRAY: groove__ivm__literalvec_clone(&dst->u.items, &src->u.items); break;
    case GROOVE_LIT_NULLABLE:
        if (src->u.nullable) {
            dst->u.nullable = JZ_NEW(groove_LiteralValue);
            groove__ivm__op_types__LiteralValue__Clone__clone(dst->u.nullable, src->u.nullable);
        } else dst->u.nullable = NULL;
        break;
    case GROOVE_LIT_NATIVE_RECORD:
        dst->u.native.descriptor = jz_bytes_clone(&src->u.native.descriptor);
        dst->u.native.raw = jz_bytes_clone(&src->u.native.raw);
        break;
    default: dst->u = src->u; break;
    }
}

void groove__ivm__op_types__LiteralValue__drop(groove_LiteralValue *v) {
    switch (v->tag) {
    case GROOVE_LIT_STRING: jz_str_drop(&v->u.s); break;
    case GROOVE_LIT_BYTES: jz_bytes_drop(&v->u.bytes); break;
    case GROOVE_LIT_TUPLE:
    case GROOVE_LIT_ARRAY: groove__ivm__literalvec_drop(&v->u.items); break;
    case GROOVE_LIT_NULLABLE:
        if (v->u.nullable) { groove__ivm__op_types__LiteralValue__drop(v->u.nullable); jz_free(v->u.nullable); }
        break;
    case GROOVE_LIT_NATIVE_RECORD:
        jz_bytes_drop(&v->u.native.descriptor);
        jz_bytes_drop(&v->u.native.raw);
        break;
    default: break;
    }
}

bool groove__ivm__op_types__LiteralValue__PartialEq__eq(const groove_LiteralValue *a, const groove_LiteralValue *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_LIT_U8: case GROOVE_LIT_ENUM_TAG: return a->u.u8_ == b->u.u8_;
    case GROOVE_LIT_U16: return a->u.u16_ == b->u.u16_;
    case GROOVE_LIT_U32: return a->u.u32_ == b->u.u32_;
    case GROOVE_LIT_U64: case GROOVE_LIT_U48: case GROOVE_LIT_F64: return a->u.u64_ == b->u.u64_;
    case GROOVE_LIT_I64: return a->u.i64_ == b->u.i64_;
    case GROOVE_LIT_I32: return a->u.i32_ == b->u.i32_;
    case GROOVE_LIT_BOOL: return a->u.b == b->u.b;
    case GROOVE_LIT_STRING: return jz_str_eq(&a->u.s, &b->u.s);
    case GROOVE_LIT_BYTES: return jz_bytes_eq(&a->u.bytes, &b->u.bytes);
    case GROOVE_LIT_UUID: return jz_uuid_eq(&a->u.uuid, &b->u.uuid);
    case GROOVE_LIT_TUPLE: case GROOVE_LIT_ARRAY: return groove__ivm__literalvec_eq(&a->u.items, &b->u.items);
    case GROOVE_LIT_NULLABLE:
        if (!a->u.nullable || !b->u.nullable) return a->u.nullable == b->u.nullable;
        return groove__ivm__op_types__LiteralValue__PartialEq__eq(a->u.nullable, b->u.nullable);
    case GROOVE_LIT_NATIVE_RECORD:
        return jz_bytes_eq(&a->u.native.descriptor, &b->u.native.descriptor) && jz_bytes_eq(&a->u.native.raw, &b->u.native.raw);
    case GROOVE_LIT_RECORD: return true;
    }
    return false;
}

#define CMP3(x, y) ((x) < (y) ? -1 : (x) > (y) ? 1 : 0)
static int literalvec_cmp(const groove_LiteralVec *a, const groove_LiteralVec *b) {
    u32 n = a->len < b->len ? a->len : b->len;
    for (u32 i = 0; i < n; i++) {
        int c = groove__ivm__op_types__LiteralValue__PartialOrd__partial_cmp(&a->ptr[i], &b->ptr[i]);
        if (c) return c;
    }
    return CMP3(a->len, b->len);
}
/* derived lexicographic Ord: discriminant first, then payload */
int groove__ivm__op_types__LiteralValue__PartialOrd__partial_cmp(const groove_LiteralValue *a, const groove_LiteralValue *b) {
    if (a->tag != b->tag) return CMP3(a->tag, b->tag);
    switch (a->tag) {
    case GROOVE_LIT_U8: case GROOVE_LIT_ENUM_TAG: return CMP3(a->u.u8_, b->u.u8_);
    case GROOVE_LIT_U16: return CMP3(a->u.u16_, b->u.u16_);
    case GROOVE_LIT_U32: return CMP3(a->u.u32_, b->u.u32_);
    case GROOVE_LIT_U64: case GROOVE_LIT_U48: case GROOVE_LIT_F64: return CMP3(a->u.u64_, b->u.u64_);
    case GROOVE_LIT_I64: return CMP3(a->u.i64_, b->u.i64_);
    case GROOVE_LIT_I32: return CMP3(a->u.i32_, b->u.i32_);
    case GROOVE_LIT_BOOL: return CMP3(a->u.b, b->u.b);
    case GROOVE_LIT_STRING: return jz_str_cmp(&a->u.s, &b->u.s);
    case GROOVE_LIT_BYTES: return jz_bytes_cmp(&a->u.bytes, &b->u.bytes);
    case GROOVE_LIT_UUID: return jz_uuid_cmp(&a->u.uuid, &b->u.uuid);
    case GROOVE_LIT_TUPLE: case GROOVE_LIT_ARRAY: return literalvec_cmp(&a->u.items, &b->u.items);
    case GROOVE_LIT_NULLABLE:
        if (!a->u.nullable || !b->u.nullable) return CMP3(a->u.nullable != NULL, b->u.nullable != NULL);
        return groove__ivm__op_types__LiteralValue__PartialOrd__partial_cmp(a->u.nullable, b->u.nullable);
    case GROOVE_LIT_NATIVE_RECORD: {
        int c = jz_bytes_cmp(&a->u.native.descriptor, &b->u.native.descriptor);
        return c ? c : jz_bytes_cmp(&a->u.native.raw, &b->u.native.raw);
    }
    case GROOVE_LIT_RECORD: return 0;
    }
    return 0;
}

void groove__ivm__op_types__LiteralValue__Hash__hash(const groove_LiteralValue *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    switch (v->tag) {
    case GROOVE_LIT_U8: case GROOVE_LIT_ENUM_TAG: groove_hash_u8(h, v->u.u8_); break;
    case GROOVE_LIT_U16: groove_hash_u16(h, v->u.u16_); break;
    case GROOVE_LIT_U32: groove_hash_u32(h, v->u.u32_); break;
    case GROOVE_LIT_U64: case GROOVE_LIT_U48: case GROOVE_LIT_F64: groove_hash_u64(h, v->u.u64_); break;
    case GROOVE_LIT_I64: groove_hash_u64(h, (u64)v->u.i64_); break;
    case GROOVE_LIT_I32: groove_hash_u32(h, (u32)v->u.i32_); break;
    case GROOVE_LIT_BOOL: groove_hash_bool(h, v->u.b); break;
    case GROOVE_LIT_STRING: groove_hash_str(h, &v->u.s); break;
    case GROOVE_LIT_BYTES: groove_hash_bytes(h, &v->u.bytes); break;
    case GROOVE_LIT_UUID: groove_hash_usize(h, 16); groove_hash_bytes_raw(h, v->u.uuid.b, 16); break;
    case GROOVE_LIT_TUPLE: case GROOVE_LIT_ARRAY: groove__ivm__literalvec_hash(&v->u.items, h); break;
    case GROOVE_LIT_NULLABLE:
        groove_hash_discr(h, v->u.nullable != NULL);
        if (v->u.nullable) groove__ivm__op_types__LiteralValue__Hash__hash(v->u.nullable, h);
        break;
    case GROOVE_LIT_NATIVE_RECORD:
        groove_hash_bytes(h, &v->u.native.descriptor);
        groove_hash_bytes(h, &v->u.native.raw);
        break;
    case GROOVE_LIT_RECORD: break;
    }
}

void groove__ivm__op_types__LiteralValue__Debug__fmt(jz_str *out, const groove_LiteralValue *v) {
    static const char *const names[] = {"U8", "U16", "U32", "U64", "U48", "I64", "I32", "F64", "Bool", "EnumTag",
                                        "String", "Bytes", "Uuid", "Tuple", "Array", "Nullable", "NativeRecord", "Record"};
    if (v->tag == GROOVE_LIT_RECORD) { jz_str_push_cstr(out, "Record"); return; }
    if (v->tag == GROOVE_LIT_NATIVE_RECORD) {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "NativeRecord");
        groove__fmt__DebugStruct__field(&d, "descriptor");
        groove__fmt__debug_bytes(out, v->u.native.descriptor.ptr, v->u.native.descriptor.len);
        groove__fmt__DebugStruct__field(&d, "raw");
        groove__fmt__debug_bytes(out, v->u.native.raw.ptr, v->u.native.raw.len);
        groove__fmt__DebugStruct__finish(&d);
        return;
    }
    groove__fmt__debug_tuple_begin(out, names[v->tag]);
    switch (v->tag) {
    case GROOVE_LIT_U8: case GROOVE_LIT_ENUM_TAG: jz_str_appendf(out, "%u", (unsigned)v->u.u8_); break;
    case GROOVE_LIT_U16: jz_str_appendf(out, "%u", (unsigned)v->u.u16_); break;
    case GROOVE_LIT_U32: jz_str_appendf(out, "%u", (unsigned)v->u.u32_); break;
    case GROOVE_LIT_U64: case GROOVE_LIT_U48: case GROOVE_LIT_F64: jz_str_appendf(out, "%llu", (unsigned long long)v->u.u64_); break;
    case GROOVE_LIT_I64: jz_str_appendf(out, "%lld", (long long)v->u.i64_); break;
    case GROOVE_LIT_I32: jz_str_appendf(out, "%d", (int)v->u.i32_); break;
    case GROOVE_LIT_BOOL: groove__ivm__op_types__dbg_bool(out, v->u.b); break;
    case GROOVE_LIT_STRING: groove__fmt__debug_str(out, &v->u.s); break;
    case GROOVE_LIT_BYTES: groove__fmt__debug_bytes(out, v->u.bytes.ptr, v->u.bytes.len); break;
    case GROOVE_LIT_UUID: { char buf[37]; jz_uuid_to_string(&v->u.uuid, buf); jz_str_push(out, buf, 36); break; }
    case GROOVE_LIT_TUPLE: case GROOVE_LIT_ARRAY: groove__ivm__literalvec_debug(out, &v->u.items); break;
    case GROOVE_LIT_NULLABLE:
        if (v->u.nullable) {
            jz_str_push_cstr(out, "Some(");
            groove__ivm__op_types__LiteralValue__Debug__fmt(out, v->u.nullable);
            jz_str_push_char(out, ')');
        } else jz_str_push_cstr(out, "None");
        break;
    default: break;
    }
    groove__fmt__debug_tuple_end(out);
}

/* impl From<Value> for LiteralValue (consumes `value`) */
void groove__ivm__op_types__LiteralValue__From_Value__from(groove_LiteralValue *out, groove_Value *value) {
    switch (value->tag) {
    case GROOVE_V_U8: out->tag = GROOVE_LIT_U8; out->u.u8_ = value->u.u8_; break;
    case GROOVE_V_U16: out->tag = GROOVE_LIT_U16; out->u.u16_ = value->u.u16_; break;
    case GROOVE_V_U32: out->tag = GROOVE_LIT_U32; out->u.u32_ = value->u.u32_; break;
    case GROOVE_V_U64: out->tag = GROOVE_LIT_U64; out->u.u64_ = value->u.u64_; break;
    case GROOVE_V_U48: out->tag = GROOVE_LIT_U48; out->u.u64_ = value->u.u64_; break;
    case GROOVE_V_I64: out->tag = GROOVE_LIT_I64; out->u.i64_ = value->u.i64_; break;
    case GROOVE_V_I32: out->tag = GROOVE_LIT_I32; out->u.i32_ = value->u.i32_; break;
    case GROOVE_V_F64: { u64 bits; memcpy(&bits, &value->u.f64_, 8); out->tag = GROOVE_LIT_F64; out->u.u64_ = bits; break; }
    case GROOVE_V_BOOL: out->tag = GROOVE_LIT_BOOL; out->u.b = value->u.b; break;
    case GROOVE_V_ENUM_TAG: out->tag = GROOVE_LIT_ENUM_TAG; out->u.u8_ = value->u.enum_tag; break;
    case GROOVE_V_STRING: out->tag = GROOVE_LIT_STRING; out->u.s = value->u.s; break;
    case GROOVE_V_BYTES: out->tag = GROOVE_LIT_BYTES; out->u.bytes = value->u.bytes; break;
    case GROOVE_V_UUID: out->tag = GROOVE_LIT_UUID; out->u.uuid = value->u.uuid; break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY: {
        groove_ValueVec values = value->u.items;
        out->tag = value->tag == GROOVE_V_TUPLE ? GROOVE_LIT_TUPLE : GROOVE_LIT_ARRAY;
        out->u.items.ptr = NULL; out->u.items.len = 0; out->u.items.cap = 0;
        jz_vec_reserve(&out->u.items, values.len, sizeof(groove_LiteralValue));
        for (u32 i = 0; i < values.len; i++) groove__ivm__op_types__LiteralValue__From_Value__from(&out->u.items.ptr[i], &values.ptr[i]);
        out->u.items.len = values.len;
        jz_free(values.ptr);
        break;
    }
    case GROOVE_V_NULLABLE:
        out->tag = GROOVE_LIT_NULLABLE;
        if (value->u.nullable) {
            out->u.nullable = JZ_NEW(groove_LiteralValue);
            groove__ivm__op_types__LiteralValue__From_Value__from(out->u.nullable, value->u.nullable);
            jz_free(value->u.nullable);
        } else out->u.nullable = NULL;
        break;
    case GROOVE_V_RECORD: {
        jz_bytes descriptor = {0};
        jz_pc_w w = {&descriptor};
        groove__records__RecordDescriptor__Serialize__serialize(&w, value->u.record.descriptor);
        out->tag = GROOVE_LIT_NATIVE_RECORD;
        out->u.native.descriptor = descriptor;
        out->u.native.raw = jz_bytes_clone(&value->u.record.raw);
        groove__records__OwnedRecord__drop(&value->u.record);
        break;
    }
    case GROOVE_V_ENUM:
    case GROOVE_V_LARGE:
        groove__records__values__Value__drop(value);
        out->tag = GROOVE_LIT_RECORD;
        break;
    }
}

/* Infer the representation used by an untyped literal projection. */
bool groove__ivm__op_types__LiteralValue__value_type(const groove_LiteralValue *v, groove_ValueType *out) {
    switch (v->tag) {
    case GROOVE_LIT_U8: out->tag = GROOVE_VT_U8; return true;
    case GROOVE_LIT_U16: out->tag = GROOVE_VT_U16; return true;
    case GROOVE_LIT_U32: out->tag = GROOVE_VT_U32; return true;
    case GROOVE_LIT_U64: out->tag = GROOVE_VT_U64; return true;
    case GROOVE_LIT_U48: out->tag = GROOVE_VT_U48; return true;
    case GROOVE_LIT_I64: out->tag = GROOVE_VT_I64; return true;
    case GROOVE_LIT_I32: out->tag = GROOVE_VT_I32; return true;
    case GROOVE_LIT_F64: out->tag = GROOVE_VT_F64; return true;
    case GROOVE_LIT_BOOL: out->tag = GROOVE_VT_BOOL; return true;
    case GROOVE_LIT_ENUM_TAG: out->tag = GROOVE_VT_U8; return true;
    case GROOVE_LIT_STRING: out->tag = GROOVE_VT_STRING; return true;
    case GROOVE_LIT_BYTES: out->tag = GROOVE_VT_BYTES; return true;
    case GROOVE_LIT_UUID: out->tag = GROOVE_VT_UUID; return true;
    case GROOVE_LIT_TUPLE: {
        JZ_VEC(groove_ValueType) types = {0};
        for (u32 i = 0; i < v->u.items.len; i++) {
            groove_ValueType t;
            if (!groove__ivm__op_types__LiteralValue__value_type(&v->u.items.ptr[i], &t)) {
                for (u32 j = 0; j < types.len; j++) groove__records__values__ValueType__drop(&types.ptr[j]);
                jz_free(types.ptr);
                return false;
            }
            JZ_VEC_PUSH(types, t);
        }
        out->tag = GROOVE_VT_TUPLE;
        out->u.tuple.ptr = types.ptr; out->u.tuple.len = types.len; out->u.tuple.cap = types.cap;
        return true;
    }
    case GROOVE_LIT_ARRAY: {
        groove_ValueType t;
        if (v->u.items.len == 0 || !groove__ivm__op_types__LiteralValue__value_type(&v->u.items.ptr[0], &t)) return false;
        out->tag = GROOVE_VT_ARRAY;
        out->u.inner = jz_memdup(&t, sizeof t);
        return true;
    }
    case GROOVE_LIT_NULLABLE: {
        groove_ValueType t;
        if (!v->u.nullable || !groove__ivm__op_types__LiteralValue__value_type(v->u.nullable, &t)) return false;
        out->tag = GROOVE_VT_NULLABLE;
        out->u.inner = jz_memdup(&t, sizeof t);
        return true;
    }
    case GROOVE_LIT_NATIVE_RECORD: {
        jz_pc_r r = {v->u.native.descriptor.ptr, v->u.native.descriptor.ptr + v->u.native.descriptor.len, 0};
        groove_RecordDescriptor d;
        jz_err e = groove__records__RecordDescriptor__Deserialize__deserialize(&r, &d);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
        e = jz_pc_r_finish(&r);
        if (JZ_IS_ERR(e)) { jz_err_drop(&e); return false; }
        out->tag = GROOVE_VT_RECORD;
        out->u.record = jz_memdup(&d, sizeof d);
        return true;
    }
    case GROOVE_LIT_RECORD: return false;
    }
    return false;
}

void groove__ivm__op_types__LiteralValue__to_value(const groove_LiteralValue *v, groove_Value *out) {
    switch (v->tag) {
    case GROOVE_LIT_U8: out->tag = GROOVE_V_U8; out->u.u8_ = v->u.u8_; break;
    case GROOVE_LIT_U16: out->tag = GROOVE_V_U16; out->u.u16_ = v->u.u16_; break;
    case GROOVE_LIT_U32: out->tag = GROOVE_V_U32; out->u.u32_ = v->u.u32_; break;
    case GROOVE_LIT_U64: out->tag = GROOVE_V_U64; out->u.u64_ = v->u.u64_; break;
    case GROOVE_LIT_U48: out->tag = GROOVE_V_U48; out->u.u64_ = v->u.u64_; break;
    case GROOVE_LIT_I64: out->tag = GROOVE_V_I64; out->u.i64_ = v->u.i64_; break;
    case GROOVE_LIT_I32: out->tag = GROOVE_V_I32; out->u.i32_ = v->u.i32_; break;
    case GROOVE_LIT_F64: out->tag = GROOVE_V_F64; memcpy(&out->u.f64_, &v->u.u64_, 8); break;
    case GROOVE_LIT_BOOL: out->tag = GROOVE_V_BOOL; out->u.b = v->u.b; break;
    case GROOVE_LIT_ENUM_TAG: out->tag = GROOVE_V_ENUM_TAG; out->u.enum_tag = v->u.u8_; break;
    case GROOVE_LIT_STRING: out->tag = GROOVE_V_STRING; out->u.s = jz_str_clone(&v->u.s); break;
    case GROOVE_LIT_BYTES: out->tag = GROOVE_V_BYTES; out->u.bytes = jz_bytes_clone(&v->u.bytes); break;
    case GROOVE_LIT_UUID: out->tag = GROOVE_V_UUID; out->u.uuid = v->u.uuid; break;
    case GROOVE_LIT_TUPLE:
    case GROOVE_LIT_ARRAY: {
        out->tag = v->tag == GROOVE_LIT_TUPLE ? GROOVE_V_TUPLE : GROOVE_V_ARRAY;
        out->u.items.ptr = NULL; out->u.items.len = 0; out->u.items.cap = 0;
        jz_vec_reserve(&out->u.items, v->u.items.len, sizeof(groove_Value));
        for (u32 i = 0; i < v->u.items.len; i++) groove__ivm__op_types__LiteralValue__to_value(&v->u.items.ptr[i], &out->u.items.ptr[i]);
        out->u.items.len = v->u.items.len;
        break;
    }
    case GROOVE_LIT_NULLABLE:
        out->tag = GROOVE_V_NULLABLE;
        if (v->u.nullable) {
            out->u.nullable = JZ_NEW(groove_Value);
            groove__ivm__op_types__LiteralValue__to_value(v->u.nullable, out->u.nullable);
        } else out->u.nullable = NULL;
        break;
    case GROOVE_LIT_NATIVE_RECORD: {
        jz_pc_r r = {v->u.native.descriptor.ptr, v->u.native.descriptor.ptr + v->u.native.descriptor.len, 0};
        groove_RecordDescriptor d;
        jz_err e = groove__records__RecordDescriptor__Deserialize__deserialize(&r, &d);
        if (JZ_IS_ERR(e)) jz_panic("validated native record descriptor");
        out->tag = GROOVE_V_RECORD;
        out->u.record = groove__records__OwnedRecord__new(jz_bytes_clone(&v->u.native.raw), d);
        break;
    }
    case GROOVE_LIT_RECORD:
        jz_panic("internal error: entered unreachable code: unsupported literals are rejected during type validation");
    }
}

/* ======================================================================
 * StaticScanSpec
 * ====================================================================== */
void groove__ivm__op_types__StaticScanSpec__Clone__clone(groove_StaticScanSpec *dst, const groove_StaticScanSpec *src) {
    dst->tag = src->tag;
    dst->max_items = src->max_items;
    groove__ivm__literalvec_clone(&dst->values, &src->values);
    if (src->tag == GROOVE_SCAN_RANGE) groove__ivm__literalvec_clone(&dst->end, &src->end);
    else { dst->end.ptr = NULL; dst->end.len = 0; dst->end.cap = 0; }
}
void groove__ivm__op_types__StaticScanSpec__drop(groove_StaticScanSpec *v) {
    groove__ivm__literalvec_drop(&v->values);
    if (v->tag == GROOVE_SCAN_RANGE) groove__ivm__literalvec_drop(&v->end);
}
bool groove__ivm__op_types__StaticScanSpec__PartialEq__eq(const groove_StaticScanSpec *a, const groove_StaticScanSpec *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_SCAN_POINT:
    case GROOVE_SCAN_PREFIX: return groove__ivm__literalvec_eq(&a->values, &b->values);
    case GROOVE_SCAN_PREFIX_LIMIT:
    case GROOVE_SCAN_REVERSE_PREFIX_LIMIT: return groove__ivm__literalvec_eq(&a->values, &b->values) && a->max_items == b->max_items;
    case GROOVE_SCAN_RANGE: return groove__ivm__literalvec_eq(&a->values, &b->values) && groove__ivm__literalvec_eq(&a->end, &b->end);
    }
    return false;
}
void groove__ivm__op_types__StaticScanSpec__Hash__hash(const groove_StaticScanSpec *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    groove__ivm__literalvec_hash(&v->values, h);
    switch (v->tag) {
    case GROOVE_SCAN_PREFIX_LIMIT:
    case GROOVE_SCAN_REVERSE_PREFIX_LIMIT: groove_hash_usize(h, v->max_items); break;
    case GROOVE_SCAN_RANGE: groove__ivm__literalvec_hash(&v->end, h); break;
    default: break;
    }
}
void groove__ivm__op_types__StaticScanSpec__Debug__fmt(jz_str *out, const groove_StaticScanSpec *v) {
    switch (v->tag) {
    case GROOVE_SCAN_POINT:
    case GROOVE_SCAN_PREFIX:
        groove__fmt__debug_tuple_begin(out, v->tag == GROOVE_SCAN_POINT ? "Point" : "Prefix");
        groove__ivm__literalvec_debug(out, &v->values);
        groove__fmt__debug_tuple_end(out);
        break;
    case GROOVE_SCAN_PREFIX_LIMIT:
    case GROOVE_SCAN_REVERSE_PREFIX_LIMIT: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, v->tag == GROOVE_SCAN_PREFIX_LIMIT ? "PrefixLimit" : "ReversePrefixLimit");
        groove__fmt__DebugStruct__field(&d, "prefix");
        groove__ivm__literalvec_debug(out, &v->values);
        groove__fmt__DebugStruct__field(&d, "max_items");
        jz_str_appendf(out, "%u", (unsigned)v->max_items);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_SCAN_RANGE: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "Range");
        groove__fmt__DebugStruct__field(&d, "start");
        groove__ivm__literalvec_debug(out, &v->values);
        groove__fmt__DebugStruct__field(&d, "end");
        groove__ivm__literalvec_debug(out, &v->end);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    }
}

void groove__ivm__op_types__scan_opt_clone(bool has, const groove_StaticScanSpec *src, groove_StaticScanSpec *dst) {
    if (has) groove__ivm__op_types__StaticScanSpec__Clone__clone(dst, src); else memset(dst, 0, sizeof *dst);
}
bool groove__ivm__op_types__scan_opt_eq(bool ha, const groove_StaticScanSpec *a, bool hb, const groove_StaticScanSpec *b) {
    return ha == hb && (!ha || groove__ivm__op_types__StaticScanSpec__PartialEq__eq(a, b));
}
void groove__ivm__op_types__scan_opt_hash(groove_Hasher *h, bool has, const groove_StaticScanSpec *s) {
    groove_hash_discr(h, has);
    if (has) groove__ivm__op_types__StaticScanSpec__Hash__hash(s, h);
}
void groove__ivm__op_types__scan_opt_debug(jz_str *out, bool has, const groove_StaticScanSpec *s) {
    if (!has) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some(");
    groove__ivm__op_types__StaticScanSpec__Debug__fmt(out, s);
    jz_str_push_char(out, ')');
}

/* ======================================================================
 * VariantProjectionTarget / IndexCandidateFilter / DurableStorage / FieldRef
 * ====================================================================== */
static void vpt_opt_clone(bool has, const groove_VariantProjectionTarget *src, groove_VariantProjectionTarget *dst) {
    if (has) { dst->tag = src->tag; dst->name = jz_str_clone(&src->name); } else memset(dst, 0, sizeof *dst);
}
static bool vpt_eq(const groove_VariantProjectionTarget *a, const groove_VariantProjectionTarget *b) {
    return a->tag == b->tag && jz_str_eq(&a->name, &b->name);
}
static bool vpt_opt_eq(bool ha, const groove_VariantProjectionTarget *a, bool hb, const groove_VariantProjectionTarget *b) {
    return ha == hb && (!ha || vpt_eq(a, b));
}
static void vpt_opt_hash(groove_Hasher *h, bool has, const groove_VariantProjectionTarget *v) {
    groove_hash_discr(h, has);
    if (has) { groove_hash_discr(h, v->tag); groove_hash_str(h, &v->name); }
}
static void vpt_opt_debug(jz_str *out, bool has, const groove_VariantProjectionTarget *v) {
    if (!has) { jz_str_push_cstr(out, "None"); return; }
    jz_str_push_cstr(out, "Some(");
    groove__fmt__debug_tuple_begin(out, v->tag == GROOVE_VPT_NAMED ? "Named" : "SchemaIndex");
    groove__fmt__debug_str(out, &v->name);
    groove__fmt__debug_tuple_end(out);
    jz_str_push_char(out, ')');
}

void groove__ivm__graph__IndexCandidateFilter__Clone__clone(groove_IndexCandidateFilter *dst, const groove_IndexCandidateFilter *src) {
    dst->table = jz_str_clone(&src->table);
    dst->index = jz_str_clone(&src->index);
    groove__ivm__op_types__StaticScanSpec__Clone__clone(&dst->scan, &src->scan);
    dst->source_column = jz_str_clone(&src->source_column);
    dst->candidate_column = jz_str_clone(&src->candidate_column);
}
void groove__ivm__graph__IndexCandidateFilter__drop(groove_IndexCandidateFilter *v) {
    jz_str_drop(&v->table); jz_str_drop(&v->index);
    groove__ivm__op_types__StaticScanSpec__drop(&v->scan);
    jz_str_drop(&v->source_column); jz_str_drop(&v->candidate_column);
}
bool groove__ivm__graph__IndexCandidateFilter__PartialEq__eq(const groove_IndexCandidateFilter *a, const groove_IndexCandidateFilter *b) {
    return jz_str_eq(&a->table, &b->table) && jz_str_eq(&a->index, &b->index) &&
           groove__ivm__op_types__StaticScanSpec__PartialEq__eq(&a->scan, &b->scan) &&
           jz_str_eq(&a->source_column, &b->source_column) && jz_str_eq(&a->candidate_column, &b->candidate_column);
}
void groove__ivm__graph__IndexCandidateFilter__Hash__hash(const groove_IndexCandidateFilter *v, groove_Hasher *h) {
    groove_hash_str(h, &v->table); groove_hash_str(h, &v->index);
    groove__ivm__op_types__StaticScanSpec__Hash__hash(&v->scan, h);
    groove_hash_str(h, &v->source_column); groove_hash_str(h, &v->candidate_column);
}
void groove__ivm__graph__IndexCandidateFilter__Debug__fmt(jz_str *out, const groove_IndexCandidateFilter *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "IndexCandidateFilter");
    groove__fmt__DebugStruct__field(&d, "table"); groove__fmt__debug_str(out, &v->table);
    groove__fmt__DebugStruct__field(&d, "index"); groove__fmt__debug_str(out, &v->index);
    groove__fmt__DebugStruct__field(&d, "scan"); groove__ivm__op_types__StaticScanSpec__Debug__fmt(out, &v->scan);
    groove__fmt__DebugStruct__field(&d, "source_column"); groove__fmt__debug_str(out, &v->source_column);
    groove__fmt__DebugStruct__field(&d, "candidate_column"); groove__fmt__debug_str(out, &v->candidate_column);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__graph__DurableStorage__Clone__clone(groove_DurableStorage *dst, const groove_DurableStorage *src) {
    dst->column_family = jz_str_clone(&src->column_family);
    dst->key_prefix = jz_bytes_clone(&src->key_prefix);
    dst->name = jz_str_clone(&src->name);
}
void groove__ivm__graph__DurableStorage__drop(groove_DurableStorage *v) {
    jz_str_drop(&v->column_family); jz_bytes_drop(&v->key_prefix); jz_str_drop(&v->name);
}
bool groove__ivm__graph__DurableStorage__PartialEq__eq(const groove_DurableStorage *a, const groove_DurableStorage *b) {
    return jz_str_eq(&a->column_family, &b->column_family) && jz_bytes_eq(&a->key_prefix, &b->key_prefix) && jz_str_eq(&a->name, &b->name);
}
void groove__ivm__graph__DurableStorage__Hash__hash(const groove_DurableStorage *v, groove_Hasher *h) {
    groove_hash_str(h, &v->column_family); groove_hash_bytes(h, &v->key_prefix); groove_hash_str(h, &v->name);
}
void groove__ivm__graph__DurableStorage__Debug__fmt(jz_str *out, const groove_DurableStorage *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "DurableStorage");
    groove__fmt__DebugStruct__field(&d, "column_family"); groove__fmt__debug_str(out, &v->column_family);
    groove__fmt__DebugStruct__field(&d, "key_prefix"); groove__fmt__debug_bytes(out, v->key_prefix.ptr, v->key_prefix.len);
    groove__fmt__DebugStruct__field(&d, "name"); groove__fmt__debug_str(out, &v->name);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__graph__FieldRef__Clone__clone(groove_FieldRef *dst, const groove_FieldRef *src) {
    dst->tag = src->tag;
    dst->index = src->index;
    if (src->tag == GROOVE_FIELDREF_RESOLVED) memset(&dst->name, 0, sizeof dst->name);
    else dst->name = jz_str_clone(&src->name);
}
void groove__ivm__graph__FieldRef__drop(groove_FieldRef *v) { if (v->tag != GROOVE_FIELDREF_RESOLVED) jz_str_drop(&v->name); }
bool groove__ivm__graph__FieldRef__PartialEq__eq(const groove_FieldRef *a, const groove_FieldRef *b) {
    if (a->tag != b->tag) return false;
    return a->tag == GROOVE_FIELDREF_RESOLVED ? a->index == b->index : jz_str_eq(&a->name, &b->name);
}
void groove__ivm__graph__FieldRef__Hash__hash(const groove_FieldRef *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    if (v->tag == GROOVE_FIELDREF_RESOLVED) groove_hash_usize(h, v->index);
    else groove_hash_str(h, &v->name);
}
void groove__ivm__graph__FieldRef__Debug__fmt(jz_str *out, const groove_FieldRef *v) {
    switch (v->tag) {
    case GROOVE_FIELDREF_NAME: groove__fmt__debug_tuple_begin(out, "Name"); groove__fmt__debug_str(out, &v->name); break;
    case GROOVE_FIELDREF_STORED_NAME: groove__fmt__debug_tuple_begin(out, "StoredName"); groove__fmt__debug_str(out, &v->name); break;
    default: groove__fmt__debug_tuple_begin(out, "Resolved"); jz_str_appendf(out, "%u", (unsigned)v->index); break;
    }
    groove__fmt__debug_tuple_end(out);
}

/* ======================================================================
 * RecursiveEnumRemaps
 * ====================================================================== */
static const jz_btree_desc REMAP_U8_DESC = { sizeof(jz_str), sizeof(groove_OptU8Vec), jz_cmp_str, groove__ivm__jz_str_drop_v, optu8vec_drop_v };
static const jz_btree_desc REMAP_U32_DESC = { sizeof(jz_str), sizeof(groove_OptU32Vec), jz_cmp_str, groove__ivm__jz_str_drop_v, optu32vec_drop_v };
static const jz_btree_desc REMAP_STR_DESC = { sizeof(jz_str), sizeof(groove_OptStrVec), jz_cmp_str, groove__ivm__jz_str_drop_v, optstrvec_drop_v };

void groove__ivm__op_types__RecursiveEnumRemaps__Default__default(groove_RecursiveEnumRemaps *v) {
    jz_btree_init(&v->scalar, &REMAP_U8_DESC);
    jz_btree_init(&v->payload, &REMAP_U32_DESC);
    jz_btree_init(&v->payload_children, &REMAP_STR_DESC);
}
void groove__ivm__op_types__RecursiveEnumRemaps__Clone__clone(groove_RecursiveEnumRemaps *dst, const groove_RecursiveEnumRemaps *src) {
    jz_btree_clone(&dst->scalar, &src->scalar, groove__ivm__jz_str_clone_v, optu8vec_clone_v);
    jz_btree_clone(&dst->payload, &src->payload, groove__ivm__jz_str_clone_v, optu32vec_clone_v);
    jz_btree_clone(&dst->payload_children, &src->payload_children, groove__ivm__jz_str_clone_v, optstrvec_clone_v);
}
void groove__ivm__op_types__RecursiveEnumRemaps__drop(groove_RecursiveEnumRemaps *v) {
    jz_btree_drop(&v->scalar); jz_btree_drop(&v->payload); jz_btree_drop(&v->payload_children);
}
static bool optu8vec_eq_v(const void *a, const void *b) { return groove__ivm__op_types__optu8vec_eq(a, b); }
static bool optu32vec_eq_v(const void *a, const void *b) { return groove__ivm__op_types__optu32vec_eq(a, b); }
static bool optstrvec_eq_v(const void *pa, const void *pb) {
    const groove_OptStrVec *a = pa, *b = pb;
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!groove__ivm__op_types__opt_str_eq(a->ptr[i].some, &a->ptr[i].v, b->ptr[i].some, &b->ptr[i].v)) return false;
    return true;
}
bool groove__ivm__op_types__RecursiveEnumRemaps__PartialEq__eq(const groove_RecursiveEnumRemaps *a, const groove_RecursiveEnumRemaps *b) {
    return jz_btree_eq(&a->scalar, &b->scalar, optu8vec_eq_v) && jz_btree_eq(&a->payload, &b->payload, optu32vec_eq_v) &&
           jz_btree_eq(&a->payload_children, &b->payload_children, optstrvec_eq_v);
}
void groove__ivm__op_types__RecursiveEnumRemaps__Hash__hash(const groove_RecursiveEnumRemaps *v, groove_Hasher *h) {
    jz_str *k; void *val;
    groove_hash_usize(h, v->scalar.len);
    JZ_BTREE_FOR(&v->scalar, k, val) { groove_hash_str(h, k); groove__ivm__op_types__optu8vec_hash(val, h); }
    groove_hash_usize(h, v->payload.len);
    JZ_BTREE_FOR(&v->payload, k, val) { groove_hash_str(h, k); groove__ivm__op_types__optu32vec_hash(val, h); }
    groove_hash_usize(h, v->payload_children.len);
    JZ_BTREE_FOR(&v->payload_children, k, val) {
        const groove_OptStrVec *sv = val;
        groove_hash_str(h, k);
        groove_hash_usize(h, sv->len);
        for (u32 i = 0; i < sv->len; i++) groove__ivm__op_types__hash_opt_str(h, sv->ptr[i].some, &sv->ptr[i].v);
    }
}
void groove__ivm__op_types__RecursiveEnumRemaps__hash_helper(const groove_RecursiveEnumRemaps *v, groove_Hasher *h) {
    groove__ivm__op_types__RecursiveEnumRemaps__Hash__hash(v, h);
}
void groove__ivm__op_types__RecursiveEnumRemaps__Debug__fmt(jz_str *out, const groove_RecursiveEnumRemaps *v) {
    jz_str *k; void *val; bool first;
    groove_DebugStruct d = groove__fmt__debug_struct(out, "RecursiveEnumRemaps");
    groove__fmt__DebugStruct__field(&d, "scalar");
    jz_str_push_char(out, '{'); first = true;
    JZ_BTREE_FOR(&v->scalar, k, val) {
        if (!first) jz_str_push_cstr(out, ", "); first = false;
        groove__fmt__debug_str(out, k); jz_str_push_cstr(out, ": "); groove__ivm__op_types__optu8vec_debug(out, val);
    }
    jz_str_push_char(out, '}');
    groove__fmt__DebugStruct__field(&d, "payload");
    jz_str_push_char(out, '{'); first = true;
    JZ_BTREE_FOR(&v->payload, k, val) {
        if (!first) jz_str_push_cstr(out, ", "); first = false;
        groove__fmt__debug_str(out, k); jz_str_push_cstr(out, ": "); groove__ivm__op_types__optu32vec_debug(out, val);
    }
    jz_str_push_char(out, '}');
    groove__fmt__DebugStruct__field(&d, "payload_children");
    jz_str_push_char(out, '{'); first = true;
    JZ_BTREE_FOR(&v->payload_children, k, val) {
        const groove_OptStrVec *sv = val;
        if (!first) jz_str_push_cstr(out, ", "); first = false;
        groove__fmt__debug_str(out, k); jz_str_push_cstr(out, ": [");
        for (u32 i = 0; i < sv->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__dbg_opt_str(out, sv->ptr[i].some, &sv->ptr[i].v); }
        jz_str_push_char(out, ']');
    }
    jz_str_push_char(out, '}');
    groove__fmt__DebugStruct__finish(&d);
}

/* ======================================================================
 * ProjectExpr (graph.rs payload)
 * ====================================================================== */
void groove__ivm__graph__ProjectExpr__Clone__clone(groove_ProjectExpr *dst, const groove_ProjectExpr *src) {
    dst->tag = src->tag;
    switch (src->tag) {
    case GROOVE_PEXPR_TEMPLATE_ARGUMENT:
        dst->u.template_argument.slot = src->u.template_argument.slot;
        groove__records__values__ValueType__Clone__clone(&dst->u.template_argument.value_type, &src->u.template_argument.value_type);
        break;
    case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT:
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.field, &src->u.field);
        break;
    case GROOVE_PEXPR_RECORD_FIELD:
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.record_field.source, &src->u.record_field.source);
        groove__ivm__strvec_clone(&dst->u.record_field.path, &src->u.record_field.path);
        break;
    case GROOVE_PEXPR_LITERAL: groove__ivm__op_types__LiteralValue__Clone__clone(&dst->u.literal, &src->u.literal); break;
    case GROOVE_PEXPR_TYPED_LITERAL:
        groove__ivm__op_types__LiteralValue__Clone__clone(&dst->u.typed_literal.value, &src->u.typed_literal.value);
        groove__records__values__ValueType__Clone__clone(&dst->u.typed_literal.value_type, &src->u.typed_literal.value_type);
        break;
    case GROOVE_PEXPR_NULL: groove__records__values__ValueType__Clone__clone(&dst->u.null_type, &src->u.null_type); break;
    case GROOVE_PEXPR_ENUM_TAG_REMAP:
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.enum_tag_remap.source, &src->u.enum_tag_remap.source);
        groove__ivm__op_types__optu8vec_clone(&dst->u.enum_tag_remap.tags, &src->u.enum_tag_remap.tags);
        break;
    case GROOVE_PEXPR_ENUM_REMAP:
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.enum_remap.source, &src->u.enum_remap.source);
        groove__ivm__op_types__optu32vec_clone(&dst->u.enum_remap.tags, &src->u.enum_remap.tags);
        break;
    case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP:
        groove__ivm__graph__FieldRef__Clone__clone(&dst->u.recursive.source, &src->u.recursive.source);
        groove__records__values__ValueType__Clone__clone(&dst->u.recursive.target, &src->u.recursive.target);
        groove__ivm__op_types__RecursiveEnumRemaps__Clone__clone(&dst->u.recursive.remaps, &src->u.recursive.remaps);
        dst->u.recursive.omit_unrepresentable = src->u.recursive.omit_unrepresentable;
        break;
    }
}
void groove__ivm__graph__ProjectExpr__drop(groove_ProjectExpr *v) {
    switch (v->tag) {
    case GROOVE_PEXPR_TEMPLATE_ARGUMENT: groove__records__values__ValueType__drop(&v->u.template_argument.value_type); break;
    case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT: groove__ivm__graph__FieldRef__drop(&v->u.field); break;
    case GROOVE_PEXPR_RECORD_FIELD:
        groove__ivm__graph__FieldRef__drop(&v->u.record_field.source);
        groove__ivm__strvec_drop(&v->u.record_field.path);
        break;
    case GROOVE_PEXPR_LITERAL: groove__ivm__op_types__LiteralValue__drop(&v->u.literal); break;
    case GROOVE_PEXPR_TYPED_LITERAL:
        groove__ivm__op_types__LiteralValue__drop(&v->u.typed_literal.value);
        groove__records__values__ValueType__drop(&v->u.typed_literal.value_type);
        break;
    case GROOVE_PEXPR_NULL: groove__records__values__ValueType__drop(&v->u.null_type); break;
    case GROOVE_PEXPR_ENUM_TAG_REMAP:
        groove__ivm__graph__FieldRef__drop(&v->u.enum_tag_remap.source);
        jz_vec_drop(&v->u.enum_tag_remap.tags, sizeof(groove_OptU8), NULL);
        break;
    case GROOVE_PEXPR_ENUM_REMAP:
        groove__ivm__graph__FieldRef__drop(&v->u.enum_remap.source);
        jz_vec_drop(&v->u.enum_remap.tags, sizeof(groove_OptU32), NULL);
        break;
    case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP:
        groove__ivm__graph__FieldRef__drop(&v->u.recursive.source);
        groove__records__values__ValueType__drop(&v->u.recursive.target);
        groove__ivm__op_types__RecursiveEnumRemaps__drop(&v->u.recursive.remaps);
        break;
    }
}
bool groove__ivm__graph__ProjectExpr__PartialEq__eq(const groove_ProjectExpr *a, const groove_ProjectExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_PEXPR_TEMPLATE_ARGUMENT:
        return a->u.template_argument.slot == b->u.template_argument.slot &&
               groove__records__values__ValueType__PartialEq__eq(&a->u.template_argument.value_type, &b->u.template_argument.value_type);
    case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT:
        return groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.field, &b->u.field);
    case GROOVE_PEXPR_RECORD_FIELD:
        return groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.record_field.source, &b->u.record_field.source) &&
               groove__ivm__strvec_eq(&a->u.record_field.path, &b->u.record_field.path);
    case GROOVE_PEXPR_LITERAL: return groove__ivm__op_types__LiteralValue__PartialEq__eq(&a->u.literal, &b->u.literal);
    case GROOVE_PEXPR_TYPED_LITERAL:
        return groove__ivm__op_types__LiteralValue__PartialEq__eq(&a->u.typed_literal.value, &b->u.typed_literal.value) &&
               groove__records__values__ValueType__PartialEq__eq(&a->u.typed_literal.value_type, &b->u.typed_literal.value_type);
    case GROOVE_PEXPR_NULL: return groove__records__values__ValueType__PartialEq__eq(&a->u.null_type, &b->u.null_type);
    case GROOVE_PEXPR_ENUM_TAG_REMAP:
        return groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.enum_tag_remap.source, &b->u.enum_tag_remap.source) &&
               groove__ivm__op_types__optu8vec_eq(&a->u.enum_tag_remap.tags, &b->u.enum_tag_remap.tags);
    case GROOVE_PEXPR_ENUM_REMAP:
        return groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.enum_remap.source, &b->u.enum_remap.source) &&
               groove__ivm__op_types__optu32vec_eq(&a->u.enum_remap.tags, &b->u.enum_remap.tags);
    case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP:
        return groove__ivm__graph__FieldRef__PartialEq__eq(&a->u.recursive.source, &b->u.recursive.source) &&
               groove__records__values__ValueType__PartialEq__eq(&a->u.recursive.target, &b->u.recursive.target) &&
               groove__ivm__op_types__RecursiveEnumRemaps__PartialEq__eq(&a->u.recursive.remaps, &b->u.recursive.remaps) &&
               a->u.recursive.omit_unrepresentable == b->u.recursive.omit_unrepresentable;
    }
    return false;
}
void groove__ivm__graph__ProjectExpr__Hash__hash(const groove_ProjectExpr *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    switch (v->tag) {
    case GROOVE_PEXPR_TEMPLATE_ARGUMENT:
        groove_hash_u32(h, v->u.template_argument.slot);
        groove__records__values__ValueType__Hash__hash(&v->u.template_argument.value_type, h);
        break;
    case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT:
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.field, h);
        break;
    case GROOVE_PEXPR_RECORD_FIELD:
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.record_field.source, h);
        groove__ivm__strvec_hash(&v->u.record_field.path, h);
        break;
    case GROOVE_PEXPR_LITERAL: groove__ivm__op_types__LiteralValue__Hash__hash(&v->u.literal, h); break;
    case GROOVE_PEXPR_TYPED_LITERAL:
        groove__ivm__op_types__LiteralValue__Hash__hash(&v->u.typed_literal.value, h);
        groove__records__values__ValueType__Hash__hash(&v->u.typed_literal.value_type, h);
        break;
    case GROOVE_PEXPR_NULL: groove__records__values__ValueType__Hash__hash(&v->u.null_type, h); break;
    case GROOVE_PEXPR_ENUM_TAG_REMAP:
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.enum_tag_remap.source, h);
        groove__ivm__op_types__optu8vec_hash(&v->u.enum_tag_remap.tags, h);
        break;
    case GROOVE_PEXPR_ENUM_REMAP:
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.enum_remap.source, h);
        groove__ivm__op_types__optu32vec_hash(&v->u.enum_remap.tags, h);
        break;
    case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP:
        groove__ivm__graph__FieldRef__Hash__hash(&v->u.recursive.source, h);
        groove__records__values__ValueType__Hash__hash(&v->u.recursive.target, h);
        groove__ivm__op_types__RecursiveEnumRemaps__Hash__hash(&v->u.recursive.remaps, h);
        groove_hash_bool(h, v->u.recursive.omit_unrepresentable);
        break;
    }
}
void groove__ivm__graph__ProjectExpr__Debug__fmt(jz_str *out, const groove_ProjectExpr *v) {
    switch (v->tag) {
    case GROOVE_PEXPR_TEMPLATE_ARGUMENT: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "TemplateArgument");
        groove__fmt__DebugStruct__field(&d, "slot"); jz_str_appendf(out, "%u", (unsigned)v->u.template_argument.slot);
        groove__fmt__DebugStruct__field(&d, "value_type"); groove__records__values__ValueType__Debug__fmt(out, &v->u.template_argument.value_type);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_PEXPR_FIELD: case GROOVE_PEXPR_NULLABLE: case GROOVE_PEXPR_NULLABLE_FLAT:
        groove__fmt__debug_tuple_begin(out, v->tag == GROOVE_PEXPR_FIELD ? "Field" : v->tag == GROOVE_PEXPR_NULLABLE ? "Nullable" : "NullableFlat");
        groove__ivm__graph__FieldRef__Debug__fmt(out, &v->u.field);
        groove__fmt__debug_tuple_end(out);
        break;
    case GROOVE_PEXPR_RECORD_FIELD: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "RecordField");
        groove__fmt__DebugStruct__field(&d, "source"); groove__ivm__graph__FieldRef__Debug__fmt(out, &v->u.record_field.source);
        groove__fmt__DebugStruct__field(&d, "path"); groove__fmt__debug_str_list(out, v->u.record_field.path.ptr, v->u.record_field.path.len);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_PEXPR_LITERAL:
        groove__fmt__debug_tuple_begin(out, "Literal");
        groove__ivm__op_types__LiteralValue__Debug__fmt(out, &v->u.literal);
        groove__fmt__debug_tuple_end(out);
        break;
    case GROOVE_PEXPR_TYPED_LITERAL: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "TypedLiteral");
        groove__fmt__DebugStruct__field(&d, "value"); groove__ivm__op_types__LiteralValue__Debug__fmt(out, &v->u.typed_literal.value);
        groove__fmt__DebugStruct__field(&d, "value_type"); groove__records__values__ValueType__Debug__fmt(out, &v->u.typed_literal.value_type);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_PEXPR_NULL:
        groove__fmt__debug_tuple_begin(out, "Null");
        groove__records__values__ValueType__Debug__fmt(out, &v->u.null_type);
        groove__fmt__debug_tuple_end(out);
        break;
    case GROOVE_PEXPR_ENUM_TAG_REMAP: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "EnumTagRemap");
        groove__fmt__DebugStruct__field(&d, "source"); groove__ivm__graph__FieldRef__Debug__fmt(out, &v->u.enum_tag_remap.source);
        groove__fmt__DebugStruct__field(&d, "tags"); groove__ivm__op_types__optu8vec_debug(out, &v->u.enum_tag_remap.tags);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_PEXPR_ENUM_REMAP: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "EnumRemap");
        groove__fmt__DebugStruct__field(&d, "source"); groove__ivm__graph__FieldRef__Debug__fmt(out, &v->u.enum_remap.source);
        groove__fmt__DebugStruct__field(&d, "tags"); groove__ivm__op_types__optu32vec_debug(out, &v->u.enum_remap.tags);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_PEXPR_RECURSIVE_ENUM_REMAP: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "RecursiveEnumRemap");
        groove__fmt__DebugStruct__field(&d, "source"); groove__ivm__graph__FieldRef__Debug__fmt(out, &v->u.recursive.source);
        groove__fmt__DebugStruct__field(&d, "target"); groove__records__values__ValueType__Debug__fmt(out, &v->u.recursive.target);
        groove__fmt__DebugStruct__field(&d, "remaps"); groove__ivm__op_types__RecursiveEnumRemaps__Debug__fmt(out, &v->u.recursive.remaps);
        groove__fmt__DebugStruct__field(&d, "omit_unrepresentable"); groove__ivm__op_types__dbg_bool(out, v->u.recursive.omit_unrepresentable);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    }
}

/* ======================================================================
 * PredicateExpr
 * ====================================================================== */
static bool pred_is_lit(u8 tag) {
    return tag == GROOVE_PRED_EQ || tag == GROOVE_PRED_NEQ || tag == GROOVE_PRED_CONTAINS || tag == GROOVE_PRED_GT ||
           tag == GROOVE_PRED_GT_EQ || tag == GROOVE_PRED_LT || tag == GROOVE_PRED_LT_EQ;
}
static bool pred_is_fields(u8 tag) { return tag == GROOVE_PRED_EQ_FIELD || tag == GROOVE_PRED_CONTAINS_FIELD || tag == GROOVE_PRED_NEQ_FIELD; }
void groove__ivm__op_types__predvec_clone(groove_PredicateVec *dst, const groove_PredicateVec *src) {
    dst->ptr = NULL; dst->len = 0; dst->cap = 0;
    jz_vec_reserve(dst, src->len, sizeof(groove_PredicateExpr));
    for (u32 i = 0; i < src->len; i++) groove__ivm__op_types__PredicateExpr__Clone__clone(&dst->ptr[i], &src->ptr[i]);
    dst->len = src->len;
}
void groove__ivm__op_types__PredicateExpr__drop_v(void *v) { groove__ivm__op_types__PredicateExpr__drop((groove_PredicateExpr *)v); }

void groove__ivm__op_types__PredicateExpr__Clone__clone(groove_PredicateExpr *dst, const groove_PredicateExpr *src) {
    dst->tag = src->tag;
    if (src->tag == GROOVE_PRED_TEMPLATE_ARGUMENT) {
        dst->u.template_argument.slot = src->u.template_argument.slot;
        groove__ivm__strvec_clone(&dst->u.template_argument.fields, &src->u.template_argument.fields);
    } else if (pred_is_lit(src->tag)) {
        dst->u.lit.field = jz_str_clone(&src->u.lit.field);
        groove__ivm__op_types__LiteralValue__Clone__clone(&dst->u.lit.value, &src->u.lit.value);
    } else if (pred_is_fields(src->tag)) {
        dst->u.fields.field = jz_str_clone(&src->u.fields.field);
        dst->u.fields.other = jz_str_clone(&src->u.fields.other);
    } else if (src->tag == GROOVE_PRED_IS_NULL || src->tag == GROOVE_PRED_IS_NOT_NULL) {
        dst->u.null.field = jz_str_clone(&src->u.null.field);
    } else if (src->tag == GROOVE_PRED_ENUM_MATCH) {
        dst->u.enum_match.field = jz_str_clone(&src->u.enum_match.field);
        dst->u.enum_match.case_tag = src->u.enum_match.case_tag;
        dst->u.enum_match.payload = JZ_NEW(groove_PredicateExpr);
        groove__ivm__op_types__PredicateExpr__Clone__clone(dst->u.enum_match.payload, src->u.enum_match.payload);
    } else {
        groove__ivm__op_types__predvec_clone(&dst->u.children, &src->u.children);
    }
}
void groove__ivm__op_types__PredicateExpr__drop(groove_PredicateExpr *v) {
    if (v->tag == GROOVE_PRED_TEMPLATE_ARGUMENT) groove__ivm__strvec_drop(&v->u.template_argument.fields);
    else if (pred_is_lit(v->tag)) { jz_str_drop(&v->u.lit.field); groove__ivm__op_types__LiteralValue__drop(&v->u.lit.value); }
    else if (pred_is_fields(v->tag)) { jz_str_drop(&v->u.fields.field); jz_str_drop(&v->u.fields.other); }
    else if (v->tag == GROOVE_PRED_IS_NULL || v->tag == GROOVE_PRED_IS_NOT_NULL) jz_str_drop(&v->u.null.field);
    else if (v->tag == GROOVE_PRED_ENUM_MATCH) {
        jz_str_drop(&v->u.enum_match.field);
        groove__ivm__op_types__PredicateExpr__drop(v->u.enum_match.payload);
        jz_free(v->u.enum_match.payload);
    } else jz_vec_drop(&v->u.children, sizeof(groove_PredicateExpr), groove__ivm__op_types__PredicateExpr__drop_v);
}
bool groove__ivm__op_types__PredicateExpr__PartialEq__eq(const groove_PredicateExpr *a, const groove_PredicateExpr *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == GROOVE_PRED_TEMPLATE_ARGUMENT)
        return a->u.template_argument.slot == b->u.template_argument.slot &&
               groove__ivm__strvec_eq(&a->u.template_argument.fields, &b->u.template_argument.fields);
    if (pred_is_lit(a->tag))
        return jz_str_eq(&a->u.lit.field, &b->u.lit.field) && groove__ivm__op_types__LiteralValue__PartialEq__eq(&a->u.lit.value, &b->u.lit.value);
    if (pred_is_fields(a->tag)) return jz_str_eq(&a->u.fields.field, &b->u.fields.field) && jz_str_eq(&a->u.fields.other, &b->u.fields.other);
    if (a->tag == GROOVE_PRED_IS_NULL || a->tag == GROOVE_PRED_IS_NOT_NULL) return jz_str_eq(&a->u.null.field, &b->u.null.field);
    if (a->tag == GROOVE_PRED_ENUM_MATCH)
        return jz_str_eq(&a->u.enum_match.field, &b->u.enum_match.field) && a->u.enum_match.case_tag == b->u.enum_match.case_tag &&
               groove__ivm__op_types__PredicateExpr__PartialEq__eq(a->u.enum_match.payload, b->u.enum_match.payload);
    if (a->u.children.len != b->u.children.len) return false;
    for (u32 i = 0; i < a->u.children.len; i++)
        if (!groove__ivm__op_types__PredicateExpr__PartialEq__eq(&a->u.children.ptr[i], &b->u.children.ptr[i])) return false;
    return true;
}
static int strvec_cmp(const groove_StrVec *a, const groove_StrVec *b) {
    u32 n = a->len < b->len ? a->len : b->len;
    for (u32 i = 0; i < n; i++) { int c = jz_str_cmp(&a->ptr[i], &b->ptr[i]); if (c) return c; }
    return CMP3(a->len, b->len);
}
int groove__ivm__op_types__PredicateExpr__PartialOrd__partial_cmp(const groove_PredicateExpr *a, const groove_PredicateExpr *b) {
    int c;
    if (a->tag != b->tag) return CMP3(a->tag, b->tag);
    if (a->tag == GROOVE_PRED_TEMPLATE_ARGUMENT) {
        c = CMP3(a->u.template_argument.slot, b->u.template_argument.slot);
        return c ? c : strvec_cmp(&a->u.template_argument.fields, &b->u.template_argument.fields);
    }
    if (pred_is_lit(a->tag)) {
        c = jz_str_cmp(&a->u.lit.field, &b->u.lit.field);
        return c ? c : groove__ivm__op_types__LiteralValue__PartialOrd__partial_cmp(&a->u.lit.value, &b->u.lit.value);
    }
    if (pred_is_fields(a->tag)) {
        c = jz_str_cmp(&a->u.fields.field, &b->u.fields.field);
        return c ? c : jz_str_cmp(&a->u.fields.other, &b->u.fields.other);
    }
    if (a->tag == GROOVE_PRED_IS_NULL || a->tag == GROOVE_PRED_IS_NOT_NULL) return jz_str_cmp(&a->u.null.field, &b->u.null.field);
    if (a->tag == GROOVE_PRED_ENUM_MATCH) {
        c = jz_str_cmp(&a->u.enum_match.field, &b->u.enum_match.field);
        if (c) return c;
        c = CMP3(a->u.enum_match.case_tag, b->u.enum_match.case_tag);
        return c ? c : groove__ivm__op_types__PredicateExpr__PartialOrd__partial_cmp(a->u.enum_match.payload, b->u.enum_match.payload);
    }
    {
        u32 n = a->u.children.len < b->u.children.len ? a->u.children.len : b->u.children.len;
        for (u32 i = 0; i < n; i++) {
            c = groove__ivm__op_types__PredicateExpr__PartialOrd__partial_cmp(&a->u.children.ptr[i], &b->u.children.ptr[i]);
            if (c) return c;
        }
        return CMP3(a->u.children.len, b->u.children.len);
    }
}
void groove__ivm__op_types__PredicateExpr__Hash__hash(const groove_PredicateExpr *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    if (v->tag == GROOVE_PRED_TEMPLATE_ARGUMENT) {
        groove_hash_u32(h, v->u.template_argument.slot);
        groove__ivm__strvec_hash(&v->u.template_argument.fields, h);
    } else if (pred_is_lit(v->tag)) {
        groove_hash_str(h, &v->u.lit.field);
        groove__ivm__op_types__LiteralValue__Hash__hash(&v->u.lit.value, h);
    } else if (pred_is_fields(v->tag)) {
        groove_hash_str(h, &v->u.fields.field);
        groove_hash_str(h, &v->u.fields.other);
    } else if (v->tag == GROOVE_PRED_IS_NULL || v->tag == GROOVE_PRED_IS_NOT_NULL) {
        groove_hash_str(h, &v->u.null.field);
    } else if (v->tag == GROOVE_PRED_ENUM_MATCH) {
        groove_hash_str(h, &v->u.enum_match.field);
        groove_hash_u32(h, v->u.enum_match.case_tag);
        groove__ivm__op_types__PredicateExpr__Hash__hash(v->u.enum_match.payload, h);
    } else {
        groove_hash_usize(h, v->u.children.len);
        for (u32 i = 0; i < v->u.children.len; i++) groove__ivm__op_types__PredicateExpr__Hash__hash(&v->u.children.ptr[i], h);
    }
}
void groove__ivm__op_types__PredicateExpr__Debug__fmt(jz_str *out, const groove_PredicateExpr *v) {
    static const char *const names[] = {"TemplateArgument", "Eq", "Neq", "Contains", "EqField", "ContainsField", "NeqField", "Gt",
                                        "GtEq", "Lt", "LtEq", "IsNull", "IsNotNull", "EnumMatch", "And", "Or"};
    if (v->tag == GROOVE_PRED_AND || v->tag == GROOVE_PRED_OR) {
        groove__fmt__debug_tuple_begin(out, names[v->tag]);
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < v->u.children.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            groove__ivm__op_types__PredicateExpr__Debug__fmt(out, &v->u.children.ptr[i]);
        }
        jz_str_push_char(out, ']');
        groove__fmt__debug_tuple_end(out);
        return;
    }
    groove_DebugStruct d = groove__fmt__debug_struct(out, names[v->tag]);
    if (v->tag == GROOVE_PRED_TEMPLATE_ARGUMENT) {
        groove__fmt__DebugStruct__field(&d, "slot"); jz_str_appendf(out, "%u", (unsigned)v->u.template_argument.slot);
        groove__fmt__DebugStruct__field(&d, "fields");
        groove__fmt__debug_str_list(out, v->u.template_argument.fields.ptr, v->u.template_argument.fields.len);
    } else if (pred_is_lit(v->tag)) {
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->u.lit.field);
        groove__fmt__DebugStruct__field(&d, "value"); groove__ivm__op_types__LiteralValue__Debug__fmt(out, &v->u.lit.value);
    } else if (pred_is_fields(v->tag)) {
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->u.fields.field);
        groove__fmt__DebugStruct__field(&d, v->tag == GROOVE_PRED_CONTAINS_FIELD ? "needle_field" : "value_field");
        groove__fmt__debug_str(out, &v->u.fields.other);
    } else if (v->tag == GROOVE_PRED_IS_NULL || v->tag == GROOVE_PRED_IS_NOT_NULL) {
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->u.null.field);
    } else {
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->u.enum_match.field);
        groove__fmt__DebugStruct__field(&d, "case_tag"); jz_str_appendf(out, "%u", (unsigned)v->u.enum_match.case_tag);
        groove__fmt__DebugStruct__field(&d, "payload"); groove__ivm__op_types__PredicateExpr__Debug__fmt(out, v->u.enum_match.payload);
    }
    groove__fmt__DebugStruct__finish(&d);
}

bool groove__ivm__op_types__PredicateExpr__has_template_arguments(const groove_PredicateExpr *self) {
    switch (self->tag) {
    case GROOVE_PRED_TEMPLATE_ARGUMENT: return true;
    case GROOVE_PRED_AND:
    case GROOVE_PRED_OR:
        for (u32 i = 0; i < self->u.children.len; i++)
            if (groove__ivm__op_types__PredicateExpr__has_template_arguments(&self->u.children.ptr[i])) return true;
        return false;
    case GROOVE_PRED_ENUM_MATCH: return groove__ivm__op_types__PredicateExpr__has_template_arguments(self->u.enum_match.payload);
    default: return false;
    }
}

bool groove__ivm__op_types__PredicateExpr__accepts_template_argument(const groove_PredicateExpr *self, const groove_PredicateExpr *argument) {
    if (self->tag != GROOVE_PRED_TEMPLATE_ARGUMENT) return false;
    if (groove__ivm__op_types__PredicateExpr__has_template_arguments(argument)) return false;
    const groove_StrVec *fields = &self->u.template_argument.fields;
    jz_btree referenced;
    jz_btree_init(&referenced, &groove__ivm__STRSET_DESC);
    groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(argument, &referenced);
    bool all = true;
    jz_str *field; void *unused;
    JZ_BTREE_FOR(&referenced, field, unused) {
        bool found = false;
        for (u32 i = 0; i < fields->len; i++) if (jz_str_eq(&fields->ptr[i], field)) { found = true; break; }
        if (!found) { all = false; break; }
    }
    jz_btree_drop(&referenced);
    return all;
}

static int pred_cmp_v(const void *a, const void *b, void *ctx) {
    return groove__ivm__op_types__PredicateExpr__PartialOrd__partial_cmp(a, b);
}
/* fn canonicalize(self) -> Self: flattens nested And/Or and sorts children. */
void groove__ivm__op_types__PredicateExpr__canonicalize(groove_PredicateExpr *out, groove_PredicateExpr self) {
    if (self.tag != GROOVE_PRED_AND && self.tag != GROOVE_PRED_OR) { *out = self; return; }
    u8 kind = self.tag;
    groove_PredicateVec flat = {0};
    for (u32 i = 0; i < self.u.children.len; i++) {
        groove_PredicateExpr child;
        groove__ivm__op_types__PredicateExpr__canonicalize(&child, self.u.children.ptr[i]);
        if (child.tag == kind) {
            jz_vec_extend(&flat, child.u.children.ptr, child.u.children.len, sizeof(groove_PredicateExpr));
            jz_free(child.u.children.ptr);
        } else {
            jz_vec_push(&flat, &child, sizeof child);
        }
    }
    jz_free(self.u.children.ptr);
    jz_sort(flat.ptr, flat.len, sizeof(groove_PredicateExpr), pred_cmp_v, NULL);
    out->tag = kind;
    out->u.children.ptr = flat.ptr; out->u.children.len = flat.len; out->u.children.cap = flat.cap;
}

static groove_PredicateExpr pred_lit(u8 tag, jz_str field, groove_LiteralValue value) {
    groove_PredicateExpr p;
    p.tag = tag;
    p.u.lit.field = field;
    p.u.lit.value = value;
    return p;
}
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__eq(jz_str field, groove_Value value) {
    groove_LiteralValue lit;
    groove__ivm__op_types__LiteralValue__From_Value__from(&lit, &value);
    return pred_lit(GROOVE_PRED_EQ, field, lit);
}
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__gt(jz_str field, groove_Value value) {
    groove_LiteralValue lit;
    groove__ivm__op_types__LiteralValue__From_Value__from(&lit, &value);
    return pred_lit(GROOVE_PRED_GT, field, lit);
}
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__is_null(jz_str field) {
    groove_PredicateExpr p; p.tag = GROOVE_PRED_IS_NULL; p.u.null.field = field; return p;
}
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__is_not_null(jz_str field) {
    groove_PredicateExpr p; p.tag = GROOVE_PRED_IS_NOT_NULL; p.u.null.field = field; return p;
}
groove_PredicateExpr groove__ivm__op_types__PredicateExpr__from_field_literal(u8 kind, jz_str field, groove_LiteralValue value) {
    switch (kind) {
    case GROOVE_PK_EQ: return pred_lit(GROOVE_PRED_EQ, field, value);
    case GROOVE_PK_NEQ: return pred_lit(GROOVE_PRED_NEQ, field, value);
    case GROOVE_PK_GT: return pred_lit(GROOVE_PRED_GT, field, value);
    case GROOVE_PK_GT_EQ: return pred_lit(GROOVE_PRED_GT_EQ, field, value);
    case GROOVE_PK_LT: return pred_lit(GROOVE_PRED_LT, field, value);
    default: return pred_lit(GROOVE_PRED_LT_EQ, field, value);
    }
}
u8 groove__ivm__op_types__PredicateKind__reversed(u8 kind) {
    switch (kind) {
    case GROOVE_PK_EQ: return GROOVE_PK_EQ;
    case GROOVE_PK_NEQ: return GROOVE_PK_NEQ;
    case GROOVE_PK_GT: return GROOVE_PK_LT;
    case GROOVE_PK_GT_EQ: return GROOVE_PK_LT_EQ;
    case GROOVE_PK_LT: return GROOVE_PK_GT;
    default: return GROOVE_PK_GT_EQ;
    }
}

/* ======================================================================
 * PlanExpr
 * ====================================================================== */
static bool plan_is_name(u8 tag) { return tag == GROOVE_PLAN_FIELD || tag == GROOVE_PLAN_NULLABLE || tag == GROOVE_PLAN_NULLABLE_FLAT; }
void groove__ivm__op_types__PlanExpr__Clone__clone(groove_PlanExpr *dst, const groove_PlanExpr *src) {
    dst->tag = src->tag;
    switch (src->tag) {
    case GROOVE_PLAN_FIELD: case GROOVE_PLAN_NULLABLE: case GROOVE_PLAN_NULLABLE_FLAT: dst->u.field = jz_str_clone(&src->u.field); break;
    case GROOVE_PLAN_LITERAL: groove__ivm__op_types__LiteralValue__Clone__clone(&dst->u.literal, &src->u.literal); break;
    case GROOVE_PLAN_NULL: groove__records__values__ValueType__Clone__clone(&dst->u.null_type, &src->u.null_type); break;
    case GROOVE_PLAN_ENUM_TAG_REMAP:
        dst->u.enum_tag_remap.field = jz_str_clone(&src->u.enum_tag_remap.field);
        groove__ivm__op_types__optu8vec_clone(&dst->u.enum_tag_remap.tags, &src->u.enum_tag_remap.tags);
        break;
    case GROOVE_PLAN_ENUM_REMAP:
        dst->u.enum_remap.field = jz_str_clone(&src->u.enum_remap.field);
        groove__ivm__op_types__optu32vec_clone(&dst->u.enum_remap.tags, &src->u.enum_remap.tags);
        break;
    case GROOVE_PLAN_RECURSIVE_ENUM_REMAP:
        dst->u.recursive.field = jz_str_clone(&src->u.recursive.field);
        groove__ivm__op_types__RecursiveEnumRemaps__Clone__clone(&dst->u.recursive.remaps, &src->u.recursive.remaps);
        dst->u.recursive.omit_unrepresentable = src->u.recursive.omit_unrepresentable;
        break;
    }
}
void groove__ivm__op_types__PlanExpr__drop(groove_PlanExpr *v) {
    switch (v->tag) {
    case GROOVE_PLAN_FIELD: case GROOVE_PLAN_NULLABLE: case GROOVE_PLAN_NULLABLE_FLAT: jz_str_drop(&v->u.field); break;
    case GROOVE_PLAN_LITERAL: groove__ivm__op_types__LiteralValue__drop(&v->u.literal); break;
    case GROOVE_PLAN_NULL: groove__records__values__ValueType__drop(&v->u.null_type); break;
    case GROOVE_PLAN_ENUM_TAG_REMAP: jz_str_drop(&v->u.enum_tag_remap.field); jz_vec_drop(&v->u.enum_tag_remap.tags, sizeof(groove_OptU8), NULL); break;
    case GROOVE_PLAN_ENUM_REMAP: jz_str_drop(&v->u.enum_remap.field); jz_vec_drop(&v->u.enum_remap.tags, sizeof(groove_OptU32), NULL); break;
    case GROOVE_PLAN_RECURSIVE_ENUM_REMAP: jz_str_drop(&v->u.recursive.field); groove__ivm__op_types__RecursiveEnumRemaps__drop(&v->u.recursive.remaps); break;
    }
}
bool groove__ivm__op_types__PlanExpr__PartialEq__eq(const groove_PlanExpr *a, const groove_PlanExpr *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_PLAN_FIELD: case GROOVE_PLAN_NULLABLE: case GROOVE_PLAN_NULLABLE_FLAT: return jz_str_eq(&a->u.field, &b->u.field);
    case GROOVE_PLAN_LITERAL: return groove__ivm__op_types__LiteralValue__PartialEq__eq(&a->u.literal, &b->u.literal);
    case GROOVE_PLAN_NULL: return groove__records__values__ValueType__PartialEq__eq(&a->u.null_type, &b->u.null_type);
    case GROOVE_PLAN_ENUM_TAG_REMAP:
        return jz_str_eq(&a->u.enum_tag_remap.field, &b->u.enum_tag_remap.field) &&
               groove__ivm__op_types__optu8vec_eq(&a->u.enum_tag_remap.tags, &b->u.enum_tag_remap.tags);
    case GROOVE_PLAN_ENUM_REMAP:
        return jz_str_eq(&a->u.enum_remap.field, &b->u.enum_remap.field) &&
               groove__ivm__op_types__optu32vec_eq(&a->u.enum_remap.tags, &b->u.enum_remap.tags);
    case GROOVE_PLAN_RECURSIVE_ENUM_REMAP:
        return jz_str_eq(&a->u.recursive.field, &b->u.recursive.field) &&
               groove__ivm__op_types__RecursiveEnumRemaps__PartialEq__eq(&a->u.recursive.remaps, &b->u.recursive.remaps) &&
               a->u.recursive.omit_unrepresentable == b->u.recursive.omit_unrepresentable;
    }
    return false;
}
void groove__ivm__op_types__PlanExpr__Hash__hash(const groove_PlanExpr *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    switch (v->tag) {
    case GROOVE_PLAN_FIELD: case GROOVE_PLAN_NULLABLE: case GROOVE_PLAN_NULLABLE_FLAT: groove_hash_str(h, &v->u.field); break;
    case GROOVE_PLAN_LITERAL: groove__ivm__op_types__LiteralValue__Hash__hash(&v->u.literal, h); break;
    case GROOVE_PLAN_NULL: groove__records__values__ValueType__Hash__hash(&v->u.null_type, h); break;
    case GROOVE_PLAN_ENUM_TAG_REMAP: groove_hash_str(h, &v->u.enum_tag_remap.field); groove__ivm__op_types__optu8vec_hash(&v->u.enum_tag_remap.tags, h); break;
    case GROOVE_PLAN_ENUM_REMAP: groove_hash_str(h, &v->u.enum_remap.field); groove__ivm__op_types__optu32vec_hash(&v->u.enum_remap.tags, h); break;
    case GROOVE_PLAN_RECURSIVE_ENUM_REMAP:
        groove_hash_str(h, &v->u.recursive.field);
        groove__ivm__op_types__RecursiveEnumRemaps__Hash__hash(&v->u.recursive.remaps, h);
        groove_hash_bool(h, v->u.recursive.omit_unrepresentable);
        break;
    }
}
void groove__ivm__op_types__PlanExpr__Debug__fmt(jz_str *out, const groove_PlanExpr *v) {
    switch (v->tag) {
    case GROOVE_PLAN_FIELD: case GROOVE_PLAN_NULLABLE: case GROOVE_PLAN_NULLABLE_FLAT:
        groove__fmt__debug_tuple_begin(out, v->tag == GROOVE_PLAN_FIELD ? "Field" : v->tag == GROOVE_PLAN_NULLABLE ? "Nullable" : "NullableFlat");
        groove__fmt__debug_str(out, &v->u.field);
        groove__fmt__debug_tuple_end(out);
        break;
    case GROOVE_PLAN_LITERAL:
        groove__fmt__debug_tuple_begin(out, "Literal");
        groove__ivm__op_types__LiteralValue__Debug__fmt(out, &v->u.literal);
        groove__fmt__debug_tuple_end(out);
        break;
    case GROOVE_PLAN_NULL:
        groove__fmt__debug_tuple_begin(out, "Null");
        groove__records__values__ValueType__Debug__fmt(out, &v->u.null_type);
        groove__fmt__debug_tuple_end(out);
        break;
    case GROOVE_PLAN_ENUM_TAG_REMAP: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "EnumTagRemap");
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->u.enum_tag_remap.field);
        groove__fmt__DebugStruct__field(&d, "tags"); groove__ivm__op_types__optu8vec_debug(out, &v->u.enum_tag_remap.tags);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_PLAN_ENUM_REMAP: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "EnumRemap");
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->u.enum_remap.field);
        groove__fmt__DebugStruct__field(&d, "tags"); groove__ivm__op_types__optu32vec_debug(out, &v->u.enum_remap.tags);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    case GROOVE_PLAN_RECURSIVE_ENUM_REMAP: {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "RecursiveEnumRemap");
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->u.recursive.field);
        groove__fmt__DebugStruct__field(&d, "remaps"); groove__ivm__op_types__RecursiveEnumRemaps__Debug__fmt(out, &v->u.recursive.remaps);
        groove__fmt__DebugStruct__field(&d, "omit_unrepresentable"); groove__ivm__op_types__dbg_bool(out, v->u.recursive.omit_unrepresentable);
        groove__fmt__DebugStruct__finish(&d);
        break;
    }
    }
}
groove_PlanExpr groove__ivm__op_types__PlanExpr__field(jz_str name) { groove_PlanExpr p; p.tag = GROOVE_PLAN_FIELD; p.u.field = name; return p; }
groove_PlanExpr groove__ivm__op_types__PlanExpr__literal(groove_LiteralValue v) { groove_PlanExpr p; p.tag = GROOVE_PLAN_LITERAL; p.u.literal = v; return p; }
groove_PlanExpr groove__ivm__op_types__PlanExpr__null(groove_ValueType t) { groove_PlanExpr p; p.tag = GROOVE_PLAN_NULL; p.u.null_type = t; return p; }
groove_PlanExpr groove__ivm__op_types__PlanExpr__nullable(jz_str name) { groove_PlanExpr p; p.tag = GROOVE_PLAN_NULLABLE; p.u.field = name; return p; }
groove_PlanExpr groove__ivm__op_types__PlanExpr__nullable_flat(jz_str name) { groove_PlanExpr p; p.tag = GROOVE_PLAN_NULLABLE_FLAT; p.u.field = name; return p; }

static void planvec_clone(groove_PlanExprVec *dst, const groove_PlanExprVec *src) {
    dst->ptr = NULL; dst->len = 0; dst->cap = 0;
    jz_vec_reserve(dst, src->len, sizeof(groove_PlanExpr));
    for (u32 i = 0; i < src->len; i++) groove__ivm__op_types__PlanExpr__Clone__clone(&dst->ptr[i], &src->ptr[i]);
    dst->len = src->len;
}
static void plan_drop_v(void *p) { groove__ivm__op_types__PlanExpr__drop(p); }
static void planvec_drop(groove_PlanExprVec *v) { jz_vec_drop(v, sizeof(groove_PlanExpr), plan_drop_v); }
static bool planvec_eq(const groove_PlanExprVec *a, const groove_PlanExprVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__op_types__PlanExpr__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void planvec_hash(const groove_PlanExprVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove__ivm__op_types__PlanExpr__Hash__hash(&v->ptr[i], h);
}
static void planvec_debug(jz_str *out, const groove_PlanExprVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__PlanExpr__Debug__fmt(out, &v->ptr[i]); }
    jz_str_push_char(out, ']');
}

/* ======================================================================
 * Sources
 * ====================================================================== */
void groove__ivm__op_types__TableSourceOp__Clone__clone(groove_TableSourceOp *dst, const groove_TableSourceOp *src) {
    dst->table = jz_str_clone(&src->table);
    dst->has_scan = src->has_scan; groove__ivm__op_types__scan_opt_clone(src->has_scan, &src->scan, &dst->scan);
    dst->has_variant_projection = src->has_variant_projection;
    vpt_opt_clone(src->has_variant_projection, &src->variant_projection, &dst->variant_projection);
}
void groove__ivm__op_types__TableSourceOp__drop(groove_TableSourceOp *v) {
    jz_str_drop(&v->table);
    if (v->has_scan) groove__ivm__op_types__StaticScanSpec__drop(&v->scan);
    if (v->has_variant_projection) jz_str_drop(&v->variant_projection.name);
}
bool groove__ivm__op_types__TableSourceOp__PartialEq__eq(const groove_TableSourceOp *a, const groove_TableSourceOp *b) {
    return jz_str_eq(&a->table, &b->table) && groove__ivm__op_types__scan_opt_eq(a->has_scan, &a->scan, b->has_scan, &b->scan) &&
           vpt_opt_eq(a->has_variant_projection, &a->variant_projection, b->has_variant_projection, &b->variant_projection);
}
void groove__ivm__op_types__TableSourceOp__Hash__hash(const groove_TableSourceOp *v, groove_Hasher *h) {
    groove_hash_str(h, &v->table);
    groove__ivm__op_types__scan_opt_hash(h, v->has_scan, &v->scan);
    vpt_opt_hash(h, v->has_variant_projection, &v->variant_projection);
}
void groove__ivm__op_types__TableSourceOp__Debug__fmt(jz_str *out, const groove_TableSourceOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "TableSourceOp");
    groove__fmt__DebugStruct__field(&d, "table"); groove__fmt__debug_str(out, &v->table);
    groove__fmt__DebugStruct__field(&d, "scan"); groove__ivm__op_types__scan_opt_debug(out, v->has_scan, &v->scan);
    groove__fmt__DebugStruct__field(&d, "variant_projection"); vpt_opt_debug(out, v->has_variant_projection, &v->variant_projection);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__LookupOp__Clone__clone(groove_LookupOp *dst, const groove_LookupOp *src) {
    groove__ivm__op_types__TableSourceOp__Clone__clone(&dst->source, &src->source);
    dst->key_descriptor = src->key_descriptor;
    groove__ivm__usizevec_clone(&dst->key_fields, &src->key_fields);
}
void groove__ivm__op_types__LookupOp__drop(groove_LookupOp *v) {
    groove__ivm__op_types__TableSourceOp__drop(&v->source);
    jz_vec_drop(&v->key_fields, sizeof(u32), NULL);
}
bool groove__ivm__op_types__LookupOp__PartialEq__eq(const groove_LookupOp *a, const groove_LookupOp *b) {
    return groove__ivm__op_types__TableSourceOp__PartialEq__eq(&a->source, &b->source) && a->key_descriptor == b->key_descriptor &&
           groove__ivm__usizevec_eq(&a->key_fields, &b->key_fields);
}
void groove__ivm__op_types__LookupOp__Hash__hash(const groove_LookupOp *v, groove_Hasher *h) {
    groove__ivm__op_types__TableSourceOp__Hash__hash(&v->source, h);
    groove_hash_usize(h, (u32)(uintptr_t)v->key_descriptor);
    groove__ivm__usizevec_hash(&v->key_fields, h);
}
void groove__ivm__op_types__LookupOp__Debug__fmt(jz_str *out, const groove_LookupOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "LookupOp");
    groove__fmt__DebugStruct__field(&d, "source"); groove__ivm__op_types__TableSourceOp__Debug__fmt(out, &v->source);
    groove__fmt__DebugStruct__field(&d, "key_descriptor"); dbg_descriptor(out, v->key_descriptor);
    groove__fmt__DebugStruct__field(&d, "key_fields"); groove__fmt__debug_usize_list(out, v->key_fields.ptr, v->key_fields.len);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__intersection_clone_v(void *dst, const void *src) {
    const groove_ScanIntersection *s = src; groove_ScanIntersection *d = dst;
    d->index = jz_str_clone(&s->index);
    groove__ivm__op_types__StaticScanSpec__Clone__clone(&d->scan, &s->scan);
}
void groove__ivm__op_types__intersection_drop_v(void *p) {
    groove_ScanIntersection *s = p;
    jz_str_drop(&s->index);
    groove__ivm__op_types__StaticScanSpec__drop(&s->scan);
}
void groove__ivm__op_types__IndexSourceOp__Clone__clone(groove_IndexSourceOp *dst, const groove_IndexSourceOp *src) {
    dst->table = jz_str_clone(&src->table);
    dst->index = jz_str_clone(&src->index);
    dst->durable_ids = JZ_NEW(groove_IndexSourceDurableIds);
    dst->durable_ids->index = src->durable_ids->index;
    jz_vec_clone(&dst->durable_ids->intersections, &src->durable_ids->intersections, sizeof(u32), NULL);
    dst->durable_ids->has_candidate = src->durable_ids->has_candidate;
    dst->durable_ids->candidate = src->durable_ids->candidate;
    jz_vec_clone(&dst->intersections, &src->intersections, sizeof(groove_ScanIntersection), groove__ivm__op_types__intersection_clone_v);
    dst->has_candidate_filter = src->has_candidate_filter;
    if (src->has_candidate_filter) groove__ivm__graph__IndexCandidateFilter__Clone__clone(&dst->candidate_filter, &src->candidate_filter);
    dst->input_descriptor = src->input_descriptor;
    dst->has_variant_projection = src->has_variant_projection;
    vpt_opt_clone(src->has_variant_projection, &src->variant_projection, &dst->variant_projection);
    dst->has_row_projection = src->has_row_projection;
    vpt_opt_clone(src->has_row_projection, &src->row_projection, &dst->row_projection);
    groove__ivm__usizevec_clone(&dst->key_fields, &src->key_fields);
    groove__ivm__usizevec_clone(&dst->value_fields, &src->value_fields);
    dst->unique = src->unique; dst->append_value_to_key = src->append_value_to_key; dst->store_value = src->store_value;
    dst->has_scan = src->has_scan; groove__ivm__op_types__scan_opt_clone(src->has_scan, &src->scan, &dst->scan);
}
void groove__ivm__op_types__IndexSourceOp__drop(groove_IndexSourceOp *v) {
    jz_str_drop(&v->table); jz_str_drop(&v->index);
    jz_vec_drop(&v->durable_ids->intersections, sizeof(u32), NULL);
    jz_free(v->durable_ids);
    jz_vec_drop(&v->intersections, sizeof(groove_ScanIntersection), groove__ivm__op_types__intersection_drop_v);
    if (v->has_candidate_filter) groove__ivm__graph__IndexCandidateFilter__drop(&v->candidate_filter);
    if (v->has_variant_projection) jz_str_drop(&v->variant_projection.name);
    if (v->has_row_projection) jz_str_drop(&v->row_projection.name);
    jz_vec_drop(&v->key_fields, sizeof(u32), NULL);
    jz_vec_drop(&v->value_fields, sizeof(u32), NULL);
    if (v->has_scan) groove__ivm__op_types__StaticScanSpec__drop(&v->scan);
}
static bool durable_ids_eq(const groove_IndexSourceDurableIds *a, const groove_IndexSourceDurableIds *b) {
    if (a->index != b->index || a->intersections.len != b->intersections.len) return false;
    if (a->intersections.len && memcmp(a->intersections.ptr, b->intersections.ptr, a->intersections.len * 4)) return false;
    return a->has_candidate == b->has_candidate && (!a->has_candidate || a->candidate == b->candidate);
}
bool groove__ivm__op_types__IndexSourceOp__PartialEq__eq(const groove_IndexSourceOp *a, const groove_IndexSourceOp *b) {
    if (!jz_str_eq(&a->table, &b->table) || !jz_str_eq(&a->index, &b->index)) return false;
    if (!durable_ids_eq(a->durable_ids, b->durable_ids)) return false;
    if (a->intersections.len != b->intersections.len) return false;
    for (u32 i = 0; i < a->intersections.len; i++) {
        if (!jz_str_eq(&a->intersections.ptr[i].index, &b->intersections.ptr[i].index)) return false;
        if (!groove__ivm__op_types__StaticScanSpec__PartialEq__eq(&a->intersections.ptr[i].scan, &b->intersections.ptr[i].scan)) return false;
    }
    if (a->has_candidate_filter != b->has_candidate_filter) return false;
    if (a->has_candidate_filter && !groove__ivm__graph__IndexCandidateFilter__PartialEq__eq(&a->candidate_filter, &b->candidate_filter)) return false;
    return a->input_descriptor == b->input_descriptor &&
           vpt_opt_eq(a->has_variant_projection, &a->variant_projection, b->has_variant_projection, &b->variant_projection) &&
           vpt_opt_eq(a->has_row_projection, &a->row_projection, b->has_row_projection, &b->row_projection) &&
           groove__ivm__usizevec_eq(&a->key_fields, &b->key_fields) && groove__ivm__usizevec_eq(&a->value_fields, &b->value_fields) &&
           a->unique == b->unique && a->append_value_to_key == b->append_value_to_key && a->store_value == b->store_value &&
           groove__ivm__op_types__scan_opt_eq(a->has_scan, &a->scan, b->has_scan, &b->scan);
}
void groove__ivm__op_types__IndexSourceOp__Hash__hash(const groove_IndexSourceOp *v, groove_Hasher *h) {
    groove_hash_str(h, &v->table); groove_hash_str(h, &v->index);
    groove_hash_u32(h, v->durable_ids->index);
    groove_hash_usize(h, v->durable_ids->intersections.len);
    groove_hash_bytes_raw(h, v->durable_ids->intersections.ptr, v->durable_ids->intersections.len * 4);
    groove_hash_discr(h, v->durable_ids->has_candidate);
    if (v->durable_ids->has_candidate) groove_hash_u32(h, v->durable_ids->candidate);
    groove_hash_usize(h, v->intersections.len);
    for (u32 i = 0; i < v->intersections.len; i++) {
        groove_hash_str(h, &v->intersections.ptr[i].index);
        groove__ivm__op_types__StaticScanSpec__Hash__hash(&v->intersections.ptr[i].scan, h);
    }
    groove_hash_discr(h, v->has_candidate_filter);
    if (v->has_candidate_filter) groove__ivm__graph__IndexCandidateFilter__Hash__hash(&v->candidate_filter, h);
    groove_hash_usize(h, (u32)(uintptr_t)v->input_descriptor);
    vpt_opt_hash(h, v->has_variant_projection, &v->variant_projection);
    vpt_opt_hash(h, v->has_row_projection, &v->row_projection);
    groove__ivm__usizevec_hash(&v->key_fields, h);
    groove__ivm__usizevec_hash(&v->value_fields, h);
    groove_hash_bool(h, v->unique); groove_hash_bool(h, v->append_value_to_key); groove_hash_bool(h, v->store_value);
    groove__ivm__op_types__scan_opt_hash(h, v->has_scan, &v->scan);
}
void groove__ivm__op_types__IndexSourceOp__Debug__fmt(jz_str *out, const groove_IndexSourceOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "IndexSourceOp");
    groove__fmt__DebugStruct__field(&d, "table"); groove__fmt__debug_str(out, &v->table);
    groove__fmt__DebugStruct__field(&d, "index"); groove__fmt__debug_str(out, &v->index);
    groove__fmt__DebugStruct__field(&d, "durable_ids");
    {
        groove_DebugStruct d2 = groove__fmt__debug_struct(out, "IndexSourceDurableIds");
        groove__fmt__DebugStruct__field(&d2, "index"); jz_str_appendf(out, "%u", (unsigned)v->durable_ids->index);
        groove__fmt__DebugStruct__field(&d2, "intersections");
        groove__fmt__debug_usize_list(out, v->durable_ids->intersections.ptr, v->durable_ids->intersections.len);
        groove__fmt__DebugStruct__field(&d2, "candidate");
        if (v->durable_ids->has_candidate) jz_str_appendf(out, "Some(%u)", (unsigned)v->durable_ids->candidate); else jz_str_push_cstr(out, "None");
        groove__fmt__DebugStruct__finish(&d2);
    }
    groove__fmt__DebugStruct__field(&d, "intersections");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->intersections.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jz_str_push_char(out, '(');
        groove__fmt__debug_str(out, &v->intersections.ptr[i].index);
        jz_str_push_cstr(out, ", ");
        groove__ivm__op_types__StaticScanSpec__Debug__fmt(out, &v->intersections.ptr[i].scan);
        jz_str_push_char(out, ')');
    }
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__field(&d, "candidate_filter");
    if (v->has_candidate_filter) {
        jz_str_push_cstr(out, "Some(");
        groove__ivm__graph__IndexCandidateFilter__Debug__fmt(out, &v->candidate_filter);
        jz_str_push_char(out, ')');
    } else jz_str_push_cstr(out, "None");
    groove__fmt__DebugStruct__field(&d, "input_descriptor"); dbg_descriptor(out, v->input_descriptor);
    groove__fmt__DebugStruct__field(&d, "variant_projection"); vpt_opt_debug(out, v->has_variant_projection, &v->variant_projection);
    groove__fmt__DebugStruct__field(&d, "row_projection"); vpt_opt_debug(out, v->has_row_projection, &v->row_projection);
    groove__fmt__DebugStruct__field(&d, "key_fields"); groove__fmt__debug_usize_list(out, v->key_fields.ptr, v->key_fields.len);
    groove__fmt__DebugStruct__field(&d, "value_fields"); groove__fmt__debug_usize_list(out, v->value_fields.ptr, v->value_fields.len);
    groove__fmt__DebugStruct__field(&d, "unique"); groove__ivm__op_types__dbg_bool(out, v->unique);
    groove__fmt__DebugStruct__field(&d, "append_value_to_key"); groove__ivm__op_types__dbg_bool(out, v->append_value_to_key);
    groove__fmt__DebugStruct__field(&d, "store_value"); groove__ivm__op_types__dbg_bool(out, v->store_value);
    groove__fmt__DebugStruct__field(&d, "scan"); groove__ivm__op_types__scan_opt_debug(out, v->has_scan, &v->scan);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__InlineRecordsOp__Clone__clone(groove_InlineRecordsOp *dst, const groove_InlineRecordsOp *src) { groove__ivm__bytesvec_clone(&dst->records, &src->records); }
void groove__ivm__op_types__InlineRecordsOp__drop(groove_InlineRecordsOp *v) { groove__ivm__bytesvec_drop(&v->records); }
bool groove__ivm__op_types__InlineRecordsOp__PartialEq__eq(const groove_InlineRecordsOp *a, const groove_InlineRecordsOp *b) { return groove__ivm__bytesvec_eq(&a->records, &b->records); }
void groove__ivm__op_types__InlineRecordsOp__Hash__hash(const groove_InlineRecordsOp *v, groove_Hasher *h) { groove__ivm__bytesvec_hash(&v->records, h); }
void groove__ivm__op_types__InlineRecordsOp__Debug__fmt(jz_str *out, const groove_InlineRecordsOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "InlineRecordsOp");
    groove__fmt__DebugStruct__field(&d, "records");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->records.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove__fmt__debug_bytes(out, v->records.ptr[i].ptr, v->records.ptr[i].len);
    }
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__finish(&d);
}

/* BindingSourceKey */
void groove__ivm__op_types__BindingSourceKey__prepared(groove_BindingSourceKey *out, jz_str shape) {
    out->tag = GROOVE_BSK_PREPARED; out->prepared = shape; out->runtime_namespace = 0; out->local = 0;
}
const jz_str *groove__ivm__op_types__BindingSourceKey__prepared_name(const groove_BindingSourceKey *k) {
    return k->tag == GROOVE_BSK_PREPARED ? &k->prepared : NULL;
}
void groove__ivm__op_types__BindingSourceKey__Clone__clone(groove_BindingSourceKey *dst, const groove_BindingSourceKey *src) {
    *dst = *src;
    if (src->tag == GROOVE_BSK_PREPARED) dst->prepared = jz_str_clone(&src->prepared);
}
void groove__ivm__op_types__BindingSourceKey__drop(groove_BindingSourceKey *v) { if (v->tag == GROOVE_BSK_PREPARED) jz_str_drop(&v->prepared); }
void groove__ivm__op_types__BindingSourceKey__drop_v(void *k) { groove__ivm__op_types__BindingSourceKey__drop(k); }
bool groove__ivm__op_types__BindingSourceKey__PartialEq__eq(const groove_BindingSourceKey *a, const groove_BindingSourceKey *b) {
    if (a->tag != b->tag) return false;
    if (a->tag == GROOVE_BSK_PREPARED) return jz_str_eq(&a->prepared, &b->prepared);
    return a->runtime_namespace == b->runtime_namespace && a->local == b->local;
}
bool groove__ivm__op_types__BindingSourceKey__eq_v(const void *a, const void *b) { return groove__ivm__op_types__BindingSourceKey__PartialEq__eq(a, b); }
int groove__ivm__op_types__BindingSourceKey__Ord__cmp(const groove_BindingSourceKey *a, const groove_BindingSourceKey *b) {
    if (a->tag != b->tag) return CMP3(a->tag, b->tag);
    if (a->tag == GROOVE_BSK_PREPARED) return jz_str_cmp(&a->prepared, &b->prepared);
    if (a->runtime_namespace != b->runtime_namespace) return CMP3(a->runtime_namespace, b->runtime_namespace);
    return CMP3(a->local, b->local);
}
void groove__ivm__op_types__BindingSourceKey__Hash__hash(const groove_BindingSourceKey *v, groove_Hasher *h) {
    groove_hash_discr(h, v->tag);
    if (v->tag == GROOVE_BSK_PREPARED) groove_hash_str(h, &v->prepared);
    else { groove_hash_u64(h, v->runtime_namespace); groove_hash_u64(h, v->local); }
}
u64 groove__ivm__op_types__BindingSourceKey__hash_v(const void *k) {
    const groove_BindingSourceKey *v = k;
    if (v->tag == GROOVE_BSK_PREPARED) return jz_hash_combine(0, jz_str_hash(&v->prepared));
    return jz_hash_combine(jz_hash_combine(1, jz_hash_u64(v->runtime_namespace)), jz_hash_u64(v->local));
}
void groove__ivm__op_types__BindingSourceKey__Debug__fmt(jz_str *out, const groove_BindingSourceKey *v) {
    if (v->tag == GROOVE_BSK_PREPARED) {
        groove__fmt__debug_tuple_begin(out, "Prepared");
        groove__fmt__debug_str(out, &v->prepared);
        groove__fmt__debug_tuple_end(out);
    } else {
        groove_DebugStruct d = groove__fmt__debug_struct(out, "RuntimeInput");
        groove__fmt__DebugStruct__field(&d, "runtime_namespace"); jz_str_appendf(out, "%llu", (unsigned long long)v->runtime_namespace);
        groove__fmt__DebugStruct__field(&d, "local"); jz_str_appendf(out, "%llu", (unsigned long long)v->local);
        groove__fmt__DebugStruct__finish(&d);
    }
}

/* ======================================================================
 * Stateless transformations
 * ====================================================================== */
void groove__ivm__op_types__PersistOp__Clone__clone(groove_PersistOp *dst, const groove_PersistOp *src) {
    dst->name = jz_str_clone(&src->name);
    groove__ivm__graph__DurableStorage__Clone__clone(&dst->storage, &src->storage);
    groove__ivm__usizevec_clone(&dst->key_fields, &src->key_fields);
    dst->unique = src->unique;
}
void groove__ivm__op_types__PersistOp__drop(groove_PersistOp *v) {
    jz_str_drop(&v->name); groove__ivm__graph__DurableStorage__drop(&v->storage); jz_vec_drop(&v->key_fields, 4, NULL);
}
bool groove__ivm__op_types__PersistOp__PartialEq__eq(const groove_PersistOp *a, const groove_PersistOp *b) {
    return jz_str_eq(&a->name, &b->name) && groove__ivm__graph__DurableStorage__PartialEq__eq(&a->storage, &b->storage) &&
           groove__ivm__usizevec_eq(&a->key_fields, &b->key_fields) && a->unique == b->unique;
}
void groove__ivm__op_types__PersistOp__Hash__hash(const groove_PersistOp *v, groove_Hasher *h) {
    groove_hash_str(h, &v->name); groove__ivm__graph__DurableStorage__Hash__hash(&v->storage, h);
    groove__ivm__usizevec_hash(&v->key_fields, h); groove_hash_bool(h, v->unique);
}
void groove__ivm__op_types__PersistOp__Debug__fmt(jz_str *out, const groove_PersistOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "PersistOp");
    groove__fmt__DebugStruct__field(&d, "name"); groove__fmt__debug_str(out, &v->name);
    groove__fmt__DebugStruct__field(&d, "storage"); groove__ivm__graph__DurableStorage__Debug__fmt(out, &v->storage);
    groove__fmt__DebugStruct__field(&d, "key_fields"); groove__fmt__debug_usize_list(out, v->key_fields.ptr, v->key_fields.len);
    groove__fmt__DebugStruct__field(&d, "unique"); groove__ivm__op_types__dbg_bool(out, v->unique);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__FilterOp__Clone__clone(groove_FilterOp *dst, const groove_FilterOp *src) {
    groove__ivm__op_types__PredicateExpr__Clone__clone(&dst->predicate, &src->predicate);
    dst->comparison = src->comparison;
}
void groove__ivm__op_types__FilterOp__drop(groove_FilterOp *v) { groove__ivm__op_types__PredicateExpr__drop(&v->predicate); }
bool groove__ivm__op_types__FilterOp__PartialEq__eq(const groove_FilterOp *a, const groove_FilterOp *b) {
    return groove__ivm__op_types__PredicateExpr__PartialEq__eq(&a->predicate, &b->predicate) && a->comparison == b->comparison;
}
void groove__ivm__op_types__FilterOp__Hash__hash(const groove_FilterOp *v, groove_Hasher *h) {
    groove__ivm__op_types__PredicateExpr__Hash__hash(&v->predicate, h); groove_hash_discr(h, v->comparison);
}
void groove__ivm__op_types__FilterOp__Debug__fmt(jz_str *out, const groove_FilterOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "FilterOp");
    groove__fmt__DebugStruct__field(&d, "predicate"); groove__ivm__op_types__PredicateExpr__Debug__fmt(out, &v->predicate);
    groove__fmt__DebugStruct__field(&d, "comparison"); groove__ivm__op_types__dbg_cmp(out, v->comparison);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__ProjectionExpr__Clone__clone(groove_ProjectionExpr *dst, const groove_ProjectionExpr *src) {
    groove__ivm__graph__ProjectExpr__Clone__clone(&dst->expression, &src->expression);
    dst->has_output_name = src->has_output_name;
    groove__ivm__op_types__opt_str_clone(src->has_output_name, &src->output_name, &dst->output_name);
    groove__records__FieldIdentity__Clone__clone(&dst->output_identity, &src->output_identity);
}
void groove__ivm__op_types__ProjectionExpr__drop(groove_ProjectionExpr *v) {
    groove__ivm__graph__ProjectExpr__drop(&v->expression);
    if (v->has_output_name) jz_str_drop(&v->output_name);
    groove__records__FieldIdentity__drop(&v->output_identity);
}
bool groove__ivm__op_types__ProjectionExpr__PartialEq__eq(const groove_ProjectionExpr *a, const groove_ProjectionExpr *b) {
    return groove__ivm__graph__ProjectExpr__PartialEq__eq(&a->expression, &b->expression) &&
           groove__ivm__op_types__opt_str_eq(a->has_output_name, &a->output_name, b->has_output_name, &b->output_name) &&
           groove__records__FieldIdentity__PartialEq__eq(&a->output_identity, &b->output_identity);
}
void groove__ivm__op_types__ProjectionExpr__Hash__hash(const groove_ProjectionExpr *v, groove_Hasher *h) {
    groove__ivm__graph__ProjectExpr__Hash__hash(&v->expression, h);
    groove__ivm__op_types__hash_opt_str(h, v->has_output_name, &v->output_name);
    groove__records__FieldIdentity__Hash__hash(&v->output_identity, h);
}
void groove__ivm__op_types__ProjectionExpr__Debug__fmt(jz_str *out, const groove_ProjectionExpr *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "ProjectionExpr");
    groove__fmt__DebugStruct__field(&d, "expression"); groove__ivm__graph__ProjectExpr__Debug__fmt(out, &v->expression);
    groove__fmt__DebugStruct__field(&d, "output_name"); groove__ivm__op_types__dbg_opt_str(out, v->has_output_name, &v->output_name);
    groove__fmt__DebugStruct__field(&d, "output_identity"); groove__records__FieldIdentity__Debug__fmt(out, &v->output_identity);
    groove__fmt__DebugStruct__finish(&d);
}
static void projection_expr_clone_v(void *d, const void *s) { groove__ivm__op_types__ProjectionExpr__Clone__clone(d, s); }
static void projection_expr_drop_v(void *p) { groove__ivm__op_types__ProjectionExpr__drop(p); }

void groove__ivm__op_types__MapProjectOp__Clone__clone(groove_MapProjectOp *dst, const groove_MapProjectOp *src) {
    jz_vec_clone(&dst->expressions, &src->expressions, sizeof(groove_ProjectionExpr), projection_expr_clone_v);
    jz_vec_clone(&dst->mapping, &src->mapping, sizeof(groove_UsizePair), NULL);
}
void groove__ivm__op_types__MapProjectOp__drop(groove_MapProjectOp *v) {
    jz_vec_drop(&v->expressions, sizeof(groove_ProjectionExpr), projection_expr_drop_v);
    jz_vec_drop(&v->mapping, sizeof(groove_UsizePair), NULL);
}
bool groove__ivm__op_types__MapProjectOp__PartialEq__eq(const groove_MapProjectOp *a, const groove_MapProjectOp *b) {
    if (a->expressions.len != b->expressions.len || a->mapping.len != b->mapping.len) return false;
    for (u32 i = 0; i < a->expressions.len; i++)
        if (!groove__ivm__op_types__ProjectionExpr__PartialEq__eq(&a->expressions.ptr[i], &b->expressions.ptr[i])) return false;
    return a->mapping.len == 0 || memcmp(a->mapping.ptr, b->mapping.ptr, a->mapping.len * sizeof(groove_UsizePair)) == 0;
}
void groove__ivm__op_types__MapProjectOp__Hash__hash(const groove_MapProjectOp *v, groove_Hasher *h) {
    groove_hash_usize(h, v->expressions.len);
    for (u32 i = 0; i < v->expressions.len; i++) groove__ivm__op_types__ProjectionExpr__Hash__hash(&v->expressions.ptr[i], h);
    groove_hash_usize(h, v->mapping.len);
    for (u32 i = 0; i < v->mapping.len; i++) { groove_hash_usize(h, v->mapping.ptr[i].a); groove_hash_usize(h, v->mapping.ptr[i].b); }
}
void groove__ivm__op_types__MapProjectOp__Debug__fmt(jz_str *out, const groove_MapProjectOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "MapProjectOp");
    groove__fmt__DebugStruct__field(&d, "expressions");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->expressions.len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__ProjectionExpr__Debug__fmt(out, &v->expressions.ptr[i]); }
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__field(&d, "mapping");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->mapping.len; i++) jz_str_appendf(out, i ? ", (%u, %u)" : "(%u, %u)", (unsigned)v->mapping.ptr[i].a, (unsigned)v->mapping.ptr[i].b);
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__StreamingChecksumOp__Clone__clone(groove_StreamingChecksumOp *dst, const groove_StreamingChecksumOp *src) {
    *dst = *src; dst->field = jz_str_clone(&src->field); dst->output_field = jz_str_clone(&src->output_field);
}
void groove__ivm__op_types__StreamingChecksumOp__drop(groove_StreamingChecksumOp *v) { jz_str_drop(&v->field); jz_str_drop(&v->output_field); }
bool groove__ivm__op_types__StreamingChecksumOp__PartialEq__eq(const groove_StreamingChecksumOp *a, const groove_StreamingChecksumOp *b) {
    return jz_str_eq(&a->field, &b->field) && a->field_idx == b->field_idx && jz_str_eq(&a->output_field, &b->output_field) &&
           a->window_bytes == b->window_bytes && a->max_bytes_per_turn == b->max_bytes_per_turn;
}
void groove__ivm__op_types__StreamingChecksumOp__Hash__hash(const groove_StreamingChecksumOp *v, groove_Hasher *h) {
    groove_hash_str(h, &v->field); groove_hash_usize(h, v->field_idx); groove_hash_str(h, &v->output_field);
    groove_hash_usize(h, v->window_bytes); groove_hash_usize(h, v->max_bytes_per_turn);
}
void groove__ivm__op_types__StreamingChecksumOp__Debug__fmt(jz_str *out, const groove_StreamingChecksumOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "StreamingChecksumOp");
    groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->field);
    groove__fmt__DebugStruct__field(&d, "field_idx"); jz_str_appendf(out, "%u", (unsigned)v->field_idx);
    groove__fmt__DebugStruct__field(&d, "output_field"); groove__fmt__debug_str(out, &v->output_field);
    groove__fmt__DebugStruct__field(&d, "window_bytes"); jz_str_appendf(out, "%u", (unsigned)v->window_bytes);
    groove__fmt__DebugStruct__field(&d, "max_bytes_per_turn"); jz_str_appendf(out, "%u", (unsigned)v->max_bytes_per_turn);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__UnwrapNullableOp__Clone__clone(groove_UnwrapNullableOp *dst, const groove_UnwrapNullableOp *src) { dst->field = jz_str_clone(&src->field); dst->field_idx = src->field_idx; }
void groove__ivm__op_types__UnwrapNullableOp__drop(groove_UnwrapNullableOp *v) { jz_str_drop(&v->field); }
bool groove__ivm__op_types__UnwrapNullableOp__PartialEq__eq(const groove_UnwrapNullableOp *a, const groove_UnwrapNullableOp *b) { return jz_str_eq(&a->field, &b->field) && a->field_idx == b->field_idx; }
void groove__ivm__op_types__UnwrapNullableOp__Hash__hash(const groove_UnwrapNullableOp *v, groove_Hasher *h) { groove_hash_str(h, &v->field); groove_hash_usize(h, v->field_idx); }
void groove__ivm__op_types__UnwrapNullableOp__Debug__fmt(jz_str *out, const groove_UnwrapNullableOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "UnwrapNullableOp");
    groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->field);
    groove__fmt__DebugStruct__field(&d, "field_idx"); jz_str_appendf(out, "%u", (unsigned)v->field_idx);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__UnnestOp__Clone__clone(groove_UnnestOp *dst, const groove_UnnestOp *src) {
    dst->array_field = jz_str_clone(&src->array_field); dst->array_field_idx = src->array_field_idx; dst->element_field = jz_str_clone(&src->element_field);
}
void groove__ivm__op_types__UnnestOp__drop(groove_UnnestOp *v) { jz_str_drop(&v->array_field); jz_str_drop(&v->element_field); }
bool groove__ivm__op_types__UnnestOp__PartialEq__eq(const groove_UnnestOp *a, const groove_UnnestOp *b) {
    return jz_str_eq(&a->array_field, &b->array_field) && a->array_field_idx == b->array_field_idx && jz_str_eq(&a->element_field, &b->element_field);
}
void groove__ivm__op_types__UnnestOp__Hash__hash(const groove_UnnestOp *v, groove_Hasher *h) {
    groove_hash_str(h, &v->array_field); groove_hash_usize(h, v->array_field_idx); groove_hash_str(h, &v->element_field);
}
void groove__ivm__op_types__UnnestOp__Debug__fmt(jz_str *out, const groove_UnnestOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "UnnestOp");
    groove__fmt__DebugStruct__field(&d, "array_field"); groove__fmt__debug_str(out, &v->array_field);
    groove__fmt__DebugStruct__field(&d, "array_field_idx"); jz_str_appendf(out, "%u", (unsigned)v->array_field_idx);
    groove__fmt__DebugStruct__field(&d, "element_field"); groove__fmt__debug_str(out, &v->element_field);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__VariantProjectOp__Clone__clone(groove_VariantProjectOp *dst, const groove_VariantProjectOp *src) { *dst = *src; dst->field = jz_str_clone(&src->field); }
void groove__ivm__op_types__VariantProjectOp__drop(groove_VariantProjectOp *v) { jz_str_drop(&v->field); }
bool groove__ivm__op_types__VariantProjectOp__PartialEq__eq(const groove_VariantProjectOp *a, const groove_VariantProjectOp *b) {
    return jz_str_eq(&a->field, &b->field) && a->field_idx == b->field_idx && a->tag == b->tag;
}
void groove__ivm__op_types__VariantProjectOp__Hash__hash(const groove_VariantProjectOp *v, groove_Hasher *h) {
    groove_hash_str(h, &v->field); groove_hash_usize(h, v->field_idx); groove_hash_u32(h, v->tag);
}
void groove__ivm__op_types__VariantProjectOp__Debug__fmt(jz_str *out, const groove_VariantProjectOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "VariantProjectOp");
    groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->field);
    groove__fmt__DebugStruct__field(&d, "field_idx"); jz_str_appendf(out, "%u", (unsigned)v->field_idx);
    groove__fmt__DebugStruct__field(&d, "tag"); jz_str_appendf(out, "%u", (unsigned)v->tag);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__IndexByOp__Clone__clone(groove_IndexByOp *dst, const groove_IndexByOp *src) {
    planvec_clone(&dst->key_expressions, &src->key_expressions);
    planvec_clone(&dst->value_expressions, &src->value_expressions);
    dst->has_explicit_index = src->has_explicit_index;
    if (src->has_explicit_index) groove__schema__IndexSchema__Clone__clone(&dst->explicit_index, &src->explicit_index);
    groove__ivm__usizevec_clone(&dst->key_fields, &src->key_fields);
    groove__ivm__usizevec_clone(&dst->value_fields, &src->value_fields);
    dst->unique = src->unique; dst->append_value_to_key = src->append_value_to_key; dst->store_value = src->store_value;
    dst->has_scan = src->has_scan; groove__ivm__op_types__scan_opt_clone(src->has_scan, &src->scan, &dst->scan);
}
void groove__ivm__op_types__IndexByOp__drop(groove_IndexByOp *v) {
    planvec_drop(&v->key_expressions); planvec_drop(&v->value_expressions);
    if (v->has_explicit_index) groove__schema__IndexSchema__drop(&v->explicit_index);
    jz_vec_drop(&v->key_fields, 4, NULL); jz_vec_drop(&v->value_fields, 4, NULL);
    if (v->has_scan) groove__ivm__op_types__StaticScanSpec__drop(&v->scan);
}
bool groove__ivm__op_types__IndexByOp__PartialEq__eq(const groove_IndexByOp *a, const groove_IndexByOp *b) {
    if (!planvec_eq(&a->key_expressions, &b->key_expressions) || !planvec_eq(&a->value_expressions, &b->value_expressions)) return false;
    if (a->has_explicit_index != b->has_explicit_index) return false;
    if (a->has_explicit_index && !groove__schema__IndexSchema__PartialEq__eq(&a->explicit_index, &b->explicit_index)) return false;
    return groove__ivm__usizevec_eq(&a->key_fields, &b->key_fields) && groove__ivm__usizevec_eq(&a->value_fields, &b->value_fields) &&
           a->unique == b->unique && a->append_value_to_key == b->append_value_to_key && a->store_value == b->store_value &&
           groove__ivm__op_types__scan_opt_eq(a->has_scan, &a->scan, b->has_scan, &b->scan);
}
void groove__ivm__op_types__IndexByOp__Hash__hash(const groove_IndexByOp *v, groove_Hasher *h) {
    planvec_hash(&v->key_expressions, h); planvec_hash(&v->value_expressions, h);
    groove_hash_discr(h, v->has_explicit_index);
    if (v->has_explicit_index) groove__schema__IndexSchema__Hash__hash(&v->explicit_index, h);
    groove__ivm__usizevec_hash(&v->key_fields, h); groove__ivm__usizevec_hash(&v->value_fields, h);
    groove_hash_bool(h, v->unique); groove_hash_bool(h, v->append_value_to_key); groove_hash_bool(h, v->store_value);
    groove__ivm__op_types__scan_opt_hash(h, v->has_scan, &v->scan);
}
void groove__ivm__op_types__IndexByOp__Debug__fmt(jz_str *out, const groove_IndexByOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "IndexByOp");
    groove__fmt__DebugStruct__field(&d, "key_expressions"); planvec_debug(out, &v->key_expressions);
    groove__fmt__DebugStruct__field(&d, "value_expressions"); planvec_debug(out, &v->value_expressions);
    groove__fmt__DebugStruct__field(&d, "explicit_index");
    if (v->has_explicit_index) { jz_str_push_cstr(out, "Some("); groove__schema__IndexSchema__Debug__fmt(out, &v->explicit_index); jz_str_push_char(out, ')'); }
    else jz_str_push_cstr(out, "None");
    groove__fmt__DebugStruct__field(&d, "key_fields"); groove__fmt__debug_usize_list(out, v->key_fields.ptr, v->key_fields.len);
    groove__fmt__DebugStruct__field(&d, "value_fields"); groove__fmt__debug_usize_list(out, v->value_fields.ptr, v->value_fields.len);
    groove__fmt__DebugStruct__field(&d, "unique"); groove__ivm__op_types__dbg_bool(out, v->unique);
    groove__fmt__DebugStruct__field(&d, "append_value_to_key"); groove__ivm__op_types__dbg_bool(out, v->append_value_to_key);
    groove__fmt__DebugStruct__field(&d, "store_value"); groove__ivm__op_types__dbg_bool(out, v->store_value);
    groove__fmt__DebugStruct__field(&d, "scan"); groove__ivm__op_types__scan_opt_debug(out, v->has_scan, &v->scan);
    groove__fmt__DebugStruct__finish(&d);
}

/* ======================================================================
 * Stateful transformations
 * ====================================================================== */
void groove__ivm__op_types__JoinOp__Clone__clone(groove_JoinOp *dst, const groove_JoinOp *src) {
    dst->kind = src->kind;
    planvec_clone(&dst->left_key, &src->left_key);
    planvec_clone(&dst->right_key, &src->right_key);
    dst->left_descriptor = src->left_descriptor; dst->right_descriptor = src->right_descriptor;
    dst->has_residual_predicate = src->has_residual_predicate;
    if (src->has_residual_predicate) groove__ivm__op_types__PlanExpr__Clone__clone(&dst->residual_predicate, &src->residual_predicate);
    dst->comparison = src->comparison;
}
void groove__ivm__op_types__JoinOp__drop(groove_JoinOp *v) {
    planvec_drop(&v->left_key); planvec_drop(&v->right_key);
    if (v->has_residual_predicate) groove__ivm__op_types__PlanExpr__drop(&v->residual_predicate);
}
bool groove__ivm__op_types__JoinOp__PartialEq__eq(const groove_JoinOp *a, const groove_JoinOp *b) {
    return a->kind == b->kind && planvec_eq(&a->left_key, &b->left_key) && planvec_eq(&a->right_key, &b->right_key) &&
           a->left_descriptor == b->left_descriptor && a->right_descriptor == b->right_descriptor &&
           a->has_residual_predicate == b->has_residual_predicate &&
           (!a->has_residual_predicate || groove__ivm__op_types__PlanExpr__PartialEq__eq(&a->residual_predicate, &b->residual_predicate)) &&
           a->comparison == b->comparison;
}
void groove__ivm__op_types__JoinOp__Hash__hash(const groove_JoinOp *v, groove_Hasher *h) {
    groove_hash_discr(h, v->kind);
    planvec_hash(&v->left_key, h); planvec_hash(&v->right_key, h);
    groove_hash_usize(h, (u32)(uintptr_t)v->left_descriptor); groove_hash_usize(h, (u32)(uintptr_t)v->right_descriptor);
    groove_hash_discr(h, v->has_residual_predicate);
    if (v->has_residual_predicate) groove__ivm__op_types__PlanExpr__Hash__hash(&v->residual_predicate, h);
    groove_hash_discr(h, v->comparison);
}
void groove__ivm__op_types__JoinOp__Debug__fmt(jz_str *out, const groove_JoinOp *v) {
    static const char *const kinds[] = {"Inner", "Left", "Right", "Full"};
    groove_DebugStruct d = groove__fmt__debug_struct(out, "JoinOp");
    groove__fmt__DebugStruct__field(&d, "kind"); jz_str_push_cstr(out, kinds[v->kind]);
    groove__fmt__DebugStruct__field(&d, "left_key"); planvec_debug(out, &v->left_key);
    groove__fmt__DebugStruct__field(&d, "right_key"); planvec_debug(out, &v->right_key);
    groove__fmt__DebugStruct__field(&d, "left_descriptor"); dbg_descriptor(out, v->left_descriptor);
    groove__fmt__DebugStruct__field(&d, "right_descriptor"); dbg_descriptor(out, v->right_descriptor);
    groove__fmt__DebugStruct__field(&d, "residual_predicate");
    if (v->has_residual_predicate) { jz_str_push_cstr(out, "Some("); groove__ivm__op_types__PlanExpr__Debug__fmt(out, &v->residual_predicate); jz_str_push_char(out, ')'); }
    else jz_str_push_cstr(out, "None");
    groove__fmt__DebugStruct__field(&d, "comparison"); groove__ivm__op_types__dbg_cmp(out, v->comparison);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__RecursiveOp__Clone__clone(groove_RecursiveOp *dst, const groove_RecursiveOp *src) {
    *dst = *src;
    dst->frontier = jz_str_clone(&src->frontier);
    groove__ivm__strvec_clone(&dst->read_tables, &src->read_tables);
}
void groove__ivm__op_types__RecursiveOp__drop(groove_RecursiveOp *v) { jz_str_drop(&v->frontier); groove__ivm__strvec_drop(&v->read_tables); }
bool groove__ivm__op_types__RecursiveOp__PartialEq__eq(const groove_RecursiveOp *a, const groove_RecursiveOp *b) {
    return jz_str_eq(&a->frontier, &b->frontier) && a->max_iters == b->max_iters && a->truncate_at_max_iters == b->truncate_at_max_iters &&
           groove__ivm__strvec_eq(&a->read_tables, &b->read_tables) && a->has_step_witness_output == b->has_step_witness_output &&
           (!a->has_step_witness_output || a->step_witness_output == b->step_witness_output);
}
void groove__ivm__op_types__RecursiveOp__Hash__hash(const groove_RecursiveOp *v, groove_Hasher *h) {
    groove_hash_str(h, &v->frontier); groove_hash_usize(h, v->max_iters); groove_hash_bool(h, v->truncate_at_max_iters);
    groove__ivm__strvec_hash(&v->read_tables, h);
    groove_hash_discr(h, v->has_step_witness_output);
    if (v->has_step_witness_output) groove_hash_usize(h, (u32)(uintptr_t)v->step_witness_output);
}
void groove__ivm__op_types__RecursiveOp__Debug__fmt(jz_str *out, const groove_RecursiveOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "RecursiveOp");
    groove__fmt__DebugStruct__field(&d, "frontier");
    groove__fmt__debug_tuple_begin(out, "FrontierName"); groove__fmt__debug_str(out, &v->frontier); groove__fmt__debug_tuple_end(out);
    groove__fmt__DebugStruct__field(&d, "max_iters"); jz_str_appendf(out, "%u", (unsigned)v->max_iters);
    groove__fmt__DebugStruct__field(&d, "truncate_at_max_iters"); groove__ivm__op_types__dbg_bool(out, v->truncate_at_max_iters);
    groove__fmt__DebugStruct__field(&d, "read_tables"); groove__fmt__debug_str_list(out, v->read_tables.ptr, v->read_tables.len);
    groove__fmt__DebugStruct__field(&d, "step_witness_output");
    if (v->has_step_witness_output) { jz_str_push_cstr(out, "Some("); dbg_descriptor(out, v->step_witness_output); jz_str_push_char(out, ')'); }
    else jz_str_push_cstr(out, "None");
    groove__fmt__DebugStruct__finish(&d);
}

/* ======================================================================
 * Aggregate / window
 * ====================================================================== */
void groove__ivm__op_types__ArgMaxByOp__Clone__clone(groove_ArgExtremumByOp *dst, const groove_ArgExtremumByOp *src) {
    groove__ivm__strvec_clone(&dst->group_fields, &src->group_fields);
    groove__ivm__strvec_clone(&dst->order_fields, &src->order_fields);
    groove__ivm__usizevec_clone(&dst->group_field_indices, &src->group_field_indices);
    groove__ivm__usizevec_clone(&dst->comparison_field_indices, &src->comparison_field_indices);
}
void groove__ivm__op_types__ArgMaxByOp__drop(groove_ArgExtremumByOp *v) {
    groove__ivm__strvec_drop(&v->group_fields); groove__ivm__strvec_drop(&v->order_fields);
    jz_vec_drop(&v->group_field_indices, 4, NULL); jz_vec_drop(&v->comparison_field_indices, 4, NULL);
}
bool groove__ivm__op_types__ArgMaxByOp__PartialEq__eq(const groove_ArgExtremumByOp *a, const groove_ArgExtremumByOp *b) {
    return groove__ivm__strvec_eq(&a->group_fields, &b->group_fields) && groove__ivm__strvec_eq(&a->order_fields, &b->order_fields) &&
           groove__ivm__usizevec_eq(&a->group_field_indices, &b->group_field_indices) &&
           groove__ivm__usizevec_eq(&a->comparison_field_indices, &b->comparison_field_indices);
}
void groove__ivm__op_types__ArgMaxByOp__Hash__hash(const groove_ArgExtremumByOp *v, groove_Hasher *h) {
    groove__ivm__strvec_hash(&v->group_fields, h); groove__ivm__strvec_hash(&v->order_fields, h);
    groove__ivm__usizevec_hash(&v->group_field_indices, h); groove__ivm__usizevec_hash(&v->comparison_field_indices, h);
}
void groove__ivm__op_types__ArgMaxByOp__Debug__fmt(jz_str *out, const groove_ArgExtremumByOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "ArgMaxByOp");
    groove__fmt__DebugStruct__field(&d, "group_fields"); groove__fmt__debug_str_list(out, v->group_fields.ptr, v->group_fields.len);
    groove__fmt__DebugStruct__field(&d, "order_fields"); groove__fmt__debug_str_list(out, v->order_fields.ptr, v->order_fields.len);
    groove__fmt__DebugStruct__field(&d, "group_field_indices"); groove__fmt__debug_usize_list(out, v->group_field_indices.ptr, v->group_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "comparison_field_indices");
    groove__fmt__debug_usize_list(out, v->comparison_field_indices.ptr, v->comparison_field_indices.len);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__TopByLimit__Debug__fmt(jz_str *out, const groove_TopByLimit *v) {
    if (v->tag == GROOVE_TOPBY_FINITE) jz_str_appendf(out, "Finite(%llu)", (unsigned long long)v->n);
    else jz_str_push_cstr(out, "Unbounded");
}
void groove__ivm__op_types__limit_hash(groove_Hasher *h, const groove_TopByLimit *l) { groove_hash_discr(h, l->tag); if (l->tag == GROOVE_TOPBY_FINITE) groove_hash_u64(h, l->n); }
bool groove__ivm__op_types__limit_eq(const groove_TopByLimit *a, const groove_TopByLimit *b) { return a->tag == b->tag && (a->tag != GROOVE_TOPBY_FINITE || a->n == b->n); }
static void order_field_clone_v(void *d, const void *s) {
    const groove_TopByOrderField *src = s; groove_TopByOrderField *dst = d;
    dst->field = jz_str_clone(&src->field); dst->direction = src->direction;
}
static void order_field_drop_v(void *p) { jz_str_drop(&((groove_TopByOrderField *)p)->field); }
static void orderfields_clone(groove_TopByOrderFieldVec *dst, const groove_TopByOrderFieldVec *src) { jz_vec_clone(dst, src, sizeof(groove_TopByOrderField), order_field_clone_v); }
static void orderfields_drop(groove_TopByOrderFieldVec *v) { jz_vec_drop(v, sizeof(groove_TopByOrderField), order_field_drop_v); }
static bool orderfields_eq(const groove_TopByOrderFieldVec *a, const groove_TopByOrderFieldVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i].field, &b->ptr[i].field) || a->ptr[i].direction != b->ptr[i].direction) return false;
    return true;
}
static void orderfields_hash(const groove_TopByOrderFieldVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) { groove_hash_str(h, &v->ptr[i].field); groove_hash_discr(h, v->ptr[i].direction); }
}
void groove__ivm__op_types__TopByOrderFieldVec__Debug__fmt(jz_str *out, const groove_TopByOrderFieldVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove_DebugStruct d = groove__fmt__debug_struct(out, "TopByOrderField");
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->ptr[i].field);
        groove__fmt__DebugStruct__field(&d, "direction"); groove__ivm__op_types__dbg_dir(out, v->ptr[i].direction);
        groove__fmt__DebugStruct__finish(&d);
    }
    jz_str_push_char(out, ']');
}

void groove__ivm__op_types__TopByOp__Clone__clone(groove_TopByOp *dst, const groove_TopByOp *src) {
    groove__ivm__strvec_clone(&dst->group_fields, &src->group_fields);
    groove__ivm__usizevec_clone(&dst->group_field_indices, &src->group_field_indices);
    orderfields_clone(&dst->order_fields, &src->order_fields);
    groove__ivm__strvec_clone(&dst->tie_fields, &src->tie_fields);
    groove__ivm__usizevec_clone(&dst->sort_field_indices, &src->sort_field_indices);
    jz_vec_clone(&dst->sort_directions, &src->sort_directions, 1, NULL);
    dst->offset = src->offset; dst->limit = src->limit;
}
void groove__ivm__op_types__TopByOp__drop(groove_TopByOp *v) {
    groove__ivm__strvec_drop(&v->group_fields); jz_vec_drop(&v->group_field_indices, 4, NULL);
    orderfields_drop(&v->order_fields); groove__ivm__strvec_drop(&v->tie_fields);
    jz_vec_drop(&v->sort_field_indices, 4, NULL); jz_vec_drop(&v->sort_directions, 1, NULL);
}
bool groove__ivm__op_types__TopByOp__PartialEq__eq(const groove_TopByOp *a, const groove_TopByOp *b) {
    return groove__ivm__strvec_eq(&a->group_fields, &b->group_fields) && groove__ivm__usizevec_eq(&a->group_field_indices, &b->group_field_indices) &&
           orderfields_eq(&a->order_fields, &b->order_fields) && groove__ivm__strvec_eq(&a->tie_fields, &b->tie_fields) &&
           groove__ivm__usizevec_eq(&a->sort_field_indices, &b->sort_field_indices) && dirvec_eq(&a->sort_directions, &b->sort_directions) &&
           a->offset == b->offset && groove__ivm__op_types__limit_eq(&a->limit, &b->limit);
}
void groove__ivm__op_types__TopByOp__Hash__hash(const groove_TopByOp *v, groove_Hasher *h) {
    groove__ivm__strvec_hash(&v->group_fields, h); groove__ivm__usizevec_hash(&v->group_field_indices, h);
    orderfields_hash(&v->order_fields, h); groove__ivm__strvec_hash(&v->tie_fields, h);
    groove__ivm__usizevec_hash(&v->sort_field_indices, h); dirvec_hash(&v->sort_directions, h);
    groove_hash_u64(h, v->offset); groove__ivm__op_types__limit_hash(h, &v->limit);
}
void groove__ivm__op_types__TopByOp__Debug__fmt(jz_str *out, const groove_TopByOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "TopByOp");
    groove__fmt__DebugStruct__field(&d, "group_fields"); groove__fmt__debug_str_list(out, v->group_fields.ptr, v->group_fields.len);
    groove__fmt__DebugStruct__field(&d, "group_field_indices"); groove__fmt__debug_usize_list(out, v->group_field_indices.ptr, v->group_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "order_fields"); groove__ivm__op_types__TopByOrderFieldVec__Debug__fmt(out, &v->order_fields);
    groove__fmt__DebugStruct__field(&d, "tie_fields"); groove__fmt__debug_str_list(out, v->tie_fields.ptr, v->tie_fields.len);
    groove__fmt__DebugStruct__field(&d, "sort_field_indices"); groove__fmt__debug_usize_list(out, v->sort_field_indices.ptr, v->sort_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "sort_directions"); dirvec_debug(out, &v->sort_directions);
    groove__fmt__DebugStruct__field(&d, "offset"); jz_str_appendf(out, "%llu", (unsigned long long)v->offset);
    groove__fmt__DebugStruct__field(&d, "limit"); groove__ivm__op_types__TopByLimit__Debug__fmt(out, &v->limit);
    groove__fmt__DebugStruct__finish(&d);
}

static void cbp_clone_v(void *d, const void *s) {
    const groove_CollectByProjection *src = s; groove_CollectByProjection *dst = d;
    dst->field = jz_str_clone(&src->field); dst->field_idx = src->field_idx;
    dst->output_name = jz_str_clone(&src->output_name); dst->unwrap_nullable = src->unwrap_nullable;
}
static void cbp_drop_v(void *p) { groove_CollectByProjection *v = p; jz_str_drop(&v->field); jz_str_drop(&v->output_name); }
static void cbpvec_clone(groove_CollectByProjectionVec *dst, const groove_CollectByProjectionVec *src) { jz_vec_clone(dst, src, sizeof(groove_CollectByProjection), cbp_clone_v); }
static void cbpvec_drop(groove_CollectByProjectionVec *v) { jz_vec_drop(v, sizeof(groove_CollectByProjection), cbp_drop_v); }
static bool cbpvec_eq(const groove_CollectByProjectionVec *a, const groove_CollectByProjectionVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) {
        const groove_CollectByProjection *x = &a->ptr[i], *y = &b->ptr[i];
        if (!jz_str_eq(&x->field, &y->field) || x->field_idx != y->field_idx || !jz_str_eq(&x->output_name, &y->output_name) ||
            x->unwrap_nullable != y->unwrap_nullable)
            return false;
    }
    return true;
}
static void cbpvec_hash(const groove_CollectByProjectionVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) {
        groove_hash_str(h, &v->ptr[i].field); groove_hash_usize(h, v->ptr[i].field_idx);
        groove_hash_str(h, &v->ptr[i].output_name); groove_hash_bool(h, v->ptr[i].unwrap_nullable);
    }
}
static void cbpvec_debug(jz_str *out, const groove_CollectByProjectionVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        groove_DebugStruct d = groove__fmt__debug_struct(out, "CollectByProjection");
        groove__fmt__DebugStruct__field(&d, "field"); groove__fmt__debug_str(out, &v->ptr[i].field);
        groove__fmt__DebugStruct__field(&d, "field_idx"); jz_str_appendf(out, "%u", (unsigned)v->ptr[i].field_idx);
        groove__fmt__DebugStruct__field(&d, "output_name"); groove__fmt__debug_str(out, &v->ptr[i].output_name);
        groove__fmt__DebugStruct__field(&d, "unwrap_nullable"); groove__ivm__op_types__dbg_bool(out, v->ptr[i].unwrap_nullable);
        groove__fmt__DebugStruct__finish(&d);
    }
    jz_str_push_char(out, ']');
}
static void slot_clone_v(void *d, const void *s) { groove__ivm__op_types__CollectBySlot__Clone__clone(d, s); }
static void slot_drop_v(void *p) { groove__ivm__op_types__CollectBySlot__drop(p); }
static bool slots_eq(const groove_CollectBySlotVec *a, const groove_CollectBySlotVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!groove__ivm__op_types__CollectBySlot__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
static void slots_hash(const groove_CollectBySlotVec *v, groove_Hasher *h) {
    groove_hash_usize(h, v->len);
    for (u32 i = 0; i < v->len; i++) groove__ivm__op_types__CollectBySlot__Hash__hash(&v->ptr[i], h);
}
static void slots_debug(jz_str *out, const groove_CollectBySlotVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__CollectBySlot__Debug__fmt(out, &v->ptr[i]); }
    jz_str_push_char(out, ']');
}

void groove__ivm__op_types__CollectBySlot__Clone__clone(groove_CollectBySlot *dst, const groove_CollectBySlot *src) {
    groove__ivm__strvec_clone(&dst->group_fields, &src->group_fields);
    groove__ivm__usizevec_clone(&dst->group_field_indices, &src->group_field_indices);
    cbpvec_clone(&dst->child_fields, &src->child_fields);
    dst->child_descriptor = src->child_descriptor;
    dst->collection_field = jz_str_clone(&src->collection_field);
    dst->collection_field_index = src->collection_field_index;
    jz_vec_clone(&dst->slots, &src->slots, sizeof(groove_CollectBySlot), slot_clone_v);
    orderfields_clone(&dst->order_fields, &src->order_fields);
    groove__ivm__strvec_clone(&dst->tie_fields, &src->tie_fields);
    dst->has_presence_field_index = src->has_presence_field_index; dst->presence_field_index = src->presence_field_index;
    dst->has_reference_array_field_index = src->has_reference_array_field_index; dst->reference_array_field_index = src->reference_array_field_index;
    dst->reference_order = src->reference_order;
    groove__ivm__usizevec_clone(&dst->sort_field_indices, &src->sort_field_indices);
    jz_vec_clone(&dst->sort_directions, &src->sort_directions, 1, NULL);
    dst->offset = src->offset; dst->limit = src->limit;
}
void groove__ivm__op_types__CollectBySlot__drop(groove_CollectBySlot *v) {
    groove__ivm__strvec_drop(&v->group_fields); jz_vec_drop(&v->group_field_indices, 4, NULL);
    cbpvec_drop(&v->child_fields); jz_str_drop(&v->collection_field);
    jz_vec_drop(&v->slots, sizeof(groove_CollectBySlot), slot_drop_v);
    orderfields_drop(&v->order_fields); groove__ivm__strvec_drop(&v->tie_fields);
    jz_vec_drop(&v->sort_field_indices, 4, NULL); jz_vec_drop(&v->sort_directions, 1, NULL);
}
bool groove__ivm__op_types__CollectBySlot__PartialEq__eq(const groove_CollectBySlot *a, const groove_CollectBySlot *b) {
    return groove__ivm__strvec_eq(&a->group_fields, &b->group_fields) && groove__ivm__usizevec_eq(&a->group_field_indices, &b->group_field_indices) &&
           cbpvec_eq(&a->child_fields, &b->child_fields) && a->child_descriptor == b->child_descriptor &&
           jz_str_eq(&a->collection_field, &b->collection_field) && a->collection_field_index == b->collection_field_index &&
           slots_eq(&a->slots, &b->slots) && orderfields_eq(&a->order_fields, &b->order_fields) &&
           groove__ivm__strvec_eq(&a->tie_fields, &b->tie_fields) &&
           a->has_presence_field_index == b->has_presence_field_index &&
           (!a->has_presence_field_index || a->presence_field_index == b->presence_field_index) &&
           a->has_reference_array_field_index == b->has_reference_array_field_index &&
           (!a->has_reference_array_field_index || a->reference_array_field_index == b->reference_array_field_index) &&
           a->reference_order == b->reference_order && groove__ivm__usizevec_eq(&a->sort_field_indices, &b->sort_field_indices) &&
           dirvec_eq(&a->sort_directions, &b->sort_directions) && a->offset == b->offset && groove__ivm__op_types__limit_eq(&a->limit, &b->limit);
}
void groove__ivm__op_types__CollectBySlot__Hash__hash(const groove_CollectBySlot *v, groove_Hasher *h) {
    groove__ivm__strvec_hash(&v->group_fields, h); groove__ivm__usizevec_hash(&v->group_field_indices, h);
    cbpvec_hash(&v->child_fields, h); groove_hash_usize(h, (u32)(uintptr_t)v->child_descriptor);
    groove_hash_str(h, &v->collection_field); groove_hash_usize(h, v->collection_field_index);
    slots_hash(&v->slots, h); orderfields_hash(&v->order_fields, h); groove__ivm__strvec_hash(&v->tie_fields, h);
    groove_hash_discr(h, v->has_presence_field_index); if (v->has_presence_field_index) groove_hash_usize(h, v->presence_field_index);
    groove_hash_discr(h, v->has_reference_array_field_index); if (v->has_reference_array_field_index) groove_hash_usize(h, v->reference_array_field_index);
    groove_hash_bool(h, v->reference_order);
    groove__ivm__usizevec_hash(&v->sort_field_indices, h); dirvec_hash(&v->sort_directions, h);
    groove_hash_u64(h, v->offset); groove__ivm__op_types__limit_hash(h, &v->limit);
}
static void dbg_opt_usize(jz_str *out, bool has, u32 v) { if (has) jz_str_appendf(out, "Some(%u)", (unsigned)v); else jz_str_push_cstr(out, "None"); }
void groove__ivm__op_types__CollectBySlot__Debug__fmt(jz_str *out, const groove_CollectBySlot *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "CollectBySlot");
    groove__fmt__DebugStruct__field(&d, "group_fields"); groove__fmt__debug_str_list(out, v->group_fields.ptr, v->group_fields.len);
    groove__fmt__DebugStruct__field(&d, "group_field_indices"); groove__fmt__debug_usize_list(out, v->group_field_indices.ptr, v->group_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "child_fields"); cbpvec_debug(out, &v->child_fields);
    groove__fmt__DebugStruct__field(&d, "child_descriptor"); dbg_descriptor(out, v->child_descriptor);
    groove__fmt__DebugStruct__field(&d, "collection_field"); groove__fmt__debug_str(out, &v->collection_field);
    groove__fmt__DebugStruct__field(&d, "collection_field_index"); jz_str_appendf(out, "%u", (unsigned)v->collection_field_index);
    groove__fmt__DebugStruct__field(&d, "slots"); slots_debug(out, &v->slots);
    groove__fmt__DebugStruct__field(&d, "order_fields"); groove__ivm__op_types__TopByOrderFieldVec__Debug__fmt(out, &v->order_fields);
    groove__fmt__DebugStruct__field(&d, "tie_fields"); groove__fmt__debug_str_list(out, v->tie_fields.ptr, v->tie_fields.len);
    groove__fmt__DebugStruct__field(&d, "presence_field_index"); dbg_opt_usize(out, v->has_presence_field_index, v->presence_field_index);
    groove__fmt__DebugStruct__field(&d, "reference_array_field_index"); dbg_opt_usize(out, v->has_reference_array_field_index, v->reference_array_field_index);
    groove__fmt__DebugStruct__field(&d, "reference_order"); groove__ivm__op_types__dbg_bool(out, v->reference_order);
    groove__fmt__DebugStruct__field(&d, "sort_field_indices"); groove__fmt__debug_usize_list(out, v->sort_field_indices.ptr, v->sort_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "sort_directions"); dirvec_debug(out, &v->sort_directions);
    groove__fmt__DebugStruct__field(&d, "offset"); jz_str_appendf(out, "%llu", (unsigned long long)v->offset);
    groove__fmt__DebugStruct__field(&d, "limit"); groove__ivm__op_types__TopByLimit__Debug__fmt(out, &v->limit);
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__CollectByOp__Clone__clone(groove_CollectByOp *dst, const groove_CollectByOp *src) {
    dst->mode = src->mode;
    groove__ivm__strvec_clone(&dst->group_fields, &src->group_fields);
    groove__ivm__usizevec_clone(&dst->group_field_indices, &src->group_field_indices);
    cbpvec_clone(&dst->parent_fields, &src->parent_fields);
    cbpvec_clone(&dst->child_fields, &src->child_fields);
    dst->child_descriptor = src->child_descriptor;
    dst->collection_field = jz_str_clone(&src->collection_field);
    dst->collection_field_index = src->collection_field_index;
    jz_vec_clone(&dst->slots, &src->slots, sizeof(groove_CollectBySlot), slot_clone_v);
    cbpvec_clone(&dst->tuple_fields, &src->tuple_fields);
    groove__ivm__strvec_clone(&dst->occurrence_id_fields, &src->occurrence_id_fields);
    groove__ivm__usizevec_clone(&dst->occurrence_id_field_indices, &src->occurrence_id_field_indices);
    orderfields_clone(&dst->order_fields, &src->order_fields);
    groove__ivm__strvec_clone(&dst->tie_fields, &src->tie_fields);
    groove__ivm__usizevec_clone(&dst->sort_field_indices, &src->sort_field_indices);
    jz_vec_clone(&dst->sort_directions, &src->sort_directions, 1, NULL);
    dst->offset = src->offset; dst->limit = src->limit;
}
void groove__ivm__op_types__CollectByOp__drop(groove_CollectByOp *v) {
    groove__ivm__strvec_drop(&v->group_fields); jz_vec_drop(&v->group_field_indices, 4, NULL);
    cbpvec_drop(&v->parent_fields); cbpvec_drop(&v->child_fields); jz_str_drop(&v->collection_field);
    jz_vec_drop(&v->slots, sizeof(groove_CollectBySlot), slot_drop_v);
    cbpvec_drop(&v->tuple_fields); groove__ivm__strvec_drop(&v->occurrence_id_fields);
    jz_vec_drop(&v->occurrence_id_field_indices, 4, NULL);
    orderfields_drop(&v->order_fields); groove__ivm__strvec_drop(&v->tie_fields);
    jz_vec_drop(&v->sort_field_indices, 4, NULL); jz_vec_drop(&v->sort_directions, 1, NULL);
}
bool groove__ivm__op_types__CollectByOp__PartialEq__eq(const groove_CollectByOp *a, const groove_CollectByOp *b) {
    return a->mode == b->mode && groove__ivm__strvec_eq(&a->group_fields, &b->group_fields) &&
           groove__ivm__usizevec_eq(&a->group_field_indices, &b->group_field_indices) && cbpvec_eq(&a->parent_fields, &b->parent_fields) &&
           cbpvec_eq(&a->child_fields, &b->child_fields) && a->child_descriptor == b->child_descriptor &&
           jz_str_eq(&a->collection_field, &b->collection_field) && a->collection_field_index == b->collection_field_index &&
           slots_eq(&a->slots, &b->slots) && cbpvec_eq(&a->tuple_fields, &b->tuple_fields) &&
           groove__ivm__strvec_eq(&a->occurrence_id_fields, &b->occurrence_id_fields) &&
           groove__ivm__usizevec_eq(&a->occurrence_id_field_indices, &b->occurrence_id_field_indices) &&
           orderfields_eq(&a->order_fields, &b->order_fields) && groove__ivm__strvec_eq(&a->tie_fields, &b->tie_fields) &&
           groove__ivm__usizevec_eq(&a->sort_field_indices, &b->sort_field_indices) && dirvec_eq(&a->sort_directions, &b->sort_directions) &&
           a->offset == b->offset && groove__ivm__op_types__limit_eq(&a->limit, &b->limit);
}
void groove__ivm__op_types__CollectByOp__Hash__hash(const groove_CollectByOp *v, groove_Hasher *h) {
    groove_hash_discr(h, v->mode);
    groove__ivm__strvec_hash(&v->group_fields, h); groove__ivm__usizevec_hash(&v->group_field_indices, h);
    cbpvec_hash(&v->parent_fields, h); cbpvec_hash(&v->child_fields, h);
    groove_hash_usize(h, (u32)(uintptr_t)v->child_descriptor);
    groove_hash_str(h, &v->collection_field); groove_hash_usize(h, v->collection_field_index);
    slots_hash(&v->slots, h); cbpvec_hash(&v->tuple_fields, h);
    groove__ivm__strvec_hash(&v->occurrence_id_fields, h); groove__ivm__usizevec_hash(&v->occurrence_id_field_indices, h);
    orderfields_hash(&v->order_fields, h); groove__ivm__strvec_hash(&v->tie_fields, h);
    groove__ivm__usizevec_hash(&v->sort_field_indices, h); dirvec_hash(&v->sort_directions, h);
    groove_hash_u64(h, v->offset); groove__ivm__op_types__limit_hash(h, &v->limit);
}
void groove__ivm__op_types__CollectByOp__Debug__fmt(jz_str *out, const groove_CollectByOp *v) {
    static const char *const modes[] = {"Collect", "Root", "Expand"};
    groove_DebugStruct d = groove__fmt__debug_struct(out, "CollectByOp");
    groove__fmt__DebugStruct__field(&d, "mode"); jz_str_push_cstr(out, modes[v->mode]);
    groove__fmt__DebugStruct__field(&d, "group_fields"); groove__fmt__debug_str_list(out, v->group_fields.ptr, v->group_fields.len);
    groove__fmt__DebugStruct__field(&d, "group_field_indices"); groove__fmt__debug_usize_list(out, v->group_field_indices.ptr, v->group_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "parent_fields"); cbpvec_debug(out, &v->parent_fields);
    groove__fmt__DebugStruct__field(&d, "child_fields"); cbpvec_debug(out, &v->child_fields);
    groove__fmt__DebugStruct__field(&d, "child_descriptor"); dbg_descriptor(out, v->child_descriptor);
    groove__fmt__DebugStruct__field(&d, "collection_field"); groove__fmt__debug_str(out, &v->collection_field);
    groove__fmt__DebugStruct__field(&d, "collection_field_index"); jz_str_appendf(out, "%u", (unsigned)v->collection_field_index);
    groove__fmt__DebugStruct__field(&d, "slots"); slots_debug(out, &v->slots);
    groove__fmt__DebugStruct__field(&d, "tuple_fields"); cbpvec_debug(out, &v->tuple_fields);
    groove__fmt__DebugStruct__field(&d, "occurrence_id_fields"); groove__fmt__debug_str_list(out, v->occurrence_id_fields.ptr, v->occurrence_id_fields.len);
    groove__fmt__DebugStruct__field(&d, "occurrence_id_field_indices");
    groove__fmt__debug_usize_list(out, v->occurrence_id_field_indices.ptr, v->occurrence_id_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "order_fields"); groove__ivm__op_types__TopByOrderFieldVec__Debug__fmt(out, &v->order_fields);
    groove__fmt__DebugStruct__field(&d, "tie_fields"); groove__fmt__debug_str_list(out, v->tie_fields.ptr, v->tie_fields.len);
    groove__fmt__DebugStruct__field(&d, "sort_field_indices"); groove__fmt__debug_usize_list(out, v->sort_field_indices.ptr, v->sort_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "sort_directions"); dirvec_debug(out, &v->sort_directions);
    groove__fmt__DebugStruct__field(&d, "offset"); jz_str_appendf(out, "%llu", (unsigned long long)v->offset);
    groove__fmt__DebugStruct__field(&d, "limit"); groove__ivm__op_types__TopByLimit__Debug__fmt(out, &v->limit);
    groove__fmt__DebugStruct__finish(&d);
}

static void slot_extend_read_field_indices(const groove_CollectBySlot *self, groove_UsizeVec *fields) {
    jz_vec_extend(fields, self->group_field_indices.ptr, self->group_field_indices.len, sizeof(u32));
    for (u32 i = 0; i < self->child_fields.len; i++) JZ_VEC_PUSH(*fields, self->child_fields.ptr[i].field_idx);
    if (self->has_presence_field_index) JZ_VEC_PUSH(*fields, self->presence_field_index);
    if (self->has_reference_array_field_index) JZ_VEC_PUSH(*fields, self->reference_array_field_index);
    jz_vec_extend(fields, self->sort_field_indices.ptr, self->sort_field_indices.len, sizeof(u32));
    for (u32 i = 0; i < self->slots.len; i++) slot_extend_read_field_indices(&self->slots.ptr[i], fields);
}
static int cmp_u32_raw(const void *a, const void *b) { u32 x = *(const u32 *)a, y = *(const u32 *)b; return CMP3(x, y); }
/* Every input field the collector reads, sorted and deduplicated (#3830). */
void groove__ivm__op_types__CollectByOp__read_field_indices(const groove_CollectByOp *self, groove_UsizeVec *out) {
    groove_UsizeVec fields = {0};
    jz_vec_extend(&fields, self->group_field_indices.ptr, self->group_field_indices.len, sizeof(u32));
    for (u32 i = 0; i < self->parent_fields.len; i++) JZ_VEC_PUSH(fields, self->parent_fields.ptr[i].field_idx);
    for (u32 i = 0; i < self->child_fields.len; i++) JZ_VEC_PUSH(fields, self->child_fields.ptr[i].field_idx);
    for (u32 i = 0; i < self->tuple_fields.len; i++) JZ_VEC_PUSH(fields, self->tuple_fields.ptr[i].field_idx);
    jz_vec_extend(&fields, self->occurrence_id_field_indices.ptr, self->occurrence_id_field_indices.len, sizeof(u32));
    jz_vec_extend(&fields, self->sort_field_indices.ptr, self->sort_field_indices.len, sizeof(u32));
    for (u32 i = 0; i < self->slots.len; i++) slot_extend_read_field_indices(&self->slots.ptr[i], &fields);
    jz_sort_unstable(fields.ptr, fields.len, sizeof(u32), jz_cmp_u32, NULL);
    jz_vec_dedup(&fields, sizeof(u32), cmp_u32_raw, NULL);
    *out = fields;
}

void groove__ivm__op_types__agg_expr_clone_v(void *d, const void *s) { groove__ivm__op_types__AggregateExpr__Clone__clone(d, s); }
void groove__ivm__op_types__agg_expr_drop_v(void *p) { groove__ivm__op_types__AggregateExpr__drop(p); }
void groove__ivm__op_types__AggregateExpr__Clone__clone(groove_AggregateExpr *dst, const groove_AggregateExpr *src) {
    dst->function = src->function;
    dst->has_expression = src->has_expression;
    if (src->has_expression) groove__ivm__op_types__PlanExpr__Clone__clone(&dst->expression, &src->expression);
    dst->distinct = src->distinct;
    dst->has_output_name = src->has_output_name; groove__ivm__op_types__opt_str_clone(src->has_output_name, &src->output_name, &dst->output_name);
    dst->has_output_identity = src->has_output_identity;
    if (src->has_output_identity) groove__records__FieldIdentity__Clone__clone(&dst->output_identity, &src->output_identity);
}
void groove__ivm__op_types__AggregateExpr__drop(groove_AggregateExpr *v) {
    if (v->has_expression) groove__ivm__op_types__PlanExpr__drop(&v->expression);
    if (v->has_output_name) jz_str_drop(&v->output_name);
    if (v->has_output_identity) groove__records__FieldIdentity__drop(&v->output_identity);
}
bool groove__ivm__op_types__AggregateExpr__PartialEq__eq(const groove_AggregateExpr *a, const groove_AggregateExpr *b) {
    return a->function == b->function && a->has_expression == b->has_expression &&
           (!a->has_expression || groove__ivm__op_types__PlanExpr__PartialEq__eq(&a->expression, &b->expression)) &&
           a->distinct == b->distinct && groove__ivm__op_types__opt_str_eq(a->has_output_name, &a->output_name, b->has_output_name, &b->output_name) &&
           a->has_output_identity == b->has_output_identity &&
           (!a->has_output_identity || groove__records__FieldIdentity__PartialEq__eq(&a->output_identity, &b->output_identity));
}
void groove__ivm__op_types__AggregateExpr__Hash__hash(const groove_AggregateExpr *v, groove_Hasher *h) {
    groove_hash_discr(h, v->function);
    groove_hash_discr(h, v->has_expression); if (v->has_expression) groove__ivm__op_types__PlanExpr__Hash__hash(&v->expression, h);
    groove_hash_bool(h, v->distinct);
    groove__ivm__op_types__hash_opt_str(h, v->has_output_name, &v->output_name);
    groove_hash_discr(h, v->has_output_identity); if (v->has_output_identity) groove__records__FieldIdentity__Hash__hash(&v->output_identity, h);
}
void groove__ivm__op_types__AggregateExpr__Debug__fmt(jz_str *out, const groove_AggregateExpr *v) {
    static const char *const fns[] = {"Count", "Sum", "Avg", "Min", "Max"};
    groove_DebugStruct d = groove__fmt__debug_struct(out, "AggregateExpr");
    groove__fmt__DebugStruct__field(&d, "function"); jz_str_push_cstr(out, fns[v->function]);
    groove__fmt__DebugStruct__field(&d, "expression");
    if (v->has_expression) { jz_str_push_cstr(out, "Some("); groove__ivm__op_types__PlanExpr__Debug__fmt(out, &v->expression); jz_str_push_char(out, ')'); }
    else jz_str_push_cstr(out, "None");
    groove__fmt__DebugStruct__field(&d, "distinct"); groove__ivm__op_types__dbg_bool(out, v->distinct);
    groove__fmt__DebugStruct__field(&d, "output_name"); groove__ivm__op_types__dbg_opt_str(out, v->has_output_name, &v->output_name);
    groove__fmt__DebugStruct__field(&d, "output_identity");
    if (v->has_output_identity) { jz_str_push_cstr(out, "Some("); groove__records__FieldIdentity__Debug__fmt(out, &v->output_identity); jz_str_push_char(out, ')'); }
    else jz_str_push_cstr(out, "None");
    groove__fmt__DebugStruct__finish(&d);
}

void groove__ivm__op_types__AggregateOp__Clone__clone(groove_AggregateOp *dst, const groove_AggregateOp *src) {
    planvec_clone(&dst->group_key, &src->group_key);
    groove__ivm__usizevec_clone(&dst->group_field_indices, &src->group_field_indices);
    jz_vec_clone(&dst->aggregates, &src->aggregates, sizeof(groove_AggregateExpr), groove__ivm__op_types__agg_expr_clone_v);
}
void groove__ivm__op_types__AggregateOp__drop(groove_AggregateOp *v) {
    planvec_drop(&v->group_key); jz_vec_drop(&v->group_field_indices, 4, NULL);
    jz_vec_drop(&v->aggregates, sizeof(groove_AggregateExpr), groove__ivm__op_types__agg_expr_drop_v);
}
bool groove__ivm__op_types__AggregateOp__PartialEq__eq(const groove_AggregateOp *a, const groove_AggregateOp *b) {
    if (!planvec_eq(&a->group_key, &b->group_key) || !groove__ivm__usizevec_eq(&a->group_field_indices, &b->group_field_indices)) return false;
    if (a->aggregates.len != b->aggregates.len) return false;
    for (u32 i = 0; i < a->aggregates.len; i++)
        if (!groove__ivm__op_types__AggregateExpr__PartialEq__eq(&a->aggregates.ptr[i], &b->aggregates.ptr[i])) return false;
    return true;
}
void groove__ivm__op_types__AggregateOp__Hash__hash(const groove_AggregateOp *v, groove_Hasher *h) {
    planvec_hash(&v->group_key, h); groove__ivm__usizevec_hash(&v->group_field_indices, h);
    groove_hash_usize(h, v->aggregates.len);
    for (u32 i = 0; i < v->aggregates.len; i++) groove__ivm__op_types__AggregateExpr__Hash__hash(&v->aggregates.ptr[i], h);
}
void groove__ivm__op_types__AggregateOp__Debug__fmt(jz_str *out, const groove_AggregateOp *v) {
    groove_DebugStruct d = groove__fmt__debug_struct(out, "AggregateOp");
    groove__fmt__DebugStruct__field(&d, "group_key"); planvec_debug(out, &v->group_key);
    groove__fmt__DebugStruct__field(&d, "group_field_indices"); groove__fmt__debug_usize_list(out, v->group_field_indices.ptr, v->group_field_indices.len);
    groove__fmt__DebugStruct__field(&d, "aggregates");
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->aggregates.len; i++) { if (i) jz_str_push_cstr(out, ", "); groove__ivm__op_types__AggregateExpr__Debug__fmt(out, &v->aggregates.ptr[i]); }
    jz_str_push_char(out, ']');
    groove__fmt__DebugStruct__finish(&d);
}
