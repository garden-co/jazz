/* groove::records::values (crates/groove/src/records/values.rs)
 *
 * Logical record values, value types, and primitive encoders. Owns Value,
 * ValueType, enum schemas, and the recursive encode/decode routines for scalars,
 * tuples, arrays and nullable values. */
#include "jz_rt.h"
#include "values.h"
#include "mod.h"
#include "../large_values.h"

#define VT_SCALAR(t) groove__records__values__ValueType__scalar(t)

/* ===================================================================== errors
 * records::Error Display strings (thiserror). */

jz_err groove__records__Error__simple(u32 code) {
    switch (code) {
    case GROOVE_REC_E_InvalidOffset: return jz_err_new(code, "invalid offset");
    case GROOVE_REC_E_InvalidSchemaVersionHeader: return jz_err_new(code, "invalid schema-version header");
    case GROOVE_REC_E_InvalidEnumHeader: return jz_err_new(code, "invalid enum value header");
    case GROOVE_REC_E_NonCanonicalRecord: return jz_err_new(code, "nested record bytes are not canonical");
    case GROOVE_REC_E_InvalidUtf8: return jz_err_new(code, "invalid utf-8 string");
    case GROOVE_REC_E_InvalidF64NaN: return jz_err_new(code, "NaN is not a valid f64 record value");
    case GROOVE_REC_E_LengthOverflow: return jz_err_new(code, "encoded length exceeds u32::MAX");
    case GROOVE_REC_E_ProjectSourceDescriptorMismatch:
        return jz_err_new(code, "projection source record descriptor does not match projector source descriptor");
    case GROOVE_REC_E_UnexpectedEof: return jz_err_new(code, "unexpected end of record");
    default: JZ_UNREACHABLE();
    }
}

jz_err groove__records__Error__arity_mismatch(u32 expected, u32 actual) {
    return jz_err_fmt(GROOVE_REC_E_ArityMismatch, "expected %u values, got %u", expected, actual);
}

jz_err groove__records__Error__field_index_out_of_bounds(u32 index, u32 len) {
    return jz_err_fmt(GROOVE_REC_E_FieldIndexOutOfBounds, "field index %u out of bounds for descriptor length %u", index, len);
}

jz_err groove__records__Error__type_mismatch(const groove_ValueType *expected) {
    jz_str dbg = {0};
    groove__records__values__ValueType__Debug__fmt(&dbg, expected);
    jz_err e = jz_err_fmt(GROOVE_REC_E_TypeMismatch, "value does not match type %.*s", (int)dbg.len, dbg.ptr);
    jz_str_drop(&dbg);
    return e;
}

jz_err groove__records__Error__invalid_tuple_member(const groove_ValueType *member) {
    jz_str dbg = {0};
    groove__records__values__ValueType__Debug__fmt(&dbg, member);
    jz_err e = jz_err_fmt(GROOVE_REC_E_InvalidTupleMember, "tuple members must be fixed-width, got %.*s", (int)dbg.len, dbg.ptr);
    jz_str_drop(&dbg);
    return e;
}

static jz_err invalid_bool(u8 v) { return jz_err_fmt(GROOVE_REC_E_InvalidBool, "invalid boolean byte: %u", (unsigned)v); }
static jz_err invalid_null_flag(u8 v) { return jz_err_fmt(GROOVE_REC_E_InvalidNullFlag, "invalid nullable flag byte: %u", (unsigned)v); }

/* Error derives Clone + PartialEq: the message carries every field of the variant. */
jz_err groove__records__Error__clone(const jz_err *e) {
    jz_err out = {e->code, NULL};
    if (e->msg) {
        out.msg = JZ_NEW(jz_str);
        *out.msg = jz_str_clone(e->msg);
    }
    return out;
}

bool groove__records__Error__PartialEq__eq(const jz_err *a, const jz_err *b) {
    if (a->code != b->code) return false;
    if (!a->msg || !b->msg) return a->msg == b->msg;
    return jz_str_eq(a->msg, b->msg);
}

/* ===================================================================== helpers */
static inline void put_le16(u8 *p, u16 v) { p[0] = (u8)v; p[1] = (u8)(v >> 8); }
static inline void put_le32(u8 *p, u32 v) { for (int i = 0; i < 4; i++) p[i] = (u8)(v >> (8 * i)); }
static inline void put_le64(u8 *p, u64 v) { for (int i = 0; i < 8; i++) p[i] = (u8)(v >> (8 * i)); }
static inline void put_be16(u8 *p, u16 v) { p[0] = (u8)(v >> 8); p[1] = (u8)v; }
static inline void put_be32(u8 *p, u32 v) { for (int i = 0; i < 4; i++) p[i] = (u8)(v >> (8 * (3 - i))); }
static inline void put_be64(u8 *p, u64 v) { for (int i = 0; i < 8; i++) p[i] = (u8)(v >> (8 * (7 - i))); }
static inline u16 get_le16(const u8 *p) { return (u16)(p[0] | (p[1] << 8)); }
static inline u32 get_le32(const u8 *p) { u32 v = 0; for (int i = 3; i >= 0; i--) v = (v << 8) | p[i]; return v; }
static inline u64 get_le64(const u8 *p) { u64 v = 0; for (int i = 7; i >= 0; i--) v = (v << 8) | p[i]; return v; }
static inline u16 get_be16(const u8 *p) { return (u16)((p[0] << 8) | p[1]); }
static inline u32 get_be32(const u8 *p) { u32 v = 0; for (int i = 0; i < 4; i++) v = (v << 8) | p[i]; return v; }
static inline u64 get_be64(const u8 *p) { u64 v = 0; for (int i = 0; i < 8; i++) v = (v << 8) | p[i]; return v; }

static void bytes_resize_zero(jz_bytes *b, u32 len) {
    if (len > b->len) {
        jz_vec_reserve(b, len - b->len, 1);
        memset(b->ptr + b->len, 0, len - b->len);
    }
    b->len = len;
}

static jz_str str_from_bytes_move(jz_bytes b) { jz_str s = {(char *)b.ptr, b.len, b.cap}; return s; }

static void str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }
static void strvec_drop(groove_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), str_drop_v); }
static void strvec_clone(groove_StrVec *dst, const groove_StrVec *src) { jz_vec_clone(dst, src, sizeof(jz_str), str_clone_v); }
static bool strvec_eq(const groove_StrVec *a, const groove_StrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!jz_str_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
/* slice::starts_with for Vec<String> */
static bool strvec_starts_with(const groove_StrVec *a, const groove_StrVec *prefix) {
    if (prefix->len > a->len) return false;
    for (u32 i = 0; i < prefix->len; i++) if (!jz_str_eq(&a->ptr[i], &prefix->ptr[i])) return false;
    return true;
}
static u64 strvec_hash(const groove_StrVec *v, u64 h) {
    h = jz_hash_combine(h, v->len);
    for (u32 i = 0; i < v->len; i++) h = jz_hash_combine(h, jz_str_hash(&v->ptr[i]));
    return h;
}
static void debug_str(jz_str *out, const char *p, u32 n) {
    jz_str_push_char(out, '"');
    for (u32 i = 0; i < n; i++) {
        char c = p[i];
        if (c == '"' || c == '\\') { jz_str_push_char(out, '\\'); jz_str_push_char(out, c); }
        else if (c == '\n') jz_str_push_cstr(out, "\\n");
        else if (c == '\t') jz_str_push_cstr(out, "\\t");
        else if (c == '\r') jz_str_push_cstr(out, "\\r");
        else if ((u8)c < 0x20) jz_str_appendf(out, "\\u{%x}", (unsigned)(u8)c);
        else jz_str_push_char(out, c);
    }
    jz_str_push_char(out, '"');
}
static void debug_strvec(jz_str *out, const groove_StrVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        debug_str(out, v->ptr[i].ptr, v->ptr[i].len);
    }
    jz_str_push_char(out, ']');
}
static void debug_bytes(jz_str *out, const u8 *p, u32 n) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) jz_str_appendf(out, i ? ", %u" : "%u", (unsigned)p[i]);
    jz_str_push_char(out, ']');
}

/* ===================================================================== registry ids */

u64 groove__records__values__variant_registry_id_for_path(const char *p, u32 n) {
    u64 hash = 0xcbf29ce484222325ull;
    for (u32 i = 0; i < n; i++) {
        hash ^= (u64)(u8)p[i];
        hash *= 0x100000001b3ull;
    }
    hash &= ~GROOVE_SYSTEM_REGISTRY_MARKER;
    return hash == 0 ? 1 : hash;
}

groove_SystemVariantRegistry groove__records__values__SystemVariantRegistry__deletion_state(void) {
    static const char path[] = "jazz/internal/deletion";
    groove_SystemVariantRegistry r = {groove__records__values__variant_registry_id_for_path(path, sizeof(path) - 1) | GROOVE_SYSTEM_REGISTRY_MARKER};
    return r;
}

/* ===================================================================== EnumValue */

groove_EnumValue groove__records__values__EnumValue__new(u32 tag, groove_OwnedRecord record) {
    groove_EnumValue v = {tag, record};
    return v;
}

jz_err groove__records__values__EnumValue__create(u32 tag, groove_RecordDescriptor descriptor, const groove_Value *values, u32 n, groove_EnumValue *out) {
    jz_bytes raw = {0};
    JZ_TRY(groove__records__RecordDescriptor__create(descriptor, values, n, &raw));
    *out = groove__records__values__EnumValue__new(tag, groove__records__OwnedRecord__new(raw, descriptor));
    return JZ_OK;
}

u64 groove__records__values__EnumValue__Hash__hash(const groove_EnumValue *v, u64 h) {
    h = jz_hash_combine(h, v->tag);
    return groove__records__OwnedRecord__Hash__hash(&v->record, h);
}

/* ===================================================================== From<T> for Value */

groove_Value groove__records__values__Value__from_u64(u64 v) { groove_Value r = {.tag = GROOVE_V_U64}; r.u.u64_ = v; return r; }
groove_Value groove__records__values__Value__from_str(const char *p, u32 n) {
    groove_Value r = {.tag = GROOVE_V_STRING};
    r.u.s = jz_str_from_slice(jz_slice_of(p, n));
    return r;
}
groove_Value groove__records__values__Value__from_bytes(const u8 *p, u32 n) {
    groove_Value r = {.tag = GROOVE_V_BYTES};
    r.u.bytes = jz_bytes_from(p, n);
    return r;
}
groove_Value groove__records__values__Value__from_option(groove_Value *some_or_null) {
    groove_Value r = {.tag = GROOVE_V_NULLABLE};
    if (some_or_null) {
        r.u.nullable = JZ_NEW(groove_Value);
        *r.u.nullable = *some_or_null;
    }
    return r;
}

/* ===================================================================== ScalarEnumSchema */

static jz_err scalar_enum_validate_variants(const jz_str *name, const groove_StrVec *variants) {
    if (variants->len > 256) {
        return jz_err_fmt(GROOVE_REC_E_EnumTooManyVariants, "enum %.*s has %u variants; maximum is 256",
                          (int)name->len, name->ptr, variants->len);
    }
    for (u32 index = 0; index < variants->len; index++) {
        for (u32 j = 0; j < index; j++) {
            if (jz_str_eq(&variants->ptr[j], &variants->ptr[index])) {
                const jz_str *v = &variants->ptr[index];
                return jz_err_fmt(GROOVE_REC_E_DuplicateEnumVariantName, "duplicate variant %.*s in enum %.*s",
                                  (int)v->len, v->ptr, (int)name->len, name->ptr);
            }
        }
    }
    return JZ_OK;
}

jz_err groove__records__values__ScalarEnumSchema__from_parts(u64 registry_id, jz_str name, groove_StrVec variants, groove_ScalarEnumSchema *out) {
    jz_err e = scalar_enum_validate_variants(&name, &variants);
    if (e.code) {
        jz_str_drop(&name);
        strvec_drop(&variants);
        return e;
    }
    out->registry_id = registry_id;
    out->name = name;
    out->variants = variants;
    return JZ_OK;
}

jz_err groove__records__values__ScalarEnumSchema__new(jz_str name, groove_StrVec variants, groove_ScalarEnumSchema *out) {
    return groove__records__values__ScalarEnumSchema__from_parts(0, name, variants, out);
}

static jz_err scalar_enum_validate(const groove_ScalarEnumSchema *s) { return scalar_enum_validate_variants(&s->name, &s->variants); }

void groove__records__values__ScalarEnumSchema__with_registry_id(groove_ScalarEnumSchema *s, u64 registry_id) {
    s->registry_id = registry_id & ~GROOVE_SYSTEM_REGISTRY_MARKER;
}

void groove__records__values__ScalarEnumSchema__with_system_registry(groove_ScalarEnumSchema *s, groove_SystemVariantRegistry r) {
    s->registry_id = r.id;
}

jz_err groove__records__values__ScalarEnumSchema__discriminant(const groove_ScalarEnumSchema *s, const char *p, u32 n, u8 *out) {
    for (u32 idx = 0; idx < s->variants.len; idx++) {
        const jz_str *c = &s->variants.ptr[idx];
        if (c->len == n && memcmp(c->ptr, p, n) == 0) {
            if (idx <= 0xff) { *out = (u8)idx; return JZ_OK; }
            break;
        }
    }
    return jz_err_fmt(GROOVE_REC_E_UnknownEnumVariant, "unknown enum variant %.*s for enum %.*s",
                      (int)n, p, (int)s->name.len, s->name.ptr);
}

jz_err groove__records__values__ScalarEnumSchema__variant(const groove_ScalarEnumSchema *s, u8 discriminant, const jz_str **out) {
    if ((u32)discriminant < s->variants.len) {
        *out = &s->variants.ptr[discriminant];
        return JZ_OK;
    }
    return jz_err_fmt(GROOVE_REC_E_InvalidEnumDiscriminant, "invalid enum discriminant %u for enum %.*s",
                      (unsigned)discriminant, (int)s->name.len, s->name.ptr);
}

void groove__records__values__ScalarEnumSchema__drop(groove_ScalarEnumSchema *s) {
    jz_str_drop(&s->name);
    strvec_drop(&s->variants);
}

void groove__records__values__ScalarEnumSchema__Clone__clone(groove_ScalarEnumSchema *dst, const groove_ScalarEnumSchema *src) {
    dst->registry_id = src->registry_id;
    dst->name = jz_str_clone(&src->name);
    strvec_clone(&dst->variants, &src->variants);
}

bool groove__records__values__ScalarEnumSchema__PartialEq__eq(const groove_ScalarEnumSchema *a, const groove_ScalarEnumSchema *b) {
    return a->registry_id == b->registry_id && jz_str_eq(&a->name, &b->name) && strvec_eq(&a->variants, &b->variants);
}

static u64 scalar_enum_hash(const groove_ScalarEnumSchema *s, u64 h) {
    h = jz_hash_combine(h, s->registry_id);
    h = jz_hash_combine(h, jz_str_hash(&s->name));
    return strvec_hash(&s->variants, h);
}

static void scalar_enum_debug(jz_str *out, const groove_ScalarEnumSchema *s) {
    jz_str_appendf(out, "ScalarEnumSchema { registry_id: %llu, name: ", (unsigned long long)s->registry_id);
    debug_str(out, s->name.ptr, s->name.len);
    jz_str_push_cstr(out, ", variants: ");
    debug_strvec(out, &s->variants);
    jz_str_push_cstr(out, " }");
}

/* ===================================================================== EnumSchema */

groove_EnumCase groove__records__values__EnumCase__new(jz_str name, groove_RecordDescriptor payload) {
    groove_EnumCase c = {name, payload};
    return c;
}

static void enum_case_drop_v(void *p) { jz_str_drop(&((groove_EnumCase *)p)->name); }
static void enum_case_clone_v(void *dst, const void *src) {
    const groove_EnumCase *s = src;
    groove_EnumCase *d = dst;
    d->name = jz_str_clone(&s->name);
    d->payload = s->payload;
}

static jz_err enum_validate_cases(const jz_str *name, const groove_EnumCaseVec *cases) {
    /* `u32::try_from(cases.len() - 1)`: always fits on wasm32 where usize is u32. */
    if (cases->len != 0 && (u64)(cases->len - 1) > 0xffffffffull) {
        return jz_err_fmt(GROOVE_REC_E_EnumTooManyCases, "enum %.*s has %u cases; maximum is u32::MAX + 1",
                          (int)name->len, name->ptr, cases->len);
    }
    for (u32 index = 0; index < cases->len; index++) {
        for (u32 j = 0; j < index; j++) {
            if (jz_str_eq(&cases->ptr[j].name, &cases->ptr[index].name)) {
                const jz_str *c = &cases->ptr[index].name;
                return jz_err_fmt(GROOVE_REC_E_DuplicateEnumCaseName, "duplicate case %.*s in enum %.*s",
                                  (int)c->len, c->ptr, (int)name->len, name->ptr);
            }
        }
    }
    return JZ_OK;
}

jz_err groove__records__values__EnumSchema__new(jz_str name, groove_EnumCaseVec cases, groove_EnumSchema *out) {
    jz_err e = enum_validate_cases(&name, &cases);
    if (e.code) {
        jz_str_drop(&name);
        jz_vec_drop(&cases, sizeof(groove_EnumCase), enum_case_drop_v);
        return e;
    }
    out->registry_id = 0;
    out->name = name;
    out->cases = cases;
    return JZ_OK;
}

void groove__records__values__EnumSchema__with_registry_id(groove_EnumSchema *s, u64 registry_id) { s->registry_id = registry_id; }

static jz_err enum_validate(const groove_EnumSchema *s) { return enum_validate_cases(&s->name, &s->cases); }

jz_err groove__records__values__EnumSchema__case(const groove_EnumSchema *s, u32 tag, const groove_EnumCase **out) {
    if (tag < s->cases.len) {
        *out = &s->cases.ptr[tag];
        return JZ_OK;
    }
    return jz_err_fmt(GROOVE_REC_E_UnknownEnumTag, "unknown tag %u in enum %.*s", tag, (int)s->name.len, s->name.ptr);
}

jz_err groove__records__values__EnumSchema__tag(const groove_EnumSchema *s, const char *p, u32 n, u32 *out) {
    for (u32 i = 0; i < s->cases.len; i++) {
        const jz_str *c = &s->cases.ptr[i].name;
        if (c->len == n && memcmp(c->ptr, p, n) == 0) { *out = i; return JZ_OK; }
    }
    return jz_err_fmt(GROOVE_REC_E_UnknownEnumCase, "unknown case %.*s in enum %.*s", (int)n, p, (int)s->name.len, s->name.ptr);
}

void groove__records__values__EnumSchema__drop(groove_EnumSchema *s) {
    jz_str_drop(&s->name);
    jz_vec_drop(&s->cases, sizeof(groove_EnumCase), enum_case_drop_v);
}

void groove__records__values__EnumSchema__Clone__clone(groove_EnumSchema *dst, const groove_EnumSchema *src) {
    dst->registry_id = src->registry_id;
    dst->name = jz_str_clone(&src->name);
    jz_vec_clone(&dst->cases, &src->cases, sizeof(groove_EnumCase), enum_case_clone_v);
}

bool groove__records__values__EnumSchema__PartialEq__eq(const groove_EnumSchema *a, const groove_EnumSchema *b) {
    if (a->registry_id != b->registry_id || !jz_str_eq(&a->name, &b->name) || a->cases.len != b->cases.len) return false;
    for (u32 i = 0; i < a->cases.len; i++) {
        if (!jz_str_eq(&a->cases.ptr[i].name, &b->cases.ptr[i].name)) return false;
        if (a->cases.ptr[i].payload != b->cases.ptr[i].payload) return false;
    }
    return true;
}

static u64 enum_schema_hash(const groove_EnumSchema *s, u64 h) {
    h = jz_hash_combine(h, s->registry_id);
    h = jz_hash_combine(h, jz_str_hash(&s->name));
    h = jz_hash_combine(h, s->cases.len);
    for (u32 i = 0; i < s->cases.len; i++) {
        h = jz_hash_combine(h, jz_str_hash(&s->cases.ptr[i].name));
        h = groove__records__RecordDescriptor__Hash__hash(s->cases.ptr[i].payload, h);
    }
    return h;
}

static void enum_schema_debug(jz_str *out, const groove_EnumSchema *s) {
    jz_str_appendf(out, "EnumSchema { registry_id: %llu, name: ", (unsigned long long)s->registry_id);
    debug_str(out, s->name.ptr, s->name.len);
    jz_str_push_cstr(out, ", cases: [");
    for (u32 i = 0; i < s->cases.len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jz_str_push_cstr(out, "EnumCase { name: ");
        debug_str(out, s->cases.ptr[i].name.ptr, s->cases.ptr[i].name.len);
        jz_str_push_cstr(out, ", payload: ");
        groove__records__RecordDescriptor__Debug__fmt(out, s->cases.ptr[i].payload);
        jz_str_push_cstr(out, " }");
    }
    jz_str_push_cstr(out, "] }");
}

/* ===================================================================== VariantRegistry */

void groove__records__values__VariantRegistry__drop(groove_VariantRegistry *r) { strvec_drop(&r->names); }
void groove__records__values__VariantRegistry__drop_v(void *r) { groove__records__values__VariantRegistry__drop(r); }
void groove__records__values__VariantRegistry__Clone__clone(groove_VariantRegistry *dst, const groove_VariantRegistry *src) {
    dst->tag = src->tag;
    strvec_clone(&dst->names, &src->names);
}
bool groove__records__values__VariantRegistry__PartialEq__eq(const groove_VariantRegistry *a, const groove_VariantRegistry *b) {
    return a->tag == b->tag && strvec_eq(&a->names, &b->names);
}
u64 groove__records__values__VariantRegistry__Hash__hash(const groove_VariantRegistry *r, u64 h) {
    return strvec_hash(&r->names, jz_hash_combine(h, r->tag));
}
void groove__records__values__VariantRegistry__Debug__fmt(jz_str *out, const groove_VariantRegistry *r) {
    jz_str_push_cstr(out, r->tag == GROOVE_VR_ENUM_TAG ? "EnumTag { variants: " : "Enum { cases: ");
    debug_strvec(out, &r->names);
    jz_str_push_cstr(out, " }");
}
void groove__records__values__VariantRegistry__Serialize__serialize(jz_pc_w *w, const groove_VariantRegistry *r) {
    jz_pc_variant(w, r->tag);
    jz_pc_seq(w, r->names.len);
    for (u32 i = 0; i < r->names.len; i++) jz_pc_str(w, &r->names.ptr[i]);
}
static jz_err pc_read_strvec(jz_pc_r *r, groove_StrVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < n; i++) {
        jz_str s = {0};
        jz_err e = jz_pc_r_str(r, &s);
        if (e.code) { strvec_drop(out); return e; }
        jz_vec_push(out, &s, sizeof(s));
    }
    return JZ_OK;
}
jz_err groove__records__values__VariantRegistry__Deserialize__deserialize(jz_pc_r *r, groove_VariantRegistry *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_variant(r, &idx, 2));
    out->tag = (u8)idx;
    return pc_read_strvec(r, &out->names);
}

const jz_btree_desc groove__records__values__VARIANT_REGISTRY_MAP = {
    sizeof(u64), sizeof(groove_VariantRegistry), jz_cmp_u64, NULL, groove__records__values__VariantRegistry__drop_v,
};

/* ===================================================================== ValueType derives */

groove_ValueType groove__records__values__ValueType__scalar(u8 tag) {
    groove_ValueType t;
    memset(&t, 0, sizeof(t));
    t.tag = tag;
    return t;
}

void groove__records__values__ValueType__drop(groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_ENUM_TAG: groove__records__values__ScalarEnumSchema__drop(&t->u.enum_tag); break;
    case GROOVE_VT_TUPLE: jz_vec_drop(&t->u.tuple, sizeof(groove_ValueType), groove__records__values__ValueType__drop_v); break;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE:
        groove__records__values__ValueType__drop(t->u.inner);
        jz_free(t->u.inner);
        break;
    case GROOVE_VT_ENUM:
        groove__records__values__EnumSchema__drop(t->u.enum_);
        jz_free(t->u.enum_);
        break;
    default: break;
    }
    t->tag = GROOVE_VT_U8;
}
void groove__records__values__ValueType__drop_v(void *t) { groove__records__values__ValueType__drop(t); }

void groove__records__values__ValueType__Clone__clone(groove_ValueType *dst, const groove_ValueType *src) {
    memset(dst, 0, sizeof(*dst));
    dst->tag = src->tag;
    switch (src->tag) {
    case GROOVE_VT_INTERNAL: dst->u.internal = src->u.internal; break;
    case GROOVE_VT_ENUM_TAG: groove__records__values__ScalarEnumSchema__Clone__clone(&dst->u.enum_tag, &src->u.enum_tag); break;
    case GROOVE_VT_TUPLE: jz_vec_clone(&dst->u.tuple, &src->u.tuple, sizeof(groove_ValueType), groove__records__values__ValueType__Clone__clone_v); break;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE:
        dst->u.inner = JZ_NEW(groove_ValueType);
        groove__records__values__ValueType__Clone__clone(dst->u.inner, src->u.inner);
        break;
    case GROOVE_VT_RECORD: dst->u.record = src->u.record; break;
    case GROOVE_VT_ENUM:
        dst->u.enum_ = JZ_NEW(groove_EnumSchema);
        groove__records__values__EnumSchema__Clone__clone(dst->u.enum_, src->u.enum_);
        break;
    default: break;
    }
}
void groove__records__values__ValueType__Clone__clone_v(void *dst, const void *src) { groove__records__values__ValueType__Clone__clone(dst, src); }

bool groove__records__values__ValueType__PartialEq__eq(const groove_ValueType *a, const groove_ValueType *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_VT_INTERNAL:
        return a->u.internal.repr == b->u.internal.repr &&
               (a->u.internal.repr != GROOVE_IVT_STORED_SCALAR || a->u.internal.kind == b->u.internal.kind);
    case GROOVE_VT_ENUM_TAG: return groove__records__values__ScalarEnumSchema__PartialEq__eq(&a->u.enum_tag, &b->u.enum_tag);
    case GROOVE_VT_TUPLE:
        if (a->u.tuple.len != b->u.tuple.len) return false;
        for (u32 i = 0; i < a->u.tuple.len; i++)
            if (!groove__records__values__ValueType__PartialEq__eq(&a->u.tuple.ptr[i], &b->u.tuple.ptr[i])) return false;
        return true;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: return groove__records__values__ValueType__PartialEq__eq(a->u.inner, b->u.inner);
    case GROOVE_VT_RECORD: return a->u.record == b->u.record;
    case GROOVE_VT_ENUM: return groove__records__values__EnumSchema__PartialEq__eq(a->u.enum_, b->u.enum_);
    default: return true;
    }
}

u64 groove__records__values__ValueType__Hash__hash(const groove_ValueType *t, u64 h) {
    h = jz_hash_combine(h, t->tag);
    switch (t->tag) {
    case GROOVE_VT_INTERNAL:
        h = jz_hash_combine(h, t->u.internal.repr);
        if (t->u.internal.repr == GROOVE_IVT_STORED_SCALAR) h = jz_hash_combine(h, t->u.internal.kind);
        return h;
    case GROOVE_VT_ENUM_TAG: return scalar_enum_hash(&t->u.enum_tag, h);
    case GROOVE_VT_TUPLE:
        h = jz_hash_combine(h, t->u.tuple.len);
        for (u32 i = 0; i < t->u.tuple.len; i++) h = groove__records__values__ValueType__Hash__hash(&t->u.tuple.ptr[i], h);
        return h;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: return groove__records__values__ValueType__Hash__hash(t->u.inner, h);
    case GROOVE_VT_RECORD: return groove__records__RecordDescriptor__Hash__hash(t->u.record, h);
    case GROOVE_VT_ENUM: return enum_schema_hash(t->u.enum_, h);
    default: return h;
    }
}

static const char *const VALUE_TYPE_NAMES[GROOVE_VT_COUNT] = {
    "U8", "U16", "U32", "U64", "I32", "I64", "F64", "Bool", "String", "Bytes", "Internal",
    "Uuid", "EnumTag", "Tuple", "Array", "Nullable", "Record", "Enum", "U48",
};
static const char *const LARGE_VALUE_KIND_NAMES[3] = {"Bytes", "String", "Json"};
static const char *const INTERNAL_REPR_NAMES[3] = {"RawString", "RawBytes", "StoredScalar"};

void groove__records__values__ValueType__Debug__fmt(jz_str *out, const groove_ValueType *t) {
    jz_str_push_cstr(out, VALUE_TYPE_NAMES[t->tag]);
    switch (t->tag) {
    case GROOVE_VT_INTERNAL:
        jz_str_push_cstr(out, "(InternalValueType(");
        jz_str_push_cstr(out, INTERNAL_REPR_NAMES[t->u.internal.repr]);
        if (t->u.internal.repr == GROOVE_IVT_STORED_SCALAR) jz_str_appendf(out, "(%s)", LARGE_VALUE_KIND_NAMES[t->u.internal.kind]);
        jz_str_push_cstr(out, "))");
        break;
    case GROOVE_VT_ENUM_TAG:
        jz_str_push_char(out, '(');
        scalar_enum_debug(out, &t->u.enum_tag);
        jz_str_push_char(out, ')');
        break;
    case GROOVE_VT_TUPLE:
        jz_str_push_cstr(out, "([");
        for (u32 i = 0; i < t->u.tuple.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            groove__records__values__ValueType__Debug__fmt(out, &t->u.tuple.ptr[i]);
        }
        jz_str_push_cstr(out, "])");
        break;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE:
        jz_str_push_char(out, '(');
        groove__records__values__ValueType__Debug__fmt(out, t->u.inner);
        jz_str_push_char(out, ')');
        break;
    case GROOVE_VT_RECORD:
        jz_str_push_char(out, '(');
        groove__records__RecordDescriptor__Debug__fmt(out, t->u.record);
        jz_str_push_char(out, ')');
        break;
    case GROOVE_VT_ENUM:
        jz_str_push_char(out, '(');
        enum_schema_debug(out, t->u.enum_);
        jz_str_push_char(out, ')');
        break;
    default: break;
    }
}

/* ===================================================================== Value derives */

void groove__records__values__ValueVec__drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove__records__values__Value__drop_v); }

void groove__records__values__Value__drop(groove_Value *v) {
    switch (v->tag) {
    case GROOVE_V_STRING: jz_str_drop(&v->u.s); break;
    case GROOVE_V_BYTES: jz_bytes_drop(&v->u.bytes); break;
    case GROOVE_V_LARGE:
        groove__large_values__LargeValueRef__drop(v->u.large);
        jz_free(v->u.large);
        break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY: groove__records__values__ValueVec__drop(&v->u.items); break;
    case GROOVE_V_NULLABLE:
        if (v->u.nullable) {
            groove__records__values__Value__drop(v->u.nullable);
            jz_free(v->u.nullable);
        }
        break;
    case GROOVE_V_RECORD: groove__records__OwnedRecord__drop(&v->u.record); break;
    case GROOVE_V_ENUM: groove__records__OwnedRecord__drop(&v->u.enum_.record); break;
    default: break;
    }
    v->tag = GROOVE_V_U8;
}
void groove__records__values__Value__drop_v(void *v) { groove__records__values__Value__drop(v); }

void groove__records__values__Value__Clone__clone(groove_Value *dst, const groove_Value *src) {
    *dst = *src;
    switch (src->tag) {
    case GROOVE_V_STRING: dst->u.s = jz_str_clone(&src->u.s); break;
    case GROOVE_V_BYTES: dst->u.bytes = jz_bytes_clone(&src->u.bytes); break;
    case GROOVE_V_LARGE:
        dst->u.large = JZ_NEW(groove_LargeValueRef);
        groove__large_values__LargeValueRef__Clone__clone(dst->u.large, src->u.large);
        break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY:
        jz_vec_clone(&dst->u.items, &src->u.items, sizeof(groove_Value), groove__records__values__Value__Clone__clone_v);
        break;
    case GROOVE_V_NULLABLE:
        if (src->u.nullable) {
            dst->u.nullable = JZ_NEW(groove_Value);
            groove__records__values__Value__Clone__clone(dst->u.nullable, src->u.nullable);
        }
        break;
    case GROOVE_V_RECORD: groove__records__OwnedRecord__Clone__clone(&dst->u.record, &src->u.record); break;
    case GROOVE_V_ENUM:
        dst->u.enum_.tag = src->u.enum_.tag;
        groove__records__OwnedRecord__Clone__clone(&dst->u.enum_.record, &src->u.enum_.record);
        break;
    default: break;
    }
}
void groove__records__values__Value__Clone__clone_v(void *dst, const void *src) { groove__records__values__Value__Clone__clone(dst, src); }

static bool value_vec_eq(const groove_ValueVec *a, const groove_ValueVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!groove__records__values__Value__PartialEq__eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

bool groove__records__values__Value__PartialEq__eq(const groove_Value *a, const groove_Value *b) {
    if (a->tag != b->tag) return false;
    switch (a->tag) {
    case GROOVE_V_U8: return a->u.u8_ == b->u.u8_;
    case GROOVE_V_U16: return a->u.u16_ == b->u.u16_;
    case GROOVE_V_U32: return a->u.u32_ == b->u.u32_;
    case GROOVE_V_U64:
    case GROOVE_V_U48: return a->u.u64_ == b->u.u64_;
    case GROOVE_V_F64: return a->u.f64_ == b->u.f64_;
    case GROOVE_V_BOOL: return a->u.b == b->u.b;
    case GROOVE_V_STRING: return jz_str_eq(&a->u.s, &b->u.s);
    case GROOVE_V_BYTES: return jz_bytes_eq(&a->u.bytes, &b->u.bytes);
    case GROOVE_V_LARGE: return groove__large_values__LargeValueRef__PartialEq__eq(a->u.large, b->u.large);
    case GROOVE_V_UUID: return jz_uuid_eq(&a->u.uuid, &b->u.uuid);
    case GROOVE_V_ENUM_TAG: return a->u.enum_tag == b->u.enum_tag;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY: return value_vec_eq(&a->u.items, &b->u.items);
    case GROOVE_V_NULLABLE:
        if (!a->u.nullable || !b->u.nullable) return a->u.nullable == b->u.nullable;
        return groove__records__values__Value__PartialEq__eq(a->u.nullable, b->u.nullable);
    case GROOVE_V_I64: return a->u.i64_ == b->u.i64_;
    case GROOVE_V_I32: return a->u.i32_ == b->u.i32_;
    case GROOVE_V_RECORD: return groove__records__OwnedRecord__PartialEq__eq(&a->u.record, &b->u.record);
    case GROOVE_V_ENUM:
        return a->u.enum_.tag == b->u.enum_.tag && groove__records__OwnedRecord__PartialEq__eq(&a->u.enum_.record, &b->u.enum_.record);
    default: return false;
    }
}

static const char *const VALUE_NAMES[GROOVE_V_COUNT] = {
    "U8", "U16", "U32", "U64", "F64", "Bool", "String", "Bytes", "Large", "Uuid", "EnumTag",
    "Tuple", "Array", "Nullable", "I64", "I32", "Record", "Enum", "U48",
};

void groove__records__values__Value__Debug__fmt(jz_str *out, const groove_Value *v) {
    jz_str_push_cstr(out, VALUE_NAMES[v->tag]);
    jz_str_push_char(out, '(');
    switch (v->tag) {
    case GROOVE_V_U8: jz_str_appendf(out, "%u", (unsigned)v->u.u8_); break;
    case GROOVE_V_U16: jz_str_appendf(out, "%u", (unsigned)v->u.u16_); break;
    case GROOVE_V_U32: jz_str_appendf(out, "%u", v->u.u32_); break;
    case GROOVE_V_U64:
    case GROOVE_V_U48: jz_str_appendf(out, "%llu", (unsigned long long)v->u.u64_); break;
    case GROOVE_V_F64: jz_str_appendf(out, "%g", v->u.f64_); break;
    case GROOVE_V_BOOL: jz_str_push_cstr(out, v->u.b ? "true" : "false"); break;
    case GROOVE_V_STRING: debug_str(out, v->u.s.ptr, v->u.s.len); break;
    case GROOVE_V_BYTES: debug_bytes(out, v->u.bytes.ptr, v->u.bytes.len); break;
    case GROOVE_V_LARGE: groove__large_values__LargeValueRef__Debug__fmt(out, v->u.large); break;
    case GROOVE_V_UUID: {
        char buf[37];
        jz_uuid_to_string(&v->u.uuid, buf);
        jz_str_push(out, buf, 36);
        break;
    }
    case GROOVE_V_ENUM_TAG: jz_str_appendf(out, "%u", (unsigned)v->u.enum_tag); break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY:
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < v->u.items.len; i++) {
            if (i) jz_str_push_cstr(out, ", ");
            groove__records__values__Value__Debug__fmt(out, &v->u.items.ptr[i]);
        }
        jz_str_push_char(out, ']');
        break;
    case GROOVE_V_NULLABLE:
        if (v->u.nullable) {
            jz_str_push_cstr(out, "Some(");
            groove__records__values__Value__Debug__fmt(out, v->u.nullable);
            jz_str_push_char(out, ')');
        } else {
            jz_str_push_cstr(out, "None");
        }
        break;
    case GROOVE_V_I64: jz_str_appendf(out, "%lld", (long long)v->u.i64_); break;
    case GROOVE_V_I32: jz_str_appendf(out, "%d", v->u.i32_); break;
    case GROOVE_V_RECORD: groove__records__OwnedRecord__Debug__fmt(out, &v->u.record); break;
    case GROOVE_V_ENUM:
        jz_str_appendf(out, "EnumValue { tag: %u, record: ", v->u.enum_.tag);
        groove__records__OwnedRecord__Debug__fmt(out, &v->u.enum_.record);
        jz_str_push_cstr(out, " }");
        break;
    default: break;
    }
    jz_str_push_char(out, ')');
}

/* ===================================================================== serde: postcard */

void groove__records__values__Value__Serialize__serialize(jz_pc_w *w, const groove_Value *v) {
    jz_pc_variant(w, v->tag);
    switch (v->tag) {
    case GROOVE_V_U8: jz_pc_u8(w, v->u.u8_); break;
    case GROOVE_V_U16: jz_pc_varint(w, v->u.u16_); break;
    case GROOVE_V_U32: jz_pc_varint(w, v->u.u32_); break;
    case GROOVE_V_U64:
    case GROOVE_V_U48: jz_pc_varint(w, v->u.u64_); break;
    case GROOVE_V_F64: jz_pc_f64(w, v->u.f64_); break;
    case GROOVE_V_BOOL: jz_pc_bool(w, v->u.b); break;
    case GROOVE_V_STRING: jz_pc_str(w, &v->u.s); break;
    case GROOVE_V_BYTES: jz_pc_bytes(w, v->u.bytes.ptr, v->u.bytes.len); break;
    case GROOVE_V_LARGE: groove__large_values__LargeValueRef__Serialize__serialize(w, v->u.large); break;
    case GROOVE_V_UUID: jz_pc_uuid(w, &v->u.uuid); break;
    case GROOVE_V_ENUM_TAG: jz_pc_u8(w, v->u.enum_tag); break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY:
        jz_pc_seq(w, v->u.items.len);
        for (u32 i = 0; i < v->u.items.len; i++) groove__records__values__Value__Serialize__serialize(w, &v->u.items.ptr[i]);
        break;
    case GROOVE_V_NULLABLE:
        jz_pc_option(w, v->u.nullable != NULL);
        if (v->u.nullable) groove__records__values__Value__Serialize__serialize(w, v->u.nullable);
        break;
    case GROOVE_V_I64: jz_pc_zigzag(w, v->u.i64_); break;
    case GROOVE_V_I32: jz_pc_zigzag(w, v->u.i32_); break;
    case GROOVE_V_RECORD: groove__records__OwnedRecord__Serialize__serialize(w, &v->u.record); break;
    case GROOVE_V_ENUM:
        jz_pc_varint(w, v->u.enum_.tag);
        groove__records__OwnedRecord__Serialize__serialize(w, &v->u.enum_.record);
        break;
    default: break;
    }
}

static jz_err pc_read_value_vec(jz_pc_r *r, groove_ValueVec *out) {
    u32 n;
    JZ_TRY(jz_pc_r_seq(r, &n));
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < n; i++) {
        groove_Value item;
        jz_err e = groove__records__values__Value__Deserialize__deserialize(r, &item);
        if (e.code) { groove__records__values__ValueVec__drop(out); return e; }
        jz_vec_push(out, &item, sizeof(item));
    }
    return JZ_OK;
}

jz_err groove__records__values__Value__Deserialize__deserialize(jz_pc_r *r, groove_Value *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_enter(r));
    jz_err e = jz_pc_r_variant(r, &idx, GROOVE_V_COUNT);
    if (e.code) goto done;
    memset(out, 0, sizeof(*out));
    out->tag = (u8)idx;
    u64 v64;
    i64 s64;
    switch (idx) {
    case GROOVE_V_U8: e = jz_pc_r_u8(r, &out->u.u8_); break;
    case GROOVE_V_U16: e = jz_pc_r_u16(r, &out->u.u16_); break;
    case GROOVE_V_U32: e = jz_pc_r_u32(r, &out->u.u32_); break;
    case GROOVE_V_U64:
    case GROOVE_V_U48: e = jz_pc_r_varint(r, &v64); out->u.u64_ = v64; break;
    case GROOVE_V_F64: e = jz_pc_r_f64(r, &out->u.f64_); break;
    case GROOVE_V_BOOL: e = jz_pc_r_bool(r, &out->u.b); break;
    case GROOVE_V_STRING: e = jz_pc_r_str(r, &out->u.s); break;
    case GROOVE_V_BYTES: e = jz_pc_r_bytes(r, &out->u.bytes); break;
    case GROOVE_V_LARGE:
        out->u.large = JZ_NEW(groove_LargeValueRef);
        e = groove__large_values__LargeValueRef__Deserialize__deserialize(r, out->u.large);
        if (e.code) { jz_free(out->u.large); out->tag = GROOVE_V_U8; }
        break;
    case GROOVE_V_UUID: e = jz_pc_r_uuid(r, &out->u.uuid); break;
    case GROOVE_V_ENUM_TAG: e = jz_pc_r_u8(r, &out->u.enum_tag); break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY: e = pc_read_value_vec(r, &out->u.items); break;
    case GROOVE_V_NULLABLE: {
        bool some;
        e = jz_pc_r_option(r, &some);
        if (!e.code && some) {
            out->u.nullable = JZ_NEW(groove_Value);
            e = groove__records__values__Value__Deserialize__deserialize(r, out->u.nullable);
            if (e.code) { jz_free(out->u.nullable); out->u.nullable = NULL; }
        }
        break;
    }
    case GROOVE_V_I64: e = jz_pc_r_zigzag(r, &s64); out->u.i64_ = s64; break;
    case GROOVE_V_I32:
        e = jz_pc_r_zigzag(r, &s64);
        if (!e.code && (s64 < INT32_MIN || s64 > INT32_MAX)) e = jz_err_new(JZ_PC_BAD_VARINT, "invalid value: integer, expected i32");
        out->u.i32_ = (i32)s64;
        break;
    case GROOVE_V_RECORD: e = groove__records__OwnedRecord__Deserialize__deserialize(r, &out->u.record); break;
    case GROOVE_V_ENUM:
        e = jz_pc_r_u32(r, &out->u.enum_.tag);
        if (!e.code) e = groove__records__OwnedRecord__Deserialize__deserialize(r, &out->u.enum_.record);
        break;
    default: break;
    }
done:
    jz_pc_r_leave(r);
    return e;
}

static void pc_write_scalar_enum(jz_pc_w *w, const groove_ScalarEnumSchema *s) {
    jz_pc_varint(w, s->registry_id);
    jz_pc_str(w, &s->name);
    jz_pc_seq(w, s->variants.len);
    for (u32 i = 0; i < s->variants.len; i++) jz_pc_str(w, &s->variants.ptr[i]);
}

static void pc_write_enum_schema(jz_pc_w *w, const groove_EnumSchema *s) {
    jz_pc_varint(w, s->registry_id);
    jz_pc_str(w, &s->name);
    jz_pc_seq(w, s->cases.len);
    for (u32 i = 0; i < s->cases.len; i++) {
        jz_pc_str(w, &s->cases.ptr[i].name);
        groove__records__RecordDescriptor__Serialize__serialize(w, s->cases.ptr[i].payload);
    }
}

void groove__records__values__ValueType__Serialize__serialize(jz_pc_w *w, const groove_ValueType *t) {
    jz_pc_variant(w, t->tag);
    switch (t->tag) {
    case GROOVE_VT_INTERNAL:
        jz_pc_variant(w, t->u.internal.repr);
        if (t->u.internal.repr == GROOVE_IVT_STORED_SCALAR) jz_pc_variant(w, t->u.internal.kind);
        break;
    case GROOVE_VT_ENUM_TAG: pc_write_scalar_enum(w, &t->u.enum_tag); break;
    case GROOVE_VT_TUPLE:
        jz_pc_seq(w, t->u.tuple.len);
        for (u32 i = 0; i < t->u.tuple.len; i++) groove__records__values__ValueType__Serialize__serialize(w, &t->u.tuple.ptr[i]);
        break;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: groove__records__values__ValueType__Serialize__serialize(w, t->u.inner); break;
    case GROOVE_VT_RECORD: groove__records__RecordDescriptor__Serialize__serialize(w, t->u.record); break;
    case GROOVE_VT_ENUM: pc_write_enum_schema(w, t->u.enum_); break;
    default: break;
    }
}

/* ScalarEnumSchema::deserialize goes through ScalarEnumSchemaSerde then from_parts. */
static jz_err pc_read_scalar_enum(jz_pc_r *r, groove_ScalarEnumSchema *out) {
    u64 registry_id;
    jz_str name = {0};
    groove_StrVec variants = {0};
    JZ_TRY(jz_pc_r_varint(r, &registry_id));
    JZ_TRY(jz_pc_r_str(r, &name));
    jz_err e = pc_read_strvec(r, &variants);
    if (e.code) { jz_str_drop(&name); return e; }
    return groove__records__values__ScalarEnumSchema__from_parts(registry_id, name, variants, out);
}

static jz_err pc_read_enum_schema(jz_pc_r *r, groove_EnumSchema *out) {
    memset(out, 0, sizeof(*out));
    JZ_TRY(jz_pc_r_varint(r, &out->registry_id));
    JZ_TRY(jz_pc_r_str(r, &out->name));
    u32 n;
    jz_err e = jz_pc_r_seq(r, &n);
    for (u32 i = 0; !e.code && i < n; i++) {
        groove_EnumCase c = {0};
        e = jz_pc_r_str(r, &c.name);
        if (e.code) break;
        e = groove__records__RecordDescriptor__Deserialize__deserialize(r, &c.payload);
        if (e.code) { jz_str_drop(&c.name); break; }
        jz_vec_push(&out->cases, &c, sizeof(c));
    }
    if (e.code) groove__records__values__EnumSchema__drop(out);
    return e;
}

jz_err groove__records__values__ValueType__Deserialize__deserialize(jz_pc_r *r, groove_ValueType *out) {
    u32 idx;
    JZ_TRY(jz_pc_r_enter(r));
    jz_err e = jz_pc_r_variant(r, &idx, GROOVE_VT_COUNT);
    if (e.code) goto done;
    *out = VT_SCALAR((u8)idx);
    switch (idx) {
    case GROOVE_VT_INTERNAL: {
        u32 repr, kind = 0;
        e = jz_pc_r_variant(r, &repr, 3);
        if (!e.code && repr == GROOVE_IVT_STORED_SCALAR) e = jz_pc_r_variant(r, &kind, 3);
        out->u.internal.repr = (u8)repr;
        out->u.internal.kind = (u8)kind;
        break;
    }
    case GROOVE_VT_ENUM_TAG: e = pc_read_scalar_enum(r, &out->u.enum_tag); break;
    case GROOVE_VT_TUPLE: {
        u32 n;
        e = jz_pc_r_seq(r, &n);
        for (u32 i = 0; !e.code && i < n; i++) {
            groove_ValueType m;
            e = groove__records__values__ValueType__Deserialize__deserialize(r, &m);
            if (!e.code) jz_vec_push(&out->u.tuple, &m, sizeof(m));
        }
        break;
    }
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE:
        out->u.inner = JZ_NEW(groove_ValueType);
        e = groove__records__values__ValueType__Deserialize__deserialize(r, out->u.inner);
        if (e.code) { jz_free(out->u.inner); out->tag = GROOVE_VT_U8; }
        break;
    case GROOVE_VT_RECORD: e = groove__records__RecordDescriptor__Deserialize__deserialize(r, &out->u.record); break;
    case GROOVE_VT_ENUM:
        out->u.enum_ = JZ_NEW(groove_EnumSchema);
        e = pc_read_enum_schema(r, out->u.enum_);
        if (e.code) { jz_free(out->u.enum_); out->tag = GROOVE_VT_U8; }
        break;
    default: break;
    }
    if (e.code) groove__records__values__ValueType__drop(out);
done:
    jz_pc_r_leave(r);
    return e;
}

/* ===================================================================== serde: JSON (externally tagged) */

static void jw_str(jz_json_w *w, const jz_str *s) { jz_jw_str(w, s->ptr, s->len); }
static void jw_strvec(jz_json_w *w, const groove_StrVec *v) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < v->len; i++) jw_str(w, &v->ptr[i]);
    jz_jw_end_arr(w);
}
static void jw_bytes(jz_json_w *w, const u8 *p, u32 n) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < n; i++) jz_jw_u64(w, p[i]);
    jz_jw_end_arr(w);
}

void groove__records__values__Value__Serialize__serialize_json(jz_json_w *w, const groove_Value *v) {
    jz_jw_begin_obj(w);
    jz_jw_key(w, VALUE_NAMES[v->tag]);
    switch (v->tag) {
    case GROOVE_V_U8: jz_jw_u64(w, v->u.u8_); break;
    case GROOVE_V_U16: jz_jw_u64(w, v->u.u16_); break;
    case GROOVE_V_U32: jz_jw_u64(w, v->u.u32_); break;
    case GROOVE_V_U64:
    case GROOVE_V_U48: jz_jw_u64(w, v->u.u64_); break;
    case GROOVE_V_F64: jz_jw_f64(w, v->u.f64_); break;
    case GROOVE_V_BOOL: jz_jw_bool(w, v->u.b); break;
    case GROOVE_V_STRING: jw_str(w, &v->u.s); break;
    case GROOVE_V_BYTES: jw_bytes(w, v->u.bytes.ptr, v->u.bytes.len); break;
    case GROOVE_V_LARGE: groove__large_values__LargeValueRef__Serialize__serialize_json(w, v->u.large); break;
    case GROOVE_V_UUID: {
        char buf[37];
        jz_uuid_to_string(&v->u.uuid, buf);
        jz_jw_str(w, buf, 36);
        break;
    }
    case GROOVE_V_ENUM_TAG: jz_jw_u64(w, v->u.enum_tag); break;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY:
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < v->u.items.len; i++) groove__records__values__Value__Serialize__serialize_json(w, &v->u.items.ptr[i]);
        jz_jw_end_arr(w);
        break;
    case GROOVE_V_NULLABLE:
        if (v->u.nullable) groove__records__values__Value__Serialize__serialize_json(w, v->u.nullable);
        else jz_jw_null(w);
        break;
    case GROOVE_V_I64: jz_jw_i64(w, v->u.i64_); break;
    case GROOVE_V_I32: jz_jw_i64(w, v->u.i32_); break;
    case GROOVE_V_RECORD: groove__records__OwnedRecord__Serialize__serialize_json(w, &v->u.record); break;
    case GROOVE_V_ENUM:
        jz_jw_begin_obj(w);
        jz_jw_key(w, "tag");
        jz_jw_u64(w, v->u.enum_.tag);
        jz_jw_key(w, "record");
        groove__records__OwnedRecord__Serialize__serialize_json(w, &v->u.enum_.record);
        jz_jw_end_obj(w);
        break;
    default: break;
    }
    jz_jw_end_obj(w);
}

static jz_err json_type_err(const char *expected) {
    return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid type: expected %s", expected);
}

static jz_err json_u64(const jz_json *j, u64 max, const char *what, u64 *out) {
    if (!j || !jz_json_as_u64(j, out)) return json_type_err(what);
    if (*out > max) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid value: integer `%llu`, expected %s", (unsigned long long)*out, what);
    return JZ_OK;
}

static jz_err json_bytes(const jz_json *j, jz_bytes *out) {
    if (!j || j->kind != JZ_JSON_ARR) return json_type_err("a sequence");
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        u64 b;
        jz_err e = json_u64(j->v.arr.ptr[i], 0xff, "u8", &b);
        if (e.code) { jz_bytes_drop(out); return e; }
        jz_bytes_push_u8(out, (u8)b);
    }
    return JZ_OK;
}

static jz_err json_strvec(const jz_json *j, groove_StrVec *out) {
    if (!j || j->kind != JZ_JSON_ARR) return json_type_err("a sequence");
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        const jz_str *s = jz_json_as_str(j->v.arr.ptr[i]);
        if (!s) { strvec_drop(out); return json_type_err("a string"); }
        jz_str c = jz_str_clone(s);
        jz_vec_push(out, &c, sizeof(c));
    }
    return JZ_OK;
}

static jz_err json_value_vec(const jz_json *j, groove_ValueVec *out) {
    if (!j || j->kind != JZ_JSON_ARR) return json_type_err("a sequence");
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        groove_Value item;
        jz_err e = groove__records__values__Value__Deserialize__deserialize_json(j->v.arr.ptr[i], &item);
        if (e.code) { groove__records__values__ValueVec__drop(out); return e; }
        jz_vec_push(out, &item, sizeof(item));
    }
    return JZ_OK;
}

jz_err groove__records__values__Value__Deserialize__deserialize_json(const jz_json *j, groove_Value *out) {
    u32 idx;
    const jz_json *p = NULL;
    JZ_TRY(jz_json_variant(j, VALUE_NAMES, GROOVE_V_COUNT, &idx, &p));
    memset(out, 0, sizeof(*out));
    out->tag = (u8)idx;
    u64 v;
    i64 s;
    switch (idx) {
    case GROOVE_V_U8: JZ_TRY(json_u64(p, 0xff, "u8", &v)); out->u.u8_ = (u8)v; return JZ_OK;
    case GROOVE_V_U16: JZ_TRY(json_u64(p, 0xffff, "u16", &v)); out->u.u16_ = (u16)v; return JZ_OK;
    case GROOVE_V_U32: JZ_TRY(json_u64(p, 0xffffffffu, "u32", &v)); out->u.u32_ = (u32)v; return JZ_OK;
    case GROOVE_V_U64:
    case GROOVE_V_U48: return json_u64(p, UINT64_MAX, "u64", &out->u.u64_);
    case GROOVE_V_F64: if (!p || !jz_json_as_f64(p, &out->u.f64_)) return json_type_err("f64"); return JZ_OK;
    case GROOVE_V_BOOL: if (!p || p->kind != JZ_JSON_BOOL) return json_type_err("a boolean"); out->u.b = p->v.b; return JZ_OK;
    case GROOVE_V_STRING: {
        const jz_str *str = p ? jz_json_as_str(p) : NULL;
        if (!str) return json_type_err("a string");
        out->u.s = jz_str_clone(str);
        return JZ_OK;
    }
    case GROOVE_V_BYTES: return json_bytes(p, &out->u.bytes);
    case GROOVE_V_LARGE: {
        groove_LargeValueRef *lr = JZ_NEW(groove_LargeValueRef);
        jz_err e = groove__large_values__LargeValueRef__Deserialize__deserialize_json(p, lr);
        if (e.code) { jz_free(lr); out->tag = GROOVE_V_U8; return e; }
        out->u.large = lr;
        return JZ_OK;
    }
    case GROOVE_V_UUID: {
        const jz_str *str = p ? jz_json_as_str(p) : NULL;
        if (!str) return json_type_err("a UUID string");
        return jz_uuid_parse(jz_str_slice(str), &out->u.uuid);
    }
    case GROOVE_V_ENUM_TAG: JZ_TRY(json_u64(p, 0xff, "u8", &v)); out->u.enum_tag = (u8)v; return JZ_OK;
    case GROOVE_V_TUPLE:
    case GROOVE_V_ARRAY: return json_value_vec(p, &out->u.items);
    case GROOVE_V_NULLABLE:
        if (!p || p->kind == JZ_JSON_NULL) return JZ_OK;
        out->u.nullable = JZ_NEW(groove_Value);
        {
            jz_err e = groove__records__values__Value__Deserialize__deserialize_json(p, out->u.nullable);
            if (e.code) { jz_free(out->u.nullable); out->u.nullable = NULL; return e; }
        }
        return JZ_OK;
    case GROOVE_V_I64: if (!p || !jz_json_as_i64(p, &s)) return json_type_err("i64"); out->u.i64_ = s; return JZ_OK;
    case GROOVE_V_I32:
        if (!p || !jz_json_as_i64(p, &s)) return json_type_err("i32");
        if (s < INT32_MIN || s > INT32_MAX) return jz_err_fmt(JZ_JSON_ERR_TYPE, "invalid value: integer `%lld`, expected i32", (long long)s);
        out->u.i32_ = (i32)s;
        return JZ_OK;
    case GROOVE_V_RECORD: return groove__records__OwnedRecord__Deserialize__deserialize_json(p, &out->u.record);
    case GROOVE_V_ENUM: {
        JZ_TRY(jz_json_expect_obj(p, "struct EnumValue"));
        JZ_TRY(json_u64(jz_json_obj_get(p, "tag"), 0xffffffffu, "u32", &v));
        out->u.enum_.tag = (u32)v;
        const jz_json *rec = jz_json_obj_get(p, "record");
        if (!rec) return jz_err_new(JZ_JSON_ERR_MISSING_FIELD, "missing field `record`");
        return groove__records__OwnedRecord__Deserialize__deserialize_json(rec, &out->u.enum_.record);
    }
    default: return JZ_OK;
    }
}

void groove__large_values__LargeValueKind__Serialize__serialize_json(jz_json_w *w, u8 kind) {
    jz_jw_str(w, LARGE_VALUE_KIND_NAMES[kind], (u32)strlen(LARGE_VALUE_KIND_NAMES[kind]));
}

void groove__records__values__ValueType__Serialize__serialize_json(jz_json_w *w, const groove_ValueType *t) {
    const char *name = VALUE_TYPE_NAMES[t->tag];
    switch (t->tag) {
    case GROOVE_VT_INTERNAL:
        jz_jw_begin_obj(w);
        jz_jw_key(w, name);
        if (t->u.internal.repr == GROOVE_IVT_STORED_SCALAR) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "StoredScalar");
            groove__large_values__LargeValueKind__Serialize__serialize_json(w, t->u.internal.kind);
            jz_jw_end_obj(w);
        } else {
            const char *r = INTERNAL_REPR_NAMES[t->u.internal.repr];
            jz_jw_str(w, r, (u32)strlen(r));
        }
        jz_jw_end_obj(w);
        return;
    case GROOVE_VT_ENUM_TAG:
        jz_jw_begin_obj(w);
        jz_jw_key(w, name);
        jz_jw_begin_obj(w);
        jz_jw_key(w, "registry_id");
        jz_jw_u64(w, t->u.enum_tag.registry_id);
        jz_jw_key(w, "name");
        jw_str(w, &t->u.enum_tag.name);
        jz_jw_key(w, "variants");
        jw_strvec(w, &t->u.enum_tag.variants);
        jz_jw_end_obj(w);
        jz_jw_end_obj(w);
        return;
    case GROOVE_VT_TUPLE:
        jz_jw_begin_obj(w);
        jz_jw_key(w, name);
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < t->u.tuple.len; i++) groove__records__values__ValueType__Serialize__serialize_json(w, &t->u.tuple.ptr[i]);
        jz_jw_end_arr(w);
        jz_jw_end_obj(w);
        return;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE:
        jz_jw_begin_obj(w);
        jz_jw_key(w, name);
        groove__records__values__ValueType__Serialize__serialize_json(w, t->u.inner);
        jz_jw_end_obj(w);
        return;
    case GROOVE_VT_RECORD:
        jz_jw_begin_obj(w);
        jz_jw_key(w, name);
        groove__records__RecordDescriptor__Serialize__serialize_json(w, t->u.record);
        jz_jw_end_obj(w);
        return;
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *s = t->u.enum_;
        jz_jw_begin_obj(w);
        jz_jw_key(w, name);
        jz_jw_begin_obj(w);
        jz_jw_key(w, "registry_id");
        jz_jw_u64(w, s->registry_id);
        jz_jw_key(w, "name");
        jw_str(w, &s->name);
        jz_jw_key(w, "cases");
        jz_jw_begin_arr(w);
        for (u32 i = 0; i < s->cases.len; i++) {
            jz_jw_begin_obj(w);
            jz_jw_key(w, "name");
            jw_str(w, &s->cases.ptr[i].name);
            jz_jw_key(w, "payload");
            groove__records__RecordDescriptor__Serialize__serialize_json(w, s->cases.ptr[i].payload);
            jz_jw_end_obj(w);
        }
        jz_jw_end_arr(w);
        jz_jw_end_obj(w);
        jz_jw_end_obj(w);
        return;
    }
    default:
        jz_jw_str(w, name, (u32)strlen(name));
        return;
    }
}

jz_err groove__large_values__LargeValueKind__Deserialize__deserialize_json(const jz_json *j, u8 *out) {
    u32 idx;
    const jz_json *p = NULL;
    JZ_TRY(jz_json_variant(j, LARGE_VALUE_KIND_NAMES, 3, &idx, &p));
    *out = (u8)idx;
    return JZ_OK;
}

static jz_err json_registry_id(const jz_json *obj, u64 *out) {
    /* #[serde(default)] registry_id */
    const jz_json *f = jz_json_obj_get(obj, "registry_id");
    if (!f) { *out = 0; return JZ_OK; }
    return json_u64(f, UINT64_MAX, "u64", out);
}

jz_err groove__records__values__ValueType__Deserialize__deserialize_json(const jz_json *j, groove_ValueType *out) {
    u32 idx;
    const jz_json *p = NULL;
    JZ_TRY(jz_json_variant(j, VALUE_TYPE_NAMES, GROOVE_VT_COUNT, &idx, &p));
    *out = VT_SCALAR((u8)idx);
    switch (idx) {
    case GROOVE_VT_INTERNAL: {
        u32 repr;
        const jz_json *kp = NULL;
        JZ_TRY(jz_json_variant(p, INTERNAL_REPR_NAMES, 3, &repr, &kp));
        out->u.internal.repr = (u8)repr;
        if (repr == GROOVE_IVT_STORED_SCALAR) JZ_TRY(groove__large_values__LargeValueKind__Deserialize__deserialize_json(kp, &out->u.internal.kind));
        return JZ_OK;
    }
    case GROOVE_VT_ENUM_TAG: {
        u64 registry_id;
        jz_str name = {0};
        groove_StrVec variants = {0};
        JZ_TRY(jz_json_expect_obj(p, "struct ScalarEnumSchemaSerde"));
        JZ_TRY(json_registry_id(p, &registry_id));
        JZ_TRY(jz_json_field_str(p, "name", &name));
        jz_err e = json_strvec(jz_json_obj_get(p, "variants"), &variants);
        if (e.code) { jz_str_drop(&name); return e; }
        return groove__records__values__ScalarEnumSchema__from_parts(registry_id, name, variants, &out->u.enum_tag);
    }
    case GROOVE_VT_TUPLE:
        if (!p || p->kind != JZ_JSON_ARR) return json_type_err("a sequence");
        for (u32 i = 0; i < p->v.arr.len; i++) {
            groove_ValueType m;
            jz_err e = groove__records__values__ValueType__Deserialize__deserialize_json(p->v.arr.ptr[i], &m);
            if (e.code) { groove__records__values__ValueType__drop(out); return e; }
            jz_vec_push(&out->u.tuple, &m, sizeof(m));
        }
        return JZ_OK;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: {
        out->u.inner = JZ_NEW(groove_ValueType);
        jz_err e = groove__records__values__ValueType__Deserialize__deserialize_json(p, out->u.inner);
        if (e.code) { jz_free(out->u.inner); out->tag = GROOVE_VT_U8; }
        return e;
    }
    case GROOVE_VT_RECORD: return groove__records__RecordDescriptor__Deserialize__deserialize_json(p, &out->u.record);
    case GROOVE_VT_ENUM: {
        groove_EnumSchema *s = JZ_NEW(groove_EnumSchema);
        jz_err e = jz_json_expect_obj(p, "struct EnumSchema");
        if (!e.code) e = json_registry_id(p, &s->registry_id);
        if (!e.code) e = jz_json_field_str(p, "name", &s->name);
        const jz_json *cases = e.code ? NULL : jz_json_obj_get(p, "cases");
        if (!e.code && (!cases || cases->kind != JZ_JSON_ARR)) e = json_type_err("a sequence");
        for (u32 i = 0; !e.code && i < cases->v.arr.len; i++) {
            const jz_json *c = cases->v.arr.ptr[i];
            groove_EnumCase ec = {0};
            e = jz_json_expect_obj(c, "struct EnumCase");
            if (!e.code) e = jz_json_field_str(c, "name", &ec.name);
            if (!e.code) {
                const jz_json *pl = jz_json_obj_get(c, "payload");
                e = pl ? groove__records__RecordDescriptor__Deserialize__deserialize_json(pl, &ec.payload)
                       : jz_err_new(JZ_JSON_ERR_MISSING_FIELD, "missing field `payload`");
            }
            if (e.code) { jz_str_drop(&ec.name); break; }
            jz_vec_push(&s->cases, &ec, sizeof(ec));
        }
        if (e.code) {
            groove__records__values__EnumSchema__drop(s);
            jz_free(s);
            out->tag = GROOVE_VT_U8;
            return e;
        }
        out->u.enum_ = s;
        return JZ_OK;
    }
    default: return JZ_OK;
    }
}

/* ===================================================================== record variant registries */

static groove_RecordDescriptor assign_record_variant_registries(groove_RecordDescriptor descriptor, const char *path, u32 path_len, bool replace) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &n);
    groove_DescriptorFieldVec out = {0};
    jz_vec_reserve(&out, n, sizeof(groove_DescriptorField));
    for (u32 index = 0; index < n; index++) {
        groove_DescriptorField f;
        memset(&f, 0, sizeof(f));
        f.has_name = fields[index].has_name;
        if (f.has_name) f.name = jz_str_clone(&fields[index].name);
        f.has_identity = fields[index].has_identity;
        if (f.has_identity) groove__records__FieldIdentity__Clone__clone(&f.identity, &fields[index].identity);
        groove__records__values__ValueType__Clone__clone(&f.value_type, &fields[index].value_type);
        jz_str sub = jz_format("%.*s/field/%u", (int)path_len, path, index);
        groove__records__values__ValueType__assign_variant_registries(&f.value_type, sub.ptr, sub.len, replace);
        jz_str_drop(&sub);
        jz_vec_push(&out, &f, sizeof(f));
    }
    return groove__records__RecordDescriptor__from_logical_fields(out);
}

/* ===================================================================== descriptor codec */

#define DESCRIPTOR_CODEC_MAX_NODES 1024u
enum {
    DESCRIPTOR_NODE_DESCRIPTOR = 0, DESCRIPTOR_NODE_FIELD = 1, DESCRIPTOR_NODE_U8 = 2, DESCRIPTOR_NODE_U16 = 3,
    DESCRIPTOR_NODE_U32 = 4, DESCRIPTOR_NODE_U64 = 5, DESCRIPTOR_NODE_I32 = 6, DESCRIPTOR_NODE_I64 = 7,
    DESCRIPTOR_NODE_F64 = 8, DESCRIPTOR_NODE_BOOL = 9, DESCRIPTOR_NODE_STRING = 10, DESCRIPTOR_NODE_BYTES = 11,
    DESCRIPTOR_NODE_RAW_STRING = 12, DESCRIPTOR_NODE_RAW_BYTES = 13, DESCRIPTOR_NODE_STORED_BYTES = 14,
    DESCRIPTOR_NODE_STORED_STRING = 15, DESCRIPTOR_NODE_STORED_JSON = 16, DESCRIPTOR_NODE_UUID = 17,
    DESCRIPTOR_NODE_ENUM_TAG = 18, DESCRIPTOR_NODE_TUPLE = 19, DESCRIPTOR_NODE_ARRAY = 20,
    DESCRIPTOR_NODE_NULLABLE = 21, DESCRIPTOR_NODE_RECORD = 22, DESCRIPTOR_NODE_ENUM = 23,
    DESCRIPTOR_NODE_ENUM_CASE = 24, DESCRIPTOR_NODE_U48 = 25,
};

static jz_err checked_u48(u64 value, u64 *out) {
    if (value > GROOVE_U48_MAX) return jz_err_fmt(GROOVE_REC_E_U48OutOfRange, "u48 value %llu exceeds 2^48 - 1", (unsigned long long)value);
    *out = value;
    return JZ_OK;
}

static jz_err u48_le_bytes(u64 value, u8 out[6]) {
    u64 v;
    JZ_TRY(checked_u48(value, &v));
    for (int i = 0; i < 6; i++) out[i] = (u8)(v >> (8 * i));
    return JZ_OK;
}

jz_err groove__records__values__u48_be_bytes(u64 value, u8 out[6]) {
    u64 v;
    JZ_TRY(checked_u48(value, &v));
    u8 b[8];
    put_be64(b, v);
    memcpy(out, b + 2, 6);
    return JZ_OK;
}

u64 groove__records__values__u48_from_le_bytes(const u8 b[6]) {
    u8 wide[8] = {0};
    memcpy(wide, b, 6);
    return get_le64(wide);
}

u64 groove__records__values__u48_from_be_bytes(const u8 b[6]) {
    u8 wide[8] = {0};
    memcpy(wide + 2, b, 6);
    return get_be64(wide);
}

typedef struct DescriptorCodecNode {
    u8 tag;
    bool has_name; jz_str name;
    bool has_identity_name; jz_str identity_name;
    bool has_identity_slot; u64 identity_slot;
    u64 registry_id;
    u32 children;
    groove_StrVec strings;
} DescriptorCodecNode;
typedef JZ_VEC(DescriptorCodecNode) DescriptorCodecNodeVec;

static void codec_node_drop(void *p) {
    DescriptorCodecNode *n = p;
    if (n->has_name) jz_str_drop(&n->name);
    if (n->has_identity_name) jz_str_drop(&n->identity_name);
    strvec_drop(&n->strings);
}

static DescriptorCodecNode codec_scalar(u8 tag) {
    DescriptorCodecNode n;
    memset(&n, 0, sizeof(n));
    n.tag = tag;
    return n;
}

static groove_RecordDescriptor descriptor_codec_node_descriptor(void) {
    static groove_RecordDescriptor DESCRIPTOR;
    if (!DESCRIPTOR) {
        static const char *const names[7] = {"tag", "name", "identity_name", "identity_slot", "registry_id", "children", "strings"};
        groove_ValueType types[7];
        types[0] = VT_SCALAR(GROOVE_VT_U8);
        types[1] = groove__records__values__ValueType__nullable(groove__records__values__ValueType__raw_string());
        types[2] = groove__records__values__ValueType__nullable(groove__records__values__ValueType__raw_string());
        types[3] = groove__records__values__ValueType__nullable(VT_SCALAR(GROOVE_VT_U64));
        types[4] = VT_SCALAR(GROOVE_VT_U64);
        types[5] = VT_SCALAR(GROOVE_VT_U32);
        types[6] = groove__records__values__ValueType__array_of(groove__records__values__ValueType__raw_string());
        DESCRIPTOR = groove__records__RecordDescriptor__new_cstr(names, types, 7);
    }
    return DESCRIPTOR;
}

static groove_RecordDescriptor descriptor_codec_envelope(void) {
    static groove_RecordDescriptor DESCRIPTOR;
    if (!DESCRIPTOR) {
        static const char *const names[1] = {"nodes"};
        groove_ValueType rec = VT_SCALAR(GROOVE_VT_RECORD);
        rec.u.record = descriptor_codec_node_descriptor();
        groove_ValueType types[1] = {groove__records__values__ValueType__array_of(rec)};
        DESCRIPTOR = groove__records__RecordDescriptor__new_cstr(names, types, 1);
    }
    return DESCRIPTOR;
}

static groove_Value value_string_move(jz_str s) { groove_Value v = {.tag = GROOVE_V_STRING}; v.u.s = s; return v; }
static groove_Value value_nullable_box(bool some, groove_Value inner) {
    groove_Value v = {.tag = GROOVE_V_NULLABLE};
    if (some) { v.u.nullable = JZ_NEW(groove_Value); *v.u.nullable = inner; }
    return v;
}
static groove_Value value_strings_array(groove_StrVec strings) {
    groove_Value v = {.tag = GROOVE_V_ARRAY};
    jz_vec_reserve(&v.u.items, strings.len, sizeof(groove_Value));
    for (u32 i = 0; i < strings.len; i++) {
        groove_Value s = value_string_move(strings.ptr[i]);
        jz_vec_push(&v.u.items, &s, sizeof(s));
    }
    jz_free(strings.ptr);
    return v;
}
static void value_array_drop(groove_Value *vals, u32 n) { for (u32 i = 0; i < n; i++) groove__records__values__Value__drop(&vals[i]); }

/* Consumes `node`. */
static jz_err descriptor_codec_node_value(DescriptorCodecNode node, groove_Value *out) {
    groove_RecordDescriptor descriptor = descriptor_codec_node_descriptor();
    groove_Value vals[7];
    memset(vals, 0, sizeof(vals));
    vals[0].tag = GROOVE_V_U8; vals[0].u.u8_ = node.tag;
    vals[1] = value_nullable_box(node.has_name, value_string_move(node.name));
    vals[2] = value_nullable_box(node.has_identity_name, value_string_move(node.identity_name));
    groove_Value slot = {.tag = GROOVE_V_U64};
    slot.u.u64_ = node.identity_slot;
    vals[3] = value_nullable_box(node.has_identity_slot, slot);
    vals[4].tag = GROOVE_V_U64; vals[4].u.u64_ = node.registry_id;
    vals[5].tag = GROOVE_V_U32; vals[5].u.u32_ = node.children;
    vals[6] = value_strings_array(node.strings);
    jz_bytes raw = {0};
    jz_err e = groove__records__RecordDescriptor__create(descriptor, vals, 7, &raw);
    value_array_drop(vals, 7);
    if (e.code) return e;
    out->tag = GROOVE_V_RECORD;
    out->u.record = groove__records__OwnedRecord__new(raw, descriptor);
    return JZ_OK;
}

static jz_err non_canonical(void) { return groove__records__Error__simple(GROOVE_REC_E_NonCanonicalRecord); }

static jz_err codec_optional_string(const groove_Value *v, bool *has, jz_str *out) {
    if (!v->u.nullable) { *has = false; return JZ_OK; }
    if (v->u.nullable->tag != GROOVE_V_STRING) return non_canonical();
    *has = true;
    *out = jz_str_clone(&v->u.nullable->u.s);
    return JZ_OK;
}

static jz_err codec_strings(const groove_ValueVec *items, groove_StrVec *out) {
    memset(out, 0, sizeof(*out));
    for (u32 i = 0; i < items->len; i++) {
        if (items->ptr[i].tag != GROOVE_V_STRING) { strvec_drop(out); return non_canonical(); }
        jz_str s = jz_str_clone(&items->ptr[i].u.s);
        jz_vec_push(out, &s, sizeof(s));
    }
    return JZ_OK;
}

static jz_err descriptor_codec_node_from_value(const groove_Value *value, DescriptorCodecNode *out) {
    if (value->tag != GROOVE_V_RECORD) return non_canonical();
    const groove_OwnedRecord *record = &value->u.record;
    if (record->descriptor != descriptor_codec_node_descriptor()) return non_canonical();
    groove_ValueVec values = {0};
    JZ_TRY(groove__records__OwnedRecord__to_values(record, &values));
    jz_err e = JZ_OK;
    const groove_Value *v = values.ptr;
    if (values.len != 7 || v[0].tag != GROOVE_V_U8 || v[1].tag != GROOVE_V_NULLABLE || v[2].tag != GROOVE_V_NULLABLE ||
        v[3].tag != GROOVE_V_NULLABLE || v[4].tag != GROOVE_V_U64 || v[5].tag != GROOVE_V_U32 || v[6].tag != GROOVE_V_ARRAY) {
        e = non_canonical();
        goto done;
    }
    *out = codec_scalar(v[0].u.u8_);
    e = codec_optional_string(&v[1], &out->has_name, &out->name);
    if (!e.code) e = codec_optional_string(&v[2], &out->has_identity_name, &out->identity_name);
    if (!e.code && v[3].u.nullable) {
        if (v[3].u.nullable->tag != GROOVE_V_U64) e = non_canonical();
        else { out->has_identity_slot = true; out->identity_slot = v[3].u.nullable->u.u64_; }
    }
    if (!e.code) e = codec_strings(&v[6].u.items, &out->strings);
    out->registry_id = v[4].u.u64_;
    out->children = v[5].u.u32_;
    if (e.code) codec_node_drop(out);
done:
    groove__records__values__ValueVec__drop(&values);
    return e;
}

static jz_err descriptor_codec_push(DescriptorCodecNodeVec *nodes, DescriptorCodecNode node) {
    if (nodes->len == DESCRIPTOR_CODEC_MAX_NODES) {
        codec_node_drop(&node);
        return groove__records__Error__simple(GROOVE_REC_E_LengthOverflow);
    }
    jz_vec_push(nodes, &node, sizeof(node));
    return JZ_OK;
}

static jz_err descriptor_codec_push_value_type(DescriptorCodecNodeVec *nodes, const groove_ValueType *t);

static jz_err descriptor_codec_push_descriptor(DescriptorCodecNodeVec *nodes, groove_RecordDescriptor descriptor) {
    u32 n;
    const groove_DescriptorField *fields = groove__records__RecordDescriptor__fields(descriptor, &n);
    DescriptorCodecNode d = codec_scalar(DESCRIPTOR_NODE_DESCRIPTOR);
    d.children = n;
    JZ_TRY(descriptor_codec_push(nodes, d));
    for (u32 i = 0; i < n; i++) {
        const groove_DescriptorField *field = &fields[i];
        DescriptorCodecNode f = codec_scalar(DESCRIPTOR_NODE_FIELD);
        f.has_name = field->has_name;
        if (field->has_name) f.name = jz_str_clone(&field->name);
        if (field->has_identity) {
            const groove_FieldIdentity *id = &field->identity;
            if (id->tag == GROOVE_FID_NAME || id->tag == GROOVE_FID_NAMED_SLOT) {
                f.has_identity_name = true;
                f.identity_name = jz_str_clone(&id->name);
            }
            if (id->tag == GROOVE_FID_SLOT || id->tag == GROOVE_FID_NAMED_SLOT) {
                f.has_identity_slot = true;
                f.identity_slot = id->slot;
            }
        }
        f.children = 1;
        JZ_TRY(descriptor_codec_push(nodes, f));
        JZ_TRY(descriptor_codec_push_value_type(nodes, &field->value_type));
    }
    return JZ_OK;
}

static jz_err descriptor_codec_push_value_type(DescriptorCodecNodeVec *nodes, const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_U8: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_U8));
    case GROOVE_VT_U16: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_U16));
    case GROOVE_VT_U32: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_U32));
    case GROOVE_VT_U64: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_U64));
    case GROOVE_VT_U48: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_U48));
    case GROOVE_VT_I32: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_I32));
    case GROOVE_VT_I64: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_I64));
    case GROOVE_VT_F64: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_F64));
    case GROOVE_VT_BOOL: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_BOOL));
    case GROOVE_VT_STRING: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_STRING));
    case GROOVE_VT_BYTES: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_BYTES));
    case GROOVE_VT_INTERNAL:
        switch (t->u.internal.repr) {
        case GROOVE_IVT_RAW_STRING: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_RAW_STRING));
        case GROOVE_IVT_RAW_BYTES: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_RAW_BYTES));
        default:
            switch (t->u.internal.kind) {
            case GROOVE_LVK_BYTES: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_STORED_BYTES));
            case GROOVE_LVK_STRING: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_STORED_STRING));
            default: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_STORED_JSON));
            }
        }
    case GROOVE_VT_UUID: return descriptor_codec_push(nodes, codec_scalar(DESCRIPTOR_NODE_UUID));
    case GROOVE_VT_ENUM_TAG: {
        DescriptorCodecNode n = codec_scalar(DESCRIPTOR_NODE_ENUM_TAG);
        n.has_name = true;
        n.name = jz_str_clone(&t->u.enum_tag.name);
        n.registry_id = t->u.enum_tag.registry_id;
        strvec_clone(&n.strings, &t->u.enum_tag.variants);
        return descriptor_codec_push(nodes, n);
    }
    case GROOVE_VT_TUPLE: {
        DescriptorCodecNode n = codec_scalar(DESCRIPTOR_NODE_TUPLE);
        n.children = t->u.tuple.len;
        JZ_TRY(descriptor_codec_push(nodes, n));
        for (u32 i = 0; i < t->u.tuple.len; i++) JZ_TRY(descriptor_codec_push_value_type(nodes, &t->u.tuple.ptr[i]));
        return JZ_OK;
    }
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: {
        DescriptorCodecNode n = codec_scalar(t->tag == GROOVE_VT_ARRAY ? DESCRIPTOR_NODE_ARRAY : DESCRIPTOR_NODE_NULLABLE);
        n.children = 1;
        JZ_TRY(descriptor_codec_push(nodes, n));
        return descriptor_codec_push_value_type(nodes, t->u.inner);
    }
    case GROOVE_VT_RECORD: {
        DescriptorCodecNode n = codec_scalar(DESCRIPTOR_NODE_RECORD);
        n.children = 1;
        JZ_TRY(descriptor_codec_push(nodes, n));
        return descriptor_codec_push_descriptor(nodes, t->u.record);
    }
    case GROOVE_VT_ENUM: {
        const groove_EnumSchema *s = t->u.enum_;
        DescriptorCodecNode n = codec_scalar(DESCRIPTOR_NODE_ENUM);
        n.has_name = true;
        n.name = jz_str_clone(&s->name);
        n.registry_id = s->registry_id;
        n.children = s->cases.len;
        JZ_TRY(descriptor_codec_push(nodes, n));
        for (u32 i = 0; i < s->cases.len; i++) {
            DescriptorCodecNode c = codec_scalar(DESCRIPTOR_NODE_ENUM_CASE);
            c.has_name = true;
            c.name = jz_str_clone(&s->cases.ptr[i].name);
            c.children = 1;
            JZ_TRY(descriptor_codec_push(nodes, c));
            JZ_TRY(descriptor_codec_push_descriptor(nodes, s->cases.ptr[i].payload));
        }
        return JZ_OK;
    }
    default: JZ_UNREACHABLE();
    }
}

static jz_err descriptor_codec_take(const DescriptorCodecNodeVec *nodes, u32 *cursor, const DescriptorCodecNode **out) {
    if (*cursor >= nodes->len) return groove__records__Error__simple(GROOVE_REC_E_UnexpectedEof);
    *out = &nodes->ptr[*cursor];
    *cursor += 1;
    return JZ_OK;
}

static jz_err descriptor_codec_metadata_empty(const DescriptorCodecNode *n) {
    if (!n->has_name && !n->has_identity_name && !n->has_identity_slot && n->registry_id == 0 && n->children == 0 && n->strings.len == 0)
        return JZ_OK;
    return non_canonical();
}

static jz_err descriptor_codec_field_identity(const DescriptorCodecNode *n, bool *has, groove_FieldIdentity *out) {
    memset(out, 0, sizeof(*out));
    *has = true;
    if (n->has_identity_name && n->has_identity_slot) {
        out->tag = GROOVE_FID_NAMED_SLOT;
        out->name = jz_str_clone(&n->identity_name);
        out->slot = n->identity_slot;
    } else if (n->has_identity_name) {
        out->tag = GROOVE_FID_NAME;
        out->name = jz_str_clone(&n->identity_name);
    } else if (n->has_identity_slot) {
        out->tag = GROOVE_FID_SLOT;
        out->slot = n->identity_slot;
    } else {
        *has = false;
    }
    return JZ_OK;
}

static jz_err descriptor_codec_decode_value_type(const DescriptorCodecNodeVec *nodes, u32 *cursor, groove_ValueType *out);

static jz_err descriptor_codec_decode_descriptor(const DescriptorCodecNodeVec *nodes, u32 *cursor, groove_RecordDescriptor *out) {
    const DescriptorCodecNode *node;
    JZ_TRY(descriptor_codec_take(nodes, cursor, &node));
    if (node->tag != DESCRIPTOR_NODE_DESCRIPTOR || node->has_name || node->has_identity_name || node->has_identity_slot ||
        node->registry_id != 0 || node->strings.len != 0)
        return non_canonical();
    groove_DescriptorFieldVec fields = {0};
    jz_vec_reserve(&fields, node->children, sizeof(groove_DescriptorField));
    jz_err e = JZ_OK;
    for (u32 i = 0; i < node->children; i++) {
        const DescriptorCodecNode *field;
        e = descriptor_codec_take(nodes, cursor, &field);
        if (e.code) goto fail;
        if (field->tag != DESCRIPTOR_NODE_FIELD || field->registry_id != 0 || field->children != 1 || field->strings.len != 0) {
            e = non_canonical();
            goto fail;
        }
        groove_DescriptorField f;
        memset(&f, 0, sizeof(f));
        f.has_name = field->has_name;
        if (f.has_name) f.name = jz_str_clone(&field->name);
        descriptor_codec_field_identity(field, &f.has_identity, &f.identity);
        e = descriptor_codec_decode_value_type(nodes, cursor, &f.value_type);
        if (e.code) { f.value_type = VT_SCALAR(GROOVE_VT_U8); groove__records__DescriptorField__drop(&f); goto fail; }
        jz_vec_push(&fields, &f, sizeof(f));
    }
    /* from_logical_fields is the trusted constructor and panics on impossible descriptors;
     * these bytes are untrusted, so validate first. */
    for (u32 i = 0; i < fields.len; i++) {
        e = groove__records__values__validate_schema_value_type(&fields.ptr[i].value_type);
        if (e.code) goto fail;
    }
    *out = groove__records__RecordDescriptor__from_logical_fields(fields);
    return JZ_OK;
fail:
    jz_vec_drop(&fields, sizeof(groove_DescriptorField), groove__records__DescriptorField__drop_v);
    return e;
}

static jz_err codec_scalar_type(const DescriptorCodecNode *node, u8 expected, groove_ValueType value, groove_ValueType *out) {
    if (node->tag != expected) return non_canonical();
    JZ_TRY(descriptor_codec_metadata_empty(node));
    *out = value;
    return JZ_OK;
}

static jz_err descriptor_codec_decode_value_type(const DescriptorCodecNodeVec *nodes, u32 *cursor, groove_ValueType *out) {
    const DescriptorCodecNode *node;
    JZ_TRY(descriptor_codec_take(nodes, cursor, &node));
    switch (node->tag) {
    case DESCRIPTOR_NODE_U8: return codec_scalar_type(node, DESCRIPTOR_NODE_U8, VT_SCALAR(GROOVE_VT_U8), out);
    case DESCRIPTOR_NODE_U16: return codec_scalar_type(node, DESCRIPTOR_NODE_U16, VT_SCALAR(GROOVE_VT_U16), out);
    case DESCRIPTOR_NODE_U32: return codec_scalar_type(node, DESCRIPTOR_NODE_U32, VT_SCALAR(GROOVE_VT_U32), out);
    case DESCRIPTOR_NODE_U64: return codec_scalar_type(node, DESCRIPTOR_NODE_U64, VT_SCALAR(GROOVE_VT_U64), out);
    case DESCRIPTOR_NODE_U48: return codec_scalar_type(node, DESCRIPTOR_NODE_U48, VT_SCALAR(GROOVE_VT_U48), out);
    case DESCRIPTOR_NODE_I32: return codec_scalar_type(node, DESCRIPTOR_NODE_I32, VT_SCALAR(GROOVE_VT_I32), out);
    case DESCRIPTOR_NODE_I64: return codec_scalar_type(node, DESCRIPTOR_NODE_I64, VT_SCALAR(GROOVE_VT_I64), out);
    case DESCRIPTOR_NODE_F64: return codec_scalar_type(node, DESCRIPTOR_NODE_F64, VT_SCALAR(GROOVE_VT_F64), out);
    case DESCRIPTOR_NODE_BOOL: return codec_scalar_type(node, DESCRIPTOR_NODE_BOOL, VT_SCALAR(GROOVE_VT_BOOL), out);
    case DESCRIPTOR_NODE_STRING: return codec_scalar_type(node, DESCRIPTOR_NODE_STRING, VT_SCALAR(GROOVE_VT_STRING), out);
    case DESCRIPTOR_NODE_BYTES: return codec_scalar_type(node, DESCRIPTOR_NODE_BYTES, VT_SCALAR(GROOVE_VT_BYTES), out);
    case DESCRIPTOR_NODE_RAW_STRING: return codec_scalar_type(node, DESCRIPTOR_NODE_RAW_STRING, groove__records__values__ValueType__raw_string(), out);
    case DESCRIPTOR_NODE_RAW_BYTES: return codec_scalar_type(node, DESCRIPTOR_NODE_RAW_BYTES, groove__records__values__ValueType__raw_bytes(), out);
    case DESCRIPTOR_NODE_STORED_BYTES:
        return codec_scalar_type(node, DESCRIPTOR_NODE_STORED_BYTES, groove__records__values__ValueType__stored_scalar(GROOVE_LVK_BYTES), out);
    case DESCRIPTOR_NODE_STORED_STRING:
        return codec_scalar_type(node, DESCRIPTOR_NODE_STORED_STRING, groove__records__values__ValueType__stored_scalar(GROOVE_LVK_STRING), out);
    case DESCRIPTOR_NODE_STORED_JSON:
        return codec_scalar_type(node, DESCRIPTOR_NODE_STORED_JSON, groove__records__values__ValueType__stored_scalar(GROOVE_LVK_JSON), out);
    case DESCRIPTOR_NODE_UUID: return codec_scalar_type(node, DESCRIPTOR_NODE_UUID, VT_SCALAR(GROOVE_VT_UUID), out);
    case DESCRIPTOR_NODE_ENUM_TAG: {
        if (!node->has_name || node->has_identity_name || node->has_identity_slot || node->children != 0) return non_canonical();
        groove_StrVec strings = {0};
        strvec_clone(&strings, &node->strings);
        if (!node->has_name) jz_panic("checked");
        *out = VT_SCALAR(GROOVE_VT_ENUM_TAG);
        jz_err e = groove__records__values__ScalarEnumSchema__from_parts(node->registry_id, jz_str_clone(&node->name), strings, &out->u.enum_tag);
        if (e.code) out->tag = GROOVE_VT_U8;
        return e;
    }
    case DESCRIPTOR_NODE_TUPLE: {
        if (node->has_name || node->has_identity_name || node->has_identity_slot || node->registry_id != 0 || node->strings.len != 0)
            return non_canonical();
        groove_ValueType value_type = VT_SCALAR(GROOVE_VT_TUPLE);
        jz_vec_reserve(&value_type.u.tuple, node->children, sizeof(groove_ValueType));
        for (u32 i = 0; i < node->children; i++) {
            groove_ValueType m;
            jz_err e = descriptor_codec_decode_value_type(nodes, cursor, &m);
            if (e.code) { groove__records__values__ValueType__drop(&value_type); return e; }
            jz_vec_push(&value_type.u.tuple, &m, sizeof(m));
        }
        u32 size;
        if (!groove__records__values__ValueType__fixed_size(&value_type, &size)) {
            jz_err e = groove__records__Error__invalid_tuple_member(&value_type);
            groove__records__values__ValueType__drop(&value_type);
            return e;
        }
        *out = value_type;
        return JZ_OK;
    }
    case DESCRIPTOR_NODE_ARRAY:
    case DESCRIPTOR_NODE_NULLABLE: {
        if (node->has_name || node->has_identity_name || node->has_identity_slot || node->registry_id != 0 || node->children != 1 ||
            node->strings.len != 0)
            return non_canonical();
        groove_ValueType *inner = JZ_NEW(groove_ValueType);
        jz_err e = descriptor_codec_decode_value_type(nodes, cursor, inner);
        if (e.code) { jz_free(inner); return e; }
        *out = VT_SCALAR(node->tag == DESCRIPTOR_NODE_ARRAY ? GROOVE_VT_ARRAY : GROOVE_VT_NULLABLE);
        out->u.inner = inner;
        return JZ_OK;
    }
    case DESCRIPTOR_NODE_RECORD: {
        if (node->has_name || node->has_identity_name || node->has_identity_slot || node->registry_id != 0 || node->children != 1 ||
            node->strings.len != 0)
            return non_canonical();
        groove_RecordDescriptor d;
        JZ_TRY(descriptor_codec_decode_descriptor(nodes, cursor, &d));
        *out = VT_SCALAR(GROOVE_VT_RECORD);
        out->u.record = d;
        return JZ_OK;
    }
    case DESCRIPTOR_NODE_ENUM: {
        if (!node->has_name || node->has_identity_name || node->has_identity_slot || node->strings.len != 0) return non_canonical();
        groove_EnumCaseVec cases = {0};
        jz_vec_reserve(&cases, node->children, sizeof(groove_EnumCase));
        jz_err e = JZ_OK;
        for (u32 i = 0; i < node->children; i++) {
            const DescriptorCodecNode *c;
            e = descriptor_codec_take(nodes, cursor, &c);
            if (e.code) break;
            if (c->tag != DESCRIPTOR_NODE_ENUM_CASE || !c->has_name || c->has_identity_name || c->has_identity_slot ||
                c->registry_id != 0 || c->children != 1 || c->strings.len != 0) {
                e = non_canonical();
                break;
            }
            groove_RecordDescriptor payload;
            e = descriptor_codec_decode_descriptor(nodes, cursor, &payload);
            if (e.code) break;
            groove_EnumCase ec = groove__records__values__EnumCase__new(jz_str_clone(&c->name), payload);
            jz_vec_push(&cases, &ec, sizeof(ec));
        }
        if (e.code) { jz_vec_drop(&cases, sizeof(groove_EnumCase), enum_case_drop_v); return e; }
        groove_EnumSchema *schema = JZ_NEW(groove_EnumSchema);
        e = groove__records__values__EnumSchema__new(jz_str_clone(&node->name), cases, schema);
        if (e.code) { jz_free(schema); return e; }
        schema->registry_id = node->registry_id;
        *out = VT_SCALAR(GROOVE_VT_ENUM);
        out->u.enum_ = schema;
        return JZ_OK;
    }
    default: return non_canonical();
    }
}

static void codec_nodes_drop(DescriptorCodecNodeVec *nodes) { jz_vec_drop(nodes, sizeof(DescriptorCodecNode), codec_node_drop); }

typedef jz_err (*node_value_fn)(DescriptorCodecNode node, groove_Value *out);

static jz_err encode_descriptor_nodes(groove_RecordDescriptor descriptor, groove_RecordDescriptor envelope, node_value_fn to_value, jz_bytes *out) {
    DescriptorCodecNodeVec nodes = {0};
    jz_err e = descriptor_codec_push_descriptor(&nodes, descriptor);
    if (e.code) { codec_nodes_drop(&nodes); return e; }
    groove_Value array = {.tag = GROOVE_V_ARRAY};
    jz_vec_reserve(&array.u.items, nodes.len, sizeof(groove_Value));
    u32 i = 0;
    for (; i < nodes.len; i++) {
        groove_Value v;
        e = to_value(nodes.ptr[i], &v); /* moves the node */
        if (e.code) { i++; break; }
        jz_vec_push(&array.u.items, &v, sizeof(v));
    }
    for (; i < nodes.len; i++) codec_node_drop(&nodes.ptr[i]);
    jz_free(nodes.ptr);
    if (!e.code) e = groove__records__RecordDescriptor__create(envelope, &array, 1, out);
    groove__records__values__Value__drop(&array);
    return e;
}

jz_err groove__records__values__encode_record_descriptor(groove_RecordDescriptor d, jz_bytes *out) {
    return encode_descriptor_nodes(d, descriptor_codec_envelope(), descriptor_codec_node_value, out);
}

typedef jz_err (*node_from_value_fn)(const groove_Value *v, DescriptorCodecNode *out);
typedef jz_err (*encode_fn)(groove_RecordDescriptor d, jz_bytes *out);

static jz_err decode_descriptor_nodes(const u8 *encoded, u32 n, groove_RecordDescriptor envelope, node_from_value_fn from_value,
                                      encode_fn reencode, groove_RecordDescriptor *out) {
    groove_ValueVec values = {0};
    groove_BorrowedRecord rec = groove__records__RecordDescriptor__bind(envelope, encoded, n);
    JZ_TRY(groove__records__BorrowedRecord__to_values(&rec, &values));
    jz_bytes again = {0};
    DescriptorCodecNodeVec nodes = {0};
    jz_err e = groove__records__RecordDescriptor__create(envelope, values.ptr, values.len, &again);
    if (e.code) goto done;
    if (again.len != n || memcmp(again.ptr, encoded, n) != 0) { e = non_canonical(); goto done; }
    if (values.len != 1 || values.ptr[0].tag != GROOVE_V_ARRAY) { e = non_canonical(); goto done; }
    const groove_ValueVec *records = &values.ptr[0].u.items;
    if (records->len > DESCRIPTOR_CODEC_MAX_NODES) { e = groove__records__Error__simple(GROOVE_REC_E_LengthOverflow); goto done; }
    for (u32 i = 0; i < records->len; i++) {
        DescriptorCodecNode node;
        e = from_value(&records->ptr[i], &node);
        if (e.code) goto done;
        jz_vec_push(&nodes, &node, sizeof(node));
    }
    u32 cursor = 0;
    groove_RecordDescriptor descriptor;
    e = descriptor_codec_decode_descriptor(&nodes, &cursor, &descriptor);
    if (e.code) goto done;
    jz_bytes_drop(&again);
    if (cursor != nodes.len) { e = non_canonical(); goto done; }
    e = reencode(descriptor, &again);
    if (e.code) goto done;
    if (again.len != n || memcmp(again.ptr, encoded, n) != 0) { e = non_canonical(); goto done; }
    *out = descriptor;
done:
    jz_bytes_drop(&again);
    codec_nodes_drop(&nodes);
    groove__records__values__ValueVec__drop(&values);
    return e;
}

jz_err groove__records__values__decode_record_descriptor(const u8 *p, u32 n, groove_RecordDescriptor *out) {
    return decode_descriptor_nodes(p, n, descriptor_codec_envelope(), descriptor_codec_node_from_value,
                                   groove__records__values__encode_record_descriptor, out);
}

/* ---- persisted descriptor v2: one payload per node kind */

static const groove_EnumSchema *persisted_descriptor_node_schema(void) {
    static groove_EnumSchema SCHEMA;
    static bool init;
    if (!init) {
        static const char *const kinds[26] = {
            "Descriptor", "Field", "U8", "U16", "U32", "U64", "I32", "I64", "F64", "Bool", "String", "Bytes",
            "RawString", "RawBytes", "StoredBytes", "StoredString", "StoredJson", "Uuid", "EnumTag", "Tuple",
            "Array", "Nullable", "Record", "Enum", "EnumCase", "U48",
        };
        groove_EnumCaseVec cases = {0};
        for (u32 index = 0; index < 26; index++) {
            u8 tag = (u8)index;
            if (index != tag) jz_panic("assertion `left == right` failed");
            const char *names[3];
            groove_ValueType types[3];
            u32 nf = 0;
            switch (tag) {
            case DESCRIPTOR_NODE_DESCRIPTOR:
            case DESCRIPTOR_NODE_TUPLE:
                names[0] = "children"; types[0] = VT_SCALAR(GROOVE_VT_U32); nf = 1;
                break;
            case DESCRIPTOR_NODE_FIELD:
                names[0] = "name"; types[0] = groove__records__values__ValueType__nullable(groove__records__values__ValueType__raw_string()); nf = 1;
                break;
            case DESCRIPTOR_NODE_ENUM_TAG:
                names[0] = "name"; types[0] = groove__records__values__ValueType__raw_string();
                names[1] = "registry_id"; types[1] = VT_SCALAR(GROOVE_VT_U64);
                names[2] = "strings"; types[2] = groove__records__values__ValueType__array_of(groove__records__values__ValueType__raw_string());
                nf = 3;
                break;
            case DESCRIPTOR_NODE_ENUM:
                names[0] = "name"; types[0] = groove__records__values__ValueType__raw_string();
                names[1] = "registry_id"; types[1] = VT_SCALAR(GROOVE_VT_U64);
                names[2] = "children"; types[2] = VT_SCALAR(GROOVE_VT_U32);
                nf = 3;
                break;
            case DESCRIPTOR_NODE_ENUM_CASE:
                names[0] = "name"; types[0] = groove__records__values__ValueType__raw_string(); nf = 1;
                break;
            default: break;
            }
            groove_EnumCase c = groove__records__values__EnumCase__new(jz_str_from(kinds[index]),
                                                                        groove__records__RecordDescriptor__new_cstr(names, types, nf));
            jz_vec_push(&cases, &c, sizeof(c));
        }
        if (groove__records__values__EnumSchema__new(jz_str_from("groove.persisted_descriptor.v2"), cases, &SCHEMA).code)
            jz_panic("fixed persisted descriptor node schema");
        init = true;
    }
    return &SCHEMA;
}

static groove_RecordDescriptor persisted_descriptor_envelope(void) {
    static groove_RecordDescriptor DESCRIPTOR;
    if (!DESCRIPTOR) {
        static const char *const names[1] = {"nodes"};
        groove_ValueType en = VT_SCALAR(GROOVE_VT_ENUM);
        en.u.enum_ = JZ_NEW(groove_EnumSchema);
        groove__records__values__EnumSchema__Clone__clone(en.u.enum_, persisted_descriptor_node_schema());
        groove_ValueType types[1] = {groove__records__values__ValueType__array_of(en)};
        DESCRIPTOR = groove__records__RecordDescriptor__new_cstr(names, types, 1);
    }
    return DESCRIPTOR;
}

/* Consumes `node`. */
static jz_err persisted_descriptor_node_value(DescriptorCodecNode node, groove_Value *out) {
    groove_Value fields[3];
    u32 nf = 0;
    jz_err e = JZ_OK;
    memset(fields, 0, sizeof(fields));
    switch (node.tag) {
    case DESCRIPTOR_NODE_DESCRIPTOR:
    case DESCRIPTOR_NODE_TUPLE:
        fields[0].tag = GROOVE_V_U32; fields[0].u.u32_ = node.children; nf = 1;
        break;
    case DESCRIPTOR_NODE_FIELD:
        fields[0] = value_nullable_box(node.has_name, value_string_move(node.name));
        node.has_name = false;
        nf = 1;
        break;
    case DESCRIPTOR_NODE_ENUM_TAG:
        if (!node.has_name) { e = non_canonical(); break; }
        fields[0] = value_string_move(node.name);
        node.has_name = false;
        fields[1].tag = GROOVE_V_U64; fields[1].u.u64_ = node.registry_id;
        fields[2] = value_strings_array(node.strings);
        memset(&node.strings, 0, sizeof(node.strings));
        nf = 3;
        break;
    case DESCRIPTOR_NODE_ENUM:
        if (!node.has_name) { e = non_canonical(); break; }
        fields[0] = value_string_move(node.name);
        node.has_name = false;
        fields[1].tag = GROOVE_V_U64; fields[1].u.u64_ = node.registry_id;
        fields[2].tag = GROOVE_V_U32; fields[2].u.u32_ = node.children;
        nf = 3;
        break;
    case DESCRIPTOR_NODE_ENUM_CASE:
        if (!node.has_name) { e = non_canonical(); break; }
        fields[0] = value_string_move(node.name);
        node.has_name = false;
        nf = 1;
        break;
    default: break;
    }
    u32 tag = node.tag;
    codec_node_drop(&node);
    if (e.code) return e;
    const groove_EnumCase *c;
    e = groove__records__values__EnumSchema__case(persisted_descriptor_node_schema(), tag, &c);
    if (!e.code) {
        groove_EnumValue ev;
        e = groove__records__values__EnumValue__create(tag, c->payload, fields, nf, &ev);
        if (!e.code) { out->tag = GROOVE_V_ENUM; out->u.enum_ = ev; }
    }
    value_array_drop(fields, nf);
    return e;
}

static jz_err persisted_descriptor_node_from_value(const groove_Value *value, DescriptorCodecNode *out) {
    if (value->tag != GROOVE_V_ENUM) return non_canonical();
    const groove_EnumValue *ev = &value->u.enum_;
    if (ev->tag > 0xff) return non_canonical();
    u8 tag = (u8)ev->tag;
    groove_ValueVec fields = {0};
    JZ_TRY(groove__records__OwnedRecord__to_values(&ev->record, &fields));
    *out = codec_scalar(tag);
    jz_err e = JZ_OK;
    const groove_Value *f = fields.ptr;
    u32 n = fields.len;
    switch (tag) {
    case DESCRIPTOR_NODE_DESCRIPTOR:
    case DESCRIPTOR_NODE_TUPLE:
        if (n == 1 && f[0].tag == GROOVE_V_U32) out->children = f[0].u.u32_;
        else e = non_canonical();
        break;
    case DESCRIPTOR_NODE_FIELD:
        if (n == 1 && f[0].tag == GROOVE_V_NULLABLE) {
            e = codec_optional_string(&f[0], &out->has_name, &out->name);
            out->children = 1;
        } else e = non_canonical();
        break;
    case DESCRIPTOR_NODE_ENUM_TAG:
        if (n == 3 && f[0].tag == GROOVE_V_STRING && f[1].tag == GROOVE_V_U64 && f[2].tag == GROOVE_V_ARRAY) {
            out->has_name = true;
            out->name = jz_str_clone(&f[0].u.s);
            out->registry_id = f[1].u.u64_;
            e = codec_strings(&f[2].u.items, &out->strings);
        } else e = non_canonical();
        break;
    case DESCRIPTOR_NODE_ENUM:
        if (n == 3 && f[0].tag == GROOVE_V_STRING && f[1].tag == GROOVE_V_U64 && f[2].tag == GROOVE_V_U32) {
            out->has_name = true;
            out->name = jz_str_clone(&f[0].u.s);
            out->registry_id = f[1].u.u64_;
            out->children = f[2].u.u32_;
        } else e = non_canonical();
        break;
    case DESCRIPTOR_NODE_ENUM_CASE:
        if (n == 1 && f[0].tag == GROOVE_V_STRING) {
            out->has_name = true;
            out->name = jz_str_clone(&f[0].u.s);
            out->children = 1;
        } else e = non_canonical();
        break;
    case DESCRIPTOR_NODE_ARRAY:
    case DESCRIPTOR_NODE_NULLABLE:
    case DESCRIPTOR_NODE_RECORD:
        if (n == 0) out->children = 1;
        else e = non_canonical();
        break;
    default:
        if (!((tag >= DESCRIPTOR_NODE_U8 && tag <= DESCRIPTOR_NODE_UUID) || tag == DESCRIPTOR_NODE_U48) || n != 0) e = non_canonical();
        break;
    }
    if (e.code) codec_node_drop(out);
    groove__records__values__ValueVec__drop(&fields);
    return e;
}

jz_err groove__records__values__encode_persisted_record_descriptor(groove_RecordDescriptor d, jz_bytes *out) {
    return encode_descriptor_nodes(d, persisted_descriptor_envelope(), persisted_descriptor_node_value, out);
}

jz_err groove__records__values__decode_persisted_record_descriptor(const u8 *p, u32 n, groove_RecordDescriptor *out) {
    return decode_descriptor_nodes(p, n, persisted_descriptor_envelope(), persisted_descriptor_node_from_value,
                                   groove__records__values__encode_persisted_record_descriptor, out);
}

/* ===================================================================== ValueType methods */

bool groove__records__values__collect_by_ordered_scalar(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_NULLABLE: return groove__records__values__collect_by_ordered_scalar(t->u.inner);
    case GROOVE_VT_U8: case GROOVE_VT_U16: case GROOVE_VT_U32: case GROOVE_VT_U64: case GROOVE_VT_U48:
    case GROOVE_VT_I32: case GROOVE_VT_I64: case GROOVE_VT_F64: case GROOVE_VT_BOOL: case GROOVE_VT_STRING:
    case GROOVE_VT_BYTES: case GROOVE_VT_UUID: case GROOVE_VT_ENUM_TAG: return true;
    default: return false;
    }
}

const groove_ValueType *groove__records__values__ValueType__non_nullable(const groove_ValueType *t) {
    while (t->tag == GROOVE_VT_NULLABLE) t = t->u.inner;
    return t;
}

bool groove__records__values__ValueType__is_array_fk(const groove_ValueType *t) {
    return t->tag == GROOVE_VT_ARRAY && t->u.inner->tag == GROOVE_VT_UUID;
}

bool groove__records__values__ValueType__is_internal_storage_type(const groove_ValueType *t) { return t->tag == GROOVE_VT_INTERNAL; }

groove_ValueType groove__records__values__ValueType__raw_string(void) {
    groove_ValueType t = VT_SCALAR(GROOVE_VT_INTERNAL);
    t.u.internal.repr = GROOVE_IVT_RAW_STRING;
    return t;
}

groove_ValueType groove__records__values__ValueType__raw_bytes(void) {
    groove_ValueType t = VT_SCALAR(GROOVE_VT_INTERNAL);
    t.u.internal.repr = GROOVE_IVT_RAW_BYTES;
    return t;
}

groove_ValueType groove__records__values__ValueType__stored_scalar(groove_LargeValueKind kind) {
    groove_ValueType t = VT_SCALAR(GROOVE_VT_INTERNAL);
    t.u.internal.repr = GROOVE_IVT_STORED_SCALAR;
    t.u.internal.kind = (u8)kind;
    return t;
}

static bool fields_any_may_contain_stored_scalar(groove_RecordDescriptor d) {
    u32 n;
    const groove_DescriptorField *f = groove__records__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++) if (groove__records__values__ValueType__may_contain_stored_scalar(&f[i].value_type)) return true;
    return false;
}

bool groove__records__values__ValueType__may_contain_stored_scalar(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_STRING:
    case GROOVE_VT_BYTES: return true;
    case GROOVE_VT_INTERNAL: return t->u.internal.repr == GROOVE_IVT_STORED_SCALAR;
    case GROOVE_VT_TUPLE:
        for (u32 i = 0; i < t->u.tuple.len; i++)
            if (groove__records__values__ValueType__may_contain_stored_scalar(&t->u.tuple.ptr[i])) return true;
        return false;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: return groove__records__values__ValueType__may_contain_stored_scalar(t->u.inner);
    case GROOVE_VT_RECORD: return fields_any_may_contain_stored_scalar(t->u.record);
    case GROOVE_VT_ENUM:
        for (u32 i = 0; i < t->u.enum_->cases.len; i++)
            if (fields_any_may_contain_stored_scalar(t->u.enum_->cases.ptr[i].payload)) return true;
        return false;
    default: return false;
    }
}

static u32 fields_registry_occurrences(groove_RecordDescriptor d) {
    u32 n, sum = 0;
    const groove_DescriptorField *f = groove__records__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++) sum += groove__records__values__ValueType__variant_registry_occurrence_count(&f[i].value_type);
    return sum;
}

u32 groove__records__values__ValueType__variant_registry_occurrence_count(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_ENUM_TAG: return 1;
    case GROOVE_VT_ENUM: {
        u32 sum = 1;
        for (u32 i = 0; i < t->u.enum_->cases.len; i++) sum += fields_registry_occurrences(t->u.enum_->cases.ptr[i].payload);
        return sum;
    }
    case GROOVE_VT_TUPLE: {
        u32 sum = 0;
        for (u32 i = 0; i < t->u.tuple.len; i++) sum += groove__records__values__ValueType__variant_registry_occurrence_count(&t->u.tuple.ptr[i]);
        return sum;
    }
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: return groove__records__values__ValueType__variant_registry_occurrence_count(t->u.inner);
    case GROOVE_VT_RECORD: return fields_registry_occurrences(t->u.record);
    default: return 0;
    }
}

static bool opt_str_eq(bool ha, const jz_str *a, bool hb, const jz_str *b) {
    if (ha != hb) return false;
    return !ha || jz_str_eq(a, b);
}

typedef bool (*vt_rel_fn)(const groove_ValueType *, const groove_ValueType *);

/* fields equal length, pairwise name ==, and value types related by `rel` */
static bool fields_related(groove_RecordDescriptor a, groove_RecordDescriptor b, vt_rel_fn rel) {
    u32 na, nb;
    const groove_DescriptorField *fa = groove__records__RecordDescriptor__fields(a, &na);
    const groove_DescriptorField *fb = groove__records__RecordDescriptor__fields(b, &nb);
    if (na != nb) return false;
    for (u32 i = 0; i < na; i++) {
        if (!opt_str_eq(fa[i].has_name, &fa[i].name, fb[i].has_name, &fb[i].name)) return false;
        if (!rel(&fa[i].value_type, &fb[i].value_type)) return false;
    }
    return true;
}

static bool cases_related(const groove_EnumCase *a, const groove_EnumCase *b, u32 n, vt_rel_fn rel) {
    for (u32 i = 0; i < n; i++) {
        if (!jz_str_eq(&a[i].name, &b[i].name)) return false;
        if (!fields_related(a[i].payload, b[i].payload, rel)) return false;
    }
    return true;
}

static bool tuples_related(const groove_ValueTypeVec *a, const groove_ValueTypeVec *b, vt_rel_fn rel) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++) if (!rel(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

bool groove__records__values__ValueType__registry_compatible_with(const groove_ValueType *a, const groove_ValueType *b) {
    vt_rel_fn rel = groove__records__values__ValueType__registry_compatible_with;
    if (a->tag == GROOVE_VT_ENUM_TAG && b->tag == GROOVE_VT_ENUM_TAG && a->u.enum_tag.registry_id == b->u.enum_tag.registry_id)
        return strvec_starts_with(&a->u.enum_tag.variants, &b->u.enum_tag.variants) ||
               strvec_starts_with(&b->u.enum_tag.variants, &a->u.enum_tag.variants);
    if (a->tag == GROOVE_VT_ENUM && b->tag == GROOVE_VT_ENUM && a->u.enum_->registry_id == b->u.enum_->registry_id) {
        u32 shared = a->u.enum_->cases.len < b->u.enum_->cases.len ? a->u.enum_->cases.len : b->u.enum_->cases.len;
        return cases_related(a->u.enum_->cases.ptr, b->u.enum_->cases.ptr, shared, rel);
    }
    if (a->tag == GROOVE_VT_TUPLE && b->tag == GROOVE_VT_TUPLE) return tuples_related(&a->u.tuple, &b->u.tuple, rel);
    if ((a->tag == GROOVE_VT_ARRAY && b->tag == GROOVE_VT_ARRAY) || (a->tag == GROOVE_VT_NULLABLE && b->tag == GROOVE_VT_NULLABLE))
        return rel(a->u.inner, b->u.inner);
    if (a->tag == GROOVE_VT_RECORD && b->tag == GROOVE_VT_RECORD) return fields_related(a->u.record, b->u.record, rel);
    return groove__records__values__ValueType__PartialEq__eq(a, b);
}

bool groove__records__values__ValueType__registry_rebound_layout_compatible_with(const groove_ValueType *a, const groove_ValueType *b) {
    vt_rel_fn rel = groove__records__values__ValueType__registry_rebound_layout_compatible_with;
    if (a->tag == GROOVE_VT_ENUM_TAG && b->tag == GROOVE_VT_ENUM_TAG) return strvec_eq(&a->u.enum_tag.variants, &b->u.enum_tag.variants);
    if (a->tag == GROOVE_VT_ENUM && b->tag == GROOVE_VT_ENUM)
        return a->u.enum_->cases.len == b->u.enum_->cases.len &&
               cases_related(a->u.enum_->cases.ptr, b->u.enum_->cases.ptr, a->u.enum_->cases.len, rel);
    if (a->tag == GROOVE_VT_TUPLE && b->tag == GROOVE_VT_TUPLE) return tuples_related(&a->u.tuple, &b->u.tuple, rel);
    if ((a->tag == GROOVE_VT_ARRAY && b->tag == GROOVE_VT_ARRAY) || (a->tag == GROOVE_VT_NULLABLE && b->tag == GROOVE_VT_NULLABLE))
        return rel(a->u.inner, b->u.inner);
    if (a->tag == GROOVE_VT_RECORD && b->tag == GROOVE_VT_RECORD) return fields_related(a->u.record, b->u.record, rel);
    return groove__records__values__ValueType__PartialEq__eq(a, b);
}

bool groove__records__values__ValueType__can_evolve_registry_to(const groove_ValueType *a, const groove_ValueType *b) {
    vt_rel_fn rel = groove__records__values__ValueType__can_evolve_registry_to;
    if (a->tag == GROOVE_VT_ENUM_TAG && b->tag == GROOVE_VT_ENUM_TAG && a->u.enum_tag.registry_id == b->u.enum_tag.registry_id)
        return strvec_starts_with(&b->u.enum_tag.variants, &a->u.enum_tag.variants);
    if (a->tag == GROOVE_VT_ENUM && b->tag == GROOVE_VT_ENUM && a->u.enum_->registry_id == b->u.enum_->registry_id)
        return b->u.enum_->cases.len >= a->u.enum_->cases.len &&
               cases_related(a->u.enum_->cases.ptr, b->u.enum_->cases.ptr, a->u.enum_->cases.len, rel);
    if (a->tag == GROOVE_VT_TUPLE && b->tag == GROOVE_VT_TUPLE) return tuples_related(&a->u.tuple, &b->u.tuple, rel);
    if ((a->tag == GROOVE_VT_ARRAY && b->tag == GROOVE_VT_ARRAY) || (a->tag == GROOVE_VT_NULLABLE && b->tag == GROOVE_VT_NULLABLE))
        return rel(a->u.inner, b->u.inner);
    if (a->tag == GROOVE_VT_RECORD && b->tag == GROOVE_VT_RECORD) return fields_related(a->u.record, b->u.record, rel);
    return groove__records__values__ValueType__PartialEq__eq(a, b);
}

static void collect_fields_registries(groove_RecordDescriptor d, jz_btree *output) {
    u32 n;
    const groove_DescriptorField *f = groove__records__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++) groove__records__values__ValueType__collect_variant_registries(&f[i].value_type, output);
}

void groove__records__values__ValueType__collect_variant_registries(const groove_ValueType *t, jz_btree *output) {
    switch (t->tag) {
    case GROOVE_VT_ENUM_TAG: {
        groove_VariantRegistry r = {GROOVE_VR_ENUM_TAG};
        strvec_clone(&r.names, &t->u.enum_tag.variants);
        jz_btree_insert(output, &t->u.enum_tag.registry_id, &r, NULL);
        break;
    }
    case GROOVE_VT_TUPLE:
        for (u32 i = 0; i < t->u.tuple.len; i++) groove__records__values__ValueType__collect_variant_registries(&t->u.tuple.ptr[i], output);
        break;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: groove__records__values__ValueType__collect_variant_registries(t->u.inner, output); break;
    case GROOVE_VT_RECORD: collect_fields_registries(t->u.record, output); break;
    case GROOVE_VT_ENUM: {
        groove_VariantRegistry r = {GROOVE_VR_ENUM};
        for (u32 i = 0; i < t->u.enum_->cases.len; i++) {
            jz_str s = jz_str_clone(&t->u.enum_->cases.ptr[i].name);
            jz_vec_push(&r.names, &s, sizeof(s));
        }
        jz_btree_insert(output, &t->u.enum_->registry_id, &r, NULL);
        for (u32 i = 0; i < t->u.enum_->cases.len; i++) collect_fields_registries(t->u.enum_->cases.ptr[i].payload, output);
        break;
    }
    default: break;
    }
}

void groove__records__values__ValueType__stamp_variant_registries(groove_ValueType *t, const char *p, u32 n) {
    groove__records__values__ValueType__assign_variant_registries(t, p, n, false);
}

void groove__records__values__ValueType__rebind_variant_registries(groove_ValueType *t, const char *p, u32 n) {
    groove__records__values__ValueType__assign_variant_registries(t, p, n, true);
}

void groove__records__values__ValueType__assign_variant_registries(groove_ValueType *t, const char *path, u32 n, bool replace) {
    switch (t->tag) {
    case GROOVE_VT_ENUM_TAG:
        if ((t->u.enum_tag.registry_id & GROOVE_SYSTEM_REGISTRY_MARKER) == 0 && (replace || t->u.enum_tag.registry_id == 0))
            t->u.enum_tag.registry_id = groove__records__values__variant_registry_id_for_path(path, n);
        break;
    case GROOVE_VT_TUPLE:
        for (u32 index = 0; index < t->u.tuple.len; index++) {
            jz_str sub = jz_format("%.*s/tuple/%u", (int)n, path, index);
            groove__records__values__ValueType__assign_variant_registries(&t->u.tuple.ptr[index], sub.ptr, sub.len, replace);
            jz_str_drop(&sub);
        }
        break;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: {
        jz_str sub = jz_format(t->tag == GROOVE_VT_ARRAY ? "%.*s/array" : "%.*s/nullable", (int)n, path);
        groove__records__values__ValueType__assign_variant_registries(t->u.inner, sub.ptr, sub.len, replace);
        jz_str_drop(&sub);
        break;
    }
    case GROOVE_VT_RECORD: {
        jz_str sub = jz_format("%.*s/record", (int)n, path);
        t->u.record = assign_record_variant_registries(t->u.record, sub.ptr, sub.len, replace);
        jz_str_drop(&sub);
        break;
    }
    case GROOVE_VT_ENUM: {
        groove_EnumSchema *s = t->u.enum_;
        if (replace || s->registry_id == 0) s->registry_id = groove__records__values__variant_registry_id_for_path(path, n);
        for (u32 index = 0; index < s->cases.len; index++) {
            jz_str sub = jz_format("%.*s/case/%u", (int)n, path, index);
            s->cases.ptr[index].payload = assign_record_variant_registries(s->cases.ptr[index].payload, sub.ptr, sub.len, replace);
            jz_str_drop(&sub);
        }
        break;
    }
    default: break;
    }
}

groove_ValueType groove__records__values__ValueType__nullable(groove_ValueType t) {
    groove_ValueType out = VT_SCALAR(GROOVE_VT_NULLABLE);
    out.u.inner = JZ_NEW(groove_ValueType);
    *out.u.inner = t;
    return out;
}

groove_ValueType groove__records__values__ValueType__array_of(groove_ValueType t) {
    groove_ValueType out = VT_SCALAR(GROOVE_VT_ARRAY);
    out.u.inner = JZ_NEW(groove_ValueType);
    *out.u.inner = t;
    return out;
}

bool groove__records__values__ValueType__contains_record(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_TUPLE:
        for (u32 i = 0; i < t->u.tuple.len; i++) if (groove__records__values__ValueType__contains_record(&t->u.tuple.ptr[i])) return true;
        return false;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: return groove__records__values__ValueType__contains_record(t->u.inner);
    case GROOVE_VT_RECORD:
    case GROOVE_VT_ENUM: return true;
    default: return false;
    }
}

bool groove__records__values__ValueType__fixed_size(const groove_ValueType *t, u32 *out) {
    switch (t->tag) {
    case GROOVE_VT_U8: case GROOVE_VT_BOOL: *out = 1; return true;
    case GROOVE_VT_U16: *out = 2; return true;
    case GROOVE_VT_U64: case GROOVE_VT_I64: *out = 8; return true;
    case GROOVE_VT_U32: case GROOVE_VT_I32: *out = 4; return true;
    case GROOVE_VT_U48: *out = 6; return true;
    case GROOVE_VT_F64: *out = 8; return true;
    case GROOVE_VT_UUID: *out = 16; return true;
    case GROOVE_VT_ENUM_TAG: *out = 1; return true;
    case GROOVE_VT_TUPLE: {
        u32 total = 0, m;
        for (u32 i = 0; i < t->u.tuple.len; i++) {
            if (!groove__records__values__ValueType__fixed_size(&t->u.tuple.ptr[i], &m)) return false;
            total += m;
        }
        *out = total;
        return true;
    }
    case GROOVE_VT_NULLABLE: {
        u32 m;
        if (!groove__records__values__ValueType__fixed_size(t->u.inner, &m)) return false;
        *out = m + 1;
        return true;
    }
    default: return false; /* String | Bytes | Internal | Array | Record | Enum */
    }
}

bool groove__records__values__ValueType__is_fixed_size(const groove_ValueType *t) {
    u32 unused;
    return groove__records__values__ValueType__fixed_size(t, &unused);
}

/* ===================================================================== value codecs */

static jz_err encode_tuple(jz_bytes *bytes, const groove_ValueVec *values, const groove_ValueTypeVec *members);
static jz_err encode_array(jz_bytes *bytes, const groove_ValueVec *values, const groove_ValueType *element_type);
static jz_err encode_nullable(jz_bytes *bytes, const groove_Value *value, const groove_ValueType *inner_type);
static jz_err decode_tuple(const u8 *p, u32 n, const groove_ValueTypeVec *members, groove_Value *out);
static jz_err decode_array(const u8 *p, u32 n, const groove_ValueType *element_type, groove_Value *out);
static jz_err decode_nullable(const u8 *p, u32 n, const groove_ValueType *inner_type, groove_Value *out);
static jz_err ensure_enum_value(const groove_EnumValue *v, const groove_EnumSchema *schema);
static jz_err validate_value_inner(const u8 *p, u32 n, const groove_ValueType *t, bool require_constructible);
static jz_err read_u32_at(const u8 *p, u32 n, u32 start, u32 *out);
static jz_err checked_array_table_end(u32 prefix_size, u32 count, u32 *out);
static jz_err checked_offset_slot(u32 start, u32 index, u32 *out);

static jz_err unexpected_eof(void) { return groove__records__Error__simple(GROOVE_REC_E_UnexpectedEof); }
static jz_err invalid_offset(void) { return groove__records__Error__simple(GROOVE_REC_E_InvalidOffset); }
static jz_err length_overflow(void) { return groove__records__Error__simple(GROOVE_REC_E_LengthOverflow); }

jz_err groove__records__values__encode_value(const groove_Value *v, const groove_ValueType *t, jz_bytes *out) {
    jz_bytes bytes = {0};
    jz_err e = groove__records__values__encode_value_into(&bytes, v, t);
    if (e.code) { jz_bytes_drop(&bytes); return e; }
    *out = bytes;
    return JZ_OK;
}

static jz_err encode_chunked(jz_bytes *bytes, groove_LargeValueKind kind, const groove_LargeValueRef *r) {
    groove_StoredScalar s;
    memset(&s, 0, sizeof(s));
    s.tag = GROOVE_STORED_CHUNKED;
    groove__large_values__LargeValueRef__Clone__clone(&s.chunked, r);
    jz_bytes encoded = {0};
    jz_err e = groove__large_values__encode_stored_scalar(kind, &s, &encoded);
    groove__large_values__StoredScalar__drop(&s);
    if (e.code) return e;
    jz_bytes_push(bytes, encoded.ptr, encoded.len);
    jz_bytes_drop(&encoded);
    return JZ_OK;
}

jz_err groove__records__values__encode_value_into(jz_bytes *bytes, const groove_Value *v, const groove_ValueType *t) {
    u8 tt = t->tag, vt = v->tag;
    bool raw_string = tt == GROOVE_VT_INTERNAL && t->u.internal.repr == GROOVE_IVT_RAW_STRING;
    bool raw_bytes = tt == GROOVE_VT_INTERNAL && t->u.internal.repr == GROOVE_IVT_RAW_BYTES;
    bool stored = tt == GROOVE_VT_INTERNAL && t->u.internal.repr == GROOVE_IVT_STORED_SCALAR;
    u8 kind = stored ? t->u.internal.kind : 0;
    if (vt == GROOVE_V_STRING && tt == GROOVE_VT_STRING)
        return groove__large_values__encode_primitive_stored_scalar_into(GROOVE_LVK_STRING, (const u8 *)v->u.s.ptr, v->u.s.len, bytes);
    if (vt == GROOVE_V_BYTES && tt == GROOVE_VT_BYTES)
        return groove__large_values__encode_primitive_stored_scalar_into(GROOVE_LVK_BYTES, v->u.bytes.ptr, v->u.bytes.len, bytes);
    if (vt == GROOVE_V_STRING && raw_string) { jz_bytes_push(bytes, (const u8 *)v->u.s.ptr, v->u.s.len); return JZ_OK; }
    if (vt == GROOVE_V_BYTES && raw_bytes) { jz_bytes_push(bytes, v->u.bytes.ptr, v->u.bytes.len); return JZ_OK; }
    if (vt == GROOVE_V_BYTES && stored && kind == GROOVE_LVK_BYTES)
        return groove__large_values__encode_primitive_stored_scalar_into(GROOVE_LVK_BYTES, v->u.bytes.ptr, v->u.bytes.len, bytes);
    if (vt == GROOVE_V_STRING && stored && (kind == GROOVE_LVK_STRING || kind == GROOVE_LVK_JSON))
        return groove__large_values__encode_primitive_stored_scalar_into(kind, (const u8 *)v->u.s.ptr, v->u.s.len, bytes);
    if (vt == GROOVE_V_LARGE && stored && v->u.large->kind == kind) return encode_chunked(bytes, kind, v->u.large);
    if (vt == GROOVE_V_LARGE && tt == GROOVE_VT_STRING && v->u.large->kind == GROOVE_LVK_STRING)
        return encode_chunked(bytes, GROOVE_LVK_STRING, v->u.large);
    if (vt == GROOVE_V_LARGE && tt == GROOVE_VT_BYTES && v->u.large->kind == GROOVE_LVK_BYTES)
        return encode_chunked(bytes, GROOVE_LVK_BYTES, v->u.large);
    if (vt == GROOVE_V_UUID && tt == GROOVE_VT_UUID) { jz_bytes_push(bytes, v->u.uuid.b, 16); return JZ_OK; }
    if (vt == GROOVE_V_STRING && tt == GROOVE_VT_ENUM_TAG) {
        u8 d;
        JZ_TRY(groove__records__values__ScalarEnumSchema__discriminant(&t->u.enum_tag, v->u.s.ptr, v->u.s.len, &d));
        jz_bytes_push_u8(bytes, d);
        return JZ_OK;
    }
    if (vt == GROOVE_V_ENUM_TAG && tt == GROOVE_VT_ENUM_TAG) { jz_bytes_push_u8(bytes, v->u.enum_tag); return JZ_OK; }
    if (vt == GROOVE_V_TUPLE && tt == GROOVE_VT_TUPLE) return encode_tuple(bytes, &v->u.items, &t->u.tuple);
    if (vt == GROOVE_V_ARRAY && tt == GROOVE_VT_ARRAY) return encode_array(bytes, &v->u.items, t->u.inner);
    if (vt == GROOVE_V_NULLABLE && tt == GROOVE_VT_NULLABLE) return encode_nullable(bytes, v->u.nullable, t->u.inner);
    if (vt == GROOVE_V_RECORD && tt == GROOVE_VT_RECORD) {
        JZ_TRY(groove__records__values__ensure_value_type(v, t));
        jz_bytes_push(bytes, v->u.record.raw.ptr, v->u.record.raw.len);
        return JZ_OK;
    }
    if (vt == GROOVE_V_ENUM && tt == GROOVE_VT_ENUM) {
        JZ_TRY(ensure_enum_value(&v->u.enum_, t->u.enum_));
        groove__records__append_variant_record(bytes, v->u.enum_.tag, v->u.enum_.record.raw.ptr, v->u.enum_.record.raw.len);
        return JZ_OK;
    }
    if (groove__records__values__ValueType__is_fixed_size(t)) return groove__records__values__encode_fixed_value(bytes, v, t);
    return groove__records__Error__type_mismatch(t);
}

jz_err groove__records__values__encode_fixed_value(jz_bytes *bytes, const groove_Value *v, const groove_ValueType *t) {
    u8 b[16];
    u8 tt = t->tag;
    switch (v->tag) {
    case GROOVE_V_U8: if (tt != GROOVE_VT_U8) break; jz_bytes_push_u8(bytes, v->u.u8_); return JZ_OK;
    case GROOVE_V_U16: if (tt != GROOVE_VT_U16) break; put_le16(b, v->u.u16_); jz_bytes_push(bytes, b, 2); return JZ_OK;
    case GROOVE_V_U32: if (tt != GROOVE_VT_U32) break; put_le32(b, v->u.u32_); jz_bytes_push(bytes, b, 4); return JZ_OK;
    case GROOVE_V_U64: if (tt != GROOVE_VT_U64) break; put_le64(b, v->u.u64_); jz_bytes_push(bytes, b, 8); return JZ_OK;
    case GROOVE_V_U48:
        if (tt != GROOVE_VT_U48) break;
        JZ_TRY(u48_le_bytes(v->u.u64_, b));
        jz_bytes_push(bytes, b, 6);
        return JZ_OK;
    case GROOVE_V_I32: if (tt != GROOVE_VT_I32) break; put_le32(b, (u32)v->u.i32_); jz_bytes_push(bytes, b, 4); return JZ_OK;
    case GROOVE_V_I64: if (tt != GROOVE_VT_I64) break; put_le64(b, (u64)v->u.i64_); jz_bytes_push(bytes, b, 8); return JZ_OK;
    case GROOVE_V_F64: {
        if (tt != GROOVE_VT_F64) break;
        if (v->u.f64_ != v->u.f64_) return groove__records__Error__simple(GROOVE_REC_E_InvalidF64NaN);
        u64 bits;
        memcpy(&bits, &v->u.f64_, 8);
        put_le64(b, bits);
        jz_bytes_push(bytes, b, 8);
        return JZ_OK;
    }
    case GROOVE_V_BOOL: if (tt != GROOVE_VT_BOOL) break; jz_bytes_push_u8(bytes, v->u.b ? 1 : 0); return JZ_OK;
    case GROOVE_V_UUID: if (tt != GROOVE_VT_UUID) break; jz_bytes_push(bytes, v->u.uuid.b, 16); return JZ_OK;
    case GROOVE_V_STRING: {
        if (tt != GROOVE_VT_ENUM_TAG) break;
        u8 d;
        JZ_TRY(groove__records__values__ScalarEnumSchema__discriminant(&t->u.enum_tag, v->u.s.ptr, v->u.s.len, &d));
        jz_bytes_push_u8(bytes, d);
        return JZ_OK;
    }
    case GROOVE_V_ENUM_TAG: if (tt != GROOVE_VT_ENUM_TAG) break; jz_bytes_push_u8(bytes, v->u.enum_tag); return JZ_OK;
    case GROOVE_V_TUPLE: if (tt != GROOVE_VT_TUPLE) break; return encode_tuple(bytes, &v->u.items, &t->u.tuple);
    case GROOVE_V_NULLABLE: if (tt != GROOVE_VT_NULLABLE) break; return encode_nullable(bytes, v->u.nullable, t->u.inner);
    default: break;
    }
    /* (_, Record) | (_, Enum) | _ all report the expected type */
    return groove__records__Error__type_mismatch(t);
}

static jz_err read_exact(const u8 *p, u32 n, u32 want) {
    if (n != want) return unexpected_eof();
    (void)p;
    return JZ_OK;
}

static jz_err map_string_scalar_error(jz_err e) {
    if (e.code == GROOVE_LV_E_InvalidUtf8) {
        jz_err_drop(&e);
        return groove__records__Error__simple(GROOVE_REC_E_InvalidUtf8);
    }
    return e; /* Error::LargeValue(other) is transparent */
}

/* Shared tail of the String/Bytes/StoredScalar arms of decode_value. */
static jz_err decode_scalar_result(groove_StoredScalar *s, groove_LargeValueKind kind, bool as_string, const groove_ValueType *t, groove_Value *out) {
    if (s->tag == GROOVE_STORED_PRIMITIVE) {
        if (as_string) {
            if (!jz_utf8_valid(s->primitive.ptr, s->primitive.len)) {
                jz_bytes_drop(&s->primitive);
                return groove__records__Error__simple(GROOVE_REC_E_InvalidUtf8);
            }
            out->tag = GROOVE_V_STRING;
            out->u.s = str_from_bytes_move(s->primitive);
        } else {
            out->tag = GROOVE_V_BYTES;
            out->u.bytes = s->primitive;
        }
        return JZ_OK;
    }
    if (s->chunked.kind == kind) {
        out->tag = GROOVE_V_LARGE;
        out->u.large = JZ_NEW(groove_LargeValueRef);
        *out->u.large = s->chunked;
        return JZ_OK;
    }
    groove__large_values__StoredScalar__drop(s);
    return groove__records__Error__type_mismatch(t);
}

jz_err groove__records__values__decode_value(const u8 *p, u32 n, const groove_ValueType *t, groove_Value *out) {
    memset(out, 0, sizeof(*out));
    switch (t->tag) {
    case GROOVE_VT_U8: JZ_TRY(read_exact(p, n, 1)); out->tag = GROOVE_V_U8; out->u.u8_ = p[0]; return JZ_OK;
    case GROOVE_VT_U16: JZ_TRY(read_exact(p, n, 2)); out->tag = GROOVE_V_U16; out->u.u16_ = get_le16(p); return JZ_OK;
    case GROOVE_VT_U32: JZ_TRY(read_exact(p, n, 4)); out->tag = GROOVE_V_U32; out->u.u32_ = get_le32(p); return JZ_OK;
    case GROOVE_VT_U64: JZ_TRY(read_exact(p, n, 8)); out->tag = GROOVE_V_U64; out->u.u64_ = get_le64(p); return JZ_OK;
    case GROOVE_VT_U48: JZ_TRY(read_exact(p, n, 6)); out->tag = GROOVE_V_U48; out->u.u64_ = groove__records__values__u48_from_le_bytes(p); return JZ_OK;
    case GROOVE_VT_I32: JZ_TRY(read_exact(p, n, 4)); out->tag = GROOVE_V_I32; out->u.i32_ = (i32)get_le32(p); return JZ_OK;
    case GROOVE_VT_I64: JZ_TRY(read_exact(p, n, 8)); out->tag = GROOVE_V_I64; out->u.i64_ = (i64)get_le64(p); return JZ_OK;
    case GROOVE_VT_F64: {
        JZ_TRY(read_exact(p, n, 8));
        u64 bits = get_le64(p);
        f64 value;
        memcpy(&value, &bits, 8);
        if (value != value) return groove__records__Error__simple(GROOVE_REC_E_InvalidF64NaN);
        out->tag = GROOVE_V_F64;
        out->u.f64_ = value;
        return JZ_OK;
    }
    case GROOVE_VT_BOOL:
        JZ_TRY(read_exact(p, n, 1));
        if (p[0] > 1) return invalid_bool(p[0]);
        out->tag = GROOVE_V_BOOL;
        out->u.b = p[0] == 1;
        return JZ_OK;
    case GROOVE_VT_STRING: {
        groove_StoredScalar s;
        jz_err e = groove__large_values__decode_stored_scalar(GROOVE_LVK_STRING, p, n, &s);
        if (e.code) return map_string_scalar_error(e);
        return decode_scalar_result(&s, GROOVE_LVK_STRING, true, t, out);
    }
    case GROOVE_VT_BYTES: {
        groove_StoredScalar s;
        JZ_TRY(groove__large_values__decode_stored_scalar(GROOVE_LVK_BYTES, p, n, &s));
        return decode_scalar_result(&s, GROOVE_LVK_BYTES, false, t, out);
    }
    case GROOVE_VT_INTERNAL:
        switch (t->u.internal.repr) {
        case GROOVE_IVT_RAW_STRING:
            if (!jz_utf8_valid(p, n)) return groove__records__Error__simple(GROOVE_REC_E_InvalidUtf8);
            out->tag = GROOVE_V_STRING;
            out->u.s = jz_str_from_slice(jz_slice_of(p, n));
            return JZ_OK;
        case GROOVE_IVT_RAW_BYTES:
            out->tag = GROOVE_V_BYTES;
            out->u.bytes = jz_bytes_from(p, n);
            return JZ_OK;
        default: {
            groove_LargeValueKind kind = (groove_LargeValueKind)t->u.internal.kind;
            groove_StoredScalar s;
            JZ_TRY(groove__large_values__decode_stored_scalar(kind, p, n, &s));
            return decode_scalar_result(&s, kind, kind != GROOVE_LVK_BYTES, t, out);
        }
        }
    case GROOVE_VT_UUID:
        JZ_TRY(read_exact(p, n, 16));
        out->tag = GROOVE_V_UUID;
        memcpy(out->u.uuid.b, p, 16);
        return JZ_OK;
    case GROOVE_VT_ENUM_TAG: {
        JZ_TRY(read_exact(p, n, 1));
        const jz_str *unused;
        JZ_TRY(groove__records__values__ScalarEnumSchema__variant(&t->u.enum_tag, p[0], &unused));
        out->tag = GROOVE_V_ENUM_TAG;
        out->u.enum_tag = p[0];
        return JZ_OK;
    }
    case GROOVE_VT_TUPLE: return decode_tuple(p, n, &t->u.tuple, out);
    case GROOVE_VT_ARRAY: return decode_array(p, n, t->u.inner, out);
    case GROOVE_VT_NULLABLE: return decode_nullable(p, n, t->u.inner, out);
    case GROOVE_VT_RECORD:
        /* Nested values are lazy records, just like top-level OwnedRecord. */
        out->tag = GROOVE_V_RECORD;
        out->u.record = groove__records__OwnedRecord__new(jz_bytes_from(p, n), t->u.record);
        return JZ_OK;
    case GROOVE_VT_ENUM: {
        u32 tag;
        jz_slice payload;
        jz_err e = groove__records__split_variant_record(p, n, &tag, &payload);
        if (e.code) {
            if (e.code == GROOVE_REC_E_InvalidSchemaVersionHeader) {
                jz_err_drop(&e);
                return groove__records__Error__simple(GROOVE_REC_E_InvalidEnumHeader);
            }
            return e;
        }
        const groove_EnumCase *c;
        JZ_TRY(groove__records__values__EnumSchema__case(t->u.enum_, tag, &c));
        out->tag = GROOVE_V_ENUM;
        out->u.enum_ = groove__records__values__EnumValue__new(tag, groove__records__OwnedRecord__new(jz_bytes_from(payload.ptr, payload.len), c->payload));
        return JZ_OK;
    }
    default: JZ_UNREACHABLE();
    }
}

jz_err groove__records__values__validate_value(const u8 *p, u32 n, const groove_ValueType *t) { return validate_value_inner(p, n, t, false); }
jz_err groove__records__values__validate_canonical_value(const u8 *p, u32 n, const groove_ValueType *t) { return validate_value_inner(p, n, t, true); }

static jz_err validate_inline_scalar(groove_LargeValueKind kind, const u8 *p, u32 n) {
    jz_slice unused;
    jz_err e = groove__large_values__inline_scalar_bytes(kind, p, n, &unused);
    if (e.code == GROOVE_LV_E_RequiresEvaluation) { jz_err_drop(&e); return JZ_OK; }
    return e;
}

static jz_err validate_tuple(const u8 *p, u32 n, const groove_ValueTypeVec *members, bool rc);
static jz_err validate_array(const u8 *p, u32 n, const groove_ValueType *element_type, bool rc);
static jz_err validate_nullable(const u8 *p, u32 n, const groove_ValueType *inner_type, bool rc);

static jz_err split_enum_header(const u8 *p, u32 n, u32 *tag, jz_slice *payload) {
    jz_err e = groove__records__split_variant_record(p, n, tag, payload);
    if (e.code == GROOVE_REC_E_InvalidSchemaVersionHeader) {
        jz_err_drop(&e);
        return groove__records__Error__simple(GROOVE_REC_E_InvalidEnumHeader);
    }
    return e;
}

static jz_err validate_value_inner(const u8 *p, u32 n, const groove_ValueType *t, bool require_constructible) {
    switch (t->tag) {
    case GROOVE_VT_U8: return read_exact(p, n, 1);
    case GROOVE_VT_U16: return read_exact(p, n, 2);
    case GROOVE_VT_U32:
    case GROOVE_VT_I32: return read_exact(p, n, 4);
    case GROOVE_VT_U64:
    case GROOVE_VT_I64: return read_exact(p, n, 8);
    case GROOVE_VT_U48: return read_exact(p, n, 6);
    case GROOVE_VT_F64: {
        JZ_TRY(read_exact(p, n, 8));
        u64 bits = get_le64(p);
        f64 value;
        memcpy(&value, &bits, 8);
        /* NaN has no representation in any persisted record state. */
        if (value != value) return groove__records__Error__simple(GROOVE_REC_E_InvalidF64NaN);
        return JZ_OK;
    }
    case GROOVE_VT_BOOL:
        JZ_TRY(read_exact(p, n, 1));
        if (p[0] > 1) return invalid_bool(p[0]);
        return JZ_OK;
    case GROOVE_VT_STRING:
    case GROOVE_VT_BYTES: {
        groove_LargeValueKind kind;
        if (t->tag == GROOVE_VT_STRING) kind = GROOVE_LVK_STRING;
        else if (t->tag == GROOVE_VT_BYTES) kind = GROOVE_LVK_BYTES;
        else jz_panic("matched string or bytes value type");
        return validate_inline_scalar(kind, p, n);
    }
    case GROOVE_VT_INTERNAL:
        switch (t->u.internal.repr) {
        case GROOVE_IVT_RAW_STRING:
            if (!jz_utf8_valid(p, n)) return groove__records__Error__simple(GROOVE_REC_E_InvalidUtf8);
            return JZ_OK;
        case GROOVE_IVT_RAW_BYTES: return JZ_OK;
        default: return validate_inline_scalar((groove_LargeValueKind)t->u.internal.kind, p, n);
        }
    case GROOVE_VT_UUID: return read_exact(p, n, 16);
    case GROOVE_VT_ENUM_TAG: {
        JZ_TRY(read_exact(p, n, 1));
        const jz_str *unused;
        return groove__records__values__ScalarEnumSchema__variant(&t->u.enum_tag, p[0], &unused);
    }
    case GROOVE_VT_TUPLE: return validate_tuple(p, n, &t->u.tuple, require_constructible);
    case GROOVE_VT_ARRAY: return validate_array(p, n, t->u.inner, require_constructible);
    case GROOVE_VT_NULLABLE: return validate_nullable(p, n, t->u.inner, require_constructible);
    case GROOVE_VT_RECORD: {
        groove_BorrowedRecord r = groove__records__RecordDescriptor__bind(t->u.record, p, n);
        return groove__records__BorrowedRecord__validate_canonical(&r);
    }
    case GROOVE_VT_ENUM: {
        u32 tag;
        jz_slice payload;
        JZ_TRY(split_enum_header(p, n, &tag, &payload));
        const groove_EnumCase *c;
        JZ_TRY(groove__records__values__EnumSchema__case(t->u.enum_, tag, &c));
        groove_BorrowedRecord r = groove__records__RecordDescriptor__bind(c->payload, payload.ptr, payload.len);
        return groove__records__BorrowedRecord__validate_canonical(&r);
    }
    default: JZ_UNREACHABLE();
    }
}

static bool any_nonzero(const u8 *p, u32 n) {
    for (u32 i = 0; i < n; i++) if (p[i]) return true;
    return false;
}

static jz_err validate_nullable(const u8 *p, u32 n, const groove_ValueType *inner_type, bool rc) {
    if (n == 0) return unexpected_eof();
    u8 flag = p[0];
    u32 size;
    bool fixed = groove__records__values__ValueType__fixed_size(inner_type, &size);
    switch (flag) {
    case 0:
        if (fixed && any_nonzero(p + 1, n - 1)) return invalid_offset();
        if (!fixed && n - 1 != 0) return invalid_offset();
        return JZ_OK;
    case 1: return validate_value_inner(p + 1, n - 1, inner_type, rc);
    default: return invalid_null_flag(flag);
    }
}

static jz_err validate_array(const u8 *p, u32 n, const groove_ValueType *element_type, bool rc) {
    u32 element_size;
    if (groove__records__values__ValueType__fixed_size(element_type, &element_size)) {
        if (element_size == 0 || n % element_size != 0) return invalid_offset();
        for (u32 off = 0; off < n; off += element_size) JZ_TRY(validate_value_inner(p + off, element_size, element_type, rc));
        return JZ_OK;
    }
    u32 count;
    JZ_TRY(read_u32_at(p, n, 0, &count));
    if (count == 0) return n == 4 ? JZ_OK : invalid_offset();
    u32 values_start;
    if (checked_array_table_end(4, count, &values_start).code) return invalid_offset();
    if (n < values_start) return unexpected_eof();
    u32 start = values_start;
    for (u32 index = 0; index < count; index++) {
        u32 end;
        if (index + 1 == count) {
            end = n;
        } else {
            u32 slot;
            if (checked_offset_slot(4, index, &slot).code) return invalid_offset();
            JZ_TRY(read_u32_at(p, n, slot, &end));
        }
        if (end < start || end > n) return invalid_offset();
        JZ_TRY(validate_value_inner(p + start, end - start, element_type, rc));
        start = end;
    }
    return JZ_OK;
}

static jz_err validate_tuple(const u8 *p, u32 n, const groove_ValueTypeVec *members, bool rc) {
    u32 offset = 0;
    for (u32 i = 0; i < members->len; i++) {
        const groove_ValueType *member = &members->ptr[i];
        u32 width, end;
        if (!groove__records__values__ValueType__fixed_size(member, &width)) return groove__records__Error__invalid_tuple_member(member);
        JZ_TRY(groove__records__values__checked_add(offset, width, &end));
        if (end > n) return unexpected_eof();
        JZ_TRY(validate_value_inner(p + offset, width, member, rc));
        offset = end;
    }
    return offset == n ? JZ_OK : invalid_offset();
}

static jz_err encode_nullable(jz_bytes *bytes, const groove_Value *value, const groove_ValueType *inner_type) {
    if (value) {
        jz_bytes_push_u8(bytes, 1);
        /* Fixed payloads need no temporary Vec, including nested nullable values. */
        if (groove__records__values__ValueType__is_fixed_size(inner_type)) return groove__records__values__encode_fixed_value(bytes, value, inner_type);
        return groove__records__values__encode_value_into(bytes, value, inner_type);
    }
    jz_bytes_push_u8(bytes, 0);
    u32 size;
    if (groove__records__values__ValueType__fixed_size(inner_type, &size)) bytes_resize_zero(bytes, bytes->len + size);
    return JZ_OK;
}

jz_err groove__records__values__visit_encoded_indirect_values(const u8 *p, u32 n, const groove_ValueType *t, groove_IndirectVisitor visit, void *ctx, bool *out) {
    switch (t->tag) {
    case GROOVE_VT_STRING:
    case GROOVE_VT_BYTES:
    case GROOVE_VT_INTERNAL: {
        if (t->tag == GROOVE_VT_INTERNAL && t->u.internal.repr != GROOVE_IVT_STORED_SCALAR) break;
        u32 tag;
        jz_slice unused;
        JZ_TRY(groove__records__split_variant_record(p, n, &tag, &unused));
        switch (tag) {
        case 0: *out = false; return JZ_OK;
        case 1: return visit(ctx, p, n, t, out);
        default: return groove__large_values__Error__new(GROOVE_LV_E_MalformedScalar);
        }
    }
    case GROOVE_VT_NULLABLE:
        if (n == 0) return unexpected_eof();
        switch (p[0]) {
        case 0: *out = false; return JZ_OK;
        case 1: return groove__records__values__visit_encoded_indirect_values(p + 1, n - 1, t->u.inner, visit, ctx, out);
        default: return invalid_null_flag(p[0]);
        }
    case GROOVE_VT_RECORD:
        return groove__records__RecordDescriptor__visit_encoded_indirect_fields(t->u.record, p, n, NULL, 0, visit, ctx, out);
    case GROOVE_VT_ENUM: {
        u32 tag;
        jz_slice payload;
        JZ_TRY(groove__records__split_variant_record(p, n, &tag, &payload));
        const groove_EnumCase *c;
        JZ_TRY(groove__records__values__EnumSchema__case(t->u.enum_, tag, &c));
        return groove__records__RecordDescriptor__visit_encoded_indirect_fields(c->payload, payload.ptr, payload.len, NULL, 0, visit, ctx, out);
    }
    case GROOVE_VT_ARRAY: {
        if (!groove__records__values__ValueType__may_contain_stored_scalar(t->u.inner)) break;
        /* Indirect-capable array elements are variable-width. */
        u32 count, start;
        JZ_TRY(read_u32_at(p, n, 0, &count));
        if (checked_array_table_end(4, count, &start).code) return invalid_offset();
        if (start > n) return unexpected_eof();
        for (u32 index = 0; index < count; index++) {
            u32 end;
            if (index + 1 == count) {
                end = n;
            } else {
                u32 slot;
                if (checked_offset_slot(4, index, &slot).code) return invalid_offset();
                JZ_TRY(read_u32_at(p, n, slot, &end));
            }
            if (start > end || end > n) return invalid_offset();
            bool hit;
            JZ_TRY(groove__records__values__visit_encoded_indirect_values(p + start, end - start, t->u.inner, visit, ctx, &hit));
            if (hit) { *out = true; return JZ_OK; }
            start = end;
        }
        *out = false;
        return JZ_OK;
    }
    default: break;
    }
    /* Tuples are restricted to fixed-size members; raw engine fields and other scalars
     * cannot contain an indirect scalar either. */
    *out = false;
    return JZ_OK;
}

static jz_err decode_nullable(const u8 *p, u32 n, const groove_ValueType *inner_type, groove_Value *out) {
    if (n == 0) return unexpected_eof();
    u8 flag = p[0];
    switch (flag) {
    case 0: {
        u32 size;
        if (groove__records__values__ValueType__fixed_size(inner_type, &size)) {
            if (any_nonzero(p + 1, n - 1)) return invalid_offset();
        } else if (n - 1 != 0) {
            return invalid_offset();
        }
        out->tag = GROOVE_V_NULLABLE;
        out->u.nullable = NULL;
        return JZ_OK;
    }
    case 1: {
        groove_Value *inner = JZ_NEW(groove_Value);
        jz_err e = groove__records__values__decode_value(p + 1, n - 1, inner_type, inner);
        if (e.code) { jz_free(inner); return e; }
        out->tag = GROOVE_V_NULLABLE;
        out->u.nullable = inner;
        return JZ_OK;
    }
    default: return invalid_null_flag(flag);
    }
}

static jz_err encode_array(jz_bytes *bytes, const groove_ValueVec *values, const groove_ValueType *element_type) {
    for (u32 i = 0; i < values->len; i++) JZ_TRY(groove__records__values__ensure_value_type(&values->ptr[i], element_type));
    if (groove__records__values__ValueType__is_fixed_size(element_type)) {
        for (u32 i = 0; i < values->len; i++) JZ_TRY(groove__records__values__encode_fixed_value(bytes, &values->ptr[i], element_type));
        return JZ_OK;
    }
    u32 base = bytes->len;
    u32 count;
    JZ_TRY(groove__records__values__usize_to_u32(values->len, &count));
    groove__records__values__write_u32(bytes, count);
    u32 offset_count = values->len ? values->len - 1 : 0;
    u32 payload_start;
    JZ_TRY(checked_array_table_end(bytes->len, values->len, &payload_start));
    bytes_resize_zero(bytes, payload_start);
    for (u32 index = 0; index < values->len; index++) {
        JZ_TRY(groove__records__values__encode_value_into(bytes, &values->ptr[index], element_type));
        if (index < offset_count) {
            /* Offsets belong to this array, even when nested inside another container. */
            u32 end, base4, slot, slot_end;
            JZ_TRY(groove__records__values__usize_to_u32(bytes->len - base, &end));
            JZ_TRY(groove__records__values__checked_add(base, 4, &base4));
            JZ_TRY(checked_offset_slot(base4, index, &slot));
            JZ_TRY(groove__records__values__checked_add(slot, 4, &slot_end));
            put_le32(bytes->ptr + slot, end);
        }
    }
    return JZ_OK;
}

static jz_err decode_array(const u8 *p, u32 n, const groove_ValueType *element_type, groove_Value *out) {
    u32 element_size;
    groove_ValueVec values = {0};
    if (groove__records__values__ValueType__fixed_size(element_type, &element_size)) {
        if (element_size == 0 || n % element_size != 0) return invalid_offset();
        for (u32 off = 0; off < n; off += element_size) {
            groove_Value v;
            jz_err e = groove__records__values__decode_value(p + off, element_size, element_type, &v);
            if (e.code) { groove__records__values__ValueVec__drop(&values); return e; }
            jz_vec_push(&values, &v, sizeof(v));
        }
        out->tag = GROOVE_V_ARRAY;
        out->u.items = values;
        return JZ_OK;
    }
    u32 count;
    JZ_TRY(read_u32_at(p, n, 0, &count));
    if (count == 0) {
        if (n != 4) return invalid_offset();
        out->tag = GROOVE_V_ARRAY;
        out->u.items = values;
        return JZ_OK;
    }
    u32 values_start;
    if (checked_array_table_end(4, count, &values_start).code) return invalid_offset();
    if (n < values_start) return unexpected_eof();
    /* read_offsets(bytes, 4, count - 1) then push(bytes.len()) */
    JZ_VEC(u32) ends = {0};
    jz_vec_reserve(&ends, count, sizeof(u32));
    for (u32 idx = 0; idx + 1 < count; idx++) {
        u32 slot, end;
        jz_err e = checked_offset_slot(4, idx, &slot);
        if (e.code) { jz_err_drop(&e); e = invalid_offset(); }
        if (!e.code) e = read_u32_at(p, n, slot, &end);
        if (e.code) { jz_vec_drop(&ends, sizeof(u32), NULL); return e; }
        JZ_VEC_PUSH(ends, end);
    }
    JZ_VEC_PUSH(ends, n);
    jz_vec_reserve(&values, count, sizeof(groove_Value));
    u32 start = values_start;
    jz_err e = JZ_OK;
    for (u32 i = 0; i < ends.len; i++) {
        u32 end = ends.ptr[i];
        if (end < start || end > n) { e = invalid_offset(); break; }
        groove_Value v;
        e = groove__records__values__decode_value(p + start, end - start, element_type, &v);
        if (e.code) break;
        jz_vec_push(&values, &v, sizeof(v));
        start = end;
    }
    jz_vec_drop(&ends, sizeof(u32), NULL);
    if (e.code) { groove__records__values__ValueVec__drop(&values); return e; }
    out->tag = GROOVE_V_ARRAY;
    out->u.items = values;
    return JZ_OK;
}

static bool is_internal(const groove_ValueType *t, u8 repr) { return t->tag == GROOVE_VT_INTERNAL && t->u.internal.repr == repr; }

static jz_err validate_embedded_record(groove_BorrowedRecord record);

jz_err groove__records__values__ensure_value_type(const groove_Value *v, const groove_ValueType *t) {
    u8 tt = t->tag;
    switch (v->tag) {
    case GROOVE_V_U48: {
        if (tt != GROOVE_VT_U48) break;
        u64 unused;
        return checked_u48(v->u.u64_, &unused);
    }
    case GROOVE_V_U8: if (tt == GROOVE_VT_U8) return JZ_OK; break;
    case GROOVE_V_U16: if (tt == GROOVE_VT_U16) return JZ_OK; break;
    case GROOVE_V_U32: if (tt == GROOVE_VT_U32) return JZ_OK; break;
    case GROOVE_V_U64: if (tt == GROOVE_VT_U64) return JZ_OK; break;
    case GROOVE_V_I32: if (tt == GROOVE_VT_I32) return JZ_OK; break;
    case GROOVE_V_I64: if (tt == GROOVE_VT_I64) return JZ_OK; break;
    case GROOVE_V_BOOL: if (tt == GROOVE_VT_BOOL) return JZ_OK; break;
    case GROOVE_V_UUID: if (tt == GROOVE_VT_UUID) return JZ_OK; break;
    case GROOVE_V_STRING:
        if (tt == GROOVE_VT_STRING || is_internal(t, GROOVE_IVT_RAW_STRING)) return JZ_OK;
        if (is_internal(t, GROOVE_IVT_STORED_SCALAR) && (t->u.internal.kind == GROOVE_LVK_STRING || t->u.internal.kind == GROOVE_LVK_JSON))
            return JZ_OK;
        if (tt == GROOVE_VT_ENUM_TAG) {
            u8 unused;
            return groove__records__values__ScalarEnumSchema__discriminant(&t->u.enum_tag, v->u.s.ptr, v->u.s.len, &unused);
        }
        break;
    case GROOVE_V_BYTES:
        if (tt == GROOVE_VT_BYTES || is_internal(t, GROOVE_IVT_RAW_BYTES)) return JZ_OK;
        if (is_internal(t, GROOVE_IVT_STORED_SCALAR) && t->u.internal.kind == GROOVE_LVK_BYTES) return JZ_OK;
        break;
    case GROOVE_V_LARGE:
        if (is_internal(t, GROOVE_IVT_STORED_SCALAR) && v->u.large->kind == t->u.internal.kind) return JZ_OK;
        if (tt == GROOVE_VT_STRING && v->u.large->kind == GROOVE_LVK_STRING) return JZ_OK;
        if (tt == GROOVE_VT_BYTES && v->u.large->kind == GROOVE_LVK_BYTES) return JZ_OK;
        break;
    case GROOVE_V_F64:
        if (tt != GROOVE_VT_F64) break;
        if (v->u.f64_ == v->u.f64_) return JZ_OK;
        return groove__records__Error__simple(GROOVE_REC_E_InvalidF64NaN);
    case GROOVE_V_ENUM_TAG: {
        if (tt != GROOVE_VT_ENUM_TAG) break;
        const jz_str *unused;
        return groove__records__values__ScalarEnumSchema__variant(&t->u.enum_tag, v->u.enum_tag, &unused);
    }
    case GROOVE_V_TUPLE: {
        if (tt != GROOVE_VT_TUPLE) break;
        if (v->u.items.len != t->u.tuple.len) return groove__records__Error__arity_mismatch(t->u.tuple.len, v->u.items.len);
        for (u32 i = 0; i < v->u.items.len; i++) {
            const groove_ValueType *member = &t->u.tuple.ptr[i];
            if (!groove__records__values__ValueType__is_fixed_size(member)) return groove__records__Error__invalid_tuple_member(member);
            JZ_TRY(groove__records__values__ensure_value_type(&v->u.items.ptr[i], member));
        }
        return JZ_OK;
    }
    case GROOVE_V_ARRAY:
        if (tt != GROOVE_VT_ARRAY) break;
        for (u32 i = 0; i < v->u.items.len; i++) JZ_TRY(groove__records__values__ensure_value_type(&v->u.items.ptr[i], t->u.inner));
        return JZ_OK;
    case GROOVE_V_NULLABLE:
        if (tt != GROOVE_VT_NULLABLE) break;
        if (!v->u.nullable) return JZ_OK;
        return groove__records__values__ensure_value_type(v->u.nullable, t->u.inner);
    case GROOVE_V_RECORD:
        if (tt != GROOVE_VT_RECORD) break;
        if (v->u.record.descriptor != t->u.record) return groove__records__Error__type_mismatch(t);
        return validate_embedded_record(groove__records__OwnedRecord__borrowed(&v->u.record));
    case GROOVE_V_ENUM:
        if (tt != GROOVE_VT_ENUM) break;
        return ensure_enum_value(&v->u.enum_, t->u.enum_);
    default: break;
    }
    return groove__records__Error__type_mismatch(t);
}

static jz_err ensure_enum_value(const groove_EnumValue *v, const groove_EnumSchema *schema) {
    const groove_EnumCase *c;
    JZ_TRY(groove__records__values__EnumSchema__case(schema, v->tag, &c));
    if (v->record.descriptor != c->payload) {
        groove_ValueType expected = VT_SCALAR(GROOVE_VT_ENUM);
        expected.u.enum_ = JZ_NEW(groove_EnumSchema);
        groove__records__values__EnumSchema__Clone__clone(expected.u.enum_, schema);
        jz_err e = groove__records__Error__type_mismatch(&expected);
        groove__records__values__ValueType__drop(&expected);
        return e;
    }
    return validate_embedded_record(groove__records__OwnedRecord__borrowed(&v->record));
}

/* Tuple decoding and encoding have historically different constructibility rules. */
static bool contains_tuple(const groove_ValueType *t);
static bool fields_contain_tuple(groove_RecordDescriptor d) {
    u32 n;
    const groove_DescriptorField *f = groove__records__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++) if (contains_tuple(&f[i].value_type)) return true;
    return false;
}
static bool contains_tuple(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_TUPLE: return true;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: return contains_tuple(t->u.inner);
    case GROOVE_VT_RECORD: return fields_contain_tuple(t->u.record);
    case GROOVE_VT_ENUM:
        for (u32 i = 0; i < t->u.enum_->cases.len; i++) if (fields_contain_tuple(t->u.enum_->cases.ptr[i].payload)) return true;
        return false;
    default: return false;
    }
}

static jz_err validate_embedded_record(groove_BorrowedRecord record) {
    groove_RecordDescriptor descriptor = record.descriptor;
    if (!fields_contain_tuple(descriptor)) {
        jz_err e = groove__records__BorrowedRecord__validate_canonical(&record);
        if (!e.code) return JZ_OK;
        jz_err_drop(&e);
    }
    /* Preserve legacy errors on invalid input as well as legacy tuple admission semantics. */
    groove_ValueVec values = {0};
    JZ_TRY(groove__records__BorrowedRecord__to_values(&record, &values));
    jz_bytes again = {0};
    jz_err e = groove__records__RecordDescriptor__create(descriptor, values.ptr, values.len, &again);
    groove__records__values__ValueVec__drop(&values);
    if (e.code) return e;
    bool same = again.len == record.raw.len && memcmp(again.ptr, record.raw.ptr, again.len) == 0;
    jz_bytes_drop(&again);
    return same ? JZ_OK : non_canonical();
}

static jz_err validate_fields_schema(groove_RecordDescriptor d) {
    u32 n;
    const groove_DescriptorField *f = groove__records__RecordDescriptor__fields(d, &n);
    for (u32 i = 0; i < n; i++) JZ_TRY(groove__records__values__validate_schema_value_type(&f[i].value_type));
    return JZ_OK;
}

jz_err groove__records__values__validate_schema_value_type(const groove_ValueType *t) {
    switch (t->tag) {
    case GROOVE_VT_TUPLE:
        for (u32 i = 0; i < t->u.tuple.len; i++) {
            const groove_ValueType *member = &t->u.tuple.ptr[i];
            JZ_TRY(groove__records__values__validate_schema_value_type(member));
            if (!groove__records__values__ValueType__is_fixed_size(member)) return groove__records__Error__invalid_tuple_member(member);
        }
        return JZ_OK;
    case GROOVE_VT_ARRAY:
    case GROOVE_VT_NULLABLE: return groove__records__values__validate_schema_value_type(t->u.inner);
    case GROOVE_VT_RECORD: return validate_fields_schema(t->u.record);
    case GROOVE_VT_ENUM_TAG: return scalar_enum_validate(&t->u.enum_tag);
    case GROOVE_VT_ENUM:
        JZ_TRY(enum_validate(t->u.enum_));
        for (u32 i = 0; i < t->u.enum_->cases.len; i++) JZ_TRY(validate_fields_schema(t->u.enum_->cases.ptr[i].payload));
        return JZ_OK;
    default: return JZ_OK;
    }
}

static jz_err encode_tuple_member(jz_bytes *bytes, const groove_Value *v, const groove_ValueType *t);

static jz_err encode_tuple(jz_bytes *bytes, const groove_ValueVec *values, const groove_ValueTypeVec *members) {
    if (values->len != members->len) return groove__records__Error__arity_mismatch(members->len, values->len);
    for (u32 i = 0; i < values->len; i++) JZ_TRY(encode_tuple_member(bytes, &values->ptr[i], &members->ptr[i]));
    return JZ_OK;
}

static void order_preserving_i64(i64 v, u8 out[8]) { put_be64(out, ((u64)v) ^ ((u64)1 << 63)); }
static i64 i64_from_order_preserving(const u8 b[8]) { return (i64)(get_be64(b) ^ ((u64)1 << 63)); }
static void order_preserving_i32(i32 v, u8 out[4]) { put_be32(out, ((u32)v) ^ ((u32)1 << 31)); }
static i32 i32_from_order_preserving(const u8 b[4]) { return (i32)(get_be32(b) ^ ((u32)1 << 31)); }

static jz_err encode_tuple_member(jz_bytes *bytes, const groove_Value *v, const groove_ValueType *t) {
    u8 b[16];
    u8 tt = t->tag;
    switch (v->tag) {
    case GROOVE_V_U8: if (tt != GROOVE_VT_U8) break; jz_bytes_push_u8(bytes, v->u.u8_); return JZ_OK;
    case GROOVE_V_U16: if (tt != GROOVE_VT_U16) break; put_be16(b, v->u.u16_); jz_bytes_push(bytes, b, 2); return JZ_OK;
    case GROOVE_V_U32: if (tt != GROOVE_VT_U32) break; put_be32(b, v->u.u32_); jz_bytes_push(bytes, b, 4); return JZ_OK;
    case GROOVE_V_U64: if (tt != GROOVE_VT_U64) break; put_be64(b, v->u.u64_); jz_bytes_push(bytes, b, 8); return JZ_OK;
    case GROOVE_V_U48:
        if (tt != GROOVE_VT_U48) break;
        JZ_TRY(groove__records__values__u48_be_bytes(v->u.u64_, b));
        jz_bytes_push(bytes, b, 6);
        return JZ_OK;
    case GROOVE_V_I32: if (tt != GROOVE_VT_I32) break; order_preserving_i32(v->u.i32_, b); jz_bytes_push(bytes, b, 4); return JZ_OK;
    case GROOVE_V_I64: if (tt != GROOVE_VT_I64) break; order_preserving_i64(v->u.i64_, b); jz_bytes_push(bytes, b, 8); return JZ_OK;
    case GROOVE_V_BOOL: if (tt != GROOVE_VT_BOOL) break; jz_bytes_push_u8(bytes, v->u.b ? 1 : 0); return JZ_OK;
    case GROOVE_V_UUID: if (tt != GROOVE_VT_UUID) break; jz_bytes_push(bytes, v->u.uuid.b, 16); return JZ_OK;
    case GROOVE_V_STRING: {
        if (tt != GROOVE_VT_ENUM_TAG) break;
        u8 d;
        JZ_TRY(groove__records__values__ScalarEnumSchema__discriminant(&t->u.enum_tag, v->u.s.ptr, v->u.s.len, &d));
        jz_bytes_push_u8(bytes, d);
        return JZ_OK;
    }
    case GROOVE_V_ENUM_TAG: if (tt != GROOVE_VT_ENUM_TAG) break; jz_bytes_push_u8(bytes, v->u.enum_tag); return JZ_OK;
    case GROOVE_V_TUPLE: if (tt != GROOVE_VT_TUPLE) break; return encode_tuple(bytes, &v->u.items, &t->u.tuple);
    case GROOVE_V_NULLABLE: {
        if (tt != GROOVE_VT_NULLABLE) break;
        jz_bytes_push_u8(bytes, v->u.nullable ? 1 : 0);
        u32 size;
        if (v->u.nullable) return encode_tuple_member(bytes, v->u.nullable, t->u.inner);
        if (groove__records__values__ValueType__fixed_size(t->u.inner, &size)) {
            bytes_resize_zero(bytes, bytes->len + size);
            return JZ_OK;
        }
        return groove__records__Error__invalid_tuple_member(t->u.inner);
    }
    default: break;
    }
    return groove__records__Error__type_mismatch(t);
}

static jz_err decode_tuple_member(const u8 *p, u32 n, const groove_ValueType *t, groove_Value *out);

static jz_err decode_tuple(const u8 *p, u32 n, const groove_ValueTypeVec *members, groove_Value *out) {
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, members->len, sizeof(groove_Value));
    u32 offset = 0;
    jz_err e = JZ_OK;
    for (u32 i = 0; i < members->len; i++) {
        const groove_ValueType *member = &members->ptr[i];
        u32 width, end;
        if (!groove__records__values__ValueType__fixed_size(member, &width)) { e = groove__records__Error__invalid_tuple_member(member); break; }
        e = groove__records__values__checked_add(offset, width, &end);
        if (e.code) break;
        if (end > n) { e = unexpected_eof(); break; }
        groove_Value v;
        e = decode_tuple_member(p + offset, width, member, &v);
        if (e.code) break;
        jz_vec_push(&values, &v, sizeof(v));
        offset = end;
    }
    if (!e.code && offset != n) e = invalid_offset();
    if (e.code) { groove__records__values__ValueVec__drop(&values); return e; }
    out->tag = GROOVE_V_TUPLE;
    out->u.items = values;
    return JZ_OK;
}

static jz_err decode_tuple_member(const u8 *p, u32 n, const groove_ValueType *t, groove_Value *out) {
    memset(out, 0, sizeof(*out));
    switch (t->tag) {
    case GROOVE_VT_U8: JZ_TRY(read_exact(p, n, 1)); out->tag = GROOVE_V_U8; out->u.u8_ = p[0]; return JZ_OK;
    case GROOVE_VT_U16: JZ_TRY(read_exact(p, n, 2)); out->tag = GROOVE_V_U16; out->u.u16_ = get_be16(p); return JZ_OK;
    case GROOVE_VT_U32: JZ_TRY(read_exact(p, n, 4)); out->tag = GROOVE_V_U32; out->u.u32_ = get_be32(p); return JZ_OK;
    case GROOVE_VT_U64: JZ_TRY(read_exact(p, n, 8)); out->tag = GROOVE_V_U64; out->u.u64_ = get_be64(p); return JZ_OK;
    case GROOVE_VT_U48: JZ_TRY(read_exact(p, n, 6)); out->tag = GROOVE_V_U48; out->u.u64_ = groove__records__values__u48_from_be_bytes(p); return JZ_OK;
    case GROOVE_VT_I32: JZ_TRY(read_exact(p, n, 4)); out->tag = GROOVE_V_I32; out->u.i32_ = i32_from_order_preserving(p); return JZ_OK;
    case GROOVE_VT_I64: JZ_TRY(read_exact(p, n, 8)); out->tag = GROOVE_V_I64; out->u.i64_ = i64_from_order_preserving(p); return JZ_OK;
    case GROOVE_VT_BOOL:
        JZ_TRY(read_exact(p, n, 1));
        if (p[0] > 1) return invalid_bool(p[0]);
        out->tag = GROOVE_V_BOOL;
        out->u.b = p[0] == 1;
        return JZ_OK;
    case GROOVE_VT_UUID: JZ_TRY(read_exact(p, n, 16)); out->tag = GROOVE_V_UUID; memcpy(out->u.uuid.b, p, 16); return JZ_OK;
    case GROOVE_VT_ENUM_TAG: {
        JZ_TRY(read_exact(p, n, 1));
        const jz_str *unused;
        JZ_TRY(groove__records__values__ScalarEnumSchema__variant(&t->u.enum_tag, p[0], &unused));
        out->tag = GROOVE_V_ENUM_TAG;
        out->u.enum_tag = p[0];
        return JZ_OK;
    }
    case GROOVE_VT_TUPLE: return decode_tuple(p, n, &t->u.tuple, out);
    case GROOVE_VT_NULLABLE: return decode_nullable(p, n, t->u.inner, out);
    default: /* F64 | String | Bytes | Internal | Array | Record | Enum */
        return groove__records__Error__invalid_tuple_member(t);
    }
}

static jz_err read_u32_at(const u8 *p, u32 n, u32 start, u32 *out) {
    u32 end;
    JZ_TRY(groove__records__values__checked_add(start, 4, &end));
    if (end > n) return unexpected_eof();
    *out = get_le32(p + start);
    return JZ_OK;
}

void groove__records__values__write_u32(jz_bytes *bytes, u32 v) {
    u8 b[4];
    put_le32(b, v);
    jz_bytes_push(bytes, b, 4);
}

jz_err groove__records__values__checked_add(u32 a, u32 b, u32 *out) {
    if (a > UINT32_MAX - b) return length_overflow();
    *out = a + b;
    return JZ_OK;
}

static jz_err checked_array_table_end(u32 prefix_size, u32 count, u32 *out) {
    u64 table = (u64)(count ? count - 1 : 0) * 4u;
    u64 end = table + prefix_size;
    if (table > UINT32_MAX || end > UINT32_MAX) return length_overflow();
    *out = (u32)end;
    return JZ_OK;
}

static jz_err checked_offset_slot(u32 start, u32 index, u32 *out) {
    u64 slot = (u64)index * 4u + start;
    if ((u64)index * 4u > UINT32_MAX || slot > UINT32_MAX) return length_overflow();
    *out = (u32)slot;
    return JZ_OK;
}

jz_err groove__records__values__usize_to_u32(u64 v, u32 *out) {
    if (v > UINT32_MAX) return length_overflow();
    *out = (u32)v;
    return JZ_OK;
}
