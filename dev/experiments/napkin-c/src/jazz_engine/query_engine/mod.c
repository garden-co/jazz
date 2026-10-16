/* mod.rs: module root. Holds the unit's shared container descriptors and the
 * derived-Debug formatting helpers (core::fmt::DebugStruct/DebugList glue that
 * Rust instantiates per type and C writes once). */
#include "mod.h"

void jazz_engine__str_drop_v(void *s) { jz_str_drop((jz_str *)s); }
void jazz_engine__str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }

void jazz_engine__strvec_clone(jazz_engine_StrVec *dst, const jazz_engine_StrVec *src) {
    jz_vec_clone(dst, src, sizeof(jz_str), jazz_engine__str_clone_v);
}
void jazz_engine__strvec_drop(jazz_engine_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), jazz_engine__str_drop_v); }
void jazz_engine__strvec_drop_v(void *v) { jazz_engine__strvec_drop((jazz_engine_StrVec *)v); }
bool jazz_engine__strvec_eq(const jazz_engine_StrVec *a, const jazz_engine_StrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}
int jazz_engine__strvec_cmp(const jazz_engine_StrVec *a, const jazz_engine_StrVec *b) {
    u32 n = a->len < b->len ? a->len : b->len;
    for (u32 i = 0; i < n; i++) {
        int c = jz_str_cmp(&a->ptr[i], &b->ptr[i]);
        if (c) return c;
    }
    return a->len < b->len ? -1 : a->len > b->len;
}
bool jazz_engine__strvec_contains(const jazz_engine_StrVec *v, const jz_str *s) {
    JZ_VEC_FOR(const jz_str, it, *v) if (jz_str_eq(it, s)) return true;
    return false;
}
bool jazz_engine__strvec_contains_cstr(const jazz_engine_StrVec *v, const char *s) {
    JZ_VEC_FOR(const jz_str, it, *v) if (jz_str_eq_cstr(it, s)) return true;
    return false;
}
void jazz_engine__strvec_push_cstr(jazz_engine_StrVec *v, const char *s) { JZ_VEC_PUSH(*v, jz_str_from(s)); }
void jazz_engine__strvec_push_clone(jazz_engine_StrVec *v, const jz_str *s) { JZ_VEC_PUSH(*v, jz_str_clone(s)); }
jazz_engine_StrVec jazz_engine__strvec_one(jz_str s) {
    jazz_engine_StrVec v = {0};
    JZ_VEC_PUSH(v, s);
    return v;
}
jazz_engine_StrVec jazz_engine__strvec_from_set(const jz_btree *set) {
    jazz_engine_StrVec v = {0};
    jz_str *k; void *unused;
    JZ_BTREE_FOR(set, k, unused) JZ_VEC_PUSH(v, jz_str_clone(k));
    return v;
}
bool jazz_engine__str_starts_with(const jz_str *s, const char *prefix) {
    u32 n = (u32)strlen(prefix);
    return s->len >= n && memcmp(s->ptr, prefix, n) == 0;
}
bool jazz_engine__str_eq_slice(const jz_str *s, jz_slice b) { return s->len == b.len && (b.len == 0 || memcmp(s->ptr, b.ptr, b.len) == 0); }
jz_str jazz_engine__str_concat(const char *a, const jz_str *b) { return jz_format("%s%.*s", a, (int)b->len, b->ptr); }
void jazz_engine__strset_insert_cstr(jz_btree *set, const char *s) {
    jz_str k = jz_str_from(s);
    if (jz_btree_contains(set, &k)) { jz_str_drop(&k); return; }
    jz_btree_insert(set, &k, NULL, NULL);
}
void jazz_engine__strset_insert_clone(jz_btree *set, const jz_str *s) {
    if (jz_btree_contains(set, s)) return;
    jz_str k = jz_str_clone(s);
    jz_btree_insert(set, &k, NULL, NULL);
}
bool jazz_engine__strset_contains_cstr(const jz_btree *set, const char *s) {
    jz_str k = {(char *)s, (u32)strlen(s), 0};
    return jz_btree_contains(set, &k);
}
void jazz_engine__str_drop_unused(jz_str s) { jz_str_drop(&s); }

void jazz_engine__vt_drop_v(void *t) { groove__records__values__ValueType__drop((groove_ValueType *)t); }
void jazz_engine__vt_clone_v(void *dst, const void *src) { groove__records__values__ValueType__Clone__clone((groove_ValueType *)dst, (const groove_ValueType *)src); }
bool jazz_engine__vt_eq_v(const void *a, const void *b) { return groove__records__values__ValueType__PartialEq__eq((const groove_ValueType *)a, (const groove_ValueType *)b); }
static void u32_clone_v(void *dst, const void *src) { *(u32 *)dst = *(const u32 *)src; }

const jz_btree_desc jazz_engine__STRSET_DESC = {sizeof(jz_str), 0, jz_cmp_str, jazz_engine__str_drop_v, NULL};
const jz_btree_desc jazz_engine__STR_STR_MAP_DESC = {sizeof(jz_str), sizeof(jz_str), jz_cmp_str, jazz_engine__str_drop_v, jazz_engine__str_drop_v};
const jz_btree_desc jazz_engine__STR_VT_MAP_DESC = {sizeof(jz_str), sizeof(groove_ValueType), jz_cmp_str, jazz_engine__str_drop_v, jazz_engine__vt_drop_v};
const jz_btree_desc jazz_engine__STR_VALUE_MAP_DESC = {sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, jazz_engine__str_drop_v, groove__records__values__Value__drop_v};
const jz_btree_desc jazz_engine__STR_U32_MAP_DESC = {sizeof(jz_str), sizeof(u32), jz_cmp_str, jazz_engine__str_drop_v, NULL};

static u64 str_hash_v(const void *k) { return jz_str_hash((const jz_str *)k); }
static bool str_eq_v(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static u64 ptr_hash_v(const void *k) { return jz_hash_u64((u64)(uintptr_t)*(void *const *)k); }
static bool ptr_eq_v(const void *a, const void *b) { return *(void *const *)a == *(void *const *)b; }
const jz_hmap_desc jazz_engine__STRSET_HDESC = {sizeof(jz_str), 0, str_hash_v, str_eq_v, jazz_engine__str_drop_v, NULL};
const jz_hmap_desc jazz_engine__PTRSET_HDESC = {sizeof(void *), 0, ptr_hash_v, ptr_eq_v, NULL, NULL};
const jz_hmap_desc jazz_engine__PTR_U32_HDESC = {sizeof(void *), sizeof(u32), ptr_hash_v, ptr_eq_v, NULL, NULL};
const jz_hmap_desc jazz_engine__STR_U32_HDESC = {sizeof(jz_str), sizeof(u32), str_hash_v, str_eq_v, jazz_engine__str_drop_v, NULL};

void jazz_engine__strset_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, jazz_engine__str_clone_v, NULL); }
void jazz_engine__strvtmap_clone(jz_btree *dst, const jz_btree *src) { jz_btree_clone(dst, src, jazz_engine__str_clone_v, jazz_engine__vt_clone_v); }

/* ---------------------------------------------------------------- Debug */
void jazz_engine__fmt__str(jz_str *out, const jz_str *s) {
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
            if ((u8)c < 0x20 || c == 0x7f) jz_str_appendf(out, "\\u{%x}", (u8)c);
            else jz_str_push_char(out, c);
        }
    }
    jz_str_push_char(out, '"');
}
void jazz_engine__fmt__cstr(jz_str *out, const char *s) {
    jz_str t = {(char *)s, (u32)strlen(s), 0};
    jazz_engine__fmt__str(out, &t);
}
void jazz_engine__fmt__struct_begin(jz_str *out, const char *name) { jz_str_push_cstr(out, name); jz_str_push_cstr(out, " { "); }
void jazz_engine__fmt__field(jz_str *out, const char *name, bool first) {
    if (!first) jz_str_push_cstr(out, ", ");
    jz_str_push_cstr(out, name);
    jz_str_push_cstr(out, ": ");
}
void jazz_engine__fmt__struct_end(jz_str *out) { jz_str_push_cstr(out, " }"); }
void jazz_engine__fmt__tuple_begin(jz_str *out, const char *name) { jz_str_push_cstr(out, name); jz_str_push_char(out, '('); }
void jazz_engine__fmt__tuple_end(jz_str *out) { jz_str_push_char(out, ')'); }
void jazz_engine__fmt__sep(jz_str *out, bool first) { if (!first) jz_str_push_cstr(out, ", "); }
void jazz_engine__fmt__uuid(jz_str *out, const jz_uuid *u) {
    char buf[37];
    jz_uuid_to_string(u, buf);
    jz_str_push(out, buf, 36);
}
void jazz_engine__fmt__bool(jz_str *out, bool b) { jz_str_push_cstr(out, b ? "true" : "false"); }
void jazz_engine__fmt__u64(jz_str *out, u64 v) { jz_str_appendf(out, "%llu", (unsigned long long)v); }
void jazz_engine__fmt__option_none(jz_str *out) { jz_str_push_cstr(out, "None"); }
void jazz_engine__fmt__list(jz_str *out, const void *items, u32 n, usize elem, jazz_engine_fmt_fn f) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < n; i++) {
        jazz_engine__fmt__sep(out, i == 0);
        f(out, (const u8 *)items + i * elem);
    }
    jz_str_push_char(out, ']');
}
void jazz_engine__fmt__set(jz_str *out, const jz_btree *set, jazz_engine_fmt_fn f) {
    jz_str_push_char(out, '{');
    bool first = true;
    void *k, *v;
    JZ_BTREE_FOR(set, k, v) {
        jazz_engine__fmt__sep(out, first);
        first = false;
        f(out, k);
    }
    jz_str_push_char(out, '}');
}
void jazz_engine__fmt__map(jz_str *out, const jz_btree *map, jazz_engine_fmt_fn kf, jazz_engine_fmt_fn vf) {
    jz_str_push_char(out, '{');
    bool first = true;
    void *k, *v;
    JZ_BTREE_FOR(map, k, v) {
        jazz_engine__fmt__sep(out, first);
        first = false;
        kf(out, k);
        jz_str_push_cstr(out, ": ");
        vf(out, v);
    }
    jz_str_push_char(out, '}');
}
void jazz_engine__fmt__str_v(jz_str *out, const void *s) { jazz_engine__fmt__str(out, (const jz_str *)s); }
void jazz_engine__fmt__vt_v(jz_str *out, const void *t) { groove__records__values__ValueType__Debug__fmt(out, (const groove_ValueType *)t); }
void jazz_engine__fmt__value_v(jz_str *out, const void *v) { groove__records__values__Value__Debug__fmt(out, (const groove_Value *)v); }
void jazz_engine__fmt__strvec(jz_str *out, const jazz_engine_StrVec *v) { jazz_engine__fmt__list(out, v->ptr, v->len, sizeof(jz_str), jazz_engine__fmt__str_v); }
void jazz_engine__fmt__strset(jz_str *out, const jz_btree *set) { jazz_engine__fmt__set(out, set, jazz_engine__fmt__str_v); }

/* Display of an error coming back from the shared runtime / other units:
 * postcard::Error's Display strings for the postcard reader codes, otherwise the
 * carried message (or the numeric code when there is none). */
jz_str jazz_engine__err_display(const jz_err *e) {
    switch (e->code) {
    case JZ_PC_EOF: return jz_str_from("Hit the end of buffer, expected more data");
    case JZ_PC_BAD_VARINT: return jz_str_from("Found a varint that didn't terminate. Is the endianness correct?");
    case JZ_PC_BAD_BOOL: return jz_str_from("Found a bool that wasn't 0 or 1");
    case JZ_PC_BAD_UTF8: return jz_str_from("Tried to parse invalid utf-8");
    case JZ_PC_BAD_VARIANT: return jz_str_from("Found an enum discriminant that was > u32::max_value()");
    case JZ_PC_DEPTH: return jz_str_from("Serde Deserialization Error");
    default: break;
    }
    if (e->msg) return jz_str_clone(e->msg);
    return jz_format("error %u", e->code);
}
u64 jazz_engine__ptr_hash_v(const void *k) { return ptr_hash_v(k); }
bool jazz_engine__ptr_eq_v(const void *a, const void *b) { return ptr_eq_v(a, b); }

/* BTreeMap::insert keeping the existing key (the new key is dropped, the old value replaced) */
void jazz_engine__btree_put(jz_btree *t, void *key, const void *val) {
    void *old = jz_btree_get(t, key);
    if (old) {
        if (t->d->val_drop) t->d->val_drop(old);
        memcpy(old, val, t->d->val_size);
        if (t->d->key_drop) t->d->key_drop(key);
        return;
    }
    jz_btree_insert(t, key, val, NULL);
}
/* BTreeSet::insert: moves key in; drops it when already present. */
bool jazz_engine__btree_set_add(jz_btree *t, void *key) {
    if (jz_btree_contains(t, key)) {
        if (t->d->key_drop) t->d->key_drop(key);
        return false;
    }
    jz_btree_insert(t, key, NULL, NULL);
    return true;
}
/* entry(key).or_insert(val): moves key/val in when vacant, else drops both. Returns the value slot. */
void *jazz_engine__btree_or_insert(jz_btree *t, void *key, void *val) {
    void *slot = jz_btree_get(t, key);
    if (slot) {
        if (t->d->key_drop) t->d->key_drop(key);
        if (t->d->val_drop) t->d->val_drop(val);
        return slot;
    }
    jz_btree_insert(t, key, val, NULL);
    return jz_btree_get(t, key);
}
/* a borrowed jz_str view over a slice (never dropped) */
jz_str jazz_engine__str_view(jz_slice s) { jz_str v = {(char *)s.ptr, s.len, 0}; return v; }
