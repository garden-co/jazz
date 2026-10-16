/* lib.rs: crate root. Holds the type-erased helpers shared by every module of
 * the jazz_model port (the parts serde/derive/std generate per type in Rust). */
#include "jazz_model.h"
#include <stdarg.h>
#include <stdio.h>

void jazz_model__StrVec__drop(jazz_model_StrVec *v) { jz_vec_drop(v, sizeof(jz_str), jazz_model__str_drop_v); }
void jazz_model__str_drop_v(void *s) { jz_str_drop((jz_str *)s); }
void jazz_model__str_clone_v(void *dst, const void *src) { *(jz_str *)dst = jz_str_clone((const jz_str *)src); }
void jazz_model__StrVec__clone(jazz_model_StrVec *dst, const jazz_model_StrVec *src) { jz_vec_clone(dst, src, sizeof(jz_str), jazz_model__str_clone_v); }

bool jazz_model__StrVec__eq(const jazz_model_StrVec *a, const jazz_model_StrVec *b) {
    if (a->len != b->len) return false;
    for (u32 i = 0; i < a->len; i++)
        if (!jz_str_eq(&a->ptr[i], &b->ptr[i])) return false;
    return true;
}

void jazz_model__debug_str(jz_str *out, const char *p, u32 n) {
    jz_str_push_char(out, '"');
    for (u32 i = 0; i < n; i++) {
        char c = p[i];
        switch (c) {
        case '"': jz_str_push_cstr(out, "\\\""); break;
        case '\\': jz_str_push_cstr(out, "\\\\"); break;
        case '\n': jz_str_push_cstr(out, "\\n"); break;
        case '\r': jz_str_push_cstr(out, "\\r"); break;
        case '\t': jz_str_push_cstr(out, "\\t"); break;
        case '\0': jz_str_push_cstr(out, "\\0"); break;
        default:
            if ((u8)c < 0x20) jz_str_appendf(out, "\\u{%x}", (u32)(u8)c);
            else jz_str_push_char(out, c);
        }
    }
    jz_str_push_char(out, '"');
}

void jazz_model__StrVec__Debug__fmt(jz_str *out, const jazz_model_StrVec *v) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < v->len; i++) {
        if (i) jz_str_push_cstr(out, ", ");
        jazz_model__debug_str(out, v->ptr[i].ptr, v->ptr[i].len);
    }
    jz_str_push_char(out, ']');
}

void jazz_model__json_w_str(jz_json_w *w, const jz_str *s) { jz_jw_str(w, s->ptr, s->len); }
void jazz_model__json_w_cstr(jz_json_w *w, const char *s) { jz_jw_str(w, s, (u32)strlen(s)); }

void jazz_model__StrVec__serialize_json(jz_json_w *w, const jazz_model_StrVec *v) {
    jz_jw_begin_arr(w);
    for (u32 i = 0; i < v->len; i++) jazz_model__json_w_str(w, &v->ptr[i]);
    jz_jw_end_arr(w);
}

jz_err jazz_model__StrVec__deserialize_json(const jz_json *j, jazz_model_StrVec *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_model__json_expect_arr(j, "a sequence"));
    for (u32 i = 0; i < j->v.arr.len; i++) {
        jz_str s;
        jz_err e = jazz_model__json_expect_str(j->v.arr.ptr[i], &s);
        if (e.code) { jazz_model__StrVec__drop(out); return e; }
        JZ_VEC_PUSH(*out, s);
    }
    return JZ_OK;
}

void jazz_model__StrVec__serialize_pc(jz_pc_w *w, const jazz_model_StrVec *v) {
    jz_pc_seq(w, v->len);
    for (u32 i = 0; i < v->len; i++) jz_pc_str(w, &v->ptr[i]);
}

jz_err jazz_model__StrVec__deserialize_pc(jz_pc_r *r, jazz_model_StrVec *out) {
    u32 n;
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_seq(r, &n));
    for (u32 i = 0; i < n; i++) {
        jz_str s;
        jz_err e = jz_pc_r_str(r, &s);
        if (e.code) { jazz_model__StrVec__drop(out); return e; }
        JZ_VEC_PUSH(*out, s);
    }
    return JZ_OK;
}

void jazz_model__OptStr__drop(jazz_model_OptStr *s) { if (s->some) jz_str_drop(&s->s); s->some = false; }
jazz_model_OptStr jazz_model__OptStr__clone(const jazz_model_OptStr *s) {
    jazz_model_OptStr o = {s->some, {0}};
    if (s->some) o.s = jz_str_clone(&s->s);
    return o;
}
bool jazz_model__OptStr__eq(const jazz_model_OptStr *a, const jazz_model_OptStr *b) {
    if (a->some != b->some) return false;
    return !a->some || jz_str_eq(&a->s, &b->s);
}

jz_err jazz_model__serde_err(const char *fmt, ...) {
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    return jz_err_new(JAZZ_MODEL_ERR_SERDE, buf);
}

jz_err jazz_model__json_missing_field(const char *field) {
    return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `%s`", field);
}

/* serde: "unknown variant `X`, expected one of `A`, `B`" */
jz_err jazz_model__json_unknown_variant(const jz_str *name, const char *const *names, u32 n) {
    jz_str msg = jz_format("unknown variant `%.*s`, expected ", (int)name->len, name->ptr);
    if (n == 0) jz_str_push_cstr(&msg, "nothing");
    else if (n == 1) jz_str_appendf(&msg, "`%s`", names[0]);
    else if (n == 2) jz_str_appendf(&msg, "`%s` or `%s`", names[0], names[1]);
    else {
        jz_str_push_cstr(&msg, "one of ");
        for (u32 i = 0; i < n; i++) jz_str_appendf(&msg, i ? ", `%s`" : "`%s`", names[i]);
    }
    jz_err e = jz_err_fmt(JZ_JSON_ERR_UNKNOWN_VARIANT, "%.*s", (int)msg.len, msg.ptr);
    jz_str_drop(&msg);
    return e;
}

jz_err jazz_model__json_deny_unknown_fields(const jz_json *obj, const char *const *fields, u32 n) {
    jz_str *k; jz_json **v;
    JZ_BTREE_FOR(&obj->v.obj, k, v) {
        bool known = false;
        for (u32 i = 0; i < n; i++)
            if (jz_str_eq_cstr(k, fields[i])) { known = true; break; }
        if (!known) {
            jz_str msg = jz_format("unknown field `%.*s`, expected ", (int)k->len, k->ptr);
            if (n == 0) jz_str_push_cstr(&msg, "nothing");
            else {
                jz_str_push_cstr(&msg, n == 1 ? "" : "one of ");
                for (u32 i = 0; i < n; i++) jz_str_appendf(&msg, i ? ", `%s`" : "`%s`", fields[i]);
            }
            jz_err e = jz_err_fmt(JAZZ_MODEL_ERR_SERDE, "%.*s", (int)msg.len, msg.ptr);
            jz_str_drop(&msg);
            return e;
        }
    }
    return JZ_OK;
}

/* #[serde(tag = "...")]: object with the tag field naming the variant */
jz_err jazz_model__json_internal_tag(const jz_json *v, const char *tag_field, const char *const *names, u32 n, u32 *idx) {
    if (!v || v->kind != JZ_JSON_OBJ) return jazz_model__serde_err("invalid type: expected internally tagged enum");
    jz_json *tag = jz_json_obj_get(v, tag_field);
    if (!tag) return jazz_model__json_missing_field(tag_field);
    if (tag->kind != JZ_JSON_STR) return jazz_model__serde_err("invalid type: expected variant identifier");
    for (u32 i = 0; i < n; i++)
        if (jz_str_eq_cstr(&tag->v.s, names[i])) { *idx = i; return JZ_OK; }
    return jazz_model__json_unknown_variant(&tag->v.s, names, n);
}

const jz_str *jazz_model__json_str(const jz_json *v) { return v && v->kind == JZ_JSON_STR ? &v->v.s : NULL; }

jz_err jazz_model__json_expect_str(const jz_json *v, jz_str *out) {
    if (!v || v->kind != JZ_JSON_STR) return jazz_model__serde_err("invalid type: expected a string");
    *out = jz_str_clone(&v->v.s);
    return JZ_OK;
}
jz_err jazz_model__json_expect_arr(const jz_json *v, const char *what) {
    if (!v || v->kind != JZ_JSON_ARR) return jazz_model__serde_err("invalid type: expected %s", what);
    return JZ_OK;
}
jz_err jazz_model__json_expect_bool(const jz_json *v, bool *out) {
    if (!v || v->kind != JZ_JSON_BOOL) return jazz_model__serde_err("invalid type: expected a boolean");
    *out = v->v.b;
    return JZ_OK;
}
jz_err jazz_model__json_expect_u64(const jz_json *v, u64 *out) {
    if (!v || !jz_json_as_u64(v, out)) return jazz_model__serde_err("invalid type: expected u64");
    return JZ_OK;
}
jz_err jazz_model__json_expect_i64(const jz_json *v, i64 *out) {
    if (!v || !jz_json_as_i64(v, out)) return jazz_model__serde_err("invalid type: expected i64");
    return JZ_OK;
}
jz_err jazz_model__json_expect_f64(const jz_json *v, f64 *out) {
    if (!v || !jz_json_as_f64(v, out)) return jazz_model__serde_err("invalid type: expected f64");
    return JZ_OK;
}

/* unit variant of an externally tagged enum: "Name" (or {"Name": null}) */
jz_err jazz_model__json_unit_variant(const jz_json *v, const char *const *names, u32 n, u32 *idx) {
    const jz_json *payload = NULL;
    JZ_TRY(jz_json_variant(v, names, n, idx, &payload));
    if (payload && payload->kind != JZ_JSON_NULL) return jazz_model__serde_err("invalid type: expected unit variant");
    return JZ_OK;
}
void jazz_model__json_w_unit_variant(jz_json_w *w, const char *name) { jazz_model__json_w_cstr(w, name); }

jz_str jazz_model__str_from_slice(jz_slice s) { return jz_str_from_slice(s); }
bool jazz_model__slice_eq(jz_slice a, jz_slice b) { return a.len == b.len && memcmp(a.ptr, b.ptr, a.len) == 0; }
bool jazz_model__slice_eq_cstr(jz_slice a, const char *s) { u32 n = (u32)strlen(s); return a.len == n && memcmp(a.ptr, s, n) == 0; }
bool jazz_model__str_starts_with(jz_slice s, const char *prefix) { u32 n = (u32)strlen(prefix); return s.len >= n && memcmp(s.ptr, prefix, n) == 0; }

int jazz_model__cmp_str_ptr(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_str_cmp(*(const jz_str *const *)a, *(const jz_str *const *)b);
}

const jz_btree_desc jazz_model__STR_SET_DESC = { sizeof(jz_str), 0, jz_cmp_str, jazz_model__str_drop_v, NULL };
const jz_btree_desc jazz_model__STR_U32_DESC = { sizeof(jz_str), sizeof(u32), jz_cmp_str, jazz_model__str_drop_v, NULL };
const jz_btree_desc jazz_model__STRPTR_SET_DESC = { sizeof(const jz_str *), 0, jazz_model__cmp_str_ptr, NULL, NULL };

void jazz_model__json_w_value(jz_json_w *w, const jz_json *v) {
    /* serde_json::Value nested in a derived Serialize: emit its text via the writer's buffer */
    jz_str tmp = {0};
    jz_json_write(v, &tmp);
    jz_jw_null(w);
    w->out->len -= 4;
    jz_str_push(w->out, tmp.ptr, tmp.len);
    jz_str_drop(&tmp);
}
void jazz_model__OptStr__serialize_pc(jz_pc_w *w, const jazz_model_OptStr *s) {
    jz_pc_option(w, s->some);
    if (s->some) jz_pc_str(w, &s->s);
}
jz_err jazz_model__OptStr__deserialize_pc(jz_pc_r *r, jazz_model_OptStr *out) {
    JZ_TRY(jz_pc_r_option(r, &out->some));
    if (out->some) return jz_pc_r_str(r, &out->s);
    return JZ_OK;
}
void jazz_model__OptStr__serialize_json(jz_json_w *w, const jazz_model_OptStr *s) {
    if (s->some) jz_jw_str(w, s->s.ptr, s->s.len); else jz_jw_null(w);
}
void jazz_model__OptU64__serialize_pc(jz_pc_w *w, bool some, u64 v) {
    jz_pc_option(w, some);
    if (some) jz_pc_varint(w, v);
}
jz_err jazz_model__OptU64__deserialize_pc(jz_pc_r *r, bool *some, u64 *v) {
    JZ_TRY(jz_pc_r_option(r, some));
    if (*some) return jz_pc_r_varint(r, v);
    return JZ_OK;
}
void jazz_model__OptU64__serialize_json(jz_json_w *w, bool some, u64 v) {
    if (some) jz_jw_u64(w, v); else jz_jw_null(w);
}
