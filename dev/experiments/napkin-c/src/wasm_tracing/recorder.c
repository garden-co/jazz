/* crates/wasm-tracing/src/recorder.rs */
#include "wasm_tracing/wasm_tracing.h"

/* HashMap<String, String> */
static u64 wt_str_hash(const void *k) { return jz_str_hash((const jz_str *)k); }
static bool wt_str_eq(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static void wt_str_drop(void *p) { jz_str_drop((jz_str *)p); }
const jz_hmap_desc wasm_tracing__STRING_STRING_HMAP = {
    sizeof(jz_str), sizeof(jz_str), wt_str_hash, wt_str_eq, wt_str_drop, wt_str_drop,
};

void wasm_tracing__clone_string_map(jz_hmap *dst, const jz_hmap *src) {
    jz_hmap_init(dst, &wasm_tracing__STRING_STRING_HMAP);
    jz_str *k, *v;
    JZ_HMAP_FOR(src, k, v) {
        jz_str kc = jz_str_clone(k), vc = jz_str_clone(v);
        jz_hmap_insert(dst, &kc, &vc, NULL);
    }
}

void wasm_tracing__recorder__StringRecorder__Default__default(wasm_tracing_StringRecorder *r) {
    memset(r, 0, sizeof *r);
    jz_hmap_init(&r->fields, &wasm_tracing__STRING_STRING_HMAP);
}

void wasm_tracing__recorder__StringRecorder__drop(wasm_tracing_StringRecorder *r) {
    jz_str_drop(&r->display);
    jz_hmap_drop(&r->fields);
    if (r->has_message) jz_str_drop(&r->message);
    r->has_message = false;
}

/* <str as Debug>::fmt */
static void wt_fmt_debug_str(jz_str *out, jz_slice s) {
    jz_str_push_char(out, '"');
    for (u32 i = 0; i < s.len; i++) {
        u8 c = s.ptr[i];
        switch (c) {
        case '"': jz_str_push_cstr(out, "\\\""); break;
        case '\\': jz_str_push_cstr(out, "\\\\"); break;
        case '\n': jz_str_push_cstr(out, "\\n"); break;
        case '\r': jz_str_push_cstr(out, "\\r"); break;
        case '\t': jz_str_push_cstr(out, "\\t"); break;
        case '\0': jz_str_push_cstr(out, "\\0"); break;
        default:
            if (c < 0x20 || c == 0x7f) jz_str_appendf(out, "\\u{%x}", (unsigned)c);
            else jz_str_push_char(out, (char)c);
        }
    }
    jz_str_push_char(out, '"');
}

/* `{:?}` of the &dyn Debug value the Visit callback receives */
void wasm_tracing__recorder__fmt_debug_value(jz_str *out, const wasm_tracing_FieldValue *v) {
    switch (v->kind) {
    case WT_FIELD_STR: wt_fmt_debug_str(out, v->v.s); break;
    case WT_FIELD_DEBUG: jz_str_push(out, (const char *)v->v.s.ptr, v->v.s.len); break;
    case WT_FIELD_I64: jz_str_appendf(out, "%lld", (long long)v->v.i); break;
    case WT_FIELD_U64: jz_str_appendf(out, "%llu", (unsigned long long)v->v.u); break;
    case WT_FIELD_BOOL: jz_str_push_cstr(out, v->v.b ? "true" : "false"); break;
    case WT_FIELD_F64: jz_str_appendf(out, "%g", v->v.f); break;
    default: JZ_UNREACHABLE();
    }
}

static void wasm_tracing__recorder__StringRecorder__record_field(wasm_tracing_StringRecorder *r, const char *field,
                                                                 const wasm_tracing_FieldValue *value, jz_str raw) {
    if (r->is_following_args) {
        jz_str_push_char(&r->display, '\n');
    } else {
        jz_str_push_char(&r->display, ' ');
        r->is_following_args = true;
    }
    jz_str_appendf(&r->display, "%s = ", field);
    wasm_tracing__recorder__fmt_debug_value(&r->display, value);
    jz_str_push_char(&r->display, ';');
    jz_str key = jz_str_from(field);
    jz_hmap_insert(&r->fields, &key, &raw, NULL);
}

static jz_slice wasm_tracing__recorder__trim_debug_message(jz_slice message) {
    if (message.len >= 1 && message.ptr[0] == '"') {
        jz_slice rest = {message.ptr + 1, message.len - 1};
        if (rest.len >= 1 && rest.ptr[rest.len - 1] == '"') {
            rest.len -= 1;
            return rest;
        }
    }
    return message;
}

void wasm_tracing__recorder__StringRecorder__Visit__record_debug(wasm_tracing_StringRecorder *r, const char *field,
                                                                const wasm_tracing_FieldValue *value) {
    if (strcmp(field, "message") == 0) {
        jz_str message = {0};
        wasm_tracing__recorder__fmt_debug_value(&message, value);
        jz_slice trimmed = wasm_tracing__recorder__trim_debug_message(jz_str_slice(&message));
        if (r->has_message) jz_str_drop(&r->message);
        r->message = jz_str_from_slice(trimmed);
        r->has_message = true;
        if (r->display.len != 0) {
            jz_str next = {0};
            wasm_tracing__recorder__fmt_debug_value(&next, value);
            jz_str_push_char(&next, '\n');
            jz_str_push(&next, r->display.ptr, r->display.len);
            jz_str_drop(&r->display);
            r->display = next;
        } else {
            jz_str next = {0};
            wasm_tracing__recorder__fmt_debug_value(&next, value);
            jz_str_drop(&r->display);
            r->display = next;
        }
        jz_str_drop(&message);
    } else {
        jz_str raw = {0};
        wasm_tracing__recorder__fmt_debug_value(&raw, value);
        wasm_tracing__recorder__StringRecorder__record_field(r, field, value, raw);
    }
}

void wasm_tracing__recorder__StringRecorder__Visit__record_str(wasm_tracing_StringRecorder *r, const char *field, jz_slice value) {
    wasm_tracing_FieldValue v;
    v.name = field;
    v.kind = WT_FIELD_STR;
    v.v.s = value;
    if (strcmp(field, "message") == 0) {
        wasm_tracing__recorder__StringRecorder__Visit__record_debug(r, field, &v);
    } else {
        wasm_tracing__recorder__StringRecorder__record_field(r, field, &v, jz_str_from_slice(value));
    }
}

/* ValueSet::record(&mut visitor): str fields use record_str, every other
 * primitive goes through the default Visit::record_* -> record_debug. */
void wasm_tracing__recorder__record_value_set(wasm_tracing_StringRecorder *r, const wasm_tracing_ValueSet *values) {
    for (u32 i = 0; i < values->len; i++) {
        const wasm_tracing_FieldValue *f = &values->fields[i];
        if (f->kind == WT_FIELD_STR) wasm_tracing__recorder__StringRecorder__Visit__record_str(r, f->name, f->v.s);
        else wasm_tracing__recorder__StringRecorder__Visit__record_debug(r, f->name, f);
    }
}

void wasm_tracing__recorder__StringRecorder__Display__fmt(const wasm_tracing_StringRecorder *r, jz_str *out) {
    if (r->display.len != 0) {
        jz_str_push_char(out, ' ');
        jz_str_push(out, r->display.ptr, r->display.len);
    }
}
