/* wbg.c: wasm-bindgen runtime glue (the parts wasm-bindgen emits on the Rust
 * side: __wbindgen_malloc/free/realloc, closure trampolines, string/byte
 * passing, Result return slots). */
#include "jazz_wasm/wbg.h"

JZ_EXPORT("__wbindgen_malloc") void *__wbindgen_malloc(u32 size, u32 align) {
    (void)align;
    if (size == 0) return (void *)(usize)align;
    void *p = jz_alloc(size);
    if (!p) jz_oom();
    return p;
}

JZ_EXPORT("__wbindgen_realloc") void *__wbindgen_realloc(void *ptr, u32 old_size, u32 new_size, u32 align) {
    (void)old_size; (void)align;
    void *p = jz_realloc(ptr, new_size);
    if (!p) jz_oom();
    return p;
}

JZ_EXPORT("__wbindgen_free") void __wbindgen_free(void *ptr, u32 size, u32 align) {
    (void)align;
    if (size) jz_free(ptr);
}

JZ_EXPORT("__wbindgen_exn_store") void __wbindgen_exn_store(jz_js idx) { (void)idx; }

static void wbg_closure_box_drop(wbg_closure *c) {
    if (c->drop) c->drop(c->ctx);
    jz_free(c);
}

JZ_EXPORT("__wbg_closure_invoke0") jz_js __wbg_closure_invoke0(wbg_closure *c) {
    return c->call(c->ctx, WBG_UNDEFINED);
}

JZ_EXPORT("__wbg_closure_invoke1") jz_js __wbg_closure_invoke1(wbg_closure *c, jz_js arg) {
    return c->call(c->ctx, arg);
}

JZ_EXPORT("__wbg_closure_destroy") void __wbg_closure_destroy(wbg_closure *c) {
    wbg_closure_box_drop(c);
}

jz_js wbg_closure_into_js(jz_js (*call)(void *ctx, jz_js arg), void *ctx, void (*drop)(void *ctx), u32 arity) {
    wbg_closure *c = JZ_NEW(wbg_closure);
    c->call = call;
    c->ctx = ctx;
    c->drop = drop;
    c->arity = arity;
    return wbg_closure_new(c, arity);
}

jz_js wbg_str(const char *s) { return wbg_string_new(s, (u32)strlen(s)); }
jz_js wbg_str_slice(jz_slice s) { return wbg_string_new((const char *)s.ptr, s.len); }
jz_js wbg_str_owned(jz_str *s) {
    jz_js v = wbg_string_new(s->ptr, s->len);
    jz_str_drop(s);
    return v;
}
jz_js wbg_bool(bool b) { return b ? WBG_TRUE : WBG_FALSE; }

bool wbg_is_nullish(jz_js h) {
    u32 t = wbg_typeof(h);
    return t == WBG_T_NULL || t == WBG_T_UNDEFINED;
}

bool wbg_take_string(jz_js h, jz_str *out) {
    char *p = NULL;
    u32 n = 0;
    if (!wbg_as_string(h, &p, &n)) return false;
    out->ptr = p;
    out->len = n;
    out->cap = n;
    return true;
}

void wbg_uint8array_to_vec(jz_js h, jz_bytes *out) {
    u32 n = wbg_uint8array_length(h);
    out->ptr = n ? (u8 *)jz_alloc(n) : NULL;
    out->len = n;
    out->cap = n;
    if (n) wbg_uint8array_copy_to(h, out->ptr);
}

jz_js wbg_bytes_to_js(const u8 *p, u32 n) { return wbg_uint8array_from(p, n); }

/* A JsValue carried through a jz_err: the handle is stored in the message's
 * length field slot, so `?` propagation of Result<_, JsValue> stays one word. */
typedef struct wbg_err_box { jz_str text; jz_js js; } wbg_err_box;

jz_err wbg_err_from_js(jz_js v) {
    jz_err e = jz_err_new(WBG_ERR_JS, NULL);
    e.msg = (jz_str *)jz_zalloc(sizeof(wbg_err_box));
    ((wbg_err_box *)e.msg)->js = v;
    return e;
}

jz_js wbg_err_into_js(jz_err e) {
    if (e.code == WBG_ERR_JS && e.msg) {
        jz_js v = ((wbg_err_box *)e.msg)->js;
        jz_free(e.msg);
        return v;
    }
    jz_js v;
    if (e.msg) v = wbg_string_new(e.msg->ptr, e.msg->len);
    else v = wbg_str("error");
    jz_err_drop(&e);
    return v;
}

void wbg_ret_string(wbg_ret_str *ret, jz_str s) {
    ret->ptr = (u32)(usize)s.ptr;
    ret->len = s.len;
    ret->err = 0;
    ret->is_err = 0;
}

void wbg_ret_error_str(wbg_ret_str *ret, jz_js err) {
    ret->ptr = 0;
    ret->len = 0;
    ret->err = err;
    ret->is_err = 1;
}

typedef struct wbg_local_task { jz_task_fn f; void *ctx; void (*ctx_drop)(void *); } wbg_local_task;

JZ_EXPORT("__wbg_run_task") void __wbg_run_task(wbg_local_task *t) {
    t->f(t->ctx);
    if (t->ctx_drop) t->ctx_drop(t->ctx);
    jz_free(t);
}

void wbg_spawn_local(jz_task_fn f, void *ctx, void (*ctx_drop)(void *)) {
    wbg_local_task *t = JZ_NEW(wbg_local_task);
    t->f = f;
    t->ctx = ctx;
    t->ctx_drop = ctx_drop;
    wbg_queue_microtask(t);
}
