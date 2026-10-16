/* err_alloc.c: panics, error values, allocation (Rust core::panicking + alloc::alloc + anyhow-ish errors).
 * The allocator is wasi-libc's dlmalloc, the same allocator the Rust build ships. */
#include "rt_internal.h"
#include <stdio.h>

/* Rust's default panic hook prints "panicked at <loc>:\n<msg>" via console.error (console_error_panic_hook),
 * then the wasm traps with `unreachable`. */
_Noreturn void jz_panic(const char *msg) {
    static bool panicking;
    if (panicking) __builtin_trap(); /* panic inside panic: abort like std */
    panicking = true;
    jz_str s = {0};
    jz_str_push_cstr(&s, "panicked at ");
    jz_str_push_cstr(&s, msg ? msg : "explicit panic");
    jz_js_log(1, s.ptr, s.len);
    jz_js_throw(s.ptr, s.len);
    __builtin_trap();
}

_Noreturn void jz_unwrap_failed(void) { jz_panic("called `Option::unwrap()` on a `None` value"); }

_Noreturn void jz_oom(void) {
    /* alloc::alloc::handle_alloc_error: "memory allocation of N bytes failed" then abort */
    static const char m[] = "memory allocation failed";
    jz_js_log(1, m, sizeof(m) - 1);
    __builtin_trap();
}

static jz_str *err_msg_box(jz_str s) {
    jz_str *b = (jz_str *)malloc(sizeof(jz_str));
    if (!b) jz_oom();
    *b = s;
    return b;
}

jz_err jz_err_new(u32 code, const char *msg) {
    jz_err e = {code ? code : 1, NULL};
    if (msg) e.msg = err_msg_box(jz_str_from(msg));
    return e;
}

jz_err jz_err_fmt(u32 code, const char *fmt, ...) {
    jz_str s = {0};
    va_list ap;
    va_start(ap, fmt);
    jz_str_vappendf(&s, fmt, ap);
    va_end(ap);
    jz_err e = {code ? code : 1, err_msg_box(s)};
    return e;
}

void jz_err_drop(jz_err *e) {
    if (e->msg) {
        jz_str_drop(e->msg);
        free(e->msg);
        e->msg = NULL;
    }
    e->code = 0;
}

jz_err jz_err_context(jz_err e, const char *ctx) {
    if (!e.code) return e;
    jz_str s = {0};
    jz_str_push_cstr(&s, ctx);
    if (e.msg) {
        jz_str_push(&s, ": ", 2);
        jz_str_push(&s, e.msg->ptr, e.msg->len);
        jz_str_drop(e.msg);
        *e.msg = s;
    } else {
        jz_str_appendf(&s, ": error code %u", e.code);
        e.msg = err_msg_box(s);
    }
    return e;
}

/* ---------- allocation ---------- */
void *jz_alloc(usize n) {
    void *p = malloc(n ? n : 1);
    if (!p) jz_oom();
    return p;
}

void *jz_zalloc(usize n) {
    void *p = calloc(1, n ? n : 1);
    if (!p) jz_oom();
    return p;
}

void *jz_realloc(void *p, usize n) {
    void *q = realloc(p, n ? n : 1);
    if (!q) jz_oom();
    return q;
}

void jz_free(void *p) { free(p); }

void *jz_memdup(const void *p, usize n) {
    void *q = jz_alloc(n);
    if (n) memcpy(q, p, n);
    return q;
}

u32 jz_grow_cap(u32 cap, u32 need, u32 min_cap) {
    if (need < cap) return cap;
    u64 c = (u64)cap * 2;
    if (c < need) c = need;
    if (c < min_cap) c = min_cap;
    if (c > 0x7fffffffu) {
        if (need > 0x7fffffffu) jz_panic("capacity overflow");
        c = 0x7fffffffu;
    }
    return (u32)c;
}

void jz_mem_swap(void *a, void *b, usize n) {
    u8 *x = (u8 *)a, *y = (u8 *)b;
    u8 tmp[64];
    while (n) {
        usize k = n < sizeof(tmp) ? n : sizeof(tmp);
        memcpy(tmp, x, k);
        memcpy(x, y, k);
        memcpy(y, tmp, k);
        x += k; y += k; n -= k;
    }
}
