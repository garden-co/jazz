/* str.c: String / Vec<u8> / &[u8] helpers, format!/write! (alloc::string, core::fmt, core::str::from_utf8). */
#include "rt_internal.h"
#include <stdio.h>

/* ---------- jz_str ---------- */
void jz_str_reserve(jz_str *s, u32 extra) {
    u32 need = s->len + extra;
    if (need < s->len) jz_panic("capacity overflow");
    if (need <= s->cap) return;
    u32 cap = jz_grow_cap(s->cap, need, 8);
    s->ptr = (char *)jz_realloc(s->ptr, cap);
    s->cap = cap;
}

jz_str jz_str_from(const char *p) {
    jz_str s = {0};
    if (p) jz_str_push(&s, p, (u32)strlen(p));
    return s;
}

jz_str jz_str_from_slice(jz_slice sl) {
    jz_str s = {0};
    jz_str_push(&s, (const char *)sl.ptr, sl.len);
    return s;
}

jz_str jz_str_clone(const jz_str *s) {
    jz_str r = {0};
    if (s->len) {
        r.ptr = (char *)jz_memdup(s->ptr, s->len);
        r.len = r.cap = s->len;
    }
    return r;
}

void jz_str_drop(jz_str *s) {
    if (s->cap) jz_free(s->ptr);
    s->ptr = NULL;
    s->len = s->cap = 0;
}

void jz_str_push(jz_str *s, const char *p, u32 n) {
    if (!n) return;
    jz_str_reserve(s, n);
    memcpy(s->ptr + s->len, p, n);
    s->len += n;
}

void jz_str_push_cstr(jz_str *s, const char *p) { jz_str_push(s, p, (u32)strlen(p)); }

void jz_str_push_char(jz_str *s, char c) {
    if (s->len == s->cap) jz_str_reserve(s, 1);
    s->ptr[s->len++] = c;
}

bool jz_str_eq(const jz_str *a, const jz_str *b) {
    return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, a->len) == 0);
}

bool jz_str_eq_cstr(const jz_str *a, const char *b) {
    usize n = strlen(b);
    return a->len == n && (n == 0 || memcmp(a->ptr, b, n) == 0);
}

int jz_slice_cmp(jz_slice a, jz_slice b) {
    u32 n = a.len < b.len ? a.len : b.len;
    int c = n ? memcmp(a.ptr, b.ptr, n) : 0;
    if (c) return c < 0 ? -1 : 1;
    return a.len < b.len ? -1 : a.len > b.len ? 1 : 0;
}

int jz_str_cmp(const jz_str *a, const jz_str *b) { return jz_slice_cmp(jz_str_slice(a), jz_str_slice(b)); }

jz_slice jz_str_slice(const jz_str *s) {
    jz_slice r = {(const u8 *)s->ptr, s->len};
    return r;
}

u64 jz_str_hash(const jz_str *s) {
    /* Hash for str writes the bytes then a 0xff terminator (core::hash::Hash for str) */
    return jz_hash_combine(jz_hash_bytes(s->ptr, s->len), 0xff);
}

void jz_str_vappendf(jz_str *s, const char *fmt, va_list ap) {
    char buf[256];
    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    if (n < 0) {
        va_end(ap2);
        jz_panic("a formatting trait implementation returned an error");
    }
    if ((usize)n < sizeof(buf)) {
        jz_str_push(s, buf, (u32)n);
    } else {
        jz_str_reserve(s, (u32)n + 1);
        vsnprintf(s->ptr + s->len, (usize)n + 1, fmt, ap2);
        s->len += (u32)n;
    }
    va_end(ap2);
}

jz_str jz_format(const char *fmt, ...) {
    jz_str s = {0};
    va_list ap;
    va_start(ap, fmt);
    jz_str_vappendf(&s, fmt, ap);
    va_end(ap);
    return s;
}

void jz_str_appendf(jz_str *s, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    jz_str_vappendf(s, fmt, ap);
    va_end(ap);
}

/* core::str::from_utf8 validation: rejects overlong encodings, surrogates and code points > U+10FFFF.
 * ASCII fast path checks 4 bytes at a time like core's word-at-a-time loop. */
bool jz_utf8_valid(const u8 *p, u32 n) {
    u32 i = 0;
    while (i < n) {
        u8 c = p[i];
        if (c < 0x80) {
            if ((((uintptr_t)(p + i)) & 3) == 0) {
                while (i + 4 <= n) {
                    u32 w;
                    memcpy(&w, p + i, 4);
                    if (w & 0x80808080u) break;
                    i += 4;
                }
                if (i >= n) break;
                if (p[i] < 0x80) { i++; continue; }
                continue;
            }
            i++;
            continue;
        }
        u32 w;
        if (c >= 0xc2 && c <= 0xdf) w = 2;
        else if (c >= 0xe0 && c <= 0xef) w = 3;
        else if (c >= 0xf0 && c <= 0xf4) w = 4;
        else return false;
        if (i + w > n) return false;
        u8 c1 = p[i + 1];
        switch (w) {
        case 2:
            if ((c1 & 0xc0) != 0x80) return false;
            break;
        case 3:
            if (c == 0xe0 && (c1 < 0xa0 || c1 > 0xbf)) return false;
            else if (c == 0xed && (c1 < 0x80 || c1 > 0x9f)) return false;
            else if ((c1 & 0xc0) != 0x80) return false;
            if ((p[i + 2] & 0xc0) != 0x80) return false;
            break;
        case 4:
            if (c == 0xf0 && (c1 < 0x90 || c1 > 0xbf)) return false;
            else if (c == 0xf4 && (c1 < 0x80 || c1 > 0x8f)) return false;
            else if ((c1 & 0xc0) != 0x80) return false;
            if ((p[i + 2] & 0xc0) != 0x80 || (p[i + 3] & 0xc0) != 0x80) return false;
            break;
        }
        i += w;
    }
    return true;
}

/* ---------- jz_bytes ---------- */
void jz_bytes_reserve(jz_bytes *b, u32 extra) {
    u32 need = b->len + extra;
    if (need < b->len) jz_panic("capacity overflow");
    if (need <= b->cap) return;
    u32 cap = jz_grow_cap(b->cap, need, 8);
    b->ptr = (u8 *)jz_realloc(b->ptr, cap);
    b->cap = cap;
}

jz_bytes jz_bytes_from(const u8 *p, u32 n) {
    jz_bytes b = {0};
    jz_bytes_push(&b, p, n);
    return b;
}

jz_bytes jz_bytes_clone(const jz_bytes *b) { return jz_bytes_from(b->ptr, b->len); }

void jz_bytes_drop(jz_bytes *b) {
    if (b->cap) jz_free(b->ptr);
    b->ptr = NULL;
    b->len = b->cap = 0;
}

void jz_bytes_push(jz_bytes *b, const u8 *p, u32 n) {
    if (!n) return;
    jz_bytes_reserve(b, n);
    memcpy(b->ptr + b->len, p, n);
    b->len += n;
}

void jz_bytes_push_u8(jz_bytes *b, u8 v) {
    if (b->len == b->cap) jz_bytes_reserve(b, 1);
    b->ptr[b->len++] = v;
}

int jz_bytes_cmp(const jz_bytes *a, const jz_bytes *b) { return jz_slice_cmp(jz_bytes_slice(a), jz_bytes_slice(b)); }

bool jz_bytes_eq(const jz_bytes *a, const jz_bytes *b) {
    return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, a->len) == 0);
}

jz_slice jz_bytes_slice(const jz_bytes *b) {
    jz_slice r = {b->ptr, b->len};
    return r;
}
