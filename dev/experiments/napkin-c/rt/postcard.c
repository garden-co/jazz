/* postcard.c: postcard's serde wire format (varint LEB128 integers, zigzag signed ints,
 * little-endian floats, length-prefixed bytes/str/seq, varint enum discriminants), with
 * postcard's error messages. */
#include "rt_internal.h"

/* ---------- writer ---------- */
void jz_pc_u8(jz_pc_w *w, u8 v) { jz_bytes_push_u8(w->out, v); }
void jz_pc_bool(jz_pc_w *w, bool v) { jz_bytes_push_u8(w->out, v ? 1 : 0); }

void jz_pc_varint(jz_pc_w *w, u64 v) {
    u8 buf[10];
    u32 n = 0;
    do {
        u8 b = (u8)(v & 0x7f);
        v >>= 7;
        if (v) b |= 0x80;
        buf[n++] = b;
    } while (v);
    jz_bytes_push(w->out, buf, n);
}

void jz_pc_zigzag(jz_pc_w *w, i64 v) { jz_pc_varint(w, ((u64)v << 1) ^ (u64)(v >> 63)); }

void jz_pc_f64(jz_pc_w *w, f64 v) {
    u8 b[8];
    memcpy(b, &v, 8); /* wasm32 is little-endian, as postcard requires */
    jz_bytes_push(w->out, b, 8);
}

void jz_pc_f32(jz_pc_w *w, f32 v) {
    u8 b[4];
    memcpy(b, &v, 4);
    jz_bytes_push(w->out, b, 4);
}

void jz_pc_bytes(jz_pc_w *w, const u8 *p, u32 n) {
    jz_pc_varint(w, n);
    jz_bytes_push(w->out, p, n);
}

void jz_pc_str(jz_pc_w *w, const jz_str *s) { jz_pc_bytes(w, (const u8 *)s->ptr, s->len); }
void jz_pc_raw(jz_pc_w *w, const u8 *p, u32 n) { jz_bytes_push(w->out, p, n); }
/* uuid's Serialize for non-human-readable formats is serialize_bytes(&[u8; 16]) */
void jz_pc_uuid(jz_pc_w *w, const jz_uuid *u) { jz_pc_bytes(w, u->b, 16); }
void jz_pc_option(jz_pc_w *w, bool some) { jz_bytes_push_u8(w->out, some ? 1 : 0); }
void jz_pc_seq(jz_pc_w *w, u32 n) { jz_pc_varint(w, n); }
void jz_pc_variant(jz_pc_w *w, u32 idx) { jz_pc_varint(w, idx); }

/* ---------- reader ---------- */
static jz_err pc_eof(void) { return jz_err_new(JZ_PC_EOF, "Hit the end of buffer, expected more data"); }
static jz_err pc_bad_varint(void) {
    return jz_err_new(JZ_PC_BAD_VARINT, "Found a varint that didn't terminate. Is the usize too big for this platform?");
}

jz_err jz_pc_r_u8(jz_pc_r *r, u8 *v) {
    if (r->p >= r->end) return pc_eof();
    *v = *r->p++;
    return JZ_OK;
}

jz_err jz_pc_r_bool(jz_pc_r *r, bool *v) {
    u8 b;
    JZ_TRY(jz_pc_r_u8(r, &b));
    if (b > 1) return jz_err_new(JZ_PC_BAD_BOOL, "Found a bool that wasn't 0 or 1");
    *v = b == 1;
    return JZ_OK;
}

/* decode a varint of at most `bits` bits (postcard's try_take_varint_uNN) */
static jz_err pc_varint_bits(jz_pc_r *r, u32 bits, u64 *v) {
    u32 max_bytes = (bits + 6) / 7;
    u64 out = 0;
    for (u32 i = 0; i < max_bytes; i++) {
        if (r->p >= r->end) return pc_eof();
        u8 b = *r->p++;
        out |= (u64)(b & 0x7f) << (7 * i);
        if (!(b & 0x80)) {
            /* the last allowed byte must not carry bits beyond the type's width */
            if (i == max_bytes - 1 && bits < 64 && (b >> (bits - 7 * i)) != 0) return pc_bad_varint();
            if (i == max_bytes - 1 && bits == 64 && b > 1) return pc_bad_varint();
            *v = out;
            return JZ_OK;
        }
    }
    return pc_bad_varint();
}

jz_err jz_pc_r_varint(jz_pc_r *r, u64 *v) { return pc_varint_bits(r, 64, v); }

jz_err jz_pc_r_u32(jz_pc_r *r, u32 *v) {
    u64 t;
    JZ_TRY(pc_varint_bits(r, 32, &t));
    *v = (u32)t;
    return JZ_OK;
}

jz_err jz_pc_r_u16(jz_pc_r *r, u16 *v) {
    u64 t;
    JZ_TRY(pc_varint_bits(r, 16, &t));
    *v = (u16)t;
    return JZ_OK;
}

jz_err jz_pc_r_zigzag(jz_pc_r *r, i64 *v) {
    u64 t;
    JZ_TRY(pc_varint_bits(r, 64, &t));
    *v = (i64)(t >> 1) ^ -(i64)(t & 1);
    return JZ_OK;
}

jz_err jz_pc_r_f64(jz_pc_r *r, f64 *v) {
    if (r->end - r->p < 8) return pc_eof();
    memcpy(v, r->p, 8);
    r->p += 8;
    return JZ_OK;
}

static jz_err pc_take_len(jz_pc_r *r, u32 *n) {
    JZ_TRY(jz_pc_r_u32(r, n)); /* usize is 32-bit on wasm32 */
    if ((usize)(r->end - r->p) < *n) return pc_eof();
    return JZ_OK;
}

jz_err jz_pc_r_bytes_ref(jz_pc_r *r, jz_slice *out) {
    u32 n;
    JZ_TRY(pc_take_len(r, &n));
    out->ptr = r->p;
    out->len = n;
    r->p += n;
    return JZ_OK;
}

jz_err jz_pc_r_bytes(jz_pc_r *r, jz_bytes *out) {
    jz_slice s;
    JZ_TRY(jz_pc_r_bytes_ref(r, &s));
    *out = jz_bytes_from(s.ptr, s.len);
    return JZ_OK;
}

jz_err jz_pc_r_str(jz_pc_r *r, jz_str *out) {
    jz_slice s;
    JZ_TRY(jz_pc_r_bytes_ref(r, &s));
    if (!jz_utf8_valid(s.ptr, s.len)) return jz_err_new(JZ_PC_BAD_UTF8, "Tried to parse invalid utf-8");
    *out = jz_str_from_slice(s);
    return JZ_OK;
}

jz_err jz_pc_r_raw(jz_pc_r *r, u8 *out, u32 n) {
    if ((usize)(r->end - r->p) < n) return pc_eof();
    memcpy(out, r->p, n);
    r->p += n;
    return JZ_OK;
}

jz_err jz_pc_r_uuid(jz_pc_r *r, jz_uuid *out) {
    jz_slice s;
    JZ_TRY(jz_pc_r_bytes_ref(r, &s));
    if (s.len != 16) return jz_err_fmt(JZ_UUID_ERR, "invalid length: expected 16 bytes, found %u", s.len);
    memcpy(out->b, s.ptr, 16);
    return JZ_OK;
}

jz_err jz_pc_r_option(jz_pc_r *r, bool *some) {
    u8 b;
    JZ_TRY(jz_pc_r_u8(r, &b));
    if (b > 1) return jz_err_new(JZ_PC_BAD_BOOL, "Found an Option discriminant that wasn't 0 or 1");
    *some = b == 1;
    return JZ_OK;
}

jz_err jz_pc_r_seq(jz_pc_r *r, u32 *n) {
    JZ_TRY(jz_pc_r_u32(r, n));
    /* serde's size_hint::cautious: never trust a length larger than the remaining input */
    return JZ_OK;
}

jz_err jz_pc_r_variant(jz_pc_r *r, u32 *idx, u32 nvariants) {
    JZ_TRY(jz_pc_r_u32(r, idx));
    if (*idx >= nvariants)
        return jz_err_fmt(JZ_PC_BAD_VARIANT, "invalid value: integer `%u`, expected variant index 0 <= i < %u", *idx, nvariants);
    return JZ_OK;
}

jz_err jz_pc_r_finish(jz_pc_r *r) {
    if (r->p != r->end) return jz_err_new(JZ_PC_TRAILING, "Trailing bytes after the deserialized value");
    return JZ_OK;
}

jz_err jz_pc_r_enter(jz_pc_r *r) {
    if (++r->depth > 128) return jz_err_new(JZ_PC_DEPTH, "recursion limit exceeded");
    return JZ_OK;
}

void jz_pc_r_leave(jz_pc_r *r) {
    if (r->depth) r->depth--;
}
