/* base64_hex.c: the base64 crate (STANDARD / URL_SAFE engines, padding optional on encode,
 * indifferent on decode, canonical trailing-bit check) and the hex crate, with their error texts. */
#include "rt_internal.h"

static const char STD_ALPHA[64] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const char URL_ALPHA[64] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
#define INV 0xff

/* decode tables built once from the alphabets (the crate ships them precomputed) */
static u8 dec_std[256], dec_url[256];
static bool dec_ready;
static void build_tables(void) {
    memset(dec_std, INV, 256);
    memset(dec_url, INV, 256);
    for (u32 i = 0; i < 64; i++) {
        dec_std[(u8)STD_ALPHA[i]] = (u8)i;
        dec_url[(u8)URL_ALPHA[i]] = (u8)i;
    }
    dec_ready = true;
}

void jz_base64_encode(const u8 *p, u32 n, bool url, bool pad, jz_str *out) {
    const char *a = url ? URL_ALPHA : STD_ALPHA;
    u32 full = n / 3, rem = n % 3;
    u32 olen = full * 4 + (rem ? (pad ? 4 : rem + 1) : 0);
    jz_str_reserve(out, olen);
    char *o = out->ptr + out->len;
    u32 i = 0;
    for (; i + 3 <= n; i += 3) {
        u32 v = (u32)p[i] << 16 | (u32)p[i + 1] << 8 | p[i + 2];
        *o++ = a[v >> 18];
        *o++ = a[(v >> 12) & 63];
        *o++ = a[(v >> 6) & 63];
        *o++ = a[v & 63];
    }
    if (rem == 1) {
        u32 v = (u32)p[i] << 16;
        *o++ = a[v >> 18];
        *o++ = a[(v >> 12) & 63];
        if (pad) { *o++ = '='; *o++ = '='; }
    } else if (rem == 2) {
        u32 v = (u32)p[i] << 16 | (u32)p[i + 1] << 8;
        *o++ = a[v >> 18];
        *o++ = a[(v >> 12) & 63];
        *o++ = a[(v >> 6) & 63];
        if (pad) *o++ = '=';
    }
    out->len += olen;
}

static jz_err b64_invalid_byte(u32 off, u8 c) {
    return jz_err_fmt(JZ_BASE64_ERR, "Invalid symbol %u, offset %u.", c, off);
}

jz_err jz_base64_decode(jz_slice s, bool url, jz_bytes *out) {
    if (!dec_ready) build_tables();
    const u8 *t = url ? dec_url : dec_std;
    u32 n = s.len;
    /* strip (and validate) trailing padding */
    u32 data_len = n;
    while (data_len > 0 && s.ptr[data_len - 1] == '=') data_len--;
    u32 npad = n - data_len;
    if (npad > 2) return jz_err_new(JZ_BASE64_ERR, "Invalid padding");
    if (data_len % 4 == 1) return jz_err_new(JZ_BASE64_ERR, "Invalid input length.");
    if (npad && (data_len + npad) % 4 != 0) return jz_err_new(JZ_BASE64_ERR, "Invalid padding");
    jz_bytes_reserve(out, data_len / 4 * 3 + 3);
    u32 acc = 0, bits = 0;
    for (u32 i = 0; i < data_len; i++) {
        u8 c = s.ptr[i];
        u8 v = t[c];
        if (v == INV) {
            if (c == '=') return jz_err_fmt(JZ_BASE64_ERR, "Invalid symbol %u, offset %u.", c, i);
            return b64_invalid_byte(i, c);
        }
        acc = (acc << 6) | v;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out->ptr[out->len++] = (u8)(acc >> bits);
            acc &= (1u << bits) - 1;
        }
    }
    /* canonical encoding: leftover bits of the last symbol must be zero */
    if (bits && acc != 0) {
        u32 last = data_len - 1;
        return jz_err_fmt(JZ_BASE64_ERR, "Invalid last symbol %u, offset %u.", s.ptr[last], last);
    }
    return JZ_OK;
}

void jz_hex_encode(const u8 *p, u32 n, jz_str *out) {
    static const char H[16] = "0123456789abcdef";
    jz_str_reserve(out, n * 2);
    char *o = out->ptr + out->len;
    for (u32 i = 0; i < n; i++) {
        *o++ = H[p[i] >> 4];
        *o++ = H[p[i] & 15];
    }
    out->len += n * 2;
}

static int hex_nib(u8 c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

jz_err jz_hex_decode(jz_slice s, jz_bytes *out) {
    if (s.len % 2) return jz_err_new(JZ_HEX_ERR, "Odd number of digits");
    jz_bytes_reserve(out, s.len / 2);
    for (u32 i = 0; i < s.len; i += 2) {
        int h = hex_nib(s.ptr[i]), l = hex_nib(s.ptr[i + 1]);
        if (h < 0) return jz_err_fmt(JZ_HEX_ERR, "Invalid character '%c' at position %u", s.ptr[i], i);
        if (l < 0) return jz_err_fmt(JZ_HEX_ERR, "Invalid character '%c' at position %u", s.ptr[i + 1], i + 1);
        out->ptr[out->len++] = (u8)(h << 4 | l);
    }
    return JZ_OK;
}
