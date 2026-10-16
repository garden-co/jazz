/* uuid.c: the uuid crate (v4/v5/v7, hyphenated formatting, parsing with its error messages)
 * plus the host imports behind getrandom and web-time. */
#include "rt_internal.h"

JZ_IMPORT("random_bytes") void jz_host_random_bytes(u8 *out, u32 n);
JZ_IMPORT("now_ms") f64 jz_host_now_ms(void);

void jz_random_bytes(u8 *out, u32 n) {
    /* getrandom's js backend fills at most 65536 bytes per crypto.getRandomValues call */
    while (n) {
        u32 k = n > 65536 ? 65536 : n;
        jz_host_random_bytes(out, k);
        out += k;
        n -= k;
    }
}

u64 jz_now_ms(void) {
    f64 t = jz_host_now_ms();
    if (!(t >= 0)) return 0;
    return (u64)t;
}

int jz_uuid_cmp(const jz_uuid *a, const jz_uuid *b) {
    int c = memcmp(a->b, b->b, 16);
    return (c > 0) - (c < 0);
}

bool jz_uuid_eq(const jz_uuid *a, const jz_uuid *b) { return memcmp(a->b, b->b, 16) == 0; }
u64 jz_uuid_hash(const void *u) { return jz_hash_bytes(u, 16); }
bool jz_uuid_eq_v(const void *a, const void *b) { return memcmp(a, b, 16) == 0; }

jz_uuid jz_uuid_v7(u64 ms, const u8 rand[10]) {
    jz_uuid u;
    u.b[0] = (u8)(ms >> 40);
    u.b[1] = (u8)(ms >> 32);
    u.b[2] = (u8)(ms >> 24);
    u.b[3] = (u8)(ms >> 16);
    u.b[4] = (u8)(ms >> 8);
    u.b[5] = (u8)ms;
    u.b[6] = 0x70 | (rand[0] & 0x0f);
    u.b[7] = rand[1];
    u.b[8] = 0x80 | (rand[2] & 0x3f);
    memcpy(&u.b[9], &rand[3], 7);
    return u;
}

jz_uuid jz_uuid_v5(const jz_uuid *ns, const u8 *name, u32 len) {
    u8 stackbuf[128];
    u8 *buf = 16 + (usize)len <= sizeof(stackbuf) ? stackbuf : (u8 *)jz_alloc(16 + (usize)len);
    memcpy(buf, ns->b, 16);
    if (len) memcpy(buf + 16, name, len);
    u8 h[20];
    jz_sha1(buf, 16 + (usize)len, h);
    if (buf != stackbuf) jz_free(buf);
    jz_uuid u;
    memcpy(u.b, h, 16);
    u.b[6] = (u.b[6] & 0x0f) | 0x50;
    u.b[8] = (u.b[8] & 0x3f) | 0x80;
    return u;
}

jz_uuid jz_uuid_v4(void) {
    jz_uuid u;
    jz_random_bytes(u.b, 16);
    u.b[6] = (u.b[6] & 0x0f) | 0x40;
    u.b[8] = (u.b[8] & 0x3f) | 0x80;
    return u;
}

static const char LOWER_HEX[16] = "0123456789abcdef";

void jz_uuid_to_string(const jz_uuid *u, char out[37]) {
    static const u8 groups[5] = {4, 2, 2, 2, 6};
    u32 o = 0, bi = 0;
    for (u32 g = 0; g < 5; g++) {
        if (g) out[o++] = '-';
        for (u32 k = 0; k < groups[g]; k++, bi++) {
            out[o++] = LOWER_HEX[u->b[bi] >> 4];
            out[o++] = LOWER_HEX[u->b[bi] & 15];
        }
    }
    out[36] = 0;
}

static inline int hexval(u8 c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* uuid::error::InvalidUuid::into_err: work out *why* the input is invalid */
static jz_err uuid_diagnose(const u8 *s, u32 n, u32 off) {
    if (!jz_utf8_valid(s, n)) return jz_err_new(JZ_UUID_ERR, "non-UTF8 input");
    bool hyphens = false;
    for (u32 i = 0; i < n; i++) if (s[i] == '-') hyphens = true;
    if (hyphens) {
        static const u32 expected[5] = {8, 4, 4, 4, 12};
        u32 groups = 1, glen = 0, gstart = 0;
        u32 lens[16];
        u32 starts[16];
        for (u32 i = 0; i <= n; i++) {
            if (i == n || s[i] == '-') {
                if (groups <= 16) { lens[groups - 1] = glen; starts[groups - 1] = gstart; }
                if (i < n) { groups++; glen = 0; gstart = i + 1; }
            } else {
                glen++;
            }
        }
        if (groups != 5) return jz_err_fmt(JZ_UUID_ERR, "invalid group count: expected 5, found %u", groups);
        for (u32 g = 0; g < 5; g++) {
            if (lens[g] != expected[g])
                return jz_err_fmt(JZ_UUID_ERR, "invalid group length in group %u: expected %u, found %u", g, expected[g], lens[g]);
            for (u32 k = 0; k < lens[g]; k++) {
                u8 c = s[starts[g] + k];
                if (hexval(c) < 0) goto bad_char;
            }
        }
    }
bad_char:
    for (u32 i = 0; i < n; i++) {
        u8 c = s[i];
        if (hexval(c) < 0 && c != '-') {
            /* report the full UTF-8 char */
            u32 w = c < 0x80 ? 1 : c < 0xe0 ? 2 : c < 0xf0 ? 3 : 4;
            if (i + w > n) w = n - i;
            return jz_err_fmt(JZ_UUID_ERR,
                              "invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `%.*s` at %u",
                              (int)w, (const char *)s + i, i + off + 1);
        }
    }
    if (!hyphens) return jz_err_fmt(JZ_UUID_ERR, "invalid length: expected length 32 for simple format, found %u", n);
    return jz_err_new(JZ_UUID_ERR, "failed to parse a UUID");
}

static bool parse_simple(const u8 *s, jz_uuid *out) {
    for (u32 i = 0; i < 16; i++) {
        int h = hexval(s[2 * i]), l = hexval(s[2 * i + 1]);
        if ((h | l) < 0) return false;
        out->b[i] = (u8)(h << 4 | l);
    }
    return true;
}

static bool parse_hyphenated(const u8 *s, jz_uuid *out) {
    if (s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-') return false;
    u8 tmp[32];
    memcpy(tmp, s, 8);
    memcpy(tmp + 8, s + 9, 4);
    memcpy(tmp + 12, s + 14, 4);
    memcpy(tmp + 16, s + 19, 4);
    memcpy(tmp + 20, s + 24, 12);
    return parse_simple(tmp, out);
}

jz_err jz_uuid_parse(jz_slice in, jz_uuid *out) {
    const u8 *s = in.ptr;
    u32 n = in.len, off = 0;
    switch (n) {
    case 32:
        if (parse_simple(s, out)) return JZ_OK;
        break;
    case 36:
        if (parse_hyphenated(s, out)) return JZ_OK;
        break;
    case 38:
        if (s[0] == '{' && s[37] == '}') {
            if (parse_hyphenated(s + 1, out)) return JZ_OK;
            s += 1; n = 36; off = 1;
        }
        break;
    case 45:
        if (memcmp(s, "urn:uuid:", 9) == 0) {
            if (parse_hyphenated(s + 9, out)) return JZ_OK;
            s += 9; n = 36; off = 9;
        }
        break;
    default:
        break;
    }
    return uuid_diagnose(s, n, off);
}
