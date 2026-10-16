/* json.c: serde_json: Value tree, recursive-descent parser (serde_json's number handling: u64/i64
 * inference, significand+exponent, exact POW10 fast path else correctly rounded strtod, "-0" as f64), compact and pretty
 * serializers (ESCAPE table, itoa digit pairs, shortest round-trip floats formatted like ryu),
 * a streaming writer for hand-written Serialize impls and helpers for hand-written Deserialize
 * impls producing serde's error texts. Objects are BTreeMap<String, Value> (sorted keys). */
#include "rt_internal.h"
#include <stdio.h>

#define RECURSION_LIMIT 128

static void json_ptr_drop(void *p) { jz_json_free(*(jz_json **)p); }
static void json_key_drop(void *p) { jz_str_drop((jz_str *)p); }
static const jz_btree_desc JSON_OBJ_DESC = {sizeof(jz_str), sizeof(jz_json *), jz_cmp_str, json_key_drop, json_ptr_drop};

/* ---------- constructors / accessors ---------- */
static jz_json *json_new(jz_json_kind k) {
    jz_json *v = JZ_NEW(jz_json);
    v->kind = k;
    if (k == JZ_JSON_OBJ) jz_btree_init(&v->v.obj, &JSON_OBJ_DESC);
    return v;
}

jz_json *jz_json_null(void) { return json_new(JZ_JSON_NULL); }
jz_json *jz_json_bool(bool b) { jz_json *v = json_new(JZ_JSON_BOOL); v->v.b = b; return v; }
jz_json *jz_json_u64(u64 x) { jz_json *v = json_new(JZ_JSON_U64); v->v.u = x; return v; }
jz_json *jz_json_i64(i64 x) {
    /* serde_json::Number from i64 is PosInt for non-negative values */
    if (x >= 0) return jz_json_u64((u64)x);
    jz_json *v = json_new(JZ_JSON_I64);
    v->v.i = x;
    return v;
}
jz_json *jz_json_f64(f64 x) {
    /* Value::from(f64) maps NaN/inf to Null */
    if (x != x || x - x != 0) return jz_json_null();
    jz_json *v = json_new(JZ_JSON_F64);
    v->v.f = x;
    return v;
}
jz_json *jz_json_str(const char *s) { jz_json *v = json_new(JZ_JSON_STR); v->v.s = jz_str_from(s); return v; }
jz_json *jz_json_str_owned(jz_str s) { jz_json *v = json_new(JZ_JSON_STR); v->v.s = s; return v; }
jz_json *jz_json_arr(void) { return json_new(JZ_JSON_ARR); }
jz_json *jz_json_obj(void) { return json_new(JZ_JSON_OBJ); }

void jz_json_arr_push(jz_json *arr, jz_json *v) {
    if (arr->kind != JZ_JSON_ARR) jz_panic("jz_json_arr_push: not an array");
    JZ_VEC_PUSH(arr->v.arr, v);
}

void jz_json_obj_set(jz_json *obj, const char *key, jz_json *v) {
    if (obj->kind != JZ_JSON_OBJ) jz_panic("jz_json_obj_set: not an object");
    jz_str k = jz_str_from(key);
    jz_btree_insert(&obj->v.obj, &k, &v, NULL);
}

jz_json *jz_json_obj_get(const jz_json *obj, const char *key) {
    if (!obj || obj->kind != JZ_JSON_OBJ) return NULL;
    jz_str k = {(char *)key, (u32)strlen(key), 0};
    jz_json **p = (jz_json **)jz_btree_get(&obj->v.obj, &k);
    return p ? *p : NULL;
}

const jz_str *jz_json_as_str(const jz_json *v) { return v && v->kind == JZ_JSON_STR ? &v->v.s : NULL; }

bool jz_json_as_u64(const jz_json *v, u64 *out) {
    if (!v) return false;
    if (v->kind == JZ_JSON_U64) { *out = v->v.u; return true; }
    return false; /* NegInt and Float are never u64 */
}

bool jz_json_as_i64(const jz_json *v, i64 *out) {
    if (!v) return false;
    if (v->kind == JZ_JSON_I64) { *out = v->v.i; return true; }
    if (v->kind == JZ_JSON_U64 && v->v.u <= (u64)INT64_MAX) { *out = (i64)v->v.u; return true; }
    return false;
}

bool jz_json_as_f64(const jz_json *v, f64 *out) {
    if (!v) return false;
    switch (v->kind) {
    case JZ_JSON_U64: *out = (f64)v->v.u; return true;
    case JZ_JSON_I64: *out = (f64)v->v.i; return true;
    case JZ_JSON_F64: *out = v->v.f; return true;
    default: return false;
    }
}

void jz_json_free(jz_json *v) {
    if (!v) return;
    switch (v->kind) {
    case JZ_JSON_STR: jz_str_drop(&v->v.s); break;
    case JZ_JSON_ARR:
        for (u32 i = 0; i < v->v.arr.len; i++) jz_json_free(v->v.arr.ptr[i]);
        jz_vec_drop(&v->v.arr, sizeof(jz_json *), NULL);
        break;
    case JZ_JSON_OBJ: jz_btree_drop(&v->v.obj); break;
    default: break;
    }
    jz_free(v);
}

static void json_key_clone(void *d, const void *s) { *(jz_str *)d = jz_str_clone((const jz_str *)s); }
static void json_ptr_clone(void *d, const void *s) { *(jz_json **)d = jz_json_clone(*(jz_json *const *)s); }

jz_json *jz_json_clone(const jz_json *v) {
    if (!v) return NULL;
    jz_json *c = JZ_NEW(jz_json);
    c->kind = v->kind;
    switch (v->kind) {
    case JZ_JSON_STR: c->v.s = jz_str_clone(&v->v.s); break;
    case JZ_JSON_ARR:
        jz_vec_reserve(&c->v.arr, v->v.arr.len, sizeof(jz_json *));
        for (u32 i = 0; i < v->v.arr.len; i++) c->v.arr.ptr[c->v.arr.len++] = jz_json_clone(v->v.arr.ptr[i]);
        break;
    case JZ_JSON_OBJ: jz_btree_clone(&c->v.obj, &v->v.obj, json_key_clone, json_ptr_clone); break;
    default: c->v = v->v; break;
    }
    return c;
}

static bool json_ptr_eq(const void *a, const void *b) { return jz_json_eq(*(jz_json *const *)a, *(jz_json *const *)b); }

bool jz_json_eq(const jz_json *a, const jz_json *b) {
    if (a == b) return true;
    if (!a || !b || a->kind != b->kind) return false;
    switch (a->kind) {
    case JZ_JSON_NULL: return true;
    case JZ_JSON_BOOL: return a->v.b == b->v.b;
    case JZ_JSON_U64: return a->v.u == b->v.u;
    case JZ_JSON_I64: return a->v.i == b->v.i;
    case JZ_JSON_F64: return a->v.f == b->v.f;
    case JZ_JSON_STR: return jz_str_eq(&a->v.s, &b->v.s);
    case JZ_JSON_ARR:
        if (a->v.arr.len != b->v.arr.len) return false;
        for (u32 i = 0; i < a->v.arr.len; i++)
            if (!jz_json_eq(a->v.arr.ptr[i], b->v.arr.ptr[i])) return false;
        return true;
    case JZ_JSON_OBJ: return jz_btree_eq(&a->v.obj, &b->v.obj, json_ptr_eq);
    }
    return false;
}

/* ---------- parser ---------- */
typedef struct jp {
    const u8 *start, *p, *end, *num_start;
    u32 depth;
    jz_str scratch;
} jp;

static jz_err jp_error(const jp *P, const char *what) {
    u32 line = 1, col = 0;
    for (const u8 *q = P->start; q < P->p; q++) {
        if (*q == '\n') { line++; col = 0; }
        else col++;
    }
    return jz_err_fmt(JZ_JSON_ERR_SYNTAX, "%s at line %u column %u", what, line, col);
}

static inline void jp_ws(jp *P) {
    while (P->p < P->end && (*P->p == ' ' || *P->p == '\n' || *P->p == '\t' || *P->p == '\r')) P->p++;
}

static const f64 POW10[23] = {1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22};

/* serde_json's f64_from_parts, with the float_roundtrip behaviour: exact fast path when the
 * significand and power of ten are both exactly representable, otherwise a correctly rounded
 * strtod of the literal. */
static jz_err f64_from_parts(jp *P, bool positive, u64 significand, i32 exponent, f64 *out) {
    f64 f;
    if (significand <= (1ull << 53) && exponent >= -22 && exponent <= 22) {
        f = (f64)significand;
        if (exponent >= 0) f *= POW10[exponent];
        else f /= POW10[-exponent];
        *out = positive ? f : -f;
        return JZ_OK;
    }
    u32 n = (u32)(P->p - P->num_start);
    char stackbuf[64];
    char *buf = n < sizeof(stackbuf) ? stackbuf : (char *)jz_alloc(n + 1);
    memcpy(buf, P->num_start, n);
    buf[n] = 0;
    f = strtod(buf, NULL);
    if (buf != stackbuf) jz_free(buf);
    if (f - f != 0) return jp_error(P, "number out of range");
    *out = f; /* the literal carries its own sign */
    (void)positive;
    return JZ_OK;
}

static inline bool is_digit(u8 c) { return c >= '0' && c <= '9'; }

static jz_err jp_exponent(jp *P, bool positive, u64 significand, i32 starting_exp, f64 *out) {
    P->p++; /* e / E */
    bool pos_exp = true;
    if (P->p < P->end && (*P->p == '+' || *P->p == '-')) {
        pos_exp = *P->p == '+';
        P->p++;
    }
    if (P->p >= P->end) return jp_error(P, "EOF while parsing a value");
    if (!is_digit(*P->p)) return jp_error(P, "invalid number");
    i32 exp = 0;
    while (P->p < P->end && is_digit(*P->p)) {
        i32 d = *P->p - '0';
        if (exp > (INT32_MAX - d) / 10) {
            /* exponent overflow: serde_json's parse_exponent_overflow */
            while (P->p < P->end && is_digit(*P->p)) P->p++;
            if (significand != 0 && pos_exp) return jp_error(P, "number out of range");
            *out = positive ? 0.0 : -0.0;
            return JZ_OK;
        }
        exp = exp * 10 + d;
        P->p++;
    }
    i64 final = (i64)starting_exp + (pos_exp ? exp : -(i64)exp);
    if (final > INT32_MAX) final = INT32_MAX;
    if (final < INT32_MIN) final = INT32_MIN;
    return f64_from_parts(P, positive, significand, (i32)final, out);
}

static jz_err jp_decimal(jp *P, bool positive, u64 significand, i32 exponent, f64 *out) {
    P->p++; /* '.' */
    if (P->p >= P->end) return jp_error(P, "EOF while parsing a value");
    if (!is_digit(*P->p)) return jp_error(P, "invalid number");
    while (P->p < P->end && is_digit(*P->p)) {
        u64 d = (u64)(*P->p - '0');
        if (significand > (UINT64_MAX - d) / 10) {
            /* parse_decimal_overflow: further digits cannot change the f64 meaningfully */
            while (P->p < P->end && is_digit(*P->p)) P->p++;
            break;
        }
        significand = significand * 10 + d;
        exponent--;
        P->p++;
    }
    if (P->p < P->end && (*P->p == 'e' || *P->p == 'E')) return jp_exponent(P, positive, significand, exponent, out);
    return f64_from_parts(P, positive, significand, exponent, out);
}

static jz_err jp_number(jp *P, jz_json **out) {
    bool positive = true;
    P->num_start = P->p;
    if (*P->p == '-') {
        positive = false;
        P->p++;
    }
    if (P->p >= P->end) return jp_error(P, "EOF while parsing a value");
    u8 c = *P->p;
    if (!is_digit(c)) return jp_error(P, "invalid number");
    u64 sig = 0;
    i32 exponent = 0;
    bool is_float = false;
    f64 f = 0;
    P->p++;
    if (c == '0') {
        if (P->p < P->end && is_digit(*P->p)) return jp_error(P, "invalid number");
    } else {
        sig = (u64)(c - '0');
        while (P->p < P->end && is_digit(*P->p)) {
            u64 d = (u64)(*P->p - '0');
            if (sig > (UINT64_MAX - d) / 10) {
                /* parse_long_integer: count remaining integer digits into the exponent */
                while (P->p < P->end && is_digit(*P->p)) {
                    exponent++;
                    P->p++;
                }
                is_float = true;
                break;
            }
            sig = sig * 10 + d;
            P->p++;
        }
    }
    if (P->p < P->end && *P->p == '.') {
        JZ_TRY(jp_decimal(P, positive, sig, exponent, &f));
        is_float = true;
    } else if (P->p < P->end && (*P->p == 'e' || *P->p == 'E')) {
        JZ_TRY(jp_exponent(P, positive, sig, exponent, &f));
        is_float = true;
    } else if (is_float) {
        JZ_TRY(f64_from_parts(P, positive, sig, exponent, &f));
    }
    if (is_float) {
        jz_json *v = json_new(JZ_JSON_F64);
        v->v.f = f;
        *out = v;
        return JZ_OK;
    }
    if (positive) {
        *out = jz_json_u64(sig);
    } else if (sig == 0) {
        jz_json *v = json_new(JZ_JSON_F64); /* "-0" is a float in serde_json */
        v->v.f = -0.0;
        *out = v;
    } else if (sig <= (u64)INT64_MAX + 1) {
        jz_json *v = json_new(JZ_JSON_I64);
        v->v.i = (i64)(0 - sig);
        *out = v;
    } else {
        jz_json *v = json_new(JZ_JSON_F64);
        v->v.f = -(f64)sig;
        *out = v;
    }
    return JZ_OK;
}

/* serde_json's ESCAPE table: which bytes end a plain run inside a string literal */
#define BB 'b'
#define TT 't'
#define NN 'n'
#define FF 'f'
#define RR 'r'
#define QU '"'
#define BS '\\'
#define UU 'u'
#define __ 0
static const u8 ESCAPE[256] = {
    UU, UU, UU, UU, UU, UU, UU, UU, BB, TT, NN, UU, FF, RR, UU, UU,
    UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU, UU,
    __, __, QU, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, BS, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
};

static void push_utf8(jz_str *s, u32 cp) {
    char b[4];
    u32 n;
    if (cp < 0x80) { b[0] = (char)cp; n = 1; }
    else if (cp < 0x800) { b[0] = (char)(0xc0 | cp >> 6); b[1] = (char)(0x80 | (cp & 0x3f)); n = 2; }
    else if (cp < 0x10000) {
        b[0] = (char)(0xe0 | cp >> 12); b[1] = (char)(0x80 | ((cp >> 6) & 0x3f)); b[2] = (char)(0x80 | (cp & 0x3f)); n = 3;
    } else {
        b[0] = (char)(0xf0 | cp >> 18); b[1] = (char)(0x80 | ((cp >> 12) & 0x3f));
        b[2] = (char)(0x80 | ((cp >> 6) & 0x3f)); b[3] = (char)(0x80 | (cp & 0x3f)); n = 4;
    }
    jz_str_push(s, b, n);
}

static jz_err jp_hex4(jp *P, u32 *out) {
    if (P->end - P->p < 4) {
        P->p = P->end;
        return jp_error(P, "EOF while parsing a string");
    }
    u32 v = 0;
    for (int i = 0; i < 4; i++) {
        u8 c = *P->p++;
        u32 d;
        if (c >= '0' && c <= '9') d = c - '0';
        else if (c >= 'a' && c <= 'f') d = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') d = c - 'A' + 10;
        else return jp_error(P, "invalid escape");
        v = v << 4 | d;
    }
    *out = v;
    return JZ_OK;
}

/* parse a string literal (opening quote already consumed) into P->scratch */
static jz_err jp_string(jp *P) {
    P->scratch.len = 0;
    for (;;) {
        const u8 *run = P->p;
        while (P->p < P->end && !ESCAPE[*P->p]) P->p++;
        if (P->p > run) {
            if (!jz_utf8_valid(run, (u32)(P->p - run))) return jp_error(P, "invalid unicode code point");
            jz_str_push(&P->scratch, (const char *)run, (u32)(P->p - run));
        }
        if (P->p >= P->end) return jp_error(P, "EOF while parsing a string");
        u8 c = *P->p++;
        if (c == '"') return JZ_OK;
        if (c != '\\') return jp_error(P, "control character (\\u0000-\\u001F) found while parsing a string");
        if (P->p >= P->end) return jp_error(P, "EOF while parsing a string");
        u8 e = *P->p++;
        switch (e) {
        case '"': jz_str_push_char(&P->scratch, '"'); break;
        case '\\': jz_str_push_char(&P->scratch, '\\'); break;
        case '/': jz_str_push_char(&P->scratch, '/'); break;
        case 'b': jz_str_push_char(&P->scratch, '\b'); break;
        case 'f': jz_str_push_char(&P->scratch, '\f'); break;
        case 'n': jz_str_push_char(&P->scratch, '\n'); break;
        case 'r': jz_str_push_char(&P->scratch, '\r'); break;
        case 't': jz_str_push_char(&P->scratch, '\t'); break;
        case 'u': {
            u32 cp;
            JZ_TRY(jp_hex4(P, &cp));
            if (cp >= 0xDC00 && cp <= 0xDFFF) return jp_error(P, "lone leading surrogate in hex escape");
            if (cp >= 0xD800 && cp <= 0xDBFF) {
                if (P->end - P->p < 2) {
                    P->p = P->end;
                    return jp_error(P, "unexpected end of hex escape");
                }
                if (P->p[0] != '\\' || P->p[1] != 'u') return jp_error(P, "lone leading surrogate in hex escape");
                P->p += 2;
                u32 lo;
                JZ_TRY(jp_hex4(P, &lo));
                if (lo < 0xDC00 || lo > 0xDFFF) return jp_error(P, "lone leading surrogate in hex escape");
                cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
            }
            push_utf8(&P->scratch, cp);
            break;
        }
        default: return jp_error(P, "invalid escape");
        }
    }
}

static jz_err jp_ident(jp *P, const char *rest) {
    for (const char *r = rest; *r; r++) {
        if (P->p >= P->end) return jp_error(P, "EOF while parsing a value");
        if (*P->p != (u8)*r) return jp_error(P, "expected ident");
        P->p++;
    }
    return JZ_OK;
}

static jz_err jp_value(jp *P, jz_json **out);

static jz_err jp_array(jp *P, jz_json *arr) {
    jp_ws(P);
    if (P->p < P->end && *P->p == ']') { P->p++; return JZ_OK; }
    for (;;) {
        jz_json *item = NULL;
        JZ_TRY(jp_value(P, &item));
        JZ_VEC_PUSH(arr->v.arr, item);
        jp_ws(P);
        if (P->p >= P->end) return jp_error(P, "EOF while parsing a list");
        u8 c = *P->p++;
        if (c == ']') return JZ_OK;
        if (c != ',') return jp_error(P, "expected `,` or `]`");
        jp_ws(P);
        if (P->p < P->end && *P->p == ']') return jp_error(P, "trailing comma");
    }
}

static jz_err jp_object(jp *P, jz_json *obj) {
    jp_ws(P);
    if (P->p < P->end && *P->p == '}') { P->p++; return JZ_OK; }
    for (;;) {
        jp_ws(P);
        if (P->p >= P->end) return jp_error(P, "EOF while parsing an object");
        if (*P->p != '"') return jp_error(P, "key must be a string");
        P->p++;
        JZ_TRY(jp_string(P));
        jz_str key = jz_str_clone(&P->scratch);
        jp_ws(P);
        if (P->p >= P->end) { jz_str_drop(&key); return jp_error(P, "EOF while parsing an object"); }
        if (*P->p != ':') { jz_str_drop(&key); return jp_error(P, "expected `:`"); }
        P->p++;
        jz_json *val = NULL;
        jz_err e = jp_value(P, &val);
        if (e.code) { jz_str_drop(&key); return e; }
        jz_btree_insert(&obj->v.obj, &key, &val, NULL); /* duplicate keys: last one wins */
        jp_ws(P);
        if (P->p >= P->end) return jp_error(P, "EOF while parsing an object");
        u8 c = *P->p++;
        if (c == '}') return JZ_OK;
        if (c != ',') return jp_error(P, "expected `,` or `}`");
        jp_ws(P);
        if (P->p < P->end && *P->p == '}') return jp_error(P, "trailing comma");
    }
}

static jz_err jp_value(jp *P, jz_json **out) {
    jp_ws(P);
    if (P->p >= P->end) return jp_error(P, "EOF while parsing a value");
    u8 c = *P->p;
    switch (c) {
    case 'n': P->p++; JZ_TRY(jp_ident(P, "ull")); *out = jz_json_null(); return JZ_OK;
    case 't': P->p++; JZ_TRY(jp_ident(P, "rue")); *out = jz_json_bool(true); return JZ_OK;
    case 'f': P->p++; JZ_TRY(jp_ident(P, "alse")); *out = jz_json_bool(false); return JZ_OK;
    case '-': case '0': case '1': case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
        return jp_number(P, out);
    case '"':
        P->p++;
        JZ_TRY(jp_string(P));
        *out = jz_json_str_owned(jz_str_clone(&P->scratch));
        return JZ_OK;
    case '[':
    case '{': {
        if (++P->depth > RECURSION_LIMIT) return jp_error(P, "recursion limit exceeded");
        P->p++;
        jz_json *v = json_new(c == '[' ? JZ_JSON_ARR : JZ_JSON_OBJ);
        jz_err e = c == '[' ? jp_array(P, v) : jp_object(P, v);
        P->depth--;
        if (e.code) { jz_json_free(v); return e; }
        *out = v;
        return JZ_OK;
    }
    default:
        return jp_error(P, "expected value");
    }
}

jz_err jz_json_parse(jz_slice text, jz_json **out) {
    jp P = {text.ptr, text.ptr, text.ptr + text.len, NULL, 0, {0}};
    jz_json *v = NULL;
    jz_err e = jp_value(&P, &v);
    if (!e.code) {
        jp_ws(&P);
        if (P.p != P.end) {
            e = jp_error(&P, "trailing characters");
            jz_json_free(v);
            v = NULL;
        }
    }
    jz_str_drop(&P.scratch);
    *out = v;
    return e;
}

/* ---------- serializer ---------- */
static const char DIGITS_LUT[200] =
    "0001020304050607080910111213141516171819"
    "2021222324252627282930313233343536373839"
    "4041424344454647484950515253545556575859"
    "6061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

/* itoa: two digits per step from the end of a 20-byte buffer */
static void write_u64(jz_str *out, u64 v) {
    char buf[20];
    int i = 20;
    while (v >= 10000) {
        u32 rem = (u32)(v % 10000);
        v /= 10000;
        u32 d1 = (rem / 100) * 2, d2 = (rem % 100) * 2;
        i -= 4;
        buf[i] = DIGITS_LUT[d1]; buf[i + 1] = DIGITS_LUT[d1 + 1];
        buf[i + 2] = DIGITS_LUT[d2]; buf[i + 3] = DIGITS_LUT[d2 + 1];
    }
    u32 n = (u32)v;
    if (n >= 100) {
        u32 d = (n % 100) * 2;
        n /= 100;
        i -= 2;
        buf[i] = DIGITS_LUT[d]; buf[i + 1] = DIGITS_LUT[d + 1];
    }
    if (n < 10) {
        buf[--i] = (char)('0' + n);
    } else {
        u32 d = n * 2;
        i -= 2;
        buf[i] = DIGITS_LUT[d]; buf[i + 1] = DIGITS_LUT[d + 1];
    }
    jz_str_push(out, buf + i, (u32)(20 - i));
}

static void write_i64(jz_str *out, i64 v) {
    if (v < 0) {
        jz_str_push_char(out, '-');
        write_u64(out, (u64)0 - (u64)v);
    } else {
        write_u64(out, (u64)v);
    }
}

/* Shortest round-trip digits (what ryu computes), found by increasing %.e precision until
 * strtod gives back the same double, then laid out the way ryu's format_finite does. */
static void write_f64(jz_str *out, f64 v) {
    if (v != v || v - v != 0) {
        jz_str_push(out, "null", 4);
        return;
    }
    if (v == 0) {
        if (__builtin_signbit(v)) jz_str_push(out, "-0.0", 4);
        else jz_str_push(out, "0.0", 3);
        return;
    }
    char buf[40];
    int prec;
    for (prec = 1; prec <= 17; prec++) {
        snprintf(buf, sizeof(buf), "%.*e", prec - 1, v);
        if (strtod(buf, NULL) == v) break;
    }
    /* buf = [-]d[.ddd]e[+-]xx -> digits + decimal exponent */
    const char *s = buf;
    bool neg = false;
    if (*s == '-') { neg = true; s++; }
    char digits[20];
    int nd = 0;
    for (; *s && *s != 'e'; s++)
        if (*s != '.') digits[nd++] = *s;
    while (nd > 1 && digits[nd - 1] == '0') nd--;
    int e10 = atoi(s + 1);
    int kk = e10 + 1; /* value = 0.DIGITS * 10^kk */
    if (neg) jz_str_push_char(out, '-');
    if (nd <= kk && kk <= 16) {
        jz_str_push(out, digits, (u32)nd);
        for (int i = nd; i < kk; i++) jz_str_push_char(out, '0');
        jz_str_push(out, ".0", 2);
    } else if (0 < kk && kk <= 16) {
        jz_str_push(out, digits, (u32)kk);
        jz_str_push_char(out, '.');
        jz_str_push(out, digits + kk, (u32)(nd - kk));
    } else if (-5 < kk && kk <= 0) {
        jz_str_push(out, "0.", 2);
        for (int i = kk; i < 0; i++) jz_str_push_char(out, '0');
        jz_str_push(out, digits, (u32)nd);
    } else {
        jz_str_push_char(out, digits[0]);
        if (nd > 1) {
            jz_str_push_char(out, '.');
            jz_str_push(out, digits + 1, (u32)(nd - 1));
        }
        jz_str_push_char(out, 'e');
        write_i64(out, kk - 1);
    }
}

static void write_escaped(jz_str *out, const char *p, u32 n) {
    static const char HEX[16] = "0123456789abcdef";
    jz_str_push_char(out, '"');
    u32 start = 0;
    for (u32 i = 0; i < n; i++) {
        u8 esc = ESCAPE[(u8)p[i]];
        if (!esc) continue;
        if (start < i) jz_str_push(out, p + start, i - start);
        start = i + 1;
        switch (esc) {
        case UU: {
            char u[6] = {'\\', 'u', '0', '0', HEX[(u8)p[i] >> 4], HEX[(u8)p[i] & 15]};
            jz_str_push(out, u, 6);
            break;
        }
        default: {
            char e[2] = {'\\', (char)esc};
            jz_str_push(out, e, 2);
            break;
        }
        }
    }
    if (start < n) jz_str_push(out, p + start, n - start);
    jz_str_push_char(out, '"');
}

static void indent(jz_str *out, u32 level) {
    for (u32 i = 0; i < level; i++) jz_str_push(out, "  ", 2);
}

static void write_value(const jz_json *v, jz_str *out, bool pretty, u32 level) {
    switch (v->kind) {
    case JZ_JSON_NULL: jz_str_push(out, "null", 4); break;
    case JZ_JSON_BOOL: if (v->v.b) jz_str_push(out, "true", 4); else jz_str_push(out, "false", 5); break;
    case JZ_JSON_U64: write_u64(out, v->v.u); break;
    case JZ_JSON_I64: write_i64(out, v->v.i); break;
    case JZ_JSON_F64: write_f64(out, v->v.f); break;
    case JZ_JSON_STR: write_escaped(out, v->v.s.ptr, v->v.s.len); break;
    case JZ_JSON_ARR:
        jz_str_push_char(out, '[');
        for (u32 i = 0; i < v->v.arr.len; i++) {
            if (i) jz_str_push_char(out, ',');
            if (pretty) { jz_str_push_char(out, '\n'); indent(out, level + 1); }
            write_value(v->v.arr.ptr[i], out, pretty, level + 1);
        }
        if (pretty && v->v.arr.len) { jz_str_push_char(out, '\n'); indent(out, level); }
        jz_str_push_char(out, ']');
        break;
    case JZ_JSON_OBJ: {
        jz_str_push_char(out, '{');
        jz_str *k;
        jz_json **val;
        bool first = true;
        JZ_BTREE_FOR(&v->v.obj, k, val) {
            if (!first) jz_str_push_char(out, ',');
            first = false;
            if (pretty) { jz_str_push_char(out, '\n'); indent(out, level + 1); }
            write_escaped(out, k->ptr, k->len);
            if (pretty) jz_str_push(out, ": ", 2);
            else jz_str_push_char(out, ':');
            write_value(*val, out, pretty, level + 1);
        }
        if (pretty && !first) { jz_str_push_char(out, '\n'); indent(out, level); }
        jz_str_push_char(out, '}');
        break;
    }
    }
}

void jz_json_write(const jz_json *v, jz_str *out) { write_value(v, out, false, 0); }
void jz_json_write_pretty(const jz_json *v, jz_str *out) { write_value(v, out, true, 0); }

/* ---------- streaming writer ---------- */
static void jw_pre_value(jz_json_w *w) {
    if (w->need_comma[w->depth]) jz_str_push_char(w->out, ',');
    w->need_comma[w->depth] = true;
}

static void jw_open(jz_json_w *w, char c) {
    jw_pre_value(w);
    jz_str_push_char(w->out, c);
    if (w->depth + 1 >= 64) jz_panic("jz_json_w: nesting too deep");
    w->depth++;
    w->need_comma[w->depth] = false;
}

static void jw_close(jz_json_w *w, char c) {
    if (w->depth == 0) jz_panic("jz_json_w: unbalanced close");
    w->depth--;
    jz_str_push_char(w->out, c);
}

void jz_jw_begin_obj(jz_json_w *w) { jw_open(w, '{'); }
void jz_jw_end_obj(jz_json_w *w) { jw_close(w, '}'); }
void jz_jw_begin_arr(jz_json_w *w) { jw_open(w, '['); }
void jz_jw_end_arr(jz_json_w *w) { jw_close(w, ']'); }

void jz_jw_key(jz_json_w *w, const char *k) {
    if (w->need_comma[w->depth]) jz_str_push_char(w->out, ',');
    write_escaped(w->out, k, (u32)strlen(k));
    jz_str_push_char(w->out, ':');
    w->need_comma[w->depth] = false; /* the value that follows must not emit a comma */
}

void jz_jw_str(jz_json_w *w, const char *p, u32 n) { jw_pre_value(w); write_escaped(w->out, p, n); }
void jz_jw_u64(jz_json_w *w, u64 v) { jw_pre_value(w); write_u64(w->out, v); }
void jz_jw_i64(jz_json_w *w, i64 v) { jw_pre_value(w); write_i64(w->out, v); }
void jz_jw_f64(jz_json_w *w, f64 v) { jw_pre_value(w); write_f64(w->out, v); }
void jz_jw_bool(jz_json_w *w, bool v) { jw_pre_value(w); if (v) jz_str_push(w->out, "true", 4); else jz_str_push(w->out, "false", 5); }
void jz_jw_null(jz_json_w *w) { jw_pre_value(w); jz_str_push(w->out, "null", 4); }

/* ---------- Deserialize helpers (serde's de::Error texts) ---------- */
static void unexpected(jz_str *s, const jz_json *v) {
    if (!v) { jz_str_push_cstr(s, "unit"); return; }
    switch (v->kind) {
    case JZ_JSON_NULL: jz_str_push_cstr(s, "null"); break;
    case JZ_JSON_BOOL: jz_str_appendf(s, "boolean `%s`", v->v.b ? "true" : "false"); break;
    case JZ_JSON_U64: jz_str_push_cstr(s, "integer `"); write_u64(s, v->v.u); jz_str_push_char(s, '`'); break;
    case JZ_JSON_I64: jz_str_push_cstr(s, "integer `"); write_i64(s, v->v.i); jz_str_push_char(s, '`'); break;
    case JZ_JSON_F64: jz_str_push_cstr(s, "floating point `"); write_f64(s, v->v.f); jz_str_push_char(s, '`'); break;
    case JZ_JSON_STR: jz_str_appendf(s, "string \"%.*s\"", (int)v->v.s.len, v->v.s.ptr); break;
    case JZ_JSON_ARR: jz_str_push_cstr(s, "sequence"); break;
    case JZ_JSON_OBJ: jz_str_push_cstr(s, "map"); break;
    }
}

static jz_err invalid_type(const jz_json *v, const char *expected) {
    jz_str s = {0};
    jz_str_push_cstr(&s, "invalid type: ");
    unexpected(&s, v);
    jz_str_appendf(&s, ", expected %s", expected);
    jz_err e = jz_err_fmt(JZ_JSON_ERR_TYPE, "%.*s", (int)s.len, s.ptr);
    jz_str_drop(&s);
    return e;
}

jz_err jz_json_expect_obj(const jz_json *v, const char *what) {
    if (v && v->kind == JZ_JSON_OBJ) return JZ_OK;
    return invalid_type(v, what);
}

static jz_err get_field(const jz_json *obj, const char *key, const jz_json **out) {
    JZ_TRY(jz_json_expect_obj(obj, "struct"));
    const jz_json *f = jz_json_obj_get(obj, key);
    if (!f) return jz_err_fmt(JZ_JSON_ERR_MISSING_FIELD, "missing field `%s`", key);
    *out = f;
    return JZ_OK;
}

jz_err jz_json_field_str(const jz_json *obj, const char *key, jz_str *out) {
    const jz_json *f;
    JZ_TRY(get_field(obj, key, &f));
    if (f->kind != JZ_JSON_STR) return invalid_type(f, "a string");
    *out = jz_str_clone(&f->v.s);
    return JZ_OK;
}

jz_err jz_json_field_u64(const jz_json *obj, const char *key, u64 *out) {
    const jz_json *f;
    JZ_TRY(get_field(obj, key, &f));
    if (f->kind == JZ_JSON_U64) { *out = f->v.u; return JZ_OK; }
    if (f->kind == JZ_JSON_I64) {
        jz_str s = {0};
        jz_str_push_cstr(&s, "invalid value: ");
        unexpected(&s, f);
        jz_err e = jz_err_fmt(JZ_JSON_ERR_TYPE, "%.*s, expected u64", (int)s.len, s.ptr);
        jz_str_drop(&s);
        return e;
    }
    return invalid_type(f, "u64");
}

jz_err jz_json_field_bool(const jz_json *obj, const char *key, bool *out) {
    const jz_json *f;
    JZ_TRY(get_field(obj, key, &f));
    if (f->kind != JZ_JSON_BOOL) return invalid_type(f, "a boolean");
    *out = f->v.b;
    return JZ_OK;
}

static jz_err unknown_variant(const jz_str *name, const char *const *names, u32 n) {
    jz_str s = {0};
    jz_str_appendf(&s, "unknown variant `%.*s`, ", (int)name->len, name->ptr);
    if (n == 0) {
        jz_str_push_cstr(&s, "there are no variants");
    } else if (n == 1) {
        jz_str_appendf(&s, "expected `%s`", names[0]);
    } else {
        jz_str_push_cstr(&s, "expected one of ");
        for (u32 i = 0; i < n; i++) jz_str_appendf(&s, i ? ", `%s`" : "`%s`", names[i]);
    }
    jz_err e = jz_err_fmt(JZ_JSON_ERR_UNKNOWN_VARIANT, "%.*s", (int)s.len, s.ptr);
    jz_str_drop(&s);
    return e;
}

static jz_err find_variant(const jz_str *name, const char *const *names, u32 n, u32 *idx) {
    for (u32 i = 0; i < n; i++) {
        if (jz_str_eq_cstr(name, names[i])) {
            *idx = i;
            return JZ_OK;
        }
    }
    return unknown_variant(name, names, n);
}

jz_err jz_json_variant(const jz_json *v, const char *const *names, u32 n, u32 *idx, const jz_json **payload) {
    if (!v) return invalid_type(v, "string or map");
    if (v->kind == JZ_JSON_STR) {
        JZ_TRY(find_variant(&v->v.s, names, n, idx));
        *payload = NULL;
        return JZ_OK;
    }
    if (v->kind == JZ_JSON_OBJ) {
        if (v->v.obj.len != 1)
            return jz_err_new(JZ_JSON_ERR_TYPE, "invalid value: map, expected map with a single key");
        jz_str *k;
        jz_json **val;
        jz_btree_first(&v->v.obj, (void **)&k, (void **)&val);
        JZ_TRY(find_variant(k, names, n, idx));
        *payload = *val;
        return JZ_OK;
    }
    return invalid_type(v, "string or map");
}
