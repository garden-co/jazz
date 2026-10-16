/* groove::large_values::json_syntax (crates/groove/src/large_values/json_syntax.rs) */
#include "jz_rt.h"
#include "json_syntax.h"

#define GROOVE_MAX_JSON_NESTING_DEPTH 128u
/* The default serde_json::Value parser starts with a recursion budget of 128
 * and rejects the container that decrements it to zero. */
#define SERDE_JSON_MAX_CONTAINERS 127u

typedef groove_StreamingJsonValidator V;

static bool consume_idle(V *s, u8 byte);
static bool finish_number(V *s);
static bool value_complete(V *s);

void groove__large_values__json_syntax__StreamingJsonValidator__new(V *out) {
    memset(out, 0, sizeof *out);
    out->token.tag = GROOVE_JS_TOK_NONE;
    out->root_done = false;
}

void groove__large_values__json_syntax__StreamingJsonValidator__drop(V *v) { jz_vec_drop(&v->stack, sizeof(groove_JsFrame), NULL); }

static groove_JsFrame *last_frame(V *s) { return s->stack.len ? &s->stack.ptr[s->stack.len - 1] : NULL; }

/* advance_number */
static bool advance_number(u8 *state, u8 byte, bool *advanced) {
    bool digit = byte >= '0' && byte <= '9';
    u8 st = *state, next;
    if (st == GROOVE_NS_MINUS && byte == '0') next = GROOVE_NS_ZERO;
    else if (st == GROOVE_NS_MINUS && byte >= '1' && byte <= '9') next = GROOVE_NS_INTEGER;
    else if ((st == GROOVE_NS_ZERO || st == GROOVE_NS_INTEGER) && byte == '.') next = GROOVE_NS_DOT;
    else if (st == GROOVE_NS_INTEGER && digit) next = GROOVE_NS_INTEGER;
    else if ((st == GROOVE_NS_DOT || st == GROOVE_NS_FRACTION) && digit) next = GROOVE_NS_FRACTION;
    else if ((st == GROOVE_NS_ZERO || st == GROOVE_NS_INTEGER || st == GROOVE_NS_FRACTION) && (byte == 'e' || byte == 'E')) next = GROOVE_NS_EXPONENT;
    else if (st == GROOVE_NS_EXPONENT && (byte == '+' || byte == '-')) next = GROOVE_NS_EXPONENT_SIGN;
    else if ((st == GROOVE_NS_EXPONENT || st == GROOVE_NS_EXPONENT_SIGN || st == GROOVE_NS_EXPONENT_DIGITS) && digit) next = GROOVE_NS_EXPONENT_DIGITS;
    else if (st == GROOVE_NS_ZERO || st == GROOVE_NS_INTEGER || st == GROOVE_NS_FRACTION || st == GROOVE_NS_EXPONENT_DIGITS) { *advanced = false; return true; }
    else return false;
    *state = next;
    *advanced = true;
    return true;
}

static void number_new(groove_JsNumber *n, u8 byte) {
    memset(n, 0, sizeof *n);
    n->state = byte == '-' ? GROOVE_NS_MINUS : byte == '0' ? GROOVE_NS_ZERO : GROOVE_NS_INTEGER;
    n->significand = byte == '-' ? 0 : (u64)(byte - '0');
}

static bool append_digit(const groove_JsNumber *n, u8 byte, u64 *out) {
    u64 m;
    if (__builtin_mul_overflow(n->significand, (u64)10, &m)) return false;
    return !__builtin_add_overflow(m, (u64)(byte - '0'), out);
}

/* Number::advance -> Result<bool, ()> */
static bool number_advance(groove_JsNumber *n, u8 byte, bool *advanced) {
    if (!advance_number(&n->state, byte, advanced)) return false;
    if (!*advanced) return true;
    u64 next;
    switch (n->state) {
    case GROOVE_NS_ZERO:
    case GROOVE_NS_INTEGER:
        if (!n->integer_overflow) {
            if (append_digit(n, byte, &next)) n->significand = next;
            else n->integer_overflow = true;
        }
        if (n->integer_overflow) {
            /* Reject before serde_json's unchecked long-mantissa counter would overflow. */
            if (__builtin_add_overflow(n->decimal_exponent, 1, &n->decimal_exponent)) return false;
        }
        break;
    case GROOVE_NS_FRACTION:
        if (!n->fraction_overflow) {
            if (append_digit(n, byte, &next)) {
                n->significand = next;
                if (__builtin_sub_overflow(n->decimal_exponent, 1, &n->decimal_exponent)) return false;
            } else {
                n->fraction_overflow = true;
            }
        }
        break;
    case GROOVE_NS_EXPONENT_SIGN: n->exponent_negative = byte == '-'; break;
    case GROOVE_NS_EXPONENT_DIGITS:
        if (!n->exponent_overflow) {
            i32 m, a;
            if (!__builtin_mul_overflow(n->exponent, 10, &m) && !__builtin_add_overflow(m, (i32)(byte - '0'), &a))
                n->exponent = a;
            else
                n->exponent_overflow = true;
        }
        break;
    default: break;
    }
    return true;
}

static i32 sat_add_i32(i32 a, i32 b) { i32 r; if (__builtin_add_overflow(a, b, &r)) return b > 0 ? INT32_MAX : INT32_MIN; return r; }
static i32 sat_sub_i32(i32 a, i32 b) { i32 r; if (__builtin_sub_overflow(a, b, &r)) return b < 0 ? INT32_MAX : INT32_MIN; return r; }

static bool number_finish(const groove_JsNumber *n) {
    if (!(n->state == GROOVE_NS_ZERO || n->state == GROOVE_NS_INTEGER || n->state == GROOVE_NS_FRACTION ||
          n->state == GROOVE_NS_EXPONENT_DIGITS))
        return false;
    if (n->exponent_overflow) {
        /* serde_json handles exponent overflow before combining it with the mantissa's decimal places. */
        return n->exponent_negative || n->significand == 0;
    }
    i32 exponent = n->exponent_negative ? sat_sub_i32(n->decimal_exponent, n->exponent)
                                        : sat_add_i32(n->decimal_exponent, n->exponent);
    if (exponent <= 0 || n->significand == 0) return true; /* always finite */
    /* At most 20 significand digits, 'e', and 10 positive exponent digits. */
    jz_str bytes = jz_format("%llue%d", (unsigned long long)n->significand, (int)exponent);
    if (bytes.len > 31) { jz_str_drop(&bytes); return false; }
    jz_str_push_char(&bytes, 0);
    char *end = NULL;
    f64 v = strtod(bytes.ptr, &end);
    bool parsed = end == bytes.ptr + bytes.len - 1;
    jz_str_drop(&bytes);
    if (!parsed) return false;
    /* serde_json rejects non-finite numbers ("number out of range") */
    return v <= 1.7976931348623157e308;
}

static bool start_literal(V *s, const char *expected) {
    s->token.tag = GROOVE_JS_TOK_LITERAL;
    s->token.expected = (const u8 *)expected;
    s->token.expected_len = (u32)strlen(expected);
    s->token.offset = 0;
    return true;
}

static void start_string(V *s, bool key) {
    s->token.tag = GROOVE_JS_TOK_STRING;
    s->token.key = key;
    s->token.escaped = false;
    s->token.unicode_digits = 0;
    s->token.unicode_value = 0;
    s->token.high_surrogate = false;
}

static bool start_value(V *s, u8 byte) {
    u32 limit = GROOVE_MAX_JSON_NESTING_DEPTH < SERDE_JSON_MAX_CONTAINERS ? GROOVE_MAX_JSON_NESTING_DEPTH : SERDE_JSON_MAX_CONTAINERS;
    switch (byte) {
    case '{':
        if (s->stack.len >= limit) return false;
        JZ_VEC_PUSH(s->stack, ((groove_JsFrame){GROOVE_JS_FRAME_OBJECT, GROOVE_JS_OBJ_FIRST_KEY_OR_END}));
        return true;
    case '[':
        if (s->stack.len >= limit) return false;
        JZ_VEC_PUSH(s->stack, ((groove_JsFrame){GROOVE_JS_FRAME_ARRAY, GROOVE_JS_ARR_FIRST_VALUE_OR_END}));
        return true;
    case '"': start_string(s, false); return true;
    case 't': return start_literal(s, "rue");
    case 'f': return start_literal(s, "alse");
    case 'n': return start_literal(s, "ull");
    default:
        if (byte == '-' || (byte >= '0' && byte <= '9')) {
            s->token.tag = GROOVE_JS_TOK_NUMBER;
            number_new(&s->token.number, byte);
            return true;
        }
        return false;
    }
}

static bool value_complete(V *s) {
    groove_JsFrame *f = last_frame(s);
    if (f && f->kind == GROOVE_JS_FRAME_ARRAY && (f->state == GROOVE_JS_ARR_FIRST_VALUE_OR_END || f->state == GROOVE_JS_ARR_VALUE))
        f->state = GROOVE_JS_ARR_COMMA_OR_END;
    else if (f && f->kind == GROOVE_JS_FRAME_OBJECT && f->state == GROOVE_JS_OBJ_VALUE)
        f->state = GROOVE_JS_OBJ_COMMA_OR_END;
    else if (!f && !s->root_done)
        s->root_done = true;
    else
        return false;
    return true;
}

static bool close_kind(V *s, u8 kind) {
    if (!s->stack.len) return false;
    groove_JsFrame f = s->stack.ptr[--s->stack.len];
    if (f.kind != kind) return false;
    return value_complete(s);
}
static bool close_array(V *s) { return close_kind(s, GROOVE_JS_FRAME_ARRAY); }
static bool close_object(V *s) { return close_kind(s, GROOVE_JS_FRAME_OBJECT); }

static bool set_state(V *s, u8 kind, u8 next) {
    groove_JsFrame *f = last_frame(s);
    if (!f || f->kind != kind) return false;
    f->state = next;
    return true;
}

static bool finish_number(V *s) {
    if (s->token.tag != GROOVE_JS_TOK_NUMBER) return false;
    if (!number_finish(&s->token.number)) return false;
    s->token.tag = GROOVE_JS_TOK_NONE;
    return value_complete(s);
}

static bool consume_idle(V *s, u8 byte) {
    if (byte == ' ' || byte == '\t' || byte == '\n' || byte == '\r') return true;
    groove_JsFrame *f = last_frame(s);
    if (f && f->kind == GROOVE_JS_FRAME_ARRAY && f->state == GROOVE_JS_ARR_COMMA_OR_END) {
        if (byte == ',') return set_state(s, GROOVE_JS_FRAME_ARRAY, GROOVE_JS_ARR_VALUE);
        if (byte == ']') return close_array(s);
        return false;
    }
    if (f && f->kind == GROOVE_JS_FRAME_OBJECT && f->state == GROOVE_JS_OBJ_COMMA_OR_END) {
        if (byte == ',') return set_state(s, GROOVE_JS_FRAME_OBJECT, GROOVE_JS_OBJ_KEY);
        if (byte == '}') return close_object(s);
        return false;
    }
    if (f && f->kind == GROOVE_JS_FRAME_OBJECT && f->state == GROOVE_JS_OBJ_COLON) {
        if (byte != ':') return false;
        return set_state(s, GROOVE_JS_FRAME_OBJECT, GROOVE_JS_OBJ_VALUE);
    }
    if (f && f->kind == GROOVE_JS_FRAME_OBJECT && (f->state == GROOVE_JS_OBJ_FIRST_KEY_OR_END || f->state == GROOVE_JS_OBJ_KEY)) {
        if (byte == '}' && f->state == GROOVE_JS_OBJ_FIRST_KEY_OR_END) return close_object(s);
        if (byte == '"') { start_string(s, true); return true; }
        return false;
    }
    if (f && f->kind == GROOVE_JS_FRAME_ARRAY && f->state == GROOVE_JS_ARR_FIRST_VALUE_OR_END && byte == ']')
        return close_array(s);
    bool value_slot = (f && f->kind == GROOVE_JS_FRAME_ARRAY && (f->state == GROOVE_JS_ARR_FIRST_VALUE_OR_END || f->state == GROOVE_JS_ARR_VALUE)) ||
                      (f && f->kind == GROOVE_JS_FRAME_OBJECT && f->state == GROOVE_JS_OBJ_VALUE) || !f;
    if (value_slot && !s->root_done) return start_value(s, byte);
    return false;
}

static bool consume(V *s, u8 byte) {
    groove_JsToken *t = &s->token;
    switch (t->tag) {
    case GROOVE_JS_TOK_STRING: {
        if (t->unicode_digits > 0) {
            u8 digit;
            if (byte >= '0' && byte <= '9') digit = byte - '0';
            else if (byte >= 'a' && byte <= 'f') digit = byte - 'a' + 10;
            else if (byte >= 'A' && byte <= 'F') digit = byte - 'A' + 10;
            else return false;
            t->unicode_value = (u16)((t->unicode_value << 4) | digit);
            t->unicode_digits -= 1;
            if (t->unicode_digits == 0) {
                bool low = t->unicode_value >= 0xdc00 && t->unicode_value <= 0xdfff;
                if (t->high_surrogate) {
                    if (!low) return false;
                    t->high_surrogate = false;
                } else {
                    if (low) return false;
                    t->high_surrogate = t->unicode_value >= 0xd800 && t->unicode_value <= 0xdbff;
                }
            }
            return true;
        }
        if (t->escaped) {
            t->escaped = false;
            if (byte == 'u') {
                t->unicode_digits = 4;
                t->unicode_value = 0;
            } else if (t->high_surrogate || !(byte == '"' || byte == '\\' || byte == '/' || byte == 'b' ||
                                              byte == 'f' || byte == 'n' || byte == 'r' || byte == 't')) {
                return false;
            }
            return true;
        }
        if (t->high_surrogate && byte != '\\') return false;
        if (byte == '\\') {
            t->escaped = true;
        } else if (byte == '"') {
            bool key = t->key;
            t->tag = GROOVE_JS_TOK_NONE;
            if (key) {
                groove_JsFrame *f = last_frame(s);
                if (f && f->kind == GROOVE_JS_FRAME_OBJECT &&
                    (f->state == GROOVE_JS_OBJ_FIRST_KEY_OR_END || f->state == GROOVE_JS_OBJ_KEY))
                    f->state = GROOVE_JS_OBJ_COLON;
                else
                    return false;
            } else {
                if (!value_complete(s)) return false;
            }
        } else if (byte <= 0x1f) {
            return false;
        }
        return true;
    }
    case GROOVE_JS_TOK_LITERAL:
        if (t->offset >= t->expected_len || t->expected[t->offset] != byte) return false;
        t->offset += 1;
        if (t->offset == t->expected_len) {
            t->tag = GROOVE_JS_TOK_NONE;
            if (!value_complete(s)) return false;
        }
        return true;
    case GROOVE_JS_TOK_NUMBER: {
        bool advanced;
        if (!number_advance(&t->number, byte, &advanced)) return false;
        if (advanced) return true;
        if (!finish_number(s)) return false;
        return consume_idle(s, byte);
    }
    default:
        return consume_idle(s, byte);
    }
}

bool groove__large_values__json_syntax__StreamingJsonValidator__push(V *v, const u8 *p, u32 n) {
    for (u32 i = 0; i < n; i++)
        if (!consume(v, p[i])) return false;
    return true;
}

bool groove__large_values__json_syntax__StreamingJsonValidator__finish(V *v) {
    bool ok = true;
    if (v->token.tag == GROOVE_JS_TOK_NUMBER && !finish_number(v)) ok = false;
    if (ok && (v->token.tag != GROOVE_JS_TOK_NONE || v->stack.len != 0 || !v->root_done)) ok = false;
    groove__large_values__json_syntax__StreamingJsonValidator__drop(v);
    return ok;
}
