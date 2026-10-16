/* groove::large_values::json_syntax (crates/groove/src/large_values/json_syntax.rs) */
#ifndef GROOVE_LARGE_VALUES_JSON_SYNTAX_H
#define GROOVE_LARGE_VALUES_JSON_SYNTAX_H
#include "jz_rt.h"

enum { GROOVE_JS_FRAME_ARRAY = 0, GROOVE_JS_FRAME_OBJECT = 1 };
enum { GROOVE_JS_ARR_FIRST_VALUE_OR_END, GROOVE_JS_ARR_VALUE, GROOVE_JS_ARR_COMMA_OR_END };
enum { GROOVE_JS_OBJ_FIRST_KEY_OR_END, GROOVE_JS_OBJ_KEY, GROOVE_JS_OBJ_COLON, GROOVE_JS_OBJ_VALUE, GROOVE_JS_OBJ_COMMA_OR_END };
typedef struct groove_JsFrame { u8 kind; u8 state; } groove_JsFrame;

enum {
    GROOVE_NS_MINUS, GROOVE_NS_ZERO, GROOVE_NS_INTEGER, GROOVE_NS_DOT, GROOVE_NS_FRACTION,
    GROOVE_NS_EXPONENT, GROOVE_NS_EXPONENT_SIGN, GROOVE_NS_EXPONENT_DIGITS
};
typedef struct groove_JsNumber {
    u8 state;
    u64 significand;
    i32 decimal_exponent;
    bool integer_overflow, fraction_overflow;
    i32 exponent;
    bool exponent_negative, exponent_overflow;
} groove_JsNumber;

enum { GROOVE_JS_TOK_NONE, GROOVE_JS_TOK_STRING, GROOVE_JS_TOK_LITERAL, GROOVE_JS_TOK_NUMBER };
typedef struct groove_JsToken {
    u8 tag;
    /* String */
    bool key, escaped, high_surrogate;
    u8 unicode_digits;
    u16 unicode_value;
    /* Literal */
    const u8 *expected; u32 expected_len; u32 offset;
    /* Number */
    groove_JsNumber number;
} groove_JsToken;

typedef struct groove_StreamingJsonValidator {
    JZ_VEC(groove_JsFrame) stack;
    groove_JsToken token;
    bool root_done;
} groove_StreamingJsonValidator;

void groove__large_values__json_syntax__StreamingJsonValidator__new(groove_StreamingJsonValidator *out);
/* Result<(), ()> -> bool ok */
bool groove__large_values__json_syntax__StreamingJsonValidator__push(groove_StreamingJsonValidator *v, const u8 *p, u32 n);
/* consumes self */
bool groove__large_values__json_syntax__StreamingJsonValidator__finish(groove_StreamingJsonValidator *v);
void groove__large_values__json_syntax__StreamingJsonValidator__drop(groove_StreamingJsonValidator *v);
#endif
