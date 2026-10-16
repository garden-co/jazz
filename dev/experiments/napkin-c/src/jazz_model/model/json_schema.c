/* model/json_schema.rs: JSON Schema validation for JSON columns.
 * The browser build validates through a host-installed validator (the native
 * `jsonschema` path is cfg(not(wasm32)) and is not ported). */
#include "jazz_model/jazz_model.h"

#define JS(x) jazz_model__model__json_schema__##x

/* MAX_COMPILED bounds the per-thread cache of compiled validators. */
#define MAX_COMPILED 256

/* enum Compiled { Host(Box<dyn HostCompiledJsonSchema>) } behind an Rc */
typedef struct jazz_model_JsonSchemaCompiled { jazz_model_HostCompiledJsonSchema host; } jazz_model_JsonSchemaCompiled;

static bool HOST_VALIDATOR_SET;
static jazz_model_HostJsonSchemaValidator HOST_VALIDATOR;
static jz_hmap COMPILED;
static bool COMPILED_INIT;

static u64 key_hash(const void *k) { return jz_str_hash((const jz_str *)k); }
static bool key_eq(const void *a, const void *b) { return jz_str_eq((const jz_str *)a, (const jz_str *)b); }
static void compiled_rc_release_v(void *v) { jz_rc_release(*(void **)v); }
static const jz_hmap_desc COMPILED_DESC = {
    sizeof(jz_str), sizeof(jazz_model_JsonSchemaCompiled *), key_hash, key_eq, jazz_model__str_drop_v, compiled_rc_release_v,
};

static jz_hmap *compiled_cache(void) {
    if (!COMPILED_INIT) {
        jz_hmap_init(&COMPILED, &COMPILED_DESC);
        COMPILED_INIT = true;
    }
    return &COMPILED;
}

static void compiled_drop(void *p) {
    jazz_model_JsonSchemaCompiled *c = p;
    if (c->host.drop) c->host.drop(c->host.self);
}

void JS(install_host_validator)(jazz_model_HostJsonSchemaValidator validator) {
    if (HOST_VALIDATOR_SET && HOST_VALIDATOR.drop) HOST_VALIDATOR.drop(HOST_VALIDATOR.self);
    HOST_VALIDATOR = validator;
    HOST_VALIDATOR_SET = true;
    jz_hmap_clear(compiled_cache());
}

static jz_err jazz_model__model__json_schema__compile(const jz_json *schema, jz_slice schema_json, jazz_model_JsonSchemaCompiled **out) {
    (void)schema;
    if (!HOST_VALIDATOR_SET)
        return jz_err_new(JAZZ_MODEL_ERR_JSON_SCHEMA_UNAVAILABLE, "no JSON Schema validator is installed in this WASM module");
    jazz_model_HostCompiledJsonSchema host;
    jz_err e = HOST_VALIDATOR.compile(HOST_VALIDATOR.self, schema_json, &host);
    if (e.code) {
        jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_JSON_SCHEMA_INVALID, "%.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return r;
    }
    jazz_model_JsonSchemaCompiled *c = jz_rc_new(sizeof *c, compiled_drop);
    c->host = host;
    *out = c;
    return JZ_OK;
}

/* returns a new Rc reference */
static jz_err jazz_model__model__json_schema__compiled(const jz_json *schema, jazz_model_JsonSchemaCompiled **out) {
    jz_str key = {0};
    jz_json_write(schema, &key);
    jz_hmap *cache = compiled_cache();
    jazz_model_JsonSchemaCompiled **hit = jz_hmap_get(cache, &key);
    if (hit) {
        *out = jz_rc_clone(*hit);
        jz_str_drop(&key);
        return JZ_OK;
    }
    jazz_model_JsonSchemaCompiled *c;
    jz_err e = jazz_model__model__json_schema__compile(schema, jz_str_slice(&key), &c);
    if (e.code) { jz_str_drop(&key); return e; }
    if (cache->len >= MAX_COMPILED) jz_hmap_clear(cache);
    jazz_model_JsonSchemaCompiled *cached = jz_rc_clone(c);
    jz_hmap_insert(cache, &key, &cached, NULL);
    *out = c;
    return JZ_OK;
}

jz_err JS(check_schema)(const jz_json *schema) {
    jazz_model_JsonSchemaCompiled *c;
    JZ_TRY(jazz_model__model__json_schema__compiled(schema, &c));
    jz_rc_release(c);
    return JZ_OK;
}

jz_err JS(validate)(const jz_json *schema, const jz_json *instance) {
    jazz_model_JsonSchemaCompiled *c;
    JZ_TRY(jazz_model__model__json_schema__compiled(schema, &c));
    jz_str text = {0};
    jz_json_write(instance, &text);
    bool mismatch = false;
    jz_str reason = {0};
    jz_err e = c->host.validate(c->host.self, jz_str_slice(&text), &mismatch, &reason);
    jz_str_drop(&text);
    jz_rc_release(c);
    if (e.code) {
        jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_JSON_SCHEMA_UNAVAILABLE, "the JSON Schema validator failed: %.*s",
                              e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return r;
    }
    if (mismatch) {
        jz_err r = jz_err_fmt(JAZZ_MODEL_ERR_JSON_SCHEMA_MISMATCH, "%.*s", (int)reason.len, reason.ptr);
        jz_str_drop(&reason);
        return r;
    }
    return JZ_OK;
}
