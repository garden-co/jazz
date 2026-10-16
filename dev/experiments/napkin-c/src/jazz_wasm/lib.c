/* crates/jazz-wasm/src/lib.rs: the wasm-bindgen surface of the Jazz browser
 * client (WasmDb, WasmTransport, WasmWrite, streaming mutations, pending
 * reads/subscriptions, tracing + identity helpers).
 *
 * ABI: each #[wasm_bindgen] export is a JZ_EXPORT with wasm-bindgen's name
 * (free fn: js_name; method: <class>_<js_name>; destructor: __wbg_<class>_free).
 * Owned String / Vec<u8> arguments arrive as (ptr, len) allocated with
 * __wbindgen_malloc and are owned by the callee; Option<Vec<u8>> / Option<String>
 * use ptr == 0 for None; scalar options are (is_some, value). Result returns
 * go through a retptr slot (wbg_ret_*).
 *
 * Errors: every Result<_, JsValue> is a jz_err. Core jazz errors keep their
 * ErrorCode-derived code (< JAZZ_NODE_ERROR_CODE_MAX) until to_js_error turns
 * them into a JS Error with a `code` property; JsValue errors ride as
 * WBG_ERR_JS; `JsValue::from_str` errors are WBG_ERR_STR.
 *
 * Async: future_to_promise bodies run as spawned local tasks that settle a
 * promise; inner `.await`s are synchronous in the port (see PORTING.md). */
#include <stdio.h>
#include <stdarg.h>
#include "jazz_wasm/jazz_wasm.h"

/* ================================================================== */
/* error plumbing                                                      */
/* ================================================================== */
static jz_err str_err(const char *msg) { return jz_err_new(WBG_ERR_STR, msg); }

static jz_err str_errf(const char *fmt, ...) {
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    return jz_err_new(WBG_ERR_STR, buf);
}

/* `.map_err(|error| JsValue::from_str(&error))` of a Result<_, String> */
static jz_err into_str_err(jz_err e) {
    if (e.code && e.code != WBG_ERR_JS) e.code = WBG_ERR_STR;
    return e;
}

/* error.to_string() of any Display error, as a bare JS string */
static jz_err display_str_err(jz_err e) { return into_str_err(e); }

static bool is_core_error(const jz_err *e) { return e->code > 0 && e->code < JAZZ_NODE_ERROR_CODE_MAX; }

/* core_error_to_js: a real JS Error whose `code` is ErrorCode::as_str */
static jz_js jazz_wasm__core_error_to_js(const jz_err *error) {
    jz_js js_error = error->msg ? wbg_error_new(error->msg->ptr, error->msg->len) : wbg_error_new("", 0);
    jz_js key = wbg_str("code");
    jz_js code = wbg_str(jazz_node__node__api_error__ErrorCode__as_str(error->code - 1));
    jz_js thrown = 0;
    /* Setting a data property on a fresh ordinary object cannot fail. */
    (void)wbg_reflect_set(js_error, key, code, &thrown);
    jz_js_drop(key);
    jz_js_drop(code);
    return js_error;
}

/* to_js_error: core Error -> JS Error with code; everything else keeps its
 * historical bare-string (or already-JS) form. Consumes e. */
static jz_js jazz_wasm__to_js_error(jz_err e) {
    if (is_core_error(&e)) {
        jz_js v = jazz_wasm__core_error_to_js(&e);
        jz_err_drop(&e);
        return v;
    }
    return wbg_err_into_js(e);
}

/* `.map_err(to_js_error)` kept inside a jz_err (so `?` can carry it on) */
static jz_err to_js_err(jz_err e) {
    if (!e.code) return e;
    return wbg_err_from_js(jazz_wasm__to_js_error(e));
}

static jz_js js_str(jz_slice s) { return wbg_str_slice(s); }

static void ret_unit(wbg_ret_val *ret, jz_err e) {
    if (e.code) {
        ret->val = 0;
        ret->err = jazz_wasm__to_js_error(e);
        ret->is_err = 1;
    } else {
        ret->val = WBG_UNDEFINED;
        ret->err = 0;
        ret->is_err = 0;
    }
}

static void ret_val(wbg_ret_val *ret, jz_err e, jz_js v) {
    if (e.code) {
        ret->val = 0;
        ret->err = jazz_wasm__to_js_error(e);
        ret->is_err = 1;
    } else {
        ret->val = v;
        ret->err = 0;
        ret->is_err = 0;
    }
}

static void ret_ptr(wbg_ret_ptr *ret, jz_err e, void *p) {
    if (e.code) {
        ret->ptr = 0;
        ret->err = jazz_wasm__to_js_error(e);
        ret->is_err = 1;
    } else {
        ret->ptr = (u32)(usize)p;
        ret->err = 0;
        ret->is_err = 0;
    }
}

static void ret_str(wbg_ret_str *ret, jz_err e, jz_str s) {
    if (e.code) wbg_ret_error_str(ret, jazz_wasm__to_js_error(e));
    else wbg_ret_string(ret, s);
}

static void ret_bytes(wbg_ret_str *ret, jz_err e, jz_bytes b) {
    if (e.code) {
        wbg_ret_error_str(ret, jazz_wasm__to_js_error(e));
        return;
    }
    ret->ptr = (u32)(usize)b.ptr;
    ret->len = b.len;
    ret->err = 0;
    ret->is_err = 0;
}

/* owned (ptr, len) arguments handed over by the JS glue */
static jz_str arg_str(char *p, u32 n) {
    jz_str s = {p, n, n};
    return s;
}
static jz_bytes arg_bytes(u8 *p, u32 n) {
    jz_bytes b = {p, n, n};
    return b;
}

/* ================================================================== */
/* future_to_promise / Promise helpers                                  */
/* ================================================================== */
typedef jz_err (*jazz_wasm_async_fn)(void *ctx, jz_js *out);
typedef struct jazz_wasm_PromiseTask {
    jazz_wasm_async_fn body;
    void *ctx;
    void (*ctx_drop)(void *);
    jz_js resolve;
    jz_js reject;
} jazz_wasm_PromiseTask;

static void promise_task_run(void *p) {
    jazz_wasm_PromiseTask *t = (jazz_wasm_PromiseTask *)p;
    jz_js value = WBG_UNDEFINED, ignored = 0;
    jz_err e = t->body(t->ctx, &value);
    if (e.code) {
        jz_js error = jazz_wasm__to_js_error(e);
        (void)wbg_call1(t->reject, WBG_UNDEFINED, error, &ignored);
        jz_js_drop(error);
    } else {
        (void)wbg_call1(t->resolve, WBG_UNDEFINED, value, &ignored);
        jz_js_drop(value);
    }
}

static void promise_task_drop(void *p) {
    jazz_wasm_PromiseTask *t = (jazz_wasm_PromiseTask *)p;
    if (t->ctx_drop) t->ctx_drop(t->ctx);
    jz_js_drop(t->resolve);
    jz_js_drop(t->reject);
    jz_free(t);
}

/* wasm_bindgen_futures::future_to_promise */
static jz_js jazz_wasm__future_to_promise(jazz_wasm_async_fn body, void *ctx, void (*ctx_drop)(void *)) {
    jazz_wasm_PromiseTask *t = JZ_NEW(jazz_wasm_PromiseTask);
    jz_js promise = wbg_promise_with_resolvers(&t->resolve, &t->reject);
    t->body = body;
    t->ctx = ctx;
    t->ctx_drop = ctx_drop;
    wbg_spawn_local(promise_task_run, t, promise_task_drop);
    return promise;
}

static jz_js promise_reject_err(jz_err e) {
    jz_js error = jazz_wasm__to_js_error(e);
    jz_js p = wbg_promise_reject(error);
    jz_js_drop(error);
    return p;
}

static jz_js bytes_to_js(jz_bytes *bytes) {
    jz_js v = wbg_uint8array_from(bytes->ptr, bytes->len);
    jz_bytes_drop(bytes);
    return v;
}

static jz_err reflect_get(jz_js obj, const char *name, jz_js *out) {
    jz_js key = wbg_str(name);
    jz_js v = 0;
    u32 threw = wbg_reflect_get(obj, key, &v);
    jz_js_drop(key);
    if (threw) return wbg_err_from_js(v);
    *out = v;
    return JZ_OK;
}

/* set_prop */
static jz_err jazz_wasm__set_prop(jz_js object, const char *name, jz_js value) {
    jz_js key = wbg_str(name);
    jz_js thrown = 0;
    u32 threw = wbg_reflect_set(object, key, value, &thrown);
    jz_js_drop(key);
    jz_js_drop(value);
    if (threw) return wbg_err_from_js(thrown);
    return JZ_OK;
}

/* .dyn_into::<js_sys::Function>() */
static jz_err dyn_into_function(jz_js v) {
    if (!wbg_is_function(v)) return wbg_err_from_js(v);
    return JZ_OK;
}

/* ================================================================== */
/* free functions                                                       */
/* ================================================================== */
static jz_err jazz_wasm__decode_public_schema(jz_slice schema, jazz_model_JazzSchema **out) {
    return to_js_err(jazz_model__model__public_schema_convert__decode_public_schema_json(schema, out));
}

/* validateSchema(schema: Vec<u8>) -> Result<(), JsValue> */
JZ_EXPORT("validateSchema") void jazz_wasm__validate_schema(wbg_ret_val *ret, u8 *schema_ptr, u32 schema_len) {
    jz_bytes schema = arg_bytes(schema_ptr, schema_len);
    jazz_model_JazzSchema *decoded = NULL;
    jz_err e = jazz_wasm__decode_public_schema(jz_bytes_slice(&schema), &decoded);
    if (!e.code) jazz_model__schema__JazzSchema__drop(decoded);
    jz_bytes_drop(&schema);
    ret_unit(ret, e);
}

/* #[wasm_bindgen(start)] init: console_error_panic_hook is a feature-gated
 * no-op in the shipped build */
JZ_EXPORT("__wbindgen_start") void jazz_wasm__init(void) {}

static bool WASM_TRACING_INITIALIZED = false;

static u32 jazz_wasm__configured_wasm_log_level(void) {
    jz_js global = wbg_global();
    jz_js key = wbg_str("__JAZZ_WASM_LOG_LEVEL");
    jz_js value = 0;
    u32 threw = wbg_reflect_get(global, key, &value);
    jz_js_drop(key);
    jz_js_drop(global);
    jz_str s = {0};
    bool some = !threw && wbg_take_string(value, &s);
    jz_js_drop(value);
    u32 level = WT_LEVEL_WARN;
    if (some) {
        for (u32 i = 0; i < s.len; i++)
            if (s.ptr[i] >= 'A' && s.ptr[i] <= 'Z') s.ptr[i] = (char)(s.ptr[i] + 32);
        if (jz_str_eq_cstr(&s, "error")) level = WT_LEVEL_ERROR;
        else if (jz_str_eq_cstr(&s, "info")) level = WT_LEVEL_INFO;
        else if (jz_str_eq_cstr(&s, "debug")) level = WT_LEVEL_DEBUG;
        else if (jz_str_eq_cstr(&s, "trace")) level = WT_LEVEL_TRACE;
        jz_str_drop(&s);
    }
    return level;
}

static void jazz_wasm__initialize_wasm_tracing(void) {
    bool was = WASM_TRACING_INITIALIZED; /* AtomicBool::swap(true, AcqRel) */
    WASM_TRACING_INITIALIZED = true;
    if (was) return;
    u32 max_level = jazz_wasm__configured_wasm_log_level();
    wasm_tracing_WasmLayerConfig config;
    wasm_tracing__config__WasmLayerConfig__new(&config);
    wasm_tracing__config__WasmLayerConfig__with_max_level(&config, max_level);
    wasm_tracing__config__WasmLayerConfig__with_console_group_spans(&config);
    (void)wasm_tracing__set_as_global_default_with_config(&config);
}

JZ_EXPORT("setTraceEntryCollectionEnabled") void jazz_wasm__set_trace_entry_collection_enabled(u32 enabled) {
    wasm_tracing__layer__set_trace_entry_collection_enabled(enabled != 0);
}

JZ_EXPORT("drainTraceEntries") jz_js jazz_wasm__drain_trace_entries(void) { return wasm_tracing__layer__drain_trace_entries(); }

JZ_EXPORT("subscribeTraceEntries") jz_js jazz_wasm__subscribe_trace_entries(jz_js callback) {
    return wasm_tracing__layer__subscribe_trace_entries(callback);
}

/* ---- setJsonSchemaValidator: JsJsonSchemaValidator / JsCompiledJsonSchema ---- */
static jz_str jazz_wasm__js_error_message(jz_js error) {
    jz_str out = {0};
    if (wbg_is_error(error)) {
        char *p = NULL;
        u32 n = 0;
        wbg_error_message(error, &p, &n);
        out.ptr = p;
        out.len = out.cap = n;
        return out;
    }
    if (wbg_take_string(error, &out)) return out;
    char *p = NULL;
    u32 n = 0;
    wbg_debug_string(error, &p, &n);
    out.ptr = p;
    out.len = out.cap = n;
    return out;
}

typedef struct jazz_wasm_JsCompiledJsonSchema { jz_js check; } jazz_wasm_JsCompiledJsonSchema;
typedef struct jazz_wasm_JsJsonSchemaValidator { jz_js compile; } jazz_wasm_JsJsonSchemaValidator;

static jz_err JsCompiledJsonSchema__validate(void *self, jz_slice instance_json, bool *has_reason, jz_str *reason) {
    jazz_wasm_JsCompiledJsonSchema *s = (jazz_wasm_JsCompiledJsonSchema *)self;
    jz_js arg = js_str(instance_json);
    jz_js outcome = 0;
    u32 threw = wbg_call1(s->check, WBG_NULL, arg, &outcome);
    jz_js_drop(arg);
    if (threw) {
        jz_str message = jazz_wasm__js_error_message(outcome);
        jz_js_drop(outcome);
        jz_err e = jz_err_new(WBG_ERR_STR, NULL);
        e.msg = JZ_NEW(jz_str);
        *e.msg = message;
        return e;
    }
    *has_reason = wbg_take_string(outcome, reason);
    jz_js_drop(outcome);
    return JZ_OK;
}

static void JsCompiledJsonSchema__drop(void *self) {
    jz_js_drop(((jazz_wasm_JsCompiledJsonSchema *)self)->check);
    jz_free(self);
}

static const jazz_model_HostCompiledJsonSchemaVtbl JS_COMPILED_JSON_SCHEMA_VT = {JsCompiledJsonSchema__validate, JsCompiledJsonSchema__drop};

static jz_err JsJsonSchemaValidator__compile(void *self, jz_slice schema_json, jazz_model_HostCompiledJsonSchema *out) {
    jazz_wasm_JsJsonSchemaValidator *v = (jazz_wasm_JsJsonSchemaValidator *)self;
    jz_js arg = js_str(schema_json);
    jz_js check = 0;
    u32 threw = wbg_call1(v->compile, WBG_NULL, arg, &check);
    jz_js_drop(arg);
    if (threw) {
        jz_str message = jazz_wasm__js_error_message(check);
        jz_js_drop(check);
        jz_err e = jz_err_new(WBG_ERR_STR, NULL);
        e.msg = JZ_NEW(jz_str);
        *e.msg = message;
        return e;
    }
    if (!wbg_is_function(check)) {
        jz_js_drop(check);
        return str_err("the JSON Schema validator did not return a function");
    }
    jazz_wasm_JsCompiledJsonSchema *compiled = JZ_NEW(jazz_wasm_JsCompiledJsonSchema);
    compiled->check = check;
    out->vt = &JS_COMPILED_JSON_SCHEMA_VT;
    out->self = compiled;
    return JZ_OK;
}

static void JsJsonSchemaValidator__drop(void *self) {
    jz_js_drop(((jazz_wasm_JsJsonSchemaValidator *)self)->compile);
    jz_free(self);
}

static const jazz_model_HostJsonSchemaValidatorVtbl JS_JSON_SCHEMA_VALIDATOR_VT = {JsJsonSchemaValidator__compile, JsJsonSchemaValidator__drop};

JZ_EXPORT("setJsonSchemaValidator") void jazz_wasm__set_json_schema_validator(jz_js compile) {
    jazz_wasm_JsJsonSchemaValidator *v = JZ_NEW(jazz_wasm_JsJsonSchemaValidator);
    v->compile = compile;
    jazz_model_HostJsonSchemaValidator validator = {&JS_JSON_SCHEMA_VALIDATOR_VT, v};
    jazz_model__model__json_schema__install_host_validator(validator);
}

/* nativeArtifactFingerprint: option_env!("JAZZ_NATIVE_ARTIFACT_FINGERPRINT") */
#ifndef JAZZ_NATIVE_ARTIFACT_FINGERPRINT
#define JAZZ_NATIVE_ARTIFACT_FINGERPRINT "missing-build-fingerprint"
#endif
JZ_EXPORT("nativeArtifactFingerprint") void jazz_wasm__native_artifact_fingerprint(wbg_ret_str *ret) {
    wbg_ret_string(ret, jz_str_from(JAZZ_NATIVE_ARTIFACT_FINGERPRINT));
}

JZ_EXPORT("__testBindingCodecGoldenFixture") void jazz_wasm__test_binding_codec_golden_fixture(wbg_ret_str *ret) {
    wbg_ret_string(ret, jz_str_from(jazz_db__binding_codec__BINDING_CODEC_GOLDEN_FIXTURE));
}

static void bytes_vec_item_drop(void *p) { jz_bytes_drop((jz_bytes *)p); }

/* str::parse::<u64>() */
static bool parse_u64_decimal(const jz_str *s, u64 *out) {
    if (s->len == 0) return false;
    u32 i = 0;
    if (s->ptr[0] == '+') {
        if (s->len == 1) return false;
        i = 1;
    }
    u64 v = 0;
    for (; i < s->len; i++) {
        char c = s->ptr[i];
        if (c < '0' || c > '9') return false;
        u64 d = (u64)(c - '0');
        if (v > (UINT64_MAX - d) / 10) return false;
        v = v * 10 + d;
    }
    *out = v;
    return true;
}

JZ_EXPORT("__testValidateWireFrameBatchCorpus")
void jazz_wasm__test_validate_wire_frame_batch_corpus(wbg_ret_val *ret, u8 *frame_ptr, u32 frame_len, char *features_ptr, u32 features_len) {
    jz_bytes frame = arg_bytes(frame_ptr, frame_len);
    jz_str negotiated = arg_str(features_ptr, features_len);
    u64 negotiated_features = 0;
    jz_err e = JZ_OK;
    if (!parse_u64_decimal(&negotiated, &negotiated_features)) {
        e = str_err("test wire corpus features must be a u64 decimal");
    } else {
        JZ_VEC(jz_bytes) frames = {0};
        e = jazz_protocol__wire__decode_websocket_frame_batch(jz_bytes_slice(&frame), (void *)&frames);
        if (e.code) {
            e = display_str_err(e);
        } else {
            e = into_str_err(jazz_protocol__wire__validate_frames_for_artifact_corpus(frames.ptr, frames.len, negotiated_features));
        }
        jz_vec_drop(&frames, sizeof(jz_bytes), bytes_vec_item_drop);
    }
    jz_bytes_drop(&frame);
    jz_str_drop(&negotiated);
    ret_unit(ret, e);
}

JZ_EXPORT("__testWireFrameCorpusFeatures") void jazz_wasm__test_wire_frame_corpus_features(wbg_ret_str *ret) {
    wbg_ret_string(ret, jz_format("%llu", (unsigned long long)jazz_protocol__wire__current_wire_features()));
}

JZ_EXPORT("generateId") void jazz_wasm__generate_id(wbg_ret_str *ret) {
    u8 rand[10];
    jz_random_bytes(rand, sizeof rand);
    jz_uuid id = jz_uuid_v7(jz_now_ms(), rand);
    char buf[37];
    jz_uuid_to_string(&id, buf);
    wbg_ret_string(ret, jz_str_from(buf));
}

/* web_time::SystemTime::now().duration_since(UNIX_EPOCH).as_millis(), 0 on error */
static u64 jazz_wasm__current_timestamp_inner(void) { return jz_now_ms(); }
JZ_EXPORT("currentTimestamp") u64 jazz_wasm__current_timestamp(void) { return jazz_wasm__current_timestamp_inner(); }

static jz_err jazz_wasm__decode_seed(const jz_str *seed_b64, u8 out[32]) {
    jz_bytes bytes = {0};
    jz_err e = jz_base64_decode(jz_str_slice(seed_b64), true, &bytes);
    if (e.code) {
        jz_err fe = str_errf("seed base64 decode error: %.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        return fe;
    }
    if (bytes.len != 32) {
        jz_bytes_drop(&bytes);
        return str_err("seed must be exactly 32 bytes");
    }
    memcpy(out, bytes.ptr, 32);
    jz_bytes_drop(&bytes);
    return JZ_OK;
}

static const char *jazz_wasm__advice_string(u32 advice) {
    switch (advice) {
    case JAZZ_ADVICE_ALLOWED: return "allowed";
    case JAZZ_ADVICE_DENIED: return "denied";
    default: return "unknown";
    }
}

static void mint_token_export(wbg_ret_str *ret, const char *issuer, char *seed_ptr, u32 seed_len, char *aud_ptr, u32 aud_len, u32 ttl_seconds,
                              u64 now_seconds) {
    jz_str seed_b64 = arg_str(seed_ptr, seed_len);
    jz_str audience = arg_str(aud_ptr, aud_len);
    u8 seed[32];
    jz_str token = {0};
    jz_err e = jazz_wasm__decode_seed(&seed_b64, seed);
    if (!e.code)
        e = into_str_err(jazz_wasm__identity__mint_jazz_self_signed_token_at(seed, issuer, jz_str_slice(&audience), (u64)ttl_seconds, now_seconds, &token));
    jz_str_drop(&seed_b64);
    jz_str_drop(&audience);
    ret_str(ret, e, token);
}

JZ_EXPORT("mintLocalFirstToken")
void jazz_wasm__mint_local_first_token(wbg_ret_str *ret, char *seed_ptr, u32 seed_len, char *aud_ptr, u32 aud_len, u32 ttl_seconds, u64 now_seconds) {
    mint_token_export(ret, jazz_wasm__identity__LOCAL_FIRST_ISSUER, seed_ptr, seed_len, aud_ptr, aud_len, ttl_seconds, now_seconds);
}

JZ_EXPORT("deriveUserId") void jazz_wasm__derive_user_id(wbg_ret_str *ret, char *seed_ptr, u32 seed_len) {
    jz_str seed_b64 = arg_str(seed_ptr, seed_len);
    u8 seed[32];
    jz_err e = jazz_wasm__decode_seed(&seed_b64, seed);
    jz_str_drop(&seed_b64);
    jz_str out = {0};
    if (!e.code) {
        jz_uuid id = jazz_wasm__identity__derive_user_id(seed);
        char buf[37];
        jz_uuid_to_string(&id, buf);
        out = jz_str_from(buf);
    }
    ret_str(ret, e, out);
}

JZ_EXPORT("mintAnonymousToken")
void jazz_wasm__mint_anonymous_token(wbg_ret_str *ret, char *seed_ptr, u32 seed_len, char *aud_ptr, u32 aud_len, u32 ttl_seconds, u64 now_seconds) {
    mint_token_export(ret, jazz_wasm__identity__ANONYMOUS_ISSUER, seed_ptr, seed_len, aud_ptr, aud_len, ttl_seconds, now_seconds);
}

/* ================================================================== */
/* WasmOpenDbConfig / WasmWriteResult (postcard)                         */
/* ================================================================== */
typedef struct jazz_wasm_WasmDbIdentity { jazz_types_NodeUuid node; jazz_types_AuthorSubject author; } jazz_wasm_WasmDbIdentity;
typedef struct jazz_wasm_WasmOpenDbConfig {
    jazz_wasm_WasmDbIdentity identity;
    bool has_row_id_seed;
    u64 row_id_seed;
    bool history_complete;
    bool has_initial_sync_flush_every;
    u32 initial_sync_flush_every;
} jazz_wasm_WasmOpenDbConfig;

static jz_err WasmOpenDbConfig__Deserialize__postcard(jz_pc_r *r, jazz_wasm_WasmOpenDbConfig *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jz_pc_r_uuid(r, &out->identity.node));
    jz_str canonical = {0};
    JZ_TRY(jz_pc_r_str(r, &canonical));
    jz_err e = jazz_types__ids__AuthorSubject__from_canonical(jz_str_slice(&canonical), &out->identity.author);
    jz_str_drop(&canonical);
    JZ_TRY(e);
    JZ_TRY(jz_pc_r_option(r, &out->has_row_id_seed));
    if (out->has_row_id_seed) JZ_TRY(jz_pc_r_varint(r, &out->row_id_seed));
    JZ_TRY(jz_pc_r_bool(r, &out->history_complete));
    JZ_TRY(jz_pc_r_option(r, &out->has_initial_sync_flush_every));
    if (out->has_initial_sync_flush_every) JZ_TRY(jz_pc_r_u32(r, &out->initial_sync_flush_every));
    return JZ_OK;
}

static jazz_db_DbIdentity WasmDbIdentity__into_DbIdentity(jazz_wasm_WasmDbIdentity identity) {
    jazz_db_DbIdentity out = {identity.node, identity.author};
    return out;
}

/* WasmWriteResult { row_id: RowUuid, tx_id: TxId } */
static void WasmWriteResult__Serialize__postcard(const jazz_types_RowUuid *row_id, const jazz_model_TxId *tx_id, jz_bytes *out) {
    jz_pc_w w = {out};
    jz_pc_uuid(&w, row_id);
    jz_pc_varint(&w, tx_id->time);
    jz_pc_uuid(&w, &tx_id->node);
}

/* ================================================================== */
/* local futures (Pin<Box<dyn Future>>) as poll thunks                   */
/* ================================================================== */
/* poll returns true when Ready, writing the Result into *result / *out */
typedef struct jazz_wasm_BoxFuture {
    bool (*poll)(void *state, const jz_waker *waker, jz_err *result, void *out);
    void *state;
    void (*drop)(void *state);
} jazz_wasm_BoxFuture;

static void BoxFuture__drop(jazz_wasm_BoxFuture *f) {
    if (f->drop) f->drop(f->state);
    f->drop = NULL;
    f->state = NULL;
}

static void noop_wake(void *ctx) { (void)ctx; }
static const jz_waker NOOP_WAKER = {noop_wake, NULL};

/* RefCell<Option<Pin<Box<dyn Future>>>> */
typedef struct jazz_wasm_FutureSlot { bool some; jazz_wasm_BoxFuture future; } jazz_wasm_FutureSlot;

static void FutureSlot__take_drop(jazz_wasm_FutureSlot *slot) {
    if (slot->some) {
        slot->some = false;
        BoxFuture__drop(&slot->future);
    }
}

/* ---- PendingNativeRead ---- */
typedef struct jazz_wasm_WasmPendingNativeRead { jazz_wasm_FutureSlot *future; /* Rc<RefCell<Option<WasmReadFuture>>> */ } jazz_wasm_WasmPendingNativeRead;

static void future_slot_rc_drop(void *p) { FutureSlot__take_drop((jazz_wasm_FutureSlot *)p); }

static jazz_wasm_WasmPendingNativeRead *WasmPendingNativeRead__new(jazz_wasm_BoxFuture future) {
    jazz_wasm_WasmPendingNativeRead *r = JZ_NEW(jazz_wasm_WasmPendingNativeRead);
    r->future = (jazz_wasm_FutureSlot *)jz_rc_new(sizeof(jazz_wasm_FutureSlot), future_slot_rc_drop);
    r->future->some = true;
    r->future->future = future;
    return r;
}

/* Result<Option<Vec<u8>>, JsValue> */
static jz_err WasmPendingNativeRead__poll_once(jazz_wasm_WasmPendingNativeRead *self, bool *some, jz_bytes *out) {
    if (!self->future->some) return str_err("native pending read is already complete");
    jazz_wasm_BoxFuture future = self->future->future;
    self->future->some = false;
    jz_err result = JZ_OK;
    if (future.poll(future.state, &NOOP_WAKER, &result, out)) {
        BoxFuture__drop(&future);
        *some = result.code == 0;
        return result;
    }
    self->future->future = future;
    self->future->some = true;
    *some = false;
    return JZ_OK;
}

JZ_EXPORT("pendingnativeread_poll") void jazz_wasm__WasmPendingNativeRead__poll(wbg_ret_val *ret, jazz_wasm_WasmPendingNativeRead *self) {
    bool some = false;
    jz_bytes bytes = {0};
    jz_err e = WasmPendingNativeRead__poll_once(self, &some, &bytes);
    ret_val(ret, e, e.code ? 0 : (some ? bytes_to_js(&bytes) : WBG_NULL));
}

JZ_EXPORT("pendingnativeread_cancel") void jazz_wasm__WasmPendingNativeRead__cancel(jazz_wasm_WasmPendingNativeRead *self) {
    FutureSlot__take_drop(self->future);
}

JZ_EXPORT("__wbg_pendingnativeread_free") void jazz_wasm__WasmPendingNativeRead__free(jazz_wasm_WasmPendingNativeRead *self) {
    jz_rc_release(self->future);
    jz_free(self);
}

/* ---- WasmQueryRuntimeWake: unbounded channel + coalescing flag, drained by
 * a spawn_local task that calls back into JS ---- */
typedef struct jazz_wasm_WakeReceiver {
    jz_chan *channel;  /* UnboundedSender<()> / receiver */
    bool *pending;     /* Arc<AtomicBool> */
    jz_js callback;
    bool with_immediate_arg; /* tick scheduler: call1("immediate"); pending op: call0 */
} jazz_wasm_WakeReceiver;

typedef struct jazz_wasm_WasmQueryRuntimeWake {
    jazz_wasm_WakeReceiver *receiver; /* Rc, shared with the drain task */
} jazz_wasm_WasmQueryRuntimeWake;

static void wake_receiver_drop(void *p) {
    jazz_wasm_WakeReceiver *r = (jazz_wasm_WakeReceiver *)p;
    jz_chan_release(r->channel);
    jz_rc_release(r->pending);
    jz_js_drop(r->callback);
}

/* `while receiver.next().await.is_some() { notified.store(false); callback.call() }` */
static void wake_receiver_drain(void *p) {
    jazz_wasm_WakeReceiver *r = (jazz_wasm_WakeReceiver *)p;
    u8 unit = 0;
    while (jz_chan_try_recv(r->channel, &unit)) {
        *r->pending = false; /* Ordering::Release */
        jz_js ignored = 0;
        if (r->with_immediate_arg) {
            jz_js arg = wbg_str("immediate");
            (void)wbg_call1(r->callback, WBG_NULL, arg, &ignored);
            jz_js_drop(arg);
        } else {
            (void)wbg_call0(r->callback, WBG_NULL, &ignored);
        }
        jz_js_drop(ignored);
    }
}

static void wake_receiver_task_drop(void *p) { jz_rc_release(p); }

/* ArcWake::wake_by_ref */
static void WasmQueryRuntimeWake__wake_by_ref(void *ctx) {
    jazz_wasm_WasmQueryRuntimeWake *w = (jazz_wasm_WasmQueryRuntimeWake *)ctx;
    bool was = *w->receiver->pending; /* swap(true, AcqRel) */
    *w->receiver->pending = true;
    if (!was) {
        u8 unit = 0;
        jz_chan_send(w->receiver->channel, &unit);
        wbg_spawn_local(wake_receiver_drain, jz_rc_clone(w->receiver), wake_receiver_task_drop);
    }
}

static void query_runtime_wake_drop(void *p) { jz_rc_release(((jazz_wasm_WasmQueryRuntimeWake *)p)->receiver); }

static jz_waker WasmQueryRuntimeWake__into_waker(jazz_wasm_WakeReceiver *receiver) {
    jazz_wasm_WasmQueryRuntimeWake *w = (jazz_wasm_WasmQueryRuntimeWake *)jz_rc_new(sizeof *w, query_runtime_wake_drop);
    w->receiver = (jazz_wasm_WakeReceiver *)jz_rc_clone(receiver);
    jz_waker waker = {WasmQueryRuntimeWake__wake_by_ref, w};
    return waker;
}

static jazz_wasm_WakeReceiver *wake_receiver_new(jz_js callback, bool with_immediate_arg) {
    jazz_wasm_WakeReceiver *r = (jazz_wasm_WakeReceiver *)jz_rc_new(sizeof *r, wake_receiver_drop);
    r->channel = jz_chan_new(1);
    r->pending = (bool *)jz_rc_new(sizeof(bool), NULL);
    *r->pending = false;
    r->callback = callback;
    r->with_immediate_arg = with_immediate_arg;
    return r;
}

static jz_waker jazz_wasm__pending_operation_waker(jz_js callback) {
    jazz_wasm_WakeReceiver *receiver = wake_receiver_new(callback, false);
    jz_waker waker = WasmQueryRuntimeWake__into_waker(receiver);
    jz_rc_release(receiver);
    return waker;
}

static void waker_release(jz_waker *w) {
    if (w->ctx) jz_rc_release(w->ctx);
    w->ctx = NULL;
}

/* ---- WasmPendingSubscription ---- */
typedef struct jazz_wasm_WasmPendingSubscription {
    jazz_wasm_FutureSlot future; /* PendingWasmOperation<JsValue> */
    bool has_wake;
    jz_waker wake;
} jazz_wasm_WasmPendingSubscription;

/* Result<Option<JsValue>, JsValue> */
static jz_err WasmPendingSubscription__poll_once(jazz_wasm_WasmPendingSubscription *self, bool *some, jz_js *out) {
    if (!self->future.some) return to_js_err(str_err("native operation is complete or cancelled"));
    jazz_wasm_BoxFuture future = self->future.future;
    self->future.some = false;
    const jz_waker *wake = self->has_wake ? &self->wake : &NOOP_WAKER;
    jz_err result = JZ_OK;
    if (future.poll(future.state, wake, &result, out)) {
        BoxFuture__drop(&future);
        *some = result.code == 0;
        return result;
    }
    self->future.future = future;
    self->future.some = true;
    *some = false;
    return JZ_OK;
}

JZ_EXPORT("wasmpendingsubscription_setWake") void jazz_wasm__WasmPendingSubscription__set_wake(jazz_wasm_WasmPendingSubscription *self, jz_js callback) {
    if (self->has_wake) waker_release(&self->wake);
    self->wake = jazz_wasm__pending_operation_waker(callback);
    self->has_wake = true;
}

JZ_EXPORT("wasmpendingsubscription_poll") void jazz_wasm__WasmPendingSubscription__poll(wbg_ret_val *ret, jazz_wasm_WasmPendingSubscription *self) {
    bool some = false;
    jz_js v = 0;
    jz_err e = WasmPendingSubscription__poll_once(self, &some, &v);
    ret_val(ret, e, some ? v : WBG_UNDEFINED);
}

JZ_EXPORT("wasmpendingsubscription_cancel") void jazz_wasm__WasmPendingSubscription__cancel(jazz_wasm_WasmPendingSubscription *self) {
    FutureSlot__take_drop(&self->future);
    if (self->has_wake) {
        waker_release(&self->wake);
        self->has_wake = false;
    }
}

JZ_EXPORT("__wbg_wasmpendingsubscription_free") void jazz_wasm__WasmPendingSubscription__free(jazz_wasm_WasmPendingSubscription *self) {
    jazz_wasm__WasmPendingSubscription__cancel(self);
    jz_free(self);
}

/* ================================================================== */
/* WasmDbInner                                                          */
/* ================================================================== */
typedef struct jazz_wasm_WasmDbInner { jazz_db_Db *db; /* Rc<Db> */ bool drives_mutations_inline; } jazz_wasm_WasmDbInner;

static jazz_wasm_WasmDbInner WasmDbInner__clone(const jazz_wasm_WasmDbInner *i) {
    jazz_wasm_WasmDbInner c = {jazz_db__db__Db__rc_clone(i->db), i->drives_mutations_inline};
    return c;
}
static void WasmDbInner__drop(jazz_wasm_WasmDbInner *i) {
    if (i->db) jazz_db__db__Db__rc_release(i->db);
    i->db = NULL;
}
static jazz_wasm_WasmDbInner WasmDbInner__memory(jazz_db_Db *db) {
    jazz_wasm_WasmDbInner i = {db, true};
    return i;
}
static jazz_wasm_WasmDbInner WasmDbInner__browser(jazz_db_Db *db) {
    jazz_wasm_WasmDbInner i = {db, false};
    return i;
}

/* Called after every enqueued mutation. */
static void WasmDbInner__after_enqueue(const jazz_wasm_WasmDbInner *self) {
    if (self->drives_mutations_inline) jazz_db__db__lifecycle__Db__drive_queued_mutation_once(self->db);
}

/* ================================================================== */
/* WasmWrite                                                            */
/* ================================================================== */
typedef struct jazz_wasm_WasmWrite {
    jz_bytes payload;
    jazz_types_RowUuid row_id;
    jazz_model_TransactionId tx_id;
    bool has_inner;
    struct { jazz_db_Db *db; jazz_db_WriteHandle *write; } inner; /* WasmWriteInner */
} jazz_wasm_WasmWrite;

static void WasmWrite__inner_drop(jazz_wasm_WasmWrite *w) {
    if (!w->has_inner) return;
    w->has_inner = false;
    jazz_db__db__WriteHandle__drop(w->inner.write);
    jazz_db__db__Db__rc_release(w->inner.db);
}

/* wasm_write(db, write) */
static jz_err jazz_wasm__wasm_write(jazz_db_Db *db, jazz_db_WriteHandle *write, jazz_wasm_WasmWrite **out) {
    jazz_model_TxId tx_id = jazz_db__db__WriteHandle__mergeable_tx_id(write);
    jazz_types_RowUuid row_id = jazz_db__db__WriteHandle__row_uuid(write);
    jazz_wasm_WasmWrite *w = JZ_NEW(jazz_wasm_WasmWrite);
    WasmWriteResult__Serialize__postcard(&row_id, &tx_id, &w->payload);
    w->row_id = row_id;
    jazz_model__model__transaction__TransactionId__from_committed_tx(&tx_id, &w->tx_id);
    w->has_inner = true;
    w->inner.db = db;
    w->inner.write = write;
    *out = w;
    return JZ_OK;
}

static jz_err jazz_wasm__write_state_to_js(const jazz_db_WriteState *state, jz_js *out) {
    return to_js_err(into_str_err(swb__WriteState__serialize(state, out)));
}

static jz_err jazz_wasm__poll_write_state_once(const jazz_db_WriteHandle *write, jz_js *out) {
    jz_err result = JZ_OK;
    jazz_db_WriteState state;
    if (!jazz_db__db__WriteHandle__write_state_poll(write, &result, &state))
        return str_err("write state is temporarily busy; retry after the next WASM turn");
    JZ_TRY(to_js_err(result));
    jz_err e = jazz_wasm__write_state_to_js(&state, out);
    jazz_db__db__WriteState__drop(&state);
    return e;
}

JZ_EXPORT("wasmwrite_txId") void jazz_wasm__WasmWrite__tx_id(wbg_ret_str *ret, jazz_wasm_WasmWrite *self) {
    jz_str s = {0};
    jazz_model__model__transaction__TransactionId__Display__fmt(&self->tx_id, &s);
    wbg_ret_string(ret, s);
}

JZ_EXPORT("wasmwrite_payload") void jazz_wasm__WasmWrite__payload(wbg_ret_str *ret, jazz_wasm_WasmWrite *self) {
    ret_bytes(ret, JZ_OK, jz_bytes_clone(&self->payload));
}

JZ_EXPORT("wasmwrite_rowId") void jazz_wasm__WasmWrite__row_id(wbg_ret_str *ret, jazz_wasm_WasmWrite *self) {
    ret_bytes(ret, JZ_OK, jz_bytes_from(self->row_id.b, 16));
}

JZ_EXPORT("wasmwrite_writeState") void jazz_wasm__WasmWrite__write_state(wbg_ret_val *ret, jazz_wasm_WasmWrite *self) {
    jz_js v = 0;
    jz_err e = self->has_inner ? jazz_wasm__poll_write_state_once(self->inner.write, &v) : str_err("write state is unavailable");
    ret_val(ret, e, v);
}

static jz_err jazz_wasm__durability_tier_from_str(jz_slice tier, jazz_model_DurabilityTier *out) {
    return into_str_err(jazz_db__binding_args__durability_tier(tier, out));
}

typedef struct jazz_wasm_WaitPromiseCtx { jz_js resolve; jz_js reject; } jazz_wasm_WaitPromiseCtx;

static void wait_promise_callback(void *ctx, jz_err result, const jazz_model_TxId *tx) {
    (void)tx;
    jazz_wasm_WaitPromiseCtx *c = (jazz_wasm_WaitPromiseCtx *)ctx;
    jz_js ignored = 0;
    if (!result.code) {
        (void)wbg_call0(c->resolve, WBG_UNDEFINED, &ignored);
    } else {
        jz_js error = jazz_wasm__to_js_error(result);
        (void)wbg_call1(c->reject, WBG_UNDEFINED, error, &ignored);
        jz_js_drop(error);
    }
    jz_js_drop(ignored);
    jz_js_drop(c->resolve);
    jz_js_drop(c->reject);
    jz_free(c);
}

/* js_sys::Promise::new(|resolve, reject| db.wait_for_write_with(...)) */
static jz_js jazz_wasm__wait_promise(jazz_db_Db *db, const jazz_db_WriteHandle *write, jazz_db_WriteWaitOptions options) {
    jazz_wasm_WaitPromiseCtx *c = JZ_NEW(jazz_wasm_WaitPromiseCtx);
    jz_js promise = wbg_promise_with_resolvers(&c->resolve, &c->reject);
    jazz_db__db__lifecycle__Db__wait_for_write_with(db, write, options, wait_promise_callback, c);
    return promise;
}

/* wait(tier: String, observe_only: Option<bool>) -> Result<Promise, JsValue> */
JZ_EXPORT("wasmwrite_wait") void jazz_wasm__WasmWrite__wait(wbg_ret_val *ret, jazz_wasm_WasmWrite *self, char *tier_ptr, u32 tier_len, u32 observe_only) {
    jz_str tier_s = arg_str(tier_ptr, tier_len);
    jazz_model_DurabilityTier tier = JAZZ_DUR_NONE;
    jz_err e = jazz_wasm__durability_tier_from_str(jz_str_slice(&tier_s), &tier);
    jz_str_drop(&tier_s);
    jz_js promise = 0;
    if (!e.code) {
        jazz_db_WriteWaitOptions options = {tier, observe_only == 0xFFFFFFu ? false : observe_only != 0};
        if (self->has_inner) promise = jazz_wasm__wait_promise(self->inner.db, self->inner.write, options);
        else e = str_err("write state is unavailable");
    }
    ret_val(ret, e, promise);
}

JZ_EXPORT("wasmwrite_close") u32 jazz_wasm__WasmWrite__close(jazz_wasm_WasmWrite *self) {
    bool had = self->has_inner;
    WasmWrite__inner_drop(self);
    return had;
}

JZ_EXPORT("__wbg_wasmwrite_free") void jazz_wasm__WasmWrite__free(jazz_wasm_WasmWrite *self) {
    WasmWrite__inner_drop(self);
    jz_bytes_drop(&self->payload);
    jz_free(self);
}

/* ================================================================== */
/* StreamingMutation                                                    */
/* ================================================================== */
typedef struct jazz_wasm_WasmStreamingMutationState {
    jazz_wasm_WasmDbInner db;
    jazz_db_StreamingValueUpload *upload;
    u32 mutation; /* StreamingMutationKind */
    jz_str table;
    jazz_types_RowUuid row_id;
    jz_btree cells;
    jz_str column;
    jazz_db_WriteIdentity identity;
    bool has_updated_at_ms;
    u64 updated_at_ms;
    bool has_head;
    jazz_model_BranchSelector head;
    bool has_base;
    jazz_model_BranchViewBase base;
} jazz_wasm_WasmStreamingMutationState;

static void StreamingMutationState__drop(jazz_wasm_WasmStreamingMutationState *s) {
    if (!s) return;
    WasmDbInner__drop(&s->db);
    jz_str_drop(&s->table);
    jz_btree_drop(&s->cells);
    jz_str_drop(&s->column);
    if (s->has_head) jazz_model__model__branch__BranchSelector__drop(&s->head);
    if (s->has_base) jazz_model__model__branch__BranchViewBase__drop(&s->base);
    jz_free(s);
}

/* futures_channel::oneshot of Box<WasmStreamingMutationState> */
enum { ONESHOT_EMPTY = 0, ONESHOT_SENT = 1, ONESHOT_CANCELED = 2 };
typedef struct jazz_wasm_StateOneshot { u32 status; jazz_wasm_WasmStreamingMutationState *value; } jazz_wasm_StateOneshot;
static void state_oneshot_drop(void *p) {
    jazz_wasm_StateOneshot *o = (jazz_wasm_StateOneshot *)p;
    if (o->status == ONESHOT_SENT) StreamingMutationState__drop(o->value);
}

enum { SML_OPEN, SML_PUSH_IN_FLIGHT, SML_ABORT_CLAIMED, SML_CLOSED };
typedef struct jazz_wasm_WasmStreamingMutationLifecycle {
    u32 tag;
    jazz_wasm_WasmStreamingMutationState *open; /* Open(Box<State>) */
    jazz_wasm_StateOneshot *completion;         /* PushInFlight { completion } (Rc'd channel) */
} jazz_wasm_WasmStreamingMutationLifecycle;

static void lifecycle_rc_drop(void *p) {
    jazz_wasm_WasmStreamingMutationLifecycle *l = (jazz_wasm_WasmStreamingMutationLifecycle *)p;
    if (l->tag == SML_OPEN) StreamingMutationState__drop(l->open);
    if (l->tag == SML_PUSH_IN_FLIGHT && l->completion) jz_rc_release(l->completion);
}

typedef struct jazz_wasm_WasmStreamingMutation { jazz_wasm_WasmStreamingMutationLifecycle *state; /* Rc<RefCell<..>> */ } jazz_wasm_WasmStreamingMutation;

static jz_js jazz_wasm__streaming_mutation_closed(void) { return wbg_str("streaming mutation is closed"); }

static jz_js reject_streaming_mutation_closed(void) {
    jz_js reason = jazz_wasm__streaming_mutation_closed();
    jz_js p = wbg_promise_reject(reason);
    jz_js_drop(reason);
    return p;
}

typedef struct jazz_wasm_PushTask {
    jazz_wasm_WasmStreamingMutationLifecycle *state_cell; /* Rc */
    jazz_wasm_WasmStreamingMutationState *state;
    jazz_wasm_StateOneshot *completion_tx; /* Rc */
    jz_bytes chunk;
} jazz_wasm_PushTask;

static jz_err streaming_push_body(void *ctx, jz_js *out) {
    jazz_wasm_PushTask *t = (jazz_wasm_PushTask *)ctx;
    jz_err result = to_js_err(jazz_db__db__mutations__Db__push_streaming_value_upload(t->state->db.db, t->state->upload, jz_bytes_slice(&t->chunk)));
    jazz_wasm_WasmStreamingMutationLifecycle *lifecycle = t->state_cell;
    switch (lifecycle->tag) {
    case SML_ABORT_CLAIMED:
        /* The abort claim is made synchronously. Deliver the state to the
         * aborting operation even when push failed. */
        t->completion_tx->status = ONESHOT_SENT;
        t->completion_tx->value = t->state;
        t->state = NULL;
        break;
    case SML_PUSH_IN_FLIGHT:
        if (lifecycle->completion) jz_rc_release(lifecycle->completion);
        lifecycle->completion = NULL;
        if (!result.code) {
            lifecycle->tag = SML_OPEN;
            lifecycle->open = t->state;
        } else {
            lifecycle->tag = SML_CLOSED;
            StreamingMutationState__drop(t->state);
        }
        t->state = NULL;
        break;
    default:
        jz_panic("streaming mutation push completion lost its lifecycle");
    }
    JZ_TRY(result);
    *out = WBG_UNDEFINED;
    return JZ_OK;
}

static void streaming_push_task_drop(void *ctx) {
    jazz_wasm_PushTask *t = (jazz_wasm_PushTask *)ctx;
    /* dropping an unsent sender cancels the receiver */
    if (t->completion_tx->status == ONESHOT_EMPTY) t->completion_tx->status = ONESHOT_CANCELED;
    jz_rc_release(t->completion_tx);
    StreamingMutationState__drop(t->state);
    jz_rc_release(t->state_cell);
    jz_bytes_drop(&t->chunk);
    jz_free(t);
}

JZ_EXPORT("streamingmutation_push") jz_js jazz_wasm__WasmStreamingMutation__push(jazz_wasm_WasmStreamingMutation *self, u8 *chunk_ptr, u32 chunk_len) {
    jz_bytes chunk = arg_bytes(chunk_ptr, chunk_len);
    jazz_wasm_WasmStreamingMutationLifecycle *lifecycle = self->state;
    if (lifecycle->tag != SML_OPEN) {
        jz_bytes_drop(&chunk);
        return reject_streaming_mutation_closed();
    }
    jazz_wasm_PushTask *t = JZ_NEW(jazz_wasm_PushTask);
    t->state = lifecycle->open;
    t->completion_tx = (jazz_wasm_StateOneshot *)jz_rc_new(sizeof(jazz_wasm_StateOneshot), state_oneshot_drop);
    lifecycle->tag = SML_PUSH_IN_FLIGHT;
    lifecycle->open = NULL;
    lifecycle->completion = (jazz_wasm_StateOneshot *)jz_rc_clone(t->completion_tx);
    t->state_cell = (jazz_wasm_WasmStreamingMutationLifecycle *)jz_rc_clone(self->state);
    t->chunk = chunk;
    return jazz_wasm__future_to_promise(streaming_push_body, t, streaming_push_task_drop);
}

static jz_js wasm_write_into_js(jazz_wasm_WasmWrite *w) { return wbg_wrap_object(WBG_CLASS_WASMWRITE, w); }

static jz_err streaming_finish_body(void *ctx, jz_js *out) {
    jazz_wasm_WasmStreamingMutationState *state = (jazz_wasm_WasmStreamingMutationState *)ctx;
    jazz_db_Db *db = state->db.db;
    jazz_db_WriteHandle *write = NULL;
    bool has_head = state->has_head, has_base = state->has_base;
    jazz_db_StreamingValueUpload *upload = state->upload;
    jz_btree cells = state->cells;
    state->upload = NULL;
    memset(&state->cells, 0, sizeof state->cells);
    state->has_head = false; /* moved into the call */
    state->has_base = false;
    JZ_TRY(to_js_err(jazz_db__db__mutations__Db__finish_streaming_value_upload(
        db, upload, state->mutation, jz_str_slice(&state->table), state->row_id, cells, jz_str_slice(&state->column), state->identity,
        state->has_updated_at_ms, state->updated_at_ms, has_head, state->head, has_base, state->base, &write)));
    jazz_wasm_WasmWrite *w = NULL;
    JZ_TRY(jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(db), write, &w));
    *out = wasm_write_into_js(w);
    return JZ_OK;
}

static void streaming_state_task_drop(void *ctx) { StreamingMutationState__drop((jazz_wasm_WasmStreamingMutationState *)ctx); }

JZ_EXPORT("streamingmutation_finish") jz_js jazz_wasm__WasmStreamingMutation__finish(jazz_wasm_WasmStreamingMutation *self) {
    jazz_wasm_WasmStreamingMutationLifecycle *lifecycle = self->state;
    if (lifecycle->tag != SML_OPEN) return reject_streaming_mutation_closed();
    jazz_wasm_WasmStreamingMutationState *state = lifecycle->open;
    lifecycle->tag = SML_CLOSED;
    lifecycle->open = NULL;
    return jazz_wasm__future_to_promise(streaming_finish_body, state, streaming_state_task_drop);
}

enum { ABORT_IMMEDIATE, ABORT_AFTER_PUSH };
typedef struct jazz_wasm_AbortTask {
    jazz_wasm_WasmStreamingMutationLifecycle *state_cell; /* Rc */
    u32 claim;
    jazz_wasm_WasmStreamingMutationState *state; /* Immediate */
    jazz_wasm_StateOneshot *completion;          /* AfterPush */
} jazz_wasm_AbortTask;

static jz_err streaming_abort_body(void *ctx, jz_js *out) {
    jazz_wasm_AbortTask *t = (jazz_wasm_AbortTask *)ctx;
    jazz_wasm_WasmStreamingMutationState *state = NULL;
    if (t->claim == ABORT_IMMEDIATE) {
        state = t->state;
        t->state = NULL;
    } else {
        while (t->completion->status == ONESHOT_EMPTY) wbg_yield(); /* completion.await */
        if (t->completion->status == ONESHOT_CANCELED) return wbg_err_from_js(jazz_wasm__streaming_mutation_closed());
        state = t->completion->value;
        t->completion->value = NULL;
        t->completion->status = ONESHOT_CANCELED;
    }
    jazz_db_StreamingValueUpload *upload = state->upload;
    state->upload = NULL;
    jz_err result = to_js_err(jazz_db__db__mutations__Db__abort_streaming_value_upload(state->db.db, upload));
    jazz_wasm_WasmStreamingMutationLifecycle *lifecycle = t->state_cell;
    if (lifecycle->tag == SML_OPEN) StreamingMutationState__drop(lifecycle->open);
    if (lifecycle->tag == SML_PUSH_IN_FLIGHT && lifecycle->completion) jz_rc_release(lifecycle->completion);
    lifecycle->tag = SML_CLOSED;
    lifecycle->open = NULL;
    lifecycle->completion = NULL;
    StreamingMutationState__drop(state);
    JZ_TRY(result);
    *out = WBG_TRUE;
    return JZ_OK;
}

static void streaming_abort_task_drop(void *ctx) {
    jazz_wasm_AbortTask *t = (jazz_wasm_AbortTask *)ctx;
    StreamingMutationState__drop(t->state);
    if (t->completion) jz_rc_release(t->completion);
    jz_rc_release(t->state_cell);
    jz_free(t);
}

JZ_EXPORT("streamingmutation_abort") jz_js jazz_wasm__WasmStreamingMutation__abort(jazz_wasm_WasmStreamingMutation *self) {
    jazz_wasm_WasmStreamingMutationLifecycle *lifecycle = self->state;
    jazz_wasm_AbortTask *t = NULL;
    switch (lifecycle->tag) {
    case SML_OPEN:
        t = JZ_NEW(jazz_wasm_AbortTask);
        t->claim = ABORT_IMMEDIATE;
        t->state = lifecycle->open;
        lifecycle->open = NULL;
        lifecycle->tag = SML_ABORT_CLAIMED;
        break;
    case SML_PUSH_IN_FLIGHT:
        t = JZ_NEW(jazz_wasm_AbortTask);
        t->claim = ABORT_AFTER_PUSH;
        t->completion = lifecycle->completion;
        lifecycle->completion = NULL;
        lifecycle->tag = SML_ABORT_CLAIMED;
        break;
    default:
        break; /* state is put back unchanged */
    }
    if (!t) return wbg_promise_resolve(WBG_FALSE);
    t->state_cell = (jazz_wasm_WasmStreamingMutationLifecycle *)jz_rc_clone(self->state);
    return jazz_wasm__future_to_promise(streaming_abort_body, t, streaming_abort_task_drop);
}

JZ_EXPORT("__wbg_streamingmutation_free") void jazz_wasm__WasmStreamingMutation__free(jazz_wasm_WasmStreamingMutation *self) {
    jz_rc_release(self->state);
    jz_free(self);
}

/* ================================================================== */
/* WasmTransport plumbing: wire queues, WireTransport, TickScheduler     */
/* ================================================================== */
/* WasmWireQueues: three Rc'd cells cloned together */
typedef struct jazz_wasm_WasmWireQueues {
    jz_deque inbound;  /* VecDeque<Vec<u8>> */
    jz_deque outbound; /* VecDeque<Vec<u8>> */
    bool has_outbound_scheduler;
    jz_js outbound_scheduler; /* Option<js_sys::Function> */
} jazz_wasm_WasmWireQueues;

static void wire_queues_drop(void *p) {
    jazz_wasm_WasmWireQueues *q = (jazz_wasm_WasmWireQueues *)p;
    jz_deque_drop(&q->inbound, bytes_vec_item_drop);
    jz_deque_drop(&q->outbound, bytes_vec_item_drop);
    if (q->has_outbound_scheduler) jz_js_drop(q->outbound_scheduler);
}

static jazz_wasm_WasmWireQueues *WasmWireQueues__Default__default(void) {
    jazz_wasm_WasmWireQueues *q = (jazz_wasm_WasmWireQueues *)jz_rc_new(sizeof *q, wire_queues_drop);
    jz_deque_init(&q->inbound, sizeof(jz_bytes));
    jz_deque_init(&q->outbound, sizeof(jz_bytes));
    q->has_outbound_scheduler = false;
    return q;
}

typedef struct jazz_wasm_WasmWireTransport { jazz_wasm_WasmWireQueues *queues; } jazz_wasm_WasmWireTransport;

static jz_err WasmWireTransport__WireTransport__send_frame(void *self, jz_bytes frame) {
    jazz_wasm_WasmWireQueues *q = ((jazz_wasm_WasmWireTransport *)self)->queues;
    jz_deque_push_back(&q->outbound, &frame);
    if (q->has_outbound_scheduler) {
        jz_js scheduler = wbg_clone_ref(q->outbound_scheduler);
        jz_js ignored = 0;
        (void)wbg_call0(scheduler, WBG_NULL, &ignored);
        jz_js_drop(ignored);
        jz_js_drop(scheduler);
    }
    return JZ_OK;
}

static bool WasmWireTransport__WireTransport__try_recv_frame(void *self, jz_bytes *out) {
    jazz_wasm_WasmWireQueues *q = ((jazz_wasm_WasmWireTransport *)self)->queues;
    return jz_deque_pop_front(&q->inbound, out);
}

static void WasmWireTransport__drop(void *self) {
    jz_rc_release(((jazz_wasm_WasmWireTransport *)self)->queues);
    jz_free(self);
}

static const jazz_db_WireTransportVtbl WASM_WIRE_TRANSPORT_VT = {WasmWireTransport__WireTransport__send_frame,
                                                                 WasmWireTransport__WireTransport__try_recv_frame, WasmWireTransport__drop};

static jazz_wasm_WasmWireTransport *WasmWireTransport__new(jazz_wasm_WasmWireQueues *queues) {
    jazz_wasm_WasmWireTransport *t = JZ_NEW(jazz_wasm_WasmWireTransport);
    t->queues = (jazz_wasm_WasmWireQueues *)jz_rc_clone(queues);
    return t;
}

/* WasmTickScheduler */
typedef struct jazz_wasm_WasmTickScheduler {
    jz_js callback;
    jazz_wasm_WakeReceiver *progress; /* progress_wake sender + progress_wake_pending */
} jazz_wasm_WasmTickScheduler;

static void WasmTickScheduler__TickScheduler__schedule_tick(void *self, u32 urgency) {
    jazz_wasm_WasmTickScheduler *s = (jazz_wasm_WasmTickScheduler *)self;
    const char *name = urgency == JAZZ_TICK_IMMEDIATE ? "immediate" : urgency == JAZZ_TICK_DEFERRED ? "deferred" : "after-current-turn";
    jz_js arg = wbg_str(name);
    jz_js ignored = 0;
    (void)wbg_call1(s->callback, WBG_NULL, arg, &ignored);
    jz_js_drop(arg);
    jz_js_drop(ignored);
}

static void WasmTickScheduler__TickScheduler__schedule_tick_after(void *self, u64 delay_ms) {
    jazz_wasm_WasmTickScheduler *s = (jazz_wasm_WasmTickScheduler *)self;
    jz_str text = jz_format("after:%llu", (unsigned long long)delay_ms);
    jz_js arg = wbg_str_owned(&text);
    jz_js ignored = 0;
    (void)wbg_call1(s->callback, WBG_NULL, arg, &ignored);
    jz_js_drop(arg);
    jz_js_drop(ignored);
}

static bool WasmTickScheduler__TickScheduler__query_runtime_waker(void *self, jz_waker *out) {
    jazz_wasm_WasmTickScheduler *s = (jazz_wasm_WasmTickScheduler *)self;
    *out = WasmQueryRuntimeWake__into_waker(s->progress);
    return true;
}

static void WasmTickScheduler__drop(void *self) {
    jazz_wasm_WasmTickScheduler *s = (jazz_wasm_WasmTickScheduler *)self;
    jz_js_drop(s->callback);
    jz_rc_release(s->progress);
}

static const jazz_db_TickSchedulerVtbl WASM_TICK_SCHEDULER_VT = {
    WasmTickScheduler__TickScheduler__schedule_tick, WasmTickScheduler__TickScheduler__schedule_tick_after,
    WasmTickScheduler__TickScheduler__query_runtime_waker, WasmTickScheduler__drop};

/* WasmPermissionAdviceRequest { promise, cancel: Box<dyn Fn()> } */
typedef struct jazz_wasm_WasmPermissionAdviceRequest {
    jz_js promise;
    jazz_db_Db *cancel_db; /* captured Rc<Db> */
    u8 request_id[16];
} jazz_wasm_WasmPermissionAdviceRequest;

JZ_EXPORT("wasmpermissionadvicerequest_promise") jz_js jazz_wasm__WasmPermissionAdviceRequest__promise(jazz_wasm_WasmPermissionAdviceRequest *self) {
    return wbg_clone_ref(self->promise);
}

JZ_EXPORT("wasmpermissionadvicerequest_cancel") void jazz_wasm__WasmPermissionAdviceRequest__cancel(jazz_wasm_WasmPermissionAdviceRequest *self) {
    jazz_db__db__lifecycle__Db__cancel_permission_advice_request(self->cancel_db, self->request_id);
}

JZ_EXPORT("__wbg_wasmpermissionadvicerequest_free") void jazz_wasm__WasmPermissionAdviceRequest__free(jazz_wasm_WasmPermissionAdviceRequest *self) {
    jz_js_drop(self->promise);
    jazz_db__db__Db__rc_release(self->cancel_db);
    jz_free(self);
}

/* ================================================================== */
/* argument helpers                                                     */
/* ================================================================== */
static jz_err jazz_wasm__decode_cells(jz_slice bytes, jz_btree *out) {
    jz_err e = jazz_db__binding_codec__decode_named_cells(bytes, out);
    if (!e.code) return JZ_OK;
    jz_err fe = str_errf("decode cells: %.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
    jz_err_drop(&e);
    return fe;
}

static jz_err jazz_wasm__row_uuid_from_bytes(jz_slice bytes, jazz_types_RowUuid *out) {
    return into_str_err(jazz_db__binding_args__row_uuid(bytes, out));
}

static jz_err jazz_wasm__checked_js_u64(f64 value, const char *name, u64 *out) {
    return into_str_err(jazz_db__binding_args__checked_u64(value, jz_slice_of(name, (u32)strlen(name)), out));
}

static jz_err jazz_wasm__author_id_from_bytes(jz_slice bytes, jazz_types_AuthorSubject *out) {
    return into_str_err(jazz_db__binding_args__author_subject(bytes, out));
}

static jz_err jazz_wasm__claims_from_js(jazz_types_AuthorSubject author, jz_js claims, jz_btree *out) {
    jz_json *raw = NULL;
    JZ_TRY(to_js_err(swb__from_value(claims, &raw)));
    /* policy_claims takes the serde_json::Value by value */
    return into_str_err(jazz_db__binding_args__policy_claims(&author, raw, out));
}

static jz_err jazz_wasm__read_tier_from_str(jz_slice tier, u32 *out) { return into_str_err(jazz_db__binding_args__read_tier(tier, out)); }

static jz_err parse_open_transaction_id(const jz_str *s, jazz_model_OpenTransactionId *out) {
    return into_str_err(jazz_model__model__transaction__OpenTransactionId__FromStr__from_str(jz_str_slice(s), out));
}

static jz_slice opt_bytes_slice(const u8 *p, u32 n) { return jz_slice_of(p, n); }

/* ---- write options ---- */
static jz_err jazz_wasm__write_option(jz_js options, const char *name, bool *some, jz_js *out) {
    *some = false;
    if (wbg_is_nullish(options)) return JZ_OK;
    jz_js value = 0;
    JZ_TRY(reflect_get(options, name, &value));
    if (wbg_is_nullish(value)) {
        jz_js_drop(value);
        return JZ_OK;
    }
    *some = true;
    *out = value;
    return JZ_OK;
}

static jz_err jazz_wasm__has_write_option(jz_js options, const char *name, bool *out) {
    *out = false;
    if (wbg_is_nullish(options)) return JZ_OK;
    jz_js key = wbg_str(name);
    u32 has = 0;
    jz_js thrown = 0;
    u32 threw = wbg_reflect_has(options, key, &has, &thrown);
    jz_js_drop(key);
    if (threw) return wbg_err_from_js(thrown);
    *out = has != 0;
    return JZ_OK;
}

/* author_id_from_bytes(&js_sys::Uint8Array::new(&value).to_vec()) */
static jz_err author_from_js_bytes(jz_js value, jazz_types_AuthorSubject *out) {
    jz_bytes bytes = {0};
    wbg_uint8array_to_vec(value, &bytes);
    jz_err e = jazz_wasm__author_id_from_bytes(jz_bytes_slice(&bytes), out);
    jz_bytes_drop(&bytes);
    return e;
}

static jz_err jazz_wasm__write_identity_option(jz_js options, jazz_db_WriteIdentity *out) {
    bool has_author = false, has_attribution = false;
    jz_js author = 0, attribution = 0;
    JZ_TRY(jazz_wasm__write_option(options, "author", &has_author, &author));
    jz_err e = jazz_wasm__write_option(options, "attribution", &has_attribution, &attribution);
    if (e.code) {
        if (has_author) jz_js_drop(author);
        return e;
    }
    if (has_author && has_attribution) {
        e = str_err("write identity cannot contain both author and attribution");
    } else if (has_author) {
        out->tag = JAZZ_WID_SESSION;
        e = author_from_js_bytes(author, &out->author);
    } else if (has_attribution) {
        out->tag = JAZZ_WID_ATTRIBUTION;
        e = author_from_js_bytes(attribution, &out->author);
    } else {
        out->tag = JAZZ_WID_DATABASE;
    }
    if (has_author) jz_js_drop(author);
    if (has_attribution) jz_js_drop(attribution);
    return e;
}

static jz_err jazz_wasm__write_timestamp_option(jz_js options, bool *some, u64 *out) {
    bool has = false;
    jz_js value = 0;
    *some = false;
    JZ_TRY(jazz_wasm__write_option(options, "updatedAtMs", &has, &value));
    if (!has) return JZ_OK;
    f64 number = 0;
    bool is_number = wbg_as_f64(value, &number) != 0;
    jz_js_drop(value);
    if (!is_number) return str_err("updatedAtMs must be a number");
    JZ_TRY(jazz_wasm__checked_js_u64(number, "updatedAtMs", out));
    *some = true;
    return JZ_OK;
}

static void ExactWriteTarget__drop(jazz_db_ExactWriteTarget *t) {
    if (t->tag == JAZZ_EWT_BRANCH) jazz_model__model__branch__BranchSelector__drop(&t->branch);
    t->tag = JAZZ_EWT_ROOT;
}

static void WriteTarget__drop(jazz_db_WriteTarget *t) {
    if (t->tag == JAZZ_WT_BRANCH_VIEW) {
        jazz_model__model__branch__BranchSelector__drop(&t->head);
        if (t->has_base) jazz_model__model__branch__BranchViewBase__drop(&t->base);
    }
    t->tag = JAZZ_WT_ROOT;
    t->has_base = false;
}

/* write_option(&options, "branch")?.map(from_value).map(ExactWriteTarget::Branch).unwrap_or_default() */
static jz_err exact_write_target_option(jz_js options, jazz_db_ExactWriteTarget *out) {
    memset(out, 0, sizeof *out);
    out->tag = JAZZ_EWT_ROOT;
    bool has = false;
    jz_js branch = 0;
    JZ_TRY(jazz_wasm__write_option(options, "branch", &has, &branch));
    if (!has) return JZ_OK;
    jz_err e = to_js_err(swb__from_value_as_BranchSelector(branch, &out->branch));
    jz_js_drop(branch);
    JZ_TRY(e);
    out->tag = JAZZ_EWT_BRANCH;
    return JZ_OK;
}

static jz_err jazz_wasm__insert_options_from_js(jz_js options, jazz_db_InsertOptions *out) {
    memset(out, 0, sizeof *out);
    bool has_row = false;
    jz_js row_id = 0;
    JZ_TRY(jazz_wasm__write_option(options, "rowId", &has_row, &row_id));
    if (has_row) {
        jz_bytes bytes = {0};
        wbg_uint8array_to_vec(row_id, &bytes);
        jz_js_drop(row_id);
        jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&bytes), &out->row_id);
        jz_bytes_drop(&bytes);
        JZ_TRY(e);
        out->has_row_id = true;
    }
    JZ_TRY(jazz_wasm__write_identity_option(options, &out->identity));
    JZ_TRY(exact_write_target_option(options, &out->target));
    jz_err e = jazz_wasm__write_timestamp_option(options, &out->has_updated_at_ms, &out->updated_at_ms);
    if (e.code) ExactWriteTarget__drop(&out->target);
    return e;
}

/* BranchView { head: from_value(head)?, base: base.map(from_value).transpose()? } */
static jz_err branch_view_target(jz_js head, bool has_base, jz_js base, jazz_db_WriteTarget *out) {
    out->tag = JAZZ_WT_BRANCH_VIEW;
    JZ_TRY(to_js_err(swb__from_value_as_BranchSelector(head, &out->head)));
    if (has_base) {
        jz_err e = to_js_err(swb__from_value_as_BranchViewBase(base, &out->base));
        if (e.code) {
            jazz_model__model__branch__BranchSelector__drop(&out->head);
            out->tag = JAZZ_WT_ROOT;
            return e;
        }
        out->has_base = true;
    }
    return JZ_OK;
}

static jz_err jazz_wasm__update_options_from_js(jz_js options, jazz_db_UpdateOptions *out) {
    memset(out, 0, sizeof *out);
    bool has_head = false, has_base = false;
    jz_js head = 0, base = 0;
    JZ_TRY(jazz_wasm__write_option(options, "head", &has_head, &head));
    jz_err e = jazz_wasm__write_option(options, "base", &has_base, &base);
    if (!e.code) {
        if (has_head) e = branch_view_target(head, has_base, base, &out->target);
        else if (!has_base) out->target.tag = JAZZ_WT_ROOT; /* Default::default() */
        else e = str_err("branch view base requires a head selector");
    }
    if (has_head) jz_js_drop(head);
    if (has_base) jz_js_drop(base);
    JZ_TRY(e);
    if ((e = jazz_wasm__write_identity_option(options, &out->identity)).code ||
        (e = jazz_wasm__write_timestamp_option(options, &out->has_updated_at_ms, &out->updated_at_ms)).code) {
        WriteTarget__drop(&out->target);
        return e;
    }
    return JZ_OK;
}

static jz_err jazz_wasm__upsert_options_from_js(jz_js options, jazz_db_UpsertOptions *out) {
    memset(out, 0, sizeof *out);
    bool has_branch = false;
    JZ_TRY(jazz_wasm__has_write_option(options, "branch", &has_branch));
    if (has_branch) return str_err("upsert option `branch` is not supported; use `head` (and optional `base`) for a branch view");
    bool has_head = false, has_base = false;
    jz_js head = 0, base = 0;
    JZ_TRY(jazz_wasm__write_option(options, "head", &has_head, &head));
    jz_err e = jazz_wasm__write_option(options, "base", &has_base, &base);
    if (!e.code) {
        if (has_head) e = branch_view_target(head, has_base, base, &out->target);
        else if (!has_base) out->target.tag = JAZZ_WT_ROOT;
        else e = str_err("branch view base requires a head selector");
    }
    if (has_head) jz_js_drop(head);
    if (has_base) jz_js_drop(base);
    JZ_TRY(e);
    if ((e = jazz_wasm__write_identity_option(options, &out->identity)).code ||
        (e = jazz_wasm__write_timestamp_option(options, &out->has_updated_at_ms, &out->updated_at_ms)).code) {
        WriteTarget__drop(&out->target);
        return e;
    }
    return JZ_OK;
}

static jz_err jazz_wasm__delete_options_from_js(jz_js options, jazz_db_DeleteOptions *out) {
    jazz_db_UpdateOptions options_ = {0};
    JZ_TRY(jazz_wasm__update_options_from_js(options, &options_));
    out->identity = options_.identity;
    out->target = options_.target;
    out->has_updated_at_ms = options_.has_updated_at_ms;
    out->updated_at_ms = options_.updated_at_ms;
    return JZ_OK;
}

static jz_err jazz_wasm__restore_options_from_js(jz_js options, jazz_db_RestoreOptions *out) {
    memset(out, 0, sizeof *out);
    JZ_TRY(jazz_wasm__write_identity_option(options, &out->identity));
    JZ_TRY(exact_write_target_option(options, &out->target));
    jz_err e = jazz_wasm__write_timestamp_option(options, &out->has_updated_at_ms, &out->updated_at_ms);
    if (e.code) ExactWriteTarget__drop(&out->target);
    return e;
}

/* ================================================================== */
/* open helpers                                                         */
/* ================================================================== */
static jz_err jazz_wasm__decode_open_args(jz_slice schema, jz_slice config, jazz_model_JazzSchema **schema_out, jazz_wasm_WasmOpenDbConfig *config_out) {
    JZ_TRY(jazz_wasm__decode_public_schema(schema, schema_out));
    jz_pc_r r = {config.ptr, config.ptr + config.len, 0};
    jz_err e = WasmOpenDbConfig__Deserialize__postcard(&r, config_out); /* postcard::from_bytes ignores trailing bytes */
    if (e.code) {
        jz_err fe = str_errf("decode open config: %.*s", e.msg ? (int)e.msg->len : 0, e.msg ? e.msg->ptr : "");
        jz_err_drop(&e);
        jazz_model__schema__JazzSchema__drop(*schema_out);
        *schema_out = NULL;
        return fe;
    }
    return JZ_OK;
}

typedef JZ_VEC(jz_str) jazz_wasm_StringVec;
static void string_vec_item_drop(void *p) { jz_str_drop((jz_str *)p); }

/* schema.column_families().iter().map(String::as_str).collect() */
static void column_family_refs(const jazz_model_JazzSchema *schema, jazz_wasm_StringVec *families, JZ_VEC(jz_slice) *refs) {
    memset(families, 0, sizeof *families);
    memset(refs, 0, sizeof *refs);
    jazz_model__schema__JazzSchema__column_families(schema, (void *)families);
    jz_vec_reserve(refs, families->len, sizeof(jz_slice));
    for (u32 i = 0; i < families->len; i++) {
        jz_slice s = jz_str_slice(&families->ptr[i]);
        jz_vec_push(refs, &s, sizeof s);
    }
}

static void column_family_refs_drop(jazz_wasm_StringVec *families, JZ_VEC(jz_slice) *refs) {
    jz_vec_drop(families, sizeof(jz_str), string_vec_item_drop);
    jz_vec_drop(refs, sizeof(jz_slice), NULL);
}

static groove_BoxedStorage memory_storage_for(const jazz_model_JazzSchema *schema) {
    jazz_wasm_StringVec families;
    JZ_VEC(jz_slice) refs;
    column_family_refs(schema, &families, (void *)&refs);
    groove_BoxedStorage storage = {0};
    jz_err e = groove__storage__memory__MemoryStorage__new(refs.ptr, refs.len, &storage);
    column_family_refs_drop(&families, (void *)&refs);
    if (e.code) jz_panic("valid memory storage families");
    return storage;
}

static jz_err jazz_wasm__configure_initial_sync_flush_cadence(jazz_db_Db *db, bool has_every, u32 every) {
    if (!has_every) return JZ_OK;
    if (every == 0) return JZ_OK; /* NonZeroUsize::new */
    return jazz_db__db__lifecycle__Db__set_initial_sync_flush_cadence(db, (usize)every);
}

static jazz_db_DbConfig *db_config_for(jazz_model_JazzSchema *schema, groove_BoxedStorage storage, jazz_db_DbIdentity identity,
                                       const jazz_wasm_WasmOpenDbConfig *config) {
    jazz_db_DbConfig *db_config = jazz_db__db__config__DbConfig__new(schema, storage, identity);
    if (config->has_row_id_seed)
        jazz_db__db__config__DbConfig__with_id_source(db_config, &jazz_db__db__config__SeededRowIdSource__RowIdSource__VTABLE,
                                                      jazz_db__db__config__SeededRowIdSource__new(config->row_id_seed));
    return db_config;
}

static jz_err jazz_wasm__open_db(jazz_model_JazzSchema *schema, groove_BoxedStorage storage, const jazz_wasm_WasmOpenDbConfig *config, jazz_db_Db **out) {
    jazz_db_DbConfig *db_config = db_config_for(schema, storage, WasmDbIdentity__into_DbIdentity(config->identity), config);
    jazz_db_Db *db = NULL;
    if (config->history_complete) JZ_TRY(jazz_db__db__lifecycle__Db__open_history_complete(db_config, &db));
    else JZ_TRY(jazz_db__db__lifecycle__Db__open(db_config, &db));
    jz_err e = jazz_wasm__configure_initial_sync_flush_cadence(db, config->has_initial_sync_flush_every, config->initial_sync_flush_every);
    if (e.code) {
        jazz_db__db__Db__rc_release(db);
        return e;
    }
    *out = db;
    return JZ_OK;
}

/* Browser page stores are opened only after the worker has admitted their
 * durable ownership marker; the relay capability is bound at construction. */
static jz_err jazz_wasm__open_scope_isolated_relay_db(jazz_model_JazzSchema *schema, groove_BoxedStorage storage, const jazz_wasm_WasmOpenDbConfig *config,
                                                      jz_str storage_owner, jazz_db_Db **out) {
    jazz_db_DbConfig *db_config = db_config_for(schema, storage, WasmDbIdentity__into_DbIdentity(config->identity), config);
    jazz_node_ClientRelayScope scope;
    jazz_node__node__relay_scope__ClientRelayScope__from_admitted_storage_owner(storage_owner, config->identity.author, &scope);
    jazz_db_Db *db = NULL;
    JZ_TRY(jazz_db__db__lifecycle__Db__open_scope_isolated_client_relay(db_config, scope, &db));
    jz_err e = jazz_wasm__configure_initial_sync_flush_cadence(db, config->has_initial_sync_flush_every, config->initial_sync_flush_every);
    if (e.code) {
        jazz_db__db__Db__rc_release(db);
        return e;
    }
    *out = db;
    return JZ_OK;
}

static jz_err jazz_wasm__open_backend_db(jazz_model_JazzSchema *schema, groove_BoxedStorage storage, const jazz_wasm_WasmOpenDbConfig *config,
                                         jazz_db_DbIdentity identity, jazz_db_Db **out) {
    jazz_db_DbConfig *db_config = db_config_for(schema, storage, identity, config);
    jazz_db_Db *db = NULL;
    if (config->history_complete) JZ_TRY(jazz_db__db__lifecycle__Db__open_history_complete_with_backend_attribution(db_config, &db));
    else JZ_TRY(jazz_db__db__lifecycle__Db__open_with_backend_attribution(db_config, &db));
    jz_err e = jazz_wasm__configure_initial_sync_flush_cadence(db, config->has_initial_sync_flush_every, config->initial_sync_flush_every);
    if (e.code) {
        jazz_db__db__Db__rc_release(db);
        return e;
    }
    *out = db;
    return JZ_OK;
}

static jz_err jazz_wasm__validate_untrusted_open_author_core(const jazz_wasm_WasmOpenDbConfig *config) {
    jazz_types_AuthorSubject parsed;
    return jazz_types__ids__AuthorSubject__from_untrusted_canonical(jazz_types__ids__AuthorSubject__canonical(&config->identity.author), &parsed);
}

static jz_err jazz_wasm__validate_untrusted_open_author(const jazz_wasm_WasmOpenDbConfig *config) {
    return display_str_err(jazz_wasm__validate_untrusted_open_author_core(config));
}

static jz_err jazz_wasm__backend_open_identity_core(const jazz_wasm_WasmOpenDbConfig *config, jazz_db_DbIdentity *out) {
    /* Validate every caller-controlled field at the ordinary fail-closed
     * ingress before this explicit backend ABI derives SYSTEM. */
    JZ_TRY(jazz_wasm__validate_untrusted_open_author_core(config));
    out->node = config->identity.node;
    out->author = jazz_types__ids__AuthorSubject__SYSTEM;
    return JZ_OK;
}

static jz_err jazz_wasm__backend_open_identity(const jazz_wasm_WasmOpenDbConfig *config, jazz_db_DbIdentity *out) {
    return display_str_err(jazz_wasm__backend_open_identity_core(config, out));
}

static jz_err jazz_wasm__verify_self_signed_runtime_author_at(jz_slice token, jz_slice app_id, jz_slice claimed_author, u64 now_seconds,
                                                              jazz_types_AuthorSubject *out) {
    return jazz_types__identity__verify_client_runtime_author_at(token, app_id, claimed_author, now_seconds, out);
}

static jz_err jazz_wasm__verify_self_signed_runtime_author_core(jz_slice token, jz_slice app_id, jz_slice claimed_author, jazz_types_AuthorSubject *out) {
    /* web_time::SystemTime::now().duration_since(UNIX_EPOCH): the browser clock */
    u64 now_seconds = jz_now_ms() / 1000;
    return jazz_wasm__verify_self_signed_runtime_author_at(token, app_id, claimed_author, now_seconds, out);
}

static jz_err jazz_wasm__verify_self_signed_runtime_author(const jz_str *token, const jz_str *app_id, const jz_str *claimed_author,
                                                           jazz_types_AuthorSubject *out) {
    return into_str_err(jazz_wasm__verify_self_signed_runtime_author_core(jz_str_slice(token), jz_str_slice(app_id), jz_str_slice(claimed_author), out));
}

static jz_err jazz_wasm__tick_connection(jazz_db_PeerConnection *connection, u32 *out) {
    if (!connection) {
        *out = 0;
        return JZ_OK;
    }
    jazz_db_DbTickStats stats;
    JZ_TRY(to_js_err(jazz_db__db__peer_connection__PeerConnection__tick(connection, &stats))); /* lock().await.tick().await */
    *out = (u32)stats.subscription_events;
    return JZ_OK;
}

/* ================================================================== */
/* read options                                                         */
/* ================================================================== */
static jz_err jazz_wasm__optional_string_prop(jz_js value, const char *name, bool *some, jz_str *out) {
    jz_js prop = 0;
    *some = false;
    JZ_TRY(reflect_get(value, name, &prop));
    if (wbg_is_nullish(prop)) {
        jz_js_drop(prop);
        return JZ_OK;
    }
    bool is_string = wbg_take_string(prop, out);
    jz_js_drop(prop);
    if (!is_string) return str_errf("%s must be a string", name);
    *some = true;
    return JZ_OK;
}

static jz_err jazz_wasm__optional_bool_prop(jz_js value, const char *name, bool *some, bool *out) {
    *some = false;
    if (wbg_is_nullish(value)) return JZ_OK;
    jz_js prop = 0;
    JZ_TRY(reflect_get(value, name, &prop));
    if (wbg_is_nullish(prop)) {
        jz_js_drop(prop);
        return JZ_OK;
    }
    u32 b = wbg_as_bool(prop);
    jz_js_drop(prop);
    if (b == 2) return str_errf("%s must be a boolean", name);
    *some = true;
    *out = b == 1;
    return JZ_OK;
}

static jz_err jazz_wasm__optional_wait_ms_prop(jz_js value, const char *name, bool *some, u64 *out) {
    jz_js prop = 0;
    *some = false;
    JZ_TRY(reflect_get(value, name, &prop));
    if (wbg_is_nullish(prop)) {
        jz_js_drop(prop);
        return JZ_OK;
    }
    f64 ms = 0;
    bool is_number = wbg_as_f64(prop, &ms) != 0;
    jz_js_drop(prop);
    if (is_number && __builtin_isfinite(ms) && ms >= 0.0) {
        *some = true;
        *out = (u64)__builtin_floor(ms);
        return JZ_OK;
    }
    return str_errf("%s must be a non-negative number of milliseconds", name);
}

/* A local-first read's server-wait timeout. `Remote` reads ignore it. */
static jazz_db_FirstLoad jazz_wasm__local_first_server_wait(const jazz_db_ReadOpts *opts, u64 timeout_ms) {
    if (opts->tier == JAZZ_READ_TIER_LOCAL_FIRST) {
        jazz_db_FirstLoad wait = {JAZZ_FIRST_LOAD_WAIT_FOR_REMOTE, timeout_ms};
        return wait;
    }
    return opts->first_load;
}

static jz_err jazz_wasm__read_opts_from_js(jz_js value, jazz_db_ReadOpts *opts) {
    jazz_db__db__ReadOpts__Default__default(opts);
    if (wbg_is_nullish(value)) return JZ_OK;
    static const char *const READ_VIEW_NAMES[] = {"read_view", "readView"};
    jz_err e = JZ_OK;
    for (u32 i = 0; i < 2; i++) {
        jz_js prop = 0;
        if ((e = reflect_get(value, READ_VIEW_NAMES[i], &prop)).code) goto fail;
        if (!wbg_is_nullish(prop)) {
            jazz_protocol_ReadViewSpec read_view;
            jz_err de = swb__from_value_as_ReadViewSpec(prop, &read_view);
            jz_js_drop(prop);
            if (de.code) {
                e = str_errf("invalid read_view: %.*s", de.msg ? (int)de.msg->len : 0, de.msg ? de.msg->ptr : "");
                jz_err_drop(&de);
                goto fail;
            }
            jazz_protocol__protocol__ReadViewSpec__drop(&opts->read_view);
            opts->read_view = read_view;
            break;
        }
        jz_js_drop(prop);
    }
    bool some = false;
    jz_str tier = {0};
    if ((e = jazz_wasm__optional_string_prop(value, "tier", &some, &tier)).code) goto fail;
    if (some) {
        e = jazz_wasm__read_tier_from_str(jz_str_slice(&tier), &opts->tier);
        jz_str_drop(&tier);
        if (e.code) goto fail;
    }
    bool include_deleted = false;
    if ((e = jazz_wasm__optional_bool_prop(value, "include_deleted", &some, &include_deleted)).code) goto fail;
    if (some) opts->include_deleted = include_deleted;
    u64 timeout_ms = 0;
    if ((e = jazz_wasm__optional_wait_ms_prop(value, "first_load_remote_wait_ms", &some, &timeout_ms)).code) goto fail;
    if (some) opts->first_load = jazz_wasm__local_first_server_wait(opts, timeout_ms);
    return JZ_OK;
fail:
    jazz_db__db__ReadOpts__drop(opts);
    return e;
}

/* ================================================================== */
/* row encoding                                                         */
/* ================================================================== */
static bool jazz_wasm__value_contains_indirect_scalar(const groove_Value *value) {
    if (value->tag == WB_GROOVE_V_LARGE) return true;
    if (value->tag == WB_GROOVE_V_NULLABLE && value->v.nullable) return jazz_wasm__value_contains_indirect_scalar(value->v.nullable);
    return false;
}

static jz_err jazz_wasm__encode_rows(jazz_node_CurrentRow *const *rows, u32 n, jz_bytes *out) {
    return jazz_db__binding_codec__encode_rows(rows, n, out);
}

static jz_err jazz_wasm__encode_synchronous_rows(jazz_node_CurrentRow *const *rows, u32 n, jz_bytes *out) {
    for (u32 i = 0; i < n; i++) {
        const groove_RecordDescriptor *descriptor = NULL;
        jz_slice raw = {0};
        jazz_node__node__CurrentRow__encoded_record(rows[i], &descriptor, &raw);
        groove_BorrowedRecord record = groove__records__RecordDescriptor__bind(descriptor, raw);
        groove_ValueVec values = {0};
        JZ_TRY(to_js_err(groove__records__BorrowedRecord__to_values(&record, &values)));
        bool indirect = false;
        for (u32 j = 0; j < values.len && !indirect; j++) indirect = jazz_wasm__value_contains_indirect_scalar(&values.ptr[j]);
        jz_vec_drop(&values, sizeof(groove_Value), groove__records__values__Value__drop);
        if (indirect)
            return str_err("synchronous WASM all/transaction reads cannot materialize a large value; use an async relation read or subscription instead");
    }
    return to_js_err(jazz_wasm__encode_rows(rows, n, out));
}

static jz_err jazz_wasm__encode_relation_snapshot(const jazz_node_RelationSnapshot *snapshot, jz_bytes *out) {
    return jazz_db__binding_codec__encode_relation_snapshot(snapshot, out);
}

static jz_err jazz_wasm__encode_subscription_delta(const jazz_db_SubscriptionEvent *event, jz_bytes *out) {
    return jazz_db__binding_codec__encode_subscription_delta(event->added.ptr, event->added.len, event->updated.ptr, event->updated.len,
                                                             event->removed.ptr, event->removed.len, out);
}

/* ================================================================== */
/* subscription streams                                                 */
/* ================================================================== */
#define INITIAL_SUBSCRIPTION_RETRY_MS 25u
#define MAX_SUBSCRIPTION_RETRY_MS 1000u
/* Browsers and Node clamp a `setTimeout` delay above signed i32::MAX down to
 * a near-immediate timer; keep each segment below that host ceiling. */
#define MAX_JS_TIMEOUT_MS 2147483647u

static u32 jazz_wasm__local_retry_delay_ms(u8 attempt) {
    u32 shift = attempt < 6 ? attempt : 6;
    u64 delay = (u64)INITIAL_SUBSCRIPTION_RETRY_MS * ((u64)1 << shift); /* saturating_mul cannot overflow here */
    if (delay > UINT32_MAX) delay = UINT32_MAX;
    return delay < MAX_SUBSCRIPTION_RETRY_MS ? (u32)delay : MAX_SUBSCRIPTION_RETRY_MS;
}

/* The peer's retry hint is a minimum, not a suggestion to cap. */
static u32 jazz_wasm__subscription_retry_delay_ms(u8 attempt, u32 peer_retry_after_ms) {
    u32 local = jazz_wasm__local_retry_delay_ms(attempt);
    return local > peer_retry_after_ms ? local : peer_retry_after_ms;
}

static void jazz_wasm__subscription_retry_timer_segments(u32 delay_ms, JZ_VEC(u32) *segments) {
    memset(segments, 0, sizeof *segments);
    while (delay_ms > 0) {
        u32 segment_ms = delay_ms < MAX_JS_TIMEOUT_MS ? delay_ms : MAX_JS_TIMEOUT_MS;
        jz_vec_push(segments, &segment_ms, sizeof segment_ms);
        delay_ms -= segment_ms;
    }
}

/* SubscriptionRetryTimerState { fired, waker } */
typedef struct jazz_wasm_SubscriptionRetryTimerState { bool fired; bool has_waker; jz_waker waker; } jazz_wasm_SubscriptionRetryTimerState;

/* One host timer whose lifetime is owned by the Rust future. */
typedef struct jazz_wasm_SubscriptionRetryTimer {
    jazz_wasm_SubscriptionRetryTimerState *state; /* Rc */
    jz_js timer_global;
    jz_js clear_timeout;
    jz_js timeout_handle;
    jz_js callback; /* Closure<dyn FnMut()> */
} jazz_wasm_SubscriptionRetryTimer;

static jz_js retry_timer_fired(void *ctx, jz_js arg) {
    (void)arg;
    jazz_wasm_SubscriptionRetryTimerState *s = (jazz_wasm_SubscriptionRetryTimerState *)ctx;
    s->fired = true;
    if (s->has_waker) {
        s->has_waker = false;
        jz_waker_wake(&s->waker);
    }
    return WBG_UNDEFINED;
}

static void retry_timer_state_release(void *ctx) { jz_rc_release(ctx); }

static jz_err SubscriptionRetryTimer__with_functions(u32 delay_ms, jz_js timer_global, jz_js set_timeout, jz_js clear_timeout,
                                                     jazz_wasm_SubscriptionRetryTimer *out) {
    jazz_wasm_SubscriptionRetryTimerState *state = (jazz_wasm_SubscriptionRetryTimerState *)jz_rc_new(sizeof *state, NULL);
    state->fired = false;
    state->has_waker = false;
    jz_js callback = wbg_closure_into_js(retry_timer_fired, jz_rc_clone(state), retry_timer_state_release, 0);
    jz_js delay = wbg_number_new((f64)delay_ms);
    jz_js timeout_handle = 0;
    u32 threw = wbg_call2(set_timeout, timer_global, callback, delay, &timeout_handle);
    jz_js_drop(delay);
    jz_js_drop(set_timeout);
    if (threw) {
        jz_js_drop(callback);
        jz_js_drop(clear_timeout);
        jz_js_drop(timer_global);
        jz_rc_release(state);
        return wbg_err_from_js(timeout_handle);
    }
    out->state = state;
    out->timer_global = timer_global;
    out->clear_timeout = clear_timeout;
    out->timeout_handle = timeout_handle;
    out->callback = callback;
    return JZ_OK;
}

static jz_err SubscriptionRetryTimer__new(u32 delay_ms, jazz_wasm_SubscriptionRetryTimer *out) {
    jz_js timer_global = wbg_global();
    jz_js set_timeout = 0, clear_timeout = 0;
    jz_err e = reflect_get(timer_global, "setTimeout", &set_timeout);
    if (!e.code) e = dyn_into_function(set_timeout);
    if (e.code) {
        jz_js_drop(timer_global);
        return e;
    }
    e = reflect_get(timer_global, "clearTimeout", &clear_timeout);
    if (!e.code) e = dyn_into_function(clear_timeout);
    if (e.code) {
        jz_js_drop(set_timeout);
        jz_js_drop(timer_global);
        return e;
    }
    return SubscriptionRetryTimer__with_functions(delay_ms, timer_global, set_timeout, clear_timeout, out);
}

/* <SubscriptionRetryTimer as Future>::poll, driven to completion */
static void SubscriptionRetryTimer__Future__await(jazz_wasm_SubscriptionRetryTimer *t) {
    for (;;) {
        if (t->state->fired) return;
        t->state->waker = NOOP_WAKER;
        t->state->has_waker = true;
        if (t->state->fired) return;
        wbg_yield(); /* Poll::Pending: the host timer callback wakes us */
    }
}

static void SubscriptionRetryTimer__Drop__drop(jazz_wasm_SubscriptionRetryTimer *t) {
    if (!t->state->fired) {
        jz_js ignored = 0;
        (void)wbg_call1(t->clear_timeout, t->timer_global, t->timeout_handle, &ignored);
        jz_js_drop(ignored);
    }
    jz_js_drop(t->callback);
    jz_js_drop(t->timeout_handle);
    jz_js_drop(t->clear_timeout);
    jz_js_drop(t->timer_global);
    jz_rc_release(t->state);
}

static jz_err jazz_wasm__wait_for_subscription_retry(u32 delay_ms) {
    JZ_VEC(u32) segments;
    jazz_wasm__subscription_retry_timer_segments(delay_ms, (void *)&segments);
    for (u32 i = 0; i < segments.len; i++) {
        jazz_wasm_SubscriptionRetryTimer timer;
        jz_err e = SubscriptionRetryTimer__new(segments.ptr[i], &timer);
        if (e.code) {
            jz_vec_drop(&segments, sizeof(u32), NULL);
            return e;
        }
        SubscriptionRetryTimer__Future__await(&timer);
        SubscriptionRetryTimer__Drop__drop(&timer);
    }
    jz_vec_drop(&segments, sizeof(u32), NULL);
    return JZ_OK;
}

/* ---- chunk encoding ---- */
static jz_js js_object_of(const char *const *names, const jz_js *values, u32 n, jz_err *e) {
    jz_js object = wbg_object_new();
    for (u32 i = 0; i < n; i++) {
        if (e->code) {
            jz_js_drop(values[i]);
            continue;
        }
        *e = jazz_wasm__set_prop(object, names[i], values[i]);
    }
    return object;
}

/* terminal operations with every key/payload as one Uint8Array (#3369);
 * the producer-only root descriptor is omitted */
static jz_err jazz_wasm__terminal_operations_to_js(const groove_TerminalOperation *operations, u32 n, jz_js *out) {
    jz_err e = JZ_OK;
    jz_js encoded = wbg_array_new_with_length(n);
    for (u32 index = 0; index < n && !e.code; index++) {
        const groove_TerminalOperation *operation = &operations[index];
        jz_js path = wbg_array_new_with_length(operation->path.len);
        for (u32 si = 0; si < operation->path.len && !e.code; si++) {
            const groove_TerminalPathSegment *segment = &operation->path.ptr[si];
            jz_js value;
            const char *name;
            if (segment->tag == GROOVE_TPATH_COLLECTION) {
                name = "Collection";
                value = wbg_string_new(segment->collection.ptr, segment->collection.len);
            } else {
                name = "Key";
                value = wbg_uint8array_from(segment->key.ptr, segment->key.len);
            }
            jz_js seg = js_object_of(&name, &value, 1, &e);
            wbg_array_set(path, si, seg);
        }
        const groove_TerminalEdit *edit = &operation->edit;
        jz_js inner = 0;
        const char *edit_name = "Insert";
        switch (edit->tag) {
        case GROOVE_TEDIT_INSERT: {
            static const char *const names[] = {"index", "key", "value"};
            jz_js values[] = {wbg_number_new((f64)edit->index), wbg_uint8array_from(edit->key.ptr, edit->key.len),
                              wbg_uint8array_from(edit->value.ptr, edit->value.len)};
            inner = js_object_of(names, values, 3, &e);
            edit_name = "Insert";
            break;
        }
        case GROOVE_TEDIT_UPDATE: {
            static const char *const names[] = {"key", "value"};
            jz_js values[] = {wbg_uint8array_from(edit->key.ptr, edit->key.len), wbg_uint8array_from(edit->value.ptr, edit->value.len)};
            inner = js_object_of(names, values, 2, &e);
            edit_name = "Update";
            break;
        }
        case GROOVE_TEDIT_REMOVE: {
            static const char *const names[] = {"key"};
            jz_js values[] = {wbg_uint8array_from(edit->key.ptr, edit->key.len)};
            inner = js_object_of(names, values, 1, &e);
            edit_name = "Remove";
            break;
        }
        default: {
            static const char *const names[] = {"key", "index"};
            jz_js values[] = {wbg_uint8array_from(edit->key.ptr, edit->key.len), wbg_number_new((f64)edit->index)};
            inner = js_object_of(names, values, 2, &e);
            edit_name = "Move";
            break;
        }
        }
        jz_js edit_obj = js_object_of(&edit_name, &inner, 1, &e);
        static const char *const op_names[] = {"root_key", "path", "edit"};
        jz_js op_values[] = {wbg_uint8array_from(operation->root_key.ptr, operation->root_key.len), path, edit_obj};
        jz_js op = js_object_of(op_names, op_values, 3, &e);
        wbg_array_set(encoded, index, op);
    }
    if (e.code) {
        jz_js_drop(encoded);
        return e;
    }
    *out = encoded;
    return JZ_OK;
}

static const char *const DURABILITY_TIER_DEBUG[] = {"None", "Local", "Global"};

static jz_err jazz_wasm__subscription_chunk_to_js(jazz_db_SubscriptionEvent *event, jz_js *out) {
    jz_js object = wbg_object_new();
    jz_err e = JZ_OK;
    switch (event->tag) {
    case JAZZ_SUB_EVENT_DELTA: {
        jz_bytes delta = {0};
        if ((e = to_js_err(jazz_wasm__encode_subscription_delta(event, &delta))).code) break;
        bool root_op = false;
        for (u32 i = 0; i < event->terminal_operations.len; i++)
            if (event->terminal_operations.ptr[i].path.len == 0) root_op = true;
        if (root_op) {
            jz_bytes_drop(&delta);
            e = str_err("native producer emitted a root terminal operation");
            break;
        }
        jz_js terminal = 0;
        if ((e = jazz_wasm__set_prop(object, "type", wbg_str("delta"))).code) break;
        if ((e = jazz_wasm__set_prop(object, "delta", bytes_to_js(&delta))).code) break;
        if ((e = jazz_wasm__terminal_operations_to_js(event->terminal_operations.ptr, event->terminal_operations.len, &terminal)).code) break;
        if ((e = jazz_wasm__set_prop(object, "terminalOperations", terminal)).code) break;
        if ((e = jazz_wasm__set_prop(object, "reset", wbg_bool(event->reset))).code) break;
        if ((e = jazz_wasm__set_prop(object, "settled", wbg_bool(event->settled))).code) break;
        e = jazz_wasm__set_prop(object, "tier", wbg_str(DURABILITY_TIER_DEBUG[event->tier]));
        break;
    }
    case JAZZ_SUB_EVENT_CLOSED:
        e = jazz_wasm__set_prop(object, "type", wbg_str("closed"));
        break;
    default: {
        jz_js reason_object = wbg_object_new();
        const jazz_protocol_SubscribeRejectReason *reason = &event->reason;
        switch (reason->tag) {
        case JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY:
            if ((e = jazz_wasm__set_prop(reason_object, "type", wbg_str("UnsupportedShapeCapability"))).code) break;
            e = jazz_wasm__set_prop(reason_object, "detail", wbg_string_new(reason->detail.ptr, reason->detail.len));
            break;
        case JAZZ_SRR_SHAPE_REGISTRATION_PENDING:
            /* Transient: awaiting catalogue admission, distinct from the
             * permanent unsupported-capability rejection. */
            e = jazz_wasm__set_prop(reason_object, "type", wbg_str("ShapeRegistrationPendingCatalogueAdmission"));
            break;
        case JAZZ_SRR_SERVER_FAILURE: {
            if ((e = jazz_wasm__set_prop(reason_object, "type", wbg_str("ServerFailure"))).code) break;
            jz_str code = {0};
            jazz_protocol__protocol__SubscribeServerFailureCode__Debug__fmt(reason->code, &code);
            e = jazz_wasm__set_prop(reason_object, "code", wbg_str_owned(&code));
            break;
        }
        default:
            if ((e = jazz_wasm__set_prop(reason_object, "type", wbg_str("InvalidAuthoritySourceClosure"))).code) break;
            e = jazz_wasm__set_prop(reason_object, "transition", wbg_string_new(reason->transition.ptr, reason->transition.len));
            break;
        }
        if (e.code) {
            jz_js_drop(reason_object);
            break;
        }
        if ((e = jazz_wasm__set_prop(object, "type", wbg_str("rejected"))).code) {
            jz_js_drop(reason_object);
            break;
        }
        e = jazz_wasm__set_prop(object, "reason", reason_object);
        break;
    }
    }
    jazz_db__db__SubscriptionEvent__drop(event);
    if (e.code) {
        jz_js_drop(object);
        return e;
    }
    *out = object;
    return JZ_OK;
}

/* stream::unfold((db, source), ...) over SubscriptionStream */
typedef struct jazz_wasm_SubscriptionUnfold { jazz_wasm_WasmDbInner db; jazz_db_SubscriptionStream *source; } jazz_wasm_SubscriptionUnfold;

static void SubscriptionUnfold__drop(jazz_wasm_SubscriptionUnfold *s) {
    if (!s) return;
    /* dropping SubscriptionStream runs its cleanup guard */
    jazz_db__db__SubscriptionStream__drop(s->source);
    WasmDbInner__drop(&s->db);
    jz_free(s);
}

/* Stream::poll_next -> Option<Result<JsValue, JsValue>>; false = None */
static bool SubscriptionUnfold__next(jazz_wasm_SubscriptionUnfold *s, jz_err *err, jz_js *chunk) {
    jazz_db_SubscriptionEvent event;
    if (!jazz_db__db__SubscriptionStream__Stream__next(s->source, &event)) return false;
    u8 retry_attempt = 0;
    for (;;) {
        u32 retry_after_ms = 0;
        jz_err h = jazz_db__db__reads__Db__hydrate_subscription_event_for_binding_outcome(s->db.db, &event, &retry_after_ms);
        if (!h.code) break;
        if (h.code == JAZZ_BINDING_RETRYABLE) {
            jz_err_drop(&h);
            /* Keep this event ahead of the source stream: wait for a real
             * delayed wake before retrying rather than spinning. */
            u32 delay_ms = jazz_wasm__subscription_retry_delay_ms(retry_attempt, retry_after_ms);
            jz_err w = jazz_wasm__wait_for_subscription_retry(delay_ms);
            if (w.code) {
                jazz_db__db__SubscriptionEvent__drop(&event);
                *err = w;
                return true;
            }
            if (retry_attempt < 255) retry_attempt++;
            continue;
        }
        /* Do not retain a fatal event. */
        jazz_db__db__SubscriptionEvent__drop(&event);
        *err = to_js_err(h);
        return true;
    }
    *err = jazz_wasm__subscription_chunk_to_js(&event, chunk);
    return true;
}

/* ---- ReadableStream from a Rust stream ---- */
typedef struct jazz_wasm_StreamSlot { bool some; jazz_wasm_SubscriptionUnfold *stream; } jazz_wasm_StreamSlot; /* Rc<RefCell<Option<..>>> */
typedef struct jazz_wasm_AbortHandle { bool aborted; } jazz_wasm_AbortHandle;
typedef struct jazz_wasm_AbortSlot { bool some; jazz_wasm_AbortHandle *handle; } jazz_wasm_AbortSlot; /* Rc<RefCell<Option<AbortHandle>>> */

static void stream_slot_drop(void *p) {
    jazz_wasm_StreamSlot *s = (jazz_wasm_StreamSlot *)p;
    if (s->some) SubscriptionUnfold__drop(s->stream);
}
static void abort_slot_drop(void *p) {
    jazz_wasm_AbortSlot *s = (jazz_wasm_AbortSlot *)p;
    if (s->some) jz_rc_release(s->handle);
}

typedef struct jazz_wasm_ReadableCaptures {
    jazz_wasm_StreamSlot *state;
    bool *cancelled;
    jazz_wasm_AbortSlot *active_abort;
} jazz_wasm_ReadableCaptures;

static jazz_wasm_ReadableCaptures *readable_captures_clone(const jazz_wasm_ReadableCaptures *c) {
    jazz_wasm_ReadableCaptures *n = JZ_NEW(jazz_wasm_ReadableCaptures);
    n->state = (jazz_wasm_StreamSlot *)jz_rc_clone(c->state);
    n->cancelled = (bool *)jz_rc_clone(c->cancelled);
    n->active_abort = (jazz_wasm_AbortSlot *)jz_rc_clone(c->active_abort);
    return n;
}
static void readable_captures_drop(void *p) {
    jazz_wasm_ReadableCaptures *c = (jazz_wasm_ReadableCaptures *)p;
    jz_rc_release(c->state);
    jz_rc_release(c->cancelled);
    jz_rc_release(c->active_abort);
    jz_free(c);
}

static jz_err jazz_wasm__call_controller_method(jz_js controller, const char *method, jz_js arg /* 0 = None */) {
    jz_js function = 0;
    JZ_TRY(reflect_get(controller, method, &function));
    JZ_TRY(dyn_into_function(function));
    jz_js result = 0;
    u32 threw = arg ? wbg_call1(function, controller, arg, &result) : wbg_call0(function, controller, &result);
    jz_js_drop(function);
    if (threw) return wbg_err_from_js(result);
    jz_js_drop(result);
    return JZ_OK;
}

typedef struct jazz_wasm_PullTask { jazz_wasm_ReadableCaptures *captures; jz_js controller; } jazz_wasm_PullTask;

static jz_err readable_pull_body(void *ctx, jz_js *out) {
    jazz_wasm_PullTask *t = (jazz_wasm_PullTask *)ctx;
    jazz_wasm_ReadableCaptures *c = t->captures;
    *out = WBG_UNDEFINED;
    if (*c->cancelled) return JZ_OK;
    if (!c->state->some) return str_err("subscription stream pull already in progress");
    jazz_wasm_SubscriptionUnfold *stream = c->state->stream;
    c->state->some = false;
    c->state->stream = NULL;
    jazz_wasm_AbortHandle *abort_handle = (jazz_wasm_AbortHandle *)jz_rc_new(sizeof(jazz_wasm_AbortHandle), NULL);
    abort_handle->aborted = false;
    if (c->active_abort->some) jz_rc_release(c->active_abort->handle);
    c->active_abort->some = true;
    c->active_abort->handle = (jazz_wasm_AbortHandle *)jz_rc_clone(abort_handle);
    jz_err item_err = JZ_OK;
    jz_js chunk = 0;
    bool has_next = SubscriptionUnfold__next(stream, &item_err, &chunk); /* Abortable::new(stream.next(), registration).await */
    bool aborted = abort_handle->aborted;
    jz_rc_release(abort_handle);
    if (c->active_abort->some) {
        jz_rc_release(c->active_abort->handle);
        c->active_abort->some = false;
    }
    if (*c->cancelled) {
        /* Do not restore the stream after cancellation: dropping it runs the
         * subscription cleanup guard even during a retry wait. */
        if (has_next && item_err.code) jz_err_drop(&item_err);
        else if (has_next) jz_js_drop(chunk);
        SubscriptionUnfold__drop(stream);
        return JZ_OK;
    }
    if (aborted) {
        if (has_next && item_err.code) jz_err_drop(&item_err);
        else if (has_next) jz_js_drop(chunk);
        SubscriptionUnfold__drop(stream);
        return str_err("subscription pull was aborted");
    }
    if (has_next && !item_err.code) {
        c->state->some = true;
        c->state->stream = stream;
        jz_err e = jazz_wasm__call_controller_method(t->controller, "enqueue", chunk);
        jz_js_drop(chunk);
        JZ_TRY(e);
    } else if (has_next) {
        SubscriptionUnfold__drop(stream);
        jz_js error = jazz_wasm__to_js_error(item_err);
        JZ_TRY(jazz_wasm__call_controller_method(t->controller, "error", error));
        return wbg_err_from_js(error);
    } else {
        SubscriptionUnfold__drop(stream);
        JZ_TRY(jazz_wasm__call_controller_method(t->controller, "close", 0));
    }
    return JZ_OK;
}

static void pull_task_drop(void *ctx) {
    jazz_wasm_PullTask *t = (jazz_wasm_PullTask *)ctx;
    readable_captures_drop(t->captures);
    jz_js_drop(t->controller);
    jz_free(t);
}

static jz_js readable_pull(void *ctx, jz_js controller) {
    jazz_wasm_PullTask *t = JZ_NEW(jazz_wasm_PullTask);
    t->captures = readable_captures_clone((jazz_wasm_ReadableCaptures *)ctx);
    t->controller = wbg_clone_ref(controller);
    return jazz_wasm__future_to_promise(readable_pull_body, t, pull_task_drop);
}

static jz_js readable_cancel(void *ctx, jz_js arg) {
    (void)arg;
    jazz_wasm_ReadableCaptures *c = (jazz_wasm_ReadableCaptures *)ctx;
    *c->cancelled = true;
    if (c->active_abort->some) {
        c->active_abort->handle->aborted = true; /* handle.abort() */
        jz_rc_release(c->active_abort->handle);
        c->active_abort->some = false;
    }
    if (c->state->some) {
        c->state->some = false;
        SubscriptionUnfold__drop(c->state->stream);
        c->state->stream = NULL;
    }
    return WBG_UNDEFINED;
}

static jz_err jazz_wasm__readable_stream_from_stream(jazz_wasm_SubscriptionUnfold *stream, jz_js *out) {
    jazz_wasm_ReadableCaptures captures;
    captures.state = (jazz_wasm_StreamSlot *)jz_rc_new(sizeof(jazz_wasm_StreamSlot), stream_slot_drop);
    captures.state->some = true;
    captures.state->stream = stream;
    captures.cancelled = (bool *)jz_rc_new(sizeof(bool), NULL);
    *captures.cancelled = false;
    captures.active_abort = (jazz_wasm_AbortSlot *)jz_rc_new(sizeof(jazz_wasm_AbortSlot), abort_slot_drop);
    captures.active_abort->some = false;
    jz_js source = wbg_object_new();
    jz_err e = JZ_OK;

    /* pull.forget(): the JS function owns the closure from here on */
    jz_js pull = wbg_closure_into_js(readable_pull, readable_captures_clone(&captures), readable_captures_drop, 1);
    if ((e = jazz_wasm__set_prop(source, "pull", pull)).code) goto done;
    jz_js cancel = wbg_closure_into_js(readable_cancel, readable_captures_clone(&captures), readable_captures_drop, 0);
    if ((e = jazz_wasm__set_prop(source, "cancel", cancel)).code) goto done;

    jz_js strategy = wbg_object_new();
    if ((e = jazz_wasm__set_prop(strategy, "highWaterMark", wbg_number_new(0.0))).code) {
        jz_js_drop(strategy);
        goto done;
    }
    jz_js args = wbg_array_new();
    wbg_array_push(args, source);
    wbg_array_push(args, strategy);
    jz_js_drop(strategy);
    jz_js global = wbg_global();
    jz_js constructor = 0;
    e = reflect_get(global, "ReadableStream", &constructor);
    jz_js_drop(global);
    if (!e.code) e = dyn_into_function(constructor);
    if (!e.code) {
        jz_js result = 0;
        if (wbg_reflect_construct(constructor, args, &result)) e = wbg_err_from_js(result);
        else *out = result;
        jz_js_drop(constructor);
    }
    jz_js_drop(args);
done:
    jz_js_drop(source);
    jz_rc_release(captures.state);
    jz_rc_release(captures.cancelled);
    jz_rc_release(captures.active_abort);
    return e;
}

static jz_err jazz_wasm__subscription_stream_to_js(jazz_wasm_WasmDbInner db, jazz_db_SubscriptionStream *stream, jz_js *out) {
    jazz_wasm_SubscriptionUnfold *state = JZ_NEW(jazz_wasm_SubscriptionUnfold);
    state->db = db;
    state->source = stream;
    return jazz_wasm__readable_stream_from_stream(state, out);
}

/* ================================================================== */
/* WasmTransport state (methods further below)                          */
/* ================================================================== */
typedef struct jazz_wasm_WasmTransportInner {
    jazz_db_Db *db;                     /* Rc<Db> */
    jazz_db_PeerConnection *connection; /* Option<Rc<LocalMutex<PeerConnection>>>, NULL = None */
} jazz_wasm_WasmTransportInner;

typedef struct jazz_wasm_WasmTransport {
    jazz_wasm_WasmTransportInner inner;
    jazz_wasm_WasmWireQueues *queues;
    jazz_db_PeerIoPump *auxiliary_pump;
    bool has_subscriber_identity;
    jazz_types_AuthorSubject subscriber_identity;
} jazz_wasm_WasmTransport;

static jazz_db_PeerIoPump *WasmTransportInner__auxiliary_pump(const jazz_wasm_WasmTransportInner *self) {
    if (!self->connection) jz_panic("new transport has a connection");
    return jazz_db__db__peer_connection__PeerConnection__io_pump(self->connection); /* lock().await.io_pump() */
}

static jazz_wasm_WasmTransportInner WasmTransportInner__clone(const jazz_wasm_WasmTransportInner *i) {
    jazz_wasm_WasmTransportInner c = {jazz_db__db__Db__rc_clone(i->db), i->connection ? (jazz_db_PeerConnection *)jz_rc_clone(i->connection) : NULL};
    return c;
}

static void WasmTransportInner__drop(jazz_wasm_WasmTransportInner *i) {
    if (i->connection) jazz_db__db__PeerConnection__rc_release(i->connection);
    i->connection = NULL;
    if (i->db) jazz_db__db__Db__rc_release(i->db);
    i->db = NULL;
}

static jz_js wasm_transport_into_js(jazz_wasm_WasmTransport *t) { return wbg_wrap_object(WBG_CLASS_WASMTRANSPORT, t); }

/* ================================================================== */
/* WasmDbInner operations                                               */
/* ================================================================== */
typedef struct jazz_wasm_AdviceTask { jazz_db_PermissionAdviceFuture *future; } jazz_wasm_AdviceTask;

static jz_err advice_body(void *ctx, jz_js *out) {
    jazz_wasm_AdviceTask *t = (jazz_wasm_AdviceTask *)ctx;
    u32 advice = jazz_db__db__PermissionAdviceFuture__Future__await(t->future);
    t->future = NULL;
    *out = wbg_str(jazz_wasm__advice_string(advice));
    return JZ_OK;
}

static void advice_task_drop(void *ctx) {
    jazz_wasm_AdviceTask *t = (jazz_wasm_AdviceTask *)ctx;
    if (t->future) jazz_db__db__PermissionAdviceFuture__drop(t->future);
    jz_free(t);
}

static jazz_wasm_WasmPermissionAdviceRequest *WasmDbInner__request_permission_advice(const jazz_wasm_WasmDbInner *self,
                                                                                     jazz_protocol_PermissionAdviceAction action) {
    jazz_db_PermissionAdviceFuture *future = jazz_db__db__lifecycle__Db__request_permission_advice(self->db, action);
    jazz_wasm_WasmPermissionAdviceRequest *req = JZ_NEW(jazz_wasm_WasmPermissionAdviceRequest);
    jazz_db__db__PermissionAdviceFuture__request_id(future, req->request_id);
    req->cancel_db = jazz_db__db__Db__rc_clone(self->db);
    jazz_wasm_AdviceTask *t = JZ_NEW(jazz_wasm_AdviceTask);
    t->future = future;
    req->promise = jazz_wasm__future_to_promise(advice_body, t, advice_task_drop);
    return req;
}

static jz_err WasmDbInner__register_schema_view(const jazz_wasm_WasmDbInner *self, jazz_model_JazzSchema *schema, jazz_wasm_WasmDbInner *out) {
    jazz_db_Db *db = NULL;
    JZ_TRY(to_js_err(jazz_db__db__lifecycle__Db__register_schema_view(self->db, schema, &db))); /* block_on */
    out->db = db;
    out->drives_mutations_inline = self->drives_mutations_inline;
    return JZ_OK;
}

static jz_err transaction_read_cancelled(void) {
    return jz_err_new(JAZZ_ERRCODE_PROTOCOL + 1, "transaction read owner operation was cancelled");
}

typedef struct jazz_wasm_CoverageClock { f64 coverage_budget_ms; bool has_started; f64 started; } jazz_wasm_CoverageClock;

static bool coverage_expired(void *ctx) {
    jazz_wasm_CoverageClock *c = (jazz_wasm_CoverageClock *)ctx;
    if (!c->has_started) {
        c->started = wbg_date_now();
        c->has_started = true;
    }
    return wbg_date_now() - c->started >= c->coverage_budget_ms;
}

static void release_coverage(void *ctx, jazz_db_QueryAttachment *attachment) {
    jazz_db__db__subscriptions__Db__detach_query((jazz_db_Db *)ctx, attachment);
}

typedef struct jazz_wasm_SerializedQuery {
    jazz_db_Db *owner;
    jz_bytes query;
    jazz_db_ReadOpts opts;
    bool has_open_tx;
    jazz_model_OpenTransactionId open_tx;
    jazz_db_RequestScope request_scope;
    bool has_author;
    jazz_types_AuthorSubject author;
    bool require_coverage;
    jazz_wasm_CoverageClock clock;
    jazz_db_SerializedReadResult *out;
} jazz_wasm_SerializedQuery;

static jz_err run_serialized_query(void *ctx) {
    jazz_wasm_SerializedQuery *q = (jazz_wasm_SerializedQuery *)ctx;
    return jazz_db__db__reads__Db__all_serialized_query(q->owner, jz_bytes_slice(&q->query), q->opts, q->has_open_tx, q->open_tx, q->request_scope,
                                                        q->has_author ? &q->author : NULL, q->require_coverage, coverage_expired, &q->clock,
                                                        release_coverage, q->owner, q->out);
}

static jz_err WasmDbInner__all_serialized_query(const jazz_wasm_WasmDbInner *self, jz_bytes query, jazz_db_ReadOpts opts, bool has_open_tx,
                                                jazz_model_OpenTransactionId open_tx, jazz_db_RequestScope request_scope, bool has_author,
                                                jazz_types_AuthorSubject author, bool require_coverage, f64 coverage_deadline_ms,
                                                jazz_db_SerializedReadResult *out) {
    /* The coverage budget runs from the first wait on the server (#3839). */
    f64 coverage_budget_ms = __builtin_isfinite(coverage_deadline_ms) ? coverage_deadline_ms - wbg_date_now() : coverage_deadline_ms;
    jazz_wasm_SerializedQuery q = {self->db, query, opts, has_open_tx, open_tx, request_scope, has_author, author, require_coverage,
                                   {coverage_budget_ms, false, 0}, out};
    jz_err e;
    if (has_open_tx) {
        bool cancelled = false;
        e = jazz_db__db__lifecycle__Db__enqueue_transaction_read(self->db, open_tx, run_serialized_query, &q, &cancelled);
        if (cancelled) {
            jz_err_drop(&e);
            e = transaction_read_cancelled();
        }
    } else {
        e = run_serialized_query(&q);
    }
    jz_bytes_drop(&q.query);
    return e;
}

static jz_err WasmDbInner__subscribe_serialized_query(const jazz_wasm_WasmDbInner *self, jz_slice query, jazz_db_ReadOpts opts,
                                                      jazz_db_RequestScope request_scope, jazz_db_SerializedSubscriptionAuthorization authorization,
                                                      jazz_db_SubscriptionStream **out) {
    return jazz_db__db__subscriptions__Db__subscribe_serialized_query(self->db, query, opts, request_scope, authorization, out);
}

static jz_err WasmDbInner__begin_exclusive(const jazz_wasm_WasmDbInner *self, jazz_model_OpenTransactionId id, const jazz_types_AuthorSubject *author,
                                           const jazz_types_AuthorSubject *attribution) {
    JZ_TRY(jazz_db__db__transactions__Db__enqueue_begin_exclusive(self->db, id, author, attribution));
    WasmDbInner__after_enqueue(self);
    return JZ_OK;
}

static jz_err WasmDbInner__begin_mergeable(const jazz_wasm_WasmDbInner *self, jazz_model_OpenTransactionId id, const jazz_types_AuthorSubject *author,
                                           const jazz_types_AuthorSubject *attribution) {
    JZ_TRY(jazz_db__db__transactions__Db__enqueue_begin_mergeable(self->db, id, author, attribution));
    WasmDbInner__after_enqueue(self);
    return JZ_OK;
}

static jz_err WasmDbInner__abandon_transaction(const jazz_wasm_WasmDbInner *self, jazz_model_OpenTransactionId tx_id) {
    jazz_db__db__transactions__Db__enqueue_abandon_transaction_handle(self->db, tx_id);
    WasmDbInner__after_enqueue(self);
    return JZ_OK;
}

static jz_err WasmDbInner__commit_exclusive(const jazz_wasm_WasmDbInner *self, jazz_model_OpenTransactionId id, jazz_wasm_WasmWrite **out) {
    jazz_db_WriteHandle *write = NULL;
    JZ_TRY(to_js_err(jazz_db__db__transactions__Db__enqueue_commit_exclusive_handle_at_ms(self->db, id, jazz_wasm__current_timestamp_inner(), &write)));
    WasmDbInner__after_enqueue(self);
    return jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(self->db), write, out);
}

static jz_err WasmDbInner__commit_mergeable(const jazz_wasm_WasmDbInner *self, jazz_model_OpenTransactionId id, jazz_wasm_WasmWrite **out) {
    jazz_db_WriteHandle *write = NULL;
    JZ_TRY(to_js_err(jazz_db__db__transactions__Db__enqueue_commit_mergeable_handle_at_ms(self->db, id, jazz_wasm__current_timestamp_inner(), &write)));
    WasmDbInner__after_enqueue(self);
    return jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(self->db), write, out);
}

static void WasmDbInner__set_identity_claims(const jazz_wasm_WasmDbInner *self, jazz_types_AuthorSubject author, jz_btree claims) {
    jazz_db__db__mutations__Db__set_identity_claims(self->db, author, claims);
}

static void tick_scheduler_rc_drop(void *p) { WasmTickScheduler__drop(p); }

static void WasmDbInner__set_tick_scheduler(const jazz_wasm_WasmDbInner *self, jz_js callback) {
    /* progress wakes are forwarded by a local task that calls back with "immediate" */
    jazz_wasm_WakeReceiver *progress = wake_receiver_new(wbg_clone_ref(callback), true);
    jazz_wasm_WasmTickScheduler *scheduler = (jazz_wasm_WasmTickScheduler *)jz_rc_new(sizeof *scheduler, tick_scheduler_rc_drop);
    scheduler->callback = callback;
    scheduler->progress = progress;
    jazz_db__db__lifecycle__Db__set_tick_scheduler(self->db, &WASM_TICK_SCHEDULER_VT, scheduler);
}

static jz_err WasmDbInner__tick(const jazz_wasm_WasmDbInner *self) { return jazz_db__db__lifecycle__Db__tick(self->db); }

/* ================================================================== */
/* WasmDb                                                               */
/* ================================================================== */
/* Rc<RefCell<Option<WasmDbInner>>>: the runtime leaves this slot at close
 * admission; every later binding call observes None. */
typedef struct jazz_wasm_DbSlot { bool some; jazz_wasm_WasmDbInner inner; } jazz_wasm_DbSlot;
static void db_slot_drop(void *p) {
    jazz_wasm_DbSlot *s = (jazz_wasm_DbSlot *)p;
    if (s->some) WasmDbInner__drop(&s->inner);
}

typedef struct jazz_wasm_WasmDb {
    jazz_wasm_DbSlot *inner;
    bool owns_runtime;
    bool *non_durable_client; /* Rc<Cell<bool>> */
    /* set only by the explicit backend-open ABI */
    bool trusted_backend;
} jazz_wasm_WasmDb;

static jz_err WasmDb__open_inner(const jazz_wasm_WasmDb *self, jazz_wasm_WasmDbInner *out) {
    if (!self->inner->some) return str_err("WasmDb is closed");
    *out = WasmDbInner__clone(&self->inner->inner);
    return JZ_OK;
}

JZ_EXPORT("wasmdb_wireFeatures") u32 jazz_wasm__WasmDb__wire_features(jazz_wasm_WasmDb *self) {
    (void)self;
    return (u32)jazz_protocol__wire__current_wire_features();
}

static jz_err WasmDb__require_trusted_backend(const jazz_wasm_WasmDb *self) {
    if (self->trusted_backend) return JZ_OK;
    return str_err("backend attribution requires an explicit backend runtime");
}

/* read_author(Option<Vec<u8>>) -> Result<Option<AuthorSubject>> */
static jz_err WasmDb__read_author(const jazz_wasm_WasmDb *self, const u8 *author_ptr, u32 author_len, bool *some, jazz_types_AuthorSubject *out) {
    if (author_ptr) {
        JZ_TRY(jazz_wasm__author_id_from_bytes(opt_bytes_slice(author_ptr, author_len), out));
        *some = true;
    } else if (self->trusted_backend) {
        *out = jazz_types__ids__AuthorSubject__SYSTEM;
        *some = true;
    } else {
        *some = false;
    }
    return JZ_OK;
}

static jazz_wasm_WasmWrite *write_or_null(jz_err e, jazz_wasm_WasmWrite *w) { return e.code ? NULL : w; }

/* ---- single writes ---- */
JZ_EXPORT("wasmdb_insert")
void jazz_wasm__WasmDb__insert_with_options(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *cells_ptr, u32 cells_len,
                                            jz_js options) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes cells_raw = arg_bytes(cells_ptr, cells_len);
    jz_btree cells = {0};
    jazz_db_InsertOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmWrite *w = NULL;
    jz_err e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &cells);
    jz_bytes_drop(&cells_raw);
    if (!e.code && (e = jazz_wasm__insert_options_from_js(options, &opts)).code) jz_btree_drop(&cells);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&cells);
        ExactWriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        jazz_db_WriteHandle *write = NULL;
        e = to_js_err(jazz_db__db__mutations__Db__enqueue_insert(inner.db, table, cells, opts, &write));
        table.ptr = NULL;
        if (!e.code) {
            WasmDbInner__after_enqueue(&inner);
            e = jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(inner.db), write, &w);
        }
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_ptr(ret, e, write_or_null(e, w));
}

JZ_EXPORT("wasmdb_insertInTransaction")
void jazz_wasm__WasmDb__insert_in_transaction(wbg_ret_str *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *table_ptr, u32 table_len,
                                              u8 *cells_ptr, u32 cells_len, jz_js options) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes cells_raw = arg_bytes(cells_ptr, cells_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jz_btree cells = {0};
    jazz_db_InsertOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jz_bytes row_bytes = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    jz_str_drop(&tx_s);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &cells);
    jz_bytes_drop(&cells_raw);
    if (!e.code && (e = jazz_wasm__insert_options_from_js(options, &opts)).code) jz_btree_drop(&cells);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&cells);
        ExactWriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        jazz_types_RowUuid row;
        e = to_js_err(jazz_db__db__transactions__Db__enqueue_transaction_insert(inner.db, open_transaction_id, table, cells, opts, &row));
        table.ptr = NULL;
        if (!e.code) {
            WasmDbInner__after_enqueue(&inner);
            row_bytes = jz_bytes_from(row.b, 16);
        }
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_bytes(ret, e, row_bytes);
}

JZ_EXPORT("wasmdb_update")
void jazz_wasm__WasmDb__update_with_options(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len,
                                            u8 *patch_ptr, u32 patch_len, jz_js options) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), patch_raw = arg_bytes(patch_ptr, patch_len);
    jazz_types_RowUuid row_id;
    jz_btree patch = {0};
    jazz_db_UpdateOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmWrite *w = NULL;
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&patch_raw), &patch);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&patch_raw);
    if (!e.code && (e = jazz_wasm__update_options_from_js(options, &opts)).code) jz_btree_drop(&patch);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&patch);
        WriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        jazz_db_WriteHandle *write = NULL;
        e = to_js_err(jazz_db__db__mutations__Db__enqueue_update(inner.db, table, row_id, patch, opts, &write));
        table.ptr = NULL;
        if (!e.code) {
            WasmDbInner__after_enqueue(&inner);
            e = jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(inner.db), write, &w);
        }
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_ptr(ret, e, write_or_null(e, w));
}

JZ_EXPORT("wasmdb_updateInTransaction")
void jazz_wasm__WasmDb__update_in_transaction(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *table_ptr, u32 table_len,
                                              u8 *row_ptr, u32 row_len, u8 *patch_ptr, u32 patch_len, jz_js options) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), patch_raw = arg_bytes(patch_ptr, patch_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_types_RowUuid row_id;
    jz_btree patch = {0};
    jazz_db_UpdateOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    jz_str_drop(&tx_s);
    if (!e.code) e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&patch_raw), &patch);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&patch_raw);
    if (!e.code && (e = jazz_wasm__update_options_from_js(options, &opts)).code) jz_btree_drop(&patch);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&patch);
        WriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        e = to_js_err(jazz_db__db__transactions__Db__enqueue_transaction_update(inner.db, open_transaction_id, table, row_id, patch, opts));
        table.ptr = NULL;
        if (!e.code) WasmDbInner__after_enqueue(&inner);
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_unit(ret, e);
}

/* mutations: Vec<LargeValueUpdate> = serde_wasm_bindgen::from_value(mutations) */
static jz_err large_value_mutations_from_js(jz_js mutations, jazz_db_LargeValueUpdateVec *out) {
    jz_err de = swb__from_value_as_LargeValueUpdateVec(mutations, out);
    if (!de.code) return JZ_OK;
    jz_err e = str_errf("invalid partial-value update descriptor: %.*s", de.msg ? (int)de.msg->len : 0, de.msg ? de.msg->ptr : "");
    jz_err_drop(&de);
    return e;
}

JZ_EXPORT("wasmdb_updateLargeValuesInTransaction")
void jazz_wasm__WasmDb__update_large_values_in_transaction(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *table_ptr,
                                                           u32 table_len, u8 *row_ptr, u32 row_len, u8 *patch_ptr, u32 patch_len, jz_js mutations,
                                                           u32 has_updated_at_ms, f64 updated_at_ms) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), patch_raw = arg_bytes(patch_ptr, patch_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_types_RowUuid row_id;
    jz_btree patch = {0};
    jazz_db_LargeValueUpdateVec updates = {0};
    u64 updated = 0;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    jz_str_drop(&tx_s);
    if (!e.code) e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&patch_raw), &patch);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&patch_raw);
    if (!e.code && (e = large_value_mutations_from_js(mutations, &updates)).code) jz_btree_drop(&patch);
    if (!e.code && has_updated_at_ms && (e = jazz_wasm__checked_js_u64(updated_at_ms, "updatedAtMs", &updated)).code) {
        jz_btree_drop(&patch);
        swb__LargeValueUpdateVec__drop(&updates);
    }
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&patch);
        swb__LargeValueUpdateVec__drop(&updates);
    }
    if (!e.code) {
        e = to_js_err(jazz_db__db__transactions__Db__enqueue_transaction_large_value_update(inner.db, open_transaction_id, table, row_id, patch, updates,
                                                                                            has_updated_at_ms != 0, updated));
        table.ptr = NULL;
        if (!e.code) WasmDbInner__after_enqueue(&inner);
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(mutations);
    ret_unit(ret, e);
}

JZ_EXPORT("wasmdb_updateLargeValues")
void jazz_wasm__WasmDb__update_large_values(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len,
                                            u8 *patch_ptr, u32 patch_len, jz_js mutations, u32 has_updated_at_ms, f64 updated_at_ms) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), patch_raw = arg_bytes(patch_ptr, patch_len);
    jazz_types_RowUuid row_id;
    jz_btree patch = {0};
    jazz_db_LargeValueUpdateVec updates = {0};
    u64 updated = 0;
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmWrite *w = NULL;
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&patch_raw), &patch);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&patch_raw);
    if (!e.code && (e = large_value_mutations_from_js(mutations, &updates)).code) jz_btree_drop(&patch);
    if (!e.code && has_updated_at_ms && (e = jazz_wasm__checked_js_u64(updated_at_ms, "updatedAtMs", &updated)).code) {
        jz_btree_drop(&patch);
        swb__LargeValueUpdateVec__drop(&updates);
    }
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&patch);
        swb__LargeValueUpdateVec__drop(&updates);
    }
    if (!e.code) {
        jazz_db_WriteHandle *write = NULL;
        e = to_js_err(jazz_db__db__mutations__Db__enqueue_large_value_update(inner.db, table, row_id, patch, updates, has_updated_at_ms != 0, updated, &write));
        table.ptr = NULL;
        if (!e.code) {
            WasmDbInner__after_enqueue(&inner);
            e = jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(inner.db), write, &w);
        }
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(mutations);
    ret_ptr(ret, e, write_or_null(e, w));
}

JZ_EXPORT("wasmdb_upsert")
void jazz_wasm__WasmDb__upsert_with_options(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len,
                                            u8 *cells_ptr, u32 cells_len, jz_js options) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), cells_raw = arg_bytes(cells_ptr, cells_len);
    jazz_types_RowUuid row_id;
    jz_btree cells = {0};
    jazz_db_UpsertOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmWrite *w = NULL;
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &cells);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&cells_raw);
    if (!e.code && (e = jazz_wasm__upsert_options_from_js(options, &opts)).code) jz_btree_drop(&cells);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&cells);
        WriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        jazz_db_WriteHandle *write = NULL;
        e = to_js_err(jazz_db__db__mutations__Db__enqueue_upsert(inner.db, table, row_id, cells, opts, &write));
        table.ptr = NULL;
        if (!e.code) {
            WasmDbInner__after_enqueue(&inner);
            e = jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(inner.db), write, &w);
        }
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_ptr(ret, e, write_or_null(e, w));
}

JZ_EXPORT("wasmdb_upsertInTransaction")
void jazz_wasm__WasmDb__upsert_in_transaction(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *table_ptr, u32 table_len,
                                              u8 *row_ptr, u32 row_len, u8 *cells_ptr, u32 cells_len, jz_js options) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), cells_raw = arg_bytes(cells_ptr, cells_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_types_RowUuid row_id;
    jz_btree cells = {0};
    jazz_db_UpsertOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    jz_str_drop(&tx_s);
    if (!e.code) e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &cells);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&cells_raw);
    if (!e.code && (e = jazz_wasm__upsert_options_from_js(options, &opts)).code) jz_btree_drop(&cells);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&cells);
        WriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        e = to_js_err(jazz_db__db__transactions__Db__enqueue_transaction_upsert(inner.db, open_transaction_id, table, row_id, cells, opts));
        table.ptr = NULL;
        if (!e.code) WasmDbInner__after_enqueue(&inner);
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_unit(ret, e);
}

JZ_EXPORT("wasmdb_delete")
void jazz_wasm__WasmDb__delete_with_options(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len,
                                            jz_js options) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len);
    jazz_types_RowUuid row_id;
    jazz_db_DeleteOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmWrite *w = NULL;
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    jz_bytes_drop(&row_raw);
    if (!e.code) e = jazz_wasm__delete_options_from_js(options, &opts);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) WriteTarget__drop(&opts.target);
    if (!e.code) {
        jazz_db_WriteHandle *write = NULL;
        e = to_js_err(jazz_db__db__mutations__Db__enqueue_delete(inner.db, table, row_id, opts, &write));
        table.ptr = NULL;
        if (!e.code) {
            WasmDbInner__after_enqueue(&inner);
            e = jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(inner.db), write, &w);
        }
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_ptr(ret, e, write_or_null(e, w));
}

JZ_EXPORT("wasmdb_deleteInTransaction")
void jazz_wasm__WasmDb__delete_in_transaction(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *table_ptr, u32 table_len,
                                              u8 *row_ptr, u32 row_len, jz_js options) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_types_RowUuid row_id;
    jazz_db_DeleteOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    jz_str_drop(&tx_s);
    if (!e.code) e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    jz_bytes_drop(&row_raw);
    if (!e.code) e = jazz_wasm__delete_options_from_js(options, &opts);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) WriteTarget__drop(&opts.target);
    if (!e.code) {
        e = to_js_err(jazz_db__db__transactions__Db__enqueue_transaction_delete(inner.db, open_transaction_id, table, row_id, opts));
        table.ptr = NULL;
        if (!e.code) WasmDbInner__after_enqueue(&inner);
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_unit(ret, e);
}

JZ_EXPORT("wasmdb_restore")
void jazz_wasm__WasmDb__restore_with_options(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len,
                                             u8 *cells_ptr, u32 cells_len, jz_js options) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), cells_raw = arg_bytes(cells_ptr, cells_len);
    jazz_types_RowUuid row_id;
    jz_btree cells = {0};
    jazz_db_RestoreOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmWrite *w = NULL;
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &cells);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&cells_raw);
    if (!e.code && (e = jazz_wasm__restore_options_from_js(options, &opts)).code) jz_btree_drop(&cells);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&cells);
        ExactWriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        jazz_db_WriteHandle *write = NULL;
        e = to_js_err(jazz_db__db__mutations__Db__enqueue_restore(inner.db, table, row_id, true, cells, opts, &write));
        table.ptr = NULL;
        if (!e.code) {
            WasmDbInner__after_enqueue(&inner);
            e = jazz_wasm__wasm_write(jazz_db__db__Db__rc_clone(inner.db), write, &w);
        }
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_ptr(ret, e, write_or_null(e, w));
}

JZ_EXPORT("wasmdb_restoreInTransaction")
void jazz_wasm__WasmDb__restore_in_transaction(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *table_ptr, u32 table_len,
                                               u8 *row_ptr, u32 row_len, u8 *cells_ptr, u32 cells_len, jz_js options) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), cells_raw = arg_bytes(cells_ptr, cells_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_types_RowUuid row_id;
    jz_btree cells = {0};
    jazz_db_RestoreOptions opts;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    jz_str_drop(&tx_s);
    if (!e.code) e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &cells);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&cells_raw);
    if (!e.code && (e = jazz_wasm__restore_options_from_js(options, &opts)).code) jz_btree_drop(&cells);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) {
        jz_btree_drop(&cells);
        ExactWriteTarget__drop(&opts.target);
    }
    if (!e.code) {
        e = to_js_err(jazz_db__db__transactions__Db__enqueue_transaction_restore(inner.db, open_transaction_id, table, row_id, true, cells, opts));
        table.ptr = NULL;
        if (!e.code) WasmDbInner__after_enqueue(&inner);
        WasmDbInner__drop(&inner);
    }
    if (table.ptr) jz_str_drop(&table);
    jz_js_drop(options);
    ret_unit(ret, e);
}

/* ---- opening runtimes ---- */
/* A fresh root runtime owner over `inner`; every runtime this binding opens
 * defers local persistence to the host's flush cadence. */
static jazz_wasm_WasmDb *WasmDb__runtime_owner(jazz_wasm_WasmDbInner inner, bool trusted_backend) {
    jazz_db__db__lifecycle__Db__set_deferred_local_persistence(inner.db, true);
    jazz_wasm_WasmDb *db = JZ_NEW(jazz_wasm_WasmDb);
    db->inner = (jazz_wasm_DbSlot *)jz_rc_new(sizeof(jazz_wasm_DbSlot), db_slot_drop);
    db->inner->some = true;
    db->inner->inner = inner;
    db->owns_runtime = true;
    db->non_durable_client = (bool *)jz_rc_new(sizeof(bool), NULL);
    *db->non_durable_client = false;
    db->trusted_backend = trusted_backend;
    return db;
}

static jz_js wasm_db_into_js(jazz_wasm_WasmDb *db) { return wbg_wrap_object(WBG_CLASS_WASMDB, db); }

static jz_err WasmDb__open_browser_runtime(jz_js page_store, jazz_model_JazzSchema *schema, const jazz_wasm_WasmOpenDbConfig *config, jz_str storage_owner,
                                           jazz_wasm_WasmDb **out) {
    jazz_wasm_StringVec families;
    JZ_VEC(jz_slice) refs;
    column_family_refs(schema, &families, (void *)&refs);
    groove_BoxedStorage storage = {0};
    jz_err e = to_js_err(groove__storage__idb__IdbStorage__open(idb_tree__web__IndexedDbPageStore__from_js(page_store), refs.ptr, refs.len, &storage));
    column_family_refs_drop(&families, (void *)&refs);
    if (e.code) {
        jz_str_drop(&storage_owner);
        jazz_model__schema__JazzSchema__drop(schema);
        return e;
    }
    jazz_db_Db *db = NULL;
    JZ_TRY(to_js_err(jazz_wasm__open_scope_isolated_relay_db(schema, storage, config, storage_owner, &db)));
    e = to_js_err(jazz_db__db__lifecycle__Db__restore_browser_relay_pending_uploads(db));
    if (e.code) {
        jazz_db__db__Db__rc_release(db);
        return e;
    }
    *out = WasmDb__runtime_owner(WasmDbInner__browser(db), false);
    return JZ_OK;
}

JZ_EXPORT("wasmdb_openMemory") void jazz_wasm__WasmDb__open_memory(wbg_ret_ptr *ret, u8 *schema_ptr, u32 schema_len, u8 *config_ptr, u32 config_len) {
    jz_bytes schema_raw = arg_bytes(schema_ptr, schema_len), config_raw = arg_bytes(config_ptr, config_len);
    jazz_wasm__initialize_wasm_tracing();
    jazz_model_JazzSchema *schema = NULL;
    jazz_wasm_WasmOpenDbConfig config;
    jazz_wasm_WasmDb *out = NULL;
    jz_err e = jazz_wasm__decode_open_args(jz_bytes_slice(&schema_raw), jz_bytes_slice(&config_raw), &schema, &config);
    jz_bytes_drop(&schema_raw);
    jz_bytes_drop(&config_raw);
    if (!e.code && (e = jazz_wasm__validate_untrusted_open_author(&config)).code) jazz_model__schema__JazzSchema__drop(schema);
    if (!e.code) {
        groove_BoxedStorage storage = memory_storage_for(schema);
        jazz_db_Db *db = NULL;
        e = to_js_err(jazz_wasm__open_db(schema, storage, &config, &db)); /* block_on */
        if (!e.code) out = WasmDb__runtime_owner(WasmDbInner__memory(db), false);
    }
    ret_ptr(ret, e, out);
}

/* Open a deliberate backend runtime; a separate ABI from openMemory so raw
 * open configuration can never select the privileged system author. */
JZ_EXPORT("wasmdb_openMemoryAsBackend")
void jazz_wasm__WasmDb__open_memory_as_backend(wbg_ret_ptr *ret, u8 *schema_ptr, u32 schema_len, u8 *config_ptr, u32 config_len) {
    jz_bytes schema_raw = arg_bytes(schema_ptr, schema_len), config_raw = arg_bytes(config_ptr, config_len);
    jazz_wasm__initialize_wasm_tracing();
    jazz_model_JazzSchema *schema = NULL;
    jazz_wasm_WasmOpenDbConfig config;
    jazz_db_DbIdentity identity;
    jazz_wasm_WasmDb *out = NULL;
    jz_err e = jazz_wasm__decode_open_args(jz_bytes_slice(&schema_raw), jz_bytes_slice(&config_raw), &schema, &config);
    jz_bytes_drop(&schema_raw);
    jz_bytes_drop(&config_raw);
    if (!e.code && (e = jazz_wasm__backend_open_identity(&config, &identity)).code) jazz_model__schema__JazzSchema__drop(schema);
    if (!e.code) {
        groove_BoxedStorage storage = memory_storage_for(schema);
        jazz_db_Db *db = NULL;
        e = to_js_err(jazz_wasm__open_backend_db(schema, storage, &config, identity, &db));
        if (!e.code) out = WasmDb__runtime_owner(WasmDbInner__memory(db), true);
    }
    ret_ptr(ret, e, out);
}

/* Open with a verified Jazz self-signed client identity. */
JZ_EXPORT("wasmdb_openMemoryWithSelfSignedProof")
void jazz_wasm__WasmDb__open_memory_with_self_signed_proof(wbg_ret_ptr *ret, u8 *schema_ptr, u32 schema_len, u8 *config_ptr, u32 config_len,
                                                           char *token_ptr, u32 token_len, char *app_ptr, u32 app_len, char *claimed_ptr,
                                                           u32 claimed_len) {
    jz_bytes schema_raw = arg_bytes(schema_ptr, schema_len), config_raw = arg_bytes(config_ptr, config_len);
    jz_str token = arg_str(token_ptr, token_len), app_id = arg_str(app_ptr, app_len), claimed_author = arg_str(claimed_ptr, claimed_len);
    jazz_wasm__initialize_wasm_tracing();
    jazz_model_JazzSchema *schema = NULL;
    jazz_wasm_WasmOpenDbConfig config;
    jazz_wasm_WasmDb *out = NULL;
    jz_err e = jazz_wasm__decode_open_args(jz_bytes_slice(&schema_raw), jz_bytes_slice(&config_raw), &schema, &config);
    jz_bytes_drop(&schema_raw);
    jz_bytes_drop(&config_raw);
    if (!e.code && (e = jazz_wasm__verify_self_signed_runtime_author(&token, &app_id, &claimed_author, &config.identity.author)).code)
        jazz_model__schema__JazzSchema__drop(schema);
    if (!e.code) {
        groove_BoxedStorage storage = memory_storage_for(schema);
        jazz_db_Db *db = NULL;
        e = to_js_err(jazz_wasm__open_db(schema, storage, &config, &db));
        if (!e.code) out = WasmDb__runtime_owner(WasmDbInner__memory(db), false);
    }
    jz_str_drop(&token);
    jz_str_drop(&app_id);
    jz_str_drop(&claimed_author);
    ret_ptr(ret, e, out);
}

/* async fn openBrowser / openBrowserWithSelfSignedProof: the whole body runs
 * inside the returned promise */
typedef struct jazz_wasm_OpenBrowserTask {
    jz_js page_store;
    jz_bytes schema;
    jz_bytes config;
    jz_str storage_owner;
    bool with_proof;
    jz_str token, app_id, claimed_author;
} jazz_wasm_OpenBrowserTask;

static jz_err open_browser_body(void *ctx, jz_js *out) {
    jazz_wasm_OpenBrowserTask *t = (jazz_wasm_OpenBrowserTask *)ctx;
    jazz_wasm__initialize_wasm_tracing();
    jazz_model_JazzSchema *schema = NULL;
    jazz_wasm_WasmOpenDbConfig config;
    JZ_TRY(jazz_wasm__decode_open_args(jz_bytes_slice(&t->schema), jz_bytes_slice(&t->config), &schema, &config));
    jz_err e;
    if (t->with_proof) e = jazz_wasm__verify_self_signed_runtime_author(&t->token, &t->app_id, &t->claimed_author, &config.identity.author);
    else e = jazz_wasm__validate_untrusted_open_author(&config);
    if (e.code) {
        jazz_model__schema__JazzSchema__drop(schema);
        return e;
    }
    jazz_wasm_WasmDb *db = NULL;
    jz_str owner = t->storage_owner;
    memset(&t->storage_owner, 0, sizeof t->storage_owner);
    JZ_TRY(WasmDb__open_browser_runtime(t->page_store, schema, &config, owner, &db));
    t->page_store = 0;
    *out = wasm_db_into_js(db);
    return JZ_OK;
}

static void open_browser_task_drop(void *ctx) {
    jazz_wasm_OpenBrowserTask *t = (jazz_wasm_OpenBrowserTask *)ctx;
    if (t->page_store) jz_js_drop(t->page_store);
    jz_bytes_drop(&t->schema);
    jz_bytes_drop(&t->config);
    jz_str_drop(&t->storage_owner);
    jz_str_drop(&t->token);
    jz_str_drop(&t->app_id);
    jz_str_drop(&t->claimed_author);
    jz_free(t);
}

JZ_EXPORT("wasmdb_openBrowser")
jz_js jazz_wasm__WasmDb__open_browser(jz_js page_store, u8 *schema_ptr, u32 schema_len, u8 *config_ptr, u32 config_len, char *owner_ptr, u32 owner_len) {
    jazz_wasm_OpenBrowserTask *t = JZ_NEW(jazz_wasm_OpenBrowserTask);
    t->page_store = page_store;
    t->schema = arg_bytes(schema_ptr, schema_len);
    t->config = arg_bytes(config_ptr, config_len);
    t->storage_owner = arg_str(owner_ptr, owner_len);
    t->with_proof = false;
    return jazz_wasm__future_to_promise(open_browser_body, t, open_browser_task_drop);
}

JZ_EXPORT("wasmdb_openBrowserWithSelfSignedProof")
jz_js jazz_wasm__WasmDb__open_browser_with_self_signed_proof(jz_js page_store, u8 *schema_ptr, u32 schema_len, u8 *config_ptr, u32 config_len,
                                                             char *token_ptr, u32 token_len, char *app_ptr, u32 app_len, char *claimed_ptr,
                                                             u32 claimed_len, char *owner_ptr, u32 owner_len) {
    jazz_wasm_OpenBrowserTask *t = JZ_NEW(jazz_wasm_OpenBrowserTask);
    t->page_store = page_store;
    t->schema = arg_bytes(schema_ptr, schema_len);
    t->config = arg_bytes(config_ptr, config_len);
    t->storage_owner = arg_str(owner_ptr, owner_len);
    t->with_proof = true;
    t->token = arg_str(token_ptr, token_len);
    t->app_id = arg_str(app_ptr, app_len);
    t->claimed_author = arg_str(claimed_ptr, claimed_len);
    return jazz_wasm__future_to_promise(open_browser_body, t, open_browser_task_drop);
}

/* Register a typed schema view backed by this same runtime owner. */
JZ_EXPORT("wasmdb_registerSchema") void jazz_wasm__WasmDb__register_schema(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, u8 *schema_ptr, u32 schema_len) {
    jz_bytes schema_raw = arg_bytes(schema_ptr, schema_len);
    jazz_model_JazzSchema *schema = NULL;
    jazz_wasm_WasmDbInner inner = {0}, view = {0};
    jazz_wasm_WasmDb *out = NULL;
    jz_err e = jazz_wasm__decode_public_schema(jz_bytes_slice(&schema_raw), &schema);
    jz_bytes_drop(&schema_raw);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) jazz_model__schema__JazzSchema__drop(schema);
    if (!e.code) {
        e = WasmDbInner__register_schema_view(&inner, schema, &view);
        WasmDbInner__drop(&inner);
    }
    if (!e.code) {
        out = JZ_NEW(jazz_wasm_WasmDb);
        out->inner = (jazz_wasm_DbSlot *)jz_rc_new(sizeof(jazz_wasm_DbSlot), db_slot_drop);
        out->inner->some = true;
        out->inner->inner = view;
        out->owns_runtime = false;
        out->non_durable_client = (bool *)jz_rc_clone(self->non_durable_client);
        out->trusted_backend = self->trusted_backend;
    }
    ret_ptr(ret, e, out);
}

static jz_str jazz_wasm__unknown_transaction_kind_message(jz_slice kind) { return jz_format("unknown transaction kind %.*s", (int)kind.len, kind.ptr); }

static jz_err unknown_transaction_kind(jz_slice kind) {
    jz_str msg = jazz_wasm__unknown_transaction_kind_message(kind);
    jz_err e = jz_err_new(WBG_ERR_STR, NULL);
    e.msg = JZ_NEW(jz_str);
    *e.msg = msg;
    return e;
}

/* Begin one owner-wide transaction. */
JZ_EXPORT("wasmdb_beginTransaction")
void jazz_wasm__WasmDb__begin_transaction(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *kind_ptr, u32 kind_len, u8 *author_ptr,
                                          u32 author_len, u8 *attribution_ptr, u32 attribution_len) {
    jz_str tx_s = arg_str(tx_ptr, tx_len), kind = arg_str(kind_ptr, kind_len);
    jz_bytes author_raw = arg_bytes(author_ptr, author_len), attribution_raw = arg_bytes(attribution_ptr, attribution_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_types_AuthorSubject author, attribution;
    bool has_author = author_ptr != NULL, has_attribution = attribution_ptr != NULL;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    if (!e.code && has_author) e = jazz_wasm__author_id_from_bytes(jz_bytes_slice(&author_raw), &author);
    if (!e.code && has_attribution) e = jazz_wasm__author_id_from_bytes(jz_bytes_slice(&attribution_raw), &attribution);
    if (!e.code && has_author && has_attribution) e = str_err("transaction identity cannot contain both author and attribution");
    if (!e.code && has_attribution) e = WasmDb__require_trusted_backend(self);
    if (!e.code) e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        const jazz_types_AuthorSubject *a = has_author ? &author : NULL, *b = has_attribution ? &attribution : NULL;
        if (jz_str_eq_cstr(&kind, "mergeable")) e = to_js_err(WasmDbInner__begin_mergeable(&inner, open_transaction_id, a, b));
        else if (jz_str_eq_cstr(&kind, "exclusive")) e = to_js_err(WasmDbInner__begin_exclusive(&inner, open_transaction_id, a, b));
        else e = unknown_transaction_kind(jz_str_slice(&kind));
        WasmDbInner__drop(&inner);
    }
    jz_str_drop(&tx_s);
    jz_str_drop(&kind);
    if (has_author) jz_bytes_drop(&author_raw);
    if (has_attribution) jz_bytes_drop(&attribution_raw);
    ret_unit(ret, e);
}

/* Commit an owner-wide transaction by id. */
JZ_EXPORT("wasmdb_commitTransaction")
void jazz_wasm__WasmDb__commit_transaction(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len, char *kind_ptr, u32 kind_len) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_str kind_s = arg_str(kind_ptr, kind_len);
    jz_slice kind = kind_ptr ? jz_str_slice(&kind_s) : JZ_LIT("mergeable"); /* unwrap_or("mergeable") */
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmWrite *w = NULL;
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    if (!e.code) e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        if (kind.len == 9 && !memcmp(kind.ptr, "mergeable", 9)) e = WasmDbInner__commit_mergeable(&inner, open_transaction_id, &w);
        else if (kind.len == 9 && !memcmp(kind.ptr, "exclusive", 9)) e = WasmDbInner__commit_exclusive(&inner, open_transaction_id, &w);
        else e = unknown_transaction_kind(kind);
        WasmDbInner__drop(&inner);
    }
    jz_str_drop(&tx_s);
    if (kind_ptr) jz_str_drop(&kind_s);
    ret_ptr(ret, e, write_or_null(e, w));
}

/* Roll back an owner-wide open transaction by id. */
JZ_EXPORT("wasmdb_rollbackTransaction") void jazz_wasm__WasmDb__rollback_transaction(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *tx_ptr, u32 tx_len) {
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jazz_model_OpenTransactionId open_transaction_id;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = parse_open_transaction_id(&tx_s, &open_transaction_id);
    jz_str_drop(&tx_s);
    if (!e.code) e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        e = to_js_err(WasmDbInner__abandon_transaction(&inner, open_transaction_id));
        WasmDbInner__drop(&inner);
    }
    ret_unit(ret, e);
}

/* Exact local state for the write-merge bridge, matching NAPI. */
JZ_EXPORT("wasmdb_localCurrentRow")
void jazz_wasm__WasmDb__local_current_row(wbg_ret_str *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len);
    jazz_types_RowUuid row_id;
    jazz_wasm_WasmDbInner inner = {0};
    jz_bytes out = {0};
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &row_id);
    jz_bytes_drop(&row_raw);
    if (!e.code) e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        bool some = false;
        jazz_node_CurrentRow *row = NULL;
        e = to_js_err(jazz_db__db__mutations__Db__local_current_row(inner.db, jz_str_slice(&table), row_id, &some, &row)); /* block_on */
        if (!e.code) {
            e = jazz_wasm__encode_synchronous_rows(&row, some ? 1 : 0, &out);
            if (some) jazz_node__node__CurrentRow__drop(row);
        }
        WasmDbInner__drop(&inner);
    }
    jz_str_drop(&table);
    ret_bytes(ret, e, out);
}

/* ---- all(): a WasmReadFuture ---- */
typedef struct jazz_wasm_AllReadFuture {
    jazz_wasm_WasmDbInner inner;
    jz_bytes query;
    jazz_db_ReadOpts opts;
    bool has_open_tx;
    jazz_model_OpenTransactionId open_tx;
    jazz_db_RequestScope admission;
    bool has_author;
    jazz_types_AuthorSubject author;
    bool tier_is_explicit;
    bool non_durable_client;
    bool has_preceding_writes;
} jazz_wasm_AllReadFuture;

static bool all_read_poll(void *state, const jz_waker *waker, jz_err *result, void *out) {
    (void)waker;
    jazz_wasm_AllReadFuture *f = (jazz_wasm_AllReadFuture *)state;
    jz_bytes *bytes = (jz_bytes *)out;
    if (f->has_preceding_writes) {
        bool cancelled = false;
        jz_err barrier = jazz_db__db__lifecycle__Db__queued_mutation_barrier(f->inner.db, &cancelled);
        if (cancelled) {
            jz_err_drop(&barrier);
            *result = str_err("local write ordering barrier was cancelled");
            return true;
        }
        if (barrier.code) {
            *result = to_js_err(barrier);
            return true;
        }
    }
    bool requires_coverage = f->tier_is_explicit && (f->non_durable_client || f->opts.tier == JAZZ_READ_TIER_REMOTE);
    jazz_db_SerializedReadResult read;
    jz_bytes query = f->query;
    memset(&f->query, 0, sizeof f->query);
    jazz_db_ReadOpts opts = f->opts;
    memset(&f->opts, 0, sizeof f->opts);
    jz_err e = to_js_err(WasmDbInner__all_serialized_query(&f->inner, query, opts, f->has_open_tx, f->open_tx, f->admission, f->has_author, f->author,
                                                           requires_coverage, wbg_date_now() + 15000.0, &read));
    f->admission.some = false;
    if (e.code) {
        *result = e;
        return true;
    }
    if (read.tag == JAZZ_SRR_ROWS) e = jazz_wasm__encode_rows(read.rows.ptr, read.rows.len, bytes);
    else e = jazz_wasm__encode_relation_snapshot(read.relation, bytes);
    jazz_db__db__SerializedReadResult__drop(&read);
    *result = to_js_err(e);
    return true;
}

static void all_read_drop(void *state) {
    jazz_wasm_AllReadFuture *f = (jazz_wasm_AllReadFuture *)state;
    WasmDbInner__drop(&f->inner);
    jz_bytes_drop(&f->query);
    jazz_db__db__ReadOpts__drop(&f->opts); /* safe on a moved-out (zeroed) value */
    if (f->admission.some) jz_btree_drop(&f->admission.claims);
    jz_free(f);
}

static jz_js pending_native_read_into_js(jazz_wasm_WasmPendingNativeRead *r) { return wbg_wrap_object(WBG_CLASS_PENDINGNATIVEREAD, r); }

static jz_err jazz_wasm__wasm_read_or_pending(jazz_wasm_BoxFuture future, jz_js *out) {
    jazz_wasm_WasmPendingNativeRead *pending = WasmPendingNativeRead__new(future);
    bool some = false;
    jz_bytes bytes = {0};
    jz_err e = WasmPendingNativeRead__poll_once(pending, &some, &bytes);
    if (e.code || some) {
        jazz_wasm__WasmPendingNativeRead__free(pending);
        if (!e.code) *out = bytes_to_js(&bytes);
        return e;
    }
    *out = pending_native_read_into_js(pending);
    return JZ_OK;
}

/* admission = has_explicit_author.then(|| author.map(|a| (a, claims_from_js(a, claims)?))) */
static jz_err read_admission(bool has_explicit_author, bool has_author, jazz_types_AuthorSubject author, jz_js claims, jazz_db_RequestScope *out) {
    memset(out, 0, sizeof *out);
    if (!has_explicit_author || !has_author) return JZ_OK;
    JZ_TRY(jazz_wasm__claims_from_js(author, claims, &out->claims));
    out->some = true;
    out->author = author;
    return JZ_OK;
}

JZ_EXPORT("wasmdb_all")
void jazz_wasm__WasmDb__all(wbg_ret_val *ret, jazz_wasm_WasmDb *self, u8 *query_ptr, u32 query_len, jz_js opts_js, char *tx_ptr, u32 tx_len, u8 *author_ptr,
                            u32 author_len, jz_js claims) {
    jz_bytes query = arg_bytes(query_ptr, query_len);
    jz_str tx_s = arg_str(tx_ptr, tx_len);
    jz_bytes author_raw = arg_bytes(author_ptr, author_len);
    jazz_wasm_AllReadFuture *f = JZ_NEW(jazz_wasm_AllReadFuture);
    jz_js out = 0;
    jz_err e = WasmDb__open_inner(self, &f->inner);
    if (!e.code && !wbg_is_nullish(opts_js)) {
        bool some = false;
        jz_str tier = {0};
        e = jazz_wasm__optional_string_prop(opts_js, "tier", &some, &tier);
        if (some) jz_str_drop(&tier);
        f->tier_is_explicit = some;
    }
    bool opts_ok = false;
    if (!e.code) opts_ok = !(e = jazz_wasm__read_opts_from_js(opts_js, &f->opts)).code;
    bool has_explicit_author = author_ptr != NULL;
    if (!e.code) e = WasmDb__read_author(self, author_raw.ptr, author_raw.len, &f->has_author, &f->author);
    if (!e.code) e = read_admission(has_explicit_author, f->has_author, f->author, claims, &f->admission);
    if (!e.code && tx_ptr) {
        e = parse_open_transaction_id(&tx_s, &f->open_tx);
        f->has_open_tx = !e.code;
    }
    if (!e.code) {
        f->non_durable_client = *self->non_durable_client;
        f->has_preceding_writes = !f->has_open_tx;
        f->query = query;
        query.ptr = NULL;
        jazz_wasm_BoxFuture future = {all_read_poll, f, all_read_drop};
        f = NULL;
        e = jazz_wasm__wasm_read_or_pending(future, &out);
    }
    if (f) {
        if (!opts_ok) memset(&f->opts, 0, sizeof f->opts);
        all_read_drop(f);
    }
    if (query.ptr) jz_bytes_drop(&query);
    if (tx_ptr) jz_str_drop(&tx_s);
    if (author_ptr) jz_bytes_drop(&author_raw);
    jz_js_drop(opts_js);
    jz_js_drop(claims);
    ret_val(ret, e, out);
}

/* Bind correlation claims to this already-open client's own identity. */
JZ_EXPORT("wasmdb_setSessionClaims") void jazz_wasm__WasmDb__set_session_claims(wbg_ret_val *ret, jazz_wasm_WasmDb *self, jz_js claims_js) {
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        jazz_types_AuthorSubject identity = jazz_db__db__lifecycle__Db__identity(inner.db).author;
        jz_btree claims;
        e = jazz_wasm__claims_from_js(identity, claims_js, &claims);
        /* Preserve immediate subscription delivery when idle: the future is
         * polled once here and completes (the port's awaits are synchronous). */
        if (!e.code) jazz_db__db__mutations__Db__set_identity_claims_async(inner.db, identity, claims);
        WasmDbInner__drop(&inner);
    }
    jz_js_drop(claims_js);
    ret_val(ret, e, WBG_UNDEFINED);
}

JZ_EXPORT("wasmdb_setIdentityClaims")
void jazz_wasm__WasmDb__set_identity_claims(wbg_ret_val *ret, jazz_wasm_WasmDb *self, u8 *author_ptr, u32 author_len, jz_js claims_js) {
    jz_bytes author_raw = arg_bytes(author_ptr, author_len);
    jazz_types_AuthorSubject author;
    jz_btree claims = {0};
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = jazz_wasm__author_id_from_bytes(jz_bytes_slice(&author_raw), &author);
    jz_bytes_drop(&author_raw);
    if (!e.code) e = jazz_wasm__claims_from_js(author, claims_js, &claims);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) jz_btree_drop(&claims);
    if (!e.code) {
        WasmDbInner__set_identity_claims(&inner, author, claims);
        WasmDbInner__drop(&inner);
    }
    jz_js_drop(claims_js);
    ret_unit(ret, e);
}

/* ---- subscribe(): a WasmPendingSubscription ---- */
typedef struct jazz_wasm_SubscribeFuture {
    jazz_wasm_WasmDbInner db;
    jz_bytes query;
    jazz_db_ReadOpts opts;
    jazz_db_RequestScope admission;
    bool has_author;
    jazz_types_AuthorSubject author;
} jazz_wasm_SubscribeFuture;

static bool subscribe_poll(void *state, const jz_waker *waker, jz_err *result, void *out) {
    (void)waker;
    jazz_wasm_SubscribeFuture *f = (jazz_wasm_SubscribeFuture *)state;
    jazz_db_SerializedSubscriptionAuthorization authorization;
    if (f->has_author) {
        authorization.tag = JAZZ_SSA_TRUSTED_SERVING;
        authorization.author = f->author;
    } else {
        authorization.tag = JAZZ_SSA_CLIENT_LOCAL;
    }
    jazz_db_SubscriptionStream *stream = NULL;
    jazz_db_ReadOpts opts = f->opts;
    memset(&f->opts, 0, sizeof f->opts);
    jz_err e = to_js_err(WasmDbInner__subscribe_serialized_query(&f->db, jz_bytes_slice(&f->query), opts, f->admission, authorization, &stream));
    f->admission.some = false;
    if (e.code) {
        *result = e;
        return true;
    }
    jazz_wasm_WasmDbInner db = f->db;
    memset(&f->db, 0, sizeof f->db);
    *result = jazz_wasm__subscription_stream_to_js(db, stream, (jz_js *)out);
    return true;
}

static void subscribe_drop(void *state) {
    jazz_wasm_SubscribeFuture *f = (jazz_wasm_SubscribeFuture *)state;
    WasmDbInner__drop(&f->db);
    jz_bytes_drop(&f->query);
    jazz_db__db__ReadOpts__drop(&f->opts); /* safe on a moved-out (zeroed) value */
    if (f->admission.some) jz_btree_drop(&f->admission.claims);
    jz_free(f);
}

JZ_EXPORT("wasmdb_subscribe")
void jazz_wasm__WasmDb__subscribe(wbg_ret_val *ret, jazz_wasm_WasmDb *self, u8 *query_ptr, u32 query_len, jz_js opts_js, u8 *author_ptr, u32 author_len,
                                  jz_js claims) {
    jz_bytes query = arg_bytes(query_ptr, query_len);
    jz_bytes author_raw = arg_bytes(author_ptr, author_len);
    jazz_wasm_SubscribeFuture *f = JZ_NEW(jazz_wasm_SubscribeFuture);
    jz_js out = 0;
    jz_err e = jazz_wasm__read_opts_from_js(opts_js, &f->opts);
    bool opts_ok = !e.code;
    bool has_explicit_author = author_ptr != NULL;
    if (!e.code) e = WasmDb__read_author(self, author_raw.ptr, author_raw.len, &f->has_author, &f->author);
    if (!e.code) e = read_admission(has_explicit_author, f->has_author, f->author, claims, &f->admission);
    if (!e.code) e = WasmDb__open_inner(self, &f->db);
    if (!e.code) {
        f->query = query;
        query.ptr = NULL;
        jazz_wasm_WasmPendingSubscription *pending = JZ_NEW(jazz_wasm_WasmPendingSubscription);
        pending->future.some = true;
        pending->future.future = (jazz_wasm_BoxFuture){subscribe_poll, f, subscribe_drop};
        f = NULL;
        bool some = false;
        jz_js subscription = 0;
        e = WasmPendingSubscription__poll_once(pending, &some, &subscription);
        if (e.code || some) {
            jazz_wasm__WasmPendingSubscription__free(pending);
            out = subscription;
        } else {
            out = wbg_wrap_object(WBG_CLASS_WASMPENDINGSUBSCRIPTION, pending);
        }
    }
    if (f) {
        if (!opts_ok) memset(&f->opts, 0, sizeof f->opts);
        subscribe_drop(f);
    }
    if (query.ptr) jz_bytes_drop(&query);
    if (author_ptr) jz_bytes_drop(&author_raw);
    jz_js_drop(opts_js);
    jz_js_drop(claims);
    ret_val(ret, e, out);
}

JZ_EXPORT("wasmdb_setTickScheduler") void jazz_wasm__WasmDb__set_tick_scheduler(jazz_wasm_WasmDb *self, jz_js callback) {
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = WasmDb__open_inner(self, &inner);
    if (e.code) {
        jz_err_drop(&e);
        jz_js_drop(callback);
        return;
    }
    WasmDbInner__set_tick_scheduler(&inner, callback);
    WasmDbInner__drop(&inner);
}

/* ---- onMutationError ---- */
static void mutation_error_call(void *ctx, const jazz_db_MutationErrorEvent *event) {
    jz_js callback = (jz_js)(usize)ctx;
    jz_js value = 0;
    jz_err e = swb__MutationErrorEvent__serialize(event, &value);
    if (e.code) {
        jz_err_drop(&e);
        return;
    }
    jz_js ignored = 0;
    (void)wbg_call1(callback, WBG_UNDEFINED, value, &ignored);
    jz_js_drop(ignored);
    jz_js_drop(value);
}

static void mutation_error_drop(void *ctx) { jz_js_drop((jz_js)(usize)ctx); }
static void mutation_error_callback_rc_drop(void *p) {
    jazz_db_MutationErrorCallback *c = (jazz_db_MutationErrorCallback *)p;
    if (c->drop) c->drop(c->ctx);
}

/* Register a callback for rejected writes that no active wait consumed. */
JZ_EXPORT("wasmdb_onMutationError") void jazz_wasm__WasmDb__on_mutation_error(jazz_wasm_WasmDb *self, jz_js callback) {
    jazz_db_MutationErrorCallback *cb = (jazz_db_MutationErrorCallback *)jz_rc_new(sizeof *cb, mutation_error_callback_rc_drop);
    cb->call = mutation_error_call;
    cb->ctx = (void *)(usize)callback;
    cb->drop = mutation_error_drop;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = WasmDb__open_inner(self, &inner);
    if (e.code) {
        jz_err_drop(&e);
        jz_rc_release(cb);
        return;
    }
    jazz_db__db__lifecycle__Db__on_mutation_error(inner.db, (jazz_db_MutationErrorCallback *)jz_rc_clone(cb));
    jz_rc_release(cb);
    WasmDbInner__drop(&inner);
}

JZ_EXPORT("wasmdb_canInsert")
void jazz_wasm__WasmDb__can_insert(wbg_ret_str *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *cells_ptr, u32 cells_len) {
    jz_str table = arg_str(table_ptr, table_len);
    jz_bytes cells_raw = arg_bytes(cells_ptr, cells_len);
    jz_btree cells = {0};
    jazz_wasm_WasmDbInner inner = {0};
    jz_str out = {0};
    jz_err e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &cells);
    jz_bytes_drop(&cells_raw);
    if (!e.code && (e = WasmDb__open_inner(self, &inner)).code) jz_btree_drop(&cells);
    if (!e.code) {
        u32 advice = 0;
        e = to_js_err(jazz_db__db__mutations__Db__can_insert(inner.db, jz_str_slice(&table), cells, &advice));
        if (!e.code) out = jz_str_from(jazz_wasm__advice_string(advice));
        WasmDbInner__drop(&inner);
    }
    jz_str_drop(&table);
    ret_str(ret, e, out);
}

static void request_advice_export(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, jz_err e, jazz_protocol_PermissionAdviceAction action) {
    jazz_wasm_WasmDbInner inner = {0};
    jazz_wasm_WasmPermissionAdviceRequest *req = NULL;
    if (!e.code) e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        req = WasmDbInner__request_permission_advice(&inner, action);
        WasmDbInner__drop(&inner);
    } else {
        jz_str_drop(&action.table);
        if (action.tag == JAZZ_PAA_INSERT || action.tag == JAZZ_PAA_UPDATE) jz_btree_drop(&action.cells);
    }
    ret_ptr(ret, e, req);
}

JZ_EXPORT("wasmdb_requestInsertPermissionAdvice")
void jazz_wasm__WasmDb__request_insert_permission_advice(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *cells_ptr,
                                                         u32 cells_len) {
    jz_bytes cells_raw = arg_bytes(cells_ptr, cells_len);
    jazz_protocol_PermissionAdviceAction action = {0};
    action.tag = JAZZ_PAA_INSERT;
    action.table = arg_str(table_ptr, table_len);
    jz_err e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &action.cells);
    jz_bytes_drop(&cells_raw);
    if (e.code) {
        jz_str_drop(&action.table);
        ret_ptr(ret, e, NULL);
        return;
    }
    request_advice_export(ret, self, e, action);
}

JZ_EXPORT("wasmdb_requestReadPermissionAdvice")
void jazz_wasm__WasmDb__request_read_permission_advice(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len) {
    jz_bytes row_raw = arg_bytes(row_ptr, row_len);
    jazz_protocol_PermissionAdviceAction action = {0};
    action.tag = JAZZ_PAA_READ;
    action.table = arg_str(table_ptr, table_len);
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &action.row);
    jz_bytes_drop(&row_raw);
    if (e.code) {
        jz_str_drop(&action.table);
        ret_ptr(ret, e, NULL);
        return;
    }
    request_advice_export(ret, self, e, action);
}

JZ_EXPORT("wasmdb_requestUpdatePermissionAdvice")
void jazz_wasm__WasmDb__request_update_permission_advice(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len,
                                                         u8 *patch_ptr, u32 patch_len) {
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), patch_raw = arg_bytes(patch_ptr, patch_len);
    jazz_protocol_PermissionAdviceAction action = {0};
    action.tag = JAZZ_PAA_UPDATE;
    action.table = arg_str(table_ptr, table_len);
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &action.row);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&patch_raw), &action.cells);
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&patch_raw);
    if (e.code) {
        jz_str_drop(&action.table);
        ret_ptr(ret, e, NULL);
        return;
    }
    request_advice_export(ret, self, e, action);
}

JZ_EXPORT("wasmdb_requestDeletePermissionAdvice")
void jazz_wasm__WasmDb__request_delete_permission_advice(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len) {
    jz_bytes row_raw = arg_bytes(row_ptr, row_len);
    jazz_protocol_PermissionAdviceAction action = {0};
    action.tag = JAZZ_PAA_DELETE;
    action.table = arg_str(table_ptr, table_len);
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &action.row);
    jz_bytes_drop(&row_raw);
    if (e.code) {
        jz_str_drop(&action.table);
        ret_ptr(ret, e, NULL);
        return;
    }
    request_advice_export(ret, self, e, action);
}

/* ---- promise-returning maintenance calls ---- */
typedef struct jazz_wasm_InnerTask { jazz_wasm_WasmDbInner inner; jz_str tier; u64 arg; } jazz_wasm_InnerTask;

static void inner_task_drop(void *ctx) {
    jazz_wasm_InnerTask *t = (jazz_wasm_InnerTask *)ctx;
    WasmDbInner__drop(&t->inner);
    jz_str_drop(&t->tier);
    jz_free(t);
}

static jz_err wait_for_pending_writes_body(void *ctx, jz_js *out) {
    jazz_wasm_InnerTask *t = (jazz_wasm_InnerTask *)ctx;
    jazz_model_DurabilityTier tier = JAZZ_DUR_NONE;
    JZ_TRY(jazz_wasm__durability_tier_from_str(jz_str_slice(&t->tier), &tier));
    JZ_TRY(to_js_err(jazz_db__db__lifecycle__Db__wait_for_pending_writes(t->inner.db, tier)));
    *out = WBG_UNDEFINED;
    return JZ_OK;
}

static jz_js inner_promise(jazz_wasm_WasmDb *self, jazz_wasm_async_fn body, jz_str tier) {
    jazz_wasm_InnerTask *t = JZ_NEW(jazz_wasm_InnerTask);
    jz_err e = WasmDb__open_inner(self, &t->inner);
    t->tier = tier;
    if (e.code) {
        inner_task_drop(t);
        return promise_reject_err(e);
    }
    return jazz_wasm__future_to_promise(body, t, inner_task_drop);
}

JZ_EXPORT("wasmdb_waitForPendingWrites") jz_js jazz_wasm__WasmDb__wait_for_pending_writes(jazz_wasm_WasmDb *self, char *tier_ptr, u32 tier_len) {
    return inner_promise(self, wait_for_pending_writes_body, arg_str(tier_ptr, tier_len));
}

static jz_err tick_body(void *ctx, jz_js *out) {
    jazz_wasm_InnerTask *t = (jazz_wasm_InnerTask *)ctx;
    JZ_TRY(to_js_err(WasmDbInner__tick(&t->inner)));
    *out = WBG_UNDEFINED;
    return JZ_OK;
}

JZ_EXPORT("wasmdb_tick") jz_js jazz_wasm__WasmDb__tick(jazz_wasm_WasmDb *self) {
    jz_str none = {0};
    return inner_promise(self, tick_body, none);
}

/* Configure Jazz-owned upload ingress and unpublished-tree expiry limits. */
JZ_EXPORT("wasmdb_setLargeValueStagingPolicy")
void jazz_wasm__WasmDb__set_large_value_staging_policy(wbg_ret_val *ret, jazz_wasm_WasmDb *self, f64 incoming_bytes_per_window, f64 window_ms,
                                                       u32 has_max_age_ms, f64 max_age_ms) {
    jazz_node_LargeValueStagingPolicy policy;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = jazz_wasm__checked_js_u64(incoming_bytes_per_window, "incomingBytesPerWindow", &policy.incoming_bytes_per_window);
    if (!e.code) e = jazz_wasm__checked_js_u64(window_ms, "windowMs", &policy.window_ms);
    if (!e.code && policy.window_ms < 1) e = str_err("windowMs must be at least 1");
    if (!e.code) {
        if (has_max_age_ms) {
            e = jazz_wasm__checked_js_u64(max_age_ms, "maxAgeMs", &policy.max_age_ms);
        } else {
            jazz_node_LargeValueStagingPolicy defaults;
            jazz_node__node__LargeValueStagingPolicy__Default__default(&defaults);
            policy.max_age_ms = defaults.max_age_ms;
        }
    }
    if (!e.code) e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        jazz_db__db__lifecycle__Db__set_large_value_staging_policy(inner.db, policy);
        WasmDbInner__drop(&inner);
    }
    ret_unit(ret, e);
}

static jz_err evict_body(void *ctx, jz_js *out) {
    jazz_wasm_InnerTask *t = (jazz_wasm_InnerTask *)ctx;
    usize evicted = 0;
    JZ_TRY(to_js_err(jazz_db__db__lifecycle__Db__evict_expired_staged_large_values(t->inner.db, &evicted)));
    *out = wbg_number_new((f64)evicted);
    return JZ_OK;
}

/* Run one idempotent expiry pass; browser hosts normally call this from a timer. */
JZ_EXPORT("wasmdb_evictExpiredStagedLargeValues") jz_js jazz_wasm__WasmDb__evict_expired_staged_large_values(jazz_wasm_WasmDb *self) {
    jz_str none = {0};
    return inner_promise(self, evict_body, none);
}

/* ---- streaming mutations ---- */
static jz_err WasmDb__begin_streaming_mutation_inner(jazz_wasm_WasmDb *self, jz_str table, jz_bytes row_raw, jz_bytes cells_raw, jz_str column,
                                                     const jz_str *mutation_s, bool has_mutation, jazz_db_WriteIdentity identity,
                                                     bool has_updated_at_ms, f64 updated_at_ms, jz_js head, jz_js base,
                                                     jazz_wasm_WasmStreamingMutation **out) {
    jazz_wasm_WasmStreamingMutationState *state = JZ_NEW(jazz_wasm_WasmStreamingMutationState);
    state->table = table;
    state->column = column;
    state->identity = identity;
    jz_err e = jazz_wasm__row_uuid_from_bytes(jz_bytes_slice(&row_raw), &state->row_id);
    if (!e.code) e = jazz_wasm__decode_cells(jz_bytes_slice(&cells_raw), &state->cells);
    if (!e.code) {
        jz_slice m = has_mutation ? jz_str_slice(mutation_s) : JZ_LIT("insert");
        if (m.len == 6 && !memcmp(m.ptr, "insert", 6)) state->mutation = JAZZ_SMK_INSERT;
        else if (m.len == 6 && !memcmp(m.ptr, "update", 6)) state->mutation = JAZZ_SMK_UPDATE;
        else if (m.len == 6 && !memcmp(m.ptr, "upsert", 6)) state->mutation = JAZZ_SMK_UPSERT;
        else e = str_err("unknown streaming mutation kind");
    }
    if (!e.code && has_updated_at_ms) {
        e = jazz_wasm__checked_js_u64(updated_at_ms, "updatedAtMs", &state->updated_at_ms);
        state->has_updated_at_ms = !e.code;
    }
    if (!e.code && head && !wbg_is_nullish(head)) {
        e = to_js_err(swb__from_value_as_BranchSelector(head, &state->head));
        state->has_head = !e.code;
    }
    if (!e.code && base && !wbg_is_nullish(base)) {
        e = to_js_err(swb__from_value_as_BranchViewBase(base, &state->base));
        state->has_base = !e.code;
    }
    if (!e.code && state->has_base && !state->has_head) e = str_err("streaming mutation base requires a branch head");
    if (!e.code) e = WasmDb__open_inner(self, &state->db);
    if (!e.code)
        e = to_js_err(jazz_db__db__mutations__Db__begin_streaming_value_upload(state->db.db, jz_str_slice(&state->table), &state->cells,
                                                                              jz_str_slice(&state->column), &state->upload));
    jz_bytes_drop(&row_raw);
    jz_bytes_drop(&cells_raw);
    if (e.code) {
        StreamingMutationState__drop(state);
        return e;
    }
    jazz_wasm_WasmStreamingMutation *m = JZ_NEW(jazz_wasm_WasmStreamingMutation);
    m->state = (jazz_wasm_WasmStreamingMutationLifecycle *)jz_rc_new(sizeof(jazz_wasm_WasmStreamingMutationLifecycle), lifecycle_rc_drop);
    m->state->tag = SML_OPEN;
    m->state->open = state;
    m->state->completion = NULL;
    *out = m;
    return JZ_OK;
}

JZ_EXPORT("wasmdb_beginStreamingMutation")
void jazz_wasm__WasmDb__begin_streaming_mutation(wbg_ret_ptr *ret, jazz_wasm_WasmDb *self, char *table_ptr, u32 table_len, u8 *row_ptr, u32 row_len,
                                                 u8 *cells_ptr, u32 cells_len, char *column_ptr, u32 column_len, char *mutation_ptr, u32 mutation_len,
                                                 u8 *author_ptr, u32 author_len, u8 *attribution_ptr, u32 attribution_len, u32 has_updated_at_ms,
                                                 f64 updated_at_ms, jz_js head, jz_js base) {
    jz_str table = arg_str(table_ptr, table_len), column = arg_str(column_ptr, column_len), mutation = arg_str(mutation_ptr, mutation_len);
    jz_bytes row_raw = arg_bytes(row_ptr, row_len), cells_raw = arg_bytes(cells_ptr, cells_len);
    jz_bytes author_raw = arg_bytes(author_ptr, author_len), attribution_raw = arg_bytes(attribution_ptr, attribution_len);
    bool has_author = author_ptr != NULL, has_attribution = attribution_ptr != NULL;
    jazz_wasm_WasmStreamingMutation *out = NULL;
    jazz_db_WriteIdentity identity = {JAZZ_WID_DATABASE, {0}};
    jz_err e = JZ_OK;
    bool consumed = false;
    if (has_author && has_attribution) {
        e = str_err("streaming mutation identity cannot contain both author and attribution");
    } else if (has_attribution && ((head && !wbg_is_nullish(head)) || (base && !wbg_is_nullish(base)))) {
        e = str_err("backend-attributed streaming mutations do not support branch writes");
    } else if (has_attribution && (e = WasmDb__require_trusted_backend(self)).code) {
        /* error set */
    } else {
        if (has_author) {
            identity.tag = JAZZ_WID_SESSION;
            e = jazz_wasm__author_id_from_bytes(jz_bytes_slice(&author_raw), &identity.author);
        } else if (has_attribution) {
            identity.tag = JAZZ_WID_ATTRIBUTION;
            e = jazz_wasm__author_id_from_bytes(jz_bytes_slice(&attribution_raw), &identity.author);
        }
        if (!e.code) {
            consumed = true;
            e = WasmDb__begin_streaming_mutation_inner(self, table, row_raw, cells_raw, column, &mutation, mutation_ptr != NULL, identity,
                                                       has_updated_at_ms != 0, updated_at_ms, head, base, &out);
        }
    }
    if (!consumed) {
        jz_str_drop(&table);
        jz_str_drop(&column);
        jz_bytes_drop(&row_raw);
        jz_bytes_drop(&cells_raw);
    }
    if (mutation_ptr) jz_str_drop(&mutation);
    if (has_author) jz_bytes_drop(&author_raw);
    if (has_attribution) jz_bytes_drop(&attribution_raw);
    if (head) jz_js_drop(head);
    if (base) jz_js_drop(base);
    ret_ptr(ret, e, out);
}

/* Configure this runtime as the optimistic in-memory side of a browser
 * client/worker pair. */
JZ_EXPORT("wasmdb_setNonDurableClient") void jazz_wasm__WasmDb__set_non_durable_client(wbg_ret_val *ret, jazz_wasm_WasmDb *self) {
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        jazz_db__db__lifecycle__Db__set_non_durable_client(inner.db);
        *self->non_durable_client = true;
        WasmDbInner__drop(&inner);
    }
    ret_unit(ret, e);
}

/* Report what the host knows about the path to the authoritative server. */
JZ_EXPORT("wasmdb_setRemoteLinkHint") void jazz_wasm__WasmDb__set_remote_link_hint(wbg_ret_val *ret, jazz_wasm_WasmDb *self, char *state_ptr, u32 state_len) {
    jz_str state = arg_str(state_ptr, state_len);
    u32 hint = 0;
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = JZ_OK;
    if (!jazz_db__db__first_load__RemoteLinkHint__from_host_str(jz_str_slice(&state), &hint))
        e = str_errf("unknown remote link state %.*s", (int)state.len, state.ptr);
    if (!e.code) e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        jazz_db__db__first_load__Db__set_remote_link_hint(inner.db, hint);
        WasmDbInner__drop(&inner);
    }
    jz_str_drop(&state);
    ret_unit(ret, e);
}

static jz_err foreground_high_water_body(void *ctx, jz_js *out) {
    jazz_wasm_InnerTask *t = (jazz_wasm_InnerTask *)ctx;
    jazz_types_TxTime high_water = jazz_db__db__lifecycle__Db__foreground_tx_time_high_water(t->inner.db);
    *out = wbg_bigint_from_u64(high_water); /* JsValue::from(u64) is a BigInt */
    return JZ_OK;
}

/* Internal foreground-lease handoff boundary (BigInt values). The high-water
 * is read under the node lock, so it resolves on the event loop. */
JZ_EXPORT("wasmdb_foregroundTxTimeHighWater") void jazz_wasm__WasmDb__foreground_tx_time_high_water(wbg_ret_val *ret, jazz_wasm_WasmDb *self) {
    jazz_wasm_InnerTask *t = JZ_NEW(jazz_wasm_InnerTask);
    jz_err e = WasmDb__open_inner(self, &t->inner);
    if (e.code) {
        inner_task_drop(t);
        ret_val(ret, e, 0);
        return;
    }
    ret_val(ret, JZ_OK, jazz_wasm__future_to_promise(foreground_high_water_body, t, inner_task_drop));
}

JZ_EXPORT("wasmdb_seedForegroundTxTimeHighWater")
void jazz_wasm__WasmDb__seed_foreground_tx_time_high_water(wbg_ret_val *ret, jazz_wasm_WasmDb *self, u64 high_water) {
    jazz_wasm_WasmDbInner inner = {0};
    jz_err e = WasmDb__open_inner(self, &inner);
    if (!e.code) {
        jazz_db__db__lifecycle__Db__seed_foreground_tx_time_high_water(inner.db, (jazz_types_TxTime)high_water); /* block_on */
        WasmDbInner__drop(&inner);
    }
    ret_unit(ret, e);
}

/* ---- upstream / subscriber links ---- */
typedef struct jazz_wasm_ConnectTask { jazz_wasm_WasmDbInner db_inner; jazz_db_BoxTransport *transport; jazz_wasm_WasmWireQueues *queues; } jazz_wasm_ConnectTask;

static jz_err connect_upstream_body(void *ctx, jz_js *out) {
    jazz_wasm_ConnectTask *t = (jazz_wasm_ConnectTask *)ctx;
    jazz_wasm_WasmTransport *transport = JZ_NEW(jazz_wasm_WasmTransport);
    transport->inner.db = jazz_db__db__Db__rc_clone(t->db_inner.db);
    transport->inner.connection = jazz_db__db__lifecycle__Db__connect_upstream(t->db_inner.db, t->transport);
    t->transport = NULL;
    transport->auxiliary_pump = WasmTransportInner__auxiliary_pump(&transport->inner);
    transport->queues = t->queues;
    t->queues = NULL;
    transport->has_subscriber_identity = false;
    *out = wasm_transport_into_js(transport);
    return JZ_OK;
}

static void connect_task_drop(void *ctx) {
    jazz_wasm_ConnectTask *t = (jazz_wasm_ConnectTask *)ctx;
    WasmDbInner__drop(&t->db_inner);
    if (t->transport) jazz_db__db__BoxTransport__drop(t->transport);
    if (t->queues) jz_rc_release(t->queues);
    jz_free(t);
}

JZ_EXPORT("wasmdb_connectUpstream") void jazz_wasm__WasmDb__connect_upstream(wbg_ret_val *ret, jazz_wasm_WasmDb *self) {
    jazz_wasm_WasmWireQueues *queues = WasmWireQueues__Default__default();
    /* Browser WebSocket carriers negotiate ordinary sync only: no scoped
     * receipt/view features before the carrier can bind that context. */
    jazz_db_BoxTransport *transport = jazz_db__db__wire_transport__WireTransportAdapter__new(
        &WASM_WIRE_TRANSPORT_VT, WasmWireTransport__new(queues), JAZZ_WIRE_PROTOCOL_VERSION,
        jazz_protocol__wire__current_wire_features() & ~JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS, NULL);
    jazz_wasm_ConnectTask *t = JZ_NEW(jazz_wasm_ConnectTask);
    t->transport = transport;
    t->queues = queues;
    jz_err e = WasmDb__open_inner(self, &t->db_inner);
    if (e.code) {
        connect_task_drop(t);
        ret_val(ret, e, 0);
        return;
    }
    ret_val(ret, JZ_OK, jazz_wasm__future_to_promise(connect_upstream_body, t, connect_task_drop));
}

/* Connect after the browser carrier has accepted the server Hello. */
JZ_EXPORT("wasmdb_connectUpstreamWithSession")
void jazz_wasm__WasmDb__connect_upstream_with_session(wbg_ret_val *ret, jazz_wasm_WasmDb *self, u32 protocol_version, u32 features, u8 *remote_ptr,
                                                      u32 remote_len, u64 remote_epoch, u8 *local_ptr, u32 local_len, u64 local_epoch) {
    jz_bytes remote_node = arg_bytes(remote_ptr, remote_len), local_node = arg_bytes(local_ptr, local_len);
    jazz_wasm_ConnectTask *t = JZ_NEW(jazz_wasm_ConnectTask);
    jz_err e = WasmDb__open_inner(self, &t->db_inner);
    if (!e.code && remote_node.len != 16) e = str_err("server hello authority node must be 16 bytes");
    if (!e.code && local_node.len != 16) e = str_err("local peer identity must be 16 bytes");
    if (e.code) {
        jz_bytes_drop(&remote_node);
        jz_bytes_drop(&local_node);
        connect_task_drop(t);
        ret_val(ret, e, 0);
        return;
    }
    jazz_wasm_WasmWireQueues *queues = WasmWireQueues__Default__default();
    jazz_db_ConnectionSessionContext session_context;
    memcpy(session_context.local.node.b, local_node.ptr, 16);
    session_context.local.epoch = local_epoch;
    session_context.has_remote = true;
    memcpy(session_context.remote.node.b, remote_node.ptr, 16);
    session_context.remote.epoch = remote_epoch;
    session_context.link_identity = jazz_db__db__lifecycle__Db__identity(t->db_inner.db).author;
    session_context.negotiated_features = (u64)features;
    t->transport = jazz_db__db__wire_transport__WireTransportAdapter__new_with_session_context(
        &WASM_WIRE_TRANSPORT_VT, WasmWireTransport__new(queues), (u16)protocol_version, (u64)features, NULL, &session_context);
    t->queues = queues;
    jz_bytes_drop(&remote_node);
    jz_bytes_drop(&local_node);
    ret_val(ret, JZ_OK, jazz_wasm__future_to_promise(connect_upstream_body, t, connect_task_drop));
}

static jz_err WasmDb__accept_subscriber_with_admitted_identity(jazz_wasm_WasmDbInner *db_inner, jazz_types_AuthorSubject identity, jz_btree claims,
                                                               bool has_local_epoch, u64 local_epoch, jazz_wasm_WasmTransport **out) {
    jazz_wasm_WasmWireQueues *queues = WasmWireQueues__Default__default();
    jazz_db_DbIdentity owner = jazz_db__db__lifecycle__Db__identity(db_inner->db);
    jazz_db_ConnectionSessionContext context;
    if (has_local_epoch) {
        context.local.node = owner.node;
        context.local.epoch = local_epoch;
        context.has_remote = false;
        context.link_identity = identity;
        context.negotiated_features = jazz_protocol__wire__current_wire_features();
    }
    jazz_protocol_WireFeatures features = has_local_epoch ? jazz_protocol__wire__current_wire_features()
                                                          : jazz_protocol__wire__current_wire_features() & ~JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS;
    jazz_db_BoxTransport *transport = jazz_db__db__wire_transport__WireTransportAdapter__new_with_session_context(
        &WASM_WIRE_TRANSPORT_VT, WasmWireTransport__new(queues), JAZZ_WIRE_PROTOCOL_VERSION, features, NULL, has_local_epoch ? &context : NULL);
    jazz_db_PeerConnection *connection = NULL;
    jz_err e = to_js_err(jazz_db__db__lifecycle__Db__accept_subscriber_with_claims_async(db_inner->db, transport, identity, claims, &connection));
    if (e.code) {
        jz_rc_release(queues);
        return e;
    }
    jazz_wasm_WasmTransport *t = JZ_NEW(jazz_wasm_WasmTransport);
    t->inner.db = jazz_db__db__Db__rc_clone(db_inner->db);
    t->inner.connection = connection;
    t->auxiliary_pump = WasmTransportInner__auxiliary_pump(&t->inner);
    t->queues = queues;
    t->has_subscriber_identity = true;
    t->subscriber_identity = identity;
    *out = t;
    return JZ_OK;
}

typedef struct jazz_wasm_AcceptTask {
    jazz_wasm_WasmDbInner db_inner;
    jazz_types_AuthorSubject identity;
    jz_btree claims;
    bool has_claims;
    bool has_local_epoch;
    u64 local_epoch;
} jazz_wasm_AcceptTask;

static jz_err accept_subscriber_body(void *ctx, jz_js *out) {
    jazz_wasm_AcceptTask *t = (jazz_wasm_AcceptTask *)ctx;
    jazz_wasm_WasmTransport *transport = NULL;
    t->has_claims = false;
    JZ_TRY(WasmDb__accept_subscriber_with_admitted_identity(&t->db_inner, t->identity, t->claims, t->has_local_epoch, t->local_epoch, &transport));
    *out = wasm_transport_into_js(transport);
    return JZ_OK;
}

static void accept_task_drop(void *ctx) {
    jazz_wasm_AcceptTask *t = (jazz_wasm_AcceptTask *)ctx;
    WasmDbInner__drop(&t->db_inner);
    if (t->has_claims) jz_btree_drop(&t->claims);
    jz_free(t);
}

static void accept_subscriber_finish(wbg_ret_val *ret, jazz_wasm_WasmDb *self, jz_err e, jazz_wasm_AcceptTask *t) {
    if (!e.code) {
        t->has_claims = true;
        e = WasmDb__open_inner(self, &t->db_inner);
    }
    if (e.code) {
        accept_task_drop(t);
        ret_val(ret, e, 0);
        return;
    }
    ret_val(ret, JZ_OK, jazz_wasm__future_to_promise(accept_subscriber_body, t, accept_task_drop));
}

/* Admit a subscriber after pending local writes have been recovered. */
JZ_EXPORT("wasmdb_acceptSubscriber")
void jazz_wasm__WasmDb__accept_subscriber(wbg_ret_val *ret, jazz_wasm_WasmDb *self, u8 *identity_ptr, u32 identity_len, jz_js claims,
                                          u32 has_local_epoch, u64 local_epoch) {
    jz_bytes identity_raw = arg_bytes(identity_ptr, identity_len);
    jazz_wasm_AcceptTask *t = JZ_NEW(jazz_wasm_AcceptTask);
    t->has_local_epoch = has_local_epoch != 0;
    t->local_epoch = local_epoch;
    jz_err e = jazz_wasm__author_id_from_bytes(jz_bytes_slice(&identity_raw), &t->identity);
    jz_bytes_drop(&identity_raw);
    if (!e.code) e = jazz_wasm__claims_from_js(t->identity, claims, &t->claims);
    jz_js_drop(claims);
    accept_subscriber_finish(ret, self, e, t);
}

/* Attach a browser-local follower for a verified first-party identity. */
JZ_EXPORT("wasmdb_acceptSubscriberWithSelfSignedProof")
void jazz_wasm__WasmDb__accept_subscriber_with_self_signed_proof(wbg_ret_val *ret, jazz_wasm_WasmDb *self, jz_js claims, char *token_ptr, u32 token_len,
                                                                 char *app_ptr, u32 app_len, char *claimed_ptr, u32 claimed_len, u32 has_local_epoch,
                                                                 u64 local_epoch) {
    jz_str token = arg_str(token_ptr, token_len), app_id = arg_str(app_ptr, app_len), claimed_author = arg_str(claimed_ptr, claimed_len);
    jazz_wasm_AcceptTask *t = JZ_NEW(jazz_wasm_AcceptTask);
    t->has_local_epoch = has_local_epoch != 0;
    t->local_epoch = local_epoch;
    jz_err e = jazz_wasm__verify_self_signed_runtime_author(&token, &app_id, &claimed_author, &t->identity);
    if (!e.code) e = jazz_wasm__claims_from_js(t->identity, claims, &t->claims);
    jz_str_drop(&token);
    jz_str_drop(&app_id);
    jz_str_drop(&claimed_author);
    jz_js_drop(claims);
    accept_subscriber_finish(ret, self, e, t);
}

typedef struct jazz_wasm_CloseTask { jazz_wasm_WasmDbInner inner; bool owns_runtime; } jazz_wasm_CloseTask;

static jz_err close_body(void *ctx, jz_js *out) {
    jazz_wasm_CloseTask *t = (jazz_wasm_CloseTask *)ctx;
    if (t->owns_runtime) JZ_TRY(to_js_err(jazz_db__db__lifecycle__Db__close(t->inner.db)));
    *out = WBG_TRUE;
    return JZ_OK;
}

static void close_task_drop(void *ctx) {
    jazz_wasm_CloseTask *t = (jazz_wasm_CloseTask *)ctx;
    WasmDbInner__drop(&t->inner);
    jz_free(t);
}

/* A close failure still consumes the binding: physical close happens once. */
JZ_EXPORT("wasmdb_close") jz_js jazz_wasm__WasmDb__close(jazz_wasm_WasmDb *self) {
    if (!self->inner->some) return wbg_promise_resolve(WBG_FALSE);
    jazz_wasm_CloseTask *t = JZ_NEW(jazz_wasm_CloseTask);
    t->inner = self->inner->inner;
    self->inner->some = false;
    memset(&self->inner->inner, 0, sizeof self->inner->inner);
    t->owns_runtime = self->owns_runtime;
    return jazz_wasm__future_to_promise(close_body, t, close_task_drop);
}

JZ_EXPORT("__wbg_wasmdb_free") void jazz_wasm__WasmDb__free(jazz_wasm_WasmDb *self) {
    jz_rc_release(self->inner);
    jz_rc_release(self->non_durable_client);
    jz_free(self);
}

/* ================================================================== */
/* WasmTransport                                                        */
/* ================================================================== */
typedef struct jazz_wasm_ClaimsTask { jazz_wasm_WasmTransportInner inner; jz_btree claims; } jazz_wasm_ClaimsTask;

/* WasmTransportInner::update_authenticated_claims */
static jz_err update_claims_body(void *ctx, jz_js *out) {
    jazz_wasm_ClaimsTask *t = (jazz_wasm_ClaimsTask *)ctx;
    if (!t->inner.connection) return str_err("subscriber transport is closed");
    jz_btree claims = t->claims;
    memset(&t->claims, 0, sizeof t->claims);
    jazz_db__db__peer_connection__PeerConnection__update_authenticated_session_claims(t->inner.connection, claims); /* lock().await */
    *out = WBG_UNDEFINED;
    return JZ_OK;
}

static void claims_task_drop(void *ctx) {
    jazz_wasm_ClaimsTask *t = (jazz_wasm_ClaimsTask *)ctx;
    WasmTransportInner__drop(&t->inner);
    if (t->claims.d) jz_btree_drop(&t->claims);
    jz_free(t);
}

JZ_EXPORT("wasmtransport_updateAuthenticatedClaims")
void jazz_wasm__WasmTransport__update_authenticated_claims(wbg_ret_val *ret, jazz_wasm_WasmTransport *self, jz_js claims_js) {
    jz_err e = JZ_OK;
    jz_btree claims = {0};
    if (!self->has_subscriber_identity) e = str_err("transport is not a subscriber link");
    if (!e.code) e = jazz_wasm__claims_from_js(self->subscriber_identity, claims_js, &claims);
    jz_js_drop(claims_js);
    if (e.code) {
        ret_val(ret, e, 0);
        return;
    }
    jazz_wasm_ClaimsTask *t = JZ_NEW(jazz_wasm_ClaimsTask);
    t->inner = WasmTransportInner__clone(&self->inner);
    t->claims = claims;
    ret_val(ret, JZ_OK, jazz_wasm__future_to_promise(update_claims_body, t, claims_task_drop));
}

typedef struct jazz_wasm_PumpTask { jazz_db_PeerIoPump *pump; jz_bytes frame; } jazz_wasm_PumpTask;

static void pump_task_drop(void *ctx) {
    jazz_wasm_PumpTask *t = (jazz_wasm_PumpTask *)ctx;
    jazz_db__db__PeerIoPump__drop(t->pump);
    jz_bytes_drop(&t->frame);
    jz_free(t);
}

static jz_err route_frame_body(void *ctx, jz_js *out) {
    jazz_wasm_PumpTask *t = (jazz_wasm_PumpTask *)ctx;
    bool some = false;
    jz_bytes frame = {0};
    jz_bytes input = t->frame;
    memset(&t->frame, 0, sizeof t->frame);
    JZ_TRY(into_str_err(jazz_db__db__PeerIoPump__route_incoming_wire_frame(t->pump, input, &some, &frame)));
    *out = some ? bytes_to_js(&frame) : WBG_UNDEFINED;
    return JZ_OK;
}

/* Route one socket frame through the chunk lane before semantic delivery;
 * the original frame comes back when it belongs to the ordinary Jazz lane. */
JZ_EXPORT("wasmtransport_routeAuxiliaryWireFrame")
jz_js jazz_wasm__WasmTransport__route_auxiliary_wire_frame(jazz_wasm_WasmTransport *self, u8 *frame_ptr, u32 frame_len) {
    jazz_wasm_PumpTask *t = JZ_NEW(jazz_wasm_PumpTask);
    t->pump = jazz_db__db__PeerIoPump__clone(self->auxiliary_pump);
    t->frame = arg_bytes(frame_ptr, frame_len);
    return jazz_wasm__future_to_promise(route_frame_body, t, pump_task_drop);
}

/* Drain a bounded FIFO batch of complete auxiliary wire frames. */
JZ_EXPORT("wasmtransport_recvAuxiliaryWireFrames")
void jazz_wasm__WasmTransport__recv_auxiliary_wire_frames(wbg_ret_val *ret, jazz_wasm_WasmTransport *self, u32 has_max_frames, u32 max_frames_in,
                                                          u32 has_max_bytes, u32 max_bytes_in) {
    u32 max_frames = has_max_frames ? max_frames_in : 1;
    if (max_frames < 1) max_frames = 1;
    u32 max_bytes = has_max_bytes ? max_bytes_in : JAZZ_MAX_WIRE_FRAME_BYTES;
    if (max_bytes < 1) max_bytes = 1;
    jz_js frames = wbg_array_new();
    JZ_VEC(jz_bytes) taken = {0};
    jz_err e = into_str_err(jazz_db__db__PeerIoPump__take_outbound_wire_frames(self->auxiliary_pump, max_frames, max_bytes, (void *)&taken));
    if (e.code) {
        jz_js_drop(frames);
        ret_val(ret, e, 0);
        return;
    }
    for (u32 i = 0; i < taken.len; i++) {
        jz_js frame = wbg_uint8array_from(taken.ptr[i].ptr, taken.ptr[i].len);
        wbg_array_push(frames, frame);
        jz_js_drop(frame);
    }
    jz_vec_drop(&taken, sizeof(jz_bytes), bytes_vec_item_drop);
    ret_val(ret, JZ_OK, frames);
}

/* Remaining receive deadline: Option<u32> as (is_some, value) */
typedef struct jazz_wasm_OptU32 { u32 is_some; u32 value; } jazz_wasm_OptU32;
JZ_EXPORT("wasmtransport_auxiliaryReceiveTimeoutMs")
void jazz_wasm__WasmTransport__auxiliary_receive_timeout_ms(jazz_wasm_OptU32 *ret, jazz_wasm_WasmTransport *self) {
    u64 delay = 0;
    if (!jazz_db__db__PeerIoPump__incomplete_receive_timeout_ms(self->auxiliary_pump, &delay)) {
        ret->is_some = 0;
        ret->value = 0;
        return;
    }
    ret->is_some = 1;
    ret->value = delay < UINT32_MAX ? (u32)delay : UINT32_MAX;
}

/* Retire only this connection when a partial channel exceeds its deadline. */
JZ_EXPORT("wasmtransport_expireAuxiliaryReceive") void jazz_wasm__WasmTransport__expire_auxiliary_receive(wbg_ret_val *ret, jazz_wasm_WasmTransport *self) {
    jz_err e = jazz_db__db__PeerIoPump__expire_incomplete_receive(self->auxiliary_pump);
    if (e.code) jazz_db__db__PeerIoPump__disconnect(self->auxiliary_pump);
    ret_unit(ret, into_str_err(e));
}

static jz_err outbound_ready_body(void *ctx, jz_js *out) {
    jazz_wasm_PumpTask *t = (jazz_wasm_PumpTask *)ctx;
    jazz_db__db__PeerIoPump__outbound_ready(t->pump);
    *out = WBG_UNDEFINED;
    return JZ_OK;
}

/* Resolve when the independently driven chunk lane has socket output. */
JZ_EXPORT("wasmtransport_auxiliaryOutboundReady") jz_js jazz_wasm__WasmTransport__auxiliary_outbound_ready(jazz_wasm_WasmTransport *self) {
    jazz_wasm_PumpTask *t = JZ_NEW(jazz_wasm_PumpTask);
    t->pump = jazz_db__db__PeerIoPump__clone(self->auxiliary_pump);
    return jazz_wasm__future_to_promise(outbound_ready_body, t, pump_task_drop);
}

static void trace_set(jz_js entry, const char *name, jz_js value) {
    jz_js key = wbg_str(name);
    jz_js thrown = 0;
    (void)wbg_reflect_set(entry, key, value, &thrown);
    jz_js_drop(key);
    jz_js_drop(value);
}

/* Drain this transport's bounded, redacted chunk-relay flight recorder. */
JZ_EXPORT("wasmtransport_takeAuxiliaryTrace") jz_js jazz_wasm__WasmTransport__take_auxiliary_trace(jazz_wasm_WasmTransport *self) {
    jz_js entries = wbg_array_new();
    JZ_VEC(jazz_db_PeerIoTraceEntry) traces = {0};
    jazz_db__db__PeerIoPump__take_trace(self->auxiliary_pump, (void *)&traces);
    for (u32 i = 0; i < traces.len; i++) {
        const jazz_db_PeerIoTraceEntry *trace = &traces.ptr[i];
        jz_js entry = wbg_object_new();
        trace_set(entry, "event", wbg_str(trace->event));
        trace_set(entry, "role", wbg_str(trace->role));
        /* IDs are opaque correlation keys: keep the full u64 as a string */
        jz_str connection = jz_format("%llu", (unsigned long long)trace->connection);
        trace_set(entry, "connection", wbg_str_owned(&connection));
        jz_str request_id = jz_format("%llu", (unsigned long long)trace->request_id);
        trace_set(entry, "requestId", wbg_str_owned(&request_id));
        trace_set(entry, "remainingHops", wbg_number_new((f64)trace->remaining_hops));
        trace_set(entry, "objectHash", wbg_string_new(trace->object_hash.ptr, trace->object_hash.len));
        trace_set(entry, "locatorFingerprint", wbg_string_new(trace->locator_fingerprint.ptr, trace->locator_fingerprint.len));
        if (trace->response) trace_set(entry, "response", wbg_str(trace->response));
        if (trace->storage_error) trace_set(entry, "storageError", wbg_str(trace->storage_error));
        wbg_array_push(entries, entry);
        jz_js_drop(entry);
    }
    jz_vec_drop(&traces, sizeof(jazz_db_PeerIoTraceEntry), jazz_db__db__PeerIoTraceEntry__drop);
    return entries;
}

JZ_EXPORT("wasmtransport_setAuxiliaryTraceEnabled") void jazz_wasm__WasmTransport__set_auxiliary_trace_enabled(jazz_wasm_WasmTransport *self, u32 enabled) {
    jazz_db__db__PeerIoPump__set_trace_enabled(self->auxiliary_pump, enabled != 0);
}

JZ_EXPORT("wasmtransport_sendWireFrame") void jazz_wasm__WasmTransport__send_wire_frame(jazz_wasm_WasmTransport *self, u8 *frame_ptr, u32 frame_len) {
    jz_bytes frame = arg_bytes(frame_ptr, frame_len);
    jz_deque_push_back(&self->queues->inbound, &frame);
}

JZ_EXPORT("wasmtransport_sendWireFrames") void jazz_wasm__WasmTransport__send_wire_frames(jazz_wasm_WasmTransport *self, jz_js frames) {
    u32 n = wbg_array_length(frames);
    for (u32 i = 0; i < n; i++) {
        jz_js frame = wbg_array_get(frames, i);
        jz_bytes bytes = {0};
        wbg_uint8array_to_vec(frame, &bytes); /* Uint8Array::new(&frame).to_vec() */
        jz_js_drop(frame);
        jz_deque_push_back(&self->queues->inbound, &bytes);
    }
    jz_js_drop(frames);
}

JZ_EXPORT("wasmtransport_recvWireFrames") jz_js jazz_wasm__WasmTransport__recv_wire_frames(jazz_wasm_WasmTransport *self) {
    jz_js frames = wbg_array_new();
    jz_bytes frame;
    while (jz_deque_pop_front(&self->queues->outbound, &frame)) {
        jz_js js = bytes_to_js(&frame);
        wbg_array_push(frames, js);
        jz_js_drop(js);
    }
    return frames;
}

JZ_EXPORT("wasmtransport_setOutboundScheduler") void jazz_wasm__WasmTransport__set_outbound_scheduler(jazz_wasm_WasmTransport *self, jz_js callback) {
    if (self->queues->has_outbound_scheduler) jz_js_drop(self->queues->outbound_scheduler);
    self->queues->outbound_scheduler = callback;
    self->queues->has_outbound_scheduler = true;
}

JZ_EXPORT("wasmtransport_clearOutboundScheduler") void jazz_wasm__WasmTransport__clear_outbound_scheduler(jazz_wasm_WasmTransport *self) {
    if (self->queues->has_outbound_scheduler) jz_js_drop(self->queues->outbound_scheduler);
    self->queues->has_outbound_scheduler = false;
}

typedef struct jazz_wasm_TransportTickTask { jazz_wasm_WasmTransportInner inner; } jazz_wasm_TransportTickTask;

static jz_err transport_tick_body(void *ctx, jz_js *out) {
    jazz_wasm_TransportTickTask *t = (jazz_wasm_TransportTickTask *)ctx;
    u32 work = 0;
    JZ_TRY(jazz_wasm__tick_connection(t->inner.connection, &work));
    *out = wbg_number_new((f64)work);
    return JZ_OK;
}

static void transport_tick_task_drop(void *ctx) {
    jazz_wasm_TransportTickTask *t = (jazz_wasm_TransportTickTask *)ctx;
    WasmTransportInner__drop(&t->inner);
    jz_free(t);
}

JZ_EXPORT("wasmtransport_tick") jz_js jazz_wasm__WasmTransport__tick(jazz_wasm_WasmTransport *self) {
    jazz_wasm_TransportTickTask *t = JZ_NEW(jazz_wasm_TransportTickTask);
    t->inner = WasmTransportInner__clone(&self->inner);
    return jazz_wasm__future_to_promise(transport_tick_body, t, transport_tick_task_drop);
}

/* WasmTransportInner::close */
static bool WasmTransportInner__close(jazz_wasm_WasmTransportInner *self) {
    jazz_db_PeerConnection *connection = self->connection;
    if (!connection) return false;
    self->connection = NULL;
    bool detached = jazz_db__db__lifecycle__Db__detach_connection(self->db, connection);
    jazz_db__db__PeerConnection__rc_release(connection);
    return detached;
}

JZ_EXPORT("wasmtransport_close") u32 jazz_wasm__WasmTransport__close(jazz_wasm_WasmTransport *self) {
    jazz_db__db__PeerIoPump__disconnect(self->auxiliary_pump);
    return WasmTransportInner__close(&self->inner);
}

JZ_EXPORT("__wbg_wasmtransport_free") void jazz_wasm__WasmTransport__free(jazz_wasm_WasmTransport *self) {
    WasmTransportInner__drop(&self->inner);
    jz_rc_release(self->queues);
    jazz_db__db__PeerIoPump__drop(self->auxiliary_pump);
    jz_free(self);
}
