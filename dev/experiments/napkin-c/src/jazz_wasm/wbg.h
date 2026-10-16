/* wbg.h: the wasm-bindgen / js-sys / web-sys host surface used by the
 * wasm-bindings unit (jazz_wasm, idb_tree::web, wasm_tracing).
 *
 * In the Rust build these are the `__wbg_*` imports wasm-bindgen generates
 * for every js-sys/web-sys call plus the externref table glue. Here they are
 * JZ_IMPORT host functions operating on jz_js handles (indices into a
 * host-side object table, like wasm-bindgen's heap). Fallible JS calls
 * (`catch` in wasm-bindgen) return 1 when the call threw and write the thrown
 * value to *out. */
#ifndef JAZZ_WASM_WBG_H
#define JAZZ_WASM_WBG_H
#include "jz_rt.h"

/* reserved handles, like wasm-bindgen's JsValue::UNDEFINED/NULL/TRUE/FALSE */
#define WBG_UNDEFINED ((jz_js)128)
#define WBG_NULL ((jz_js)129)
#define WBG_TRUE ((jz_js)130)
#define WBG_FALSE ((jz_js)131)

/* typeof classification used by the serde-wasm-bindgen port */
enum { WBG_T_UNDEFINED, WBG_T_NULL, WBG_T_BOOL, WBG_T_NUMBER, WBG_T_BIGINT, WBG_T_STRING,
       WBG_T_ARRAY, WBG_T_UINT8ARRAY, WBG_T_FUNCTION, WBG_T_OBJECT, WBG_T_MAP, WBG_T_OTHER };

JZ_IMPORT("wbg_clone_ref") jz_js wbg_clone_ref(jz_js h);
JZ_IMPORT("wbg_typeof") u32 wbg_typeof(jz_js h);
JZ_IMPORT("wbg_string_new") jz_js wbg_string_new(const char *p, u32 n);
JZ_IMPORT("wbg_number_new") jz_js wbg_number_new(f64 v);
JZ_IMPORT("wbg_bigint_from_u64") jz_js wbg_bigint_from_u64(u64 v);
JZ_IMPORT("wbg_bigint_to_u64") u32 wbg_bigint_to_u64(jz_js h, u64 *out);
JZ_IMPORT("wbg_as_f64") u32 wbg_as_f64(jz_js h, f64 *out);           /* 1 if number */
JZ_IMPORT("wbg_as_bool") u32 wbg_as_bool(jz_js h);                   /* 0 false, 1 true, 2 not a bool */
/* JsValue::as_string: host allocates with __wbindgen_malloc; returns 1 if a string */
JZ_IMPORT("wbg_as_string") u32 wbg_as_string(jz_js h, char **ptr, u32 *len);
JZ_IMPORT("wbg_debug_string") void wbg_debug_string(jz_js h, char **ptr, u32 *len); /* format!("{:?}") */
JZ_IMPORT("wbg_is_function") u32 wbg_is_function(jz_js h);
JZ_IMPORT("wbg_is_error") u32 wbg_is_error(jz_js h);                 /* dyn_ref::<js_sys::Error> */
JZ_IMPORT("wbg_error_new") jz_js wbg_error_new(const char *p, u32 n);
JZ_IMPORT("wbg_error_message") void wbg_error_message(jz_js h, char **ptr, u32 *len);
JZ_IMPORT("wbg_global") jz_js wbg_global(void);
JZ_IMPORT("wbg_reflect_get") u32 wbg_reflect_get(jz_js obj, jz_js key, jz_js *out);
JZ_IMPORT("wbg_reflect_set") u32 wbg_reflect_set(jz_js obj, jz_js key, jz_js val, jz_js *err);
JZ_IMPORT("wbg_reflect_has") u32 wbg_reflect_has(jz_js obj, jz_js key, u32 *has, jz_js *err);
JZ_IMPORT("wbg_reflect_construct") u32 wbg_reflect_construct(jz_js ctor, jz_js args, jz_js *out);
JZ_IMPORT("wbg_object_new") jz_js wbg_object_new(void);
JZ_IMPORT("wbg_object_keys") jz_js wbg_object_keys(jz_js obj);
JZ_IMPORT("wbg_array_new") jz_js wbg_array_new(void);
JZ_IMPORT("wbg_array_new_with_length") jz_js wbg_array_new_with_length(u32 n);
JZ_IMPORT("wbg_array_push") void wbg_array_push(jz_js arr, jz_js v);
JZ_IMPORT("wbg_array_set") void wbg_array_set(jz_js arr, u32 i, jz_js v);
JZ_IMPORT("wbg_array_get") jz_js wbg_array_get(jz_js arr, u32 i);
JZ_IMPORT("wbg_array_length") u32 wbg_array_length(jz_js arr);
JZ_IMPORT("wbg_is_array") u32 wbg_is_array(jz_js h);
JZ_IMPORT("wbg_uint8array_from") jz_js wbg_uint8array_from(const u8 *p, u32 n);
JZ_IMPORT("wbg_uint8array_length") u32 wbg_uint8array_length(jz_js h); /* Uint8Array::new(&v).length() */
JZ_IMPORT("wbg_uint8array_copy_to") void wbg_uint8array_copy_to(jz_js h, u8 *dst);
JZ_IMPORT("wbg_call0") u32 wbg_call0(jz_js fn, jz_js this_, jz_js *out);
JZ_IMPORT("wbg_call1") u32 wbg_call1(jz_js fn, jz_js this_, jz_js a, jz_js *out);
JZ_IMPORT("wbg_call2") u32 wbg_call2(jz_js fn, jz_js this_, jz_js a, jz_js b, jz_js *out);
JZ_IMPORT("wbg_promise_resolve") jz_js wbg_promise_resolve(jz_js v);
JZ_IMPORT("wbg_promise_reject") jz_js wbg_promise_reject(jz_js v);
/* js_sys::Promise::new(&mut |resolve, reject| ..): returns the promise and both functions */
JZ_IMPORT("wbg_promise_with_resolvers") jz_js wbg_promise_with_resolvers(jz_js *resolve, jz_js *reject);
/* JsFuture::from(promise).await: synchronous in the port (see PORTING.md); 1 if rejected */
JZ_IMPORT("wbg_await") u32 wbg_await(jz_js promise, jz_js *out);
JZ_IMPORT("wbg_date_now") f64 wbg_date_now(void);
JZ_IMPORT("wbg_throw_val") _Noreturn void wbg_throw_val(jz_js v);
/* Closure::new / Closure::wrap: the host builds a JS function that calls back
 * into the exported __wbg_closure_invoke{0,1} with `data`. dtor runs when the
 * JS side drops it (or never, after `forget`). */
JZ_IMPORT("wbg_closure_new") jz_js wbg_closure_new(void *data, u32 arity);
/* globalThis.console */
JZ_IMPORT("wbg_console_log4") void wbg_console_log4(const char *m1, u32 n1, const char *m2, u32 n2, const char *m3, u32 n3, const char *m4, u32 n4);
/* wasm_bindgen_futures::spawn_local: queue a task on the JS microtask loop */
JZ_IMPORT("wbg_queue_microtask") void wbg_queue_microtask(void *task);

/* Map.prototype.entries() as an array of [key, value] pairs */
JZ_IMPORT("wbg_map_entries") jz_js wbg_map_entries(jz_js map);
/* `impl From<T> for JsValue` of a #[wasm_bindgen] struct: the JS glue wraps
 * the boxed pointer in an instance of the exported class */
enum { WBG_CLASS_WASMDB = 1, WBG_CLASS_WASMWRITE, WBG_CLASS_WASMTRANSPORT, WBG_CLASS_PENDINGNATIVEREAD,
       WBG_CLASS_WASMPENDINGSUBSCRIPTION, WBG_CLASS_STREAMINGMUTATION, WBG_CLASS_WASMPERMISSIONADVICEREQUEST };
JZ_IMPORT("wbg_wrap_object") jz_js wbg_wrap_object(u32 class_id, void *ptr);
/* yield to the JS event loop until a host callback (timer/microtask) has run;
 * the port's stand-in for a Pending poll inside future_to_promise */
JZ_IMPORT("wbg_yield") void wbg_yield(void);

/* ---- C-side closure object (Box<dyn FnMut(JsValue) -> JsValue>) ---- */
typedef struct wbg_closure {
    jz_js (*call)(void *ctx, jz_js arg);
    void *ctx;
    void (*drop)(void *ctx);
    u32 arity;
} wbg_closure;
jz_js wbg_closure_into_js(jz_js (*call)(void *ctx, jz_js arg), void *ctx, void (*drop)(void *ctx), u32 arity);

/* ---- wasm-bindgen return slots: (value.., err, is_err) ---- */
typedef struct wbg_ret_str { u32 ptr; u32 len; jz_js err; u32 is_err; } wbg_ret_str;
typedef struct wbg_ret_val { jz_js val; jz_js err; u32 is_err; } wbg_ret_val;
typedef struct wbg_ret_ptr { u32 ptr; jz_js err; u32 is_err; } wbg_ret_ptr;

/* ---- helpers (wbg.c) ---- */
jz_js wbg_str(const char *s);
jz_js wbg_str_slice(jz_slice s);
jz_js wbg_str_owned(jz_str *s);           /* JsValue::from_str(&s), drops s */
jz_js wbg_bool(bool b);
bool wbg_is_nullish(jz_js h);
bool wbg_take_string(jz_js h, jz_str *out); /* as_string() */
void wbg_uint8array_to_vec(jz_js h, jz_bytes *out);
jz_js wbg_bytes_to_js(const u8 *p, u32 n);
jz_err wbg_err_from_js(jz_js v);          /* carry a JsValue inside a jz_err (code WBG_ERR_JS) */
jz_js wbg_err_into_js(jz_err e);          /* inverse; string errors become JS strings */
void wbg_ret_string(wbg_ret_str *ret, jz_str s);
void wbg_ret_error_str(wbg_ret_str *ret, jz_js err);
void wbg_spawn_local(jz_task_fn f, void *ctx, void (*ctx_drop)(void *));
enum { WBG_ERR_JS = 0xfe00, WBG_ERR_STR };
#endif
