/* wasm_tracing.h: crate wasm_tracing (crates/wasm-tracing), the tracing
 * subscriber layer that writes to the browser console and buffers trace
 * entries for JavaScript drains. */
#ifndef WASM_TRACING_H
#define WASM_TRACING_H
#include "jz_rt.h"

/* tracing::Level, ordered like tracing (more verbose compares greater). */
typedef enum { WT_LEVEL_ERROR = 1, WT_LEVEL_WARN = 2, WT_LEVEL_INFO = 3, WT_LEVEL_DEBUG = 4, WT_LEVEL_TRACE = 5 } wasm_tracing_Level;
const char *wasm_tracing__Level__as_str(u32 level); /* <tracing::Level as Display> */

/* config.rs */
typedef struct wasm_tracing_WasmLayerConfig {
    bool enabled;
    u32 max_level;
    bool console_group_spans;
} wasm_tracing_WasmLayerConfig;
void wasm_tracing__config__WasmLayerConfig__Default__default(wasm_tracing_WasmLayerConfig *out);
void wasm_tracing__config__WasmLayerConfig__new(wasm_tracing_WasmLayerConfig *out);
void wasm_tracing__config__WasmLayerConfig__disable(wasm_tracing_WasmLayerConfig *c);
void wasm_tracing__config__WasmLayerConfig__with_max_level(wasm_tracing_WasmLayerConfig *c, u32 max_level);
void wasm_tracing__config__WasmLayerConfig__with_console_group_spans(wasm_tracing_WasmLayerConfig *c);

/* tracing_core::Metadata / Field / ValueSet equivalents */
typedef struct wasm_tracing_Metadata {
    const char *name;
    const char *target;
    u32 level;
    const char *file; /* may be NULL */
    u32 line;
    bool has_line;
} wasm_tracing_Metadata;
enum { WT_FIELD_STR, WT_FIELD_DEBUG, WT_FIELD_I64, WT_FIELD_U64, WT_FIELD_BOOL, WT_FIELD_F64 };
typedef struct wasm_tracing_FieldValue {
    const char *name;
    u8 kind;
    union { jz_slice s; i64 i; u64 u; bool b; f64 f; } v; /* WT_FIELD_DEBUG: s is the pre-rendered Debug text */
} wasm_tracing_FieldValue;
typedef struct wasm_tracing_ValueSet { const wasm_tracing_FieldValue *fields; u32 len; } wasm_tracing_ValueSet;

/* recorder.rs */
typedef struct wasm_tracing_StringRecorder {
    jz_str display;
    bool is_following_args;
    jz_hmap fields; /* String -> String */
    bool has_message;
    jz_str message;
} wasm_tracing_StringRecorder;
void wasm_tracing__recorder__StringRecorder__Default__default(wasm_tracing_StringRecorder *r);
void wasm_tracing__recorder__StringRecorder__drop(wasm_tracing_StringRecorder *r);
void wasm_tracing__recorder__StringRecorder__Visit__record_str(wasm_tracing_StringRecorder *r, const char *field, jz_slice value);
void wasm_tracing__recorder__StringRecorder__Visit__record_debug(wasm_tracing_StringRecorder *r, const char *field, const wasm_tracing_FieldValue *value);
void wasm_tracing__recorder__StringRecorder__Display__fmt(const wasm_tracing_StringRecorder *r, jz_str *out);
void wasm_tracing__recorder__record_value_set(wasm_tracing_StringRecorder *r, const wasm_tracing_ValueSet *values);
void wasm_tracing__recorder__fmt_debug_value(jz_str *out, const wasm_tracing_FieldValue *v);
extern const jz_hmap_desc wasm_tracing__STRING_STRING_HMAP;
void wasm_tracing__clone_string_map(jz_hmap *dst, const jz_hmap *src);

/* layer.rs: the span/registry state the layer reads extensions from */
typedef struct wasm_tracing_SpanTiming { u32 start_secs; u32 start_nanos; } wasm_tracing_SpanTiming;
typedef struct wasm_tracing_SpanData {
    const wasm_tracing_Metadata *meta;
    u64 parent;
    u32 refs;
    bool has_recorder;
    wasm_tracing_StringRecorder recorder;
    bool has_timing;
    wasm_tracing_SpanTiming timing;
} wasm_tracing_SpanData;
typedef struct wasm_tracing_WasmLayer { wasm_tracing_WasmLayerConfig config; } wasm_tracing_WasmLayer;
/* Context<'_, S>: lookup of spans in the registry */
typedef struct wasm_tracing_Context wasm_tracing_Context;
wasm_tracing_SpanData *wasm_tracing__Context__span(wasm_tracing_Context *ctx, u64 id);
wasm_tracing_SpanData *wasm_tracing__Context__lookup_current(wasm_tracing_Context *ctx);

void wasm_tracing__layer__WasmLayer__new(wasm_tracing_WasmLayer *out, const wasm_tracing_WasmLayerConfig *config);
void wasm_tracing__layer__WasmLayer__Default__default(wasm_tracing_WasmLayer *out);
bool wasm_tracing__layer__WasmLayer__Layer__enabled(const wasm_tracing_WasmLayer *l, const wasm_tracing_Metadata *meta);
void wasm_tracing__layer__WasmLayer__Layer__on_new_span(const wasm_tracing_WasmLayer *l, const wasm_tracing_ValueSet *attrs, u64 id, wasm_tracing_Context *ctx);
void wasm_tracing__layer__WasmLayer__Layer__on_record(const wasm_tracing_WasmLayer *l, u64 id, const wasm_tracing_ValueSet *values, wasm_tracing_Context *ctx);
void wasm_tracing__layer__WasmLayer__Layer__on_event(const wasm_tracing_WasmLayer *l, const wasm_tracing_Metadata *meta, const wasm_tracing_ValueSet *values, wasm_tracing_Context *ctx);
void wasm_tracing__layer__WasmLayer__Layer__on_enter(const wasm_tracing_WasmLayer *l, u64 id, wasm_tracing_Context *ctx);
void wasm_tracing__layer__WasmLayer__Layer__on_close(const wasm_tracing_WasmLayer *l, u64 id, wasm_tracing_Context *ctx);

void wasm_tracing__layer__set_trace_entry_collection_enabled(bool enabled);
jz_js wasm_tracing__layer__subscribe_trace_entries(jz_js callback);
jz_js wasm_tracing__layer__drain_trace_entries(void);

/* lib.rs */
void wasm_tracing__log4(jz_str message1, const char *message2, const char *message3, const char *message4);
/* Err(SetGlobalDefaultError) is returned as false */
bool wasm_tracing__set_as_global_default_with_config(const wasm_tracing_WasmLayerConfig *config);

/* the Layered<WasmLayer, Registry> subscriber entry points (tracing dispatch) */
u64 wasm_tracing__subscriber__new_span(const wasm_tracing_Metadata *meta, const wasm_tracing_ValueSet *attrs);
void wasm_tracing__subscriber__record(u64 id, const wasm_tracing_ValueSet *values);
void wasm_tracing__subscriber__event(const wasm_tracing_Metadata *meta, const wasm_tracing_ValueSet *values);
void wasm_tracing__subscriber__enter(u64 id);
void wasm_tracing__subscriber__exit(u64 id);
void wasm_tracing__subscriber__clone_span(u64 id);
void wasm_tracing__subscriber__try_close(u64 id);
bool wasm_tracing__subscriber__enabled(const wasm_tracing_Metadata *meta);
#endif
