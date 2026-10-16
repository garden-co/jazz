/* crates/wasm-tracing/src/layer.rs */
#include "wasm_tracing/wasm_tracing.h"
#include "jazz_wasm/wbg.h"

#define MAX_TRACE_ENTRIES 5000u

typedef struct wt_UnixNano { u32 secs; u32 nanos; } wt_UnixNano;

typedef enum { WT_ENTRY_SPAN = 0, WT_ENTRY_LOG = 1, WT_ENTRY_DROPPED = 2 } wt_TraceEntryKind;
typedef struct wasm_tracing_TraceEntry {
    u32 kind;
    u64 sequence;          /* Span, Log */
    jz_str name;           /* Span */
    jz_str target;         /* Span, Log */
    jz_str level;          /* Span, Log */
    wt_UnixNano start_unix_nano; /* Span */
    wt_UnixNano end_unix_nano;   /* Span */
    wt_UnixNano timestamp_unix_nano; /* Log */
    jz_str message;        /* Log */
    jz_hmap fields;        /* Span, Log */
    u64 count;             /* Dropped */
} wasm_tracing_TraceEntry;

static void wasm_tracing__layer__TraceEntry__drop(void *p) {
    wasm_tracing_TraceEntry *e = (wasm_tracing_TraceEntry *)p;
    switch (e->kind) {
    case WT_ENTRY_SPAN:
        jz_str_drop(&e->name);
        jz_str_drop(&e->target);
        jz_str_drop(&e->level);
        jz_hmap_drop(&e->fields);
        break;
    case WT_ENTRY_LOG:
        jz_str_drop(&e->target);
        jz_str_drop(&e->level);
        jz_str_drop(&e->message);
        jz_hmap_drop(&e->fields);
        break;
    case WT_ENTRY_DROPPED:
        break;
    }
}

typedef JZ_VEC(wasm_tracing_TraceEntry) wt_TraceEntryVec;

static bool TRACE_ENTRY_COLLECTION_ENABLED = false;
static bool TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING = false;
static u64 TRACE_ENTRY_SEQUENCE = 0;
static u64 DROPPED_TRACE_ENTRY_COUNT = 0;
static wt_TraceEntryVec TRACE_ENTRIES;
static bool TRACE_ENTRIES_POISONED = false;
/* thread_local! TRACE_ENTRY_DRAIN_CALLBACK: RefCell<Option<js_sys::Function>> */
static bool TRACE_ENTRY_DRAIN_CALLBACK_SET = false;
static jz_js TRACE_ENTRY_DRAIN_CALLBACK;

static wt_TraceEntryVec *wt_lock_entries(void) {
    if (TRACE_ENTRIES_POISONED) jz_panic("trace entries mutex poisoned");
    return &TRACE_ENTRIES;
}

static void wasm_tracing__layer__clear_trace_entries(void);
static void wasm_tracing__layer__notify_trace_entries_ready(void);

void wasm_tracing__layer__set_trace_entry_collection_enabled(bool enabled) {
    TRACE_ENTRY_COLLECTION_ENABLED = enabled;
    if (!enabled) wasm_tracing__layer__clear_trace_entries();
}

static bool wasm_tracing__layer__has_trace_entries_to_drain(void) {
    return wt_lock_entries()->len != 0 || DROPPED_TRACE_ENTRY_COUNT > 0;
}

static void wasm_tracing__layer__clear_trace_entry_drain_callback(void) {
    if (TRACE_ENTRY_DRAIN_CALLBACK_SET) jz_js_drop(TRACE_ENTRY_DRAIN_CALLBACK);
    TRACE_ENTRY_DRAIN_CALLBACK_SET = false;
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING = false;
}

static jz_js wasm_tracing__layer__subscribe_trace_entries__closure(void *ctx, jz_js arg) {
    (void)ctx; (void)arg;
    wasm_tracing__layer__clear_trace_entry_drain_callback();
    return WBG_UNDEFINED;
}

jz_js wasm_tracing__layer__subscribe_trace_entries(jz_js callback) {
    if (TRACE_ENTRY_DRAIN_CALLBACK_SET) jz_js_drop(TRACE_ENTRY_DRAIN_CALLBACK);
    TRACE_ENTRY_DRAIN_CALLBACK = callback;
    TRACE_ENTRY_DRAIN_CALLBACK_SET = true;
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING = false;

    if (wasm_tracing__layer__has_trace_entries_to_drain()) wasm_tracing__layer__notify_trace_entries_ready();

    return wbg_closure_into_js(wasm_tracing__layer__subscribe_trace_entries__closure, NULL, NULL, 0);
}

static bool wasm_tracing__layer__trace_entry_collection_enabled(void) { return TRACE_ENTRY_COLLECTION_ENABLED; }

static u64 wasm_tracing__layer__next_trace_entry_sequence(void) { return TRACE_ENTRY_SEQUENCE++; }

static void wasm_tracing__layer__push_trace_entry(wasm_tracing_TraceEntry *entry) {
    if (!wasm_tracing__layer__trace_entry_collection_enabled()) {
        wasm_tracing__layer__TraceEntry__drop(entry);
        return;
    }
    {
        wt_TraceEntryVec *entries = wt_lock_entries();
        if (entries->len >= MAX_TRACE_ENTRIES) {
            DROPPED_TRACE_ENTRY_COUNT += 1;
            wasm_tracing__layer__TraceEntry__drop(entry);
        } else {
            jz_vec_push(entries, entry, sizeof *entry);
        }
    }
    wasm_tracing__layer__notify_trace_entries_ready();
}

static void wasm_tracing__layer__drain_trace_entries_internal(wt_TraceEntryVec *drained) {
    wt_TraceEntryVec *entries = wt_lock_entries();
    *drained = *entries; /* mem::take */
    memset(entries, 0, sizeof *entries);
    u64 dropped = DROPPED_TRACE_ENTRY_COUNT;
    DROPPED_TRACE_ENTRY_COUNT = 0;
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING = false;
    if (dropped > 0) {
        wasm_tracing_TraceEntry d;
        memset(&d, 0, sizeof d);
        d.kind = WT_ENTRY_DROPPED;
        d.count = dropped;
        jz_vec_push(drained, &d, sizeof d);
    }
}

/* serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true) on
 * #[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase")] */
static bool wt_ser_u64(u64 v, jz_js *out) {
    if (v > 9007199254740991ull) return false; /* serde_wasm_bindgen: u64 out of safe range */
    *out = wbg_number_new((f64)v);
    return true;
}

static bool wt_set(jz_js obj, const char *k, jz_js v) {
    jz_js err;
    jz_js key = wbg_str(k);
    u32 failed = wbg_reflect_set(obj, key, v, &err);
    jz_js_drop(key);
    jz_js_drop(v);
    return !failed;
}

static jz_js wt_ser_str(const jz_str *s) { return wbg_string_new(s->ptr, s->len); }

static jz_js wt_ser_unix_nano(wt_UnixNano t) {
    jz_js arr = wbg_array_new();
    wbg_array_push(arr, wbg_number_new((f64)t.secs));
    wbg_array_push(arr, wbg_number_new((f64)t.nanos));
    return arr;
}

static jz_js wt_ser_fields(const jz_hmap *fields) {
    jz_js obj = wbg_object_new();
    jz_str *k, *v;
    JZ_HMAP_FOR(fields, k, v) {
        jz_js err;
        jz_js key = wt_ser_str(k), val = wt_ser_str(v);
        wbg_reflect_set(obj, key, val, &err);
        jz_js_drop(key);
        jz_js_drop(val);
    }
    return obj;
}

static bool wasm_tracing__layer__TraceEntry__Serialize__serialize(const wasm_tracing_TraceEntry *e, jz_js *out) {
    jz_js obj = wbg_object_new();
    jz_js n;
    switch (e->kind) {
    case WT_ENTRY_SPAN:
        wt_set(obj, "kind", wbg_str("span"));
        if (!wt_ser_u64(e->sequence, &n)) goto fail;
        wt_set(obj, "sequence", n);
        wt_set(obj, "name", wt_ser_str(&e->name));
        wt_set(obj, "target", wt_ser_str(&e->target));
        wt_set(obj, "level", wt_ser_str(&e->level));
        wt_set(obj, "startUnixNano", wt_ser_unix_nano(e->start_unix_nano));
        wt_set(obj, "endUnixNano", wt_ser_unix_nano(e->end_unix_nano));
        wt_set(obj, "fields", wt_ser_fields(&e->fields));
        break;
    case WT_ENTRY_LOG:
        wt_set(obj, "kind", wbg_str("log"));
        if (!wt_ser_u64(e->sequence, &n)) goto fail;
        wt_set(obj, "sequence", n);
        wt_set(obj, "target", wt_ser_str(&e->target));
        wt_set(obj, "level", wt_ser_str(&e->level));
        wt_set(obj, "timestampUnixNano", wt_ser_unix_nano(e->timestamp_unix_nano));
        wt_set(obj, "message", wt_ser_str(&e->message));
        wt_set(obj, "fields", wt_ser_fields(&e->fields));
        break;
    case WT_ENTRY_DROPPED:
        wt_set(obj, "kind", wbg_str("dropped"));
        if (!wt_ser_u64(e->count, &n)) goto fail;
        wt_set(obj, "count", n);
        break;
    default:
        JZ_UNREACHABLE();
    }
    *out = obj;
    return true;
fail:
    jz_js_drop(obj);
    return false;
}

jz_js wasm_tracing__layer__drain_trace_entries(void) {
    wt_TraceEntryVec drained;
    wasm_tracing__layer__drain_trace_entries_internal(&drained);
    jz_js arr = wbg_array_new();
    bool ok = true;
    for (u32 i = 0; i < drained.len && ok; i++) {
        jz_js v;
        ok = wasm_tracing__layer__TraceEntry__Serialize__serialize(&drained.ptr[i], &v);
        if (ok) wbg_array_push(arr, v);
    }
    jz_vec_drop(&drained, sizeof(wasm_tracing_TraceEntry), wasm_tracing__layer__TraceEntry__drop);
    if (!ok) {
        jz_js_drop(arr);
        return WBG_NULL;
    }
    return arr;
}

static void wasm_tracing__layer__notify_trace_entries_ready(void) {
    if (TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING) return;
    if (!TRACE_ENTRY_DRAIN_CALLBACK_SET) return;
    jz_js callback = wbg_clone_ref(TRACE_ENTRY_DRAIN_CALLBACK);
    bool was_pending = TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING;
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING = true;
    if (was_pending) {
        jz_js_drop(callback);
        return;
    }
    jz_js out;
    if (wbg_call0(callback, WBG_UNDEFINED, &out)) {
        TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING = false;
    }
    jz_js_drop(out);
    jz_js_drop(callback);
}

static void wasm_tracing__layer__clear_trace_entries(void) {
    wt_TraceEntryVec *entries = wt_lock_entries();
    jz_vec_truncate(entries, 0, sizeof(wasm_tracing_TraceEntry), wasm_tracing__layer__TraceEntry__drop);
    DROPPED_TRACE_ENTRY_COUNT = 0;
    TRACE_ENTRY_SEQUENCE = 0;
    TRACE_ENTRY_DRAIN_NOTIFICATION_PENDING = false;
}

/* ---------------- WasmLayer ---------------- */

void wasm_tracing__layer__WasmLayer__new(wasm_tracing_WasmLayer *out, const wasm_tracing_WasmLayerConfig *config) {
    out->config = *config;
}

void wasm_tracing__layer__WasmLayer__Default__default(wasm_tracing_WasmLayer *out) {
    wasm_tracing_WasmLayerConfig c;
    wasm_tracing__config__WasmLayerConfig__Default__default(&c);
    wasm_tracing__layer__WasmLayer__new(out, &c);
}

static wt_UnixNano wasm_tracing__layer__unix_nano_now(void) {
    f64 ms = wbg_date_now();
    f64 seconds = __builtin_floor(ms / 1000.0);
    f64 nanos_f = __builtin_round((ms - seconds * 1000.0) * 1000000.0);
    u32 secs = seconds <= 0 ? 0 : seconds >= 4294967295.0 ? 0xffffffffu : (u32)seconds;
    u32 nanos = nanos_f <= 0 ? 0 : nanos_f >= 4294967295.0 ? 0xffffffffu : (u32)nanos_f;
    if (nanos >= 1000000000u) {
        nanos -= 1000000000u;
        secs = secs == 0xffffffffu ? secs : secs + 1; /* saturating_add */
    }
    wt_UnixNano t = {secs, nanos};
    return t;
}

const char *wasm_tracing__Level__as_str(u32 level) {
    switch (level) {
    case WT_LEVEL_TRACE: return "TRACE";
    case WT_LEVEL_DEBUG: return "DEBUG";
    case WT_LEVEL_INFO: return "INFO";
    case WT_LEVEL_WARN: return "WARN";
    case WT_LEVEL_ERROR: return "ERROR";
    default: JZ_UNREACHABLE();
    }
}

/* <Level as LevelExt>::color */
static const char *wasm_tracing__layer__Level__LevelExt__color(u32 level) {
    switch (level) {
    case WT_LEVEL_TRACE: return "color: dodgerblue; background: #444";
    case WT_LEVEL_DEBUG: return "color: lawngreen; background: #444";
    case WT_LEVEL_INFO: return "color: whitesmoke; background: #444";
    case WT_LEVEL_WARN: return "color: orange; background: #444";
    case WT_LEVEL_ERROR: return "color: red; background: #444";
    default: JZ_UNREACHABLE();
    }
}

static void wasm_tracing__layer__log_with_color(jz_str message, u32 level) {
    wasm_tracing__log4(message, wasm_tracing__layer__Level__LevelExt__color(level),
                       "color: gray; font-style: italic", "color: inherit");
}

static void wasm_tracing__layer__record_span_trace_entry(const wasm_tracing_Metadata *meta,
                                                          const wasm_tracing_StringRecorder *recorder,
                                                          const wasm_tracing_SpanTiming *timing) {
    if (!wasm_tracing__layer__trace_entry_collection_enabled()) return;
    wt_UnixNano end_unix_nano = wasm_tracing__layer__unix_nano_now();
    wasm_tracing_TraceEntry e;
    memset(&e, 0, sizeof e);
    e.kind = WT_ENTRY_SPAN;
    e.sequence = wasm_tracing__layer__next_trace_entry_sequence();
    e.name = jz_str_from(meta->name);
    e.target = jz_str_from(meta->target);
    e.level = jz_str_from(wasm_tracing__Level__as_str(meta->level));
    e.start_unix_nano.secs = timing->start_secs;
    e.start_unix_nano.nanos = timing->start_nanos;
    e.end_unix_nano = end_unix_nano;
    if (recorder) wasm_tracing__clone_string_map(&e.fields, &recorder->fields);
    else jz_hmap_init(&e.fields, &wasm_tracing__STRING_STRING_HMAP);
    wasm_tracing__layer__push_trace_entry(&e);
}

/* str::trim */
static jz_slice wt_trim(jz_slice s) {
    while (s.len && (s.ptr[0] == ' ' || s.ptr[0] == '\n' || s.ptr[0] == '\t' || s.ptr[0] == '\r')) { s.ptr++; s.len--; }
    while (s.len && (s.ptr[s.len - 1] == ' ' || s.ptr[s.len - 1] == '\n' || s.ptr[s.len - 1] == '\t' || s.ptr[s.len - 1] == '\r')) s.len--;
    return s;
}

static void wasm_tracing__layer__record_log_trace_entry(const wasm_tracing_Metadata *meta, u32 level,
                                                         const wasm_tracing_StringRecorder *recorder) {
    if (!wasm_tracing__layer__trace_entry_collection_enabled()) return;
    wasm_tracing_TraceEntry e;
    memset(&e, 0, sizeof e);
    e.kind = WT_ENTRY_LOG;
    e.sequence = wasm_tracing__layer__next_trace_entry_sequence();
    e.target = jz_str_from(meta->target);
    e.level = jz_str_from(wasm_tracing__Level__as_str(level));
    e.timestamp_unix_nano = wasm_tracing__layer__unix_nano_now();
    if (recorder->has_message) e.message = jz_str_clone(&recorder->message);
    else e.message = jz_str_from_slice(wt_trim(jz_str_slice(&recorder->display)));
    wasm_tracing__clone_string_map(&e.fields, &recorder->fields);
    wasm_tracing__layer__push_trace_entry(&e);
}

bool wasm_tracing__layer__WasmLayer__Layer__enabled(const wasm_tracing_WasmLayer *l, const wasm_tracing_Metadata *meta) {
    return meta->level <= l->config.max_level;
}

void wasm_tracing__layer__WasmLayer__Layer__on_new_span(const wasm_tracing_WasmLayer *l, const wasm_tracing_ValueSet *attrs, u64 id,
                                                        wasm_tracing_Context *ctx) {
    if (!l->config.enabled && !wasm_tracing__layer__trace_entry_collection_enabled()) return;
    wasm_tracing_StringRecorder rec;
    wasm_tracing__recorder__StringRecorder__Default__default(&rec);
    wasm_tracing__recorder__record_value_set(&rec, attrs);
    wasm_tracing_SpanData *span = wasm_tracing__Context__span(ctx, id);
    if (span) {
        /* extensions_mut().insert::<StringRecorder>() replaces any previous one */
        if (span->has_recorder) wasm_tracing__recorder__StringRecorder__drop(&span->recorder);
        span->recorder = rec;
        span->has_recorder = true;
    } else {
        wasm_tracing__recorder__StringRecorder__drop(&rec);
    }
}

void wasm_tracing__layer__WasmLayer__Layer__on_record(const wasm_tracing_WasmLayer *l, u64 id, const wasm_tracing_ValueSet *values,
                                                      wasm_tracing_Context *ctx) {
    (void)l;
    wasm_tracing_SpanData *span = wasm_tracing__Context__span(ctx, id);
    if (span && span->has_recorder) wasm_tracing__recorder__record_value_set(&span->recorder, values);
}

void wasm_tracing__layer__WasmLayer__Layer__on_event(const wasm_tracing_WasmLayer *l, const wasm_tracing_Metadata *meta,
                                                     const wasm_tracing_ValueSet *values, wasm_tracing_Context *ctx) {
    bool should_collect_entry = wasm_tracing__layer__trace_entry_collection_enabled();
    if (!l->config.enabled && !should_collect_entry) return;

    wasm_tracing_StringRecorder recorder;
    wasm_tracing__recorder__StringRecorder__Default__default(&recorder);
    wasm_tracing__recorder__record_value_set(&recorder, values);
    u32 level = meta->level;
    if (should_collect_entry) wasm_tracing__layer__record_log_trace_entry(meta, level, &recorder);

    if (!l->config.enabled) goto out;
    if (should_collect_entry && level > WT_LEVEL_WARN) goto out;

    jz_str origin = {0};
    if (meta->file && meta->has_line) origin = jz_format("%s:%u", meta->file, meta->line);

    jz_str fields = {0};
    wasm_tracing_SpanData *span = wasm_tracing__Context__lookup_current(ctx);
    if (span && span->has_recorder) {
        jz_str *k, *v;
        JZ_HMAP_FOR(&span->recorder.fields, k, v) {
            jz_str_push_cstr(&fields, "\n\t");
            jz_str_push(&fields, k->ptr, k->len);
            jz_str_push_cstr(&fields, ": ");
            jz_str_push(&fields, v->ptr, v->len);
        }
    }
    jz_str rendered = {0};
    wasm_tracing__recorder__StringRecorder__Display__fmt(&recorder, &rendered);
    jz_str msg = jz_format("%%c%s%%c %.*s%%c%.*s%.*s", wasm_tracing__Level__as_str(level), (int)origin.len, origin.ptr,
                           (int)rendered.len, rendered.ptr, (int)fields.len, fields.ptr);
    jz_str_drop(&origin);
    jz_str_drop(&fields);
    jz_str_drop(&rendered);
    wasm_tracing__layer__log_with_color(msg, level);
out:
    wasm_tracing__recorder__StringRecorder__drop(&recorder);
}

void wasm_tracing__layer__WasmLayer__Layer__on_enter(const wasm_tracing_WasmLayer *l, u64 id, wasm_tracing_Context *ctx) {
    wasm_tracing_SpanData *span = wasm_tracing__Context__span(ctx, id);
    if (span) {
        if (!span->has_timing) {
            wt_UnixNano now = wasm_tracing__layer__unix_nano_now();
            span->timing.start_secs = now.secs;
            span->timing.start_nanos = now.nanos;
            span->has_timing = true;
        }
    }
    if (l->config.console_group_spans) {
        span = wasm_tracing__Context__span(ctx, id);
        if (span) {
            const wasm_tracing_Metadata *meta = span->meta;
            u32 level = meta->level;
            if (wasm_tracing__layer__trace_entry_collection_enabled() && level > WT_LEVEL_WARN) return;
            jz_str fields = {0};
            if (span->has_recorder) wasm_tracing__recorder__StringRecorder__Display__fmt(&span->recorder, &fields);
            jz_str msg = jz_format("%%c%s%%c \xE2\x96\xB6 \"%s\"%.*s", wasm_tracing__Level__as_str(level), meta->name,
                                   (int)fields.len, fields.ptr);
            jz_str_drop(&fields);
            wasm_tracing__layer__log_with_color(msg, level);
        }
    }
}

void wasm_tracing__layer__WasmLayer__Layer__on_close(const wasm_tracing_WasmLayer *l, u64 id, wasm_tracing_Context *ctx) {
    (void)l;
    if (!wasm_tracing__layer__trace_entry_collection_enabled()) return;
    wasm_tracing_SpanData *span = wasm_tracing__Context__span(ctx, id);
    if (!span) return;
    if (!span->has_timing) return;
    wasm_tracing__layer__record_span_trace_entry(span->meta, span->has_recorder ? &span->recorder : NULL, &span->timing);
}
