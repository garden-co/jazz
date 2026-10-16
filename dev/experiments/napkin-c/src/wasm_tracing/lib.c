/* crates/wasm-tracing/src/lib.rs
 *
 * Also holds the C counterpart of the subscriber this crate installs:
 * `Registry::default().with(WasmLayer::new(config))` (tracing-subscriber's
 * Layered<WasmLayer, Registry>), i.e. the span store whose extensions the
 * layer reads, current-span stack, and the global-default dispatcher slot. */
#include "wasm_tracing/wasm_tracing.h"
#include "jazz_wasm/wbg.h"

void wasm_tracing__log4(jz_str message1, const char *message2, const char *message3, const char *message4) {
    wbg_console_log4(message1.ptr, message1.len, message2, (u32)strlen(message2), message3, (u32)strlen(message3), message4,
                     (u32)strlen(message4));
    jz_str_drop(&message1);
}

/* ---- tracing_subscriber::registry::Registry (sharded slab -> one hash map) ---- */
struct wasm_tracing_Context {
    jz_hmap spans;            /* u64 -> SpanData */
    JZ_VEC(u64) current;      /* entered span stack */
    u64 next_id;
};

static u64 wt_u64_hash(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool wt_u64_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static void wt_span_drop(void *p) {
    wasm_tracing_SpanData *s = (wasm_tracing_SpanData *)p;
    if (s->has_recorder) wasm_tracing__recorder__StringRecorder__drop(&s->recorder);
}
static const jz_hmap_desc WT_SPANS = {sizeof(u64), sizeof(wasm_tracing_SpanData), wt_u64_hash, wt_u64_eq, NULL, wt_span_drop};

typedef struct wt_Layered {
    wasm_tracing_WasmLayer layer;
    wasm_tracing_Context registry;
} wt_Layered;

static bool GLOBAL_DEFAULT_SET = false;
static wt_Layered GLOBAL_DEFAULT;

wasm_tracing_SpanData *wasm_tracing__Context__span(wasm_tracing_Context *ctx, u64 id) {
    return (wasm_tracing_SpanData *)jz_hmap_get(&ctx->spans, &id);
}

wasm_tracing_SpanData *wasm_tracing__Context__lookup_current(wasm_tracing_Context *ctx) {
    for (u32 i = ctx->current.len; i > 0; i--) {
        u64 id = ctx->current.ptr[i - 1];
        wasm_tracing_SpanData *s = wasm_tracing__Context__span(ctx, id);
        /* Registry::lookup_current skips spans disabled by per-layer filters */
        if (s && GLOBAL_DEFAULT_SET && wasm_tracing__layer__WasmLayer__Layer__enabled(&GLOBAL_DEFAULT.layer, s->meta)) return s;
    }
    return NULL;
}

bool wasm_tracing__set_as_global_default_with_config(const wasm_tracing_WasmLayerConfig *config) {
    if (GLOBAL_DEFAULT_SET) return false; /* SetGlobalDefaultError */
    wasm_tracing__layer__WasmLayer__new(&GLOBAL_DEFAULT.layer, config);
    jz_hmap_init(&GLOBAL_DEFAULT.registry.spans, &WT_SPANS);
    memset(&GLOBAL_DEFAULT.registry.current, 0, sizeof GLOBAL_DEFAULT.registry.current);
    GLOBAL_DEFAULT.registry.next_id = 1;
    GLOBAL_DEFAULT_SET = true;
    /* the C tracing macros consult this level before building an event */
    jz_trace_level = config->max_level;
    return true;
}

bool wasm_tracing__subscriber__enabled(const wasm_tracing_Metadata *meta) {
    return GLOBAL_DEFAULT_SET && wasm_tracing__layer__WasmLayer__Layer__enabled(&GLOBAL_DEFAULT.layer, meta);
}

u64 wasm_tracing__subscriber__new_span(const wasm_tracing_Metadata *meta, const wasm_tracing_ValueSet *attrs) {
    if (!GLOBAL_DEFAULT_SET) return 0;
    wasm_tracing_Context *r = &GLOBAL_DEFAULT.registry;
    u64 id = r->next_id++;
    wasm_tracing_SpanData data;
    memset(&data, 0, sizeof data);
    data.meta = meta;
    data.parent = r->current.len ? r->current.ptr[r->current.len - 1] : 0;
    data.refs = 1;
    if (data.parent) {
        wasm_tracing_SpanData *p = wasm_tracing__Context__span(r, data.parent);
        if (p) p->refs++; /* a child keeps its parent open */
        else data.parent = 0;
    }
    jz_hmap_insert(&r->spans, &id, &data, NULL);
    if (wasm_tracing__layer__WasmLayer__Layer__enabled(&GLOBAL_DEFAULT.layer, meta))
        wasm_tracing__layer__WasmLayer__Layer__on_new_span(&GLOBAL_DEFAULT.layer, attrs, id, r);
    return id;
}

void wasm_tracing__subscriber__record(u64 id, const wasm_tracing_ValueSet *values) {
    if (!GLOBAL_DEFAULT_SET || id == 0) return;
    wasm_tracing__layer__WasmLayer__Layer__on_record(&GLOBAL_DEFAULT.layer, id, values, &GLOBAL_DEFAULT.registry);
}

void wasm_tracing__subscriber__event(const wasm_tracing_Metadata *meta, const wasm_tracing_ValueSet *values) {
    if (!wasm_tracing__subscriber__enabled(meta)) return;
    wasm_tracing__layer__WasmLayer__Layer__on_event(&GLOBAL_DEFAULT.layer, meta, values, &GLOBAL_DEFAULT.registry);
}

void wasm_tracing__subscriber__enter(u64 id) {
    if (!GLOBAL_DEFAULT_SET || id == 0) return;
    wasm_tracing_Context *r = &GLOBAL_DEFAULT.registry;
    JZ_VEC_PUSH(r->current, id);
    wasm_tracing_SpanData *s = wasm_tracing__Context__span(r, id);
    if (s && wasm_tracing__layer__WasmLayer__Layer__enabled(&GLOBAL_DEFAULT.layer, s->meta))
        wasm_tracing__layer__WasmLayer__Layer__on_enter(&GLOBAL_DEFAULT.layer, id, r);
}

void wasm_tracing__subscriber__exit(u64 id) {
    if (!GLOBAL_DEFAULT_SET || id == 0) return;
    wasm_tracing_Context *r = &GLOBAL_DEFAULT.registry;
    for (u32 i = r->current.len; i > 0; i--) {
        if (r->current.ptr[i - 1] == id) {
            jz_vec_remove(&r->current, i - 1, NULL, sizeof(u64));
            break;
        }
    }
}

void wasm_tracing__subscriber__clone_span(u64 id) {
    if (!GLOBAL_DEFAULT_SET || id == 0) return;
    wasm_tracing_SpanData *s = wasm_tracing__Context__span(&GLOBAL_DEFAULT.registry, id);
    if (!s) jz_panic("tried to clone a span that already closed");
    s->refs++;
}

void wasm_tracing__subscriber__try_close(u64 id) {
    if (!GLOBAL_DEFAULT_SET || id == 0) return;
    wasm_tracing_Context *r = &GLOBAL_DEFAULT.registry;
    wasm_tracing_SpanData *s = wasm_tracing__Context__span(r, id);
    if (!s) return;
    if (--s->refs != 0) return;
    if (wasm_tracing__layer__WasmLayer__Layer__enabled(&GLOBAL_DEFAULT.layer, s->meta))
        wasm_tracing__layer__WasmLayer__Layer__on_close(&GLOBAL_DEFAULT.layer, id, r);
    u64 parent = s->parent;
    jz_hmap_remove(&r->spans, &id, NULL, NULL);
    if (parent) wasm_tracing__subscriber__try_close(parent);
}
