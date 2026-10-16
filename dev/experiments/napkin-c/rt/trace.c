/* trace.c: tracing + tracing-wasm subscriber equivalent: level-filtered events formatted as
 * "LEVEL target: message" and sent to console via js_log, a span stack with busy-time
 * measurement on close, and named counters. */
#include "rt_internal.h"

u32 jz_trace_level = 0; /* 0 = off, 1 = error .. 5 = trace */

static const char *const LEVEL_NAMES[6] = {"OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"};

#define MAX_SPANS 64
typedef struct span { const char *name; u64 start_ms; u32 id; } span;
static span g_spans[MAX_SPANS];
static u32 g_nspans, g_next_span_id = 1;

static void emit(u32 level, jz_str *s) {
    jz_js_log(level, s->ptr, s->len);
}

void jz_trace_event(u32 level, const char *target, const char *fmt, ...) {
    if (level > jz_trace_level || level == 0) return;
    jz_str s = {0};
    jz_str_push_cstr(&s, LEVEL_NAMES[level > 5 ? 5 : level]);
    jz_str_push_char(&s, ' ');
    /* span context like tracing-subscriber's fmt layer: outer:inner: */
    for (u32 i = 0; i < g_nspans; i++) {
        jz_str_push_cstr(&s, g_spans[i].name);
        jz_str_push_char(&s, ':');
    }
    if (g_nspans) jz_str_push_char(&s, ' ');
    jz_str_push_cstr(&s, target ? target : "");
    jz_str_push(&s, ": ", 2);
    va_list ap;
    va_start(ap, fmt);
    jz_str_vappendf(&s, fmt, ap);
    va_end(ap);
    emit(level, &s);
    jz_str_drop(&s);
}

u32 jz_trace_span_enter(const char *name) {
    if (jz_trace_level < 4) return 0; /* spans only recorded at debug and above */
    u32 id = g_next_span_id++;
    if (g_nspans < MAX_SPANS) {
        g_spans[g_nspans].name = name;
        g_spans[g_nspans].start_ms = jz_now_ms();
        g_spans[g_nspans].id = id;
        g_nspans++;
    }
    return id;
}

void jz_trace_span_exit(u32 id) {
    if (!id) return;
    /* close the span (and any unclosed children) */
    for (u32 i = g_nspans; i-- > 0;) {
        if (g_spans[i].id != id) continue;
        if (jz_trace_level >= 5) {
            jz_str s = {0};
            u64 busy = jz_now_ms() - g_spans[i].start_ms;
            jz_str_appendf(&s, "TRACE %s: close time.busy=%llums", g_spans[i].name, (unsigned long long)busy);
            emit(5, &s);
            jz_str_drop(&s);
        }
        g_nspans = i;
        return;
    }
}

typedef struct counter { const char *name; u64 v; } counter;
static JZ_VEC(counter) g_counters;

void jz_counter_add(const char *name, u64 v) {
    if (!jz_trace_level) return;
    JZ_VEC_FOR(counter, c, g_counters) {
        if (c->name == name || strcmp(c->name, name) == 0) {
            c->v += v;
            return;
        }
    }
    counter c = {name, v};
    JZ_VEC_PUSH(g_counters, c);
}

/* (debug aid) dump counters to the console: not exported by default */
void jz_dump_counters(void) {
    JZ_VEC_FOR(counter, c, g_counters) {
        jz_str s = jz_format("counter %s = %llu", c->name, (unsigned long long)c->v);
        emit(3, &s);
        jz_str_drop(&s);
    }
}
