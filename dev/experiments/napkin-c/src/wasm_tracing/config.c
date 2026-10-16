/* crates/wasm-tracing/src/config.rs */
#include "wasm_tracing/wasm_tracing.h"

void wasm_tracing__config__WasmLayerConfig__Default__default(wasm_tracing_WasmLayerConfig *out) {
    out->enabled = true;
    out->max_level = WT_LEVEL_TRACE;
    out->console_group_spans = false;
}

void wasm_tracing__config__WasmLayerConfig__new(wasm_tracing_WasmLayerConfig *out) {
    wasm_tracing__config__WasmLayerConfig__Default__default(out);
}

void wasm_tracing__config__WasmLayerConfig__disable(wasm_tracing_WasmLayerConfig *c) {
    c->enabled = false;
}

void wasm_tracing__config__WasmLayerConfig__with_max_level(wasm_tracing_WasmLayerConfig *c, u32 max_level) {
    c->max_level = max_level;
}

void wasm_tracing__config__WasmLayerConfig__with_console_group_spans(wasm_tracing_WasmLayerConfig *c) {
    c->console_group_spans = true;
}
