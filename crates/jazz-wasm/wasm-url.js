/** Bundler-resolved URL for passing the Jazz binary into a separate worker. */
export const bundledWasmUrl = new URL("./pkg/jazz_wasm_bg.wasm", import.meta.url).href;
