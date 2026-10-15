// `wasm-tracing`: profile via `console`, installed as the global trace sink.
import { setTraceSink } from "../support/runtime.js";
import { WasmLayerConfig } from "./config.js";
import { WasmLayer } from "./layer.js";

export * from "./config.js";
export * from "./layer.js";

/** Install a WasmLayer as the global trace sink. */
export function setAsGlobalDefaultWithConfig(config: WasmLayerConfig): void {
  const layer = new WasmLayer(config);
  setTraceSink({
    span(name, fields) {
      const span = layer.newSpan({ name, target: "jazz", level: "TRACE" }, fields);
      layer.enter(span);
      return () => {
        layer.exit(span);
        layer.close(span);
      };
    },
    count() {},
  });
}
