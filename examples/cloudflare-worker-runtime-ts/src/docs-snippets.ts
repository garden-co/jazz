import { createDb } from "jazz-tools";
import jazzWasmModule from "jazz-wasm/pkg/jazz_wasm_bg.wasm";

// #region edge-wasm-module
const db = await createDb({
  appId: "my-app",
  runtimeSources: {
    wasmModule: jazzWasmModule,
  },
});
// #endregion edge-wasm-module

void db;
