import { createDb, type AccountHandle } from "jazz-tools";
import jazzWasmModule from "jazz-wasm/pkg/jazz_wasm_bg.wasm";

// #region edge-wasm-module
// Prepare this request's account handle before opening its context.
export function openRequestDb(account: AccountHandle) {
  return createDb({
    appId: "my-app",
    account,
    runtimeSources: { wasmModule: jazzWasmModule },
  });
}
// #endregion edge-wasm-module
