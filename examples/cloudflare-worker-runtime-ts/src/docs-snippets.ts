import { createJazzClient, type AccountStore } from "jazz-tools";
import jazzWasmModule from "jazz-wasm/pkg/jazz_wasm_bg.wasm";

// #region edge-wasm-module
// Give the request an account store (a Worker has no browser storage), and
// pass the precompiled Wasm module: there is no asset URL to fetch it from.
export async function openRequestDb(store: AccountStore) {
  const session = await createJazzClient({
    serverUrl: null,
    client: () => import("jazz-tools/local-first"),
    initial: "device",
    store,
    runtimeSources: { wasmModule: jazzWasmModule },
  });
  return session.db;
}
// #endregion edge-wasm-module
