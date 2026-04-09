import { createDb } from "jazz-tools";

// #region context-setup-ts-runtime-sources
const db = await createDb({
  appId: "my-app",
  serverUrl: "https://my-jazz-server.example.com",
  runtimeSources: {
    wasmUrl: "/static/jazz/jazz_wasm_bg.wasm",
    workerUrl: "/static/jazz/worker/jazz-worker.js",
  },
});
// #endregion context-setup-ts-runtime-sources

void db;
