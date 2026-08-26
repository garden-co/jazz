import { createDb } from "jazz-tools";

// #region context-setup-ts-runtime-sources
const db = await createDb({
  appId: "my-app",
  serverUrl: "https://my-jazz-server.example.com",
  runtimeSources: {
    wasmUrl: "/static/jazz/jazz_wasm_bg.wasm",
    wasmVersion: "2026-08-25", // Change this for every deployed asset build.
  },
});
// #endregion context-setup-ts-runtime-sources

void db;
