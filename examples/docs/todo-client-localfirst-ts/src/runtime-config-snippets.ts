import { createJazzClient } from "jazz-tools/client";

// #region context-setup-ts-runtime-sources
const session = await createJazzClient({
  serverUrl: "https://my-jazz-server.example.com",
  initial: "device",
  runtimeSources: {
    wasmUrl: "/static/jazz/jazz_wasm_bg.wasm",
    wasmVersion: "2026-08-25", // Change this for every deployed asset build.
  },
});
const db = session.db;
// #endregion context-setup-ts-runtime-sources

void db;
