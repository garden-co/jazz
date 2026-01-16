import { createSSRJazzAgent } from "jazz-tools/ssr";
import { initWasm } from "jazz-tools/wasm";

// Init WASM asynchronously to avoid blocking the main thread.
await initWasm();

export const jazzSSR = createSSRJazzAgent({ 
  peer: "wss://cloud.jazz.tools/",
});
