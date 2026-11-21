import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [
    react(),
    jazzBackend({
      // JAZZ_DATA_DIR: bench only (dev/app-comparisons), for a separate data directory.
      dataDir:
        process.env.JAZZ_IN_MEMORY === "1" ? undefined : (process.env.JAZZ_DATA_DIR ?? ".jazz"),
    }),
  ],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
