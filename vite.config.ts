// vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  root: "./",
  test: {
    projects: [
      "packages/*",
      "tests/browser-integration",
      "tests/cloudflare-workers",
      "tests/vercel-functions",
      "crates/cojson-core-napi",
      "crates/cojson-core-wasm",
    ],
  },
});
