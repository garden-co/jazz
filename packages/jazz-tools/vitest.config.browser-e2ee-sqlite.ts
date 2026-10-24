import { mergeConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { createProductionWorkerBundleFixture } from "./tests/browser/worker-fault-bundle-node.js";
import storage from "./vitest.config.browser-storage.js";

const worker = createProductionWorkerBundleFixture(
  fileURLToPath(new URL("../../crates/jazz-wasm/pkg", import.meta.url)),
);

export default mergeConfig(storage, {
  plugins: [worker.plugin],
  optimizeDeps: {
    include: ["@noble/hashes/legacy.js", "@noble/hashes/sha2.js", "libsodium-wrappers"],
  },
  test: {
    browser: { commands: { sqliteE2eeWorkerUrl: async () => worker.url() } },
    include: ["tests/sqlite-e2ee/encrypted-chat.test.ts"],
    globalSetup: ["tests/sqlite-e2ee/setup.ts"],
    testTimeout: 60000,
  },
});
