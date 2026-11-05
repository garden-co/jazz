import { mergeConfig, type ViteUserConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import { createProductionWorkerBundleFixture } from "./tests/browser/worker-fault-bundle-node.js";
import { profileScenario } from "./tests/sqlite-e2ee/profiles-node.js";
import storage from "./vitest.config.browser-storage.js";

const worker = createProductionWorkerBundleFixture(
  fileURLToPath(new URL("../../crates/jazz-wasm/pkg", import.meta.url)),
);

const config = mergeConfig(storage as ViteUserConfig, {
  plugins: [worker.plugin],
  optimizeDeps: {
    include: ["@noble/hashes/legacy.js", "@noble/hashes/sha2.js", "libsodium-wrappers"],
  },
  test: {
    browser: {
      commands: {
        sqliteE2eeWorkerUrl: async () => worker.url(),
        sqliteE2eeProfileScenario: async ({ context, page }, input) =>
          profileScenario(context, page, input),
      },
    },
    include: ["tests/sqlite-e2ee/encrypted-chat.test.ts"],
    globalSetup: ["tests/sqlite-e2ee/setup.ts"],
    testTimeout: 60000,
  },
});

const browserName = process.env.JAZZ_BROWSER ?? "chromium";
if (browserName !== "chromium" && browserName !== "firefox" && browserName !== "webkit")
  throw new Error(`Unsupported JAZZ_BROWSER=${browserName}`);
// Replace the inherited list: Vite merges arrays by concatenation, which would
// otherwise run Chromium as well when selecting a different engine.
config.test.browser.instances = [{ browser: browserName, headless: true }];
export default config;
