import { resolve } from "node:path";
import { defineConfig, type ViteUserConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import {
  jazzAppFront,
  jazzAppRequest,
  jazzAppUser,
  jazzServerInfo,
  stopJazzServerByUrl,
  jazzDeploy,
} from "../../packages/jazz-tools/tests/browser/testing-server-node.js";
import { TEST_APP_ID } from "./tests/browser/test-constants.js";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

export default defineConfig(async (): Promise<ViteUserConfig> => {
  return {
    resolve: {
      alias: sealedWasmPackage ? { "jazz-wasm": resolve(sealedWasmPackage, "jazz_wasm.js") } : {},
    },
    plugins: [wasm(), topLevelAwait(), react()],
    worker: {
      plugins: () => [wasm(), topLevelAwait()],
    },
    define: {
      __APP_ID__: JSON.stringify(TEST_APP_ID),
    },
    test: {
      browser: {
        enabled: true,
        provider: playwright(),
        instances: [{ browser: "chromium", headless: true }],
        commands: {
          jazzDeploy: async (_context, options) => jazzDeploy(options),
          jazzServerInfo: async (_context, appId) => jazzServerInfo(appId),
          jazzServerStop: async (_context, serverUrl) => stopJazzServerByUrl(serverUrl),
          jazzAppFront: async (_context, coreUrl, app) => jazzAppFront(coreUrl, app),
          jazzAppUser: async (_context, frontUrl, userId, claims) =>
            jazzAppUser(frontUrl, userId, claims),
          jazzAppRequest: async (_context, url, init) => jazzAppRequest(url, init),
        },
      },
      include: ["tests/browser/**/*.test.{ts,tsx}"],
      globalSetup: ["../../packages/jazz-tools/tests/browser/global-setup.ts"],
      testTimeout: 30000,
    },
  };
});
