import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import {
  blockJazzServerNetwork,
  jazzServerInfo,
  jazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../../../packages/jazz-tools/tests/browser/testing-server-node.js";

function jazzBrowserTopologyLog(
  _context: unknown,
  status: "start" | "complete" | "failed",
  label: string,
  elapsedMs: number,
) {
  console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
}

export default defineConfig({
  define: {
    "process.env.NEXT_PUBLIC_JAZZ_APP_ID": JSON.stringify("band-chat-browser-tests"),
    "process.env.NEXT_PUBLIC_JAZZ_SERVER_URL": "undefined",
  },
  plugins: [wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    include: ["tests/browser/**/*.test.tsx"],
    globalSetup: ["../../../../packages/jazz-tools/tests/browser/global-setup.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        jazzBrowserTopologyLog,
        jazzServerInfo: async (_context, appId, schema) => jazzServerInfo(appId, schema),
        jazzServerBlockNetwork: async ({ context }, serverUrl) =>
          blockJazzServerNetwork(context, serverUrl),
        jazzServerUnblockNetwork: async ({ context }, serverUrl) =>
          unblockJazzServerNetwork(context, serverUrl),
        jazzServerJwtForUser: async (_context, userId, claims, appId) =>
          jazzServerJwtForUser(userId, claims, appId),
        jazzBandChatBootstrapProfile: async (_context, server, userId, displayName) => {
          // Exercise the application's actual trusted bootstrap path. The backend
          // secret stays in the Node-side browser command and is never given to a
          // browser client.
          process.env.NEXT_PUBLIC_JAZZ_APP_ID = server.appId;
          process.env.NEXT_PUBLIC_JAZZ_SERVER_URL = server.serverUrl;
          process.env.BACKEND_SECRET = "jazz-browser-test-backend";
          const { ensureProfile } = await import("./src/lib/bootstrap.ts");
          const { shutdownAuthJazzContext } = await import("./src/lib/auth-jazz-context.ts");
          try {
            await ensureProfile(userId, displayName);
          } finally {
            await shutdownAuthJazzContext();
          }
        },
      },
    },
    setupFiles: ["tests/browser/setup-react.ts"],
    testTimeout: 30000,
    sequence: { concurrent: false },
  },
});
