import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";
import type { BrowserCommandContext } from "vitest/node";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";
import type {
  JazzServerBrowserCommands,
  JazzTopologyBrowserCommands,
} from "../../packages/jazz-tools/tests/browser/browser-commands.js";
import {
  blockJazzServerNetwork,
  jazzServerInfo,
  jazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../packages/jazz-tools/tests/browser/testing-server-node.js";

async function jazzBrowserTopologyLog(
  _context: unknown,
  status: "start" | "complete" | "failed",
  label: string,
  elapsedMs: number,
) {
  console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
}

type JazzBrowserCommands = JazzServerBrowserCommands & JazzTopologyBrowserCommands;
type JazzBrowserCommandHandlers = {
  [Name in keyof JazzBrowserCommands]: (
    context: BrowserCommandContext,
    ...args: Parameters<JazzBrowserCommands[Name]>
  ) => ReturnType<JazzBrowserCommands[Name]>;
};

const jazzBrowserCommands = {
  jazzBrowserTopologyLog,
  jazzServerInfo: async (_context, appId, schema) => jazzServerInfo(appId, schema),
  jazzServerBlockNetwork: async ({ context }, serverUrl) =>
    blockJazzServerNetwork(context, serverUrl),
  jazzServerUnblockNetwork: async ({ context }, serverUrl) =>
    unblockJazzServerNetwork(context, serverUrl),
  jazzServerJwtForUser: async (_context, userId, claims, appId) =>
    jazzServerJwtForUser(userId, claims, appId),
} satisfies JazzBrowserCommandHandlers;

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    include: ["tests/browser/**/*.test.ts"],
    globalSetup: ["../../packages/jazz-tools/tests/browser/global-setup.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: jazzBrowserCommands,
    },
    setupFiles: ["tests/browser/setup-react.ts"],
    testTimeout: 90_000,
    sequence: { concurrent: false },
  },
});
