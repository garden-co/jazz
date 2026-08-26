import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import type { BrowserContext, Page } from "playwright";
import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";
import {
  blockJazzServerNetwork,
  jazzServerInfo,
  jazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../../../packages/jazz-tools/tests/browser/testing-server-node.js";

interface ConnectedPeerInput {
  id: string;
  appId: string;
  dbName: string;
  table: string;
  queryJson?: string;
  schemaJson: string;
  serverUrl: string;
  jwtToken?: string;
  adminSecret?: string;
}

const connectedPeers = new Map<string, { context: BrowserContext; page: Page }>();
const remoteHarnessModulePath = `/@fs${fileURLToPath(
  new URL("../../../../packages/jazz-tools/tests/browser/remote-db-harness.ts", import.meta.url),
)}`;
const remoteHarnessHtmlPath = `/@fs${fileURLToPath(
  new URL("../../../../packages/jazz-tools/tests/browser/remote-db-harness.html", import.meta.url),
)}`;

async function callRemotePeer<TResult>(
  page: Page,
  method: string,
  input: unknown,
): Promise<TResult> {
  return page.evaluate(
    async ({ modulePath, method, input }) => {
      const harness = await import(/* @vite-ignore */ modulePath);
      return await harness[method](input);
    },
    { modulePath: remoteHarnessModulePath, method, input },
  );
}

async function openConnectedPeer(
  currentContext: BrowserContext,
  currentPage: Page,
  input: ConnectedPeerInput,
): Promise<void> {
  await closeConnectedPeer(input.id);
  const browser = currentContext.browser();
  if (!browser) throw new Error("PosterShop connected peer needs an attached browser");
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto(new URL(remoteHarnessHtmlPath, currentPage.url()).toString(), {
    waitUntil: "domcontentloaded",
  });
  await callRemotePeer(page, "createRemoteBrowserDb", input);
  connectedPeers.set(input.id, { context, page });
}

async function queryConnectedPeer(id: string): Promise<Record<string, unknown>[]> {
  const peer = connectedPeers.get(id);
  if (!peer) throw new Error(`PosterShop connected peer ${id} is not open`);
  return callRemotePeer(peer.page, "queryRemoteBrowserDbRows", { id, tier: "edge" });
}

async function insertConnectedPeer(
  id: string,
  table: string,
  row: Record<string, unknown>,
): Promise<string> {
  const peer = connectedPeers.get(id);
  if (!peer) throw new Error(`PosterShop connected peer ${id} is not open`);
  return callRemotePeer(peer.page, "insertRemoteBrowserDbRow", {
    id,
    table,
    row,
    tier: "edge",
  });
}

async function closeConnectedPeer(id: string): Promise<void> {
  const peer = connectedPeers.get(id);
  if (!peer) return;
  connectedPeers.delete(id);
  await callRemotePeer(peer.page, "closeRemoteBrowserDb", id).catch(() => undefined);
  await peer.context.close();
}

/** Browser topology is deliberately separate from the fast node policy receipts. */
export default defineConfig({
  server: { fs: { allow: [fileURLToPath(new URL("../../../..", import.meta.url))] } },
  define: {
    __JAZZ_EXAMPLE_TOPOLOGY_SEED__: JSON.stringify(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? "47"),
  },
  plugins: [wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    globalSetup: ["../../../../packages/jazz-tools/tests/browser/global-setup.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        jazzServerInfo: async (_context, appId, schema) => jazzServerInfo(appId, schema),
        jazzServerBlockNetwork: async ({ context }, serverUrl) =>
          blockJazzServerNetwork(context, serverUrl),
        jazzServerUnblockNetwork: async ({ context }, serverUrl) =>
          unblockJazzServerNetwork(context, serverUrl),
        posterShopOpenConnectedPeer: async ({ context, page }, input) =>
          openConnectedPeer(context, page, input),
        posterShopQueryConnectedPeer: async (_context, id) => queryConnectedPeer(id),
        posterShopInsertConnectedPeer: async (_context, id, table, row) =>
          insertConnectedPeer(id, table, row),
        posterShopCloseConnectedPeer: async (_context, id) => closeConnectedPeer(id),
        jazzServerJwtForUser: async (_context, userId, claims, appId) =>
          jazzServerJwtForUser(userId, claims, appId),
        jazzBrowserTopologyLog: async (_context, status, label, elapsedMs) => {
          console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
        },
      },
    },
    include: ["tests/browser/**/*.test.tsx"],
    sequence: { concurrent: false },
    testTimeout: 30_000,
  },
});
