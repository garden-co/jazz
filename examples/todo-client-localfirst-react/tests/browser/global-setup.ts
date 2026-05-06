import { join } from "node:path";
import { startLocalJazzServer, pushSchemaCatalogue } from "jazz-tools/testing";
import {
  EDGE_TEST_PORT,
  CORE_TEST_PORT,
  TEST_PORT,
  JWT_SECRET,
  ADMIN_SECRET,
  PEER_SECRET,
  APP_ID,
} from "./test-constants.js";

export { TEST_PORT, JWT_SECRET, ADMIN_SECRET, PEER_SECRET, APP_ID };

type LocalServer = Awaited<ReturnType<typeof startLocalJazzServer>>;

let coreServer: LocalServer | null = null;
let edgeServer: LocalServer | null = null;
let setupPromise: Promise<void> | null = null;

async function waitUntil(
  message: string,
  check: () => Promise<boolean>,
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  const detail = lastError instanceof Error ? `: ${lastError.message}` : "";
  throw new Error(`Timed out waiting for ${message}${detail}`);
}

async function fetchJson<T>(url: string): Promise<T | null> {
  const response = await fetch(url, {
    headers: {
      "X-Jazz-Admin-Secret": ADMIN_SECRET,
    },
  });

  if (!response.ok) return null;
  return (await response.json()) as T;
}

async function waitForCatalogueOnEdge(edge: LocalServer, schemaHash: string): Promise<void> {
  await waitUntil(`schema ${schemaHash} to propagate to the edge server`, async () => {
    const body = await fetchJson<{ hashes?: string[] }>(`${edge.url}/apps/${edge.appId}/schemas`);
    return body?.hashes?.includes(schemaHash) ?? false;
  });

  await waitUntil(
    `permissions for schema ${schemaHash} to propagate to the edge server`,
    async () => {
      const body = await fetchJson<{ head?: { schemaHash?: string } | null }>(
        `${edge.url}/apps/${edge.appId}/admin/permissions/head`,
      );
      return body?.head?.schemaHash === schemaHash;
    },
  );
}

export async function setup(): Promise<void> {
  if (!setupPromise) {
    setupPromise = (async () => {
      coreServer = await startLocalJazzServer({
        appId: APP_ID,
        port: CORE_TEST_PORT,
        adminSecret: ADMIN_SECRET,
        peerSecret: PEER_SECRET,
        inMemory: true,
      });

      edgeServer = await startLocalJazzServer({
        appId: APP_ID,
        port: EDGE_TEST_PORT,
        adminSecret: ADMIN_SECRET,
        peerSecret: PEER_SECRET,
        upstreamUrl: coreServer.url,
        inMemory: true,
      });

      const { hash } = await pushSchemaCatalogue({
        serverUrl: coreServer.url,
        appId: coreServer.appId,
        adminSecret: coreServer.adminSecret!,
        schemaDir: join(import.meta.dirname ?? __dirname, "../.."),
      });

      await waitForCatalogueOnEdge(edgeServer, hash);
    })();
  }

  await setupPromise;
}

export async function teardown(): Promise<void> {
  await edgeServer?.stop();
  await coreServer?.stop();
  edgeServer = null;
  coreServer = null;
  setupPromise = null;
}
