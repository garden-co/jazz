import { expect } from "@playwright/test";
import { fetchSchemaHashes } from "jazz-tools";
import { startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import runServer from "../../scripts/dev-sync-server.js";
import { createServer } from "vite";
import { fileURLToPath } from "node:url";
import { createServer as createHttpServer } from "node:http";
import { startSessionTenantManager } from "./session-tenant-manager.js";
import { standalonePermissions } from "./schema.js";

export default async function globalSetup(): Promise<() => Promise<void>> {
  // #2641: let each listener own an OS-assigned port for its entire lifetime.
  // Concurrent worktrees must neither collide nor reuse another checkout.
  const servers: LocalJazzServerHandle[] = [];
  const httpServer = createHttpServer();
  let tenant: Awaited<ReturnType<typeof startSessionTenantManager>> | undefined;
  let webServer: Awaited<ReturnType<typeof createServer>> | undefined;
  const cleanup = async () => {
    tenant?.close();
    await Promise.all([
      webServer?.close(),
      ...servers.map((server) => server.stop()),
      new Promise<void>((resolve, reject) => {
        if (!httpServer.listening) return resolve();
        httpServer.close((error) => (error ? reject(error) : resolve()));
        httpServer.closeAllConnections();
      }),
    ]);
  };
  try {
    const { serverHandle: overlay } = await runServer({ port: 0 });
    servers.push(overlay);
    // Seed only Core. Standalone Inspector must retrieve protected remote rows
    // through Edge, while the embedded host keeps its public-read application.
    const { serverHandle: core } = await runServer({
      port: 0,
      serverPermissions: standalonePermissions,
    });
    servers.push(core);
    const edge = await startLocalJazzServer({
      appId: core.appId,
      port: 0,
      adminSecret: core.adminSecret,
      backendSecret: core.backendSecret,
      upstreamUrl: core.url,
    });
    servers.push(edge);
    // Binding a listener does not mean Edge has installed Core's catalogue yet.
    await expect
      .poll(async () => (await fetch(`${edge.url}/health`)).status, { timeout: 15_000 })
      .toBe(200);

    tenant = await startSessionTenantManager(edge.url);
    webServer = await createServer({
      define: { "import.meta.env.VITE_INSPECTOR_DASHBOARD_ORIGIN": JSON.stringify(tenant.origin) },
      root: fileURLToPath(new URL("../..", import.meta.url)),
      // Vite treats port 0 as its default port; Node owns the actual listener.
      server: { middlewareMode: true, hmr: { server: httpServer } },
    });
    httpServer.on("request", webServer.middlewares);
    await new Promise<void>((resolve, reject) => {
      httpServer.once("error", reject);
      httpServer.listen(0, "127.0.0.1", () => {
        httpServer.off("error", reject);
        resolve();
      });
    });
    const address = httpServer.address();
    if (!address || typeof address === "string") {
      throw new Error("Inspector browser web server did not bind a TCP listener");
    }
    process.env.JAZZ_INSPECTOR_TEST_WEB_URL = `http://127.0.0.1:${address.port}`;
    tenant.setInspectorOrigin(process.env.JAZZ_INSPECTOR_TEST_WEB_URL);
    process.env.JAZZ_INSPECTOR_TEST_DASHBOARD_URL = tenant.origin;
    process.env.JAZZ_INSPECTOR_TEST_CORE_SERVER_URL = core.url;
    process.env.JAZZ_INSPECTOR_TEST_SERVER_URL = overlay.url;
    process.env.JAZZ_INSPECTOR_TEST_STANDALONE_SERVER_URL = edge.url;
    console.log("Inspector browser endpoints", {
      web: process.env.JAZZ_INSPECTOR_TEST_WEB_URL,
      sync: overlay.url,
      standalone: edge.url,
    });

    const { hashes } = await fetchSchemaHashes(edge.url, {
      appId: edge.appId,
      adminSecret: edge.adminSecret,
    });

    const publishedSchemaHash = hashes.at(-1);
    if (!publishedSchemaHash) {
      throw new Error("No schema hashes were published during inspector browser global setup.");
    }

    process.env.PUBLISHED_SCHEMA_HASH = publishedSchemaHash;

    return cleanup;
  } catch (error) {
    await cleanup().catch((cleanupError) => {
      console.error("Inspector browser cleanup after setup failure failed", cleanupError);
    });
    throw error;
  }
}
