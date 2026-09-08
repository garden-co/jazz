import { fetchSchemaHashes } from "jazz-tools";
import runServer from "../../scripts/dev-sync-server.js";
import { createServer } from "vite";
import { fileURLToPath } from "node:url";
import { createServer as createHttpServer } from "node:http";

export default async function globalSetup(): Promise<() => Promise<void>> {
  // #2641: let each listener own an OS-assigned port for its entire lifetime.
  // Concurrent worktrees must neither collide nor reuse another checkout.
  const { serverHandle } = await runServer({ port: 0 });
  const httpServer = createHttpServer();
  let webServer: Awaited<ReturnType<typeof createServer>> | undefined;
  const cleanup = async () => {
    await Promise.all([
      webServer?.close(),
      serverHandle.stop(),
      new Promise<void>((resolve, reject) => {
        if (!httpServer.listening) return resolve();
        httpServer.close((error) => (error ? reject(error) : resolve()));
        httpServer.closeAllConnections();
      }),
    ]);
  };
  try {
    webServer = await createServer({
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
    process.env.JAZZ_INSPECTOR_TEST_SERVER_URL = serverHandle.url;
    console.log("Inspector browser endpoints", {
      web: process.env.JAZZ_INSPECTOR_TEST_WEB_URL,
      sync: serverHandle.url,
    });

    const { hashes } = await fetchSchemaHashes(serverHandle.url, {
      appId: serverHandle.appId,
      adminSecret: serverHandle.adminSecret,
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
