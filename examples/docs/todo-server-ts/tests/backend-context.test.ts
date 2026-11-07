import { describe, expect, it } from "vitest";
import { WebSocket as UndiciWebSocket } from "undici";
import { startLocalJazzServer } from "jazz-tools/testing";
import { createServer, startServer, stopServer, type RunningServer } from "../src/main.ts";

globalThis.WebSocket ??= UndiciWebSocket as unknown as typeof globalThis.WebSocket;

describe("Todo Server backend context", () => {
  it("refuses a Jazz core without its app id and admin secret", async () => {
    await expect(createServer({ serverUrl: "http://127.0.0.1:1" })).rejects.toThrow(
      /JAZZ_SERVER_URL needs JAZZ_APP_ID and JAZZ_ADMIN_SECRET/,
    );
  });

  it("answers a request without a signed-in user as unauthorized", async () => {
    const server = await startServer(await createServer(), 0);
    try {
      const response = await fetch(`${server.baseUrl}/todos/as/someone`);
      expect(response.status).toBe(401);
      await expect(response.json()).resolves.toEqual({ error: "Unauthorized" });
    } finally {
      await stopServer(server);
    }
  });

  // `startLocalJazzServer` resolves only after the native server is ready,
  // but durable-store startup can contend with the workspace's parallel TS
  // suites in CI. Keep the normal 6s budget for request behavior and grant
  // this explicit server-lifecycle test its own readiness budget.
  it("boots against a Jazz core elsewhere", async () => {
    const upstream = await startLocalJazzServer();
    let server: RunningServer | undefined;
    try {
      server = await startServer(
        await createServer({
          appId: upstream.appId,
          serverUrl: upstream.url,
          adminSecret: upstream.adminSecret,
        }),
        0,
      );

      const res = await fetch(`${server.baseUrl}/health`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ status: "healthy" });
    } finally {
      if (server) {
        await stopServer(server);
      }
      await upstream.stop();
    }
  }, 20_000);
});
