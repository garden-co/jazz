import { afterAll, describe, expect, it } from "vitest";
import { TestingServer } from "jazz-tools/testing";
import { createServer, startServer, stopServer, type RunningServer } from "../src/main.ts";

const originalEnv = {
  appId: process.env.JAZZ_APP_ID,
  serverUrl: process.env.JAZZ_SERVER_URL,
  backendSecret: process.env.JAZZ_BACKEND_SECRET,
  adminSecret: process.env.JAZZ_ADMIN_SECRET,
};

function restoreEnv(): void {
  process.env.JAZZ_APP_ID = originalEnv.appId;
  process.env.JAZZ_SERVER_URL = originalEnv.serverUrl;
  process.env.JAZZ_BACKEND_SECRET = originalEnv.backendSecret;
  process.env.JAZZ_ADMIN_SECRET = originalEnv.adminSecret;
}

describe("Todo Server backend context", () => {
  afterAll(() => {
    restoreEnv();
  });

  it("requires upstream sync config", async () => {
    delete process.env.JAZZ_SERVER_URL;
    delete process.env.JAZZ_BACKEND_SECRET;
    delete process.env.JAZZ_ADMIN_SECRET;

    await expect(createServer()).rejects.toThrow(
      /JAZZ_SERVER_URL and JAZZ_BACKEND_SECRET are required/i,
    );
  });

  it("boots against an upstream Jazz server", async () => {
    const upstream = await TestingServer.start();
    let server: RunningServer | undefined;
    try {
      process.env.JAZZ_APP_ID = upstream.appId;
      process.env.JAZZ_SERVER_URL = upstream.url;
      process.env.JAZZ_BACKEND_SECRET = upstream.backendSecret;
      process.env.JAZZ_ADMIN_SECRET = upstream.adminSecret;

      server = await startServer(await createServer(), 0);

      const res = await fetch(`${server.baseUrl}/health`);
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ status: "healthy" });
    } finally {
      if (server) {
        await stopServer(server);
      }
      await upstream.stop();
      restoreEnv();
    }
  });
});
