import { describe, expect, it } from "vitest";
import { TestingServer } from "jazz-tools/testing";
import { createServer, startServer, stopServer, type RunningServer } from "../src/main.ts";

describe("Todo Server backend context", () => {
  it("requires upstream sync config", async () => {
    await expect(
      createServer({
        appId: "todo-server-ts",
        serverUrl: undefined,
        backendSecret: undefined,
        adminSecret: undefined,
      }),
    ).rejects.toThrow(/JAZZ_SERVER_URL and JAZZ_BACKEND_SECRET are required/i);
  });

  it("boots against an upstream Jazz server", async () => {
    const upstream = await TestingServer.start();
    let server: RunningServer | undefined;
    try {
      server = await startServer(
        await createServer({
          appId: upstream.appId,
          serverUrl: upstream.url,
          backendSecret: upstream.backendSecret,
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
  });
});
