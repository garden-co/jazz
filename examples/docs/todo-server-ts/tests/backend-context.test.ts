import { afterEach, describe, expect, it, vi } from "vitest";
import { mintLocalFirstToken } from "jazz-napi";
import { WebSocket as UndiciWebSocket } from "undici";
import { startLocalJazzServer } from "jazz-tools/testing";
import { createServer, startServer, stopServer, type RunningServer } from "../src/main.ts";

globalThis.WebSocket ??= UndiciWebSocket as unknown as typeof globalThis.WebSocket;

describe("Todo Server backend context", () => {
  afterEach(() => vi.unstubAllEnvs());

  it.each([
    { nodeEnv: "development", envOverride: undefined, configOverride: undefined, status: 403 },
    { nodeEnv: "production", envOverride: undefined, configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "true", configOverride: undefined, status: 403 },
    { nodeEnv: "production", envOverride: "false", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "FALSE", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "0", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "off", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "no", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "invalid", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: " TRUE ", configOverride: undefined, status: 403 },
    { nodeEnv: "production", envOverride: "1", configOverride: undefined, status: 403 },
    { nodeEnv: "development", envOverride: "false", configOverride: undefined, status: 401 },
    { nodeEnv: "production", envOverride: "false", configOverride: true, status: 403 },
    { nodeEnv: "production", envOverride: "true", configOverride: false, status: 401 },
  ])(
    "uses local-first auth defaults and overrides: NODE_ENV=$nodeEnv, env=$envOverride, config=$configOverride",
    async ({ nodeEnv, envOverride, configOverride, status }) => {
      vi.stubEnv("NODE_ENV", nodeEnv);
      vi.stubEnv("JAZZ_ALLOW_LOCAL_FIRST_AUTH", envOverride);
      const upstream = await startLocalJazzServer({ inMemory: true, allowLocalFirstAuth: true });
      let server: RunningServer | undefined;
      try {
        server = await startServer(
          await createServer({
            appId: upstream.appId,
            serverUrl: upstream.url,
            backendSecret: upstream.backendSecret,
            allowLocalFirstAuth: configOverride,
          }),
          0,
        );
        const token = mintLocalFirstToken(
          Buffer.alloc(32, 7).toString("base64url"),
          upstream.appId,
          3600,
        );
        // An accepted token reaches the account-mismatch check (403);
        // disabled local-first auth rejects the token first (401).
        const response = await fetch(`${server.baseUrl}/todos/as/different-account`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        expect(response.status).toBe(status);
        await expect(response.json()).resolves.toEqual({
          error: status === 403 ? "Account mismatch" : "Unauthorized",
        });
      } finally {
        if (server) await stopServer(server);
        await upstream.stop();
      }
    },
    20_000,
  );

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

  // `startLocalJazzServer` resolves only after the native server is ready,
  // but durable-store startup can contend with the workspace's parallel TS
  // suites in CI. Keep the normal 6s budget for request behavior and grant
  // this explicit server-lifecycle test its own readiness budget.
  it("boots against an upstream Jazz server", async () => {
    const upstream = await startLocalJazzServer();
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
  }, 20_000);
});
