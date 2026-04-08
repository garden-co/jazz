import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { parseCombinedServerCommand, startReverseProxyServer } from "./combined-server.js";

interface TestHttpServerHandle {
  close(): Promise<void>;
  url: string;
}

async function startTestHttpServer(
  handler: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<TestHttpServerHandle> {
  const server = createServer(handler);

  await new Promise<void>((resolve, reject) => {
    server.listen(0, "127.0.0.1", (error?: Error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });

  const address = server.address() as AddressInfo;

  return {
    async close(): Promise<void> {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    },
    url: `http://127.0.0.1:${address.port}`,
  };
}

const activeHandles: Array<{ close(): Promise<void> }> = [];

afterEach(async () => {
  await Promise.all(
    activeHandles
      .splice(0)
      .reverse()
      .map((handle) => handle.close()),
  );
});

describe("startReverseProxyServer", () => {
  it("parses bundled-auth server options without leaking auth-only flags to Rust", () => {
    expect(
      parseCombinedServerCommand([
        "server",
        "app-123",
        "--port",
        "1700",
        "--auth=none",
        "--data-dir",
        "./data",
      ]),
    ).toEqual({
      appId: "app-123",
      authMode: "none",
      explicitJwksUrl: undefined,
      publicPort: 1700,
      rustArgs: ["server", "app-123", "--port", "1700", "--data-dir", "./data"],
    });
  });

  it("routes auth-owned paths to the bundled auth server and sync paths to the Rust server", async () => {
    const authServer = await startTestHttpServer((req, res) => {
      if (req.url === "/auth/sign-in") {
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end("<h1>auth-sign-in</h1>");
        return;
      }

      if (req.url === "/.well-known/jwks.json") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ keys: [{ kid: "auth-key" }] }));
        return;
      }

      if (req.url === "/api/auth/sign-in/email" && req.method === "POST") {
        let body = "";
        req.setEncoding("utf8");
        req.on("data", (chunk) => {
          body += chunk;
        });
        req.on("end", () => {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ upstream: "auth", body }));
        });
        return;
      }

      res.writeHead(404);
      res.end("auth-not-found");
    });
    activeHandles.push(authServer);

    const syncServer = await startTestHttpServer((req, res) => {
      if (req.url === "/health") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ upstream: "sync" }));
        return;
      }

      if (req.url === "/sync") {
        res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
        res.end("sync-route");
        return;
      }

      res.writeHead(404);
      res.end("sync-not-found");
    });
    activeHandles.push(syncServer);

    const proxy = await startReverseProxyServer({
      authOrigin: authServer.url,
      port: 0,
      syncOrigin: syncServer.url,
    });
    activeHandles.push(proxy);

    const signInResponse = await fetch(`${proxy.url}/auth/sign-in`);
    expect(signInResponse.status).toBe(200);
    await expect(signInResponse.text()).resolves.toContain("auth-sign-in");

    const jwksResponse = await fetch(`${proxy.url}/.well-known/jwks.json`);
    expect(jwksResponse.status).toBe(200);
    await expect(jwksResponse.json()).resolves.toEqual({ keys: [{ kid: "auth-key" }] });

    const authApiResponse = await fetch(`${proxy.url}/api/auth/sign-in/email`, {
      body: JSON.stringify({ email: "alice@example.com" }),
      headers: { "content-type": "application/json" },
      method: "POST",
    });
    await expect(authApiResponse.json()).resolves.toEqual({
      body: JSON.stringify({ email: "alice@example.com" }),
      upstream: "auth",
    });

    const healthResponse = await fetch(`${proxy.url}/health`);
    await expect(healthResponse.json()).resolves.toEqual({ upstream: "sync" });

    const syncResponse = await fetch(`${proxy.url}/sync`);
    await expect(syncResponse.text()).resolves.toBe("sync-route");
  });
});
