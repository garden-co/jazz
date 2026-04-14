import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { TestingServer, pushSchemaCatalogue, startLocalJazzServer } from "./index.js";

// ---------------------------------------------------------------------------
// WebSocket auth helper
// ---------------------------------------------------------------------------

interface WsAuth {
  admin_secret?: string;
  backend_secret?: string;
  jwt_token?: string;
}

/**
 * Opens a WebSocket to `{baseUrl}/ws`, sends the Jazz auth handshake, and
 * resolves with the server-assigned client_id on success or rejects when the
 * server closes / sends an Error frame.
 */
async function connectWs(baseUrl: string, auth: WsAuth): Promise<string> {
  const wsUrl = baseUrl.replace(/^http/, "ws") + "/ws";
  return new Promise<string>((resolve, reject) => {
    const ws = new WebSocket(wsUrl);
    ws.binaryType = "arraybuffer";
    let settled = false;

    function settle(fn: () => void): void {
      if (settled) return;
      settled = true;
      ws.close();
      fn();
    }

    ws.onopen = (): void => {
      const handshake = {
        client_id: crypto.randomUUID(),
        auth: {
          jwt_token: auth.jwt_token ?? null,
          backend_secret: auth.backend_secret ?? null,
          admin_secret: auth.admin_secret ?? null,
          backend_session: null,
          local_mode: null,
          local_token: null,
        },
        catalogue_state_hash: null,
      };
      const payload = new TextEncoder().encode(JSON.stringify(handshake));
      const frame = new Uint8Array(4 + payload.length);
      new DataView(frame.buffer).setUint32(0, payload.length, false);
      frame.set(payload, 4);
      ws.send(frame);
    };

    ws.onmessage = (event: MessageEvent<ArrayBuffer>): void => {
      const buf = new Uint8Array(event.data);
      const len = new DataView(buf.buffer).getUint32(0, false);
      const msg = JSON.parse(new TextDecoder().decode(buf.subarray(4, 4 + len))) as {
        type: string;
        client_id?: string;
        message?: string;
      };

      if (msg.type === "Connected") {
        settle(() => resolve(msg.client_id ?? ""));
      } else {
        settle(() => reject(new Error(msg.message ?? `Unexpected frame: ${msg.type}`)));
      }
    };

    ws.onerror = (): void => settle(() => reject(new Error("WebSocket connection failed")));
    ws.onclose = (ev: CloseEvent): void => {
      settle(() => reject(new Error(`WebSocket closed before Connected (code=${ev.code})`)));
    };
  });
}

const tempRoots: string[] = [];

afterEach(async () => {
  await Promise.all(
    tempRoots.splice(0).map((rootPath) => rm(rootPath, { recursive: true, force: true })),
  );
});

async function createTempRoot(prefix: string): Promise<string> {
  const rootPath = await mkdtemp(join(tmpdir(), prefix));
  tempRoots.push(rootPath);
  return rootPath;
}

async function canBindPort(port: number): Promise<boolean> {
  return await new Promise<boolean>((resolve) => {
    const server = createServer();
    server.once("error", () => {
      resolve(false);
    });
    server.listen(port, "127.0.0.1", () => {
      server.close((error) => {
        void error;
        resolve(true);
      });
    });
  });
}

async function getAvailablePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }
          reject(new Error("Failed to allocate an available port."));
        });
        return;
      }

      const port = address.port;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
  });
}

describe("TestingServer", () => {
  it("starts and is reachable at /health", async () => {
    const server = await TestingServer.start();
    try {
      const response = await fetch(`${server.url}/health`);
      expect(response.status).toBe(200);
    } finally {
      await server.stop();
    }
  }, 15_000);

  it("exposes appId, url, port, adminSecret, backendSecret", async () => {
    const server = await TestingServer.start();
    try {
      expect(server.appId).toEqual(expect.any(String));
      expect(server.url).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);
      expect(server.port).toEqual(expect.any(Number));
      expect(server.adminSecret).toEqual(expect.any(String));
      expect(server.backendSecret).toEqual(expect.any(String));
    } finally {
      await server.stop();
    }
  }, 15_000);

  it("respects custom adminSecret and backendSecret", async () => {
    const adminSecret = "custom-admin-secret-test";
    const backendSecret = "custom-backend-secret-test";
    const server = await TestingServer.start({ adminSecret, backendSecret });
    try {
      expect(server.adminSecret).toBe(adminSecret);
      expect(server.backendSecret).toBe(backendSecret);

      // Correct admin secret → server accepts the WebSocket connection.
      const clientId = await connectWs(server.url, { admin_secret: adminSecret });
      expect(clientId).toEqual(expect.any(String));

      // Correct backend secret → server accepts the WebSocket connection.
      const backendClientId = await connectWs(server.url, { backend_secret: backendSecret });
      expect(backendClientId).toEqual(expect.any(String));

      // Wrong admin secret → server rejects the connection.
      await expect(connectWs(server.url, { admin_secret: "wrong-secret" })).rejects.toThrow();
    } finally {
      await server.stop();
    }
  }, 15_000);

  it("generates valid JWTs via jwtForUser", async () => {
    const server = await TestingServer.start();
    try {
      const token = server.jwtForUser("test-user");
      expect(typeof token).toBe("string");
      expect(token.split(".")).toHaveLength(3);
    } finally {
      await server.stop();
    }
  }, 15_000);
});

describe("startLocalJazzServer", () => {
  it("starts the process, waits for /health, and stops cleanly", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-capture-");
    const dataDir = join(captureRoot, "data-dir");
    const port = await getAvailablePort();

    const server = await startLocalJazzServer({
      appId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      port,
      dataDir,
      backendSecret: "test-backend-secret",
      adminSecret: "test-admin-secret",
      allowAnonymous: true,
      allowDemo: true,
    });

    const healthResponse = await fetch(`${server.url}/health`);
    expect(healthResponse.status).toBe(200);
    expect(server.dataDir).toBe(dataDir);
    expect(server.adminSecret).toBe("test-admin-secret");
    expect(server.backendSecret).toBe("test-backend-secret");

    await server.stop();
  }, 15_000);

  it("frees the port after stop so it can be rebound", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-port-free-");
    const dataDir = join(captureRoot, "data-dir");
    const port = await getAvailablePort();

    const server = await startLocalJazzServer({
      appId: "cccccccc-cccc-cccc-cccc-cccccccccccc",
      port,
      dataDir,
    });

    await server.stop();

    const canRebind = await canBindPort(port);
    expect(canRebind).toBe(true);
  });

  it("can start a server with enableLogs turned on", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-logs-");
    const dataDir = join(captureRoot, "data-dir");
    const port = await getAvailablePort();

    const server = await startLocalJazzServer({
      appId: "dddddddd-dddd-dddd-dddd-dddddddddddd",
      port,
      dataDir,
      enableLogs: true,
    });

    const healthResponse = await fetch(`${server.url}/health`);
    expect(healthResponse.status).toBe(200);

    await server.stop();
  }, 15_000);

  it("accepts a WebSocket connection with correct admin secret", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret-for-ts-schema-sync";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const clientId = await connectWs(server.url, { admin_secret: adminSecret });
      expect(clientId).toEqual(expect.any(String));
    } finally {
      await server.stop();
    }
  });

  it("rejects a WebSocket connection with wrong admin secret", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      await expect(connectWs(server.url, { admin_secret: "wrong-admin-secret" })).rejects.toThrow();
    } finally {
      await server.stop();
    }
  });
});

describe("pushSchemaCatalogue", () => {
  it("rejects when no root schema.ts can be found", async () => {
    const root = await createTempRoot("jazz-tools-testing-missing-schema-");

    await expect(
      pushSchemaCatalogue({
        serverUrl: "http://127.0.0.1:9999",
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret: "admin-secret",
        schemaDir: root,
      }),
    ).rejects.toThrow(/schema file not found/i);
  });

  it("publishes the current schema object via schema.ts using pushSchemaCatalogue", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const beforeResponse = await fetch(`${server.url}/schemas`, {
        headers: {
          "X-Jazz-Admin-Secret": adminSecret,
        },
      });
      expect(beforeResponse.status).toBe(200);
      const beforeBody = (await beforeResponse.json()) as { hashes?: string[] };

      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret,
        schemaDir: join(import.meta.dirname ?? __dirname, "fixtures/basic"),
      });

      const response = await fetch(`${server.url}/schemas`, {
        headers: {
          "X-Jazz-Admin-Secret": adminSecret,
        },
      });
      expect(response.status).toBe(200);

      const body = (await response.json()) as { hashes?: string[] };
      expect(body.hashes?.length).toBeGreaterThan(beforeBody.hashes?.length ?? 0);
    } finally {
      await server.stop();
    }
  }, 30_000);

  it("rejects when server is unreachable", async () => {
    await expect(
      pushSchemaCatalogue({
        serverUrl: "http://127.0.0.1:9",
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret: "admin-secret",
        schemaDir: join(import.meta.dirname ?? __dirname, "fixtures/basic"),
      }),
    ).rejects.toThrow();
  }, 10_000);
});
