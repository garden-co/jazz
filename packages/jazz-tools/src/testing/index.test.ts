import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { TestingServer, pushSchemaCatalogue, startLocalJazzServer } from "./index.js";

const tempRoots: string[] = [];
const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

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

async function createFailingFakeJazzBinary(stderrText: string): Promise<string> {
  const rootPath = await createTempRoot("jazz-tools-testing-fake-fail-");
  const binaryPath = join(rootPath, "fake-jazz-fail");
  const script = `#!/bin/sh
echo "${stderrText}" 1>&2
exit 13
`;
  await writeFile(binaryPath, script, "utf8");
  await chmod(binaryPath, 0o755);
  return binaryPath;
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

      const allowed = await fetch(`${server.url}/admin/schemas`, {
        method: "POST",
        headers: { "content-type": "application/json", "X-Jazz-Admin-Secret": adminSecret },
        body: JSON.stringify({ schema: TEST_SCHEMA }),
      });
      expect(allowed.status).toBe(201);

      const denied = await fetch(`${server.url}/admin/schemas`, {
        method: "POST",
        headers: { "content-type": "application/json", "X-Jazz-Admin-Secret": "wrong-secret" },
        body: JSON.stringify({ schema: TEST_SCHEMA }),
      });
      expect(denied.status).toBe(401);
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
    });

    const healthResponse = await fetch(`${server.url}/health`);
    expect(healthResponse.status).toBe(200);
    expect(server.adminSecret).toBe("test-admin-secret");
    expect(server.backendSecret).toBe("test-backend-secret");

    await server.stop();
  }, 15_000);

  it("allocates a fresh port when no explicit port is provided", async () => {
    const firstRoot = await createTempRoot("jazz-tools-testing-auto-port-a-");
    const secondRoot = await createTempRoot("jazz-tools-testing-auto-port-b-");

    const firstServer = await startLocalJazzServer({
      appId: "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
      dataDir: join(firstRoot, "data-dir"),
    });
    const firstPort = firstServer.port;
    await firstServer.stop();

    const secondServer = await startLocalJazzServer({
      appId: "ffffffff-ffff-ffff-ffff-ffffffffffff",
      dataDir: join(secondRoot, "data-dir"),
    });

    try {
      expect(secondServer.port).not.toBe(firstPort);
      const healthResponse = await fetch(`${secondServer.url}/health`);
      expect(healthResponse.status).toBe(200);
    } finally {
      await secondServer.stop();
    }
  }, 20_000);

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

  it("accepts a schema publish via /admin/schemas when admin secret matches", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret-for-ts-schema-sync";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const response = await fetch(`${server.url}/admin/schemas`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": adminSecret,
        },
        body: JSON.stringify({ schema: TEST_SCHEMA }),
      });

      expect(response.status).toBe(201);
    } finally {
      await server.stop();
    }
  });

  it("rejects a schema publish via /admin/schemas when admin secret doesn't match", async () => {
    const port = await getAvailablePort();
    const adminSecret = "admin-secret";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const response = await fetch(`${server.url}/admin/schemas`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": "wrong-admin-secret",
        },
        body: JSON.stringify({ schema: TEST_SCHEMA }),
      });

      expect(response.status).toBe(401);
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
      const { hash } = await pushSchemaCatalogue({
        serverUrl: server.url,
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret,
        schemaDir: join(import.meta.dirname ?? __dirname, "fixtures/basic"),
      });

      expect(hash).toBeTruthy();

      const response = await fetch(`${server.url}/schemas`, {
        headers: {
          "X-Jazz-Admin-Secret": adminSecret,
        },
      });
      expect(response.status).toBe(200);

      const body = (await response.json()) as { hashes?: string[] };
      expect(body.hashes?.length).toBeGreaterThan(0);
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
