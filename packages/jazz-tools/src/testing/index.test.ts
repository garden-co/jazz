import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { pushSchemaCatalogue, startLocalJazzServer } from "./index.js";

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

describe("startLocalJazzServer", () => {
  it("starts the process, waits for /health, and stops cleanly", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-capture-");
    const dataDir = join(captureRoot, "data-dir");

    const server = await startLocalJazzServer({
      appId: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      port: 19111,
      dataDir,
      backendSecret: "test-backend-secret",
      adminSecret: "test-admin-secret",
      allowAnonymous: true,
      allowDemo: true,
      healthTimeoutMs: 5_000,
    });

    const healthResponse = await fetch(`${server.url}/health`);
    expect(healthResponse.status).toBe(200);
    expect(server.dataDir).toBe(dataDir);

    await server.stop();
  });

  it("frees the port after stop so it can be rebound", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-port-free-");
    const dataDir = join(captureRoot, "data-dir");
    const port = 19222;

    const server = await startLocalJazzServer({
      appId: "cccccccc-cccc-cccc-cccc-cccccccccccc",
      port,
      dataDir,
      healthTimeoutMs: 5_000,
    });

    await server.stop();

    const canRebind = await canBindPort(port);
    expect(canRebind).toBe(true);
  });

  it("can start a server with enableLogs turned on", async () => {
    const captureRoot = await createTempRoot("jazz-tools-testing-logs-");
    const dataDir = join(captureRoot, "data-dir");
    const port = 19444;

    const server = await startLocalJazzServer({
      appId: "dddddddd-dddd-dddd-dddd-dddddddddddd",
      port,
      dataDir,
      healthTimeoutMs: 5_000,
    });

    const healthResponse = await fetch(`${server.url}/health`);
    expect(healthResponse.status).toBe(200);

    await server.stop();
  });

  it("rejects with child stderr when process exits before health", async () => {
    const binaryPath = await createFailingFakeJazzBinary("startup-failed-on-purpose");

    await expect(
      startLocalJazzServer({
        appId: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        binaryPath,
        healthTimeoutMs: 3_000,
      }),
    ).rejects.toThrow(/startup-failed-on-purpose/);
  });

  it("accepts a catalogue schema sync payload via /sync when admin secret matches", async () => {
    const port = 19333;
    const adminSecret = "admin-secret-for-ts-schema-sync";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const syncBody = {
        client_id: "01234567-89ab-cdef-0123-456789abcdef",
        payload: {
          ObjectUpdated: {
            object_id: "01234567-89ab-cdef-0123-456789abcdef",
            metadata: {
              id: "01234567-89ab-cdef-0123-456789abcdef",
              metadata: { type: "catalogue_schema" },
            },
            branch_name: "main",
            commits: [],
          },
        },
      };

      const response = await fetch(`${server.url}/sync`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": adminSecret,
        },
        body: JSON.stringify(syncBody),
      });

      expect(response.status).toBe(200);
    } finally {
      await server.stop();
    }
  });

  it("rejects a catalogue schema sync payload via /sync when admin secret doesn't match", async () => {
    const port = 19333;
    const adminSecret = "admin-secret";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      const syncBody = {
        client_id: "01234567-89ab-cdef-0123-456789abcdef",
        payload: {
          ObjectUpdated: {
            object_id: "01234567-89ab-cdef-0123-456789abcdef",
            metadata: {
              id: "01234567-89ab-cdef-0123-456789abcdef",
              metadata: { type: "catalogue_schema" },
            },
            branch_name: "main",
            commits: [],
          },
        },
      };

      const response = await fetch(`${server.url}/sync`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Jazz-Admin-Secret": "wrong-admin-secret",
        },
        body: JSON.stringify(syncBody),
      });

      expect(response.status).toBe(401);
    } finally {
      await server.stop();
    }
  });
});

describe("pushSchemaCatalogue", () => {
  it("reject if binary fails", async () => {
    const binaryPath = await createFailingFakeJazzBinary("startup-failed-on-purpose");

    await expect(
      pushSchemaCatalogue({
        serverUrl: "http://127.0.0.1:9999",
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret: "admin-secret",
        schemaDir: "/tmp/schema",
        binaryPath,
      }),
    ).rejects.toThrow(/startup-failed-on-purpose/);
  });

  it("pushes schema catalogue via schema directory using pushSchemaCatalogue", async () => {
    const port = 19333;
    const adminSecret = "admin-secret";

    const server = await startLocalJazzServer({
      appId: "00000000-0000-0000-0000-000000000001",
      port,
      adminSecret,
    });

    try {
      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId: "00000000-0000-0000-0000-000000000001",
        adminSecret: adminSecret,
        schemaDir: join(import.meta.dirname ?? __dirname, "fixtures/basic"),
      });
    } finally {
      await server.stop();
    }
  });
});
