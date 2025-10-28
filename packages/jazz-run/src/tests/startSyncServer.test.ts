import { randomUUID } from "crypto";
import { tmpdir } from "os";
import { join } from "path";
import { co, z } from "jazz-tools";
import { startWorker } from "jazz-tools/worker";
import { describe, expect, test, afterAll } from "vitest";
import { createWorkerAccount } from "../createWorkerAccount.js";
import { startSyncServer } from "../startSyncServer.js";
import { serverDefaults } from "../config.js";
import { unlinkSync } from "node:fs";

const TestMap = co.map({
  value: z.string(),
});

const dbPath = join(tmpdir(), `test-${randomUUID()}.db`);

afterAll(() => {
  unlinkSync(dbPath);
});

describe("startSyncServer", () => {
  test("persists values in storage and loads them after restart", async () => {
    // Start first server instance
    const server1 = await startSyncServer({
      host: serverDefaults.host,
      port: "0", // Random available port
      inMemory: false,
      db: dbPath,
    });

    const port = (server1.address() as any).port;
    const syncServer = `ws://localhost:${port}`;

    // Create worker account and start first worker
    const { accountID, agentSecret } = await createWorkerAccount({
      name: "test-worker",
      peer: syncServer,
    });

    const worker1 = await startWorker({
      accountID,
      accountSecret: agentSecret,
      syncServer,
    });

    // Create and sync test data
    const map = TestMap.create({ value: "testValue" });

    // Close first server
    await worker1.done();
    server1.close();

    // Start second server instance with same DB
    const server2 = await startSyncServer({
      host: serverDefaults.host,
      port: "0",
      inMemory: false,
      db: dbPath,
    });

    const port2 = (server2.address() as any).port;
    const syncServer2 = `ws://localhost:${port2}`;

    // Start second worker with same account
    const worker2 = await startWorker({
      accountID,
      accountSecret: agentSecret,
      syncServer: syncServer2,
    });

    // Try to load the previously created map
    const loadedMap = await TestMap.load(map.$jazz.id, {});

    // Verify the data persisted
    expect(loadedMap).not.toBe(null);
    expect(loadedMap?.value).toBe("testValue");

    // Cleanup
    await worker2.done();
    server2.close();
  });

  test("starts a sync server with a specific host and port", async () => {
    const server = await startSyncServer({
      host: "0.0.0.0",
      port: "4900",
      inMemory: false,
      db: dbPath,
    });

    expect(server.address()).toEqual({
      address: "0.0.0.0",
      port: 4900,
      family: "IPv4",
    });

    server.close();
  });
});
