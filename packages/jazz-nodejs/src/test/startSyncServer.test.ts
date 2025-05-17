import { randomUUID } from "crypto";
import { tmpdir } from "os";
import { join } from "path";
import { createWorkerAccount } from "jazz-run/createWorkerAccount";
import { startSyncServer } from "jazz-run/startSyncServer";
import { co, z } from "jazz-tools";
import { describe, expect, test } from "vitest";
import { startWorker } from "../index.js";

const TestMap = co.map({
  value: z.string(),
});

describe("startSyncServer", () => {
  test("persists values in storage and loads them after restart", async () => {
    // Create a temporary database file
    const dbPath = join(tmpdir(), `test-${randomUUID()}.db`);

    // Start first server instance
    const server1 = await startSyncServer({
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
    const loadedMap = await TestMap.load(map.id, {});

    // Verify the data persisted
    expect(loadedMap).not.toBe(null);
    expect(loadedMap?.value).toBe("testValue");

    // Cleanup
    await worker2.done();
    server2.close();
  });
});
