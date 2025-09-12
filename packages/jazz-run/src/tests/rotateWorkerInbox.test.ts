import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalNode, RawCoMap } from "cojson";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WebSocket } from "ws";
import { describe, expect, test, afterAll, onTestFinished } from "vitest";
import { createWorkerAccount } from "../createWorkerAccount.js";
import { rotateWorkerInbox } from "../rotateWorkerInbox.js";
import { startSyncServer } from "../startSyncServer.js";
import { serverDefaults } from "../config.js";
import { Account } from "jazz-tools";
import { waitFor } from "./utils.js";

const dbPath = join(tmpdir(), `test-${randomUUID()}.db`);

afterAll(() => {
  unlinkSync(dbPath);
});

describe("rotateWorkerInbox", () => {
  test("should rotate inbox and change inbox ID", async () => {
    // Set up sync server
    const server = await startSyncServer({
      host: serverDefaults.host,
      port: "0", // Random available port
      inMemory: false,
      db: dbPath,
    });

    onTestFinished(() => {
      server.close();
    });

    const address = server.address();
    if (typeof address !== "object" || address === null) {
      throw new Error("Server address is not an object");
    }

    const syncServer = `ws://localhost:${address.port}`;

    // Create worker account
    const { accountID, agentSecret } = await createWorkerAccount({
      name: "test-worker",
      peer: syncServer,
    });

    // Set up environment variables for rotateWorkerInbox
    const originalAccount = process.env.JAZZ_WORKER_ACCOUNT;
    const originalSecret = process.env.JAZZ_WORKER_SECRET;

    process.env.JAZZ_WORKER_ACCOUNT = accountID;
    process.env.JAZZ_WORKER_SECRET = agentSecret;

    onTestFinished(() => {
      delete process.env.JAZZ_WORKER_ACCOUNT;
      delete process.env.JAZZ_WORKER_SECRET;
    });

    // Get initial inbox state
    const crypto = await WasmCrypto.create();
    const peer = createWebSocketPeer({
      id: "upstream",
      websocket: new WebSocket(syncServer),
      role: "server",
    });

    const node = await LocalNode.withLoadedAccount({
      accountID: accountID as any,
      accountSecret: agentSecret as any,
      peersToLoadFrom: [peer],
      crypto,
      sessionID: crypto.newRandomSessionID(accountID as any),
    });

    const account = Account.fromNode(node);

    const profile = node
      .expectCoValueLoaded(account.$jazz.raw.get("profile")!)
      .getCurrentContent() as RawCoMap;

    const initialInboxId = profile.get("inbox");
    const initialInboxInvite = profile.get("inboxInvite");

    expect(initialInboxId).toBeDefined();
    expect(initialInboxInvite).toBeDefined();

    // Rotate the inbox
    await rotateWorkerInbox({ peer: syncServer });

    await waitFor(() => {
      // Verify that the inbox ID has changed
      expect(profile.get("inbox")).not.toBe(initialInboxId);

      // Verify that the inbox invite has changed
      expect(profile.get("inboxInvite")).not.toBe(initialInboxInvite);
    });
  });

  test("should throw error when environment variables are not set", async () => {
    // Set up sync server
    const server = await startSyncServer({
      host: serverDefaults.host,
      port: "0",
      inMemory: true,
      db: "",
    });

    onTestFinished(() => {
      server.close();
    });

    const address = server.address();
    if (typeof address !== "object" || address === null) {
      throw new Error("Server address is not an object");
    }

    const syncServer = `ws://localhost:${address.port}`;

    // Clear environment variables
    const originalAccount = process.env.JAZZ_WORKER_ACCOUNT;
    const originalSecret = process.env.JAZZ_WORKER_SECRET;

    delete process.env.JAZZ_WORKER_ACCOUNT;
    delete process.env.JAZZ_WORKER_SECRET;

    try {
      // Should throw error when environment variables are missing
      await expect(rotateWorkerInbox({ peer: syncServer })).rejects.toThrow(
        "JAZZ_WORKER_ACCOUNT and JAZZ_WORKER_SECRET environment variables must be set",
      );
    } finally {
      // Restore original environment variables
      if (originalAccount !== undefined) {
        process.env.JAZZ_WORKER_ACCOUNT = originalAccount;
      }
      if (originalSecret !== undefined) {
        process.env.JAZZ_WORKER_SECRET = originalSecret;
      }
    }
  });

  test("should throw error when only JAZZ_WORKER_ACCOUNT is set", async () => {
    // Set up sync server
    const server = await startSyncServer({
      host: serverDefaults.host,
      port: "0",
      inMemory: true,
      db: "",
    });

    onTestFinished(() => {
      server.close();
    });

    const address = server.address();
    if (typeof address !== "object" || address === null) {
      throw new Error("Server address is not an object");
    }

    const syncServer = `ws://localhost:${address.port}`;

    // Set only account ID
    const originalAccount = process.env.JAZZ_WORKER_ACCOUNT;
    const originalSecret = process.env.JAZZ_WORKER_SECRET;

    process.env.JAZZ_WORKER_ACCOUNT = "co_test_account";
    delete process.env.JAZZ_WORKER_SECRET;

    try {
      // Should throw error when JAZZ_WORKER_SECRET is missing
      await expect(rotateWorkerInbox({ peer: syncServer })).rejects.toThrow(
        "JAZZ_WORKER_ACCOUNT and JAZZ_WORKER_SECRET environment variables must be set",
      );
    } finally {
      // Restore original environment variables
      if (originalAccount !== undefined) {
        process.env.JAZZ_WORKER_ACCOUNT = originalAccount;
      } else {
        delete process.env.JAZZ_WORKER_ACCOUNT;
      }
      if (originalSecret !== undefined) {
        process.env.JAZZ_WORKER_SECRET = originalSecret;
      }
    }
  });

  test("should throw error when only JAZZ_WORKER_SECRET is set", async () => {
    // Set up sync server
    const server = await startSyncServer({
      host: serverDefaults.host,
      port: "0",
      inMemory: true,
      db: "",
    });

    onTestFinished(() => {
      server.close();
    });

    const address = server.address();
    if (typeof address !== "object" || address === null) {
      throw new Error("Server address is not an object");
    }

    const syncServer = `ws://localhost:${address.port}`;

    // Set only secret
    const originalAccount = process.env.JAZZ_WORKER_ACCOUNT;
    const originalSecret = process.env.JAZZ_WORKER_SECRET;

    delete process.env.JAZZ_WORKER_ACCOUNT;
    process.env.JAZZ_WORKER_SECRET = "sealerSecret_test";

    try {
      // Should throw error when JAZZ_WORKER_ACCOUNT is missing
      await expect(rotateWorkerInbox({ peer: syncServer })).rejects.toThrow(
        "JAZZ_WORKER_ACCOUNT and JAZZ_WORKER_SECRET environment variables must be set",
      );
    } finally {
      // Restore original environment variables
      if (originalAccount !== undefined) {
        process.env.JAZZ_WORKER_ACCOUNT = originalAccount;
      }
      if (originalSecret !== undefined) {
        process.env.JAZZ_WORKER_SECRET = originalSecret;
      } else {
        delete process.env.JAZZ_WORKER_SECRET;
      }
    }
  });
});
