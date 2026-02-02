import { beforeEach, describe, expect, test } from "vitest";
import { RawCoMap } from "../exports";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  setupTestNode,
  waitFor,
} from "./testUtils";
import {
  setStorageReconciliationBatchSize,
  STORAGE_RECONCILIATION_CONFIG,
} from "../config";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

let jazzCloud: ReturnType<typeof setupTestNode>;
const originalBatchSize = STORAGE_RECONCILIATION_CONFIG.BATCH_SIZE;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
  setStorageReconciliationBatchSize(originalBatchSize);
});

describe("full storage reconciliation", () => {
  test("startStorageReconciliation sends 'reconcile' message, server responds with 'known' messages for missing CoValues", async () => {
    const client = setupTestNode();
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    const anotherClient = setupTestNode();
    anotherClient.addStorage({ storage });
    anotherClient.connectToSyncServer({
      persistent: true,
      skipReconciliation: true,
    });

    SyncMessagesLog.clear();

    const serverPeer = Object.values(anotherClient.node.syncManager.peers).find(
      (p) => p.role === "server" && p.persistent,
    )!;
    anotherClient.node.syncManager.startStorageReconciliation(serverPeer);

    const pendingReconciliationAck =
      anotherClient.node.syncManager.pendingReconciliationAck;
    expect(pendingReconciliationAck.size).toEqual(1);

    await waitFor(() => pendingReconciliationAck.size === 0);

    const messages = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });
    expect(messages).toMatchInlineSnapshot(`
      [
        "client -> storage | GET_KNOWN_STATE Group",
        "storage -> client | GET_KNOWN_STATE_RESULT Group sessions: header/3",
        "client -> storage | GET_KNOWN_STATE Map",
        "storage -> client | GET_KNOWN_STATE_RESULT Map sessions: header/1",
        "client -> server | RECONCILE",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "server -> client | RECONCILE_ACK",
        "client -> storage | LOAD Group sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("startStorageReconciliation sends 'reconcile' message, server responds with 'known' messages for outdated CoValues", async () => {
    const client = setupTestNode();
    const { storage } = client.addStorage();
    client.connectToSyncServer({ persistent: true });

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    map.set("hello", "world2", "trusting");

    // Restart the client before the latest change is synced to the sync server
    await client.restart();
    client.addStorage({ storage });
    client.connectToSyncServer({ persistent: true, skipReconciliation: true });

    SyncMessagesLog.clear();

    const serverPeer = Object.values(client.node.syncManager.peers).find(
      (p) => p.role === "server" && p.persistent,
    )!;
    client.node.syncManager.startStorageReconciliation(serverPeer);

    const pendingReconciliationAck =
      client.node.syncManager.pendingReconciliationAck;
    expect(pendingReconciliationAck.size).toEqual(1);

    await waitFor(() => pendingReconciliationAck.size === 0);

    const messages = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });
    expect(messages).toMatchInlineSnapshot(`
      [
        "client -> storage | GET_KNOWN_STATE Group",
        "storage -> client | GET_KNOWN_STATE_RESULT Group sessions: header/3",
        "client -> storage | GET_KNOWN_STATE Map",
        "storage -> client | GET_KNOWN_STATE_RESULT Map sessions: header/2",
        "client -> server | RECONCILE",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> client | RECONCILE_ACK",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | LOAD Group sessions: header/3",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 2",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("pendingReconciliationAck is cleared when 'reconcile-ack' is received", async () => {
    const client = setupTestNode();
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    const anotherClient = setupTestNode();
    anotherClient.addStorage({ storage });
    anotherClient.connectToSyncServer({
      persistent: true,
      skipReconciliation: true,
    });

    const serverPeer = Object.values(anotherClient.node.syncManager.peers).find(
      (p) => p.role === "server" && p.persistent,
    )!;
    anotherClient.node.syncManager.startStorageReconciliation(serverPeer);

    expect(
      anotherClient.node.syncManager.pendingReconciliationAck.size,
    ).toBeGreaterThan(0);

    await waitFor(
      () => anotherClient.node.syncManager.pendingReconciliationAck.size === 0,
    );
  });

  test("in-memory CoValues are not reconciled", async () => {
    const client = setupTestNode();
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    const anotherClient = setupTestNode();
    anotherClient.addStorage({ storage });
    anotherClient.connectToSyncServer({
      persistent: true,
      skipReconciliation: true,
    });

    const group2 = anotherClient.node.createGroup();
    const map2 = group2.createMap();
    map2.set("hello2", "world2", "trusting");

    await map2.core.waitForSync();

    SyncMessagesLog.clear();

    const serverPeer = Object.values(anotherClient.node.syncManager.peers).find(
      (p) => p.role === "server" && p.persistent,
    )!;
    anotherClient.node.syncManager.startStorageReconciliation(serverPeer);

    const pendingReconciliationAck =
      anotherClient.node.syncManager.pendingReconciliationAck;
    expect(pendingReconciliationAck.size).toEqual(1);

    await waitFor(() => pendingReconciliationAck.size === 0);

    const messages = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });
    // In-memory CoValues are skipped
    expect(messages).toMatchInlineSnapshot(`
      [
        "client -> storage | GET_KNOWN_STATE Group",
        "storage -> client | GET_KNOWN_STATE_RESULT Group sessions: header/3",
        "client -> storage | GET_KNOWN_STATE Map",
        "storage -> client | GET_KNOWN_STATE_RESULT Map sessions: header/1",
        "client -> server | RECONCILE",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "server -> client | RECONCILE_ACK",
        "client -> storage | LOAD Group sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("'reconcile' message is not sent if there are no CoValues to reconcile", async () => {
    const client = setupTestNode({ connected: true });
    client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    SyncMessagesLog.clear();

    // CoValue is in memory, so it will be skipped
    const serverPeer = Object.values(client.node.syncManager.peers).find(
      (p) => p.role === "server" && p.persistent,
    )!;
    client.node.syncManager.startStorageReconciliation(serverPeer);

    // Wait for reconciliation to complete
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(client.node.syncManager.pendingReconciliationAck.size).toEqual(0);
    const messages = SyncMessagesLog.getMessages({
      Group: group.core,
      Map: map.core,
    });
    expect(messages).toMatchInlineSnapshot(`[]`);
  });

  test("sends reconcile messages for each batch, each batch gets reconcile-ack", async () => {
    setStorageReconciliationBatchSize(2);

    const client = setupTestNode();
    client.connectToSyncServer({ persistent: true });
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const maps: RawCoMap[] = [];
    for (let i = 0; i < 4; i++) {
      const m = group.createMap();
      m.set("i", i, "trusting");
      maps.push(m);
    }

    await Promise.all(maps.map((m) => m.core.waitForSync()));

    SyncMessagesLog.clear();

    const anotherClient = setupTestNode();
    anotherClient.connectToSyncServer({ persistent: true });
    anotherClient.addStorage({ storage });

    const serverPeer = Object.values(anotherClient.node.syncManager.peers).find(
      (p) => p.role === "server" && p.persistent,
    )!;
    anotherClient.node.syncManager.startStorageReconciliation(serverPeer);

    await waitFor(
      () => anotherClient.node.syncManager.pendingReconciliationAck.size === 0,
    );

    const coValueMapping = Object.fromEntries([
      ["Group", group.core],
      ...maps.map((m, i) => [`Map${i}`, m.core]),
    ]);
    const messages = SyncMessagesLog.getMessages(coValueMapping);
    expect(messages).toMatchInlineSnapshot(`
      [
        "client -> storage | GET_KNOWN_STATE Group",
        "storage -> client | GET_KNOWN_STATE_RESULT Group sessions: header/3",
        "client -> storage | GET_KNOWN_STATE Map0",
        "storage -> client | GET_KNOWN_STATE_RESULT Map0 sessions: header/1",
        "client -> server | RECONCILE",
        "client -> storage | GET_KNOWN_STATE Map1",
        "storage -> client | GET_KNOWN_STATE_RESULT Map1 sessions: header/1",
        "client -> storage | GET_KNOWN_STATE Map2",
        "storage -> client | GET_KNOWN_STATE_RESULT Map2 sessions: header/1",
        "client -> server | RECONCILE",
        "client -> storage | GET_KNOWN_STATE Map3",
        "storage -> client | GET_KNOWN_STATE_RESULT Map3 sessions: header/1",
        "client -> server | RECONCILE",
        "server -> client | RECONCILE_ACK",
        "server -> client | RECONCILE_ACK",
        "server -> client | RECONCILE_ACK",
      ]
    `);
  });

  describe("scheduling", () => {
    test("full storage reconciliation is ran when adding a new persistent server peer", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage();

      const group = client.node.createGroup();
      const map = group.createMap();
      map.set("hello", "world", "trusting");

      await map.core.waitForSync();

      const anotherClient = setupTestNode();
      anotherClient.addStorage({ storage });

      SyncMessagesLog.clear();

      // Connecting to the sync server will trigger a full storage reconciliation
      anotherClient.connectToSyncServer({
        persistent: true,
      });

      const pendingReconciliationAck =
        anotherClient.node.syncManager.pendingReconciliationAck;
      expect(pendingReconciliationAck.size).toEqual(1);

      await waitFor(() => pendingReconciliationAck.size === 0);

      const messages = SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      });
      expect(messages).toMatchInlineSnapshot(`
      [
        "client -> storage | GET_KNOWN_STATE Group",
        "storage -> client | GET_KNOWN_STATE_RESULT Group sessions: header/3",
        "client -> storage | GET_KNOWN_STATE Map",
        "storage -> client | GET_KNOWN_STATE_RESULT Map sessions: header/1",
        "client -> server | RECONCILE",
        "server -> client | KNOWN Group sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "server -> client | RECONCILE_ACK",
        "client -> storage | LOAD Group sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
    });
  });
});
