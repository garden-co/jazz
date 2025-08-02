import {
  assert,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from "vitest";

import { emptyKnownState } from "../exports";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  createTestMetricReader,
  loadCoValueOrFail,
  setupTestNode,
  tearDownTestMetricReader,
  waitFor,
} from "./testUtils";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

describe("client with storage syncs with server", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

  beforeEach(async () => {
    SyncMessagesLog.clear();
    jazzCloud = setupTestNode({
      isSyncServer: true,
    });
  });

  test("coValue loading (empty storage)", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    client.addStorage();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | KNOWN Map sessions: header/1",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("coValue loading (synced storage)", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    const { storage } = client.addStorage();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await loadCoValueOrFail(client.node, map.id);

    client.restart();

    client.connectToSyncServer();
    client.addStorage({
      ourName: "client",
      storage,
    });

    SyncMessagesLog.clear();

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | LOAD Group sessions: header/3",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | LOAD Map sessions: header/1",
      ]
    `);
  });

  test("coValue with parent groups loading", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    client.addStorage();

    const group = jazzCloud.node.createGroup();
    const parentGroup = jazzCloud.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    group.extend(parentGroup);

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN ParentGroup sessions: header/6",
        "client -> storage | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Map sessions: header/1",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("updating a coValue while offline", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();
    client.addStorage();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    client.node.syncManager.getPeers()[0]?.gracefulShutdown();

    SyncMessagesLog.clear();
    map.set("hello", "updated", "trusting");

    client.connectToSyncServer();

    await map.core.waitForSync();

    expect(mapOnClient.get("hello")).toEqual("updated");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
        "client -> storage | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
        "client -> storage | CONTENT Map header: false new: After: 1 New: 1",
      ]
    `);
  });
});

describe("client syncs with a server with storage", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;
  let metricReader: ReturnType<typeof createTestMetricReader>;

  beforeEach(async () => {
    SyncMessagesLog.clear();
    metricReader = createTestMetricReader();
    jazzCloud = setupTestNode({
      isSyncServer: true,
    });
    jazzCloud.addStorage({
      ourName: "server",
    });
  });

  afterEach(() => {
    tearDownTestMetricReader();
  });

  test("coValue uploading", async () => {
    const client = setupTestNode();

    client.connectToSyncServer();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await map.core.waitForSync();

    const mapOnServer = await loadCoValueOrFail(jazzCloud.node, map.id);
    expect(mapOnServer.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("loading a large coValue from storage", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServer: jazzCloud.node,
    });

    const { storage } = client.addStorage({
      ourName: "client",
    });

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    // Generate a large amount of data (about 100MB)
    const dataSize = 1 * 200 * 1024;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      largeMap.set(key, value, "trusting");
    }

    await largeMap.core.waitForSync();

    // Test streaming counter during initial sync
    // The streaming counter should be 0 after the sync is complete
    const streamingCounterAfterSync = await metricReader.getMetricValue(
      "jazz.storage.streaming",
    );
    expect(streamingCounterAfterSync).toBe(0);

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | CONTENT Group header: true new: After: 0 New: 5",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 73",
        "client -> server | CONTENT Map header: true new: After: 0 New: 73",
        "client -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server | CONTENT Map header: false new: After: 73 New: 73",
        "client -> storage | CONTENT Map header: false new: After: 146 New: 54",
        "client -> server | CONTENT Map header: false new: After: 146 New: 54",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | KNOWN Map sessions: header/73",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 73",
        "server -> client | KNOWN Map sessions: header/146",
        "server -> storage | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | KNOWN Map sessions: header/200",
        "server -> storage | CONTENT Map header: false new: After: 146 New: 54",
      ]
    `);

    SyncMessagesLog.clear();

    client.restart();

    client.connectToSyncServer({
      ourName: "client",
      syncServer: jazzCloud.node,
    });

    client.addStorage({
      ourName: "client",
      storage,
    });

    // Test streaming counter before loading the large coValue
    const streamingCounterBeforeLoad = await metricReader.getMetricValue(
      "jazz.storage.streaming",
    );
    expect(streamingCounterBeforeLoad).toBe(0);

    const promise = loadCoValueOrFail(client.node, largeMap.id);

    // Test streaming counter during loading (should be 1 during streaming)
    const streamingCounterDuringLoad = await metricReader.getMetricValue(
      "jazz.storage.streaming",
    );
    expect(streamingCounterDuringLoad).toBe(1);

    const mapOnClient2 = await promise;
    await mapOnClient2.core.waitForSync();

    // Test streaming counter after loading is complete (should be 0)
    await waitFor(async () => {
      const streamingCounterAfterLoad = await metricReader.getMetricValue(
        "jazz.storage.streaming",
      );
      expect(streamingCounterAfterLoad).toBe(0);
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | LOAD Group sessions: header/5",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 73 expectContentUntil: header/200",
        "client -> server | LOAD Map sessions: header/200",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> client | KNOWN Map sessions: header/200",
        "storage -> client | CONTENT Map header: true new: After: 73 New: 73",
        "storage -> client | CONTENT Map header: true new: After: 146 New: 54",
      ]
    `);
  });

  test("storing stale data should not compromise the signatures", async () => {
    const client = setupTestNode();

    client.connectToSyncServer({
      syncServer: jazzCloud.node,
    });

    const { storage } = client.addStorage({
      ourName: "client",
    });

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    // Generate a large amount of data (about 100MB)
    const dataSize = 1 * 200 * 1024;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      largeMap.set(key, value, "trusting");
    }

    await largeMap.core.waitForSync();

    const newContentChunks = largeMap.core.verified.newContentSince(
      emptyKnownState(largeMap.id),
    );

    assert(newContentChunks);
    assert(newContentChunks.length > 1);

    const correctionSpy = vi.fn();

    client.node.storage?.store(newContentChunks[1]!, correctionSpy);

    expect(correctionSpy).not.toHaveBeenCalled();

    client.restart();

    client.connectToSyncServer({
      ourName: "client",
      syncServer: jazzCloud.node,
    });

    client.addStorage({
      ourName: "client",
      storage,
    });

    const mapOnClient2 = await loadCoValueOrFail(client.node, largeMap.id);

    await waitFor(async () => {
      expect(mapOnClient2.core.knownState()).toEqual(
        largeMap.core.knownState(),
      );
    });
  });
});
