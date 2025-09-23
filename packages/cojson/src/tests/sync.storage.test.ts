import {
  assert,
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from "vitest";

import { setMaxRecommendedTxSize } from "../config";
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
import { stableStringify } from "../jsonStringify";
import { determineValidTransactions } from "../permissions";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

describe("client with storage syncs with server", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

  beforeEach(async () => {
    SyncMessagesLog.clear();
    setMaxRecommendedTxSize(100 * 1024);
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

  test("persists meta information", async () => {
    const client = setupTestNode();

    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.core.makeTransaction([], "trusting", {
      meta: true,
    });

    await map.core.waitForSync();

    client.restart();

    client.addStorage({
      storage,
    });

    const loadedValue = await loadCoValueOrFail(client.node, map.id);

    expect(loadedValue.core.verifiedTransactions[0]?.tx.meta).toBe(
      `{"meta":true}`,
    );

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
        "client -> storage | LOAD Map sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("loading a branch from storage", async () => {
    const client = setupTestNode({
      connected: true,
    });
    const { storage } = client.addStorage();

    const group = client.node.createGroup();
    const map = group.createMap();
    const branchName = "feature-branch";

    map.set("key1", "value1");
    map.set("key2", "value2");

    const branch = await client.node.checkoutBranch(map.id, branchName);

    if (branch === "unavailable") {
      throw new Error("Branch is unavailable");
    }

    branch.set("branchKey", "branchValue");
    await branch.core.waitForSync();

    client.restart();
    client.addStorage({
      storage,
    });

    SyncMessagesLog.clear();

    const loadedBranch = await loadCoValueOrFail(client.node, branch.id);

    expect(branch.get("key1")).toBe("value1");
    expect(branch.get("key2")).toBe("value2");
    expect(branch.get("branchKey")).toBe("branchValue");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
        Branch: branch.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | LOAD Branch sessions: empty",
        "storage -> client | CONTENT Group header: true new: After: 0 New: 3",
        "storage -> client | CONTENT Map header: true new: After: 0 New: 3",
        "storage -> client | CONTENT Branch header: true new: After: 0 New: 2",
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

    client.node.syncManager.getPeers(map.id)[0]?.gracefulShutdown();

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
    setMaxRecommendedTxSize(1000);
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
    const chunks = 100;

    const value = "1".repeat(10);

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
        "client -> storage | CONTENT Map header: true new: After: 0 New: 20",
        "client -> server | CONTENT Map header: true new: After: 0 New: 20",
        "client -> storage | CONTENT Map header: false new: After: 20 New: 21",
        "client -> server | CONTENT Map header: false new: After: 20 New: 21",
        "client -> storage | CONTENT Map header: false new: After: 41 New: 21",
        "client -> server | CONTENT Map header: false new: After: 41 New: 21",
        "client -> storage | CONTENT Map header: false new: After: 62 New: 21",
        "client -> server | CONTENT Map header: false new: After: 62 New: 21",
        "client -> storage | CONTENT Map header: false new: After: 83 New: 17",
        "client -> server | CONTENT Map header: false new: After: 83 New: 17",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | KNOWN Map sessions: header/20",
        "server -> storage | CONTENT Map header: true new: After: 0 New: 20",
        "server -> client | KNOWN Map sessions: header/41",
        "server -> storage | CONTENT Map header: false new: After: 20 New: 21",
        "server -> client | KNOWN Map sessions: header/62",
        "server -> storage | CONTENT Map header: false new: After: 41 New: 21",
        "server -> client | KNOWN Map sessions: header/83",
        "server -> storage | CONTENT Map header: false new: After: 62 New: 21",
        "server -> client | KNOWN Map sessions: header/100",
        "server -> storage | CONTENT Map header: false new: After: 83 New: 17",
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
        "storage -> client | CONTENT Map header: true new: After: 0 New: 41 expectContentUntil: header/100",
        "client -> server | LOAD Map sessions: header/100",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> client | KNOWN Map sessions: header/100",
        "storage -> client | CONTENT Map header: true new: After: 41 New: 21",
        "storage -> client | CONTENT Map header: true new: After: 62 New: 21",
        "storage -> client | CONTENT Map header: true new: After: 83 New: 17",
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

  test("sessions with invalid assumptions should not be attempted to be stored", async () => {
    const client = setupTestNode();
    client.connectToSyncServer();

    const serverStorage = jazzCloud.node.storage!;

    const group = client.node.createGroup();
    const map = group.createMap();

    // Set an initial value on the map and let client/server sync
    map.set("hello", "world", "trusting");
    await map.core.waitForSync();

    // Verify that the initial known state is in server storage
    const initialKnownState = map.core.knownState();
    expect(serverStorage.getKnownState(map.id)).toEqual(initialKnownState);

    // Disable the next invocation of handleNewContent on the server
    vi.spyOn(
      jazzCloud.node.syncManager,
      "handleNewContent",
    ).mockImplementationOnce(() => {}); // noop

    // Update the map and let the client try to send the new content to the server.
    // The server won't receive it since it's disabled.
    map.set("hello", "world2", "trusting");
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Verify that the known state is still the same in server storage
    expect(serverStorage.getKnownState(map.id)).toEqual(initialKnownState);

    const storeSpy = vi.spyOn(serverStorage, "store");

    // Update the map once again, causing the client to send an update that the server
    // can't handle due to the previous transaction being missing. This will be followed
    // up by another update containing the missing transaction.
    map.set("hello", "world3", "trusting");
    await map.core.waitForSync();

    // We expect store() to have only been called once even though handleNewContent()
    // will have been called twice. The first handleNewContent call would have been
    // "unstorable" because of the missing transaction.
    expect(storeSpy).toHaveBeenCalledTimes(1);

    // Verify that the known state is updated in server storage
    expect(serverStorage.getKnownState(map.id)).toEqual(map.core.knownState());
  });

  test("sessions with invalid signatures should not be attempted to be stored", async () => {
    // Create a new client, to prepare the map
    const alice = setupTestNode({
      connected: true,
    });
    const group = alice.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();

    // Set an initial value on the map and let client/server sync
    map.set("hello", "world", "trusting");
    await map.core.waitForSync();

    const knwonStateAfterFirstUpdate = map.core.knownState();

    // Spawn another client, to add a new session and generate the content with an invalid signature
    const bob = setupTestNode({
      connected: true,
    });

    // Load the map and update it with a new session
    const mapOnBob = await loadCoValueOrFail(bob.node, map.id);
    mapOnBob.set("hello", "world2", "trusting");
    await mapOnBob.core.waitForSync();

    const client = setupTestNode();
    const { storage } = client.addStorage();

    SyncMessagesLog.clear(); // We want to focus on the sync messages happening from now

    // Import the group in the client, to have the dependencies availble and test that the import persists on storage
    const groupContent = group.core.verified.newContentSince(undefined)?.[0];
    assert(groupContent);
    client.node.syncManager.handleNewContent(groupContent, "import");
    expect(storage.getKnownState(groupContent.id)).toEqual(
      group.core.knownState(),
    );

    // Export the map content with the two sessions
    const mapContent = mapOnBob.core.verified.newContentSince(undefined)?.[0];
    assert(mapContent);

    // Tamper Bob's session
    const invalidMapContent = structuredClone(mapContent);
    invalidMapContent.new[bob.node.currentSessionID]!.newTransactions.push({
      privacy: "trusting",
      changes: stableStringify([{ op: "set", key: "hello", value: "updated" }]),
      madeAt: Date.now(),
    });
    client.node.syncManager.handleNewContent(invalidMapContent, "import");

    // We should store only Alice's session, because Bob's session is invalid
    expect(client.node.storage?.getKnownState(map.id)).toEqual(
      knwonStateAfterFirstUpdate,
    );

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });

  test("should store values with no transactions", async () => {
    const alice = setupTestNode({
      connected: true,
    });
    const group = alice.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();

    await map.core.waitForSync();

    const bob = setupTestNode();
    bob.connectToSyncServer({
      ourName: "bob",
    });
    const { storage } = bob.addStorage({
      ourName: "bob",
    });

    SyncMessagesLog.clear(); // We want to focus on the sync messages happening from now

    await loadCoValueOrFail(bob.node, map.id);

    // The map should be stored in bob's storage
    expect(storage.getKnownState(map.id)).toEqual({
      header: true,
      id: map.id,
      sessions: {},
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "bob -> storage | LOAD Map sessions: empty",
        "storage -> bob | KNOWN Map sessions: empty",
        "bob -> server | LOAD Map sessions: empty",
        "server -> bob | CONTENT Group header: true new: After: 0 New: 5",
        "server -> bob | CONTENT Map header: true new: ",
        "bob -> server | KNOWN Group sessions: header/5",
        "bob -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "bob -> server | KNOWN Map sessions: header/0",
        "bob -> storage | CONTENT Map header: true new: ",
      ]
    `);
  });

  test("when falling back to network, we should never emit unavailable", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();
    group.addMember("everyone", "reader");
    map.set("hello", "world");

    await map.core.waitForSync();

    const newSession = client.spawnNewSession();
    newSession.addStorage({
      ourName: "newSession",
    });

    const coValue = newSession.node.getCoValue(map.id);
    const spy = vi.fn();
    coValue.subscribe((core) => {
      spy(core.loadingState);
    });

    await newSession.node.loadCoValueCore(map.id, undefined, true);

    expect(spy).not.toHaveBeenCalledWith("unavailable");
    expect(coValue.loadingState).toBe("available");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | CONTENT Map header: true new: ",
        "client -> server | CONTENT Group header: false new: After: 3 New: 2",
        "client -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> client | KNOWN Group sessions: header/3",
        "server -> storage | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | KNOWN Map sessions: header/0",
        "server -> storage | CONTENT Map header: true new: ",
        "server -> client | KNOWN Group sessions: header/5",
        "server -> storage | CONTENT Group header: false new: After: 3 New: 2",
        "server -> client | KNOWN Map sessions: header/1",
        "server -> storage | CONTENT Map header: false new: After: 0 New: 1",
        "newSession -> storage | LOAD Map sessions: empty",
        "storage -> newSession | KNOWN Map sessions: empty",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/5",
        "newSession -> storage | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Map sessions: header/1",
        "newSession -> storage | CONTENT Map header: true new: After: 0 New: 1",
      ]
    `);
  });
});
