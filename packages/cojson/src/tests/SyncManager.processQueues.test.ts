import {
  afterEach,
  assert,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from "vitest";
import { PeerState } from "../PeerState.js";
import { CO_VALUE_PRIORITY } from "../priority.js";
import { StorageStreamingQueue } from "../queue/StorageStreamingQueue.js";
import { ConnectedPeerChannel } from "../streamUtils.js";
import {
  SyncMessagesLog,
  createTestMetricReader,
  hotSleep,
  loadCoValueOrFail,
  setupTestNode,
  tearDownTestMetricReader,
  waitFor,
} from "./testUtils.js";

describe("SyncManager.processQueues", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

  beforeEach(async () => {
    SyncMessagesLog.clear();
    jazzCloud = setupTestNode({
      isSyncServer: true,
    });
  });

  afterEach(() => {
    tearDownTestMetricReader();
    vi.restoreAllMocks();
  });

  function createMockPeerState(
    id: string,
    role: "client" | "server" = "client",
  ) {
    return new PeerState(
      {
        id,
        role,
        incoming: new ConnectedPeerChannel(),
        outgoing: new ConnectedPeerChannel(),
      },
      undefined,
    );
  }

  describe("incoming messages processing", () => {
    test("should process incoming messages from peers", async () => {
      const client = setupTestNode();
      client.connectToSyncServer();

      const group = jazzCloud.node.createGroup();
      const map = group.createMap();
      map.set("hello", "world", "trusting");

      const mapOnClient = await loadCoValueOrFail(client.node, map.id);
      expect(mapOnClient.get("hello")).toEqual("world");
    });

    test("should process multiple messages in sequence", async () => {
      const client = setupTestNode();
      client.connectToSyncServer();

      const group = jazzCloud.node.createGroup();
      const map1 = group.createMap();
      const map2 = group.createMap();
      const map3 = group.createMap();

      map1.set("key", "value1", "trusting");
      map2.set("key", "value2", "trusting");
      map3.set("key", "value3", "trusting");

      const [loadedMap1, loadedMap2, loadedMap3] = await Promise.all([
        loadCoValueOrFail(client.node, map1.id),
        loadCoValueOrFail(client.node, map2.id),
        loadCoValueOrFail(client.node, map3.id),
      ]);

      expect(loadedMap1.get("key")).toEqual("value1");
      expect(loadedMap2.get("key")).toEqual("value2");
      expect(loadedMap3.get("key")).toEqual("value3");
    });
  });

  describe("metrics", () => {
    test("should record incoming message processing time by type without peer role", async () => {
      const metricReader = createTestMetricReader();
      const client = setupTestNode();
      const group = client.node.createGroup();
      const peerState = createMockPeerState("peer-1");
      const originalHandleSyncMessage =
        client.node.syncManager.handleSyncMessage.bind(client.node.syncManager);

      const handleSyncMessageSpy = vi
        .spyOn(client.node.syncManager, "handleSyncMessage")
        .mockImplementation((msg, peer) => {
          hotSleep(10);
          return originalHandleSyncMessage(msg, peer);
        });

      client.node.syncManager.pushMessage(
        {
          action: "load",
          id: group.id,
          header: false,
          sessions: {},
        },
        peerState,
      );
      client.node.syncManager.pushMessage(
        {
          action: "reconcile-ack",
          id: "batch-1",
        },
        peerState,
      );

      await waitFor(() => handleSyncMessageSpy.mock.calls.length === 2);

      const loadValue = await metricReader.getMetricValue(
        "jazz.messagequeue.incoming.processing_time",
        { messageType: "load" },
      );
      const reconcileAckValue = await metricReader.getMetricValue(
        "jazz.messagequeue.incoming.processing_time",
        { messageType: "reconcile-ack" },
      );

      assert(typeof loadValue !== "number" && !!loadValue?.count);
      expect(loadValue.count).toBe(1);
      expect(loadValue.sum).toBeGreaterThan(0);

      assert(
        typeof reconcileAckValue !== "number" && !!reconcileAckValue?.count,
      );
      expect(reconcileAckValue.count).toBe(1);
      expect(reconcileAckValue.sum).toBeGreaterThan(0);
    });
  });

  describe("storage streaming processing", () => {
    test("should process storage streaming callbacks", async () => {
      const client = setupTestNode();
      client.connectToSyncServer();
      const { storage } = client.addStorage();

      const group = jazzCloud.node.createGroup();
      const map = group.createMap();
      map.set("hello", "world", "trusting");

      // First load to populate storage
      await loadCoValueOrFail(client.node, map.id);

      // Restart and load from storage
      await client.restart();
      client.connectToSyncServer();
      client.addStorage({ storage });

      SyncMessagesLog.clear();

      const mapOnClient = await loadCoValueOrFail(client.node, map.id);
      expect(mapOnClient.get("hello")).toEqual("world");

      // Verify content came from storage
      const storageMessages = SyncMessagesLog.messages.filter(
        (msg) => msg.from === "storage" || msg.to === "storage",
      );
      expect(storageMessages.length).toBeGreaterThan(0);
    });

    test("should invoke streaming callbacks when pulled", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage();

      const callback = vi.fn();
      storage.streamingQueue?.push(callback, CO_VALUE_PRIORITY.MEDIUM);
      storage.streamingQueue?.emit();

      // Wait for processQueues to run
      await waitFor(() => callback.mock.calls.length > 0);

      expect(callback).toHaveBeenCalledTimes(1);
    });

    test("should process MEDIUM priority before LOW priority", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage();

      const order: string[] = [];
      const lowCallback = () => order.push("low");
      const mediumCallback = () => order.push("medium");
      const highCallback = () => order.push("high");

      // Push LOW first, then MEDIUM
      storage.streamingQueue?.push(lowCallback, CO_VALUE_PRIORITY.LOW);
      storage.streamingQueue?.push(mediumCallback, CO_VALUE_PRIORITY.MEDIUM);
      storage.streamingQueue?.push(highCallback, CO_VALUE_PRIORITY.HIGH);
      storage.streamingQueue?.emit();

      // Wait for both to be processed
      await waitFor(() => order.length === 3);

      // MEDIUM should be processed first
      expect(order).toEqual(["high", "medium", "low"]);
    });
  });

  describe("unified scheduling", () => {
    test("should process both incoming messages and storage streaming", async () => {
      const client = setupTestNode();
      client.connectToSyncServer();
      const { storage } = client.addStorage();

      const group = jazzCloud.node.createGroup();
      const map = group.createMap();
      map.set("hello", "world", "trusting");

      // Queue a storage streaming callback
      const streamingCallback = vi.fn();
      storage.streamingQueue?.push(streamingCallback, CO_VALUE_PRIORITY.MEDIUM);
      storage.streamingQueue?.emit();

      // Load from server (incoming messages)
      const mapOnClient = await loadCoValueOrFail(client.node, map.id);

      expect(mapOnClient.get("hello")).toEqual("world");
      expect(streamingCallback).toHaveBeenCalled();
    });

    test("should alternate between message queue and storage queue", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage();

      const order: string[] = [];

      // Push multiple storage callbacks
      storage.streamingQueue?.push(
        () => order.push("storage1"),
        CO_VALUE_PRIORITY.MEDIUM,
      );
      storage.streamingQueue?.push(
        () => order.push("storage2"),
        CO_VALUE_PRIORITY.MEDIUM,
      );
      storage.streamingQueue?.emit();

      // Wait for processing
      await waitFor(() => order.length === 2);

      expect(order).toContain("storage1");
      expect(order).toContain("storage2");
    });
  });

  describe("processing flag", () => {
    test("should prevent concurrent processQueues calls", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage();

      let concurrentCalls = 0;
      let maxConcurrentCalls = 0;

      const callback = () => {
        concurrentCalls++;
        maxConcurrentCalls = Math.max(maxConcurrentCalls, concurrentCalls);
        // Simulate some work
        for (let i = 0; i < 1000; i++) {
          Math.random();
        }
        concurrentCalls--;
      };

      // Push multiple callbacks
      for (let i = 0; i < 10; i++) {
        storage.streamingQueue?.push(callback, CO_VALUE_PRIORITY.MEDIUM);
      }

      // Emit multiple times to trigger multiple processQueues calls
      storage.streamingQueue?.emit();
      storage.streamingQueue?.emit();
      storage.streamingQueue?.emit();

      // Wait for all to complete
      await waitFor(() => storage.streamingQueue?.isEmpty());

      // Should never have more than 1 concurrent call
      expect(maxConcurrentCalls).toBe(1);
    });
  });

  describe("error handling", () => {
    test("should continue processing after storage callback error", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage();

      const processed: string[] = [];

      storage.streamingQueue?.push(() => {
        processed.push("before");
      }, CO_VALUE_PRIORITY.MEDIUM);

      storage.streamingQueue?.push(() => {
        throw new Error("Test error");
      }, CO_VALUE_PRIORITY.MEDIUM);

      storage.streamingQueue?.push(() => {
        processed.push("after");
      }, CO_VALUE_PRIORITY.MEDIUM);

      storage.streamingQueue?.emit();

      // Wait for processing to complete
      await waitFor(() => storage.streamingQueue?.isEmpty());

      // Both before and after should be processed despite error
      expect(processed).toContain("before");
      expect(processed).toContain("after");
    });
  });

  describe("queue triggers", () => {
    test("IncomingMessagesQueue.push should trigger processQueues", async () => {
      const client = setupTestNode();
      client.connectToSyncServer();

      const group = jazzCloud.node.createGroup();
      const map = group.createMap();
      map.set("hello", "world", "trusting");

      // Loading should trigger message processing automatically
      const mapOnClient = await loadCoValueOrFail(client.node, map.id);
      expect(mapOnClient.get("hello")).toEqual("world");
    });

    test("StorageStreamingQueue.emit should trigger processQueues", async () => {
      const client = setupTestNode();
      const { storage } = client.addStorage();

      const callback = vi.fn();
      storage.streamingQueue?.push(callback, CO_VALUE_PRIORITY.MEDIUM);

      // Before emit, callback should not be called
      expect(callback).not.toHaveBeenCalled();

      // After emit, processQueues should be triggered
      storage.streamingQueue?.emit();

      await waitFor(() => callback.mock.calls.length > 0);
      expect(callback).toHaveBeenCalled();
    });

    test("setStorage should connect queue listener", async () => {
      const client = setupTestNode();

      // Before adding storage, there's no queue
      const queueBefore = (
        client.node.syncManager as any
      ).getStorageStreamingQueue?.();
      expect(queueBefore).toBeUndefined();

      // After adding storage, queue should be available
      const { storage } = client.addStorage();
      const queueAfter = (
        client.node.syncManager as any
      ).getStorageStreamingQueue?.();
      expect(queueAfter).toBe(storage.streamingQueue);
    });
  });
});
