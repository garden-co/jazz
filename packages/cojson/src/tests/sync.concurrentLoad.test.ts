import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
  CO_VALUE_LOADING_CONFIG,
  setMaxInFlightLoadsPerPeer,
} from "../config.js";
import {
  blockMessageTypeOnOutgoingPeer,
  loadCoValueOrFail,
  setupTestNode,
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  waitFor,
} from "./testUtils.js";

let jazzCloud: ReturnType<typeof setupTestNode>;

// Store original config values
let originalMaxInFlightLoads: number;
let originalTimeout: number;

beforeEach(async () => {
  // We want to simulate a real world communication that happens asynchronously
  TEST_NODE_CONFIG.withAsyncPeers = true;

  originalMaxInFlightLoads =
    CO_VALUE_LOADING_CONFIG.MAX_IN_FLIGHT_LOADS_PER_PEER;
  originalTimeout = CO_VALUE_LOADING_CONFIG.TIMEOUT;

  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

afterEach(() => {
  // Restore original config
  setMaxInFlightLoadsPerPeer(originalMaxInFlightLoads);
  CO_VALUE_LOADING_CONFIG.TIMEOUT = originalTimeout;
  vi.useRealTimers();
});

describe("concurrent load", () => {
  test("should throttle load requests when at capacity", async () => {
    setMaxInFlightLoadsPerPeer(2);

    const client = setupTestNode({
      connected: false,
    });

    const { peerOnServer } = client.connectToSyncServer();

    // Create multiple CoValues on the server
    const group = jazzCloud.node.createGroup();
    const map1 = group.createMap();
    const map2 = group.createMap();
    const map3 = group.createMap();

    map1.set("key", "value1");
    map2.set("key", "value2");
    map3.set("key", "value3");

    // Block content responses to see the throttling effect
    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {});

    // Start loading all three
    const promise1 = client.node.loadCoValueCore(map1.id);
    const promise2 = client.node.loadCoValueCore(map2.id);
    const promise3 = client.node.loadCoValueCore(map3.id);

    // Wait for messages to be sent
    await new Promise((resolve) => setTimeout(resolve, 10));

    // Get the LOAD messages sent
    const loadMessages = SyncMessagesLog.messages.filter(
      (m) => m.msg.action === "load",
    );

    // Only 2 LOAD messages should have been sent (throttled)
    expect(loadMessages.length).toBe(2);

    // Unblock and let it complete
    blocker.unblock();
    blocker.sendBlockedMessages();

    await Promise.all([promise1, promise2, promise3]);

    // After completion, all 3 should have been loaded
    const allLoadMessages = SyncMessagesLog.messages.filter(
      (m) => m.msg.action === "load",
    );
    expect(allLoadMessages.length).toBe(3);

    // Verify both were loaded successfully despite throttling
    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map1: map1.core,
        Map2: map2.core,
        Map3: map3.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map1 sessions: empty",
        "client -> server | LOAD Map2 sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | CONTENT Map1 header: true new: After: 0 New: 1",
        "server -> client | CONTENT Map2 header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> server | KNOWN Map1 sessions: header/1",
        "client -> server | LOAD Map3 sessions: empty",
        "client -> server | KNOWN Map2 sessions: header/1",
        "server -> client | CONTENT Map3 header: true new: After: 0 New: 1",
        "client -> server | KNOWN Map3 sessions: header/1",
      ]
    `);
  });

  test("should process pending loads when capacity becomes available", async () => {
    setMaxInFlightLoadsPerPeer(1);

    const client = setupTestNode({
      connected: true,
    });

    // Create multiple CoValues on the server
    const group = jazzCloud.node.createGroup();
    const map1 = group.createMap();
    const map2 = group.createMap();

    map1.set("key", "value1", "trusting");
    map2.set("key", "value2", "trusting");

    // Load both sequentially due to throttling
    const [result1, result2] = await Promise.all([
      loadCoValueOrFail(client.node, map1.id),
      loadCoValueOrFail(client.node, map2.id),
    ]);

    expect(result1.get("key")).toBe("value1");
    expect(result2.get("key")).toBe("value2");

    // Verify both were loaded successfully despite throttling
    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map1: map1.core,
        Map2: map2.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map1 sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "server -> client | CONTENT Map1 header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/3",
        "client -> server | KNOWN Map1 sessions: header/1",
        "client -> server | LOAD Map2 sessions: empty",
        "server -> client | CONTENT Map2 header: true new: After: 0 New: 1",
        "client -> server | KNOWN Map2 sessions: header/1",
      ]
    `);
  });

  test("should prioritize unavailable CoValues over available ones", async () => {
    setMaxInFlightLoadsPerPeer(1);

    const client = setupTestNode({
      connected: true,
    });

    // Create CoValues on the server
    const group = jazzCloud.node.createGroup();
    const map1 = group.createMap();
    const map2 = group.createMap();
    const map3 = group.createMap();

    map1.set("key", "value1", "trusting");
    map2.set("key", "value2", "trusting");
    map3.set("key", "value3", "trusting");

    // First, load map1 to make it "available" locally
    await loadCoValueOrFail(client.node, map1.id);

    SyncMessagesLog.clear();

    // Update map1 on server (so client has stale version)
    map1.set("key", "updated1", "trusting");

    // Now load map2 (unavailable) and reload map1 (available but outdated)
    // map2 should be prioritized
    const [result1, result2] = await Promise.all([
      loadCoValueOrFail(client.node, map1.id), // Available, lower priority
      loadCoValueOrFail(client.node, map2.id), // Unavailable, higher priority
    ]);

    // Both should succeed
    expect(result2.get("key")).toBe("value2");

    // map2 (unavailable) should have been loaded first
    const loadMessages = SyncMessagesLog.messages.filter(
      (m) => m.msg.action === "load",
    );
    expect(loadMessages.length).toBeGreaterThanOrEqual(1);
    // The first load should be for map2 (unavailable, high priority)
    expect(loadMessages[0]?.msg).toMatchObject({
      action: "load",
      id: map2.id,
    });
  });

  test("should handle high load with many concurrent requests", async () => {
    setMaxInFlightLoadsPerPeer(5);

    const client = setupTestNode({
      connected: true,
    });

    // Create many CoValues on the server
    const group = jazzCloud.node.createGroup();
    const maps = Array.from({ length: 20 }, (_, i) => {
      const map = group.createMap();
      map.set("index", i, "trusting");
      return map;
    });

    // Load all of them concurrently
    const results = await Promise.all(
      maps.map((map) => loadCoValueOrFail(client.node, map.id)),
    );

    // All should have been loaded successfully
    results.forEach((result, i) => {
      expect(result.get("index")).toBe(i);
    });
  });

  test("should timeout load requests that take too long", async () => {
    vi.useFakeTimers();
    setMaxInFlightLoadsPerPeer(1);
    CO_VALUE_LOADING_CONFIG.TIMEOUT = 1000;

    const client = setupTestNode({
      connected: false,
    });

    const { peerOnServer } = client.connectToSyncServer();

    // Create a CoValue on the server
    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Block content to simulate a slow/unresponsive server
    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: map.id,
    });

    const loadPromise = client.node.loadCoValueCore(map.id);

    // Advance past the timeout
    await vi.advanceTimersByTimeAsync(CO_VALUE_LOADING_CONFIG.TIMEOUT + 100);

    // The queue slot should be freed
    // The second retry attempt should happen after RETRY_DELAY
    await vi.advanceTimersByTimeAsync(
      CO_VALUE_LOADING_CONFIG.RETRY_DELAY + 100,
    );

    // Unblock to let retries succeed
    blocker.sendBlockedMessages();
    blocker.unblock();

    // Wait for the retry to complete
    await vi.advanceTimersByTimeAsync(100);

    const result = await loadPromise;

    // The retry should have succeeded (since we unblocked)
    expect(result.isAvailable()).toBe(true);
  });

  test("should free queue slots on disconnect", async () => {
    setMaxInFlightLoadsPerPeer(2);

    const client = setupTestNode({
      connected: false,
    });

    const { peerState, peerOnServer } = client.connectToSyncServer();

    // Create CoValues on the server
    const group = jazzCloud.node.createGroup();
    const map1 = group.createMap();
    const map2 = group.createMap();
    const map3 = group.createMap();

    // Block content to keep requests in-flight
    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {});

    // Start loading (will be in-flight)
    client.node.loadCoValueCore(map1.id);
    client.node.loadCoValueCore(map2.id);
    client.node.loadCoValueCore(map3.id);

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Disconnect
    peerState.gracefulShutdown();

    // Queue should be cleared
    // Reconnect and verify new requests can be sent
    client.connectToSyncServer();

    const result = await loadCoValueOrFail(client.node, map1.id);
    expect(result.get("key")).toBeUndefined(); // map1 was created without a key

    blocker.unblock();
  });

  test("should handle reconnection with pending loads", async () => {
    setMaxInFlightLoadsPerPeer(1);

    const client = setupTestNode({
      connected: false,
    });

    const { peerState, peerOnServer } = client.connectToSyncServer({
      persistent: true,
    });

    // Create a CoValue on the server
    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Block content to keep request in-flight
    blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {
      id: map.id,
    });

    // Start loading
    const loadPromise = client.node.loadCoValueCore(map.id);

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Disconnect
    peerState.gracefulShutdown();

    // Reconnect
    client.connectToSyncServer({
      persistent: true,
    });

    // The load should complete after reconnection
    const result = await loadPromise;
    expect(result.isAvailable()).toBe(true);
  });

  test("should maintain FIFO order for queued requests", async () => {
    setMaxInFlightLoadsPerPeer(1);

    const client = setupTestNode({
      connected: false,
    });

    const { peerOnServer } = client.connectToSyncServer();

    // Create CoValues on the server
    const group = jazzCloud.node.createGroup();
    const maps = Array.from({ length: 5 }, () => group.createMap());

    // Block content to build up the queue
    const blocker = blockMessageTypeOnOutgoingPeer(peerOnServer, "content", {});

    // Start loading all maps (first one goes in-flight, rest queued)
    const loadPromises = maps.map((map) => client.node.loadCoValueCore(map.id));

    await new Promise((resolve) => setTimeout(resolve, 10));

    // Get the LOAD messages before unblocking
    const loadMessagesBefore = SyncMessagesLog.messages.filter(
      (m) => m.msg.action === "load",
    );

    // Only 1 should be sent (at capacity)
    expect(loadMessagesBefore.length).toBe(1);
    expect(loadMessagesBefore[0]?.msg).toMatchObject({
      action: "load",
      id: maps[0]?.id,
    });

    // Unblock to process the queue
    blocker.sendBlockedMessages();
    blocker.unblock();

    await Promise.all(loadPromises);

    // Verify all LOAD messages were sent
    const allLoadMessages = SyncMessagesLog.messages.filter(
      (m) => m.msg.action === "load",
    );

    // All 5 should eventually be sent
    expect(allLoadMessages.length).toBe(5);

    // They should be in order (maps[0], maps[1], maps[2], maps[3], maps[4])
    for (let i = 0; i < allLoadMessages.length; i++) {
      expect(allLoadMessages[i]?.msg).toMatchObject({
        action: "load",
        id: maps[i]?.id,
      });
    }
  });
});
