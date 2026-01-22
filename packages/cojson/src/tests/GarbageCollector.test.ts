import { assert, beforeEach, describe, expect, test, vi } from "vitest";

import { setGarbageCollectorMaxAge } from "../config";
import {
  blockMessageTypeOnOutgoingPeer,
  TEST_NODE_CONFIG,
  setupTestAccount,
  setupTestNode,
} from "./testUtils";
import { createSyncStorage } from "./testStorage.js";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

beforeEach(() => {
  // We want to test what happens when the garbage collector kicks in and removes a coValue
  // We set the max age to -1 to make it remove everything
  setGarbageCollectorMaxAge(-1);

  setupTestNode({ isSyncServer: true });
});

describe("garbage collector", () => {
  test("coValues are garbage collected when maxAge is reached", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.connectToSyncServer();
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await client.node.syncManager.waitForAllCoValuesSync();

    client.node.garbageCollector?.collect();

    const coValue = client.node.getCoValue(map.id);

    expect(coValue.isAvailable()).toBe(false);
  });

  test("coValues are not garbage collected if they have listeners", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.connectToSyncServer();
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Add a listener to the map
    const unsubscribe = map.subscribe(() => {
      // This listener keeps the coValue alive
    });

    await client.node.syncManager.waitForAllCoValuesSync();

    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(true);

    // Clean up the listener
    unsubscribe();

    // The coValue should be collected after the listener is removed
    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(false);
  });

  test("coValues are not garbage collected if they are not synced with server peers", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();
    const { peer: serverPeer } = client.connectToSyncServer();
    // Block sync with server
    const blocker = blockMessageTypeOnOutgoingPeer(serverPeer, "content", {});

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await new Promise((resolve) => setTimeout(resolve, 10));

    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(true);

    // Resume sync with server
    blocker.sendBlockedMessages();
    blocker.unblock();
    await client.node.syncManager.waitForAllCoValuesSync();

    // The coValue should now be collected
    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(false);
  });

  test("coValues are garbage collected if there are no server peers", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();
    // Client is not connected to the sync server

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await client.node.syncManager.waitForAllCoValuesSync();

    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(false);
  });

  test("account coValues are not garbage collected if they have dependencies", async () => {
    const client = await setupTestAccount({
      // Add storage before creating the account so it's persisted
      storage: createSyncStorage({
        nodeName: "client",
        storageName: "storage",
      }),
    });
    // The account is created along with its profile, and the group that owns the profile
    const profile = client.node.expectProfileLoaded(client.accountID);
    const profileId = profile.id;
    const profileOwnerId = profile.group.id;

    client.connectToSyncServer();
    client.node.enableGarbageCollector();

    await client.node.syncManager.waitForAllCoValuesSync();

    // First collect removes the profile
    client.node.garbageCollector?.collect();
    expect(client.node.getCoValue(profileId).isAvailable()).toBe(false);
    expect(client.node.getCoValue(profileOwnerId).isAvailable()).toBe(true);
    expect(client.node.getCoValue(client.accountID).isAvailable()).toBe(true);

    // Second collect removes the profile owner
    client.node.garbageCollector?.collect();
    expect(client.node.getCoValue(profileOwnerId).isAvailable()).toBe(false);
    expect(client.node.getCoValue(client.accountID).isAvailable()).toBe(true);

    // Third collect removes the account
    client.node.garbageCollector?.collect();
    expect(client.node.getCoValue(client.accountID).isAvailable()).toBe(false);
  });

  test("group coValues are garbage collected if they have no dependencies", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.connectToSyncServer();
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();

    await client.node.syncManager.waitForAllCoValuesSync();

    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(group.id).isAvailable()).toBe(false);
  });

  test("group coValues are not garbage collected if they have dependencies", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await client.node.syncManager.waitForAllCoValuesSync();

    // First collect removes the map
    client.node.garbageCollector?.collect();
    expect(client.node.getCoValue(group.id).isAvailable()).toBe(true);
    expect(client.node.getCoValue(map.id).isAvailable()).toBe(false);

    // Second collect removes the group
    client.node.garbageCollector?.collect();
    expect(client.node.getCoValue(group.id).isAvailable()).toBe(false);
  });

  test("coValues are not garbage collected if the maxAge is not reached", async () => {
    vi.useFakeTimers();

    setGarbageCollectorMaxAge(1000);

    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();

    const garbageCollector = client.node.garbageCollector;

    assert(garbageCollector);

    await vi.advanceTimersByTimeAsync(100);

    const group = client.node.createGroup();
    const map1 = group.createMap();
    const map2 = group.createMap();

    await vi.advanceTimersByTimeAsync(800);

    // Access map1 again, to prevent it from being garbage collected
    map1.set("hello", "world", "trusting");

    await vi.advanceTimersByTimeAsync(300);

    garbageCollector.collect();

    const coValue = client.node.getCoValue(map1.id);
    expect(coValue.isAvailable()).toBe(true);

    const coValue2 = client.node.getCoValue(map2.id);
    expect(coValue2.isAvailable()).toBe(false);
  });
});
