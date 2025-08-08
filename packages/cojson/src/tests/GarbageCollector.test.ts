import { assert, beforeEach, describe, expect, test, vi } from "vitest";

import { setGarbageCollectorMaxAge } from "../config";
import { TEST_NODE_CONFIG, setupTestAccount, setupTestNode } from "./testUtils";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

beforeEach(() => {
  // We want to test what happens when the garbage collector kicks in and removes a coValue
  // We set the max age to -1 to make it remove everything
  setGarbageCollectorMaxAge(-1);
});

describe("garbage collector", () => {
  test("coValues are garbage collected when maxAge is reached", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    await new Promise((resolve) => setTimeout(resolve, 10));

    client.node.garbageCollector?.collect();

    const coValue = client.node.getCoValue(map.id);

    expect(coValue.isAvailable()).toBe(false);
  });

  test("coValues are not garbage collected if they have listeners", async () => {
    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    // Add a listener to the map
    const unsubscribe = map.subscribe(() => {
      // This listener keeps the coValue alive
    });

    await new Promise((resolve) => setTimeout(resolve, 10));

    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(true);

    // Clean up the listener
    unsubscribe();

    // The coValue should be collected after the listener is removed
    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(map.id).isAvailable()).toBe(false);
  });

  test("coValues are not garbage collected if they are a group or account", async () => {
    const client = await setupTestAccount();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();

    const group = client.node.createGroup();

    await new Promise((resolve) => setTimeout(resolve, 10));

    client.node.garbageCollector?.collect();

    expect(client.node.getCoValue(group.id).isAvailable()).toBe(true);
    expect(client.node.getCoValue(client.accountID).isAvailable()).toBe(true);
  });

  test("coValues are not garbage collected if the maxAge is not reached", async () => {
    setGarbageCollectorMaxAge(1000);

    const client = setupTestNode();

    client.addStorage({
      ourName: "client",
    });
    client.node.enableGarbageCollector();

    const garbageCollector = client.node.garbageCollector;

    assert(garbageCollector);

    const getCurrentTime = vi.spyOn(garbageCollector, "getCurrentTime");

    getCurrentTime.mockReturnValue(1);

    const group = client.node.createGroup();
    const map1 = group.createMap();
    const map2 = group.createMap();

    await new Promise((resolve) => setTimeout(resolve, 10));

    map1.set("hello", "world", "trusting");

    getCurrentTime.mockReturnValue(2000);

    await new Promise((resolve) => setTimeout(resolve, 10));

    garbageCollector.collect();

    const coValue = client.node.getCoValue(map1.id);

    expect(coValue.isAvailable()).toBe(true);

    const coValue2 = client.node.getCoValue(map2.id);

    expect(coValue2.isAvailable()).toBe(false);
  });
});
