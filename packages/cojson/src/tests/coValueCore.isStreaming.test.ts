import { assert, beforeEach, describe, expect, test } from "vitest";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils";
import type { RawCoMap } from "../exports";
import type { CoID } from "../coValue";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  // We want to simulate a real world communication that happens asynchronously
  TEST_NODE_CONFIG.withAsyncPeers = true;

  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("isStreaming", () => {
  test("loading a small value should not be streaming", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const newSession = client.spawnNewSession();
    const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

    // For a small value, isStreaming should always be false
    expect(mapInNewSession.core.isStreaming()).toBe(false);
  });

  test("loading a large value should be streaming until all chunks are sent", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();

    await group.core.waitForSync();
    client.disconnect();

    const map = group.createMap();

    // Generate a large amount of data that requires multiple chunks
    const dataSize = 1 * 1024 * 100;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      map.set(key, value, "trusting");
    }

    const newSession = client.spawnNewSession();

    await loadCoValueOrFail(client.node, group.id);

    const content = map.core.verified.newContentSince(undefined);
    assert(content);
    const lastChunk = content.pop();
    assert(lastChunk);

    for (const chunk of content) {
      newSession.node.syncManager.handleNewContent(chunk, "import");
    }

    const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

    expect(mapInNewSession.core.isStreaming()).toBe(true);

    newSession.node.syncManager.handleNewContent(lastChunk, "import");

    expect(mapInNewSession.core.isStreaming()).toBe(false);
  });

  test("loading a large content update should be streaming until all chunks are sent", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const map = group.createMap();

    map.set("hello", "world", "trusting");

    const knownState = map.core.knownState();

    await map.core.waitForSync();
    client.disconnect();

    // Generate a large amount of data that requires multiple chunks
    const dataSize = 1 * 1024 * 100;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      map.set(key, value, "trusting");
    }

    const newSession = client.spawnNewSession();

    await loadCoValueOrFail(client.node, group.id);

    const content = map.core.verified.newContentSince(knownState);
    assert(content);
    const lastChunk = content.pop();
    assert(lastChunk);

    const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

    for (const chunk of content) {
      newSession.node.syncManager.handleNewContent(chunk, "import");
    }

    expect(mapInNewSession.core.isStreaming()).toBe(true);

    newSession.node.syncManager.handleNewContent(lastChunk, "import");

    expect(mapInNewSession.core.isStreaming()).toBe(false);
  });

  test("streaming a large value between two clients should be streaming until all chunks are sent", async () => {
    const client = setupTestNode();
    client.connectToSyncServer({
      ourName: "initialClient",
    });
    const streamingClient = client.spawnNewSession();
    streamingClient.connectToSyncServer({
      ourName: "streamingClient",
    });

    const group = client.node.createGroup();

    await group.core.waitForSync();
    client.disconnect();

    const map = group.createMap();

    // Generate a large amount of data that requires multiple chunks
    const dataSize = 1 * 1024 * 100;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      map.set(key, value, "trusting");
    }

    const loadingClient = client.spawnNewSession();
    loadingClient.connectToSyncServer({
      ourName: "loadingClient",
    });

    await loadCoValueOrFail(loadingClient.node, group.id);

    const content = map.core.verified.newContentSince(undefined);
    assert(content);
    const lastChunk = content.pop();
    assert(lastChunk);

    for (const chunk of content) {
      streamingClient.node.syncManager.handleNewContent(chunk, "import");
    }

    await streamingClient.node.syncManager.waitForAllCoValuesSync();

    const mapInLoadingClient = await loadCoValueOrFail(
      loadingClient.node,
      map.id,
    );

    expect(mapInLoadingClient.core.isStreaming()).toBe(true);

    streamingClient.node.syncManager.handleNewContent(lastChunk, "import");

    await waitFor(() => {
      expect(mapInLoadingClient.core.knownState()).toEqual(
        map.core.knownState(),
      );
    });

    expect(mapInLoadingClient.core.isStreaming()).toBe(false);
  });

  test("should be false when getting streaming content that is already in the known state", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();

    await group.core.waitForSync();

    const map = group.createMap();

    // Generate a large amount of data that requires multiple chunks
    const dataSize = 1 * 1024 * 100;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      map.set(key, value, "trusting");
    }

    await map.core.waitForSync();
    const newSession = client.spawnNewSession();

    const mapInNewSession1 = await loadCoValueOrFail(newSession.node, map.id);
    await mapInNewSession1.core.waitForFullStreaming();

    const content = map.core.verified.newContentSince(undefined);
    assert(content);
    const lastChunk = content.pop();
    assert(lastChunk);

    for (const chunk of content) {
      newSession.node.syncManager.handleNewContent(chunk, "import");
    }

    const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

    expect(mapInNewSession.core.isStreaming()).toBe(false);
  });

  test("should be false when getting streaming content that's not really streaming", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();

    await group.core.waitForSync();
    client.disconnect();

    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const newSession = client.spawnNewSession();

    await loadCoValueOrFail(client.node, group.id);

    const content = map.core.verified.newContentSince(undefined);
    assert(content);

    content[0]!.expectContentUntil = map.core.knownState().sessions;

    for (const chunk of content) {
      newSession.node.syncManager.handleNewContent(chunk, "import");
    }

    const mapInNewSession = await loadCoValueOrFail(newSession.node, map.id);

    expect(mapInNewSession.core.isStreaming()).toBe(false);
  });

  test("mixed updates should not leave isStreaming to true (3 sessions)", async () => {
    const aliceLaptop = await setupTestAccount({
      connected: true,
    });

    const group = aliceLaptop.node.createGroup();
    const map = group.createMap();

    map.set("count", 0, "trusting");
    map.set("count", 1, "trusting");

    await map.core.waitForSync();

    const alicePhone = await aliceLaptop.spawnNewSession();

    const mapOnPhone = await loadCoValueOrFail(alicePhone.node, map.id);

    mapOnPhone.set("count", 2, "trusting");
    mapOnPhone.set("count", 3, "trusting");
    mapOnPhone.set("count", 4, "trusting");

    await mapOnPhone.core.waitForSync();

    const aliceTablet = await alicePhone.spawnNewSession();
    const mapOnTablet = await loadCoValueOrFail(aliceTablet.node, map.id);

    mapOnTablet.set("count", 5, "trusting");
    mapOnTablet.set("count", 6, "trusting");
    mapOnTablet.set("count", 7, "trusting");

    await mapOnTablet.core.waitForSync();

    map.set("count", 8, "trusting");
    map.set("count", 9, "trusting");
    map.set("count", 10, "trusting");

    mapOnPhone.set("count", 11, "trusting");
    mapOnTablet.set("count", 12, "trusting");

    await map.core.waitForSync();
    await mapOnPhone.core.waitForSync();
    await mapOnTablet.core.waitForSync();

    expect(map.core.isStreaming()).toBe(false);
    expect(mapOnPhone.core.isStreaming()).toBe(false);
    expect(mapOnTablet.core.isStreaming()).toBe(false);

    const mapBranch = map.core.createBranch("test-branch");

    const aliceTv = await aliceTablet.spawnNewSession();

    const mapBranchOnTv = await loadCoValueOrFail(
      aliceTv.node,
      mapBranch.id as unknown as CoID<RawCoMap>,
    );

    mapBranchOnTv.set("count", 13, "trusting");
    mapBranchOnTv.set("count", 14, "trusting");
    mapBranchOnTv.set("count", 15, "trusting");

    await mapBranchOnTv.core.waitForSync();

    group.addMember("everyone", "reader");

    const bob = await setupTestAccount({
      connected: true,
    });

    const mapBranchOnBob = await loadCoValueOrFail(bob.node, mapBranchOnTv.id);

    expect(mapBranchOnBob.core.isStreaming()).toBe(false);
    expect(map.core.isStreaming()).toBe(false);
    expect(mapOnPhone.core.isStreaming()).toBe(false);
    expect(mapOnTablet.core.isStreaming()).toBe(false);

    expect(mapBranchOnBob.get("count")).toBe(15);
  });
});
