import { assert, beforeEach, describe, expect, test } from "vitest";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestAccount,
  setupTestNode,
  waitFor,
} from "./testUtils.js";
import { expectMap } from "../coValue.js";

// We want to simulate a real world communication that happens asynchronously
TEST_NODE_CONFIG.withAsyncPeers = true;

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("CoValueCore.isCompletelyDownloaded", () => {
  let admin: Awaited<ReturnType<typeof setupTestAccount>>;
  let bob: Awaited<ReturnType<typeof setupTestAccount>>;

  beforeEach(async () => {
    admin = await setupTestAccount({
      connected: true,
    });
    bob = await setupTestAccount({
      connected: true,
    });
  });

  describe("streaming from start", () => {
    test("should return false when the value itself is streaming", async () => {
      const group = admin.node.createGroup();

      const map = group.createMap();
      map.set("initial", "value");

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually and simulate the delay of the last chunk
      admin.disconnect();

      // Make the map large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        map.set(`key${i}`, "1".repeat(1024));
      }

      const content = map.core.verified.newContentSince(undefined);
      assert(content);
      expect(content.length).toBeGreaterThan(1);

      const lastChunk = content.pop();
      assert(lastChunk);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last one
      for (const chunk of content) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // The map should not be completely downloaded yet
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk
      bobSession.syncManager.handleNewContent(lastChunk, "import");

      // Wait for the notification to be scheduled and executed
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should return false when the owner of the value is streaming", async () => {
      const group = admin.node.createGroup();

      const map = group.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually
      admin.disconnect();

      // Make the group large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        // @ts-expect-error - test property is not part of the group shape
        group.set(`key${i}`, "1".repeat(1024));
      }

      const content = group.core.verified.newContentSince(undefined);
      assert(content);
      expect(content.length).toBeGreaterThan(1);

      const lastChunk = content.pop();
      assert(lastChunk);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last one
      for (const chunk of content) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // The map should not be completely downloaded because its owner (group) is still streaming
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk
      bobSession.syncManager.handleNewContent(lastChunk, "import");

      // Wait for the notification to be scheduled and executed
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should return false when the parent of the owner is streaming", async () => {
      const parentGroup = admin.node.createGroup();
      const childGroup = admin.node.createGroup();

      childGroup.extend(parentGroup);

      const map = childGroup.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually
      admin.disconnect();

      // Make the parent group large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        // @ts-expect-error - test property is not part of the group shape
        parentGroup.set(`key${i}`, "1".repeat(1024));
      }

      const content = parentGroup.core.verified.newContentSince(undefined);
      assert(content);
      expect(content.length).toBeGreaterThan(1);

      const lastChunk = content.pop();
      assert(lastChunk);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last one
      for (const chunk of content) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // The map should not be completely downloaded because the parent of its owner is still streaming
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk
      bobSession.syncManager.handleNewContent(lastChunk, "import");

      // Wait for the notification to be scheduled and executed
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should return false when the grandparent of the owner is streaming", async () => {
      const grandParentGroup = admin.node.createGroup();
      const parentGroup = admin.node.createGroup();
      const childGroup = admin.node.createGroup();

      parentGroup.extend(grandParentGroup);
      childGroup.extend(parentGroup);

      const map = childGroup.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually
      admin.disconnect();

      // Make the grandparent group large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        // @ts-expect-error - test property is not part of the group shape
        grandParentGroup.set(`key${i}`, "1".repeat(1024));
      }

      const content = grandParentGroup.core.verified.newContentSince(undefined);
      assert(content);
      expect(content.length).toBeGreaterThan(1);

      const lastChunk = content.pop();
      assert(lastChunk);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last one
      for (const chunk of content) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // The map should not be completely downloaded because the grandparent of its owner is still streaming
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk
      bobSession.syncManager.handleNewContent(lastChunk, "import");

      // Wait for the notification to be scheduled and executed
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should return false when multiple parents of the owner are streaming", async () => {
      const parentGroup1 = admin.node.createGroup();
      const parentGroup2 = admin.node.createGroup();
      const childGroup = admin.node.createGroup();

      childGroup.extend(parentGroup1);
      childGroup.extend(parentGroup2);

      const map = childGroup.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually
      admin.disconnect();

      // Make both parent groups large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        // @ts-expect-error - test property is not part of the group shape
        parentGroup1.set(`key1_${i}`, "1".repeat(1024));
        // @ts-expect-error - test property is not part of the group shape
        parentGroup2.set(`key2_${i}`, "1".repeat(1024));
      }

      const content1 = parentGroup1.core.verified.newContentSince(undefined);
      const content2 = parentGroup2.core.verified.newContentSince(undefined);
      assert(content1);
      assert(content2);
      expect(content1.length).toBeGreaterThan(1);
      expect(content2.length).toBeGreaterThan(1);

      const lastChunk1 = content1.pop();
      const lastChunk2 = content2.pop();
      assert(lastChunk1);
      assert(lastChunk2);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last ones from both parents
      for (const chunk of content1) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }
      for (const chunk of content2) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // The map should not be completely downloaded because both parents are still streaming
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk from first parent
      bobSession.syncManager.handleNewContent(lastChunk1, "import");

      // Still should not be completely downloaded because second parent is still streaming
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk from second parent
      bobSession.syncManager.handleNewContent(lastChunk2, "import");

      // Now it should be completely downloaded
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should return false when the source branch of the value is streaming", async () => {
      const group = admin.node.createGroup();
      const originalMap = group.createMap();

      // Add initial data to original map
      originalMap.set("key1", "value1");

      await originalMap.core.waitForSync();

      // Disconnect the admin node to sync the content manually
      admin.disconnect();

      // Make the original map large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        originalMap.set(`key${i}`, "1".repeat(1024));
      }

      // Create a branch from the original map
      const branch = expectMap(
        originalMap.core
          .createBranch("feature-branch", group.id)
          .getCurrentContent(),
      );

      branch.set("branchKey", "branchValue");

      const originalMapContent =
        originalMap.core.verified.newContentSince(undefined);
      const branchContent = branch.core.verified.newContentSince(undefined);

      assert(originalMapContent);
      assert(branchContent);
      expect(originalMapContent.length).toBeGreaterThan(1);

      const lastOriginalMapChunk = originalMapContent.pop();
      assert(lastOriginalMapChunk);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last one from the original map
      for (const chunk of originalMapContent) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      // Feed all branch content
      for (const chunk of branchContent) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const branchOnBob = await loadCoValueOrFail(bobSession, branch.id);

      // The branch should not be completely downloaded because its source (original map) is still streaming
      expect(branchOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk from the original map
      bobSession.syncManager.handleNewContent(lastOriginalMapChunk, "import");

      // Now the branch should be completely downloaded
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(branchOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should return true when the value and all its dependencies are fully downloaded", async () => {
      const parentGroup = admin.node.createGroup();
      const childGroup = admin.node.createGroup();

      childGroup.extend(parentGroup);

      const map = childGroup.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      const mapOnBob = await loadCoValueOrFail(bob.node, map.id);

      // Everything should be fully synced
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should handle complex nested hierarchy with streaming at different levels", async () => {
      const grandParentGroup = admin.node.createGroup();
      const parentGroup = admin.node.createGroup();
      const childGroup = admin.node.createGroup();

      parentGroup.extend(grandParentGroup);
      childGroup.extend(parentGroup);

      const map = childGroup.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      // Disconnect the admin node
      admin.disconnect();

      // Make both the map and grandparent group large enough to require multiple chunks
      for (let i = 0; i < 100; i++) {
        map.set(`mapKey${i}`, "1".repeat(1024));
        // @ts-expect-error - test property is not part of the group shape
        grandParentGroup.set(`groupKey${i}`, "1".repeat(1024));
      }

      const mapContent = map.core.verified.newContentSince(undefined);
      const groupContent =
        grandParentGroup.core.verified.newContentSince(undefined);

      assert(mapContent);
      assert(groupContent);
      expect(mapContent.length).toBeGreaterThan(1);
      expect(groupContent.length).toBeGreaterThan(1);

      const lastMapChunk = mapContent.pop();
      const lastGroupChunk = groupContent.pop();
      assert(lastMapChunk);
      assert(lastGroupChunk);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last ones
      for (const chunk of mapContent) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }
      for (const chunk of groupContent) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // Should not be completely downloaded
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last map chunk
      bobSession.syncManager.handleNewContent(lastMapChunk, "import");

      // Still should not be completely downloaded because grandparent is still streaming
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last group chunk
      bobSession.syncManager.handleNewContent(lastGroupChunk, "import");

      // Now it should be completely downloaded
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });

    test("should emit an update when isCompletelyDownloaded status changes due to parent group streaming", async () => {
      const group = admin.node.createGroup();
      const map = group.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually
      admin.disconnect();

      // Make the group large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        // @ts-expect-error - test property is not part of the group shape
        group.set(`key${i}`, "1".repeat(1024));
      }

      const content = group.core.verified.newContentSince(undefined);
      assert(content);
      expect(content.length).toBeGreaterThan(1);

      const lastChunk = content.pop();
      assert(lastChunk);

      // Create a new session for bob
      const bobSession = bob.node;

      // Feed all chunks except the last one
      for (const chunk of content) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // Verify the map is not completely downloaded yet because its owner (group) is still streaming
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Set up a listener to track updates
      const updates: boolean[] = [];
      mapOnBob.core.subscribe((content) => {
        updates.push(content.isCompletelyDownloaded());
      });

      // Feed the last chunk
      bobSession.syncManager.handleNewContent(lastChunk, "import");

      // Wait for the notification to be scheduled and executed
      await new Promise<void>((resolve) => queueMicrotask(resolve));

      // Verify the CoValue is now completely downloaded
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);

      // Verify the listener was called with the new status
      expect(updates).toContain(true);
      expect(updates[updates.length - 1]).toBe(true);
    });
  });

  describe("streaming from the middle", () => {
    test("should return false when the value itself is streaming", async () => {
      const group = admin.node.createGroup();

      const map = group.createMap();
      map.set("initial", "value");

      const initialKnownState = map.core.knownState();

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually and simulate the delay of the last chunk
      admin.disconnect();

      // Create a new session for bob
      const bobSession = bob.node;

      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);

      // Make the map large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        map.set(`key${i}`, "1".repeat(1024));
      }

      const content = map.core.verified.newContentSince(initialKnownState);
      assert(content);
      expect(content.length).toBeGreaterThan(1);

      const lastChunk = content.pop();
      assert(lastChunk);

      // Feed all chunks except the last one
      for (const chunk of content) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      // The map should not be completely downloaded yet
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk
      bobSession.syncManager.handleNewContent(lastChunk, "import");

      // Wait for the notification to be scheduled and executed
      await waitFor(() => {
        expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
      });
    });

    test.skip("should return false when the owner of the value is streaming", async () => {
      const group = admin.node.createGroup();

      const map = group.createMap();
      map.set("test", "value");

      await map.core.waitForSync();

      // Disconnect the admin node to sync the content manually
      admin.disconnect();

      const bobSession = bob.node;
      const mapOnBob = await loadCoValueOrFail(bobSession, map.id);

      // The map should not be completely downloaded because its owner (group) is still streaming
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);

      // Make the group large enough to require multiple messages to be synced
      for (let i = 0; i < 100; i++) {
        // @ts-expect-error - test property is not part of the group shape
        group.set(`key${i}`, "1".repeat(1024));
      }

      const content = group.core.verified.newContentSince(undefined);
      assert(content);
      expect(content.length).toBeGreaterThan(1);

      const lastChunk = content.pop();
      assert(lastChunk);

      // Feed all chunks except the last one
      for (const chunk of content) {
        bobSession.syncManager.handleNewContent(chunk, "import");
      }

      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(false);

      // Feed the last chunk
      bobSession.syncManager.handleNewContent(lastChunk, "import");

      // Wait for the notification to be scheduled and executed
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(mapOnBob.core.isCompletelyDownloaded()).toBe(true);
    });
  });
});
