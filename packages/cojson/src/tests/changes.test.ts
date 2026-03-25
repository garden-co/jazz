import { beforeEach, describe, expect, test } from "vitest";
import {
  CursorError,
  extractDirectRefIds,
  loadChangesCursor,
  subscribeToChanges,
} from "../changes.js";
import type { ChangesMessage, ChangesCursor } from "../changes.js";
import type { RawCoID } from "../ids.js";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils.js";

let jazzCloud: ReturnType<typeof setupTestNode>;

beforeEach(async () => {
  TEST_NODE_CONFIG.withAsyncPeers = true;
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("extractDirectRefIds", () => {
  test("extracts ref IDs from a CoMap", () => {
    const group = jazzCloud.node.createGroup();
    const child1 = group.createMap();
    const child2 = group.createMap();
    const root = group.createMap();
    root.set("ref1", child1.id, "trusting");
    root.set("ref2", child2.id, "trusting");
    root.set("name", "not a ref", "trusting");

    const refs = extractDirectRefIds(root);
    expect(refs).toContain(child1.id);
    expect(refs).toContain(child2.id);
    expect(refs.size).toBe(2);
  });

  test("extracts ref IDs from a CoList", () => {
    const group = jazzCloud.node.createGroup();
    const child1 = group.createMap();
    const child2 = group.createMap();
    const list = group.createList();
    list.append(child1.id);
    list.append(child2.id);
    list.append("not a ref");

    const refs = extractDirectRefIds(list);
    expect(refs).toContain(child1.id);
    expect(refs).toContain(child2.id);
    expect(refs.size).toBe(2);
  });
});

describe("loadChangesCursor", () => {
  test("creates a cursor with root and ref frontiers", async () => {
    const group = jazzCloud.node.createGroup();
    const child = group.createMap();
    child.set("key", "value", "trusting");
    const list = group.createList();
    list.append(child.id);

    const cursor = await loadChangesCursor(jazzCloud.node, list.id);

    expect(cursor.rootId).toBe(list.id);
    expect(cursor.frontiers[list.id]).toBeDefined();
    expect(cursor.frontiers[child.id]).toBeDefined();
    expect(Object.keys(cursor.frontiers).length).toBe(2);
  });

  test("rejects when root is unavailable", async () => {
    const { node: client } = setupTestNode({ connected: true });

    await expect(
      loadChangesCursor(client, "co_zFAKEID123" as RawCoID),
    ).rejects.toThrow();
  });
});

describe("subscribeToChanges", () => {
  test("throws CursorError for wrong rootId", () => {
    const { node: client } = setupTestNode({ connected: true });

    const cursor: ChangesCursor = {
      rootId: "co_zWrongRoot" as RawCoID,
      frontiers: {},
    };

    expect(() =>
      subscribeToChanges(
        client,
        "co_zCorrectRoot" as RawCoID,
        cursor,
        () => {},
      ),
    ).toThrow(CursorError);
  });

  test("detects changed items on resume", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const child = group.createMap();
    child.set("key", "value1", "trusting");
    const list = group.createList();
    list.append(child.id);

    // Create a client, load everything, take cursor
    const { node: client } = setupTestNode({ connected: true });
    await loadCoValueOrFail(client, list.id);
    await loadCoValueOrFail(client, child.id);

    const cursor = await loadChangesCursor(client, list.id);

    // Mutate on the server side after cursor
    child.set("key", "value2", "trusting");

    // Wait for sync
    await child.core.waitForSync();

    const batches: ChangesMessage[] = [];
    const sub = subscribeToChanges(client, list.id, cursor, (changes) => {
      batches.push(changes);
    });

    await waitFor(() => batches.length > 0);

    // The child should be in the changed set
    const allChanged = new Set<RawCoID>();
    for (const batch of batches) {
      for (const id of batch.changed) allChanged.add(id);
    }
    expect(allChanged).toContain(child.id);

    sub.unsubscribe();
  });

  test("detects added items on resume", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const list = group.createList();

    // Create a client, load, take cursor
    const { node: client } = setupTestNode({ connected: true });
    await loadCoValueOrFail(client, list.id);

    const cursor = await loadChangesCursor(client, list.id);

    // Add a new item on the server
    const newChild = group.createMap();
    newChild.set("key", "new", "trusting");
    list.append(newChild.id);
    await list.core.waitForSync();

    const batches: ChangesMessage[] = [];
    const sub = subscribeToChanges(client, list.id, cursor, (changes) => {
      batches.push(changes);
    });

    await waitFor(() => batches.length > 0);

    const allAdded = new Set<RawCoID>();
    for (const batch of batches) {
      for (const id of batch.added) allAdded.add(id);
    }
    expect(allAdded).toContain(newChild.id);

    sub.unsubscribe();
  });

  test("detects removed items on resume", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const child = group.createMap();
    const list = group.createList();
    list.append(child.id);

    // Create a client, load, take cursor
    const { node: client } = setupTestNode({ connected: true });
    await loadCoValueOrFail(client, list.id);
    await loadCoValueOrFail(client, child.id);

    const cursor = await loadChangesCursor(client, list.id);

    // Remove item on server
    list.delete(0);
    await list.core.waitForSync();

    const batches: ChangesMessage[] = [];
    const sub = subscribeToChanges(client, list.id, cursor, (changes) => {
      batches.push(changes);
    });

    await waitFor(() => batches.length > 0);

    const allRemoved = new Set<RawCoID>();
    for (const batch of batches) {
      for (const id of batch.removed) allRemoved.add(id);
    }
    expect(allRemoved).toContain(child.id);

    sub.unsubscribe();
  });

  test("live tail: emits changed when descendant mutates", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const child = group.createMap();
    child.set("key", "value1", "trusting");
    const list = group.createList();
    list.append(child.id);

    // Create a client, load everything, take cursor
    const { node: client } = setupTestNode({ connected: true });
    await loadCoValueOrFail(client, list.id);
    await loadCoValueOrFail(client, child.id);

    const cursor = await loadChangesCursor(client, list.id);

    const batches: ChangesMessage[] = [];
    const sub = subscribeToChanges(client, list.id, cursor, (changes) => {
      batches.push(changes);
    });

    // Wait for initial subscription setup then reset to capture only live changes
    await waitFor(() => {});
    batches.length = 0;

    // Mutate child on server
    child.set("key", "value2", "trusting");
    await child.core.waitForSync();

    await waitFor(() => batches.length > 0);
    const allChanged = new Set<RawCoID>();
    for (const batch of batches) {
      for (const id of batch.changed) allChanged.add(id);
    }
    expect(allChanged).toContain(child.id);

    sub.unsubscribe();
  });

  test("live tail: emits added when item is added to root", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const list = group.createList();

    const { node: client } = setupTestNode({ connected: true });
    await loadCoValueOrFail(client, list.id);

    const cursor = await loadChangesCursor(client, list.id);

    const batches: ChangesMessage[] = [];
    const sub = subscribeToChanges(client, list.id, cursor, (changes) => {
      batches.push(changes);
    });

    await waitFor(() => {});
    batches.length = 0;

    // Add item on server
    const newChild = group.createMap();
    list.append(newChild.id);
    await list.core.waitForSync();

    await waitFor(() => batches.length > 0);
    const allAdded = new Set<RawCoID>();
    for (const batch of batches) {
      for (const id of batch.added) allAdded.add(id);
    }
    expect(allAdded).toContain(newChild.id);

    sub.unsubscribe();
  });

  test("cursor is updated after emission", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const child = group.createMap();
    child.set("key", "value1", "trusting");
    const list = group.createList();
    list.append(child.id);

    const { node: client } = setupTestNode({ connected: true });
    await loadCoValueOrFail(client, list.id);
    await loadCoValueOrFail(client, child.id);

    const cursor = await loadChangesCursor(client, list.id);

    const sub = subscribeToChanges(client, list.id, cursor, () => {});

    // Initially undefined (no emission yet)
    expect(sub.cursor()).toBeUndefined();

    // Trigger a change
    child.set("key", "value2", "trusting");
    await child.core.waitForSync();
    await waitFor(() => sub.cursor() !== undefined);

    // Should have a cursor now
    const updatedCursor = sub.cursor();
    expect(updatedCursor).toBeDefined();
    expect(updatedCursor!.rootId).toBe(list.id);

    sub.unsubscribe();
  });

  test("nothing changed since cursor results in no emission", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");
    const child = group.createMap();
    child.set("key", "value1", "trusting");
    const list = group.createList();
    list.append(child.id);

    const { node: client } = setupTestNode({ connected: true });
    await loadCoValueOrFail(client, list.id);
    await loadCoValueOrFail(client, child.id);

    const cursor = await loadChangesCursor(client, list.id);

    // No mutations after cursor

    const batches: ChangesMessage[] = [];
    const sub = subscribeToChanges(client, list.id, cursor, (changes) => {
      batches.push(changes);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    // Should not have emitted anything since nothing changed
    expect(batches.length).toBe(0);

    sub.unsubscribe();
  });
});
