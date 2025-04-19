import { beforeEach, describe, expect, test } from "vitest";

import { expectMap } from "../coValue";
import { toSimplifiedMessages } from "./messagesTestUtils";
import {
  SyncMessagesLog,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";

let jazzCloud = setupTestNode({ isSyncServer: true });

beforeEach(async () => {
  SyncMessagesLog.clear();
  jazzCloud = setupTestNode({ isSyncServer: true });
});

describe("loading coValues from server", () => {
  test("coValue loading", async () => {
    const { node: client } = setupTestNode({
      connected: true,
    });

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 3",
        "client -> server | KNOWN Group sessions: header/3",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with parent groups loading", async () => {
    const client = setupTestNode({
      connected: true,
    });

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
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> server | KNOWN ParentGroup sessions: header/6",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Group sessions: header/5",
        "client -> server | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("updating a coValue while offline", async () => {
    const client = setupTestNode({
      connected: false,
    });

    const { peerState } = client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    peerState.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    SyncMessagesLog.clear();
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
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | LOAD Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("updating a coValue on both sides while offline", async () => {
    const client = setupTestNode({});

    const { peerState } = client.connectToSyncServer();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    const mapOnClient = await loadCoValueOrFail(client.node, map.id);

    peerState.gracefulShutdown();

    map.set("fromServer", "updated", "trusting");
    mapOnClient.set("fromClient", "updated", "trusting");

    SyncMessagesLog.clear();
    client.connectToSyncServer();

    await map.core.waitForSync();
    await mapOnClient.core.waitForSync();

    expect(mapOnClient.get("fromServer")).toEqual("updated");
    expect(mapOnClient.get("fromClient")).toEqual("updated");
    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Group sessions: header/5",
        "server -> client | KNOWN Group sessions: header/5",
        "client -> server | LOAD Map sessions: header/2",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN Map sessions: header/3",
        "client -> server | CONTENT Map header: false new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/3",
      ]
    `);
  });

  test("wrong optimistic known state should be corrected", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Load the coValue on the client
    await loadCoValueOrFail(client.node, map.id);

    // Forcefully delete the coValue from the client (simulating some data loss)
    client.node.coValuesStore.coValues.delete(map.id);

    map.set("fromServer", "updated", "trusting");

    await waitFor(() => {
      const coValue = expectMap(
        client.node.expectCoValueLoaded(map.id).getCurrentContent(),
      );
      expect(coValue.get("fromServer")).toEqual("updated");
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Group sessions: header/5",
        "server -> client | CONTENT Map header: true new: After: 0 New: 1",
        "client -> server | KNOWN Map sessions: header/1",
        "server -> client | CONTENT Map header: false new: After: 1 New: 1",
        "client -> server | KNOWN CORRECTION Map sessions: empty",
        "server -> client | CONTENT Map header: true new: After: 0 New: 2",
        "client -> server | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("unavailable coValue", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Makes the CoValues unavailable on the server
    jazzCloud.restart();

    const client = setupTestNode({
      connected: true,
    });

    // Load the coValue on the client
    const value = await client.node.load(map.id);
    expect(value).toEqual("unavailable");

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
        "client -> server | LOAD Map sessions: empty",
        "server -> client | KNOWN Map sessions: empty",
      ]
    `);
  });

  test("large coValue streaming", async () => {
    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const largeMap = group.createMap();

    // Generate a large amount of data (about 100MB)
    const dataSize = 1 * 1024 * 1024;
    const chunkSize = 1024; // 1KB chunks
    const chunks = dataSize / chunkSize;

    const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

    for (let i = 0; i < chunks; i++) {
      const key = `key${i}`;
      largeMap.set(key, value, "trusting");
    }

    const client = setupTestNode({
      connected: true,
    });

    await loadCoValueOrFail(client.node, largeMap.id);

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | LOAD Map sessions: empty",
        "server -> client | CONTENT Group header: true new: After: 0 New: 5",
        "client -> server | KNOWN Group sessions: header/5",
        "server -> client | CONTENT Map header: true new: ",
        "server -> client | CONTENT Map header: false new: After: 0 New: 73",
        "client -> server | KNOWN Map sessions: header/0",
        "server -> client | CONTENT Map header: false new: After: 73 New: 73",
        "server -> client | CONTENT Map header: false new: After: 146 New: 73",
        "client -> server | KNOWN Map sessions: header/73",
        "server -> client | CONTENT Map header: false new: After: 219 New: 73",
        "server -> client | CONTENT Map header: false new: After: 292 New: 73",
        "client -> server | KNOWN Map sessions: header/146",
        "server -> client | CONTENT Map header: false new: After: 365 New: 73",
        "server -> client | CONTENT Map header: false new: After: 438 New: 73",
        "client -> server | KNOWN Map sessions: header/219",
        "server -> client | CONTENT Map header: false new: After: 511 New: 73",
        "server -> client | CONTENT Map header: false new: After: 584 New: 73",
        "client -> server | KNOWN Map sessions: header/292",
        "server -> client | CONTENT Map header: false new: After: 657 New: 73",
        "server -> client | CONTENT Map header: false new: After: 730 New: 73",
        "client -> server | KNOWN Map sessions: header/365",
        "server -> client | CONTENT Map header: false new: After: 803 New: 73",
        "server -> client | CONTENT Map header: false new: After: 876 New: 73",
        "client -> server | KNOWN Map sessions: header/438",
        "server -> client | CONTENT Map header: false new: After: 949 New: 73",
        "server -> client | CONTENT Map header: false new: After: 1022 New: 2",
        "client -> server | KNOWN Map sessions: header/511",
        "client -> server | KNOWN Map sessions: header/584",
        "client -> server | KNOWN Map sessions: header/657",
        "client -> server | KNOWN Map sessions: header/730",
        "client -> server | KNOWN Map sessions: header/803",
        "client -> server | KNOWN Map sessions: header/876",
        "client -> server | KNOWN Map sessions: header/949",
        "client -> server | KNOWN Map sessions: header/1022",
        "client -> server | KNOWN Map sessions: header/1024",
      ]
    `);
  });

  test.todo("should mark the coValue as unavailable if the peer is closed");
});
