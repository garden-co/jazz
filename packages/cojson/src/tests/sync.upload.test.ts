import { beforeEach, describe, expect, test } from "vitest";

import { expectMap } from "../coValue";
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

describe("client to server upload", () => {
  test("coValue uploading", async () => {
    const client = setupTestNode({
      connected: true,
    });

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
        "server -> client | KNOWN Group sessions: header/3",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with parent groups uploading", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    const parentGroup = client.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    group.extend(parentGroup);

    const map = group.createMap();
    map.set("hello", "world");

    await map.core.waitForSync();

    const mapOnServer = await loadCoValueOrFail(jazzCloud.node, map.id);
    expect(mapOnServer.get("hello")).toEqual("world");

    expect(
      SyncMessagesLog.getMessages({
        ParentGroup: parentGroup.core,
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT ParentGroup header: true new: After: 0 New: 6",
        "server -> client | KNOWN ParentGroup sessions: header/6",
        "client -> server | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | KNOWN Group sessions: header/5",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("wrong optimistic known state should be corrected", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Load the coValue on the client
    await map.core.waitForSync();

    // Forcefully delete the coValue from the client (simulating some data loss)
    jazzCloud.node.coValuesStore.coValues.delete(map.id);

    map.set("fromClient", "updated", "trusting");

    await waitFor(() => {
      const coValue = expectMap(
        jazzCloud.node.expectCoValueLoaded(map.id).getCurrentContent(),
      );
      expect(coValue.get("fromClient")).toEqual("updated");
    });

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: map.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | KNOWN Group sessions: header/5",
        "client -> server | CONTENT Map header: true new: After: 0 New: 1",
        "server -> client | KNOWN Map sessions: header/1",
        "client -> server | CONTENT Map header: false new: After: 1 New: 1",
        "server -> client | KNOWN CORRECTION Map sessions: empty",
        "client -> server | CONTENT Map header: true new: After: 0 New: 2",
        "server -> client | KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("large coValue upload streaming", async () => {
    const client = setupTestNode({
      connected: true,
    });

    const group = client.node.createGroup();
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

    await loadCoValueOrFail(client.node, largeMap.id);

    await largeMap.core.waitForSync();

    expect(
      SyncMessagesLog.getMessages({
        Group: group.core,
        Map: largeMap.core,
      }),
    ).toMatchInlineSnapshot(`
      [
        "client -> server | CONTENT Group header: true new: After: 0 New: 5",
        "server -> client | KNOWN Group sessions: header/5",
        "client -> server | CONTENT Map header: true new: ",
        "server -> client | KNOWN Map sessions: header/0",
        "client -> server | CONTENT Map header: false new: After: 0 New: 73",
        "server -> client | KNOWN Map sessions: header/73",
        "client -> server | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server | CONTENT Map header: false new: After: 146 New: 73",
        "server -> client | KNOWN Map sessions: header/146",
        "client -> server | CONTENT Map header: false new: After: 219 New: 73",
        "client -> server | CONTENT Map header: false new: After: 292 New: 73",
        "server -> client | KNOWN Map sessions: header/219",
        "client -> server | CONTENT Map header: false new: After: 365 New: 73",
        "client -> server | CONTENT Map header: false new: After: 438 New: 73",
        "server -> client | KNOWN Map sessions: header/292",
        "client -> server | CONTENT Map header: false new: After: 511 New: 73",
        "client -> server | CONTENT Map header: false new: After: 584 New: 73",
        "server -> client | KNOWN Map sessions: header/365",
        "client -> server | CONTENT Map header: false new: After: 657 New: 73",
        "client -> server | CONTENT Map header: false new: After: 730 New: 73",
        "server -> client | KNOWN Map sessions: header/438",
        "client -> server | CONTENT Map header: false new: After: 803 New: 73",
        "client -> server | CONTENT Map header: false new: After: 876 New: 73",
        "server -> client | KNOWN Map sessions: header/511",
        "client -> server | CONTENT Map header: false new: After: 949 New: 73",
        "client -> server | CONTENT Map header: false new: After: 1022 New: 2",
        "server -> client | KNOWN Map sessions: header/584",
        "server -> client | KNOWN Map sessions: header/657",
        "server -> client | KNOWN Map sessions: header/730",
        "server -> client | KNOWN Map sessions: header/803",
        "server -> client | KNOWN Map sessions: header/876",
        "server -> client | KNOWN Map sessions: header/949",
        "server -> client | KNOWN Map sessions: header/1022",
        "server -> client | KNOWN Map sessions: header/1024",
      ]
    `);
  });
});
