import { beforeEach, describe, expect, test } from "vitest";

import { expectList, expectMap } from "../coValue";
import { WasmCrypto } from "../crypto/WasmCrypto";
import {
  SyncMessagesLog,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils";

const Crypto = await WasmCrypto.create();
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

  test("syncing changes between two clients with a sync server in the middle", async () => {
    const client = setupTestNode({
      connected: true,
    });
    const otherClient = setupTestNode({});

    const otherClientConnection = otherClient.connectToSyncServer({
      ourName: "otherClient",
    });

    const coValue = client.node.createCoValue({
      type: "colist",
      ruleset: { type: "unsafeAllowAll" },
      meta: null,
      ...Crypto.createdNowUnique(),
    });

    const list = expectList(coValue.getCurrentContent());

    list.append(1, undefined, "trusting");

    const listOnOtherClient = await loadCoValueOrFail(
      otherClient.node,
      list.id,
    );

    otherClientConnection.peerState.gracefulShutdown();

    listOnOtherClient.append(1, undefined, "trusting");

    await new Promise((resolve) => setTimeout(resolve, 50));

    list.append(1, undefined, "trusting");

    await new Promise((resolve) => setTimeout(resolve, 50));

    listOnOtherClient.append(1, undefined, "trusting");

    await new Promise((resolve) => setTimeout(resolve, 50));

    list.append(1, undefined, "trusting");

    otherClient.connectToSyncServer({
      ourName: "otherClient",
    });

    await waitFor(() => {
      expect(list.toJSON()).toEqual([1, 1, 1, 1, 1]);
    });

    expect(
      SyncMessagesLog.getMessages({
        Colist: coValue,
      }),
    ).toMatchInlineSnapshot(`
      [
        "otherClient -> server | LOAD Colist sessions: empty",
        "client -> server | CONTENT Colist header: true new: After: 0 New: 1",
        "server -> otherClient | KNOWN Colist sessions: empty",
        "server -> client | KNOWN Colist sessions: header/1",
        "server -> otherClient | CONTENT Colist header: true new: After: 0 New: 1",
        "otherClient -> server | KNOWN Colist sessions: header/1",
        "client -> server | CONTENT Colist header: false new: After: 1 New: 1",
        "server -> client | KNOWN Colist sessions: header/2",
        "otherClient -> server | LOAD Colist sessions: header/3",
        "client -> server | CONTENT Colist header: false new: After: 2 New: 1",
        "server -> otherClient | CONTENT Colist header: false new: After: 1 New: 1",
        "server -> client | KNOWN Colist sessions: header/3",
        "otherClient -> server | KNOWN Colist sessions: header/4",
        "server -> otherClient | CONTENT Colist header: false new: After: 2 New: 1",
        "otherClient -> server | CONTENT Colist header: false new: After: 0 New: 2",
        "server -> otherClient | KNOWN Colist sessions: header/5",
        "server -> client | CONTENT Colist header: false new: After: 0 New: 2",
        "otherClient -> server | KNOWN Colist sessions: header/5",
        "client -> server | KNOWN Colist sessions: header/5",
      ]
    `);
  });

  test("large coValue upload streaming", async () => {
    const client = setupTestNode({
      connected: true,
    });

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
        "client -> server | KNOWN Map sessions: header/0",
        "server -> client | CONTENT Map header: false new: After: 0 New: 73",
        "client -> server | KNOWN Map sessions: header/73",
        "server -> client | CONTENT Map header: false new: After: 73 New: 73",
        "client -> server | KNOWN Map sessions: header/146",
        "server -> client | CONTENT Map header: false new: After: 146 New: 73",
        "client -> server | KNOWN Map sessions: header/219",
        "server -> client | CONTENT Map header: false new: After: 219 New: 73",
        "client -> server | KNOWN Map sessions: header/292",
        "server -> client | CONTENT Map header: false new: After: 292 New: 73",
        "client -> server | KNOWN Map sessions: header/365",
        "server -> client | CONTENT Map header: false new: After: 365 New: 73",
        "client -> server | KNOWN Map sessions: header/438",
        "server -> client | CONTENT Map header: false new: After: 438 New: 73",
        "client -> server | KNOWN Map sessions: header/511",
        "server -> client | CONTENT Map header: false new: After: 511 New: 73",
        "client -> server | KNOWN Map sessions: header/584",
        "server -> client | CONTENT Map header: false new: After: 584 New: 73",
        "client -> server | KNOWN Map sessions: header/657",
        "server -> client | CONTENT Map header: false new: After: 657 New: 73",
        "client -> server | KNOWN Map sessions: header/730",
        "server -> client | CONTENT Map header: false new: After: 730 New: 73",
        "client -> server | KNOWN Map sessions: header/803",
        "server -> client | CONTENT Map header: false new: After: 803 New: 73",
        "client -> server | KNOWN Map sessions: header/876",
        "server -> client | CONTENT Map header: false new: After: 876 New: 73",
        "client -> server | KNOWN Map sessions: header/949",
        "server -> client | CONTENT Map header: false new: After: 949 New: 73",
        "client -> server | KNOWN Map sessions: header/1022",
        "server -> client | CONTENT Map header: false new: After: 1022 New: 2",
        "client -> server | KNOWN Map sessions: header/1024",
      ]
    `);
  });
});
