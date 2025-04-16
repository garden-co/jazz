import { beforeEach, describe, expect, test } from "vitest";

import { expectMap } from "../coValue";
import { toSimplifiedMessages } from "./messagesTestUtils";
import {
  connectNodeToSyncServer,
  createConnectedTestAgentNode,
  loadCoValueOrFail,
  setupSyncServer,
  waitFor,
} from "./testUtils";

let jazzCloud = setupSyncServer();

beforeEach(async () => {
  jazzCloud = setupSyncServer();
});

describe("sync protocol", () => {
  test("coValue loading", async () => {
    const { node: client, messages } = await createConnectedTestAgentNode();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "server -> CONTENT Group header: true new: After: 0 New: 3",
        "client -> KNOWN Group sessions: header/3",
        "server -> CONTENT Map header: true new: After: 0 New: 1",
        "client -> KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("coValue with parent groups loading", async () => {
    const { node: client, messages } = await createConnectedTestAgentNode();

    const group = jazzCloud.node.createGroup();
    const parentGroup = jazzCloud.node.createGroup();
    parentGroup.addMember("everyone", "reader");

    group.extend(parentGroup);

    const map = group.createMap();
    map.set("hello", "world");

    const mapOnClient = await loadCoValueOrFail(client, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    expect(
      toSimplifiedMessages(
        {
          ParentGroup: parentGroup.core,
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "server -> CONTENT ParentGroup header: true new: After: 0 New: 6",
        "client -> KNOWN ParentGroup sessions: header/6",
        "server -> CONTENT Group header: true new: After: 0 New: 5",
        "client -> KNOWN Group sessions: header/5",
        "server -> CONTENT Map header: true new: After: 0 New: 1",
        "client -> KNOWN Map sessions: header/1",
      ]
    `);
  });

  test("updating a coValue while offline", async () => {
    const { node: client } = await createConnectedTestAgentNode();

    const group = jazzCloud.node.createGroup();
    const map = group.createMap();
    map.set("hello", "world", "trusting");

    const mapOnClient = await loadCoValueOrFail(client, map.id);
    expect(mapOnClient.get("hello")).toEqual("world");

    client.syncManager.getPeers()[0]?.gracefulShutdown();

    map.set("hello", "updated", "trusting");

    const { messages } = connectNodeToSyncServer(client, true);

    await map.core.waitForSync();

    expect(mapOnClient.get("hello")).toEqual("updated");

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/3",
        "server -> KNOWN Group sessions: header/3",
        "client -> LOAD Map sessions: header/1",
        "server -> CONTENT Map header: false new: After: 1 New: 1",
        "client -> KNOWN Map sessions: header/2",
      ]
    `);
  });

  test("updating a coValue on both sides while offline", async () => {
    const { node: client } = await createConnectedTestAgentNode();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    const mapOnClient = await loadCoValueOrFail(client, map.id);

    client.syncManager.getPeers()[0]?.gracefulShutdown();

    map.set("fromServer", "updated", "trusting");
    mapOnClient.set("fromClient", "updated", "trusting");

    const { messages } = connectNodeToSyncServer(client, true);

    await map.core.waitForSync();
    await mapOnClient.core.waitForSync();

    expect(mapOnClient.get("fromServer")).toEqual("updated");
    expect(mapOnClient.get("fromClient")).toEqual("updated");
    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Group sessions: header/5",
        "server -> KNOWN Group sessions: header/5",
        "client -> LOAD Map sessions: header/2",
        "server -> CONTENT Map header: false new: After: 1 New: 1",
        "client -> KNOWN Map sessions: header/3",
        "client -> CONTENT Map header: false new: After: 0 New: 1",
        "server -> KNOWN Map sessions: header/3",
      ]
    `);
  });

  test("wrong optimistic known state should be corrected", async () => {
    const { node: client, messages } = await createConnectedTestAgentNode();

    const group = jazzCloud.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap({
      fromServer: "initial",
      fromClient: "initial",
    });

    // Load the coValue on the client
    await loadCoValueOrFail(client, map.id);

    // Forcefully delete the coValue from the client (simulating some data loss)
    client.coValuesStore.coValues.delete(map.id);

    map.set("fromServer", "updated", "trusting");

    await waitFor(() => {
      const coValue = expectMap(
        client.expectCoValueLoaded(map.id).getCurrentContent(),
      );
      expect(coValue.get("fromServer")).toEqual("updated");
    });

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "server -> CONTENT Group header: true new: After: 0 New: 5",
        "client -> KNOWN Group sessions: header/5",
        "server -> CONTENT Map header: true new: After: 0 New: 1",
        "client -> KNOWN Map sessions: header/1",
        "server -> CONTENT Map header: false new: After: 1 New: 1",
        "client -> KNOWN CORRECTION Map sessions: empty",
        "server -> CONTENT Map header: true new: After: 0 New: 2",
        "client -> KNOWN Map sessions: header/2",
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

    const { node: client, messages } = await createConnectedTestAgentNode();

    // Load the coValue on the client
    const value = await client.load(map.id);
    expect(value).toEqual("unavailable");

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: map.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "server -> KNOWN Map sessions: empty",
        "client -> LOAD Map sessions: empty",
        "server -> KNOWN Map sessions: empty",
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

    const { node: client, messages } = await createConnectedTestAgentNode();

    await loadCoValueOrFail(client, largeMap.id);

    await largeMap.core.waitForSync();

    expect(
      toSimplifiedMessages(
        {
          Group: group.core,
          Map: largeMap.core,
        },
        messages,
      ),
    ).toMatchInlineSnapshot(`
      [
        "client -> LOAD Map sessions: empty",
        "server -> CONTENT Group header: true new: After: 0 New: 5",
        "client -> KNOWN Group sessions: header/5",
        "server -> CONTENT Map header: true new: ",
        "client -> KNOWN Map sessions: header/0",
        "server -> CONTENT Map header: false new: After: 0 New: 73",
        "client -> KNOWN Map sessions: header/73",
        "server -> CONTENT Map header: false new: After: 73 New: 73",
        "client -> KNOWN Map sessions: header/146",
        "server -> CONTENT Map header: false new: After: 146 New: 73",
        "client -> KNOWN Map sessions: header/219",
        "server -> CONTENT Map header: false new: After: 219 New: 73",
        "client -> KNOWN Map sessions: header/292",
        "server -> CONTENT Map header: false new: After: 292 New: 73",
        "client -> KNOWN Map sessions: header/365",
        "server -> CONTENT Map header: false new: After: 365 New: 73",
        "client -> KNOWN Map sessions: header/438",
        "server -> CONTENT Map header: false new: After: 438 New: 73",
        "client -> KNOWN Map sessions: header/511",
        "server -> CONTENT Map header: false new: After: 511 New: 73",
        "client -> KNOWN Map sessions: header/584",
        "server -> CONTENT Map header: false new: After: 584 New: 73",
        "client -> KNOWN Map sessions: header/657",
        "server -> CONTENT Map header: false new: After: 657 New: 73",
        "client -> KNOWN Map sessions: header/730",
        "server -> CONTENT Map header: false new: After: 730 New: 73",
        "client -> KNOWN Map sessions: header/803",
        "server -> CONTENT Map header: false new: After: 803 New: 73",
        "client -> KNOWN Map sessions: header/876",
        "server -> CONTENT Map header: false new: After: 876 New: 73",
        "client -> KNOWN Map sessions: header/949",
        "server -> CONTENT Map header: false new: After: 949 New: 73",
        "client -> KNOWN Map sessions: header/1022",
        "server -> CONTENT Map header: false new: After: 1022 New: 2",
        "client -> KNOWN Map sessions: header/1024",
      ]
    `);
  });
});
