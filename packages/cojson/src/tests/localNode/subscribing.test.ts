import { describe, expect, test } from "vitest";
import { RawCoID } from "../../exports.js";
import { subscribe, unsubscribe } from "../../localNode/actions/subscribing.js";

import { emptyNode } from "../../localNode/structure.js";

describe("Subscribing to a CoValue", () => {
  test("creates an empty entry if none exists yet", () => {
    const node = emptyNode();
    const id = "co_fakeCoValueID" as RawCoID;
    const { listenerID } = subscribe(node, id);

    expect(listenerID).toBeDefined();

    expect(node.coValues[id]).toEqual({
      id,
      header: null,
      sessions: {},
      storageState: "unknown",
      peerState: {},
      listeners: { [listenerID]: "unknown" },
      incomingMessages: {},
      dependents: [],
    });
  });

  test("adds a listener if an entry already exists", () => {
    const node = emptyNode();
    const id = "co_fakeCoValueID" as RawCoID;
    const { listenerID: firstListenerID } = subscribe(node, id);
    const { listenerID: secondListenerID } = subscribe(node, id);

    expect(firstListenerID).toBeDefined();
    expect(secondListenerID).toBeDefined();
    expect(firstListenerID).not.toEqual(secondListenerID);

    expect(node.coValues[id]).toEqual({
      id,
      header: null,
      sessions: {},
      storageState: "unknown",
      peerState: {},
      listeners: {
        [firstListenerID]: "unknown",
        [secondListenerID]: "unknown",
      },
      dependents: [],
      incomingMessages: {},
    });
  });

  test("unsubscribing from a CoValue removes the listener", () => {
    const node = emptyNode();
    const id = "co_fakeCoValueID" as RawCoID;

    const { listenerID } = subscribe(node, id);
    expect(node.coValues[id].listeners[listenerID]).toBe("unknown");

    unsubscribe(node, id, listenerID);
    expect(node.coValues[id].listeners[listenerID]).toBeUndefined();
  });
});
