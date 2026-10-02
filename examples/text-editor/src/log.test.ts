import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import { HEADER, frame, updates } from "./log.js";

// Yjs v1: client 1 inserts "hi" into the shared text named "text".
const UPDATE = new Uint8Array([1, 1, 1, 0, 4, 1, 4, 116, 101, 120, 116, 2, 104, 105, 0]);
const LOG = new Uint8Array([
  74, 89, 76, 71, 0, 0, 0, 1, 0, 0, 0, 15, 1, 1, 1, 0, 4, 1, 4, 116, 101, 120, 116, 2, 104, 105, 0,
]);

describe("Jazz Yjs Log v1", () => {
  it("pins the header, framing and Yjs payload bytes", () => {
    expect(new Uint8Array([...HEADER, ...frame(UPDATE)])).toEqual(LOG);
    const doc = new Y.Doc();
    doc.clientID = 1;
    let emitted: Uint8Array | undefined;
    doc.on("update", (update) => {
      emitted = update;
    });
    doc.getText("text").insert(0, "hi");
    expect(emitted).toEqual(UPDATE);
    doc.destroy();
  });

  it("replays a saved log, appended deletion, and duplicate delivery", () => {
    const source = new Y.Doc();
    for (const update of updates(LOG, true)) Y.applyUpdate(source, update);
    expect(source.getText("text").toString()).toBe("hi");
    let tail = new Uint8Array();
    source.on("update", (update) => {
      tail = new Uint8Array(frame(update));
    });
    source.getText("text").delete(0, 1);
    const loaded = new Y.Doc();
    for (const update of updates(new Uint8Array([...LOG, ...tail]), true)) {
      Y.applyUpdate(loaded, update);
    }
    for (const update of updates(tail, false)) Y.applyUpdate(loaded, update);
    expect(loaded.getText("text").toString()).toBe("i");
    source.destroy();
    loaded.destroy();
  });

  it("rejects unknown versions and incomplete records", () => {
    expect(() => updates(new Uint8Array([74, 89, 76, 71, 0, 0, 0, 2]), true)).toThrow(/header/);
    expect(() => updates(LOG.subarray(0, 10), true)).toThrow(/Truncated/);
    expect(() => updates(LOG.subarray(0, LOG.length - 1), true)).toThrow(/length/);
    expect(() => updates(new Uint8Array([0, 0, 0, 0]), false)).toThrow(/length/);
    expect(updates(HEADER, true)).toEqual([]);
  });
});
