import { describe, expect, it } from "vitest";
import * as Y from "yjs";
import { applyUpdates } from "./log.js";

// Yjs v1: client 1 inserts "hi" into the shared text named "text".
const UPDATE = new Uint8Array([1, 1, 1, 0, 4, 1, 4, 116, 101, 120, 116, 2, 104, 105, 0]);
describe("Yjs update log", () => {
  it("pins the Yjs payload bytes", () => {
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
    applyUpdates(source, UPDATE);
    expect(source.getText("text").toString()).toBe("hi");
    let tail = new Uint8Array();
    source.on("update", (update) => {
      tail = new Uint8Array(update);
    });
    source.getText("text").delete(0, 1);
    const loaded = new Y.Doc();
    applyUpdates(loaded, new Uint8Array([...UPDATE, ...tail]));
    applyUpdates(loaded, tail);
    expect(loaded.getText("text").toString()).toBe("i");
    source.destroy();
    loaded.destroy();
  });

  it("accepts an empty log and rejects truncated updates", () => {
    const empty = new Y.Doc();
    expect(() => applyUpdates(empty, new Uint8Array())).not.toThrow();
    empty.destroy();
    for (let end = 1; end < UPDATE.length; end++) {
      const doc = new Y.Doc();
      expect(() => applyUpdates(doc, UPDATE.subarray(0, end))).toThrow();
      doc.destroy();
    }
  });

  it("preserves individual updates and their origin across a concatenated tail", () => {
    const source = new Y.Doc();
    const records: Uint8Array[] = [];
    source.on("update", (update: Uint8Array) => records.push(update));
    source.getText("text").insert(0, "a");
    source.getText("text").insert(1, "longer paste");
    source.getText("text").delete(0, 1);
    const loaded = new Y.Doc();
    const origin = Symbol("remote");
    const origins: unknown[] = [];
    loaded.on("update", (_update, source) => origins.push(source));
    applyUpdates(loaded, records[0]!, origin);
    const tail = new Uint8Array(records.slice(1).flatMap((record) => [...record]));
    applyUpdates(loaded, tail, origin);
    expect(loaded.getText("text").toString()).toBe("longer paste");
    expect(origins).toEqual([origin, origin, origin]);
    applyUpdates(loaded, tail, origin);
    expect(origins).toHaveLength(3);
    source.destroy();
    loaded.destroy();
  });
});
