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

  it("merges a batched replay's structs as the writer and an update-by-update replay do", () => {
    // patches/yjs@13.6.33.patch compacts the struct store in one pass after a
    // transaction with many deletions; its result must not change.
    const layout = (doc: Y.Doc) =>
      [...doc.store.clients.values()]
        .map((structs) => structs.map((s) => `${s.id.clock}+${s.length}${s.deleted ? "d" : ""}`))
        .join("|");
    let seed = 7;
    const random = (n: number) => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed % n;
    };
    const source = new Y.Doc();
    source.clientID = 1;
    const records: Uint8Array[] = [];
    source.on("update", (update: Uint8Array) => records.push(update));
    const text = source.getText("text");
    for (let i = 0; i < 3000; i++) {
      if (text.length && random(3) === 0) {
        const at = random(text.length);
        text.delete(at, 1 + random(Math.min(5, text.length - at)));
      } else text.insert(random(text.length + 1), "xy".slice(0, 1 + random(2)));
    }
    const batched = new Y.Doc();
    batched.transact(() => applyUpdates(batched, new Uint8Array(records.flatMap((r) => [...r]))));
    const each = new Y.Doc();
    for (const record of records) applyUpdates(each, record);
    expect(batched.getText("text").toString()).toBe(text.toString());
    expect(layout(batched)).toBe(layout(source));
    expect(layout(each)).toBe(layout(source));
    for (const doc of [source, batched, each]) doc.destroy();
  });
});
