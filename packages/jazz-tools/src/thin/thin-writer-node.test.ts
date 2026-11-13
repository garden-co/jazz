import { describe, expect, it } from "vitest";
import { boundedWriterNode } from "./thin-runtime.js";

// The authority drops the dedupe entries of a thin runtime's writes only
// when its writeKey node carries this mark (SPEC 8 §8.14; `jzRB` in
// `crates/jazz/layers/db/src/sync.rs`, `bounds_resends`): it is the
// runtime's promise to stop resending a write 10 minutes after its first send.
describe("thin writer node", () => {
  it("is marked as bounding its resends: jzRB, then a version-8 UUID", () => {
    for (let i = 0; i < 64; i++) {
      const node = boundedWriterNode();
      expect(node).toHaveLength(16);
      expect(Array.from(node.slice(0, 4))).toEqual([0x6a, 0x7a, 0x52, 0x42]);
      expect(node[6]! >> 4).toBe(8);
      expect(node[8]! >> 6).toBe(0b10);
    }
  });

  it("is random past its mark", () => {
    const a = boundedWriterNode();
    const b = boundedWriterNode();
    expect(Array.from(a.slice(9))).not.toEqual(Array.from(b.slice(9)));
  });
});
