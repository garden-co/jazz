import { describe, expect, it } from "vitest";

import { bfsWaves } from "./presets";

// Abstract graph mechanics — terse ids are appropriate.
describe("bfsWaves", () => {
  it("groups directed hops into breadth-first waves from a start node", () => {
    const adjacency = {
      global: ["edge1", "edge2"],
      edge1: ["alice", "bob", "global"],
      edge2: ["charlie", "global"],
      alice: ["edge1"],
      bob: ["edge1"],
      charlie: ["edge2"],
    };
    // A write at alice propagates: alice→edge1, then edge1→{bob,global},
    // then global→edge2, then edge2→charlie.
    expect(bfsWaves(adjacency, "alice")).toEqual([
      [{ from: "alice", to: "edge1" }],
      [
        { from: "edge1", to: "bob" },
        { from: "edge1", to: "global" },
      ],
      [{ from: "global", to: "edge2" }],
      [{ from: "edge2", to: "charlie" }],
    ]);
  });

  it("visits every node once (no revisits, terminates on cycles)", () => {
    const adjacency = { a: ["b"], b: ["a", "c"], c: ["b"] };
    expect(bfsWaves(adjacency, "a")).toEqual([[{ from: "a", to: "b" }], [{ from: "b", to: "c" }]]);
  });

  it("returns no waves for an isolated start", () => {
    expect(bfsWaves({ a: [], b: ["a"] }, "a")).toEqual([]);
  });
});
