import { describe, expect, test } from "vitest";
import { branchPath, latestLeaf, siblingsOf, type TurnNode } from "../src/ui/transcript";

const turn = (
  id: string,
  parentId: string | undefined,
  role: TurnNode["role"],
  at: number,
): TurnNode => ({
  id,
  parentId,
  role,
  createdAt: new Date(at),
});

// u1 → a1 → u2 → a2
//    ↘ a1b (regenerated) → u3 → a3
const turns = [
  turn("u1", undefined, "user", 1),
  turn("a1", "u1", "assistant", 2),
  turn("u2", "a1", "user", 3),
  turn("a2", "u2", "assistant", 4),
  turn("a1b", "u1", "assistant", 5),
  turn("u3", "a1b", "user", 6),
  turn("a3", "u3", "assistant", 7),
];

describe("conversation branches", () => {
  test("the path follows parents from the head back to the first turn", () => {
    expect(branchPath(turns, "a2").map((t) => t.id)).toEqual(["u1", "a1", "u2", "a2"]);
    expect(branchPath(turns, "a3").map((t) => t.id)).toEqual(["u1", "a1b", "u3", "a3"]);
    expect(branchPath(turns, undefined)).toEqual([]);
  });

  test("regenerated replies are siblings in creation order", () => {
    expect(siblingsOf(turns, turns[1]!).map((t) => t.id)).toEqual(["a1", "a1b"]);
  });

  test("switching to a sibling continues on its newest leaf", () => {
    expect(latestLeaf(turns, "a1")).toBe("a2");
    expect(latestLeaf(turns, "a1b")).toBe("a3");
    expect(latestLeaf(turns, "a3")).toBe("a3");
  });
});
