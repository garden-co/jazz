import { describe, it, expect } from "vitest";
import {
  serializeActiveSubscriptions,
  type InspectorSubscription,
} from "./inspector-host-types.js";

describe("serializeActiveSubscriptions", () => {
  it("drops the stack and keeps the metadata", () => {
    const out = serializeActiveSubscriptions([
      {
        id: "s1",
        query: '{"from":"todos"}',
        table: "todos",
        branches: ["main"],
        tier: "edge",
        propagation: "full",
        createdAt: "2026-06-30T00:00:00.000Z",
        stack: "Error\n  at X",
      },
    ]);

    const expected: InspectorSubscription[] = [
      {
        id: "s1",
        query: '{"from":"todos"}',
        table: "todos",
        branches: ["main"],
        tier: "edge",
        propagation: "full",
        createdAt: "2026-06-30T00:00:00.000Z",
      },
    ];
    expect(out).toEqual(expected);
    expect("stack" in out[0]!).toBe(false);
  });

  it("returns an empty array for no traces", () => {
    expect(serializeActiveSubscriptions([])).toEqual([]);
  });
});
