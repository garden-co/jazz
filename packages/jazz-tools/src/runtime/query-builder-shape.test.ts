import { describe, expect, it } from "vitest";
import { normalizeBuiltQuery } from "./query-builder-shape.js";

describe("query builder shape", () => {
  it("preserves branch and diff metadata during normalization", () => {
    const draftBranchId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const reviewBranchId = "01963f3e-5cbe-7a62-8d7c-abcdefabcdef";
    const normalized = normalizeBuiltQuery(
      {
        table: "todos",
        conditions: [],
        includes: {
          owner: {
            table: "users",
            conditions: [],
            includes: {},
            select: [],
            orderBy: [],
            branches: [draftBranchId],
            diff: true,
          },
        },
        orderBy: [],
        branches: [draftBranchId, 42, reviewBranchId],
        diff: true,
      },
      "",
    );

    expect(normalized.branches).toEqual([draftBranchId, reviewBranchId]);
    expect(normalized.diff).toBe(true);
    expect(normalized.includes.owner?.branches).toEqual([draftBranchId]);
    expect(normalized.includes.owner?.diff).toBe(true);
  });
});
