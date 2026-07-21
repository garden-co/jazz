import { describe, expect, it } from "vitest";
import { shouldApplyPaginationCursor } from "../../src/use-timeline-hydration.js";

describe("timeline pagination", () => {
  it("does not rewind pagination when a head refresh finishes", () => {
    expect(shouldApplyPaginationCursor(null, true)).toBe(false);
    expect(shouldApplyPaginationCursor("older", true)).toBe(true);
    expect(shouldApplyPaginationCursor(null, false)).toBe(true);
  });
});
