import { describe, expect, it } from "vitest";

import { traceDuration } from "./traces";

describe("traceDuration", () => {
  it("scales with length and clamps to sane defaults", () => {
    expect(traceDuration(0)).toBe(500); // min
    expect(traceDuration(100)).toBe(500); // 135 → clamped up
    expect(traceDuration(1000)).toBe(1350); // 1000 * 1.35
    expect(traceDuration(100_000)).toBe(2600); // max
  });

  it("honours per-diagram overrides (so TierSync/Lens keep their feel)", () => {
    expect(traceDuration(1000, { perPx: 1.43, min: 500, max: 3000 })).toBe(1430);
    expect(traceDuration(10, { min: 700 })).toBe(700);
    expect(traceDuration(5000, { max: 1040 })).toBe(1040);
  });
});
