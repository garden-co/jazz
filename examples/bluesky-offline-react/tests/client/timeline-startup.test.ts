import { describe, expect, it } from "vitest";
import { shouldStartTimelineHydration } from "../../src/timeline-startup.js";

describe("timeline startup", () => {
  it("waits for the local Jazz query before triggering BFF hydration", () => {
    expect(shouldStartTimelineHydration(false, true)).toBe(false);
    expect(shouldStartTimelineHydration(true, true)).toBe(true);
  });

  it("does not trigger hydration while the browser is offline", () => {
    expect(shouldStartTimelineHydration(true, false)).toBe(false);
  });
});
