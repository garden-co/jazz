import { describe, expect, it } from "vitest";
import { nextTimelinePageSource } from "../../src/use-timeline-projection.js";

describe("timeline pagination", () => {
  it("reveals cached Jazz rows before asking the BFF for another page", () => {
    expect(nextTimelinePageSource({
      cachedRowsRemaining: true,
      localQueryRefreshing: false,
      remoteRowsRemaining: true,
    })).toBe("local");
  });

  it("waits for a refreshing Jazz query before asking the BFF for another page", () => {
    expect(nextTimelinePageSource({
      cachedRowsRemaining: false,
      localQueryRefreshing: true,
      remoteRowsRemaining: true,
    })).toBeUndefined();
  });

  it("asks the BFF only after cached Jazz rows are exhausted", () => {
    expect(nextTimelinePageSource({
      cachedRowsRemaining: false,
      localQueryRefreshing: false,
      remoteRowsRemaining: true,
    })).toBe("remote");
  });
});
