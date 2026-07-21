import { describe, expect, it } from "vitest";
import {
  initialTimelineLimit,
  nextTimelineLimit,
  timelineQueryLimit,
  windowTimelineRows,
} from "../../../src/model/timeline-data.js";

describe("local timeline window", () => {
  it("reveals cached roots 20 at a time while querying one look-ahead row", () => {
    const cachedRoots = Array.from({ length: 41 }, (_, index) => index);

    expect(initialTimelineLimit).toBe(20);
    expect(timelineQueryLimit(initialTimelineLimit)).toBe(21);
    expect(windowTimelineRows(cachedRoots.slice(0, 21), initialTimelineLimit)).toEqual({
      rows: cachedRoots.slice(0, 20),
      hasMore: true,
    });

    const secondPageLimit = nextTimelineLimit(initialTimelineLimit);
    expect(secondPageLimit).toBe(40);
    expect(timelineQueryLimit(secondPageLimit)).toBe(41);
  });
});
