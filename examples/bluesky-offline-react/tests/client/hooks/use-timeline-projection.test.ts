import { describe, expect, it } from "vitest";
import {
  fetchingMorePosts,
  nextInfiniteScrollState,
  nextTimelinePageSource,
} from "../../../src/hooks/use-timeline-projection.js";

describe("timeline pagination", () => {
  it("reveals cached Jazz rows before asking the BFF for another page", () => {
    expect(
      nextTimelinePageSource({
        cachedRowsRemaining: true,
        localQueryRefreshing: false,
        remoteRowsRemaining: true,
      }),
    ).toBe("local");
  });

  it("waits for a refreshing Jazz query before asking the BFF for another page", () => {
    expect(
      nextTimelinePageSource({
        cachedRowsRemaining: false,
        localQueryRefreshing: true,
        remoteRowsRemaining: true,
      }),
    ).toBeUndefined();
  });

  it("asks the BFF only after cached Jazz rows are exhausted", () => {
    expect(
      nextTimelinePageSource({
        cachedRowsRemaining: false,
        localQueryRefreshing: false,
        remoteRowsRemaining: true,
      }),
    ).toBe("remote");
  });

  it("keeps pagination visible until a locally cached page appears", () => {
    expect(fetchingMorePosts({ localStartCount: 20, itemCount: 20, remote: false })).toBe(true);
    expect(fetchingMorePosts({ localStartCount: 20, itemCount: 40, remote: false })).toBe(false);
    expect(fetchingMorePosts({ localStartCount: null, itemCount: 40, remote: true })).toBe(true);
  });

  it("requires the sentinel to leave before triggering another page", () => {
    expect(nextInfiniteScrollState({ armed: true, intersecting: true, canLoad: true })).toEqual({
      armed: false,
      trigger: true,
    });
    expect(nextInfiniteScrollState({ armed: false, intersecting: true, canLoad: true })).toEqual({
      armed: false,
      trigger: false,
    });
    expect(nextInfiniteScrollState({ armed: false, intersecting: false, canLoad: true })).toEqual({
      armed: true,
      trigger: false,
    });
  });
});
