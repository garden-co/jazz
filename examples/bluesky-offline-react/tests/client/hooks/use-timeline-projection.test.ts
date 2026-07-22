import { describe, expect, it } from "vitest";
import {
  canLoadNextPage,
  fetchingMorePosts,
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
    expect(fetchingMorePosts({ startCount: 20, rowCount: 20, remote: false })).toBe(true);
    expect(fetchingMorePosts({ startCount: 20, rowCount: 40, remote: false })).toBe(false);
    expect(fetchingMorePosts({ startCount: null, rowCount: 40, remote: true })).toBe(true);
  });

  it("enables explicit pagination only when another page is ready", () => {
    expect(canLoadNextPage({ source: "local", loadingMore: false })).toBe(true);
    expect(canLoadNextPage({ source: "remote", loadingMore: true })).toBe(false);
    expect(canLoadNextPage({ source: undefined, loadingMore: false })).toBe(false);
  });
});
