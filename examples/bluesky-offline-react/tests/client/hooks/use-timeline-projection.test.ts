import { describe, expect, it } from "vitest";
import {
  fetchingMorePosts,
  nextTimelinePageSource,
  shouldLoadNextPage,
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

  it("loads again while the sentinel remains visible once the previous page settles", () => {
    expect(shouldLoadNextPage({ intersecting: true, canLoad: true, loadingMore: false })).toBe(
      true,
    );
    expect(shouldLoadNextPage({ intersecting: true, canLoad: true, loadingMore: true })).toBe(
      false,
    );
    expect(shouldLoadNextPage({ intersecting: true, canLoad: true, loadingMore: false })).toBe(
      true,
    );
  });
});
