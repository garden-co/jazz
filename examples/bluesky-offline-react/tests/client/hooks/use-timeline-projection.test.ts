import { describe, expect, it } from "vitest";
import {
  canLoadNextPage,
  needsMoreRootCards,
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

  it("keeps loading until twenty new root cards appear", () => {
    expect(needsMoreRootCards({ itemCount: 20, targetItemCount: 40 })).toBe(true);
    expect(needsMoreRootCards({ itemCount: 39, targetItemCount: 40 })).toBe(true);
    expect(needsMoreRootCards({ itemCount: 40, targetItemCount: 40 })).toBe(false);
  });

  it("enables explicit pagination only when another page is ready", () => {
    expect(canLoadNextPage({ source: "local", loadingMore: false })).toBe(true);
    expect(canLoadNextPage({ source: "remote", loadingMore: true })).toBe(false);
    expect(canLoadNextPage({ source: undefined, loadingMore: false })).toBe(false);
  });
});
