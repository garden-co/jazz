import { describe, expect, it } from "vitest";
import { SEARCH_EVENT, SEARCH_NO_RESULTS_EVENT, searchEvent } from "./search-analytics";

describe("searchEvent", () => {
  it("records the normalized query and its source", () => {
    expect(searchEvent("  Row  Policies ", "ui", 4)).toEqual({
      name: SEARCH_EVENT,
      properties: { query: "row policies", source: "ui" },
    });
  });

  it("uses a separate event for searches without results", () => {
    expect(searchEvent("zzqxv", "api", 0)?.name).toBe(SEARCH_NO_RESULTS_EVENT);
  });

  it("skips queries too short to mean anything", () => {
    expect(searchEvent("a", "ui", 3)).toBeNull();
    expect(searchEvent("   ", "api", 0)).toBeNull();
  });

  it("caps the query at Vercel's 255 characters", () => {
    expect(searchEvent("x".repeat(300), "api", 1)?.properties.query).toHaveLength(255);
  });
});
