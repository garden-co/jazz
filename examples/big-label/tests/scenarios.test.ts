import { describe, expect, it } from "vitest";
import { createFixture } from "../src/fixtures.js";
import { tenantOperations } from "../src/scenarios.js";

describe("BigLabel public workload fixtures", () => {
  it("is deterministic and keeps an owned slice stable", () => {
    expect(createFixture("small", 44)).toEqual(createFixture("small", 44));
    expect(tenantOperations("small", 44)).toMatchObject({
      visibleArtists: 8,
      visibleReleases: 8,
      foreignRows: 0,
    });
  });
  it("asserts foreign-tenant isolation for every supported profile", () => {
    for (const profile of ["smoke", "small", "scaled"] as const)
      // This verifies only deterministic fixture construction. Deployed
      // tenant isolation is covered by authority.server.test.ts, where a
      // foreign client queries the real edge.
      expect(tenantOperations(profile).foreignRows).toBe(0);
  });
});
