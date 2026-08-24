import { describe, expect, it } from "vitest";
import { TOPOLOGY_SEED } from "./topology-seed.js";
import { assertWorldTourTopologyContract } from "./topology-contract.js";

describe("WorldTour browser topology contract", () => {
  it("registers the complete Jazz command adapter and injects a safe seed", () => {
    expect(Number.isSafeInteger(TOPOLOGY_SEED)).toBe(true);
    assertWorldTourTopologyContract();
  });
});
